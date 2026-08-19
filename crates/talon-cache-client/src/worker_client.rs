//! Data-plane client for fetching byte ranges from a worker.
//!
//! Where the [`CoordinatorClient`](crate::CoordinatorClient) answers *where* a
//! block lives, [`WorkerClient`] fetches the bytes. It speaks the data plane: a
//! single [`MsgType::GetRange`] frame whose body is a bincode
//! [`RangeRequest`] (object + `[offset, len)`), and a reply that is a
//! `GetRange` frame carrying the **raw range bytes** — or, if the
//! [`Flags::ERROR`] bit is set, a typed error envelope (or a legacy string).
//!
//! The response body is raw (no bincode envelope) precisely so a production
//! worker can `sendfile` the range straight from a file into the socket; this
//! client only needs to read the framed bytes back. A fresh TCP connection is
//! opened per fetch for simplicity, mirroring
//! [`CoordinatorClient`](crate::CoordinatorClient).

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use talon_core::{BlockId, ObjectId, RequestId, TenantId, Version};
use talon_transport::frame::{FrameHeader, MsgType, HEADER_LEN};
use talon_transport::{
    decode_error_payload, encode_cached_block_put_header, encode_cached_request,
    encode_cached_tenant_request, encode_delete, encode_put_header, encode_request,
    encode_tenant_request, CachedBlockPutRequest, CachedRangeRequest, DataPlaneError, DeleteRequest,
    Flags, PutRequest, RangeRequest, TenantScopedCachedRange, TenantScopedRange,
    MAX_CONTROL_PAYLOAD_LEN,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::pool::ConnectionPool;

/// Errors from a worker range fetch.
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    /// Failed to connect or an I/O error mid-request.
    #[error("worker I/O: {0}")]
    Io(#[from] std::io::Error),
    /// The request frame could not be encoded.
    #[error("worker request encode: {0}")]
    Encode(#[from] talon_transport::DataError),
    /// The reply frame header was invalid.
    #[error("worker frame: {0}")]
    Frame(#[from] talon_transport::FrameError),
    /// The worker replied with a non-`GetRange` frame.
    #[error("expected a GetRange reply, got {0:?}")]
    NotGetRange(MsgType),
    /// A successful range reply did not contain exactly the requested bytes.
    #[error("worker range length mismatch: expected {expected}, got {actual}")]
    RangeLengthMismatch {
        /// Number of bytes requested from the worker.
        expected: u64,
        /// Number of bytes advertised or returned by the worker.
        actual: u64,
    },
    /// A non-range payload exceeded the small-message safety cap.
    #[error("worker payload length {length} exceeds cap {cap}")]
    PayloadTooLarge {
        /// Number of bytes advertised by the worker.
        length: u32,
        /// Maximum accepted payload size for this reply.
        cap: u32,
    },
    /// The worker set the ERROR flag and returned this typed or legacy error.
    #[error("worker error: {0}")]
    Remote(DataPlaneError),
}

impl WorkerError {
    /// True when the failure is transport-level (the socket died) rather than
    /// the worker answering with a refusal.
    ///
    /// This is the retry gate for pooled connections. A *reused* connection may
    /// have been closed by the peer while it sat idle (server idle timeout,
    /// restart), so a transport failure on one is retried once on a fresh dial
    /// — see [`ConnectionPool::checkout`]. Every other variant means the worker
    /// answered and refused; re-asking the same peer would only repeat the
    /// refusal, so those propagate immediately.
    ///
    /// Matched exhaustively on purpose: a new variant has to be classified
    /// here, in one place, instead of silently defaulting at each call site.
    pub(crate) fn is_transport_failure(&self) -> bool {
        match self {
            WorkerError::Io(_) => true,
            WorkerError::Encode(_)
            | WorkerError::Frame(_)
            | WorkerError::NotGetRange(_)
            | WorkerError::RangeLengthMismatch { .. }
            | WorkerError::PayloadTooLarge { .. }
            | WorkerError::Remote(_) => false,
        }
    }
}

/// A thin data-plane client bound to one worker address.
///
/// Reuses connections from a shared [`ConnectionPool`] so warm fetches skip the
/// TCP handshake (issue #181). Cloneable — clones share the same pool.
#[derive(Debug, Clone)]
pub struct WorkerClient {
    addr: String,
    pool: Arc<ConnectionPool>,
    /// Tenant every fetch from this client is attributed to. `Unattributed`
    /// unless set with [`with_tenant`](Self::with_tenant).
    tenant: TenantId,
}

impl WorkerClient {
    /// Create a client that dials `addr` (`host:port`), with its own pool.
    ///
    /// Prefer [`with_pool`](Self::with_pool) when several clients should share a
    /// pool (the normal case on the read path); this convenience constructor is
    /// for one-off use and tests.
    pub fn new(addr: impl Into<String>) -> Self {
        Self::with_pool(addr, Arc::new(ConnectionPool::new()))
    }

    /// Create a client that reuses connections from the shared `pool`.
    pub fn with_pool(addr: impl Into<String>, pool: Arc<ConnectionPool>) -> Self {
        Self {
            addr: addr.into(),
            pool,
            tenant: TenantId::Unattributed,
        }
    }

    /// The worker address this client talks to.
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Attribute every fetch from this client to `tenant` for per-tenant QoS.
    ///
    /// A named tenant is sent as a `GetRangeTenant` frame; the default
    /// (`Unattributed`) sends an ordinary `GetRange`. Send a named tenant only
    /// to workers known to understand the type — an older worker fails the
    /// request closed rather than misreading it.
    pub fn with_tenant(mut self, tenant: TenantId) -> Self {
        self.tenant = tenant;
        self
    }

    /// Encode a range request, as a tenant-scoped frame when this client is
    /// bound to a named tenant, else as a plain `GetRange`.
    fn encode_range_request(
        &self,
        request_id: u32,
        request: RangeRequest,
    ) -> Result<Vec<u8>, talon_transport::DataError> {
        if self.tenant.is_unattributed() {
            encode_request(request_id, &request)
        } else {
            encode_tenant_request(
                request_id,
                &TenantScopedRange {
                    tenant: self.tenant.clone(),
                    request,
                },
            )
        }
    }

    /// Encode a cache-only range request, as a tenant-scoped frame when this
    /// client is bound to a named tenant, else as a plain `GetCachedRange`. A
    /// named tenant keeps resident-only reads attributed so they are metered by
    /// the worker like any other read.
    fn encode_cached_range_request(
        &self,
        request_id: u32,
        request: CachedRangeRequest,
    ) -> Result<Vec<u8>, talon_transport::DataError> {
        if self.tenant.is_unattributed() {
            encode_cached_request(request_id, &request)
        } else {
            encode_cached_tenant_request(
                request_id,
                &TenantScopedCachedRange {
                    tenant: self.tenant.clone(),
                    request,
                },
            )
        }
    }

    /// Fetch `[offset, offset+len)` of `object` from the worker.
    ///
    /// Returns the raw range bytes on success. A worker-side error (block not
    /// present, backend failure, etc.) surfaces as [`WorkerError::Remote`] with
    /// a stable code when the worker supports typed errors. Legacy string-only
    /// replies remain [`talon_transport::DataErrorCode::Unknown`] and must not
    /// be classified by matching their text.
    ///
    /// Uses a pooled connection when one is warm; the connection is returned to
    /// the pool only after a fully successful exchange, so a broken socket is
    /// never reused (a peer failure still surfaces as an I/O error for the
    /// caller's fallback logic).
    pub async fn fetch_range(
        &self,
        object: &ObjectId,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, WorkerError> {
        let req = RangeRequest {
            object: object.clone(),
            offset,
            len,
        };
        // Allocate a correlation id and put it on the wire, so the worker's
        // logs for this fetch can be joined with the client's (#304).
        let req_id = RequestId::next();
        let out = self.encode_range_request(req_id.0, req)?;
        // Try a pooled connection first; if it was reused and fails with an I/O
        // error (the peer may have closed it while idle), retry once on a fresh
        // dial so a stale pooled socket never turns a healthy peer into a
        // spurious failure. A failure on a fresh connection, or a non-I/O error
        // on a reused one (the peer answered but refused/rejected the request),
        // propagates immediately rather than re-asking the same peer.
        match self.exchange(&out, len).await {
            Ok(bytes) => Ok(bytes),
            Err((true, err)) if err.is_transport_failure() => {
                let mut stream = self.pool.fresh(&self.addr).await?;
                let bytes = self
                    .pool
                    .with_request_deadline("worker fetch_range retry", async {
                        stream.write_all(&out).await?;
                        stream.flush().await?;
                        read_range_reply(&mut stream, len).await
                    })
                    .await?;
                self.pool.release(&self.addr, stream);
                Ok(bytes)
            }
            Err((_, err)) => {
                tracing::error!(
                    req = %req_id,
                    worker = %self.addr,
                    object = %object.to_path(),
                    offset,
                    len,
                    error = %err,
                    "worker range fetch failed"
                );
                Err(err)
            }
        }
    }

    /// Fetch `[offset, offset + dst.len())` of `object` into `dst`.
    ///
    /// This avoids allocating an intermediate range buffer for callers that
    /// retain ownership of their destination storage, such as C bindings.
    pub async fn fetch_range_into(
        &self,
        object: &ObjectId,
        offset: u64,
        dst: &mut [u8],
    ) -> Result<usize, WorkerError> {
        let request = RangeRequest {
            object: object.clone(),
            offset,
            len: dst.len() as u64,
        };
        let request_id = RequestId::next();
        let output = self.encode_range_request(request_id.0, request)?;
        match self.exchange_into(&output, dst).await {
            Ok(n) => Ok(n),
            Err((true, err)) if err.is_transport_failure() => {
                let mut stream = self.pool.fresh(&self.addr).await?;
                let n = self
                    .pool
                    .with_request_deadline("worker fetch_range retry", async {
                        stream.write_all(&output).await?;
                        stream.flush().await?;
                        read_range_reply_into(&mut stream, dst).await
                    })
                    .await?;
                self.pool.release(&self.addr, stream);
                Ok(n)
            }
            Err((_, error)) => {
                tracing::error!(
                    req = %request_id,
                    worker = %self.addr,
                    object = %object.to_path(),
                    offset,
                    len = dst.len(),
                    error = %error,
                    "worker range fetch failed"
                );
                Err(error)
            }
        }
    }

    /// Fetch a versioned range only if it is already resident on the worker.
    ///
    /// The distinct wire operation is fail-closed during rolling upgrades: an
    /// older worker rejects it instead of performing an origin-backed read.
    pub async fn fetch_cached_range(
        &self,
        object: &ObjectId,
        version: &Version,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, WorkerError> {
        let request = CachedRangeRequest {
            object: object.clone(),
            version: version.clone(),
            offset,
            len,
        };
        let request_id = RequestId::next();
        let output = self.encode_cached_range_request(request_id.0, request)?;
        match self.exchange(&output, len).await {
            Ok(bytes) => Ok(bytes),
            Err((true, err)) if err.is_transport_failure() => {
                let mut stream = self.pool.fresh(&self.addr).await?;
                let bytes = self
                    .pool
                    .with_request_deadline("worker fetch_cached_range retry", async {
                        stream.write_all(&output).await?;
                        stream.flush().await?;
                        read_range_reply(&mut stream, len).await
                    })
                    .await?;
                self.pool.release(&self.addr, stream);
                Ok(bytes)
            }
            Err((_, error)) => Err(error),
        }
    }

    /// Admit one complete, versioned block without asking the worker to access
    /// its configured backend.
    pub async fn admit_cached_block(
        &self,
        block: &BlockId,
        object_len: u64,
        body: &[u8],
    ) -> Result<(), WorkerError> {
        let request_id = RequestId::next();
        let header = encode_cached_block_put_header(
            request_id.0,
            &CachedBlockPutRequest {
                block: block.clone(),
                object_len,
                body_len: body.len() as u64,
            },
        )?;
        match self.admit_exchange(&header, body).await {
            Ok(()) => Ok(()),
            Err((true, err)) if err.is_transport_failure() => {
                let mut stream = self.pool.fresh(&self.addr).await?;
                self.pool
                    .with_deadline(
                        "worker admit_cached_block retry",
                        streamed_put_timeout(body.len() as u64),
                        async {
                            stream.write_all(&header).await?;
                            stream.write_all(body).await?;
                            stream.flush().await?;
                            read_range_reply(&mut stream, 0).await.map(|_| ())
                        },
                    )
                    .await?;
                self.pool.release(&self.addr, stream);
                Ok(())
            }
            Err((_, error)) => Err(error),
        }
    }

    async fn admit_exchange(&self, header: &[u8], body: &[u8]) -> Result<(), (bool, WorkerError)> {
        let (mut stream, reused) = self
            .pool
            .checkout(&self.addr)
            .await
            .map_err(|error| (false, WorkerError::from(error)))?;
        let result = self
            .pool
            .with_deadline(
                "worker admit_cached_block",
                streamed_put_timeout(body.len() as u64),
                async {
                    stream.write_all(header).await?;
                    stream.write_all(body).await?;
                    stream.flush().await?;
                    read_range_reply(&mut stream, 0).await.map(|_| ())
                },
            )
            .await;
        match result {
            Ok(()) => {
                self.pool.release(&self.addr, stream);
                Ok(())
            }
            Err(error) => Err((reused, error)),
        }
    }

    /// One request/response over a pooled-or-fresh connection.
    ///
    /// On success releases the connection for reuse. On error returns
    /// `(was_reused, err)` so the caller can decide whether to retry (a reused
    /// connection may simply have been closed while idle).
    async fn exchange(
        &self,
        out: &[u8],
        expected_len: u64,
    ) -> Result<Vec<u8>, (bool, WorkerError)> {
        let (mut stream, reused) = self
            .pool
            .checkout(&self.addr)
            .await
            .map_err(|e| (false, WorkerError::from(e)))?;
        // On any error, `stream` is dropped (not released), so a half-broken
        // connection is never returned to the pool.
        let result: Result<Vec<u8>, WorkerError> = self
            .pool
            .with_request_deadline("worker fetch_range", async {
                stream.write_all(out).await?;
                stream.flush().await?;
                read_range_reply(&mut stream, expected_len).await
            })
            .await;
        match result {
            Ok(bytes) => {
                self.pool.release(&self.addr, stream);
                Ok(bytes)
            }
            Err(err) => Err((reused, err)),
        }
    }

    /// One request/response into a caller-owned buffer.
    async fn exchange_into(
        &self,
        output: &[u8],
        dst: &mut [u8],
    ) -> Result<usize, (bool, WorkerError)> {
        let (mut stream, reused) = self
            .pool
            .checkout(&self.addr)
            .await
            .map_err(|error| (false, WorkerError::from(error)))?;
        let result: Result<usize, WorkerError> = self
            .pool
            .with_request_deadline("worker fetch_range", async {
                stream.write_all(output).await?;
                stream.flush().await?;
                read_range_reply_into(&mut stream, dst).await
            })
            .await;
        match result {
            Ok(n) => {
                self.pool.release(&self.addr, stream);
                Ok(n)
            }
            Err(error) => Err((reused, error)),
        }
    }
}

/// Data-plane client for **writing** and **deleting** objects on a worker.
///
/// The write analogue of [`WorkerClient`]: it streams a whole object to the
/// owning worker over a [`MsgType::Put`] frame (a small `PutRequest` header, then
/// the raw object bytes), or removes it with a [`MsgType::Delete`] frame. The
/// worker writes through to the backend and replies with the committed
/// [`Version`] (or an `ERROR`-flagged frame). Reuses the shared
/// [`ConnectionPool`] like the read path (issue #181, #226/#230).
#[derive(Debug, Clone)]
pub struct WriteClient {
    addr: String,
    pool: Arc<ConnectionPool>,
}

impl WriteClient {
    /// Create a client that dials `addr`, with its own pool.
    pub fn new(addr: impl Into<String>) -> Self {
        Self::with_pool(addr, Arc::new(ConnectionPool::new()))
    }

    /// Create a client that reuses connections from the shared `pool`.
    pub fn with_pool(addr: impl Into<String>, pool: Arc<ConnectionPool>) -> Self {
        Self {
            addr: addr.into(),
            pool,
        }
    }

    /// The worker address this client talks to.
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Write the whole `object` through the worker to the backend.
    ///
    /// Sends a `Put` header naming the object + body length, then streams the
    /// `body` bytes, and returns the backend-committed [`Version`]. A worker- or
    /// backend-side failure surfaces as [`WorkerError::Remote`].
    ///
    /// Retries once when a *reused* pooled connection fails while transmitting
    /// the request (the peer may have closed it while idle). Once the complete
    /// request has been sent, a response failure is ambiguous and propagates
    /// instead of risking a duplicate backend write.
    pub async fn put_object(&self, object: &ObjectId, body: &[u8]) -> Result<Version, WorkerError> {
        let req_id = RequestId::next();
        let header = encode_put_header(
            req_id.0,
            &PutRequest {
                object: object.clone(),
                body_len: body.len() as u64,
            },
        )?;
        match self.put_exchange(&header, body).await {
            Ok(version) => Ok(version),
            Err((true, true, err)) if err.is_transport_failure() => {
                let mut stream = self.pool.fresh(&self.addr).await?;
                let version = self
                    .pool
                    .with_request_deadline("worker put_object retry", async {
                        stream.write_all(&header).await?;
                        stream.write_all(body).await?;
                        stream.flush().await?;
                        read_version_reply(&mut stream).await
                    })
                    .await?;
                self.pool.release(&self.addr, stream);
                Ok(version)
            }
            Err((_, _, err)) => {
                tracing::error!(
                    req = %req_id,
                    worker = %self.addr,
                    object = %object.to_path(),
                    bytes = body.len(),
                    error = %err,
                    "worker put failed"
                );
                Err(err)
            }
        }
    }

    /// Stream a staged file through the worker without loading it into memory.
    pub async fn put_object_file(
        &self,
        object: &ObjectId,
        path: &Path,
        len: u64,
    ) -> Result<Version, WorkerError> {
        let mut file = tokio::fs::File::open(path).await?;
        let actual_len = file.metadata().await?.len();
        if actual_len < len {
            return Err(short_file_error(actual_len, len));
        }
        let req_id = RequestId::next();
        let header = encode_put_header(
            req_id.0,
            &PutRequest {
                object: object.clone(),
                body_len: len,
            },
        )?;
        match self.put_file_exchange(&header, &mut file, len).await {
            Ok(version) => Ok(version),
            Err((true, true, err)) if err.is_transport_failure() => {
                file.seek(std::io::SeekFrom::Start(0)).await?;
                let mut stream = self.pool.fresh(&self.addr).await?;
                let version = self
                    .pool
                    .with_deadline(
                        "worker put_object_file retry",
                        streamed_put_timeout(len),
                        async {
                            stream.write_all(&header).await?;
                            stream_file(&mut stream, &mut file, len, None).await?;
                            read_version_reply(&mut stream).await
                        },
                    )
                    .await?;
                self.pool.release(&self.addr, stream);
                Ok(version)
            }
            Err((_, _, error)) => {
                tracing::error!(
                    req = %req_id,
                    worker = %self.addr,
                    object = %object.to_path(),
                    bytes = len,
                    %error,
                    "worker streamed put failed"
                );
                Err(error)
            }
        }
    }

    /// One PUT exchange over a pooled-or-fresh connection; on error returns
    /// `(was_reused, err)` so a stale pooled connection can be retried.
    async fn put_exchange(
        &self,
        header: &[u8],
        body: &[u8],
    ) -> Result<Version, (bool, bool, WorkerError)> {
        let (mut stream, reused) = self
            .pool
            .checkout(&self.addr)
            .await
            .map_err(|e| (false, false, WorkerError::from(e)))?;
        let retry_safe = AtomicBool::new(true);
        let result: Result<Version, WorkerError> = self
            .pool
            .with_request_deadline("worker put_object", async {
                stream.write_all(header).await?;
                stream.write_all(body).await?;
                stream.flush().await?;
                retry_safe.store(false, Ordering::Relaxed);
                read_version_reply(&mut stream).await
            })
            .await;
        match result {
            Ok(version) => {
                self.pool.release(&self.addr, stream);
                Ok(version)
            }
            Err(err) => Err((reused, retry_safe.load(Ordering::Relaxed), err)),
        }
    }

    async fn put_file_exchange(
        &self,
        header: &[u8],
        file: &mut tokio::fs::File,
        len: u64,
    ) -> Result<Version, (bool, bool, WorkerError)> {
        let (mut stream, reused) = self
            .pool
            .checkout(&self.addr)
            .await
            .map_err(|error| (false, false, WorkerError::from(error)))?;
        let retry_safe = AtomicBool::new(true);
        let result: Result<Version, WorkerError> = self
            .pool
            .with_deadline("worker put_object_file", streamed_put_timeout(len), async {
                stream.write_all(header).await?;
                stream_file(&mut stream, file, len, Some(&retry_safe)).await?;
                retry_safe.store(false, Ordering::Relaxed);
                read_version_reply(&mut stream).await
            })
            .await;
        match result {
            Ok(version) => {
                self.pool.release(&self.addr, stream);
                Ok(version)
            }
            Err(error) => Err((reused, retry_safe.load(Ordering::Relaxed), error)),
        }
    }

    /// Delete `object` at the worker (which deletes it at the backend).
    pub async fn delete_object(&self, object: &ObjectId) -> Result<(), WorkerError> {
        let req_id = RequestId::next();
        let frame = encode_delete(
            req_id.0,
            &DeleteRequest {
                object: object.clone(),
            },
        )?;
        match self.delete_exchange(&frame).await {
            Ok(()) => Ok(()),
            Err((true, true, err)) if err.is_transport_failure() => {
                let mut stream = self.pool.fresh(&self.addr).await?;
                self.pool
                    .with_request_deadline("worker delete_object retry", async {
                        stream.write_all(&frame).await?;
                        stream.flush().await?;
                        read_version_reply(&mut stream).await.map(|_| ())
                    })
                    .await?;
                self.pool.release(&self.addr, stream);
                Ok(())
            }
            Err((_, _, err)) => {
                tracing::error!(
                    req = %req_id,
                    worker = %self.addr,
                    object = %object.to_path(),
                    error = %err,
                    "worker delete failed"
                );
                Err(err)
            }
        }
    }

    async fn delete_exchange(&self, frame: &[u8]) -> Result<(), (bool, bool, WorkerError)> {
        let (mut stream, reused) = self
            .pool
            .checkout(&self.addr)
            .await
            .map_err(|e| (false, false, WorkerError::from(e)))?;
        let retry_safe = AtomicBool::new(true);
        let result: Result<(), WorkerError> = self
            .pool
            .with_request_deadline("worker delete_object", async {
                stream.write_all(frame).await?;
                stream.flush().await?;
                retry_safe.store(false, Ordering::Relaxed);
                read_version_reply(&mut stream).await.map(|_| ())
            })
            .await;
        match result {
            Ok(()) => {
                self.pool.release(&self.addr, stream);
                Ok(())
            }
            Err(err) => Err((reused, retry_safe.load(Ordering::Relaxed), err)),
        }
    }
}

fn streamed_put_timeout(len: u64) -> Duration {
    const MIN_BYTES_PER_SECOND: u64 = 4 * 1024 * 1024;
    const MAX_SECONDS: u64 = 30 * 60;
    let transfer_seconds = len.div_ceil(MIN_BYTES_PER_SECOND);
    Duration::from_secs((30 + transfer_seconds).min(MAX_SECONDS))
}

async fn stream_file(
    stream: &mut TcpStream,
    file: &mut tokio::fs::File,
    len: u64,
    retry_safe: Option<&AtomicBool>,
) -> Result<(), WorkerError> {
    let mut remaining = len;
    let mut buffer = vec![0_u8; 64 * 1024];
    while remaining > 0 {
        if let Some(retry_safe) = retry_safe {
            retry_safe.store(false, Ordering::Relaxed);
        }
        let limit = remaining.min(buffer.len() as u64) as usize;
        let read = file.read(&mut buffer[..limit]).await?;
        if read == 0 {
            return Err(short_file_error(len - remaining, len));
        }
        if let Some(retry_safe) = retry_safe {
            retry_safe.store(true, Ordering::Relaxed);
        }
        stream.write_all(&buffer[..read]).await?;
        remaining -= read as u64;
    }
    stream.flush().await?;
    Ok(())
}

fn short_file_error(actual_len: u64, expected_len: u64) -> WorkerError {
    WorkerError::Io(std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        format!("staged file is {actual_len} bytes, expected at least {expected_len}"),
    ))
}

/// Read a framed write/delete reply: OK carries the committed version bytes (a
/// UTF-8 [`Version`]); an `ERROR`-flagged frame carries a message.
async fn read_version_reply(stream: &mut TcpStream) -> Result<Version, WorkerError> {
    let mut header_buf = [0u8; HEADER_LEN];
    stream.read_exact(&mut header_buf).await?;
    let header = FrameHeader::decode(&header_buf)?;
    if header.msg_type != MsgType::GetRange {
        return Err(WorkerError::NotGetRange(header.msg_type));
    }
    if header.length > MAX_CONTROL_PAYLOAD_LEN {
        return Err(WorkerError::PayloadTooLarge {
            length: header.length,
            cap: MAX_CONTROL_PAYLOAD_LEN,
        });
    }
    let mut body = vec![0u8; header.length as usize];
    stream.read_exact(&mut body).await?;
    if header.flags.contains(Flags::ERROR) {
        return Err(WorkerError::Remote(decode_error_payload(&body)));
    }
    Ok(Version::new(String::from_utf8_lossy(&body).into_owned()))
}

/// Read one framed data-plane reply for a request of `expected_len` bytes.
///
/// A successful reply must advertise exactly `expected_len` before its buffer is
/// allocated. If the header carries [`Flags::ERROR`], the body is instead a
/// typed or legacy message capped at [`MAX_CONTROL_PAYLOAD_LEN`] and returned as
/// [`WorkerError::Remote`].
async fn read_range_reply(
    stream: &mut TcpStream,
    expected_len: u64,
) -> Result<Vec<u8>, WorkerError> {
    let mut header_buf = [0u8; HEADER_LEN];
    stream.read_exact(&mut header_buf).await?;
    let header = FrameHeader::decode(&header_buf)?;
    if header.msg_type != MsgType::GetRange {
        return Err(WorkerError::NotGetRange(header.msg_type));
    }
    if header.flags.contains(Flags::ERROR) {
        if header.length > MAX_CONTROL_PAYLOAD_LEN {
            return Err(WorkerError::PayloadTooLarge {
                length: header.length,
                cap: MAX_CONTROL_PAYLOAD_LEN,
            });
        }
    } else if u64::from(header.length) != expected_len {
        return Err(WorkerError::RangeLengthMismatch {
            expected: expected_len,
            actual: u64::from(header.length),
        });
    }
    let mut body = vec![0u8; header.length as usize];
    stream.read_exact(&mut body).await?;
    if header.flags.contains(Flags::ERROR) {
        return Err(WorkerError::Remote(decode_error_payload(&body)));
    }
    Ok(body)
}

/// Read a successful range reply directly into `dst`.
async fn read_range_reply_into(
    stream: &mut TcpStream,
    dst: &mut [u8],
) -> Result<usize, WorkerError> {
    let mut header_buf = [0u8; HEADER_LEN];
    stream.read_exact(&mut header_buf).await?;
    let header = FrameHeader::decode(&header_buf)?;
    if header.msg_type != MsgType::GetRange {
        return Err(WorkerError::NotGetRange(header.msg_type));
    }
    if header.flags.contains(Flags::ERROR) {
        if header.length > MAX_CONTROL_PAYLOAD_LEN {
            return Err(WorkerError::PayloadTooLarge {
                length: header.length,
                cap: MAX_CONTROL_PAYLOAD_LEN,
            });
        }
        let mut body = vec![0u8; header.length as usize];
        stream.read_exact(&mut body).await?;
        return Err(WorkerError::Remote(decode_error_payload(&body)));
    }
    let expected_len = dst.len() as u64;
    if u64::from(header.length) != expected_len {
        return Err(WorkerError::RangeLengthMismatch {
            expected: expected_len,
            actual: u64::from(header.length),
        });
    }
    stream.read_exact(dst).await?;
    Ok(dst.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use talon_core::Backend;
    use talon_transport::{
        decode_cached_block_put_header, decode_cached_request, decode_cached_tenant_request,
        decode_request, decode_tenant_request, encode_error, response_header_ok,
    };
    use tokio::net::TcpListener;

    fn object() -> ObjectId {
        ObjectId::new(Backend::Azure, "container", "path/to/blob.bin")
    }

    #[tokio::test]
    async fn cache_admission_sends_exact_versioned_block_and_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let block = BlockId::new(object(), 8, 8, Version::new("etag-v2"));
        let expected = block.clone();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header_bytes = [0_u8; HEADER_LEN];
            socket.read_exact(&mut header_bytes).await.unwrap();
            let header = FrameHeader::decode(&header_bytes).unwrap();
            assert_eq!(header.msg_type, MsgType::AdmitCachedBlock);
            let mut payload = vec![0_u8; header.length as usize];
            socket.read_exact(&mut payload).await.unwrap();
            let mut frame = header_bytes.to_vec();
            frame.extend_from_slice(&payload);
            let (_, request) = decode_cached_block_put_header(&frame).unwrap();
            assert_eq!(request.block, expected);
            assert_eq!(request.object_len, 13);
            assert_eq!(request.body_len, 5);
            let mut body = vec![0_u8; 5];
            socket.read_exact(&mut body).await.unwrap();
            assert_eq!(body, b"tail!");
            socket
                .write_all(&response_header_ok(header.request_id, 0))
                .await
                .unwrap();
        });

        WorkerClient::new(addr)
            .admit_cached_block(&block, 13, b"tail!")
            .await
            .unwrap();
    }

    /// What a mock write-worker records: the object and body it received.
    type RecordedPut = Arc<std::sync::Mutex<Option<(ObjectId, Vec<u8>)>>>;

    /// Spawn a mock worker: reads one RangeRequest, then replies with the bytes
    /// produced by `respond(req)`.
    async fn mock_worker<F>(respond: F) -> String
    where
        F: Fn(RangeRequest) -> Vec<u8> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut hdr = [0u8; HEADER_LEN];
            sock.read_exact(&mut hdr).await.unwrap();
            let header = FrameHeader::decode(&hdr).unwrap();
            let mut body = vec![0u8; header.length as usize];
            sock.read_exact(&mut body).await.unwrap();
            let mut full = hdr.to_vec();
            full.extend_from_slice(&body);
            let (_h, req) = decode_request(&full).unwrap();
            let reply = respond(req);
            sock.write_all(&reply).await.unwrap();
            sock.flush().await.unwrap();
        });
        addr
    }

    /// Spawn a one-shot range worker that writes only `reply_header`, then keeps
    /// the socket open. A correct client must reject the header before trying to
    /// allocate or read its advertised body.
    async fn header_only_range_worker(reply_header: FrameHeader) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut hdr = [0u8; HEADER_LEN];
            sock.read_exact(&mut hdr).await.unwrap();
            let header = FrameHeader::decode(&hdr).unwrap();
            let mut body = vec![0u8; header.length as usize];
            sock.read_exact(&mut body).await.unwrap();
            sock.write_all(&reply_header.encode()).await.unwrap();
            sock.flush().await.unwrap();
            tokio::time::sleep(Duration::from_secs(2)).await;
            drop(sock);
        });
        addr
    }

    /// Spawn a worker that records the `request_id` of each frame it receives,
    /// serving `count` sequential requests with an empty-range reply.
    async fn request_id_recording_worker(
        count: usize,
    ) -> (String, Arc<std::sync::Mutex<Vec<u32>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let seen: Arc<std::sync::Mutex<Vec<u32>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = Arc::clone(&seen);
        tokio::spawn(async move {
            for _ in 0..count {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut hdr = [0u8; HEADER_LEN];
                sock.read_exact(&mut hdr).await.unwrap();
                let header = FrameHeader::decode(&hdr).unwrap();
                let mut body = vec![0u8; header.length as usize];
                sock.read_exact(&mut body).await.unwrap();
                recorded.lock().unwrap().push(header.request_id);
                // Echo the id back on the reply, as a real worker does.
                let reply = response_header_ok(header.request_id, 0).to_vec();
                sock.write_all(&reply).await.unwrap();
                sock.flush().await.unwrap();
            }
        });
        (addr, seen)
    }

    #[tokio::test]
    async fn fetch_puts_a_unique_nonzero_request_id_on_the_wire() {
        // Correlation (#304) depends on the client stamping a real id into the
        // frame header: a hardcoded 0 makes client and worker logs unjoinable.
        let (addr, seen) = request_id_recording_worker(2).await;
        let client = WorkerClient::new(addr);
        client.fetch_range(&object(), 0, 0).await.unwrap();
        client.fetch_range(&object(), 0, 0).await.unwrap();

        let ids = seen.lock().unwrap().clone();
        assert_eq!(ids.len(), 2, "worker should have seen two requests");
        assert!(ids[0] != 0 && ids[1] != 0, "request_id must not be zero");
        assert_ne!(
            ids[0], ids[1],
            "each request needs a distinct id to be correlatable"
        );
    }

    #[tokio::test]
    async fn tenant_client_sends_a_tenant_scoped_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let seen: Arc<std::sync::Mutex<Option<TenantId>>> = Arc::new(std::sync::Mutex::new(None));
        let recorded = Arc::clone(&seen);
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut hdr = [0u8; HEADER_LEN];
            sock.read_exact(&mut hdr).await.unwrap();
            let header = FrameHeader::decode(&hdr).unwrap();
            assert_eq!(header.msg_type, MsgType::GetRangeTenant);
            let mut body = vec![0u8; header.length as usize];
            sock.read_exact(&mut body).await.unwrap();
            let mut full = hdr.to_vec();
            full.extend_from_slice(&body);
            let (_h, scoped) = decode_tenant_request(&full).unwrap();
            *recorded.lock().unwrap() = Some(scoped.tenant);
            // Reply with the requested bytes so the client fetch succeeds; the
            // reply is an ordinary GetRange frame.
            let mut out = response_header_ok(header.request_id, scoped.request.len as u32).to_vec();
            out.resize(out.len() + scoped.request.len as usize, 0);
            sock.write_all(&out).await.unwrap();
            sock.flush().await.unwrap();
        });

        let client = WorkerClient::new(addr).with_tenant(TenantId::named("acme"));
        let bytes = client.fetch_range(&object(), 0, 16).await.unwrap();
        assert_eq!(bytes.len(), 16);
        assert_eq!(seen.lock().unwrap().take(), Some(TenantId::named("acme")));
    }

    #[tokio::test]
    async fn default_client_sends_a_plain_getrange() {
        // Without a tenant the client sends an ordinary GetRange, which the
        // GetRange-only mock worker decodes successfully (backward compatible).
        let addr = mock_worker(|req| {
            let mut out = response_header_ok(0, req.len as u32).to_vec();
            out.resize(out.len() + req.len as usize, 0);
            out
        })
        .await;
        let bytes = WorkerClient::new(addr)
            .fetch_range(&object(), 0, 8)
            .await
            .unwrap();
        assert_eq!(bytes.len(), 8);
    }

    #[tokio::test]
    async fn fetch_returns_raw_bytes() {
        let addr = mock_worker(|req| {
            let payload: Vec<u8> = (0..req.len).map(|i| (i % 251) as u8).collect();
            let mut out = response_header_ok(0, payload.len() as u32).to_vec();
            out.extend_from_slice(&payload);
            out
        })
        .await;
        let client = WorkerClient::new(addr);
        let bytes = client.fetch_range(&object(), 0, 4096).await.unwrap();
        assert_eq!(bytes.len(), 4096);
        assert_eq!(bytes[0], 0);
        assert_eq!(bytes[250], 250);
        assert_eq!(bytes[251], 0);
    }

    #[tokio::test]
    async fn fetch_range_into_fills_caller_buffer() {
        let addr = mock_worker(|req| {
            let payload: Vec<u8> = (0..req.len).map(|i| (i % 251) as u8).collect();
            let mut out = response_header_ok(0, payload.len() as u32).to_vec();
            out.extend_from_slice(&payload);
            out
        })
        .await;
        let client = WorkerClient::new(addr);
        let mut dst = vec![0u8; 4096];
        let n = client
            .fetch_range_into(&object(), 0, &mut dst)
            .await
            .unwrap();
        assert_eq!(n, dst.len());
        assert_eq!(dst[0], 0);
        assert_eq!(dst[250], 250);
        assert_eq!(dst[251], 0);
    }

    #[tokio::test]
    async fn cached_fetch_sends_the_versioned_fail_closed_operation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header = [0_u8; HEADER_LEN];
            socket.read_exact(&mut header).await.unwrap();
            let frame = FrameHeader::decode(&header).unwrap();
            assert_eq!(frame.msg_type, MsgType::GetCachedRange);
            let mut body = vec![0_u8; frame.length as usize];
            socket.read_exact(&mut body).await.unwrap();
            let mut request = header.to_vec();
            request.extend_from_slice(&body);
            let (_, request) = decode_cached_request(&request).unwrap();
            assert_eq!(request.version, Version::new("etag-v2"));
            assert_eq!((request.offset, request.len), (3, 4));
            socket
                .write_all(&response_header_ok(frame.request_id, 4))
                .await
                .unwrap();
            socket.write_all(b"data").await.unwrap();
        });

        let bytes = WorkerClient::new(address)
            .fetch_cached_range(&object(), &Version::new("etag-v2"), 3, 4)
            .await
            .unwrap();
        assert_eq!(bytes, b"data");
    }

    #[tokio::test]
    async fn tenant_client_sends_a_cached_tenant_scoped_frame() {
        // A tenant-bound client attributes cache-only reads too, so a resident
        // read cannot slip past per-tenant metering on the worker.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let seen: Arc<std::sync::Mutex<Option<TenantId>>> = Arc::new(std::sync::Mutex::new(None));
        let recorded = Arc::clone(&seen);
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut hdr = [0u8; HEADER_LEN];
            sock.read_exact(&mut hdr).await.unwrap();
            let header = FrameHeader::decode(&hdr).unwrap();
            assert_eq!(header.msg_type, MsgType::GetCachedRangeTenant);
            let mut body = vec![0u8; header.length as usize];
            sock.read_exact(&mut body).await.unwrap();
            let mut full = hdr.to_vec();
            full.extend_from_slice(&body);
            let (_h, scoped) = decode_cached_tenant_request(&full).unwrap();
            assert_eq!(scoped.request.version, Version::new("etag-v2"));
            *recorded.lock().unwrap() = Some(scoped.tenant);
            let mut out = response_header_ok(header.request_id, scoped.request.len as u32).to_vec();
            out.resize(out.len() + scoped.request.len as usize, 0);
            sock.write_all(&out).await.unwrap();
            sock.flush().await.unwrap();
        });

        let client = WorkerClient::new(addr).with_tenant(TenantId::named("acme"));
        let bytes = client
            .fetch_cached_range(&object(), &Version::new("etag-v2"), 0, 16)
            .await
            .unwrap();
        assert_eq!(bytes.len(), 16);
        assert_eq!(seen.lock().unwrap().take(), Some(TenantId::named("acme")));
    }

    #[tokio::test]
    async fn error_flag_becomes_remote_error() {
        let addr = mock_worker(|_req| encode_error(0, "block not present")).await;
        let client = WorkerClient::new(addr);
        let err = client.fetch_range(&object(), 0, 16).await.unwrap_err();
        match err {
            WorkerError::Remote(error) => {
                assert_eq!(error.code, talon_transport::DataErrorCode::Unknown);
                assert_eq!(error.message, "block not present");
            }
            other => panic!("expected Remote, got {other:?}"),
        }
    }

    /// A mock worker that loops, serving many sequential requests on the SAME
    /// connection. Counts accepted connections and requests served, and lets
    /// the caller build each reply: `respond` gets the request header, the
    /// decoded request, and the 0-based index of this request across all
    /// connections.
    async fn looping_mock_worker_with<F>(
        accepts: Arc<std::sync::atomic::AtomicU32>,
        requests: Arc<std::sync::atomic::AtomicU32>,
        respond: F,
    ) -> String
    where
        F: Fn(&FrameHeader, &RangeRequest, u32) -> Vec<u8> + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let respond = Arc::new(respond);
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                accepts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let requests = Arc::clone(&requests);
                let respond = Arc::clone(&respond);
                tokio::spawn(async move {
                    loop {
                        let mut hdr = [0u8; HEADER_LEN];
                        if sock.read_exact(&mut hdr).await.is_err() {
                            return;
                        }
                        let header = FrameHeader::decode(&hdr).unwrap();
                        let mut body = vec![0u8; header.length as usize];
                        sock.read_exact(&mut body).await.unwrap();
                        let mut full = hdr.to_vec();
                        full.extend_from_slice(&body);
                        let (_h, req): (_, RangeRequest) = decode_request(&full).unwrap();
                        let n = requests.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        sock.write_all(&respond(&header, &req, n)).await.unwrap();
                        sock.flush().await.unwrap();
                    }
                });
            }
        });
        addr
    }

    /// A looping mock worker that serves every request successfully.
    async fn looping_mock_worker(accepts: Arc<std::sync::atomic::AtomicU32>) -> String {
        let requests = Arc::new(std::sync::atomic::AtomicU32::new(0));
        looping_mock_worker_with(accepts, requests, |_header, req, _n| {
            let payload: Vec<u8> = (0..req.len).map(|i| (i % 251) as u8).collect();
            let mut out = response_header_ok(0, payload.len() as u32).to_vec();
            out.extend_from_slice(&payload);
            out
        })
        .await
    }

    #[tokio::test]
    async fn reuses_pooled_connection_across_fetches() {
        use std::sync::atomic::Ordering;
        let accepts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let addr = looping_mock_worker(Arc::clone(&accepts)).await;
        let client = WorkerClient::new(addr);

        // Two fetches on the same client reuse one pooled connection.
        for _ in 0..2 {
            let bytes = client.fetch_range(&object(), 0, 16).await.unwrap();
            assert_eq!(bytes.len(), 16);
        }
        assert_eq!(
            accepts.load(Ordering::SeqCst),
            1,
            "second fetch reused the pooled connection"
        );
    }

    #[tokio::test]
    async fn retries_on_a_stale_pooled_connection() {
        use std::sync::atomic::Ordering;
        // A worker that serves exactly one request per connection then closes.
        // After the first fetch pools the (now server-closed) connection, the
        // second fetch's reuse fails and must transparently retry on a fresh
        // dial — so both fetches succeed despite the server closing each conn.
        let accepts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let accepts_srv = Arc::clone(&accepts);
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                accepts_srv.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let mut hdr = [0u8; HEADER_LEN];
                    if sock.read_exact(&mut hdr).await.is_err() {
                        return;
                    }
                    let header = FrameHeader::decode(&hdr).unwrap();
                    let mut body = vec![0u8; header.length as usize];
                    sock.read_exact(&mut body).await.unwrap();
                    let out = response_header_ok(0, 8).to_vec();
                    let mut full = out;
                    full.extend_from_slice(&[7u8; 8]);
                    sock.write_all(&full).await.unwrap();
                    sock.flush().await.unwrap();
                    // Connection closes here (task ends), so a pooled reuse fails.
                });
            }
        });
        let client = WorkerClient::new(addr);

        let a = client.fetch_range(&object(), 0, 8).await.unwrap();
        assert_eq!(a, vec![7u8; 8]);
        // Second fetch: the pooled connection is stale → retry on a fresh dial.
        let b = client.fetch_range(&object(), 0, 8).await.unwrap();
        assert_eq!(b, vec![7u8; 8]);
        // Two connections were accepted (the retry dialed a fresh one).
        assert_eq!(accepts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn remote_error_is_not_retried_on_the_same_replica() {
        use std::sync::atomic::{AtomicU32, Ordering};
        // A worker that serves the first request successfully, then refuses
        // every request after that on the SAME (now reused) connection. A
        // remote refusal must propagate on the first try, not re-dial the
        // replica that just told us it doesn't have the block.
        let accepts = Arc::new(AtomicU32::new(0));
        let requests = Arc::new(AtomicU32::new(0));
        let addr = looping_mock_worker_with(
            Arc::clone(&accepts),
            Arc::clone(&requests),
            |header, _req, n| {
                if n == 0 {
                    let mut out = response_header_ok(header.request_id, 8).to_vec();
                    out.extend_from_slice(&[7u8; 8]);
                    out
                } else {
                    encode_error(header.request_id, "block not present")
                }
            },
        )
        .await;
        let client = WorkerClient::new(addr);

        // Warm the pool with a successful fetch.
        let first = client.fetch_range(&object(), 0, 8).await.unwrap();
        assert_eq!(first, vec![7u8; 8]);

        // Second fetch reuses the pooled connection and gets a remote refusal.
        let err = client.fetch_range(&object(), 0, 8).await.unwrap_err();
        match err {
            WorkerError::Remote(error) => {
                assert_eq!(error.code, talon_transport::DataErrorCode::Unknown);
                assert_eq!(error.message, "block not present");
            }
            other => panic!("expected Remote, got {other:?}"),
        }

        // The load-bearing assertions: a remote error on a reused connection
        // must not open a second connection or double the request.
        assert_eq!(
            accepts.load(Ordering::SeqCst),
            1,
            "a remote error must not re-dial the same replica"
        );
        assert_eq!(
            requests.load(Ordering::SeqCst),
            2,
            "the wrong-owner round trip must not be doubled"
        );
    }

    #[tokio::test]
    async fn wrong_reply_type_rejected() {
        // Reply with a Control-type frame instead of GetRange.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut hdr = [0u8; HEADER_LEN];
            sock.read_exact(&mut hdr).await.unwrap();
            let header = FrameHeader::decode(&hdr).unwrap();
            let mut body = vec![0u8; header.length as usize];
            sock.read_exact(&mut body).await.unwrap();
            let reply = FrameHeader::new(MsgType::Control, 0, 0).encode();
            sock.write_all(&reply).await.unwrap();
            sock.flush().await.unwrap();
        });
        let client = WorkerClient::new(addr);
        let err = client.fetch_range(&object(), 0, 16).await.unwrap_err();
        assert!(matches!(err, WorkerError::NotGetRange(MsgType::Control)));
    }

    #[tokio::test]
    async fn mismatched_range_length_is_rejected_before_body() {
        let header = FrameHeader::new(MsgType::GetRange, 0, 17);
        let addr = header_only_range_worker(header).await;
        let client = WorkerClient::with_pool(addr, impatient_pool());

        let err = client.fetch_range(&object(), 0, 16).await.unwrap_err();
        assert!(matches!(
            err,
            WorkerError::RangeLengthMismatch {
                expected: 16,
                actual: 17
            }
        ));
    }

    #[tokio::test]
    async fn oversized_error_payload_is_rejected_before_body() {
        let mut header = FrameHeader::new(MsgType::GetRange, 0, MAX_CONTROL_PAYLOAD_LEN + 1);
        header.flags = Flags(Flags::ERROR);
        let addr = header_only_range_worker(header).await;
        let client = WorkerClient::with_pool(addr, impatient_pool());

        let err = client.fetch_range(&object(), 0, 16).await.unwrap_err();
        assert!(matches!(
            err,
            WorkerError::PayloadTooLarge {
                length,
                cap: MAX_CONTROL_PAYLOAD_LEN
            } if length == MAX_CONTROL_PAYLOAD_LEN + 1
        ));
    }

    #[tokio::test]
    async fn connect_failure_is_io_error() {
        let client = WorkerClient::new("127.0.0.1:1");
        let err = client.fetch_range(&object(), 0, 16).await.unwrap_err();
        assert!(matches!(err, WorkerError::Io(_)));
    }

    /// Spawn a mock write-worker: reads one Put header + its body, hands the
    /// (object, body) to `on_put`, and replies OK with `version` bytes.
    async fn mock_write_worker(version: &'static str, recorded: RecordedPut) -> String {
        use talon_transport::decode_put_header;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut hdr = [0u8; HEADER_LEN];
            sock.read_exact(&mut hdr).await.unwrap();
            let header = FrameHeader::decode(&hdr).unwrap();
            let mut hbody = vec![0u8; header.length as usize];
            sock.read_exact(&mut hbody).await.unwrap();
            let mut full = hdr.to_vec();
            full.extend_from_slice(&hbody);
            let (_h, req) = decode_put_header(&full).unwrap();
            // Read the raw object body that follows the header.
            let mut body = vec![0u8; req.body_len as usize];
            sock.read_exact(&mut body).await.unwrap();
            *recorded.lock().unwrap() = Some((req.object.clone(), body));
            let mut out = response_header_ok(0, version.len() as u32).to_vec();
            out.extend_from_slice(version.as_bytes());
            sock.write_all(&out).await.unwrap();
            sock.flush().await.unwrap();
        });
        addr
    }

    /// Spawn a one-shot write worker that consumes a complete Put request and
    /// writes only `reply_header`, keeping the connection open afterwards.
    async fn header_only_write_worker(reply_header: FrameHeader) -> String {
        use talon_transport::decode_put_header;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut hdr = [0u8; HEADER_LEN];
            sock.read_exact(&mut hdr).await.unwrap();
            let header = FrameHeader::decode(&hdr).unwrap();
            let mut hbody = vec![0u8; header.length as usize];
            sock.read_exact(&mut hbody).await.unwrap();
            let mut full = hdr.to_vec();
            full.extend_from_slice(&hbody);
            let (_header, req) = decode_put_header(&full).unwrap();
            let mut body = vec![0u8; req.body_len as usize];
            sock.read_exact(&mut body).await.unwrap();
            sock.write_all(&reply_header.encode()).await.unwrap();
            sock.flush().await.unwrap();
            tokio::time::sleep(Duration::from_secs(2)).await;
            drop(sock);
        });
        addr
    }

    #[tokio::test]
    async fn put_object_sends_header_and_body_and_returns_version() {
        let recorded = Arc::new(std::sync::Mutex::new(None));
        let addr = mock_write_worker("committed-v9", Arc::clone(&recorded)).await;
        let client = WriteClient::new(addr);
        let body = b"the whole object contents";
        let version = client.put_object(&object(), body).await.unwrap();
        assert_eq!(version, talon_core::Version::new("committed-v9"));
        let (obj, got) = recorded.lock().unwrap().take().unwrap();
        assert_eq!(obj, object());
        assert_eq!(got, body, "worker received the exact object bytes");
    }

    #[tokio::test]
    async fn version_reply_with_wrong_message_type_is_rejected() {
        let header = FrameHeader::new(MsgType::Control, 0, 0);
        let addr = header_only_write_worker(header).await;
        let client = WriteClient::new(addr);

        let err = client.put_object(&object(), b"x").await.unwrap_err();
        assert!(matches!(err, WorkerError::NotGetRange(MsgType::Control)));
    }

    #[tokio::test]
    async fn oversized_version_payload_is_rejected_before_body() {
        let header = FrameHeader::new(MsgType::GetRange, 0, MAX_CONTROL_PAYLOAD_LEN + 1);
        let addr = header_only_write_worker(header).await;
        let client = WriteClient::with_pool(addr, impatient_pool());

        let err = client.put_object(&object(), b"x").await.unwrap_err();
        assert!(matches!(
            err,
            WorkerError::PayloadTooLarge {
                length,
                cap: MAX_CONTROL_PAYLOAD_LEN
            } if length == MAX_CONTROL_PAYLOAD_LEN + 1
        ));
    }

    #[tokio::test]
    async fn put_object_file_streams_exact_bytes() {
        let recorded = Arc::new(std::sync::Mutex::new(None));
        let addr = mock_write_worker("stream-v1", Arc::clone(&recorded)).await;
        let client = WriteClient::new(addr);
        let mut staged = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut staged, b"streamed-file-body").unwrap();

        let version = client
            .put_object_file(&object(), staged.path(), 18)
            .await
            .unwrap();
        assert_eq!(version, talon_core::Version::new("stream-v1"));
        let (obj, got) = recorded.lock().unwrap().take().unwrap();
        assert_eq!(obj, object());
        assert_eq!(got, b"streamed-file-body");
    }

    #[tokio::test]
    async fn put_object_error_frame_becomes_remote() {
        use talon_transport::decode_put_header;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut hdr = [0u8; HEADER_LEN];
            sock.read_exact(&mut hdr).await.unwrap();
            let header = FrameHeader::decode(&hdr).unwrap();
            let mut hbody = vec![0u8; header.length as usize];
            sock.read_exact(&mut hbody).await.unwrap();
            let mut full = hdr.to_vec();
            full.extend_from_slice(&hbody);
            let (_h, req) = decode_put_header(&full).unwrap();
            let mut body = vec![0u8; req.body_len as usize];
            sock.read_exact(&mut body).await.unwrap();
            sock.write_all(&encode_error(0, "backend PUT failed"))
                .await
                .unwrap();
            sock.flush().await.unwrap();
        });
        let client = WriteClient::new(addr);
        let err = client.put_object(&object(), b"x").await.unwrap_err();
        match err {
            WorkerError::Remote(error) => {
                assert_eq!(error.code, talon_transport::DataErrorCode::Unknown);
                assert_eq!(error.message, "backend PUT failed");
            }
            other => panic!("expected Remote, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn delete_object_sends_delete_frame() {
        use talon_transport::decode_delete;
        let recorded = Arc::new(std::sync::Mutex::new(None));
        let rec = Arc::clone(&recorded);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut hdr = [0u8; HEADER_LEN];
            sock.read_exact(&mut hdr).await.unwrap();
            let header = FrameHeader::decode(&hdr).unwrap();
            let mut hbody = vec![0u8; header.length as usize];
            sock.read_exact(&mut hbody).await.unwrap();
            let mut full = hdr.to_vec();
            full.extend_from_slice(&hbody);
            let (_h, req) = decode_delete(&full).unwrap();
            *rec.lock().unwrap() = Some(req.object);
            let out = response_header_ok(0, 0).to_vec();
            sock.write_all(&out).await.unwrap();
            sock.flush().await.unwrap();
        });
        let client = WriteClient::new(addr);
        client.delete_object(&object()).await.unwrap();
        assert_eq!(recorded.lock().unwrap().take().unwrap(), object());
    }

    #[tokio::test]
    async fn put_connect_failure_is_io_error() {
        let client = WriteClient::new("127.0.0.1:1");
        let err = client.put_object(&object(), b"x").await.unwrap_err();
        assert!(matches!(err, WorkerError::Io(_)));
    }

    /// Accept the connection, read the request, then never reply and never
    /// close. This is the realistic worker-wedge case (GC pause, disk stall,
    /// half-open socket after a crash with no FIN) and it used to hang the
    /// caller forever — which on the read path means an unkillable mount, and
    /// which also silently defeats replica fallback, since the next replica is
    /// only tried once the current attempt returns.
    async fn stalling_worker() -> (String, Arc<std::sync::Mutex<Vec<TcpStream>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let held: Arc<std::sync::Mutex<Vec<TcpStream>>> = Arc::new(std::sync::Mutex::new(vec![]));
        let keep = Arc::clone(&held);
        tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    return;
                };
                keep.lock().unwrap().push(sock);
            }
        });
        (addr, held)
    }

    fn impatient_pool() -> Arc<ConnectionPool> {
        Arc::new(
            ConnectionPool::new().with_timeouts(Duration::from_secs(5), Duration::from_millis(150)),
        )
    }

    #[tokio::test]
    async fn fetch_range_times_out_against_a_stalling_worker() {
        let (addr, _held) = stalling_worker().await;
        let client = WorkerClient::with_pool(addr, impatient_pool());

        let started = std::time::Instant::now();
        let err = client
            .fetch_range(&object(), 0, 4096)
            .await
            .expect_err("a stalling worker must not hang the read");
        match err {
            WorkerError::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::TimedOut),
            other => panic!("expected a TimedOut I/O error, got {other:?}"),
        }
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "took {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn put_object_times_out_against_a_stalling_worker() {
        let (addr, _held) = stalling_worker().await;
        let client = WriteClient::with_pool(addr, impatient_pool());

        let started = std::time::Instant::now();
        let err = client
            .put_object(&object(), b"payload")
            .await
            .expect_err("a stalling worker must not hang the write");
        assert!(matches!(err, WorkerError::Io(_)));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "took {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn delete_object_times_out_against_a_stalling_worker() {
        let (addr, _held) = stalling_worker().await;
        let client = WriteClient::with_pool(addr, impatient_pool());

        let started = std::time::Instant::now();
        let err = client
            .delete_object(&object())
            .await
            .expect_err("a stalling worker must not hang the delete");
        assert!(matches!(err, WorkerError::Io(_)));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "took {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn local_file_error_on_reused_connection_does_not_redial() {
        use std::sync::atomic::{AtomicU32, Ordering};
        use talon_transport::decode_put_header;

        let accepts = Arc::new(AtomicU32::new(0));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server_accepts = Arc::clone(&accepts);
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                server_accepts.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    loop {
                        let mut header_bytes = [0_u8; HEADER_LEN];
                        if socket.read_exact(&mut header_bytes).await.is_err() {
                            return;
                        }
                        let header = FrameHeader::decode(&header_bytes).unwrap();
                        let mut payload = vec![0_u8; header.length as usize];
                        if socket.read_exact(&mut payload).await.is_err() {
                            return;
                        }
                        let mut frame = header_bytes.to_vec();
                        frame.extend_from_slice(&payload);
                        let (_, request) = decode_put_header(&frame).unwrap();
                        let mut body = vec![0_u8; request.body_len as usize];
                        if socket.read_exact(&mut body).await.is_err() {
                            return;
                        }
                        let mut response = response_header_ok(header.request_id, 2).to_vec();
                        response.extend_from_slice(b"v1");
                        socket.write_all(&response).await.unwrap();
                        socket.flush().await.unwrap();
                    }
                });
            }
        });

        let client = WriteClient::new(addr);
        client.put_object(&object(), b"warm").await.unwrap();
        let missing = tempfile::tempdir().unwrap().path().join("missing");
        let error = client
            .put_object_file(&object(), &missing, 1)
            .await
            .unwrap_err();
        assert!(matches!(error, WorkerError::Io(_)));

        for _ in 0..100 {
            if accepts.load(Ordering::SeqCst) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            accepts.load(Ordering::SeqCst),
            1,
            "a local staged-file failure must not open a fresh connection"
        );
    }

    #[tokio::test]
    async fn put_response_failure_after_request_is_sent_does_not_redial() {
        use std::sync::atomic::{AtomicU32, Ordering};
        use talon_transport::decode_put_header;

        let accepts = Arc::new(AtomicU32::new(0));
        let requests = Arc::new(AtomicU32::new(0));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server_accepts = Arc::clone(&accepts);
        let server_requests = Arc::clone(&requests);
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                server_accepts.fetch_add(1, Ordering::SeqCst);
                let requests = Arc::clone(&server_requests);
                tokio::spawn(async move {
                    loop {
                        let mut header_bytes = [0_u8; HEADER_LEN];
                        if socket.read_exact(&mut header_bytes).await.is_err() {
                            return;
                        }
                        let header = FrameHeader::decode(&header_bytes).unwrap();
                        let mut payload = vec![0_u8; header.length as usize];
                        socket.read_exact(&mut payload).await.unwrap();
                        let mut frame = header_bytes.to_vec();
                        frame.extend_from_slice(&payload);
                        let (_, request) = decode_put_header(&frame).unwrap();
                        let mut body = vec![0_u8; request.body_len as usize];
                        socket.read_exact(&mut body).await.unwrap();

                        if requests.fetch_add(1, Ordering::SeqCst) == 0 {
                            let mut response = response_header_ok(header.request_id, 2).to_vec();
                            response.extend_from_slice(b"v1");
                            socket.write_all(&response).await.unwrap();
                            socket.flush().await.unwrap();
                        } else {
                            return;
                        }
                    }
                });
            }
        });

        let client = WriteClient::new(addr);
        client.put_object(&object(), b"warm").await.unwrap();
        let error = client.put_object(&object(), b"commit-ambiguous").await;
        assert!(matches!(error, Err(WorkerError::Io(_))));

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        assert_eq!(
            accepts.load(Ordering::SeqCst),
            1,
            "an ambiguous response failure must not resend the PUT"
        );
    }
}
