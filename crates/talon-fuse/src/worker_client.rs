//! Data-plane client for fetching byte ranges from a worker.
//!
//! Where the [`CoordinatorClient`](crate::CoordinatorClient) answers *where* a
//! block lives, [`WorkerClient`] fetches the bytes. It speaks the data plane: a
//! single [`MsgType::GetRange`] frame whose body is a bincode
//! [`RangeRequest`] (object + `[offset, len)`), and a reply that is a
//! `GetRange` frame carrying the **raw range bytes** — or, if the
//! [`Flags::ERROR`] bit is set, a UTF-8 error string.
//!
//! The response body is raw (no bincode envelope) precisely so a production
//! worker can `sendfile` the range straight from a file into the socket; this
//! client only needs to read the framed bytes back. A fresh TCP connection is
//! opened per fetch for simplicity, mirroring
//! [`CoordinatorClient`](crate::CoordinatorClient).

use std::sync::Arc;

use talon_core::{ObjectId, Version};
use talon_transport::frame::{FrameHeader, MsgType, HEADER_LEN};
use talon_transport::{
    encode_delete, encode_put_header, encode_request, DeleteRequest, Flags, PutRequest,
    RangeRequest,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
    /// The worker set the ERROR flag and returned this message.
    #[error("worker error: {0}")]
    Remote(String),
}

/// A thin data-plane client bound to one worker address.
///
/// Reuses connections from a shared [`ConnectionPool`] so warm fetches skip the
/// TCP handshake (issue #181). Cloneable — clones share the same pool.
#[derive(Debug, Clone)]
pub struct WorkerClient {
    addr: String,
    pool: Arc<ConnectionPool>,
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
        }
    }

    /// The worker address this client talks to.
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Fetch `[offset, offset+len)` of `object` from the worker.
    ///
    /// Returns the raw range bytes on success. A worker-side error (block not
    /// present, backend failure, etc.) surfaces as [`WorkerError::Remote`] with
    /// the worker's message, which the caller can use to trigger a placement
    /// refresh or replica fallback.
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
        let out = encode_request(0, &req)?;
        // Try a pooled connection first; if it was reused and fails (the peer may
        // have closed it while idle), retry once on a fresh dial so a stale
        // pooled socket never turns a healthy peer into a spurious failure. A
        // failure on a *fresh* connection is a real peer error and propagates.
        match self.exchange(&out).await {
            Ok(bytes) => Ok(bytes),
            Err((true, _stale)) => {
                let mut stream = self.pool.fresh(&self.addr).await?;
                stream.write_all(&out).await?;
                stream.flush().await?;
                let bytes = read_range_reply(&mut stream).await?;
                self.pool.release(&self.addr, stream);
                Ok(bytes)
            }
            Err((false, err)) => Err(err),
        }
    }

    /// One request/response over a pooled-or-fresh connection.
    ///
    /// On success releases the connection for reuse. On error returns
    /// `(was_reused, err)` so the caller can decide whether to retry (a reused
    /// connection may simply have been closed while idle).
    async fn exchange(&self, out: &[u8]) -> Result<Vec<u8>, (bool, WorkerError)> {
        let (mut stream, reused) = self
            .pool
            .checkout(&self.addr)
            .await
            .map_err(|e| (false, WorkerError::from(e)))?;
        // On any error, `stream` is dropped (not released), so a half-broken
        // connection is never returned to the pool.
        let result: Result<Vec<u8>, WorkerError> = async {
            stream.write_all(out).await?;
            stream.flush().await?;
            read_range_reply(&mut stream).await
        }
        .await;
        match result {
            Ok(bytes) => {
                self.pool.release(&self.addr, stream);
                Ok(bytes)
            }
            Err(err) => Err((reused, err)),
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
    /// Retries once on a *stale pooled* connection (the peer may have closed it
    /// while idle): because a retry re-sends the full header+body on a fresh
    /// connection, it is safe — the first attempt sent nothing the backend
    /// committed (a mid-stream failure is not retried, it propagates).
    pub async fn put_object(&self, object: &ObjectId, body: &[u8]) -> Result<Version, WorkerError> {
        let header = encode_put_header(
            0,
            &PutRequest {
                object: object.clone(),
                body_len: body.len() as u64,
            },
        )?;
        match self.put_exchange(&header, body).await {
            Ok(version) => Ok(version),
            Err((true, _stale)) => {
                // Stale pooled connection: retry once on a fresh dial.
                let mut stream = self.pool.fresh(&self.addr).await?;
                stream.write_all(&header).await?;
                stream.write_all(body).await?;
                stream.flush().await?;
                let version = read_version_reply(&mut stream).await?;
                self.pool.release(&self.addr, stream);
                Ok(version)
            }
            Err((false, err)) => Err(err),
        }
    }

    /// One PUT exchange over a pooled-or-fresh connection; on error returns
    /// `(was_reused, err)` so a stale pooled connection can be retried.
    async fn put_exchange(
        &self,
        header: &[u8],
        body: &[u8],
    ) -> Result<Version, (bool, WorkerError)> {
        let (mut stream, reused) = self
            .pool
            .checkout(&self.addr)
            .await
            .map_err(|e| (false, WorkerError::from(e)))?;
        let result: Result<Version, WorkerError> = async {
            stream.write_all(header).await?;
            stream.write_all(body).await?;
            stream.flush().await?;
            read_version_reply(&mut stream).await
        }
        .await;
        match result {
            Ok(version) => {
                self.pool.release(&self.addr, stream);
                Ok(version)
            }
            Err(err) => Err((reused, err)),
        }
    }

    /// Delete `object` at the worker (which deletes it at the backend).
    pub async fn delete_object(&self, object: &ObjectId) -> Result<(), WorkerError> {
        let frame = encode_delete(
            0,
            &DeleteRequest {
                object: object.clone(),
            },
        )?;
        match self.delete_exchange(&frame).await {
            Ok(()) => Ok(()),
            Err((true, _stale)) => {
                let mut stream = self.pool.fresh(&self.addr).await?;
                stream.write_all(&frame).await?;
                stream.flush().await?;
                let _ = read_version_reply(&mut stream).await?;
                self.pool.release(&self.addr, stream);
                Ok(())
            }
            Err((false, err)) => Err(err),
        }
    }

    async fn delete_exchange(&self, frame: &[u8]) -> Result<(), (bool, WorkerError)> {
        let (mut stream, reused) = self
            .pool
            .checkout(&self.addr)
            .await
            .map_err(|e| (false, WorkerError::from(e)))?;
        let result: Result<(), WorkerError> = async {
            stream.write_all(frame).await?;
            stream.flush().await?;
            read_version_reply(&mut stream).await.map(|_| ())
        }
        .await;
        match result {
            Ok(()) => {
                self.pool.release(&self.addr, stream);
                Ok(())
            }
            Err(err) => Err((reused, err)),
        }
    }
}

/// Read a framed write/delete reply: OK carries the committed version bytes (a
/// UTF-8 [`Version`]); an `ERROR`-flagged frame carries a message.
async fn read_version_reply(stream: &mut TcpStream) -> Result<Version, WorkerError> {
    let mut header_buf = [0u8; HEADER_LEN];
    stream.read_exact(&mut header_buf).await?;
    let header = FrameHeader::decode(&header_buf)?;
    let mut body = vec![0u8; header.length as usize];
    stream.read_exact(&mut body).await?;
    if header.flags.contains(Flags::ERROR) {
        return Err(WorkerError::Remote(
            String::from_utf8_lossy(&body).into_owned(),
        ));
    }
    Ok(Version::new(String::from_utf8_lossy(&body).into_owned()))
}

/// Read one framed data-plane reply: header, then exactly `length` bytes.
///
/// If the header carries [`Flags::ERROR`], the body is a UTF-8 message and is
/// returned as [`WorkerError::Remote`]; otherwise the body is the raw range.
async fn read_range_reply(stream: &mut TcpStream) -> Result<Vec<u8>, WorkerError> {
    let mut header_buf = [0u8; HEADER_LEN];
    stream.read_exact(&mut header_buf).await?;
    let header = FrameHeader::decode(&header_buf)?;
    if header.msg_type != MsgType::GetRange {
        return Err(WorkerError::NotGetRange(header.msg_type));
    }
    let mut body = vec![0u8; header.length as usize];
    stream.read_exact(&mut body).await?;
    if header.flags.contains(Flags::ERROR) {
        let msg = String::from_utf8_lossy(&body).into_owned();
        return Err(WorkerError::Remote(msg));
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::Backend;
    use talon_transport::{decode_request, encode_error, response_header_ok};
    use tokio::net::TcpListener;

    fn object() -> ObjectId {
        ObjectId::new(Backend::Azure, "container", "path/to/blob.bin")
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

    #[tokio::test]
    async fn fetch_returns_raw_bytes() {
        // Worker returns deterministic bytes for the requested range.
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
    async fn error_flag_becomes_remote_error() {
        let addr = mock_worker(|_req| encode_error(0, "block not present")).await;
        let client = WorkerClient::new(addr);
        let err = client.fetch_range(&object(), 0, 16).await.unwrap_err();
        match err {
            WorkerError::Remote(m) => assert_eq!(m, "block not present"),
            other => panic!("expected Remote, got {other:?}"),
        }
    }

    /// A mock worker that loops, serving many sequential requests on the SAME
    /// connection, and counts accepted connections.
    async fn looping_mock_worker(accepts: Arc<std::sync::atomic::AtomicU32>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                accepts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
                        let payload: Vec<u8> = (0..req.len).map(|i| (i % 251) as u8).collect();
                        let mut out = response_header_ok(0, payload.len() as u32).to_vec();
                        out.extend_from_slice(&payload);
                        sock.write_all(&out).await.unwrap();
                        sock.flush().await.unwrap();
                    }
                });
            }
        });
        addr
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
            WorkerError::Remote(m) => assert_eq!(m, "backend PUT failed"),
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
}
