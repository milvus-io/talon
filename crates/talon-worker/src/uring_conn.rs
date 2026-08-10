//! Data-plane connection handling on an io_uring ring (#285).
//!
//! This is the completion-based counterpart to the Tokio `handle_conn` in
//! `main.rs`. It serves the identical protocol — same frames, same limits, same
//! error semantics — over monoio's owned-buffer I/O.
//!
//! # Why the sendfile path gets simpler
//!
//! The Tokio path cannot `sendfile` directly onto its socket: a Tokio
//! `TcpStream` is non-blocking, so a blocking `sendfile` on it would spuriously
//! `EAGAIN`. It therefore round-trips the socket out of and back into the
//! runtime on **every transfer** — `into_std`, `set_nonblocking(false)`, move to
//! a blocking thread, move back, `set_nonblocking(true)`, `from_std`.
//!
//! A ring-owned fd needs none of that. It is handed straight to the blocking
//! pool, and the ring resumes on the same stream afterwards. Measured working
//! in #273; the round-trip disappears entirely.
//!
//! # What is preserved exactly
//!
//! - per-message-type payload caps enforced *before* allocation, and read
//!   timeouts (#111) — via [`talon_transport::uring::read_frame`];
//! - `GetRange`/`GetCachedRange`/`AdmitCachedBlock`/`Put`/`Delete` dispatch,
//!   with any other type rejected before per-request work is done;
//! - readiness gating, so an unready worker returns an error frame rather than
//!   serving;
//! - the desync rule: once a response header promising `len` bytes is on the
//!   wire, a short `sendfile` cannot be reported as an error frame, so the
//!   connection is dropped instead.
//!
//! # A Tokio context is still required on the ring thread
//!
//! This handler is monoio-native, but the worker *below* it is not.
//! [`WorkerRuntime`] reaches Tokio internally: `block_store` runs its
//! filesystem I/O on `tokio::task::spawn_blocking` so a large read or
//! write-plus-fsync never stalls the reactor (#115), and parts of the miss path
//! use Tokio timers and sync primitives. On a bare monoio ring those calls
//! panic with *"there is no reactor running"*.
//!
//! So a ring thread must enter a Tokio runtime handle before driving these
//! futures:
//!
//! ```ignore
//! let _guard = tokio_handle.enter();   // makes spawn_blocking/timers work
//! monoio_runtime.block_on(handle_conn(stream, worker, observability));
//! ```
//!
//! This is coexistence, not a layering violation: the ring owns protocol
//! scheduling and the zero-copy path, while Tokio's blocking pool absorbs
//! filesystem work that must not run on either. Removing the dependency would
//! mean porting `block_store` and the miss path to monoio's own blocking pool —
//! worth doing eventually, but a separate change from the data plane itself.

use std::io::{Seek, Write};
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::time::Instant;

use monoio::net::TcpStream;
use talon_core::{BlockHandle, RequestId};
use talon_transport::data;
use talon_transport::frame::{FrameHeader, MsgType, HEADER_LEN};
use talon_transport::uring::{write_all, BufferedFrameReader};
use talon_transport::DataErrorCode;

use crate::data_error::encode_runtime_error;
use crate::observability::WorkerObservability;
use crate::runtime::{ServeOutcome, WorkerRuntime};
use crate::{send_header_and_file_range, DEFAULT_CHUNK};

/// Serve one accepted data-plane connection until EOF or a fatal error.
pub async fn handle_conn(
    mut stream: TcpStream,
    worker: Arc<WorkerRuntime>,
    observability: Arc<WorkerObservability>,
) -> anyhow::Result<()> {
    let _active_connection = observability.metrics().track_connection();
    // One buffered reader per connection. A client that pipelines costs a single
    // `recv` for the whole batch instead of two ring operations per request; a
    // client that does not is unaffected beyond a copy.
    let mut reader = BufferedFrameReader::new();
    loop {
        let request_started = Instant::now();
        let (header, payload) = match reader
            .next_frame(&mut stream, talon_transport::DEFAULT_READ_TIMEOUT)
            .await
        {
            Ok(frame) => frame,
            Err(talon_transport::ReadFrameError::Eof) => return Ok(()),
            Err(talon_transport::ReadFrameError::Timeout) => {
                tracing::debug!("worker: connection read timed out");
                return Ok(());
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        };

        match header.msg_type {
            MsgType::Put => {
                handle_put(
                    &mut stream,
                    &mut reader,
                    &header,
                    &payload,
                    &worker,
                    &observability,
                    request_started,
                )
                .await?;
                continue;
            }
            MsgType::AdmitCachedBlock => {
                if !handle_cached_block_admission(
                    &mut stream,
                    &mut reader,
                    &header,
                    &payload,
                    &worker,
                    &observability,
                    request_started,
                )
                .await?
                {
                    return Ok(());
                }
                continue;
            }
            MsgType::Delete => {
                handle_delete(
                    &mut stream,
                    &header,
                    &payload,
                    &worker,
                    &observability,
                    request_started,
                )
                .await?;
                continue;
            }
            // A Control frame on the data plane carries StatObject (#318):
            // clients already hold this connection, and only a worker has the
            // backend credentials to resolve a version.
            MsgType::Control => {
                handle_control_frame(
                    &mut stream,
                    &header,
                    &payload,
                    &worker,
                    &observability,
                    request_started,
                )
                .await?;
                continue;
            }
            MsgType::GetCachedRange => {
                handle_cached_range(
                    &mut stream,
                    &header,
                    &payload,
                    &worker,
                    &observability,
                    request_started,
                )
                .await?;
                continue;
            }
            MsgType::GetRange => {}
            // A data listener serves only GetRange/Put/Delete; anything else is
            // rejected before any per-request work.
            _ => {
                let err = data::encode_typed_error(
                    header.request_id,
                    DataErrorCode::InvalidRequest,
                    "worker only serves GetRange/Put/Delete/StatObject/ListObjects",
                );
                write_all(&mut stream, err).await?;
                observability
                    .metrics()
                    .record_request_error(request_started.elapsed());
                continue;
            }
        }

        let (h, req) = match data::decode_request(&rejoin(&header, &payload)) {
            Ok(v) => v,
            Err(e) => {
                let err = data::encode_typed_error(
                    header.request_id,
                    DataErrorCode::InvalidRequest,
                    format!("bad request: {e}"),
                );
                write_all(&mut stream, err).await?;
                observability
                    .metrics()
                    .record_request_error(request_started.elapsed());
                continue;
            }
        };

        if !observability.is_ready() {
            let err = data::encode_typed_error(
                h.request_id,
                DataErrorCode::Unavailable,
                "worker is not ready",
            );
            write_all(&mut stream, err).await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
            continue;
        }

        if req.offset.checked_add(req.len).is_none() {
            let err = data::encode_typed_error(
                h.request_id,
                DataErrorCode::InvalidRequest,
                "range offset+len overflows u64",
            );
            write_all(&mut stream, err).await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
            continue;
        }

        match worker.serve(&req).await {
            Ok(ServeOutcome::Sendfile(handle)) => {
                let len = handle.len;
                let hdr = data::response_header_ok(h.request_id, len as u32).to_vec();
                match sendfile_payload(&stream, hdr, handle).await {
                    Ok(()) => observability
                        .metrics()
                        .record_request_success(len, request_started.elapsed()),
                    Err(error) => {
                        // The header is already on the wire, so an error frame
                        // would be read as payload. The connection is desynced;
                        // drop it.
                        observability
                            .metrics()
                            .record_request_error(request_started.elapsed());
                        return Err(error);
                    }
                }
            }
            Ok(ServeOutcome::Bytes(bytes)) => {
                let hdr = data::response_header_ok(h.request_id, bytes.len() as u32).to_vec();
                write_all(&mut stream, hdr).await?;
                let n = bytes.len() as u64;
                write_all(&mut stream, bytes.to_vec()).await?;
                observability
                    .metrics()
                    .record_request_success(n, request_started.elapsed());
            }
            Err(e) => {
                tracing::error!(
                    req = %RequestId(h.request_id),
                    object = %req.object.to_path(),
                    offset = req.offset,
                    len = req.len,
                    error = %e,
                    "serving range failed"
                );
                let err = encode_runtime_error(h.request_id, &e);
                write_all(&mut stream, err).await?;
                observability
                    .metrics()
                    .record_request_error(request_started.elapsed());
            }
        }
    }
}

async fn handle_cached_block_admission(
    stream: &mut TcpStream,
    reader: &mut BufferedFrameReader,
    header: &FrameHeader,
    payload: &[u8],
    worker: &Arc<WorkerRuntime>,
    observability: &Arc<WorkerObservability>,
    request_started: Instant,
) -> anyhow::Result<bool> {
    let (h, req) = match data::decode_cached_block_put_header(&rejoin(header, payload)) {
        Ok(value) => value,
        Err(error) => {
            let reply = data::encode_typed_error(
                header.request_id,
                DataErrorCode::InvalidRequest,
                format!("bad cache admission: {error}"),
            );
            write_all(stream, reply).await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
            return Ok(false);
        }
    };
    if !observability.is_ready() {
        let reply = data::encode_typed_error(
            h.request_id,
            DataErrorCode::Unavailable,
            "worker is not ready",
        );
        write_all(stream, reply).await?;
        return Ok(false);
    }
    if let Err(error) = worker.validate_cached_block_admission(&req) {
        let reply = data::encode_typed_error(
            h.request_id,
            DataErrorCode::InvalidRequest,
            error.to_string(),
        );
        write_all(stream, reply).await?;
        observability
            .metrics()
            .record_request_error(request_started.elapsed());
        return Ok(false);
    }

    let body_len = usize::try_from(req.body_len)
        .map_err(|_| anyhow::anyhow!("cache admission body length is not representable"))?;
    let body = reader
        .read_exact_bytes(stream, body_len, talon_transport::DEFAULT_READ_TIMEOUT)
        .await?;
    match worker
        .admit_cached_block(&req, bytes::Bytes::from(body))
        .await
    {
        Ok(()) => {
            write_all(stream, data::response_header_ok(h.request_id, 0).to_vec()).await?;
            observability
                .metrics()
                .record_request_success(req.body_len, request_started.elapsed());
        }
        Err(error) => {
            write_all(stream, encode_runtime_error(h.request_id, &error)).await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
        }
    }
    Ok(true)
}

async fn handle_cached_range(
    stream: &mut TcpStream,
    header: &FrameHeader,
    payload: &[u8],
    worker: &WorkerRuntime,
    observability: &WorkerObservability,
    request_started: Instant,
) -> std::io::Result<()> {
    let (decoded, request) = match data::decode_cached_request(&rejoin(header, payload)) {
        Ok(value) => value,
        Err(error) => {
            let reply = data::encode_typed_error(
                header.request_id,
                DataErrorCode::InvalidRequest,
                format!("bad cache-only request: {error}"),
            );
            write_all(stream, reply).await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
            return Ok(());
        }
    };
    if !observability.is_ready() {
        let reply = data::encode_typed_error(
            decoded.request_id,
            DataErrorCode::Unavailable,
            "worker is not ready",
        );
        write_all(stream, reply).await?;
        observability
            .metrics()
            .record_request_error(request_started.elapsed());
        return Ok(());
    }
    if request.offset.checked_add(request.len).is_none() {
        let reply = data::encode_typed_error(
            decoded.request_id,
            DataErrorCode::InvalidRequest,
            "range offset+len overflows u64",
        );
        write_all(stream, reply).await?;
        observability
            .metrics()
            .record_request_error(request_started.elapsed());
        return Ok(());
    }
    match worker.serve_cached(&request).await {
        Ok(bytes) => {
            let header = data::response_header_ok(decoded.request_id, bytes.len() as u32);
            write_all(stream, header.to_vec()).await?;
            let len = bytes.len() as u64;
            write_all(stream, bytes.to_vec()).await?;
            observability
                .metrics()
                .record_request_success(len, request_started.elapsed());
        }
        Err(error) => {
            write_all(stream, encode_runtime_error(decoded.request_id, &error)).await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
        }
    }
    Ok(())
}

/// Rebuild the contiguous `header || payload` buffer the `data` decoders expect.
fn rejoin(header: &FrameHeader, payload: &[u8]) -> Vec<u8> {
    let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
    full.extend_from_slice(&header.encode());
    full.extend_from_slice(payload);
    full
}

/// Stream a resident block to the client with `sendfile(2)` from the blocking
/// pool, writing the response frame header in the same blocking step.
///
/// The ring keeps ownership of the socket throughout: only the raw fd crosses
/// to the blocking thread, so there is no `into_std`/`from_std` round-trip and
/// no non-blocking-mode toggling. `sendfile` must not run on the ring — it is
/// blocking, and a slow client would stall every connection this ring owns.
///
/// The header travels with the payload rather than being written from the ring
/// first: that removes one ring-to-pool hand-off (and its futex wake) per
/// request, and `MSG_MORE` lets the kernel put the header and the first
/// payload chunk in one segment.
async fn sendfile_payload(
    stream: &TcpStream,
    header: Vec<u8>,
    handle: BlockHandle,
) -> anyhow::Result<()> {
    let sock_fd = stream.as_raw_fd();
    let len = handle.len;
    let sent = monoio::spawn_blocking(move || {
        // SAFETY-adjacent note: the fd outlives this call because `stream` is
        // borrowed for the duration of the await, and the connection task is the
        // only owner.
        send_header_and_file_range(
            &FdRef(sock_fd),
            &header,
            &handle.fd,
            handle.offset,
            handle.len,
            DEFAULT_CHUNK,
        )
    })
    .await
    .map_err(|e| anyhow::anyhow!("sendfile task failed: {e:?}"))??;

    if sent != len {
        // sendfile hit EOF before the advertised length: the block file is
        // shorter than the index claimed. The header already promised `len`
        // bytes, so the connection is desynced.
        anyhow::bail!("sendfile short read: sent {sent} of {len} bytes; block file truncated");
    }
    Ok(())
}

/// A borrowed raw fd that satisfies [`AsRawFd`] without owning or closing it.
struct FdRef(std::os::fd::RawFd);

impl AsRawFd for FdRef {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.0
    }
}

/// Handle a `Control` frame on the data plane.
///
/// Only `StatObject` is served (#318). Mirrors the Tokio path's semantics
/// exactly — the two data planes must not diverge in what they answer, only in
/// how they move bytes.
async fn handle_control_frame(
    stream: &mut TcpStream,
    header: &FrameHeader,
    payload: &[u8],
    worker: &Arc<WorkerRuntime>,
    observability: &Arc<WorkerObservability>,
    request_started: Instant,
) -> anyhow::Result<()> {
    let (h, message) = match talon_transport::codec::decode(&rejoin(header, payload)) {
        Ok(v) => v,
        Err(e) => {
            let reply = talon_transport::codec::encode(
                header.request_id,
                &talon_transport::ControlMessage::Ack {
                    ok: false,
                    detail: Some(format!("bad control message: {e}")),
                },
            )?;
            write_all(stream, reply).await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
            return Ok(());
        }
    };

    let reply = match message {
        talon_transport::ControlMessage::StatObject { object } => {
            if !observability.is_ready() {
                talon_transport::ControlMessage::Ack {
                    ok: false,
                    detail: Some("worker is not ready".into()),
                }
            } else {
                match worker.stat_object(&object).await {
                    Ok(stat) => talon_transport::ControlMessage::ObjectStat {
                        size: stat.len,
                        version: stat.version.as_str().to_string(),
                    },
                    Err(error) => talon_transport::ControlMessage::Ack {
                        ok: false,
                        detail: Some(error.to_string()),
                    },
                }
            }
        }
        talon_transport::ControlMessage::ListObjects { prefix } => {
            if !observability.is_ready() {
                talon_transport::ControlMessage::Ack {
                    ok: false,
                    detail: Some("worker is not ready".into()),
                }
            } else {
                match worker.list_objects(&prefix).await {
                    Ok(entries) => talon_transport::ControlMessage::ObjectList {
                        entries: entries
                            .into_iter()
                            .map(|(path, size)| talon_transport::ObjectEntry { path, size })
                            .collect(),
                    },
                    Err(error) => talon_transport::ControlMessage::Ack {
                        ok: false,
                        detail: Some(error.to_string()),
                    },
                }
            }
        }
        other => talon_transport::ControlMessage::Ack {
            ok: false,
            detail: Some(format!(
                "worker serves only StatObject/ListObjects on the data plane, got {other:?}"
            )),
        },
    };

    let is_error = matches!(
        reply,
        talon_transport::ControlMessage::Ack { ok: false, .. }
    );
    let buf = talon_transport::codec::encode(h.request_id, &reply)?;
    write_all(stream, buf).await?;
    if is_error {
        observability
            .metrics()
            .record_request_error(request_started.elapsed());
    } else {
        observability
            .metrics()
            .record_request_success(0, request_started.elapsed());
    }
    Ok(())
}

/// Handle a `Put`: read the object body off the wire, write through to the
/// backend, cache it, and reply with the committed version.
async fn handle_put(
    stream: &mut TcpStream,
    reader: &mut BufferedFrameReader,
    header: &FrameHeader,
    payload: &[u8],
    worker: &Arc<WorkerRuntime>,
    observability: &Arc<WorkerObservability>,
    request_started: Instant,
) -> anyhow::Result<()> {
    let (h, req) = match data::decode_put_header(&rejoin(header, payload)) {
        Ok(v) => v,
        Err(e) => {
            let err = data::encode_error(header.request_id, &format!("bad put: {e}"));
            write_all(stream, err).await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
            return Ok(());
        }
    };
    if !observability.is_ready() {
        let err = data::encode_error(h.request_id, "worker is not ready");
        write_all(stream, err).await?;
        return Ok(());
    }

    // The body is unframed and follows the header directly, so it must be read
    // through `reader`: while batching, the reader may already hold some or all
    // of these bytes, and going to the socket would skip them and store a
    // corrupt object.
    let write_result = if req.body_len <= worker.max_inline_write_bytes() {
        let body_len = usize::try_from(req.body_len)
            .map_err(|_| anyhow::anyhow!("PUT body length is not representable"))?;
        let body = reader
            .read_exact_bytes(stream, body_len, talon_transport::DEFAULT_READ_TIMEOUT)
            .await?;
        worker
            .write_object(&req.object, bytes::Bytes::from(body))
            .await
    } else {
        let staged = tempfile::NamedTempFile::new()?;
        let mut file = staged.reopen()?;
        let mut remaining = req.body_len;
        while remaining > 0 {
            let chunk_len = remaining.min(8 * 1024 * 1024) as usize;
            let chunk = reader
                .read_exact_bytes(stream, chunk_len, talon_transport::DEFAULT_READ_TIMEOUT)
                .await?;
            if chunk.iter().all(|byte| *byte == 0) {
                file.seek(std::io::SeekFrom::Current(chunk_len as i64))?;
            } else {
                file.write_all(&chunk)?;
            }
            remaining -= chunk_len as u64;
        }
        file.set_len(req.body_len)?;
        file.flush()?;
        worker
            .write_object_file(&req.object, staged.path(), req.body_len)
            .await
    };

    match write_result {
        Ok(version) => {
            let vbytes = version.as_str().as_bytes().to_vec();
            let hdr = data::response_header_ok(h.request_id, vbytes.len() as u32).to_vec();
            write_all(stream, hdr).await?;
            write_all(stream, vbytes).await?;
            observability
                .metrics()
                .record_request_success(req.body_len, request_started.elapsed());
        }
        Err(error) => {
            let err = data::encode_error(h.request_id, &error.to_string());
            write_all(stream, err).await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
        }
    }
    Ok(())
}

/// Handle a `Delete`: delete at the backend and evict locally.
async fn handle_delete(
    stream: &mut TcpStream,
    header: &FrameHeader,
    payload: &[u8],
    worker: &Arc<WorkerRuntime>,
    observability: &Arc<WorkerObservability>,
    request_started: Instant,
) -> anyhow::Result<()> {
    let (h, req) = match data::decode_delete(&rejoin(header, payload)) {
        Ok(v) => v,
        Err(e) => {
            let err = data::encode_error(header.request_id, &format!("bad delete: {e}"));
            write_all(stream, err).await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
            return Ok(());
        }
    };
    if !observability.is_ready() {
        let err = data::encode_error(h.request_id, "worker is not ready");
        write_all(stream, err).await?;
        return Ok(());
    }
    match worker.delete_object(&req.object).await {
        Ok(()) => {
            let hdr = data::response_header_ok(h.request_id, 0).to_vec();
            write_all(stream, hdr).await?;
            observability
                .metrics()
                .record_request_success(0, request_started.elapsed());
        }
        Err(error) => {
            let err = data::encode_error(h.request_id, &error.to_string());
            write_all(stream, err).await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
        }
    }
    Ok(())
}

/// Adapts [`handle_conn`] to the ring runtime's [`crate::uring_serve::RingHandler`]
/// trait.
///
/// Carries the shared worker state and enforces the same connection cap as the
/// Tokio path (#111). The cap is **per ring**, not global: each ring owns an
/// independent semaphore, so N rings admit `N * max_connections` in total.
/// Callers should divide the global budget by the ring count.
#[derive(Clone)]
pub struct RingConnHandler {
    worker: Arc<WorkerRuntime>,
    observability: Arc<WorkerObservability>,
    limit: Arc<tokio::sync::Semaphore>,
}

impl RingConnHandler {
    /// Build a handler admitting at most `max_connections` concurrent
    /// connections on the ring that owns it.
    pub fn new(
        worker: Arc<WorkerRuntime>,
        observability: Arc<WorkerObservability>,
        max_connections: usize,
    ) -> Self {
        Self {
            worker,
            observability,
            limit: Arc::new(tokio::sync::Semaphore::new(max_connections)),
        }
    }
}

impl crate::uring_serve::RingHandler for RingConnHandler {
    async fn handle(&self, stream: TcpStream) -> anyhow::Result<()> {
        // Acquire before serving and hold for the connection's lifetime, so a
        // flood of idle peers cannot exhaust memory or file descriptors.
        let _permit = self.limit.clone().acquire_owned().await?;
        handle_conn(
            stream,
            Arc::clone(&self.worker),
            Arc::clone(&self.observability),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BlockIndex, InFlightLoads, WholeBlockStore};
    use async_trait::async_trait;
    use bytes::Bytes;
    use monoio::io::{AsyncReadRentExt, AsyncWriteRentExt};
    use std::collections::HashMap;
    use std::sync::Mutex;
    use talon_core::{
        BackendStore, BlockId, NodeId, NodeInfo, NodeRole, ObjectId, ObjectStat, Result, Version,
    };
    use talon_transport::data::{
        encode_cached_block_put_header, encode_delete, encode_put_header, encode_request,
        CachedBlockPutRequest, CachedRangeRequest, DeleteRequest, PutRequest, RangeRequest,
    };
    use talon_transport::Flags;

    fn tmp_root(tag: &str) -> std::path::PathBuf {
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("talon-uring-conn-{tag}-{}-{n}", std::process::id()))
    }

    /// Deterministic ramp bytes so a range's expected content is computable.
    struct RampBackend;

    #[async_trait]
    impl BackendStore for RampBackend {
        async fn fetch_range(&self, _o: &ObjectId, offset: u64, len: u64) -> Result<Bytes> {
            Ok(Bytes::from(
                (0..len)
                    .map(|i| ((offset + i) % 251) as u8)
                    .collect::<Vec<u8>>(),
            ))
        }
        async fn head(&self, _o: &ObjectId) -> Result<ObjectStat> {
            Ok(ObjectStat {
                len: u64::MAX,
                version: Version::new("v1"),
            })
        }
    }

    /// Stores whole-object PUTs so a write-through can be read back.
    #[derive(Default)]
    struct StoringBackend {
        objects: Mutex<HashMap<String, Bytes>>,
        deleted: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl BackendStore for StoringBackend {
        async fn fetch_range(&self, o: &ObjectId, offset: u64, len: u64) -> Result<Bytes> {
            let objs = self.objects.lock().unwrap();
            let full = objs
                .get(&o.to_path())
                .cloned()
                .unwrap_or_else(|| Bytes::from_static(b""));
            let start = (offset as usize).min(full.len());
            let end = (start + len as usize).min(full.len());
            Ok(full.slice(start..end))
        }
        async fn head(&self, o: &ObjectId) -> Result<ObjectStat> {
            let objs = self.objects.lock().unwrap();
            let len = objs.get(&o.to_path()).map(|b| b.len()).unwrap_or(0) as u64;
            Ok(ObjectStat {
                len,
                version: Version::new("v1"),
            })
        }
        async fn put(&self, o: &ObjectId, body: Bytes) -> Result<Version> {
            self.objects.lock().unwrap().insert(o.to_path(), body);
            Ok(Version::new("v2"))
        }
        async fn delete(&self, o: &ObjectId) -> Result<()> {
            self.objects.lock().unwrap().remove(&o.to_path());
            self.deleted.lock().unwrap().push(o.to_path());
            Ok(())
        }
    }

    fn build(
        root: &std::path::Path,
        backend: Arc<dyn BackendStore>,
        block_size: u32,
    ) -> (Arc<WorkerRuntime>, Arc<WorkerObservability>) {
        let index = Arc::new(BlockIndex::new());
        let inflight = Arc::new(InFlightLoads::new());
        let node = NodeInfo {
            id: NodeId::new("w"),
            address: "127.0.0.1:7001".into(),
            role: NodeRole::Worker,
        };
        let obs = Arc::new(
            WorkerObservability::new(
                "c".into(),
                node,
                "127.0.0.1:8001".into(),
                1024,
                Arc::clone(&index),
                Arc::clone(&inflight),
            )
            .unwrap(),
        );
        obs.readiness().set_backend_ready(true);
        obs.readiness().set_store_ready(true);
        obs.readiness().set_control_registered(true);
        let worker = Arc::new(WorkerRuntime::new(
            WholeBlockStore::open(root).unwrap(),
            index,
            inflight,
            backend,
            block_size,
            0,
            obs.metrics().clone(),
        ));
        (worker, obs)
    }

    /// Drive `fut` on an io_uring ring with a Tokio context entered.
    ///
    /// `WorkerRuntime` reaches Tokio internally — `block_store` runs its
    /// filesystem I/O on `tokio::task::spawn_blocking` (#115), and parts of the
    /// miss path use Tokio timers and sync primitives. A bare monoio ring has
    /// no Tokio reactor, so those calls panic with "there is no reactor
    /// running". Entering a Tokio handle on the ring thread makes both
    /// available, which is what the production runtime does too.
    fn run<F: std::future::Future>(fut: F) -> F::Output {
        let tokio_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime");
        let _guard = tokio_rt.enter();
        monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .attach_thread_pool(Box::new(monoio::blocking::DefaultThreadPool::new(4)))
            .enable_timer()
            .build()
            .expect("io_uring runtime")
            .block_on(fut)
    }

    /// Read one framed response: header then exactly `length` body bytes.
    async fn read_response(c: &mut TcpStream) -> (FrameHeader, Vec<u8>) {
        let (r, hdr) = c.read_exact(vec![0u8; HEADER_LEN]).await;
        r.unwrap();
        let header = FrameHeader::decode(&hdr).unwrap();
        let body = if header.length > 0 {
            let (r, b) = c.read_exact(vec![0u8; header.length as usize]).await;
            r.unwrap();
            b
        } else {
            Vec::new()
        };
        (header, body)
    }

    /// The core guarantee: a miss (Bytes path) and a subsequent hit (sendfile
    /// path) return byte-identical ranges. This mirrors the Tokio test in
    /// main.rs assertion-for-assertion, so any divergence between the two data
    /// planes shows up here.
    #[test]
    fn serves_a_hit_via_sendfile_byte_exact() {
        let root = tmp_root("hit");
        run(async {
            let (worker, obs) = build(&root, Arc::new(RampBackend), 16);
            let l = monoio::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            monoio::spawn(async move {
                let (s, _) = l.accept().await.unwrap();
                let _ = handle_conn(s, worker, obs).await;
            });

            let obj = ObjectId::new(talon_core::Backend::Azure, "c", "obj");
            let req = RangeRequest {
                object: obj,
                offset: 3,
                len: 8,
            };
            let expected: Vec<u8> = (0..8u64).map(|i| ((3 + i) % 251) as u8).collect();
            let mut c = TcpStream::connect(addr).await.unwrap();

            // First is a miss (Bytes), second a resident hit (sendfile).
            for pass in 0..2 {
                let (r, _) = c.write_all(encode_request(0, &req).unwrap()).await;
                r.unwrap();
                let (header, body) = read_response(&mut c).await;
                assert!(
                    !header.flags.contains(Flags::ERROR),
                    "pass {pass}: worker returned an error frame"
                );
                assert_eq!(body, expected, "pass {pass}: range bytes must match");
            }
        });
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn admits_a_complete_block_through_the_ring_handler() {
        let root = tmp_root("admit");
        run(async {
            let (worker, obs) = build(&root, Arc::new(RampBackend), 8);
            let retained = Arc::clone(&worker);
            let listener = monoio::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            monoio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let _ = handle_conn(stream, worker, obs).await;
            });
            let object = ObjectId::new(talon_core::Backend::Azure, "c", "admitted");
            let request = CachedBlockPutRequest {
                block: BlockId::new(object.clone(), 0, 8, Version::new("origin-v2")),
                object_len: 8,
                body_len: 8,
            };
            let mut client = TcpStream::connect(address).await.unwrap();
            let mut wire = encode_cached_block_put_header(7, &request).unwrap();
            wire.extend_from_slice(b"gateway!");
            let (result, _) = client.write_all(wire).await;
            result.unwrap();
            let (reply, body) = read_response(&mut client).await;
            assert!(!reply.flags.contains(Flags::ERROR));
            assert!(body.is_empty());
            assert_eq!(
                retained
                    .serve_cached(&CachedRangeRequest {
                        object,
                        version: Version::new("origin-v2"),
                        offset: 0,
                        len: 8,
                    })
                    .await
                    .unwrap(),
                Bytes::from_static(b"gateway!")
            );
        });
        std::fs::remove_dir_all(root).ok();
    }

    /// A range spanning more than one chunk exercises the sendfile loop rather
    /// than a single syscall.
    #[test]
    fn sendfile_streams_a_multi_chunk_range() {
        let root = tmp_root("multi");
        run(async {
            let (worker, obs) = build(&root, Arc::new(RampBackend), 1 << 20);
            let l = monoio::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            monoio::spawn(async move {
                let (s, _) = l.accept().await.unwrap();
                let _ = handle_conn(s, worker, obs).await;
            });

            let obj = ObjectId::new(talon_core::Backend::Azure, "c", "big");
            let len = 300 * 1024u64;
            let req = RangeRequest {
                object: obj,
                offset: 0,
                len,
            };
            let expected: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            let mut c = TcpStream::connect(addr).await.unwrap();
            for _ in 0..2 {
                let (r, _) = c.write_all(encode_request(0, &req).unwrap()).await;
                r.unwrap();
                let (header, body) = read_response(&mut c).await;
                assert!(!header.flags.contains(Flags::ERROR));
                assert_eq!(body.len(), len as usize);
                assert_eq!(body, expected);
            }
        });
        std::fs::remove_dir_all(root).ok();
    }

    /// A frame type the data plane does not serve is rejected with an error
    /// frame, and the connection stays usable for the next request.
    #[test]
    fn rejects_unsupported_frame_type_and_keeps_serving() {
        let root = tmp_root("badtype");
        run(async {
            let (worker, obs) = build(&root, Arc::new(RampBackend), 16);
            let l = monoio::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            monoio::spawn(async move {
                let (s, _) = l.accept().await.unwrap();
                let _ = handle_conn(s, worker, obs).await;
            });

            let mut c = TcpStream::connect(addr).await.unwrap();
            // A Ping is valid on the wire but not served by the data plane.
            let ping = FrameHeader::new(MsgType::Ping, 1, 0).encode().to_vec();
            let (r, _) = c.write_all(ping).await;
            r.unwrap();
            let (header, _) = read_response(&mut c).await;
            assert!(header.flags.contains(Flags::ERROR));

            // The connection must still serve a valid request afterwards.
            let obj = ObjectId::new(talon_core::Backend::Azure, "c", "obj");
            let req = RangeRequest {
                object: obj,
                offset: 0,
                len: 4,
            };
            let (r, _) = c.write_all(encode_request(2, &req).unwrap()).await;
            r.unwrap();
            let (header, body) = read_response(&mut c).await;
            assert!(!header.flags.contains(Flags::ERROR));
            assert_eq!(body, vec![0, 1, 2, 3]);
        });
        std::fs::remove_dir_all(root).ok();
    }

    /// Write-through: a Put stores at the backend and the bytes read back.
    #[test]
    fn put_writes_through_and_reads_back() {
        let root = tmp_root("put");
        run(async {
            let backend = Arc::new(StoringBackend::default());
            let (worker, obs) = build(
                &root,
                Arc::clone(&backend) as Arc<dyn BackendStore>,
                1 << 20,
            );
            let l = monoio::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            monoio::spawn(async move {
                let (s, _) = l.accept().await.unwrap();
                let _ = handle_conn(s, worker, obs).await;
            });

            let obj = ObjectId::new(talon_core::Backend::Azure, "c", "written");
            let body = b"hello uring data plane".to_vec();
            let mut c = TcpStream::connect(addr).await.unwrap();

            let put = encode_put_header(
                1,
                &PutRequest {
                    object: obj.clone(),
                    body_len: body.len() as u64,
                },
            )
            .unwrap();
            let (r, _) = c.write_all(put).await;
            r.unwrap();
            let (r, _) = c.write_all(body.clone()).await;
            r.unwrap();
            let (header, version) = read_response(&mut c).await;
            assert!(!header.flags.contains(Flags::ERROR));
            assert_eq!(String::from_utf8(version).unwrap(), "v2");

            // Read the object back through the same connection.
            let req = RangeRequest {
                object: obj.clone(),
                offset: 0,
                len: body.len() as u64,
            };
            let (r, _) = c.write_all(encode_request(2, &req).unwrap()).await;
            r.unwrap();
            let (header, got) = read_response(&mut c).await;
            assert!(!header.flags.contains(Flags::ERROR));
            assert_eq!(got, body);
        });
        std::fs::remove_dir_all(root).ok();
    }

    /// Delete removes the object at the backend.
    #[test]
    fn delete_removes_the_object() {
        let root = tmp_root("del");
        run(async {
            let backend = Arc::new(StoringBackend::default());
            let (worker, obs) = build(
                &root,
                Arc::clone(&backend) as Arc<dyn BackendStore>,
                1 << 20,
            );
            let l = monoio::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            monoio::spawn(async move {
                let (s, _) = l.accept().await.unwrap();
                let _ = handle_conn(s, worker, obs).await;
            });

            let obj = ObjectId::new(talon_core::Backend::Azure, "c", "doomed");
            let mut c = TcpStream::connect(addr).await.unwrap();
            let put = encode_put_header(
                1,
                &PutRequest {
                    object: obj.clone(),
                    body_len: 3,
                },
            )
            .unwrap();
            let (r, _) = c.write_all(put).await;
            r.unwrap();
            let (r, _) = c.write_all(b"abc".to_vec()).await;
            r.unwrap();
            let _ = read_response(&mut c).await;

            let (r, _) = c
                .write_all(
                    encode_delete(
                        2,
                        &DeleteRequest {
                            object: obj.clone(),
                        },
                    )
                    .unwrap(),
                )
                .await;
            r.unwrap();
            let (header, _) = read_response(&mut c).await;
            assert!(!header.flags.contains(Flags::ERROR));
            assert_eq!(backend.deleted.lock().unwrap().len(), 1);
        });
        std::fs::remove_dir_all(root).ok();
    }

    /// An unready worker refuses to serve rather than returning stale or empty
    /// data.
    #[test]
    fn unready_worker_returns_an_error_frame() {
        let root = tmp_root("unready");
        run(async {
            let (worker, obs) = build(&root, Arc::new(RampBackend), 16);
            obs.readiness().set_backend_ready(false);
            let l = monoio::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            monoio::spawn(async move {
                let (s, _) = l.accept().await.unwrap();
                let _ = handle_conn(s, worker, obs).await;
            });

            let obj = ObjectId::new(talon_core::Backend::Azure, "c", "obj");
            let req = RangeRequest {
                object: obj,
                offset: 0,
                len: 4,
            };
            let mut c = TcpStream::connect(addr).await.unwrap();
            let (r, _) = c.write_all(encode_request(0, &req).unwrap()).await;
            r.unwrap();
            let (header, _) = read_response(&mut c).await;
            assert!(header.flags.contains(Flags::ERROR));
        });
        std::fs::remove_dir_all(root).ok();
    }

    /// A clean disconnect ends the loop without an error.
    #[test]
    fn clean_disconnect_ends_the_connection() {
        let root = tmp_root("eof");
        run(async {
            let (worker, obs) = build(&root, Arc::new(RampBackend), 16);
            let l = monoio::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            let served = monoio::spawn(async move {
                let (s, _) = l.accept().await.unwrap();
                handle_conn(s, worker, obs).await
            });
            let c = TcpStream::connect(addr).await.unwrap();
            drop(c);
            assert!(served.await.is_ok());
        });
        std::fs::remove_dir_all(root).ok();
    }

    /// The RingConnHandler admits up to its cap and serves real traffic through
    /// the ring runtime — the wiring main.rs uses.
    #[test]
    fn ring_handler_serves_through_the_ring_runtime() {
        use crate::uring_serve::serve;
        let root = tmp_root("ringwire");
        let tokio_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let (worker, obs) = {
            let _g = tokio_rt.enter();
            build(&root, Arc::new(RampBackend), 16)
        };

        // Reserve then release a port so the rings can SO_REUSEPORT bind it.
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap().to_string();
        drop(probe);

        let handler = RingConnHandler::new(worker, obs, 8);
        let handle = tokio_rt.handle().clone();
        let serve_addr = addr.clone();
        std::thread::spawn(move || {
            let _ = serve(serve_addr, 2, 2, handler, handle);
        });

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while std::net::TcpStream::connect(&addr).is_err() {
            assert!(std::time::Instant::now() < deadline, "rings never bound");
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        // Drive a real request/response with a plain std client, proving a
        // Tokio-side client interoperates with the ring data plane unchanged.
        use std::io::{Read, Write};
        let obj = ObjectId::new(talon_core::Backend::Azure, "c", "obj");
        let req = RangeRequest {
            object: obj,
            offset: 3,
            len: 8,
        };
        let expected: Vec<u8> = (0..8u64).map(|i| ((3 + i) % 251) as u8).collect();
        let mut c = std::net::TcpStream::connect(&addr).unwrap();
        c.write_all(&encode_request(0, &req).unwrap()).unwrap();
        let mut hdr = [0u8; HEADER_LEN];
        c.read_exact(&mut hdr).unwrap();
        let header = FrameHeader::decode(&hdr).unwrap();
        assert!(!header.flags.contains(Flags::ERROR));
        let mut body = vec![0u8; header.length as usize];
        c.read_exact(&mut body).unwrap();
        assert_eq!(body, expected);

        std::fs::remove_dir_all(root).ok();
    }
}
