//! Data-plane connection handling on the Tokio fallback path.
//!
//! This is the readiness-based counterpart to [`uring_conn`](crate::uring_conn).
//! It serves the identical protocol — same frames, same limits, same error
//! semantics — over Tokio's `TcpStream`, and is selected when io_uring is
//! unavailable (old kernel, seccomp) or when `TALON_WORKER_FORCE_TOKIO_DATA_PLANE`
//! is set. io_uring is the default since #299.
//!
//! # Why this path pays for sendfile
//!
//! A Tokio `TcpStream` is non-blocking, so a blocking `sendfile(2)` on it would
//! spuriously `EAGAIN`. The `sendfile_payload` helper therefore round-trips the
//! socket out of and back into the runtime on **every transfer**: `into_std`,
//! `set_nonblocking(false)`, move to the blocking pool, move back,
//! `set_nonblocking(true)`, `from_std`. The ring path needs none of that (see the
//! module docs on `uring_conn`), which is a large part of why it is the default.
//!
//! Keeping the two side by side as sibling modules — rather than one in the
//! library and one inline in `main.rs` — is deliberate: the protocol handling
//! must stay behaviourally identical between them, and divergence is easier to
//! spot when they are structurally parallel.

use std::sync::Arc;
use std::time::Instant;

use talon_core::RequestId;
use talon_transport::data;
use talon_transport::frame::{MsgType, HEADER_LEN};
use talon_transport::{codec, ControlMessage, FrameHeader};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::{send_file_range, ServeOutcome, WorkerObservability, WorkerRuntime, DEFAULT_CHUNK};

/// Serve data-plane range requests on one connection until EOF.
pub async fn handle_conn(
    mut stream: TcpStream,
    worker: Arc<WorkerRuntime>,
    observability: Arc<WorkerObservability>,
) -> anyhow::Result<()> {
    let _active_connection = observability.metrics().track_connection();
    loop {
        let request_started = Instant::now();
        // Read one frame with a per-type size cap enforced BEFORE allocation and
        // a read timeout, so a peer cannot pin a 320 MiB buffer by advertising a
        // huge length and stalling (issue #111).
        let (header, payload) =
            match talon_transport::read_frame(&mut stream, talon_transport::DEFAULT_READ_TIMEOUT)
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

        // A write (Put) or delete (Delete) is handled here and loops; only a
        // GetRange falls through to the read-serve path below.
        if header.msg_type == MsgType::Put {
            handle_put(
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
        if header.msg_type == MsgType::Delete {
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

        // A Control frame on the data plane carries StatObject (#318). Clients
        // already hold a data-plane connection, and only a worker has backend
        // credentials, so answering here avoids a second connection and avoids
        // giving the coordinator backend access.
        if header.msg_type == MsgType::Control {
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

        // Type check BEFORE any per-request work; a data listener only serves
        // GetRange (plus the Put/Delete/Control handled above); other frames are
        // capped tightly by read_frame.
        if header.msg_type != MsgType::GetRange {
            let err = data::encode_error(
                header.request_id,
                "worker only serves GetRange/Put/Delete/StatObject/ListObjects",
            );
            stream.write_all(&err).await?;
            stream.flush().await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
            continue;
        }

        let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
        full.extend_from_slice(&header.encode());
        full.extend_from_slice(&payload);
        let (h, req) = match data::decode_request(&full) {
            Ok(v) => v,
            Err(e) => {
                let err = data::encode_error(header.request_id, &format!("bad request: {e}"));
                stream.write_all(&err).await?;
                stream.flush().await?;
                observability
                    .metrics()
                    .record_request_error(request_started.elapsed());
                continue;
            }
        };

        if !observability.is_ready() {
            let err = data::encode_error(h.request_id, "worker is not ready");
            stream.write_all(&err).await?;
            stream.flush().await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
            continue;
        }

        match worker.serve(&req).await {
            Ok(ServeOutcome::Sendfile(handle)) => {
                // Zero-copy hit: write the frame header async, then stream the
                // block file's fd straight into the socket with sendfile(2) on
                // the blocking pool. The header's advertised length equals the
                // handle length, so a short read can never desync the client.
                let len = handle.len;
                let hdr = data::response_header_ok(h.request_id, len as u32);
                stream.write_all(&hdr).await?;
                stream.flush().await?;
                match sendfile_payload(stream, handle).await {
                    Ok(returned) => {
                        stream = returned;
                        observability
                            .metrics()
                            .record_request_success(len, request_started.elapsed());
                    }
                    Err(error) => {
                        // The header is already on the wire, so we cannot send an
                        // error frame; the connection is desynced. Drop it.
                        observability
                            .metrics()
                            .record_request_error(request_started.elapsed());
                        return Err(error);
                    }
                }
            }
            Ok(ServeOutcome::Bytes(bytes)) => {
                let hdr = data::response_header_ok(h.request_id, bytes.len() as u32);
                stream.write_all(&hdr).await?;
                stream.write_all(&bytes).await?;
                stream.flush().await?;
                observability
                    .metrics()
                    .record_request_success(bytes.len() as u64, request_started.elapsed());
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
                let err = data::encode_error(h.request_id, &e.to_string());
                stream.write_all(&err).await?;
                stream.flush().await?;
                observability
                    .metrics()
                    .record_request_error(request_started.elapsed());
            }
        }
    }
}

/// Handle a `Control` frame on the data plane.
///
/// Only `StatObject` is served here (#318): a client must know an object's
/// version before it can address any block, and only a worker holds the backend
/// credentials needed to resolve it. Anything else gets an `Ack` naming what
/// was rejected, so a client sees the reason rather than a closed connection.
async fn handle_control_frame(
    stream: &mut TcpStream,
    header: &FrameHeader,
    payload: &[u8],
    worker: &Arc<WorkerRuntime>,
    observability: &Arc<WorkerObservability>,
    request_started: Instant,
) -> anyhow::Result<()> {
    let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
    full.extend_from_slice(&header.encode());
    full.extend_from_slice(payload);

    let (h, message) = match codec::decode(&full) {
        Ok(v) => v,
        Err(e) => {
            let reply = codec::encode(
                header.request_id,
                &ControlMessage::Ack {
                    ok: false,
                    detail: Some(format!("bad control message: {e}")),
                },
            )?;
            stream.write_all(&reply).await?;
            stream.flush().await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
            return Ok(());
        }
    };

    let reply = match message {
        ControlMessage::StatObject { object } => {
            if !observability.is_ready() {
                ControlMessage::Ack {
                    ok: false,
                    detail: Some("worker is not ready".into()),
                }
            } else {
                match worker.stat_object(&object).await {
                    Ok(stat) => ControlMessage::ObjectStat {
                        size: stat.len,
                        version: stat.version.as_str().to_string(),
                    },
                    Err(error) => ControlMessage::Ack {
                        ok: false,
                        detail: Some(error.to_string()),
                    },
                }
            }
        }
        ControlMessage::ListObjects { prefix } => {
            if !observability.is_ready() {
                ControlMessage::Ack {
                    ok: false,
                    detail: Some("worker is not ready".into()),
                }
            } else {
                match worker.list_objects(&prefix).await {
                    Ok(entries) => ControlMessage::ObjectList {
                        entries: entries
                            .into_iter()
                            .map(|(path, size)| talon_transport::ObjectEntry { path, size })
                            .collect(),
                    },
                    Err(error) => ControlMessage::Ack {
                        ok: false,
                        detail: Some(error.to_string()),
                    },
                }
            }
        }
        other => ControlMessage::Ack {
            ok: false,
            detail: Some(format!(
                "worker serves only StatObject/ListObjects on the data plane, got {other:?}"
            )),
        },
    };

    let is_error = matches!(reply, ControlMessage::Ack { ok: false, .. });
    let buf = codec::encode(h.request_id, &reply)?;
    stream.write_all(&buf).await?;
    stream.flush().await?;
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

/// Handle a `Put` frame: read the whole object body, write it through to the
/// backend, and cache it (#229).
///
/// The frame `payload` is the small bincode [`PutRequest`] header; the raw object
/// bytes (`body_len` of them) follow on the stream, so we read exactly that many
/// into memory (v1 objects are single-block) and hand them to
/// [`WorkerRuntime::write_object`], which PUTs to the origin then caches the
/// bytes. Replies OK with the committed version, or an `ERROR` frame.
async fn handle_put(
    stream: &mut TcpStream,
    header: &FrameHeader,
    payload: &[u8],
    worker: &Arc<WorkerRuntime>,
    observability: &Arc<WorkerObservability>,
    request_started: Instant,
) -> anyhow::Result<()> {
    let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
    full.extend_from_slice(&header.encode());
    full.extend_from_slice(payload);
    let (h, req) = match data::decode_put_header(&full) {
        Ok(v) => v,
        Err(e) => {
            let err = data::encode_error(header.request_id, &format!("bad put: {e}"));
            stream.write_all(&err).await?;
            stream.flush().await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
            return Ok(());
        }
    };
    if !observability.is_ready() {
        let err = data::encode_error(h.request_id, "worker is not ready");
        stream.write_all(&err).await?;
        stream.flush().await?;
        return Ok(());
    }
    let write_result = if req.body_len <= worker.max_inline_write_bytes() {
        let body_len = usize::try_from(req.body_len)
            .map_err(|_| anyhow::anyhow!("PUT body length is not representable"))?;
        let mut body = vec![0u8; body_len];
        stream.read_exact(&mut body).await?;
        worker
            .write_object(&req.object, bytes::Bytes::from(body))
            .await
    } else {
        let staged = tempfile::NamedTempFile::new()?;
        let file = staged.reopen()?;
        let mut file = tokio::fs::File::from_std(file);
        let mut remaining = req.body_len;
        let mut buffer = vec![0u8; 8 * 1024 * 1024];
        while remaining > 0 {
            let count = remaining.min(buffer.len() as u64) as usize;
            stream.read_exact(&mut buffer[..count]).await?;
            if buffer[..count].iter().all(|byte| *byte == 0) {
                file.seek(std::io::SeekFrom::Current(count as i64)).await?;
            } else {
                file.write_all(&buffer[..count]).await?;
            }
            remaining -= count as u64;
        }
        file.set_len(req.body_len).await?;
        file.flush().await?;
        worker
            .write_object_file(&req.object, staged.path(), req.body_len)
            .await
    };
    match write_result {
        Ok(version) => {
            // Reply OK; the body carries the committed version so the client can
            // record read-after-write consistency.
            let vbytes = version.as_str().as_bytes();
            let hdr = data::response_header_ok(h.request_id, vbytes.len() as u32);
            stream.write_all(&hdr).await?;
            stream.write_all(vbytes).await?;
            stream.flush().await?;
            observability
                .metrics()
                .record_request_success(req.body_len, request_started.elapsed());
        }
        Err(error) => {
            let err = data::encode_error(h.request_id, &error.to_string());
            stream.write_all(&err).await?;
            stream.flush().await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
        }
    }
    Ok(())
}

/// Handle a `Delete` frame: delete the object at the backend and evict locally.
async fn handle_delete(
    stream: &mut TcpStream,
    header: &FrameHeader,
    payload: &[u8],
    worker: &Arc<WorkerRuntime>,
    observability: &Arc<WorkerObservability>,
    request_started: Instant,
) -> anyhow::Result<()> {
    let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
    full.extend_from_slice(&header.encode());
    full.extend_from_slice(payload);
    let (h, req) = match data::decode_delete(&full) {
        Ok(v) => v,
        Err(e) => {
            let err = data::encode_error(header.request_id, &format!("bad delete: {e}"));
            stream.write_all(&err).await?;
            stream.flush().await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
            return Ok(());
        }
    };
    if !observability.is_ready() {
        let err = data::encode_error(h.request_id, "worker is not ready");
        stream.write_all(&err).await?;
        stream.flush().await?;
        return Ok(());
    }
    match worker.delete_object(&req.object).await {
        Ok(()) => {
            let hdr = data::response_header_ok(h.request_id, 0);
            stream.write_all(&hdr).await?;
            stream.flush().await?;
            observability
                .metrics()
                .record_request_success(0, request_started.elapsed());
        }
        Err(error) => {
            let err = data::encode_error(h.request_id, &error.to_string());
            stream.write_all(&err).await?;
            stream.flush().await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
        }
    }
    Ok(())
}

/// Stream a resident block's bytes to the client with `sendfile(2)`.
///
/// `sendfile` is blocking and Linux-specific, and the tokio [`TcpStream`] is
/// non-blocking (a blocking `sendfile` on it would spuriously `EAGAIN`). So we
/// take the stream out of tokio ([`into_std`]), put the socket in blocking mode,
/// run the chunked [`send_file_range`] loop on the blocking helper pool
/// ([`spawn_blocking`]) — never on the async reactor, per DESIGN.md — then
/// restore non-blocking mode and hand the stream back for the next request.
///
/// [`into_std`]: tokio::net::TcpStream::into_std
/// [`spawn_blocking`]: tokio::task::spawn_blocking
async fn sendfile_payload(
    stream: TcpStream,
    handle: talon_core::BlockHandle,
) -> anyhow::Result<TcpStream> {
    let std_stream = stream.into_std()?;
    std_stream.set_nonblocking(false)?;
    let (std_stream, result) = tokio::task::spawn_blocking(move || {
        let res = send_file_range(
            &std_stream,
            &handle.fd,
            handle.offset,
            handle.len,
            DEFAULT_CHUNK,
        );
        (std_stream, res)
    })
    .await?;
    let sent = result?;
    if sent != handle.len {
        // sendfile hit EOF before the advertised length: the block file is
        // shorter than the index claimed. The header already promised `len`
        // bytes, so the connection is desynced — surface an error to drop it.
        anyhow::bail!(
            "sendfile short read: sent {sent} of {} bytes; block file truncated",
            handle.len
        );
    }
    std_stream.set_nonblocking(true)?;
    Ok(TcpStream::from_std(std_stream)?)
}

/// Read one framed control message (header + payload). `Ok(None)` on clean EOF.
pub async fn read_control(stream: &mut TcpStream) -> anyhow::Result<Option<ControlMessage>> {
    let mut header_buf = [0u8; HEADER_LEN];
    match stream.read_exact(&mut header_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let header = FrameHeader::decode(&header_buf)?;
    let mut payload = vec![0u8; header.length as usize];
    stream.read_exact(&mut payload).await?;
    let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
    full.extend_from_slice(&header_buf);
    full.extend_from_slice(&payload);
    let (_h, msg) = codec::decode(&full)?;
    Ok(Some(msg))
}
