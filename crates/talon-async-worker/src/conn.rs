// SPDX-License-Identifier: Apache-2.0
//! The data-plane connection loop.
//!
//! Speaks the same frame protocol `talon-worker` speaks, so an existing client
//! reaches this worker without knowing the difference. Reads are served from
//! the extent cache; writes are refused.
//!
//! # Refusing a write without desyncing the connection
//!
//! A `Put` frame is a small bincode header followed by `body_len` raw bytes
//! that are *not* part of the frame. Replying with an error and looping would
//! leave those bytes in the socket, and the next frame read would start
//! mid-body and see garbage — a rejected write would corrupt every subsequent
//! request on that connection. So the body is drained before the refusal is
//! sent. See ADR 0005 §8 for why the refusal happens at all.

use std::sync::Arc;
use std::time::Instant;

use talon_transport::codec;
use talon_transport::data::{self, RangeRequest};
use talon_transport::frame::{FrameHeader, MsgType, HEADER_LEN};
use talon_transport::ControlMessage;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::metrics::AsyncWorkerMetrics;
use crate::runtime::{AsyncWorkerRuntime, ServeOutcome};
use crate::sendfile::{send_file_range, DEFAULT_CHUNK};

/// Bytes drained per read when discarding a refused write's body.
const DRAIN_CHUNK: usize = 64 * 1024;

/// Serve one connection until EOF.
///
/// # Errors
/// Only for failures that desync the connection and force it closed. A refused
/// or failed *request* is answered with an error frame and the loop continues.
pub async fn handle_conn(
    mut stream: TcpStream,
    worker: Arc<AsyncWorkerRuntime>,
    metrics: Arc<AsyncWorkerMetrics>,
) -> anyhow::Result<()> {
    let _connection = metrics.track_connection();

    loop {
        let started = Instant::now();
        let (header, payload) =
            match talon_transport::read_frame(&mut stream, talon_transport::DEFAULT_READ_TIMEOUT)
                .await
            {
                Ok(frame) => frame,
                Err(talon_transport::ReadFrameError::Eof) => return Ok(()),
                Err(talon_transport::ReadFrameError::Timeout) => {
                    tracing::debug!("async worker: connection read timed out");
                    return Ok(());
                }
                Err(e) => return Err(anyhow::anyhow!(e)),
            };

        match header.msg_type {
            MsgType::GetRange => {
                stream = serve_range(stream, &header, &payload, &worker, &metrics, started).await?;
            }
            MsgType::Put => {
                refuse_put(&mut stream, &header, &payload, &metrics, started).await?;
            }
            MsgType::Delete => {
                refuse_delete(&mut stream, &header, &payload, &metrics, started).await?;
            }
            MsgType::Control => {
                serve_control(&mut stream, &header, &payload, &worker, &metrics, started).await?;
            }
            MsgType::Ping => {
                let hdr = data::response_header_ok(header.request_id, 0);
                stream.write_all(&hdr).await?;
                stream.flush().await?;
            }
            MsgType::Get => {
                // Whole-block fetch. There are no blocks here, and the client
                // should be sending GetRange; say so rather than failing vaguely.
                reply_error(
                    &mut stream,
                    header.request_id,
                    "async worker has no blocks; use GetRange",
                    &metrics,
                    started,
                )
                .await?;
            }
        }
    }
}

/// Rebuild the framed buffer a decoder expects from a split header and payload.
fn reassemble(header: &FrameHeader, payload: &[u8]) -> Vec<u8> {
    let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
    full.extend_from_slice(&header.encode());
    full.extend_from_slice(payload);
    full
}

async fn reply_error(
    stream: &mut TcpStream,
    request_id: u32,
    message: &str,
    metrics: &AsyncWorkerMetrics,
    started: Instant,
) -> anyhow::Result<()> {
    let frame = data::encode_error(request_id, message);
    stream.write_all(&frame).await?;
    stream.flush().await?;
    metrics.record_error(started.elapsed());
    Ok(())
}

/// Serve a `GetRange`.
///
/// Takes and returns the stream by value because the zero-copy path converts it
/// to a blocking `std` socket for the duration of the `sendfile`.
async fn serve_range(
    mut stream: TcpStream,
    header: &FrameHeader,
    payload: &[u8],
    worker: &Arc<AsyncWorkerRuntime>,
    metrics: &Arc<AsyncWorkerMetrics>,
    started: Instant,
) -> anyhow::Result<TcpStream> {
    let request: RangeRequest = match data::decode_request(&reassemble(header, payload)) {
        Ok((_, req)) => req,
        Err(e) => {
            reply_error(
                &mut stream,
                header.request_id,
                &format!("bad request: {e}"),
                metrics,
                started,
            )
            .await?;
            return Ok(stream);
        }
    };

    match worker.serve(&request).await {
        Ok(ServeOutcome::Bytes(bytes)) => {
            let hdr = data::response_header_ok(header.request_id, bytes.len() as u32);
            stream.write_all(&hdr).await?;
            stream.write_all(&bytes).await?;
            stream.flush().await?;
            metrics.record_success(bytes.len() as u64, false, started.elapsed());
            Ok(stream)
        }
        Ok(ServeOutcome::Sendfile(pinned)) => {
            let len = pinned.len();
            let hdr = data::response_header_ok(header.request_id, len as u32);
            stream.write_all(&hdr).await?;
            stream.flush().await?;
            // The header has promised `len` bytes. From here a failure desyncs
            // the connection, so it propagates and drops it rather than trying
            // to send an error frame the client would read as payload.
            let stream = sendfile_payload(stream, pinned).await?;
            metrics.record_success(len, true, started.elapsed());
            Ok(stream)
        }
        Err(e) => {
            tracing::warn!(
                object = %request.object.to_path(),
                offset = request.offset,
                len = request.len,
                error = %e,
                "serving range failed"
            );
            reply_error(
                &mut stream,
                header.request_id,
                &e.to_string(),
                metrics,
                started,
            )
            .await?;
            Ok(stream)
        }
    }
}

/// Stream a pinned extent into the socket with `sendfile`.
///
/// The pin is moved into the blocking task and dropped only after the transfer
/// finishes, so region reclamation cannot overwrite the bytes underneath it.
async fn sendfile_payload(
    stream: TcpStream,
    pinned: crate::cache::region::PinnedExtent,
) -> anyhow::Result<TcpStream> {
    let len = pinned.len();
    let std_stream = stream.into_std()?;
    std_stream.set_nonblocking(false)?;

    let (std_stream, sent) = tokio::task::spawn_blocking(move || {
        let handle = pinned.handle();
        let result = send_file_range(
            &std_stream,
            &handle.fd,
            handle.offset,
            handle.len,
            DEFAULT_CHUNK,
        );
        // `pinned` is dropped here, after the transfer and not before.
        drop(pinned);
        (std_stream, result)
    })
    .await?;
    let sent = sent?;

    if sent != len {
        anyhow::bail!("sendfile sent {sent} of {len} bytes; shard file truncated");
    }
    std_stream.set_nonblocking(true)?;
    Ok(TcpStream::from_std(std_stream)?)
}

/// Refuse a write, draining its trailing body first.
async fn refuse_put(
    stream: &mut TcpStream,
    header: &FrameHeader,
    payload: &[u8],
    metrics: &AsyncWorkerMetrics,
    started: Instant,
) -> anyhow::Result<()> {
    // Decode only to learn the body length and the object name. A header we
    // cannot parse leaves an unknown number of trailing bytes, and guessing
    // would desync the connection — close it instead.
    let (object, body_len) = match data::decode_put_header(&reassemble(header, payload)) {
        Ok((_, req)) => (req.object, req.body_len),
        Err(e) => {
            anyhow::bail!("unparseable Put header, cannot find the body to drain: {e}");
        }
    };

    drain(stream, body_len).await?;
    metrics.record_write_rejected();
    let message = AsyncWorkerRuntime::write_unsupported(&object).to_string();
    tracing::warn!(object = %object.to_path(), body_len, "refused a write");
    reply_error(stream, header.request_id, &message, metrics, started).await
}

/// Refuse a delete. No trailing body to drain.
async fn refuse_delete(
    stream: &mut TcpStream,
    header: &FrameHeader,
    payload: &[u8],
    metrics: &AsyncWorkerMetrics,
    started: Instant,
) -> anyhow::Result<()> {
    let detail = match data::decode_delete(&reassemble(header, payload)) {
        Ok((_, req)) => AsyncWorkerRuntime::write_unsupported(&req.object).to_string(),
        Err(e) => format!("async worker is read-only; also, bad delete request: {e}"),
    };
    metrics.record_write_rejected();
    reply_error(stream, header.request_id, &detail, metrics, started).await
}

/// Read and discard exactly `n` bytes.
async fn drain(stream: &mut TcpStream, n: u64) -> anyhow::Result<()> {
    let mut remaining = n;
    let mut buf = vec![0u8; DRAIN_CHUNK.min(n.max(1) as usize)];
    while remaining > 0 {
        let want = remaining.min(buf.len() as u64) as usize;
        stream.read_exact(&mut buf[..want]).await?;
        remaining -= want as u64;
    }
    Ok(())
}

/// Serve a control frame. Only the read-only queries are answered.
async fn serve_control(
    stream: &mut TcpStream,
    header: &FrameHeader,
    payload: &[u8],
    worker: &Arc<AsyncWorkerRuntime>,
    metrics: &AsyncWorkerMetrics,
    started: Instant,
) -> anyhow::Result<()> {
    let message = match codec::decode(&reassemble(header, payload)) {
        Ok((_, m)) => m,
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
            metrics.record_error(started.elapsed());
            return Ok(());
        }
    };

    let reply = match message {
        ControlMessage::StatObject { object } => match worker.stat_object(&object).await {
            Ok(stat) => ControlMessage::ObjectStat {
                size: stat.len,
                version: stat.version.as_str().to_string(),
            },
            Err(error) => ControlMessage::Ack {
                ok: false,
                detail: Some(error.to_string()),
            },
        },
        other => ControlMessage::Ack {
            ok: false,
            detail: Some(format!(
                "async worker serves only StatObject on the data plane, got {other:?}"
            )),
        },
    };

    let failed = matches!(reply, ControlMessage::Ack { ok: false, .. });
    let buf = codec::encode(header.request_id, &reply)?;
    stream.write_all(&buf).await?;
    stream.flush().await?;
    if failed {
        metrics.record_error(started.elapsed());
    } else {
        metrics.record_success(0, false, started.elapsed());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::tiered::{ExtentCacheConfig, TieredExtentCache};
    use bytes::Bytes;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::path::PathBuf;
    use talon_core::{Backend, BackendStore, ObjectId, ObjectStat, Result as CoreResult, Version};
    use talon_transport::data::{DeleteRequest, PutRequest};
    use tokio::net::TcpListener;

    fn tmp_root(tag: &str) -> PathBuf {
        let mut h = DefaultHasher::new();
        std::time::SystemTime::now().hash(&mut h);
        std::thread::current().id().hash(&mut h);
        tag.hash(&mut h);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "talon-conn-{}-{:x}",
            std::process::id(),
            h.finish()
        ));
        p
    }

    #[derive(Debug)]
    struct StubBackend {
        body: Bytes,
    }

    #[async_trait::async_trait]
    impl BackendStore for StubBackend {
        async fn fetch_range(&self, _o: &ObjectId, offset: u64, len: u64) -> CoreResult<Bytes> {
            let start = (offset as usize).min(self.body.len());
            let end = (start + len as usize).min(self.body.len());
            Ok(self.body.slice(start..end))
        }
        async fn head(&self, _o: &ObjectId) -> CoreResult<ObjectStat> {
            Ok(ObjectStat {
                len: self.body.len() as u64,
                version: Version::new("etag-1"),
            })
        }
    }

    fn object(path: &str) -> ObjectId {
        ObjectId::new(Backend::S3, "bucket", path)
    }

    /// Start a worker on loopback; return its address, metrics and root dir.
    async fn serve(
        tag: &str,
        body: &[u8],
        memory_bytes: u64,
    ) -> (
        std::net::SocketAddr,
        Arc<AsyncWorkerMetrics>,
        Arc<AsyncWorkerRuntime>,
        PathBuf,
    ) {
        let root = tmp_root(tag);
        let cache = TieredExtentCache::new(&ExtentCacheConfig {
            memory_bytes,
            memory_shards: 1,
            disk_dir: Some(root.clone()),
            disk_bytes: crate::cache::region::REGION_SIZE * 2,
            disk_shards: 1,
            disk_checksums: false,
        })
        .await
        .unwrap();
        let worker = Arc::new(AsyncWorkerRuntime::new(
            cache,
            Arc::new(StubBackend {
                body: Bytes::copy_from_slice(body),
            }),
        ));
        let metrics = Arc::new(AsyncWorkerMetrics::new("s3"));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (w, m) = (Arc::clone(&worker), Arc::clone(&metrics));
        tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                let (w, m) = (Arc::clone(&w), Arc::clone(&m));
                tokio::spawn(async move {
                    let _ = handle_conn(sock, w, m).await;
                });
            }
        });
        (addr, metrics, worker, root)
    }

    /// Read one response frame: header plus its advertised payload.
    async fn read_response(stream: &mut TcpStream) -> (FrameHeader, Vec<u8>) {
        let mut head = [0u8; HEADER_LEN];
        stream.read_exact(&mut head).await.unwrap();
        let header = FrameHeader::decode(&head).unwrap();
        let mut payload = vec![0u8; header.length as usize];
        if !payload.is_empty() {
            stream.read_exact(&mut payload).await.unwrap();
        }
        (header, payload)
    }

    fn is_error(header: &FrameHeader) -> bool {
        header.flags.contains(talon_transport::frame::Flags::ERROR)
    }

    #[tokio::test]
    async fn a_range_read_comes_back_over_the_wire() {
        let (addr, _m, _w, root) = serve("get", b"hello world", 1 << 20).await;
        let mut c = TcpStream::connect(addr).await.unwrap();

        let req = data::encode_request(
            1,
            &RangeRequest {
                object: object("f.parquet"),
                offset: 6,
                len: 5,
            },
        )
        .unwrap();
        c.write_all(&req).await.unwrap();

        let (header, body) = read_response(&mut c).await;
        assert!(!is_error(&header));
        assert_eq!(body, b"world");

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_zero_copy_response_is_byte_exact() {
        // L1 off, so the second read is served by sendfile from a pinned extent.
        let (addr, metrics, worker, root) = serve("zerocopy", &[0x5Au8; 8192], 0).await;
        let mut c = TcpStream::connect(addr).await.unwrap();

        for i in 0..2u32 {
            let req = data::encode_request(
                i + 1,
                &RangeRequest {
                    object: object("f.parquet"),
                    offset: 0,
                    len: 4096,
                },
            )
            .unwrap();
            c.write_all(&req).await.unwrap();
            let (header, body) = read_response(&mut c).await;
            assert!(!is_error(&header), "read {i} failed");
            assert_eq!(body.len(), 4096);
            assert!(body.iter().all(|&b| b == 0x5A), "read {i} corrupt");
            if i == 0 {
                worker.cache().flush().await;
            }
        }

        assert!(
            metrics.render().contains("sendfile_responses_total 1"),
            "expected the second read to go zero-copy"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_write_is_refused_and_the_connection_stays_usable() {
        // The subtle part: the refused Put's body must be drained, or the next
        // frame read starts mid-body and every later request is garbage.
        let (addr, metrics, _w, root) = serve("put", b"hello world", 1 << 20).await;
        let mut c = TcpStream::connect(addr).await.unwrap();

        let body = vec![0xEEu8; 4096];
        let hdr = data::encode_put_header(
            1,
            &PutRequest {
                object: object("f.parquet"),
                body_len: body.len() as u64,
            },
        )
        .unwrap();
        c.write_all(&hdr).await.unwrap();
        c.write_all(&body).await.unwrap();

        let (header, detail) = read_response(&mut c).await;
        assert!(is_error(&header));
        let text = String::from_utf8_lossy(&detail);
        assert!(text.contains("read-only"), "unhelpful refusal: {text}");
        assert!(text.contains("talon-worker"), "must name the pool: {text}");

        // The connection must still work: this is the desync regression test.
        let req = data::encode_request(
            2,
            &RangeRequest {
                object: object("f.parquet"),
                offset: 0,
                len: 5,
            },
        )
        .unwrap();
        c.write_all(&req).await.unwrap();
        let (header, got) = read_response(&mut c).await;
        assert!(!is_error(&header), "connection desynced after the refusal");
        assert_eq!(got, b"hello");

        assert!(metrics.render().contains("writes_rejected_total 1"));

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_delete_is_refused_and_the_connection_stays_usable() {
        let (addr, _m, _w, root) = serve("delete", b"hello world", 1 << 20).await;
        let mut c = TcpStream::connect(addr).await.unwrap();

        let frame = data::encode_delete(
            1,
            &DeleteRequest {
                object: object("f.parquet"),
            },
        )
        .unwrap();
        c.write_all(&frame).await.unwrap();
        let (header, detail) = read_response(&mut c).await;
        assert!(is_error(&header));
        assert!(String::from_utf8_lossy(&detail).contains("read-only"));

        let req = data::encode_request(
            2,
            &RangeRequest {
                object: object("f.parquet"),
                offset: 0,
                len: 5,
            },
        )
        .unwrap();
        c.write_all(&req).await.unwrap();
        let (header, got) = read_response(&mut c).await;
        assert!(!is_error(&header));
        assert_eq!(got, b"hello");

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn stat_object_answers_size_and_version() {
        let (addr, _m, _w, root) = serve("stat", b"hello world", 1 << 20).await;
        let mut c = TcpStream::connect(addr).await.unwrap();

        let frame = codec::encode(
            1,
            &ControlMessage::StatObject {
                object: object("f.parquet"),
            },
        )
        .unwrap();
        c.write_all(&frame).await.unwrap();

        let (header, payload) = read_response(&mut c).await;
        let mut full = Vec::new();
        full.extend_from_slice(&header.encode());
        full.extend_from_slice(&payload);
        let (_, reply) = codec::decode(&full).unwrap();
        match reply {
            ControlMessage::ObjectStat { size, version } => {
                assert_eq!(size, 11);
                assert_eq!(version, "etag-1");
            }
            other => panic!("expected ObjectStat, got {other:?}"),
        }

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn several_reads_pipeline_on_one_connection() {
        let (addr, _m, _w, root) = serve("pipeline", b"0123456789", 1 << 20).await;
        let mut c = TcpStream::connect(addr).await.unwrap();

        for i in 0..5u32 {
            let req = data::encode_request(
                i + 1,
                &RangeRequest {
                    object: object("f.parquet"),
                    offset: i as u64,
                    len: 2,
                },
            )
            .unwrap();
            c.write_all(&req).await.unwrap();
            let (header, got) = read_response(&mut c).await;
            assert_eq!(header.request_id, i + 1, "response ids out of order");
            assert_eq!(got, format!("{}{}", i, i + 1).as_bytes());
        }

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_whole_block_get_is_refused_with_a_usable_message() {
        let (addr, _m, _w, root) = serve("get-block", b"hello", 1 << 20).await;
        let mut c = TcpStream::connect(addr).await.unwrap();

        // A Get frame carrying the payload a GetRange would.
        let encoded = data::encode_request(
            1,
            &RangeRequest {
                object: object("f.parquet"),
                offset: 0,
                len: 5,
            },
        )
        .unwrap();
        let body = &encoded[HEADER_LEN..];
        let mut frame = FrameHeader::new(MsgType::Get, 1, body.len() as u32)
            .encode()
            .to_vec();
        frame.extend_from_slice(body);

        c.write_all(&frame).await.unwrap();
        let (header, detail) = read_response(&mut c).await;
        assert!(is_error(&header));
        assert!(String::from_utf8_lossy(&detail).contains("GetRange"));

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_clean_disconnect_is_not_an_error() {
        let (addr, _m, _w, root) = serve("eof", b"hello", 1 << 20).await;
        let c = TcpStream::connect(addr).await.unwrap();
        drop(c);
        tokio::task::yield_now().await;
        std::fs::remove_dir_all(root).ok();
    }
}
