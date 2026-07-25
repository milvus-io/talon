//! Frame I/O for the io_uring (monoio) data plane.
//!
//! This is the completion-based counterpart to [`crate::limits::read_frame`].
//! The two exist side by side because the runtimes have incompatible I/O
//! models, not because the wire format differs — both read the identical
//! [`FrameHeader`] and payload bytes, and both enforce the identical limits.
//!
//! # Why a separate function
//!
//! Tokio's `AsyncRead` **borrows** a buffer for the duration of a read. io_uring
//! is completion-based: the kernel owns the buffer while the operation is in
//! flight, so monoio's [`AsyncReadRent`] instead **moves** the buffer in and
//! hands it back with the result. A borrowed-buffer signature cannot be made
//! sound over a completion ring, so the reader is rewritten rather than
//! abstracted over both.
//!
//! # Limits are preserved verbatim
//!
//! The DoS protections from issue #111 are the reason this module cannot be a
//! thin wrapper, and they are reimplemented exactly:
//!
//! - the advertised length is checked against the **per-message-type cap**
//!   ([`max_payload_for`]) *before* the payload buffer is allocated, so a peer
//!   cannot pin a 320 MiB allocation by lying about a control frame's size;
//! - both the header and payload reads are bounded by a **timeout**, so a peer
//!   that stalls mid-frame is dropped rather than holding the buffer forever;
//! - a clean EOF at a frame boundary is [`ReadFrameError::Eof`], not an error.
//!
//! Connection-count limiting is orthogonal and still provided by
//! [`crate::limits::ConnectionLimit`], which is runtime-agnostic.

use std::io;
use std::time::Duration;

use monoio::io::{AsyncReadRent, AsyncReadRentExt, AsyncWriteRent, AsyncWriteRentExt};

use crate::frame::{FrameHeader, HEADER_LEN};
use crate::limits::{max_payload_for, ReadFrameError};

/// Read exactly one frame from `stream`, allocating the payload buffer **only
/// after** the message type's size cap is satisfied, and bounding each read
/// with `timeout`.
///
/// Returns the decoded header and the raw payload bytes. A clean EOF at a frame
/// boundary is [`ReadFrameError::Eof`].
///
/// This is [`crate::limits::read_frame`] for a monoio stream; see the module
/// docs for why the two cannot share an implementation.
pub async fn read_frame<S>(
    stream: &mut S,
    timeout: Duration,
) -> Result<(FrameHeader, Vec<u8>), ReadFrameError>
where
    S: AsyncReadRent,
{
    // Header. `read_exact` returns (result, buffer) because the kernel owned
    // the buffer for the duration of the operation.
    let header_buf = vec![0u8; HEADER_LEN];
    let (res, header_buf) =
        match monoio::time::timeout(timeout, stream.read_exact(header_buf)).await {
            Ok(pair) => pair,
            Err(_) => return Err(ReadFrameError::Timeout),
        };
    match res {
        Ok(n) if n == HEADER_LEN => {}
        // A short read at a frame boundary is a clean disconnect.
        Ok(_) => return Err(ReadFrameError::Eof),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Err(ReadFrameError::Eof),
        Err(e) => return Err(ReadFrameError::Io(e)),
    }

    // Decode (validates magic/version/type and the global max), then enforce the
    // per-type cap BEFORE allocating the payload.
    let header = FrameHeader::decode(&header_buf)?;
    let cap = max_payload_for(header.msg_type);
    if header.length > cap {
        return Err(ReadFrameError::PayloadTooLarge {
            msg_type: header.msg_type,
            length: header.length,
            cap,
        });
    }

    if header.length == 0 {
        return Ok((header, Vec::new()));
    }

    let payload = vec![0u8; header.length as usize];
    let (res, payload) = match monoio::time::timeout(timeout, stream.read_exact(payload)).await {
        Ok(pair) => pair,
        Err(_) => return Err(ReadFrameError::Timeout),
    };
    match res {
        Ok(n) if n == header.length as usize => Ok((header, payload)),
        // The header promised more than arrived: the peer went away mid-frame.
        Ok(_) => Err(ReadFrameError::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "payload truncated",
        ))),
        Err(e) => Err(ReadFrameError::Io(e)),
    }
}

/// Write a whole buffer to a monoio stream, returning the buffer.
///
/// monoio's `write_all` moves the buffer into the kernel and returns it, so
/// callers that reuse a buffer get it back; callers that do not can drop it.
/// Errors are surfaced as [`io::Error`], matching the Tokio path.
pub async fn write_all<S>(stream: &mut S, buf: Vec<u8>) -> io::Result<Vec<u8>>
where
    S: AsyncWriteRent,
{
    let (res, buf) = stream.write_all(buf).await;
    res?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::MsgType;
    use crate::limits::{MAX_CONTROL_PAYLOAD_LEN, MAX_PING_PAYLOAD_LEN};
    use monoio::net::{TcpListener, TcpStream};

    const T: Duration = Duration::from_secs(5);

    /// Build a runtime with a blocking pool attached (monoio panics without one).
    /// Run `fut` on a fresh io_uring runtime with the timer enabled.
    fn run<F: std::future::Future>(fut: F) -> F::Output {
        monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .enable_timer()
            .build()
            .expect("io_uring runtime")
            .block_on(fut)
    }

    fn frame(msg_type: MsgType, payload: &[u8]) -> Vec<u8> {
        let header = FrameHeader::new(msg_type, 7, payload.len() as u32);
        let mut buf = header.encode().to_vec();
        buf.extend_from_slice(payload);
        buf
    }

    /// Serve `script` bytes to one client, then optionally hold the connection
    /// open so the reader hits its timeout rather than EOF.
    async fn serve_once(listener: TcpListener, script: Vec<u8>, hold: bool) {
        let (mut s, _) = listener.accept().await.unwrap();
        let (r, _) = s.write_all(script).await;
        r.unwrap();
        if hold {
            // Never send the rest; let the reader time out.
            monoio::time::sleep(Duration::from_secs(30)).await;
        }
    }

    #[test]
    fn reads_a_frame_and_its_payload() {
        run(async {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            let payload = b"hello uring".to_vec();
            let script = frame(MsgType::GetRange, &payload);
            monoio::spawn(serve_once(l, script, false));

            let mut c = TcpStream::connect(addr).await.unwrap();
            let (h, p) = read_frame(&mut c, T).await.unwrap();
            assert_eq!(h.msg_type, MsgType::GetRange);
            assert_eq!(h.request_id, 7);
            assert_eq!(p, payload);
        });
    }

    /// A zero-length payload must not allocate or block on a second read.
    #[test]
    fn reads_an_empty_payload_frame() {
        run(async {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            monoio::spawn(serve_once(l, frame(MsgType::Ping, &[]), false));

            let mut c = TcpStream::connect(addr).await.unwrap();
            let (h, p) = read_frame(&mut c, T).await.unwrap();
            assert_eq!(h.msg_type, MsgType::Ping);
            assert!(p.is_empty());
        });
    }

    /// Clean disconnect at a frame boundary is Eof, not an error.
    #[test]
    fn clean_eof_at_frame_boundary() {
        run(async {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            monoio::spawn(async move {
                let (s, _) = l.accept().await.unwrap();
                drop(s);
            });

            let mut c = TcpStream::connect(addr).await.unwrap();
            assert!(matches!(
                read_frame(&mut c, T).await,
                Err(ReadFrameError::Eof)
            ));
        });
    }

    /// The per-type cap is enforced BEFORE the payload is allocated — the whole
    /// point of issue #111. A control frame advertising a data-plane-sized
    /// payload must be rejected without committing that memory.
    #[test]
    fn rejects_oversized_control_payload_before_allocating() {
        run(async {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            // Header only: advertise a huge length and send no payload at all.
            // If the reader allocated first, it would block here instead of
            // returning immediately.
            let header = FrameHeader::new(MsgType::Control, 1, MAX_CONTROL_PAYLOAD_LEN + 1);
            monoio::spawn(serve_once(l, header.encode().to_vec(), true));

            let mut c = TcpStream::connect(addr).await.unwrap();
            match read_frame(&mut c, T).await {
                Err(ReadFrameError::PayloadTooLarge {
                    msg_type,
                    length,
                    cap,
                }) => {
                    assert_eq!(msg_type, MsgType::Control);
                    assert_eq!(length, MAX_CONTROL_PAYLOAD_LEN + 1);
                    assert_eq!(cap, MAX_CONTROL_PAYLOAD_LEN);
                }
                other => panic!("expected PayloadTooLarge, got {other:?}"),
            }
        });
    }

    /// A Ping carries no payload, so any advertised length is over its cap.
    #[test]
    fn rejects_ping_with_payload() {
        run(async {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            let header = FrameHeader::new(MsgType::Ping, 1, 1);
            monoio::spawn(serve_once(l, header.encode().to_vec(), true));

            let mut c = TcpStream::connect(addr).await.unwrap();
            match read_frame(&mut c, T).await {
                Err(ReadFrameError::PayloadTooLarge { cap, .. }) => {
                    assert_eq!(cap, MAX_PING_PAYLOAD_LEN);
                }
                other => panic!("expected PayloadTooLarge, got {other:?}"),
            }
        });
    }

    /// A peer that sends a header and then stalls must be dropped, not allowed
    /// to pin the payload buffer indefinitely.
    #[test]
    fn times_out_a_stalled_payload() {
        run(async {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            // Valid header promising 64 bytes, then silence.
            let header = FrameHeader::new(MsgType::GetRange, 1, 64);
            monoio::spawn(serve_once(l, header.encode().to_vec(), true));

            let mut c = TcpStream::connect(addr).await.unwrap();
            let started = std::time::Instant::now();
            assert!(matches!(
                read_frame(&mut c, Duration::from_millis(200)).await,
                Err(ReadFrameError::Timeout)
            ));
            assert!(started.elapsed() < Duration::from_secs(5));
        });
    }

    /// A truncated payload (peer disconnects mid-frame) is an I/O error, not a
    /// silently short frame that would desync the protocol.
    #[test]
    fn truncated_payload_is_an_error() {
        run(async {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            let header = FrameHeader::new(MsgType::GetRange, 1, 64);
            let mut script = header.encode().to_vec();
            script.extend_from_slice(&[0u8; 10]); // 10 of the promised 64
            monoio::spawn(serve_once(l, script, false));

            let mut c = TcpStream::connect(addr).await.unwrap();
            assert!(matches!(
                read_frame(&mut c, T).await,
                Err(ReadFrameError::Io(_))
            ));
        });
    }

    /// Two frames on one connection: the reader must leave the stream positioned
    /// exactly at the next frame boundary.
    #[test]
    fn reads_consecutive_frames() {
        run(async {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            let mut script = frame(MsgType::GetRange, b"first");
            script.extend_from_slice(&frame(MsgType::GetRange, b"second"));
            monoio::spawn(serve_once(l, script, false));

            let mut c = TcpStream::connect(addr).await.unwrap();
            let (_, p1) = read_frame(&mut c, T).await.unwrap();
            let (_, p2) = read_frame(&mut c, T).await.unwrap();
            assert_eq!(p1, b"first");
            assert_eq!(p2, b"second");
        });
    }

    /// The write helper returns the buffer so callers can reuse it.
    #[test]
    fn write_all_returns_the_buffer() {
        run(async {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            monoio::spawn(async move {
                let (mut s, _) = l.accept().await.unwrap();
                let (r, b) = s.read_exact(vec![0u8; 4]).await;
                r.unwrap();
                assert_eq!(b, b"ping");
            });

            let mut c = TcpStream::connect(addr).await.unwrap();
            let returned = write_all(&mut c, b"ping".to_vec()).await.unwrap();
            assert_eq!(returned, b"ping");
        });
    }
}
