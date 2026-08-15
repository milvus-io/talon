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

use monoio::buf::IoBufMut;
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

/// Write a whole buffer to a monoio stream, returning the buffer.
///
/// Generic over any `IoBuf`, so callers holding a refcounted `bytes::Bytes`
/// can hand it straight to the ring instead of copying it into a fresh `Vec`.
pub async fn write_all_buf<S, B>(stream: &mut S, buf: B) -> io::Result<B>
where
    S: AsyncWriteRent,
    B: monoio::buf::IoBuf,
{
    let (res, buf) = stream.write_all(buf).await;
    res?;
    Ok(buf)
}

/// Capacity of one refill read. Sized so a burst of pipelined range requests
/// (a 16-byte header plus a small bincode body each) lands in a single `recv`.
const REFILL_CAPACITY: usize = 64 * 1024;

/// A per-connection reader that decodes frames out of a buffer, refilling it
/// with one `recv` per batch instead of one ring operation per read.
///
/// [`read_frame`] issues **two** submissions per frame — one for the 16-byte
/// header, one for the payload — and each costs a submission, a completion, and
/// a task wakeup. When a client pipelines, the socket already holds several
/// whole frames, so those wakeups buy nothing: the bytes are there to be taken.
///
/// This reader takes whatever has arrived (`read`, not `read_exact`) and serves
/// subsequent frames straight from memory, so a batch of *n* pipelined requests
/// costs one `recv` rather than 2*n* ring operations. Against a client that does
/// not pipeline it degrades to the same one-frame-per-refill shape, plus a copy.
///
/// Limits are preserved exactly as in [`read_frame`]: the per-type cap is
/// enforced **before** the payload is consumed or allocated, every refill is
/// bounded by the timeout, and a clean EOF at a frame boundary is
/// [`ReadFrameError::Eof`] while an EOF mid-frame is an error.
pub struct BufferedFrameReader {
    buf: Vec<u8>,
    /// Offset of the first unconsumed byte in `buf`.
    pos: usize,
    /// Reusable read buffer. monoio moves a buffer into the kernel and hands it
    /// back, so it is parked here between refills rather than reallocated.
    scratch: Option<Vec<u8>>,
}

impl Default for BufferedFrameReader {
    fn default() -> Self {
        Self::new()
    }
}

impl BufferedFrameReader {
    /// Create an empty reader. The buffer grows on first use.
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            pos: 0,
            scratch: None,
        }
    }

    /// Bytes decoded but not yet consumed.
    fn buffered(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Drop the consumed prefix so the buffer does not grow without bound.
    ///
    /// Only ever called immediately before a refill, never mid-frame, so no
    /// unconsumed bytes can be lost.
    fn compact(&mut self) {
        if self.pos == 0 {
            return;
        }
        self.buf.copy_within(self.pos.., 0);
        self.buf.truncate(self.buffered());
        self.pos = 0;
    }

    /// Read until at least `want` bytes are buffered.
    ///
    /// Uses `read`, not `read_exact`: taking whatever has already arrived is the
    /// entire point — it is what collapses a batch of pipelined frames into one
    /// `recv`. `at_frame_boundary` distinguishes a peer that closed cleanly
    /// between frames from one that vanished halfway through a frame.
    async fn fill_to<S>(
        &mut self,
        stream: &mut S,
        want: usize,
        timeout: Duration,
        at_frame_boundary: bool,
    ) -> Result<(), ReadFrameError>
    where
        S: AsyncReadRent,
    {
        while self.buffered() < want {
            self.compact();
            // Ask for at least the shortfall, but never less than a full refill,
            // so a large payload does not degenerate into many tiny reads. The
            // scratch buffer is owned by the reader and swapped in and out of
            // the kernel, so a refill costs no allocation on the steady path.
            let need = (want - self.buffered()).max(REFILL_CAPACITY);
            let mut scratch = self.scratch.take().unwrap_or_default();
            if scratch.len() < need {
                scratch.resize(need, 0);
            }
            let (res, scratch) = match monoio::time::timeout(timeout, stream.read(scratch)).await {
                Ok(pair) => pair,
                Err(_) => return Err(ReadFrameError::Timeout),
            };
            let n = match res {
                Ok(0) => {
                    self.scratch = Some(scratch);
                    return if at_frame_boundary && self.buffered() == 0 {
                        Err(ReadFrameError::Eof)
                    } else {
                        Err(ReadFrameError::Io(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "peer closed mid-frame",
                        )))
                    };
                }
                Ok(n) => n,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof && at_frame_boundary => {
                    self.scratch = Some(scratch);
                    return Err(ReadFrameError::Eof);
                }
                Err(e) => {
                    self.scratch = Some(scratch);
                    return Err(ReadFrameError::Io(e));
                }
            };
            self.buf.extend_from_slice(&scratch[..n]);
            self.scratch = Some(scratch);
        }
        Ok(())
    }

    /// Read exactly one frame, serving it from the buffer when possible.
    ///
    /// Semantics are identical to [`read_frame`]; only the number of syscalls
    /// differs.
    pub async fn next_frame<S>(
        &mut self,
        stream: &mut S,
        timeout: Duration,
    ) -> Result<(FrameHeader, Vec<u8>), ReadFrameError>
    where
        S: AsyncReadRent,
    {
        self.fill_to(stream, HEADER_LEN, timeout, true).await?;
        let header = FrameHeader::decode(&self.buf[self.pos..self.pos + HEADER_LEN])?;

        // Enforce the per-type cap BEFORE consuming the header or allocating the
        // payload, so a lying peer cannot pin memory (issue #111).
        let cap = max_payload_for(header.msg_type);
        if header.length > cap {
            return Err(ReadFrameError::PayloadTooLarge {
                msg_type: header.msg_type,
                length: header.length,
                cap,
            });
        }

        let len = header.length as usize;
        // Wait for the whole frame before consuming any of it, so a timeout or a
        // truncation leaves the reader re-entrant at this frame's start.
        self.fill_to(stream, HEADER_LEN + len, timeout, false)
            .await?;
        let start = self.pos + HEADER_LEN;
        let payload = self.buf[start..start + len].to_vec();
        self.pos = start + len;
        Ok((header, payload))
    }

    /// Read exactly `len` more bytes of unframed stream data.
    ///
    /// A `Put` frame's header advertises only the small bincode preamble; the
    /// body follows it raw and unframed. This reader may have already pulled
    /// part (or all) of that body into its buffer while batching, so a caller
    /// that went back to reading the socket directly would skip those bytes and
    /// silently store corrupt data. Every unframed read after a frame must go
    /// through here, which drains the buffer first and only then touches the
    /// socket.
    pub async fn read_exact_bytes<S>(
        &mut self,
        stream: &mut S,
        len: usize,
        timeout: Duration,
    ) -> Result<Vec<u8>, ReadFrameError>
    where
        S: AsyncReadRent,
    {
        if len == 0 {
            return Ok(Vec::new());
        }
        // Serve what is buffered without waiting, then read only the shortfall
        // straight into the output buffer.
        let from_buf = self.buffered().min(len);
        let mut out = Vec::with_capacity(len);
        out.extend_from_slice(&self.buf[self.pos..self.pos + from_buf]);
        self.pos += from_buf;
        if from_buf == len {
            return Ok(out);
        }

        let mut remaining = len - from_buf;
        while remaining > 0 {
            let mut scratch = self.scratch.take().unwrap_or_default();
            if scratch.len() < remaining {
                scratch.resize(remaining, 0);
            }
            // Read at most the shortfall: anything past the body belongs to the
            // next frame and must stay on the socket for `next_frame`.
            let slice = scratch.slice_mut(..remaining);
            let (res, slice) = match monoio::time::timeout(timeout, stream.read(slice)).await {
                Ok(pair) => pair,
                Err(_) => return Err(ReadFrameError::Timeout),
            };
            let scratch = slice.into_inner();
            match res {
                Ok(0) => {
                    self.scratch = Some(scratch);
                    return Err(ReadFrameError::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "peer closed mid-body",
                    )));
                }
                Ok(n) => {
                    out.extend_from_slice(&scratch[..n]);
                    remaining -= n;
                    self.scratch = Some(scratch);
                }
                Err(e) => {
                    self.scratch = Some(scratch);
                    return Err(ReadFrameError::Io(e));
                }
            }
        }
        Ok(out)
    }
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

    /// The batching case: many frames arriving together must all decode, in
    /// order, from the buffer.
    #[test]
    fn buffered_reader_decodes_a_pipelined_batch() {
        run(async {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            let mut script = Vec::new();
            for i in 0..32u32 {
                script.extend_from_slice(&frame(MsgType::GetRange, format!("p{i}").as_bytes()));
            }
            monoio::spawn(serve_once(l, script, true));

            let mut c = TcpStream::connect(addr).await.unwrap();
            let mut r = BufferedFrameReader::new();
            for i in 0..32u32 {
                let (h, p) = r.next_frame(&mut c, T).await.unwrap();
                assert_eq!(h.msg_type, MsgType::GetRange);
                assert_eq!(p, format!("p{i}").as_bytes());
            }
        });
    }

    /// A payload larger than one refill must be stitched across reads.
    #[test]
    fn buffered_reader_reads_a_payload_larger_than_the_refill() {
        run(async {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            let payload = vec![0xABu8; REFILL_CAPACITY * 3 + 7];
            let script = frame(MsgType::GetRange, &payload);
            monoio::spawn(serve_once(l, script, true));

            let mut c = TcpStream::connect(addr).await.unwrap();
            let mut r = BufferedFrameReader::new();
            let (h, p) = r.next_frame(&mut c, T).await.unwrap();
            assert_eq!(h.length as usize, payload.len());
            assert_eq!(p, payload);
        });
    }

    /// A frame split across two TCP segments must still decode, and the reader
    /// must not treat the first short read as EOF.
    #[test]
    fn buffered_reader_stitches_a_split_frame() {
        run(async {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            let script = frame(MsgType::GetRange, b"split payload");
            monoio::spawn(async move {
                let (mut s, _) = l.accept().await.unwrap();
                // Cut mid-header so both fill_to calls must loop.
                let (a, b) = script.split_at(9);
                let (r, _) = s.write_all(a.to_vec()).await;
                r.unwrap();
                monoio::time::sleep(Duration::from_millis(50)).await;
                let (r, _) = s.write_all(b.to_vec()).await;
                r.unwrap();
                monoio::time::sleep(Duration::from_secs(30)).await;
            });

            let mut c = TcpStream::connect(addr).await.unwrap();
            let mut r = BufferedFrameReader::new();
            let (_, p) = r.next_frame(&mut c, T).await.unwrap();
            assert_eq!(p, b"split payload");
        });
    }

    /// Clean disconnect at a frame boundary is Eof; the buffered reader must not
    /// report it as an I/O error.
    #[test]
    fn buffered_reader_reports_clean_eof() {
        run(async {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            monoio::spawn(serve_once(l, frame(MsgType::GetRange, b"only"), false));

            let mut c = TcpStream::connect(addr).await.unwrap();
            let mut r = BufferedFrameReader::new();
            let (_, p) = r.next_frame(&mut c, T).await.unwrap();
            assert_eq!(p, b"only");
            assert!(matches!(
                r.next_frame(&mut c, T).await,
                Err(ReadFrameError::Eof)
            ));
        });
    }

    /// A peer that vanishes mid-frame is an error, not a clean EOF — otherwise a
    /// truncated request would be silently dropped.
    #[test]
    fn buffered_reader_rejects_a_truncated_payload() {
        run(async {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            let header = FrameHeader::new(MsgType::GetRange, 1, 64);
            let mut script = header.encode().to_vec();
            script.extend_from_slice(&[0u8; 10]);
            monoio::spawn(serve_once(l, script, false));

            let mut c = TcpStream::connect(addr).await.unwrap();
            let mut r = BufferedFrameReader::new();
            assert!(matches!(
                r.next_frame(&mut c, T).await,
                Err(ReadFrameError::Io(_))
            ));
        });
    }

    /// The #111 cap must be enforced before the payload is consumed, exactly as
    /// in `read_frame` — the reader must return immediately rather than wait for
    /// bytes the peer never sends.
    #[test]
    fn buffered_reader_rejects_oversized_control_before_allocating() {
        run(async {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            let header = FrameHeader::new(MsgType::Control, 1, MAX_CONTROL_PAYLOAD_LEN + 1);
            monoio::spawn(serve_once(l, header.encode().to_vec(), true));

            let mut c = TcpStream::connect(addr).await.unwrap();
            let mut r = BufferedFrameReader::new();
            match r.next_frame(&mut c, T).await {
                Err(ReadFrameError::PayloadTooLarge { cap, length, .. }) => {
                    assert_eq!(cap, MAX_CONTROL_PAYLOAD_LEN);
                    assert_eq!(length, MAX_CONTROL_PAYLOAD_LEN + 1);
                }
                other => panic!("expected PayloadTooLarge, got {other:?}"),
            }
        });
    }

    /// A peer that sends a header then stalls must hit the timeout, not pin the
    /// connection forever.
    #[test]
    fn buffered_reader_times_out_a_stalled_payload() {
        run(async {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            let header = FrameHeader::new(MsgType::GetRange, 1, 64);
            monoio::spawn(serve_once(l, header.encode().to_vec(), true));

            let mut c = TcpStream::connect(addr).await.unwrap();
            let mut r = BufferedFrameReader::new();
            let started = std::time::Instant::now();
            assert!(matches!(
                r.next_frame(&mut c, Duration::from_millis(200)).await,
                Err(ReadFrameError::Timeout)
            ));
            assert!(started.elapsed() < Duration::from_secs(5));
        });
    }

    /// An empty-payload frame must not stall waiting for bytes that will never
    /// come, and must leave the next frame decodable.
    #[test]
    fn buffered_reader_handles_empty_payloads() {
        run(async {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            let mut script = frame(MsgType::Ping, &[]);
            script.extend_from_slice(&frame(MsgType::GetRange, b"after"));
            monoio::spawn(serve_once(l, script, true));

            let mut c = TcpStream::connect(addr).await.unwrap();
            let mut r = BufferedFrameReader::new();
            let (h1, p1) = r.next_frame(&mut c, T).await.unwrap();
            assert_eq!(h1.msg_type, MsgType::Ping);
            assert!(p1.is_empty());
            let (_, p2) = r.next_frame(&mut c, T).await.unwrap();
            assert_eq!(p2, b"after");
        });
    }

    /// Long-lived connections must not grow the buffer without bound: the
    /// consumed prefix is reclaimed rather than accumulated.
    #[test]
    fn buffered_reader_reclaims_consumed_bytes() {
        run(async {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            let one = frame(MsgType::GetRange, &vec![7u8; 4096]);
            let mut script = Vec::new();
            for _ in 0..200 {
                script.extend_from_slice(&one);
            }
            let total = script.len();
            monoio::spawn(serve_once(l, script, true));

            let mut c = TcpStream::connect(addr).await.unwrap();
            let mut r = BufferedFrameReader::new();
            for _ in 0..200 {
                r.next_frame(&mut c, T).await.unwrap();
            }
            assert!(
                r.buf.len() < total / 4,
                "buffer grew to {} of {total} streamed bytes",
                r.buf.len()
            );
        });
    }

    /// The PUT hazard: a frame followed by an unframed body, all arriving in one
    /// segment. The reader will have swallowed the body while batching, so
    /// reading it back must come out of the buffer. Going to the socket instead
    /// would skip these bytes and store corrupt data.
    #[test]
    fn buffered_reader_serves_an_unframed_body_from_the_buffer() {
        run(async {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            let body: Vec<u8> = (0..4096u32).map(|i| i as u8).collect();
            let mut script = frame(MsgType::Put, b"put-preamble");
            script.extend_from_slice(&body);
            monoio::spawn(serve_once(l, script, true));

            let mut c = TcpStream::connect(addr).await.unwrap();
            let mut r = BufferedFrameReader::new();
            let (h, p) = r.next_frame(&mut c, T).await.unwrap();
            assert_eq!(h.msg_type, MsgType::Put);
            assert_eq!(p, b"put-preamble");
            let got = r.read_exact_bytes(&mut c, body.len(), T).await.unwrap();
            assert_eq!(got, body);
        });
    }

    /// A body only partly buffered must stitch the buffered prefix with the
    /// bytes still on the socket, in that order.
    #[test]
    fn buffered_reader_stitches_a_partly_buffered_body() {
        run(async {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            let body: Vec<u8> = (0..200_000u32).map(|i| i as u8).collect();
            let expected = body.clone();
            monoio::spawn(async move {
                let (mut s, _) = l.accept().await.unwrap();
                let mut script = frame(MsgType::Put, b"pre");
                script.extend_from_slice(&body);
                let (r, _) = s.write_all(script).await;
                r.unwrap();
                monoio::time::sleep(Duration::from_secs(30)).await;
            });

            let mut c = TcpStream::connect(addr).await.unwrap();
            let mut r = BufferedFrameReader::new();
            let (_, p) = r.next_frame(&mut c, T).await.unwrap();
            assert_eq!(p, b"pre");
            let got = r.read_exact_bytes(&mut c, expected.len(), T).await.unwrap();
            assert_eq!(got.len(), expected.len());
            assert_eq!(got, expected);
        });
    }
}
