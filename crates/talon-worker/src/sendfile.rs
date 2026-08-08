//! Zero-copy GET / GET_RANGE data path via `sendfile(2)`.
//!
//! A cached block is served straight from its `.blk` file descriptor into the
//! client socket with `sendfile(2)` — the bytes never enter userspace, so
//! there is no per-request heap allocation on the hot path. This module wraps
//! the raw syscall in a **chunked loop** that:
//!
//! - serves an arbitrary `[offset, offset + len)` sub-range (GET_RANGE), and
//! - handles short writes (a partial `sendfile` return advances the offset and
//!   retries) and `EINTR`.
//!
//! `sendfile` is Linux-specific and blocking; per DESIGN.md it runs in the
//! worker's blocking helper pool, never on the io_uring control ring. The
//! caller writes the response frame header first, then calls
//! [`send_file_range`] to stream the payload.

use std::io;
use std::os::fd::{AsRawFd, RawFd};

/// Default chunk size for a single `sendfile` call (1 MiB).
pub const DEFAULT_CHUNK: usize = 1 << 20;

/// Stream `len` bytes starting at `offset` from `file` to `sock` via
/// `sendfile(2)`, looping until all bytes are sent.
///
/// Returns the total number of bytes sent (== `len` on success). Handles short
/// writes and `EINTR`; any other error is returned. `chunk` bounds a single
/// syscall's transfer (use [`DEFAULT_CHUNK`]).
pub fn send_file_range(
    sock: &impl AsRawFd,
    file: &impl AsRawFd,
    offset: u64,
    len: u64,
    chunk: usize,
) -> io::Result<u64> {
    let out_fd: RawFd = sock.as_raw_fd();
    let in_fd: RawFd = file.as_raw_fd();
    let chunk = chunk.max(1);

    let mut off = offset as i64;
    let mut remaining = len;
    let mut sent_total = 0u64;

    while remaining > 0 {
        let want = remaining.min(chunk as u64) as usize;
        // SAFETY: valid fds; `off` is a valid pointer to an i64 we own. On
        // return the kernel advances `off` by the number of bytes sent.
        let n = unsafe { libc::sendfile(out_fd, in_fd, &mut off as *mut i64, want) };
        if n < 0 {
            let err = io::Error::last_os_error();
            match err.raw_os_error() {
                Some(libc::EINTR) => continue,
                _ => return Err(err),
            }
        }
        if n == 0 {
            // EOF before we expected it: the file is shorter than requested.
            break;
        }
        let n = n as u64;
        sent_total += n;
        remaining -= n;
    }
    Ok(sent_total)
}

/// Send `header` and then `[offset, offset + len)` of `file` to `sock` in a
/// single blocking step.
///
/// This is [`send_file_range`] with the response frame header prepended. Doing
/// both here rather than writing the header from the ring saves a full
/// ring-to-blocking-pool round-trip per request — measured as the dominant
/// per-request cost on the serve path, since the payload copy itself is
/// already zero-copy.
///
/// The header goes out with `MSG_MORE` so the kernel holds it back and
/// coalesces it with the first `sendfile` chunk into one TCP segment instead
/// of emitting a tiny header-only packet.
///
/// Returns the number of *payload* bytes sent (header bytes are not counted),
/// so the caller can apply the same short-send check as [`send_file_range`].
pub fn send_header_and_file_range(
    sock: &impl AsRawFd,
    header: &[u8],
    file: &impl AsRawFd,
    offset: u64,
    len: u64,
    chunk: usize,
) -> io::Result<u64> {
    let out_fd: RawFd = sock.as_raw_fd();

    let mut written = 0usize;
    while written < header.len() {
        // SAFETY: valid fd; the pointer/length pair stays inside `header`.
        let n = unsafe {
            libc::send(
                out_fd,
                header[written..].as_ptr() as *const libc::c_void,
                header.len() - written,
                libc::MSG_MORE | libc::MSG_NOSIGNAL,
            )
        };
        if n < 0 {
            let err = io::Error::last_os_error();
            match err.raw_os_error() {
                Some(libc::EINTR) => continue,
                _ => return Err(err),
            }
        }
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "socket accepted no header bytes",
            ));
        }
        written += n as usize;
    }

    // `MSG_MORE` leaves the header corked; the sendfile below uncorks it by
    // filling the segment. A zero-length payload would strand it, so flush.
    if len == 0 {
        // SAFETY: valid fd; a zero-length send with no MSG_MORE flushes.
        unsafe { libc::send(out_fd, [].as_ptr(), 0, libc::MSG_NOSIGNAL) };
        return Ok(0);
    }

    send_file_range(sock, file, offset, len, chunk)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};

    fn temp_file_with(contents: &[u8]) -> std::fs::File {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        std::time::SystemTime::now().hash(&mut h);
        std::thread::current().id().hash(&mut h);
        let path = std::env::temp_dir().join(format!(
            "talon-sf-{}-{}.blk",
            std::process::id(),
            h.finish()
        ));
        std::fs::write(&path, contents).unwrap();
        let f = std::fs::File::open(&path).unwrap();
        std::fs::remove_file(&path).ok(); // unlink; fd keeps it alive
        f
    }

    /// Serve `[offset,len)` of `data` over loopback and return what the client read.
    fn roundtrip(data: &[u8], offset: u64, len: u64, chunk: usize) -> Vec<u8> {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let file = temp_file_with(data);

        let server = std::thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            let sent = send_file_range(&conn, &file, offset, len, chunk).unwrap();
            conn.flush().unwrap();
            sent
        });

        let mut client = TcpStream::connect(addr).unwrap();
        let mut got = Vec::new();
        client.read_to_end(&mut got).unwrap();
        let sent = server.join().unwrap();
        assert_eq!(sent as usize, got.len());
        got
    }

    #[test]
    fn whole_file_is_byte_exact() {
        let data: Vec<u8> = (0..4096u32).map(|i| (i % 256) as u8).collect();
        let got = roundtrip(&data, 0, data.len() as u64, DEFAULT_CHUNK);
        assert_eq!(got, data);
    }

    #[test]
    fn sub_range_is_byte_exact() {
        let data: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
        // A footer-style range in the middle.
        let (off, len) = (1234u64, 4321u64);
        let got = roundtrip(&data, off, len, DEFAULT_CHUNK);
        assert_eq!(got, &data[off as usize..(off + len) as usize]);
    }

    #[test]
    fn small_chunk_forces_multiple_sendfile_calls() {
        // A tiny chunk exercises the partial-write loop.
        let data: Vec<u8> = (0..5000u32).map(|i| (i * 7 % 256) as u8).collect();
        let got = roundtrip(&data, 0, data.len() as u64, 64);
        assert_eq!(got, data);
    }

    #[test]
    fn stops_cleanly_at_eof_when_len_exceeds_file() {
        let data = b"short";
        // Ask for more than the file holds; sendfile stops at EOF.
        let got = roundtrip(data, 0, 1000, DEFAULT_CHUNK);
        assert_eq!(got, data);
    }

    /// Same as [`roundtrip`] but through the header-coalescing entry point.
    fn roundtrip_with_header(
        header: &[u8],
        data: &[u8],
        offset: u64,
        len: u64,
        chunk: usize,
    ) -> Vec<u8> {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let file = temp_file_with(data);
        let header = header.to_vec();

        let server = std::thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            let sent =
                send_header_and_file_range(&conn, &header, &file, offset, len, chunk).unwrap();
            conn.flush().unwrap();
            sent
        });

        let mut client = TcpStream::connect(addr).unwrap();
        let mut got = Vec::new();
        client.read_to_end(&mut got).unwrap();
        let payload_sent = server.join().unwrap();
        // The return value counts payload only, never the header.
        assert_eq!(payload_sent as usize, got.len() - HDR.len());
        got
    }

    const HDR: &[u8] = b"\x01\x02\x03\x04HEADERBYTES!";

    #[test]
    fn header_precedes_payload_byte_exactly() {
        let data: Vec<u8> = (0..8192u32).map(|i| (i % 256) as u8).collect();
        let got = roundtrip_with_header(HDR, &data, 0, data.len() as u64, DEFAULT_CHUNK);
        assert_eq!(&got[..HDR.len()], HDR);
        assert_eq!(&got[HDR.len()..], &data[..]);
    }

    #[test]
    fn header_precedes_sub_range_and_survives_chunking() {
        let data: Vec<u8> = (0..10_000u32).map(|i| (i * 13 % 251) as u8).collect();
        let (off, len) = (777u64, 3333u64);
        // A tiny chunk forces many sendfile calls after the corked header.
        let got = roundtrip_with_header(HDR, &data, off, len, 64);
        assert_eq!(&got[..HDR.len()], HDR);
        assert_eq!(&got[HDR.len()..], &data[off as usize..(off + len) as usize]);
    }

    /// `MSG_MORE` corks the header; with no payload to uncork it the bytes
    /// would sit in the kernel forever. The zero-length flush must release it.
    #[test]
    fn header_is_flushed_when_payload_is_empty() {
        let got = roundtrip_with_header(HDR, b"anything", 0, 0, DEFAULT_CHUNK);
        assert_eq!(got, HDR);
    }
}
