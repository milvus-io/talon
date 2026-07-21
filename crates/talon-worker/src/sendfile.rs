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
}
