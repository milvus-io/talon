// SPDX-License-Identifier: Apache-2.0
//! Zero-copy payload transfer via `sendfile(2)`.
//!
//! A cached extent is served straight from its shard file's descriptor into the
//! client socket, so the bytes never enter userspace and the hot path makes no
//! per-request allocation. [`send_file_range`] wraps the raw syscall in a
//! chunked loop that serves an arbitrary `[offset, offset + len)` sub-range and
//! handles short writes and `EINTR`.
//!
//! `talon-worker` has an equivalent helper, but it is Linux-only and lives in a
//! crate this one deliberately does not depend on (ADR 0005 §1). This version
//! also implements the macOS variant — not because the worker is deployed
//! there, but because a cache that cannot be built and tested on a developer's
//! laptop gets tested less.
//!
//! The call is blocking and belongs on the blocking pool, never on a reactor
//! thread.

use std::io;
use std::os::fd::{AsRawFd, RawFd};

/// Default bound on a single `sendfile` transfer (1 MiB).
pub const DEFAULT_CHUNK: usize = 1 << 20;

/// Stream `len` bytes starting at `offset` from `file` into `sock`.
///
/// Returns the total sent, which equals `len` on success and is short only if
/// the file ended early. Handles partial transfers and `EINTR`; any other
/// error is returned. `chunk` bounds one syscall's transfer.
///
/// # Errors
/// Any `sendfile` failure other than `EINTR`.
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
        let sent = match sendfile_once(out_fd, in_fd, &mut off, want) {
            Ok(n) => n,
            Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
            Err(e) => return Err(e),
        };
        if sent == 0 {
            // EOF before the requested length: the file is shorter than the
            // caller believed. Report the short count and let them decide.
            break;
        }
        sent_total += sent;
        remaining -= sent;
    }
    Ok(sent_total)
}

/// One `sendfile` call. Advances `off` by the bytes transferred.
#[cfg(target_os = "linux")]
fn sendfile_once(out_fd: RawFd, in_fd: RawFd, off: &mut i64, want: usize) -> io::Result<u64> {
    // SAFETY: both descriptors are borrowed for the call and `off` points at an
    // i64 we own. The kernel advances `off` by the number of bytes sent.
    let n = unsafe { libc::sendfile(out_fd, in_fd, off as *mut i64, want) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(n as u64)
}

/// One `sendfile` call, BSD flavour.
///
/// The macOS signature differs in three ways that are easy to get wrong: the
/// descriptors are in the opposite order, the offset is passed by value rather
/// than advanced in place, and the length is an in/out parameter that reports
/// bytes sent even when the call returns an error. Partial progress on `EINTR`
/// therefore has to be picked up out of `len` rather than inferred.
#[cfg(target_os = "macos")]
fn sendfile_once(out_fd: RawFd, in_fd: RawFd, off: &mut i64, want: usize) -> io::Result<u64> {
    let mut len: libc::off_t = want as libc::off_t;
    // SAFETY: both descriptors are borrowed for the call, `len` is a valid
    // pointer to an off_t we own, and a null `sf_hdtr` means "no headers".
    let rc = unsafe {
        libc::sendfile(
            in_fd,
            out_fd,
            *off as libc::off_t,
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    let sent = len.max(0) as u64;
    *off += sent as i64;
    if rc < 0 && sent == 0 {
        return Err(io::Error::last_os_error());
    }
    // rc < 0 with sent > 0 is partial progress; the caller loops.
    Ok(sent)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("talon-async-worker's zero-copy path requires Linux or macOS sendfile(2)");

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::net::{TcpListener, TcpStream};

    fn temp_file_with(contents: &[u8]) -> std::fs::File {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        std::time::SystemTime::now().hash(&mut h);
        std::thread::current().id().hash(&mut h);
        let path = std::env::temp_dir().join(format!(
            "talon-asf-{}-{:x}.bin",
            std::process::id(),
            h.finish()
        ));
        std::fs::write(&path, contents).unwrap();
        let f = std::fs::File::open(&path).unwrap();
        std::fs::remove_file(&path).ok(); // unlink; the fd keeps it alive
        f
    }

    /// Serve `[offset, offset+len)` of `data` over loopback, return what arrived.
    fn roundtrip(data: &[u8], offset: u64, len: u64, chunk: usize) -> Vec<u8> {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let file = temp_file_with(data);

        let server = std::thread::spawn(move || {
            let (conn, _) = listener.accept().unwrap();
            send_file_range(&conn, &file, offset, len, chunk).unwrap()
        });

        let mut client = TcpStream::connect(addr).unwrap();
        let mut got = Vec::new();
        client.read_to_end(&mut got).unwrap();
        let sent = server.join().unwrap();
        assert_eq!(sent as usize, got.len(), "reported count disagrees");
        got
    }

    #[test]
    fn a_whole_file_arrives_byte_exact() {
        let data: Vec<u8> = (0..4096u32).map(|i| (i % 256) as u8).collect();
        assert_eq!(roundtrip(&data, 0, data.len() as u64, DEFAULT_CHUNK), data);
    }

    #[test]
    fn a_sub_range_arrives_byte_exact() {
        // A footer-shaped read from the middle: the case this worker is for.
        let data: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
        let (off, len) = (1234u64, 4321u64);
        assert_eq!(
            roundtrip(&data, off, len, DEFAULT_CHUNK),
            &data[off as usize..(off + len) as usize]
        );
    }

    #[test]
    fn a_small_chunk_forces_the_partial_transfer_loop() {
        let data: Vec<u8> = (0..5000u32).map(|i| (i * 7 % 256) as u8).collect();
        assert_eq!(roundtrip(&data, 0, data.len() as u64, 64), data);
    }

    #[test]
    fn a_length_past_the_end_stops_at_eof() {
        let data = b"short";
        assert_eq!(roundtrip(data, 0, 1000, DEFAULT_CHUNK), data);
    }

    #[test]
    fn a_range_starting_at_eof_sends_nothing() {
        let data = b"abcdefgh";
        assert!(roundtrip(data, 8, 16, DEFAULT_CHUNK).is_empty());
    }
}
