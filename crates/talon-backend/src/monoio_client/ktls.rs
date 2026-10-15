//! Linux kTLS record I/O. Rustls still owns authentication and key derivation.
use monoio::{
    buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut},
    io::{AsyncReadRent, AsyncReadRentExt, AsyncWriteRent},
    net::TcpStream,
    BufResult,
};
use rustls::{client::ClientConnectionData, kernel::KernelConnection, ConnectionTrafficSecrets};
use std::{io, os::fd::AsRawFd};
use zeroize::Zeroizing;

const SOL_TLS: i32 = 282;
const TLS_TX: i32 = 1;
const TLS_RX: i32 = 2;
const MAX_CONTROL: usize = 65536;
// Close well before AES-GCM's 2^24.5 record confidentiality limit. The kernel's
// sequence counter includes empty/control records, unlike a body-byte counter.
const RECORD_BUDGET: u64 = 1 << 20;

fn invalid(message: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn set(socket: &impl AsRawFd, level: i32, option: i32, bytes: &[u8]) -> io::Result<()> {
    let rc = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            level,
            option,
            bytes.as_ptr().cast(),
            bytes.len() as _,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Linux UAPI byte layout, deliberately without Rust struct padding. Secrets
/// and getsockopt output are wiped; neither is ever formatted in diagnostics.
fn crypto(
    version: u16,
    (sequence, secret): (u64, ConnectionTrafficSecrets),
) -> io::Result<Zeroizing<Vec<u8>>> {
    let (cipher, key, iv) = match secret {
        ConnectionTrafficSecrets::Aes128Gcm { key, iv } => (51_u16, key, iv),
        ConnectionTrafficSecrets::Aes256Gcm { key, iv } => (52_u16, key, iv),
        ConnectionTrafficSecrets::Chacha20Poly1305 { key, iv } => (54_u16, key, iv),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "unsupported kTLS cipher",
            ))
        }
    };
    let mut bytes = Zeroizing::new(Vec::with_capacity(4 + 12 + key.as_ref().len() + 8));
    bytes.extend_from_slice(&version.to_ne_bytes());
    bytes.extend_from_slice(&cipher.to_ne_bytes());
    if cipher == 54 {
        bytes.extend_from_slice(iv.as_ref());
        bytes.extend_from_slice(key.as_ref());
    } else {
        bytes.extend_from_slice(&iv.as_ref()[4..]);
        bytes.extend_from_slice(key.as_ref());
        bytes.extend_from_slice(&iv.as_ref()[..4]);
    }
    bytes.extend_from_slice(&sequence.to_be_bytes());
    Ok(bytes)
}

/// Probe a private loopback pair with dummy keys at service startup. No live
/// TLS connection or application traffic is involved. Use the existing startup
/// Tokio runtime; probing never creates an origin thread or Monoio runtime.
pub(super) async fn probe(version: u16, cipher: u16, size: usize) -> (bool, bool) {
    let probe = async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let socket = tokio::net::TcpStream::connect(listener.local_addr()?).await?;
        let (_peer, _) = listener.accept().await?;
        let mut dummy = Zeroizing::new(vec![0; size]);
        dummy[..2].copy_from_slice(&version.to_ne_bytes());
        dummy[2..4].copy_from_slice(&cipher.to_ne_bytes());
        set(&socket, libc::IPPROTO_TCP, libc::TCP_ULP, b"tls\0")?;
        set(&socket, SOL_TLS, TLS_TX, &dummy)?;
        set(&socket, SOL_TLS, TLS_RX, &dummy)?;
        // Distinct new keys, sequence zero, no application data sent/received.
        if version != 0x304 {
            return Ok::<_, io::Error>((true, false));
        }
        dummy[if cipher == 54 { 16 } else { 12 }] = 1;
        let rekey = set(&socket, SOL_TLS, TLS_TX, &dummy)
            .and_then(|()| set(&socket, SOL_TLS, TLS_RX, &dummy))
            .is_ok();
        Ok((true, rekey))
    };
    tokio::time::timeout(std::time::Duration::from_secs(1), probe)
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or((false, false))
}

// ring used base XOR sequence for prior TLS 1.2 explicit nonces; Linux
// increments its IV instead. Reusing the extracted base would repeat the
// Finished nonce! Move TX to the opposite high-bit half, starting at its lowest
// value. With both handoff and lifetime < 2^20 records, the two sets are disjoint
// and neither can cross halves. The explicit nonce is carried on the wire;
// salt, traffic key, and authenticated record sequence remain unchanged.
fn tls12_tx_nonce(base: u64) -> u64 {
    (base ^ (1 << 63)) & (1 << 63)
}

pub(super) struct KtlsStream {
    socket: TcpStream,
    session: KernelConnection<ClientConnectionData>,
    version: u16,
    info_len: usize,
    control: Vec<u8>,
    control_kind: u8,
    closed: bool,
    rekey_supported: bool,
    rekey_failed: bool,
}

impl KtlsStream {
    pub(super) async fn connect(
        tls: super::tls::TlsStream,
        no_pad: bool,
        rekey_supported: bool,
    ) -> io::Result<Self> {
        let (socket, secrets, session) = tls.into_kernel()?;
        let version = match session.protocol_version() {
            rustls::ProtocolVersion::TLSv1_2 => 0x303,
            rustls::ProtocolVersion::TLSv1_3 => 0x304,
            _ => return Err(invalid("unsupported kTLS version")),
        };
        let mut tx = crypto(version, secrets.tx)?;
        if version == 0x303 && u16::from_ne_bytes(tx[2..4].try_into().unwrap()) != 54 {
            let sequence = u64::from_be_bytes(tx[tx.len() - 8..].try_into().unwrap());
            if sequence >= RECORD_BUDGET {
                return Err(invalid("TLS handoff record budget exhausted"));
            }
            let base = u64::from_be_bytes(tx[4..12].try_into().unwrap());
            tx[4..12].copy_from_slice(&tls12_tx_nonce(base).to_be_bytes());
        }
        let rx = crypto(version, secrets.rx)?;
        set(&socket, libc::IPPROTO_TCP, libc::TCP_ULP, b"tls\0")?;
        set(&socket, SOL_TLS, TLS_TX, &tx)?;
        set(&socket, SOL_TLS, TLS_RX, &rx)?;
        if no_pad && version == 0x304 {
            set(&socket, SOL_TLS, 4, &1_i32.to_ne_bytes())?; // TLS_RX_EXPECT_NO_PAD
        }
        let stream = Self {
            socket,
            session,
            version,
            info_len: rx.len(),
            control: Vec::new(),
            control_kind: 0,
            closed: false,
            rekey_supported,
            rekey_failed: false,
        };
        stream.check_budget(TLS_RX)?;
        stream.check_budget(TLS_TX)?;
        Ok(stream)
    }

    pub(super) fn needs_key_update_reconnect(&self) -> bool {
        self.rekey_failed
    }

    fn key_update_failed(&mut self) {
        self.closed = true;
        self.rekey_failed = true;
    }

    pub(super) fn socket(&self) -> &TcpStream {
        &self.socket
    }
    pub(super) fn tcp_quiet(&self) -> bool {
        // kTLS poll/MSG_PEEK can return EAGAIN for an incomplete encrypted
        // record. TCP's SIOCINQ also sees those pending ciphertext bytes.
        let mut queued: libc::c_int = 0;
        unsafe {
            libc::ioctl(self.socket.as_raw_fd(), libc::FIONREAD, &mut queued) == 0 && queued == 0
        }
    }
    pub(super) fn can_idle(&self) -> bool {
        !self.closed && self.control.is_empty()
    }

    fn check_budget(&self, direction: i32) -> io::Result<()> {
        let mut info = Zeroizing::new([0_u8; 56]);
        let mut len = self.info_len as libc::socklen_t;
        let rc = unsafe {
            libc::getsockopt(
                self.socket.as_raw_fd(),
                SOL_TLS,
                direction,
                info.as_mut_ptr().cast(),
                &mut len,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        if len as usize != self.info_len {
            return Err(invalid("invalid kTLS crypto info length"));
        }
        let sequence =
            u64::from_be_bytes(info[self.info_len - 8..self.info_len].try_into().unwrap());
        if sequence >= RECORD_BUDGET {
            return Err(invalid("kTLS record budget exhausted; reconnect"));
        }
        Ok(())
    }

    async fn send_control(&mut self, bytes: Vec<u8>, kind: u8) -> io::Result<()> {
        self.check_budget(TLS_TX)?;
        let expected = bytes.len();
        let (result, _) = self.socket.send_tls(bytes, kind).await;
        if result? != expected {
            return Err(io::ErrorKind::WriteZero.into());
        }
        Ok(())
    }

    async fn control(&mut self, kind: u8, bytes: &[u8], end_record: bool) -> io::Result<()> {
        if !matches!(kind, 21 | 22) || (!self.control.is_empty() && self.control_kind != kind) {
            return Err(invalid("unexpected/interleaved kTLS control record"));
        }
        self.control_kind = kind;
        if self.control.len() + bytes.len() > MAX_CONTROL {
            return Err(invalid("oversized TLS control message"));
        }
        self.control.extend_from_slice(bytes);
        if kind == 21 {
            // Alerts must not be fragmented across TLS records.
            if !end_record {
                return Ok(());
            }
            if self.control.as_slice() != [1, 0] {
                return Err(invalid("TLS peer alert"));
            }
            self.closed = true;
            self.control.clear();
            return Ok(());
        }
        if self.version != 0x304 {
            return Err(invalid("TLS 1.2 renegotiation is unsupported"));
        }
        loop {
            if self.control.len() < 4 {
                return Ok(());
            }
            let size =
                u32::from_be_bytes([0, self.control[1], self.control[2], self.control[3]]) as usize;
            let end = 4 + size;
            if end > MAX_CONTROL {
                return Err(invalid("oversized TLS handshake message"));
            }
            if self.control.len() < end {
                return Ok(());
            }
            match self.control[0] {
                4 => self
                    .session
                    .handle_new_session_ticket(&self.control[4..end])
                    .map_err(invalid)?,
                24 => {
                    if size != 1 || self.control[4] > 1 || !end_record || self.control.len() != end
                    {
                        return Err(invalid("invalid TLS KeyUpdate boundary"));
                    }
                    if !self.rekey_supported {
                        self.key_update_failed();
                        return Err(invalid("kTLS KeyUpdate requires a fresh connection"));
                    }
                    let update = async {
                        let request = self.control[4];
                        let rx = crypto(
                            self.version,
                            self.session.update_rx_secret().map_err(invalid)?,
                        )?;
                        set(&self.socket, SOL_TLS, TLS_RX, &rx)?;
                        if request == 1 {
                            self.send_control(vec![24, 0, 0, 1, 0], 22).await?;
                            let tx = crypto(
                                self.version,
                                self.session.update_tx_secret().map_err(invalid)?,
                            )?;
                            set(&self.socket, SOL_TLS, TLS_TX, &tx)?;
                        }
                        Ok::<(), io::Error>(())
                    }
                    .await;
                    if let Err(error) = update {
                        self.key_update_failed();
                        return Err(error);
                    }
                }
                _ => return Err(invalid("unexpected TLS post-handshake message")),
            }
            self.control.drain(..end);
        }
    }

    pub(super) async fn read_idle(&mut self) -> io::Result<()> {
        if self.closed {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        // A full record-sized control allocation avoids repeated tiny reads.
        for _ in 0..64 {
            self.check_budget(TLS_RX)?;
            let (result, bytes) = self.socket.recv_tls(vec![0; 16384], 0).await;
            let (n, kind, flags) = result?;
            if n == 0 || kind == 23 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            self.control(kind, &bytes[..n], flags & libc::MSG_EOR != 0)
                .await?;
            if self.closed {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            if self.control.is_empty() {
                return Ok(());
            }
        }
        Err(invalid("too many fragmented TLS control records"))
    }

    pub(super) async fn line(&mut self, output: &mut Vec<u8>, limit: usize) -> io::Result<()> {
        for _ in 0..1024 {
            self.check_budget(TLS_RX)?;
            let (result, peek) = self.socket.recv_tls(vec![0; 1024], libc::MSG_PEEK).await;
            let (n, kind, _) = result?;
            if n == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            if kind != 23 {
                self.read_idle().await?;
                continue;
            }
            if !self.control.is_empty() {
                return Err(invalid("application data interrupts TLS handshake"));
            }
            let end = peek[..n].iter().position(|b| *b == b'\n');
            let take = end.map_or(n, |end| end + 1);
            if take > limit.saturating_sub(output.len()) {
                return Err(invalid("HTTP line limit"));
            }
            let start = output.len();
            output.resize(start + take, 0);
            let (result, bytes) = self
                .read_exact(std::mem::take(output).slice_mut(start..start + take))
                .await;
            *output = bytes.into_inner();
            result?;
            if end.is_some() {
                return Ok(());
            }
        }
        Err(invalid("too many TLS framing/control records"))
    }
}

impl AsyncReadRent for KtlsStream {
    async fn read<T: IoBufMut>(&mut self, mut buffer: T) -> BufResult<usize, T> {
        if buffer.bytes_total() == 0 || self.closed {
            return (Ok(0), buffer);
        }
        // Control records can arrive during a body. Temporarily receiving them
        // into the caller allocation is safe: only app bytes advance its cursor.
        for _ in 0..128 {
            if let Err(e) = self.check_budget(TLS_RX) {
                return (Err(e), buffer);
            }
            let (result, returned) = self.socket.recv_tls(buffer, 0).await;
            buffer = returned;
            let (n, kind, flags) = match result {
                Ok(v) => v,
                Err(e) => return (Err(e), buffer),
            };
            if n == 0 {
                return (Err(io::ErrorKind::UnexpectedEof.into()), buffer);
            }
            if kind == 23 {
                if !self.control.is_empty() {
                    return (
                        Err(invalid("application data interrupts TLS handshake")),
                        buffer,
                    );
                }
                return (Ok(n), buffer);
            }
            let bytes = unsafe { std::slice::from_raw_parts(buffer.write_ptr(), n) };
            if let Err(e) = self.control(kind, bytes, flags & libc::MSG_EOR != 0).await {
                return (Err(e), buffer);
            }
            if self.closed {
                return (Ok(0), buffer);
            }
            // A small HTTP framing destination must not force a large ticket
            // through hundreds of two-byte reads. Finish control traffic in a
            // separate bounded allocation; application payload stays in buffer.
            if !self.control.is_empty() {
                if let Err(e) = self.read_idle().await {
                    return (Err(e), buffer);
                }
            }
        }
        (
            Err(invalid("too many consecutive TLS control fragments")),
            buffer,
        )
    }
    async fn readv<T: IoVecBufMut>(&mut self, buffer: T) -> BufResult<usize, T> {
        // Not used by the HTTP decoder. Reject instead of lending a borrowed
        // RawBuf to a kernel operation that could outlive its owner's future.
        (Err(io::ErrorKind::Unsupported.into()), buffer)
    }
}

impl AsyncWriteRent for KtlsStream {
    async fn write<T: IoBuf>(&mut self, buffer: T) -> BufResult<usize, T> {
        if let Err(e) = self.check_budget(TLS_TX) {
            return (Err(e), buffer);
        }
        if self.closed {
            return (Err(io::ErrorKind::BrokenPipe.into()), buffer);
        }
        // Bound each submission and retain its owner through completion.
        let end = buffer.bytes_init().min(16384);
        let (result, buffer) = self.socket.write(buffer.slice(..end)).await;
        (result, buffer.into_inner())
    }
    async fn writev<T: IoVecBuf>(&mut self, buffer: T) -> BufResult<usize, T> {
        // The encoder can use writev: this projection owns the entire iovec
        // owner, unlike RawBuf. A partial first-vector write is permitted.
        let first = First::new(buffer);
        let (result, first) = self.write(first).await;
        (result, first.owner)
    }
    async fn flush(&mut self) -> io::Result<()> {
        self.socket.flush().await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        self.send_control(vec![1, 0], 21).await?;
        self.closed = true;
        self.socket.shutdown().await
    }
}

struct First<T> {
    owner: T,
    ptr: *const u8,
    len: usize,
}
impl<T: IoVecBuf> First<T> {
    fn new(owner: T) -> Self {
        let mut ptr = std::ptr::NonNull::<u8>::dangling().as_ptr() as *const u8;
        let mut len = 0;
        for index in 0..owner.read_iovec_len() {
            let v = unsafe { &*owner.read_iovec_ptr().add(index) };
            if v.iov_len > 0 {
                ptr = v.iov_base.cast();
                len = v.iov_len;
                break;
            }
        }
        Self { owner, ptr, len }
    }
}
// SAFETY: IoVecBuf guarantees stable initialized payload pointers; ownership of
// every vector is retained until Monoio has completed or cancelled the SQE.
unsafe impl<T: IoVecBuf> IoBuf for First<T> {
    fn read_ptr(&self) -> *const u8 {
        self.ptr
    }
    fn bytes_init(&self) -> usize {
        self.len
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tls12_handoff_never_reuses_a_handshake_nonce() {
        for base in [0, 1, 2, 0xabcdef0123456789, u64::MAX, 1 << 63] {
            let first = tls12_tx_nonce(base);
            for seq in [0, 1, 2, 127, RECORD_BUDGET - 1] {
                let before = base ^ seq;
                assert_ne!(before >> 63, first >> 63);
                assert_ne!(before >> 63, (first + RECORD_BUDGET - 1) >> 63);
            }
        }
    }
}
