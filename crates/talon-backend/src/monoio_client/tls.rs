//! Owned io_uring input, authenticated in-place TLS plaintext, no adapter queue.
use monoio::{
    buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut, RawBuf},
    io::{AsyncReadRent, AsyncReadRentExt, AsyncWriteRent, AsyncWriteRentExt},
    net::TcpStream,
    BufResult,
};
use rustls::{client::UnbufferedClientConnection, unbuffered::ConnectionState};
use std::{io, ops::Range, sync::Arc};

// TLS ciphertext records are bounded by rustls too. The larger input limit is
// for handshake messages fragmented across records (e.g. certificate chains).
const MAX_RECORD: usize = 18_432;
const MAX_INPUT: usize = 256 * 1024;

pub(super) struct TlsStream {
    socket: TcpStream,
    session: UnbufferedClientConnection,
    input: Vec<u8>,
    used: usize,
    plaintext: Range<usize>,
    discard: usize,
    outgoing: Vec<u8>,
    closed: bool,
}

enum Action {
    Read,
    Send,
    Ready,
    Plain,
    Closed,
    Continue,
}

fn invalid(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

impl TlsStream {
    pub(super) fn socket(&self) -> &TcpStream {
        &self.socket
    }

    pub(super) fn can_idle(&self) -> bool {
        self.plaintext.is_empty() && !self.closed
    }

    /// Process idle TLS traffic through rustls (tickets, KeyUpdate, alerts),
    /// rather than treating every readable socket as a dead connection.
    /// Cancellation is only safe if the caller drops this entire stream.
    pub(super) async fn read_idle(&mut self) -> io::Result<()> {
        if !self.can_idle() {
            return Err(invalid(
                "unexpected application data or closure on idle TLS",
            ));
        }
        self.discard_consumed();
        self.receive_record().await?;
        self.advance(false).await?;
        if !self.can_idle() {
            return Err(invalid(
                "unexpected application data or closure on idle TLS",
            ));
        }
        Ok(())
    }

    pub(super) async fn connect(
        socket: TcpStream,
        config: Arc<rustls::ClientConfig>,
        name: rustls::pki_types::ServerName<'static>,
    ) -> io::Result<Self> {
        let mut stream = Self {
            socket,
            session: UnbufferedClientConnection::new(config, name).map_err(invalid)?,
            input: vec![0; MAX_RECORD + 5],
            used: 0,
            plaintext: 0..0,
            discard: 0,
            outgoing: Vec::new(),
            closed: false,
        };
        stream.advance(false).await?;
        if stream.closed || stream.session.is_handshaking() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        Ok(stream)
    }

    fn step(&mut self) -> io::Result<Action> {
        let base = self.input.as_ptr() as usize;
        let status = self
            .session
            .process_tls_records_in_place(&mut self.input[..self.used]);
        self.discard = status.discard;
        let action = match status.state.map_err(invalid)? {
            ConnectionState::ReadTraffic(mut traffic) => {
                if let Some(record) = traffic.next_record() {
                    let record = record.map_err(invalid)?;
                    self.discard += record.discard;
                    let start = (record.payload.as_ptr() as usize)
                        .checked_sub(base)
                        .ok_or_else(|| invalid("TLS plaintext is not borrowed from input"))?;
                    let end = start
                        .checked_add(record.payload.len())
                        .filter(|end| *end <= self.used)
                        .ok_or_else(|| invalid("TLS plaintext lies outside input"))?;
                    // The patched API returns one borrowed record per status.
                    // Keep its input alive until the HTTP consumer consumes it.
                    self.plaintext = start..end;
                    Action::Plain
                } else {
                    Action::Continue
                }
            }
            ConnectionState::EncodeTlsData(mut encode) => {
                let mut encoded = vec![0; 4096];
                let written = match encode.encode(&mut encoded) {
                    Ok(written) => written,
                    Err(rustls::unbuffered::EncodeError::InsufficientSize(size)) => {
                        encoded.resize(size.required_size, 0);
                        encode.encode(&mut encoded).map_err(invalid)?
                    }
                    Err(error) => return Err(invalid(error)),
                };
                self.outgoing.extend_from_slice(&encoded[..written]);
                Action::Continue
            }
            ConnectionState::TransmitTlsData(transmit) => {
                // No more protocol processing happens before the owned output
                // write completes. Cancellation drops this entire connection.
                transmit.done();
                Action::Send
            }
            ConnectionState::BlockedHandshake => Action::Read,
            ConnectionState::WriteTraffic(_) => Action::Ready,
            ConnectionState::PeerClosed | ConnectionState::Closed => Action::Closed,
            _ => return Err(invalid("unsupported TLS state")),
        };
        Ok(action)
    }

    fn discard_consumed(&mut self) {
        debug_assert!(self.plaintext.is_empty());
        self.plaintext = 0..0;
        if self.discard != 0 {
            // Full application records are read individually and fully consumed:
            // they take the reset branch without compacting ciphertext/payload.
            // Only fragmented handshake messages can require this move.
            if self.discard < self.used {
                self.input.copy_within(self.discard..self.used, 0);
            }
            self.used -= self.discard;
            self.discard = 0;
        }
    }

    async fn receive_record(&mut self) -> io::Result<()> {
        let start = self.used;
        self.receive_to(start + 5).await?;
        let length = u16::from_be_bytes([self.input[start + 3], self.input[start + 4]]) as usize;
        if length > MAX_RECORD {
            return Err(invalid("oversized TLS record"));
        }
        self.receive_to(start + 5 + length).await
    }

    async fn receive_to(&mut self, end: usize) -> io::Result<()> {
        if end > MAX_INPUT {
            return Err(invalid("TLS input limit exceeded"));
        }
        if end > self.input.len() {
            self.input.resize(end, 0);
        }
        if self.used == end {
            return Ok(());
        }
        // Transfer the allocation into the operation. Even if this future and
        // the TLS session are dropped, Monoio retains it until kernel completion.
        let input = std::mem::take(&mut self.input).slice_mut(self.used..end);
        let (result, input) = self.socket.read_exact(input).await;
        self.input = input.into_inner();
        result?;
        self.used = end;
        Ok(())
    }

    async fn advance(&mut self, want_plaintext: bool) -> io::Result<()> {
        loop {
            if !self.plaintext.is_empty() || self.closed {
                return Ok(());
            }
            self.discard_consumed();
            let action = self.step()?;
            if !matches!(action, Action::Plain) {
                self.discard_consumed();
            }
            match action {
                Action::Send => {
                    let outgoing = std::mem::take(&mut self.outgoing);
                    let (result, _) = self.socket.write_all(outgoing).await;
                    result?;
                }
                Action::Ready if !want_plaintext => return Ok(()),
                Action::Ready | Action::Read => self.receive_record().await?,
                Action::Closed => self.closed = true,
                Action::Plain | Action::Continue => (),
            }
        }
    }

    pub(super) async fn line(&mut self, output: &mut Vec<u8>, limit: usize) -> io::Result<()> {
        loop {
            self.advance(true).await?;
            if self.closed {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            let plain = &self.input[self.plaintext.clone()];
            let newline = plain.iter().position(|byte| *byte == b'\n');
            let take = newline.map_or(plain.len(), |end| end + 1);
            if take > limit.saturating_sub(output.len()) {
                return Err(invalid("TLS HTTP framing limit"));
            }
            output.extend_from_slice(&plain[..take]);
            self.plaintext.start += take;
            if newline.is_some() {
                return Ok(());
            }
        }
    }

    async fn send_plaintext(&mut self, plain: &[u8], close: bool) -> io::Result<usize> {
        self.advance(false).await?;
        if self.closed || !self.plaintext.is_empty() {
            return Err(invalid("TLS connection cannot accept a request"));
        }
        let status = self
            .session
            .process_tls_records_in_place(&mut self.input[..self.used]);
        self.discard = status.discard;
        let ConnectionState::WriteTraffic(mut traffic) = status.state.map_err(invalid)? else {
            return Err(invalid("TLS connection is not ready to write"));
        };
        let consumed = plain.len().min(16_384);
        let mut output = vec![0; consumed + 1024];
        let written = if close {
            traffic.queue_close_notify(&mut output)
        } else {
            traffic.encrypt(&plain[..consumed], &mut output)
        }
        .map_err(invalid)?;
        self.discard_consumed();
        output.truncate(written);
        let (result, _) = self.socket.write_all(output).await;
        result?;
        Ok(consumed)
    }
}

impl AsyncReadRent for TlsStream {
    async fn read<T: IoBufMut>(&mut self, mut buffer: T) -> BufResult<usize, T> {
        if buffer.bytes_total() == 0 {
            return (Ok(0), buffer);
        }
        if let Err(error) = self.advance(true).await {
            return (Err(error), buffer);
        }
        let take = self.plaintext.len().min(buffer.bytes_total());
        // The one remaining body copy also assembles HTTP body bytes in the
        // final aligned allocation. No separate alignment/assembly buffer.
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.input[self.plaintext.clone()].as_ptr(),
                buffer.write_ptr(),
                take,
            );
            buffer.set_init(take);
        }
        self.plaintext.start += take;
        (Ok(take), buffer)
    }

    async fn readv<T: IoVecBufMut>(&mut self, mut buffer: T) -> BufResult<usize, T> {
        // The borrowed view is only used for a synchronous plaintext copy;
        // kernel I/O always owns a separate input Vec.
        let result = match unsafe { RawBuf::new_from_iovec_mut(&mut buffer) } {
            Some(first) => self.read(first).await.0,
            None => Ok(0),
        };
        if let Ok(read) = result {
            unsafe { buffer.set_init(read) };
        }
        (result, buffer)
    }
}

impl AsyncWriteRent for TlsStream {
    async fn write<T: IoBuf>(&mut self, buffer: T) -> BufResult<usize, T> {
        // Plaintext is borrowed only while encrypting. The socket owns ciphertext.
        let plain = unsafe { std::slice::from_raw_parts(buffer.read_ptr(), buffer.bytes_init()) };
        let result = self.send_plaintext(plain, false).await;
        (result, buffer)
    }
    async fn writev<T: IoVecBuf>(&mut self, buffer: T) -> BufResult<usize, T> {
        let result = match unsafe { RawBuf::new_from_iovec(&buffer) } {
            Some(first) => self.write(first).await.0,
            None => Ok(0),
        };
        (result, buffer)
    }
    async fn flush(&mut self) -> io::Result<()> {
        self.socket.flush().await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        self.send_plaintext(&[], true).await?;
        self.socket.shutdown().await
    }
}
