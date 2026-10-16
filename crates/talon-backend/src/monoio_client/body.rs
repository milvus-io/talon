//! Decode framing separately so downloaded payload lands in its final allocation.
use bytes::{Bytes, BytesMut};
use monoio::{
    buf::IoBufMut,
    io::{AsyncReadRent, AsyncReadRentExt, AsyncWriteRent},
};
use monoio_codec::{Decoded, Decoder};
use monoio_http::h1::codec::decoder::ResponseHeadDecoder;
use std::{future::Future, io};
use talon_uring::AlignedBuf;

pub(super) trait OriginIo: AsyncReadRent + AsyncWriteRent {
    fn line(&mut self, output: &mut Vec<u8>, limit: usize) -> impl Future<Output = io::Result<()>>;
}
impl OriginIo for monoio_rustls::ClientTlsStream<monoio::net::TcpStream> {
    async fn line(&mut self, output: &mut Vec<u8>, limit: usize) -> io::Result<()> {
        self.read_until_delimiter(b'\n', output, limit).await
    }
}
impl OriginIo for monoio::net::TcpStream {
    async fn line(&mut self, output: &mut Vec<u8>, limit: usize) -> io::Result<()> {
        use std::os::fd::AsRawFd;
        loop {
            // Peek only to delimit headers/framing. Consume exactly that prefix;
            // never copy peeked payload into the download allocation. EAGAIN
            // waits on the native ring, just like the data plane's pipe drain.
            let mut peek = [0_u8; 1024];
            let n = unsafe {
                libc::recv(
                    self.as_raw_fd(),
                    peek.as_mut_ptr().cast(),
                    peek.len(),
                    libc::MSG_PEEK | libc::MSG_DONTWAIT,
                )
            };
            if n < 0 {
                let error = io::Error::last_os_error();
                match error.kind() {
                    io::ErrorKind::Interrupted => continue,
                    io::ErrorKind::WouldBlock => {
                        self.readable(false).await?;
                        continue;
                    }
                    _ => return Err(error),
                }
            }
            if n == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            let end = peek[..n as usize].iter().position(|byte| *byte == b'\n');
            let take = end.map_or(n as usize, |end| end + 1);
            if take > limit.saturating_sub(output.len()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "HTTP line limit",
                ));
            }
            let (result, consumed) = self.read_exact(vec![0; take]).await;
            result?;
            output.extend_from_slice(&consumed);
            if end.is_some() {
                return Ok(());
            }
        }
    }
}

pub(super) async fn head<IO: OriginIo>(io: &mut IO) -> Result<http::response::Parts, String> {
    let mut header = Vec::new();
    loop {
        io.line(&mut header, 32768)
            .await
            .map_err(|_| "origin HTTP header truncated or too large")?;
        if header.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let mut header = BytesMut::from(header.as_slice());
    match ResponseHeadDecoder
        .decode(&mut header)
        .map_err(|_| "invalid origin HTTP header")?
    {
        Decoded::Some(parts) => Ok(parts),
        _ => Err("incomplete origin HTTP header".into()),
    }
}

pub(super) async fn payload<IO: OriginIo>(
    io: &mut IO,
    headers: &http::HeaderMap,
    bound: Option<usize>,
    download: Option<&talon_uring::Download>,
) -> Result<Bytes, String> {
    let alignment = download
        .map(|d| d.alignment())
        .unwrap_or(talon_uring::DirectIoAlignment::new(1, 1).unwrap());
    let freeze = |buffer: AlignedBuf, len| match download {
        Some(download) => download.finish(buffer, len),
        None => buffer.freeze(len).into_bytes(),
    };
    if let Some(length) = headers.get(http::header::CONTENT_LENGTH) {
        let len: usize = length
            .to_str()
            .map_err(|_| "invalid Content-Length")?
            .parse()
            .map_err(|_| "invalid Content-Length")?;
        if bound.is_some_and(|bound| len > bound) {
            return Err("origin body exceeds requested range".into());
        }
        let buffer = AlignedBuf::new(len, alignment).map_err(|e| e.to_string())?;
        let (result, buffer) = io.read_exact(buffer).await;
        result.map_err(|_| "origin HTTP body truncated or invalid")?;
        return Ok(freeze(buffer, len));
    }
    let encodings: Vec<_> = headers
        .get_all(http::header::TRANSFER_ENCODING)
        .iter()
        .collect();
    if encodings.len() != 1
        || encodings[0]
            .to_str()
            .map_or(true, |v| !v.trim().eq_ignore_ascii_case("chunked"))
    {
        return Err("origin response requires Content-Length or chunked framing".into());
    }
    // Range requests give the capacity in advance, including chunked responses.
    // Chunk boundaries only change where we read into this single allocation.
    let mut buffer = bound
        .map(|len| AlignedBuf::new(len, alignment))
        .transpose()
        .map_err(|e| e.to_string())?;
    let mut unbounded = BytesMut::new();
    let mut written = 0usize;
    loop {
        let mut line = Vec::new();
        io.line(&mut line, 1024)
            .await
            .map_err(|_| "invalid origin chunk header")?;
        let line = line
            .strip_suffix(b"\r\n")
            .ok_or("invalid origin chunk terminator")?;
        let size = line.split(|b| *b == b';').next().unwrap_or_default();
        if size.is_empty() || !size.iter().all(u8::is_ascii_hexdigit) {
            return Err("invalid origin chunk size".into());
        }
        let size = usize::from_str_radix(
            std::str::from_utf8(size).map_err(|_| "invalid chunk size")?,
            16,
        )
        .map_err(|_| "chunk size overflow")?;
        if size == 0 {
            // Consume bounded trailers before reusing this connection.
            let mut trailers = Vec::new();
            loop {
                let start = trailers.len();
                io.line(&mut trailers, 32768)
                    .await
                    .map_err(|_| "invalid origin trailers")?;
                let line = &trailers[start..];
                if line == b"\r\n" {
                    break;
                }
                if !line.ends_with(b"\r\n") || !line.contains(&b':') {
                    return Err("invalid origin trailer".into());
                }
            }
            return Ok(match buffer {
                Some(buffer) => freeze(buffer, written),
                None => unbounded.freeze(),
            });
        }
        let end = written
            .checked_add(size)
            .ok_or("origin chunked body too large")?;
        if let Some(owned) = buffer.take() {
            if end > bound.expect("bounded buffer") {
                return Err("origin body exceeds requested range".into());
            }
            let (result, slice) = io.read_exact(owned.slice_mut(written..end)).await;
            result.map_err(|_| "origin HTTP body truncated or invalid")?;
            buffer = Some(slice.into_inner());
        } else {
            // Non-Range APIs (e.g. listings) retain their growable-body contract.
            let (result, chunk) = io.read_exact(vec![0; size]).await;
            result.map_err(|_| "origin HTTP body truncated or invalid")?;
            unbounded.extend_from_slice(&chunk);
        }
        let (result, end_line) = io.read_exact(vec![0; 2]).await;
        result.map_err(|_| "origin HTTP chunk truncated")?;
        if end_line != b"\r\n" {
            return Err("invalid origin chunk terminator".into());
        }
        written = end;
    }
}
