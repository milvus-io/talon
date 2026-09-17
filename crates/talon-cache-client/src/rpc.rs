//! Runtime-independent request/response semantics. Runtime implementations own
//! their sockets and buffers; no Tokio AsyncRead/AsyncWrite adapter is involved.
use crate::{CoordinatorError, WorkerError};
use bytes::Bytes;
use std::{future::Future, io, path::PathBuf, time::Duration};
use talon_core::Version;
use talon_transport::{
    ControlMessage, Flags, FrameHeader, MsgType, HEADER_LEN, MAX_CONTROL_PAYLOAD_LEN,
};

/// Executes a complete RPC on its owning runtime. Native implementations may
/// return thread-local futures; the default pool remains usable by Tokio tasks.
pub trait RequestExecutor {
    /// Send one owned request and return a fully validated owned response.
    fn execute_request(
        &self,
        addr: &str,
        request: Request,
    ) -> impl Future<Output = Result<Reply, Error>>;

    /// Execute a range using the caller's exclusive target. Local executors
    /// borrow the target; executors crossing threads retain an owned loan.
    fn execute_range_into(
        &self,
        addr: &str,
        frame: Vec<u8>,
        target: &mut crate::read_buffer::ReadTarget,
    ) -> impl Future<Output = Result<Reply, Error>> {
        async {
            let request = Request::range_into(frame, target.lend().await);
            let result = self.execute_request(addr, request).await;
            target.wait_idle().await;
            result
        }
    }
}

impl RequestExecutor for crate::ConnectionPool {
    async fn execute_range_into(
        &self,
        addr: &str,
        frame: Vec<u8>,
        target: &mut crate::read_buffer::ReadTarget,
    ) -> Result<Reply, Error> {
        self.rpc_range_into(addr, frame, target).await
    }
    async fn execute_request(&self, addr: &str, request: Request) -> Result<Reply, Error> {
        self.rpc(addr, request).await
    }
}

/// Complete wire request for the native Monoio client.
/// Constructors determine response limits and whether a stale socket may retry.
pub struct Request {
    pub(crate) frame: Bytes,
    pub(crate) body: Body,
    pub(crate) expected: Expected,
    pub(crate) timeout: Option<Duration>,
    pub(crate) target: Option<crate::read_buffer::ReadTarget>,
}
#[derive(Clone)]
pub(crate) enum Body {
    Empty,
    Bytes(Bytes),
    File { path: PathBuf, len: u64 },
}
#[derive(Clone, Copy)]
pub(crate) enum Expected {
    Range(u64),
    Version,
    Control,
}
impl Request {
    /// An encoded range request with the exact expected successful response size.
    pub fn range(frame: Vec<u8>, len: u64) -> Self {
        Self {
            frame: frame.into(),
            body: Body::Empty,
            expected: Expected::Range(len),
            timeout: None,
            target: None,
        }
    }
    /// Read directly into an exclusively owned destination region.
    pub fn range_into(frame: Vec<u8>, target: crate::read_buffer::ReadTarget) -> Self {
        let len = target.len() as u64;
        Self {
            target: Some(target),
            ..Self::range(frame, len)
        }
    }
    /// An encoded coordinator request. Control responses retain their size cap.
    pub fn control(frame: Vec<u8>) -> Self {
        Self {
            expected: Expected::Control,
            ..Self::range(frame, 0)
        }
    }
    /// An encoded PUT preamble and owned body; reply failure is never retried.
    pub fn put(frame: Vec<u8>, body: Bytes) -> Self {
        Self {
            body: Body::Bytes(body),
            expected: Expected::Version,
            ..Self::range(frame, 0)
        }
    }
    /// An encoded PUT preamble and staged file. Open once and retain that file
    /// through retry; read it on the selected runtime with bounded buffers.
    pub fn put_file(frame: Vec<u8>, path: PathBuf, len: u64) -> Self {
        Self {
            body: Body::File { path, len },
            expected: Expected::Version,
            timeout: Some(crate::worker_client::streamed_put_timeout(len)),
            ..Self::range(frame, 0)
        }
    }
    /// An encoded DELETE request. Reply failure must not replay a committed delete.
    pub fn delete(frame: Vec<u8>) -> Self {
        Self {
            expected: Expected::Version,
            ..Self::range(frame, 0)
        }
    }
    /// An encoded cache admission preamble and owned block. Admission retains
    /// the existing idempotent stale-socket retry policy and upload deadline.
    pub fn admit(frame: Vec<u8>, body: Bytes) -> Self {
        let timeout = Some(crate::worker_client::streamed_put_timeout(body.len() as u64));
        Self {
            body: Body::Bytes(body),
            timeout,
            ..Self::range(frame, 0)
        }
    }
}

/// Validated response returned by a complete native exchange.
#[derive(Debug)]
pub enum Reply {
    /// Exact-length range payload; its allocation transfers to the caller.
    Range(Vec<u8>),
    /// Payload received directly into the supplied destination.
    Written(usize),
    /// Backend-committed version from a PUT/DELETE response.
    Version(Version),
    /// Decoded coordinator reply.
    Control(ControlMessage),
}
/// Transport and protocol errors from a complete native exchange.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Socket, file, queue or deadline failure.
    #[error("RPC I/O: {0}")]
    Io(#[from] io::Error),
    /// Worker framing, payload limit or remote error.
    #[error(transparent)]
    Worker(#[from] WorkerError),
    /// Coordinator framing, payload limit or codec error.
    #[error(transparent)]
    Coordinator(#[from] CoordinatorError),
}
impl Error {
    pub(crate) fn transport(&self) -> bool {
        match self {
            Self::Io(_) => true,
            Self::Worker(e) => e.is_transport_failure(),
            Self::Coordinator(e) => e.is_transport_failure(),
        }
    }
    pub(crate) fn worker(self) -> WorkerError {
        match self {
            Self::Worker(e) => e,
            Self::Io(e) => e.into(),
            Self::Coordinator(e) => io::Error::other(e).into(),
        }
    }
    pub(crate) fn coordinator(self) -> CoordinatorError {
        match self {
            Self::Coordinator(e) => e,
            Self::Io(e) => e.into(),
            Self::Worker(e) => io::Error::other(e).into(),
        }
    }
}

pub(crate) trait Socket {
    async fn write(&mut self, bytes: Bytes) -> io::Result<()>;
    async fn write_vec(&mut self, bytes: Vec<u8>) -> io::Result<Vec<u8>>;
    async fn read_exact(&mut self, len: usize) -> io::Result<Vec<u8>>;
    async fn read_into(&mut self, target: &mut crate::read_buffer::ReadTarget) -> io::Result<()>;
    async fn response_into(
        &mut self,
        expected: Expected,
        target: &mut crate::read_buffer::ReadTarget,
    ) -> Result<Reply, Error> {
        let header = self.read_exact(HEADER_LEN).await?;
        let decoded = response_header(&header, expected)?;
        if decoded.flags.contains(Flags::ERROR) {
            let body = self.read_exact(decoded.length as usize).await?;
            return decode(header, decoded, body, expected);
        }
        self.read_into(target).await?;
        Ok(Reply::Written(target.len()))
    }
    async fn response(&mut self, expected: Expected) -> Result<Reply, Error> {
        read_response(self, expected).await
    }
}
pub(crate) trait Runtime {
    type File;
    async fn open(path: &std::path::Path) -> io::Result<(Self::File, u64)>;
    async fn read_file(
        file: &mut Self::File,
        buffer: Vec<u8>,
        offset: u64,
        len: usize,
    ) -> io::Result<Vec<u8>>;
    async fn timeout<F: Future>(duration: Duration, future: F) -> io::Result<F::Output>;
}

pub(crate) async fn prepare<R: Runtime>(req: &Request) -> Result<Option<R::File>, Error> {
    if let Body::File { path, len } = &req.body {
        let (file, size) = R::open(path).await?;
        if size < *len {
            return Err(crate::worker_client::short_file_error(size, *len).into());
        }
        Ok(Some(file))
    } else {
        Ok(None)
    }
}

pub(crate) async fn exchange<R: Runtime>(
    socket: &mut impl Socket,
    req: &mut Request,
    file: &mut Option<R::File>,
    retry_safe: &mut bool,
) -> Result<Reply, Error> {
    socket.write(req.frame.clone()).await?;
    match &req.body {
        Body::Empty => {}
        Body::Bytes(bytes) => socket.write(bytes.clone()).await?,
        Body::File { len, .. } => {
            let mut offset = 0;
            let mut buffer = Vec::with_capacity(64 * 1024);
            while offset < *len {
                // A local file failure must never trigger another PUT.
                *retry_safe = false;
                let want = (*len - offset).min(64 * 1024) as usize;
                buffer = R::read_file(file.as_mut().expect("prepared file"), buffer, offset, want)
                    .await?;
                if buffer.is_empty() {
                    return Err(crate::worker_client::short_file_error(offset, *len).into());
                }
                offset += buffer.len() as u64;
                *retry_safe = true;
                buffer = socket.write_vec(buffer).await?;
            }
        }
    }
    if matches!(req.expected, Expected::Version) {
        *retry_safe = false;
    }
    if let Some(target) = req.target.as_mut() {
        socket.response_into(req.expected, target).await
    } else {
        socket.response(req.expected).await
    }
}

pub(crate) async fn read_response(
    socket: &mut (impl Socket + ?Sized),
    expected: Expected,
) -> Result<Reply, Error> {
    let header = socket.read_exact(HEADER_LEN).await?;
    let decoded = response_header(&header, expected)?;
    let body = socket.read_exact(decoded.length as usize).await?;
    decode(header, decoded, body, expected)
}

pub(crate) fn response_header(header: &[u8], expected: Expected) -> Result<FrameHeader, Error> {
    let decoded = match expected {
        Expected::Control => {
            FrameHeader::decode(header).map_err(|e| CoordinatorError::Codec(e.into()))?
        }
        _ => FrameHeader::decode(header).map_err(WorkerError::from)?,
    };
    validate(&decoded, expected)?;
    Ok(decoded)
}

fn validate(h: &FrameHeader, expected: Expected) -> Result<(), Error> {
    match expected {
        Expected::Control => {
            if h.msg_type != MsgType::Control {
                return Err(
                    CoordinatorError::Codec(talon_transport::CodecError::NotControl(h.msg_type))
                        .into(),
                );
            }
            if h.length > MAX_CONTROL_PAYLOAD_LEN {
                return Err(CoordinatorError::PayloadTooLarge {
                    length: h.length,
                    cap: MAX_CONTROL_PAYLOAD_LEN,
                }
                .into());
            }
        }
        _ => {
            if h.msg_type != MsgType::GetRange {
                return Err(WorkerError::NotGetRange(h.msg_type).into());
            }
            if h.flags.contains(Flags::ERROR) || matches!(expected, Expected::Version) {
                if h.length > MAX_CONTROL_PAYLOAD_LEN {
                    return Err(WorkerError::PayloadTooLarge {
                        length: h.length,
                        cap: MAX_CONTROL_PAYLOAD_LEN,
                    }
                    .into());
                }
            } else if let Expected::Range(len) = expected {
                if u64::from(h.length) != len {
                    return Err(WorkerError::RangeLengthMismatch {
                        expected: len,
                        actual: h.length.into(),
                    }
                    .into());
                }
            }
        }
    }
    Ok(())
}
pub(crate) fn decode(
    mut header: Vec<u8>,
    h: FrameHeader,
    body: Vec<u8>,
    expected: Expected,
) -> Result<Reply, Error> {
    if matches!(expected, Expected::Control) {
        header.extend_from_slice(&body);
        let (_, message) = talon_transport::decode(&header).map_err(CoordinatorError::from)?;
        return Ok(Reply::Control(message));
    }
    if h.flags.contains(Flags::ERROR) {
        return Err(WorkerError::Remote(talon_transport::decode_error_payload(&body)).into());
    }
    Ok(match expected {
        Expected::Range(_) => Reply::Range(body),
        Expected::Version => {
            Reply::Version(Version::new(String::from_utf8_lossy(&body).into_owned()))
        }
        Expected::Control => unreachable!(),
    })
}

pub(crate) struct Tokio;
impl Socket for tokio::net::TcpStream {
    async fn response_into(
        &mut self,
        expected: Expected,
        target: &mut crate::read_buffer::ReadTarget,
    ) -> Result<Reply, Error> {
        let mut header = [0; HEADER_LEN];
        tokio::io::AsyncReadExt::read_exact(self, &mut header).await?;
        let decoded = response_header(&header, expected)?;
        if decoded.flags.contains(Flags::ERROR) {
            let body = Socket::read_exact(self, decoded.length as usize).await?;
            return decode(header.to_vec(), decoded, body, expected);
        }
        self.read_into(target).await?;
        Ok(Reply::Written(target.len()))
    }
    async fn write(&mut self, bytes: Bytes) -> io::Result<()> {
        tokio::io::AsyncWriteExt::write_all(self, &bytes).await
    }
    async fn write_vec(&mut self, bytes: Vec<u8>) -> io::Result<Vec<u8>> {
        tokio::io::AsyncWriteExt::write_all(self, &bytes).await?;
        Ok(bytes)
    }
    async fn read_into(&mut self, target: &mut crate::read_buffer::ReadTarget) -> io::Result<()> {
        let mut read = tokio::io::ReadBuf::uninit(target.uninit_bytes_mut().await);
        while read.remaining() != 0 {
            let before = read.filled().len();
            std::future::poll_fn(|cx| {
                tokio::io::AsyncRead::poll_read(std::pin::Pin::new(&mut *self), cx, &mut read)
            })
            .await?;
            if read.filled().len() == before {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "incomplete range body",
                ));
            }
        }
        Ok(())
    }

    async fn read_exact(&mut self, len: usize) -> io::Result<Vec<u8>> {
        let mut out = vec![0; len];
        tokio::io::AsyncReadExt::read_exact(self, &mut out).await?;
        Ok(out)
    }
}
impl Runtime for Tokio {
    type File = tokio::fs::File;
    async fn open(path: &std::path::Path) -> io::Result<(Self::File, u64)> {
        let file = tokio::fs::File::open(path).await?;
        let len = file.metadata().await?.len();
        Ok((file, len))
    }
    async fn read_file(
        file: &mut Self::File,
        mut buffer: Vec<u8>,
        offset: u64,
        len: usize,
    ) -> io::Result<Vec<u8>> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        buffer.resize(len, 0);
        let n = file.read(&mut buffer).await?;
        buffer.truncate(n);
        Ok(buffer)
    }
    async fn timeout<F: Future>(duration: Duration, future: F) -> io::Result<F::Output> {
        tokio::time::timeout(duration, future)
            .await
            .map_err(|_| crate::pool::timeout_error("RPC", duration))
    }
}
