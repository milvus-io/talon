//! Data-plane client for fetching byte ranges from a worker.
//!
//! Where the [`CoordinatorClient`](crate::CoordinatorClient) answers *where* a
//! block lives, [`WorkerClient`] fetches the bytes. It speaks the data plane: a
//! single [`MsgType::GetRange`] frame whose body is a bincode
//! [`RangeRequest`] (object + `[offset, len)`), and a reply that is a
//! `GetRange` frame carrying the **raw range bytes** — or, if the
//! [`Flags::ERROR`] bit is set, a UTF-8 error string.
//!
//! The response body is raw (no bincode envelope) precisely so a production
//! worker can `sendfile` the range straight from a file into the socket; this
//! client only needs to read the framed bytes back. A fresh TCP connection is
//! opened per fetch for simplicity, mirroring
//! [`CoordinatorClient`](crate::CoordinatorClient).

use talon_core::ObjectId;
use talon_transport::frame::{FrameHeader, MsgType, HEADER_LEN};
use talon_transport::{encode_request, Flags, RangeRequest};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Errors from a worker range fetch.
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    /// Failed to connect or an I/O error mid-request.
    #[error("worker I/O: {0}")]
    Io(#[from] std::io::Error),
    /// The request frame could not be encoded.
    #[error("worker request encode: {0}")]
    Encode(#[from] talon_transport::DataError),
    /// The reply frame header was invalid.
    #[error("worker frame: {0}")]
    Frame(#[from] talon_transport::FrameError),
    /// The worker replied with a non-`GetRange` frame.
    #[error("expected a GetRange reply, got {0:?}")]
    NotGetRange(MsgType),
    /// The worker set the ERROR flag and returned this message.
    #[error("worker error: {0}")]
    Remote(String),
}

/// A thin data-plane client bound to one worker address.
#[derive(Debug, Clone)]
pub struct WorkerClient {
    addr: String,
}

impl WorkerClient {
    /// Create a client that dials `addr` (`host:port`) per fetch.
    pub fn new(addr: impl Into<String>) -> Self {
        Self { addr: addr.into() }
    }

    /// The worker address this client talks to.
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Fetch `[offset, offset+len)` of `object` from the worker.
    ///
    /// Returns the raw range bytes on success. A worker-side error (block not
    /// present, backend failure, etc.) surfaces as [`WorkerError::Remote`] with
    /// the worker's message, which the caller can use to trigger a placement
    /// refresh or replica fallback.
    pub async fn fetch_range(
        &self,
        object: &ObjectId,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, WorkerError> {
        let req = RangeRequest {
            object: object.clone(),
            offset,
            len,
        };
        let out = encode_request(0, &req)?;
        let mut stream = TcpStream::connect(&self.addr).await?;
        stream.write_all(&out).await?;
        stream.flush().await?;
        read_range_reply(&mut stream).await
    }
}

/// Read one framed data-plane reply: header, then exactly `length` bytes.
///
/// If the header carries [`Flags::ERROR`], the body is a UTF-8 message and is
/// returned as [`WorkerError::Remote`]; otherwise the body is the raw range.
async fn read_range_reply(stream: &mut TcpStream) -> Result<Vec<u8>, WorkerError> {
    let mut header_buf = [0u8; HEADER_LEN];
    stream.read_exact(&mut header_buf).await?;
    let header = FrameHeader::decode(&header_buf)?;
    if header.msg_type != MsgType::GetRange {
        return Err(WorkerError::NotGetRange(header.msg_type));
    }
    let mut body = vec![0u8; header.length as usize];
    stream.read_exact(&mut body).await?;
    if header.flags.contains(Flags::ERROR) {
        let msg = String::from_utf8_lossy(&body).into_owned();
        return Err(WorkerError::Remote(msg));
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::Backend;
    use talon_transport::{decode_request, encode_error, response_header_ok};
    use tokio::net::TcpListener;

    fn object() -> ObjectId {
        ObjectId::new(Backend::Azure, "container", "path/to/blob.bin")
    }

    /// Spawn a mock worker: reads one RangeRequest, then replies with the bytes
    /// produced by `respond(req)`.
    async fn mock_worker<F>(respond: F) -> String
    where
        F: Fn(RangeRequest) -> Vec<u8> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut hdr = [0u8; HEADER_LEN];
            sock.read_exact(&mut hdr).await.unwrap();
            let header = FrameHeader::decode(&hdr).unwrap();
            let mut body = vec![0u8; header.length as usize];
            sock.read_exact(&mut body).await.unwrap();
            let mut full = hdr.to_vec();
            full.extend_from_slice(&body);
            let (_h, req) = decode_request(&full).unwrap();
            let reply = respond(req);
            sock.write_all(&reply).await.unwrap();
            sock.flush().await.unwrap();
        });
        addr
    }

    #[tokio::test]
    async fn fetch_returns_raw_bytes() {
        // Worker returns deterministic bytes for the requested range.
        let addr = mock_worker(|req| {
            let payload: Vec<u8> = (0..req.len).map(|i| (i % 251) as u8).collect();
            let mut out = response_header_ok(0, payload.len() as u32).to_vec();
            out.extend_from_slice(&payload);
            out
        })
        .await;
        let client = WorkerClient::new(addr);
        let bytes = client.fetch_range(&object(), 0, 4096).await.unwrap();
        assert_eq!(bytes.len(), 4096);
        assert_eq!(bytes[0], 0);
        assert_eq!(bytes[250], 250);
        assert_eq!(bytes[251], 0);
    }

    #[tokio::test]
    async fn error_flag_becomes_remote_error() {
        let addr = mock_worker(|_req| encode_error(0, "block not present")).await;
        let client = WorkerClient::new(addr);
        let err = client.fetch_range(&object(), 0, 16).await.unwrap_err();
        match err {
            WorkerError::Remote(m) => assert_eq!(m, "block not present"),
            other => panic!("expected Remote, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn wrong_reply_type_rejected() {
        // Reply with a Control-type frame instead of GetRange.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut hdr = [0u8; HEADER_LEN];
            sock.read_exact(&mut hdr).await.unwrap();
            let header = FrameHeader::decode(&hdr).unwrap();
            let mut body = vec![0u8; header.length as usize];
            sock.read_exact(&mut body).await.unwrap();
            let reply = FrameHeader::new(MsgType::Control, 0, 0).encode();
            sock.write_all(&reply).await.unwrap();
            sock.flush().await.unwrap();
        });
        let client = WorkerClient::new(addr);
        let err = client.fetch_range(&object(), 0, 16).await.unwrap_err();
        assert!(matches!(err, WorkerError::NotGetRange(MsgType::Control)));
    }

    #[tokio::test]
    async fn connect_failure_is_io_error() {
        let client = WorkerClient::new("127.0.0.1:1");
        let err = client.fetch_range(&object(), 0, 16).await.unwrap_err();
        assert!(matches!(err, WorkerError::Io(_)));
    }
}
