//! Demand-driven, bounded-memory range streaming.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use talon_core::{BlockId, ObjectId, Version};
use talon_transport::DataErrorCode;

use crate::block_reader::{BlockReadError, DetailedBlockReadError};
use crate::coordinator_client::CoordinatorError;
use crate::worker_client::WorkerError;
use crate::{BlockReader, FileView};

/// Default maximum bytes held by one in-flight worker response.
pub const DEFAULT_TRANSFER_CHUNK_BYTES: u32 = 4 * 1024 * 1024;

/// A typed cache-read failure suitable for an HTTP routing decision.
#[derive(Debug, thiserror::Error)]
pub enum CacheReadError {
    /// The requested range or stream configuration is invalid.
    #[error("invalid cache request: {0}")]
    InvalidRequest(String),
    /// The authoritative object does not exist.
    #[error("object not found: {0}")]
    NotFound(String),
    /// The block is absent and the selected route disabled origin fill.
    #[error("cache miss: {0}")]
    CacheMiss(String),
    /// Cache infrastructure is unavailable.
    #[error("cache unavailable: {0}")]
    Unavailable(String),
    /// Cache infrastructure exceeded its deadline.
    #[error("cache timeout: {0}")]
    Timeout(String),
    /// The source version changed during the read.
    #[error("source version mismatch: {0}")]
    VersionMismatch(String),
    /// The authoritative origin failed while a worker was filling the cache.
    #[error("origin failure: {0}")]
    Origin(String),
    /// A wire/framing invariant was violated.
    #[error("cache protocol failure: {0}")]
    Protocol(String),
    /// An explicitly typed worker-internal failure.
    #[error("cache internal failure: {0}")]
    Internal(String),
    /// A legacy worker returned only an unclassified string.
    #[error("unclassified legacy worker failure: {0}")]
    Unknown(String),
}

impl CacheReadError {
    /// Whether direct-origin fallback may safely hide this cache-path failure.
    ///
    /// Policy must still opt into fallback. Correctness, origin, protocol, and
    /// legacy-unknown failures remain visible rather than being guessed from
    /// diagnostic text.
    pub fn fallback_eligible(&self) -> bool {
        matches!(self, Self::Unavailable(_) | Self::Timeout(_))
    }
}

impl From<WorkerError> for CacheReadError {
    fn from(error: WorkerError) -> Self {
        match error {
            WorkerError::Io(error) if error.kind() == std::io::ErrorKind::TimedOut => {
                Self::Timeout(error.to_string())
            }
            WorkerError::Io(error) => Self::Unavailable(error.to_string()),
            WorkerError::Remote(error) => match error.code {
                DataErrorCode::Unknown => Self::Unknown(error.message),
                DataErrorCode::InvalidRequest => Self::InvalidRequest(error.message),
                DataErrorCode::NotFound => Self::NotFound(error.message),
                DataErrorCode::CacheMiss => Self::CacheMiss(error.message),
                DataErrorCode::Unavailable => Self::Unavailable(error.message),
                DataErrorCode::Timeout => Self::Timeout(error.message),
                DataErrorCode::VersionMismatch => Self::VersionMismatch(error.message),
                DataErrorCode::Origin => Self::Origin(error.message),
                DataErrorCode::Internal => Self::Internal(error.message),
            },
            other => Self::Protocol(other.to_string()),
        }
    }
}

impl From<CoordinatorError> for CacheReadError {
    fn from(error: CoordinatorError) -> Self {
        match error {
            CoordinatorError::Io(error) if error.kind() == std::io::ErrorKind::TimedOut => {
                Self::Timeout(error.to_string())
            }
            CoordinatorError::Io(error) => Self::Unavailable(error.to_string()),
            other => Self::Protocol(other.to_string()),
        }
    }
}

impl From<DetailedBlockReadError> for CacheReadError {
    fn from(error: DetailedBlockReadError) -> Self {
        match error {
            DetailedBlockReadError::Worker(error) => error.into(),
            DetailedBlockReadError::Block(BlockReadError::Coordinator(error)) => error.into(),
            DetailedBlockReadError::Block(BlockReadError::Worker(error)) => error.into(),
            DetailedBlockReadError::Block(error) => Self::Unavailable(error.to_string()),
        }
    }
}

struct StreamState {
    reader: BlockReader,
    object: ObjectId,
    version: Version,
    block_size: u32,
    position: u64,
    end: u64,
    chunk_size: u32,
    now_ms: u64,
}

/// A cache range body that starts at most one bounded worker request per poll.
pub struct RangeChunkStream {
    inner: Pin<Box<dyn Stream<Item = Result<Bytes, CacheReadError>> + Send>>,
}

impl Stream for RangeChunkStream {
    type Item = Result<Bytes, CacheReadError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl BlockReader {
    /// Stream `[offset, offset + len)` in chunks bounded by both the Talon block
    /// boundary and `chunk_size`.
    ///
    /// No worker request starts until the consumer polls the stream. A slow
    /// consumer therefore applies backpressure naturally, and dropping the
    /// stream drops its in-flight worker socket rather than leaving detached
    /// cache work behind.
    pub fn stream_range(
        &self,
        file: &FileView<'_>,
        offset: u64,
        len: u64,
        chunk_size: u32,
        now_ms: u64,
    ) -> Result<RangeChunkStream, CacheReadError> {
        if file.block_size == 0 {
            return Err(CacheReadError::InvalidRequest(
                "block size must be greater than zero".into(),
            ));
        }
        if chunk_size == 0 {
            return Err(CacheReadError::InvalidRequest(
                "transfer chunk size must be greater than zero".into(),
            ));
        }
        let requested_end = offset.checked_add(len).ok_or_else(|| {
            CacheReadError::InvalidRequest("range offset+len overflows u64".into())
        })?;
        let state = StreamState {
            reader: self.clone(),
            object: file.object.clone(),
            version: file.version.clone(),
            block_size: file.block_size,
            position: offset.min(file.size),
            end: requested_end.min(file.size),
            chunk_size,
            now_ms,
        };
        let stream = futures::stream::try_unfold(state, |mut state| async move {
            if state.position >= state.end {
                return Ok(None);
            }
            let block_size = u64::from(state.block_size);
            let block_start = (state.position / block_size) * block_size;
            let offset_in_block = (state.position - block_start) as u32;
            let block_remaining = block_size - u64::from(offset_in_block);
            let remaining = state.end - state.position;
            let take = remaining
                .min(block_remaining)
                .min(u64::from(state.chunk_size)) as u32;
            let block = BlockId::new(
                state.object.clone(),
                block_start,
                state.block_size,
                state.version.clone(),
            );
            let bytes = state
                .reader
                .read_block_detailed(&block, offset_in_block, take, state.now_ms)
                .await
                .map_err(CacheReadError::from)?;
            if bytes.len() != take as usize {
                return Err(CacheReadError::Protocol(format!(
                    "worker returned {} bytes for a {take}-byte chunk",
                    bytes.len()
                )));
            }
            state.position += u64::from(take);
            Ok(Some((Bytes::from(bytes), state)))
        });
        Ok(RangeChunkStream {
            inner: stream.boxed(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use talon_core::{Backend, NodeId, NodeInfo, NodeRole};
    use talon_transport::frame::{FrameHeader, HEADER_LEN};
    use talon_transport::{
        decode_request, encode_typed_error, response_header_ok, ControlMessage, DataPlaneError,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn coordinator(worker_addr: String) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let worker_addr = worker_addr.clone();
                tokio::spawn(async move {
                    loop {
                        let mut header = [0; HEADER_LEN];
                        if socket.read_exact(&mut header).await.is_err() {
                            return;
                        }
                        let header = FrameHeader::decode(&header).unwrap();
                        let mut body = vec![0; header.length as usize];
                        socket.read_exact(&mut body).await.unwrap();
                        let mut frame = header.encode().to_vec();
                        frame.extend_from_slice(&body);
                        let reply = match talon_transport::decode(&frame).unwrap().1 {
                            ControlMessage::MembershipQuery {} => ControlMessage::MembershipList {
                                nodes: vec![NodeInfo {
                                    id: NodeId("w1".into()),
                                    address: worker_addr.clone(),
                                    role: NodeRole::Worker,
                                }],
                            },
                            ControlMessage::MembershipQueryV2 {} => {
                                ControlMessage::MembershipListV2 {
                                    nodes: vec![talon_transport::ZonedNodeInfo {
                                        info: NodeInfo {
                                            id: NodeId("w1".into()),
                                            address: worker_addr.clone(),
                                            role: NodeRole::Worker,
                                        },
                                        zone: None,
                                    }],
                                }
                            }
                            other => panic!("unexpected control request: {other:?}"),
                        };
                        socket
                            .write_all(&talon_transport::encode(0, &reply).unwrap())
                            .await
                            .unwrap();
                    }
                });
            }
        });
        addr
    }

    async fn byte_worker(requests: Arc<AtomicUsize>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let requests = Arc::clone(&requests);
                tokio::spawn(async move {
                    loop {
                        let mut header = [0; HEADER_LEN];
                        if socket.read_exact(&mut header).await.is_err() {
                            return;
                        }
                        let header = FrameHeader::decode(&header).unwrap();
                        let mut body = vec![0; header.length as usize];
                        socket.read_exact(&mut body).await.unwrap();
                        let mut frame = header.encode().to_vec();
                        frame.extend_from_slice(&body);
                        let request = decode_request(&frame).unwrap().1;
                        requests.fetch_add(1, Ordering::SeqCst);
                        let bytes: Vec<u8> = (request.offset..request.offset + request.len)
                            .map(|value| value as u8)
                            .collect();
                        socket
                            .write_all(&response_header_ok(0, bytes.len() as u32))
                            .await
                            .unwrap();
                        socket.write_all(&bytes).await.unwrap();
                    }
                });
            }
        });
        addr
    }

    fn file<'a>(object: &'a ObjectId, version: &'a Version) -> FileView<'a> {
        FileView {
            object,
            block_size: 8,
            version,
            size: 10,
        }
    }

    #[tokio::test]
    async fn polling_drives_one_bounded_chunk_at_a_time() {
        let requests = Arc::new(AtomicUsize::new(0));
        let worker = byte_worker(Arc::clone(&requests)).await;
        let coordinator = coordinator(worker).await;
        let reader = BlockReader::new(
            crate::CoordinatorClient::new(coordinator),
            Arc::new(crate::PlacementCache::new(60_000)),
            1,
        );
        let object = ObjectId::new(Backend::S3, "bucket", "object");
        let version = Version::new("v1");
        let mut stream = reader
            .stream_range(&file(&object, &version), 0, 10, 3, 0)
            .unwrap();

        assert_eq!(requests.load(Ordering::SeqCst), 0);
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            Bytes::from_static(&[0, 1, 2])
        );
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            Bytes::from_static(&[3, 4, 5])
        );
        assert_eq!(requests.load(Ordering::SeqCst), 2);

        let rest = stream.map(|chunk| chunk.unwrap()).collect::<Vec<_>>().await;
        assert_eq!(
            rest,
            vec![Bytes::from_static(&[6, 7]), Bytes::from_static(&[8, 9])]
        );
        assert_eq!(requests.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn only_infrastructure_failures_allow_fallback() {
        assert!(CacheReadError::Unavailable("down".into()).fallback_eligible());
        assert!(CacheReadError::Timeout("slow".into()).fallback_eligible());
        assert!(!CacheReadError::NotFound("missing".into()).fallback_eligible());
        assert!(!CacheReadError::Protocol("bad frame".into()).fallback_eligible());
        assert!(!CacheReadError::Unknown("legacy text".into()).fallback_eligible());
    }

    #[test]
    fn typed_worker_errors_map_without_reading_messages() {
        let error = WorkerError::Remote(DataPlaneError {
            code: DataErrorCode::VersionMismatch,
            message: "arbitrary diagnostic".into(),
        });
        assert!(matches!(
            CacheReadError::from(error),
            CacheReadError::VersionMismatch(_)
        ));

        let error = WorkerError::Remote(DataPlaneError {
            code: DataErrorCode::Unavailable,
            message: "does not contain the word unavailable".into(),
        });
        assert!(CacheReadError::from(error).fallback_eligible());

        let error = WorkerError::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "deadline elapsed",
        ));
        assert!(matches!(
            CacheReadError::from(error),
            CacheReadError::Timeout(_)
        ));
    }

    #[test]
    fn invalid_ranges_fail_before_creating_a_stream() {
        let reader = BlockReader::new(
            crate::CoordinatorClient::new("127.0.0.1:1"),
            Arc::new(crate::PlacementCache::new(1)),
            1,
        );
        let object = ObjectId::new(Backend::S3, "bucket", "object");
        let version = Version::new("v1");
        assert!(matches!(
            reader.stream_range(&file(&object, &version), 0, 1, 0, 0),
            Err(CacheReadError::InvalidRequest(_))
        ));
        assert!(matches!(
            reader.stream_range(&file(&object, &version), u64::MAX, 2, 1, 0),
            Err(CacheReadError::InvalidRequest(_))
        ));
    }

    #[tokio::test]
    async fn typed_remote_failure_reaches_the_stream() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let worker = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header = [0; HEADER_LEN];
            socket.read_exact(&mut header).await.unwrap();
            let header = FrameHeader::decode(&header).unwrap();
            let mut body = vec![0; header.length as usize];
            socket.read_exact(&mut body).await.unwrap();
            socket
                .write_all(&encode_typed_error(
                    header.request_id,
                    DataErrorCode::NotFound,
                    "gone",
                ))
                .await
                .unwrap();
        });
        let coordinator = coordinator(worker).await;
        let reader = BlockReader::new(
            crate::CoordinatorClient::new(coordinator),
            Arc::new(crate::PlacementCache::new(60_000)),
            1,
        );
        let object = ObjectId::new(Backend::S3, "bucket", "object");
        let version = Version::new("v1");
        let mut stream = reader
            .stream_range(&file(&object, &version), 0, 1, 1, 0)
            .unwrap();
        assert!(matches!(
            stream.next().await.unwrap(),
            Err(CacheReadError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn dropping_stream_cancels_the_in_flight_worker_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let worker = listener.local_addr().unwrap().to_string();
        let (request_seen_tx, request_seen_rx) = tokio::sync::oneshot::channel();
        let (eof_tx, eof_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header = [0; HEADER_LEN];
            socket.read_exact(&mut header).await.unwrap();
            let header = FrameHeader::decode(&header).unwrap();
            let mut body = vec![0; header.length as usize];
            socket.read_exact(&mut body).await.unwrap();
            request_seen_tx.send(()).unwrap();
            let mut byte = [0; 1];
            let closed = socket.read(&mut byte).await.unwrap() == 0;
            eof_tx.send(closed).unwrap();
        });
        let coordinator = coordinator(worker).await;
        let reader = BlockReader::new(
            crate::CoordinatorClient::new(coordinator),
            Arc::new(crate::PlacementCache::new(60_000)),
            1,
        );
        let object = ObjectId::new(Backend::S3, "bucket", "object");
        let version = Version::new("v1");
        let mut stream = reader
            .stream_range(&file(&object, &version), 0, 1, 1, 0)
            .unwrap();

        {
            let next = stream.next();
            tokio::pin!(next);
            tokio::select! {
                result = &mut next => panic!("stalled worker unexpectedly replied: {result:?}"),
                result = request_seen_rx => result.unwrap(),
            }
        }
        drop(stream);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), eof_rx)
                .await
                .unwrap()
                .unwrap()
        );
    }
}
