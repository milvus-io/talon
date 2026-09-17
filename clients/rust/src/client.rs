use std::sync::Arc;

use crate::{Error, ObjectEntry, ObjectId, ObjectStat, UriError, Version};
use futures::stream::{FuturesUnordered, StreamExt};
use talon_cache_client::pool::{DEFAULT_IDLE_TTL, DEFAULT_MAX_IDLE_PER_ADDR};
use talon_cache_client::{
    iter_read, BlockReader, BlockSegment, ConnectionPool, CoordinatorClient, PlacementCache,
};

const PLACEMENT_TTL_MS: u64 = 30_000;
const REPLICAS_K: u8 = 1;

// Limit task allocation and let other logical reads make progress.
const MAX_CONCURRENT_BLOCK_READS_PER_READ: usize = 8;

/// A reusable, runtime-owning-neutral Talon client.
#[derive(Clone)]
pub struct Client {
    coordinator: CoordinatorClient,
    reader: BlockReader,
    block_size: u32,
}

/// Collects client configuration without allocating connection pools or caches.
///
/// Defaults to 256 MiB blocks and 8 idle connections per peer address.
/// A coordinator address must be supplied before [`build`](Self::build).
pub struct ClientBuilder {
    coordinator: String,
    block_size: u32,
    max_idle_per_addr: usize,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            coordinator: String::new(),
            block_size: 256 << 20,
            max_idle_per_addr: DEFAULT_MAX_IDLE_PER_ADDR,
        }
    }
}

impl ClientBuilder {
    /// Set the coordinator address (`host:port`).
    pub fn with_coordinator(mut self, coordinator: impl Into<String>) -> Self {
        self.coordinator = coordinator.into();
        self
    }

    /// Set the block size in bytes; it must match the workers' block size.
    pub fn with_block_size(mut self, block_size: u32) -> Self {
        self.block_size = block_size;
        self
    }

    /// Set the idle connection limit per peer in both coordinator and worker pools.
    /// This does not cap in-flight connections or change the idle TTL and timeouts.
    pub fn with_max_idle_per_addr(mut self, max_idle_per_addr: usize) -> Self {
        self.max_idle_per_addr = max_idle_per_addr;
        self
    }

    /// Validate configuration and construct a client without selecting a Tokio runtime.
    pub fn build(self) -> Result<Client, Error> {
        let Self {
            coordinator,
            block_size,
            max_idle_per_addr,
        } = self;
        if coordinator.is_empty() {
            return Err(Error::InvalidArgument(
                "coordinator must be non-empty".into(),
            ));
        }
        if block_size == 0 {
            return Err(Error::InvalidArgument("block_size must be non-zero".into()));
        }
        if max_idle_per_addr == 0 {
            return Err(Error::InvalidArgument(
                "max_idle_per_addr must be non-zero".into(),
            ));
        }
        // Keep control and data connections in separate pools.
        let coordinator = CoordinatorClient::with_pool(
            coordinator,
            Arc::new(ConnectionPool::with_limits(
                max_idle_per_addr,
                DEFAULT_IDLE_TTL,
            )),
        );
        let cache = Arc::new(PlacementCache::new(PLACEMENT_TTL_MS));
        let reader =
            BlockReader::new(coordinator.clone(), cache, REPLICAS_K).with_worker_pool(Arc::new(
                ConnectionPool::with_limits(max_idle_per_addr, DEFAULT_IDLE_TTL),
            ));
        Ok(Client {
            coordinator,
            reader,
            block_size,
        })
    }
}

impl Client {
    /// Address of the coordinator used by this client.
    pub fn coordinator_addr(&self) -> &str {
        self.coordinator.addr()
    }

    /// Logical block size used for range planning.
    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    /// Return an object's current size and source version.
    pub async fn stat(&self, object: &ObjectId) -> Result<ObjectStat, Error> {
        self.stat_with_options(object, &crate::RequestOptions::default())
            .await
    }

    /// Stat with an explicit request-local parent policy.
    pub async fn stat_with_options(
        &self,
        object: &ObjectId,
        options: &crate::RequestOptions<'_>,
    ) -> Result<ObjectStat, Error> {
        let op = talon_telemetry::Operation::new("talon.stat", "internal", options.parent);
        let result = op.scope(self.stat_inner(object)).await;
        op.outcome(if result.is_ok() { "success" } else { "error" });
        result
    }

    async fn stat_inner(&self, object: &ObjectId) -> Result<ObjectStat, Error> {
        Ok(self.coordinator.stat_object(object).await?)
    }

    /// List objects below a mount-relative prefix.
    pub async fn list(&self, prefix: &str) -> Result<Vec<ObjectEntry>, Error> {
        Ok(self.coordinator.list_objects(prefix).await?)
    }

    /// Read an object range into a newly allocated buffer.
    ///
    /// `known_stat` pins all returned bytes to its exact source version. Workers
    /// serve matching cached bytes or conditionally fill them from the origin;
    /// a version mismatch fails the read instead of substituting newer bytes.
    /// Large ranges are planned lazily, with at most eight block reads in flight
    /// per logical read. Assembly retains byte order without intermediate block buffers.
    pub async fn read(
        &self,
        object: &ObjectId,
        offset: u64,
        length: Option<u64>,
        known_stat: Option<&ObjectStat>,
    ) -> Result<Vec<u8>, Error> {
        self.read_with_options(
            object,
            offset,
            length,
            known_stat,
            &crate::RequestOptions::default(),
        )
        .await
    }

    /// Read with explicit parent selection; stat and block RPCs share this scope.
    pub async fn read_with_options(
        &self,
        object: &ObjectId,
        offset: u64,
        length: Option<u64>,
        known_stat: Option<&ObjectStat>,
        options: &crate::RequestOptions<'_>,
    ) -> Result<Vec<u8>, Error> {
        let op = talon_telemetry::Operation::new("talon.read", "internal", options.parent);
        let result = op
            .scope(self.read_inner(object, offset, length, known_stat))
            .await;
        op.outcome(if result.is_ok() { "success" } else { "error" });
        result
    }

    async fn read_inner(
        &self,
        object: &ObjectId,
        offset: u64,
        length: Option<u64>,
        known_stat: Option<&ObjectStat>,
    ) -> Result<Vec<u8>, Error> {
        if length == Some(0) {
            return Ok(Vec::new());
        }
        let stat = match known_stat {
            Some(stat) => stat.clone(),
            None => self.stat_inner(object).await?,
        };
        let requested = length.unwrap_or_else(|| stat.size.saturating_sub(offset));
        let planned = requested.min(stat.size.saturating_sub(offset));
        let planned = usize::try_from(planned).map_err(|_| {
            Error::InvalidArgument("planned read length does not fit in usize".into())
        })?;
        // Byte-vector allocations cannot exceed isize::MAX even when usize is wider.
        if planned > isize::MAX as usize {
            return Err(Error::InvalidArgument(
                "planned read length exceeds Vec capacity".into(),
            ));
        }
        if planned == 0 {
            return Ok(Vec::new());
        }

        let mut bytes = vec![0_u8; planned];
        let written = self
            .read_into_resolved(object, offset, &mut bytes, &stat)
            .await?;
        bytes.truncate(written);
        Ok(bytes)
    }

    /// Read an object range into a caller-owned buffer.
    ///
    /// `known_stat` has the same exact-version semantics as [`read`](Self::read).
    pub async fn read_into(
        &self,
        object: &ObjectId,
        offset: u64,
        dst: &mut [u8],
        known_stat: Option<&ObjectStat>,
    ) -> Result<usize, Error> {
        self.read_into_with_options(
            object,
            offset,
            dst,
            known_stat,
            &crate::RequestOptions::default(),
        )
        .await
    }

    /// Read with explicit parent selection; stat and block RPCs share this scope.
    pub async fn read_into_with_options(
        &self,
        object: &ObjectId,
        offset: u64,
        dst: &mut [u8],
        known_stat: Option<&ObjectStat>,
        options: &crate::RequestOptions<'_>,
    ) -> Result<usize, Error> {
        let op = talon_telemetry::Operation::new("talon.read", "internal", options.parent);
        let result = op
            .scope(self.read_into_inner(object, offset, dst, known_stat))
            .await;
        op.outcome(if result.is_ok() { "success" } else { "error" });
        result
    }

    async fn read_into_inner(
        &self,
        object: &ObjectId,
        offset: u64,
        dst: &mut [u8],
        known_stat: Option<&ObjectStat>,
    ) -> Result<usize, Error> {
        if dst.is_empty() {
            return Ok(0);
        }
        let stat = match known_stat {
            Some(stat) => stat.clone(),
            None => self.stat_inner(object).await?,
        };
        self.read_into_resolved(object, offset, dst, &stat).await
    }

    async fn read_into_resolved(
        &self,
        object: &ObjectId,
        offset: u64,
        dst: &mut [u8],
        stat: &ObjectStat,
    ) -> Result<usize, Error> {
        let requested = u64::try_from(dst.len())
            .map_err(|_| Error::InvalidArgument("destination length does not fit in u64".into()))?;
        if stat.version.trim().is_empty() {
            return Err(Error::InvalidArgument(
                "object version must be non-empty".into(),
            ));
        }
        let version = Version::new(stat.version.as_str());
        let planned_len = requested.min(stat.size.saturating_sub(offset)) as usize;
        let mut plan = iter_read(
            object,
            offset,
            requested,
            self.block_size,
            &version,
            stat.size,
        );
        talon_telemetry::record("talon.range.offset", offset);
        talon_telemetry::record("talon.range.length", planned_len as u64);
        let blocks = if planned_len == 0 {
            0
        } else {
            (offset % u64::from(self.block_size) + planned_len as u64)
                .div_ceil(u64::from(self.block_size))
        };
        talon_telemetry::record("talon.read.planned_blocks", blocks);
        if planned_len == 0 {
            return Ok(0);
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);
        let mut pending = FuturesUnordered::new();
        let mut rest = &mut dst[..planned_len];
        for segment in plan.by_ref().take(MAX_CONCURRENT_BLOCK_READS_PER_READ) {
            let (chunk, tail) = rest.split_at_mut(segment.len as usize);
            rest = tail;
            pending.push(read_segment_into(&self.reader, segment, chunk, now_ms));
        }

        let mut written = 0;
        while let Some(result) = pending.next().await {
            written += result?;
            if let Some(segment) = plan.next() {
                let (chunk, tail) = rest.split_at_mut(segment.len as usize);
                rest = tail;
                pending.push(read_segment_into(&self.reader, segment, chunk, now_ms));
            }
        }
        Ok(written)
    }
}

async fn read_segment_into(
    reader: &BlockReader,
    segment: BlockSegment,
    dst: &mut [u8],
    now_ms: u64,
) -> Result<usize, Error> {
    reader
        .read_versioned_block_into(&segment.block, segment.offset_in_block, dst, now_ms)
        .await
        .map_err(Error::from)
}

/// Parse a `scheme://bucket/key` URI into an object id.
pub fn parse_uri(uri: &str) -> Result<ObjectId, Error> {
    let (scheme, rest) = uri
        .split_once("://")
        .ok_or_else(|| UriError::MissingScheme {
            uri: uri.to_string(),
        })?;
    let backend = scheme.parse().map_err(|_| UriError::UnknownScheme {
        scheme: scheme.to_string(),
    })?;
    let (bucket, key) = rest.split_once('/').ok_or_else(|| UriError::MissingKey {
        uri: uri.to_string(),
        scheme: scheme.to_string(),
    })?;
    if bucket.is_empty() {
        return Err(UriError::EmptyBucket {
            uri: uri.to_string(),
        }
        .into());
    }
    if key.is_empty() {
        return Err(UriError::EmptyKey {
            uri: uri.to_string(),
        }
        .into());
    }
    Ok(ObjectId::new(backend, bucket, key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use talon_core::{Backend, NodeId, NodeInfo, NodeRole};
    use talon_transport::frame::{FrameHeader, HEADER_LEN};
    use talon_transport::{
        decode_versioned_request, encode_typed_error, response_header_ok, ControlMessage,
        DataErrorCode,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::{Notify, Semaphore};

    async fn mock_coordinator() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut header_bytes = [0_u8; HEADER_LEN];
                    socket.read_exact(&mut header_bytes).await.unwrap();
                    let header = FrameHeader::decode(&header_bytes).unwrap();
                    let mut payload = vec![0_u8; header.length as usize];
                    socket.read_exact(&mut payload).await.unwrap();
                    let mut frame = header_bytes.to_vec();
                    frame.extend_from_slice(&payload);
                    let (_, request) = talon_transport::decode(&frame).unwrap();
                    let response = match request {
                        ControlMessage::StatObject { .. } => ControlMessage::ObjectStat {
                            size: 8192,
                            version: "test-version".into(),
                        },
                        ControlMessage::ListObjects { prefix } => {
                            assert_eq!(prefix, "s3/bucket/data");
                            ControlMessage::ObjectList {
                                entries: vec![ObjectEntry {
                                    path: "s3/bucket/data/a.parquet".into(),
                                    size: 17,
                                }],
                            }
                        }
                        other => panic!("unexpected request: {other:?}"),
                    };
                    socket
                        .write_all(&talon_transport::encode(0, &response).unwrap())
                        .await
                        .unwrap();
                });
            }
        });
        addr
    }

    async fn mock_worker() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut header_bytes = [0_u8; HEADER_LEN];
                    if socket.read_exact(&mut header_bytes).await.is_err() {
                        return;
                    }
                    let header = FrameHeader::decode(&header_bytes).unwrap();
                    let mut payload = vec![0_u8; header.length as usize];
                    socket.read_exact(&mut payload).await.unwrap();
                    let mut frame = header_bytes.to_vec();
                    frame.extend_from_slice(&payload);
                    let (_, versioned) = decode_versioned_request(&frame).unwrap();
                    assert_eq!(versioned.version, Version::new("test-version"));
                    let request = versioned.request;
                    let bytes: Vec<u8> = (0..request.len)
                        .map(|index| ((request.offset + index) % 251) as u8)
                        .collect();
                    let mut response = response_header_ok(0, bytes.len() as u32).to_vec();
                    response.extend_from_slice(&bytes);
                    socket.write_all(&response).await.unwrap();
                });
            }
        });
        addr
    }

    async fn mock_short_worker() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut header_bytes = [0_u8; HEADER_LEN];
                    if socket.read_exact(&mut header_bytes).await.is_err() {
                        return;
                    }
                    let header = FrameHeader::decode(&header_bytes).unwrap();
                    let mut payload = vec![0_u8; header.length as usize];
                    socket.read_exact(&mut payload).await.unwrap();
                    let mut frame = header_bytes.to_vec();
                    frame.extend_from_slice(&payload);
                    let (_, versioned) = decode_versioned_request(&frame).unwrap();
                    assert_eq!(versioned.version, Version::new("test-version"));
                    let request = versioned.request;
                    let short_len = request.len.saturating_sub(1) as usize;
                    let mut response = response_header_ok(0, short_len as u32).to_vec();
                    response.extend_from_slice(&vec![0_u8; short_len]);
                    socket.write_all(&response).await.unwrap();
                });
            }
        });
        addr
    }

    async fn mock_delayed_worker(
        second_block_started: Arc<Notify>,
        release_first_block: Arc<Notify>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let second_block_started = Arc::clone(&second_block_started);
                let release_first_block = Arc::clone(&release_first_block);
                tokio::spawn(async move {
                    let mut header_bytes = [0_u8; HEADER_LEN];
                    if socket.read_exact(&mut header_bytes).await.is_err() {
                        return;
                    }
                    let header = FrameHeader::decode(&header_bytes).unwrap();
                    let mut payload = vec![0_u8; header.length as usize];
                    socket.read_exact(&mut payload).await.unwrap();
                    let mut frame = header_bytes.to_vec();
                    frame.extend_from_slice(&payload);
                    let (_, versioned) = decode_versioned_request(&frame).unwrap();
                    assert_eq!(versioned.version, Version::new("test-version"));
                    let request = versioned.request;
                    if request.offset < 8 {
                        release_first_block.notified().await;
                    } else {
                        second_block_started.notify_one();
                    }
                    let bytes: Vec<u8> = (0..request.len)
                        .map(|index| ((request.offset + index) % 251) as u8)
                        .collect();
                    let mut response = response_header_ok(0, bytes.len() as u32).to_vec();
                    response.extend_from_slice(&bytes);
                    socket.write_all(&response).await.unwrap();
                });
            }
        });
        addr
    }

    async fn mock_failure_worker(
        stalled_block_started: Arc<Notify>,
        stalled_block_disconnected: Arc<Notify>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let stalled_block_started = Arc::clone(&stalled_block_started);
                let stalled_block_disconnected = Arc::clone(&stalled_block_disconnected);
                tokio::spawn(async move {
                    let mut header_bytes = [0_u8; HEADER_LEN];
                    if socket.read_exact(&mut header_bytes).await.is_err() {
                        return;
                    }
                    let header = FrameHeader::decode(&header_bytes).unwrap();
                    let mut payload = vec![0_u8; header.length as usize];
                    socket.read_exact(&mut payload).await.unwrap();
                    let mut frame = header_bytes.to_vec();
                    frame.extend_from_slice(&payload);
                    let (_, versioned) = decode_versioned_request(&frame).unwrap();
                    assert_eq!(versioned.version, Version::new("test-version"));
                    let request = versioned.request;
                    if request.offset < 8 {
                        stalled_block_started.notify_one();
                        let mut byte = [0_u8; 1];
                        if socket.read(&mut byte).await.unwrap() == 0 {
                            stalled_block_disconnected.notify_one();
                        }
                    } else {
                        stalled_block_started.notified().await;
                        socket
                            .write_all(&encode_typed_error(
                                0,
                                DataErrorCode::InvalidRequest,
                                "injected block failure",
                            ))
                            .await
                            .unwrap();
                    }
                });
            }
        });
        addr
    }

    async fn mock_read_coordinator(
        worker_addr: String,
        size: u64,
        stat_calls: Arc<AtomicUsize>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let worker_addr = worker_addr.clone();
                let stat_calls = Arc::clone(&stat_calls);
                tokio::spawn(async move {
                    let mut header_bytes = [0_u8; HEADER_LEN];
                    if socket.read_exact(&mut header_bytes).await.is_err() {
                        return;
                    }
                    let header = FrameHeader::decode(&header_bytes).unwrap();
                    let mut payload = vec![0_u8; header.length as usize];
                    socket.read_exact(&mut payload).await.unwrap();
                    let mut frame = header_bytes.to_vec();
                    frame.extend_from_slice(&payload);
                    let (_, request) = talon_transport::decode(&frame).unwrap();
                    let response = match request {
                        ControlMessage::StatObject { .. } => {
                            stat_calls.fetch_add(1, Ordering::SeqCst);
                            ControlMessage::ObjectStat {
                                size,
                                version: "test-version".into(),
                            }
                        }
                        ControlMessage::MembershipQuery {} => ControlMessage::MembershipList {
                            nodes: vec![NodeInfo {
                                id: NodeId::new("worker-a"),
                                address: worker_addr,
                                role: NodeRole::Worker,
                            }],
                        },
                        ControlMessage::MembershipQueryV2 {} => ControlMessage::MembershipListV2 {
                            nodes: vec![talon_transport::ZonedNodeInfo {
                                info: NodeInfo {
                                    id: NodeId::new("worker-a"),
                                    address: worker_addr,
                                    role: NodeRole::Worker,
                                },
                                zone: None,
                            }],
                        },
                        other => panic!("unexpected request: {other:?}"),
                    };
                    socket
                        .write_all(&talon_transport::encode(0, &response).unwrap())
                        .await
                        .unwrap();
                });
            }
        });
        addr
    }

    async fn mock_bounded_worker(
        active: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        started: Arc<AtomicUsize>,
        started_notify: Arc<Notify>,
        release: Arc<Semaphore>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let active = Arc::clone(&active);
                let peak = Arc::clone(&peak);
                let started = Arc::clone(&started);
                let started_notify = Arc::clone(&started_notify);
                let release = Arc::clone(&release);
                tokio::spawn(async move {
                    let mut header_bytes = [0_u8; HEADER_LEN];
                    if socket.read_exact(&mut header_bytes).await.is_err() {
                        return;
                    }
                    let header = FrameHeader::decode(&header_bytes).unwrap();
                    let mut payload = vec![0_u8; header.length as usize];
                    socket.read_exact(&mut payload).await.unwrap();
                    let mut frame = header_bytes.to_vec();
                    frame.extend_from_slice(&payload);
                    let (_, request) = decode_versioned_request(&frame).unwrap();
                    assert_eq!(request.version.0.as_str(), "test-version");
                    let request = request.request;

                    let active_now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(active_now, Ordering::SeqCst);
                    started.fetch_add(1, Ordering::SeqCst);
                    started_notify.notify_waiters();
                    release.acquire().await.unwrap().forget();

                    let bytes: Vec<u8> = (0..request.len)
                        .map(|index| ((request.offset + index) % 251) as u8)
                        .collect();
                    let mut response = response_header_ok(0, bytes.len() as u32).to_vec();
                    response.extend_from_slice(&bytes);
                    active.fetch_sub(1, Ordering::SeqCst);
                    socket.write_all(&response).await.unwrap();
                });
            }
        });
        addr
    }

    async fn wait_for_started(started: &AtomicUsize, notify: &Notify, target: usize) {
        loop {
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if started.load(Ordering::SeqCst) >= target {
                return;
            }
            tokio::time::timeout(Duration::from_secs(2), notified)
                .await
                .expect("worker requests did not start");
        }
    }

    async fn read_client(size: u64) -> (Client, Arc<AtomicUsize>) {
        let worker = mock_worker().await;
        let stat_calls = Arc::new(AtomicUsize::new(0));
        let coordinator = mock_read_coordinator(worker, size, Arc::clone(&stat_calls)).await;
        (
            ClientBuilder::default()
                .with_coordinator(coordinator)
                .with_block_size(8)
                .build()
                .unwrap(),
            stat_calls,
        )
    }

    #[test]
    fn parses_supported_object_uris() {
        for (uri, backend) in [
            ("s3://bucket/key", Backend::S3),
            ("gcs://bucket/key", Backend::Gcs),
            ("az://container/a/b.parquet", Backend::Azure),
        ] {
            let object = parse_uri(uri).unwrap();
            assert_eq!(object.backend, backend);
        }
    }

    #[test]
    fn parse_uri_keeps_nested_keys_intact() {
        let object = parse_uri("az://container/a/b/c.parquet").unwrap();

        assert_eq!(object.bucket, "container");
        assert_eq!(object.object_path, "a/b/c.parquet");
    }

    #[test]
    fn parse_uri_rejects_malformed_input_with_a_useful_message() {
        for (uri, expected) in [
            ("bucket/key", "scheme://bucket/key"),
            ("ftp://bucket/key", "unknown backend scheme"),
            ("az://bucket", "missing an object key"),
            ("az:///key", "empty bucket"),
            ("az://bucket/", "empty object key"),
        ] {
            let message = parse_uri(uri).unwrap_err().to_string();
            assert!(
                message.contains(expected),
                "error for {uri:?} should mention {expected:?}, got {message:?}"
            );
        }
    }

    #[test]
    fn build_requires_a_coordinator() {
        let error = ClientBuilder::default()
            .build()
            .err()
            .expect("a coordinator must be provided");
        assert!(matches!(error, Error::InvalidArgument(_)));
        assert!(error.to_string().contains("coordinator"));
    }

    #[test]
    fn build_uses_defaults_and_final_overrides() {
        let default_client = ClientBuilder::default()
            .with_coordinator("127.0.0.1:7000")
            .build()
            .unwrap();
        assert_eq!(default_client.coordinator_addr(), "127.0.0.1:7000");
        assert_eq!(default_client.block_size(), 256 * 1024 * 1024);

        // Setters only collect configuration; validation uses the final values.
        let client = ClientBuilder::default()
            .with_block_size(0)
            .with_max_idle_per_addr(0)
            .with_coordinator("127.0.0.1:7001")
            .with_max_idle_per_addr(1)
            .with_block_size(1024)
            .build()
            .unwrap();
        assert_eq!(client.coordinator_addr(), "127.0.0.1:7001");
        assert_eq!(client.block_size(), 1024);
    }

    #[test]
    fn rejects_zero_block_size() {
        let error = ClientBuilder::default()
            .with_coordinator("127.0.0.1:7000")
            .with_block_size(0)
            .build()
            .err()
            .expect("zero block size must fail");
        assert!(matches!(error, Error::InvalidArgument(_)));
    }

    #[test]
    fn rejects_zero_max_idle_per_addr() {
        let error = ClientBuilder::default()
            .with_coordinator("127.0.0.1:7000")
            .with_max_idle_per_addr(0)
            .build()
            .err()
            .expect("zero idle connection limit must fail");
        assert!(matches!(error, Error::InvalidArgument(_)));
        assert!(error.to_string().contains("max_idle_per_addr"));
    }

    #[tokio::test]
    async fn max_idle_per_addr_controls_coordinator_and_worker_connection_reuse() {
        tokio::time::timeout(Duration::from_secs(10), async {
            for (limit, concurrent, second_batch_accepts) in
                [(None, 10, 12), (Some(1), 3, 5), (Some(12), 14, 16)]
            {
                for worker in [false, true] {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap().to_string();
                    let accepts = Arc::new(AtomicUsize::new(0));
                    // Hold each batch until every request has its own connection.
                    // The idle limit must not cap concurrent requests.
                    let barrier = Arc::new(tokio::sync::Barrier::new(concurrent));
                    let count = Arc::clone(&accepts);
                    let server = tokio::spawn(async move {
                        loop {
                            let (mut socket, _) = listener.accept().await.unwrap();
                            count.fetch_add(1, Ordering::SeqCst);
                            let barrier = Arc::clone(&barrier);
                            tokio::spawn(async move {
                                loop {
                                    let mut header_bytes = [0_u8; HEADER_LEN];
                                    if socket.read_exact(&mut header_bytes).await.is_err() {
                                        return;
                                    }
                                    let header = FrameHeader::decode(&header_bytes).unwrap();
                                    let mut payload = vec![0_u8; header.length as usize];
                                    socket.read_exact(&mut payload).await.unwrap();
                                    let response = if worker {
                                        let mut frame = header_bytes.to_vec();
                                        frame.extend_from_slice(&payload);
                                        let (_, versioned) =
                                            decode_versioned_request(&frame).unwrap();
                                        assert_eq!(versioned.version, Version::new("test-version"));
                                        let request = versioned.request;
                                        assert_eq!(request.len, 8);
                                        let mut response = response_header_ok(0, 8).to_vec();
                                        response.extend_from_slice(&[7; 8]);
                                        response
                                    } else {
                                        talon_transport::encode(
                                            0,
                                            &ControlMessage::ObjectStat {
                                                size: 8,
                                                version: "test-version".into(),
                                            },
                                        )
                                        .unwrap()
                                    };
                                    barrier.wait().await;
                                    socket.write_all(&response).await.unwrap();
                                }
                            });
                        }
                    });
                    let coordinator = if worker {
                        mock_read_coordinator(addr, 8, Arc::new(AtomicUsize::new(0))).await
                    } else {
                        addr
                    };
                    let builder = ClientBuilder::default()
                        .with_coordinator(coordinator)
                        .with_block_size(1024);
                    let builder = match limit {
                        Some(limit) => builder.with_max_idle_per_addr(limit),
                        None => builder,
                    };
                    let client = builder.build().unwrap();
                    let object = parse_uri("s3://bucket/key").unwrap();
                    let stat = ObjectStat {
                        size: 8,
                        version: "test-version".into(),
                    };
                    for (client, expected_accepts) in
                        [(client.clone(), concurrent), (client, second_batch_accepts)]
                    {
                        futures::future::join_all((0..concurrent).map(|_| async {
                            if worker {
                                assert_eq!(
                                    client.read(&object, 0, Some(8), Some(&stat)).await.unwrap(),
                                    vec![7; 8]
                                );
                            } else {
                                assert_eq!(client.stat(&object).await.unwrap().size, 8);
                            }
                        }))
                        .await;
                        assert_eq!(
                            accepts.load(Ordering::SeqCst),
                            expected_accepts,
                            "worker={worker}, limit={limit:?}"
                        );
                    }
                    server.abort();
                }
            }
        })
        .await
        .expect("idle limit must not block concurrent requests");
    }

    #[tokio::test]
    async fn stat_returns_coordinator_metadata() {
        let client = ClientBuilder::default()
            .with_coordinator(mock_coordinator().await)
            .with_block_size(1024)
            .build()
            .unwrap();

        let stat = client
            .stat(&parse_uri("s3://bucket/key").unwrap())
            .await
            .unwrap();

        assert_eq!(stat.size, 8192);
        assert_eq!(stat.version, "test-version");
    }

    #[tokio::test]
    async fn list_returns_existing_object_entries() {
        let client = ClientBuilder::default()
            .with_coordinator(mock_coordinator().await)
            .with_block_size(1024)
            .build()
            .unwrap();

        let entries = client.list("s3/bucket/data").await.unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "s3/bucket/data/a.parquet");
        assert_eq!(entries[0].size, 17);
    }

    #[tokio::test]
    async fn known_stat_skips_coordinator_stat() {
        let (client, stat_calls) = read_client(16).await;
        let object = parse_uri("s3://bucket/key").unwrap();
        let stat = ObjectStat {
            size: 16,
            version: "test-version".into(),
        };

        let bytes = client.read(&object, 2, Some(6), Some(&stat)).await.unwrap();

        assert_eq!(bytes, vec![2, 3, 4, 5, 6, 7]);
        assert_eq!(stat_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn missing_stat_is_resolved_once() {
        let (client, stat_calls) = read_client(16).await;
        let object = parse_uri("s3://bucket/key").unwrap();

        let bytes = client.read(&object, 0, Some(4), None).await.unwrap();

        assert_eq!(bytes, vec![0, 1, 2, 3]);
        assert_eq!(stat_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn read_to_end_uses_resolved_size() {
        let (client, _) = read_client(10).await;
        let object = parse_uri("s3://bucket/key").unwrap();

        let bytes = client.read(&object, 4, None, None).await.unwrap();

        assert_eq!(bytes, vec![4, 5, 6, 7, 8, 9]);
    }

    #[tokio::test]
    async fn allocating_read_rejects_length_above_vec_capacity() {
        let client = ClientBuilder::default()
            .with_coordinator("127.0.0.1:1")
            .with_block_size(8)
            .build()
            .unwrap();
        let object = parse_uri("s3://bucket/key").unwrap();
        let too_large = isize::MAX as u64 + 1;
        let stat = ObjectStat {
            size: too_large,
            version: "test-version".into(),
        };

        let error = client
            .read(&object, 0, Some(too_large), Some(&stat))
            .await
            .expect_err("lengths above Vec capacity must be rejected");

        assert!(matches!(error, Error::InvalidArgument(_)));
    }

    #[tokio::test]
    async fn read_into_crossing_eof_returns_short_count() {
        let (client, _) = read_client(10).await;
        let object = parse_uri("s3://bucket/key").unwrap();
        let stat = ObjectStat {
            size: 10,
            version: "test-version".into(),
        };
        let mut dst = [0_u8; 8];

        let written = client
            .read_into(&object, 6, &mut dst, Some(&stat))
            .await
            .unwrap();

        assert_eq!(written, 4);
        assert_eq!(&dst[..written], &[6, 7, 8, 9]);
    }

    #[tokio::test]
    async fn empty_reads_do_not_stat_or_fetch() {
        let (client, stat_calls) = read_client(16).await;
        let object = parse_uri("s3://bucket/key").unwrap();
        let mut dst = [];

        assert!(client
            .read(&object, 0, Some(0), None)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            client.read_into(&object, 0, &mut dst, None).await.unwrap(),
            0
        );
        assert_eq!(stat_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multi_block_read_is_concurrent_and_keeps_byte_order() {
        let second_block_started = Arc::new(Notify::new());
        let release_first_block = Arc::new(Notify::new());
        let worker = mock_delayed_worker(
            Arc::clone(&second_block_started),
            Arc::clone(&release_first_block),
        )
        .await;
        let stat_calls = Arc::new(AtomicUsize::new(0));
        let coordinator = mock_read_coordinator(worker, 16, stat_calls).await;
        let client = ClientBuilder::default()
            .with_coordinator(coordinator)
            .with_block_size(8)
            .build()
            .unwrap();
        let object = parse_uri("s3://bucket/key").unwrap();
        let stat = ObjectStat {
            size: 16,
            version: "test-version".into(),
        };
        let read = tokio::spawn(async move { client.read(&object, 6, Some(8), Some(&stat)).await });

        let overlapped =
            tokio::time::timeout(Duration::from_millis(250), second_block_started.notified())
                .await
                .is_ok();
        release_first_block.notify_one();
        let bytes = tokio::time::timeout(Duration::from_secs(2), read)
            .await
            .expect("read did not finish")
            .unwrap()
            .unwrap();

        assert!(
            overlapped,
            "second block did not start while first was pending"
        );
        assert_eq!(bytes, vec![6, 7, 8, 9, 10, 11, 12, 13]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multi_block_read_refills_the_internal_eight_request_window() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let started_notify = Arc::new(Notify::new());
        let release = Arc::new(Semaphore::new(0));
        let worker = mock_bounded_worker(
            Arc::clone(&active),
            Arc::clone(&peak),
            Arc::clone(&started),
            Arc::clone(&started_notify),
            Arc::clone(&release),
        )
        .await;
        let stat_calls = Arc::new(AtomicUsize::new(0));
        let coordinator = mock_read_coordinator(worker, 10, stat_calls).await;
        let client = ClientBuilder::default()
            .with_coordinator(coordinator)
            .with_block_size(1)
            .build()
            .unwrap();
        let object = parse_uri("s3://bucket/key").unwrap();
        let stat = ObjectStat {
            size: 10,
            version: "test-version".into(),
        };
        let read =
            tokio::spawn(async move { client.read(&object, 0, Some(10), Some(&stat)).await });

        wait_for_started(
            &started,
            &started_notify,
            MAX_CONCURRENT_BLOCK_READS_PER_READ,
        )
        .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            started.load(Ordering::SeqCst),
            MAX_CONCURRENT_BLOCK_READS_PER_READ,
            "a ninth block started before one of the first eight completed"
        );

        release.add_permits(1);
        wait_for_started(&started, &started_notify, 9).await;
        release.add_permits(9);
        let bytes = tokio::time::timeout(Duration::from_secs(2), read)
            .await
            .expect("bounded read did not finish")
            .unwrap()
            .unwrap();

        assert_eq!(bytes, (0_u8..10).collect::<Vec<_>>());
        assert_eq!(started.load(Ordering::SeqCst), 10);
        assert_eq!(peak.load(Ordering::SeqCst), 8);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn block_failure_drops_the_other_unfinished_read() {
        let stalled_block_started = Arc::new(Notify::new());
        let stalled_block_disconnected = Arc::new(Notify::new());
        let worker = mock_failure_worker(
            Arc::clone(&stalled_block_started),
            Arc::clone(&stalled_block_disconnected),
        )
        .await;
        let stat_calls = Arc::new(AtomicUsize::new(0));
        let coordinator = mock_read_coordinator(worker, 16, stat_calls).await;
        let client = ClientBuilder::default()
            .with_coordinator(coordinator)
            .with_block_size(8)
            .build()
            .unwrap();
        let object = parse_uri("s3://bucket/key").unwrap();
        let stat = ObjectStat {
            size: 16,
            version: "test-version".into(),
        };
        let mut dst = [0_u8; 16];

        let error = client
            .read_into(&object, 0, &mut dst, Some(&stat))
            .await
            .expect_err("one failed block must fail the whole read");

        assert!(matches!(error, Error::Block(_)));
        tokio::time::timeout(
            Duration::from_secs(2),
            stalled_block_disconnected.notified(),
        )
        .await
        .expect("unfinished block connection was not cancelled");
    }

    #[tokio::test]
    async fn short_worker_reply_fails_the_whole_read() {
        let worker = mock_short_worker().await;
        let stat_calls = Arc::new(AtomicUsize::new(0));
        let coordinator = mock_read_coordinator(worker, 8, stat_calls).await;
        let client = ClientBuilder::default()
            .with_coordinator(coordinator)
            .with_block_size(8)
            .build()
            .unwrap();
        let object = parse_uri("s3://bucket/key").unwrap();
        let stat = ObjectStat {
            size: 8,
            version: "test-version".into(),
        };
        let mut dst = [0_u8; 8];

        let error = client
            .read_into(&object, 0, &mut dst, Some(&stat))
            .await
            .expect_err("short worker reply must fail the whole read");

        assert!(matches!(error, Error::Block(_)));
    }
}
