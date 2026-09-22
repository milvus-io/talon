use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::{
    Error, ObjectEntry, ObjectId, ObjectStat, RequestOptions, TraceContext, TraceParent, UriError,
    Version,
};
use talon_cache_client::pool::{DEFAULT_IDLE_TTL, DEFAULT_MAX_IDLE_PER_ADDR};
use talon_cache_client::{ConnectionPool, PlacementCache};
const PLACEMENT_TTL_MS: u64 = 30_000;
const REPLICAS_K: u8 = 1;
use futures::stream::{FuturesUnordered, StreamExt};
use talon_cache_client::{iter_read, BlockReader, BlockSegment, CoordinatorClient};

// Limit task allocation and let other logical reads make progress.
const MAX_CONCURRENT_BLOCK_READS_PER_READ: usize = 8;
/// Default active block-read budget shared by a client and all its clones.
pub const DEFAULT_MAX_IN_FLIGHT_BLOCK_READS: usize = 1024;

/// Shared business state, with connection pools local to each I/O thread.
#[derive(Clone)]
pub(crate) struct Core {
    pub(crate) coordinator: CoordinatorClient,
    pub(crate) reader: BlockReader,
    pub(crate) block_read_permits: Arc<Semaphore>,
    pub(crate) block_size: u32,
}

/// Client owning fixed Tokio I/O threads and their connection pools.
/// Clones share the executor, metadata and request budgets. Awaiting methods
/// needs no caller Tokio runtime. Dropping a pending async read cancels it.
#[derive(Clone)]
pub struct Client {
    executor: Arc<crate::executor::Executor>,
    coordinator: Arc<str>,
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
    max_in_flight_block_reads: usize,
    io_threads: Option<usize>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            coordinator: String::new(),
            block_size: 256 << 20,
            max_idle_per_addr: DEFAULT_MAX_IDLE_PER_ADDR,
            max_in_flight_block_reads: DEFAULT_MAX_IN_FLIGHT_BLOCK_READS,
            io_threads: None,
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

    /// Set the active block-read limit shared by this client and its clones.
    ///
    /// Defaults to 1024. A permit covers placement resolution, worker attempts,
    /// and retries for one block. Metadata calls and separate clients have
    /// independent budgets. Waiting and active reads release capacity on cancellation.
    pub fn with_max_in_flight_block_reads(mut self, max: usize) -> Self {
        self.max_in_flight_block_reads = max;
        self
    }

    /// Set the number of fixed Tokio I/O threads owned by this client.
    /// Defaults to TOKIO_WORKER_THREADS, then the available CPU count.
    pub fn with_io_threads(mut self, threads: usize) -> Self {
        self.io_threads = Some(threads);
        self
    }

    /// Construct the client and its Tokio I/O threads. No caller runtime is needed.
    pub fn build(self) -> Result<Client, Error> {
        let threads = self.io_threads;
        let core = self.build_core()?;
        let coordinator = Arc::from(core.coordinator_addr());
        let block_size = core.block_size();
        let executor = Arc::new(crate::executor::Executor::new(core, threads)?);
        Ok(Client {
            executor,
            coordinator,
            block_size,
        })
    }

    pub(crate) fn build_core(self) -> Result<Core, Error> {
        let Self {
            coordinator,
            block_size,
            max_idle_per_addr,
            max_in_flight_block_reads,
            ..
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
        if max_in_flight_block_reads == 0 || max_in_flight_block_reads > Semaphore::MAX_PERMITS {
            return Err(Error::InvalidArgument(
                "max_in_flight_block_reads is outside the supported nonzero range".into(),
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
        Ok(Core {
            coordinator,
            reader,
            block_read_permits: Arc::new(Semaphore::new(max_in_flight_block_reads)),
            block_size,
        })
    }
}

impl Core {
    pub(crate) fn with_sharded_connections(&self) -> Self {
        let coordinator = self.coordinator.with_sharded_connections();
        Self {
            reader: self.reader.with_sharded_connections(coordinator.clone()),
            coordinator,
            ..self.clone()
        }
    }

    /// Address of the coordinator used by this client.
    pub fn coordinator_addr(&self) -> &str {
        self.coordinator.addr()
    }

    /// Logical block size used for range planning.
    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    /// Return an object's current size and source version.
    #[cfg(test)]
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
    #[cfg(test)]
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
    #[cfg(test)]
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
        if blocks == 1 {
            // A single block needs the shared permit, but no fan-out queue.
            let segment = plan.next().expect("nonempty single-block read");
            return read_segment_into(
                &self.reader,
                &self.block_read_permits,
                segment,
                &mut dst[..planned_len],
                now_ms,
            )
            .await;
        }
        let mut pending = FuturesUnordered::new();
        let mut rest = &mut dst[..planned_len];
        for segment in plan.by_ref().take(MAX_CONCURRENT_BLOCK_READS_PER_READ) {
            let (chunk, tail) = rest.split_at_mut(segment.len as usize);
            rest = tail;
            pending.push(read_segment_into(
                &self.reader,
                &self.block_read_permits,
                segment,
                chunk,
                now_ms,
            ));
        }

        let mut written = 0;
        while let Some(result) = pending.next().await {
            written += result?;
            if let Some(segment) = plan.next() {
                let (chunk, tail) = rest.split_at_mut(segment.len as usize);
                rest = tail;
                pending.push(read_segment_into(
                    &self.reader,
                    &self.block_read_permits,
                    segment,
                    chunk,
                    now_ms,
                ));
            }
        }
        Ok(written)
    }
}

async fn read_segment_into(
    reader: &BlockReader,
    permits: &Semaphore,
    segment: BlockSegment,
    dst: &mut [u8],
    now_ms: u64,
) -> Result<usize, Error> {
    let _permit = permits
        .acquire()
        .await
        .expect("client never closes its read budget");
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

impl Client {
    /// Address of the configured coordinator.
    pub fn coordinator_addr(&self) -> &str {
        &self.coordinator
    }
    /// Logical block size used for range planning.
    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    pub async fn stat(&self, object: &ObjectId) -> Result<ObjectStat, Error> {
        self.stat_with_options(object, &RequestOptions::default())
            .await
    }
    pub async fn stat_with_options(
        &self,
        object: &ObjectId,
        options: &RequestOptions<'_>,
    ) -> Result<ObjectStat, Error> {
        let object = object.clone();
        let parent = capture_parent(options);
        crate::executor::Request(self.executor.spawn(move |core| async move {
            core.stat_with_options(&object, &options_for(&parent)).await
        }))
        .await?
    }
    pub async fn list(&self, prefix: &str) -> Result<Vec<ObjectEntry>, Error> {
        let prefix = prefix.to_owned();
        let parent = capture_parent(&RequestOptions::default());
        crate::executor::Request(self.executor.spawn(move |core| async move {
            let operation = talon_telemetry::Operation::new(
                "talon.list",
                "internal",
                options_for(&parent).parent,
            );
            let result = operation.scope(core.list(&prefix)).await;
            operation.outcome(if result.is_ok() { "success" } else { "error" });
            result
        }))
        .await?
    }
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
            &RequestOptions::default(),
        )
        .await
    }
    /// Read on the client's I/O threads. A known stat pins the source version.
    pub async fn read_with_options(
        &self,
        object: &ObjectId,
        offset: u64,
        length: Option<u64>,
        known_stat: Option<&ObjectStat>,
        options: &RequestOptions<'_>,
    ) -> Result<Vec<u8>, Error> {
        let object = object.clone();
        let stat = known_stat.cloned();
        let parent = capture_parent(options);
        crate::executor::Request(self.executor.spawn(move |core| async move {
            core.read_with_options(
                &object,
                offset,
                length,
                stat.as_ref(),
                &options_for(&parent),
            )
            .await
        }))
        .await?
    }
    /// Read directly into an owned destination, returning `(bytes_written, buffer)`.
    ///
    /// Moving a `Vec<u8>` or `Box<[u8]>` here transfers ownership, not its bytes.
    /// The destination must already have the desired length; capacity alone does
    /// not make bytes writable. Bytes past the returned count remain unchanged.
    /// On error the buffer is dropped and may have been partially written.
    /// Dropping the future cancels the operation; the I/O task keeps the buffer
    /// alive until it stops accessing it. Forgetting the future may leak resources
    /// but cannot leave the task accessing freed caller memory.
    ///
    /// ```no_run
    /// # async fn example(client: &talon_rust_client::Client,
    /// # object: &talon_rust_client::ObjectId) -> Result<(), talon_rust_client::Error> {
    /// let buffer = vec![0; 4096];
    /// let (written, buffer) = client.read_into(object, 0, buffer, None).await?;
    /// // Consume &buffer[..written], then reuse the same allocation for another read.
    /// # let _ = (written, buffer);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read_into<B>(
        &self,
        object: &ObjectId,
        offset: u64,
        buffer: B,
        known_stat: Option<&ObjectStat>,
    ) -> Result<(usize, B), Error>
    where
        B: AsMut<[u8]> + Send + 'static,
    {
        self.read_into_with_options(
            object,
            offset,
            buffer,
            known_stat,
            &RequestOptions::default(),
        )
        .await
    }
    /// Direct owned-buffer read with explicit tracing parent selection.
    /// Has the same ownership, error and cancellation rules as [`Self::read_into`].
    pub async fn read_into_with_options<B>(
        &self,
        object: &ObjectId,
        offset: u64,
        mut buffer: B,
        known_stat: Option<&ObjectStat>,
        options: &RequestOptions<'_>,
    ) -> Result<(usize, B), Error>
    where
        B: AsMut<[u8]> + Send + 'static,
    {
        let object = object.clone();
        let stat = known_stat.cloned();
        let parent = capture_parent(options);
        crate::executor::Request(self.executor.spawn(move |core| async move {
            let written = core
                .read_into_with_options(
                    &object,
                    offset,
                    buffer.as_mut(),
                    stat.as_ref(),
                    &options_for(&parent),
                )
                .await?;
            Ok((written, buffer))
        }))
        .await?
    }
    /// Submit a direct read into storage owned by the operation. The buffer is
    /// dropped before invoking the callback, on the same fixed I/O thread.
    /// Keep a client clone alive until the callback returns. Dropping the last
    /// clone cancels queued and running operations without invoking callbacks.
    pub fn read_into_owned_with_callback<B>(
        &self,
        object: ObjectId,
        offset: u64,
        mut buffer: B,
        stat: Option<ObjectStat>,
        options: &RequestOptions<'_>,
        callback: impl FnOnce(Result<usize, Error>) + Send + 'static,
    ) where
        B: AsMut<[u8]> + Send + 'static,
    {
        let parent = capture_parent(options);
        self.executor.spawn(move |core| async move {
            let result = core
                .read_into_with_options(
                    &object,
                    offset,
                    buffer.as_mut(),
                    stat.as_ref(),
                    &options_for(&parent),
                )
                .await;
            drop(buffer);
            callback(result);
        });
    }
    /// Submit a stat with the same callback and lifetime rules as a direct read.
    pub fn stat_with_callback(
        &self,
        object: ObjectId,
        options: &RequestOptions<'_>,
        callback: impl FnOnce(Result<ObjectStat, Error>) + Send + 'static,
    ) {
        let parent = capture_parent(options);
        self.executor.spawn(move |core| async move {
            callback(core.stat_with_options(&object, &options_for(&parent)).await);
        });
    }
}
fn capture_parent(options: &RequestOptions<'_>) -> Option<TraceContext> {
    match options.parent {
        TraceParent::Explicit(parent) => Some(parent.clone()),
        TraceParent::Inherit => talon_telemetry::current_carrier(),
        TraceParent::Root => None,
    }
}
fn options_for(parent: &Option<TraceContext>) -> RequestOptions<'_> {
    RequestOptions {
        parent: parent
            .as_ref()
            .map(TraceParent::Explicit)
            .unwrap_or(TraceParent::Root),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ClientBuilder;
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

    async fn read_client(size: u64) -> (Core, Arc<AtomicUsize>) {
        let worker = mock_worker().await;
        let stat_calls = Arc::new(AtomicUsize::new(0));
        let coordinator = mock_read_coordinator(worker, size, Arc::clone(&stat_calls)).await;
        (
            ClientBuilder::default()
                .with_coordinator(coordinator)
                .with_block_size(8)
                .build_core()
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
            .build_core()
            .err()
            .expect("a coordinator must be provided");
        assert!(matches!(error, Error::InvalidArgument(_)));
        assert!(error.to_string().contains("coordinator"));
    }

    #[test]
    fn build_uses_defaults_and_final_overrides() {
        let default_client = ClientBuilder::default()
            .with_coordinator("127.0.0.1:7000")
            .build_core()
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
            .build_core()
            .unwrap();
        assert_eq!(client.coordinator_addr(), "127.0.0.1:7001");
        assert_eq!(client.block_size(), 1024);
    }

    #[test]
    fn rejects_zero_block_size() {
        let error = ClientBuilder::default()
            .with_coordinator("127.0.0.1:7000")
            .with_block_size(0)
            .build_core()
            .err()
            .expect("zero block size must fail");
        assert!(matches!(error, Error::InvalidArgument(_)));
    }

    #[test]
    fn rejects_zero_max_idle_per_addr() {
        let error = ClientBuilder::default()
            .with_coordinator("127.0.0.1:7000")
            .with_max_idle_per_addr(0)
            .build_core()
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
                    let client = builder.build_core().unwrap();
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
            .build_core()
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
            .build_core()
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
            .build_core()
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
        // Cover a cross-block read, single-block reads, and an empty EOF read.
        for offset in [6, 8, 9, 10] {
            let mut dst = [0xff_u8; 8];
            let written = client
                .read_into(&object, offset, &mut dst, Some(&stat))
                .await
                .unwrap();

            assert_eq!(written, (stat.size - offset) as usize);
            assert_eq!(&dst[..written], &(offset as u8..10).collect::<Vec<_>>());
            assert!(dst[written..].iter().all(|&byte| byte == 0xff));
        }
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
            .build_core()
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
            .build_core()
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
    async fn aggregate_block_limit_is_shared_by_concurrent_reads() {
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
        let coordinator = mock_read_coordinator(worker, 16, stat_calls).await;
        let client = ClientBuilder::default()
            .with_coordinator(coordinator)
            .with_block_size(8)
            .with_max_in_flight_block_reads(1)
            .build_core()
            .unwrap();
        let object = parse_uri("s3://bucket/key").unwrap();
        let stat = ObjectStat {
            size: 16,
            version: "test-version".into(),
        };

        let first_client = client.clone();
        let first_object = object.clone();
        let first_stat = stat.clone();
        let first = tokio::spawn(async move {
            first_client
                .read(&first_object, 0, Some(8), Some(&first_stat))
                .await
        });
        let second =
            tokio::spawn(async move { client.read(&object, 8, Some(8), Some(&stat)).await });

        wait_for_started(&started, &started_notify, 1).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            started.load(Ordering::SeqCst),
            1,
            "a second logical read bypassed the shared aggregate limit"
        );

        release.add_permits(1);
        wait_for_started(&started, &started_notify, 2).await;
        release.add_permits(1);
        let first_bytes = tokio::time::timeout(Duration::from_secs(2), first)
            .await
            .expect("first read did not finish")
            .unwrap()
            .unwrap();
        let second_bytes = tokio::time::timeout(Duration::from_secs(2), second)
            .await
            .expect("second read did not finish")
            .unwrap()
            .unwrap();

        assert_eq!(first_bytes, (0_u8..8).collect::<Vec<_>>());
        assert_eq!(second_bytes, (8_u8..16).collect::<Vec<_>>());
    }

    #[test]
    fn aggregate_budget_validates_capacity_and_clones_share_it() {
        for invalid in [0, usize::MAX] {
            assert!(ClientBuilder::default()
                .with_coordinator("localhost:1")
                .with_max_in_flight_block_reads(invalid)
                .build_core()
                .is_err());
        }
        let client = ClientBuilder::default()
            .with_coordinator("localhost:1")
            .build_core()
            .unwrap();
        let clone = client.clone();
        assert!(Arc::ptr_eq(
            &client.block_read_permits,
            &clone.block_read_permits
        ));
        assert_eq!(client.block_read_permits.available_permits(), 1024);
    }

    #[tokio::test]
    async fn cancelling_a_budget_waiter_preserves_capacity() {
        let client = ClientBuilder::default()
            .with_coordinator("localhost:1")
            .with_max_in_flight_block_reads(1)
            .build_core()
            .unwrap();
        let held = client.block_read_permits.acquire().await.unwrap();
        let object = parse_uri("s3://bucket/key").unwrap();
        let stat = ObjectStat {
            size: 1,
            version: "v1".into(),
        };
        let read = client.read(&object, 0, Some(1), Some(&stat));
        assert!(tokio::time::timeout(Duration::from_millis(20), read)
            .await
            .is_err());
        drop(held);
        assert_eq!(client.block_read_permits.available_permits(), 1);
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
            .build_core()
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
        assert_eq!(
            client.block_read_permits.available_permits(),
            DEFAULT_MAX_IN_FLIGHT_BLOCK_READS
        );
    }

    #[tokio::test]
    async fn short_worker_reply_fails_the_whole_read() {
        let worker = mock_short_worker().await;
        let stat_calls = Arc::new(AtomicUsize::new(0));
        let coordinator = mock_read_coordinator(worker, 8, stat_calls).await;
        let client = ClientBuilder::default()
            .with_coordinator(coordinator)
            .with_block_size(8)
            .build_core()
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn owned_client_read_and_stat_work_without_a_caller_runtime() {
        struct Buffer(Vec<u8>, usize, std::thread::ThreadId, Arc<AtomicUsize>);
        impl AsMut<[u8]> for Buffer {
            fn as_mut(&mut self) -> &mut [u8] {
                assert_eq!(self.0.as_ptr() as usize, self.1);
                assert_ne!(std::thread::current().id(), self.2);
                self.3.fetch_add(1, Ordering::SeqCst);
                &mut self.0
            }
        }
        let worker = mock_worker().await;
        let coordinator = mock_read_coordinator(worker, 16, Arc::new(AtomicUsize::new(0))).await;
        let client = ClientBuilder::default()
            .with_coordinator(coordinator)
            .with_block_size(8)
            .with_io_threads(2)
            .build()
            .unwrap();
        std::thread::spawn(move || {
            let object = parse_uri("s3://bucket/key").unwrap();
            let stat = futures::executor::block_on(client.stat(&object)).unwrap();
            assert_eq!(stat.size, 16);
            let bytes = futures::executor::block_on(client.read(&object, 0, Some(12), Some(&stat)))
                .unwrap();
            assert_eq!(bytes, (0_u8..12).collect::<Vec<_>>());
            let bytes = vec![0xff; 12];
            let address = bytes.as_ptr() as usize;
            let accesses = Arc::new(AtomicUsize::new(0));
            let dst = Buffer(
                bytes,
                address,
                std::thread::current().id(),
                accesses.clone(),
            );
            let (written, dst) =
                futures::executor::block_on(client.read_into(&object, 0, dst, Some(&stat)))
                    .unwrap();
            assert_eq!(written, 12);
            assert_eq!(dst.0, (0_u8..12).collect::<Vec<_>>());
            assert_eq!(dst.0.as_ptr() as usize, address);
            let (written, dst) = futures::executor::block_on(client.read_into_with_options(
                &object,
                14,
                dst,
                Some(&stat),
                &RequestOptions::default(),
            ))
            .unwrap();
            assert_eq!(written, 2);
            assert_eq!(&dst.0[..2], &[14, 15]);
            assert_eq!(&dst.0[2..], &(2_u8..12).collect::<Vec<_>>());
            assert_eq!(dst.0.as_ptr() as usize, address);
            assert_eq!(accesses.load(Ordering::SeqCst), 2);
            let (written, empty) =
                futures::executor::block_on(client.read_into(&object, 0, Vec::<u8>::new(), None))
                    .unwrap();
            assert_eq!(written, 0);
            assert!(empty.is_empty());
        })
        .join()
        .unwrap();
    }

    async fn numbered_stat_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let mut id = 0;
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                id += 1;
                tokio::spawn(async move {
                    loop {
                        let mut header = [0; HEADER_LEN];
                        if socket.read_exact(&mut header).await.is_err() {
                            return;
                        }
                        let header = FrameHeader::decode(&header).unwrap();
                        let mut body = vec![0; header.length as usize];
                        socket.read_exact(&mut body).await.unwrap();
                        tokio::task::yield_now().await;
                        let response = talon_transport::encode(
                            0,
                            &ControlMessage::ObjectStat {
                                size: id,
                                version: "v1".into(),
                            },
                        )
                        .unwrap();
                        if socket.write_all(&response).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        addr
    }

    fn stat_chain(
        client: Client,
        mut observations: Vec<(std::thread::ThreadId, u64)>,
        done: tokio::sync::oneshot::Sender<Vec<(std::thread::ThreadId, u64)>>,
    ) {
        let next = client.clone();
        client.stat_with_callback(
            parse_uri("s3://bucket/key").unwrap(),
            &RequestOptions::default(),
            move |result| {
                observations.push((std::thread::current().id(), result.unwrap().size));
                if observations.len() == 8 {
                    let _ = done.send(observations);
                } else {
                    stat_chain(next, observations, done);
                }
            },
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn callback_chains_keep_their_thread_and_connection() {
        let client = ClientBuilder::default()
            .with_coordinator(numbered_stat_server().await)
            .with_io_threads(2)
            .build()
            .unwrap();
        let (a, ar) = tokio::sync::oneshot::channel();
        let (b, br) = tokio::sync::oneshot::channel();
        stat_chain(client.clone(), Vec::new(), a);
        stat_chain(client.clone(), Vec::new(), b);
        let a = tokio::time::timeout(Duration::from_secs(2), ar)
            .await
            .unwrap()
            .unwrap();
        let b = tokio::time::timeout(Duration::from_secs(2), br)
            .await
            .unwrap()
            .unwrap();
        assert!(a.iter().all(|x| *x == a[0]));
        assert!(b.iter().all(|x| *x == b[0]));
        assert_ne!(
            a[0].0, b[0].0,
            "independent submissions use different I/O threads"
        );
        assert_ne!(a[0].1, b[0].1, "connections must not cross threads");
        assert_ne!(a[0].0, std::thread::current().id());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn owned_executor_threads_share_the_block_read_budget() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(Notify::new());
        let release = Arc::new(Semaphore::new(0));
        let worker = mock_bounded_worker(
            active,
            peak.clone(),
            started.clone(),
            notify.clone(),
            release.clone(),
        )
        .await;
        let coordinator = mock_read_coordinator(worker, 16, Arc::new(AtomicUsize::new(0))).await;
        let client = ClientBuilder::default()
            .with_coordinator(coordinator)
            .with_block_size(8)
            .with_io_threads(2)
            .with_max_in_flight_block_reads(1)
            .build()
            .unwrap();
        let object = parse_uri("s3://bucket/key").unwrap();
        let stat = ObjectStat {
            size: 16,
            version: "test-version".into(),
        };
        let a = client.read(&object, 0, Some(8), Some(&stat));
        let b = client.read(&object, 8, Some(8), Some(&stat));
        tokio::pin!(a, b);
        assert!(futures::poll!(a.as_mut()).is_pending());
        assert!(futures::poll!(b.as_mut()).is_pending());
        wait_for_started(&started, &notify, 1).await;
        assert!(tokio::time::timeout(
            Duration::from_millis(30),
            wait_for_started(&started, &notify, 2)
        )
        .await
        .is_err());
        release.add_permits(1);
        wait_for_started(&started, &notify, 2).await;
        release.add_permits(1);
        assert_eq!(a.await.unwrap().len(), 8);
        assert_eq!(b.await.unwrap().len(), 8);
        assert_eq!(peak.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn failed_owned_read_releases_the_destination_on_its_io_thread() {
        struct Buffer(Vec<u8>, std::sync::mpsc::Sender<std::thread::ThreadId>);
        impl AsMut<[u8]> for Buffer {
            fn as_mut(&mut self) -> &mut [u8] {
                &mut self.0
            }
        }
        impl Drop for Buffer {
            fn drop(&mut self) {
                let _ = self.1.send(std::thread::current().id());
            }
        }
        let client = ClientBuilder::default()
            .with_coordinator("localhost:1")
            .with_io_threads(1)
            .build()
            .unwrap();
        let object = parse_uri("s3://bucket/key").unwrap();
        let stat = ObjectStat {
            size: 8,
            version: String::new(),
        };
        let (dropped, observed) = std::sync::mpsc::channel();
        let result = futures::executor::block_on(client.read_into(
            &object,
            0,
            Buffer(vec![0; 8], dropped),
            Some(&stat),
        ));
        assert!(matches!(result, Err(Error::InvalidArgument(_))));
        assert_ne!(observed.try_recv().unwrap(), std::thread::current().id());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn owned_read_retains_its_buffer_without_polling_or_dropping_the_future() {
        let started = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(Notify::new());
        let release = Arc::new(Semaphore::new(0));
        let worker = mock_bounded_worker(
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            started.clone(),
            notify.clone(),
            release.clone(),
        )
        .await;
        let coordinator = mock_read_coordinator(worker, 8, Arc::new(AtomicUsize::new(0))).await;
        let client = ClientBuilder::default()
            .with_coordinator(coordinator)
            .with_block_size(8)
            .with_io_threads(1)
            .with_max_in_flight_block_reads(1)
            .build()
            .unwrap();
        let object = parse_uri("s3://bucket/key").unwrap();
        let stat = ObjectStat {
            size: 8,
            version: "test-version".into(),
        };
        let dst = vec![0xff; 8];
        let address = dst.as_ptr() as usize;
        let mut read = Box::pin(client.read_into(&object, 0, dst, Some(&stat)));
        assert!(futures::poll!(read.as_mut()).is_pending());
        wait_for_started(&started, &notify, 1).await;
        // Suppress Drop just as forget would, then reclaim the future after the
        // background read completes so this regression test does not leak it.
        let read = std::mem::ManuallyDrop::new(read);
        release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if client
                    .executor
                    .spawn(|core| async move { core.block_read_permits.available_permits() })
                    .await
                    .unwrap()
                    == 1
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let (written, dst) = std::mem::ManuallyDrop::into_inner(read).await.unwrap();
        assert_eq!(written, 8);
        assert_eq!(dst, (0_u8..8).collect::<Vec<_>>());
        assert_eq!(dst.as_ptr() as usize, address);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn direct_destination_drops_before_callback_and_last_owner_can_drop_there() {
        struct Buffer(Vec<u8>, std::sync::mpsc::Sender<std::thread::ThreadId>);
        impl AsMut<[u8]> for Buffer {
            fn as_mut(&mut self) -> &mut [u8] {
                &mut self.0
            }
        }
        impl Drop for Buffer {
            fn drop(&mut self) {
                self.1.send(std::thread::current().id()).unwrap();
            }
        }
        let started = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(Notify::new());
        let release = Arc::new(Semaphore::new(0));
        let worker = mock_bounded_worker(
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            started.clone(),
            notify.clone(),
            release.clone(),
        )
        .await;
        let coordinator = mock_read_coordinator(worker, 8, Arc::new(AtomicUsize::new(0))).await;
        let client = ClientBuilder::default()
            .with_coordinator(coordinator)
            .with_block_size(8)
            .with_io_threads(2)
            .build()
            .unwrap();
        let last = client.clone();
        let (dropped, observed) = std::sync::mpsc::channel();
        let (done, finished) = tokio::sync::oneshot::channel();
        client.read_into_owned_with_callback(
            parse_uri("s3://bucket/key").unwrap(),
            0,
            Buffer(vec![0; 8], dropped),
            Some(ObjectStat {
                size: 8,
                version: "test-version".into(),
            }),
            &RequestOptions::default(),
            move |result| {
                assert_eq!(result.unwrap(), 8);
                assert_eq!(observed.try_recv().unwrap(), std::thread::current().id());
                drop(last);
                let _ = done.send(());
            },
        );
        wait_for_started(&started, &notify, 1).await;
        drop(client);
        release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(2), finished)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelling_an_owned_request_closes_its_socket_and_returns_budget() {
        struct Buffer(Vec<u8>, Option<tokio::sync::oneshot::Sender<()>>);
        impl AsMut<[u8]> for Buffer {
            fn as_mut(&mut self) -> &mut [u8] {
                &mut self.0
            }
        }
        impl Drop for Buffer {
            fn drop(&mut self) {
                let _ = self.1.take().unwrap().send(());
            }
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let worker = listener.local_addr().unwrap().to_string();
        let (started, accepted) = tokio::sync::oneshot::channel();
        let (closed, disconnected) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header = [0; HEADER_LEN];
            socket.read_exact(&mut header).await.unwrap();
            let header = FrameHeader::decode(&header).unwrap();
            let mut body = vec![0; header.length as usize];
            socket.read_exact(&mut body).await.unwrap();
            started.send(()).unwrap();
            let mut byte = [0];
            assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
            closed.send(()).unwrap();
        });
        let coordinator = mock_read_coordinator(worker, 8, Arc::new(AtomicUsize::new(0))).await;
        let client = ClientBuilder::default()
            .with_coordinator(coordinator)
            .with_io_threads(2)
            .with_max_in_flight_block_reads(1)
            .build()
            .unwrap();
        let object = parse_uri("s3://bucket/key").unwrap();
        let stat = ObjectStat {
            size: 8,
            version: "test-version".into(),
        };
        let (dropped, mut released) = tokio::sync::oneshot::channel();
        let mut read =
            Box::pin(client.read_into(&object, 0, Buffer(vec![0; 8], Some(dropped)), Some(&stat)));
        assert!(futures::poll!(read.as_mut()).is_pending());
        tokio::time::timeout(Duration::from_secs(2), accepted)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            released.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        drop(read);
        tokio::time::timeout(Duration::from_secs(2), released)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), disconnected)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            client
                .executor
                .spawn(|core| async move { core.block_read_permits.available_permits() })
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn last_owner_shutdown_drops_pending_tasks_on_their_io_thread() {
        struct Guard(std::sync::mpsc::Sender<std::thread::ThreadId>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.send(std::thread::current().id()).unwrap();
            }
        }
        let client = ClientBuilder::default()
            .with_coordinator("localhost:1")
            .with_io_threads(2)
            .build()
            .unwrap();
        let (started, ready) = tokio::sync::oneshot::channel();
        let (dropped, finished) = std::sync::mpsc::channel();
        client.executor.spawn(move |_| async move {
            let _guard = Guard(dropped);
            started.send(std::thread::current().id()).unwrap();
            std::future::pending::<()>().await;
        });
        let owner = ready.await.unwrap();
        drop(client);
        assert_eq!(
            finished.recv_timeout(Duration::from_secs(2)).unwrap(),
            owner
        );
        assert_ne!(owner, std::thread::current().id());
        assert!(matches!(
            ClientBuilder::default()
                .with_coordinator("localhost:1")
                .with_io_threads(0)
                .build(),
            Err(Error::InvalidArgument(_))
        ));
    }
}
