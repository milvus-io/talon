//! Block read orchestration over persistent logical membership.
//!
//! [`BlockReader`] ranks logical worker IDs with the cached placement table,
//! then resolves up to `replicas_k` candidates from bounded instance discovery.
//! Offline or conflicted candidates keep their rank but can be skipped. Retryable
//! read failures try the next ranked candidate without an
//! in-request discovery refresh. Expired discovery cannot route any candidate.
//! A stale reused connection may be redialed once while discovery is valid.
//! Instance-specific connection pools prevent a replacement process from
//! inheriting an old process's sockets.
//!
//! Multi-block splitting is handled by [`crate::read_plan`]; protocol frontends
//! own their prefetch policy.

#[cfg(test)]
use crate::membership_fixture;

use std::sync::Arc;

use talon_core::{BlockId, ObjectId, Version};

use crate::coordinator_client::{CoordinatorClient, CoordinatorError};
use crate::membership_cache::{MembershipCache, MembershipSnapshot};
use crate::metrics::{ReadStats, ZoneMatch, ZoneReadObserver};
use crate::placement_cache::PlacementCache;
use crate::pool::ConnectionPool;
use crate::range_stream::CacheReadError;
use crate::read_plan::plan_read;
use crate::worker_client::WorkerError;

pub(crate) enum DetailedBlockReadError {
    Block(BlockReadError),
}

impl From<BlockReadError> for DetailedBlockReadError {
    fn from(error: BlockReadError) -> Self {
        Self::Block(error)
    }
}

/// Whether a miss follows the current origin or the block's exact version.
#[derive(Clone, Copy)]
enum OriginReadMode {
    Current,
    ExactVersion,
}

/// Errors from a block read.
#[derive(Debug, thiserror::Error)]
pub enum BlockReadError {
    /// Authoritative membership refresh failed.
    #[error(transparent)]
    Coordinator(#[from] CoordinatorError),
    /// The worker fetch failed.
    #[error(transparent)]
    Worker(#[from] WorkerError),
    #[error("worker {worker_id}, instance {instance_id} at {address}: {source}")]
    Target {
        worker_id: String,
        instance_id: String,
        address: String,
        #[source]
        source: WorkerError,
    },
    /// The cluster returned no owners for the block (empty cluster).
    #[error("no owners for block")]
    NoOwners,
    /// All configured logical candidates failed, without refreshing discovery.
    #[error("all replicas failed; last worker {worker_id}, instance {instance_id:?} at {worker}: {source}")]
    AllReplicasFailed {
        /// Last candidate's address when known, otherwise its logical worker ID.
        worker: String,
        /// Last candidate logical identity, including unavailable candidates.
        worker_id: String,
        /// Last observed instance identity, absent for offline or conflicting members.
        instance_id: Option<String>,
        /// Original failure, preserved for diagnostics and classification.
        #[source]
        source: WorkerError,
    },
}

/// The coordinates of an open file needed to plan a read: its object identity,
/// logical block size, source version/etag, and total size (for EOF clamping).
///
/// Grouping these keeps [`BlockReader::read`] to a small argument list and
/// mirrors what a `getattr`/HEAD lookup yields for an open handle.
#[derive(Debug, Clone)]
pub struct FileView<'a> {
    /// The object being read.
    pub object: &'a ObjectId,
    /// Logical block size in bytes.
    pub block_size: u32,
    /// Source version/etag guarding the blocks.
    pub version: &'a Version,
    /// Total object length, used to clamp reads at EOF.
    pub size: u64,
}

/// Orchestrates local block placement and worker reads with caching.
#[derive(Clone)]
pub struct BlockReader {
    coordinator: CoordinatorClient,
    cache: Arc<PlacementCache>,
    /// Maximum number of ranked logical workers attempted per block read.
    replicas_k: u8,
    /// Read-path counters (cache hit/miss, worker fetches, bytes served).
    stats: ReadStats,
    /// Settings template for isolated per-instance connection pools.
    worker_pool: Arc<ConnectionPool>,
    /// Logical membership and bounded instance discovery; expired instances fail closed.
    pub(crate) membership: Arc<MembershipCache>,
    /// Active LOAD RPCs shared by this reader and its clones.
    pub(crate) load_slots: Arc<tokio::sync::Semaphore>,
    /// Serializes cold refreshes so an expired snapshot causes one control request.
    membership_refresh: Arc<tokio::sync::Mutex<Option<std::time::Instant>>>,
    /// This reader's own deployment zone, for read classification (ADR 0006).
    zone: Option<String>,
    /// Sink for zone-classified read events; defaults to a no-op.
    zone_observer: Arc<dyn ZoneReadObserver>,
}

impl BlockReader {
    /// Create a reader over the given coordinator client and placement cache.
    ///
    /// `replicas_k` bounds the ordered candidate list, primary first. It does not
    /// populate replicas; zero is treated as one.
    /// Metrics are collected into a fresh [`ReadStats`]; use
    /// [`with_stats`](Self::with_stats) to share an existing one.
    pub fn new(coordinator: CoordinatorClient, cache: Arc<PlacementCache>, replicas_k: u8) -> Self {
        let membership = Arc::new(MembershipCache::new(cache.ttl_ms()));
        Self {
            coordinator,
            cache,
            replicas_k: replicas_k.max(1),
            stats: ReadStats::new(),
            worker_pool: Arc::new(ConnectionPool::new()),
            membership,
            load_slots: Arc::new(tokio::sync::Semaphore::new(8)),
            membership_refresh: Arc::new(tokio::sync::Mutex::new(None)),
            zone: None,
            zone_observer: Arc::new(crate::metrics::NoopZoneReadObserver),
        }
    }

    /// Record metrics into the provided counters, shared by reader clones.
    pub fn with_stats(mut self, stats: ReadStats) -> Self {
        self.stats = stats;
        self
    }

    /// Use these pool settings for each isolated worker instance pool.
    pub fn with_worker_pool(mut self, worker_pool: Arc<ConnectionPool>) -> Self {
        self.worker_pool = worker_pool;
        self
    }

    /// Configure zone-affine placement (ADR 0006).
    ///
    /// With `enabled` and a known `zone`, placement is computed over the
    /// same-zone worker subset; an empty subset falls back to the full
    /// membership and reports through `observer`. Served reads are classified
    /// same/cross/unknown against `zone` regardless of `enabled`, so the
    /// observer also measures the cross-zone baseline before the filter is
    /// turned on.
    pub fn with_zone_affinity(
        mut self,
        zone: Option<String>,
        enabled: bool,
        observer: Arc<dyn ZoneReadObserver>,
    ) -> Self {
        self.membership = Arc::new(
            MembershipCache::new(self.cache.ttl_ms()).with_zone_affinity(zone.clone(), enabled),
        );
        self.membership_refresh = Arc::new(tokio::sync::Mutex::new(None));
        self.zone = zone;
        self.zone_observer = observer;
        self
    }

    /// The coordinator address this reader resolves placement against.
    pub fn coordinator_addr(&self) -> &str {
        self.coordinator.addr()
    }

    /// Drop all local placement entries for an object after an origin mutation.
    pub fn invalidate_object(&self, object: &ObjectId) -> usize {
        self.cache.invalidate_object(object)
    }

    /// The read-path counters this reader updates.
    pub fn stats(&self) -> &ReadStats {
        &self.stats
    }

    /// Read a versioned block slice only when it is already resident in Talon.
    ///
    /// Unlike [`read_block`](Self::read_block), this operation cannot invoke a
    /// worker backend. It is therefore suitable for a gateway that must use a
    /// request-scoped client capability for every origin miss.
    pub async fn read_cached_block(
        &self,
        block: &BlockId,
        offset_in_block: u32,
        len: u32,
    ) -> Result<Vec<u8>, CacheReadError> {
        let snapshot = self
            .membership_snapshot()
            .await
            .map_err(cache_block_error)?;
        let mut last = BlockReadError::NoOwners;
        for (target, candidate) in snapshot.candidates(block, self.replicas_k) {
            let worker = match candidate {
                Ok(worker) => worker,
                Err(error) => {
                    last = snapshot.replica_error(target, error);
                    continue;
                }
            };
            let result = worker
                .fetch_cached_range(
                    &block.object,
                    &block.version,
                    block.offset + u64::from(offset_in_block),
                    u64::from(len),
                )
                .await;
            let error = match result {
                Ok(bytes) if bytes.len() == len as usize => {
                    self.record_zone_read(worker.addr(), bytes.len() as u64);
                    return Ok(bytes);
                }
                Ok(bytes) => WorkerError::RangeLengthMismatch {
                    expected: u64::from(len),
                    actual: bytes.len() as u64,
                },
                Err(error) => error,
            };
            if !replica_retryable(&error) {
                return Err(error.into());
            }
            last = snapshot.replica_error(target, error);
        }
        Err(cache_block_error(last))
    }

    /// Admit one complete versioned block to its current primary owner.
    ///
    /// The worker validates alignment, object length, and exact body length and
    /// commits atomically without invoking its configured backend.
    pub async fn admit_block(
        &self,
        block: &BlockId,
        object_len: u64,
        body: &[u8],
    ) -> Result<(), CacheReadError> {
        let snapshot = self
            .membership_snapshot()
            .await
            .map_err(cache_block_error)?;
        let worker = snapshot.owner(block).map_err(cache_block_error)?;
        worker
            .admit_cached_block(block, object_len, body)
            .await
            .map_err(Into::into)
    }

    /// Read `len` bytes at `offset_in_block` within `block`.
    ///
    /// Rank up to `replicas_k` logical candidates and resolve their unexpired
    /// instances. Retryable failures try the next candidate without refreshing
    /// discovery. Stale reused connections may be redialed once before expiry.
    ///
    /// This FUSE entry point follows the current origin generation. Use
    /// [`read_versioned_block_into`](Self::read_versioned_block_into) to pin the source.
    pub async fn read_block(
        &self,
        block: &BlockId,
        offset_in_block: u32,
        len: u32,
    ) -> Result<Vec<u8>, BlockReadError> {
        match self
            .read_block_detailed_with_mode(block, offset_in_block, len, OriginReadMode::Current)
            .await
        {
            Ok(bytes) => Ok(bytes),
            Err(DetailedBlockReadError::Block(error)) => Err(error),
        }
    }

    /// Read `dst.len()` bytes at `offset_in_block` within `block` into `dst`.
    ///
    /// This follows the same logical-candidate selection and replica fallback
    /// behavior as [`read_block`](Self::read_block), without allocating an
    /// intermediate result buffer.
    pub async fn read_block_into(
        &self,
        block: &BlockId,
        offset_in_block: u32,
        dst: &mut [u8],
    ) -> Result<usize, BlockReadError> {
        self.read_block_into_with_mode(block, offset_in_block, dst, OriginReadMode::Current)
            .await
    }

    /// Read into `dst` from the exact source version carried by `block`.
    ///
    /// A miss is conditionally fetched without resolving a newer generation.
    /// Version mismatches retain their worker error classification.
    /// [`read_block_into`](Self::read_block_into) keeps following the current origin.
    pub async fn read_versioned_block_into(
        &self,
        block: &BlockId,
        offset_in_block: u32,
        dst: &mut [u8],
    ) -> Result<usize, BlockReadError> {
        self.read_block_into_with_mode(block, offset_in_block, dst, OriginReadMode::ExactVersion)
            .await
    }

    async fn read_block_into_with_mode(
        &self,
        block: &BlockId,
        offset_in_block: u32,
        dst: &mut [u8],
        mode: OriginReadMode,
    ) -> Result<usize, BlockReadError> {
        let snapshot = self.membership_snapshot().await?;
        self.stats.record_cache_miss();
        if snapshot.affinity_fallback {
            self.zone_observer.affinity_fallback();
        }
        let offset = block.offset + u64::from(offset_in_block);
        let mut last = BlockReadError::NoOwners;
        for (target, candidate) in snapshot.candidates(block, self.replicas_k) {
            let worker = match candidate {
                Ok(worker) => worker,
                Err(error) => {
                    last = snapshot.replica_error(target, error);
                    continue;
                }
            };
            self.stats.record_worker_fetch();
            let result = match mode {
                OriginReadMode::Current => {
                    worker.fetch_range_into(&block.object, offset, dst).await
                }
                OriginReadMode::ExactVersion => {
                    worker
                        .fetch_versioned_range_into(&block.object, &block.version, offset, dst)
                        .await
                }
            };
            let error = match result {
                Ok(n) if n == dst.len() => {
                    self.stats.add_bytes_served(n as u64);
                    self.record_zone_read(worker.addr(), n as u64);
                    return Ok(n);
                }
                Ok(n) => WorkerError::RangeLengthMismatch {
                    expected: dst.len() as u64,
                    actual: n as u64,
                },
                Err(error) => error,
            };
            self.stats.record_worker_failure();
            if !replica_retryable(&error) {
                return Err(snapshot.target_error(target, error));
            }
            last = snapshot.replica_error(target, error);
        }
        Err(last)
    }

    /// Read one exact-version block slice while preserving the typed worker failure
    /// for the streaming frontend's fallback decision.
    pub(crate) async fn read_block_detailed(
        &self,
        block: &BlockId,
        offset_in_block: u32,
        len: u32,
    ) -> Result<Vec<u8>, DetailedBlockReadError> {
        self.read_block_detailed_with_mode(
            block,
            offset_in_block,
            len,
            OriginReadMode::ExactVersion,
        )
        .await
    }

    async fn read_block_detailed_with_mode(
        &self,
        block: &BlockId,
        offset_in_block: u32,
        len: u32,
        mode: OriginReadMode,
    ) -> Result<Vec<u8>, DetailedBlockReadError> {
        let snapshot = self.membership_snapshot().await?;
        self.stats.record_cache_miss();
        if snapshot.affinity_fallback {
            self.zone_observer.affinity_fallback();
        }
        let offset = block.offset + u64::from(offset_in_block);
        let mut last = BlockReadError::NoOwners;
        for (target, candidate) in snapshot.candidates(block, self.replicas_k) {
            let worker = match candidate {
                Ok(worker) => worker,
                Err(error) => {
                    last = snapshot.replica_error(target, error);
                    continue;
                }
            };
            self.stats.record_worker_fetch();
            let result = match mode {
                OriginReadMode::Current => {
                    worker
                        .fetch_range(&block.object, offset, u64::from(len))
                        .await
                }
                OriginReadMode::ExactVersion => {
                    worker
                        .fetch_versioned_range(
                            &block.object,
                            &block.version,
                            offset,
                            u64::from(len),
                        )
                        .await
                }
            };
            let error = match result {
                Ok(bytes) if bytes.len() == len as usize => {
                    self.stats.add_bytes_served(bytes.len() as u64);
                    self.record_zone_read(worker.addr(), bytes.len() as u64);
                    return Ok(bytes);
                }
                Ok(bytes) => WorkerError::RangeLengthMismatch {
                    expected: u64::from(len),
                    actual: bytes.len() as u64,
                },
                Err(error) => error,
            };
            self.stats.record_worker_failure();
            if !replica_retryable(&error) {
                return Err(snapshot.target_error(target, error).into());
            }
            last = snapshot.replica_error(target, error);
        }
        Err(last.into())
    }

    /// Reconcile the cache against an observed placement version.
    ///
    /// This updates the supplied [`PlacementCache`] for callers that populate it.
    /// Block reads rank the discovery snapshot directly and do not use these
    /// per-block entries. Tokens are compared for equality, not ordering.
    /// Returns `true` if the entry was invalidated.
    pub fn observe_epoch(&self, block: &BlockId, observed_epoch: u64) -> bool {
        self.cache.observe_epoch(block, observed_epoch)
    }

    /// Read `[offset, offset+len)` of a file, spanning block boundaries.
    ///
    /// Splits the request into per-block segments via
    /// [`crate::read_plan::plan_read`] (clamped to `file.size` at EOF),
    /// fetches each segment through [`read_block`](Self::read_block) — so each
    /// segment resolves its owner from the shared discovery snapshot — and
    /// concatenates the results in order. A read at or past EOF returns an empty
    /// buffer (POSIX short read).
    pub async fn read(
        &self,
        file: &FileView<'_>,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, BlockReadError> {
        let plan = plan_read(
            file.object,
            offset,
            len,
            file.block_size,
            file.version,
            file.size,
        );
        let mut out = Vec::with_capacity(plan.iter().map(|s| s.len as usize).sum());
        for seg in plan {
            let bytes = self
                .read_block(&seg.block, seg.offset_in_block, seg.len)
                .await?;
            if bytes.len() as u64 != u64::from(seg.len) {
                return Err(WorkerError::RangeLengthMismatch {
                    expected: u64::from(seg.len),
                    actual: bytes.len() as u64,
                }
                .into());
            }
            out.extend_from_slice(&bytes);
        }
        Ok(out)
    }

    /// Read from `file` at `offset` into `dst`, spanning block boundaries.
    ///
    /// The returned value is the number of bytes written. Reads at or past EOF
    /// return `0`, and reads overlapping EOF return a short count, matching
    /// [`read`](Self::read).
    pub async fn read_into(
        &self,
        file: &FileView<'_>,
        offset: u64,
        dst: &mut [u8],
    ) -> Result<usize, BlockReadError> {
        let plan = plan_read(
            file.object,
            offset,
            dst.len() as u64,
            file.block_size,
            file.version,
            file.size,
        );
        let mut written = 0usize;
        for seg in plan {
            let len = seg.len as usize;
            let end = written + len;
            let n = self
                .read_block_into(&seg.block, seg.offset_in_block, &mut dst[written..end])
                .await?;
            if n != len {
                return Err(WorkerError::RangeLengthMismatch {
                    expected: len as u64,
                    actual: n as u64,
                }
                .into());
            }
            written = end;
        }
        Ok(written)
    }

    /// Resolve the worker address that owns `object` (for a write/delete).
    ///
    /// Placement is by `BlockId`; a write addresses the object's first block
    /// under `version` (the mount uses a canonical version, #182), so this
    /// resolves the primary owner of block 0 through the same client-side
    /// placement path reads use, reusing bounded discovery. Returns the
    /// dialable `host:port` of the primary owner.
    pub async fn resolve_owner(
        &self,
        object: &ObjectId,
        block_size: u32,
        version: &Version,
    ) -> Result<String, BlockReadError> {
        let block = BlockId::new(object.clone(), 0, block_size, version.clone());
        let snapshot = self.membership_snapshot().await?;
        Ok(snapshot.owner(&block)?.addr().to_owned())
    }

    /// Classify one served worker read against this reader's zone and report
    /// it. The last-good snapshot is authoritative enough for metrics; a
    /// worker whose zone is not (yet) known classifies as `unknown`.
    fn record_zone_read(&self, address: &str, bytes: u64) {
        let matched = match (&self.zone, self.membership.last_good()) {
            (Some(zone), Some(snapshot)) => match snapshot.zones_by_address.get(address) {
                Some(worker_zone) if worker_zone == zone => ZoneMatch::Same,
                Some(_) => ZoneMatch::Cross,
                None => ZoneMatch::Unknown,
            },
            _ => ZoneMatch::Unknown,
        };
        self.zone_observer.worker_read(matched, bytes);
    }

    pub(crate) async fn membership_snapshot(&self) -> Result<MembershipSnapshot, BlockReadError> {
        if let Some(snapshot) = self.membership.fresh() {
            return Ok(snapshot);
        }
        let mut refresh = self.membership_refresh.lock().await;
        if let Some(snapshot) = self.membership.fresh() {
            return Ok(snapshot);
        }
        let observed = std::time::Instant::now();
        if refresh
            .is_some_and(|at| observed.duration_since(at) < std::time::Duration::from_millis(100))
        {
            if let Some(snapshot) = self.membership.last_good() {
                // Throttle discovery requests, but never route with an expired instance.
                return Ok(snapshot);
            }
        }
        *refresh = Some(observed);
        let view = self.coordinator.discovery().await?;
        self.cache.clear();
        Ok(self.membership.replace(view, observed, &self.worker_pool))
    }
}

fn cache_block_error(error: BlockReadError) -> CacheReadError {
    match error {
        BlockReadError::Coordinator(error) => error.into(),
        BlockReadError::Worker(error)
        | BlockReadError::Target { source: error, .. }
        | BlockReadError::AllReplicasFailed { source: error, .. } => error.into(),
        other => CacheReadError::Unavailable(other.to_string()),
    }
}

/// Candidate fallback cannot hide authoritative errors or evade tenant limits.
fn replica_retryable(error: &WorkerError) -> bool {
    match error {
        WorkerError::Remote(error) => !matches!(
            error.code,
            talon_transport::DataErrorCode::InvalidRequest
                | talon_transport::DataErrorCode::NotFound
                | talon_transport::DataErrorCode::VersionMismatch
                | talon_transport::DataErrorCode::Origin
                | talon_transport::DataErrorCode::RateLimited
        ),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::placement_cache::Cached;
    use std::sync::atomic::Ordering;
    use talon_core::{Backend, NodeId, NodeInfo, NodeRole, ObjectId, Version};
    use talon_transport::frame::{FrameHeader, HEADER_LEN};
    use talon_transport::{
        decode_cached_block_put_header, decode_cached_request, decode_request, encode_error,
        encode_typed_error, response_header_ok, ControlMessage, DataErrorCode, MsgType,
        RangeRequest,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::Barrier;

    fn block() -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "b", "o/1"),
            256 << 20, // second block, non-zero offset
            256 << 20,
            Version::new("v1"),
        )
    }

    /// A mock coordinator that advertises one worker membership entry.
    async fn mock_switching_coordinator(
        first_worker: String,
        next_worker: String,
        calls: Arc<std::sync::atomic::AtomicU32>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = match listener.accept().await {
                    Ok(value) => value,
                    Err(_) => return,
                };
                let first_worker = first_worker.clone();
                let next_worker = next_worker.clone();
                let calls = Arc::clone(&calls);
                tokio::spawn(async move {
                    let mut header = [0_u8; HEADER_LEN];
                    if socket.read_exact(&mut header).await.is_err() {
                        return;
                    }
                    let decoded = FrameHeader::decode(&header).unwrap();
                    let mut body = vec![0_u8; decoded.length as usize];
                    socket.read_exact(&mut body).await.unwrap();
                    let call = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let worker = if call == 0 { first_worker } else { next_worker };
                    let response =
                        membership_fixture::zoned(vec![talon_transport::ZonedNodeInfo {
                            info: NodeInfo {
                                id: NodeId::new("w1"),
                                address: worker,
                                role: NodeRole::Worker,
                            },
                            zone: None,
                        }]);
                    socket
                        .write_all(&talon_transport::encode(0, &response).unwrap())
                        .await
                        .unwrap();
                });
            }
        });
        addr
    }

    async fn spawn_barrier_error_worker(
        requests: Arc<std::sync::atomic::AtomicU32>,
        barrier: Arc<Barrier>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = match listener.accept().await {
                    Ok(value) => value,
                    Err(_) => return,
                };
                let requests = Arc::clone(&requests);
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    let mut header = [0_u8; HEADER_LEN];
                    if socket.read_exact(&mut header).await.is_err() {
                        return;
                    }
                    let decoded = FrameHeader::decode(&header).unwrap();
                    let mut body = vec![0_u8; decoded.length as usize];
                    socket.read_exact(&mut body).await.unwrap();
                    requests.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    barrier.wait().await;
                    socket
                        .write_all(&encode_error(decoded.request_id, "stale owner"))
                        .await
                        .unwrap();
                });
            }
        });
        addr
    }

    async fn mock_intermediate_then_switching_coordinator(
        stale_worker: String,
        recovered_worker: String,
        calls: Arc<std::sync::atomic::AtomicU32>,
        fail_intermediate: bool,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = match listener.accept().await {
                    Ok(value) => value,
                    Err(_) => return,
                };
                let stale_worker = stale_worker.clone();
                let recovered_worker = recovered_worker.clone();
                let calls = Arc::clone(&calls);
                tokio::spawn(async move {
                    let mut header = [0_u8; HEADER_LEN];
                    if socket.read_exact(&mut header).await.is_err() {
                        return;
                    }
                    let decoded = FrameHeader::decode(&header).unwrap();
                    let mut body = vec![0_u8; decoded.length as usize];
                    socket.read_exact(&mut body).await.unwrap();
                    let call = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let response = if call == 1 && fail_intermediate {
                        ControlMessage::Ack {
                            ok: false,
                            detail: Some("control plane unavailable".into()),
                        }
                    } else {
                        let worker = if call <= 1 {
                            stale_worker
                        } else {
                            recovered_worker
                        };
                        membership_fixture::zoned(vec![talon_transport::ZonedNodeInfo {
                            info: NodeInfo {
                                id: NodeId::new("w1"),
                                address: worker,
                                role: NodeRole::Worker,
                            },
                            zone: None,
                        }])
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

    async fn mock_coordinator(worker_addr: String) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let worker_addr = worker_addr.clone();
                tokio::spawn(async move {
                    let mut hdr = [0u8; HEADER_LEN];
                    if s.read_exact(&mut hdr).await.is_err() {
                        return;
                    }
                    let h = FrameHeader::decode(&hdr).unwrap();
                    let mut body = vec![0u8; h.length as usize];
                    s.read_exact(&mut body).await.unwrap();
                    let mut full = hdr.to_vec();
                    full.extend_from_slice(&body);
                    let (_h, msg) = talon_transport::decode(&full).unwrap();
                    let reply = match msg {
                        ControlMessage::MembershipQuery {} => {
                            membership_fixture::zoned(vec![talon_transport::ZonedNodeInfo {
                                info: NodeInfo {
                                    id: NodeId::new("w1"),
                                    address: worker_addr.clone(),
                                    role: NodeRole::Worker,
                                },
                                zone: None,
                            }])
                        }
                        _ => ControlMessage::Ack {
                            ok: false,
                            detail: None,
                        },
                    };
                    let out = talon_transport::encode(0, &reply).unwrap();
                    s.write_all(&out).await.unwrap();
                    s.flush().await.unwrap();
                });
            }
        });
        addr
    }

    /// A mock worker that returns deterministic bytes for the requested range,
    /// and records how many fetches it served.
    async fn mock_worker(hits: Arc<std::sync::atomic::AtomicU32>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let hits = Arc::clone(&hits);
                tokio::spawn(async move {
                    loop {
                        let mut hdr = [0u8; HEADER_LEN];
                        if s.read_exact(&mut hdr).await.is_err() {
                            return;
                        }
                        let h = FrameHeader::decode(&hdr).unwrap();
                        let mut body = vec![0u8; h.length as usize];
                        s.read_exact(&mut body).await.unwrap();
                        let mut full = hdr.to_vec();
                        full.extend_from_slice(&body);
                        let req: RangeRequest = match h.msg_type {
                            MsgType::GetRange => decode_request(&full).unwrap().1,
                            MsgType::GetVersionedRange => {
                                let request =
                                    talon_transport::decode_versioned_request(&full).unwrap().1;
                                assert_eq!(request.version, block().version);
                                request.request
                            }
                            MsgType::GetCachedRange => {
                                let request = decode_cached_request(&full).unwrap().1;
                                assert_eq!(request.version, block().version);
                                RangeRequest {
                                    object: request.object,
                                    offset: request.offset,
                                    len: request.len,
                                }
                            }
                            other => panic!("unexpected read request: {other:?}"),
                        };
                        hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        // Encode the absolute offset into the bytes so tests can
                        // verify the worker got the right sub-range.
                        let payload: Vec<u8> = (0..req.len)
                            .map(|i| ((req.offset + i) % 256) as u8)
                            .collect();
                        let mut out = response_header_ok(0, payload.len() as u32).to_vec();
                        out.extend_from_slice(&payload);
                        s.write_all(&out).await.unwrap();
                        s.flush().await.unwrap();
                    }
                });
            }
        });
        addr
    }

    /// A mock worker that always replies with an ERROR frame ("not present"),
    /// counting how many requests it saw. Loops so it survives retries.
    async fn spawn_erroring_worker(count: Arc<std::sync::atomic::AtomicU32>) -> String {
        spawn_worker_error(count, encode_error(0, "block not present")).await
    }

    async fn spawn_worker_error(
        count: Arc<std::sync::atomic::AtomicU32>,
        response: Vec<u8>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let count = Arc::clone(&count);
                let response = response.clone();
                tokio::spawn(async move {
                    let mut hdr = [0u8; HEADER_LEN];
                    if s.read_exact(&mut hdr).await.is_err() {
                        return;
                    }
                    let h = FrameHeader::decode(&hdr).unwrap();
                    let mut body = vec![0u8; h.length as usize];
                    s.read_exact(&mut body).await.unwrap();
                    count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    s.write_all(&response).await.unwrap();
                    s.flush().await.unwrap();
                });
            }
        });
        addr
    }

    /// A mock worker that returns a self-consistent but one-byte-short success
    /// frame, counting how many requests it saw.
    async fn spawn_short_reply_worker(count: Arc<std::sync::atomic::AtomicU32>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let count = Arc::clone(&count);
                tokio::spawn(async move {
                    let mut hdr = [0u8; HEADER_LEN];
                    if s.read_exact(&mut hdr).await.is_err() {
                        return;
                    }
                    let h = FrameHeader::decode(&hdr).unwrap();
                    let mut body = vec![0u8; h.length as usize];
                    s.read_exact(&mut body).await.unwrap();
                    let mut full = hdr.to_vec();
                    full.extend_from_slice(&body);
                    let req: RangeRequest = match h.msg_type {
                        MsgType::GetRange => decode_request(&full).unwrap().1,
                        MsgType::GetVersionedRange => {
                            let request =
                                talon_transport::decode_versioned_request(&full).unwrap().1;
                            assert_eq!(request.version, block().version);
                            request.request
                        }
                        MsgType::GetCachedRange => {
                            let request = decode_cached_request(&full).unwrap().1;
                            assert_eq!(request.version, block().version);
                            RangeRequest {
                                object: request.object,
                                offset: request.offset,
                                len: request.len,
                            }
                        }
                        other => panic!("unexpected read request: {other:?}"),
                    };
                    count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let short_len = req.len.saturating_sub(1) as usize;
                    let payload = vec![0u8; short_len];
                    let mut out = response_header_ok(0, short_len as u32).to_vec();
                    out.extend_from_slice(&payload);
                    s.write_all(&out).await.unwrap();
                    s.flush().await.unwrap();
                });
            }
        });
        addr
    }

    /// Advertise two workers while assigning `primary` to whichever stable ID
    /// ranks first for the test block.
    async fn mock_coordinator_two(primary: String, secondary: String) -> String {
        let candidates = vec![
            NodeInfo {
                id: NodeId::new("w1"),
                address: String::new(),
                role: NodeRole::Worker,
            },
            NodeInfo {
                id: NodeId::new("w2"),
                address: String::new(),
                role: NodeRole::Worker,
            },
        ];
        let first = talon_core::CachePlacementTable::new(&candidates)
            .primary(&block())
            .unwrap()
            .id
            .clone();
        let (w1, w2) = if first == NodeId::new("w1") {
            (primary, secondary)
        } else {
            (secondary, primary)
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let (w1, w2) = (w1.clone(), w2.clone());
                tokio::spawn(async move {
                    let mut hdr = [0u8; HEADER_LEN];
                    if s.read_exact(&mut hdr).await.is_err() {
                        return;
                    }
                    let h = FrameHeader::decode(&hdr).unwrap();
                    let mut body = vec![0u8; h.length as usize];
                    s.read_exact(&mut body).await.unwrap();
                    let mut full = hdr.to_vec();
                    full.extend_from_slice(&body);
                    let (_h, msg) = talon_transport::decode(&full).unwrap();
                    let reply = match msg {
                        ControlMessage::MembershipQuery {} => membership_fixture::zoned(
                            [w1.clone(), w2.clone()]
                                .into_iter()
                                .enumerate()
                                .map(|(i, address)| talon_transport::ZonedNodeInfo {
                                    info: NodeInfo {
                                        id: NodeId::new(format!("w{}", i + 1)),
                                        address,
                                        role: NodeRole::Worker,
                                    },
                                    zone: None,
                                })
                                .collect(),
                        ),
                        _ => ControlMessage::Ack {
                            ok: false,
                            detail: None,
                        },
                    };
                    let out = talon_transport::encode(0, &reply).unwrap();
                    s.write_all(&out).await.unwrap();
                    s.flush().await.unwrap();
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn stats_and_worker_pool_builders_are_independent() {
        for stats_first in [false, true] {
            let hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
            let worker_addr = mock_worker(hits).await;
            let coordinator = mock_coordinator(worker_addr.clone()).await;
            let stats = ReadStats::new();
            let pool = Arc::new(ConnectionPool::with_limits(
                1,
                crate::pool::DEFAULT_IDLE_TTL,
            ));
            let reader = BlockReader::new(
                CoordinatorClient::new(coordinator),
                Arc::new(PlacementCache::new(10_000)),
                1,
            );
            let reader = if stats_first {
                reader
                    .with_stats(stats.clone())
                    .with_worker_pool(Arc::clone(&pool))
            } else {
                reader
                    .with_worker_pool(Arc::clone(&pool))
                    .with_stats(stats.clone())
            };

            let bytes = reader.clone().read_block(&block(), 0, 64).await.unwrap();
            assert_eq!(bytes.len(), 64);
            assert_eq!(stats.snapshot().bytes_served, 64);
            assert_eq!(stats.snapshot().worker_fetches, 1);
            assert!(Arc::ptr_eq(&reader.worker_pool, &pool));
            assert_eq!(
                pool.idle_count(&worker_addr),
                0,
                "instance pools are isolated"
            );
        }
    }

    #[tokio::test]
    async fn repeated_reads_reuse_discovery_and_fetch_correct_range() {
        let hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let worker_addr = mock_worker(Arc::clone(&hits)).await;
        let coord_addr = mock_coordinator(worker_addr).await;

        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(coord_addr), Arc::clone(&cache), 1);

        let blk = block();
        // First read: discover membership, rank the owner, and fetch.
        let bytes = reader.read_block(&blk, 100, 64).await.unwrap();
        assert_eq!(bytes.len(), 64);
        let abs = blk.offset + 100;
        assert_eq!(bytes[0], (abs % 256) as u8);
        assert_eq!(bytes[1], ((abs + 1) % 256) as u8);
        assert!(cache.is_empty(), "reads rank logical IDs");
        let first = reader.membership.last_good().unwrap();

        // Second read: reuse logical membership and the instance connection.
        let _ = reader.read_block(&blk, 0, 16).await.unwrap();
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 2);

        assert!(Arc::ptr_eq(
            &first.placement,
            &reader.membership.last_good().unwrap().placement
        ));
        // Each read ranks the cached logical membership and attempts one worker.
        let snap = reader.stats().snapshot();
        assert_eq!(snap.cache_misses, 2);
        assert_eq!(snap.cache_hits, 0);
        assert_eq!(snap.worker_fetches, 2);
        assert_eq!(snap.worker_failures, 0);
        assert_eq!(snap.bytes_served, 64 + 16);
        assert_eq!(snap.hit_ratio(), 0.0);
    }

    /// A coordinator advertising two logical workers in different zones.
    async fn mock_zoned_coordinator(az_a: String, az_b: String) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let (az_a, az_b) = (az_a.clone(), az_b.clone());
                tokio::spawn(async move {
                    loop {
                        let mut hdr = [0u8; HEADER_LEN];
                        if s.read_exact(&mut hdr).await.is_err() {
                            return;
                        }
                        let h = FrameHeader::decode(&hdr).unwrap();
                        let mut body = vec![0u8; h.length as usize];
                        s.read_exact(&mut body).await.unwrap();
                        let reply = membership_fixture::zoned(vec![
                            talon_transport::ZonedNodeInfo {
                                info: NodeInfo {
                                    id: NodeId::new("w1"),
                                    address: az_a.clone(),
                                    role: NodeRole::Worker,
                                },
                                zone: Some("az-a".into()),
                            },
                            talon_transport::ZonedNodeInfo {
                                info: NodeInfo {
                                    id: NodeId::new("w2"),
                                    address: az_b.clone(),
                                    role: NodeRole::Worker,
                                },
                                zone: Some("az-b".into()),
                            },
                        ]);
                        s.write_all(&talon_transport::encode(0, &reply).unwrap())
                            .await
                            .unwrap();
                        s.flush().await.unwrap();
                    }
                });
            }
        });
        addr
    }

    #[derive(Default)]
    struct CountingZoneObserver {
        same: std::sync::atomic::AtomicU32,
        cross: std::sync::atomic::AtomicU32,
        unknown: std::sync::atomic::AtomicU32,
        fallbacks: std::sync::atomic::AtomicU32,
    }

    impl crate::metrics::ZoneReadObserver for CountingZoneObserver {
        fn worker_read(&self, matched: crate::metrics::ZoneMatch, _bytes: u64) {
            use std::sync::atomic::Ordering;
            match matched {
                crate::metrics::ZoneMatch::Same => self.same.fetch_add(1, Ordering::SeqCst),
                crate::metrics::ZoneMatch::Cross => self.cross.fetch_add(1, Ordering::SeqCst),
                crate::metrics::ZoneMatch::Unknown => self.unknown.fetch_add(1, Ordering::SeqCst),
            };
        }

        fn affinity_fallback(&self) {
            self.fallbacks
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// With affinity on, every block lands on the same-zone worker even when a
    /// worker in another zone would rank first globally; the observer counts
    /// only same-zone reads and no fallbacks.
    #[tokio::test]
    async fn zone_affinity_reads_stay_in_the_readers_zone() {
        let hits_a = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let hits_b = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let worker_a = mock_worker(Arc::clone(&hits_a)).await;
        let worker_b = mock_worker(Arc::clone(&hits_b)).await;
        let coordinator = mock_zoned_coordinator(worker_a, worker_b).await;

        let observer = Arc::new(CountingZoneObserver::default());
        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(coordinator), cache, 1)
            .with_zone_affinity(
                Some("az-a".into()),
                true,
                Arc::clone(&observer) as Arc<dyn crate::metrics::ZoneReadObserver>,
            );

        // Several distinct blocks: all owners must come from az-a.
        for i in 0..4u64 {
            let mut blk = block();
            blk.offset = i * u64::from(blk.block_size);
            reader.read_block(&blk, 0, 16).await.unwrap();
        }
        use std::sync::atomic::Ordering;
        assert!(hits_a.load(Ordering::SeqCst) >= 4);
        assert_eq!(hits_b.load(Ordering::SeqCst), 0);
        assert_eq!(observer.same.load(Ordering::SeqCst), 4);
        assert_eq!(observer.cross.load(Ordering::SeqCst), 0);
        assert_eq!(observer.fallbacks.load(Ordering::SeqCst), 0);
    }

    /// With affinity on but no same-zone worker, reads fall back to the full
    /// membership and the fallback is observed.
    #[tokio::test]
    async fn missing_local_zone_falls_back_to_full_membership() {
        let hits_a = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let hits_b = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let worker_a = mock_worker(Arc::clone(&hits_a)).await;
        let worker_b = mock_worker(Arc::clone(&hits_b)).await;
        let coordinator = mock_zoned_coordinator(worker_a, worker_b).await;

        let observer = Arc::new(CountingZoneObserver::default());
        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(coordinator), cache, 1)
            .with_zone_affinity(
                Some("az-c".into()),
                true,
                Arc::clone(&observer) as Arc<dyn crate::metrics::ZoneReadObserver>,
            );

        reader.read_block(&block(), 0, 16).await.unwrap();
        use std::sync::atomic::Ordering;
        assert_eq!(
            hits_a.load(Ordering::SeqCst) + hits_b.load(Ordering::SeqCst),
            1
        );
        assert_eq!(observer.fallbacks.load(Ordering::SeqCst), 1);
        assert_eq!(observer.cross.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn empty_cluster_yields_no_owners() {
        // Coordinator advertises an empty worker membership.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut hdr = [0u8; HEADER_LEN];
            s.read_exact(&mut hdr).await.unwrap();
            let h = FrameHeader::decode(&hdr).unwrap();
            let mut body = vec![0u8; h.length as usize];
            s.read_exact(&mut body).await.unwrap();
            let reply = membership_fixture::zoned(Vec::new());
            s.write_all(&talon_transport::encode(0, &reply).unwrap())
                .await
                .unwrap();
            s.flush().await.unwrap();
        });
        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(addr), cache, 1);
        let err = reader.read_block(&block(), 0, 16).await.unwrap_err();
        assert!(matches!(err, BlockReadError::NoOwners));
    }

    #[tokio::test]
    async fn coordinator_outage_does_not_use_expired_instances() {
        let hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let worker_addr = mock_worker(Arc::clone(&hits)).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let coordinator = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; HEADER_LEN];
            stream.read_exact(&mut header).await.unwrap();
            let header = FrameHeader::decode(&header).unwrap();
            let mut body = vec![0u8; header.length as usize];
            stream.read_exact(&mut body).await.unwrap();
            let reply = membership_fixture::zoned(vec![talon_transport::ZonedNodeInfo {
                info: NodeInfo {
                    id: NodeId::new("worker-a"),
                    address: worker_addr,
                    role: NodeRole::Worker,
                },
                zone: None,
            }]);
            stream
                .write_all(&talon_transport::encode(0, &reply).unwrap())
                .await
                .unwrap();
            stream.flush().await.unwrap();
            // Dropping the listener simulates the complete management-plane outage.
        });

        let cache = Arc::new(PlacementCache::new(500));
        let reader = BlockReader::new(CoordinatorClient::new(coordinator), cache, 1);
        reader.read_block(&block(), 0, 16).await.unwrap();
        let mut next = block();
        next.offset += u64::from(next.block_size);
        tokio::time::sleep(std::time::Duration::from_millis(510)).await;
        assert!(reader.read_block(&next, 0, 16).await.is_err());
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_block_failures_do_not_refresh_or_resend() {
        const BLOCKS: u32 = 4;
        let stale_requests = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let stale_worker = spawn_barrier_error_worker(
            Arc::clone(&stale_requests),
            Arc::new(Barrier::new(BLOCKS as usize)),
        )
        .await;
        let fresh_requests = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let fresh_worker = mock_worker(Arc::clone(&fresh_requests)).await;
        let membership_calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let coordinator =
            mock_switching_coordinator(stale_worker, fresh_worker, Arc::clone(&membership_calls))
                .await;
        let reader = BlockReader::new(
            CoordinatorClient::new(coordinator),
            Arc::new(PlacementCache::new(10_000)),
            1,
        );

        let reads = (0..BLOCKS).map(|index| {
            let reader = reader.clone();
            async move {
                let mut requested = block();
                requested.offset = u64::from(index) * u64::from(requested.block_size);
                reader.read_block(&requested, 0, 16).await
            }
        });
        for result in futures::future::join_all(reads).await {
            assert!(matches!(
                result,
                Err(BlockReadError::AllReplicasFailed { .. })
            ));
        }

        assert_eq!(
            stale_requests.load(std::sync::atomic::Ordering::SeqCst),
            BLOCKS,
            "every block must first observe the stale placement"
        );
        assert_eq!(
            fresh_requests.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "failed reads must not reach the replacement worker"
        );
        assert_eq!(
            membership_calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "only the initial discovery should be fetched"
        );
        assert_eq!(
            reader.stats().snapshot().coordinator_refreshes,
            0,
            "read errors do not trigger an in-request refresh"
        );
    }

    async fn assert_intermediate_refresh_does_not_suppress_next(fail_intermediate: bool) {
        let stale_requests = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let stale_worker = spawn_erroring_worker(Arc::clone(&stale_requests)).await;
        let recovered_requests = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let recovered_worker = mock_worker(Arc::clone(&recovered_requests)).await;
        let membership_calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let coordinator = mock_intermediate_then_switching_coordinator(
            stale_worker,
            recovered_worker,
            Arc::clone(&membership_calls),
            fail_intermediate,
        )
        .await;
        let reader = BlockReader::new(
            CoordinatorClient::new(coordinator),
            Arc::new(PlacementCache::new(100)),
            1,
        );
        let first = block();
        let mut second = block();
        second.offset += u64::from(second.block_size);

        reader.membership_snapshot().await.unwrap();
        assert!(reader.read_block(&first, 0, 16).await.is_err());
        tokio::time::sleep(std::time::Duration::from_millis(110)).await;
        let intermediate = reader.membership_snapshot().await;
        assert_eq!(intermediate.is_err(), fail_intermediate);
        assert_eq!(membership_calls.load(Ordering::SeqCst), 2);
        // A later refresh can recover after either an error or an unchanged view.
        tokio::time::sleep(std::time::Duration::from_millis(110)).await;
        reader.membership_snapshot().await.unwrap();
        let bytes = reader.read_block(&second, 0, 16).await.unwrap();
        assert_eq!(bytes.len(), 16);
        assert_eq!(membership_calls.load(Ordering::SeqCst), 3);
        assert_eq!(recovered_requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_refresh_does_not_suppress_the_next_sequential_refresh() {
        assert_intermediate_refresh_does_not_suppress_next(true).await;
    }

    #[tokio::test]
    async fn unchanged_refresh_does_not_suppress_the_next_sequential_refresh() {
        assert_intermediate_refresh_does_not_suppress_next(false).await;
    }

    #[tokio::test]
    async fn transport_failure_is_returned_without_refresh() {
        let hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let worker = spawn_worker_error(hits.clone(), vec![]).await;
        let coordinator = mock_coordinator(worker).await;
        let reader = BlockReader::new(
            CoordinatorClient::new(coordinator),
            Arc::new(PlacementCache::new(10_000)),
            1,
        );
        assert!(matches!(
            reader.read_block(&block(), 0, 16).await,
            Err(BlockReadError::AllReplicasFailed {
                source: WorkerError::Io(_),
                ..
            })
        ));
        assert!(matches!(
            reader.read_block_into(&block(), 0, &mut [0; 16]).await,
            Err(BlockReadError::AllReplicasFailed {
                source: WorkerError::Io(_),
                ..
            })
        ));
        assert_eq!(
            hits.load(Ordering::SeqCst),
            2,
            "one attempt per caller read"
        );
        assert_eq!(reader.stats().snapshot().coordinator_refreshes, 0);
    }

    #[tokio::test]
    async fn exhausted_reads_preserve_last_candidate_and_source() {
        use std::error::Error as _;
        for k in [1, 2, 3] {
            for api in [
                ReadApi::Allocated,
                ReadApi::Into,
                ReadApi::VersionedInto,
                ReadApi::Detailed,
            ] {
                let first_hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
                let last_hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
                let primary = spawn_worker_error(
                    first_hits.clone(),
                    encode_typed_error(0, DataErrorCode::Unavailable, "primary failure"),
                )
                .await;
                let secondary = spawn_worker_error(
                    last_hits.clone(),
                    encode_typed_error(0, DataErrorCode::Timeout, "last candidate failure"),
                )
                .await;
                let coordinator = mock_coordinator_two(primary.clone(), secondary.clone()).await;
                let reader = BlockReader::new(
                    CoordinatorClient::new(coordinator),
                    Arc::new(PlacementCache::new(500)),
                    k,
                );
                let mut dst = [0; 32];
                let error = match api {
                    ReadApi::Allocated => reader.read_block(&block(), 7, 32).await.unwrap_err(),
                    ReadApi::Into => reader
                        .read_block_into(&block(), 7, &mut dst)
                        .await
                        .unwrap_err(),
                    ReadApi::VersionedInto => reader
                        .read_versioned_block_into(&block(), 7, &mut dst)
                        .await
                        .unwrap_err(),
                    ReadApi::Detailed => match reader.read_block_detailed(&block(), 7, 32).await {
                        Err(DetailedBlockReadError::Block(error)) => error,
                        _ => panic!("exhausted detailed read must preserve the aggregate error"),
                    },
                    ReadApi::Cached => unreachable!(),
                };
                let (expected_worker, expected_code, expected_message) = if k == 1 {
                    (&primary, DataErrorCode::Unavailable, "primary failure")
                } else {
                    (&secondary, DataErrorCode::Timeout, "last candidate failure")
                };
                assert!(matches!(&error, BlockReadError::AllReplicasFailed {
                    worker, worker_id, instance_id, source: WorkerError::Remote(remote),
                } if worker_id == if k == 1 { "w1" } else { "w2" } && instance_id.is_some() && worker == expected_worker && remote.code == expected_code && remote.message == expected_message));
                assert!(error
                    .source()
                    .unwrap()
                    .downcast_ref::<WorkerError>()
                    .is_some());
                assert!(error.to_string().contains(expected_worker));
                assert_eq!(first_hits.load(Ordering::SeqCst), 1);
                assert_eq!(last_hits.load(Ordering::SeqCst), u32::from(k > 1));
            }
        }
    }

    #[tokio::test]
    async fn worker_diagnostics_survive_both_read_apis() {
        use std::sync::atomic::{AtomicU32, Ordering};

        for code in [DataErrorCode::Timeout, DataErrorCode::Origin] {
            for into in [false, true] {
                let hits = Arc::new(AtomicU32::new(0));
                let worker = spawn_worker_error(
                    Arc::clone(&hits),
                    encode_typed_error(0, code, "backend request deadline exceeded"),
                )
                .await;
                let coord = mock_coordinator(worker.clone()).await;
                let reader = BlockReader::new(
                    CoordinatorClient::new(coord),
                    Arc::new(PlacementCache::new(10_000)),
                    1,
                );
                let error = if into {
                    reader
                        .read_block_into(&block(), 0, &mut [0; 16])
                        .await
                        .unwrap_err()
                } else {
                    reader.read_block(&block(), 0, 16).await.unwrap_err()
                };
                let message = error.to_string();
                assert!(
                    message.contains("backend request deadline exceeded"),
                    "{message}"
                );
                assert!(message.contains(&format!("{code:?}")), "{message}");
                assert!(!message.contains("after refresh"), "{message}");
                if code == DataErrorCode::Timeout {
                    assert!(matches!(error, BlockReadError::AllReplicasFailed {
                        worker: ref last_worker,
                        source: WorkerError::Remote(ref remote), ..
                    } if last_worker == &worker && remote.code == code));
                } else {
                    assert!(
                        matches!(error, BlockReadError::Target { source: WorkerError::Remote(ref remote), .. }
                        if remote.code == code)
                    );
                }
                assert_eq!(hits.load(Ordering::SeqCst), 1);
            }
        }
    }

    #[tokio::test]
    async fn terminal_replica_failure_reports_the_actual_candidate() {
        for into in [false, true] {
            let primary = spawn_worker_error(
                Arc::new(std::sync::atomic::AtomicU32::new(0)),
                encode_typed_error(0, DataErrorCode::Unavailable, "primary unavailable"),
            )
            .await;
            let secondary = spawn_worker_error(
                Arc::new(std::sync::atomic::AtomicU32::new(0)),
                encode_typed_error(0, DataErrorCode::Origin, "secondary origin failure"),
            )
            .await;
            let coordinator = mock_coordinator_two(primary, secondary.clone()).await;
            let reader = BlockReader::new(
                CoordinatorClient::new(coordinator),
                Arc::new(PlacementCache::new(500)),
                2,
            );
            let error = if into {
                reader
                    .read_block_into(&block(), 0, &mut [0; 8])
                    .await
                    .unwrap_err()
            } else {
                reader.read_block(&block(), 0, 8).await.unwrap_err()
            };
            assert!(matches!(error, BlockReadError::Target {
                worker_id, address, source: WorkerError::Remote(remote), ..
            } if worker_id == "w2" && address == secondary && remote.code == DataErrorCode::Origin));
        }
    }

    #[tokio::test]
    async fn single_candidate_does_not_fall_back_to_second_replica() {
        // Primary w1 always errors "not present"; secondary w2 serves the bytes.
        // The reader must preserve the primary failure without contacting w2.
        let bad = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let good = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let w1 = spawn_erroring_worker(Arc::clone(&bad)).await;
        let w2 = mock_worker(Arc::clone(&good)).await;
        let coord = mock_coordinator_two(w1, w2).await;
        let cache = Arc::new(PlacementCache::new(10_000));
        // k=1 must not reach a candidate outside the configured list.
        let reader = BlockReader::new(CoordinatorClient::new(coord), Arc::clone(&cache), 1);

        assert!(reader.read_block(&block(), 0, 32).await.is_err());
        assert_eq!(
            bad.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "primary tried"
        );
        assert_eq!(
            good.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "must not reroute to w2"
        );
        // No address-based placement is cached.
        assert!(cache.is_empty());
    }

    #[tokio::test]
    async fn single_candidate_returns_short_reply_error() {
        let short = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let good = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let w1 = spawn_short_reply_worker(Arc::clone(&short)).await;
        let w2 = mock_worker(Arc::clone(&good)).await;
        let coord = mock_coordinator_two(w1, w2).await;
        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(coord), Arc::clone(&cache), 1);

        assert!(reader.read_block(&block(), 0, 32).await.is_err());
        assert_eq!(
            short.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "short primary reply was rejected"
        );
        assert_eq!(
            good.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "must not reroute after an incomplete response"
        );
        assert!(cache.is_empty());
        let stats = reader.stats().snapshot();
        assert_eq!(stats.worker_fetches, 1);
        assert_eq!(stats.worker_failures, 1);
        assert_eq!(stats.coordinator_refreshes, 0);
        assert_eq!(stats.bytes_served, 0);
    }

    #[derive(Clone, Copy, Debug)]
    enum ReadApi {
        Allocated,
        Into,
        VersionedInto,
        Detailed,
        Cached,
    }

    const READ_APIS: [ReadApi; 5] = [
        ReadApi::Allocated,
        ReadApi::Into,
        ReadApi::VersionedInto,
        ReadApi::Detailed,
        ReadApi::Cached,
    ];

    async fn read_via(reader: &BlockReader, api: ReadApi) -> Result<Vec<u8>, CacheReadError> {
        match api {
            ReadApi::Allocated => reader
                .read_block(&block(), 7, 32)
                .await
                .map_err(cache_block_error),
            ReadApi::Into | ReadApi::VersionedInto => {
                let mut dst = vec![42; 32];
                let n = if matches!(api, ReadApi::VersionedInto) {
                    reader
                        .read_versioned_block_into(&block(), 7, &mut dst)
                        .await
                } else {
                    reader.read_block_into(&block(), 7, &mut dst).await
                }
                .map_err(cache_block_error)?;
                assert_eq!(n, dst.len());
                Ok(dst)
            }
            ReadApi::Detailed => reader
                .read_block_detailed(&block(), 7, 32)
                .await
                .map_err(Into::into),
            ReadApi::Cached => reader.read_cached_block(&block(), 7, 32).await,
        }
    }

    #[tokio::test]
    async fn ranked_replica_fallback_preserves_range_and_bounds_attempts() {
        for api in READ_APIS {
            for response in [
                Vec::new(), // EOF after the request, with no complete response.
                encode_typed_error(0, DataErrorCode::Unavailable, "restarting"),
                encode_typed_error(0, DataErrorCode::CacheMiss, "not resident"),
                response_header_ok(0, 0).to_vec(), // Incomplete range.
            ] {
                for k in [0, 1, 2, 3] {
                    let bad = Arc::new(std::sync::atomic::AtomicU32::new(0));
                    let good = Arc::new(std::sync::atomic::AtomicU32::new(0));
                    let primary = spawn_worker_error(bad.clone(), response.clone()).await;
                    let secondary = mock_worker(good.clone()).await;
                    let coordinator = mock_coordinator_two(primary, secondary).await;
                    let reader = BlockReader::new(
                        CoordinatorClient::new(coordinator),
                        Arc::new(PlacementCache::new(500)),
                        k,
                    );
                    let result = read_via(&reader, api).await;
                    if k >= 2 {
                        assert_eq!(result.unwrap(), (7..39).collect::<Vec<u8>>(), "{api:?}");
                    } else {
                        assert!(result.is_err(), "{api:?}, k={k}");
                    }
                    assert_eq!(bad.load(Ordering::SeqCst), 1, "one primary attempt");
                    assert_eq!(good.load(Ordering::SeqCst), u32::from(k >= 2));
                    assert_eq!(reader.stats().snapshot().coordinator_refreshes, 0);
                }
            }
        }
    }

    #[tokio::test]
    async fn replica_fallback_preserves_terminal_errors_and_last_failure() {
        for api in READ_APIS {
            for code in [
                DataErrorCode::InvalidRequest,
                DataErrorCode::NotFound,
                DataErrorCode::VersionMismatch,
                DataErrorCode::Origin,
                DataErrorCode::RateLimited,
                DataErrorCode::Unavailable,
            ] {
                let first = Arc::new(std::sync::atomic::AtomicU32::new(0));
                let second = Arc::new(std::sync::atomic::AtomicU32::new(0));
                let primary = spawn_worker_error(
                    first.clone(),
                    encode_typed_error(0, code, "primary failure"),
                )
                .await;
                let secondary = spawn_worker_error(
                    second.clone(),
                    encode_typed_error(0, DataErrorCode::Timeout, "secondary failure"),
                )
                .await;
                let coordinator = mock_coordinator_two(primary, secondary).await;
                let reader = BlockReader::new(
                    CoordinatorClient::new(coordinator),
                    Arc::new(PlacementCache::new(500)),
                    3,
                );
                let error = read_via(&reader, api).await.unwrap_err();
                let fallback = code == DataErrorCode::Unavailable;
                match code {
                    DataErrorCode::InvalidRequest => {
                        assert!(matches!(error, CacheReadError::InvalidRequest(_)))
                    }
                    DataErrorCode::NotFound => {
                        assert!(matches!(error, CacheReadError::NotFound(_)))
                    }
                    DataErrorCode::VersionMismatch => {
                        assert!(matches!(error, CacheReadError::VersionMismatch(_)))
                    }
                    DataErrorCode::Origin => assert!(matches!(error, CacheReadError::Origin(_))),
                    DataErrorCode::RateLimited => {
                        assert!(matches!(error, CacheReadError::RateLimited(_)))
                    }
                    DataErrorCode::Unavailable => {
                        assert!(matches!(error, CacheReadError::Timeout(_)))
                    }
                    _ => unreachable!(),
                }
                assert!(error.to_string().contains(if fallback {
                    "secondary failure"
                } else {
                    "primary failure"
                }));
                assert_eq!(first.load(Ordering::SeqCst), 1);
                assert_eq!(second.load(Ordering::SeqCst), u32::from(fallback));
            }
        }
    }

    #[tokio::test]
    async fn offline_and_conflicted_candidates_keep_rank_but_allow_replica_reads() {
        use talon_core::worker_membership::InstanceState;
        for api in READ_APIS {
            for state in [InstanceState::Offline, InstanceState::Conflict] {
                let hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
                let secondary = mock_worker(hits.clone()).await;
                let coordinator = mock_coordinator_two("127.0.0.1:1".into(), secondary).await;
                let reader = BlockReader::new(
                    CoordinatorClient::new(coordinator),
                    Arc::new(PlacementCache::new(500)),
                    2,
                );
                let before = reader.membership_snapshot().await.unwrap();
                let primary_id = before.placement.primary(&block()).unwrap().id.0.clone();
                let mut view = reader.coordinator.discovery().await.unwrap();
                view.workers
                    .iter_mut()
                    .find(|w| w.member.worker_id == primary_id)
                    .unwrap()
                    .state = state;
                let after =
                    reader
                        .membership
                        .replace(view, std::time::Instant::now(), &reader.worker_pool);
                assert!(Arc::ptr_eq(&before.placement, &after.placement));
                assert_eq!(after.placement.primary(&block()).unwrap().id.0, primary_id);
                assert_eq!(
                    read_via(&reader, api).await.unwrap(),
                    (7..39).collect::<Vec<u8>>()
                );
                assert_eq!(hits.load(Ordering::SeqCst), 1);
                // Admission still targets only the primary; it must not silently move data.
                assert!(reader
                    .admit_block(&block(), block().offset + 32, &[0; 32])
                    .await
                    .is_err());
                assert_eq!(hits.load(Ordering::SeqCst), 1);
            }
        }
    }

    #[tokio::test]
    async fn discovery_expiring_during_reused_read_prevents_redial() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            for n in 0..2 {
                let mut header = [0; HEADER_LEN];
                socket.read_exact(&mut header).await.unwrap();
                let header = FrameHeader::decode(&header).unwrap();
                let mut body = vec![0; header.length as usize];
                socket.read_exact(&mut body).await.unwrap();
                if n == 0 {
                    let mut response = response_header_ok(header.request_id, 8).to_vec();
                    response.extend_from_slice(&[7; 8]);
                    socket.write_all(&response).await.unwrap();
                } else {
                    tokio::time::sleep(std::time::Duration::from_millis(510)).await;
                }
            }
            drop(socket);
            // Keep listening so a forbidden retry would connect and send bytes.
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(50), listener.accept())
                    .await
                    .is_err()
            );
        });
        let coordinator = mock_coordinator(address).await;
        let reader = BlockReader::new(
            CoordinatorClient::new(coordinator),
            Arc::new(PlacementCache::new(500)),
            1,
        );
        assert_eq!(reader.read_block(&block(), 0, 8).await.unwrap(), vec![7; 8]);
        let error = reader.read_block(&block(), 0, 8).await.unwrap_err();
        assert!(matches!(error, BlockReadError::AllReplicasFailed {
            source: WorkerError::Remote(ref remote), ..
        } if remote.code == DataErrorCode::Unavailable && remote.message.contains("expired before read retry")));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn discovery_expiring_during_primary_read_prevents_replica_attempt() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header = [0; HEADER_LEN];
            socket.read_exact(&mut header).await.unwrap();
            let header = FrameHeader::decode(&header).unwrap();
            let mut body = vec![0; header.length as usize];
            socket.read_exact(&mut body).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(510)).await;
            socket
                .write_all(&encode_typed_error(
                    0,
                    DataErrorCode::Unavailable,
                    "draining",
                ))
                .await
                .unwrap();
        });
        let hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let secondary = mock_worker(hits.clone()).await;
        let coordinator = mock_coordinator_two(primary, secondary).await;
        let reader = BlockReader::new(
            CoordinatorClient::new(coordinator),
            Arc::new(PlacementCache::new(500)),
            2,
        );
        let error = read_via(&reader, ReadApi::Into).await.unwrap_err();
        assert!(matches!(error, CacheReadError::Unavailable(_)));
        assert!(error.to_string().contains("expired instance discovery"));
        assert_eq!(hits.load(Ordering::SeqCst), 0);
        assert_eq!(reader.stats().snapshot().worker_fetches, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn observe_epoch_invalidates_stale_entry() {
        let hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let worker_addr = mock_worker(Arc::clone(&hits)).await;
        let coord_addr = mock_coordinator(worker_addr).await;
        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(coord_addr), Arc::clone(&cache), 1);

        reader.read_block(&block(), 0, 8).await.unwrap();
        let epoch = reader.membership.last_good().unwrap().epoch;
        // The public epoch hook still invalidates externally supplied entries.
        cache.insert(
            block(),
            Cached {
                replicas: vec!["old:7001".into()],
                epoch,
            },
            0,
        );
        // The identical version token does not invalidate.
        assert!(!reader.observe_epoch(&block(), epoch));
        assert_eq!(cache.len(), 1);
        // A different token drops the entry so the next read re-looks-up.
        assert!(reader.observe_epoch(&block(), epoch.wrapping_add(1)));
        assert_eq!(cache.len(), 0);
    }

    #[tokio::test]
    async fn multi_block_read_stitches_in_order() {
        // Small block size so a modest read spans several blocks.
        let hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let worker_addr = mock_worker(Arc::clone(&hits)).await;
        let coord_addr = mock_coordinator(worker_addr).await;
        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(coord_addr), Arc::clone(&cache), 1);

        let obj = ObjectId::new(Backend::S3, "b", "o/1");
        let ver = Version::new("v1");
        let bs = 1024u32;
        let size = 100_000u64;
        // Read 900..900+2300 → spans 4 blocks (tail, full, full, head).
        let offset = 900u64;
        let len = 2300u64;
        let file = FileView {
            object: &obj,
            block_size: bs,
            version: &ver,
            size,
        };
        let bytes = reader.read(&file, offset, len).await.unwrap();
        assert_eq!(bytes.len() as u64, len);
        // The mock worker fills each byte with (absolute_offset % 256); the
        // stitched buffer must be contiguous across block boundaries.
        for (i, b) in bytes.iter().enumerate() {
            assert_eq!(*b, ((offset + i as u64) % 256) as u8, "byte {i} mismatch");
        }
        // Four distinct blocks were fetched (one worker call each).
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 4);
        assert!(cache.is_empty(), "blocks share one logical placement table");
    }

    #[tokio::test]
    async fn multi_block_read_into_stitches_in_caller_buffer() {
        let hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let worker_addr = mock_worker(Arc::clone(&hits)).await;
        let coord_addr = mock_coordinator(worker_addr).await;
        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(coord_addr), Arc::clone(&cache), 1);

        let object = ObjectId::new(Backend::S3, "b", "o/1");
        let version = Version::new("v1");
        let offset = 900u64;
        let mut dst = vec![0u8; 2300];
        let file = FileView {
            object: &object,
            block_size: 1024,
            version: &version,
            size: 100_000,
        };

        let n = reader.read_into(&file, offset, &mut dst).await.unwrap();
        assert_eq!(n, dst.len());
        for (index, byte) in dst.iter().enumerate() {
            assert_eq!(*byte, ((offset + index as u64) % 256) as u8);
        }
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 4);
        assert!(cache.is_empty());
    }

    #[tokio::test]
    async fn read_past_eof_is_empty_without_fetch() {
        let hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let worker_addr = mock_worker(Arc::clone(&hits)).await;
        let coord_addr = mock_coordinator(worker_addr).await;
        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(coord_addr), cache, 1);

        let obj = ObjectId::new(Backend::S3, "b", "o/1");
        let ver = Version::new("v1");
        let file = FileView {
            object: &obj,
            block_size: 1024,
            version: &ver,
            size: 1500,
        };
        let bytes = reader.read(&file, 5000, 10).await.unwrap();
        assert!(bytes.is_empty());
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn read_into_past_eof_is_empty_without_fetch() {
        let hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let worker_addr = mock_worker(Arc::clone(&hits)).await;
        let coord_addr = mock_coordinator(worker_addr).await;
        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(coord_addr), cache, 1);
        let object = ObjectId::new(Backend::S3, "b", "o/1");
        let version = Version::new("v1");
        let file = FileView {
            object: &object,
            block_size: 1024,
            version: &version,
            size: 1500,
        };
        let mut dst = vec![7u8; 10];

        let n = reader.read_into(&file, 5000, &mut dst).await.unwrap();
        assert_eq!(n, 0);
        assert_eq!(dst, vec![7u8; 10]);
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cached_block_read_uses_fail_closed_wire_operation_and_typed_miss() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let worker = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header = [0_u8; HEADER_LEN];
            socket.read_exact(&mut header).await.unwrap();
            let frame = FrameHeader::decode(&header).unwrap();
            assert_eq!(frame.msg_type, MsgType::GetCachedRange);
            let mut body = vec![0_u8; frame.length as usize];
            socket.read_exact(&mut body).await.unwrap();
            let mut encoded = header.to_vec();
            encoded.extend_from_slice(&body);
            let (_, request) = decode_cached_request(&encoded).unwrap();
            assert_eq!(request.version, Version::new("v1"));
            assert_eq!((request.offset, request.len), (block().offset + 7, 11));
            socket
                .write_all(&encode_typed_error(
                    frame.request_id,
                    DataErrorCode::CacheMiss,
                    "block is not resident",
                ))
                .await
                .unwrap();
        });
        let coordinator = mock_coordinator(worker).await;
        let reader = BlockReader::new(
            CoordinatorClient::new(coordinator),
            Arc::new(PlacementCache::new(10_000)),
            1,
        );

        let error = reader.read_cached_block(&block(), 7, 11).await.unwrap_err();
        assert!(matches!(error, CacheReadError::CacheMiss(_)));
    }

    #[tokio::test]
    async fn block_admission_resolves_primary_and_sends_exact_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let worker = listener.local_addr().unwrap().to_string();
        let expected = block();
        let expected_on_worker = expected.clone();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header = [0_u8; HEADER_LEN];
            socket.read_exact(&mut header).await.unwrap();
            let frame = FrameHeader::decode(&header).unwrap();
            assert_eq!(frame.msg_type, MsgType::AdmitCachedBlock);
            let mut payload = vec![0_u8; frame.length as usize];
            socket.read_exact(&mut payload).await.unwrap();
            let mut encoded = header.to_vec();
            encoded.extend_from_slice(&payload);
            let (_, request) = decode_cached_block_put_header(&encoded).unwrap();
            assert_eq!(request.block, expected_on_worker);
            assert_eq!(request.object_len, expected_on_worker.offset + 5);
            let mut body = vec![0_u8; request.body_len as usize];
            socket.read_exact(&mut body).await.unwrap();
            assert_eq!(body, b"tail!");
            socket
                .write_all(&response_header_ok(frame.request_id, 0))
                .await
                .unwrap();
        });
        let coordinator = mock_coordinator(worker).await;
        let reader = BlockReader::new(
            CoordinatorClient::new(coordinator),
            Arc::new(PlacementCache::new(10_000)),
            1,
        );

        reader
            .admit_block(&expected, expected.offset + 5, b"tail!")
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn all_offline_candidates_return_unavailable() {
        use talon_core::worker_membership::*;
        let reader = BlockReader::new(
            CoordinatorClient::new("127.0.0.1:1"),
            Arc::new(PlacementCache::new(30_000)),
            2,
        );
        let view = WorkerDiscovery {
            topology_token: 9,
            state_token: 1,
            valid_for_ms: 500,
            workers: vec![
                DiscoveredWorker {
                    member: WorkerMember {
                        worker_id: "a".into(),
                        zone: None,
                        retired: false,
                    },
                    state: InstanceState::Offline,
                },
                DiscoveredWorker {
                    member: WorkerMember {
                        worker_id: "b".into(),
                        zone: None,
                        retired: false,
                    },
                    state: InstanceState::Offline,
                },
            ],
        };
        let snapshot =
            reader
                .membership
                .replace(view, std::time::Instant::now(), &reader.worker_pool);
        let last_id = snapshot
            .placement
            .rank(&block(), 2)
            .last()
            .unwrap()
            .id
            .0
            .clone();
        let error = reader.read_block(&block(), 0, 4).await.unwrap_err();
        assert!(matches!(
            error,
            BlockReadError::AllReplicasFailed {
                source: WorkerError::Remote(talon_transport::DataPlaneError {
                    code: DataErrorCode::Unavailable,
                    ..
                }),
                worker, ..
            } if worker == last_id
        ));
        let mut dst = [42; 4];
        assert!(reader.read_block_into(&block(), 0, &mut dst).await.is_err());
        assert_eq!(dst, [42; 4]);
        assert!(matches!(
            reader.read_cached_block(&block(), 0, 4).await,
            Err(CacheReadError::Unavailable(_))
        ));
    }
    #[tokio::test]
    async fn stream_preserves_domain_and_protocol_failures() {
        use futures::StreamExt;
        use talon_core::worker_membership::*;
        for code in [
            Some(DataErrorCode::VersionMismatch),
            Some(DataErrorCode::RateLimited),
            Some(DataErrorCode::Origin),
            None,
        ] {
            let hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
            let response = code.map_or_else(
                || vec![0; HEADER_LEN],
                |c| encode_typed_error(0, c, "original cause"),
            );
            let address = spawn_worker_error(hits.clone(), response).await;
            let reader = BlockReader::new(
                CoordinatorClient::new("127.0.0.1:1"),
                Arc::new(PlacementCache::new(30_000)),
                1,
            );
            reader.membership.replace(
                WorkerDiscovery {
                    topology_token: 1,
                    state_token: 1,
                    valid_for_ms: 500,
                    workers: vec![DiscoveredWorker {
                        member: WorkerMember {
                            worker_id: "w1".into(),
                            zone: None,
                            retired: false,
                        },
                        state: InstanceState::Serving {
                            instance_id: "instance".into(),
                            address,
                        },
                    }],
                },
                std::time::Instant::now(),
                &reader.worker_pool,
            );
            let block = block();
            let file = FileView {
                object: &block.object,
                version: &block.version,
                block_size: block.block_size,
                size: 16,
            };
            let mut stream = reader.stream_range(&file, 0, 4, 4).unwrap();
            let error = stream.next().await.unwrap().unwrap_err();
            assert!(!error.fallback_eligible(), "{error}");
            match code {
                Some(DataErrorCode::VersionMismatch) => {
                    assert!(matches!(error, CacheReadError::VersionMismatch(_)))
                }
                Some(DataErrorCode::RateLimited) => {
                    assert!(matches!(error, CacheReadError::RateLimited(_)))
                }
                Some(DataErrorCode::Origin) => assert!(matches!(error, CacheReadError::Origin(_))),
                None => assert!(matches!(error, CacheReadError::Protocol(_))),
                _ => unreachable!(),
            }
            assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert!(stream.next().await.is_none());
        }
    }
}
