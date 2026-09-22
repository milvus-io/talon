//! Block read orchestration: local placement cache -> membership -> worker.
//!
//! [`BlockReader`] is the heart of the FUSE read path. Given a [`BlockId`] and a
//! sub-range within it, it answers "where does this block live?" from the
//! client-side [`PlacementCache`] when warm. On a miss it ranks a cached worker
//! membership snapshot locally, then fetches the bytes from the owning worker
//! with a [`WorkerClient`].
//!
//! # Cached addresses, not node ids
//!
//! The placement cache stores an ordered list of **worker addresses** (what the
//! client actually dials), derived from the same membership snapshot used for
//! local Maglev lookup. Storing the dialable address keeps the hot path
//! allocation-light and makes replica fallback a simple walk down the ordered
//! list.
//!
//! On a fetch failure the reader walks the cached replicas in order; if all are
//! exhausted it invalidates the entry and attempts one membership refresh
//! before giving up. A failed refresh keeps using the last-good snapshot.
//! [`BlockReader::observe_epoch`] reconciles the cache when a different
//! membership token is seen. Multi-block splitting is handled by
//! [`crate::read_plan`]; protocol frontends own their prefetch policy.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use talon_core::{BlockId, ObjectId, Version};

use crate::coordinator_client::{CoordinatorClient, CoordinatorError};
use crate::membership_cache::{MembershipCache, MembershipSnapshot};
use crate::metrics::{ReadStats, ZoneMatch, ZoneReadObserver};
use crate::placement_cache::{Cached, PlacementCache, RefreshReason};
use crate::pool::ConnectionPool;
use crate::range_stream::CacheReadError;
use crate::read_plan::plan_read;
use crate::worker_client::{WorkerClient, WorkerError};

pub(crate) enum DetailedBlockReadError {
    Block(BlockReadError),
    Worker(WorkerError),
}

impl From<BlockReadError> for DetailedBlockReadError {
    fn from(error: BlockReadError) -> Self {
        Self::Block(error)
    }
}

struct ReplicaFailure {
    worker: String,
    reason: RefreshReason,
    error: WorkerError,
    retryable: bool,
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
    /// Membership refresh failed before any snapshot was cached.
    #[error(transparent)]
    Coordinator(#[from] CoordinatorError),
    /// The worker fetch failed.
    #[error(transparent)]
    Worker(#[from] WorkerError),
    /// The cluster returned no owners for the block (empty cluster).
    #[error("no owners for block")]
    NoOwners,
    /// An owner id had no resolvable worker address in membership.
    #[error("owner has no known worker address")]
    UnresolvedOwner,
    /// Every replica failed, including after a membership refresh.
    #[error("all replicas failed after refresh; last worker {worker}: {source}")]
    AllReplicasFailed {
        /// Address of the last worker attempted.
        worker: String,
        /// The last worker failure, preserved for diagnostics and classification.
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
    /// Number of workers retained from local ranking (RF=1 -> 1 in v1).
    replicas_k: u8,
    /// Read-path counters (cache hit/miss, worker fetches, bytes served).
    stats: ReadStats,
    /// Shared pool of worker connections, so warm fetches skip the TCP handshake
    /// (issue #181).
    worker_pool: Arc<ConnectionPool>,
    /// Last authoritative worker set; stale snapshots remain usable on outage.
    membership: Arc<MembershipCache>,
    /// Serializes cold refreshes so an expired snapshot causes one control request.
    membership_refresh: Arc<tokio::sync::Mutex<()>>,
    /// Completed refresh attempts, sampled separately by each logical read.
    membership_refresh_generation: Arc<AtomicU64>,
    /// This reader's own deployment zone, for read classification (ADR 0006).
    zone: Option<String>,
    /// Sink for zone-classified read events; defaults to a no-op.
    zone_observer: Arc<dyn ZoneReadObserver>,
}

impl BlockReader {
    /// Keep caches, refresh coordination and metrics, but use local connections.
    pub fn with_sharded_connections(&self, coordinator: CoordinatorClient) -> Self {
        Self {
            coordinator,
            worker_pool: Arc::new(self.worker_pool.shard()),
            ..self.clone()
        }
    }

    /// Create a reader over the given coordinator client and placement cache.
    ///
    /// `replicas_k` is how many owners to retain from local placement; with RF=1
    /// this is `1`, but a larger value reserves an ordered fallback list.
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
            membership_refresh: Arc::new(tokio::sync::Mutex::new(())),
            membership_refresh_generation: Arc::new(AtomicU64::new(0)),
            zone: None,
            zone_observer: Arc::new(crate::metrics::NoopZoneReadObserver),
        }
    }

    /// Record metrics into the provided counters, shared by reader clones.
    pub fn with_stats(mut self, stats: ReadStats) -> Self {
        self.stats = stats;
        self
    }

    /// Use the provided worker connection pool, shared by reader clones.
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
        self.membership_refresh = Arc::new(tokio::sync::Mutex::new(()));
        self.membership_refresh_generation = Arc::new(AtomicU64::new(0));
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
        now_ms: u64,
    ) -> Result<Vec<u8>, CacheReadError> {
        let placement = match self.cache.get(block, now_ms) {
            Some(cached) => cached,
            None => self
                .resolve_and_cache(block, now_ms)
                .await
                .map_err(cache_block_error)?,
        };
        let offset = block.offset + u64::from(offset_in_block);
        let mut last_error = None;
        for address in &placement.replicas {
            let worker = WorkerClient::with_pool(address.clone(), Arc::clone(&self.worker_pool));
            match worker
                .fetch_cached_range(&block.object, &block.version, offset, u64::from(len))
                .await
            {
                Ok(bytes) if bytes.len() == len as usize => {
                    self.record_zone_read(address, bytes.len() as u64);
                    return Ok(bytes);
                }
                Ok(bytes) => {
                    last_error = Some(CacheReadError::Protocol(
                        WorkerError::RangeLengthMismatch {
                            expected: u64::from(len),
                            actual: bytes.len() as u64,
                        }
                        .to_string(),
                    ));
                }
                Err(error) => last_error = Some(error.into()),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            CacheReadError::Unavailable("placement contained no worker addresses".into())
        }))
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
        now_ms: u64,
    ) -> Result<(), CacheReadError> {
        let placement = match self.cache.get(block, now_ms) {
            Some(cached) => cached,
            None => self
                .resolve_and_cache(block, now_ms)
                .await
                .map_err(cache_block_error)?,
        };
        let address = placement
            .replicas
            .first()
            .ok_or_else(|| CacheReadError::Unavailable("block has no primary owner".into()))?;
        WorkerClient::with_pool(address.clone(), Arc::clone(&self.worker_pool))
            .admit_cached_block(block, object_len, body)
            .await
            .map_err(Into::into)
    }

    /// Read `len` bytes at `offset_in_block` within `block`.
    ///
    /// Resolves placement (cache hit, else local Maglev lookup),
    /// then fetches the sub-range from an owner. The
    /// absolute object offset handed to the worker is
    /// `block.offset + offset_in_block`.
    ///
    /// # Replica fallback & refresh
    ///
    /// A worker that is unreachable ([`WorkerError::Io`], a
    /// [`RefreshReason::ConnectFailure`]) or that no longer holds the block
    /// ([`WorkerError::Remote`], a [`RefreshReason::WrongOwner`]) does not fail
    /// the read outright: the reader walks the ordered replica list from the
    /// cached placement. If every cached replica is exhausted, it invalidates
    /// the entry and performs **one** membership refresh (which may return a
    /// different worker set), then retries against the fresh primary.
    /// Only if that also fails does the error propagate.
    ///
    /// This legacy/FUSE entry point follows the current origin generation. Use
    /// [`read_versioned_block_into`](Self::read_versioned_block_into) to pin the source.
    pub async fn read_block(
        &self,
        block: &BlockId,
        offset_in_block: u32,
        len: u32,
        now_ms: u64,
    ) -> Result<Vec<u8>, BlockReadError> {
        match self
            .read_block_detailed_with_mode(
                block,
                offset_in_block,
                len,
                now_ms,
                OriginReadMode::Current,
            )
            .await
        {
            Ok(bytes) => Ok(bytes),
            Err(DetailedBlockReadError::Block(error)) => Err(error),
            Err(DetailedBlockReadError::Worker(error)) => Err(BlockReadError::Worker(error)),
        }
    }

    /// Read `dst.len()` bytes at `offset_in_block` within `block` into `dst`.
    ///
    /// This follows the same placement-cache, replica fallback, and refresh
    /// behavior as [`read_block`](Self::read_block), without allocating an
    /// intermediate result buffer.
    pub async fn read_block_into(
        &self,
        block: &BlockId,
        offset_in_block: u32,
        dst: &mut [u8],
        now_ms: u64,
    ) -> Result<usize, BlockReadError> {
        self.read_block_into_with_mode(block, offset_in_block, dst, now_ms, OriginReadMode::Current)
            .await
    }

    /// Read into `dst` from the exact source version carried by `block`.
    ///
    /// A miss is conditionally fetched without resolving a newer generation.
    /// Version mismatches retain their worker error classification. Legacy
    /// [`read_block_into`](Self::read_block_into) keeps following the current origin.
    pub async fn read_versioned_block_into(
        &self,
        block: &BlockId,
        offset_in_block: u32,
        dst: &mut [u8],
        now_ms: u64,
    ) -> Result<usize, BlockReadError> {
        self.read_block_into_with_mode(
            block,
            offset_in_block,
            dst,
            now_ms,
            OriginReadMode::ExactVersion,
        )
        .await
    }

    async fn read_block_into_with_mode(
        &self,
        block: &BlockId,
        offset_in_block: u32,
        dst: &mut [u8],
        now_ms: u64,
        mode: OriginReadMode,
    ) -> Result<usize, BlockReadError> {
        let cached = match self.cache.get(block, now_ms) {
            Some(cached) => {
                self.stats.record_cache_hit();
                cached
            }
            None => {
                self.stats.record_cache_miss();
                self.resolve_and_cache(block, now_ms).await?
            }
        };
        let observed_generation = self.membership_refresh_generation.load(Ordering::Acquire);
        let abs_offset = block.offset + u64::from(offset_in_block);

        match self
            .try_replicas_into(block, &cached.replicas, abs_offset, dst, mode)
            .await
        {
            Ok(n) => {
                self.stats.add_bytes_served(n as u64);
                return Ok(n);
            }
            Err(failure) => {
                if !failure.retryable {
                    return Err(BlockReadError::Worker(failure.error));
                }
                tracing::debug!(%block, reason = ?failure.reason, "all cached replicas failed; refreshing placement");
                self.cache.invalidate(block, failure.reason);
            }
        }

        let fresh = self
            .resolve_and_cache_forced(block, now_ms, observed_generation)
            .await?;
        let n = self
            .try_replicas_into(block, &fresh.replicas, abs_offset, dst, mode)
            .await
            .map_err(|failure| BlockReadError::AllReplicasFailed {
                worker: failure.worker,
                source: failure.error,
            })?;
        self.stats.add_bytes_served(n as u64);
        Ok(n)
    }

    /// Read one exact-version block slice while preserving the last worker failure.
    ///
    /// Exhausted retries retain the last worker address and typed failure in
    /// `AllReplicasFailed`. Streaming protocol frontends use that final failure
    /// to make a deterministic fallback decision.
    pub(crate) async fn read_block_detailed(
        &self,
        block: &BlockId,
        offset_in_block: u32,
        len: u32,
        now_ms: u64,
    ) -> Result<Vec<u8>, DetailedBlockReadError> {
        self.read_block_detailed_with_mode(
            block,
            offset_in_block,
            len,
            now_ms,
            OriginReadMode::ExactVersion,
        )
        .await
    }

    async fn read_block_detailed_with_mode(
        &self,
        block: &BlockId,
        offset_in_block: u32,
        len: u32,
        now_ms: u64,
        mode: OriginReadMode,
    ) -> Result<Vec<u8>, DetailedBlockReadError> {
        let cached = match self.cache.get(block, now_ms) {
            Some(c) => {
                self.stats.record_cache_hit();
                c
            }
            None => {
                self.stats.record_cache_miss();
                self.resolve_and_cache(block, now_ms).await?
            }
        };
        let observed_generation = self.membership_refresh_generation.load(Ordering::Acquire);
        let abs_offset = block.offset + offset_in_block as u64;

        // First pass: walk the cached replica list in order.
        match self
            .try_replicas(block, &cached.replicas, abs_offset, len, mode)
            .await
        {
            Ok(bytes) => {
                self.stats.add_bytes_served(bytes.len() as u64);
                return Ok(bytes);
            }
            Err(failure) => {
                if !failure.retryable {
                    return Err(DetailedBlockReadError::Worker(failure.error));
                }
                // Every cached replica failed; drop the stale placement and do a
                // single membership refresh before giving up.
                tracing::debug!(%block, reason = ?failure.reason, "all cached replicas failed; refreshing placement");
                self.cache.invalidate(block, failure.reason);
            }
        }

        let fresh = self
            .resolve_and_cache_forced(block, now_ms, observed_generation)
            .await?;
        let bytes = self
            .try_replicas(block, &fresh.replicas, abs_offset, len, mode)
            .await
            .map_err(|failure| BlockReadError::AllReplicasFailed {
                worker: failure.worker,
                source: failure.error,
            })?;
        self.stats.add_bytes_served(bytes.len() as u64);
        Ok(bytes)
    }

    /// Try each replica address in order; return the first success, or the
    /// [`RefreshReason`] describing why the whole list failed.
    ///
    /// A connect failure or a remote "not present" is retryable against the next
    /// replica; the returned reason reflects the last failure so the caller can
    /// record an accurate invalidation cause.
    async fn try_replicas(
        &self,
        block: &BlockId,
        replicas: &[String],
        abs_offset: u64,
        len: u32,
        mode: OriginReadMode,
    ) -> Result<Vec<u8>, ReplicaFailure> {
        if replicas.is_empty() {
            return Err(ReplicaFailure {
                worker: "<none>".into(),
                reason: RefreshReason::WrongOwner,
                error: WorkerError::Remote(talon_transport::DataPlaneError {
                    code: talon_transport::DataErrorCode::Internal,
                    message: "placement contained no worker addresses".into(),
                }),
                retryable: true,
            });
        }
        let mut last = None;
        for addr in replicas {
            let worker = WorkerClient::with_pool(addr.clone(), Arc::clone(&self.worker_pool));
            self.stats.record_worker_fetch();
            let result = match mode {
                OriginReadMode::Current => {
                    worker
                        .fetch_range(&block.object, abs_offset, len as u64)
                        .await
                }
                OriginReadMode::ExactVersion => {
                    worker
                        .fetch_versioned_range(
                            &block.object,
                            &block.version,
                            abs_offset,
                            len as u64,
                        )
                        .await
                }
            };
            match result {
                Ok(bytes) if bytes.len() as u64 == u64::from(len) => {
                    self.record_zone_read(addr, bytes.len() as u64);
                    return Ok(bytes);
                }
                Ok(bytes) => {
                    self.stats.record_worker_failure();
                    last = Some(ReplicaFailure {
                        worker: addr.clone(),
                        reason: RefreshReason::WrongOwner,
                        error: WorkerError::RangeLengthMismatch {
                            expected: u64::from(len),
                            actual: bytes.len() as u64,
                        },
                        retryable: true,
                    });
                }
                Err(e) => {
                    self.stats.record_worker_failure();
                    let reason = match &e {
                        WorkerError::Io(_) => RefreshReason::ConnectFailure,
                        // A framing/encode error is not a placement problem, but
                        // treat it as a wrong-owner refresh so we still recover.
                        _ => RefreshReason::WrongOwner,
                    };
                    let retryable = replica_retryable(&e);
                    let failure = ReplicaFailure {
                        worker: addr.clone(),
                        reason,
                        error: e,
                        retryable,
                    };
                    if !retryable {
                        return Err(failure);
                    }
                    last = Some(failure);
                }
            }
        }
        Err(last.expect("non-empty replica list records a failure"))
    }

    /// Buffer-oriented variant of [`try_replicas`](Self::try_replicas).
    async fn try_replicas_into(
        &self,
        block: &BlockId,
        replicas: &[String],
        abs_offset: u64,
        dst: &mut [u8],
        mode: OriginReadMode,
    ) -> Result<usize, ReplicaFailure> {
        if replicas.is_empty() {
            return Err(ReplicaFailure {
                worker: "<none>".into(),
                reason: RefreshReason::WrongOwner,
                error: WorkerError::Remote(talon_transport::DataPlaneError {
                    code: talon_transport::DataErrorCode::Internal,
                    message: "placement contained no worker addresses".into(),
                }),
                retryable: true,
            });
        }
        let mut last = None;
        for addr in replicas {
            let worker = WorkerClient::with_pool(addr.clone(), Arc::clone(&self.worker_pool));
            self.stats.record_worker_fetch();
            let result = match mode {
                OriginReadMode::Current => {
                    worker
                        .fetch_range_into(&block.object, abs_offset, dst)
                        .await
                }
                OriginReadMode::ExactVersion => {
                    worker
                        .fetch_versioned_range_into(&block.object, &block.version, abs_offset, dst)
                        .await
                }
            };
            match result {
                Ok(n) if n == dst.len() => {
                    self.record_zone_read(addr, n as u64);
                    return Ok(n);
                }
                Ok(n) => {
                    self.stats.record_worker_failure();
                    last = Some(ReplicaFailure {
                        worker: addr.clone(),
                        reason: RefreshReason::WrongOwner,
                        error: WorkerError::RangeLengthMismatch {
                            expected: dst.len() as u64,
                            actual: n as u64,
                        },
                        retryable: true,
                    });
                }
                Err(error) => {
                    self.stats.record_worker_failure();
                    let reason = match &error {
                        WorkerError::Io(_) => RefreshReason::ConnectFailure,
                        _ => RefreshReason::WrongOwner,
                    };
                    let retryable = replica_retryable(&error);
                    let failure = ReplicaFailure {
                        worker: addr.clone(),
                        reason,
                        error,
                        retryable,
                    };
                    if !retryable {
                        return Err(failure);
                    }
                    last = Some(failure);
                }
            }
        }
        Err(last.expect("non-empty replica list records a failure"))
    }

    /// Reconcile the cache against an observed placement version.
    ///
    /// When a response (placement or otherwise) carries a version token that
    /// differs from a cached entry's, that entry is dropped so the next read
    /// re-looks-up. This is the client half of the coordinator's placement
    /// versioning: a membership change changes the deterministic version token,
    /// and any client holding a placement computed against a different set
    /// refreshes. The token is a content hash, not an ordered counter, so any
    /// difference triggers a refresh (see [`crate::PlacementCache::observe_epoch`]).
    /// Returns `true` if the entry was invalidated.
    pub fn observe_epoch(&self, block: &BlockId, observed_epoch: u64) -> bool {
        self.cache.observe_epoch(block, observed_epoch)
    }

    /// Read `[offset, offset+len)` of a file, spanning block boundaries.
    ///
    /// Splits the request into per-block segments via
    /// [`crate::read_plan::plan_read`] (clamped to `file.size` at EOF),
    /// fetches each segment through [`read_block`](Self::read_block) — so each
    /// segment independently benefits from the placement cache — and
    /// concatenates the results in order. A read at or past EOF returns an empty
    /// buffer (POSIX short read).
    pub async fn read(
        &self,
        file: &FileView<'_>,
        offset: u64,
        len: u64,
        now_ms: u64,
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
                .read_block(&seg.block, seg.offset_in_block, seg.len, now_ms)
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
        now_ms: u64,
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
                .read_block_into(
                    &seg.block,
                    seg.offset_in_block,
                    &mut dst[written..end],
                    now_ms,
                )
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
    /// placement path reads use, reusing the placement cache. Returns the
    /// dialable `host:port` of the primary owner.
    pub async fn resolve_owner(
        &self,
        object: &ObjectId,
        block_size: u32,
        version: &Version,
        now_ms: u64,
    ) -> Result<String, BlockReadError> {
        let block = BlockId::new(object.clone(), 0, block_size, version.clone());
        let cached = match self.cache.get(&block, now_ms) {
            Some(c) => c,
            None => self.resolve_and_cache(&block, now_ms).await?,
        };
        cached
            .replicas
            .first()
            .cloned()
            .ok_or(BlockReadError::UnresolvedOwner)
    }

    /// Rank the block against cached membership and cache the ordered addresses.
    async fn resolve_and_cache(
        &self,
        block: &BlockId,
        now_ms: u64,
    ) -> Result<Cached, BlockReadError> {
        self.resolve_and_cache_inner(block, now_ms, None).await
    }

    async fn resolve_and_cache_forced(
        &self,
        block: &BlockId,
        now_ms: u64,
        observed_generation: u64,
    ) -> Result<Cached, BlockReadError> {
        self.resolve_and_cache_inner(block, now_ms, Some(observed_generation))
            .await
    }

    async fn resolve_and_cache_inner(
        &self,
        block: &BlockId,
        now_ms: u64,
        force_membership_refresh: Option<u64>,
    ) -> Result<Cached, BlockReadError> {
        let membership = self
            .membership_snapshot(now_ms, force_membership_refresh)
            .await?;
        if membership.affinity_fallback {
            self.zone_observer.affinity_fallback();
        }
        let replicas = if self.replicas_k == 1 {
            vec![membership
                .placement
                .primary(block)
                .ok_or(BlockReadError::NoOwners)?
                .address
                .clone()]
        } else {
            let owners = membership.placement.rank(block, self.replicas_k as usize);
            if owners.is_empty() {
                return Err(BlockReadError::NoOwners);
            }
            owners.iter().map(|node| node.address.clone()).collect()
        };
        if replicas.is_empty() {
            return Err(BlockReadError::UnresolvedOwner);
        }
        let cached = Cached {
            replicas,
            epoch: membership.epoch,
        };
        self.cache.insert(block.clone(), cached.clone(), now_ms);
        Ok(cached)
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

    async fn membership_snapshot(
        &self,
        now_ms: u64,
        force_after: Option<u64>,
    ) -> Result<MembershipSnapshot, BlockReadError> {
        if force_after.is_none() {
            if let Some(snapshot) = self.membership.fresh(now_ms) {
                return Ok(snapshot);
            }
        }
        let _refresh = self.membership_refresh.lock().await;
        if let Some(observed) = force_after {
            if self.membership_refresh_generation.load(Ordering::Acquire) != observed {
                if let Some(snapshot) = self.membership.last_good() {
                    return Ok(snapshot);
                }
            }
            self.stats.record_coordinator_refresh();
        } else if let Some(snapshot) = self.membership.fresh(now_ms) {
            return Ok(snapshot);
        }
        let snapshot = match self.coordinator.membership_zoned(now_ms).await {
            Ok(members) => {
                let (snapshot, changed) = self.membership.replace(members, now_ms);
                if changed {
                    self.cache.clear();
                }
                snapshot
            }
            Err(error) => {
                if let Some(snapshot) = self.membership.last_good() {
                    tracing::warn!(%error, "membership refresh failed; using last-good snapshot");
                    snapshot
                } else {
                    return Err(error.into());
                }
            }
        };
        // Publish only after installing the snapshot and invalidating placements.
        // Unchanged/failed attempts still coalesce overlapping reads. Each new
        // read samples the current generation before its worker attempt, so it
        // can refresh again after a later failure, including after recovery.
        self.membership_refresh_generation
            .fetch_add(1, Ordering::Release);
        Ok(snapshot)
    }
}

fn cache_block_error(error: BlockReadError) -> CacheReadError {
    match error {
        BlockReadError::Coordinator(error) => error.into(),
        BlockReadError::Worker(error) => error.into(),
        other => CacheReadError::Unavailable(other.to_string()),
    }
}

fn replica_retryable(error: &WorkerError) -> bool {
    match error {
        WorkerError::Remote(error) => !matches!(
            error.code,
            talon_transport::DataErrorCode::InvalidRequest
                | talon_transport::DataErrorCode::NotFound
                | talon_transport::DataErrorCode::VersionMismatch
                | talon_transport::DataErrorCode::Origin
        ),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
                    let response = ControlMessage::MembershipListV2 {
                        nodes: vec![talon_transport::ZonedNodeInfo {
                            info: NodeInfo {
                                id: NodeId::new("w1"),
                                address: worker,
                                role: NodeRole::Worker,
                            },
                            zone: None,
                        }],
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
                        ControlMessage::MembershipListV2 {
                            nodes: vec![talon_transport::ZonedNodeInfo {
                                info: NodeInfo {
                                    id: NodeId::new("w1"),
                                    address: worker,
                                    role: NodeRole::Worker,
                                },
                                zone: None,
                            }],
                        }
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
                        ControlMessage::MembershipQuery {} => ControlMessage::MembershipList {
                            nodes: vec![NodeInfo {
                                id: NodeId::new("w1"),
                                address: worker_addr.clone(),
                                role: NodeRole::Worker,
                            }],
                        },
                        ControlMessage::MembershipQueryV2 {} => ControlMessage::MembershipListV2 {
                            nodes: vec![talon_transport::ZonedNodeInfo {
                                info: NodeInfo {
                                    id: NodeId::new("w1"),
                                    address: worker_addr.clone(),
                                    role: NodeRole::Worker,
                                },
                                zone: None,
                            }],
                        },
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
                    let mut hdr = [0u8; HEADER_LEN];
                    if s.read_exact(&mut hdr).await.is_err() {
                        return;
                    }
                    let h = FrameHeader::decode(&hdr).unwrap();
                    let mut body = vec![0u8; h.length as usize];
                    s.read_exact(&mut body).await.unwrap();
                    let mut full = hdr.to_vec();
                    full.extend_from_slice(&body);
                    let (_h, req): (_, RangeRequest) = decode_request(&full).unwrap();
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
                    let (_h, req): (_, RangeRequest) = decode_request(&full).unwrap();
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
                        ControlMessage::MembershipQuery {} => ControlMessage::MembershipList {
                            nodes: vec![
                                NodeInfo {
                                    id: NodeId::new("w1"),
                                    address: w1.clone(),
                                    role: NodeRole::Worker,
                                },
                                NodeInfo {
                                    id: NodeId::new("w2"),
                                    address: w2.clone(),
                                    role: NodeRole::Worker,
                                },
                            ],
                        },
                        ControlMessage::MembershipQueryV2 {} => ControlMessage::MembershipListV2 {
                            nodes: [w1.clone(), w2.clone()]
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
                        },
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

            let bytes = reader.clone().read_block(&block(), 0, 64, 0).await.unwrap();
            assert_eq!(bytes.len(), 64);
            assert_eq!(stats.snapshot().bytes_served, 64);
            assert_eq!(stats.snapshot().worker_fetches, 1);
            assert_eq!(pool.idle_count(&worker_addr), 1);
        }
    }

    #[tokio::test]
    async fn miss_then_hit_fetches_correct_range() {
        let hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let worker_addr = mock_worker(Arc::clone(&hits)).await;
        let coord_addr = mock_coordinator(worker_addr).await;

        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(coord_addr), Arc::clone(&cache), 1);

        let blk = block();
        // First read: cache miss → coordinator resolve → worker fetch.
        let bytes = reader.read_block(&blk, 100, 64, 0).await.unwrap();
        assert_eq!(bytes.len(), 64);
        let abs = blk.offset + 100;
        assert_eq!(bytes[0], (abs % 256) as u8);
        assert_eq!(bytes[1], ((abs + 1) % 256) as u8);
        assert_eq!(cache.len(), 1, "placement cached after miss");

        // Second read: cache hit (still 1 entry), worker serves again.
        let _ = reader.read_block(&blk, 0, 16, 1).await.unwrap();
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 2);

        // Metrics reflect one miss then one hit, two worker fetches, bytes served.
        let snap = reader.stats().snapshot();
        assert_eq!(snap.cache_misses, 1);
        assert_eq!(snap.cache_hits, 1);
        assert_eq!(snap.worker_fetches, 2);
        assert_eq!(snap.worker_failures, 0);
        assert_eq!(snap.bytes_served, 64 + 16);
        assert_eq!(snap.hit_ratio(), 0.5);
    }

    /// A schema-v5 coordinator advertising two workers in different zones.
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
                        let reply = ControlMessage::MembershipListV2 {
                            nodes: vec![
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
                            ],
                        };
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
            reader.read_block(&blk, 0, 16, 0).await.unwrap();
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

        reader.read_block(&block(), 0, 16, 0).await.unwrap();
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
            let reply = ControlMessage::MembershipListV2 { nodes: Vec::new() };
            s.write_all(&talon_transport::encode(0, &reply).unwrap())
                .await
                .unwrap();
            s.flush().await.unwrap();
        });
        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(addr), cache, 1);
        let err = reader.read_block(&block(), 0, 16, 0).await.unwrap_err();
        assert!(matches!(err, BlockReadError::NoOwners));
    }

    #[tokio::test]
    async fn coordinator_outage_uses_last_good_membership_for_a_new_block() {
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
            let reply = ControlMessage::MembershipListV2 {
                nodes: vec![talon_transport::ZonedNodeInfo {
                    info: NodeInfo {
                        id: NodeId::new("worker-a"),
                        address: worker_addr,
                        role: NodeRole::Worker,
                    },
                    zone: None,
                }],
            };
            stream
                .write_all(&talon_transport::encode(0, &reply).unwrap())
                .await
                .unwrap();
            stream.flush().await.unwrap();
            // Dropping the listener simulates the complete management-plane outage.
        });

        let cache = Arc::new(PlacementCache::new(0));
        let reader = BlockReader::new(CoordinatorClient::new(coordinator), cache, 1);
        reader.read_block(&block(), 0, 16, 0).await.unwrap();
        let mut next = block();
        next.offset += u64::from(next.block_size);
        let bytes = reader.read_block(&next, 0, 16, 1).await.unwrap();

        assert_eq!(bytes.len(), 16);
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_block_failures_share_one_forced_membership_refresh() {
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
                reader.read_block(&requested, 0, 16, 0).await
            }
        });
        for result in futures::future::join_all(reads).await {
            assert_eq!(result.unwrap().len(), 16);
        }

        assert_eq!(
            stale_requests.load(std::sync::atomic::Ordering::SeqCst),
            BLOCKS,
            "every block must first observe the stale placement"
        );
        assert_eq!(
            fresh_requests.load(std::sync::atomic::Ordering::SeqCst),
            BLOCKS,
            "every block must retry against the refreshed worker"
        );
        assert_eq!(
            membership_calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "one cold lookup plus one shared forced refresh are sufficient"
        );
        assert_eq!(
            reader.stats().snapshot().coordinator_refreshes,
            1,
            "the metric counts the one actual forced refresh, not its waiters"
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
            Arc::new(PlacementCache::new(10_000)),
            1,
        );
        let first = block();
        let mut second = block();
        second.offset += u64::from(second.block_size);

        // Populate two placements from the same initial membership generation.
        reader.resolve_and_cache(&first, 0).await.unwrap();
        reader.resolve_and_cache(&second, 0).await.unwrap();

        let observed = reader.membership_refresh_generation.load(Ordering::Acquire);
        assert!(reader.read_block(&first, 0, 16, 0).await.is_err());
        // Overlapping readers of the old generation reuse even an ineffective attempt.
        for _ in 0..4 {
            reader.membership_snapshot(0, Some(observed)).await.unwrap();
        }
        assert_eq!(membership_calls.load(Ordering::SeqCst), 2);
        let bytes = reader.read_block(&second, 0, 16, 0).await.unwrap();

        assert_eq!(bytes.len(), 16);
        assert_eq!(
            membership_calls.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "cold lookup, ineffective refresh, and recovered refresh must all run"
        );
        assert_eq!(
            recovered_requests.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the second read must recover without a user-visible extra failure"
        );
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
    async fn all_replicas_failing_errors_after_refresh() {
        // Single owner whose worker serves exactly one error then closes. The
        // reader tries it, refreshes (same owner), and on the second attempt the
        // worker is gone → connect failure → AllReplicasFailed.
        let worker_addr = {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let a = listener.local_addr().unwrap().to_string();
            tokio::spawn(async move {
                let (mut s, _) = listener.accept().await.unwrap();
                let mut hdr = [0u8; HEADER_LEN];
                s.read_exact(&mut hdr).await.unwrap();
                let h = FrameHeader::decode(&hdr).unwrap();
                let mut body = vec![0u8; h.length as usize];
                s.read_exact(&mut body).await.unwrap();
                s.write_all(&encode_error(0, "block not present"))
                    .await
                    .unwrap();
                s.flush().await.unwrap();
            });
            a
        };
        let coord_addr = mock_coordinator(worker_addr.clone()).await;
        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(coord_addr), cache, 1);
        let err = reader.read_block(&block(), 0, 16, 0).await.unwrap_err();
        let into_err = reader
            .read_block_into(&block(), 0, &mut [0; 16], 0)
            .await
            .unwrap_err();
        for err in [err, into_err] {
            assert!(err.to_string().contains(&worker_addr));
            assert!(err.to_string().contains("worker I/O:"));
            assert!(std::error::Error::source(&err).is_some());
            assert!(matches!(err, BlockReadError::AllReplicasFailed {
                worker, source: WorkerError::Io(_),
            } if worker == worker_addr));
        }
    }

    #[tokio::test]
    async fn worker_diagnostics_survive_both_read_apis() {
        use std::sync::atomic::{AtomicU32, Ordering};

        for (code, retries) in [
            (DataErrorCode::Timeout, true),
            (DataErrorCode::Origin, false),
        ] {
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
                        .read_block_into(&block(), 0, &mut [0; 16], 0)
                        .await
                        .unwrap_err()
                } else {
                    reader.read_block(&block(), 0, 16, 0).await.unwrap_err()
                };
                let message = error.to_string();
                assert!(
                    message.contains("backend request deadline exceeded"),
                    "{message}"
                );
                assert!(message.contains(&format!("{code:?}")), "{message}");
                if retries {
                    assert!(message.contains(&worker), "{message}");
                    assert!(matches!(error, BlockReadError::AllReplicasFailed {
                        source: WorkerError::Remote(ref remote), ..
                    } if remote.code == code));
                    assert_eq!(hits.load(Ordering::SeqCst), 2);
                } else {
                    assert!(!message.contains("after refresh"), "{message}");
                    assert!(
                        matches!(error, BlockReadError::Worker(WorkerError::Remote(ref remote))
                        if remote.code == code)
                    );
                    assert_eq!(hits.load(Ordering::SeqCst), 1);
                }
            }
        }
    }

    #[tokio::test]
    async fn falls_back_to_second_replica_on_wrong_owner() {
        // Primary w1 always errors "not present"; secondary w2 serves the bytes.
        // The reader must walk from w1 to w2 within the cached list — no refresh.
        let bad = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let good = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let w1 = spawn_erroring_worker(Arc::clone(&bad)).await;
        let w2 = mock_worker(Arc::clone(&good)).await;
        let coord = mock_coordinator_two(w1, w2).await;
        let cache = Arc::new(PlacementCache::new(10_000));
        // Request k=2 so both owners are cached.
        let reader = BlockReader::new(CoordinatorClient::new(coord), Arc::clone(&cache), 2);

        let bytes = reader.read_block(&block(), 0, 32, 0).await.unwrap();
        assert_eq!(bytes.len(), 32);
        assert_eq!(
            bad.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "primary tried"
        );
        assert_eq!(
            good.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "fell back to w2"
        );
        // Placement stays cached (fallback within the list, no invalidation).
        assert_eq!(cache.len(), 1);
    }

    #[tokio::test]
    async fn short_reply_from_worker_falls_back_to_next_replica() {
        let short = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let good = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let w1 = spawn_short_reply_worker(Arc::clone(&short)).await;
        let w2 = mock_worker(Arc::clone(&good)).await;
        let coord = mock_coordinator_two(w1, w2).await;
        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(coord), Arc::clone(&cache), 2);

        let bytes = reader.read_block(&block(), 0, 32, 0).await.unwrap();
        assert_eq!(bytes.len(), 32);
        assert_eq!(
            short.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "short primary reply was rejected"
        );
        assert_eq!(
            good.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "read fell back to the healthy replica"
        );
        assert_eq!(cache.len(), 1, "fallback did not invalidate placement");
        let stats = reader.stats().snapshot();
        assert_eq!(stats.worker_fetches, 2);
        assert_eq!(stats.worker_failures, 1);
        assert_eq!(stats.coordinator_refreshes, 0);
        assert_eq!(stats.bytes_served, 32);
    }

    #[tokio::test]
    async fn observe_epoch_invalidates_stale_entry() {
        let hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let worker_addr = mock_worker(Arc::clone(&hits)).await;
        let coord_addr = mock_coordinator(worker_addr).await;
        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(coord_addr), Arc::clone(&cache), 1);

        // Warm the cache from the locally versioned membership snapshot.
        let _ = reader.read_block(&block(), 0, 8, 0).await.unwrap();
        assert_eq!(cache.len(), 1);
        let epoch = cache.get(&block(), 0).unwrap().epoch;
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
        let bytes = reader.read(&file, offset, len, 0).await.unwrap();
        assert_eq!(bytes.len() as u64, len);
        // The mock worker fills each byte with (absolute_offset % 256); the
        // stitched buffer must be contiguous across block boundaries.
        for (i, b) in bytes.iter().enumerate() {
            assert_eq!(*b, ((offset + i as u64) % 256) as u8, "byte {i} mismatch");
        }
        // Four distinct blocks were fetched (one worker call each).
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 4);
        assert_eq!(cache.len(), 4, "each block's placement cached");
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

        let n = reader.read_into(&file, offset, &mut dst, 0).await.unwrap();
        assert_eq!(n, dst.len());
        for (index, byte) in dst.iter().enumerate() {
            assert_eq!(*byte, ((offset + index as u64) % 256) as u8);
        }
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 4);
        assert_eq!(cache.len(), 4);
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
        let bytes = reader.read(&file, 5000, 10, 0).await.unwrap();
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

        let n = reader.read_into(&file, 5000, &mut dst, 0).await.unwrap();
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

        let error = reader
            .read_cached_block(&block(), 7, 11, 0)
            .await
            .unwrap_err();
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
            .admit_block(&expected, expected.offset + 5, b"tail!", 0)
            .await
            .unwrap();
    }
}
