//! Instrumented worker cache request runtime.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use talon_core::{
    Backend, BackendStore, BlockForm, BlockHandle, BlockId, BlockMeta, Error, ObjectId,
    ObjectStore, PageIndex, Version,
};
use talon_transport::data::RangeRequest;
use talon_transport::frame::HEADER_LEN;
use talon_transport::{codec, ControlMessage, ObjectEntry, MAX_CONTROL_PAYLOAD_LEN};

use crate::{
    BlockIndex, CacheUnit, InFlightLoads, LoadKey, Lru, MemoryInsert, MemoryStore, Presence,
    WholeBlockStore, WorkerMetrics,
};

/// Default lifetime of a cached resolved object version.
///
/// A short TTL keeps warm cache hits from paying a backend `HEAD` per read
/// while still bounding how long a source overwrite can go unnoticed on the
/// read path; the conditional GET (`If-Match`) is the hard correctness guard
/// that catches an overwrite inside the window (issue #163).
const DEFAULT_VERSION_TTL: Duration = Duration::from_secs(3);

/// Maximum number of objects that can be returned by one non-paginated
/// `ListObjects` control request.
const MAX_LIST_OBJECTS: usize = 10_000;

/// Maximum backend pages drained by one non-paginated `ListObjects` request.
const MAX_LIST_PAGES: usize = 20;

/// Objects requested per backend page.
const LIST_PAGE_SIZE: u32 = 1000;

/// A per-object resolved version with the instant it was resolved.
struct CachedVersion {
    version: Version,
    resolved_at: Instant,
}

/// How the serve loop should transmit a range's bytes to the client.
///
/// The whole point is to avoid pulling a resident block through user space: when
/// the request lies entirely within a single already-cached block, the runtime
/// hands back an open file descriptor over the exact sub-range so the caller can
/// stream it with `sendfile(2)` (zero-copy, no per-request allocation). Every
/// other shape — a cache miss, or a request spanning block boundaries — falls
/// back to the in-memory byte path.
pub enum ServeOutcome {
    /// Serve these bytes with `sendfile` straight from the block file's fd.
    Sendfile(BlockHandle),
    /// Serve these already-in-memory bytes (miss just fetched, or a stitched
    /// multi-block read).
    Bytes(bytes::Bytes),
}

/// Shared state required to serve instrumented data-plane range requests.
pub struct WorkerRuntime {
    /// Small-block DRAM cache. L1 is inclusive: every entry also exists in L2.
    l1: Arc<MemoryStore>,
    /// Persistent local-NVMe cache.
    store: WholeBlockStore,
    index: Arc<BlockIndex>,
    inflight: Arc<InFlightLoads>,
    backend: Arc<dyn BackendStore>,
    /// Backend selected by the worker process. Optional only so unit-test
    /// runtimes that do not exercise backend routing retain their compact
    /// constructors.
    configured_backend: Option<Backend>,
    block_size: u32,
    metrics: WorkerMetrics,
    /// Byte-accounted LRU driving capacity enforcement/eviction (issue #159).
    lru: Arc<Lru>,
    /// Maximum resident cache bytes before eviction reclaims the coldest blocks.
    /// `0` disables capacity enforcement (unbounded — tests/dev only).
    capacity_bytes: u64,
    /// Short-TTL cache of resolved object versions, so a warm read does not pay
    /// a backend `HEAD` per request (issue #163).
    version_cache: Mutex<HashMap<ObjectId, CachedVersion>>,
    version_ttl: Duration,
}

impl WorkerRuntime {
    /// Create a request runtime over an initialized cache and backend.
    ///
    /// `capacity_bytes` bounds resident cache bytes; on each commit the runtime
    /// evicts the coldest unpinned blocks back under the cap (issue #159). A
    /// `capacity_bytes` of `0` disables enforcement (unbounded). The eviction
    /// tracker is seeded from `index` so blocks already resident from an on-disk
    /// scan count against capacity immediately.
    pub fn new(
        store: WholeBlockStore,
        index: Arc<BlockIndex>,
        inflight: Arc<InFlightLoads>,
        backend: Arc<dyn BackendStore>,
        block_size: u32,
        capacity_bytes: u64,
        metrics: WorkerMetrics,
    ) -> Self {
        Self::new_with_l1(
            store,
            index,
            inflight,
            backend,
            block_size,
            capacity_bytes,
            0,
            0,
            metrics,
        )
    }

    /// Create a runtime with an explicit L1 DRAM tier over the L2 NVMe store.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_l1(
        store: WholeBlockStore,
        index: Arc<BlockIndex>,
        inflight: Arc<InFlightLoads>,
        backend: Arc<dyn BackendStore>,
        block_size: u32,
        capacity_bytes: u64,
        l1_capacity_bytes: u64,
        l1_max_entry_bytes: u64,
        metrics: WorkerMetrics,
    ) -> Self {
        let lru = Arc::new(Lru::new());
        for (id, len) in index.snapshot_lens() {
            lru.insert(CacheUnit::Whole(id), len);
        }
        let l1 = Arc::new(MemoryStore::with_limits(
            l1_capacity_bytes,
            l1_max_entry_bytes,
        ));
        metrics.set_l1_capacity(l1_capacity_bytes);
        metrics.update_l1_residency(0, 0);
        Self {
            l1,
            store,
            index,
            inflight,
            backend,
            configured_backend: None,
            block_size,
            metrics,
            lru,
            capacity_bytes,
            version_cache: Mutex::new(HashMap::new()),
            version_ttl: DEFAULT_VERSION_TTL,
        }
    }

    /// Record the backend selected by the worker process.
    ///
    /// Requests carry a backend either directly in their [`ObjectId`] or in a
    /// namespace prefix (`s3`, `gcs`, or `az`). A worker has exactly one
    /// configured backend, so retaining that selection lets it reject a request
    /// routed to the wrong worker instead of addressing the same bucket/key on
    /// a different object store.
    pub fn with_backend_kind(mut self, backend: Backend) -> Self {
        self.configured_backend = Some(backend);
        self
    }

    /// Reject an operation routed to a worker for another object-store backend.
    fn ensure_configured_backend(&self, requested: Backend) -> anyhow::Result<()> {
        let Some(configured) = self.configured_backend else {
            return Ok(());
        };
        if requested != configured {
            anyhow::bail!(
                "request selects backend {requested}, but this worker is configured for \
                 {configured}; route the request to a {requested} worker"
            );
        }
        Ok(())
    }

    /// Override the resolved-version cache TTL (test hook).
    #[cfg(test)]
    fn with_version_ttl(mut self, ttl: Duration) -> Self {
        self.version_ttl = ttl;
        self
    }

    /// The block-aligned [`BlockId`] containing `offset` of `object` at a given
    /// object `version`.
    fn block_for(&self, object: &ObjectId, offset: u64, version: &Version) -> BlockId {
        let block_size = self.block_size as u64;
        let block_start = (offset / block_size) * block_size;
        BlockId::new(
            object.clone(),
            block_start,
            self.block_size,
            version.clone(),
        )
    }

    /// Serve `[offset, offset + len)`, spanning block boundaries as needed.
    ///
    /// A request whose range crosses one or more block boundaries is split into
    /// per-block reads (each a cache hit or a backend miss) and the pieces are
    /// stitched into one contiguous buffer. Previously only the block containing
    /// the *start* offset was read and the result clamped to that block's end,
    /// silently truncating cross-block reads (issue #112).
    ///
    /// The object's real version (ETag/generation) is resolved via a backend
    /// `head()` and folded into every `BlockId`, so an overwrite at the source
    /// produces distinct keys and the stale cached block is no longer served
    /// (issue #119). A missing/empty version is refused rather than cached under
    /// a placeholder.
    ///
    /// The resolved version is cached per object with a short TTL so a warm read
    /// does not pay a `HEAD` per request, and it is carried as an `If-Match`
    /// precondition into the miss GET so an overwrite inside the TTL window is
    /// caught (`412` → [`Error::VersionMismatch`]) rather than silently commits
    /// newer bytes under the older version's key. On a mismatch the cache is
    /// invalidated and the request is retried once against the freshly-resolved
    /// version (issue #163).
    pub async fn serve_range(&self, request: &RangeRequest) -> anyhow::Result<bytes::Bytes> {
        self.ensure_configured_backend(request.object.backend)?;
        if request.len == 0 {
            return Ok(bytes::Bytes::new());
        }

        // Resolve using the cache first; on a precondition failure (the object
        // was overwritten within the version-cache window) drop the stale entry
        // and retry once against a force-resolved version.
        let version = self.resolve_version(&request.object, false).await?;
        match self.serve_range_at(request, &version).await {
            Ok(bytes) => Ok(bytes),
            Err(error) if is_version_mismatch(&error) => {
                self.invalidate_version(&request.object);
                let version = self.resolve_version(&request.object, true).await?;
                self.serve_range_at(request, &version).await
            }
            Err(error) => Err(error),
        }
    }

    /// Serve `[offset, offset + len)`, choosing a zero-copy `sendfile` path when
    /// the whole range lies within a single already-resident block.
    ///
    /// This is the preferred entry point for the data plane: for the common case
    /// (a sub-range read of a cached block) it returns a [`ServeOutcome::Sendfile`]
    /// carrying an open fd over the exact bytes, so the caller streams them with
    /// `sendfile(2)` without ever reading the block into user space or allocating
    /// (issue #179). A cache miss, a boundary-spanning read, or a lost open race
    /// falls back to [`ServeOutcome::Bytes`] via the existing in-memory path.
    ///
    /// Version resolution and the precondition retry mirror [`serve_range`](Self::serve_range):
    /// blocks are keyed by the resolved ETag, and a `412`/`VersionMismatch` inside
    /// the version-cache window re-resolves once (issues #119, #163).
    pub async fn serve(&self, request: &RangeRequest) -> anyhow::Result<ServeOutcome> {
        self.ensure_configured_backend(request.object.backend)?;
        if request.len == 0 {
            return Ok(ServeOutcome::Bytes(bytes::Bytes::new()));
        }
        let version = self.resolve_version(&request.object, false).await?;
        match self.serve_at(request, &version).await {
            Ok(outcome) => Ok(outcome),
            Err(error) if is_version_mismatch(&error) => {
                self.invalidate_version(&request.object);
                let version = self.resolve_version(&request.object, true).await?;
                self.serve_at(request, &version).await
            }
            Err(error) => Err(error),
        }
    }

    /// The single-resolved-version body of [`serve`](Self::serve).
    ///
    /// Tries the zero-copy fast path: if the request lies within one block and
    /// that block is already resident, open an fd over the exact sub-range and
    /// return [`ServeOutcome::Sendfile`]. Anything else (miss, boundary span)
    /// falls through to [`serve_range_at`](Self::serve_range_at) and returns [`ServeOutcome::Bytes`].
    async fn serve_at(
        &self,
        request: &RangeRequest,
        version: &Version,
    ) -> anyhow::Result<ServeOutcome> {
        let block_size = self.block_size as u64;
        let end = request
            .offset
            .checked_add(request.len)
            .ok_or_else(|| anyhow::anyhow!("range offset+len overflows u64"))?;
        let start_block = (request.offset / block_size) * block_size;

        // Single-block fast paths: L1 bytes first, then an L2 sendfile handle.
        if end <= start_block + block_size {
            let block = self.block_for(&request.object, request.offset, version);
            let offset_in_block = request.offset - block.offset;
            if let Some(bytes) = self.l1_get(&block) {
                return Ok(ServeOutcome::Bytes(slice(
                    &bytes,
                    offset_in_block,
                    request.len,
                )?));
            }
            if matches!(
                self.index.presence(&block, PageIndex(0), PageIndex(1)),
                Presence::Whole
            ) {
                // Small blocks are promoted into L1 on their first post-restart L2
                // hit. Large blocks preserve the zero-copy sendfile path.
                if self
                    .index
                    .get(&block)
                    .is_some_and(|meta| self.l1.is_eligible(meta.len))
                {
                    match self.store.get_bytes(&block).await {
                        Ok(bytes) => {
                            self.record_l2_hit(&block);
                            self.admit_l1(&block, bytes.clone());
                            return Ok(ServeOutcome::Bytes(slice(
                                &bytes,
                                offset_in_block,
                                request.len,
                            )?));
                        }
                        Err(error) => {
                            tracing::debug!(%block, %error, "L2 promotion read lost an eviction race");
                        }
                    }
                }
                // Open an fd over exactly the requested window. This can fail if
                // the block was evicted between the presence check and the open
                // (a benign race); fall through to the byte path in that case.
                match self
                    .store
                    .get_range(&block, offset_in_block, request.len)
                    .await
                {
                    // The whole-block store returns exactly one handle spanning
                    // the requested window.
                    Ok(mut handles) if handles.len() == 1 => {
                        let handle = handles.pop().expect("one handle");
                        self.record_l2_hit(&block);
                        tracing::info!(block = %block, tier = "l2", "HIT (sendfile)");
                        return Ok(ServeOutcome::Sendfile(handle));
                    }
                    Ok(_) | Err(_) => {
                        // Lost the race with eviction, or an unexpected multi-handle
                        // result; serve via the byte path (which re-fetches if
                        // still absent).
                    }
                }
            }

            self.metrics.record_l2_miss();
            self.metrics.record_cache_miss();
            let bytes = self.load_block_bytes(request, &block).await?;
            return Ok(ServeOutcome::Bytes(slice(
                &bytes,
                offset_in_block,
                request.len,
            )?));
        }

        // Fallback: miss or boundary-spanning read → in-memory bytes.
        let bytes = self.serve_range_at(request, version).await?;
        Ok(ServeOutcome::Bytes(bytes))
    }

    /// Serve `[offset, offset + len)` against an already-resolved `version`.
    ///
    /// A request whose range crosses one or more block boundaries is split into
    /// per-block reads (each a cache hit or a backend miss) and the pieces are
    /// stitched into one contiguous buffer. Previously only the block containing
    /// the *start* offset was read and the result clamped to that block's end,
    /// silently truncating cross-block reads (issue #112).
    async fn serve_range_at(
        &self,
        request: &RangeRequest,
        version: &Version,
    ) -> anyhow::Result<bytes::Bytes> {
        let block_size = self.block_size as u64;
        let end = request
            .offset
            .checked_add(request.len)
            .ok_or_else(|| anyhow::anyhow!("range offset+len overflows u64"))?;

        // Fast path: the whole range lies within a single block. Keeps the
        // common case allocation-free (returns the per-block slice directly).
        let start_block = (request.offset / block_size) * block_size;
        if end <= start_block + block_size {
            let block = self.block_for(&request.object, request.offset, version);
            let offset_in_block = request.offset - block.offset;
            let bytes = self.block_bytes(request, &block).await?;
            return slice(&bytes, offset_in_block, request.len);
        }

        // Slow path: stitch across blocks.
        let mut out = bytes::BytesMut::with_capacity(request.len as usize);
        let mut cursor = request.offset;
        while cursor < end {
            let block = self.block_for(&request.object, cursor, version);
            let offset_in_block = cursor - block.offset;
            let block_end = block.offset + block_size;
            let take = block_end.min(end) - cursor;
            let bytes = self.block_bytes(request, &block).await?;
            let piece = slice(&bytes, offset_in_block, take)?;
            // A block that returned fewer bytes than its share means the object
            // ends inside it; stop rather than silently returning a short read.
            let short = piece.len() < take as usize;
            out.extend_from_slice(&piece);
            if short {
                break;
            }
            cursor += take;
        }
        Ok(out.freeze())
    }

    /// List objects under a mount-relative prefix (#332).
    ///
    /// `prefix` is a namespace path like `az/container/dir`: the first segment
    /// selects the backend, the second the bucket/container, and the rest is a
    /// key prefix. Returned paths are in the same namespace, so a client can
    /// feed them straight back to `read`.
    ///
    /// Pages are drained here rather than exposed to the client: the control
    /// protocol has no cursor, and adding one would be a wire change. Object,
    /// page, and encoded-response limits bound the operation. Hitting any limit
    /// is an error: returning a partial success would mount an apparently
    /// healthy but incomplete namespace.
    pub async fn list_objects(&self, prefix: &str) -> anyhow::Result<Vec<(String, u64)>> {
        let trimmed = prefix.trim_start_matches('/');
        let mut parts = trimmed.splitn(3, '/');
        let backend_prefix = parts.next().unwrap_or("");
        if backend_prefix.is_empty() {
            anyhow::bail!(
                "listing prefix must name a backend, e.g. `az/container/dir`; got {prefix:?}"
            );
        }
        let backend: Backend = backend_prefix.parse().map_err(|_| {
            anyhow::anyhow!("unknown backend {backend_prefix:?} in listing prefix {prefix:?}")
        })?;
        self.ensure_configured_backend(backend)?;
        let bucket = parts.next().unwrap_or("");
        if bucket.is_empty() {
            anyhow::bail!(
                "listing prefix must name a bucket/container, e.g. `az/container`; got {prefix:?}"
            );
        }
        let key_prefix = parts.next().unwrap_or("");

        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        for page_number in 1..=MAX_LIST_PAGES {
            let page = self
                .backend
                .list_objects(bucket, key_prefix, cursor.as_deref(), LIST_PAGE_SIZE)
                .await
                .map_err(|error| anyhow::anyhow!("list {prefix}: {error}"))?;

            let page_len = page.objects.len();
            let remaining = MAX_LIST_OBJECTS.saturating_sub(out.len());
            if page_len > remaining || (page_len == remaining && page.next.is_some()) {
                anyhow::bail!(
                    "listing {prefix:?} exceeds the 10,000-object control-response \
                     limit; narrow the namespace prefix"
                );
            }
            for object in page.objects {
                out.push((
                    format!("{}/{}/{}", backend.prefix(), bucket, object.key),
                    object.size,
                ));
            }

            ensure_listing_fits_control_frame(prefix, &out)?;

            match page.next {
                Some(next) => cursor = Some(next),
                None => return Ok(out),
            }
            if page_number == MAX_LIST_PAGES {
                anyhow::bail!(
                    "listing {prefix:?} did not complete within {MAX_LIST_PAGES} backend pages; \
                     narrow the namespace prefix"
                );
            }
        }

        unreachable!("the bounded listing loop always returns or reports its limit")
    }

    /// Return an object's size and current version (#318).
    ///
    /// Backs the `StatObject` control message. A client needs the version
    /// before it can address any block, since blocks are keyed by it, so this
    /// is a prerequisite for reading rather than a convenience.
    ///
    /// The result is not cached beyond the version cache the read path already
    /// maintains: a size can change under an overwrite, and serving a stale one
    /// would make a client read past the end of the new object.
    pub async fn stat_object(&self, object: &ObjectId) -> anyhow::Result<talon_core::ObjectStat> {
        self.ensure_configured_backend(object.backend)?;
        let stat = self
            .backend
            .head(object)
            .await
            .map_err(|error| anyhow::anyhow!("stat object (HEAD): {error}"))?;
        if stat.version.0.trim().is_empty() {
            anyhow::bail!(
                "backend returned no version/etag for {object}; a client cannot address blocks \
                 without one"
            );
        }
        // Reuse the read path's cache so a stat immediately followed by a read
        // does not pay a second HEAD.
        self.store_version(object, &stat.version);
        Ok(stat)
    }

    /// Resolve the object's current version, refusing an empty/missing version
    /// rather than caching under a placeholder (#119).
    ///
    /// Returns a fresh cached value when one is within the TTL and `force` is
    /// false; otherwise issues a backend `head()`, caches the result, and
    /// returns it. `force` bypasses the cache (used after a precondition
    /// failure) so the retry always sees the newest version (#163).
    async fn resolve_version(&self, object: &ObjectId, force: bool) -> anyhow::Result<Version> {
        if !force {
            if let Some(version) = self.cached_version(object) {
                return Ok(version);
            }
        }
        let stat = self
            .backend
            .head(object)
            .await
            .map_err(|error| anyhow::anyhow!("resolve object version (HEAD): {error}"))?;
        if stat.version.0.trim().is_empty() {
            anyhow::bail!(
                "backend returned no version/etag for {object}; refusing to cache without a version"
            );
        }
        self.store_version(object, &stat.version);
        Ok(stat.version)
    }

    /// Return a cached version for `object` if one is within the TTL.
    fn cached_version(&self, object: &ObjectId) -> Option<Version> {
        let cache = self.version_cache.lock().unwrap();
        let entry = cache.get(object)?;
        if entry.resolved_at.elapsed() < self.version_ttl {
            Some(entry.version.clone())
        } else {
            None
        }
    }

    /// Record a freshly-resolved version for `object`.
    fn store_version(&self, object: &ObjectId, version: &Version) {
        self.version_cache.lock().unwrap().insert(
            object.clone(),
            CachedVersion {
                version: version.clone(),
                resolved_at: Instant::now(),
            },
        );
    }

    /// Drop any cached version for `object` (after a precondition failure).
    fn invalidate_version(&self, object: &ObjectId) {
        self.version_cache.lock().unwrap().remove(object);
    }

    /// Return the full committed/fetched bytes of a single block, using the
    /// cache-hit path when resident and the backend-miss path otherwise.
    ///
    /// Concurrent misses for the same block are deduplicated: the first caller
    /// (the leader, holding an `InFlightGuard`) performs the backend fetch; the
    /// rest wait for it and then serve from the now-warm cache, so N concurrent
    /// misses trigger a single backend fetch instead of N (issue #113). The
    /// guard clears the in-flight marker on drop, so a cancelled or panicking
    /// leader can never orphan the key and hang the waiters (issue #162).
    async fn block_bytes(
        &self,
        request: &RangeRequest,
        block: &BlockId,
    ) -> anyhow::Result<bytes::Bytes> {
        if let Some(bytes) = self.cached_block(block).await? {
            return Ok(bytes);
        }

        self.metrics.record_cache_miss();
        self.load_block_bytes(request, block).await
    }

    /// Load one block after both L1 and L2 have missed.
    async fn load_block_bytes(
        &self,
        request: &RangeRequest,
        block: &BlockId,
    ) -> anyhow::Result<bytes::Bytes> {
        let key = LoadKey::Whole(block.clone());
        match self.inflight.admit_owned(key.clone()) {
            Some(guard) => {
                // Leader: fetch and commit; the guard wakes waiters on drop
                // (including on cancellation/panic).
                let result = self.fetch_and_commit(request, block).await;
                drop(guard);
                result
            }
            None => {
                // A peer is already fetching this block; wait for it and serve
                // from cache rather than issuing a duplicate backend fetch.
                self.inflight.wait(&key).await;
                if let Some(bytes) = self.cached_block(block).await? {
                    return Ok(bytes);
                }
                // The leader's load failed (marker cleared, block still absent).
                // Try to become the leader ourselves.
                match self.inflight.admit_owned(key.clone()) {
                    Some(guard) => {
                        let result = self.fetch_and_commit(request, block).await;
                        drop(guard);
                        result
                    }
                    None => {
                        // Another peer already restarted the load; wait once
                        // more, then, if still absent, fetch without holding
                        // admission to avoid an unbounded wait loop.
                        self.inflight.wait(&key).await;
                        if let Some(bytes) = self.cached_block(block).await? {
                            return Ok(bytes);
                        }
                        self.fetch_and_commit(request, block).await
                    }
                }
            }
        }
    }

    /// Return a block's bytes from the local cache if resident, else `None`.
    async fn cached_block(&self, block: &BlockId) -> anyhow::Result<Option<bytes::Bytes>> {
        if let Some(bytes) = self.l1_get(block) {
            return Ok(Some(bytes));
        }
        if matches!(
            self.index.presence(block, PageIndex(0), PageIndex(1)),
            Presence::Whole
        ) {
            self.record_l2_hit(block);
            tracing::info!(block = %block, tier = "l2", "HIT");
            let bytes = self
                .store
                .get_bytes(block)
                .await
                .map_err(|error| anyhow::anyhow!("read committed block: {error}"))?;
            self.admit_l1(block, bytes.clone());
            Ok(Some(bytes))
        } else {
            self.metrics.record_l2_miss();
            Ok(None)
        }
    }

    /// Read L1 and keep the inclusive L2 parent hot when present.
    fn l1_get(&self, block: &BlockId) -> Option<bytes::Bytes> {
        if !self.l1.is_enabled() {
            return None;
        }
        match self.l1.get(block) {
            Some(bytes) => {
                self.metrics.record_l1_hit();
                self.metrics.record_cache_hit();
                self.lru.touch(&CacheUnit::Whole(block.clone()));
                tracing::info!(block = %block, tier = "l1", "HIT");
                Some(bytes)
            }
            None => {
                self.metrics.record_l1_miss();
                None
            }
        }
    }

    /// Record an L2 hit and touch its capacity LRU.
    fn record_l2_hit(&self, block: &BlockId) {
        self.metrics.record_l2_hit();
        self.metrics.record_cache_hit();
        self.lru.touch(&CacheUnit::Whole(block.clone()));
    }

    /// Admit eligible bytes into L1 and publish resulting residency/evictions.
    fn admit_l1(&self, block: &BlockId, bytes: bytes::Bytes) {
        match self.l1.insert(block.clone(), bytes) {
            MemoryInsert::Inserted { evicted } => {
                self.metrics.record_l1_admission();
                for victim in evicted {
                    self.metrics.record_l1_eviction();
                    tracing::debug!(block = %victim, tier = "l1", "evicted block");
                }
                self.refresh_l1_metrics();
            }
            MemoryInsert::Disabled | MemoryInsert::TooLarge => {}
        }
    }

    fn invalidate_l1(&self, block: &BlockId) {
        if self.l1.remove(block) {
            self.refresh_l1_metrics();
        }
    }

    fn refresh_l1_metrics(&self) {
        self.metrics
            .update_l1_residency(self.l1.len() as u64, self.l1.resident_bytes());
    }

    /// Fetch a block from the backend and commit it to the local cache.
    async fn fetch_and_commit(
        &self,
        request: &RangeRequest,
        block: &BlockId,
    ) -> anyhow::Result<bytes::Bytes> {
        tracing::info!(block = %block, "MISS -> backend fetch");
        let started = Instant::now();
        // Carry the resolved version as an If-Match precondition so an overwrite
        // between version resolution and this GET is rejected (412) rather than
        // committing newer bytes under the older version's key (issue #163).
        let fetched = self
            .backend
            .fetch_range_if_match(
                &request.object,
                block.offset,
                self.block_size as u64,
                Some(&block.version),
            )
            .await;
        let bytes = match fetched {
            Ok(bytes) => {
                self.metrics
                    .record_backend_fetch_success(bytes.len() as u64, started.elapsed());
                bytes
            }
            Err(error) => {
                self.metrics.record_backend_fetch_error(started.elapsed());
                return Err(error.into());
            }
        };

        let len = bytes.len() as u64;
        self.store
            .put(block, bytes.clone())
            .await
            .map_err(|error| anyhow::anyhow!("commit block failed: {error}"))?;
        self.index.commit(BlockMeta {
            id: block.clone(),
            form: BlockForm::Whole,
            len,
        });
        // Track the freshly-committed block for eviction, then reclaim space:
        // first any superseded version of the same (object, offset) — version
        // churn would otherwise accumulate stale .blk files forever (issue #159,
        // #119) — then the coldest blocks until we are back under capacity. The
        // block just committed is pinned for the duration so it is never the
        // victim of its own commit.
        self.lru.insert(CacheUnit::Whole(block.clone()), len);
        self.lru.pin(&CacheUnit::Whole(block.clone()));
        let superseded = self.lru.evict_superseded(block);
        self.unlink_units(superseded).await;
        self.enforce_capacity().await;
        self.lru.unpin(&CacheUnit::Whole(block.clone()));
        if !self.l1.remove_superseded(block).is_empty() {
            self.refresh_l1_metrics();
        }
        self.admit_l1(block, bytes.clone());
        tracing::info!(block = %block, bytes = len, "committed block");
        Ok(bytes)
    }

    /// Write a whole object through to the backend, then cache it (#226/#229).
    ///
    /// Uploads `body` to the origin (`backend.put`) and, on success, commits the
    /// bytes to the local cache under the version the store assigned, so an
    /// immediate read-after-write is a cache hit. If the backend PUT fails,
    /// nothing is cached and the error propagates — a failed write is never
    /// silently cached.
    ///
    /// v1 handles single-block objects (`body.len() <= block_size`); a larger
    /// object is rejected with a clear error (multi-block write is future work).
    /// Returns the committed version.
    pub async fn write_object(
        &self,
        object: &ObjectId,
        body: bytes::Bytes,
    ) -> anyhow::Result<Version> {
        self.ensure_configured_backend(object.backend)?;
        if body.len() as u64 > self.block_size as u64 {
            anyhow::bail!(
                "object {} is {} bytes; v1 write supports at most one block ({} bytes)",
                object.to_path(),
                body.len(),
                self.block_size
            );
        }
        // Write through to the origin first; the backend PUT is the durability
        // point. Only cache after it succeeds.
        let version = match self.backend.put(object, body.clone()).await {
            Ok(v) => {
                self.metrics.record_backend_write_success(body.len() as u64);
                v
            }
            Err(error) => {
                self.metrics.record_backend_write_error();
                return Err(error.into());
            }
        };
        // Refresh the version cache so a subsequent read resolves the new version
        // without a HEAD, and addresses the just-written block correctly (#163).
        self.store_version(object, &version);
        // Commit the written bytes to the local cache under the new version, so
        // read-after-write is a hit. Mirrors the miss-commit path.
        let block = self.block_for(object, 0, &version);
        let len = body.len() as u64;
        self.store
            .put(&block, body.clone())
            .await
            .map_err(|error| anyhow::anyhow!("commit written block failed: {error}"))?;
        self.index.commit(BlockMeta {
            id: block.clone(),
            form: BlockForm::Whole,
            len,
        });
        self.lru.insert(CacheUnit::Whole(block.clone()), len);
        self.lru.pin(&CacheUnit::Whole(block.clone()));
        // Drop any superseded prior version of this object from the cache.
        let superseded = self.lru.evict_superseded(&block);
        self.unlink_units(superseded).await;
        self.enforce_capacity().await;
        self.lru.unpin(&CacheUnit::Whole(block.clone()));
        if !self.l1.remove_superseded(&block).is_empty() {
            self.refresh_l1_metrics();
        }
        self.admit_l1(&block, body);
        tracing::info!(object = %object.to_path(), bytes = len, version = %version, "wrote object");
        Ok(version)
    }

    /// Write a staged file through to the backend with bounded memory.
    ///
    /// Small files retain the existing cache-populating path. Larger files are
    /// streamed to the origin and intentionally left out of the single-block
    /// cache; subsequent reads resolve the new version and load ranges normally.
    pub async fn write_object_file(
        &self,
        object: &ObjectId,
        path: &Path,
        len: u64,
    ) -> anyhow::Result<Version> {
        self.ensure_configured_backend(object.backend)?;
        if len <= self.block_size as u64 {
            let body = tokio::fs::read(path).await?;
            if body.len() as u64 != len {
                anyhow::bail!(
                    "staged object {} changed size: expected {len}, found {}",
                    object.to_path(),
                    body.len()
                );
            }
            return self.write_object(object, bytes::Bytes::from(body)).await;
        }
        let old_version = self.cached_version(object);
        let version = match self.backend.put_file(object, path, len).await {
            Ok(version) => {
                self.metrics.record_backend_write_success(len);
                version
            }
            Err(error) => {
                self.metrics.record_backend_write_error();
                return Err(error.into());
            }
        };
        if let Some(old_version) = old_version {
            let old_block = self.block_for(object, 0, &old_version);
            self.unlink_units(vec![CacheUnit::Whole(old_block)]).await;
        }
        self.store_version(object, &version);
        tracing::info!(
            object = %object.to_path(),
            bytes = len,
            version = %version,
            "streamed object"
        );
        Ok(version)
    }

    /// Maximum PUT body retained in memory and cached as one block.
    pub fn max_inline_write_bytes(&self) -> u64 {
        self.block_size as u64
    }

    /// Delete an object from the backend and evict it locally (#226/#229).
    ///
    /// Deletes at the origin (`backend.delete`, idempotent), then invalidates the
    /// cached version so a subsequent read re-resolves (and sees the object gone).
    /// Best-effort evicts the object's currently-cached block.
    pub async fn delete_object(&self, object: &ObjectId) -> anyhow::Result<()> {
        self.ensure_configured_backend(object.backend)?;
        match self.backend.delete(object).await {
            Ok(()) => self.metrics.record_backend_delete_success(),
            Err(error) => {
                self.metrics.record_backend_delete_error();
                return Err(error.into());
            }
        }
        // Evict the locally-cached block for the last-known version, if any, and
        // drop the cached version so the next read re-resolves.
        if let Some(version) = self.cached_version(object) {
            let block = self.block_for(object, 0, &version);
            self.unlink_units(vec![CacheUnit::Whole(block)]).await;
        }
        self.invalidate_version(object);
        tracing::info!(object = %object.to_path(), "deleted object");
        Ok(())
    }

    /// Evict the coldest unpinned blocks until resident bytes are back under the
    /// configured capacity, unlinking each evicted block's file and index entry.
    /// A `capacity_bytes` of `0` disables enforcement.
    async fn enforce_capacity(&self) {
        if self.capacity_bytes == 0 {
            return;
        }
        let evicted = self.lru.evict_to_fit(self.capacity_bytes);
        self.unlink_units(evicted).await;
    }

    /// Unlink each evicted cache unit: delete its on-disk file, drop it from the
    /// index, and count the eviction. Best-effort — a failed unlink is logged
    /// but does not abort the serve path (the space is reclaimed on the next
    /// pass or restart scan).
    async fn unlink_units(&self, units: Vec<CacheUnit>) {
        for unit in units {
            let CacheUnit::Whole(id) = unit else {
                continue;
            };
            self.invalidate_l1(&id);
            if let Err(error) = self.store.delete(&id).await {
                tracing::warn!(block = %id, %error, "failed to unlink evicted block");
            }
            self.index.remove(&id);
            self.metrics.record_eviction();
            tracing::info!(block = %id, "evicted block");
        }
    }

    /// Number of blocks currently indexed.
    pub fn block_count(&self) -> u64 {
        self.index.len() as u64
    }

    /// Total resident cache bytes across all indexed blocks.
    pub fn resident_bytes(&self) -> u64 {
        self.index.resident_bytes()
    }

    /// Number of blocks resident in L1.
    pub fn l1_block_count(&self) -> u64 {
        self.l1.len() as u64
    }

    /// Bytes resident in L1.
    pub fn l1_resident_bytes(&self) -> u64 {
        self.l1.resident_bytes()
    }

    /// Number of backend loads currently in flight.
    pub fn inflight_loads(&self) -> u64 {
        self.inflight.len() as u64
    }
}

/// Ensure a successful listing reply can cross the control-plane transport.
///
/// The transport reader rejects control payloads above 1 MiB. Check the exact
/// codec output here so the worker can return a small, actionable `Ack(false)`
/// instead of writing a frame that every conforming client must reject.
fn ensure_listing_fits_control_frame(
    prefix: &str,
    entries: &[(String, u64)],
) -> anyhow::Result<()> {
    let message = ControlMessage::ObjectList {
        entries: entries
            .iter()
            .map(|(path, size)| ObjectEntry {
                path: path.clone(),
                size: *size,
            })
            .collect(),
    };
    let encoded = codec::encode(0, &message)
        .map_err(|error| anyhow::anyhow!("encode listing response for {prefix:?}: {error}"))?;
    let payload_len = encoded.len().saturating_sub(HEADER_LEN);
    if payload_len > MAX_CONTROL_PAYLOAD_LEN as usize {
        anyhow::bail!(
            "listing {prefix:?} requires a {payload_len}-byte control response, exceeding the \
             {MAX_CONTROL_PAYLOAD_LEN}-byte transport limit; narrow the namespace prefix"
        );
    }
    Ok(())
}

fn slice(buffer: &bytes::Bytes, offset: u64, len: u64) -> anyhow::Result<bytes::Bytes> {
    let start = usize::try_from(offset).map_err(|_| anyhow::anyhow!("offset is too large"))?;
    if start > buffer.len() {
        anyhow::bail!("offset {offset} beyond block length {} bytes", buffer.len());
    }
    let requested = usize::try_from(len).unwrap_or(usize::MAX);
    let end = start.saturating_add(requested).min(buffer.len());
    Ok(buffer.slice(start..end))
}

/// Whether an error chain carries a backend [`Error::VersionMismatch`], i.e. an
/// `If-Match` precondition failed because the object was overwritten (issue
/// #163). Used to trigger a single re-resolve-and-retry.
fn is_version_mismatch(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<Error>(),
            Some(Error::VersionMismatch { .. })
        )
    })
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::collections::VecDeque;
    use std::hash::{Hash, Hasher};
    use std::os::unix::fs::FileExt;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::SystemTime;

    use async_trait::async_trait;
    use bytes::Bytes;
    use talon_core::{Backend, Error, ListPage, ListedObject, ObjectStat, Result};

    use super::*;

    struct MockBackend {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl BackendStore for MockBackend {
        async fn fetch_range(&self, object: &ObjectId, _offset: u64, _len: u64) -> Result<Bytes> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if object.object_path == "failure" {
                Err(Error::Backend("simulated failure".into()))
            } else {
                Ok(Bytes::from_static(b"abcdefgh"))
            }
        }

        async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
            Ok(ObjectStat {
                len: 8,
                version: Version::new("v1"),
            })
        }
    }

    struct ListingBackend {
        pages: Mutex<VecDeque<ListPage>>,
        calls: AtomicUsize,
    }

    impl ListingBackend {
        fn new(pages: impl IntoIterator<Item = ListPage>) -> Self {
            Self {
                pages: Mutex::new(pages.into_iter().collect()),
                calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl BackendStore for ListingBackend {
        async fn list_objects(
            &self,
            _bucket: &str,
            _prefix: &str,
            _cursor: Option<&str>,
            _max_keys: u32,
        ) -> Result<ListPage> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.pages
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| Error::Backend("unexpected extra listing page".into()))
        }

        async fn fetch_range(&self, _object: &ObjectId, _offset: u64, _len: u64) -> Result<Bytes> {
            Err(Error::Backend("not used by listing tests".into()))
        }

        async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
            Err(Error::Backend("not used by listing tests".into()))
        }
    }

    fn request(path: &str) -> RangeRequest {
        RangeRequest {
            object: ObjectId::new(Backend::Azure, "container", path),
            offset: 0,
            len: 4,
        }
    }

    fn runtime(backend: Arc<MockBackend>, metrics: WorkerMetrics, root: &PathBuf) -> WorkerRuntime {
        WorkerRuntime::new(
            WholeBlockStore::open(root).unwrap(),
            Arc::new(BlockIndex::new()),
            Arc::new(InFlightLoads::new()),
            backend,
            8,
            0,
            metrics,
        )
        // Most tests assert version-sensitivity per read; a zero TTL keeps the
        // resolved-version cache from masking a source overwrite between reads.
        .with_version_ttl(Duration::ZERO)
    }

    fn listing_runtime(backend: Arc<ListingBackend>, root: &Path) -> WorkerRuntime {
        WorkerRuntime::new(
            WholeBlockStore::open(root).unwrap(),
            Arc::new(BlockIndex::new()),
            Arc::new(InFlightLoads::new()),
            backend,
            8,
            0,
            WorkerMetrics::new(1024),
        )
        .with_backend_kind(Backend::Azure)
    }

    fn listing_page(page: usize, count: usize, has_next: bool) -> ListPage {
        ListPage {
            objects: (0..count)
                .map(|index| ListedObject {
                    key: format!("dir/object-{page}-{index}"),
                    size: index as u64,
                })
                .collect(),
            next: has_next.then(|| format!("cursor-{page}")),
        }
    }

    #[tokio::test]
    async fn listing_rejects_a_backend_prefix_for_another_worker() {
        let root = tmp_root();
        let backend = Arc::new(ListingBackend::new([listing_page(0, 1, false)]));
        let runtime = listing_runtime(Arc::clone(&backend), &root);

        let error = runtime.list_objects("s3/bucket/dir").await.unwrap_err();

        assert!(error.to_string().contains("configured for az"));
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
        std::fs::remove_dir_all(root).ok();
    }

    fn assert_backend_mismatch(error: anyhow::Error) {
        let detail = error.to_string();
        assert!(detail.contains("selects backend s3"), "{detail}");
        assert!(detail.contains("configured for az"), "{detail}");
    }

    #[tokio::test]
    async fn object_operations_reject_a_backend_for_another_worker() {
        let root = tmp_root();
        let backend = Arc::new(ListingBackend::new([]));
        let runtime = listing_runtime(Arc::clone(&backend), &root);
        let object = ObjectId::new(Backend::S3, "bucket", "object");
        let request = RangeRequest {
            object: object.clone(),
            offset: 0,
            len: 0,
        };

        assert_backend_mismatch(runtime.serve_range(&request).await.unwrap_err());
        let serve_error = match runtime.serve(&request).await {
            Ok(_) => panic!("serve accepted an object for another backend"),
            Err(error) => error,
        };
        assert_backend_mismatch(serve_error);
        assert_backend_mismatch(runtime.stat_object(&object).await.unwrap_err());
        assert_backend_mismatch(
            runtime
                .write_object(&object, Bytes::from_static(b"x"))
                .await
                .unwrap_err(),
        );
        assert_backend_mismatch(
            runtime
                .write_object_file(&object, Path::new("unused-mismatched-backend-file"), 9)
                .await
                .unwrap_err(),
        );
        assert_backend_mismatch(runtime.delete_object(&object).await.unwrap_err());

        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn listing_reports_object_limit_instead_of_returning_a_partial_success() {
        let root = tmp_root();
        let pages = (0..10).map(|page| listing_page(page, 1000, true));
        let backend = Arc::new(ListingBackend::new(pages));
        let runtime = listing_runtime(Arc::clone(&backend), &root);

        let error = runtime.list_objects("az/bucket/dir").await.unwrap_err();

        assert!(error.to_string().contains("10,000-object"));
        assert!(error.to_string().contains("narrow the namespace prefix"));
        assert_eq!(backend.calls.load(Ordering::SeqCst), 10);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn listing_reports_page_limit_instead_of_returning_a_partial_success() {
        let root = tmp_root();
        let pages = (0..MAX_LIST_PAGES).map(|page| listing_page(page, 1, true));
        let backend = Arc::new(ListingBackend::new(pages));
        let runtime = listing_runtime(Arc::clone(&backend), &root);

        let error = runtime.list_objects("az/bucket/dir").await.unwrap_err();

        assert!(error.to_string().contains("20 backend pages"));
        assert!(error.to_string().contains("narrow the namespace prefix"));
        assert_eq!(backend.calls.load(Ordering::SeqCst), MAX_LIST_PAGES);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn listing_accepts_exact_object_and_page_boundaries_when_complete() {
        let root = tmp_root();
        let pages = (0..MAX_LIST_PAGES).map(|page| {
            listing_page(
                page,
                MAX_LIST_OBJECTS / MAX_LIST_PAGES,
                page + 1 < MAX_LIST_PAGES,
            )
        });
        let backend = Arc::new(ListingBackend::new(pages));
        let runtime = listing_runtime(Arc::clone(&backend), &root);

        let entries = runtime.list_objects("az/bucket/dir").await.unwrap();

        assert_eq!(entries.len(), MAX_LIST_OBJECTS);
        assert_eq!(backend.calls.load(Ordering::SeqCst), MAX_LIST_PAGES);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn listing_reports_transport_limit_before_writing_an_oversized_frame() {
        let root = tmp_root();
        let page = ListPage {
            objects: vec![ListedObject {
                key: "x".repeat(MAX_CONTROL_PAYLOAD_LEN as usize),
                size: 1,
            }],
            next: None,
        };
        let backend = Arc::new(ListingBackend::new([page]));
        let runtime = listing_runtime(Arc::clone(&backend), &root);

        let error = runtime.list_objects("az/bucket").await.unwrap_err();

        assert!(error.to_string().contains("transport limit"));
        assert!(error.to_string().contains("narrow the namespace prefix"));
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(root).ok();
    }

    fn runtime_l1(
        backend: Arc<MockBackend>,
        metrics: WorkerMetrics,
        root: &PathBuf,
        l2_capacity: u64,
        l1_capacity: u64,
        l1_max_entry: u64,
    ) -> WorkerRuntime {
        WorkerRuntime::new_with_l1(
            WholeBlockStore::open(root).unwrap(),
            Arc::new(BlockIndex::new()),
            Arc::new(InFlightLoads::new()),
            backend,
            8,
            l2_capacity,
            l1_capacity,
            l1_max_entry,
            metrics,
        )
        .with_version_ttl(Duration::ZERO)
    }

    #[tokio::test]
    async fn miss_then_hit_records_cache_and_backend_metrics() {
        let root = tmp_root();
        let backend = Arc::new(MockBackend {
            calls: AtomicUsize::new(0),
        });
        let metrics = WorkerMetrics::new(1024);
        let runtime = runtime(Arc::clone(&backend), metrics.clone(), &root);

        assert_eq!(
            runtime.serve_range(&request("ok")).await.unwrap(),
            Bytes::from_static(b"abcd")
        );
        assert_eq!(
            runtime.serve_range(&request("ok")).await.unwrap(),
            Bytes::from_static(b"abcd")
        );
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.block_count(), 1);

        let rendered = metrics.render();
        assert!(rendered.contains("talon_worker_cache_misses_total{form=\"whole\"} 1"));
        assert!(rendered.contains("talon_worker_cache_hits_total{form=\"whole\"} 1"));
        assert!(rendered.contains("talon_worker_backend_fetch_bytes_total{backend=\"azure\"} 8"));
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn l1_origin_fill_then_hit_uses_memory_bytes() {
        let root = tmp_root();
        let backend = Arc::new(MockBackend {
            calls: AtomicUsize::new(0),
        });
        let metrics = WorkerMetrics::new(1024);
        let runtime = runtime_l1(Arc::clone(&backend), metrics.clone(), &root, 1024, 16, 8);

        match runtime.serve(&request("l1")).await.unwrap() {
            ServeOutcome::Bytes(bytes) => assert_eq!(bytes, Bytes::from_static(b"abcd")),
            ServeOutcome::Sendfile(_) => panic!("origin miss must return fetched bytes"),
        }
        assert_eq!(runtime.l1_block_count(), 1);
        assert_eq!(runtime.l1_resident_bytes(), 8);

        match runtime.serve(&request("l1")).await.unwrap() {
            ServeOutcome::Bytes(bytes) => assert_eq!(bytes, Bytes::from_static(b"abcd")),
            ServeOutcome::Sendfile(_) => panic!("eligible warm block must hit L1"),
        }
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        let rendered = metrics.render();
        assert!(rendered.contains("talon_worker_cache_tier_hits_total{tier=\"l1\"} 1"));
        assert!(rendered.contains("talon_worker_cache_tier_misses_total{tier=\"l1\"} 1"));
        assert!(rendered.contains("talon_worker_cache_tier_misses_total{tier=\"l2\"} 1"));
        assert!(rendered.contains("talon_worker_l1_admissions_total 1"));
        assert!(rendered.contains("talon_worker_l1_blocks 1"));
        assert!(rendered.contains("talon_worker_l1_resident_bytes 8"));
        assert!(rendered.contains("talon_worker_l1_capacity_bytes 16"));
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn disabled_l1_preserves_l2_sendfile_behavior() {
        let root = tmp_root();
        let backend = Arc::new(MockBackend {
            calls: AtomicUsize::new(0),
        });
        let metrics = WorkerMetrics::new(1024);
        let runtime = runtime_l1(Arc::clone(&backend), metrics.clone(), &root, 1024, 0, 0);

        let _ = runtime.serve(&request("disabled")).await.unwrap();
        assert_eq!(
            read_handle(runtime.serve(&request("disabled")).await.unwrap()),
            b"abcd"
        );
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.l1_block_count(), 0);
        assert_eq!(runtime.l1_resident_bytes(), 0);
        let rendered = metrics.render();
        assert!(rendered.contains("talon_worker_cache_tier_hits_total{tier=\"l2\"} 1"));
        assert!(rendered.contains("talon_worker_l1_admissions_total 0"));
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn entry_at_l1_size_limit_is_admitted() {
        let root = tmp_root();
        let backend = Arc::new(MockBackend {
            calls: AtomicUsize::new(0),
        });
        let runtime = runtime_l1(
            Arc::clone(&backend),
            WorkerMetrics::new(1024),
            &root,
            1024,
            8,
            8,
        );

        let _ = runtime.serve(&request("boundary")).await.unwrap();
        assert_eq!(runtime.l1_block_count(), 1);
        assert_eq!(runtime.l1_resident_bytes(), 8);
        match runtime.serve(&request("boundary")).await.unwrap() {
            ServeOutcome::Bytes(bytes) => assert_eq!(bytes, Bytes::from_static(b"abcd")),
            ServeOutcome::Sendfile(_) => panic!("entry at the limit must be admitted to L1"),
        }
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn oversized_block_stays_on_l2_sendfile_path() {
        let root = tmp_root();
        let backend = Arc::new(MockBackend {
            calls: AtomicUsize::new(0),
        });
        let metrics = WorkerMetrics::new(1024);
        let runtime = runtime_l1(Arc::clone(&backend), metrics.clone(), &root, 1024, 16, 4);

        let _ = runtime.serve(&request("large")).await.unwrap();
        assert_eq!(runtime.l1_block_count(), 0);
        assert_eq!(
            read_handle(runtime.serve(&request("large")).await.unwrap()),
            b"abcd"
        );
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        assert!(metrics
            .render()
            .contains("talon_worker_cache_tier_hits_total{tier=\"l2\"} 1"));
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn l1_eviction_falls_back_to_l2_without_origin_fetch() {
        let root = tmp_root();
        let backend = Arc::new(MockBackend {
            calls: AtomicUsize::new(0),
        });
        let metrics = WorkerMetrics::new(1024);
        let runtime = runtime_l1(Arc::clone(&backend), metrics.clone(), &root, 1024, 8, 8);

        let _ = runtime.serve(&request("a")).await.unwrap();
        let _ = runtime.serve(&request("b")).await.unwrap();
        assert_eq!(runtime.block_count(), 2, "both parents remain in L2");
        assert_eq!(runtime.l1_block_count(), 1, "L1 holds only the MRU block");
        assert_eq!(backend.calls.load(Ordering::SeqCst), 2);

        match runtime.serve(&request("a")).await.unwrap() {
            ServeOutcome::Bytes(bytes) => assert_eq!(bytes, Bytes::from_static(b"abcd")),
            ServeOutcome::Sendfile(_) => panic!("eligible L2 hit should promote to L1"),
        }
        assert_eq!(
            backend.calls.load(Ordering::SeqCst),
            2,
            "L1 eviction must degrade to L2, not origin"
        );
        assert_eq!(runtime.block_count(), 2);
        assert_eq!(runtime.l1_block_count(), 1);
        assert!(metrics
            .render()
            .contains("talon_worker_l1_evictions_total 2"));
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn l2_eviction_invalidates_inclusive_l1_copy() {
        let root = tmp_root();
        let backend = Arc::new(MockBackend {
            calls: AtomicUsize::new(0),
        });
        let runtime = runtime_l1(Arc::clone(&backend), WorkerMetrics::new(8), &root, 8, 16, 8);

        let _ = runtime.serve(&request("a")).await.unwrap();
        let _ = runtime.serve(&request("b")).await.unwrap();
        assert_eq!(runtime.block_count(), 1, "L2 capacity keeps one block");
        assert_eq!(
            runtime.l1_block_count(),
            1,
            "evicted L2 parent must not leave an orphan L1 copy"
        );

        let _ = runtime.serve(&request("a")).await.unwrap();
        assert_eq!(
            backend.calls.load(Ordering::SeqCst),
            3,
            "reading the L2-evicted block must fetch origin again"
        );
        assert_eq!(runtime.block_count(), 1);
        assert_eq!(runtime.l1_block_count(), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn restart_rebuilds_l2_then_promotes_without_refetching_body() {
        let root = tmp_root();
        let backend = Arc::new(MockBackend {
            calls: AtomicUsize::new(0),
        });
        let first = runtime_l1(
            Arc::clone(&backend),
            WorkerMetrics::new(1024),
            &root,
            1024,
            16,
            8,
        );
        let _ = first.serve(&request("restart")).await.unwrap();
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        drop(first);

        let store = WholeBlockStore::open(&root).unwrap();
        let index = Arc::new(BlockIndex::new());
        for meta in store.scan().unwrap() {
            index.commit(meta);
        }
        let restarted = WorkerRuntime::new_with_l1(
            store,
            index,
            Arc::new(InFlightLoads::new()),
            Arc::clone(&backend) as Arc<dyn BackendStore>,
            8,
            1024,
            16,
            8,
            WorkerMetrics::new(1024),
        )
        .with_version_ttl(Duration::ZERO);
        assert_eq!(restarted.l1_block_count(), 0);
        assert_eq!(restarted.block_count(), 1);

        match restarted.serve(&request("restart")).await.unwrap() {
            ServeOutcome::Bytes(bytes) => assert_eq!(bytes, Bytes::from_static(b"abcd")),
            ServeOutcome::Sendfile(_) => panic!("eligible L2 block should promote after restart"),
        }
        assert_eq!(
            backend.calls.load(Ordering::SeqCst),
            1,
            "restart promotion must not refetch object bytes"
        );
        assert_eq!(restarted.l1_block_count(), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn concurrent_warm_reads_are_all_served_from_l1() {
        let root = tmp_root();
        let backend = Arc::new(MockBackend {
            calls: AtomicUsize::new(0),
        });
        let metrics = WorkerMetrics::new(1024);
        let runtime = Arc::new(runtime_l1(
            Arc::clone(&backend),
            metrics.clone(),
            &root,
            1024,
            16,
            8,
        ));
        let _ = runtime.serve(&request("hot")).await.unwrap();

        let mut tasks = Vec::new();
        for _ in 0..64 {
            let runtime = Arc::clone(&runtime);
            tasks.push(tokio::spawn(async move {
                runtime.serve_range(&request("hot")).await.unwrap()
            }));
        }
        for task in tasks {
            assert_eq!(task.await.unwrap(), Bytes::from_static(b"abcd"));
        }
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        assert!(metrics
            .render()
            .contains("talon_worker_cache_tier_hits_total{tier=\"l1\"} 64"));
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn cross_block_read_is_filled_then_served_entirely_from_l1() {
        let root = tmp_root();
        let calls = Arc::new(AtomicUsize::new(0));
        let backend = Arc::new(CountingRampBackend {
            block_size: 8,
            calls: Arc::clone(&calls),
        });
        let metrics = WorkerMetrics::new(1024);
        let runtime = WorkerRuntime::new_with_l1(
            WholeBlockStore::open(&root).unwrap(),
            Arc::new(BlockIndex::new()),
            Arc::new(InFlightLoads::new()),
            backend,
            8,
            1024,
            16,
            8,
            metrics.clone(),
        )
        .with_version_ttl(Duration::ZERO);
        let req = RangeRequest {
            object: ObjectId::new(Backend::Azure, "container", "cross-l1"),
            offset: 6,
            len: 8,
        };

        assert_eq!(runtime.serve_range(&req).await.unwrap(), expected(6, 8));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(runtime.l1_block_count(), 2);
        assert_eq!(runtime.l1_resident_bytes(), 16);

        assert_eq!(runtime.serve_range(&req).await.unwrap(), expected(6, 8));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "both blocks must be served from L1 after the first read"
        );
        assert!(metrics
            .render()
            .contains("talon_worker_cache_tier_hits_total{tier=\"l1\"} 2"));
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn backend_failure_does_not_populate_either_cache_tier() {
        let root = tmp_root();
        let backend = Arc::new(MockBackend {
            calls: AtomicUsize::new(0),
        });
        let runtime = runtime_l1(
            Arc::clone(&backend),
            WorkerMetrics::new(1024),
            &root,
            1024,
            16,
            8,
        );

        assert!(runtime.serve(&request("failure")).await.is_err());
        assert_eq!(runtime.inflight_loads(), 0);
        assert_eq!(runtime.block_count(), 0);
        assert_eq!(runtime.resident_bytes(), 0);
        assert_eq!(runtime.l1_block_count(), 0);
        assert_eq!(runtime.l1_resident_bytes(), 0);
        std::fs::remove_dir_all(root).ok();
    }

    /// Read the bytes a `Sendfile` outcome would transmit, straight from its fd.
    fn read_handle(outcome: ServeOutcome) -> Vec<u8> {
        use std::io::{Read, Seek, SeekFrom};
        match outcome {
            ServeOutcome::Sendfile(handle) => {
                let mut f = std::fs::File::from(handle.fd);
                f.seek(SeekFrom::Start(handle.offset)).unwrap();
                let mut buf = vec![0u8; handle.len as usize];
                f.read_exact(&mut buf).unwrap();
                buf
            }
            ServeOutcome::Bytes(_) => panic!("expected Sendfile, got Bytes"),
        }
    }

    #[tokio::test]
    async fn serve_uses_sendfile_on_a_resident_hit() {
        // First serve is a miss → in-memory Bytes (block just fetched). The
        // second serve of the same resident block returns a Sendfile handle over
        // exactly the requested sub-range, byte-for-byte (issue #179).
        let root = tmp_root();
        let backend = Arc::new(MockBackend {
            calls: AtomicUsize::new(0),
        });
        let runtime = runtime(Arc::clone(&backend), WorkerMetrics::new(1024), &root);

        // Miss: bytes path.
        match runtime.serve(&request("ok")).await.unwrap() {
            ServeOutcome::Bytes(b) => assert_eq!(b, Bytes::from_static(b"abcd")),
            ServeOutcome::Sendfile(_) => panic!("first serve (miss) must be Bytes"),
        }

        // Hit: sendfile path, exact sub-range.
        let outcome = runtime.serve(&request("ok")).await.unwrap();
        assert_eq!(read_handle(outcome), b"abcd");
        // No second backend fetch.
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn serve_sendfile_serves_a_mid_block_subrange() {
        // A sub-range that does not start at 0 must open an fd at the right
        // offset, so the handle covers exactly [offset, offset+len).
        let root = tmp_root();
        let backend = Arc::new(MockBackend {
            calls: AtomicUsize::new(0),
        });
        let runtime = runtime(Arc::clone(&backend), WorkerMetrics::new(1024), &root);

        // Warm the block (returns b"abcdefgh").
        let _ = runtime.serve(&request("ok")).await.unwrap();
        // Request bytes [3, 7) of the same block: "defg".
        let req = RangeRequest {
            object: ObjectId::new(Backend::Azure, "container", "ok"),
            offset: 3,
            len: 4,
        };
        let outcome = runtime.serve(&req).await.unwrap();
        assert_eq!(read_handle(outcome), b"defg");
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn serve_falls_back_to_bytes_across_block_boundary() {
        // A read spanning two blocks cannot be a single-fd sendfile; it must
        // stitch in memory and return Bytes even when both blocks are resident.
        let root = tmp_root();
        let runtime = runtime_with(
            Arc::new(RampBackend { block_size: 8 }),
            WorkerMetrics::new(1024),
            &root,
            8,
        );
        // [6, 14) spans block0 [0,8) and block1 [8,16).
        let req = RangeRequest {
            object: ObjectId::new(Backend::Azure, "container", "ramp"),
            offset: 6,
            len: 8,
        };
        match runtime.serve(&req).await.unwrap() {
            ServeOutcome::Bytes(b) => assert_eq!(b, expected(6, 8)),
            ServeOutcome::Sendfile(_) => panic!("boundary-spanning read must be Bytes"),
        }
        std::fs::remove_dir_all(root).ok();
    }

    /// A backend whose block content is deterministic per absolute offset, so a
    /// stitched multi-block read can be verified byte-for-byte.
    struct RampBackend {
        block_size: u64,
    }
    #[async_trait]
    impl BackendStore for RampBackend {
        async fn fetch_range(&self, _object: &ObjectId, offset: u64, len: u64) -> Result<Bytes> {
            // Return one block worth of bytes starting at `offset`; byte i has
            // value (offset + i) % 251 (prime, so no accidental alignment).
            let n = len.min(self.block_size) as usize;
            let buf: Vec<u8> = (0..n).map(|i| ((offset + i as u64) % 251) as u8).collect();
            Ok(Bytes::from(buf))
        }

        async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
            Ok(ObjectStat {
                len: u64::MAX,
                version: Version::new("v1"),
            })
        }
    }

    struct CountingRampBackend {
        block_size: u64,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl BackendStore for CountingRampBackend {
        async fn fetch_range(&self, _object: &ObjectId, offset: u64, len: u64) -> Result<Bytes> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let n = len.min(self.block_size) as usize;
            let buf: Vec<u8> = (0..n).map(|i| ((offset + i as u64) % 251) as u8).collect();
            Ok(Bytes::from(buf))
        }

        async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
            Ok(ObjectStat {
                len: u64::MAX,
                version: Version::new("v1"),
            })
        }
    }

    fn expected(offset: u64, len: u64) -> Bytes {
        Bytes::from(
            (0..len)
                .map(|i| ((offset + i) % 251) as u8)
                .collect::<Vec<u8>>(),
        )
    }

    fn runtime_with<B: BackendStore + 'static>(
        backend: Arc<B>,
        metrics: WorkerMetrics,
        root: &PathBuf,
        block_size: u32,
    ) -> WorkerRuntime {
        WorkerRuntime::new(
            WholeBlockStore::open(root).unwrap(),
            Arc::new(BlockIndex::new()),
            Arc::new(InFlightLoads::new()),
            backend,
            block_size,
            0,
            metrics,
        )
        // Default to always-resolve so version-sensitivity assertions are not
        // masked by the version cache; caching is exercised explicitly below.
        .with_version_ttl(Duration::ZERO)
    }

    struct StreamPutBackend {
        uploaded: std::sync::Mutex<Option<(u64, u8)>>,
    }

    #[async_trait]
    impl BackendStore for StreamPutBackend {
        async fn fetch_range(&self, _object: &ObjectId, _offset: u64, _len: u64) -> Result<Bytes> {
            Ok(Bytes::new())
        }

        async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
            Ok(ObjectStat {
                len: 0,
                version: Version::new("stream-v1"),
            })
        }

        async fn put_file(&self, _object: &ObjectId, path: &Path, len: u64) -> Result<Version> {
            let file = std::fs::File::open(path).unwrap();
            let mut tail = [0u8; 1];
            file.read_exact_at(&mut tail, len - 1).unwrap();
            *self.uploaded.lock().unwrap() = Some((len, tail[0]));
            Ok(Version::new("stream-v1"))
        }
    }

    #[tokio::test]
    async fn large_staged_write_streams_without_entering_single_block_cache() {
        let root = tmp_root();
        let backend = Arc::new(StreamPutBackend {
            uploaded: std::sync::Mutex::new(None),
        });
        let metrics = WorkerMetrics::new(1024);
        let runtime = runtime_with(Arc::clone(&backend), metrics.clone(), &root, 8);
        let staged = tempfile::NamedTempFile::new().unwrap();
        staged.as_file().set_len(4097).unwrap();
        staged.as_file().write_all_at(b"x", 4096).unwrap();
        let object = ObjectId::new(Backend::S3, "bucket", "large.bin");

        let version = runtime
            .write_object_file(&object, staged.path(), 4097)
            .await
            .unwrap();

        assert_eq!(version, Version::new("stream-v1"));
        assert_eq!(*backend.uploaded.lock().unwrap(), Some((4097, b'x')));
        assert_eq!(runtime.block_count(), 0);
        assert!(metrics
            .render()
            .contains("talon_worker_backend_write_bytes_total{backend=\"azure\"} 4097"));
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn cross_block_read_stitches_multiple_blocks() {
        // block_size 8; read [6, 14) spans blocks [0,8) and [8,16), so it must
        // stitch two per-block fetches into one 8-byte contiguous result.
        let root = tmp_root();
        let runtime = runtime_with(
            Arc::new(RampBackend { block_size: 8 }),
            WorkerMetrics::new(1024),
            &root,
            8,
        );
        let req = RangeRequest {
            object: ObjectId::new(Backend::Azure, "container", "ramp"),
            offset: 6,
            len: 8,
        };
        let got = runtime.serve_range(&req).await.unwrap();
        assert_eq!(got.len(), 8);
        assert_eq!(got, expected(6, 8));
        assert_eq!(runtime.block_count(), 2);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn read_spanning_three_blocks_is_contiguous() {
        let root = tmp_root();
        let runtime = runtime_with(
            Arc::new(RampBackend { block_size: 8 }),
            WorkerMetrics::new(1024),
            &root,
            8,
        );
        // [4, 22): tail of block0, all of block1, head of block2.
        let req = RangeRequest {
            object: ObjectId::new(Backend::Azure, "container", "ramp"),
            offset: 4,
            len: 18,
        };
        let got = runtime.serve_range(&req).await.unwrap();
        assert_eq!(got, expected(4, 18));
        assert_eq!(runtime.block_count(), 3);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn single_block_read_still_works() {
        let root = tmp_root();
        let runtime = runtime_with(
            Arc::new(RampBackend { block_size: 8 }),
            WorkerMetrics::new(1024),
            &root,
            8,
        );
        let req = RangeRequest {
            object: ObjectId::new(Backend::Azure, "container", "ramp"),
            offset: 2,
            len: 4,
        };
        let got = runtime.serve_range(&req).await.unwrap();
        assert_eq!(got, expected(2, 4));
        assert_eq!(runtime.block_count(), 1);
        std::fs::remove_dir_all(root).ok();
    }

    /// A backend that counts calls and is slow enough that concurrent misses
    /// overlap, so the dedup path is actually exercised.
    struct SlowCountingBackend {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl BackendStore for SlowCountingBackend {
        async fn fetch_range(&self, _object: &ObjectId, _offset: u64, _len: u64) -> Result<Bytes> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok(Bytes::from_static(b"abcdefgh"))
        }

        async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
            Ok(ObjectStat {
                len: 8,
                version: Version::new("v1"),
            })
        }
    }

    #[tokio::test]
    async fn concurrent_misses_trigger_a_single_backend_fetch() {
        // Many simultaneous misses for the same block must dedup to one backend
        // fetch; the followers wait for the leader and serve from cache (#113).
        let root = tmp_root();
        let calls = Arc::new(AtomicUsize::new(0));
        let backend = Arc::new(SlowCountingBackend {
            calls: Arc::clone(&calls),
        });
        let runtime = Arc::new(runtime_with(backend, WorkerMetrics::new(1024), &root, 8));

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let runtime = Arc::clone(&runtime);
            tasks.push(tokio::spawn(async move {
                runtime.serve_range(&request("ok")).await.unwrap()
            }));
        }
        for t in tasks {
            assert_eq!(t.await.unwrap(), Bytes::from_static(b"abcd"));
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "concurrent misses must dedup to one backend fetch"
        );
        assert_eq!(runtime.block_count(), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn backend_error_is_counted_and_clears_inflight_state() {
        let root = tmp_root();
        let backend = Arc::new(MockBackend {
            calls: AtomicUsize::new(0),
        });
        let metrics = WorkerMetrics::new(1024);
        let runtime = runtime(backend, metrics.clone(), &root);

        assert!(runtime.serve_range(&request("failure")).await.is_err());
        assert_eq!(runtime.inflight_loads(), 0);
        assert!(metrics
            .render()
            .contains("talon_worker_backend_fetch_errors_total{backend=\"azure\"} 1"));
        std::fs::remove_dir_all(root).ok();
    }

    fn tmp_root() -> PathBuf {
        let mut hasher = DefaultHasher::new();
        SystemTime::now().hash(&mut hasher);
        std::thread::current().id().hash(&mut hasher);
        std::env::temp_dir().join(format!(
            "talon-runtime-{}-{}",
            std::process::id(),
            hasher.finish()
        ))
    }

    /// A backend whose reported version and body are swappable at runtime, and
    /// which counts fetches, so a source "overwrite" can be simulated.
    struct VersionedBackend {
        version: std::sync::Mutex<String>,
        body: std::sync::Mutex<Bytes>,
        fetches: AtomicUsize,
    }

    #[async_trait]
    impl BackendStore for VersionedBackend {
        async fn fetch_range(&self, _object: &ObjectId, _offset: u64, _len: u64) -> Result<Bytes> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            Ok(self.body.lock().unwrap().clone())
        }

        async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
            Ok(ObjectStat {
                len: self.body.lock().unwrap().len() as u64,
                version: Version::new(self.version.lock().unwrap().clone()),
            })
        }
    }

    #[tokio::test]
    async fn overwrite_at_source_invalidates_stale_cache() {
        // First read caches under version "v1". The source is then overwritten
        // (new etag "v2" + new bytes); the next read must resolve the new
        // version, miss the stale block, and serve the fresh bytes (issue #119).
        let root = tmp_root();
        let backend = Arc::new(VersionedBackend {
            version: std::sync::Mutex::new("v1".into()),
            body: std::sync::Mutex::new(Bytes::from_static(b"old-data")),
            fetches: AtomicUsize::new(0),
        });
        let runtime = runtime_with(Arc::clone(&backend), WorkerMetrics::new(1024), &root, 8);

        let first = runtime.serve_range(&request("obj")).await.unwrap();
        assert_eq!(first, Bytes::from_static(b"old-"));
        assert_eq!(backend.fetches.load(Ordering::SeqCst), 1);

        // A second read of the same version is a cache hit (no new fetch).
        let _ = runtime.serve_range(&request("obj")).await.unwrap();
        assert_eq!(backend.fetches.load(Ordering::SeqCst), 1);

        // Overwrite the source: new version + new content.
        *backend.version.lock().unwrap() = "v2".into();
        *backend.body.lock().unwrap() = Bytes::from_static(b"new-data");

        let after = runtime.serve_range(&request("obj")).await.unwrap();
        assert_eq!(after, Bytes::from_static(b"new-"), "must serve fresh bytes");
        assert_eq!(
            backend.fetches.load(Ordering::SeqCst),
            2,
            "overwrite must trigger a fresh backend fetch, not serve the stale block"
        );
        // The superseded v1 block is evicted on commit of v2, so only the fresh
        // version remains resident — version churn no longer accumulates stale
        // .blk files (issue #159).
        assert_eq!(runtime.block_count(), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn source_overwrite_replaces_old_version_in_both_tiers() {
        let root = tmp_root();
        let backend = Arc::new(VersionedBackend {
            version: std::sync::Mutex::new("v1".into()),
            body: std::sync::Mutex::new(Bytes::from_static(b"old-data")),
            fetches: AtomicUsize::new(0),
        });
        let runtime = WorkerRuntime::new_with_l1(
            WholeBlockStore::open(&root).unwrap(),
            Arc::new(BlockIndex::new()),
            Arc::new(InFlightLoads::new()),
            Arc::clone(&backend) as Arc<dyn BackendStore>,
            8,
            1024,
            16,
            8,
            WorkerMetrics::new(1024),
        )
        .with_version_ttl(Duration::ZERO);

        assert_eq!(
            runtime.serve_range(&request("versioned")).await.unwrap(),
            Bytes::from_static(b"old-")
        );
        assert_eq!(runtime.block_count(), 1);
        assert_eq!(runtime.l1_block_count(), 1);

        *backend.version.lock().unwrap() = "v2".into();
        *backend.body.lock().unwrap() = Bytes::from_static(b"new-data");
        assert_eq!(
            runtime.serve_range(&request("versioned")).await.unwrap(),
            Bytes::from_static(b"new-")
        );
        assert_eq!(runtime.block_count(), 1, "old L2 version must be removed");
        assert_eq!(
            runtime.l1_block_count(),
            1,
            "old L1 version must be removed"
        );
        assert_eq!(runtime.l1_resident_bytes(), 8);
        std::fs::remove_dir_all(root).ok();
    }

    struct MutableBackend {
        body: std::sync::Mutex<Option<Bytes>>,
        version: std::sync::Mutex<String>,
        fetches: AtomicUsize,
        puts: AtomicUsize,
        deletes: AtomicUsize,
    }

    #[async_trait]
    impl BackendStore for MutableBackend {
        async fn fetch_range(&self, _object: &ObjectId, _offset: u64, _len: u64) -> Result<Bytes> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            self.body
                .lock()
                .unwrap()
                .clone()
                .ok_or_else(|| Error::NotFound("deleted".into()))
        }

        async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
            let body = self.body.lock().unwrap();
            let body = body
                .as_ref()
                .ok_or_else(|| Error::NotFound("deleted".into()))?;
            Ok(ObjectStat {
                len: body.len() as u64,
                version: Version::new(self.version.lock().unwrap().clone()),
            })
        }

        async fn put(&self, _object: &ObjectId, body: Bytes) -> Result<Version> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            *self.body.lock().unwrap() = Some(body);
            *self.version.lock().unwrap() = "written-v2".into();
            Ok(Version::new("written-v2"))
        }

        async fn delete(&self, _object: &ObjectId) -> Result<()> {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            *self.body.lock().unwrap() = None;
            Ok(())
        }
    }

    #[tokio::test]
    async fn write_through_populates_l1_and_l2_then_delete_invalidates_both() {
        let root = tmp_root();
        let backend = Arc::new(MutableBackend {
            body: std::sync::Mutex::new(Some(Bytes::from_static(b"old-data"))),
            version: std::sync::Mutex::new("v1".into()),
            fetches: AtomicUsize::new(0),
            puts: AtomicUsize::new(0),
            deletes: AtomicUsize::new(0),
        });
        let runtime = WorkerRuntime::new_with_l1(
            WholeBlockStore::open(&root).unwrap(),
            Arc::new(BlockIndex::new()),
            Arc::new(InFlightLoads::new()),
            Arc::clone(&backend) as Arc<dyn BackendStore>,
            8,
            1024,
            16,
            8,
            WorkerMetrics::new(1024),
        );
        let object = ObjectId::new(Backend::Azure, "container", "mutable");

        assert_eq!(
            runtime
                .write_object(&object, Bytes::from_static(b"new-data"))
                .await
                .unwrap(),
            Version::new("written-v2")
        );
        assert_eq!(backend.puts.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.block_count(), 1);
        assert_eq!(runtime.l1_block_count(), 1);

        assert_eq!(
            runtime
                .serve_range(&RangeRequest {
                    object: object.clone(),
                    offset: 0,
                    len: 8,
                })
                .await
                .unwrap(),
            Bytes::from_static(b"new-data")
        );
        assert_eq!(
            backend.fetches.load(Ordering::SeqCst),
            0,
            "read-after-write must be an L1 hit"
        );

        runtime.delete_object(&object).await.unwrap();
        assert_eq!(backend.deletes.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.block_count(), 0);
        assert_eq!(runtime.l1_block_count(), 0);
        assert_eq!(runtime.l1_resident_bytes(), 0);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn missing_version_is_refused_not_cached_under_placeholder() {
        let root = tmp_root();
        let backend = Arc::new(VersionedBackend {
            version: std::sync::Mutex::new("   ".into()), // blank/whitespace etag
            body: std::sync::Mutex::new(Bytes::from_static(b"data")),
            fetches: AtomicUsize::new(0),
        });
        let runtime = runtime_with(Arc::clone(&backend), WorkerMetrics::new(1024), &root, 8);

        let err = runtime.serve_range(&request("obj")).await.unwrap_err();
        assert!(err.to_string().contains("no version"), "{err}");
        // Nothing was fetched or cached without a version.
        assert_eq!(backend.fetches.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.block_count(), 0);
        std::fs::remove_dir_all(root).ok();
    }

    /// A backend that counts HEADs and, when `enforce_precondition` is set, fails
    /// a fetch carrying a stale `If-Match` with [`Error::VersionMismatch`] — the
    /// 412 the real backends map — so the retry-on-mismatch path is exercised.
    struct CondBackend {
        version: std::sync::Mutex<String>,
        body: std::sync::Mutex<Bytes>,
        heads: AtomicUsize,
        fetches: AtomicUsize,
        enforce_precondition: bool,
    }

    #[async_trait]
    impl BackendStore for CondBackend {
        async fn fetch_range(&self, _object: &ObjectId, _offset: u64, _len: u64) -> Result<Bytes> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            Ok(self.body.lock().unwrap().clone())
        }

        async fn fetch_range_if_match(
            &self,
            object: &ObjectId,
            offset: u64,
            len: u64,
            if_match: Option<&Version>,
        ) -> Result<Bytes> {
            if self.enforce_precondition {
                if let Some(expected) = if_match {
                    let current = self.version.lock().unwrap().clone();
                    if expected.as_str() != current {
                        return Err(Error::VersionMismatch {
                            expected: expected.0.clone(),
                            found: current,
                        });
                    }
                }
            }
            self.fetch_range(object, offset, len).await
        }

        async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
            self.heads.fetch_add(1, Ordering::SeqCst);
            Ok(ObjectStat {
                len: self.body.lock().unwrap().len() as u64,
                version: Version::new(self.version.lock().unwrap().clone()),
            })
        }
    }

    fn cond_runtime(backend: Arc<CondBackend>, root: &PathBuf, ttl: Duration) -> WorkerRuntime {
        WorkerRuntime::new(
            WholeBlockStore::open(root).unwrap(),
            Arc::new(BlockIndex::new()),
            Arc::new(InFlightLoads::new()),
            backend as Arc<dyn BackendStore>,
            8,
            0,
            WorkerMetrics::new(1024),
        )
        .with_version_ttl(ttl)
    }

    #[tokio::test]
    async fn warm_read_within_ttl_skips_the_head() {
        // With a live version-cache TTL, a warm cache hit must not pay a backend
        // HEAD per read — only the first read resolves the version (issue #163).
        let root = tmp_root();
        let backend = Arc::new(CondBackend {
            version: std::sync::Mutex::new("v1".into()),
            body: std::sync::Mutex::new(Bytes::from_static(b"abcdefgh")),
            heads: AtomicUsize::new(0),
            fetches: AtomicUsize::new(0),
            enforce_precondition: false,
        });
        let runtime = cond_runtime(Arc::clone(&backend), &root, Duration::from_secs(60));

        for _ in 0..3 {
            let _ = runtime.serve_range(&request("obj")).await.unwrap();
        }
        assert_eq!(
            backend.heads.load(Ordering::SeqCst),
            1,
            "warm reads within the TTL must reuse the cached version, not re-HEAD"
        );
        assert_eq!(backend.fetches.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn precondition_failure_reresolves_and_retries_once() {
        // Read block0 first: resolves+caches version v1 and caches block0. The
        // source is then overwritten to v2 while the version cache still holds
        // v1. A read of block1 (not yet cached) is a miss that issues an
        // If-Match(v1) GET, which fails the precondition (412 -> VersionMismatch)
        // because the object is now v2. The runtime must invalidate the cached
        // version, re-resolve v2, and retry so it commits the fresh bytes under
        // the v2 key rather than surfacing the error (issue #163 TOCTOU).
        let root = tmp_root();
        let backend = Arc::new(CondBackend {
            version: std::sync::Mutex::new("v1".into()),
            body: std::sync::Mutex::new(Bytes::from_static(b"old-data-old-data")),
            heads: AtomicUsize::new(0),
            fetches: AtomicUsize::new(0),
            enforce_precondition: true,
        });
        let runtime = cond_runtime(Arc::clone(&backend), &root, Duration::from_secs(60));

        // block0 (offset 0): resolves v1, caches block0 under v1.
        let obj = ObjectId::new(Backend::Azure, "container", "obj");
        let read = |offset| RangeRequest {
            object: obj.clone(),
            offset,
            len: 4,
        };
        let first = runtime.serve_range(&read(0)).await.unwrap();
        assert_eq!(first, Bytes::from_static(b"old-"));

        // Overwrite the source; the version cache still holds v1.
        *backend.version.lock().unwrap() = "v2".into();
        *backend.body.lock().unwrap() = Bytes::from_static(b"new-data-new-data");

        // block1 (offset 8): a miss under the stale cached v1 -> If-Match(v1)
        // 412 -> re-resolve v2 -> refetch. Must serve the fresh v2 bytes.
        let after = runtime.serve_range(&read(8)).await.unwrap();
        assert_eq!(
            after,
            Bytes::from_static(b"new-"),
            "must re-resolve and serve fresh bytes after a precondition failure"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn capacity_enforcement_evicts_coldest_blocks() {
        // Each distinct object is one 8-byte block. With a 16-byte cap, reading
        // three distinct objects must leave only two resident: committing the
        // third evicts the coldest block back under capacity (issue #159).
        let root = tmp_root();
        let backend = Arc::new(RampBackend { block_size: 8 });
        let metrics = WorkerMetrics::new(1024);
        let runtime = WorkerRuntime::new(
            WholeBlockStore::open(&root).unwrap(),
            Arc::new(BlockIndex::new()),
            Arc::new(InFlightLoads::new()),
            Arc::clone(&backend) as Arc<dyn BackendStore>,
            8,
            16, // capacity: two 8-byte blocks
            metrics.clone(),
        );

        let read = |name: &str| RangeRequest {
            object: ObjectId::new(Backend::Azure, "c", name),
            offset: 0,
            len: 8,
        };
        // Read three distinct objects; the working set (24 bytes) exceeds the cap.
        for name in ["a", "b", "c"] {
            let _ = runtime.serve_range(&read(name)).await.unwrap();
        }
        // Only two blocks fit under the 16-byte cap.
        assert_eq!(runtime.block_count(), 2, "cache must stay under capacity");
        assert!(runtime.resident_bytes() <= 16);
        // At least one eviction was recorded.
        assert!(metrics.render().contains("talon_worker_evictions_total 1"));
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn zero_capacity_disables_eviction() {
        // capacity_bytes == 0 means unbounded: no block is ever evicted.
        let root = tmp_root();
        let backend = Arc::new(RampBackend { block_size: 8 });
        let runtime = WorkerRuntime::new(
            WholeBlockStore::open(&root).unwrap(),
            Arc::new(BlockIndex::new()),
            Arc::new(InFlightLoads::new()),
            Arc::clone(&backend) as Arc<dyn BackendStore>,
            8,
            0,
            WorkerMetrics::new(1024),
        );
        for name in ["a", "b", "c", "d"] {
            let req = RangeRequest {
                object: ObjectId::new(Backend::Azure, "c", name),
                offset: 0,
                len: 8,
            };
            let _ = runtime.serve_range(&req).await.unwrap();
        }
        assert_eq!(runtime.block_count(), 4, "no eviction with zero capacity");
        std::fs::remove_dir_all(root).ok();
    }
}
