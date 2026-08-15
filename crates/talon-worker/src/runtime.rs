//! Instrumented worker cache request runtime.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use talon_core::{
    Backend, BackendStore, BlockForm, BlockHandle, BlockId, BlockMeta, Error, ObjectId,
    ObjectStore, PageIndex, Version,
};
use talon_transport::data::{CachedBlockPutRequest, CachedRangeRequest, RangeRequest};
use talon_transport::frame::{HEADER_LEN, MAX_PAYLOAD_LEN};
use talon_transport::{codec, ControlMessage, ObjectEntry, MAX_CONTROL_PAYLOAD_LEN};

use crate::data_error::CacheMiss;
use crate::{
    miss::touched_pages, BlockIndex, CacheUnit, InFlightLoads, LoadKey, Lru, MemoryInsert,
    MemoryStore, PagedBlockStore, Presence, WholeBlockStore, WorkerMetrics,
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

/// Smallest request that is worth serving with sendfile when the pages are
/// already resident in L1.
///
/// Zero-copy is not free: it costs one `sendfile` per page touched, plus an
/// `openat` on an fd-cache miss. When L1 already holds the bytes, the byte path
/// answers from DRAM with a single small copy and no per-page syscalls, so
/// below some size the copy it avoids is cheaper than the syscalls it adds.
///
/// Measured on a read straddling a 1 MiB page boundary, with the paged store's
/// fd cache warm (byte path vs sendfile): 4 KiB 215 vs 227 us, 16 KiB 213 vs
/// 222 us, 64 KiB 234 vs 233 us, 256 KiB 364 vs 330 us, 1 MiB 843 vs 736 us.
/// The crossover sits just under 32 KiB; the small-read deficit is ~5% and the
/// large-read gain reaches 22%.
///
/// This threshold used to be 256 KiB. Giving the paged store an fd cache
/// removed the per-page `openat` and moved the crossover down by 8x, which is
/// what the fixed cost of zero-copy was mostly made of.
///
/// With L1 off there is no such tradeoff — the byte path must hit the disk
/// anyway — so sendfile is used at every size.
const L1_SENDFILE_MIN_LEN: u64 = 32 << 10;

/// A per-object resolved version with the instant it was resolved.
struct CachedVersion {
    version: Version,
    /// Total object length reported by the same `HEAD`, so the paged path can
    /// compute a block's logical length (the last block is usually short)
    /// without a second round trip.
    object_len: u64,
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
    /// Serve these segments with one `sendfile` each, in order. A paged block
    /// keeps every page in its own file, so a read spanning N pages is N
    /// contiguous runs rather than one — still zero-copy, just N calls.
    SendfileMany(Vec<BlockHandle>),
    /// Serve these already-in-memory bytes (miss just fetched, or a stitched
    /// multi-block read).
    Bytes(bytes::Bytes),
}

/// Shared state required to serve instrumented data-plane range requests.
pub struct WorkerRuntime {
    /// Fine-grained DRAM page cache. L1 is inclusive: every page has an L2
    /// whole-block parent.
    l1: Arc<MemoryStore>,
    /// Persistent local-NVMe cache.
    store: WholeBlockStore,
    /// Per-page L2 cache, enabled when the worker runs in paged mode. When set,
    /// a miss materializes only the pages a read touches instead of the whole
    /// (256 MiB default) block, and eviction reclaims individual pages.
    paged: Option<PagedBlockStore>,
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
        l1_page_size_bytes: u64,
        metrics: WorkerMetrics,
    ) -> Self {
        let lru = Arc::new(Lru::new());
        for (id, page, bytes) in index.snapshot_units() {
            let unit = match page {
                Some(page) => CacheUnit::Page(id, page),
                None => CacheUnit::Whole(id),
            };
            lru.insert(unit, bytes);
        }
        let l1 = Arc::new(MemoryStore::with_limits(
            l1_capacity_bytes,
            l1_page_size_bytes,
        ));
        metrics.set_l1_capacity(l1_capacity_bytes);
        metrics.update_l1_residency(0, 0);
        Self {
            l1,
            store,
            paged: None,
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

    /// Enable paged L2: cache individual pages instead of whole blocks.
    ///
    /// With this set, a read that misses fetches only the pages it touches from
    /// the origin and commits them as per-page files, so a point query into a
    /// 256 MiB block costs one page (256 KiB by default) of backend traffic and
    /// of local disk rather than the whole block. Eviction reclaims individual
    /// pages, leaving the block's other pages intact.
    pub fn with_paged_store(mut self, paged: PagedBlockStore) -> Self {
        self.paged = Some(paged);
        self
    }

    /// The paged L2 page size, or `None` when paged mode is off.
    fn paged_page_size(&self) -> Option<u32> {
        self.paged.as_ref().map(|p| p.page_size())
    }

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

    /// Serve a versioned range only from resident cache state.
    ///
    /// Unlike [`serve`](Self::serve), this path neither resolves metadata nor
    /// invokes `BackendStore`. A partially resident multi-block/page request is
    /// a complete miss; no prefix is returned.
    pub async fn serve_cached(&self, request: &CachedRangeRequest) -> anyhow::Result<bytes::Bytes> {
        self.ensure_configured_backend(request.object.backend)?;
        if request.len == 0 {
            return Ok(bytes::Bytes::new());
        }
        if request.len > u64::from(MAX_PAYLOAD_LEN) {
            anyhow::bail!(
                "cache-only range length {} exceeds response frame limit {}",
                request.len,
                MAX_PAYLOAD_LEN
            );
        }
        let end = request
            .offset
            .checked_add(request.len)
            .ok_or_else(|| anyhow::anyhow!("range offset+len overflows u64"))?;
        let block_size = u64::from(self.block_size);
        let mut output = bytes::BytesMut::with_capacity(request.len as usize);
        let mut cursor = request.offset;
        while cursor < end {
            let block = self.block_for(&request.object, cursor, &request.version);
            let offset = cursor - block.offset;
            let take = (block.offset + block_size).min(end) - cursor;
            let Some(meta) = self.index.get(&block) else {
                return Err(CacheMiss.into());
            };
            let piece = match meta.form {
                BlockForm::Whole => self
                    .cached_block_range(&block, offset, take)
                    .await?
                    .ok_or(CacheMiss)?,
                BlockForm::Paged { page_size, .. } => {
                    let available = available_range_len(meta.len, offset, take)?;
                    if available != take {
                        return Err(CacheMiss.into());
                    }
                    let mut piece = bytes::BytesMut::with_capacity(take as usize);
                    for page in touched_pages(offset, take, page_size) {
                        let page_start = u64::from(page.0) * u64::from(page_size);
                        let page_len = talon_core::page_len(meta.len, page_size, page);
                        let from = offset.max(page_start);
                        let to = (offset + take).min(page_start + page_len);
                        if to <= from {
                            continue;
                        }
                        let bytes = self.cached_page(&block, page).await?.ok_or(CacheMiss)?;
                        piece.extend_from_slice(&slice(&bytes, from - page_start, to - from)?);
                    }
                    self.metrics.record_cache_hit();
                    piece.freeze()
                }
            };
            if piece.len() != take as usize {
                return Err(CacheMiss.into());
            }
            output.extend_from_slice(&piece);
            cursor += take;
        }
        debug_assert_eq!(request.len as usize, output.len());
        Ok(output.freeze())
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

        // With L1 enabled, use the page-granular byte path. With L1 disabled,
        // preserve the whole-block L2 sendfile fast path.
        if end <= start_block + block_size {
            let block = self.block_for(&request.object, request.offset, version);
            let offset_in_block = request.offset - block.offset;
            // Paged mode: pages live in their own files, so a read spanning N
            // pages is N contiguous runs. Each run is still `sendfile`-able, so
            // serve the whole span zero-copy rather than stitching pages in
            // userspace.
            //
            // With L1 on, this is only safe when every covered page is already
            // L1-resident: the byte path is what admits pages into L1, and
            // sendfile never brings bytes through userspace to admit. Serving a
            // non-resident page here would silently stop populating L1.
            //
            // Even when resident, sendfile is only worth it above
            // `L1_SENDFILE_MIN_LEN` — see that constant.
            if let Some(paged) = &self.paged {
                let page_size = u64::from(paged.page_size());
                let first = PageIndex((offset_in_block / page_size) as u32);
                let last = PageIndex(((offset_in_block + request.len - 1) / page_size) as u32);
                let l1_ok = !self.l1.is_enabled()
                    || ((first.0..=last.0).all(|p| self.l1.contains_page(&block, PageIndex(p)))
                        && request.len >= L1_SENDFILE_MIN_LEN);
                if request.len > 0
                    && l1_ok
                    && matches!(
                        self.index.presence(&block, first, PageIndex(last.0 + 1)),
                        Presence::PageHit
                    )
                {
                    match paged.get_range(&block, offset_in_block, request.len) {
                        Ok(handles) if !handles.is_empty() => {
                            let served: u64 = handles.iter().map(|h| h.len).sum();
                            // Only take the fast path when the handles cover the
                            // request exactly; a short cover means we raced an
                            // eviction, so fall through and re-fetch.
                            if served == request.len {
                                self.metrics.record_l2_hit();
                                self.metrics.record_cache_hit();
                                for p in first.0..=last.0 {
                                    self.lru
                                        .touch(&CacheUnit::Page(block.clone(), PageIndex(p)));
                                }
                                tracing::info!(
                                    block = %block,
                                    first_page = first.0,
                                    pages = handles.len(),
                                    tier = "l2",
                                    "HIT (sendfile)"
                                );
                                return Ok(if handles.len() == 1 {
                                    let mut handles = handles;
                                    ServeOutcome::Sendfile(handles.pop().expect("one handle"))
                                } else {
                                    ServeOutcome::SendfileMany(handles)
                                });
                            }
                        }
                        // Lost the race with page eviction, or an absent page;
                        // fall through to the byte path, which re-fetches.
                        Ok(_) | Err(_) => {}
                    }
                }
            }
            if self.l1.is_enabled() {
                return Ok(ServeOutcome::Bytes(
                    self.block_range_bytes(request, &block, offset_in_block, request.len)
                        .await?,
                ));
            }
            if matches!(
                self.index.presence(&block, PageIndex(0), PageIndex(1)),
                Presence::Whole
            ) {
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

            return Ok(ServeOutcome::Bytes(
                self.block_range_bytes(request, &block, offset_in_block, request.len)
                    .await?,
            ));
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
            return self
                .block_range_bytes(request, &block, offset_in_block, request.len)
                .await;
        }

        // Slow path: stitch across blocks.
        let mut out = bytes::BytesMut::with_capacity(request.len as usize);
        let mut cursor = request.offset;
        while cursor < end {
            let block = self.block_for(&request.object, cursor, version);
            let offset_in_block = cursor - block.offset;
            let block_end = block.offset + block_size;
            let take = block_end.min(end) - cursor;
            let piece = self
                .block_range_bytes(request, &block, offset_in_block, take)
                .await?;
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
        self.store_version(object, &stat.version, stat.len);
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
        self.store_version(object, &stat.version, stat.len);
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

    /// Total object length recorded alongside a cached version, if any.
    ///
    /// The paged path uses this to size the last (short) block of an object, so
    /// it never charges capacity for — or tries to read — pages past EOF.
    fn cached_object_len(&self, object: &ObjectId, version: &Version) -> Option<u64> {
        let cache = self.version_cache.lock().unwrap();
        let entry = cache.get(object)?;
        (entry.version == *version).then_some(entry.object_len)
    }

    /// Record a freshly-resolved version for `object`.
    fn store_version(&self, object: &ObjectId, version: &Version, object_len: u64) {
        self.version_cache.lock().unwrap().insert(
            object.clone(),
            CachedVersion {
                version: version.clone(),
                object_len,
                resolved_at: Instant::now(),
            },
        );
    }

    /// Drop any cached version for `object` (after a precondition failure).
    fn invalidate_version(&self, object: &ObjectId) {
        self.version_cache.lock().unwrap().remove(object);
    }

    /// Return one block-relative range, using L1 pages, then an aligned L2 read,
    /// then a whole-block origin fill.
    ///
    /// Concurrent misses for the same block are deduplicated: the first caller
    /// (the leader, holding an `InFlightGuard`) performs the backend fetch; the
    /// rest wait for it and then serve from the now-warm cache, so N concurrent
    /// misses trigger a single backend fetch instead of N (issue #113). The
    /// guard clears the in-flight marker on drop, so a cancelled or panicking
    /// leader can never orphan the key and hang the waiters (issue #162).
    async fn block_range_bytes(
        &self,
        request: &RangeRequest,
        block: &BlockId,
        offset: u64,
        len: u64,
    ) -> anyhow::Result<bytes::Bytes> {
        if self.paged.is_some() {
            return self.paged_block_range(request, block, offset, len).await;
        }
        if let Some(bytes) = self.cached_block_range(block, offset, len).await? {
            return Ok(bytes);
        }

        self.metrics.record_cache_miss();
        self.load_block_range(request, block, offset, len).await
    }

    /// Serve a block-relative range in paged mode: L1, then per-page L2 files,
    /// then a per-page origin fetch for whatever is still absent.
    ///
    /// Unlike the whole-block path this never materializes the full block: only
    /// the pages the read actually touches are fetched and committed, so a point
    /// query costs one page rather than 256 MiB of backend traffic and disk.
    async fn paged_block_range(
        &self,
        request: &RangeRequest,
        block: &BlockId,
        offset: u64,
        len: u64,
    ) -> anyhow::Result<bytes::Bytes> {
        let page_size = self
            .paged_page_size()
            .ok_or_else(|| anyhow::anyhow!("paged read on a runtime without a paged store"))?;

        // A write-through commits the whole block as one file (read-after-write
        // must hit). Serve such a block from the whole-block store rather than
        // re-fetching it a page at a time from the origin.
        if matches!(
            self.index.presence(block, PageIndex(0), PageIndex(1)),
            Presence::Whole
        ) {
            if let Some(bytes) = self.cached_block_range(block, offset, len).await? {
                return Ok(bytes);
            }
        }

        let block_len = self.block_len(&request.object, block).await?;
        let len = available_range_len(block_len, offset, len)?;
        if len == 0 {
            return Ok(bytes::Bytes::new());
        }

        // Ensure the block has an index entry so page presence can be tracked.
        // Idempotent: a concurrent reader's entry (and its bitmap) is preserved.
        self.index.init_paged(block.clone(), page_size, block_len);

        let mut out = bytes::BytesMut::with_capacity(len as usize);
        let mut hit_all = true;
        for page in touched_pages(offset, len, page_size) {
            let page_start = u64::from(page.0) * u64::from(page_size);
            let page_bytes_len = talon_core::page_len(block_len, page_size, page);
            // Intersect the requested window with this page's span.
            let from = offset.max(page_start);
            let to = (offset + len).min(page_start + page_bytes_len);
            if to <= from {
                continue;
            }
            let (page_bytes, hit) = self.page_bytes(request, block, page, block_len).await?;
            hit_all &= hit;
            out.extend_from_slice(&slice(&page_bytes, from - page_start, to - from)?);
        }
        if hit_all {
            self.metrics.record_cache_hit();
        } else {
            self.metrics.record_cache_miss();
        }
        Ok(out.freeze())
    }

    /// Return one page's bytes, and whether it was served from cache.
    ///
    /// Tries L1, then the on-disk page file, then a deduplicated origin fetch of
    /// just that page. Concurrent misses for the same `(block, page)` collapse
    /// to a single backend request via [`LoadKey::Page`], mirroring the
    /// whole-block leader/follower protocol (issues #113, #162).
    async fn page_bytes(
        &self,
        request: &RangeRequest,
        block: &BlockId,
        page: PageIndex,
        block_len: u64,
    ) -> anyhow::Result<(bytes::Bytes, bool)> {
        if let Some(bytes) = self.cached_page(block, page).await? {
            return Ok((bytes, true));
        }
        let key = LoadKey::Page(block.clone(), page);
        match self.inflight.admit_owned(key.clone()) {
            Some(guard) => {
                let result = self
                    .fetch_and_commit_page(request, block, page, block_len)
                    .await;
                drop(guard);
                Ok((result?, false))
            }
            None => {
                // A peer is fetching this exact page; wait and serve from cache.
                self.inflight.wait(&key).await;
                if let Some(bytes) = self.cached_page(block, page).await? {
                    return Ok((bytes, true));
                }
                // The leader's load failed; fetch it ourselves rather than
                // looping on admission, which could wait unboundedly.
                let bytes = self
                    .fetch_and_commit_page(request, block, page, block_len)
                    .await?;
                Ok((bytes, false))
            }
        }
    }

    /// Return a page from L1 or the on-disk page file, promoting L2 hits to L1.
    async fn cached_page(
        &self,
        block: &BlockId,
        page: PageIndex,
    ) -> anyhow::Result<Option<bytes::Bytes>> {
        if self.l1.is_enabled() {
            if let Some(bytes) = self.l1.get_page(block, page) {
                self.metrics.record_l1_hit();
                self.lru.touch(&CacheUnit::Page(block.clone(), page));
                tracing::debug!(block = %block, page = page.0, tier = "l1", "HIT");
                return Ok(Some(bytes));
            }
            self.metrics.record_l1_miss();
        }
        if !matches!(
            self.index.presence(block, page, PageIndex(page.0 + 1)),
            Presence::PageHit
        ) {
            self.metrics.record_l2_miss();
            return Ok(None);
        }
        let paged = self.paged.as_ref().expect("paged store");
        match paged.get_page_bytes(block, page).await {
            Ok(bytes) => {
                self.metrics.record_l2_hit();
                self.lru.touch(&CacheUnit::Page(block.clone(), page));
                tracing::debug!(block = %block, page = page.0, tier = "l2", "HIT");
                if self.l1.is_enabled() {
                    self.admit_l1_page(block, page, bytes.clone());
                }
                Ok(Some(bytes))
            }
            Err(Error::NotFound(_)) => {
                // Index said present but the file is gone — an eviction race.
                // Drop the stale bit so the miss path re-fetches it.
                self.index.clear_page(block, page);
                self.lru.remove(&CacheUnit::Page(block.clone(), page));
                self.metrics.record_l2_miss();
                Ok(None)
            }
            Err(error) => Err(anyhow::anyhow!("read cached page: {error}")),
        }
    }

    /// Fetch one page from the origin and commit it as a per-page file.
    async fn fetch_and_commit_page(
        &self,
        request: &RangeRequest,
        block: &BlockId,
        page: PageIndex,
        block_len: u64,
    ) -> anyhow::Result<bytes::Bytes> {
        let page_size = self.paged_page_size().expect("paged store");
        let page_start = u64::from(page.0) * u64::from(page_size);
        let want = talon_core::page_len(block_len, page_size, page);
        tracing::info!(block = %block, page = page.0, "MISS -> backend page fetch");
        let started = Instant::now();
        // Same If-Match precondition as the whole-block path: an overwrite
        // between version resolution and this GET must be rejected rather than
        // committing newer bytes under the older version's key (issue #163).
        let fetched = self
            .backend
            .fetch_range_if_match(
                &request.object,
                block.offset + page_start,
                want,
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

        let paged = self.paged.as_ref().expect("paged store");
        // Record the block's identity once so a restart can rebuild its index
        // entry from the page files on disk.
        if let Err(error) = paged.write_sidecar(block, block_len) {
            tracing::warn!(block = %block, %error, "failed to write paged sidecar");
        }
        paged
            .put_page_async(block, page, bytes.clone())
            .await
            .map_err(|error| anyhow::anyhow!("commit page failed: {error}"))?;
        self.index.init_paged(block.clone(), page_size, block_len);
        self.index.mark_page(block, page);

        let unit = CacheUnit::Page(block.clone(), page);
        self.lru.insert(unit.clone(), bytes.len() as u64);
        self.lru.pin(&unit);
        let superseded = self.lru.evict_superseded(block);
        self.unlink_units(superseded).await;
        self.enforce_capacity().await;
        self.lru.unpin(&unit);
        if !self.l1.remove_superseded(block).is_empty() {
            self.refresh_l1_metrics();
        }
        if self.l1.is_enabled() {
            self.admit_l1_page(block, page, bytes.clone());
        }
        tracing::info!(block = %block, page = page.0, bytes = bytes.len(), "committed page");
        Ok(bytes)
    }

    /// Admit one page into L1, recording admissions and evictions.
    fn admit_l1_page(&self, block: &BlockId, page: PageIndex, bytes: bytes::Bytes) {
        match self.l1.insert_page(block.clone(), page, bytes) {
            MemoryInsert::Inserted { evicted } => {
                self.metrics.record_l1_admission();
                for victim in evicted {
                    self.metrics.record_l1_eviction();
                    tracing::debug!(
                        block = %victim.block,
                        page = victim.page.0,
                        tier = "l1",
                        "evicted page"
                    );
                }
                self.refresh_l1_metrics();
            }
            MemoryInsert::Disabled | MemoryInsert::TooLarge => {}
        }
    }

    /// The logical byte length of `block` — `block_size`, except for an object's
    /// last block, which is short.
    ///
    /// Uses the length recorded alongside the cached version when available, so
    /// a warm read pays no extra `HEAD`; falls back to the index entry, then to
    /// a `HEAD`.
    async fn block_len(&self, object: &ObjectId, block: &BlockId) -> anyhow::Result<u64> {
        let block_size = self.block_size as u64;
        if let Some(meta) = self.index.get(block) {
            return Ok(meta.len);
        }
        let object_len = match self.cached_object_len(object, &block.version) {
            Some(len) => len,
            None => {
                let stat =
                    self.backend.head(object).await.map_err(|error| {
                        anyhow::anyhow!("resolve object length (HEAD): {error}")
                    })?;
                self.store_version(object, &stat.version, stat.len);
                stat.len
            }
        };
        Ok(object_len.saturating_sub(block.offset).min(block_size))
    }

    /// Load one block after both L1 and L2 have missed.
    async fn load_block_range(
        &self,
        request: &RangeRequest,
        block: &BlockId,
        offset: u64,
        len: u64,
    ) -> anyhow::Result<bytes::Bytes> {
        let key = LoadKey::Whole(block.clone());
        match self.inflight.admit_owned(key.clone()) {
            Some(guard) => {
                // Leader: fetch and commit; the guard wakes waiters on drop
                // (including on cancellation/panic).
                let result = self.fetch_and_commit(request, block).await;
                drop(guard);
                let bytes = result?;
                self.range_from_fetched(block, bytes, offset, len)
            }
            None => {
                // A peer is already fetching this block; wait for it and serve
                // from cache rather than issuing a duplicate backend fetch.
                self.inflight.wait(&key).await;
                if let Some(bytes) = self.cached_block_range(block, offset, len).await? {
                    return Ok(bytes);
                }
                // The leader's load failed (marker cleared, block still absent).
                // Try to become the leader ourselves.
                match self.inflight.admit_owned(key.clone()) {
                    Some(guard) => {
                        let result = self.fetch_and_commit(request, block).await;
                        drop(guard);
                        let bytes = result?;
                        self.range_from_fetched(block, bytes, offset, len)
                    }
                    None => {
                        // Another peer already restarted the load; wait once
                        // more, then, if still absent, fetch without holding
                        // admission to avoid an unbounded wait loop.
                        self.inflight.wait(&key).await;
                        if let Some(bytes) = self.cached_block_range(block, offset, len).await? {
                            return Ok(bytes);
                        }
                        let bytes = self.fetch_and_commit(request, block).await?;
                        self.range_from_fetched(block, bytes, offset, len)
                    }
                }
            }
        }
    }

    /// Return a block range from the local cache if its L2 parent is resident.
    async fn cached_block_range(
        &self,
        block: &BlockId,
        offset: u64,
        len: u64,
    ) -> anyhow::Result<Option<bytes::Bytes>> {
        let Some(meta) = self.index.get(block) else {
            if self.l1.is_enabled() {
                self.metrics.record_l1_miss();
            }
            self.metrics.record_l2_miss();
            return Ok(None);
        };
        if !matches!(meta.form, BlockForm::Whole) {
            self.metrics.record_l2_miss();
            return Ok(None);
        }
        let len = available_range_len(meta.len, offset, len)?;
        if self.l1.is_enabled() {
            match self.l1.get_range(block, offset, len) {
                Some(bytes) => {
                    self.metrics.record_l1_hit();
                    self.metrics.record_cache_hit();
                    self.lru.touch(&CacheUnit::Whole(block.clone()));
                    tracing::debug!(block = %block, offset, len, tier = "l1", "HIT");
                    return Ok(Some(bytes));
                }
                None => {
                    self.metrics.record_l1_miss();
                }
            }
        }

        self.record_l2_hit(block);
        tracing::debug!(block = %block, offset, len, tier = "l2", "HIT");
        if !self.l1.is_enabled() {
            let bytes = match self.store.get_range_bytes(block, offset, len).await {
                Ok(bytes) => bytes,
                Err(Error::NotFound(_)) => {
                    self.forget_missing_l2_parent(block);
                    return Ok(None);
                }
                Err(error) => {
                    return Err(anyhow::anyhow!("read committed block range: {error}"));
                }
            };
            return Ok(Some(bytes));
        }

        let (page_start, page_len) = self.page_window(offset, len, meta.len)?;
        let pages = match self
            .store
            .get_range_bytes(block, page_start, page_len)
            .await
        {
            Ok(bytes) => bytes,
            Err(Error::NotFound(_)) => {
                self.forget_missing_l2_parent(block);
                return Ok(None);
            }
            Err(error) => return Err(anyhow::anyhow!("read committed L2 pages: {error}")),
        };
        self.admit_l1_pages(block, page_start, pages.clone());
        Ok(Some(slice(&pages, offset - page_start, len)?))
    }

    /// Drop stale metadata after an index-hit/file-miss eviction race.
    fn forget_missing_l2_parent(&self, block: &BlockId) {
        self.invalidate_l1(block);
        self.index.remove(block);
        self.lru.remove(&CacheUnit::Whole(block.clone()));
        tracing::debug!(%block, "discarded stale L2 index entry after file miss");
    }

    /// Record an L2 hit and touch its capacity LRU.
    fn record_l2_hit(&self, block: &BlockId) {
        self.metrics.record_l2_hit();
        self.metrics.record_cache_hit();
        self.lru.touch(&CacheUnit::Whole(block.clone()));
    }

    fn page_window(&self, offset: u64, len: u64, block_len: u64) -> anyhow::Result<(u64, u64)> {
        if len == 0 {
            return Ok((offset, 0));
        }
        let page_size = self.l1.page_size_bytes();
        let end = offset
            .checked_add(len)
            .ok_or_else(|| anyhow::anyhow!("range offset+len overflows u64"))?;
        let start = (offset / page_size) * page_size;
        let aligned_end = end
            .div_ceil(page_size)
            .saturating_mul(page_size)
            .min(block_len);
        Ok((start, aligned_end.saturating_sub(start)))
    }

    /// Split an aligned byte window into pages and publish L1 residency changes.
    fn admit_l1_pages(&self, block: &BlockId, start: u64, bytes: bytes::Bytes) {
        if !self.l1.is_enabled() || bytes.is_empty() {
            return;
        }
        let page_size = self.l1.page_size_bytes();
        debug_assert_eq!(start % page_size, 0);
        let first_page = start / page_size;
        let mut chunk_start = 0_usize;
        let Ok(page_size_usize) = usize::try_from(page_size) else {
            return;
        };
        let mut relative = 0_u64;
        while chunk_start < bytes.len() {
            let chunk_end = (chunk_start + page_size_usize).min(bytes.len());
            let page_number = first_page + relative;
            let Ok(page_number) = u32::try_from(page_number) else {
                break;
            };
            match self.l1.insert_page(
                block.clone(),
                PageIndex(page_number),
                bytes.slice(chunk_start..chunk_end),
            ) {
                MemoryInsert::Inserted { evicted } => {
                    self.metrics.record_l1_admission();
                    for victim in evicted {
                        self.metrics.record_l1_eviction();
                        tracing::debug!(
                            block = %victim.block,
                            page = victim.page.0,
                            tier = "l1",
                            "evicted page"
                        );
                    }
                }
                MemoryInsert::Disabled | MemoryInsert::TooLarge => {}
            }
            chunk_start = chunk_end;
            relative += 1;
        }
        self.refresh_l1_metrics();
    }

    fn range_from_fetched(
        &self,
        block: &BlockId,
        bytes: bytes::Bytes,
        offset: u64,
        len: u64,
    ) -> anyhow::Result<bytes::Bytes> {
        let len = available_range_len(bytes.len() as u64, offset, len)?;
        if self.l1.is_enabled() && len > 0 {
            let (page_start, page_len) = self.page_window(offset, len, bytes.len() as u64)?;
            let pages = slice(&bytes, page_start, page_len)?;
            self.admit_l1_pages(block, page_start, pages);
        }
        slice(&bytes, offset, len)
    }

    fn invalidate_l1(&self, block: &BlockId) {
        if !self.l1.remove_block(block).is_empty() {
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

        self.commit_cached_block(block, bytes.clone()).await?;
        Ok(bytes)
    }

    async fn commit_cached_block(
        &self,
        block: &BlockId,
        bytes: bytes::Bytes,
    ) -> anyhow::Result<()> {
        let len = bytes.len() as u64;
        self.store
            .put(block, bytes)
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
        tracing::info!(block = %block, bytes = len, "committed block");
        Ok(())
    }

    /// Validate a gateway cache admission before its raw body is read.
    pub fn validate_cached_block_admission(
        &self,
        request: &CachedBlockPutRequest,
    ) -> anyhow::Result<()> {
        self.ensure_configured_backend(request.block.object.backend)?;
        let block_size = u64::from(self.block_size);
        if request.block.block_size != self.block_size {
            anyhow::bail!(
                "admission block size {} does not match worker block size {}",
                request.block.block_size,
                self.block_size
            );
        }
        if request.block.offset % block_size != 0 {
            anyhow::bail!(
                "admission block offset {} is not aligned",
                request.block.offset
            );
        }
        if request.block.offset >= request.object_len {
            anyhow::bail!(
                "admission block offset {} is outside object length {}",
                request.block.offset,
                request.object_len
            );
        }
        let expected = block_size.min(request.object_len - request.block.offset);
        if request.body_len != expected {
            anyhow::bail!(
                "admission body length {} does not match complete block length {}",
                request.body_len,
                expected
            );
        }
        Ok(())
    }

    /// Admit one complete versioned block without consulting the backend.
    pub async fn admit_cached_block(
        &self,
        request: &CachedBlockPutRequest,
        body: bytes::Bytes,
    ) -> anyhow::Result<()> {
        self.validate_cached_block_admission(request)?;
        if body.len() as u64 != request.body_len {
            anyhow::bail!(
                "admission body length changed: expected {}, received {}",
                request.body_len,
                body.len()
            );
        }
        self.commit_cached_block(&request.block, body).await
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
        self.store_version(object, &version, body.len() as u64);
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
        self.admit_l1_pages(&block, 0, body);
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
        self.store_version(object, &version, len);
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
            match unit {
                CacheUnit::Whole(id) => {
                    self.invalidate_l1(&id);
                    if let Err(error) = self.store.delete(&id).await {
                        tracing::warn!(block = %id, %error, "failed to unlink evicted block");
                    }
                    // A paged block's directory shares the block identity; drop
                    // it too so a whole-block eviction cannot leave orphaned
                    // pages behind.
                    if let Some(paged) = &self.paged {
                        if let Err(error) = paged.delete_block_async(&id).await {
                            tracing::warn!(block = %id, %error, "failed to unlink evicted pages");
                        }
                    }
                    self.index.remove(&id);
                    self.metrics.record_eviction();
                    tracing::info!(block = %id, "evicted block");
                }
                CacheUnit::Page(id, page) => {
                    // Page-level eviction: unlink just this page file and clear
                    // its bit. The block entry and its other pages stay intact.
                    self.l1.remove_page(&id, page);
                    if let Some(paged) = &self.paged {
                        if let Err(error) = paged.evict_page_async(&id, page).await {
                            tracing::warn!(
                                block = %id, page = page.0, %error,
                                "failed to unlink evicted page"
                            );
                        }
                    }
                    self.index.clear_page(&id, page);
                    self.metrics.record_eviction();
                    self.refresh_l1_metrics();
                    tracing::info!(block = %id, page = page.0, "evicted page");
                    // Evicting the last page leaves an empty entry that would
                    // otherwise linger in the index (and its `.pages` directory
                    // on disk) forever. Drop both once nothing is resident.
                    if self
                        .index
                        .get(&id)
                        .is_some_and(|meta| meta.resident_bytes() == 0)
                    {
                        if let Some(paged) = &self.paged {
                            if let Err(error) = paged.delete_block_async(&id).await {
                                tracing::warn!(
                                    block = %id, %error,
                                    "failed to remove emptied paged block directory"
                                );
                            }
                        }
                        self.index.remove(&id);
                        tracing::debug!(block = %id, "dropped paged block with no resident pages");
                    }
                }
            }
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

    /// Number of pages resident in L1.
    pub fn l1_page_count(&self) -> u64 {
        self.l1.len() as u64
    }

    /// Bytes resident in L1.
    pub fn l1_resident_bytes(&self) -> u64 {
        self.l1.resident_bytes()
    }

    /// The block covering `offset` of `object`, at the currently cached
    /// version.
    ///
    /// Exposed for benchmarks that need to name a specific page of a warmed
    /// span. Returns `None` if the object's version has not been resolved yet.
    #[doc(hidden)]
    pub fn block_for_bench(&self, object: &ObjectId, offset: u64) -> Option<BlockId> {
        let version = self.cached_version(object)?;
        Some(self.block_for(object, offset, &version))
    }

    /// Drop one page from L1, leaving L2 untouched. Returns whether it was
    /// resident.
    ///
    /// Exposed for tests that need to construct a *partially* L1-resident span
    /// — the state a real worker reaches whenever L1 evicts under pressure
    /// while L2 still holds every page.
    #[doc(hidden)]
    pub fn l1_drop_page_for_test(&self, block: &BlockId, page: PageIndex) -> bool {
        self.l1.remove_page(block, page)
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

fn available_range_len(block_len: u64, offset: u64, requested: u64) -> anyhow::Result<u64> {
    if offset > block_len {
        anyhow::bail!("offset {offset} beyond block length {block_len} bytes");
    }
    Ok(requested.min(block_len - offset))
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
        l1_page_size: u64,
    ) -> WorkerRuntime {
        WorkerRuntime::new_with_l1(
            WholeBlockStore::open(root).unwrap(),
            Arc::new(BlockIndex::new()),
            Arc::new(InFlightLoads::new()),
            backend,
            8,
            l2_capacity,
            l1_capacity,
            l1_page_size,
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
            ServeOutcome::Sendfile(_) | ServeOutcome::SendfileMany(_) => {
                panic!("origin miss must return fetched bytes")
            }
        }
        assert_eq!(runtime.l1_page_count(), 1);
        assert_eq!(runtime.l1_resident_bytes(), 8);

        match runtime.serve(&request("l1")).await.unwrap() {
            ServeOutcome::Bytes(bytes) => assert_eq!(bytes, Bytes::from_static(b"abcd")),
            ServeOutcome::Sendfile(_) | ServeOutcome::SendfileMany(_) => {
                panic!("eligible warm block must hit L1")
            }
        }
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        let rendered = metrics.render();
        assert!(rendered.contains("talon_worker_cache_tier_hits_total{tier=\"l1\"} 1"));
        assert!(rendered.contains("talon_worker_cache_tier_misses_total{tier=\"l1\"} 1"));
        assert!(rendered.contains("talon_worker_cache_tier_misses_total{tier=\"l2\"} 1"));
        assert!(rendered.contains("talon_worker_l1_admissions_total 1"));
        assert!(rendered.contains("talon_worker_l1_pages 1"));
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
        assert_eq!(runtime.l1_page_count(), 0);
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
        assert_eq!(runtime.l1_page_count(), 1);
        assert_eq!(runtime.l1_resident_bytes(), 8);
        match runtime.serve(&request("boundary")).await.unwrap() {
            ServeOutcome::Bytes(bytes) => assert_eq!(bytes, Bytes::from_static(b"abcd")),
            ServeOutcome::Sendfile(_) | ServeOutcome::SendfileMany(_) => {
                panic!("entry at the limit must be admitted to L1")
            }
        }
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn block_larger_than_l1_still_caches_its_hot_page() {
        let root = tmp_root();
        let backend = Arc::new(MockBackend {
            calls: AtomicUsize::new(0),
        });
        let metrics = WorkerMetrics::new(1024);
        let runtime = runtime_l1(Arc::clone(&backend), metrics.clone(), &root, 1024, 16, 4);

        let _ = runtime.serve(&request("large")).await.unwrap();
        assert_eq!(runtime.l1_page_count(), 1);
        match runtime.serve(&request("large")).await.unwrap() {
            ServeOutcome::Bytes(bytes) => assert_eq!(bytes, Bytes::from_static(b"abcd")),
            ServeOutcome::Sendfile(_) | ServeOutcome::SendfileMany(_) => {
                panic!("hot page must be served from L1")
            }
        }
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        assert!(metrics
            .render()
            .contains("talon_worker_cache_tier_hits_total{tier=\"l1\"} 1"));
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn different_ranges_of_one_block_admit_only_their_touched_pages() {
        let root = tmp_root();
        let calls = Arc::new(AtomicUsize::new(0));
        let backend = Arc::new(CountingRampBackend {
            block_size: 16,
            calls: Arc::clone(&calls),
        });
        let runtime = WorkerRuntime::new_with_l1(
            WholeBlockStore::open(&root).unwrap(),
            Arc::new(BlockIndex::new()),
            Arc::new(InFlightLoads::new()),
            backend,
            16,
            1024,
            8,
            4,
            WorkerMetrics::new(1024),
        )
        .with_version_ttl(Duration::ZERO);
        let object = ObjectId::new(Backend::Azure, "container", "page-hotness");

        let first = RangeRequest {
            object: object.clone(),
            offset: 1,
            len: 2,
        };
        assert_eq!(runtime.serve_range(&first).await.unwrap(), expected(1, 2));
        let block = BlockId::new(object.clone(), 0, 16, Version::new("v1"));
        assert!(runtime.l1.get_page(&block, PageIndex(0)).is_some());
        assert!(runtime.l1.get_page(&block, PageIndex(1)).is_none());
        assert!(runtime.l1.get_page(&block, PageIndex(2)).is_none());
        assert_eq!(runtime.l1_page_count(), 1);

        let second = RangeRequest {
            object,
            offset: 9,
            len: 2,
        };
        assert_eq!(runtime.serve_range(&second).await.unwrap(), expected(9, 2));
        assert!(runtime.l1.get_page(&block, PageIndex(0)).is_some());
        assert!(runtime.l1.get_page(&block, PageIndex(1)).is_none());
        assert!(runtime.l1.get_page(&block, PageIndex(2)).is_some());
        assert!(runtime.l1.get_page(&block, PageIndex(3)).is_none());
        assert_eq!(runtime.l1_page_count(), 2);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn range_crossing_page_boundaries_admits_and_hits_all_touched_pages() {
        let root = tmp_root();
        let calls = Arc::new(AtomicUsize::new(0));
        let backend = Arc::new(CountingRampBackend {
            block_size: 16,
            calls: Arc::clone(&calls),
        });
        let metrics = WorkerMetrics::new(1024);
        let runtime = WorkerRuntime::new_with_l1(
            WholeBlockStore::open(&root).unwrap(),
            Arc::new(BlockIndex::new()),
            Arc::new(InFlightLoads::new()),
            backend,
            16,
            1024,
            16,
            4,
            metrics.clone(),
        )
        .with_version_ttl(Duration::ZERO);
        let request = RangeRequest {
            object: ObjectId::new(Backend::Azure, "container", "cross-pages"),
            offset: 3,
            len: 10,
        };

        assert_eq!(
            runtime.serve_range(&request).await.unwrap(),
            expected(3, 10)
        );
        assert_eq!(runtime.l1_page_count(), 4);
        assert_eq!(
            runtime.serve_range(&request).await.unwrap(),
            expected(3, 10)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(metrics
            .render()
            .contains("talon_worker_cache_tier_hits_total{tier=\"l1\"} 1"));
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn default_large_block_can_cache_one_small_hot_page() {
        const BLOCK_SIZE: u32 = 256 << 20;
        const PAGE_SIZE: u64 = 256 << 10;

        let root = tmp_root();
        let store = WholeBlockStore::open(&root).unwrap();
        let object = ObjectId::new(Backend::Azure, "container", "large-block");
        let block = BlockId::new(object.clone(), 0, BLOCK_SIZE, Version::new("v1"));
        let bytes = Bytes::from(vec![7_u8; (PAGE_SIZE * 2) as usize]);
        store.put(&block, bytes.clone()).await.unwrap();
        let index = Arc::new(BlockIndex::new());
        index.commit(BlockMeta {
            id: block.clone(),
            form: BlockForm::Whole,
            len: bytes.len() as u64,
        });
        let runtime = WorkerRuntime::new_with_l1(
            store,
            index,
            Arc::new(InFlightLoads::new()),
            Arc::new(RampBackend {
                block_size: BLOCK_SIZE as u64,
            }),
            BLOCK_SIZE,
            BLOCK_SIZE as u64,
            PAGE_SIZE * 2,
            PAGE_SIZE,
            WorkerMetrics::new(BLOCK_SIZE as u64),
        );
        runtime.store_version(&object, &Version::new("v1"), 0);
        let request = RangeRequest {
            object,
            offset: PAGE_SIZE + 17,
            len: 64,
        };

        assert_eq!(
            runtime.serve_range(&request).await.unwrap(),
            Bytes::from(vec![7_u8; 64])
        );
        assert_eq!(runtime.l1_page_count(), 1);
        assert!(runtime.l1.get_page(&block, PageIndex(0)).is_none());
        assert!(runtime.l1.get_page(&block, PageIndex(1)).is_some());
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
        assert_eq!(runtime.l1_page_count(), 1, "L1 holds only the MRU block");
        assert_eq!(backend.calls.load(Ordering::SeqCst), 2);

        match runtime.serve(&request("a")).await.unwrap() {
            ServeOutcome::Bytes(bytes) => assert_eq!(bytes, Bytes::from_static(b"abcd")),
            ServeOutcome::Sendfile(_) | ServeOutcome::SendfileMany(_) => {
                panic!("eligible L2 hit should promote to L1")
            }
        }
        assert_eq!(
            backend.calls.load(Ordering::SeqCst),
            2,
            "L1 eviction must degrade to L2, not origin"
        );
        assert_eq!(runtime.block_count(), 2);
        assert_eq!(runtime.l1_page_count(), 1);
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
        let runtime = runtime_l1(Arc::clone(&backend), WorkerMetrics::new(8), &root, 8, 16, 4);
        let full = |name| RangeRequest {
            object: ObjectId::new(Backend::Azure, "container", name),
            offset: 0,
            len: 8,
        };

        let _ = runtime.serve(&full("a")).await.unwrap();
        assert_eq!(runtime.l1_page_count(), 2);
        let _ = runtime.serve(&full("b")).await.unwrap();
        assert_eq!(runtime.block_count(), 1, "L2 capacity keeps one block");
        assert_eq!(
            runtime.l1_page_count(),
            2,
            "evicted L2 parent must remove all of its child pages"
        );

        let _ = runtime.serve(&full("a")).await.unwrap();
        assert_eq!(
            backend.calls.load(Ordering::SeqCst),
            3,
            "reading the L2-evicted block must fetch origin again"
        );
        assert_eq!(runtime.block_count(), 1);
        assert_eq!(runtime.l1_page_count(), 2);
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
            4,
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
            4,
            WorkerMetrics::new(1024),
        )
        .with_version_ttl(Duration::ZERO);
        assert_eq!(restarted.l1_page_count(), 0);
        assert_eq!(restarted.block_count(), 1);

        match restarted.serve(&request("restart")).await.unwrap() {
            ServeOutcome::Bytes(bytes) => assert_eq!(bytes, Bytes::from_static(b"abcd")),
            ServeOutcome::Sendfile(_) | ServeOutcome::SendfileMany(_) => {
                panic!("eligible L2 block should promote after restart")
            }
        }
        assert_eq!(
            backend.calls.load(Ordering::SeqCst),
            1,
            "restart promotion must not refetch object bytes"
        );
        assert_eq!(restarted.l1_page_count(), 1);
        let block = restarted.block_for(
            &ObjectId::new(Backend::Azure, "container", "restart"),
            0,
            &Version::new("v1"),
        );
        assert!(restarted.l1.get_page(&block, PageIndex(0)).is_some());
        assert!(restarted.l1.get_page(&block, PageIndex(1)).is_none());
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn stale_l2_index_entry_refetches_after_file_disappears() {
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
            4,
        );
        let req = request("lost-file");
        assert_eq!(
            runtime.serve_range(&req).await.unwrap(),
            Bytes::from_static(b"abcd")
        );

        let block = runtime.block_for(&req.object, req.offset, &Version::new("v1"));
        runtime.l1.remove_block(&block);
        runtime.store.delete(&block).await.unwrap();
        assert!(
            runtime.index.get(&block).is_some(),
            "test must leave stale metadata behind"
        );

        assert_eq!(
            runtime.serve_range(&req).await.unwrap(),
            Bytes::from_static(b"abcd")
        );
        assert_eq!(backend.calls.load(Ordering::SeqCst), 2);
        assert!(runtime.index.get(&block).is_some());
        assert_eq!(runtime.l1_page_count(), 1);
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
            4,
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
            4,
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
        assert_eq!(runtime.l1_page_count(), 3);
        assert_eq!(runtime.l1_resident_bytes(), 12);

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
            4,
        );

        assert!(runtime.serve(&request("failure")).await.is_err());
        assert_eq!(runtime.inflight_loads(), 0);
        assert_eq!(runtime.block_count(), 0);
        assert_eq!(runtime.resident_bytes(), 0);
        assert_eq!(runtime.l1_page_count(), 0);
        assert_eq!(runtime.l1_resident_bytes(), 0);
        std::fs::remove_dir_all(root).ok();
    }

    /// Read the bytes a `Sendfile` outcome would transmit, straight from its fd.
    fn read_handle(outcome: ServeOutcome) -> Vec<u8> {
        use std::io::{Read, Seek, SeekFrom};
        match outcome {
            ServeOutcome::Sendfile(handle) => {
                // Dup: the descriptor is shared, so this reader must not close it.
                let mut f = std::fs::File::from(handle.fd.try_clone().unwrap());
                f.seek(SeekFrom::Start(handle.offset)).unwrap();
                let mut buf = vec![0u8; handle.len as usize];
                f.read_exact(&mut buf).unwrap();
                buf
            }
            ServeOutcome::SendfileMany(handles) => {
                // Concatenate the segments in order: this is what the wire sees
                // after N sendfile calls, so tests can assert on the payload.
                let mut out = Vec::new();
                for handle in handles {
                    let mut f = std::fs::File::from(handle.fd.try_clone().unwrap());
                    f.seek(SeekFrom::Start(handle.offset)).unwrap();
                    let mut buf = vec![0u8; handle.len as usize];
                    f.read_exact(&mut buf).unwrap();
                    out.extend_from_slice(&buf);
                }
                out
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
            ServeOutcome::Sendfile(_) | ServeOutcome::SendfileMany(_) => {
                panic!("first serve (miss) must be Bytes")
            }
        }

        // Hit: sendfile path, exact sub-range.
        let outcome = runtime.serve(&request("ok")).await.unwrap();
        assert_eq!(read_handle(outcome), b"abcd");
        // No second backend fetch.
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn cache_only_probe_never_fetches_the_backend() {
        let root = tmp_root();
        let backend = Arc::new(MockBackend {
            calls: AtomicUsize::new(0),
        });
        let runtime = runtime(Arc::clone(&backend), WorkerMetrics::new(1024), &root);
        let probe = CachedRangeRequest {
            object: request("ok").object,
            version: Version::new("v1"),
            offset: 0,
            len: 4,
        };

        let miss = runtime.serve_cached(&probe).await.unwrap_err();
        assert!(miss.downcast_ref::<CacheMiss>().is_some());
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);

        assert_eq!(
            runtime.serve_range(&request("ok")).await.unwrap(),
            Bytes::from_static(b"abcd")
        );
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            runtime.serve_cached(&probe).await.unwrap(),
            Bytes::from_static(b"abcd")
        );
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);

        let wrong_version = CachedRangeRequest {
            version: Version::new("different"),
            ..probe
        };
        let miss = runtime.serve_cached(&wrong_version).await.unwrap_err();
        assert!(miss.downcast_ref::<CacheMiss>().is_some());
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn cache_admission_is_versioned_local_only_and_rejects_partial_blocks() {
        let root = tmp_root();
        let backend = Arc::new(MockBackend {
            calls: AtomicUsize::new(0),
        });
        let runtime = runtime(Arc::clone(&backend), WorkerMetrics::new(1024), &root);
        let object = request("admitted").object;
        let block = BlockId::new(object.clone(), 0, 8, Version::new("origin-v2"));
        let admission = CachedBlockPutRequest {
            block: block.clone(),
            object_len: 8,
            body_len: 8,
        };

        runtime
            .admit_cached_block(&admission, Bytes::from_static(b"gateway!"))
            .await
            .unwrap();
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            runtime
                .serve_cached(&CachedRangeRequest {
                    object: object.clone(),
                    version: Version::new("origin-v2"),
                    offset: 1,
                    len: 4,
                })
                .await
                .unwrap(),
            Bytes::from_static(b"atew")
        );
        let wrong_version = runtime
            .serve_cached(&CachedRangeRequest {
                object: object.clone(),
                version: Version::new("other"),
                offset: 0,
                len: 1,
            })
            .await
            .unwrap_err();
        assert!(wrong_version.downcast_ref::<CacheMiss>().is_some());
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);

        let tail = CachedBlockPutRequest {
            block: BlockId::new(object.clone(), 8, 8, Version::new("origin-v2")),
            object_len: 11,
            body_len: 3,
        };
        runtime
            .admit_cached_block(&tail, Bytes::from_static(b"end"))
            .await
            .unwrap();
        assert_eq!(
            runtime
                .serve_cached(&CachedRangeRequest {
                    object: object.clone(),
                    version: Version::new("origin-v2"),
                    offset: 8,
                    len: 3,
                })
                .await
                .unwrap(),
            Bytes::from_static(b"end")
        );

        let invalid = [
            CachedBlockPutRequest {
                block: BlockId::new(object.clone(), 1, 8, Version::new("misaligned")),
                object_len: 9,
                body_len: 8,
            },
            CachedBlockPutRequest {
                block: BlockId::new(object.clone(), 8, 8, Version::new("partial")),
                object_len: 16,
                body_len: 7,
            },
            CachedBlockPutRequest {
                block: BlockId::new(object.clone(), 0, 4, Version::new("wrong-size")),
                object_len: 4,
                body_len: 4,
            },
        ];
        for request in invalid {
            assert!(runtime
                .admit_cached_block(&request, Bytes::from(vec![0; request.body_len as usize]))
                .await
                .is_err());
            assert!(runtime
                .serve_cached(&CachedRangeRequest {
                    object: object.clone(),
                    version: request.block.version,
                    offset: request.block.offset,
                    len: 1,
                })
                .await
                .is_err());
        }
        assert_eq!(runtime.block_count(), 2);
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
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
            ServeOutcome::Sendfile(_) | ServeOutcome::SendfileMany(_) => {
                panic!("boundary-spanning read must be Bytes")
            }
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
            4,
            WorkerMetrics::new(1024),
        )
        .with_version_ttl(Duration::ZERO);

        let full_request = RangeRequest {
            object: ObjectId::new(Backend::Azure, "container", "versioned"),
            offset: 0,
            len: 8,
        };
        assert_eq!(
            runtime.serve_range(&full_request).await.unwrap(),
            Bytes::from_static(b"old-data")
        );
        assert_eq!(runtime.block_count(), 1);
        assert_eq!(runtime.l1_page_count(), 2);

        *backend.version.lock().unwrap() = "v2".into();
        *backend.body.lock().unwrap() = Bytes::from_static(b"new-data");
        assert_eq!(
            runtime.serve_range(&full_request).await.unwrap(),
            Bytes::from_static(b"new-data")
        );
        assert_eq!(runtime.block_count(), 1, "old L2 version must be removed");
        assert_eq!(runtime.l1_page_count(), 2, "old L1 pages must be removed");
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
            4,
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
        assert_eq!(runtime.l1_page_count(), 2);

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
        assert_eq!(runtime.l1_page_count(), 0);
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

    // ---- paged L2 -------------------------------------------------------

    /// A backend recording every `(offset, len)` fetched, so a test can assert
    /// that a paged miss pulled only the pages it needed.
    struct PagedRampBackend {
        object_len: u64,
        ranges: Mutex<Vec<(u64, u64)>>,
    }

    impl PagedRampBackend {
        fn new(object_len: u64) -> Self {
            Self {
                object_len,
                ranges: Mutex::new(Vec::new()),
            }
        }

        fn ranges(&self) -> Vec<(u64, u64)> {
            self.ranges.lock().unwrap().clone()
        }

        fn fetched_bytes(&self) -> u64 {
            self.ranges.lock().unwrap().iter().map(|(_, l)| l).sum()
        }
    }

    #[async_trait]
    impl BackendStore for PagedRampBackend {
        async fn fetch_range(&self, _object: &ObjectId, offset: u64, len: u64) -> Result<Bytes> {
            self.ranges.lock().unwrap().push((offset, len));
            let end = (offset + len).min(self.object_len);
            let n = end.saturating_sub(offset) as usize;
            Ok(Bytes::from(
                (0..n)
                    .map(|i| ((offset + i as u64) % 251) as u8)
                    .collect::<Vec<u8>>(),
            ))
        }

        async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
            Ok(ObjectStat {
                len: self.object_len,
                version: Version::new("v1"),
            })
        }
    }

    /// Build a paged-mode runtime: `block_size`-byte blocks split into
    /// `page_size`-byte pages, L1 disabled so L2 behavior is observable.
    fn paged_runtime<B: BackendStore + 'static>(
        backend: Arc<B>,
        root: &Path,
        block_size: u32,
        page_size: u32,
        capacity_bytes: u64,
        metrics: WorkerMetrics,
    ) -> WorkerRuntime {
        WorkerRuntime::new(
            WholeBlockStore::open(root.join("whole")).unwrap(),
            Arc::new(BlockIndex::new()),
            Arc::new(InFlightLoads::new()),
            backend,
            block_size,
            capacity_bytes,
            metrics,
        )
        .with_paged_store(PagedBlockStore::open(root.join("paged"), page_size).unwrap())
        .with_version_ttl(Duration::ZERO)
    }

    fn req(object: &ObjectId, offset: u64, len: u64) -> RangeRequest {
        RangeRequest {
            object: object.clone(),
            offset,
            len,
        }
    }

    #[tokio::test]
    async fn a_point_read_fetches_one_page_not_the_whole_block() {
        let root = tmp_root();
        // 1 KiB blocks, 64-byte pages: a 16-page block.
        let backend = Arc::new(PagedRampBackend::new(4096));
        let runtime = paged_runtime(
            Arc::clone(&backend),
            &root,
            1024,
            64,
            0,
            WorkerMetrics::new(1024),
        );
        let object = ObjectId::new(Backend::Azure, "bucket", "obj");

        // Read 8 bytes in the middle of page 5 of block 0.
        let got = runtime
            .serve_range(&req(&object, 5 * 64 + 8, 8))
            .await
            .unwrap();

        assert_eq!(got, expected(5 * 64 + 8, 8));
        // Exactly one page fetched — not the 1 KiB block.
        assert_eq!(backend.ranges(), vec![(5 * 64, 64)]);
        assert_eq!(backend.fetched_bytes(), 64);
        // Only that page is resident, so capacity accounting charges 64 bytes.
        assert_eq!(runtime.resident_bytes(), 64);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_second_read_of_the_same_page_hits_cache() {
        let root = tmp_root();
        let backend = Arc::new(PagedRampBackend::new(4096));
        let runtime = paged_runtime(
            Arc::clone(&backend),
            &root,
            1024,
            64,
            0,
            WorkerMetrics::new(1024),
        );
        let object = ObjectId::new(Backend::Azure, "bucket", "obj");

        let first = runtime.serve_range(&req(&object, 100, 16)).await.unwrap();
        let second = runtime.serve_range(&req(&object, 100, 16)).await.unwrap();

        assert_eq!(first, expected(100, 16));
        assert_eq!(second, first);
        // The second read touched the same page; no new backend traffic.
        assert_eq!(backend.ranges().len(), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_read_spanning_pages_stitches_them_and_fetches_only_those_pages() {
        let root = tmp_root();
        let backend = Arc::new(PagedRampBackend::new(4096));
        let runtime = paged_runtime(
            Arc::clone(&backend),
            &root,
            1024,
            64,
            0,
            WorkerMetrics::new(1024),
        );
        let object = ObjectId::new(Backend::Azure, "bucket", "obj");

        // Straddle the page 1/2/3 boundaries: from mid-page-1 to mid-page-3.
        let got = runtime
            .serve_range(&req(&object, 64 + 32, 128))
            .await
            .unwrap();

        assert_eq!(got, expected(64 + 32, 128));
        let mut ranges = backend.ranges();
        ranges.sort();
        assert_eq!(ranges, vec![(64, 64), (128, 64), (192, 64)]);
        assert_eq!(runtime.resident_bytes(), 3 * 64);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn the_last_page_of_a_short_object_is_truncated_not_padded() {
        let root = tmp_root();
        // 100-byte object with 64-byte pages: page 1 holds only 36 bytes.
        let backend = Arc::new(PagedRampBackend::new(100));
        let runtime = paged_runtime(
            Arc::clone(&backend),
            &root,
            1024,
            64,
            0,
            WorkerMetrics::new(1024),
        );
        let object = ObjectId::new(Backend::Azure, "bucket", "obj");

        // Ask for more than remains; the read is clamped to EOF.
        let got = runtime.serve_range(&req(&object, 90, 64)).await.unwrap();

        assert_eq!(got, expected(90, 10));
        assert_eq!(backend.ranges(), vec![(64, 36)]);
        // Only the 36 real bytes are charged, not a full 64-byte page.
        assert_eq!(runtime.resident_bytes(), 36);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn evicting_a_cold_page_leaves_the_blocks_other_pages_intact() {
        let root = tmp_root();
        let backend = Arc::new(PagedRampBackend::new(4096));
        // Capacity for exactly two 64-byte pages.
        let runtime = paged_runtime(
            Arc::clone(&backend),
            &root,
            1024,
            64,
            128,
            WorkerMetrics::new(1024),
        );
        let object = ObjectId::new(Backend::Azure, "bucket", "obj");

        // Fill pages 0 and 1, then touch page 0 so page 1 is coldest.
        runtime.serve_range(&req(&object, 0, 8)).await.unwrap();
        runtime.serve_range(&req(&object, 64, 8)).await.unwrap();
        runtime.serve_range(&req(&object, 0, 8)).await.unwrap();
        // Admitting page 2 must evict page 1, the coldest.
        runtime.serve_range(&req(&object, 128, 8)).await.unwrap();

        assert_eq!(runtime.resident_bytes(), 128, "back under capacity");
        // The block entry survives: page 0 is still a cache hit.
        let before = backend.ranges().len();
        assert_eq!(
            runtime.serve_range(&req(&object, 0, 8)).await.unwrap(),
            expected(0, 8)
        );
        assert_eq!(backend.ranges().len(), before, "page 0 still cached");
        // Page 1 was evicted, so re-reading it costs a fresh fetch.
        assert_eq!(
            runtime.serve_range(&req(&object, 64, 8)).await.unwrap(),
            expected(64, 8)
        );
        assert_eq!(backend.ranges().len(), before + 1, "page 1 refetched");
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn concurrent_reads_of_one_page_trigger_a_single_backend_fetch() {
        let root = tmp_root();
        let backend = Arc::new(PagedRampBackend::new(4096));
        let runtime = Arc::new(paged_runtime(
            Arc::clone(&backend),
            &root,
            1024,
            64,
            0,
            WorkerMetrics::new(1024),
        ));
        let object = ObjectId::new(Backend::Azure, "bucket", "obj");

        let mut handles = Vec::new();
        for i in 0..8 {
            let runtime = Arc::clone(&runtime);
            let object = object.clone();
            handles.push(tokio::spawn(async move {
                runtime.serve_range(&req(&object, 64 + i, 4)).await.unwrap()
            }));
        }
        for (i, handle) in handles.into_iter().enumerate() {
            assert_eq!(handle.await.unwrap(), expected(64 + i as u64, 4));
        }

        // All eight readers touched page 1; exactly one fetch was issued.
        assert_eq!(backend.ranges(), vec![(64, 64)]);
        assert_eq!(runtime.inflight_loads(), 0);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_restart_rebuilds_paged_residency_from_the_page_files_on_disk() {
        let root = tmp_root();
        let backend = Arc::new(PagedRampBackend::new(4096));
        let metrics = WorkerMetrics::new(1024);
        let object = ObjectId::new(Backend::Azure, "bucket", "obj");
        {
            let runtime = paged_runtime(Arc::clone(&backend), &root, 1024, 64, 0, metrics.clone());
            runtime.serve_range(&req(&object, 0, 8)).await.unwrap();
            runtime.serve_range(&req(&object, 128, 8)).await.unwrap();
        }
        let fetches_before = backend.ranges().len();

        // Restart: a fresh index rebuilt from the on-disk paged cache.
        let paged = PagedBlockStore::open(root.join("paged"), 64).unwrap();
        let index = Arc::new(BlockIndex::new());
        let metas = paged.scan().unwrap();
        assert_eq!(metas.len(), 1, "one paged block on disk");
        for meta in metas {
            index.commit(meta);
        }
        assert_eq!(index.page_count(), 2, "pages 0 and 2 rebuilt");
        assert_eq!(index.resident_bytes(), 128);

        let runtime = WorkerRuntime::new(
            WholeBlockStore::open(root.join("whole")).unwrap(),
            index,
            Arc::new(InFlightLoads::new()),
            Arc::clone(&backend) as Arc<dyn BackendStore>,
            1024,
            0,
            metrics,
        )
        .with_paged_store(paged)
        .with_version_ttl(Duration::ZERO);

        // Both previously-cached pages serve without new backend traffic.
        assert_eq!(
            runtime.serve_range(&req(&object, 0, 8)).await.unwrap(),
            expected(0, 8)
        );
        assert_eq!(
            runtime.serve_range(&req(&object, 128, 8)).await.unwrap(),
            expected(128, 8)
        );
        assert_eq!(backend.ranges().len(), fetches_before, "served from disk");
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn an_index_hit_with_a_deleted_page_file_refetches_instead_of_failing() {
        let root = tmp_root();
        let backend = Arc::new(PagedRampBackend::new(4096));
        let runtime = paged_runtime(
            Arc::clone(&backend),
            &root,
            1024,
            64,
            0,
            WorkerMetrics::new(1024),
        );
        let object = ObjectId::new(Backend::Azure, "bucket", "obj");
        runtime.serve_range(&req(&object, 0, 8)).await.unwrap();

        // Simulate an out-of-band deletion of the page file while the index
        // still believes the page is resident.
        let mut removed = 0;
        for shard in std::fs::read_dir(root.join("paged")).unwrap().flatten() {
            if !shard.file_type().unwrap().is_dir() {
                continue;
            }
            for dir in std::fs::read_dir(shard.path()).unwrap().flatten() {
                let page = dir.path().join("0.page");
                if page.exists() {
                    std::fs::remove_file(&page).unwrap();
                    removed += 1;
                }
            }
        }
        assert_eq!(removed, 1, "found the page file to delete");

        // The read recovers by re-fetching rather than surfacing an error.
        let got = runtime.serve_range(&req(&object, 0, 8)).await.unwrap();
        assert_eq!(got, expected(0, 8));
        assert_eq!(backend.ranges().len(), 2);
        std::fs::remove_dir_all(root).ok();
    }

    /// Build a paged runtime that also has an inclusive L1.
    fn paged_runtime_l1<B: BackendStore + 'static>(
        backend: Arc<B>,
        root: &Path,
        block_size: u32,
        page_size: u32,
        l1_capacity: u64,
        metrics: WorkerMetrics,
    ) -> WorkerRuntime {
        WorkerRuntime::new_with_l1(
            WholeBlockStore::open(root.join("whole")).unwrap(),
            Arc::new(BlockIndex::new()),
            Arc::new(InFlightLoads::new()),
            backend,
            block_size,
            0,
            l1_capacity,
            u64::from(page_size),
            metrics,
        )
        .with_paged_store(PagedBlockStore::open(root.join("paged"), page_size).unwrap())
        .with_version_ttl(Duration::ZERO)
    }

    /// L1 is inclusive and only the byte path admits pages into it. Taking the
    /// zero-copy path for a page that is *not* yet in L1 would silently stop L1
    /// from ever being populated, so the fast path must decline until the pages
    /// are L1-resident — and must still return correct bytes throughout.
    #[tokio::test]
    async fn with_l1_enabled_zero_copy_waits_until_the_pages_are_l1_resident() {
        let root = tmp_root();
        // Sizes large enough to straddle `L1_SENDFILE_MIN_LEN` from both sides:
        // 4 MiB blocks of 256 KiB pages.
        let page = 256u32 << 10;
        let backend = Arc::new(PagedRampBackend::new(8 << 20));
        let metrics = WorkerMetrics::new(1 << 20);
        let runtime = paged_runtime_l1(
            Arc::clone(&backend),
            &root,
            4 << 20,
            page,
            8 << 20,
            metrics.clone(),
        );
        let object = ObjectId::new(Backend::Azure, "bucket", "obj");
        // Straddles the page-0/page-1 boundary and clears the threshold.
        let offset = u64::from(page) - 1024;
        let len = L1_SENDFILE_MIN_LEN + 4096;
        let request = req(&object, offset, len);

        // First read: origin miss, bytes, and it admits the pages into L1.
        assert!(matches!(
            runtime.serve(&request).await.unwrap(),
            ServeOutcome::Bytes(_)
        ));
        assert!(runtime.l1_page_count() > 0, "byte path must populate L1");

        // Now that every covered page is L1-resident, zero-copy is safe and
        // must produce byte-identical output.
        let outcome = runtime.serve(&request).await.unwrap();
        assert!(
            matches!(outcome, ServeOutcome::SendfileMany(_)),
            "L1-resident cross-page read must go zero-copy"
        );
        assert_eq!(read_handle(outcome), expected(offset, len));
        std::fs::remove_dir_all(root).ok();
    }

    /// A span that is *partially* L1-resident — some pages in DRAM, the rest
    /// only on disk — is the state a real worker reaches constantly, since L1
    /// is much smaller than L2 and evicts under pressure while L2 keeps every
    /// page.
    ///
    /// Zero-copy must decline here. L1 is inclusive and only the byte path
    /// admits pages into it; serving the whole span with `sendfile` would leave
    /// the missing pages permanently absent from L1, so a hot range that lost
    /// one page could never be fully re-promoted. The byte path re-admits it,
    /// and the *next* read of the span goes zero-copy.
    #[tokio::test]
    async fn a_partially_l1_resident_span_falls_back_and_re_admits_the_missing_page() {
        let root = tmp_root();
        let page = 256u32 << 10;
        let backend = Arc::new(PagedRampBackend::new(8 << 20));
        let runtime = paged_runtime_l1(
            Arc::clone(&backend),
            &root,
            4 << 20,
            page,
            8 << 20,
            WorkerMetrics::new(1 << 20),
        );
        let object = ObjectId::new(Backend::Azure, "bucket", "obj");
        // Spans pages 0..=2 and clears the size threshold.
        let offset = u64::from(page) - 1024;
        let len = u64::from(page) + 4096;
        let request = req(&object, offset, len);

        // Warm: byte path admits every covered page into L1 and L2.
        let _ = runtime.serve(&request).await.unwrap();
        let block = runtime.block_for(&object, offset, &Version::new("v1"));

        // Evict exactly one covered page from L1. L2 still has all three.
        assert!(
            runtime.l1_drop_page_for_test(&block, PageIndex(1)),
            "page 1 must have been L1-resident to evict it"
        );

        // Partial residency: must fall back to the byte path, byte-exact.
        let outcome = runtime.serve(&request).await.unwrap();
        let bytes = match outcome {
            ServeOutcome::Bytes(b) => b,
            ServeOutcome::Sendfile(_) | ServeOutcome::SendfileMany(_) => panic!(
                "a partially L1-resident span must not go zero-copy: it would \
                 leave the missing page out of L1 forever"
            ),
        };
        assert_eq!(bytes, expected(offset, len));

        // The fallback re-admitted the missing page, so the span is whole again
        // and the next read is zero-copy.
        let outcome = runtime.serve(&request).await.unwrap();
        assert!(
            matches!(outcome, ServeOutcome::SendfileMany(_)),
            "once every page is back in L1 the read must go zero-copy again"
        );
        assert_eq!(read_handle(outcome), expected(offset, len));

        std::fs::remove_dir_all(root).ok();
    }

    /// Below `L1_SENDFILE_MIN_LEN` an L1-resident cross-page read is faster
    /// served straight from DRAM than through one `openat` + `sendfile` per
    /// page, so the fast path must decline even though the pages are resident.
    #[tokio::test]
    async fn a_small_l1_resident_cross_page_read_stays_on_the_byte_path() {
        let root = tmp_root();
        let page = 256u32 << 10;
        let backend = Arc::new(PagedRampBackend::new(8 << 20));
        let runtime = paged_runtime_l1(
            Arc::clone(&backend),
            &root,
            4 << 20,
            page,
            8 << 20,
            WorkerMetrics::new(1 << 20),
        );
        let object = ObjectId::new(Backend::Azure, "bucket", "obj");
        // Straddles a page boundary but stays under the threshold.
        let offset = u64::from(page) - 2048;
        let len = 4096;
        let request = req(&object, offset, len);

        // Warm L1 so residency is not what makes the fast path decline.
        let _ = runtime.serve(&request).await.unwrap();
        assert!(runtime.l1_page_count() > 0, "byte path must populate L1");

        let outcome = runtime.serve(&request).await.unwrap();
        let bytes = match outcome {
            ServeOutcome::Bytes(b) => b,
            ServeOutcome::Sendfile(_) | ServeOutcome::SendfileMany(_) => {
                panic!("a sub-threshold L1-resident read must stay on the byte path")
            }
        };
        assert_eq!(bytes, expected(offset, len));
        std::fs::remove_dir_all(root).ok();
    }

    /// A read spanning several pages must come back as one `sendfile` segment
    /// per page — never as stitched bytes — and the concatenation of those
    /// segments must equal the requested range exactly.
    #[tokio::test]
    async fn paged_mode_serves_a_cross_page_read_via_multi_sendfile() {
        let root = tmp_root();
        let backend = Arc::new(PagedRampBackend::new(4096));
        let runtime = paged_runtime(
            Arc::clone(&backend),
            &root,
            1024,
            64,
            0,
            WorkerMetrics::new(1024),
        );
        let object = ObjectId::new(Backend::Azure, "bucket", "obj");
        // 200 bytes from offset 100 spans pages 1..=4 at a 64-byte page size.
        let request = req(&object, 100, 200);

        // Warm every covered page.
        assert_eq!(
            runtime.serve_range(&request).await.unwrap(),
            expected(100, 200)
        );

        match runtime.serve(&request).await.unwrap() {
            ServeOutcome::SendfileMany(handles) => {
                assert_eq!(handles.len(), 4, "one handle per covered page");
                let total: u64 = handles.iter().map(|h| h.len).sum();
                assert_eq!(total, 200, "segments must cover the request exactly");
                assert_eq!(
                    read_handle(ServeOutcome::SendfileMany(handles)),
                    expected(100, 200)
                );
            }
            ServeOutcome::Sendfile(_) => panic!("a 4-page span must not be one handle"),
            ServeOutcome::Bytes(_) => panic!("a resident cross-page read must be zero-copy"),
        }
        std::fs::remove_dir_all(root).ok();
    }

    /// A range whose ends land mid-page must be clipped at both ends, not
    /// rounded out to whole pages.
    #[tokio::test]
    async fn cross_page_sendfile_clips_partial_pages_at_both_ends() {
        let root = tmp_root();
        let backend = Arc::new(PagedRampBackend::new(4096));
        let runtime = paged_runtime(
            Arc::clone(&backend),
            &root,
            1024,
            64,
            0,
            WorkerMetrics::new(1024),
        );
        let object = ObjectId::new(Backend::Azure, "bucket", "obj");
        // Starts 30 into page 0 and ends 10 into page 2.
        let request = req(&object, 30, 108);
        assert_eq!(
            runtime.serve_range(&request).await.unwrap(),
            expected(30, 108)
        );

        let outcome = runtime.serve(&request).await.unwrap();
        assert!(
            matches!(outcome, ServeOutcome::SendfileMany(_)),
            "resident cross-page read must be zero-copy"
        );
        assert_eq!(read_handle(outcome), expected(30, 108));
        std::fs::remove_dir_all(root).ok();
    }

    /// If any covered page is missing, the fast path must decline and let the
    /// byte path re-fetch, rather than serving a short or torn range.
    #[tokio::test]
    async fn cross_page_sendfile_declines_when_a_covered_page_is_absent() {
        let root = tmp_root();
        let backend = Arc::new(PagedRampBackend::new(4096));
        let runtime = paged_runtime(
            Arc::clone(&backend),
            &root,
            1024,
            64,
            0,
            WorkerMetrics::new(1024),
        );
        let object = ObjectId::new(Backend::Azure, "bucket", "obj");
        let request = req(&object, 100, 200);
        assert_eq!(
            runtime.serve_range(&request).await.unwrap(),
            expected(100, 200)
        );

        // Evict one interior page file behind the index's back.
        let mut removed = 0;
        for shard in std::fs::read_dir(root.join("paged")).unwrap().flatten() {
            if !shard.file_type().unwrap().is_dir() {
                continue;
            }
            for dir in std::fs::read_dir(shard.path()).unwrap().flatten() {
                let page = dir.path().join("2.page");
                if page.exists() {
                    std::fs::remove_file(&page).unwrap();
                    removed += 1;
                }
            }
        }
        assert_eq!(removed, 1, "found the interior page to delete");

        // Must still return correct bytes, by falling back and re-fetching.
        assert_eq!(
            runtime.serve_range(&request).await.unwrap(),
            expected(100, 200)
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn paged_mode_serves_a_within_page_read_via_sendfile() {
        let root = tmp_root();
        let backend = Arc::new(PagedRampBackend::new(4096));
        let runtime = paged_runtime(
            Arc::clone(&backend),
            &root,
            1024,
            64,
            0,
            WorkerMetrics::new(1024),
        );
        let object = ObjectId::new(Backend::Azure, "bucket", "obj");
        let request = req(&object, 70, 10);

        // First read is a miss and comes back as bytes.
        assert!(matches!(
            runtime.serve(&request).await.unwrap(),
            ServeOutcome::Bytes(_)
        ));
        // Once resident, the same within-page read is served zero-copy.
        match runtime.serve(&request).await.unwrap() {
            ServeOutcome::Sendfile(handle) => {
                let mut buf = vec![0u8; handle.len as usize];
                // Dup: the descriptor is shared, so this reader must not close it.
                let file = std::fs::File::from(handle.fd.try_clone().unwrap());
                file.read_exact_at(&mut buf, handle.offset).unwrap();
                assert_eq!(Bytes::from(buf), expected(70, 10));
            }
            ServeOutcome::SendfileMany(_) => {
                panic!("a within-page read must be a single sendfile handle")
            }
            ServeOutcome::Bytes(_) => panic!("expected a sendfile handle for a resident page"),
        }
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn overwriting_an_object_reclaims_the_old_versions_pages() {
        let root = tmp_root();
        let backend = Arc::new(VersionedBackend {
            version: std::sync::Mutex::new("v1".into()),
            body: std::sync::Mutex::new(Bytes::from_static(b"aaaaaaaa")),
            fetches: AtomicUsize::new(0),
        });
        let runtime = paged_runtime(
            Arc::clone(&backend),
            &root,
            1024,
            64,
            0,
            WorkerMetrics::new(1024),
        );
        let object = ObjectId::new(Backend::Azure, "bucket", "obj");

        runtime.serve_range(&req(&object, 0, 8)).await.unwrap();
        assert_eq!(runtime.block_count(), 1);

        // The source is overwritten: a new etag yields a new BlockId.
        *backend.version.lock().unwrap() = "v2".into();
        *backend.body.lock().unwrap() = Bytes::from_static(b"bbbbbbbb");
        let got = runtime.serve_range(&req(&object, 0, 8)).await.unwrap();

        assert_eq!(got, Bytes::from_static(b"bbbbbbbb"));
        // The superseded version's pages were reclaimed, not left resident.
        assert_eq!(runtime.block_count(), 1, "old version dropped");
        assert_eq!(runtime.resident_bytes(), 8);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_paged_read_after_write_hits_the_whole_block_the_write_committed() {
        let root = tmp_root();
        let backend = Arc::new(MutableBackend {
            body: std::sync::Mutex::new(None),
            version: std::sync::Mutex::new("v1".into()),
            fetches: AtomicUsize::new(0),
            puts: AtomicUsize::new(0),
            deletes: AtomicUsize::new(0),
        });
        let runtime = paged_runtime(
            Arc::clone(&backend),
            &root,
            1024,
            64,
            0,
            WorkerMetrics::new(1024),
        );
        let object = ObjectId::new(Backend::Azure, "bucket", "obj");
        let body = Bytes::from((0..200u32).map(|i| (i % 251) as u8).collect::<Vec<u8>>());

        runtime.write_object(&object, body.clone()).await.unwrap();
        let fetches_before = backend.fetches.load(Ordering::SeqCst);

        // The write committed a whole block; a paged read must serve from it
        // rather than going back to the origin.
        let got = runtime.serve_range(&req(&object, 70, 10)).await.unwrap();

        assert_eq!(got, body.slice(70..80));
        assert_eq!(
            backend.fetches.load(Ordering::SeqCst),
            fetches_before,
            "read-after-write must hit"
        );
        std::fs::remove_dir_all(root).ok();
    }
}
