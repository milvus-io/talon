//! Instrumented worker cache request runtime.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use talon_core::{
    BackendStore, BlockForm, BlockId, BlockMeta, Error, ObjectId, ObjectStore, PageIndex, Version,
};
use talon_transport::data::RangeRequest;

use crate::{BlockIndex, InFlightLoads, LoadKey, Presence, WholeBlockStore, WorkerMetrics};

/// Default lifetime of a cached resolved object version.
///
/// A short TTL keeps warm cache hits from paying a backend `HEAD` per read
/// while still bounding how long a source overwrite can go unnoticed on the
/// read path; the conditional GET (`If-Match`) is the hard correctness guard
/// that catches an overwrite inside the window (issue #163).
const DEFAULT_VERSION_TTL: Duration = Duration::from_secs(3);

/// A per-object resolved version with the instant it was resolved.
struct CachedVersion {
    version: Version,
    resolved_at: Instant,
}

/// Shared state required to serve instrumented data-plane range requests.
pub struct WorkerRuntime {
    store: WholeBlockStore,
    index: Arc<BlockIndex>,
    inflight: Arc<InFlightLoads>,
    backend: Arc<dyn BackendStore>,
    block_size: u32,
    metrics: WorkerMetrics,
    /// Short-TTL cache of resolved object versions, so a warm read does not pay
    /// a backend `HEAD` per request (issue #163).
    version_cache: Mutex<HashMap<ObjectId, CachedVersion>>,
    version_ttl: Duration,
}

impl WorkerRuntime {
    /// Create a request runtime over an initialized cache and backend.
    pub fn new(
        store: WholeBlockStore,
        index: Arc<BlockIndex>,
        inflight: Arc<InFlightLoads>,
        backend: Arc<dyn BackendStore>,
        block_size: u32,
        metrics: WorkerMetrics,
    ) -> Self {
        Self {
            store,
            index,
            inflight,
            backend,
            block_size,
            metrics,
            version_cache: Mutex::new(HashMap::new()),
            version_ttl: DEFAULT_VERSION_TTL,
        }
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
        if matches!(
            self.index.presence(block, PageIndex(0), PageIndex(1)),
            Presence::Whole
        ) {
            self.metrics.record_cache_hit();
            tracing::info!(block = %block, "HIT");
            let bytes = self
                .store
                .get_bytes(block)
                .await
                .map_err(|error| anyhow::anyhow!("read committed block: {error}"))?;
            Ok(Some(bytes))
        } else {
            Ok(None)
        }
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

        self.store
            .put(block, bytes.clone())
            .await
            .map_err(|error| anyhow::anyhow!("commit block failed: {error}"))?;
        self.index.commit(BlockMeta {
            id: block.clone(),
            form: BlockForm::Whole,
            len: bytes.len() as u64,
        });
        tracing::info!(block = %block, bytes = bytes.len(), "committed block");
        Ok(bytes)
    }

    /// Number of blocks currently indexed.
    pub fn block_count(&self) -> u64 {
        self.index.len() as u64
    }

    /// Number of backend loads currently in flight.
    pub fn inflight_loads(&self) -> u64 {
        self.inflight.len() as u64
    }
}

fn slice(buffer: &[u8], offset: u64, len: u64) -> anyhow::Result<bytes::Bytes> {
    let start = usize::try_from(offset).map_err(|_| anyhow::anyhow!("offset is too large"))?;
    if start > buffer.len() {
        anyhow::bail!("offset {offset} beyond block length {} bytes", buffer.len());
    }
    let requested = usize::try_from(len).unwrap_or(usize::MAX);
    let end = start.saturating_add(requested).min(buffer.len());
    Ok(bytes::Bytes::copy_from_slice(&buffer[start..end]))
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
    use std::hash::{Hash, Hasher};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::SystemTime;

    use async_trait::async_trait;
    use bytes::Bytes;
    use talon_core::{Backend, Error, ObjectStat, Result};

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
            metrics,
        )
        // Most tests assert version-sensitivity per read; a zero TTL keeps the
        // resolved-version cache from masking a source overwrite between reads.
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
            metrics,
        )
        // Default to always-resolve so version-sensitivity assertions are not
        // masked by the version cache; caching is exercised explicitly below.
        .with_version_ttl(Duration::ZERO)
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
        // Both versions are cached under distinct keys.
        assert_eq!(runtime.block_count(), 2);
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
}
