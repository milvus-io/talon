// SPDX-License-Identifier: Apache-2.0
//! Serving reads out of the extent cache.
//!
//! [`AsyncWorkerRuntime::serve`] answers the same [`RangeRequest`] the block
//! worker answers, on the same wire types, so an existing client cannot tell
//! the two apart. What differs is behind it: a miss fetches exactly the
//! requested range instead of the 256MB block containing it.
//!
//! # Reads only
//!
//! There is no write path here. `talon-worker` accepts writes and sequences
//! them origin-PUT-first per ADR 0002; this worker returns
//! [`Error::Unsupported`] and write traffic goes to a block-worker pool. See
//! ADR 0005 §8.
//!
//! # Exact-offset keys
//!
//! An extent is keyed by its start offset, so a read hits only if a previous
//! read started at the *same* offset. A read at offset 1050 misses even when a
//! cached extent at offset 1000 already contains those bytes. This is
//! deliberate: resolving containment would need an interval structure per
//! stream on the hot path, and the readers this worker targets — a query engine
//! re-reading a footer, then the same column chunks — repeat offsets rather
//! than sliding across them. The cost is real for a reader that does slide.
//!
//! # Objects are immutable
//!
//! Nothing here resolves, caches, or compares a version. An object's bytes at a
//! given offset are assumed never to change, so a republish at the same path is
//! served from cache indefinitely. The origin ETag survives in exactly one
//! place — [`stat_object`](AsyncWorkerRuntime::stat_object), which proxies a
//! client's metadata request straight through. See ADR 0005 §3.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use talon_core::{Backend, BackendStore, Error, ObjectId};
use talon_transport::data::RangeRequest;

use crate::cache::region::PinnedExtent;
use crate::cache::tiered::TieredExtentCache;
use crate::cache::ExtentKey;

/// How a served read should reach the socket.
pub enum ServeOutcome {
    /// Zero-copy: `sendfile` straight from a pinned NVMe extent.
    ///
    /// The guard keeps the extent's region pinned, so region reclamation cannot
    /// overwrite the bytes mid-transfer. Hold it until the transfer completes,
    /// then drop it.
    Sendfile(PinnedExtent),
    /// Bytes already in memory.
    Bytes(Bytes),
}

impl ServeOutcome {
    /// Bytes this outcome will put on the wire.
    pub fn len(&self) -> u64 {
        match self {
            ServeOutcome::Sendfile(p) => p.len(),
            ServeOutcome::Bytes(b) => b.len() as u64,
        }
    }

    /// Whether the response body is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl std::fmt::Debug for ServeOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServeOutcome::Sendfile(p) => f.debug_tuple("Sendfile").field(p).finish(),
            ServeOutcome::Bytes(b) => f.debug_tuple("Bytes").field(&b.len()).finish(),
        }
    }
}

/// Counters for the serve path itself, distinct from the cache's own.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ServeStats {
    /// Reads served.
    pub served: u64,
    /// Bytes put on the wire.
    pub bytes_served: u64,
    /// Reads answered with `sendfile` from a pinned extent.
    pub sendfile_served: u64,
    /// Object sizes resolved by a backend HEAD. At most one per object per
    /// process, since an immutable object's size cannot change.
    pub size_lookups: u64,
    /// Size resolutions answered from the in-memory table.
    pub size_cache_hits: u64,
    /// Reads clamped because they ran past the end of the object.
    pub reads_clamped: u64,
    /// Bytes fetched from the origin. The number this worker exists to lower.
    pub origin_bytes_fetched: u64,
}

#[derive(Debug, Default)]
struct Counters {
    served: AtomicU64,
    bytes_served: AtomicU64,
    sendfile_served: AtomicU64,
    size_lookups: AtomicU64,
    size_cache_hits: AtomicU64,
    reads_clamped: AtomicU64,
    origin_bytes_fetched: AtomicU64,
}

/// Serves range reads from a [`TieredExtentCache`], filling from a backend.
pub struct AsyncWorkerRuntime {
    cache: Arc<TieredExtentCache>,
    backend: Arc<dyn BackendStore>,
    /// Backend this process is configured for. A request naming any other one
    /// is refused rather than served from the wrong store.
    configured_backend: Option<Backend>,
    /// Object sizes, resolved once and kept for the life of the process.
    ///
    /// No TTL, because there is nothing for one to catch: the cache assumes
    /// objects are immutable, and an immutable object's size cannot change. The
    /// TTL this replaced existed only to re-check the version.
    ///
    /// Grows with every distinct object served and is never pruned — the same
    /// shape of leak as the stream-id table, and worth fixing in the same pass.
    sizes: Mutex<HashMap<ObjectId, u64>>,
    counters: Arc<Counters>,
}

impl std::fmt::Debug for AsyncWorkerRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncWorkerRuntime")
            .field("backend", &self.configured_backend)
            .field("cache", &self.cache)
            .finish()
    }
}

impl AsyncWorkerRuntime {
    /// Build a runtime over a cache and a backend.
    pub fn new(cache: Arc<TieredExtentCache>, backend: Arc<dyn BackendStore>) -> Self {
        Self {
            cache,
            backend,
            configured_backend: None,
            sizes: Mutex::new(HashMap::new()),
            counters: Arc::new(Counters::default()),
        }
    }

    /// Refuse requests naming a backend other than this one.
    pub fn with_configured_backend(mut self, backend: Backend) -> Self {
        self.configured_backend = Some(backend);
        self
    }

    /// The cache being served from.
    pub fn cache(&self) -> &Arc<TieredExtentCache> {
        &self.cache
    }

    /// Serve-path counters.
    pub fn stats(&self) -> ServeStats {
        let c = &self.counters;
        ServeStats {
            served: c.served.load(Ordering::Relaxed),
            bytes_served: c.bytes_served.load(Ordering::Relaxed),
            sendfile_served: c.sendfile_served.load(Ordering::Relaxed),
            size_lookups: c.size_lookups.load(Ordering::Relaxed),
            size_cache_hits: c.size_cache_hits.load(Ordering::Relaxed),
            reads_clamped: c.reads_clamped.load(Ordering::Relaxed),
            origin_bytes_fetched: c.origin_bytes_fetched.load(Ordering::Relaxed),
        }
    }

    /// The error this worker answers a write with.
    ///
    /// Not a silent drop and not a generic failure: a client that sent a write
    /// to the wrong pool needs to be told which pool to use. See ADR 0005 §8.
    pub fn write_unsupported(object: &ObjectId) -> Error {
        Error::Unsupported(format!(
            "async worker is read-only and cannot write {}; \
             route write traffic to a talon-worker pool",
            object.to_path()
        ))
    }

    /// Resolve an object's size and version for a client.
    ///
    /// Goes to the origin every time, deliberately. This is the one place an
    /// ETag still means something here, and a client asking for metadata wants
    /// the current answer — not the size this worker happens to have cached
    /// under an immutability assumption the client never agreed to. Read-only,
    /// so this worker serves it.
    pub async fn stat_object(&self, object: &ObjectId) -> anyhow::Result<talon_core::ObjectStat> {
        self.ensure_configured_backend(object.backend)?;
        self.backend
            .head(object)
            .await
            .map_err(|error| anyhow::anyhow!("stat object (HEAD): {error}"))
    }

    /// Serve one range read.
    pub async fn serve(&self, request: &RangeRequest) -> anyhow::Result<ServeOutcome> {
        self.ensure_configured_backend(request.object.backend)?;
        if request.len == 0 {
            return Ok(ServeOutcome::Bytes(Bytes::new()));
        }

        let object_size = self.object_size(&request.object).await?;
        let outcome = self.serve_at(request, object_size).await?;

        self.counters.served.fetch_add(1, Ordering::Relaxed);
        self.counters
            .bytes_served
            .fetch_add(outcome.len(), Ordering::Relaxed);
        if matches!(outcome, ServeOutcome::Sendfile(_)) {
            self.counters
                .sendfile_served
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(outcome)
    }

    async fn serve_at(
        &self,
        request: &RangeRequest,
        object_size: u64,
    ) -> anyhow::Result<ServeOutcome> {
        // Clamp to the object's end before anything is keyed or cached.
        //
        // Without this a read running past EOF would store a short extent under
        // the requested length's key, and the next identical read would find it
        // too short, discard it, and refetch — forever. Clamping makes both
        // reads ask for the same thing.
        let available = object_size.saturating_sub(request.offset);
        if available == 0 {
            return Ok(ServeOutcome::Bytes(Bytes::new()));
        }
        let len = request.len.min(available);
        if len < request.len {
            self.counters.reads_clamped.fetch_add(1, Ordering::Relaxed);
        }

        let stream_id = self.cache.intern(&request.object);
        let key = ExtentKey::new(stream_id, request.offset);

        // Zero-copy is only reachable with the DRAM tier off. With L1 on, an L2
        // hit is a promotion source: it is read into userspace so the next read
        // is a DRAM hit, which beats a second trip to NVMe. With L1 off there is
        // nothing to promote into, and sendfile keeps the bytes out of userspace
        // entirely.
        if !self.cache.memory().is_enabled() {
            if let Some(disk) = self.cache.disk() {
                if let Some(pinned) = disk.pin(key, len).await? {
                    return Ok(ServeOutcome::Sendfile(pinned));
                }
            }
        }

        let object = request.object.clone();
        let backend = Arc::clone(&self.backend);
        let counters = Arc::clone(&self.counters);
        let offset = request.offset;
        let bytes = self
            .cache
            .get_or_load(
                stream_id,
                offset,
                len,
                Box::pin(async move {
                    // Unconditional. With no version tracked there is nothing an
                    // `If-Match` could be matched against.
                    let fetched = backend.fetch_range(&object, offset, len).await?;
                    // Counted here rather than at the call site so it reflects
                    // fetches that actually reached the origin: a read that
                    // coalesced onto another reader's loader never runs this.
                    counters
                        .origin_bytes_fetched
                        .fetch_add(fetched.len() as u64, Ordering::Relaxed);
                    Ok(fetched)
                }),
            )
            .await?;

        Ok(ServeOutcome::Bytes(bytes))
    }

    /// The object's size, HEADing the origin the first time it is asked for.
    ///
    /// Kept for the life of the process once resolved. An immutable object
    /// cannot change size, so there is nothing a re-check could discover — and
    /// this is the only origin round trip on the read path that is not a cache
    /// fill, so paying it once per object rather than once per TTL is the
    /// difference between negligible and constant overhead.
    async fn object_size(&self, object: &ObjectId) -> anyhow::Result<u64> {
        if let Some(&size) = self.sizes.lock().unwrap().get(object) {
            self.counters
                .size_cache_hits
                .fetch_add(1, Ordering::Relaxed);
            return Ok(size);
        }

        self.counters.size_lookups.fetch_add(1, Ordering::Relaxed);
        let stat = self
            .backend
            .head(object)
            .await
            .map_err(|error| anyhow::anyhow!("resolve object size (HEAD): {error}"))?;

        // Racing first readers of one object both HEAD and both insert the same
        // answer. Not worth coalescing: the duplicate costs one request on the
        // very first read, and a coalescing map here would be a second copy of
        // machinery L1 already has.
        self.sizes.lock().unwrap().insert(object.clone(), stat.len);
        Ok(stat.len)
    }

    fn ensure_configured_backend(&self, requested: Backend) -> anyhow::Result<()> {
        match self.configured_backend {
            Some(configured) if configured != requested => anyhow::bail!(
                "worker is configured for the {configured:?} backend but the request named \
                 {requested:?}"
            ),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::tiered::ExtentCacheConfig;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::path::PathBuf;
    use talon_core::{ObjectStat, Result as CoreResult, Version};

    fn tmp_root(tag: &str) -> PathBuf {
        let mut h = DefaultHasher::new();
        std::time::SystemTime::now().hash(&mut h);
        std::thread::current().id().hash(&mut h);
        tag.hash(&mut h);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "talon-serve-{}-{:x}",
            std::process::id(),
            h.finish()
        ));
        p
    }

    /// A backend that records every range it was asked for, so a test can
    /// assert on origin bytes rather than on cache internals.
    #[derive(Debug)]
    struct RecordingBackend {
        body: Mutex<Bytes>,
        /// Ranges requested, as `(offset, len)`.
        ranges: Mutex<Vec<(u64, u64)>>,
        heads: AtomicU64,
    }

    impl RecordingBackend {
        fn new(body: &[u8]) -> Arc<Self> {
            Arc::new(Self {
                body: Mutex::new(Bytes::copy_from_slice(body)),
                ranges: Mutex::new(Vec::new()),
                heads: AtomicU64::new(0),
            })
        }

        fn origin_bytes(&self) -> u64 {
            self.ranges.lock().unwrap().iter().map(|(_, l)| l).sum()
        }

        fn fetches(&self) -> usize {
            self.ranges.lock().unwrap().len()
        }

        /// Overwrite the object at the same path — the case this worker
        /// assumes away.
        fn republish(&self, body: &[u8]) {
            *self.body.lock().unwrap() = Bytes::copy_from_slice(body);
        }
    }

    #[async_trait::async_trait]
    impl BackendStore for RecordingBackend {
        async fn fetch_range(&self, _obj: &ObjectId, offset: u64, len: u64) -> CoreResult<Bytes> {
            self.ranges.lock().unwrap().push((offset, len));
            let body = self.body.lock().unwrap().clone();
            let start = (offset as usize).min(body.len());
            let end = (start + len as usize).min(body.len());
            Ok(body.slice(start..end))
        }

        async fn head(&self, _obj: &ObjectId) -> CoreResult<ObjectStat> {
            self.heads.fetch_add(1, Ordering::Relaxed);
            Ok(ObjectStat {
                len: self.body.lock().unwrap().len() as u64,
                version: Version::new("etag-1"),
            })
        }
    }

    fn object(path: &str) -> ObjectId {
        ObjectId::new(Backend::S3, "bucket", path)
    }

    fn req(obj: &ObjectId, offset: u64, len: u64) -> RangeRequest {
        RangeRequest {
            object: obj.clone(),
            offset,
            len,
        }
    }

    async fn runtime(
        root: &std::path::Path,
        memory_bytes: u64,
        backend: Arc<RecordingBackend>,
    ) -> AsyncWorkerRuntime {
        let cache = TieredExtentCache::new(&ExtentCacheConfig {
            memory_bytes,
            memory_shards: 1,
            disk_dir: Some(root.to_path_buf()),
            disk_bytes: crate::cache::region::REGION_SIZE * 2,
            disk_shards: 1,
            disk_checksums: false,
            checkpoint_interval_bytes: 0,
        })
        .await
        .unwrap();
        AsyncWorkerRuntime::new(cache, backend)
    }

    fn body_of(outcome: &ServeOutcome) -> Bytes {
        match outcome {
            ServeOutcome::Bytes(b) => b.clone(),
            ServeOutcome::Sendfile(_) => panic!("expected Bytes, got Sendfile"),
        }
    }

    #[tokio::test]
    async fn a_selective_read_fetches_only_what_it_asked_for() {
        // The claim the whole crate exists to make. A 4KB read of a 64MB object
        // must cost 4KB at the origin, not a 256MB block.
        let root = tmp_root("selective");
        let backend = RecordingBackend::new(&vec![0xABu8; 64 * 1024 * 1024]);
        let rt = runtime(&root, 1 << 20, Arc::clone(&backend)).await;
        let obj = object("part-0.parquet");

        let out = rt.serve(&req(&obj, 32 * 1024 * 1024, 4096)).await.unwrap();
        assert_eq!(out.len(), 4096);
        assert_eq!(
            backend.origin_bytes(),
            4096,
            "origin was asked for more than the read wanted"
        );
        assert_eq!(rt.stats().origin_bytes_fetched, 4096);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_repeated_read_does_not_touch_the_origin() {
        let root = tmp_root("repeat");
        let backend = RecordingBackend::new(&[7u8; 8192]);
        let rt = runtime(&root, 1 << 20, Arc::clone(&backend)).await;
        let obj = object("f.parquet");

        for _ in 0..5 {
            let out = rt.serve(&req(&obj, 0, 1024)).await.unwrap();
            assert_eq!(out.len(), 1024);
        }
        assert_eq!(backend.fetches(), 1, "cache did not absorb the repeats");
        assert_eq!(rt.stats().served, 5);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_shorter_read_at_a_cached_offset_is_served_from_the_larger_extent() {
        let root = tmp_root("prefix");
        let backend = RecordingBackend::new(&[3u8; 8192]);
        let rt = runtime(&root, 1 << 20, Arc::clone(&backend)).await;
        let obj = object("f.parquet");

        rt.serve(&req(&obj, 0, 4096)).await.unwrap();
        let short = rt.serve(&req(&obj, 0, 128)).await.unwrap();

        assert_eq!(short.len(), 128);
        assert_eq!(backend.fetches(), 1, "the prefix should not refetch");

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_longer_read_at_a_cached_offset_refetches_at_the_larger_size() {
        let root = tmp_root("grow");
        let backend = RecordingBackend::new(&[3u8; 8192]);
        let rt = runtime(&root, 1 << 20, Arc::clone(&backend)).await;
        let obj = object("f.parquet");

        rt.serve(&req(&obj, 0, 128)).await.unwrap();
        let long = rt.serve(&req(&obj, 0, 4096)).await.unwrap();
        assert_eq!(long.len(), 4096);
        assert_eq!(backend.origin_bytes(), 128 + 4096);

        // And the larger extent now backs the smaller read too.
        rt.serve(&req(&obj, 0, 128)).await.unwrap();
        assert_eq!(backend.fetches(), 2, "largest-wins should have converged");

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_read_past_the_end_is_clamped_and_does_not_refetch_forever() {
        // Without clamping, the first read stores a 10-byte extent under a
        // 1024-byte request; the next identical read finds it too short,
        // discards it, and refetches — every time.
        let root = tmp_root("eof");
        let backend = RecordingBackend::new(&[1u8; 100]);
        let rt = runtime(&root, 1 << 20, Arc::clone(&backend)).await;
        let obj = object("small.parquet");

        let first = rt.serve(&req(&obj, 90, 1024)).await.unwrap();
        assert_eq!(first.len(), 10, "clamped to the object's end");
        assert_eq!(rt.stats().reads_clamped, 1);

        for _ in 0..4 {
            assert_eq!(rt.serve(&req(&obj, 90, 1024)).await.unwrap().len(), 10);
        }
        assert_eq!(backend.fetches(), 1, "clamped reads must still hit cache");

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_read_starting_past_the_end_is_empty() {
        let root = tmp_root("beyond");
        let backend = RecordingBackend::new(&[1u8; 100]);
        let rt = runtime(&root, 1 << 20, Arc::clone(&backend)).await;

        let out = rt
            .serve(&req(&object("small.parquet"), 200, 50))
            .await
            .unwrap();
        assert!(out.is_empty());
        assert_eq!(backend.fetches(), 0);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_zero_length_read_is_empty_and_costs_nothing() {
        let root = tmp_root("zero");
        let backend = RecordingBackend::new(&[1u8; 100]);
        let rt = runtime(&root, 1 << 20, Arc::clone(&backend)).await;

        let out = rt.serve(&req(&object("f.parquet"), 0, 0)).await.unwrap();
        assert!(out.is_empty());
        assert_eq!(backend.heads.load(Ordering::Relaxed), 0, "no HEAD either");

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn the_size_is_headed_once_per_object_and_reused_thereafter() {
        // The only origin round trip on the read path that is not a cache fill.
        // An immutable object's size cannot change, so one HEAD per object per
        // process is the whole cost, regardless of how long the process runs.
        let root = tmp_root("size-once");
        let backend = RecordingBackend::new(&[1u8; 4096]);
        let rt = runtime(&root, 1 << 20, Arc::clone(&backend)).await;
        let obj = object("f.parquet");

        for i in 0..5u64 {
            rt.serve(&req(&obj, i * 64, 64)).await.unwrap();
        }
        assert_eq!(backend.heads.load(Ordering::Relaxed), 1);
        assert_eq!(rt.stats().size_lookups, 1);
        assert_eq!(rt.stats().size_cache_hits, 4);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn each_object_pays_its_own_size_lookup() {
        let root = tmp_root("size-per-object");
        let backend = RecordingBackend::new(&[1u8; 4096]);
        let rt = runtime(&root, 1 << 20, Arc::clone(&backend)).await;

        for n in 0..3 {
            rt.serve(&req(&object(&format!("p-{n}.parquet")), 0, 64))
                .await
                .unwrap();
        }
        assert_eq!(rt.stats().size_lookups, 3);
        assert_eq!(rt.stats().size_cache_hits, 0);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_republished_object_is_served_from_cache_indefinitely() {
        // The accepted limitation, pinned so it stays a decision rather than a
        // surprise. Nothing here tracks a version, so an overwrite at the same
        // path is invisible: the stream id is unchanged, the cached extent
        // answers, and no refresh ever comes along to notice. Objects this
        // worker serves are assumed immutable — ADR 0005 §3.
        let root = tmp_root("republish");
        let backend = RecordingBackend::new(b"aaaaaaaa");
        let rt = runtime(&root, 1 << 20, Arc::clone(&backend)).await;
        let obj = object("f.parquet");

        assert_eq!(
            body_of(&rt.serve(&req(&obj, 0, 8)).await.unwrap()),
            Bytes::from_static(b"aaaaaaaa")
        );

        backend.republish(b"bbbbbbbb");
        assert_eq!(
            body_of(&rt.serve(&req(&obj, 0, 8)).await.unwrap()),
            Bytes::from_static(b"aaaaaaaa"),
            "a republish is not detected, by design"
        );
        assert_eq!(backend.heads.load(Ordering::Relaxed), 1, "no re-HEAD");
        assert_eq!(backend.fetches(), 1);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn stat_object_goes_to_the_origin_every_time() {
        // The one place an ETag still matters: a client asking for metadata
        // gets the origin's answer, not a cached size. Proxied straight
        // through so `stat` never reports a version this worker invented.
        let root = tmp_root("stat");
        let backend = RecordingBackend::new(&[1u8; 512]);
        let rt = runtime(&root, 1 << 20, Arc::clone(&backend)).await;
        let obj = object("f.parquet");

        rt.serve(&req(&obj, 0, 64)).await.unwrap();
        let stat = rt.stat_object(&obj).await.unwrap();

        assert_eq!(stat.len, 512);
        assert_eq!(stat.version, Version::new("etag-1"));
        assert_eq!(
            backend.heads.load(Ordering::Relaxed),
            2,
            "the read's cached size must not answer a stat"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn with_l1_disabled_a_disk_hit_is_served_zero_copy() {
        let root = tmp_root("sendfile");
        let backend = RecordingBackend::new(&[0x5Au8; 8192]);
        let rt = runtime(&root, 0, Arc::clone(&backend)).await;
        let obj = object("f.parquet");

        // First read misses everywhere and comes back as Bytes.
        let first = rt.serve(&req(&obj, 0, 4096)).await.unwrap();
        assert!(matches!(first, ServeOutcome::Bytes(_)));
        rt.cache().flush().await;

        // Second read finds it on disk and hands back a pinned handle.
        let second = rt.serve(&req(&obj, 0, 4096)).await.unwrap();
        match &second {
            ServeOutcome::Sendfile(p) => assert_eq!(p.len(), 4096),
            other => panic!("expected Sendfile, got {other:?}"),
        }
        assert_eq!(rt.stats().sendfile_served, 1);
        assert_eq!(backend.fetches(), 1);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn with_l1_enabled_reads_stay_in_memory() {
        // L2 is a promotion source, not a serve source, when there is a DRAM
        // tier to promote into.
        let root = tmp_root("nosendfile");
        let backend = RecordingBackend::new(&[1u8; 8192]);
        let rt = runtime(&root, 1 << 20, Arc::clone(&backend)).await;
        let obj = object("f.parquet");

        for _ in 0..3 {
            let out = rt.serve(&req(&obj, 0, 4096)).await.unwrap();
            assert!(matches!(out, ServeOutcome::Bytes(_)));
        }
        assert_eq!(rt.stats().sendfile_served, 0);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_request_for_the_wrong_backend_is_refused() {
        let root = tmp_root("backend");
        let backend = RecordingBackend::new(&[1u8; 64]);
        let rt = runtime(&root, 1 << 20, Arc::clone(&backend))
            .await
            .with_configured_backend(Backend::Gcs);

        let err = rt
            .serve(&req(&object("f.parquet"), 0, 8))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("configured for"), "unhelpful error: {err}");
        assert_eq!(backend.heads.load(Ordering::Relaxed), 0);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn concurrent_reads_of_one_extent_hit_the_origin_once() {
        let root = tmp_root("stampede");
        let backend = RecordingBackend::new(&[4u8; 65536]);
        let rt = Arc::new(runtime(&root, 1 << 20, Arc::clone(&backend)).await);
        let obj = object("f.parquet");

        let mut handles = Vec::new();
        for _ in 0..16 {
            let rt = Arc::clone(&rt);
            let obj = obj.clone();
            handles.push(tokio::spawn(async move {
                rt.serve(&req(&obj, 0, 8192)).await.map(|o| o.len())
            }));
        }
        for h in handles {
            assert_eq!(h.await.unwrap().unwrap(), 8192);
        }
        assert_eq!(backend.fetches(), 1, "16 readers stampeded the origin");

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn a_write_is_refused_with_a_pointer_to_the_right_pool() {
        let err = AsyncWorkerRuntime::write_unsupported(&object("f.parquet")).to_string();
        assert!(err.contains("read-only"));
        assert!(
            err.contains("talon-worker"),
            "must name the right pool: {err}"
        );
    }
}
