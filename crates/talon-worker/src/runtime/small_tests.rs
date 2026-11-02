use super::*;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use talon_core::WorkerConfig;

struct Origin {
    fetches: AtomicUsize,
    short: AtomicBool,
    ranges: Mutex<Vec<(u64, u64)>>,
}
fn object() -> ObjectId {
    ObjectId::new(Backend::S3, "bucket", "small")
}
fn data() -> Bytes {
    Bytes::from((0..3072).map(|n| (n % 251) as u8).collect::<Vec<_>>())
}
#[async_trait]
impl BackendStore for Origin {
    async fn head(&self, _: &ObjectId) -> talon_core::Result<talon_core::ObjectStat> {
        Ok(talon_core::ObjectStat {
            len: 3072,
            version: Version::new("v1"),
        })
    }
    async fn fetch_range(&self, _: &ObjectId, offset: u64, len: u64) -> talon_core::Result<Bytes> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        self.ranges.lock().unwrap().push((offset, len));
        tokio::time::sleep(Duration::from_millis(5)).await;
        if self.short.load(Ordering::SeqCst) {
            return Ok(Bytes::from_static(b"short"));
        }
        let bytes = data();
        let end = offset.saturating_add(len).min(bytes.len() as u64) as usize;
        Ok(bytes.slice(offset as usize..end))
    }
    async fn fetch_range_if_match(
        &self,
        obj: &ObjectId,
        offset: u64,
        len: u64,
        version: Option<&Version>,
    ) -> talon_core::Result<Bytes> {
        if version.is_some_and(|v| v.as_str() != "v1") {
            return Err(Error::VersionMismatch {
                expected: version.unwrap().0.clone(),
                found: "v1".into(),
            });
        }
        self.fetch_range(obj, offset, len).await
    }
    async fn put(&self, _: &ObjectId, _: Bytes) -> talon_core::Result<Version> {
        Ok(Version::new("v1"))
    }
    async fn delete(&self, _: &ObjectId) -> talon_core::Result<()> {
        Ok(())
    }
}
fn runtime(root: &Path, paged: bool, l1: bool) -> (Arc<Origin>, WorkerRuntime) {
    runtime_with_geometry(root, paged, l1, 128 << 10, 256 << 10, 16 << 20)
}

fn runtime_with_geometry(
    root: &Path,
    paged: bool,
    l1: bool,
    threshold: u64,
    segment: u64,
    capacity: u64,
) -> (Arc<Origin>, WorkerRuntime) {
    let backend = Arc::new(Origin {
        fetches: AtomicUsize::new(0),
        short: AtomicBool::new(false),
        ranges: Mutex::new(Vec::new()),
    });
    let store = WholeBlockStore::open(root).unwrap();
    let small = cache_routing::open_small_store(root, threshold, segment, capacity, None).unwrap();
    let index = Arc::new(BlockIndex::new());
    store.restore_index(&index).unwrap();
    let mut worker = WorkerRuntime::new_with_l1_policy(
        store,
        small,
        index,
        Arc::new(InFlightLoads::new()),
        backend.clone(),
        1 << 20,
        capacity,
        if l1 { 1 << 20 } else { 0 },
        1024,
        WorkerMetrics::new(capacity),
        true,
    );
    if paged {
        worker = worker.with_paged_store(PagedBlockStore::open(root.join("paged"), 1024).unwrap());
    }
    (backend, worker)
}
fn request() -> RangeRequest {
    RangeRequest {
        object: object(),
        offset: 100,
        len: 500,
    }
}
fn id(worker: &WorkerRuntime) -> BlockId {
    worker.block_for(&object(), 0, &Version::new("v1"))
}

#[tokio::test]
async fn small_l2_promotion_retains_only_each_pages_allocation() {
    let dir = tempfile::tempdir().unwrap();
    let (backend, mut w) =
        runtime_with_geometry(dir.path(), false, false, 1 << 20, 2 << 20, 16 << 20);
    let block = id(&w);
    let payload = Bytes::from(vec![7; 512 << 10]);
    w.l1 = Arc::new(crate::MemoryStore::with_limits(512 << 10, 4096));
    // Public admission, like LOAD, leaves L1 empty.
    let admission = CachedBlockPutRequest {
        block: block.clone(),
        object_len: payload.len() as u64,
        body_len: payload.len() as u64,
    };
    w.admit_cached_block(&admission, payload.clone())
        .await
        .unwrap();
    assert_eq!(w.l1.len(), 0);
    for page in 0..128 {
        let request = RangeRequest {
            object: object(),
            offset: page * 4096,
            len: 4096,
        };
        let ServeOutcome::Bytes(bytes) = w
            .serve_versioned(&request, &block.version, payload.len() as u64)
            .await
            .unwrap()
        else {
            panic!("L1 enabled read must return bytes");
        };
        assert_eq!(bytes.as_ref(), &payload[..4096]);
        assert_eq!(
            bytes.try_into_mut().unwrap().capacity(),
            4096,
            "the response must not retain the full object or share a cached page"
        );
    }
    assert_eq!(backend.fetches.load(Ordering::SeqCst), 0);
    assert_eq!(w.l1.resident_bytes(), 512 << 10);
    let pages: Vec<_> = (0..128)
        .map(|p| w.l1.get_page(&block, PageIndex(p)).unwrap())
        .collect();
    w.invalidate_l1(&block);
    for page in pages {
        let allocation = page
            .try_into_mut()
            .expect("each page owns its backing allocation");
        assert_eq!(
            allocation.capacity(),
            4096,
            "page must not retain the full 512 KiB read"
        );
    }
    // A multi-page promotion must also detach siblings, so retaining just one
    // of them after eviction cannot pin an entire promotion window.
    drop(w.cached_small_range(&block, 0, 8192).await.unwrap());
    let first = w.l1.get_page(&block, PageIndex(0)).unwrap();
    let second = w.l1.get_page(&block, PageIndex(1)).unwrap();
    w.invalidate_l1(&block);
    assert_eq!(first.try_into_mut().unwrap().capacity(), 4096);
    assert_eq!(second.try_into_mut().unwrap().capacity(), 4096);
    // Detaching the result must not weaken full-object checksum validation.
    let payload_path = std::fs::read_dir(dir.path().join("small/ring-0"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            p.extension().is_some_and(|e| e == "data") && std::fs::metadata(p).unwrap().len() > 0
        })
        .unwrap();
    use std::os::unix::fs::FileExt;
    std::fs::OpenOptions::new()
        .write(true)
        .open(payload_path)
        .unwrap()
        .write_all_at(&[9], payload.len() as u64 - 1)
        .unwrap();
    assert!(w
        .cached_small_range(&block, 0, 1)
        .await
        .unwrap_err()
        .to_string()
        .contains("checksum"));
}

#[tokio::test]
async fn small_routing_rejects_whole_only_upgrade_without_orphaning_files() {
    for versioned in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let block = BlockId::new(object(), 0, 1 << 20, Version::new("v1"));
        let store = WholeBlockStore::open(dir.path()).unwrap();
        if versioned {
            assert!(
                cache_routing::open_small_store(dir.path(), 0, 256 << 10, 16 << 20, None)
                    .unwrap()
                    .is_none()
            );
        }
        store.put(&block, data()).await.unwrap();
        let allocated = store.scan_allocated().unwrap()[0].allocated.clone();
        drop(store);
        assert!(
            cache_routing::open_small_store(dir.path(), 128 << 10, 256 << 10, 16 << 20, None)
                .is_err()
        );
        let store = WholeBlockStore::open(dir.path()).unwrap();
        assert_eq!(store.get_bytes(&block).await.unwrap(), data());
        assert_eq!(store.scan_allocated().unwrap()[0].allocated, allocated);
        assert!(!dir.path().join("small").exists());
        drop(store);
        if versioned {
            let (_, worker) =
                runtime_with_geometry(dir.path(), false, false, 0, 256 << 10, 16 << 20);
            let admission = CachedBlockPutRequest {
                block: block.clone(),
                object_len: 3072,
                body_len: 3072,
            };
            worker.admit_cached_block(&admission, data()).await.unwrap();
            let bytes = worker.resident_bytes();
            assert_eq!(worker.block_count(), 1);
            assert_eq!(
                worker.store.scan_allocated().unwrap()[0].allocated[0].1,
                bytes
            );
            drop(worker);
            let (_, worker) =
                runtime_with_geometry(dir.path(), false, false, 0, 256 << 10, 16 << 20);
            assert_eq!(worker.resident_bytes(), bytes);
            assert_eq!(worker.block_count(), 1);
        }
    }
}

#[tokio::test]
async fn small_background_reclaims_after_bounded_scans_of_hot_or_pinned_keys() {
    for pinned in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (_, mut w) = runtime(dir.path(), false, false);
        w.page_gc_config.scan_batch_size = 2;
        w.page_gc_config.delete_batch_size = 1;
        let mut blocks = Vec::new();
        for n in 0..16 {
            let mut block = id(&w);
            block.object.object_path = format!("hot-{n}");
            w.commit_cached_block(&block, Bytes::from(vec![n; 16 << 10]))
                .await
                .unwrap();
            blocks.push(block);
            if n == 3 {
                w.maintain_small(true).await.unwrap();
            }
        }
        w.maintain_small(true).await.unwrap();
        for block in &blocks[..3] {
            w.unlink_units(w.lru.block_candidates(block), 2).await;
        }
        let before = w.resident_bytes();
        let config = WorkerConfig {
            async_eviction_low_watermark: 0.1,
            ..Default::default()
        };
        let target = (before as f64 * config.async_eviction_low_watermark) as u64;
        let credit = w.small_reclaim_credit().await;
        assert!(credit > 0 && credit < before - target);
        assert!(w.lru.len() > w.page_gc_config.scan_batch_size);
        let live = w.block_count();
        let _pins: Vec<_> = if pinned {
            blocks[3..]
                .iter()
                .map(|block| w.lru.pin_guard(CacheUnit::Whole(block.clone())).unwrap())
                .collect()
        } else {
            Vec::new()
        };
        let mut cycle = async_eviction::EvictionCycle::new(before, &config);
        for _ in 0..=w.lru.len() {
            if !pinned {
                // Ordinary reads renew second chances between every bounded scan.
                for block in &blocks[3..] {
                    assert!(w.cached_small_range(block, 0, 1).await.unwrap().is_some());
                }
            }
            cycle.tick(&w).await;
            w.maintain_small_background().await;
            if w.resident_bytes() < before {
                break;
            }
        }
        assert!(
            w.resident_bytes() < before,
            "eligible garbage must not be gated forever by Selecting"
        );
        assert_eq!(
            w.block_count(),
            live,
            "reclaim existing garbage without evicting hot or pinned survivors"
        );
    }
}

#[tokio::test]
async fn small_read_fetches_once_and_returns_a_segment_range() {
    for paged in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (backend, w) = runtime(dir.path(), paged, false);
        match w
            .serve_versioned(&request(), &Version::new("v1"), 3072)
            .await
            .unwrap()
        {
            ServeOutcome::Bytes(bytes) => assert_eq!(bytes, data().slice(100..600)),
            _ => panic!("cold read"),
        }
        assert_eq!(*backend.ranges.lock().unwrap(), vec![(0, 3072)]);
        assert!(w.small.as_ref().unwrap().contains(&id(&w)));
        assert!(w.store.scan().unwrap().is_empty(), "no per-object sidecar");
        if let Some(p) = &w.paged {
            assert!(p.scan().unwrap().is_empty());
        }
        assert!(matches!(
            w.serve_versioned(&request(), &Version::new("v1"), 3072)
                .await
                .unwrap(),
            ServeOutcome::Sendfile(_)
        ));
        let cached = CachedRangeRequest {
            object: object(),
            version: Version::new("v1"),
            offset: 100,
            len: 500,
        };
        assert!(w.serve_cached(&cached).await.unwrap_err().is::<CacheMiss>());
        assert!(
            w.index.is_empty(),
            "packed objects own their only residency directory"
        );
        assert_eq!(w.block_count(), 1);
        assert_eq!(backend.fetches.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn small_load_deduplicates_whole_objects_and_does_not_populate_l1() {
    for paged in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (backend, w) = runtime(dir.path(), paged, true);
        let block = id(&w);
        let (a, b) = tokio::join!(w.load_block(&block, 3072), w.load_block(&block, 3072));
        a.unwrap();
        b.unwrap();
        assert_eq!(*backend.ranges.lock().unwrap(), vec![(0, 3072)]);
        assert_eq!(w.l1_page_count(), 0);
        w.load_block(&block, 3072).await.unwrap();
        assert_eq!(w.l1_page_count(), 0);
        w.serve_versioned(&request(), &Version::new("v1"), 3072)
            .await
            .unwrap();
        assert!(w.l1_page_count() > 0);
    }
}

#[tokio::test]
async fn small_residency_and_reads_survive_restart_without_block_index() {
    for paged in [false, true] {
        for l1 in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (_, w) = runtime(dir.path(), paged, l1);
            w.serve_versioned(&request(), &Version::new("v1"), 3072)
                .await
                .unwrap();
            assert!(w.index.is_empty());
            assert_eq!(w.block_count(), 1);
            assert_eq!(w.logical_resident_bytes(), 3072);
            w.maintain_small(true).await.unwrap();
            drop(w);
            let (origin, w) = runtime(dir.path(), paged, l1);
            assert!(w.index.is_empty());
            assert_eq!(w.block_count(), 1);
            for _ in 0..2 {
                let result = w
                    .serve_versioned(&request(), &Version::new("v1"), 3072)
                    .await
                    .unwrap();
                let bytes = match result {
                    ServeOutcome::Bytes(bytes) => bytes.to_vec(),
                    result => super::tests::read_handle(result),
                };
                assert_eq!(bytes, data().slice(100..600));
            }
            w.load_block(&id(&w), 3072).await.unwrap();
            assert_eq!(origin.fetches.load(Ordering::SeqCst), 0);
            assert!(w.index.is_empty());
            assert_eq!(w.l1_page_count() > 0, l1);
            // Cache-only requests intentionally retain the ordinary-store API.
            let cached = CachedRangeRequest {
                object: object(),
                version: Version::new("v1"),
                offset: 100,
                len: 500,
            };
            assert!(w.serve_cached(&cached).await.unwrap_err().is::<CacheMiss>());
            // At the threshold the object belongs to ordinary storage, which
            // still participates in BlockIndex and cache-only reads.
            let mut large = id(&w);
            large.object.object_path = "at-threshold".into();
            w.commit_cached_block(&large, Bytes::from(vec![7; 128 << 10]))
                .await
                .unwrap();
            assert_eq!(w.index.len(), 1);
            assert_eq!(w.block_count(), 2);
            let cached = CachedRangeRequest {
                object: large.object,
                version: large.version,
                offset: 100,
                len: 500,
            };
            assert_eq!(
                w.serve_cached(&cached).await.unwrap(),
                Bytes::from(vec![7; 500])
            );
        }
    }
}

#[tokio::test]
async fn small_routing_keeps_the_resolved_version_and_size_together() {
    for paged in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (origin, w) = runtime(dir.path(), paged, false);
        let stat = w.resolve_object_stat(&object(), false).await.unwrap();
        w.store_version(&object(), &Version::new("v2"), 1 << 20);
        w.serve_at(&request(), &stat.version, Some(stat.len))
            .await
            .unwrap();
        assert!(w.index.is_empty());
        assert!(w.small.as_ref().unwrap().contains(&id(&w)));
        w.serve_range_at(&request(), &stat.version, Some(stat.len))
            .await
            .unwrap();
        assert_eq!(origin.fetches.load(Ordering::SeqCst), 1);
        assert_eq!(w.cached_version(&object()), Some(Version::new("v2")));
    }
}

#[tokio::test]
async fn small_and_ordinary_admissions_cannot_share_an_identity() {
    let dir = tempfile::tempdir().unwrap();
    let (_, w) = runtime(dir.path(), true, false);
    let block = id(&w);
    w.write_object(&object(), data()).await.unwrap();
    assert!(w
        .serve_versioned(&request(), &block.version, 128 << 10)
        .await
        .is_err());
    assert!(w
        .commit_cached_block(&block, Bytes::from(vec![7; 128 << 10]))
        .await
        .is_err());
    assert!(w.index.is_empty());
    assert!(w.paged.as_ref().unwrap().scan().unwrap().is_empty());
    assert_eq!(w.block_count(), 1);
    assert_eq!(
        w.small.as_ref().unwrap().read(&block).await.unwrap(),
        Some(data())
    );
    w.delete_object(&object()).await.unwrap();
    assert_eq!(w.block_count(), 0);
    assert!(w.lru.block_candidates(&block).is_empty());
    assert_eq!(w.lru.len(), 0);

    for paged in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (_, w) = runtime(dir.path(), paged, false);
        let block = id(&w);
        if paged {
            w.commit_fetched_page(&block, PageIndex(0), 128 << 10, Bytes::from(vec![7; 1024]))
                .await
                .unwrap();
        } else {
            w.commit_cached_block(&block, Bytes::from(vec![7; 128 << 10]))
                .await
                .unwrap();
        }
        assert!(w.commit_cached_block(&block, data()).await.is_err());
        assert!(!w.small.as_ref().unwrap().contains(&block));
        assert_eq!(w.index.len(), 1);
    }
}

#[tokio::test]
async fn small_routing_rejects_incompatible_or_unversioned_cache_at_startup() {
    let dir = tempfile::tempdir().unwrap();
    let (_, w) = runtime(dir.path(), false, false);
    w.write_object(&object(), data()).await.unwrap();
    w.maintain_small(true).await.unwrap();
    drop(w);
    let marker = dir.path().join("small-routing.json");
    let original = std::fs::read(&marker).unwrap();
    for threshold in [0, 64 << 10, 256 << 10] {
        let error =
            cache_routing::open_small_store(dir.path(), threshold, 512 << 10, 16 << 20, None)
                .err()
                .unwrap();
        assert!(error
            .to_string()
            .contains("incompatible small store routing"));
        assert_eq!(std::fs::read(&marker).unwrap(), original);
    }
    let (_, recovered) = runtime(dir.path(), false, false);
    assert_eq!(recovered.block_count(), 1);
    drop(recovered);
    std::fs::write(&marker, b"broken").unwrap();
    let error = cache_routing::open_small_store(dir.path(), 128 << 10, 256 << 10, 16 << 20, None)
        .err()
        .unwrap();
    assert!(error.to_string().contains("invalid small store routing"));
    std::fs::remove_file(&marker).unwrap();
    let error = cache_routing::open_small_store(dir.path(), 128 << 10, 256 << 10, 16 << 20, None)
        .err()
        .unwrap();
    assert!(error
        .to_string()
        .contains("predates small store routing metadata"));
    assert!(
        !marker.exists(),
        "a failed compatibility check must not adopt the old cache"
    );
    assert!(dir.path().join("small").exists());
}

#[tokio::test]
async fn small_short_origin_and_wrong_version_are_never_published() {
    let dir = tempfile::tempdir().unwrap();
    let (backend, w) = runtime(dir.path(), true, false);
    backend.short.store(true, Ordering::SeqCst);
    assert!(w
        .serve_versioned(&request(), &Version::new("v1"), 3072)
        .await
        .is_err());
    assert!(w.index.is_empty());
    assert!(!w.small.as_ref().unwrap().contains(&id(&w)));
    backend.short.store(false, Ordering::SeqCst);
    assert!(w
        .serve_versioned(&request(), &Version::new("v2"), 3072)
        .await
        .is_err());
    assert!(w.index.is_empty());
}

#[tokio::test]
async fn small_admission_write_through_delete_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (_, w) = runtime(dir.path(), true, false);
    w.write_object(&object(), data()).await.unwrap();
    w.maintain_small(true).await.unwrap();
    drop(w);
    let (backend, w) = runtime(dir.path(), true, false);
    assert!(matches!(
        w.serve_versioned(&request(), &Version::new("v1"), 3072)
            .await
            .unwrap(),
        ServeOutcome::Sendfile(_)
    ));
    assert_eq!(backend.fetches.load(Ordering::SeqCst), 0);
    let admission = CachedBlockPutRequest {
        block: id(&w),
        object_len: 3072,
        body_len: 3072,
    };
    w.admit_cached_block(&admission, data()).await.unwrap();
    w.stat_object(&object()).await.unwrap();
    w.delete_object(&object()).await.unwrap();
    assert!(!w.small.as_ref().unwrap().contains(&id(&w)));
    w.maintain_small(true).await.unwrap();
    drop(w);
    let (_, w) = runtime(dir.path(), true, false);
    assert!(!w.small.as_ref().unwrap().contains(&id(&w)));
}

#[tokio::test]
async fn small_cache_pressure_counts_dead_and_retired_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let (_, w) = runtime(dir.path(), false, false);
    w.write_object(&object(), data()).await.unwrap();
    w.maintain_small(true).await.unwrap();
    let handle = w
        .small
        .as_ref()
        .unwrap()
        .get(&id(&w))
        .await
        .unwrap()
        .unwrap();
    w.evict_to_target(0).await;
    assert!(w.index.is_empty());
    assert!(
        w.resident_bytes() > 0,
        "open retired inode still occupies disk"
    );
    drop(handle);
    let stats = w.small.as_ref().unwrap().stats().unwrap();
    assert_eq!(stats.retired_bytes, 0);
    assert_eq!(stats.live_bytes, 0);
    assert_eq!(stats.segments, 1, "only the prepared spare remains");
    assert_eq!(w.resident_bytes(), stats.allocated_bytes);
    assert!(
        stats.allocated_bytes <= 8192,
        "only the spare index header is allocated"
    );
}

#[tokio::test]
async fn small_idle_eviction_uses_whole_object_access_and_preserves_open_fd() {
    let dir = tempfile::tempdir().unwrap();
    let (_, mut w) = runtime(dir.path(), true, false);
    w.page_gc_config.tti_ms = 1000;
    w.write_object(&object(), data()).await.unwrap();
    let handle = w
        .small
        .as_ref()
        .unwrap()
        .get(&id(&w))
        .await
        .unwrap()
        .unwrap();
    w.gc_small_idle_at(crate::eviction::access_millis() + 2000)
        .await;
    assert!(!w.small.as_ref().unwrap().contains(&id(&w)));
    assert!(w.index.is_empty());
    use std::os::unix::fs::FileExt;
    let file = std::fs::File::from(handle.fd.try_clone().unwrap());
    let mut bytes = vec![0; 3072];
    file.read_exact_at(&mut bytes, handle.offset).unwrap();
    assert_eq!(bytes, data());
}

#[tokio::test]
async fn small_range_from_large_object_still_uses_paged_storage() {
    let dir = tempfile::tempdir().unwrap();
    let (_, w) = runtime(dir.path(), true, false);
    w.serve_versioned(&request(), &Version::new("v1"), 1 << 20)
        .await
        .unwrap();
    assert!(!w.small.as_ref().unwrap().contains(&id(&w)));
    assert_eq!(w.paged.as_ref().unwrap().scan().unwrap().len(), 1);
}

#[tokio::test]
async fn small_foreground_pressure_reclaims_after_admission_without_async_gc() {
    let dir = tempfile::tempdir().unwrap();
    let (_, w) = runtime_with_geometry(dir.path(), false, false, 128 << 10, 256 << 10, 1 << 20);
    for n in 0..400 {
        let obj = ObjectId::new(Backend::S3, "bucket", format!("object-{n}"));
        w.write_object(&obj, data()).await.unwrap();
        assert!(w.resident_bytes() <= w.capacity_bytes);
    }
    assert!(w.block_count() < 400);
}

#[tokio::test]
async fn small_concurrent_admission_succeeds_under_capacity_pressure() {
    let dir = tempfile::tempdir().unwrap();
    let (_, mut w) = runtime_with_geometry(dir.path(), false, false, 128 << 10, 256 << 10, 1 << 20);
    w.background_budget = Some(crate::background::BackgroundBudget::new(
        &talon_core::WorkerConfig::default(),
    ));
    for batch in 0..4 {
        let mut writes = tokio::task::JoinSet::new();
        for n in batch * 128..(batch + 1) * 128 {
            let w = w.clone();
            writes.spawn(async move {
                let obj = ObjectId::new(Backend::S3, "bucket", format!("concurrent-{n}"));
                w.write_object(&obj, Bytes::from(vec![n as u8; 4096])).await
            });
        }
        while let Some(result) = writes.join_next().await {
            result.unwrap().unwrap();
        }
    }
    w.enforce_capacity().await;
    assert!(w.resident_bytes() <= w.capacity_bytes);
    assert!(w.block_count() < 512);
    // A subsequent admission still publishes intact bytes after reclamation.
    let obj = ObjectId::new(Backend::S3, "bucket", "after-pressure");
    let version = w.write_object(&obj, data()).await.unwrap();
    let block = BlockId::new(obj, 0, 1 << 20, version);
    assert_eq!(
        w.small
            .as_ref()
            .unwrap()
            .read(&block)
            .await
            .unwrap()
            .unwrap(),
        data()
    );
}

#[tokio::test]
async fn small_tti_checkpoint_survives_restart_relocation_and_deletion() {
    let dir = tempfile::tempdir().unwrap();
    let (_, mut w) = runtime(dir.path(), false, false);
    w.page_gc_config.tti_ms = 3_600_000;
    let mut blocks = Vec::new();
    for n in 0..4 {
        let mut block = id(&w);
        block.object.object_path = format!("tti-{n}");
        w.commit_cached_block(&block, Bytes::from(vec![n; 48 << 10]))
            .await
            .unwrap();
        blocks.push(block);
    }
    w.maintain_small(true).await.unwrap();
    let cold = crate::eviction::access_millis() - 7_200_000;
    w.small
        .as_ref()
        .unwrap()
        .access(&blocks[0])
        .unwrap()
        .restore(cold);
    w.checkpoint_small_access().await;
    assert!(dir.path().join("small/access.checkpoint").exists());
    // A stale snapshot still contains the deleted identity. Relocate the other
    // records without rewriting their access snapshot, then restart.
    w.unlink_units(w.lru.block_candidates(&blocks[1]), 2).await;
    w.unlink_units(w.lru.block_candidates(&blocks[2]), 2).await;
    assert!(w
        .small
        .as_ref()
        .unwrap()
        .reclaim_with_budget(u64::MAX, None)
        .await
        .unwrap());
    drop(w);
    let (_, mut recovered) = runtime(dir.path(), false, false);
    recovered.page_gc_config.tti_ms = 3_600_000;
    assert_eq!(
        recovered
            .small
            .as_ref()
            .unwrap()
            .access(&blocks[0])
            .unwrap()
            .last_access(),
        cold
    );
    assert!(!recovered.contains_block(&blocks[1]));
    assert!(!recovered.contains_block(&blocks[2]));
    recovered
        .gc_small_idle_at(crate::eviction::access_millis())
        .await;
    assert!(
        !recovered.contains_block(&blocks[0]),
        "cold object must expire immediately after restart"
    );
    for block in &blocks[3..] {
        assert!(
            recovered.contains_block(block),
            "recent objects retain their idle window"
        );
    }
}

#[tokio::test]
async fn small_tti_hit_is_checkpointed_and_bad_snapshot_does_not_drop_data() {
    let dir = tempfile::tempdir().unwrap();
    let (_, mut w) = runtime(dir.path(), false, false);
    w.page_gc_config.tti_ms = 1000;
    w.write_object(&object(), data()).await.unwrap();
    let block = id(&w);
    w.small
        .as_ref()
        .unwrap()
        .access(&block)
        .unwrap()
        .restore(crate::eviction::access_millis() - 10_000);
    w.serve_versioned(&request(), &Version::new("v1"), 3072)
        .await
        .unwrap();
    let last = w
        .small
        .as_ref()
        .unwrap()
        .access(&block)
        .unwrap()
        .last_access();
    w.maintain_small(true).await.unwrap();
    w.checkpoint_small_access().await;
    drop(w);
    let (_, w) = runtime(dir.path(), false, false);
    assert_eq!(
        w.small
            .as_ref()
            .unwrap()
            .access(&block)
            .unwrap()
            .last_access(),
        last
    );
    drop(w);
    let path = dir.path().join("small/access.checkpoint");
    let mut corrupt = std::fs::read(&path).unwrap();
    *corrupt.last_mut().unwrap() ^= 1;
    std::fs::write(path, corrupt).unwrap();
    let before = crate::eviction::access_millis();
    let (_, w) = runtime(dir.path(), false, false);
    assert!(
        w.small
            .as_ref()
            .unwrap()
            .access(&block)
            .unwrap()
            .last_access()
            >= before
    );
    assert_eq!(
        w.small
            .as_ref()
            .unwrap()
            .read(&block)
            .await
            .unwrap()
            .unwrap(),
        data()
    );
    // Future wall-clock values are clamped, not made immortal by clock rollback.
    w.small
        .as_ref()
        .unwrap()
        .access(&block)
        .unwrap()
        .restore(u64::MAX);
    assert!(
        w.small
            .as_ref()
            .unwrap()
            .access(&block)
            .unwrap()
            .last_access()
            <= crate::eviction::access_millis()
    );
}

#[tokio::test]
async fn small_tti_background_and_shutdown_checkpoint_without_paged_cache() {
    let dir = tempfile::tempdir().unwrap();
    let (_, mut w) = runtime(dir.path(), false, false);
    w = w
        .with_page_gc(crate::page_gc::PageGcConfig {
            tti_ms: 3_600_000,
            checkpoint_interval_ms: 1,
            ..Default::default()
        })
        .unwrap();
    w.write_object(&object(), data()).await.unwrap();
    let block = id(&w);
    let w = Arc::new(w);
    let service = WorkerBackground::start(
        w.clone(),
        &WorkerConfig {
            page_tti_hours: 1,
            stale_gc_enabled: false,
            ownership_gc_enabled: false,
            async_eviction_enabled: false,
            background_io_max_mb_per_sec: 0,
            ..Default::default()
        },
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        while !dir.path().join("small/access.checkpoint").exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    // Change age after the periodic checkpoint; orderly shutdown must flush it.
    let cold = crate::eviction::access_millis() - 60_000;
    w.small
        .as_ref()
        .unwrap()
        .access(&block)
        .unwrap()
        .restore(cold);
    service.shutdown().await;
    drop(w);
    let (_, w) = runtime(dir.path(), false, false);
    assert_eq!(
        w.small
            .as_ref()
            .unwrap()
            .access(&block)
            .unwrap()
            .last_access(),
        cold
    );
}

#[tokio::test]
async fn small_background_capacity_keeps_evicting_until_segment_threshold() {
    for sealed in [false, true] {
        for predeleted in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (_, mut w) = runtime(dir.path(), false, false);
            w.page_gc_config.delete_batch_size = 1;
            let mut blocks = Vec::new();
            for n in 0..4 {
                let mut block = id(&w);
                block.object.object_path = format!("pressure-{n}");
                w.commit_cached_block(&block, Bytes::from(vec![n; 48 << 10]))
                    .await
                    .unwrap();
                blocks.push(block);
            }
            if sealed {
                w.maintain_small(true).await.unwrap();
            }
            let before = w.resident_bytes();
            if predeleted {
                w.unlink_units(w.lru.block_candidates(&blocks[0]), 2).await;
                assert_eq!(
                    w.resident_bytes(),
                    before,
                    "tombstone alone does not release payload allocation"
                );
                w.maintain_small(false).await.unwrap();
                assert_eq!(w.block_count(), 3);
                assert_eq!(
                    w.resident_bytes(),
                    before,
                    "25% garbage is below routine compaction threshold"
                );
            }
            let config = WorkerConfig::default();
            let target = (before as f64 * config.async_eviction_low_watermark) as u64;
            let mut cycle = async_eviction::EvictionCycle::new(before, &config);
            cycle.tick(&w).await;
            if !predeleted {
                assert_eq!(w.block_count(), 3);
                assert_eq!(
                    w.resident_bytes(),
                    before,
                    "pressure cannot bypass compaction threshold"
                );
                assert_eq!(w.small.as_ref().unwrap().reclaimable_bytes().await, 0);
                cycle.tick(&w).await;
            }
            assert!(
                w.resident_bytes() <= target,
                "capacity tick must release physical space"
            );
            assert_eq!(
                w.block_count(),
                2,
                "keep selecting items until the segment reaches its garbage threshold"
            );
            let allocated = std::fs::read_dir(dir.path().join("small/ring-0"))
                .unwrap()
                .map(|e| e.unwrap().path())
                .filter(|p| {
                    matches!(
                        p.extension().and_then(|e| e.to_str()),
                        Some("data" | "index")
                    )
                })
                .map(|p| {
                    use std::os::unix::fs::MetadataExt;
                    std::fs::metadata(p).unwrap().blocks() * 512
                })
                .sum::<u64>();
            assert_eq!(
                allocated,
                w.resident_bytes(),
                "verify filesystem blocks, not just logical accounting"
            );
        }
    }
}

#[tokio::test]
async fn small_background_capacity_waits_for_retired_readers_without_extra_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let (_, w) = runtime(dir.path(), false, false);
    let mut held = None;
    for n in 0..4 {
        let mut block = id(&w);
        block.object.object_path = format!("held-{n}");
        w.commit_cached_block(&block, Bytes::from(vec![n; 48 << 10]))
            .await
            .unwrap();
        if n == 0 {
            held = w.small.as_ref().unwrap().get(&block).await.unwrap();
        }
    }
    w.maintain_small(true).await.unwrap();
    let before = w.resident_bytes();
    let config = WorkerConfig::default();
    let target = (before as f64 * config.async_eviction_low_watermark) as u64;
    let mut cycle = async_eviction::EvictionCycle::new(before, &config);
    for _ in 0..4 {
        cycle.tick(&w).await;
        if w.small.as_ref().unwrap().stats().unwrap().retired_bytes > 0 {
            break;
        }
    }
    assert_eq!(w.block_count(), 2);
    assert!(w.small.as_ref().unwrap().stats().unwrap().retired_bytes > 0);
    cycle.tick(&w).await;
    assert_eq!(
        w.block_count(),
        2,
        "unlinked reader-held space cannot be freed by deleting more objects"
    );
    drop(held);
    cycle.tick(&w).await;
    assert!(w.resident_bytes() <= target);
    assert_eq!(w.block_count(), 2);
}

#[tokio::test]
async fn small_capacity_batches_same_segment_victims_before_one_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let (_, mut w) = runtime(dir.path(), false, false);
    w.page_gc_config.delete_batch_size = 1;
    for n in 0..10 {
        let mut block = id(&w);
        block.object.object_path = format!("batch-{n}");
        w.commit_cached_block(&block, Bytes::from(vec![n; 16 << 10]))
            .await
            .unwrap();
    }
    w.maintain_small(true).await.unwrap();
    let payload_files = || {
        let mut files: Vec<_> = std::fs::read_dir(dir.path().join("small/ring-0"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| {
                p.extension().is_some_and(|e| e == "data")
                    && std::fs::metadata(p).unwrap().len() > 0
            })
            .collect();
        files.sort();
        files
    };
    let mut files = payload_files();
    assert_eq!(files.len(), 1);
    let before = w.resident_bytes();
    let config = WorkerConfig {
        async_eviction_low_watermark: 0.25,
        ..Default::default()
    };
    let target = (before as f64 * 0.25) as u64;
    let mut cycle = async_eviction::EvictionCycle::new(before, &config);
    let mut rewrites = 0;
    let mut batches = 0;
    while w.resident_bytes() > target && batches < 16 {
        cycle.tick(&w).await;
        // Even after the sixth tombstone crosses the routine GC threshold,
        // routine maintenance must not copy survivors halfway through selection.
        w.maintain_small_background().await;
        let current = payload_files();
        if current != files {
            rewrites += 1;
        }
        files = current;
        batches += 1;
    }
    assert!(
        batches >= 8,
        "victim selection must span several bounded batches"
    );
    assert!(w.resident_bytes() <= target);
    assert_eq!(
        rewrites, 1,
        "same-segment survivors must be copied only once in this cycle"
    );
    assert_eq!(
        w.block_count(),
        2,
        "pending garbage credit must prevent extra logical eviction"
    );
}

#[tokio::test]
async fn small_capacity_shares_pending_reclaim_credit_across_namespaces() {
    let dir = tempfile::tempdir().unwrap();
    let config = WorkerConfig {
        backend: Some("s3".into()),
        block_size: 1 << 20,
        capacity_bytes: 16 << 20,
        l2_page_size_bytes: 0,
        small_object_threshold_bytes: 128 << 10,
        small_object_segment_bytes: 256 << 10,
        page_gc_delete_batch_size: 1,
        background_io_max_mb_per_sec: 0,
        ..Default::default()
    };
    let origin = Arc::new(Origin {
        fetches: AtomicUsize::new(0),
        short: AtomicBool::new(false),
        ranges: Mutex::new(Vec::new()),
    });
    let lock = Arc::new(crate::page_access_store::CacheRootLock::acquire(dir.path()).unwrap());
    let worker = WorkerRuntime::open_namespaces(
        &config,
        lock,
        origin,
        WorkerMetrics::new(config.capacity_bytes),
    )
    .unwrap();
    let worker = Arc::new(worker);
    let mut namespaces = Vec::new();
    for ns in 0..2 {
        let w = worker
            .for_namespace(&talon_core::NamespaceId::named(format!("credit-{ns}")))
            .await
            .unwrap();
        for n in 0..4 {
            let mut block = id(&w);
            block.object.object_path = format!("ns-object-{n}");
            w.commit_cached_block(&block, Bytes::from(vec![n; 48 << 10]))
                .await
                .unwrap();
        }
        w.maintain_small(true).await.unwrap();
        namespaces.push(w);
    }
    let resident = || namespaces.iter().map(|w| w.resident_bytes()).sum::<u64>();
    let before = resident();
    // Only eligible garbage in another namespace may cover the shared deficit.
    let first = &namespaces[0];
    for n in 0..2 {
        let mut block = id(first);
        block.object.object_path = format!("ns-object-{n}");
        first
            .unlink_units(first.lru.block_candidates(&block), 2)
            .await;
    }
    assert!(first.small.as_ref().unwrap().reclaimable_bytes().await > 0);
    let target = (before as f64 * config.async_eviction_low_watermark) as u64;
    let mut cycle = async_eviction::EvictionCycle::new(before, &config);
    for _ in 0..8 {
        cycle.tick(&worker).await;
        if resident() <= target {
            break;
        }
    }
    assert!(resident() <= target);
    assert_eq!(
        namespaces.iter().map(|w| w.block_count()).sum::<u64>(),
        6,
        "pending tombstones in another namespace must not trigger duplicate victim selection"
    );
}

#[tokio::test]
async fn small_pressure_does_not_rewrite_64mb_segment_for_one_kib_tombstone() {
    let dir = tempfile::tempdir().unwrap();
    let (_, mut w) = runtime_with_geometry(dir.path(), false, false, 1 << 20, 64 << 20, 1 << 30);
    let mut tiny = id(&w);
    tiny.object.object_path = "tiny-victim".into();
    w.commit_cached_block(&tiny, Bytes::from(vec![1; 1024]))
        .await
        .unwrap();
    let payload = Bytes::from(vec![2; (1 << 20) - 1024]);
    let mut pins = Vec::new();
    for n in 0..64 {
        let mut block = id(&w);
        block.object.object_path = format!("large-survivor-{n}");
        w.commit_cached_block(&block, payload.clone())
            .await
            .unwrap();
        pins.push(w.lru.pin_guard(CacheUnit::Whole(block)).unwrap());
    }
    w.maintain_small(true).await.unwrap();
    let files = || {
        let mut paths: Vec<_> = std::fs::read_dir(dir.path().join("small/ring-0"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        paths.sort();
        paths
    };
    let original = files();
    let payload_files: Vec<_> = original
        .iter()
        .filter(|p| {
            p.extension().is_some_and(|e| e == "data") && std::fs::metadata(p).unwrap().len() > 0
        })
        .collect();
    assert_eq!(payload_files.len(), 1);
    assert!(std::fs::metadata(payload_files[0]).unwrap().len() > 63 << 20);
    let before = w.resident_bytes();
    w.unlink_units(w.lru.block_candidates(&tiny), 2).await;
    assert_eq!(w.small.as_ref().unwrap().reclaimable_bytes().await, 0);
    assert!(!w
        .small
        .as_ref()
        .unwrap()
        .reclaim_with_budget(u64::MAX, None)
        .await
        .unwrap());
    w.capacity_bytes = before - 4096;
    // Exhausted/unavailable victims must not force a below-threshold rewrite.
    w.enforce_capacity().await;
    let config = WorkerConfig::default();
    let mut cycle = async_eviction::EvictionCycle::new(before, &config);
    cycle.tick(&w).await;
    assert_eq!(w.block_count(), 64);
    assert_eq!(w.resident_bytes(), before);
    assert_eq!(files(), original);
    // Once unpinned, capacity enforcement keeps evicting items until the same
    // threshold permits an actual rewrite, even for a small capacity deficit.
    drop(pins);
    w.enforce_capacity().await;
    assert_eq!(w.block_count(), 32);
    assert!(w.resident_bytes() <= w.capacity_bytes);
    assert!(!payload_files[0].exists());
}

#[tokio::test]
async fn packed_versions_remain_independent_across_restart_and_exact_deletion() {
    let dir = tempfile::tempdir().unwrap();
    let (_, w) = runtime(dir.path(), true, true);
    let v1 = id(&w);
    let mut v2 = v1.clone();
    v2.version = Version::new("v2");
    w.commit_cached_block(&v1, data()).await.unwrap();
    w.commit_cached_block(&v2, data()).await.unwrap();
    for key in [&v1, &v2] {
        assert!(w.small.as_ref().unwrap().contains(key));
        assert!(w.index.get(key).is_none());
        assert!(w.small.as_ref().unwrap().access(key).is_some());
        assert_eq!(w.lru.block_candidates(key).len(), 1);
    }
    assert!(w.lru.superseded_candidates(&v2).is_empty());
    w.maintain_small(true).await.unwrap();
    drop(w);
    let (_, w) = runtime(dir.path(), true, true);
    assert!(w.lru.superseded_candidates(&v2).is_empty());
    for key in [&v1, &v2] {
        assert!(w.index.get(key).is_none());
        assert!(w.small.as_ref().unwrap().access(key).is_some());
        assert_eq!(
            w.small.as_ref().unwrap().read(key).await.unwrap().unwrap(),
            data()
        );
    }
    w.unlink_units(w.lru.block_candidates(&v1), 2).await;
    assert!(!w.small.as_ref().unwrap().contains(&v1));
    assert!(w.small.as_ref().unwrap().contains(&v2));
    assert!(w.lru.block_candidates(&v1).is_empty());
    assert_eq!(w.lru.block_candidates(&v2).len(), 1);
}

#[tokio::test]
async fn object_store_contract_with_small_engine_enabled() {
    let root = tempfile::tempdir().unwrap();
    let open = || runtime(root.path(), false, false).1;
    let worker = open();
    let id = id(&worker);
    let value = Bytes::from_static(b"short whole block");
    assert!(!worker.store.contains(&id).await.unwrap());
    worker.store.put(&id, value.clone()).await.unwrap();
    assert!(worker.store.contains(&id).await.unwrap());
    assert_eq!(worker.store.get_bytes(&id).await.unwrap(), value);
    assert_eq!(worker.store.scan_allocated().unwrap().len(), 1);
    assert!(!worker.small.as_ref().unwrap().contains(&id));
    drop(worker);
    let worker = open();
    assert!(worker.store.contains(&id).await.unwrap());
    assert_eq!(worker.store.get_bytes(&id).await.unwrap(), value);
    worker.store.delete(&id).await.unwrap();
    assert!(!worker.store.contains(&id).await.unwrap());
    drop(worker);
    assert!(!open().store.contains(&id).await.unwrap());
}
