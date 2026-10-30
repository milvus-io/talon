use super::*;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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
    let backend = Arc::new(Origin {
        fetches: AtomicUsize::new(0),
        short: AtomicBool::new(false),
        ranges: Mutex::new(Vec::new()),
    });
    let store = WholeBlockStore::open(root)
        .unwrap()
        .with_small_objects(128 << 10, 256 << 10, 16 << 20)
        .unwrap();
    let index = Arc::new(BlockIndex::new());
    store.restore_index(&index).unwrap();
    let mut worker = WorkerRuntime::new_with_l1(
        store,
        index,
        Arc::new(InFlightLoads::new()),
        backend.clone(),
        1 << 20,
        16 << 20,
        if l1 { 1 << 20 } else { 0 },
        1024,
        WorkerMetrics::new(16 << 20),
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
        assert!(w.store.small().unwrap().contains(&id(&w)));
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
        assert_eq!(
            w.serve_cached(&cached).await.unwrap(),
            data().slice(100..600)
        );
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
async fn small_short_origin_and_wrong_version_are_never_published() {
    let dir = tempfile::tempdir().unwrap();
    let (backend, w) = runtime(dir.path(), true, false);
    backend.short.store(true, Ordering::SeqCst);
    assert!(w
        .serve_versioned(&request(), &Version::new("v1"), 3072)
        .await
        .is_err());
    assert!(w.index.is_empty());
    assert!(!w.store.small().unwrap().contains(&id(&w)));
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
    w.store.maintain_small(true).await.unwrap();
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
    assert!(!w.store.small().unwrap().contains(&id(&w)));
    w.store.maintain_small(true).await.unwrap();
    drop(w);
    let (_, w) = runtime(dir.path(), true, false);
    assert!(!w.store.small().unwrap().contains(&id(&w)));
}

#[tokio::test]
async fn small_cache_pressure_counts_dead_and_retired_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let (_, w) = runtime(dir.path(), false, false);
    w.write_object(&object(), data()).await.unwrap();
    w.store.maintain_small(true).await.unwrap();
    let handle = w.store.get_block(&id(&w)).await.unwrap();
    w.evict_to_target(0).await;
    assert!(w.index.is_empty());
    assert!(
        w.resident_bytes() > 0,
        "open retired inode still occupies disk"
    );
    drop(handle);
    assert_eq!(w.resident_bytes(), 0);
}

#[tokio::test]
async fn small_idle_eviction_uses_whole_object_access_and_preserves_open_fd() {
    let dir = tempfile::tempdir().unwrap();
    let (_, mut w) = runtime(dir.path(), true, false);
    w.page_gc_config.tti_ms = 1000;
    w.write_object(&object(), data()).await.unwrap();
    let handle = w.store.get_block(&id(&w)).await.unwrap();
    w.gc_small_idle_at(crate::eviction::access_millis() + 2000)
        .await;
    assert!(!w.store.small().unwrap().contains(&id(&w)));
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
    assert!(!w.store.small().unwrap().contains(&id(&w)));
    assert_eq!(w.paged.as_ref().unwrap().scan().unwrap().len(), 1);
}

#[tokio::test]
async fn small_foreground_pressure_reclaims_after_admission_without_async_gc() {
    let dir = tempfile::tempdir().unwrap();
    let (_, mut w) = runtime(dir.path(), false, false);
    w.capacity_bytes = 1 << 20;
    w.store = Arc::new(
        WholeBlockStore::open(dir.path())
            .unwrap()
            .with_small_objects(128 << 10, 256 << 10, 1 << 20)
            .unwrap(),
    );
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
    let (_, mut w) = runtime(dir.path(), false, false);
    w.capacity_bytes = 1 << 20;
    w.store = Arc::new(
        WholeBlockStore::open(dir.path())
            .unwrap()
            .with_small_objects(128 << 10, 256 << 10, 1 << 20)
            .unwrap(),
    );
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
        w.store
            .small()
            .unwrap()
            .read(&block)
            .await
            .unwrap()
            .unwrap(),
        data()
    );
}
