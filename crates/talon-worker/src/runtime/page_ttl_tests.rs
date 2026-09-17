use super::*;
use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};
use talon_core::ObjectStat;

#[derive(Default)]
struct Origin {
    fetches: AtomicUsize,
}
#[async_trait::async_trait]
impl BackendStore for Origin {
    async fn fetch_range_if_match(
        &self,
        object: &ObjectId,
        offset: u64,
        len: u64,
        if_match: Option<&Version>,
    ) -> talon_core::Result<Bytes> {
        if let Some(expected) = if_match {
            if expected.as_str() != "v1" {
                return Err(Error::VersionMismatch {
                    expected: expected.0.clone(),
                    found: "v1".into(),
                });
            }
        }
        self.fetch_range(object, offset, len).await
    }

    async fn fetch_range(&self, _: &ObjectId, offset: u64, len: u64) -> talon_core::Result<Bytes> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        Ok(Bytes::from(
            (offset..offset + len)
                .map(|i| (i % 251) as u8)
                .collect::<Vec<_>>(),
        ))
    }
    async fn head(&self, _: &ObjectId) -> talon_core::Result<ObjectStat> {
        Ok(ObjectStat {
            len: 1024,
            version: Version::new("v1"),
        })
    }
}
fn object() -> ObjectId {
    ObjectId::new(Backend::S3, "bucket", "object")
}
fn id() -> BlockId {
    BlockId::new(object(), 0, 256, Version::new("v1"))
}
fn request(offset: u64, len: u64) -> RangeRequest {
    RangeRequest {
        object: object(),
        offset,
        len,
    }
}
fn runtime(root: &Path, l1: bool, capacity: u64, ttl: u64) -> WorkerRuntime {
    let paged = PagedBlockStore::open(root.join("paged"), 16).unwrap();
    let index = Arc::new(BlockIndex::new());
    for meta in paged.scan().unwrap() {
        index.commit(meta);
    }
    WorkerRuntime::new_with_l1(
        WholeBlockStore::open(root.join("whole")).unwrap(),
        index,
        Arc::new(InFlightLoads::new()),
        Arc::new(Origin::default()),
        256,
        capacity,
        if l1 { 256 } else { 0 },
        16,
        WorkerMetrics::new(capacity),
    )
    .with_paged_store(paged)
    .with_page_gc(PageGcConfig {
        ttl_ms: ttl,
        checkpoint_interval_ms: 10,
        ..Default::default()
    })
    .unwrap()
}
async fn collect_all(r: &WorkerRuntime) -> u64 {
    let mut bytes = 0;
    for _ in 0..100 {
        let report = r.gc_once().await;
        bytes += report.bytes;
        if report.completed_scan {
            return bytes;
        }
    }
    panic!("scan failed to finish");
}

#[tokio::test]
async fn ttl_strict_boundary_and_l1_l2_access_refresh_only_touched_page() {
    for l1 in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let r = runtime(root.path(), l1, 0, 100);
        r.page_clock.set(10000);
        r.serve_range(&request(0, 32)).await.unwrap();
        r.page_clock.set(10100);
        assert_eq!(collect_all(&r).await, 0);
        r.serve_range(&request(16, 1)).await.unwrap();
        r.page_clock.set(10101);
        assert_eq!(collect_all(&r).await, 16);
        assert!(!r.paged.as_ref().unwrap().has_page(&id(), PageIndex(0)));
        assert!(r.paged.as_ref().unwrap().has_page(&id(), PageIndex(1)));
        assert_eq!(r.lru.total_bytes(), 16);
        assert_eq!(r.resident_bytes(), 16);
        assert!(!r.l1.contains_page(&id(), PageIndex(0)));
    }
}

#[tokio::test]
async fn sendfile_refresh_and_open_handle_survives_gc() {
    use std::os::unix::fs::FileExt;
    let root = tempfile::tempdir().unwrap();
    let r = runtime(root.path(), false, 0, 100);
    r.page_clock.set(10000);
    r.serve_range(&request(0, 32)).await.unwrap();
    r.page_clock.set(10099);
    let ServeOutcome::SendfileMany(handles) = r.serve(&request(0, 32)).await.unwrap() else {
        panic!("expected paged sendfile");
    };
    r.page_clock.set(10101);
    assert_eq!(collect_all(&r).await, 0);
    r.page_clock.set(10200);
    assert_eq!(collect_all(&r).await, 32);
    for (i, handle) in handles.iter().enumerate() {
        let f = std::fs::File::from(handle.fd.as_ref().try_clone().unwrap());
        let mut bytes = [0u8; 16];
        f.read_exact_at(&mut bytes, handle.offset).unwrap();
        assert_eq!(
            bytes.as_slice(),
            &(i as u8 * 16..i as u8 * 16 + 16).collect::<Vec<_>>()
        );
    }
    assert_eq!(r.resident_bytes(), 0);
}

#[tokio::test]
async fn restart_and_failback_never_renew_missing_or_old_access() {
    let root = tempfile::tempdir().unwrap();
    let r = runtime(root.path(), false, 0, 100);
    r.page_clock.set(10000);
    r.serve_range(&request(0, 32)).await.unwrap();
    assert_eq!(r.checkpoint_access_times().await.blocks, 1);
    // This access is intentionally lost on restart.
    r.page_clock.set(20000);
    r.serve_range(&request(0, 1)).await.unwrap();
    drop(r);
    for _ in 0..3 {
        let r = runtime(root.path(), false, 0, 100);
        let state = r.page_lifecycle.get(&id()).unwrap();
        assert_eq!(
            state.inner.lock().unwrap().pages[&0].last_access,
            Some(10000)
        );
    }
    let r = runtime(root.path(), false, 0, 100);
    assert_eq!(collect_all(&r).await, 32);
    assert_eq!(
        r.serve_range(&request(0, 1)).await.unwrap(),
        Bytes::from_static(&[0])
    );
    // A second worker on a different disk starts cold and can fetch independently.
    let other = tempfile::tempdir().unwrap();
    let failover = runtime(other.path(), false, 0, 100);
    assert_eq!(failover.resident_bytes(), 0);
    failover.serve_range(&request(0, 1)).await.unwrap();
    assert_eq!(failover.resident_bytes(), 16);
}

#[tokio::test]
async fn no_checkpoint_corrupt_checkpoint_and_disabled_ttl() {
    let root = tempfile::tempdir().unwrap();
    let r = runtime(root.path(), false, 0, 0);
    r.serve_range(&request(0, 16)).await.unwrap();
    assert_eq!(r.checkpoint_access_times().await.blocks, 0);
    assert_eq!(r.gc_once().await.reclaimed, 0);
    drop(r);
    let r = runtime(root.path(), false, 0, 100);
    assert_eq!(collect_all(&r).await, 16);
    r.serve_range(&request(0, 16)).await.unwrap();
    r.checkpoint_access_times().await;
    let dir = r.paged.as_ref().unwrap().dir_for(&id());
    drop(r);
    std::fs::write(dir.join("access.meta"), b"torn").unwrap();
    let r = runtime(root.path(), false, 0, 100);
    assert_eq!(collect_all(&r).await, 16);
}

#[tokio::test]
async fn unknown_page_can_be_accessed_before_claim_and_cache_only_never_fetches() {
    let root = tempfile::tempdir().unwrap();
    let r = runtime(root.path(), false, 0, 0);
    r.serve_range(&request(0, 16)).await.unwrap();
    drop(r);
    let r = runtime(root.path(), false, 0, 100);
    let cached = CachedRangeRequest {
        object: object(),
        version: Version::new("v1"),
        offset: 0,
        len: 1,
    };
    r.serve_cached(&cached).await.unwrap();
    assert_eq!(collect_all(&r).await, 0);
    let now = r.page_clock.now();
    r.page_clock.set(now + 101);
    collect_all(&r).await;
    assert!(r
        .serve_cached(&cached)
        .await
        .unwrap_err()
        .downcast_ref::<CacheMiss>()
        .is_some());
}

#[tokio::test]
async fn unlink_failure_keeps_accounting_and_later_touch_cancels_retry() {
    let root = tempfile::tempdir().unwrap();
    let r = runtime(root.path(), true, 0, 100);
    r.page_clock.set(10000);
    r.serve_range(&request(0, 16)).await.unwrap();
    let path = r.paged.as_ref().unwrap().dir_for(&id()).join("0.page");
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    r.page_clock.set(10101);
    assert_eq!(collect_all(&r).await, 0);
    assert_eq!(r.resident_bytes(), 16);
    assert_eq!(r.lru.total_bytes(), 16);
    r.serve_range(&request(0, 1)).await.unwrap(); // L1 renews after failed unlink.
    assert_eq!(collect_all(&r).await, 0);
    std::fs::remove_dir(&path).unwrap();
    std::fs::write(&path, [0u8; 16]).unwrap();
    r.page_clock.set(10202);
    assert_eq!(collect_all(&r).await, 16);
}

#[tokio::test]
async fn checkpoint_failure_retries_and_does_not_recreate_empty_directory() {
    let root = tempfile::tempdir().unwrap();
    let r = runtime(root.path(), false, 0, 100);
    r.page_clock.set(10000);
    r.serve_range(&request(0, 16)).await.unwrap();
    let dir = r.paged.as_ref().unwrap().dir_for(&id());
    std::fs::create_dir(dir.join("access.meta")).unwrap();
    assert_eq!(r.checkpoint_access_times().await.failures, 1);
    assert!(r
        .page_lifecycle
        .get(&id())
        .unwrap()
        .inner
        .lock()
        .unwrap()
        .dirty_since
        .is_some());
    std::fs::remove_dir(dir.join("access.meta")).unwrap();
    assert_eq!(r.checkpoint_access_times().await.blocks, 1);
    r.page_clock.set(10101);
    collect_all(&r).await;
    assert_eq!(r.checkpoint_access_times().await.blocks, 0);
    assert!(!dir.exists());
}

#[tokio::test]
async fn read_pin_blocks_ttl_and_capacity_until_resource_is_obtained() {
    let root = tempfile::tempdir().unwrap();
    let mut r = runtime(root.path(), false, 0, 100);
    r.page_clock.set(10000);
    r.serve_range(&request(0, 16)).await.unwrap();
    let state = r.page_lifecycle.get(&id()).unwrap();
    let read = state.acquire(PageIndex(0)).unwrap();
    r.page_clock.set(10101);
    assert_eq!(collect_all(&r).await, 0);
    r.capacity_bytes = 1;
    r.enforce_capacity().await;
    assert_eq!(r.resident_bytes(), 16);
    drop(read);
    r.enforce_capacity().await;
    assert_eq!(r.resident_bytes(), 0);
    assert_eq!(collect_all(&r).await, 0);
    assert_eq!(r.lru.total_bytes(), 0);
}

#[tokio::test]
async fn bounded_deletion_and_concurrent_sibling_commit() {
    let root = tempfile::tempdir().unwrap();
    let mut r = runtime(root.path(), false, 0, 100);
    r.page_gc_config.delete_batch_size = 1;
    r.page_clock.set(10000);
    r.serve_range(&request(0, 48)).await.unwrap();
    r.page_clock.set(10101);
    assert!(r.gc_once().await.reclaimed <= 1);
    let request = request(48, 16);
    let ((), data) = tokio::join!(
        async {
            for _ in 0..4 {
                r.gc_once().await;
            }
        },
        r.serve_range(&request)
    );
    assert_eq!(data.unwrap().len(), 16);
    assert!(r.paged.as_ref().unwrap().has_page(&id(), PageIndex(3)));
    assert_eq!(r.resident_bytes(), r.lru.total_bytes());
    assert_eq!(r.resident_bytes(), 16);
}

#[tokio::test]
async fn cancelling_request_cannot_split_commit_transaction() {
    let root = tempfile::tempdir().unwrap();
    let r = runtime(root.path(), false, 0, 100);
    let state = r.page_lifecycle.block(&id());
    let gate = state.gate.lock().await;
    let task = {
        let r = r.clone();
        tokio::spawn(async move {
            r.commit_fetched_page(&id(), PageIndex(0), 256, Bytes::from_static(&[7; 16]))
                .await
        })
    };
    for _ in 0..100 {
        if r.page_mutations.active_count() > 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(r.page_mutations.active_count() > 0);
    task.abort();
    let _ = task.await;
    drop(gate);
    r.drain_page_mutations().await;
    assert!(r.paged.as_ref().unwrap().has_page(&id(), PageIndex(0)));
    assert_eq!(r.resident_bytes(), 16);
    assert_eq!(r.lru.total_bytes(), 16);
}

#[tokio::test]
async fn full_admission_over_partial_pages_preserves_form_and_covers_body() {
    let root = tempfile::tempdir().unwrap();
    let r = runtime(root.path(), false, 0, 100);
    r.serve_range(&request(0, 16)).await.unwrap();
    let body = Bytes::from((0..256).map(|i| (i % 251) as u8).collect::<Vec<_>>());
    r.commit_cached_block(&id(), body.clone()).await.unwrap();
    let cached = CachedRangeRequest {
        object: object(),
        version: Version::new("v1"),
        offset: 0,
        len: 256,
    };
    assert_eq!(r.serve_cached(&cached).await.unwrap(), body);
    assert_eq!(r.resident_bytes(), 256);
    assert_eq!(r.lru.total_bytes(), 256);
}

#[tokio::test]
async fn background_service_collects_without_requests_and_shutdown_checkpoints() {
    let root = tempfile::tempdir().unwrap();
    let mut r = runtime(root.path(), false, 0, 100);
    r.page_gc_config.interval_ms = 1;
    r.page_clock.set(10000);
    r.serve_range(&request(0, 16)).await.unwrap();
    let r = Arc::new(r);
    r.page_clock.set(10101);
    let service = crate::page_gc::PageGcService::start(r.clone(), r.page_gc_config.clone());
    tokio::time::timeout(Duration::from_secs(2), async {
        while r.resident_bytes() != 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    r.serve_range(&request(16, 16)).await.unwrap();
    service.shutdown().await;
    assert!(r
        .paged
        .as_ref()
        .unwrap()
        .dir_for(&id())
        .join("access.meta")
        .exists());
    assert_eq!(r.page_mutations.active_count(), 0);
}

/// Reproducible metadata-scale probe. This deliberately does not model origin I/O
/// or claim end-to-end read throughput. Run with --ignored --nocapture.
#[test]
#[ignore = "manual 100k/1m page metadata and checkpoint scale probe"]
fn page_ttl_metadata_scale() {
    use crate::page_access_store::{AccessSnapshot, PageAccessStore};
    use std::hint::black_box;
    fn rss_kib() -> usize {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|line| line.starts_with("VmRSS:"))
                    .and_then(|line| line.split_whitespace().nth(1))
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(0)
    }
    let sizes = std::env::var("TALON_TTL_BENCH_PAGES")
        .ok()
        .map(|s| vec![s.parse::<usize>().expect("page count")])
        .unwrap_or_else(|| vec![100_000, 1_000_000]);
    for count in sizes {
        assert!(count > 0);
        let root = tempfile::tempdir().unwrap();
        let before_rss = rss_kib();
        let life = PageLifecycle::new();
        let mut blocks = Vec::new();
        let started = Instant::now();
        for b in 0..count.div_ceil(1024) {
            let id = BlockId::new(object(), b as u64 * 16384, 16384, Version::new("v1"));
            let state = life.block(&id);
            for p in 0..1024.min(count - b * 1024) {
                state.register(PageIndex(p as u32), Some(1000), 1000, true);
            }
            blocks.push(state);
        }
        let build_ms = started.elapsed().as_secs_f64() * 1000.0;
        println!(
            "{{\"pages\":{count},\"rss_before_kib\":{before_rss},\"rss_after_build_kib\":{}}}",
            rss_kib()
        );
        for (scenario, hot_modulus) in [
            ("cold", usize::MAX),
            ("hot", 1),
            ("mixed", 2),
            ("expired", usize::MAX),
        ] {
            for block in &blocks {
                let mut state = block.inner.lock().unwrap();
                for (&p, e) in state.pages.iter_mut() {
                    e.last_access = Some(
                        if hot_modulus != usize::MAX && p as usize % hot_modulus == 0 {
                            1999
                        } else {
                            1000
                        },
                    );
                }
            }
            let now = if scenario == "cold" { 1050 } else { 2000 };
            let mut samples = Vec::with_capacity(10000);
            let reads = Instant::now();
            for i in 0..10000 {
                let block = &blocks[i % blocks.len()];
                let started = Instant::now();
                let read = block.acquire(PageIndex(0)).unwrap();
                read.record_access(now - 1);
                drop(read);
                samples.push(started.elapsed().as_nanos() as u64);
            }
            let metadata_ops = 10000.0 / reads.elapsed().as_secs_f64();
            samples.sort_unstable();
            let started = Instant::now();
            let mut cursor = ScanCursor::default();
            let mut expired = 0;
            let mut checked = 0;
            loop {
                let (c, r) = life.scan(&mut cursor, 65536, now, Some(100), usize::MAX);
                expired += c.len();
                checked += r.checked;
                black_box(c);
                if r.completed {
                    break;
                }
            }
            let scan_ms = started.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(checked, count);
            println!("{{\"pages\":{count},\"scenario\":\"{scenario}\",\"build_ms\":{build_ms:.3},\"metadata_ops_s\":{metadata_ops:.0},\"metadata_p99_ns\":{},\"scan_ms\":{scan_ms:.3},\"expired_candidates\":{expired},\"entry_bytes\":{}}}", samples[9900], std::mem::size_of::<crate::page_lifecycle::PageEntry>());
        }
        let started = Instant::now();
        let mut bytes = 0;
        for (i, block) in blocks.iter().enumerate() {
            let dir = root.path().join(i.to_string());
            std::fs::create_dir(&dir).unwrap();
            let state = block.inner.lock().unwrap();
            let snapshot = AccessSnapshot {
                revision: state.revision,
                sampled_at: 2000,
                records: state
                    .pages
                    .iter()
                    .map(|(&p, e)| (p, e.last_access.unwrap()))
                    .collect(),
            };
            drop(state);
            bytes += PageAccessStore::checkpoint(&dir, &block.id, 16, &snapshot).unwrap();
        }
        println!(
            "{{\"pages\":{count},\"checkpoint_bytes\":{bytes},\"checkpoint_ms\":{:.3}}}",
            started.elapsed().as_secs_f64() * 1000.0
        );
    }
}

#[tokio::test]
async fn stale_gc_candidate_cannot_unlink_a_recommitted_page() {
    let root = tempfile::tempdir().unwrap();
    let r = runtime(root.path(), false, 0, 100);
    r.page_clock.set(10000);
    r.serve_range(&request(0, 16)).await.unwrap();
    let state = r.page_lifecycle.get(&id()).unwrap();
    let stale = state.candidate(PageIndex(0)).unwrap();
    r.page_clock.set(10101);
    r.commit_fetched_page(
        &id(),
        PageIndex(0),
        256,
        Bytes::from((0..16).collect::<Vec<u8>>()),
    )
    .await
    .unwrap();
    r.page_clock.set(20000); // New instance is old enough too; token identity still matters.
    assert_eq!(r.evict_page_candidate(stale, 0).await, None);
    assert!(r.paged.as_ref().unwrap().has_page(&id(), PageIndex(0)));
    assert_eq!(r.resident_bytes(), 16);
}

#[tokio::test]
async fn failed_first_candidate_must_not_starve_later_pages() {
    let root = tempfile::tempdir().unwrap();
    let mut r = runtime(root.path(), false, 0, 100);
    r.page_gc_config.delete_batch_size = 1;
    r.page_clock.set(10000);
    r.serve_range(&request(0, 32)).await.unwrap();
    let dir = r.paged.as_ref().unwrap().dir_for(&id());
    std::fs::remove_file(dir.join("0.page")).unwrap();
    std::fs::create_dir(dir.join("0.page")).unwrap();
    r.page_clock.set(10101);
    for _ in 0..10 {
        r.gc_once().await;
    }
    assert!(
        !dir.join("1.page").exists(),
        "healthy expired page is starved by a failing first candidate"
    );
}

#[tokio::test]
async fn capacity_should_skip_protected_page_and_choose_another() {
    let root = tempfile::tempdir().unwrap();
    let mut r = runtime(root.path(), false, 0, 0);
    r.serve_range(&request(0, 48)).await.unwrap();
    let state = r.page_lifecycle.get(&id()).unwrap();
    let _read = state.acquire(PageIndex(0)).unwrap();
    r.capacity_bytes = 32;
    r.enforce_capacity().await;
    assert!(
        r.resident_bytes() <= 32,
        "capacity enforcement stopped at an ineligible oldest page despite other victims"
    );
}

#[tokio::test]
async fn cancelled_admission_must_finish_capacity_enforcement() {
    let root = tempfile::tempdir().unwrap();
    let r = runtime(root.path(), false, 16, 0);
    r.serve_range(&request(0, 16)).await.unwrap();
    let state = r.page_lifecycle.block(&id());
    let gate = state.gate.lock().await;
    let task = {
        let r = r.clone();
        tokio::spawn(async move {
            r.commit_fetched_page(&id(), PageIndex(1), 256, Bytes::from_static(&[7; 16]))
                .await
        })
    };
    while r.page_mutations.active_count() == 0 {
        tokio::task::yield_now().await;
    }
    task.abort();
    let _ = task.await;
    drop(gate);
    r.drain_page_mutations().await;
    assert!(
        r.resident_bytes() <= 16,
        "detached admission exceeds capacity after requester cancellation"
    );
}

#[tokio::test]
async fn stale_whole_capacity_candidate_must_recheck_pressure() {
    let root = tempfile::tempdir().unwrap();
    let mut r = runtime(root.path(), false, 0, 0);
    let a = id();
    let mut b = a.clone();
    b.offset = 256;
    r.commit_cached_block(&a, Bytes::from_static(&[1; 16]))
        .await
        .unwrap();
    r.commit_cached_block(&b, Bytes::from_static(&[2; 16]))
        .await
        .unwrap();
    r.capacity_bytes = 16;
    let pending = r.lru.candidates_to_fit(16, &HashSet::new());
    assert_eq!(pending.len(), 1);
    // Another request has already relieved pressure by evicting B.
    r.unlink_units(r.lru.block_candidates(&b), 1).await;
    assert_eq!(r.resident_bytes(), 16);
    let _pin = r.lru.pin_guard(CacheUnit::Whole(a.clone())).unwrap();
    r.unlink_units(pending, 1).await;
    assert_eq!(
        r.resident_bytes(),
        16,
        "stale candidate deletes pinned A after capacity is already satisfied"
    );
}

#[tokio::test]
async fn whole_candidates_recheck_pins_and_recommits_under_pressure() {
    let root = tempfile::tempdir().unwrap();
    let mut r = runtime(root.path(), false, 0, 0);
    let a = id();
    let mut b = a.clone();
    b.offset = 256;
    r.commit_cached_block(&a, Bytes::from_static(&[1; 16]))
        .await
        .unwrap();
    r.commit_cached_block(&b, Bytes::from_static(&[2; 16]))
        .await
        .unwrap();
    r.capacity_bytes = 16;
    let stale = r.lru.candidates_to_fit(16, &HashSet::new());
    let pin = r.lru.pin_guard(CacheUnit::Whole(a.clone())).unwrap();
    r.unlink_units(stale.clone(), 1).await;
    assert_eq!(r.resident_bytes(), 32);
    drop(pin);
    // Keep pressure handling out of this recommit so the stale candidate is
    // tested while the cache is still over capacity.
    r.capacity_bytes = 0;
    r.commit_cached_block(&a, Bytes::from_static(&[1; 16]))
        .await
        .unwrap();
    r.capacity_bytes = 16;
    r.unlink_units(stale, 1).await;
    assert_eq!(r.resident_bytes(), 32);
    assert!(r.cached_block_range(&a, 0, 16).await.unwrap().is_some());
}

#[tokio::test]
async fn cancelled_whole_admission_finishes_capacity_and_paged_fallback() {
    for paged in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let r = runtime(root.path(), false, 16, 0);
        let mut target = id();
        let body;
        if paged {
            r.serve_range(&request(0, 16)).await.unwrap();
            body = Bytes::from((0..32).collect::<Vec<u8>>());
        } else {
            r.commit_cached_block(&id(), Bytes::from_static(&[1; 16]))
                .await
                .unwrap();
            target.offset = 256;
            body = Bytes::from_static(&[2; 16]);
        }
        let state = r.page_lifecycle.block(&target);
        let gate = state.gate.lock().await;
        let task = {
            let r = r.clone();
            let target = target.clone();
            tokio::spawn(async move { r.commit_cached_block(&target, body).await })
        };
        while r.page_mutations.active_count() == 0 {
            tokio::task::yield_now().await;
        }
        task.abort();
        let _ = task.await;
        drop(gate);
        r.drain_page_mutations().await;
        assert_eq!(r.resident_bytes(), 16);
        assert_eq!(r.lru.total_bytes(), 16);
        if paged {
            assert!(r.paged.as_ref().unwrap().has_page(&target, PageIndex(1)));
        } else {
            assert!(r
                .cached_block_range(&target, 0, 16)
                .await
                .unwrap()
                .is_some());
        }
    }
}

#[tokio::test]
async fn capacity_retries_other_victims_after_unlink_failure() {
    let root = tempfile::tempdir().unwrap();
    let mut r = runtime(root.path(), false, 0, 0);
    r.serve_range(&request(0, 48)).await.unwrap();
    let path = r.paged.as_ref().unwrap().dir_for(&id()).join("0.page");
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    r.capacity_bytes = 32;
    r.enforce_capacity().await;
    assert_eq!(r.resident_bytes(), 32);
    assert_eq!(r.lru.total_bytes(), 32);
    // If no candidate can be unlinked, the pass must still terminate.
    r.capacity_bytes = 1;
    tokio::time::timeout(Duration::from_secs(2), r.enforce_capacity())
        .await
        .unwrap();
    assert_eq!(r.resident_bytes(), 16);
}

#[tokio::test]
async fn orphan_cleanup_retries_with_ttl_disabled_and_preserves_live_snapshots() {
    let root = tempfile::tempdir().unwrap();
    let mut r = runtime(root.path(), false, 0, 0);
    r.serve_range(&request(0, 16)).await.unwrap();
    let dir = r.paged.as_ref().unwrap().dir_for(&id());
    std::fs::write(dir.join("access.meta"), b"existing checkpoint").unwrap();
    std::fs::write(dir.join("access.meta.tmp.crashed"), b"partial").unwrap();
    r.page_cleanup.lock().unwrap().fail_deletes = true;
    r.recover_page_file_cleanup().await;
    assert!(dir.join("access.meta.tmp.crashed").exists());
    assert!(r.page_gc_metrics.cleanup_errors.get() > 0);
    assert_eq!(r.page_gc_metrics.cleanup_pending.get(), 1.0);
    r.page_cleanup.lock().unwrap().fail_deletes = false;
    r.page_gc_config.interval_ms = 1;
    let r = Arc::new(r);
    let service = crate::page_gc::PageGcService::start(r.clone(), r.page_gc_config.clone());
    tokio::time::timeout(Duration::from_secs(2), async {
        while dir.join("access.meta.tmp.crashed").exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    service.shutdown().await;
    r.recover_page_file_cleanup().await;
    assert_eq!(r.page_gc_metrics.cleanup_pending.get(), 0.0);
    assert!(r.page_gc_metrics.cleanup_removed.get() > 0);
    assert!(r.page_gc_metrics.cleanup_scan_at.get() > 0.0);
    assert!(dir.join("0.page").exists());
    assert_eq!(
        std::fs::read(dir.join("access.meta")).unwrap(),
        b"existing checkpoint"
    );
    assert_eq!(r.resident_bytes(), 16);
}

#[tokio::test]
async fn orphan_cleanup_covers_unindexed_directories_with_paged_reads_disabled() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("paged/00/0000000000000001.pages");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("access.meta"), b"leftover").unwrap();
    let r = WorkerRuntime::new(
        WholeBlockStore::open(root.path()).unwrap(),
        Arc::new(BlockIndex::new()),
        Arc::new(InFlightLoads::new()),
        Arc::new(Origin::default()),
        256,
        0,
        WorkerMetrics::new(0),
    );
    assert!(r.paged.is_none());
    r.recover_page_file_cleanup().await;
    assert!(!dir.exists());
    // Cleanup after a crash between unlinking the last page and rmdir.
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("block.meta"), b"unreadable identity").unwrap();
    let r = Arc::new(r);
    let service = crate::page_gc::PageGcService::start(
        r.clone(),
        PageGcConfig {
            interval_ms: 1,
            ..Default::default()
        },
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while dir.exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    service.shutdown().await;
}

#[tokio::test]
async fn orphan_cleanup_waits_for_active_checkpoint_and_commit_gate() {
    let root = tempfile::tempdir().unwrap();
    let r = runtime(root.path(), false, 0, 0);
    r.serve_range(&request(0, 16)).await.unwrap();
    let dir = r.paged.as_ref().unwrap().dir_for(&id());
    let state = r.page_lifecycle.block(&id());
    let gate = state.gate.lock().await;
    let tmp = dir.join("access.meta.tmp.inflight");
    std::fs::write(&tmp, b"new checkpoint").unwrap();
    let cleanup = {
        let r = r.clone();
        tokio::spawn(async move { r.recover_page_file_cleanup().await })
    };
    while r.page_mutations.active_count() == 0 {
        tokio::task::yield_now().await;
    }
    assert!(tokio::time::timeout(Duration::from_millis(20), async {
        while !cleanup.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_err());
    assert!(tmp.exists());
    std::fs::rename(&tmp, dir.join("access.meta")).unwrap();
    drop(gate);
    tokio::time::timeout(Duration::from_secs(2), cleanup)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        std::fs::read(dir.join("access.meta")).unwrap(),
        b"new checkpoint"
    );
    assert!(dir.join("0.page").exists());
}

#[test]
fn orphan_cleanup_recovers_sigkill_at_checkpoint_boundaries() {
    const ROOT: &str = "TALON_CHECKPOINT_CRASH_ROOT";
    if let Some(root) = std::env::var_os(ROOT) {
        let root = std::path::PathBuf::from(root);
        let _lease = crate::page_access_store::CacheRootLock::acquire(&root).unwrap();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let r = runtime(&root, false, 0, 100);
            r.serve_range(&request(0, 16)).await.unwrap();
            // Parent kills at the injected disk mutation boundary.
            if std::env::var("TALON_CHECKPOINT_CRASH_STAGE").as_deref() == Ok("last_page_unlink") {
                r.page_clock.set(r.page_clock.now() + 101);
                r.gc_once().await;
            } else {
                r.checkpoint_access_times().await;
            }
            panic!("checkpoint did not stop at the crash boundary");
        });
        return;
    }
    for stage in ["file_sync", "rename", "directory_sync", "last_page_unlink"] {
        let root = tempfile::tempdir().unwrap();
        let ready = root.path().join("crash-ready");
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let _lease = crate::page_access_store::CacheRootLock::acquire(root.path()).unwrap();
            let r = runtime(root.path(), false, 0, 100);
            r.serve_range(&request(0, 16)).await.unwrap();
            assert_eq!(r.checkpoint_access_times().await.blocks, 1);
        });
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime::page_ttl_tests::orphan_cleanup_recovers_sigkill_at_checkpoint_boundaries",
                "--nocapture",
            ])
            .env(ROOT, root.path())
            .env("TALON_CHECKPOINT_CRASH_STAGE", stage)
            .env("TALON_CHECKPOINT_CRASH_READY", &ready)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready.exists() && Instant::now() < deadline {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let reached = ready.exists();
        let _ = child.kill();
        child.wait().unwrap();
        assert!(reached, "child did not reach {stage}");
        rt.block_on(async {
            // SIGKILL also releases the exclusive root lease.
            let _lease = crate::page_access_store::CacheRootLock::acquire(root.path()).unwrap();
            for _ in 0..2 {
                let r = runtime(root.path(), false, 0, 100);
                r.recover_page_file_cleanup().await;
                let paged = r.paged.as_ref().unwrap();
                let dir = paged.dir_for(&id());
                if stage == "last_page_unlink" {
                    assert!(!dir.exists());
                    assert_eq!(r.resident_bytes(), 0);
                    continue;
                }
                assert!(dir.join("0.page").exists());
                assert!(std::fs::read_dir(&dir)
                    .unwrap()
                    .all(|e| { !e.unwrap().file_name().to_string_lossy().contains(".tmp.") }));
                let recovery = crate::page_access_store::PageAccessStore::load(
                    &dir,
                    &id(),
                    16,
                    r.page_clock.now(),
                );
                assert!(!recovery.corrupt);
                assert_eq!(recovery.records.len(), 1);
                assert_eq!(
                    r.serve_range(&request(0, 16)).await.unwrap(),
                    Bytes::from((0..16).collect::<Vec<u8>>())
                );
            }
        });
    }
}
