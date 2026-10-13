//! Reclaim cold local cache entries only after an authoritative backend NotFound.
use super::WorkerRuntime;
use crate::eviction::access_millis;
use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};
use talon_core::Error;

const DAY_MS: u64 = 24 * 60 * 60 * 1000;
const MAX_HEADS: usize = 32;

#[derive(Default)]
pub(super) struct StaleGc {
    cursor: (u64, u64),
    round_time: Option<u64>,
    pending: VecDeque<crate::eviction::StaleBlockCandidate>,
}

impl StaleGc {
    /// Preserve the round's age cutoff and admission ceiling across scheduler batches.
    async fn run_batch(&mut self, worker: &WorkerRuntime) -> Duration {
        let now = *self.round_time.get_or_insert_with(access_millis);
        self.tick_at(worker, now).await;
        if self.cursor.0 >= self.cursor.1 && self.pending.is_empty() {
            self.round_time = None;
            Duration::from_secs(3600)
        } else {
            Duration::from_secs(1)
        }
    }

    #[cfg(test)]
    async fn run_round_at(&mut self, worker: &WorkerRuntime, now: u64) {
        loop {
            self.tick_at(worker, now).await;
            if self.cursor.0 >= self.cursor.1 && self.pending.is_empty() {
                break;
            }
            // Pace batches within this round without repeatedly scanning the cache
            // between hourly rounds. Newly admitted units wait for the next round.
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn tick_at(&mut self, worker: &WorkerRuntime, now: u64) {
        if self.pending.is_empty() {
            let lru = worker.lru.clone();
            let mut cursor = self.cursor;
            let scan = worker.page_gc_config.scan_batch_size.min(4096);
            let deletes = worker.page_gc_config.delete_batch_size;
            let (cursor, candidates) = tokio::task::spawn_blocking(move || {
                let candidates = lru.stale_gc_candidates(&mut cursor, now, DAY_MS, scan, deletes);
                (cursor, candidates)
            })
            .await
            .expect("stale GC scan panicked");
            self.cursor = cursor;
            self.pending = candidates.into();
        }
        // Bound backend work separately from the shared *local disk* budget.
        // Neither transient errors nor NotFound are cached across batches.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut checked = HashMap::new();
        while let Some(candidate) = self.pending.front() {
            let object = &candidate.block.object;
            if !checked.contains_key(object)
                && (checked.len() >= MAX_HEADS || tokio::time::Instant::now() >= deadline)
            {
                break;
            }
            let candidate = self.pending.pop_front().expect("pending candidate");
            if !worker.lru.stale_candidate_is_current(&candidate) {
                continue;
            }
            let object = &candidate.block.object;
            if !worker.lru.stale_check_due(object, now, DAY_MS) {
                continue;
            }
            let missing = if let Some(missing) = checked.get(object) {
                *missing
            } else {
                let missing =
                    match tokio::time::timeout_at(deadline, worker.backend.head(object)).await {
                        Ok(Ok(_)) => {
                            worker.lru.stale_verified(object, now.max(access_millis()));
                            false
                        }
                        Ok(Err(Error::NotFound(_))) => true,
                        Ok(Err(error)) => {
                            tracing::debug!(%error, "stale GC HEAD failed; retaining cache");
                            false
                        }
                        Err(_) => break,
                    };
                checked.insert(object.clone(), missing);
                missing
            };
            if missing {
                // Admission revisions, block activity and pins are rechecked under
                // the mutation gate; I/O failures retain resident accounting.
                worker.unlink_stale_block(candidate).await;
            }
        }
    }
}

/// One scheduler registration for independent backend and ownership policies.
/// A waiting policy releases the shared task slot; each keeps its own round clock.
pub(super) struct StaleGcService {
    backend: Option<StaleGc>,
    backend_next: tokio::time::Instant,
    ownership: Option<super::ownership_gc::OwnershipService>,
    ownership_next: tokio::time::Instant,
}
impl StaleGcService {
    pub(super) fn new(
        backend_enabled: bool,
        ownership: Option<super::ownership_gc::OwnershipService>,
    ) -> Self {
        Self {
            backend: backend_enabled.then(StaleGc::default),
            backend_next: tokio::time::Instant::now(),
            ownership,
            ownership_next: tokio::time::Instant::now(),
        }
    }

    pub(super) async fn run_batch(&mut self, worker: &WorkerRuntime) -> Duration {
        let now = tokio::time::Instant::now();
        // Admit one policy per invocation. Oldest-due first prevents a long
        // backend scan from postponing every ownership batch until round end.
        let ownership_due = self.ownership.is_some() && self.ownership_next <= now;
        let backend_due = self.backend.is_some() && self.backend_next <= now;
        if ownership_due && (!backend_due || self.ownership_next <= self.backend_next) {
            let delay = self
                .ownership
                .as_mut()
                .expect("enabled ownership policy")
                .run_batch(worker)
                .await;
            self.ownership_next = tokio::time::Instant::now() + delay;
        } else if backend_due {
            let delay = self
                .backend
                .as_mut()
                .expect("enabled backend policy")
                .run_batch(worker)
                .await;
            self.backend_next = tokio::time::Instant::now() + delay;
        }
        let next = self
            .backend
            .as_ref()
            .map(|_| self.backend_next)
            .into_iter()
            .chain(self.ownership.as_ref().map(|_| self.ownership_next))
            .min();
        next.map_or(Duration::from_secs(3600), |at| {
            at.saturating_duration_since(tokio::time::Instant::now())
                .max(Duration::from_millis(1))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BlockIndex, CacheUnit, InFlightLoads, PagedBlockStore, WholeBlockStore, WorkerMetrics,
    };
    use bytes::Bytes;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };
    use talon_core::{Backend, BackendStore, BlockId, ObjectId, ObjectStat, PageIndex, Version};

    #[derive(Default)]
    struct Origin {
        calls: AtomicUsize,
        mode: AtomicUsize, // 0 missing, 1 exists, 2 forbidden, 3 timeout
        hold: AtomicBool,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }
    #[async_trait::async_trait]
    impl BackendStore for Origin {
        async fn fetch_range(&self, _: &ObjectId, _: u64, _: u64) -> talon_core::Result<Bytes> {
            panic!("GC must not fetch data")
        }
        async fn head(&self, object: &ObjectId) -> talon_core::Result<ObjectStat> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.entered.notify_one();
            if self.hold.load(Ordering::Relaxed) {
                self.release.notified().await;
            }
            match self.mode.load(Ordering::Relaxed) {
                0 => Err(Error::NotFound(object.to_path())),
                1 => Ok(ObjectStat {
                    len: 32,
                    version: Version::new("new-version"),
                }),
                2 => Err(Error::Backend("403 Forbidden".into())),
                _ => std::future::pending().await,
            }
        }
    }
    fn block(n: u64) -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "bucket", "key"),
            n * 32,
            32,
            Version::new("v1"),
        )
    }
    fn runtime(root: &std::path::Path, paged: bool, origin: Arc<Origin>) -> WorkerRuntime {
        let store = WholeBlockStore::open(root.join("whole")).unwrap();
        let index = Arc::new(BlockIndex::new());
        for meta in store.scan().unwrap() {
            index.commit(meta);
        }
        let r = WorkerRuntime::new(
            store,
            index,
            Arc::new(InFlightLoads::new()),
            origin,
            32,
            0,
            WorkerMetrics::new(0),
        );
        if paged {
            r.with_paged_store(PagedBlockStore::open(root.join("paged"), 16).unwrap())
        } else {
            r
        }
    }
    async fn fill(r: &WorkerRuntime, id: &BlockId, paged: bool) {
        if paged {
            for page in 0..2 {
                r.commit_fetched_page(id, PageIndex(page), 32, Bytes::from_static(&[1; 16]))
                    .await
                    .unwrap();
            }
        } else {
            r.commit_cached_block(id, Bytes::from_static(&[1; 32]))
                .await
                .unwrap();
        }
    }
    fn cold_now() -> u64 {
        access_millis() + DAY_MS + 1
    }

    #[tokio::test]
    async fn stale_service_interleaves_ownership_with_backend_round_batches() {
        use talon_core::worker_membership::{MemberRegistry, WorkerDiscovery, WorkerMember};
        use talon_core::{NodeId, WorkerConfig};
        use talon_transport::{codec, ControlMessage};
        use tokio::{io::AsyncWriteExt, net::TcpListener};
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.mode.store(2, Ordering::Relaxed);
        let mut r = runtime(root.path(), false, origin.clone());
        r.page_gc_config.scan_batch_size = 1;
        for n in 0..3 {
            fill(&r, &block(n), false).await;
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cfg = WorkerConfig {
            coordinator: listener.local_addr().unwrap().to_string(),
            ownership_gc_interval_secs: 3600,
            ..Default::default()
        };
        let query = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            assert_eq!(
                crate::tokio_conn::read_control(&mut stream).await.unwrap(),
                Some(ControlMessage::MembershipQuery {})
            );
            let view = WorkerDiscovery::retained(
                &MemberRegistry {
                    members: vec![WorkerMember {
                        worker_id: "a".into(),
                        zone: None,
                        retired: false,
                    }],
                    ..Default::default()
                },
                &[],
            );
            stream
                .write_all(&codec::encode(0, &ControlMessage::MembershipList { view }).unwrap())
                .await
                .unwrap();
        });
        let ownership =
            super::super::ownership_gc::OwnershipService::new(NodeId::new("a"), &r, &cfg, None);
        let mut service = StaleGcService::new(true, Some(ownership));
        service.backend.as_mut().unwrap().round_time = Some(cold_now());
        service.backend_next = tokio::time::Instant::now() - Duration::from_secs(1);
        service.run_batch(&r).await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), 1);
        assert!(service.backend.as_ref().unwrap().round_time.is_some());
        // Ownership is due before the next backend batch, even though its round
        // is unfinished. The sole logical member retains all three blocks.
        service.run_batch(&r).await;
        tokio::time::timeout(Duration::from_secs(1), query)
            .await
            .unwrap()
            .unwrap();
        assert!(service.ownership_next > tokio::time::Instant::now() + Duration::from_secs(3599));
        assert_eq!(origin.calls.load(Ordering::Relaxed), 1);
        service.backend_next = tokio::time::Instant::now();
        service.run_batch(&r).await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), 2);
        assert_eq!(r.resident_bytes(), 96);
    }

    #[tokio::test]
    async fn stale_gc_filters_warm_entries_then_removes_both_cache_forms() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            let r = runtime(root.path(), paged, origin.clone());
            fill(&r, &block(0), paged).await;
            fill(&r, &block(1), paged).await;
            let mut gc = StaleGc::default();
            gc.tick_at(&r, access_millis()).await;
            assert_eq!(origin.calls.load(Ordering::Relaxed), 0);
            gc.tick_at(&r, cold_now()).await;
            assert_eq!(
                origin.calls.load(Ordering::Relaxed),
                1,
                "one HEAD per object per batch"
            );
            assert_eq!(r.resident_bytes(), 0);
            assert_eq!(r.lru.total_bytes(), 0);
            assert!(r.index.get(&block(0)).is_none());
            assert!(r.index.get(&block(1)).is_none());
            let recovered = runtime(root.path(), paged, origin);
            assert_eq!(recovered.resident_bytes(), 0);
        }
    }

    #[tokio::test]
    async fn stale_gc_positive_checks_cover_other_offsets_for_24_hours() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            origin.mode.store(1, Ordering::Relaxed);
            let mut r = runtime(root.path(), paged, origin.clone());
            r.page_gc_config.delete_batch_size = 1;
            fill(&r, &block(0), paged).await;
            fill(&r, &block(1), paged).await;
            let mut gc = StaleGc::default();
            let now = cold_now();
            gc.tick_at(&r, now).await;
            origin.mode.store(0, Ordering::Relaxed);
            for _ in 0..8 {
                gc.tick_at(&r, now + DAY_MS - 1).await;
            }
            assert_eq!(origin.calls.load(Ordering::Relaxed), 1);
            assert_eq!(
                r.resident_bytes(),
                64,
                "a version change alone is not deletion"
            );
            for _ in 0..8 {
                gc.tick_at(&r, now + DAY_MS).await;
            }
            assert_eq!(r.resident_bytes(), 0);
        }
    }

    #[tokio::test]
    async fn stale_gc_errors_retain_cache_and_retry() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            origin.mode.store(2, Ordering::Relaxed);
            let r = runtime(root.path(), paged, origin.clone());
            fill(&r, &block(0), paged).await;
            let mut gc = StaleGc::default();
            let now = cold_now();
            gc.tick_at(&r, now).await;
            assert_eq!(r.resident_bytes(), 32);
            origin.mode.store(0, Ordering::Relaxed);
            for _ in 0..2 {
                gc.tick_at(&r, now).await;
            }
            assert_eq!(r.resident_bytes(), 0);
            assert_eq!(origin.calls.load(Ordering::Relaxed), 2);
        }
    }

    #[tokio::test]
    async fn stale_gc_head_timeout_is_bounded_and_non_destructive() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.mode.store(3, Ordering::Relaxed);
        let r = Arc::new(runtime(root.path(), false, origin.clone()));
        fill(&r, &block(0), false).await;
        let task = tokio::spawn({
            let r = r.clone();
            async move {
                StaleGc::default().tick_at(&r, cold_now()).await;
            }
        });
        origin.entered.notified().await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(6)).await;
        task.await.unwrap();
        tokio::time::resume();
        assert_eq!(r.resident_bytes(), 32);
    }

    #[tokio::test]
    async fn stale_gc_read_during_head_invalidates_the_entire_block() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            origin.hold.store(true, Ordering::Relaxed);
            let r = Arc::new(runtime(root.path(), paged, origin.clone()));
            fill(&r, &block(0), paged).await;
            let task = tokio::spawn({
                let r = r.clone();
                async move {
                    StaleGc::default().tick_at(&r, cold_now()).await;
                }
            });
            origin.entered.notified().await;
            tokio::time::sleep(Duration::from_millis(2)).await;
            if paged {
                let guards = r
                    .index
                    .page_reads_and_touch(&block(0), PageIndex(1), PageIndex(2))
                    .unwrap();
                drop(guards);
            } else {
                assert!(r.index.is_whole_and_touch(&block(0)));
            }
            origin.release.notify_one();
            task.await.unwrap();
            assert_eq!(r.resident_bytes(), 32);
        }
    }

    #[tokio::test]
    async fn stale_gc_failed_block_unlink_reopens_all_pages_and_requires_a_fresh_head() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let r = runtime(root.path(), true, origin.clone());
        fill(&r, &block(0), true).await;
        let dir = r.paged.as_ref().unwrap().dir_for(&block(0));
        let saved = dir.with_extension("saved");
        std::fs::rename(&dir, &saved).unwrap();
        // A file where the block directory should be makes remove_dir_all fail
        // deterministically, including when the test runs as root.
        std::fs::write(&dir, b"not a directory").unwrap();
        let mut gc = StaleGc::default();
        gc.tick_at(&r, cold_now()).await;
        assert_eq!(r.resident_bytes(), 32);
        assert_eq!(r.lru.total_bytes(), 32);
        let state = r.page_lifecycle.block(&block(0));
        for page in 0..2 {
            assert!(
                state.acquire(PageIndex(page)).is_some(),
                "failed unlink must reopen every page"
            );
        }
        std::fs::remove_file(&dir).unwrap();
        std::fs::rename(&saved, &dir).unwrap();
        origin.mode.store(1, Ordering::Relaxed);
        r.gc_once().await;
        assert_eq!(
            r.resident_bytes(),
            32,
            "TTI GC cannot retry an old NotFound"
        );
        for _ in 0..2 {
            gc.tick_at(&r, cold_now()).await;
        }
        assert_eq!(origin.calls.load(Ordering::Relaxed), 2);
        assert_eq!(r.resident_bytes(), 32);
    }

    #[tokio::test]
    async fn stale_gc_respects_pins_and_scan_delete_limits() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            let mut r = runtime(root.path(), paged, origin.clone());
            r.page_gc_config.scan_batch_size = 1;
            r.page_gc_config.delete_batch_size = 1;
            fill(&r, &block(0), paged).await;
            fill(&r, &block(1), paged).await;
            let unit = if paged {
                CacheUnit::Page(block(0), PageIndex(0))
            } else {
                CacheUnit::Whole(block(0))
            };
            let pin = r.lru.pin_guard(unit).unwrap();
            let mut gc = StaleGc::default();
            let now = cold_now();
            gc.tick_at(&r, now).await;
            assert_eq!(origin.calls.load(Ordering::Relaxed), 0);
            assert_eq!(r.resident_bytes(), 64);
            gc.tick_at(&r, now).await;
            assert_eq!(
                r.resident_bytes(),
                32,
                "one candidate reclaims a complete block"
            );
            drop(pin);
            for _ in 0..8 {
                gc.tick_at(&r, now).await;
            }
            assert_eq!(r.resident_bytes(), 0);
        }
    }
    #[tokio::test]
    async fn stale_gc_rechecks_sibling_activity_after_waiting_for_mutation_gate() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let r = Arc::new(runtime(root.path(), true, origin));
        fill(&r, &block(0), true).await;
        let state = r.page_lifecycle.block(&block(0));
        let gate = state.gate.lock().await;
        let task = tokio::spawn({
            let r = r.clone();
            async move {
                StaleGc::default().tick_at(&r, cold_now()).await;
            }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while r.page_mutations.active_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        drop(
            r.index
                .page_reads_and_touch(&block(0), PageIndex(1), PageIndex(2))
                .unwrap(),
        );
        drop(gate);
        task.await.unwrap();
        assert_eq!(r.resident_bytes(), 32);
    }

    #[tokio::test]
    async fn stale_gc_caps_heads_per_batch() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.mode.store(2, Ordering::Relaxed);
        let r = runtime(root.path(), false, origin.clone());
        for n in 0..40 {
            let mut id = block(0);
            id.object = ObjectId::new(Backend::S3, "bucket", format!("key-{n}"));
            fill(&r, &id, false).await;
        }
        let mut gc = StaleGc::default();
        gc.tick_at(&r, cold_now()).await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), MAX_HEADS);
        assert_eq!(r.resident_bytes(), 40 * 32);
        gc.tick_at(&r, cold_now()).await;
        assert_eq!(
            origin.calls.load(Ordering::Relaxed),
            40,
            "resume skipped candidates instead of starving them"
        );
    }
    #[tokio::test]
    async fn stale_gc_pending_candidates_honor_positive_checks_from_previous_batch() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.mode.store(1, Ordering::Relaxed);
        let r = runtime(root.path(), false, origin.clone());
        for n in 0..33 {
            let mut id = block(0);
            id.object = ObjectId::new(Backend::S3, "bucket", format!("key-{n}"));
            fill(&r, &id, false).await;
        }
        let mut id = block(1);
        id.object = ObjectId::new(Backend::S3, "bucket", "key-0");
        fill(&r, &id, false).await;
        let mut gc = StaleGc::default();
        let now = cold_now();
        gc.tick_at(&r, now).await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), MAX_HEADS);
        gc.tick_at(&r, now + 1).await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), 33);
        assert_eq!(r.resident_bytes(), 34 * 32);
    }
    #[tokio::test]
    async fn stale_gc_round_drains_batches_without_rescanning_or_chasing_new_entries() {
        for scan_budget in [16, 4096] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            origin.mode.store(2, Ordering::Relaxed);
            origin.hold.store(true, Ordering::Relaxed);
            let mut r = runtime(root.path(), false, origin.clone());
            r.page_gc_config.scan_batch_size = scan_budget;
            for n in 0..40 {
                let mut id = block(0);
                id.object = ObjectId::new(Backend::S3, "bucket", format!("key-{n}"));
                fill(&r, &id, false).await;
            }
            let r = Arc::new(r);
            let task = tokio::spawn({
                let r = r.clone();
                async move {
                    StaleGc::default().run_round_at(&r, cold_now()).await;
                }
            });
            origin.entered.notified().await;
            let mut late = block(0);
            late.object = ObjectId::new(Backend::S3, "bucket", "late");
            fill(&r, &late, false).await;
            origin.hold.store(false, Ordering::Relaxed);
            origin.release.notify_one();
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(origin.calls.load(Ordering::Relaxed), 40,
                "drain the scan/HEAD batches exactly once, retaining the round's original upper bound");
            assert_eq!(r.resident_bytes(), 41 * 32);
        }
    }

    #[tokio::test]
    async fn stale_gc_scheduler_waits_an_hour_between_complete_rounds() {
        let root = tempfile::tempdir().unwrap();
        let r = Arc::new(runtime(root.path(), false, Arc::new(Origin::default())));
        let service = super::super::WorkerBackground::start(r.clone(), &Default::default());
        let completed = r.metrics.registry.counter(
            "talon_worker_background_task_completed_total",
            "Completed background batches by task.",
            talon_core::metrics::labels(&[("task", "stale_gc")]),
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while completed.get() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // Let the scheduler record the completion time before advancing its clock.
        tokio::time::sleep(Duration::from_millis(5)).await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(3599)).await;
        tokio::time::resume();
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(
            completed.get(),
            1,
            "no repeated scan during the hourly wait"
        );
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::time::resume();
        tokio::time::timeout(Duration::from_secs(2), async {
            while completed.get() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        service.shutdown().await;
        assert_eq!(completed.get(), 2);
    }
    #[tokio::test]
    async fn stale_gc_busy_page_defers_the_entire_block_and_releases_partial_claims() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let r = runtime(root.path(), true, origin.clone());
        fill(&r, &block(0), true).await;
        let state = r.page_lifecycle.block(&block(0));
        let reader = state.acquire(PageIndex(1)).unwrap();
        let mut gc = StaleGc::default();
        let now = cold_now();
        gc.tick_at(&r, now).await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), 1);
        assert_eq!(r.resident_bytes(), 32);
        assert!(
            state.acquire(PageIndex(0)).is_some(),
            "page 0's claim must roll back if page 1 is busy"
        );
        assert!(r
            .paged
            .as_ref()
            .unwrap()
            .dir_for(&block(0))
            .join("0.page")
            .exists());
        drop(reader);
        gc.tick_at(&r, now).await;
        assert_eq!(r.resident_bytes(), 0);
        assert!(state.acquire(PageIndex(0)).is_none());
        assert!(!r.paged.as_ref().unwrap().dir_for(&block(0)).exists());
    }

    #[tokio::test]
    async fn stale_gc_new_sibling_during_head_invalidates_the_block_candidate() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.hold.store(true, Ordering::Relaxed);
        let r = Arc::new(runtime(root.path(), true, origin.clone()));
        r.commit_fetched_page(&block(0), PageIndex(0), 32, Bytes::from_static(&[1; 16]))
            .await
            .unwrap();
        let task = tokio::spawn({
            let r = r.clone();
            async move {
                StaleGc::default().tick_at(&r, cold_now()).await;
            }
        });
        origin.entered.notified().await;
        r.commit_fetched_page(&block(0), PageIndex(1), 32, Bytes::from_static(&[2; 16]))
            .await
            .unwrap();
        origin.release.notify_one();
        task.await.unwrap();
        assert_eq!(r.resident_bytes(), 32);
        assert_eq!(r.lru.total_bytes(), 32);
    }

    #[tokio::test]
    async fn stale_gc_cancelled_caller_still_finishes_whole_block_mutation_with_disk_budget() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let mut r = runtime(root.path(), true, origin);
        let config = talon_core::WorkerConfig {
            background_io_concurrency: 1,
            background_delete_max_per_sec: 0,
            ..Default::default()
        };
        let budget = crate::background::BackgroundBudget::new(&config);
        r.background_budget = Some(budget.clone());
        fill(&r, &block(0), true).await;
        let slot = budget.io().await;
        let r = Arc::new(r);
        let task = tokio::spawn({
            let r = r.clone();
            async move {
                StaleGc::default().tick_at(&r, cold_now()).await;
            }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while r.page_mutations.active_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            r.resident_bytes(),
            32,
            "shared I/O budget holds back the entire block"
        );
        drop(slot);
        r.drain_page_mutations().await;
        assert_eq!(r.resident_bytes(), 0);
        assert_eq!(r.lru.total_bytes(), 0);
        assert!(!r.paged.as_ref().unwrap().dir_for(&block(0)).exists());
    }
}
