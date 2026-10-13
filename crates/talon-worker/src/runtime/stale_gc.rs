//! One bounded block scan for backend-deleted and misplaced cache entries.
use super::{ownership_gc::Ownership, WorkerRuntime};
use crate::eviction::{access_millis, StaleBlockCandidate};
use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};
use talon_core::{Error, WorkerConfig};

const DAY_MS: u64 = 24 * 60 * 60 * 1000;
const MAX_HEADS: usize = 32;

pub(super) struct StaleGc {
    cursor: (u64, u64),
    round_time: Option<u64>,
    pending: VecDeque<StaleBlockCandidate>,
    backend_enabled: bool,
    ownership: Option<Ownership>,
    ownership_watermark: u64,
    ownership_active: bool,
    interval: Duration,
    limits: Option<(usize, usize)>,
}

impl Default for StaleGc {
    fn default() -> Self {
        Self {
            cursor: (0, 0),
            round_time: None,
            pending: VecDeque::new(),
            backend_enabled: true,
            ownership: None,
            ownership_watermark: 0,
            ownership_active: false,
            interval: Duration::from_secs(3600),
            limits: None,
        }
    }
}

impl StaleGc {
    pub(super) fn new(config: &WorkerConfig, ownership: Option<Ownership>) -> Self {
        let enabled = ownership.is_some();
        Self {
            backend_enabled: config.stale_gc_enabled,
            ownership,
            interval: Duration::from_secs(if enabled {
                config.ownership_gc_interval_secs
            } else {
                3600
            }),
            limits: enabled.then_some((
                config.ownership_gc_scan_batch_size,
                config.ownership_gc_delete_batch_size,
            )),
            ..Self::default()
        }
    }

    /// The scheduler owns the only clock: one-second batches, then one round interval.
    pub(super) async fn run_batch(&mut self, worker: &WorkerRuntime) -> Duration {
        let now = *self.round_time.get_or_insert_with(access_millis);
        self.tick_at(worker, now).await;
        if self.cursor.0 >= self.cursor.1 && self.pending.is_empty() {
            self.round_time = None;
            self.interval
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
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    fn pause_ownership(&mut self) {
        self.ownership_active = false;
        self.ownership_watermark = 0;
    }

    async fn tick_at(&mut self, worker: &WorkerRuntime, now: u64) {
        let starting = self.cursor.0 >= self.cursor.1 && self.pending.is_empty();
        if starting {
            self.ownership_active = self.ownership.is_some();
        }
        if self.ownership_active {
            match self
                .ownership
                .as_mut()
                .expect("enabled ownership")
                .refresh()
                .await
            {
                Ok(true) => {}
                Ok(false) if starting => self.ownership_watermark = 0,
                result => {
                    tracing::warn!(?result, "stale GC ownership view unavailable or changed; deferring ownership until next round");
                    self.pause_ownership();
                }
            }
        }
        let mut ownership_deadline = self
            .ownership
            .as_ref()
            .map_or_else(Instant::now, |policy| policy.valid_until);
        if starting {
            self.cursor = (
                if self.backend_enabled {
                    0
                } else {
                    self.ownership_watermark
                },
                worker.lru.stale_scan_ceiling(),
            );
        }
        if !self.backend_enabled && !self.ownership_active {
            self.pending.clear();
            self.cursor.0 = self.cursor.1;
            return;
        }
        if self.pending.is_empty() {
            let lru = worker.lru.clone();
            let mut cursor = self.cursor;
            let (scan, deletes) = self.limits.unwrap_or((usize::MAX, usize::MAX));
            let limits = (
                worker.page_gc_config.scan_batch_size.min(4096).min(scan),
                worker.page_gc_config.delete_batch_size.min(deletes),
            );
            let ownership = self.ownership_active;
            let (cursor, candidates) = tokio::task::spawn_blocking(move || {
                let candidates = lru.scan_stale_blocks(&mut cursor, now, DAY_MS, limits, ownership);
                (cursor, candidates)
            })
            .await
            .expect("stale GC scan panicked");
            self.cursor = cursor;
            self.pending = candidates.into();
        }
        // Ownership is checked before the cold/HEAD filter. Reuse the same queue
        // for deferred blocks, after the frozen boundary, so retries cannot
        // extend this round or need another per-block index.
        let mut misplaced = Vec::new();
        if self.ownership_active {
            let ownership = self.ownership.as_ref().expect("enabled ownership");
            self.pending.retain(|candidate| {
                if candidate.position < self.ownership_watermark {
                    return true;
                }
                worker.page_gc_metrics.ownership_checked.inc();
                if ownership.owns(&candidate.block) {
                    return true;
                }
                worker.lru.defer_stale_block(&candidate.block);
                misplaced.push(candidate.clone());
                false
            });
        }
        if !misplaced.is_empty() {
            // Charge every page, including blocks larger than the batch target.
            // Rate waiting precedes final membership authorization; a large block
            // must not permanently lose its one-second authorization while pacing.
            if let Some(budget) = &worker.background_budget {
                for candidate in &misplaced {
                    budget.delete_units(candidate.units as u64).await;
                }
            }
            if Instant::now() >= ownership_deadline {
                if matches!(
                    self.ownership
                        .as_mut()
                        .expect("enabled ownership")
                        .refresh()
                        .await,
                    Ok(true)
                ) {
                    ownership_deadline = self
                        .ownership
                        .as_ref()
                        .expect("enabled ownership")
                        .valid_until;
                } else {
                    self.pause_ownership();
                }
            }
            if self.ownership_active {
                for candidate in misplaced {
                    if Instant::now() >= ownership_deadline {
                        break;
                    }
                    worker
                        .page_gc_metrics
                        .ownership_attempted
                        .add(candidate.units as u64);
                    worker
                        .unlink_stale_block_for(candidate, Some(ownership_deadline))
                        .await;
                }
            }
        }
        // Backend work is bounded independently of local disk pacing. Warm blocks
        // and successful checks within 24 hours never issue a HEAD.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut checked = HashMap::new();
        while let Some(candidate) = self.pending.front() {
            if !self.backend_enabled || now.saturating_sub(candidate.last_access) <= DAY_MS {
                self.pending.pop_front();
                continue;
            }
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
                worker.unlink_stale_block(candidate).await;
            }
        }
        if self.cursor.0 >= self.cursor.1 && self.pending.is_empty() && self.ownership_active {
            self.ownership_watermark = self.cursor.1;
        }
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
    struct MembershipServer {
        members: Arc<std::sync::Mutex<Option<Vec<&'static str>>>>,
        calls: Arc<AtomicUsize>,
        config: WorkerConfig,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for MembershipServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
    impl MembershipServer {
        async fn start() -> Self {
            use talon_core::worker_membership::{MemberRegistry, WorkerDiscovery, WorkerMember};
            use talon_transport::{codec, ControlMessage};
            use tokio::io::AsyncWriteExt;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let config = WorkerConfig {
                coordinator: listener.local_addr().unwrap().to_string(),
                ownership_gc_interval_secs: 3600,
                stale_gc_enabled: false,
                ..Default::default()
            };
            let members = Arc::new(std::sync::Mutex::new(Some(vec!["a", "b"])));
            let calls = Arc::new(AtomicUsize::new(0));
            let (snapshot, seen) = (members.clone(), calls.clone());
            let task = tokio::spawn(async move {
                loop {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    assert_eq!(
                        crate::tokio_conn::read_control(&mut stream).await.unwrap(),
                        Some(ControlMessage::MembershipQuery {})
                    );
                    seen.fetch_add(1, Ordering::Relaxed);
                    let reply = match snapshot.lock().unwrap().clone() {
                        Some(members) => ControlMessage::MembershipList {
                            view: WorkerDiscovery::retained(
                                &MemberRegistry {
                                    members: members
                                        .into_iter()
                                        .map(|id| WorkerMember {
                                            worker_id: id.into(),
                                            zone: None,
                                            retired: false,
                                        })
                                        .collect(),
                                    ..Default::default()
                                },
                                &[],
                            ),
                        },
                        None => ControlMessage::Ack {
                            ok: false,
                            detail: None,
                        },
                    };
                    stream
                        .write_all(&codec::encode(0, &reply).unwrap())
                        .await
                        .unwrap();
                }
            });
            Self {
                members,
                calls,
                config,
                task,
            }
        }
        fn gc(&self) -> StaleGc {
            StaleGc::new(
                &self.config,
                Some(Ownership::new(
                    talon_core::NodeId::new("a"),
                    &self.config,
                    None,
                )),
            )
        }
    }
    fn placed(owner: &str, offset: usize) -> BlockId {
        let nodes = ["a", "b"].map(|id| talon_core::NodeInfo {
            id: talon_core::NodeId::new(id),
            address: String::new(),
            role: talon_core::NodeRole::Worker,
        });
        let table = talon_core::CachePlacementTable::new(&nodes);
        (0..10000)
            .map(block)
            .filter(|id| table.primary(id).unwrap().id.0 == owner)
            .nth(offset)
            .unwrap()
    }

    #[tokio::test]
    async fn one_scan_checks_ownership_before_cold_backend_filter() {
        for paged in [false, true] {
            let mut server = MembershipServer::start().await;
            server.config.stale_gc_enabled = true;
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            let mut r = runtime(root.path(), paged, origin.clone());
            r.page_gc_config.scan_batch_size = 1;
            let misplaced = placed("b", 0);
            let owned = placed("a", 0);
            fill(&r, &misplaced, paged).await;
            fill(&r, &owned, paged).await;
            let mut gc = server.gc();
            gc.tick_at(&r, access_millis()).await;
            assert!(r.index.get(&misplaced).is_none());
            assert_eq!(origin.calls.load(Ordering::Relaxed), 0);
            assert!(gc.cursor.0 < gc.cursor.1);
            gc.tick_at(&r, cold_now()).await;
            assert!(r.index.get(&owned).is_none());
            assert_eq!(origin.calls.load(Ordering::Relaxed), 1);
            assert_eq!(server.calls.load(Ordering::Relaxed), 2);
            assert_eq!(r.resident_bytes(), 0);
            assert_eq!(r.l1.resident_bytes(), 0);
        }
    }

    #[tokio::test]
    async fn shared_queue_preserves_incremental_ownership_and_finite_rounds() {
        let server = MembershipServer::start().await;
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let mut r = runtime(root.path(), false, origin.clone());
        r.page_gc_config.scan_batch_size = 1;
        fill(&r, &placed("a", 0), false).await;
        fill(&r, &placed("a", 1), false).await;
        let mut gc = server.gc();
        assert_eq!(gc.run_batch(&r).await, Duration::from_secs(1));
        let ceiling = gc.cursor.1;
        fill(&r, &placed("b", 0), false).await;
        assert_eq!(gc.run_batch(&r).await, Duration::from_secs(3600));
        assert_eq!(gc.cursor.1, ceiling);
        assert!(r.index.get(&placed("b", 0)).is_some());
        assert_eq!(gc.run_batch(&r).await, Duration::from_secs(3600));
        assert!(r.index.get(&placed("b", 0)).is_none());
        // No rescanning of stable owned blocks. The only residual queue range
        // is the now-removed deferred block; afterwards start equals end.
        gc.run_batch(&r).await;
        assert_eq!(gc.cursor.0, gc.ownership_watermark);
        assert_eq!(gc.cursor.0, r.lru.stale_scan_ceiling());
        let cursor = gc.cursor;
        let checks = r.page_gc_metrics.ownership_checked.get();
        gc.run_batch(&r).await;
        assert_eq!(gc.cursor, cursor);
        assert_eq!(r.page_gc_metrics.ownership_checked.get(), checks);
        assert_eq!(origin.calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn membership_change_or_failure_pauses_ownership_until_next_round() {
        for failed in [false, true] {
            let server = MembershipServer::start().await;
            *server.members.lock().unwrap() = Some(vec!["a"]);
            let root = tempfile::tempdir().unwrap();
            let mut r = runtime(root.path(), false, Arc::new(Origin::default()));
            r.page_gc_config.scan_batch_size = 1;
            for n in 0..2 {
                fill(&r, &placed("b", n), false).await;
            }
            let mut gc = server.gc();
            assert_eq!(gc.run_batch(&r).await, Duration::from_secs(1));
            *server.members.lock().unwrap() = if failed { None } else { Some(vec!["a", "b"]) };
            assert_eq!(gc.run_batch(&r).await, Duration::from_secs(3600));
            assert_eq!(r.index.len(), 2);
            *server.members.lock().unwrap() = Some(vec!["a", "b"]);
            while gc.run_batch(&r).await != Duration::from_secs(3600) {}
            assert!(r.index.is_empty());
        }
    }

    #[tokio::test]
    async fn ownership_reader_and_pin_defer_entire_block_to_next_round() {
        let server = MembershipServer::start().await;
        let root = tempfile::tempdir().unwrap();
        let r = runtime(root.path(), true, Arc::new(Origin::default()));
        let id = placed("b", 0);
        fill(&r, &id, true).await;
        let state = r.page_lifecycle.block(&id);
        let reader = state.acquire(PageIndex(0)).unwrap();
        let mut gc = server.gc();
        gc.run_batch(&r).await;
        assert_eq!(r.index.page_count(), 2);
        drop(reader);
        let pin = r
            .lru
            .pin_guard(CacheUnit::Page(id.clone(), PageIndex(1)))
            .unwrap();
        gc.run_batch(&r).await;
        assert_eq!(r.index.page_count(), 2);
        drop(pin);
        gc.run_batch(&r).await;
        assert!(r.index.is_empty());
        assert!(state.acquire(PageIndex(0)).is_none());
        assert_eq!(r.resident_bytes(), 0);
    }

    #[tokio::test]
    async fn failed_membership_does_not_disable_backend_deletion() {
        let mut server = MembershipServer::start().await;
        server.config.stale_gc_enabled = true;
        *server.members.lock().unwrap() = None;
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let r = runtime(root.path(), false, origin.clone());
        fill(&r, &placed("b", 0), false).await;
        server.gc().tick_at(&r, cold_now()).await;
        assert!(r.index.is_empty());
        assert_eq!(origin.calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn ownership_deadline_bounds_io_and_gate_waits() {
        for gate_wait in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut r = runtime(root.path(), true, Arc::new(Origin::default()));
            let id = placed("b", 0);
            fill(&r, &id, true).await;
            let budget = crate::background::BackgroundBudget::new(&WorkerConfig {
                background_io_concurrency: 1,
                ..Default::default()
            });
            let held_io = if gate_wait {
                None
            } else {
                Some(budget.io().await)
            };
            r.background_budget = Some(budget);
            let state = r.page_lifecycle.block(&id);
            let held_gate = if gate_wait {
                Some(state.gate.lock().await)
            } else {
                None
            };
            let candidate = r
                .lru
                .scan_stale_blocks(
                    &mut (0, r.lru.stale_scan_ceiling()),
                    access_millis(),
                    DAY_MS,
                    (1, 1),
                    true,
                )
                .pop()
                .unwrap();
            tokio::time::timeout(
                Duration::from_secs(1),
                r.unlink_stale_block_for(
                    candidate,
                    Some(Instant::now() + Duration::from_millis(20)),
                ),
            )
            .await
            .unwrap();
            assert_eq!(r.page_mutations.active_count(), 0);
            assert_eq!(r.index.page_count(), 2);
            drop(held_gate);
            drop(held_io);
        }
    }

    #[tokio::test]
    async fn ownership_unlink_failure_requires_fresh_membership_on_retry() {
        let server = MembershipServer::start().await;
        let root = tempfile::tempdir().unwrap();
        let r = runtime(root.path(), true, Arc::new(Origin::default()));
        let id = placed("b", 0);
        fill(&r, &id, true).await;
        let dir = r.paged.as_ref().unwrap().dir_for(&id);
        let saved = dir.with_extension("saved");
        std::fs::rename(&dir, &saved).unwrap();
        std::fs::write(&dir, b"not a directory").unwrap();
        let mut gc = server.gc();
        gc.run_batch(&r).await;
        assert_eq!(r.resident_bytes(), 32);
        let state = r.page_lifecycle.block(&id);
        for page in 0..2 {
            assert!(state.acquire(PageIndex(page)).is_some());
        }
        std::fs::remove_file(&dir).unwrap();
        std::fs::rename(&saved, &dir).unwrap();
        r.gc_once().await;
        assert_eq!(r.resident_bytes(), 32);
        *server.members.lock().unwrap() = None;
        gc.run_batch(&r).await;
        assert_eq!(r.resident_bytes(), 32);
        *server.members.lock().unwrap() = Some(vec!["a", "b"]);
        gc.run_batch(&r).await;
        assert!(r.index.is_empty());
    }

    #[tokio::test]
    async fn rate_wait_refreshes_membership_before_whole_block_deletion() {
        for changed in [false, true] {
            let server = MembershipServer::start().await;
            let root = tempfile::tempdir().unwrap();
            let mut r = runtime(root.path(), true, Arc::new(Origin::default()));
            let id = placed("b", 0);
            // More pages than one second of credits: progress requires a new
            // observation after waiting, and a changed view must prevent deletion.
            for p in 0..3 {
                r.commit_fetched_page(&id, PageIndex(p), 64, Bytes::from_static(&[1; 16]))
                    .await
                    .unwrap();
            }
            r.background_budget = Some(crate::background::BackgroundBudget::new(&WorkerConfig {
                background_delete_max_per_sec: 1,
                ..Default::default()
            }));
            let mut gc = server.gc();
            let run = gc.run_batch(&r);
            let disturbance = async {
                if changed {
                    while server.calls.load(Ordering::Relaxed) == 0 {
                        tokio::task::yield_now().await;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    *server.members.lock().unwrap() = Some(vec!["a"]);
                }
            };
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(run, disturbance);
            })
            .await
            .unwrap();
            assert!(server.calls.load(Ordering::Relaxed) >= 2);
            assert_eq!(r.index.is_empty(), !changed);
        }
    }
}
