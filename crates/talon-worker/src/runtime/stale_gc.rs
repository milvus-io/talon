//! One bounded block scan for backend-deleted and misplaced cache entries.
use super::{MembershipSnapshot, WorkerRuntime};
use crate::eviction::{access_millis, StaleBlockCandidate};
use std::sync::Arc;
use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};
use talon_core::{BlockId, CachePlacementTable, Error, NodeId, WorkerConfig};
use talon_transport::ZonedNodeInfo;

const DAY_MS: u64 = 24 * 60 * 60 * 1000;
const MAX_HEADS: usize = 32;

pub(super) struct StaleGc {
    cursor: (u64, u64),
    pending: VecDeque<StaleBlockCandidate>,
    backend_enabled: bool,
    worker: Option<NodeId>,
    membership: Option<Arc<MembershipSnapshot>>,
    members: Vec<ZonedNodeInfo>,
    global_placement: Option<CachePlacementTable>,
    local_placement: Option<CachePlacementTable>,
    replicas: usize,
    ownership_watermark: u64,
    ownership_active: bool,
    limits: Option<(usize, usize)>,
}

impl Default for StaleGc {
    fn default() -> Self {
        Self {
            cursor: (0, 0),
            pending: VecDeque::new(),
            backend_enabled: true,
            worker: None,
            membership: None,
            members: Vec::new(),
            global_placement: None,
            local_placement: None,
            replicas: 1,
            ownership_watermark: 0,
            ownership_active: false,
            limits: None,
        }
    }
}

impl StaleGc {
    pub(super) fn new(config: &WorkerConfig) -> Self {
        let enabled = config.ownership_gc_interval_secs > 0;
        let worker = enabled.then(|| {
            NodeId::new(
                config
                    .node_id
                    .clone()
                    .expect("ownership cleanup requires a resolved worker identity"),
            )
        });
        Self {
            backend_enabled: config.stale_gc_enabled,
            worker,
            replicas: config.ownership_gc_replicas,
            limits: enabled.then_some((
                config.ownership_gc_scan_batch_size,
                config.ownership_gc_delete_batch_size,
            )),
            ..Self::default()
        }
    }

    /// Consume the latest local snapshot; no network or refresh work belongs to GC.
    fn read_membership(&mut self, worker: &WorkerRuntime) -> anyhow::Result<bool> {
        let snapshot = worker
            .membership_snapshot()
            .ok_or_else(|| anyhow::anyhow!("membership unavailable or expired"))?;
        let unchanged = if self
            .membership
            .as_ref()
            .is_some_and(|old| Arc::ptr_eq(old, &snapshot))
        {
            true
        } else {
            self.observe_membership(snapshot.members.clone())?
        };
        self.membership = Some(snapshot);
        Ok(unchanged)
    }

    fn observe_membership(&mut self, members: Vec<ZonedNodeInfo>) -> anyhow::Result<bool> {
        anyhow::ensure!(
            members
                .iter()
                .any(|n| Some(&n.info.id) == self.worker.as_ref()),
            "local worker missing from membership"
        );
        if self.global_placement.is_some() && self.members == members {
            return Ok(true);
        }
        self.members = members;
        self.local_placement = None;
        let nodes: Vec<_> = self.members.iter().map(|n| n.info.clone()).collect();
        if let Some(zone) = self
            .members
            .iter()
            .find(|n| Some(&n.info.id) == self.worker.as_ref())
            .and_then(|n| n.zone.as_ref())
        {
            let local: Vec<_> = self
                .members
                .iter()
                .filter(|n| n.zone.as_ref() == Some(zone))
                .map(|n| n.info.clone())
                .collect();
            if local.len() != nodes.len() {
                self.local_placement = Some(CachePlacementTable::new(&local));
            }
        }
        self.global_placement = Some(CachePlacementTable::new(&nodes));
        Ok(false)
    }

    fn owns(&self, block: &BlockId) -> bool {
        self.global_placement
            .iter()
            .chain(self.local_placement.iter())
            .any(|table| {
                if self.replicas == 1 {
                    table
                        .primary(block)
                        .is_some_and(|n| Some(&n.id) == self.worker.as_ref())
                } else {
                    table
                        .rank(block, self.replicas)
                        .iter()
                        .any(|n| Some(&n.id) == self.worker.as_ref())
                }
            })
    }

    /// Complete one bounded queue generation; the scheduler supplies the round interval.
    pub(super) async fn run_round(&mut self, worker: &WorkerRuntime) {
        self.run_round_at(worker, access_millis()).await;
    }

    fn round_complete(&self) -> bool {
        self.cursor.0 >= self.cursor.1 && self.pending.is_empty()
    }

    async fn run_round_at(&mut self, worker: &WorkerRuntime, now: u64) {
        loop {
            self.tick_at(worker, now).await;
            if self.round_complete() {
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
        let starting = self.round_complete();
        if starting {
            self.ownership_active = self.worker.is_some();
        }
        if self.ownership_active {
            match self.read_membership(worker) {
                Ok(true) => {}
                Ok(false) if starting => self.ownership_watermark = 0,
                result => {
                    tracing::warn!(?result, "stale GC ownership view unavailable or changed; deferring ownership until next round");
                    self.pause_ownership();
                }
            }
        }
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
            for candidate in std::mem::take(&mut self.pending) {
                if candidate.position >= self.ownership_watermark {
                    worker.page_gc_metrics.ownership_checked.inc();
                    if !self.owns(&candidate.block) {
                        worker.lru.defer_stale_block(&candidate.block);
                        misplaced.push(candidate);
                        continue;
                    }
                }
                self.pending.push_back(candidate);
            }
        }
        if !misplaced.is_empty() {
            // Pay deletion credits first, then consume the latest snapshot again.
            // A refresh during pacing may extend validity, but must not change placement.
            if let Some(budget) = &worker.background_budget {
                for candidate in &misplaced {
                    budget.delete_units(candidate.units as u64).await;
                }
            }
            if !matches!(self.read_membership(worker), Ok(true)) {
                self.pause_ownership();
            }
            if self.ownership_active {
                let membership = self.membership.as_ref().expect("validated membership");
                for candidate in misplaced {
                    if !worker.membership_is_current(membership) {
                        self.pause_ownership();
                        break;
                    }
                    worker
                        .page_gc_metrics
                        .ownership_attempted
                        .add(candidate.units as u64);
                    worker
                        .unlink_stale_block_for(candidate, Some(membership.clone()))
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
        if self.round_complete() && self.ownership_active {
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
    use std::time::Instant;
    use talon_core::{Backend, BackendStore, BlockId, ObjectId, ObjectStat, PageIndex, Version};
    use talon_core::{NodeInfo, NodeRole};

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
    async fn stale_gc_scheduler_counts_rounds_and_waits_configured_interval() {
        for ownership_enabled in [false, true] {
            let server = if ownership_enabled {
                Some(MembershipFixture::new())
            } else {
                None
            };
            let root = tempfile::tempdir().unwrap();
            let r = Arc::new(runtime(root.path(), false, Arc::new(Origin::default())));
            let (service, interval) = if let Some(server) = &server {
                for n in 0..2 {
                    fill(&r, &placed("a", n), false).await;
                }
                server.publish(&r);
                let mut config = server.config.clone();
                config.ownership_gc_interval_secs = 7200;
                config.background_scan_batch_size = 1;
                (
                    super::super::WorkerBackground::start(r.clone(), &config),
                    7200,
                )
            } else {
                (
                    super::super::WorkerBackground::start(r.clone(), &Default::default()),
                    3600,
                )
            };
            let completed = r.metrics.registry.counter(
                "talon_worker_background_task_completed_total",
                "Completed background batches by task.",
                talon_core::metrics::labels(&[("task", "stale_gc")]),
            );
            tokio::time::timeout(Duration::from_secs(5), async {
                while completed.get() == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            if server.is_some() {
                assert_eq!(
                    r.page_gc_metrics.ownership_checked.get(),
                    2,
                    "both batches belong to one invocation"
                );
            }
            // Let the scheduler record the completion time before advancing its clock.
            tokio::time::sleep(Duration::from_millis(5)).await;
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(interval - 1)).await;
            tokio::time::resume();
            tokio::time::sleep(Duration::from_millis(5)).await;
            assert_eq!(
                completed.get(),
                1,
                "no repeated scan during the configured round interval"
            );
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(2)).await;
            tokio::time::resume();
            tokio::time::timeout(Duration::from_secs(5), async {
                while completed.get() < 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            service.shutdown().await;
            assert_eq!(completed.get(), 2);
        }
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
    struct MembershipFixture {
        members: std::sync::Mutex<Option<Vec<&'static str>>>,
        config: WorkerConfig,
    }
    impl MembershipFixture {
        fn new() -> Self {
            Self {
                members: std::sync::Mutex::new(Some(vec!["a", "b"])),
                config: WorkerConfig {
                    node_id: Some("a".into()),
                    ownership_gc_interval_secs: 3600,
                    stale_gc_enabled: false,
                    ..Default::default()
                },
            }
        }
        fn publish(&self, worker: &WorkerRuntime) {
            let view = self.members.lock().unwrap().as_ref().map(|ids| {
                discovery(
                    &ids.iter()
                        .map(|id| ZonedNodeInfo {
                            info: NodeInfo {
                                id: NodeId::new(*id),
                                address: String::new(),
                                role: NodeRole::Worker,
                            },
                            zone: None,
                        })
                        .collect::<Vec<_>>(),
                )
            });
            worker.update_membership(view, Instant::now()).unwrap();
        }
        fn gc(&self, worker: &WorkerRuntime) -> StaleGc {
            self.publish(worker);
            StaleGc::new(&self.config)
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
            let mut server = MembershipFixture::new();
            server.config.stale_gc_enabled = true;
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            let mut r = runtime(root.path(), paged, origin.clone());
            r.page_gc_config.scan_batch_size = 1;
            let misplaced = placed("b", 0);
            let owned = placed("a", 0);
            fill(&r, &misplaced, paged).await;
            fill(&r, &owned, paged).await;
            let mut gc = server.gc(&r);
            gc.tick_at(&r, access_millis()).await;
            assert!(r.index.get(&misplaced).is_none());
            assert_eq!(origin.calls.load(Ordering::Relaxed), 0);
            assert!(gc.cursor.0 < gc.cursor.1);
            gc.tick_at(&r, cold_now()).await;
            assert!(r.index.get(&owned).is_none());
            assert_eq!(origin.calls.load(Ordering::Relaxed), 1);
            assert_eq!(r.resident_bytes(), 0);
            assert_eq!(r.l1.resident_bytes(), 0);
        }
    }

    #[tokio::test]
    async fn shared_queue_preserves_incremental_ownership_and_finite_rounds() {
        let server = MembershipFixture::new();
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let mut r = runtime(root.path(), false, origin.clone());
        r.page_gc_config.scan_batch_size = 1;
        fill(&r, &placed("a", 0), false).await;
        fill(&r, &placed("a", 1), false).await;
        let mut gc = server.gc(&r);
        gc.tick_at(&r, access_millis()).await;
        assert!(!gc.round_complete());
        let ceiling = gc.cursor.1;
        fill(&r, &placed("b", 0), false).await;
        gc.tick_at(&r, access_millis()).await;
        assert!(gc.round_complete());
        assert_eq!(gc.cursor.1, ceiling);
        assert!(r.index.get(&placed("b", 0)).is_some());
        gc.tick_at(&r, access_millis()).await;
        assert!(gc.round_complete());
        assert!(r.index.get(&placed("b", 0)).is_none());
        // No rescanning of stable owned blocks. The only residual queue range
        // is the now-removed deferred block; afterwards start equals end.
        gc.run_round(&r).await;
        assert_eq!(gc.cursor.0, gc.ownership_watermark);
        assert_eq!(gc.cursor.0, r.lru.stale_scan_ceiling());
        let cursor = gc.cursor;
        let checks = r.page_gc_metrics.ownership_checked.get();
        gc.run_round(&r).await;
        assert_eq!(gc.cursor, cursor);
        assert_eq!(r.page_gc_metrics.ownership_checked.get(), checks);
        assert_eq!(origin.calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn membership_change_or_failure_pauses_ownership_until_next_round() {
        for failed in [false, true] {
            let server = MembershipFixture::new();
            *server.members.lock().unwrap() = Some(vec!["a"]);
            let root = tempfile::tempdir().unwrap();
            let mut r = runtime(root.path(), false, Arc::new(Origin::default()));
            r.page_gc_config.scan_batch_size = 1;
            for n in 0..2 {
                fill(&r, &placed("b", n), false).await;
            }
            let mut gc = server.gc(&r);
            gc.tick_at(&r, access_millis()).await;
            assert!(!gc.round_complete());
            *server.members.lock().unwrap() = if failed { None } else { Some(vec!["a", "b"]) };
            server.publish(&r);
            gc.tick_at(&r, access_millis()).await;
            assert!(gc.round_complete());
            assert_eq!(r.index.len(), 2);
            *server.members.lock().unwrap() = Some(vec!["a", "b"]);
            server.publish(&r);
            gc.run_round(&r).await;
            assert!(r.index.is_empty());
        }
    }

    #[tokio::test]
    async fn ownership_reader_and_pin_defer_entire_block_to_next_round() {
        let server = MembershipFixture::new();
        let root = tempfile::tempdir().unwrap();
        let r = runtime(root.path(), true, Arc::new(Origin::default()));
        let id = placed("b", 0);
        fill(&r, &id, true).await;
        let state = r.page_lifecycle.block(&id);
        let reader = state.acquire(PageIndex(0)).unwrap();
        let mut gc = server.gc(&r);
        gc.run_round(&r).await;
        assert_eq!(r.index.page_count(), 2);
        drop(reader);
        let pin = r
            .lru
            .pin_guard(CacheUnit::Page(id.clone(), PageIndex(1)))
            .unwrap();
        gc.run_round(&r).await;
        assert_eq!(r.index.page_count(), 2);
        drop(pin);
        gc.run_round(&r).await;
        assert!(r.index.is_empty());
        assert!(state.acquire(PageIndex(0)).is_none());
        assert_eq!(r.resident_bytes(), 0);
    }

    #[tokio::test]
    async fn failed_membership_does_not_disable_backend_deletion() {
        let mut server = MembershipFixture::new();
        server.config.stale_gc_enabled = true;
        *server.members.lock().unwrap() = None;
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let r = runtime(root.path(), false, origin.clone());
        fill(&r, &placed("b", 0), false).await;
        server.gc(&r).tick_at(&r, cold_now()).await;
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
                    Some(Arc::new(MembershipSnapshot {
                        members: vec![],
                        valid_until: Instant::now() + Duration::from_millis(20),
                    })),
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
        let server = MembershipFixture::new();
        let root = tempfile::tempdir().unwrap();
        let r = runtime(root.path(), true, Arc::new(Origin::default()));
        let id = placed("b", 0);
        fill(&r, &id, true).await;
        let dir = r.paged.as_ref().unwrap().dir_for(&id);
        let saved = dir.with_extension("saved");
        std::fs::rename(&dir, &saved).unwrap();
        std::fs::write(&dir, b"not a directory").unwrap();
        let mut gc = server.gc(&r);
        gc.run_round(&r).await;
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
        server.publish(&r);
        gc.run_round(&r).await;
        assert_eq!(r.resident_bytes(), 32);
        *server.members.lock().unwrap() = Some(vec!["a", "b"]);
        server.publish(&r);
        gc.run_round(&r).await;
        assert!(r.index.is_empty());
    }

    #[tokio::test]
    async fn rate_wait_consumes_new_snapshot_before_whole_block_deletion() {
        for changed in [false, true] {
            let server = MembershipFixture::new();
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
            let mut gc = server.gc(&r);
            // Expire the initial observation during rate waiting. The independent
            // publisher supplies a replacement; GC only consumes it.
            let initial = r.membership_snapshot().unwrap();
            *r.membership.lock().unwrap() = Some(Arc::new(MembershipSnapshot {
                members: initial.members.clone(),
                valid_until: Instant::now() + Duration::from_millis(200),
            }));
            let run = gc.run_round(&r);
            let disturbance = async {
                tokio::time::sleep(Duration::from_millis(300)).await;
                if changed {
                    *server.members.lock().unwrap() = Some(vec!["a"]);
                }
                server.publish(&r);
            };
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(run, disturbance);
            })
            .await
            .unwrap();
            assert_eq!(r.index.is_empty(), !changed);
        }
    }
    fn members() -> Vec<ZonedNodeInfo> {
        ["a", "b", "c"]
            .into_iter()
            .map(|id| ZonedNodeInfo {
                info: NodeInfo {
                    id: NodeId::new(id),
                    address: String::new(),
                    role: NodeRole::Worker,
                },
                zone: None,
            })
            .collect()
    }
    fn discovery(nodes: &[ZonedNodeInfo]) -> talon_core::worker_membership::WorkerDiscovery {
        use talon_core::worker_membership::*;
        WorkerDiscovery::retained(
            &MemberRegistry {
                members: nodes
                    .iter()
                    .map(|n| WorkerMember {
                        worker_id: n.info.id.0.clone(),
                        zone: n.zone.clone(),
                        retired: false,
                    })
                    .collect(),
                ..Default::default()
            },
            &[],
        )
    }
    #[tokio::test]
    async fn missing_expired_or_invalid_snapshots_never_authorize_ownership() {
        let fixture = MembershipFixture::new();
        let root = tempfile::tempdir().unwrap();
        let r = runtime(root.path(), false, Arc::new(Origin::default()));
        let id = placed("b", 0);
        fill(&r, &id, false).await;
        let mut gc = StaleGc::new(&fixture.config);
        gc.run_round(&r).await;
        assert_eq!(r.resident_bytes(), 32, "startup has no membership");
        fixture.publish(&r);
        let snapshot = r.membership_snapshot().unwrap();
        *r.membership.lock().unwrap() = Some(Arc::new(MembershipSnapshot {
            members: snapshot.members.clone(),
            valid_until: Instant::now(),
        }));
        gc.run_round(&r).await;
        assert_eq!(
            r.resident_bytes(),
            32,
            "stopped publisher cannot extend validity"
        );
        for expired in [false, true] {
            fixture.publish(&r);
            let mut invalid = discovery(&members());
            if expired {
                invalid.valid_for_ms = 1;
            } else {
                invalid.workers.push(invalid.workers[0].clone());
            }
            assert!(r
                .update_membership(Some(invalid), Instant::now() - Duration::from_secs(1))
                .is_err());
            assert!(
                r.membership_snapshot().is_none(),
                "invalid publication revokes old view"
            );
            gc.run_round(&r).await;
            assert_eq!(r.resident_bytes(), 32);
        }
        fixture.publish(&r);
        gc.run_round(&r).await;
        assert_eq!(r.resident_bytes(), 0);
    }

    #[tokio::test]
    async fn membership_changes_behind_mutation_gate_retain_whole_block() {
        for unavailable in [false, true] {
            let fixture = MembershipFixture::new();
            let root = tempfile::tempdir().unwrap();
            let r = runtime(root.path(), true, Arc::new(Origin::default()));
            let id = placed("b", 0);
            fill(&r, &id, true).await;
            let state = r.page_lifecycle.block(&id);
            let gate = state.gate.lock().await;
            let mut gc = fixture.gc(&r);
            let run = gc.run_round(&r);
            let change = async {
                while r.page_mutations.active_count() == 0 {
                    tokio::task::yield_now().await;
                }
                *fixture.members.lock().unwrap() = if unavailable { None } else { Some(vec!["a"]) };
                fixture.publish(&r);
                drop(gate);
            };
            tokio::time::timeout(Duration::from_secs(2), async {
                tokio::join!(run, change);
            })
            .await
            .unwrap();
            assert_eq!(r.resident_bytes(), 32);
            assert_eq!(r.index.page_count(), 2);
            assert!(
                state.acquire(PageIndex(0)).is_some(),
                "claims reopen on changed membership"
            );
        }
    }

    #[test]
    fn membership_uses_logical_ids_and_zones_across_instance_changes() {
        use talon_core::worker_membership::*;
        let root = tempfile::tempdir().unwrap();
        let r = runtime(root.path(), false, Arc::new(Origin::default()));
        let mut view = discovery(&members());
        let mut snapshots = Vec::new();
        for state in [
            InstanceState::Offline,
            InstanceState::Conflict,
            InstanceState::Serving {
                instance_id: "new-process".into(),
                address: "new-address:7001".into(),
            },
        ] {
            view.workers[0].state = state;
            r.update_membership(Some(view.clone()), Instant::now())
                .unwrap();
            snapshots.push(r.membership_snapshot().unwrap().members.clone());
        }
        assert_eq!(snapshots[0].len(), 3, "offline members still own placement");
        assert_eq!(snapshots[0], snapshots[1]);
        assert_eq!(snapshots[1], snapshots[2]);
    }

    #[test]
    fn validates_membership_and_retains_global_and_zone_candidates() {
        let config = WorkerConfig {
            node_id: Some("a".into()),
            ownership_gc_interval_secs: 3600,
            ..Default::default()
        };
        let root = tempfile::tempdir().unwrap();
        let r = runtime(root.path(), false, Arc::new(Origin::default()));
        let mut gc = StaleGc::new(&config);
        r.update_membership(Some(discovery(&members())), Instant::now())
            .unwrap();
        assert!(!gc.read_membership(&r).unwrap());
        let mut reordered = discovery(&members());
        reordered.workers.reverse();
        r.update_membership(Some(reordered), Instant::now())
            .unwrap();
        assert!(gc.read_membership(&r).unwrap());
        let mut duplicate = members();
        duplicate.push(duplicate[0].clone());
        for invalid in [Vec::new(), members()[1..].to_vec(), duplicate] {
            let published = r.update_membership(Some(discovery(&invalid)), Instant::now());
            assert!(published.is_err() || gc.read_membership(&r).is_err());
        }
        let table =
            CachePlacementTable::new(&members().into_iter().map(|n| n.info).collect::<Vec<_>>());
        let id = (0..10000)
            .map(|n| {
                BlockId::new(
                    ObjectId::new(Backend::S3, "b", n.to_string()),
                    0,
                    256,
                    Version::new("v1"),
                )
            })
            .find(|id| table.primary(id).unwrap().id.0 == "b")
            .unwrap();
        assert!(!gc.owns(&id));
        gc.replicas = 3;
        assert!(gc.owns(&id));
        gc.replicas = 1;
        let mut zoned = members();
        zoned[0].zone = Some("zone-a".into());
        assert!(!gc.observe_membership(zoned).unwrap());
        assert!(gc.owns(&id));
    }
}
