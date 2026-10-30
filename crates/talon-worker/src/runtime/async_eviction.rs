//! Background capacity reclamation with high/low watermark hysteresis.
use super::WorkerRuntime;
#[cfg(test)]
use std::sync::Arc;
use std::time::Duration;
use talon_core::WorkerConfig;

#[derive(Default, PartialEq, Eq)]
pub(super) enum SmallCapacityPhase {
    #[default]
    Idle,
    Selecting,
    Reclaiming,
}

pub(super) struct EvictionCycle {
    high: u64,
    low: u64,
    active: bool,
    progressed: bool,
}

impl EvictionCycle {
    pub(super) fn new(capacity: u64, config: &WorkerConfig) -> Self {
        Self {
            high: ((capacity as f64 * config.async_eviction_high_watermark).ceil() as u64).max(1),
            low: (capacity as f64 * config.async_eviction_low_watermark).floor() as u64,
            active: false,
            progressed: false,
        }
    }

    pub(super) fn next_delay(&self, idle: Duration) -> Option<Duration> {
        // A short retry also bounds spinning when readers/pins prevent progress.
        Some(if self.active {
            if self.progressed {
                Duration::ZERO
            } else {
                Duration::from_secs(1)
            }
        } else {
            idle
        })
    }

    pub(super) async fn tick(&mut self, worker: &WorkerRuntime) {
        self.progressed = false;
        if let Some(registry) = worker.registry() {
            let workers = registry.snapshot();
            let total: u64 = workers.iter().map(|w| w.resident_bytes()).sum();
            let logical: u64 = workers.iter().map(|w| w.lru.total_bytes()).sum();
            if total <= self.low {
                self.active = false;
            } else if total >= self.high {
                self.active = true;
            }
            if self.active {
                for namespace in &workers {
                    // Cache units are indivisible: one namespace can release more
                    // than requested. Recompute the global excess before reclaiming
                    // from the next namespace instead of giving each a fixed share.
                    let total: u64 = workers.iter().map(|w| w.resident_bytes()).sum();
                    if total <= self.low {
                        break;
                    }
                    let other_credit =
                        WorkerRuntime::other_small_reclaim_credit(&workers, namespace).await;
                    let total: u64 = workers.iter().map(|w| w.resident_bytes()).sum();
                    if total <= self.low {
                        break;
                    }
                    let target = namespace
                        .resident_bytes()
                        .saturating_sub((total - self.low).saturating_sub(other_credit));
                    namespace.evict_background_batch(target).await;
                }
                let remaining = workers.iter().map(|w| w.resident_bytes()).sum::<u64>();
                self.progressed = remaining < total
                    || workers.iter().map(|w| w.lru.total_bytes()).sum::<u64>() < logical;
                self.active = remaining > self.low;
            }
            if !self.active {
                for namespace in workers {
                    *namespace.small_capacity_eviction.lock().await = SmallCapacityPhase::Idle;
                }
            }
            return;
        }
        let bytes = worker.resident_bytes();
        let logical = worker.lru.total_bytes();
        if bytes <= self.low {
            self.active = false;
        } else if bytes >= self.high {
            self.active = true;
        }
        if self.active {
            worker.evict_background_batch(self.low).await;
            let remaining = worker.resident_bytes();
            self.progressed = remaining < bytes || worker.lru.total_bytes() < logical;
            self.active = remaining > self.low;
        }
        if !self.active {
            *worker.small_capacity_eviction.lock().await = SmallCapacityPhase::Idle;
        }
    }
}

impl WorkerRuntime {
    async fn evict_background_batch(&self, target: u64) {
        let mut phase = if self.store.small().is_some() {
            Some(self.small_capacity_eviction.lock().await)
        } else {
            None
        };
        let mut local_phase = SmallCapacityPhase::Idle;
        let phase = phase.as_deref_mut().unwrap_or(&mut local_phase);
        if self.resident_bytes() <= target {
            *phase = SmallCapacityPhase::Idle;
            return;
        }
        if *phase == SmallCapacityPhase::Idle {
            *phase = SmallCapacityPhase::Selecting;
        }
        let missing = self
            .resident_bytes()
            .saturating_sub(target)
            .saturating_sub(self.small_reclaim_credit().await);
        if missing == 0 {
            *phase = SmallCapacityPhase::Reclaiming;
        }
        if *phase == SmallCapacityPhase::Selecting {
            let policy_target = self.lru.total_bytes().saturating_sub(missing);
            let lru = self.lru.clone();
            let count = lru.len();
            let scan = self.page_gc_config.scan_batch_size;
            let deletes = self.page_gc_config.delete_batch_size;
            let units = tokio::task::spawn_blocking(move || {
                lru.candidates_to_fit_bounded(policy_target, &Default::default(), scan, deletes)
            })
            .await
            .expect("eviction scan panicked");
            let no_victims = units.is_empty();
            self.unlink_units_to_target(units, 1, target).await;
            let excess = self.resident_bytes().saturating_sub(target);
            if self.small_reclaim_credit().await >= excess
                || (no_victims && scan >= count.saturating_mul(2))
            {
                // Exhausted unpinned victims may yield only partial progress.
                *phase = SmallCapacityPhase::Reclaiming;
            } else {
                // Keep tombstones as credit across bounded selection batches.
                // Routine GC also defers relocation while this plan is active.
                return;
            }
        }
        if self.resident_bytes() <= target {
            *phase = SmallCapacityPhase::Idle;
            return;
        }
        if self.small_readers_cover_excess(target) {
            return;
        }
        match self
            .reclaim_small_to_target(target, self.background_budget.clone())
            .await
        {
            Ok(true) => {} // Continue relocation next tick, without selecting more victims.
            Ok(false) => *phase = SmallCapacityPhase::Idle,
            Err(error) => {
                tracing::warn!(%error, "small object background pressure reclamation failed")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::disk_unit;
    use crate::{
        BlockIndex, CacheUnit, InFlightLoads, PagedBlockStore, WholeBlockStore, WorkerMetrics,
    };
    use bytes::Bytes;
    use std::path::Path;
    use talon_core::{Backend, BackendStore, BlockId, ObjectId, ObjectStat, PageIndex, Version};

    struct UnusedOrigin;
    #[async_trait::async_trait]
    impl BackendStore for UnusedOrigin {
        async fn fetch_range(&self, _: &ObjectId, _: u64, _: u64) -> talon_core::Result<Bytes> {
            panic!("eviction must not fetch origin data")
        }
        async fn head(&self, _: &ObjectId) -> talon_core::Result<ObjectStat> {
            panic!("eviction must not stat origin data")
        }
    }

    fn runtime(root: &Path, paged: bool, capacity: u64) -> WorkerRuntime {
        let capacity = capacity.div_ceil(16) * disk_unit();
        let store = WholeBlockStore::open(root.join("whole")).unwrap();
        let index = Arc::new(BlockIndex::new());
        for entry in store.scan_allocated().unwrap() {
            index.commit_allocated(entry.meta, entry.allocated);
        }
        let r = WorkerRuntime::new(
            store,
            index,
            Arc::new(InFlightLoads::new()),
            Arc::new(UnusedOrigin),
            16,
            capacity,
            WorkerMetrics::new(capacity),
        );
        if paged {
            r.with_paged_store(PagedBlockStore::open(root.join("paged"), 16).unwrap())
        } else {
            r
        }
    }

    fn block(n: u64) -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "b", format!("key-{n}")),
            0,
            16,
            Version::new("v1"),
        )
    }

    fn unit(n: u64, paged: bool) -> CacheUnit {
        if paged {
            CacheUnit::Page(block(n), PageIndex(0))
        } else {
            CacheUnit::Whole(block(n))
        }
    }

    async fn fill(r: &WorkerRuntime, n: u64, paged: bool) {
        if paged {
            r.commit_fetched_page(&block(n), PageIndex(0), 16, Bytes::from_static(&[1; 16]))
                .await
                .unwrap();
        } else {
            r.commit_cached_block(&block(n), Bytes::from_static(&[1; 16]))
                .await
                .unwrap();
        }
    }

    async fn filled_namespaces(
        root: &Path,
        paged: bool,
        low_watermark: f64,
    ) -> (Arc<WorkerRuntime>, WorkerConfig, Vec<Arc<WorkerRuntime>>) {
        let config = WorkerConfig {
            small_object_threshold_bytes: 0,
            block_size: 16,
            capacity_bytes: 10 * disk_unit(),
            l2_page_size_bytes: if paged { 16 } else { 0 },
            page_tti_hours: 0,
            backend: Some("s3".into()),
            async_eviction_low_watermark: low_watermark,
            ..Default::default()
        };
        let lock = Arc::new(crate::page_access_store::CacheRootLock::acquire(root).unwrap());
        let worker = Arc::new(
            WorkerRuntime::open_namespaces(
                &config,
                lock,
                Arc::new(UnusedOrigin),
                WorkerMetrics::new(config.capacity_bytes),
            )
            .unwrap(),
        );
        let mut namespaces = Vec::new();
        for n in 0..9 {
            let namespace = worker
                .for_namespace(&talon_core::NamespaceId::named(format!("ns-{n}")))
                .await
                .unwrap();
            fill(&namespace, n, paged).await;
            namespaces.push(namespace);
        }
        (worker, config, namespaces)
    }

    #[tokio::test]
    async fn namespaces_stop_eviction_at_global_low_watermark() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let (worker, config, namespaces) = filled_namespaces(root.path(), paged, 0.8).await;
            let resident = || namespaces.iter().map(|w| w.resident_bytes()).sum::<u64>();
            assert_eq!(resident(), 9 * disk_unit());
            let mut cycle = EvictionCycle::new(config.capacity_bytes, &config);
            cycle.tick(&worker).await;
            assert_eq!(
                resident(),
                8 * disk_unit(),
                "only one of the nine cache units must be evicted"
            );
            assert_eq!(
                namespaces.iter().filter(|w| w.block_count() == 1).count(),
                8
            );
            assert!(!cycle.active);
            cycle.tick(&worker).await;
            assert_eq!(
                resident(),
                8 * disk_unit(),
                "a completed cycle must leave other namespaces intact"
            );
        }
    }

    #[tokio::test]
    async fn namespaces_resume_eviction_below_high_watermark_after_pins_release() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let (worker, config, namespaces) = filled_namespaces(root.path(), paged, 0.7).await;
            let resident = || namespaces.iter().map(|w| w.resident_bytes()).sum::<u64>();
            let pins: Vec<_> = namespaces[..8]
                .iter()
                .enumerate()
                .map(|(n, w)| w.lru.pin_guard(unit(n as u64, paged)).unwrap())
                .collect();
            let mut cycle = EvictionCycle::new(config.capacity_bytes, &config);
            cycle.tick(&worker).await;
            assert_eq!(resident(), 8 * disk_unit());
            assert!(
                cycle.active,
                "pins prevent reaching the global low watermark of seven units"
            );
            assert_eq!(namespaces[8].resident_bytes(), 0);
            assert_eq!(
                cycle.next_delay(Duration::from_secs(60)),
                Some(Duration::ZERO)
            );
            cycle.tick(&worker).await;
            assert_eq!(
                cycle.next_delay(Duration::from_secs(60)),
                Some(Duration::from_secs(1))
            );
            drop(pins);
            cycle.tick(&worker).await;
            assert_eq!(
                resident(),
                7 * disk_unit(),
                "resume below the high watermark and reclaim only one more unit"
            );
            assert!(!cycle.active);
        }
    }

    #[tokio::test]
    async fn scan_and_delete_budgets_bound_each_eviction_batch() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut r = runtime(root.path(), paged, 160);
            r.page_gc_config.scan_batch_size = 1;
            r.page_gc_config.delete_batch_size = 1;
            for n in 0..9 {
                fill(&r, n, paged).await;
            }
            for n in 0..9 {
                r.lru.touch(&unit(n, paged));
            }
            let config = WorkerConfig {
                async_eviction_low_watermark: 0.2,
                ..Default::default()
            };
            let mut cycle = EvictionCycle::new(10 * disk_unit(), &config);
            for _ in 0..9 {
                cycle.tick(&r).await;
                assert_eq!(
                    r.resident_bytes(),
                    9 * disk_unit(),
                    "one scan step only clears one reference bit"
                );
            }
            for remaining in (2..9).rev() {
                cycle.tick(&r).await;
                assert_eq!(r.resident_bytes(), remaining * disk_unit());
            }
            assert!(!cycle.active);
        }
    }

    #[tokio::test]
    async fn both_eviction_forms_obey_shared_io_and_delete_rates() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut r = runtime(root.path(), paged, 48);
            let config = WorkerConfig {
                async_eviction_low_watermark: 0.4,
                background_io_concurrency: 1,
                background_delete_max_per_sec: 25,
                ..Default::default()
            };
            let budget = crate::background::BackgroundBudget::new(&config);
            r.background_budget = Some(budget.clone());
            for n in 0..3 {
                fill(&r, n, paged).await;
            }
            let slot = budget.io().await;
            let r = Arc::new(r);
            let worker = r.clone();
            let started = std::time::Instant::now();
            let task = tokio::spawn(async move {
                EvictionCycle::new(3 * disk_unit(), &config)
                    .tick(&worker)
                    .await;
            });
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(!task.is_finished());
            assert_eq!(
                r.resident_bytes(),
                3 * disk_unit(),
                "no unlink while the shared disk slot is occupied"
            );
            drop(slot);
            tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(r.resident_bytes(), disk_unit());
            assert!(started.elapsed() >= Duration::from_millis(80));
        }
    }

    #[tokio::test]
    async fn cancelled_caller_keeps_the_disk_slot_until_owned_unlink_finishes() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut r = runtime(root.path(), paged, 32);
            let config = WorkerConfig {
                background_io_concurrency: 1,
                background_delete_max_per_sec: 0,
                ..Default::default()
            };
            let budget = crate::background::BackgroundBudget::new(&config);
            r.background_budget = Some(budget.clone());
            fill(&r, 0, paged).await;
            fill(&r, 1, paged).await;
            let state = r.page_lifecycle.block(&block(0));
            let gate = state.gate.lock().await;
            let r = Arc::new(r);
            let worker = r.clone();
            let task = tokio::spawn(async move {
                EvictionCycle::new(2 * disk_unit(), &config)
                    .tick(&worker)
                    .await;
            });
            tokio::time::timeout(Duration::from_secs(2), async {
                while budget.available_io() != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(tokio::time::timeout(Duration::from_millis(20), budget.io())
                .await
                .is_err());
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert!(tokio::time::timeout(Duration::from_millis(20), budget.io())
                .await
                .is_err());
            assert_eq!(r.resident_bytes(), 2 * disk_unit());
            drop(gate);
            r.drain_page_mutations().await;
            assert_eq!(r.resident_bytes(), disk_unit());
            let _slot = tokio::time::timeout(Duration::from_secs(1), budget.io())
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn high_watermark_starts_and_low_watermark_stops_both_cache_forms() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let r = runtime(root.path(), paged, 160);
            let mut cycle = EvictionCycle::new(
                10 * disk_unit(),
                &WorkerConfig {
                    ownership_gc_enabled: false,
                    ..Default::default()
                },
            );
            for n in 0..8 {
                fill(&r, n, paged).await;
            }
            cycle.tick(&r).await;
            assert_eq!(r.resident_bytes(), 8 * disk_unit());
            fill(&r, 8, paged).await;
            assert_eq!(
                r.resident_bytes(),
                9 * disk_unit(),
                "admission below capacity must not wait for watermark reclamation"
            );
            cycle.tick(&r).await;
            assert_eq!(r.resident_bytes(), 8 * disk_unit());
            assert_eq!(r.lru.total_bytes(), 8 * disk_unit());
            assert!(!cycle.active);
            cycle.tick(&r).await;
            assert_eq!(
                r.resident_bytes(),
                8 * disk_unit(),
                "no repeated eviction at the low watermark"
            );
        }
    }

    #[tokio::test]
    async fn pins_pause_a_cycle_and_it_resumes_below_high_watermark() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let r = runtime(root.path(), paged, 160);
            for n in 0..9 {
                fill(&r, n, paged).await;
            }
            let pins: Vec<_> = (0..8)
                .map(|n| r.lru.pin_guard(unit(n, paged)).unwrap())
                .collect();
            let config = WorkerConfig {
                async_eviction_low_watermark: 0.7,
                ..Default::default()
            };
            let mut cycle = EvictionCycle::new(10 * disk_unit(), &config);
            tokio::time::timeout(Duration::from_secs(2), cycle.tick(&r))
                .await
                .unwrap();
            assert_eq!(r.resident_bytes(), 8 * disk_unit());
            assert!(cycle.active, "stay active between high=144 and low=112");
            drop(pins);
            cycle.tick(&r).await;
            assert_eq!(r.resident_bytes(), 7 * disk_unit());
            assert!(!cycle.active);
        }
    }

    #[tokio::test]
    async fn failed_unlink_retains_accounting_and_cycle_retries() {
        let root = tempfile::tempdir().unwrap();
        let r = runtime(root.path(), true, 32);
        fill(&r, 0, true).await;
        fill(&r, 1, true).await;
        let path = r.paged.as_ref().unwrap().dir_for(&block(0)).join("0.page");
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        let config = WorkerConfig {
            async_eviction_low_watermark: 0.25,
            ..Default::default()
        };
        let mut cycle = EvictionCycle::new(2 * disk_unit(), &config);
        tokio::time::timeout(Duration::from_secs(2), cycle.tick(&r))
            .await
            .unwrap();
        assert_eq!(r.resident_bytes(), disk_unit());
        assert_eq!(r.lru.total_bytes(), disk_unit());
        assert!(cycle.active);
        std::fs::remove_dir(&path).unwrap();
        std::fs::write(&path, [1; 16]).unwrap();
        cycle.tick(&r).await;
        assert_eq!(r.resident_bytes(), 0);
        assert!(!cycle.active);
    }

    #[tokio::test]
    async fn backlog_continues_without_waiting_for_idle_interval() {
        let root = tempfile::tempdir().unwrap();
        let r = Arc::new(runtime(root.path(), false, 160));
        for n in 0..9 {
            fill(&r, n, false).await;
        }
        let config = WorkerConfig {
            async_eviction_enabled: true,
            async_eviction_low_watermark: 0.2,
            background_delete_batch_size: 1,
            background_task_concurrency: 1,
            ownership_gc_enabled: false,
            stale_gc_enabled: false,
            ..Default::default()
        };
        let service = crate::runtime::WorkerBackground::start(r.clone(), &config);
        tokio::time::timeout(Duration::from_secs(3), async {
            while r.resident_bytes() > 2 * disk_unit() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("backlog must not sleep a minute between deletions");
        service.shutdown().await;
    }

    #[tokio::test]
    async fn recovered_cache_is_reclaimed_by_default_and_service_stops() {
        let root = tempfile::tempdir().unwrap();
        let r = runtime(root.path(), false, 160);
        for n in 0..9 {
            fill(&r, n, false).await;
        }
        drop(r);
        let r = Arc::new(runtime(root.path(), false, 160));
        let config = WorkerConfig {
            ownership_gc_enabled: false,
            ..Default::default()
        };
        let service = crate::runtime::WorkerBackground::start(r.clone(), &config);
        tokio::time::timeout(Duration::from_secs(2), async {
            while r.resident_bytes() > 8 * disk_unit() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // The periodic tick also handles new occupancy without a request driving eviction.
        fill(&r, 10, false).await;
        tokio::task::yield_now().await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(59)).await;
        tokio::time::resume();
        tokio::task::yield_now().await;
        assert_eq!(
            r.resident_bytes(),
            9 * disk_unit(),
            "configured interval uses minutes"
        );
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::time::resume();
        tokio::time::timeout(Duration::from_secs(2), async {
            while r.resident_bytes() > 8 * disk_unit() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        service.shutdown().await;
        r.drain_page_mutations().await;
        fill(&r, 13, false).await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(61)).await;
        tokio::time::resume();
        tokio::task::yield_now().await;
        assert_eq!(r.resident_bytes(), 9 * disk_unit());
        let disabled = crate::runtime::WorkerBackground::start(
            r.clone(),
            &WorkerConfig {
                async_eviction_enabled: false,
                ownership_gc_enabled: false,
                ..Default::default()
            },
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(r.resident_bytes(), 9 * disk_unit());
        disabled.shutdown().await;
        // The foreground fallback still enforces the hard cap.
        fill(&r, 11, false).await;
        fill(&r, 12, false).await;
        assert_eq!(r.resident_bytes(), 10 * disk_unit());
    }

    #[tokio::test]
    async fn shutdown_waits_for_owned_unlink() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let r = Arc::new(runtime(root.path(), paged, 32));
            fill(&r, 0, paged).await;
            fill(&r, 1, paged).await;
            let state = r.page_lifecycle.block(&block(0));
            let gate = state.gate.lock().await;
            let config = WorkerConfig {
                async_eviction_enabled: true,
                background_task_concurrency: 4,
                ownership_gc_enabled: false,
                ..Default::default()
            };
            let service = crate::runtime::WorkerBackground::start(r.clone(), &config);
            tokio::time::timeout(Duration::from_secs(2), async {
                while r.page_mutations.active_count() == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let shutdown = tokio::spawn(service.shutdown());
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(!shutdown.is_finished(), "shutdown retains owned disk work");
            assert_eq!(
                r.resident_bytes(),
                2 * disk_unit(),
                "blocked unlink remains accounted"
            );
            drop(gate);
            tokio::time::timeout(Duration::from_secs(2), shutdown)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(r.resident_bytes(), disk_unit());
            assert_eq!(r.lru.total_bytes(), disk_unit());
        }
    }

    #[tokio::test]
    async fn stale_background_candidates_recheck_the_low_target() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let r = runtime(root.path(), paged, 64);
            for n in 0..3 {
                fill(&r, n, paged).await;
            }
            let pending = r
                .lru
                .candidates_to_fit(2 * disk_unit(), &Default::default());
            assert_eq!(pending.len(), 1);
            // Independent GC has already brought usage to the background target.
            r.unlink_units(r.lru.block_candidates(&block(2)), 2).await;
            r.unlink_units_to_target(pending, 1, 2 * disk_unit()).await;
            assert_eq!(r.resident_bytes(), 2 * disk_unit());
        }
    }
}
