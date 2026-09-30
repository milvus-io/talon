//! Background capacity reclamation with high/low watermark hysteresis.
use super::WorkerRuntime;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;
use talon_core::WorkerConfig;

pub(super) struct EvictionCycle {
    high: u64,
    low: u64,
    active: bool,
}

impl EvictionCycle {
    pub(super) fn new(capacity: u64, config: &WorkerConfig) -> Self {
        Self {
            high: ((capacity as f64 * config.async_eviction_high_watermark).ceil() as u64).max(1),
            low: (capacity as f64 * config.async_eviction_low_watermark).floor() as u64,
            active: false,
        }
    }

    pub(super) async fn tick(&mut self, worker: &WorkerRuntime) {
        let bytes = worker.lru.total_bytes();
        if bytes <= self.low {
            self.active = false;
        } else if bytes >= self.high {
            self.active = true;
        }
        if self.active {
            // One bounded batch. If readers, budgets, or I/O prevent reaching the target,
            // retain the cycle across ticks, including below the high watermark.
            let lru = worker.lru.clone();
            let target = self.low;
            let scan = worker.page_gc_config.scan_batch_size;
            let deletes = worker.page_gc_config.delete_batch_size;
            let units = tokio::task::spawn_blocking(move || {
                lru.candidates_to_fit_bounded(target, &Default::default(), scan, deletes)
            })
            .await
            .expect("eviction scan panicked");
            worker.unlink_units_to_target(units, 1, target).await;
            self.active = worker.lru.total_bytes() > self.low;
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
        let store = WholeBlockStore::open(root.join("whole")).unwrap();
        let index = Arc::new(BlockIndex::new());
        for meta in store.scan().unwrap() {
            index.commit(meta);
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
            let mut cycle = EvictionCycle::new(160, &config);
            for _ in 0..9 {
                cycle.tick(&r).await;
                assert_eq!(
                    r.resident_bytes(),
                    144,
                    "one scan step only clears one reference bit"
                );
            }
            for remaining in (2..9).rev() {
                cycle.tick(&r).await;
                assert_eq!(r.resident_bytes(), remaining * 16);
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
                EvictionCycle::new(48, &config).tick(&worker).await;
            });
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(!task.is_finished());
            assert_eq!(
                r.resident_bytes(),
                48,
                "no unlink while the shared disk slot is occupied"
            );
            drop(slot);
            tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(r.resident_bytes(), 16);
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
                EvictionCycle::new(32, &config).tick(&worker).await;
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
            assert_eq!(r.resident_bytes(), 32);
            drop(gate);
            r.drain_page_mutations().await;
            assert_eq!(r.resident_bytes(), 16);
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
            let mut cycle = EvictionCycle::new(160, &WorkerConfig::default());
            for n in 0..8 {
                fill(&r, n, paged).await;
            }
            cycle.tick(&r).await;
            assert_eq!(r.resident_bytes(), 128);
            fill(&r, 8, paged).await;
            assert_eq!(
                r.resident_bytes(),
                144,
                "admission below capacity must not wait for watermark reclamation"
            );
            cycle.tick(&r).await;
            assert_eq!(r.resident_bytes(), 128);
            assert_eq!(r.lru.total_bytes(), 128);
            assert!(!cycle.active);
            cycle.tick(&r).await;
            assert_eq!(
                r.resident_bytes(),
                128,
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
            let mut cycle = EvictionCycle::new(160, &config);
            tokio::time::timeout(Duration::from_secs(2), cycle.tick(&r))
                .await
                .unwrap();
            assert_eq!(r.resident_bytes(), 128);
            assert!(cycle.active, "stay active between high=144 and low=112");
            drop(pins);
            cycle.tick(&r).await;
            assert_eq!(r.resident_bytes(), 112);
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
        let mut cycle = EvictionCycle::new(32, &config);
        tokio::time::timeout(Duration::from_secs(2), cycle.tick(&r))
            .await
            .unwrap();
        assert_eq!(r.resident_bytes(), 16);
        assert_eq!(r.lru.total_bytes(), 16);
        assert!(cycle.active);
        std::fs::remove_dir(&path).unwrap();
        std::fs::write(&path, [1; 16]).unwrap();
        cycle.tick(&r).await;
        assert_eq!(r.resident_bytes(), 0);
        assert!(!cycle.active);
    }

    #[tokio::test]
    async fn recovered_cache_is_reclaimed_without_requests_and_service_stops() {
        let root = tempfile::tempdir().unwrap();
        let r = runtime(root.path(), false, 160);
        for n in 0..9 {
            fill(&r, n, false).await;
        }
        drop(r);
        let r = Arc::new(runtime(root.path(), false, 160));
        let config = WorkerConfig {
            async_eviction_enabled: true,
            async_eviction_check_interval_secs: 1,
            ..Default::default()
        };
        let started = tokio::time::Instant::now();
        let service = crate::runtime::WorkerBackground::start(r.clone(), &config);
        tokio::time::timeout(Duration::from_secs(2), async {
            while r.resident_bytes() > 128 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // The periodic tick also handles new occupancy without a request driving eviction.
        fill(&r, 10, false).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while r.resident_bytes() > 128 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            started.elapsed() >= Duration::from_secs(1),
            "configured interval uses seconds"
        );
        service.shutdown().await;
        r.drain_page_mutations().await;
        fill(&r, 13, false).await;
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(r.resident_bytes(), 144);
        let disabled = crate::runtime::WorkerBackground::start(r.clone(), &WorkerConfig::default());
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(r.resident_bytes(), 144);
        disabled.shutdown().await;
        // The foreground fallback still enforces the hard cap.
        fill(&r, 11, false).await;
        fill(&r, 12, false).await;
        assert_eq!(r.resident_bytes(), 160);
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
            assert_eq!(r.resident_bytes(), 32, "blocked unlink remains accounted");
            drop(gate);
            tokio::time::timeout(Duration::from_secs(2), shutdown)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(r.resident_bytes(), 16);
            assert_eq!(r.lru.total_bytes(), 16);
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
            let pending = r.lru.candidates_to_fit(32, &Default::default());
            assert_eq!(pending.len(), 1);
            // Independent GC has already brought usage to the background target.
            r.unlink_units(r.lru.block_candidates(&block(2)), 2).await;
            r.unlink_units_to_target(pending, 1, 32).await;
            assert_eq!(r.resident_bytes(), 32);
        }
    }
}
