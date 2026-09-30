//! Worker maintenance registrations; scheduling policy lives in `background`.
use super::{async_eviction::EvictionCycle, WorkerRuntime};
use crate::background::{BackgroundBudget, BackgroundHandle, BackgroundScheduler};
use std::{sync::Arc, time::Duration};
use talon_core::WorkerConfig;

/// Unified worker GC, checkpoint, disk cleanup, and capacity eviction lifecycle.
pub struct WorkerBackground {
    scheduler: BackgroundHandle,
    worker: Arc<WorkerRuntime>,
}
impl WorkerBackground {
    /// Start maintenance using validated configuration and one shared resource budget.
    pub fn start(worker: Arc<WorkerRuntime>, config: &WorkerConfig) -> Self {
        let mut background = (*worker).clone();
        background.background_budget = Some(BackgroundBudget::new(config));
        background.page_gc_config.scan_batch_size = background
            .page_gc_config
            .scan_batch_size
            .min(config.background_scan_batch_size);
        background.page_gc_config.delete_batch_size = background
            .page_gc_config
            .delete_batch_size
            .min(config.background_delete_batch_size);
        let worker = Arc::new(background);
        let mut scheduler = BackgroundScheduler::new(config.background_task_concurrency)
            .with_metrics(worker.metrics.registry.clone());
        let gc = &worker.page_gc_config;
        let period = Duration::from_millis(gc.interval_ms);
        let w = worker.clone();
        scheduler.register("page_gc", period, move || {
            let w = w.clone();
            async move {
                w.gc_once().await;
            }
        });
        let w = worker.clone();
        scheduler.register("file_cleanup", period, move || {
            let w = w.clone();
            async move {
                w.cleanup_page_files_once().await;
            }
        });
        if gc.tti_ms > 0 {
            let w = worker.clone();
            let period = Duration::from_millis(
                (gc.checkpoint_interval_ms / crate::page_lifecycle::SHARDS as u64).max(1),
            );
            scheduler.register("access_checkpoint", period, move || {
                let w = w.clone();
                async move {
                    w.checkpoint_next_shard().await;
                }
            });
        }
        if config.async_eviction_enabled && worker.capacity_bytes > 0 {
            let cycle = Arc::new(tokio::sync::Mutex::new(EvictionCycle::new(
                worker.capacity_bytes,
                config,
            )));
            let w = worker.clone();
            scheduler.register(
                "cache_eviction",
                Duration::from_secs(config.async_eviction_check_interval_secs),
                move || {
                    let w = w.clone();
                    let cycle = cycle.clone();
                    async move {
                        cycle.lock().await.tick(&w).await;
                    }
                },
            );
        }
        Self {
            scheduler: scheduler.start(),
            worker,
        }
    }
    /// Stop admission, finish batches and owned mutations, then flush access metadata
    /// using the same disk budget. External process shutdown deadlines still apply.
    pub async fn shutdown(self) {
        self.scheduler.shutdown().await;
        self.worker.drain_page_mutations().await;
        self.worker.checkpoint_access_times().await;
    }
}
