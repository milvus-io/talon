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
        Self::start_inner(worker, config, None)
    }

    /// Include ownership cleanup on the same scheduler and disk budget.
    pub fn start_with_ownership(
        worker: Arc<WorkerRuntime>,
        config: &WorkerConfig,
        node: talon_core::NodeId,
        channel: Option<talon_transport::control_tls::ControlTlsChannel>,
    ) -> Self {
        Self::start_inner(worker, config, Some((node, channel)))
    }

    fn start_inner(
        worker: Arc<WorkerRuntime>,
        config: &WorkerConfig,
        ownership: Option<(
            talon_core::NodeId,
            Option<talon_transport::control_tls::ControlTlsChannel>,
        )>,
    ) -> Self {
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
        scheduler.register("tti_gc", period, move || {
            let w = w.clone();
            async move {
                w.gc_once().await;
            }
        });
        let w = worker.clone();
        scheduler.register(
            "file_cleanup",
            Duration::from_secs(config.file_cleanup_interval_hours * 3600),
            move || {
                let w = w.clone();
                async move {
                    w.cleanup_page_files_once().await;
                }
            },
        );
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
                Duration::from_secs(config.async_eviction_check_interval_minutes * 60),
                move || {
                    let w = w.clone();
                    let cycle = cycle.clone();
                    async move {
                        cycle.lock().await.tick(&w).await;
                    }
                },
            );
        }
        let ownership = ownership
            .filter(|_| config.ownership_gc_interval_secs > 0)
            .map(|(node, channel)| {
                super::ownership_gc::OwnershipService::new(node, &worker, config, channel)
            });
        if config.stale_gc_enabled || ownership.is_some() {
            let service = Arc::new(tokio::sync::Mutex::new(
                super::stale_gc::StaleGcService::new(config.stale_gc_enabled, ownership),
            ));
            let w = worker.clone();
            scheduler.register_with_delay("stale_gc", Duration::from_secs(3600), move || {
                let w = w.clone();
                let service = service.clone();
                async move { service.lock().await.run_batch(&w).await }
            });
        }
        Self {
            scheduler: scheduler.start(),
            worker,
        }
    }
    /// Stop scheduling maintenance as soon as the worker starts draining requests.
    pub fn begin_shutdown(&self) {
        self.scheduler.begin_shutdown();
    }

    /// Stop admission, finish active maintenance and owned mutations, then flush access metadata
    /// using the same disk budget. External process shutdown deadlines still apply.
    pub async fn shutdown(self) {
        self.scheduler.shutdown().await;
        self.worker.drain_page_mutations().await;
        self.worker.checkpoint_access_times().await;
    }
}
