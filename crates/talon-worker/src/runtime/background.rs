//! Worker maintenance registrations; scheduling policy lives in `background`.
use super::{async_eviction::EvictionCycle, WorkerRuntime};
use crate::background::{BackgroundBudget, BackgroundHandle, BackgroundScheduler};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use talon_core::WorkerConfig;

/// Unified worker GC, checkpoint, disk cleanup, and capacity eviction lifecycle.
pub struct WorkerBackground {
    scheduler: BackgroundHandle,
    worker: Arc<WorkerRuntime>,
}
impl WorkerBackground {
    /// Start maintenance using validated configuration.
    /// Ownership cleanup requires the resolved persistent identity in `config.node_id`.
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
        let global_gc = crate::page_gc::PageGcConfig::from(config);
        let period = Duration::from_millis(
            worker
                .tenant_runtimes()
                .iter()
                .map(|w| w.page_gc_config.interval_ms)
                .chain([gc.interval_ms, global_gc.interval_ms])
                .min()
                .unwrap(),
        );
        let w = worker.clone();
        let last_gc = Arc::new(Mutex::new(HashMap::new()));
        scheduler.register("tti_gc", period, move || {
            let w = w.clone();
            let last = last_gc.clone();
            async move {
                for tenant in w.tenant_runtimes() {
                    if due(&last, &tenant, tenant.page_gc_config.interval_ms) {
                        tenant.gc_once().await;
                    }
                }
            }
        });
        let w = worker.clone();
        scheduler.register(
            "file_cleanup",
            Duration::from_secs(config.file_cleanup_interval_hours * 3600),
            move || {
                let w = w.clone();
                async move {
                    for tenant in w.tenant_runtimes() {
                        tenant.cleanup_page_files_once().await;
                    }
                }
            },
        );
        if worker
            .tenant_runtimes()
            .iter()
            .any(|w| w.page_gc_config.tti_ms > 0)
            || config.page_tti_hours > 0
        {
            let w = worker.clone();
            let period = Duration::from_millis(
                (worker
                    .tenant_runtimes()
                    .iter()
                    .map(|w| w.page_gc_config.checkpoint_interval_ms)
                    .chain([gc.checkpoint_interval_ms, global_gc.checkpoint_interval_ms])
                    .min()
                    .unwrap()
                    / crate::page_lifecycle::SHARDS as u64)
                    .max(1),
            );
            let last_checkpoint = Arc::new(Mutex::new(HashMap::new()));
            scheduler.register("access_checkpoint", period, move || {
                let w = w.clone();
                let last = last_checkpoint.clone();
                async move {
                    for tenant in w.tenant_runtimes() {
                        if tenant.page_gc_config.tti_ms > 0
                            && due(
                                &last,
                                &tenant,
                                (tenant.page_gc_config.checkpoint_interval_ms
                                    / crate::page_lifecycle::SHARDS as u64)
                                    .max(1),
                            )
                        {
                            tenant.checkpoint_next_shard().await;
                        }
                    }
                }
            });
        }
        if config.stale_gc_enabled {
            let w = worker.clone();
            scheduler.register("stale_gc", Duration::from_secs(3600), move || {
                let w = w.clone();
                async move {
                    for tenant in w.tenant_runtimes() {
                        super::stale_gc::StaleGc::run_round(&tenant).await;
                    }
                }
            });
        }
        if config.ownership_gc_enabled {
            let gc = Arc::new(tokio::sync::Mutex::new(
                super::ownership_gc::OwnershipGc::new(config),
            ));
            let w = worker.clone();
            scheduler.register_triggered(
                "ownership_gc",
                worker.membership_changed.subscribe(),
                move || {
                    let w = w.clone();
                    let gc = gc.clone();
                    async move {
                        let mut gc = gc.lock().await;
                        gc.run_round(&w).await;
                        gc.next_delay(&w)
                    }
                },
            );
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
        self.worker.drain_tenant_initializers().await;
        for tenant in self.worker.tenant_runtimes() {
            tenant.drain_page_mutations().await;
            tenant.checkpoint_access_times().await;
        }
    }
}

fn due(last: &Mutex<HashMap<String, Instant>>, worker: &WorkerRuntime, millis: u64) -> bool {
    let mut last = last.lock().unwrap();
    let now = Instant::now();
    let name = worker.tenant.effective_name();
    if last
        .get(name)
        .is_some_and(|previous| now.duration_since(*previous) < Duration::from_millis(millis))
    {
        return false;
    }
    last.insert(name.to_owned(), now);
    true
}
