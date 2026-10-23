//! Worker maintenance registrations; scheduling policy lives in `background`.
use super::{async_eviction::EvictionCycle, namespaces::CacheIdentity, WorkerRuntime};
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
                .namespace_runtimes()
                .iter()
                .map(|w| w.page_gc_config.interval_ms)
                .chain([gc.interval_ms, global_gc.interval_ms])
                .min()
                .unwrap(),
        );
        let w = worker.clone();
        let gc_schedule = Arc::new(tokio::sync::Mutex::new(SliceSchedule::default()));
        scheduler.register_adaptive("tti_gc", period, move || {
            let w = w.clone();
            let schedule = gc_schedule.clone();
            async move {
                let mut schedule = schedule.lock().await;
                for namespace in w.namespace_runtimes() {
                    let name = &namespace.cache_identity;
                    if schedule.ready(name) {
                        let complete = namespace.gc_next_batch().await.completed_scan;
                        schedule.defer(
                            name,
                            if complete {
                                Duration::from_millis(namespace.page_gc_config.interval_ms)
                            } else {
                                Duration::ZERO
                            },
                        );
                    }
                }
                Some(schedule.delay(period))
            }
        });
        let w = worker.clone();
        let cleanup_period = Duration::from_secs(config.file_cleanup_interval_hours * 3600);
        let cleanup_schedule = Arc::new(tokio::sync::Mutex::new(SliceSchedule::default()));
        scheduler.register_adaptive("file_cleanup", cleanup_period, move || {
            let w = w.clone();
            let schedule = cleanup_schedule.clone();
            async move {
                let mut schedule = schedule.lock().await;
                for namespace in w.namespace_runtimes() {
                    let name = &namespace.cache_identity;
                    if schedule.ready(name) {
                        let complete = namespace
                            .cleanup_page_files_batch(4096, 64)
                            .await
                            .completed_scan;
                        schedule.defer(
                            name,
                            if complete {
                                cleanup_period
                            } else {
                                Duration::ZERO
                            },
                        );
                    }
                }
                Some(schedule.delay(cleanup_period))
            }
        });
        if worker
            .namespace_runtimes()
            .iter()
            .any(|w| w.page_gc_config.tti_ms > 0)
            || config.page_tti_hours > 0
        {
            let w = worker.clone();
            let period = Duration::from_millis(
                (worker
                    .namespace_runtimes()
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
                    for namespace in w.namespace_runtimes() {
                        if namespace.page_gc_config.tti_ms > 0
                            && due(
                                &last,
                                &namespace,
                                (namespace.page_gc_config.checkpoint_interval_ms
                                    / crate::page_lifecycle::SHARDS as u64)
                                    .max(1),
                            )
                        {
                            namespace.checkpoint_next_shard().await;
                        }
                    }
                }
            });
        }
        if config.stale_gc_enabled {
            let w = worker.clone();
            let states = Arc::new(tokio::sync::Mutex::new((
                SliceSchedule::default(),
                HashMap::new(),
            )));
            let config = config.clone();
            let heads = Arc::new(crate::background::Rate::new(
                config.stale_gc_head_max_per_sec,
            ));
            scheduler.register_adaptive("stale_gc", Duration::from_secs(3600), move || {
                let w = w.clone();
                let states = states.clone();
                let config = config.clone();
                let heads = heads.clone();
                async move {
                    let mut states = states.lock().await;
                    let (schedule, collectors) = &mut *states;
                    for namespace in w.namespace_runtimes() {
                        let name = &namespace.cache_identity;
                        if schedule.ready(name) {
                            let gc = collectors.entry(name.to_owned()).or_insert_with(|| {
                                super::stale_gc::StaleGc::with_head_rate(&config, heads.clone())
                            });
                            let delay = gc
                                .run_slice(&namespace)
                                .await
                                .unwrap_or(Duration::from_secs(3600));
                            schedule.defer(name, delay);
                        }
                    }
                    Some(schedule.delay(Duration::from_secs(3600)))
                }
            });
        }
        if config.ownership_gc_enabled {
            let gc = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
            let config = Arc::new(config.clone());
            let w = worker.clone();
            scheduler.register_triggered(
                "ownership_gc",
                worker.membership_changed.subscribe(),
                move || {
                    let w = w.clone();
                    let gc = gc.clone();
                    let config = config.clone();
                    async move {
                        let mut states = gc.lock().await;
                        let mut next = None;
                        for namespace in w.namespace_runtimes() {
                            let gc = states
                                .entry(namespace.cache_identity.clone())
                                .or_insert_with(|| super::ownership_gc::OwnershipGc::new(&config));
                            gc.run_slice(&namespace).await;
                            if let Some(delay) = gc.next_delay(&namespace) {
                                next = Some(next.map_or(delay, |old: Duration| old.min(delay)));
                            }
                        }
                        next
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
            let period = Duration::from_secs(config.async_eviction_check_interval_minutes * 60);
            scheduler.register_adaptive("cache_eviction", period, move || {
                let w = w.clone();
                let cycle = cycle.clone();
                async move {
                    let mut cycle = cycle.lock().await;
                    cycle.tick(&w).await;
                    cycle.next_delay(period)
                }
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
        self.worker.stop_background_heads();
    }

    /// Stop admission, finish active maintenance and owned mutations, then flush access metadata
    /// using the same disk budget. External process shutdown deadlines still apply.
    pub async fn shutdown(self) {
        self.begin_shutdown();
        self.scheduler.shutdown().await;
        self.worker.drain_namespace_initializers().await;
        for namespace in self.worker.namespace_runtimes() {
            namespace.drain_page_mutations().await;
            namespace.checkpoint_access_times().await;
        }
    }
}

fn due(last: &Mutex<HashMap<CacheIdentity, Instant>>, worker: &WorkerRuntime, millis: u64) -> bool {
    let mut last = last.lock().unwrap();
    let now = Instant::now();
    let name = &worker.cache_identity;
    if last
        .get(name)
        .is_some_and(|previous| now.duration_since(*previous) < Duration::from_millis(millis))
    {
        return false;
    }
    last.insert(name.to_owned(), now);
    true
}

// A namespace that finishes early must stay idle while another continues scanning.
#[derive(Default)]
struct SliceSchedule {
    next: HashMap<CacheIdentity, tokio::time::Instant>,
}
impl SliceSchedule {
    fn ready(&self, name: &CacheIdentity) -> bool {
        self.next
            .get(name)
            .map_or(true, |next| *next <= tokio::time::Instant::now())
    }
    fn defer(&mut self, name: &CacheIdentity, delay: Duration) {
        self.next
            .insert(name.to_owned(), tokio::time::Instant::now() + delay);
    }
    fn delay(&self, idle: Duration) -> Duration {
        let now = tokio::time::Instant::now();
        self.next
            .values()
            .map(|next| next.saturating_duration_since(now))
            .fold(idle, Duration::min)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn namespace_slices_continue_without_rescanning_completed_namespaces() {
        let mut schedule = SliceSchedule::default();
        let idle = Duration::from_secs(3600);
        let new = CacheIdentity::new(&talon_core::NamespaceId::named("new"), None);
        let finished = CacheIdentity::new(&talon_core::NamespaceId::named("finished"), None);
        let scanning = CacheIdentity::new(&talon_core::NamespaceId::named("scanning"), None);
        let scoped = CacheIdentity::new(
            &talon_core::NamespaceId::named("finished"),
            Some(format!("v1-{}", "a".repeat(64)).parse().unwrap()),
        );
        assert!(schedule.ready(&new));
        schedule.defer(&finished, idle);
        schedule.defer(&scanning, Duration::ZERO);
        assert!(!schedule.ready(&finished));
        assert!(
            schedule.ready(&scoped),
            "one credential scope cannot defer another"
        );
        assert!(schedule.ready(&scanning));
        assert_eq!(schedule.delay(idle), Duration::ZERO);
        schedule.defer(&scanning, idle);
        assert_eq!(schedule.delay(idle), idle);
        tokio::time::advance(idle).await;
        assert!(schedule.ready(&finished));
        assert!(schedule.ready(&scanning));
        assert_eq!(schedule.delay(idle), Duration::ZERO);
    }
}
