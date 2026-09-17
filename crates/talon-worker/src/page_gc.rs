//! Worker-local idle collection and checkpoint scheduling, independent of registration.
use crate::WorkerRuntime;
use std::hash::{BuildHasher, Hasher};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use talon_core::{Counter, Gauge, Histogram, Metrics, WorkerConfig};
use tokio::sync::Notify;

#[derive(Clone, Debug)]
pub struct PageGcConfig {
    pub ttl_ms: u64,
    pub checkpoint_interval_ms: u64,
    pub interval_ms: u64,
    pub scan_batch_size: usize,
    pub delete_batch_size: usize,
    pub io_concurrency: usize,
}
impl From<&WorkerConfig> for PageGcConfig {
    fn from(c: &WorkerConfig) -> Self {
        Self {
            ttl_ms: c.page_ttl_ms,
            checkpoint_interval_ms: c.page_access_checkpoint_interval_ms,
            interval_ms: c.page_gc_interval_ms,
            scan_batch_size: c.page_gc_scan_batch_size,
            delete_batch_size: c.page_gc_delete_batch_size,
            io_concurrency: c.page_gc_io_concurrency,
        }
    }
}
impl Default for PageGcConfig {
    fn default() -> Self {
        Self::from(&WorkerConfig::default())
    }
}

#[derive(Default, Debug)]
pub struct GcReport {
    pub checked: usize,
    pub reclaimed: usize,
    pub bytes: u64,
    pub completed_scan: bool,
}
#[derive(Default, Debug)]
pub struct CheckpointReport {
    pub blocks: usize,
    pub failures: usize,
    pub bytes: u64,
}

/// Bounded disk cleanup progress. Pending and duration describe a completed pass.
#[derive(Default, Debug)]
pub struct CleanupReport {
    pub checked: usize,
    pub attempted: usize,
    pub removed: usize,
    pub errors: usize,
    pub pending: usize,
    pub completed_scan: bool,
    pub scan_seconds: f64,
}

/// Owned tasks finish their disk+metadata transaction even if the requester is cancelled.
#[derive(Default)]
pub(crate) struct Mutations {
    active: AtomicUsize,
    idle: Notify,
}
struct MutationGuard(Arc<Mutations>);
impl Drop for MutationGuard {
    fn drop(&mut self) {
        if self.0.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}
impl Mutations {
    #[cfg(test)]
    pub fn active_count(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }
    pub async fn run<T: Send + 'static>(
        self: &Arc<Self>,
        work: impl std::future::Future<Output = T> + Send + 'static,
    ) -> T {
        self.active.fetch_add(1, Ordering::AcqRel);
        let guard = MutationGuard(self.clone());
        tokio::spawn(async move {
            let _guard = guard;
            work.await
        })
        .await
        .expect("page mutation task panicked")
    }
    pub async fn drain(&self) {
        loop {
            let idle = self.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if self.active.load(Ordering::Acquire) == 0 {
                return;
            }
            idle.await;
        }
    }
}

#[derive(Clone)]
pub(crate) struct PageGcMetrics {
    pub scanned: Counter,
    pub deleted: [Counter; 3],
    pub bytes: [Counter; 3],
    pub delete_errors: Counter,
    pub retries: Gauge,
    pub batch_duration: Histogram,
    pub scan_duration: Gauge,
    pub dirty_blocks: Gauge,
    pub dirty_age: Gauge,
    pub checkpoint_bytes: Counter,
    pub checkpoint_errors: Counter,
    pub checkpoint_at: Gauge,
    pub missing: Counter,
    pub corrupt: Counter,
    pub future: Counter,
    pub ttl_seconds: Gauge,
    pub checkpoint_interval: Gauge,
    pub cleanup_scanned: Counter,
    pub cleanup_removed: Counter,
    pub cleanup_errors: Counter,
    pub cleanup_pending: Gauge,
    pub cleanup_scan_seconds: Gauge,
    pub cleanup_scan_at: Gauge,
}
impl PageGcMetrics {
    pub fn new(r: &Metrics) -> Self {
        let c = |name, help| r.counter(name, help, Default::default());
        let g = |name, help| r.gauge(name, help, Default::default());
        Self {
            cleanup_scanned: c("talon_worker_page_cleanup_scanned_total", "Disk entries examined for checkpoint leftovers."),
            cleanup_removed: c("talon_worker_page_cleanup_removed_total", "Leftover files and empty page directories removed."),
            cleanup_errors: c("talon_worker_page_cleanup_errors_total", "Failed disk cleanup operations; retried on the next pass."),
            cleanup_pending: g("talon_worker_page_cleanup_pending", "Paths with cleanup errors observed in the latest completed disk scan."),
            cleanup_scan_seconds: g("talon_worker_page_cleanup_scan_seconds", "Latest complete disk cleanup scan duration."),
            cleanup_scan_at: g("talon_worker_page_cleanup_scan_timestamp_seconds", "Last completed disk cleanup scan Unix timestamp."),
            scanned: c("talon_worker_page_gc_scanned_total", "Pages examined by idle GC."),
            deleted: ["ttl", "capacity", "superseded"].map(|reason| r.counter("talon_worker_page_gc_reclaimed_total", "Pages successfully unlinked by reason.", talon_core::metrics::labels(&[("reason", reason)]))),
            bytes: ["ttl", "capacity", "superseded"].map(|reason| r.counter("talon_worker_page_gc_reclaimed_bytes_total", "Logical bytes successfully unlinked by reason; open descriptors may delay physical release.", talon_core::metrics::labels(&[("reason", reason)]))),
            delete_errors: c("talon_worker_page_gc_delete_errors_total", "Failed page unlink attempts."),
            retries: g("talon_worker_page_gc_pending_retries", "Retry pages observed in the latest complete scan."),
            batch_duration: r.histogram("talon_worker_page_gc_batch_seconds", "Idle GC batch duration.", Default::default()),
            scan_duration: g("talon_worker_page_gc_scan_seconds", "Most recent complete idle scan duration."),
            dirty_blocks: g("talon_worker_page_access_dirty_blocks", "Dirty blocks observed in checkpoint traversal."),
            dirty_age: g("talon_worker_page_access_oldest_dirty_seconds", "Age of oldest uncheckpointed modification observed in traversal."),
            checkpoint_bytes: c("talon_worker_page_access_checkpoint_bytes_total", "Access metadata bytes checkpointed."),
            checkpoint_errors: c("talon_worker_page_access_checkpoint_errors_total", "Failed access checkpoints."),
            checkpoint_at: g("talon_worker_page_access_checkpoint_timestamp_seconds", "Last successful access checkpoint Unix timestamp."),
            missing: c("talon_worker_page_access_recovery_missing_total", "Recovered pages without a valid access record."),
            corrupt: c("talon_worker_page_access_recovery_corrupt_total", "Pages in corrupt access checkpoints."),
            future: c("talon_worker_page_access_recovery_future_total", "Future access records rejected on recovery."),
            ttl_seconds: g("talon_worker_page_ttl_seconds", "Configured page idle lifetime; zero disables GC."),
            checkpoint_interval: g("talon_worker_page_access_checkpoint_interval_seconds", "Configured access checkpoint period."),
        }
    }
}

/// Start collector, checkpoint, and disk cleanup loops with orderly shutdown.
pub struct PageGcService {
    stop: tokio::sync::watch::Sender<bool>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    worker: Arc<WorkerRuntime>,
}
impl PageGcService {
    pub fn start(worker: Arc<WorkerRuntime>, config: PageGcConfig) -> Self {
        let (stop, _) = tokio::sync::watch::channel(false);
        let mut tasks = Vec::new();
        // Disk leftovers must be reclaimed even after TTL is disabled. Keep
        // the loops independent so a long checkpoint cannot starve cleanup.
        for maintenance in 0..3 {
            if maintenance == 1 && config.ttl_ms == 0 {
                continue;
            }
            let worker = worker.clone();
            let mut stopped = stop.subscribe();
            let config = config.clone();
            tasks.push(tokio::spawn(async move {
                let millis = if maintenance == 1 {
                    config.checkpoint_interval_ms
                } else {
                    config.interval_ms
                };
                let period = Duration::from_millis(millis);
                let jitter = std::collections::hash_map::RandomState::new()
                    .build_hasher()
                    .finish()
                    % millis;
                let mut ticker = tokio::time::interval_at(
                    tokio::time::Instant::now() + Duration::from_millis(jitter),
                    period,
                );
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {biased;
                        _ = stopped.changed() => break,
                        _ = ticker.tick() => {
                            match maintenance {
                                0 => { worker.gc_once().await; }
                                1 => { worker.checkpoint_access_times().await; }
                                _ => { worker.cleanup_page_files_once().await; }
                            }
                        }
                    }
                }
            }));
        }
        Self {
            stop,
            tasks,
            worker,
        }
    }
    pub async fn shutdown(mut self) {
        let _ = self.stop.send(true);
        for task in self.tasks.drain(..) {
            let _ = task.await;
        }
        self.worker.drain_page_mutations().await;
        self.worker.checkpoint_access_times().await;
    }
}
impl Drop for PageGcService {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}
