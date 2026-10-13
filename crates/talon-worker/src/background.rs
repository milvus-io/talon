//! Shared scheduling and resource budgets for worker-local maintenance.
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use talon_core::{Metrics, WorkerConfig};
use tokio::{
    sync::{watch, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

type Work = Pin<Box<dyn Future<Output = Option<Duration>> + Send>>;
struct Task {
    name: &'static str,
    retry: Duration,
    next: Option<Instant>,
    trigger: Option<watch::Receiver<()>>,
    running: bool,
    run: Box<dyn FnMut() -> Work + Send>,
    metrics: Option<TaskMetrics>,
}

#[derive(Clone)]
struct TaskMetrics {
    active: talon_core::Gauge,
    completed: talon_core::Counter,
    panics: talon_core::Counter,
    duration: talon_core::Histogram,
}

/// Timer/event scheduler. Each registration has at most one active invocation;
/// overdue work is coalesced, and ready tasks are admitted in round-robin order.
pub struct BackgroundScheduler {
    tasks: Vec<Task>,
    concurrency: usize,
    metrics: Option<Metrics>,
}
impl BackgroundScheduler {
    /// Construct a scheduler with a positive global task concurrency limit.
    pub fn new(concurrency: usize) -> Self {
        assert!(concurrency > 0);
        Self {
            tasks: Vec::new(),
            concurrency,
            metrics: None,
        }
    }
    /// Attach the worker registry before registering tasks.
    pub fn with_metrics(mut self, metrics: Metrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Register a maintenance invocation. The first invocation is immediately eligible.
    pub fn register<F, Fut>(&mut self, name: &'static str, period: Duration, mut run: F)
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        assert!(!period.is_zero() && Instant::now().checked_add(period).is_some());
        self.register_work(name, period, None, move || {
            let work = run();
            async move {
                work.await;
                Some(period)
            }
        });
    }

    /// Register event-driven work. Initial admission observes any event published
    /// before subscription. Return a delay only for pending retry or periodic work;
    /// `None` sleeps until the next event without occupying a task slot.
    pub fn register_triggered<F, Fut>(
        &mut self,
        name: &'static str,
        trigger: watch::Receiver<()>,
        run: F,
    ) where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Option<Duration>> + Send + 'static,
    {
        self.register_work(name, Duration::from_secs(1), Some(trigger), run);
    }

    fn register_work<F, Fut>(
        &mut self,
        name: &'static str,
        retry: Duration,
        trigger: Option<watch::Receiver<()>>,
        mut run: F,
    ) where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Option<Duration>> + Send + 'static,
    {
        assert!(!self.tasks.iter().any(|task| task.name == name));
        let metrics = self.metrics.as_ref().map(|r| {
            let labels = || talon_core::metrics::labels(&[("task", name)]);
            TaskMetrics {
                active: r.gauge(
                    "talon_worker_background_task_active",
                    "Active background batches by task.",
                    labels(),
                ),
                completed: r.counter(
                    "talon_worker_background_task_completed_total",
                    "Completed background batches by task.",
                    labels(),
                ),
                panics: r.counter(
                    "talon_worker_background_task_panics_total",
                    "Background batches that panicked.",
                    labels(),
                ),
                duration: r.histogram(
                    "talon_worker_background_task_seconds",
                    "Background batch duration including resource waits.",
                    labels(),
                ),
            }
        });
        self.tasks.push(Task {
            metrics,
            name,
            retry,
            next: Some(Instant::now()),
            trigger,
            running: false,
            run: Box::new(move || Box::pin(run())),
        });
    }
    /// Start dispatching. Stopping prevents admission, then finishes active batches.
    pub fn start(self) -> BackgroundHandle {
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(self.run(stopped));
        BackgroundHandle {
            stop,
            task: Some(task),
        }
    }
    async fn run(mut self, mut stopped: watch::Receiver<bool>) {
        async fn changed(
            i: usize,
            mut trigger: watch::Receiver<()>,
        ) -> (usize, Option<watch::Receiver<()>>) {
            let open = trigger.changed().await.is_ok();
            (i, open.then_some(trigger))
        }
        let mut triggers = FuturesUnordered::new();
        for (i, task) in self.tasks.iter_mut().enumerate() {
            if let Some(trigger) = task.trigger.take() {
                triggers.push(changed(i, trigger));
            }
        }
        let mut active = FuturesUnordered::new();
        let mut cursor = 0;
        loop {
            if *stopped.borrow() {
                break;
            }
            for _ in 0..self.tasks.len() {
                if active.len() == self.concurrency {
                    break;
                }
                let i = cursor;
                cursor = (cursor + 1) % self.tasks.len();
                let task = &mut self.tasks[i];
                if !task.running && task.next.is_some_and(|at| at <= Instant::now()) {
                    task.running = true;
                    task.next = None;
                    // Catch both construction and polling panics so one task cannot
                    // silently disable the other maintenance registrations.
                    let future =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (task.run)()));
                    let name = task.name;
                    let retry = task.retry;
                    let metrics = task.metrics.clone();
                    active.push(async move {
                        let start = Instant::now();
                        if let Some(m) = &metrics {
                            m.active.set(1.0);
                        }
                        let result = match future {
                            Ok(future) => std::panic::AssertUnwindSafe(future).catch_unwind().await,
                            Err(error) => Err(error),
                        };
                        let ok = result.is_ok();
                        let delay = result.unwrap_or(Some(retry));
                        if let Some(m) = &metrics {
                            m.active.set(0.0);
                            m.completed.inc();
                            m.duration.observe(start.elapsed().as_secs_f64());
                            if !ok {
                                m.panics.inc();
                            }
                        }
                        if !ok {
                            tracing::error!(
                                task = name,
                                "background task panicked; retrying after its interval"
                            );
                        }
                        (i, delay)
                    });
                }
            }
            let next = if active.len() < self.concurrency {
                self.tasks
                    .iter()
                    .filter(|t| !t.running)
                    .filter_map(|t| t.next)
                    .min()
            } else {
                None
            };
            tokio::select! { biased;
                _ = stopped.changed() => break,
                Some((i, delay)) = active.next(), if !active.is_empty() => {
                    self.tasks[i].running = false;
                    let timed = delay.map(|delay| Instant::now() + delay);
                    // Preserve/coalesce a trigger received while the invocation ran.
                    self.tasks[i].next = self.tasks[i].next.into_iter().chain(timed).min();
                }
                Some((i, trigger)) = triggers.next(), if !triggers.is_empty() => {
                    if let Some(trigger) = trigger {
                        self.tasks[i].next = Some(Instant::now());
                        triggers.push(changed(i, trigger));
                    }
                }
                _ = async { match next { Some(at) => tokio::time::sleep_until(at).await, None => std::future::pending().await } } => {}
            }
        }
        // Never release task slots while their owned I/O is still running.
        while active.next().await.is_some() {}
    }
}

/// Owning scheduler handle; dropping it signals an orderly stop.
pub struct BackgroundHandle {
    stop: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl BackgroundHandle {
    /// Stop admitting new invocations while active tasks finish.
    pub fn begin_shutdown(&self) {
        let _ = self.stop.send(true);
    }

    /// Stop admission and wait for all admitted batches to finish.
    pub async fn shutdown(mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl Drop for BackgroundHandle {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

/// A shared, paced reservation clock. Charges precede work; idle time does not
/// accumulate burst credit. Cancellation may waste credit but cannot exceed the rate.
struct Rate {
    per_second: u64,
    next: Mutex<std::time::Instant>,
}
impl Rate {
    fn new(per_second: u64) -> Self {
        Self {
            per_second,
            next: Mutex::new(std::time::Instant::now()),
        }
    }
    fn reserve(&self, units: u64) -> Option<std::time::Instant> {
        if self.per_second == 0 || units == 0 {
            return None;
        }
        let mut next = self.next.lock().unwrap();
        *next = (*next).max(std::time::Instant::now())
            + Duration::from_secs_f64(units as f64 / self.per_second as f64);
        Some(*next)
    }
    async fn charge(&self, units: u64) {
        if let Some(at) = self.reserve(units) {
            tokio::time::sleep_until(at.into()).await;
        }
    }
}

/// Shared background disk resources. Foreground requests do not use these budgets.
/// Keep the I/O permit inside owned mutations through disk and metadata completion.
pub(crate) struct BackgroundBudget {
    io: Arc<Semaphore>,
    bytes: Rate,
    deletes: Rate,
}
impl BackgroundBudget {
    pub fn new(config: &WorkerConfig) -> Arc<Self> {
        Arc::new(Self {
            io: Arc::new(Semaphore::new(config.background_io_concurrency)),
            bytes: Rate::new(config.background_io_max_mb_per_sec * 1_000_000),
            deletes: Rate::new(config.background_delete_max_per_sec),
        })
    }
    #[cfg(test)]
    pub fn available_io(&self) -> usize {
        self.io.available_permits()
    }

    pub async fn io(&self) -> OwnedSemaphorePermit {
        self.io
            .clone()
            .acquire_owned()
            .await
            .expect("background budget remains open")
    }
    pub async fn delete(&self) {
        self.delete_units(1).await;
    }
    /// Charge each resident page even when a stale block is removed as a directory.
    pub async fn delete_units(&self, units: u64) {
        self.deletes.charge(units).await;
    }
    /// Await before each bounded read/write chunk.
    pub async fn bytes(&self, bytes: usize) {
        self.bytes.charge(bytes as u64).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn event_during_work_is_coalesced_and_idle_wait_releases_slot() {
        let (trigger, receiver) = watch::channel(());
        let gate = Arc::new(Semaphore::new(0));
        let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
        let mut scheduler = BackgroundScheduler::new(1);
        let event_gate = gate.clone();
        let event_sent = sent.clone();
        scheduler.register_triggered("event", receiver, move || {
            let gate = event_gate.clone();
            let sent = event_sent.clone();
            async move {
                sent.send("event").unwrap();
                gate.acquire().await.unwrap().forget();
                None
            }
        });
        scheduler.register("timer", Duration::from_secs(3600), move || {
            let sent = sent.clone();
            async move {
                sent.send("timer").unwrap();
            }
        });
        let handle = scheduler.start();
        assert_eq!(received.recv().await, Some("event"));
        trigger.send_replace(());
        trigger.send_replace(());
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            received.try_recv().is_err(),
            "event must not overlap itself"
        );
        gate.add_permits(1);
        assert_eq!(received.recv().await, Some("timer"));
        assert_eq!(received.recv().await, Some("event"));
        gate.add_permits(1);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(received.try_recv().is_err(), "multiple events coalesce");
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(3601)).await;
        tokio::time::resume();
        assert_eq!(
            received.recv().await,
            Some("timer"),
            "idle event wait holds no slot"
        );
        trigger.send_replace(());
        assert_eq!(received.recv().await, Some("event"));
        handle.begin_shutdown();
        trigger.send_replace(());
        gate.add_permits(1);
        handle.shutdown().await;
        assert!(
            received.try_recv().is_err(),
            "no event admission after stop"
        );
    }

    #[tokio::test]
    async fn event_work_retries_only_while_requested() {
        let (_trigger, receiver) = watch::channel(());
        let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
        let mut scheduler = BackgroundScheduler::new(1);
        let mut calls = 0;
        scheduler.register_triggered("event", receiver, move || {
            calls += 1;
            let call = calls;
            let sent = sent.clone();
            async move {
                sent.send(call).unwrap();
                (call == 1).then_some(Duration::from_millis(10))
            }
        });
        let handle = scheduler.start();
        assert_eq!(received.recv().await, Some(1));
        assert_eq!(received.recv().await, Some(2));
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(received.try_recv().is_err());
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn begin_shutdown_stops_admission_before_join_and_drains_active_work() {
        let gate = Arc::new(Semaphore::new(0));
        let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
        let mut scheduler = BackgroundScheduler::new(1);
        for name in ["active", "queued"] {
            let gate = gate.clone();
            let sent = sent.clone();
            scheduler.register(name, Duration::from_millis(1), move || {
                let gate = gate.clone();
                let sent = sent.clone();
                async move {
                    sent.send(name).unwrap();
                    gate.acquire().await.unwrap().forget();
                }
            });
        }
        let mut handle = scheduler.start();
        assert_eq!(received.recv().await.unwrap(), "active");
        handle.begin_shutdown();
        assert!(!handle.task.as_ref().unwrap().is_finished());
        gate.add_permits(1);
        // The scheduler must finish without waiting for the later shutdown join:
        // admitting the queued registration would block on the empty gate.
        tokio::time::timeout(Duration::from_secs(1), handle.task.take().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert!(
            received.try_recv().is_err(),
            "no admission after begin_shutdown"
        );
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn bounded_fair_non_reentrant_and_shutdown_drains() {
        let gate = Arc::new(Semaphore::new(0));
        let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
        let mut scheduler = BackgroundScheduler::new(2);
        let running = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        for name in ["gc", "checkpoint", "cleanup", "eviction"] {
            let gate = gate.clone();
            let sent = sent.clone();
            let running = running.clone();
            let maximum = maximum.clone();
            let own = Arc::new(AtomicUsize::new(0));
            scheduler.register(name, Duration::from_millis(1), move || {
                let gate = gate.clone();
                let sent = sent.clone();
                let own = own.clone();
                let running = running.clone();
                let maximum = maximum.clone();
                async move {
                    assert_eq!(own.fetch_add(1, Ordering::SeqCst), 0);
                    let count = running.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum.fetch_max(count, Ordering::SeqCst);
                    sent.send(name).unwrap();
                    gate.acquire().await.unwrap().forget();
                    own.fetch_sub(1, Ordering::SeqCst);
                    running.fetch_sub(1, Ordering::SeqCst);
                }
            });
        }
        let handle = scheduler.start();
        assert_eq!(received.recv().await.unwrap(), "gc");
        assert_eq!(received.recv().await.unwrap(), "checkpoint");
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            received.try_recv().is_err(),
            "missed periods do not enqueue overlapping runs"
        );
        gate.add_permits(1);
        assert_eq!(received.recv().await.unwrap(), "cleanup");
        gate.add_permits(1);
        assert_eq!(received.recv().await.unwrap(), "eviction");
        let stopping = tokio::spawn(handle.shutdown());
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!stopping.is_finished());
        gate.add_permits(2);
        tokio::time::timeout(Duration::from_secs(1), stopping)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(running.load(Ordering::SeqCst), 0);
        assert_eq!(maximum.load(Ordering::SeqCst), 2);
        assert!(received.try_recv().is_err(), "no admission after stop");
    }

    #[tokio::test]
    async fn panic_does_not_stop_other_registrations_and_drop_stops_admission() {
        let mut scheduler = BackgroundScheduler::new(1);
        scheduler.register("broken", Duration::from_secs(60), || async {
            panic!("injected");
        });
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        scheduler.register("healthy", Duration::from_secs(60), move || {
            let tx = tx.clone();
            async move {
                tx.send(()).unwrap();
            }
        });
        let handle = scheduler.start();
        tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        drop(handle);
        assert!(tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn shared_disk_slots_and_independent_rate_dimensions() {
        let budget = BackgroundBudget::new(&WorkerConfig {
            background_io_concurrency: 1,
            background_io_max_mb_per_sec: 1,
            background_delete_max_per_sec: 100,
            ..Default::default()
        });
        let permit = budget.io().await;
        let other = budget.clone();
        let waiting = tokio::spawn(async move { other.io().await });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(permit);
        drop(waiting.await.unwrap());
        let start = std::time::Instant::now();
        let other = budget.clone();
        let write = tokio::spawn(async move { other.bytes(50_000).await });
        budget.delete().await;
        budget.delete().await;
        write.await.unwrap();
        assert!(start.elapsed() >= Duration::from_millis(50));
        // A second client of the same byte clock cannot reuse the first client's credit.
        let first = budget.bytes.reserve(1_000_000).unwrap();
        let second = budget.bytes.reserve(1_000_000).unwrap();
        assert!(second.duration_since(first) >= Duration::from_secs(1));
        assert!(Rate::new(0).reserve(u64::MAX).is_none());
    }
}
