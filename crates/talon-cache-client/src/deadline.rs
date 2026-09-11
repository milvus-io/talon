//! Share the initial deadline wakeup of short exchanges. Slow exchanges still
//! use their original, exact deadline; this never extends a request timeout.
use std::cell::Cell;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::task::Poll;
use std::time::Duration;

use futures::future::{poll_fn, Shared};
use futures::FutureExt;
use tokio::runtime::Id;
use tokio::time::{Instant, Sleep};

use crate::lock::MutexExt;

const SHARDS: usize = 32;
const GRACE: Duration = Duration::from_millis(10);
static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);
thread_local! {
    static SHARD: Cell<usize> = Cell::new(NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % SHARDS);
}

struct Wakeup {
    runtime: Id,
    at: Instant,
    sleep: Shared<Sleep>,
}

pub(crate) struct Deadlines {
    wakeups: [Mutex<Option<Wakeup>>; SHARDS],
}

impl Default for Deadlines {
    fn default() -> Self {
        Self {
            wakeups: std::array::from_fn(|_| Mutex::new(None)),
        }
    }
}

impl Deadlines {
    fn wakeup(&self, now: Instant) -> Shared<Sleep> {
        let runtime = tokio::runtime::Handle::current().id();
        let mut slot = self.wakeups[SHARD.get()].lock_recover();
        if let Some(wakeup) = slot.as_ref() {
            if wakeup.runtime == runtime && wakeup.at > now {
                return wakeup.sleep.clone();
            }
        }
        let at = now + GRACE;
        let sleep = tokio::time::sleep_until(at).shared();
        *slot = Some(Wakeup {
            runtime,
            at,
            sleep: sleep.clone(),
        });
        sleep
    }

    pub(crate) async fn run<T, E>(
        &self,
        what: &str,
        timeout: Duration,
        future: impl Future<Output = Result<T, E>>,
    ) -> Result<T, E>
    where
        E: From<std::io::Error>,
    {
        let now = Instant::now();
        let Some(deadline) = now.checked_add(timeout) else {
            return tokio::time::timeout(timeout, future)
                .await
                .unwrap_or_else(|_| Err(E::from(crate::pool::timeout_error(what, timeout))));
        };
        tokio::pin!(future);
        if timeout > GRACE {
            let mut wakeup = None;
            // A request is always polled immediately. The shared wakeup only
            // decides when to arm its private timer, not when to start I/O.
            let completed = poll_fn(|cx| {
                if let Poll::Ready(result) = future.as_mut().poll(cx) {
                    return Poll::Ready(Some(result));
                }
                let wakeup = wakeup.get_or_insert_with(|| self.wakeup(now));
                // A future that exhausts Tokio's cooperative budget must
                // not prevent the deadline wakeup from being polled.
                match tokio::task::coop::unconstrained(wakeup).poll_unpin(cx) {
                    Poll::Ready(()) => Poll::Ready(None),
                    Poll::Pending => Poll::Pending,
                }
            })
            .await;
            if let Some(result) = completed {
                return result;
            }
        }
        match tokio::time::timeout_at(deadline, future).await {
            Ok(result) => result,
            Err(_) => Err(E::from(crate::pool::timeout_error(what, timeout))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::task::{waker, ArcWake};
    use std::sync::Arc;
    use std::task::Context;

    #[tokio::test(start_paused = true)]
    async fn pending_exchange_keeps_original_deadline() {
        let deadlines = Deadlines::default();
        let started = Instant::now();
        let result: Result<(), std::io::Error> = deadlines
            .run("stalled", Duration::from_millis(50), std::future::pending())
            .await;
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
        assert!(started.elapsed() >= Duration::from_millis(50));
        assert!(started.elapsed() <= Duration::from_millis(52));
    }

    #[tokio::test(start_paused = true)]
    async fn short_deadline_does_not_wait_for_shared_wakeup() {
        let deadlines = Deadlines::default();
        let started = Instant::now();
        let result: Result<(), std::io::Error> = deadlines
            .run("short", Duration::from_millis(2), std::future::pending())
            .await;
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
        assert!(started.elapsed() < GRACE);
    }

    #[tokio::test(start_paused = true)]
    async fn fast_and_slow_successes_do_not_wait_for_deadline() {
        let deadlines = Deadlines::default();
        for delay in [Duration::from_millis(1), GRACE * 2] {
            let started = Instant::now();
            let result: Result<u8, std::io::Error> = deadlines
                .run("success", Duration::from_secs(1), async {
                    tokio::time::sleep(delay).await;
                    Ok(42)
                })
                .await;
            assert_eq!(result.unwrap(), 42);
            assert!(started.elapsed() <= delay + Duration::from_millis(2));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn immediate_results_preserve_zero_timeout_behavior() {
        let deadlines = Deadlines::default();
        let result: Result<u8, std::io::Error> = deadlines
            .run("ready", Duration::ZERO, async { Ok(7) })
            .await;
        assert_eq!(result.unwrap(), 7);
    }

    struct Task;
    impl ArcWake for Task {
        fn wake_by_ref(_: &Arc<Self>) {}
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_releases_task_waker_without_waiting_for_timer() {
        let deadlines = Deadlines::default();
        let task = Arc::new(Task);
        let weak = Arc::downgrade(&task);
        let waker = waker(task);
        let mut cx = Context::from_waker(&waker);
        let mut request = Box::pin(deadlines.run::<(), std::io::Error>(
            "cancelled",
            Duration::from_secs(30),
            std::future::pending(),
        ));
        assert!(request.as_mut().poll(&mut cx).is_pending());
        drop(request);
        drop(waker);
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_requests_keep_individual_deadlines() {
        let deadlines = Deadlines::default();
        let started = Instant::now();
        let first = async {
            let result: Result<(), std::io::Error> = deadlines
                .run("first", Duration::from_millis(50), std::future::pending())
                .await;
            assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
            assert!(started.elapsed() >= Duration::from_millis(50));
            assert!(started.elapsed() <= Duration::from_millis(52));
        };
        let second = async {
            tokio::time::sleep(Duration::from_millis(3)).await;
            let start = Instant::now();
            let result: Result<(), std::io::Error> = deadlines
                .run("second", Duration::from_millis(20), std::future::pending())
                .await;
            assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
            assert!(start.elapsed() >= Duration::from_millis(20));
            assert!(start.elapsed() <= Duration::from_millis(22));
        };
        tokio::join!(first, second);
    }

    #[tokio::test(start_paused = true)]
    async fn cooperative_budget_exhaustion_cannot_starve_deadline() {
        let deadlines = Deadlines::default();
        let busy = poll_fn(|cx| loop {
            let budget = tokio::task::consume_budget();
            tokio::pin!(budget);
            if budget.poll(cx).is_pending() {
                return Poll::<Result<(), std::io::Error>>::Pending;
            }
        });
        let mut request = Box::pin(deadlines.run("busy", Duration::from_millis(50), busy));
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(request.as_mut().poll(&mut cx).is_pending());
        tokio::time::advance(Duration::from_millis(52)).await;
        // Allow the driver to process elapsed timers, and a fresh task budget
        // when transitioning from the shared wakeup to the private deadline.
        tokio::time::sleep(Duration::from_millis(1)).await;
        for _ in 0..4 {
            if let Poll::Ready(result) = request.as_mut().poll(&mut cx) {
                assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("exhausted request budget starved the deadline");
    }

    #[test]
    fn cached_wakeup_is_not_reused_across_runtimes() {
        let deadlines = Deadlines::default();
        for _ in 0..2 {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let result: Result<(), std::io::Error> = deadlines
                        .run("runtime", Duration::from_millis(20), std::future::pending())
                        .await;
                    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
                });
        }
    }
}
