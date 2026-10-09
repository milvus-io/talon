//! One-way shutdown notification. Task owners, rather than request counters,
//! are responsible for joining accepted work before releasing resources.
use std::future::{poll_fn, Future};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Poll, Waker};
use tokio::sync::Notify;

#[derive(Default)]
pub struct Shutdown {
    stopped: AtomicBool,
    changed: Notify,
}

impl Shutdown {
    pub fn begin(&self) {
        if !self.stopped.swap(true, Ordering::AcqRel) {
            self.changed.notify_waiters();
        }
    }

    /// A request that observes false belongs to its connection's accepted work.
    /// Shutdown must join that connection even if begin() races with this load.
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    /// Pin once per connection and reuse across reads, so normal requests do not
    /// repeatedly register and remove notification waiters.
    pub async fn stopped(&self) {
        // notify_waiters also wakes futures created before the broadcast but
        // not yet polled. Create this before checking the flag to avoid a gap.
        let notified = self.changed.notified();
        tokio::pin!(notified);
        let mut registered: Option<Waker> = None;
        poll_fn(|cx| {
            if self.is_stopped() {
                return Poll::Ready(());
            }
            // Notify locks its waiter list on every pending poll. The stop flag
            // is the only event we need, so keep the existing registration while
            // the task's waker is unchanged. Register again if the future moves
            // to a different task. begin() stores the flag before waking us.
            if registered.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                return Poll::Pending;
            }
            let result = notified.as_mut().poll(cx);
            if result.is_pending() {
                registered = Some(cx.waker().clone());
            }
            result
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_waiter_updates_its_waker_when_moved() {
        use std::sync::{atomic::AtomicUsize, Arc};
        use std::task::{Context, Wake};
        #[derive(Default)]
        struct WakeCount(AtomicUsize);
        impl Wake for WakeCount {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let shutdown = Shutdown::default();
        let first = Arc::new(WakeCount::default());
        let second = Arc::new(WakeCount::default());
        let first_waker = Waker::from(first.clone());
        let second_waker = Waker::from(second.clone());
        let mut stopped = Box::pin(shutdown.stopped());
        for waker in [&first_waker, &first_waker, &second_waker, &second_waker] {
            assert!(stopped
                .as_mut()
                .poll(&mut Context::from_waker(waker))
                .is_pending());
        }
        shutdown.begin();
        assert_eq!(first.0.load(Ordering::Relaxed), 0);
        assert_eq!(second.0.load(Ordering::Relaxed), 1);
        assert!(stopped
            .as_mut()
            .poll(&mut Context::from_waker(&second_waker))
            .is_ready());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registration_racing_shutdown_does_not_miss_wakeup() {
        for _ in 0..128 {
            let shutdown = std::sync::Arc::new(Shutdown::default());
            let sender = shutdown.clone();
            let task = tokio::spawn(async move {
                sender.begin();
            });
            tokio::time::timeout(std::time::Duration::from_secs(1), shutdown.stopped())
                .await
                .unwrap();
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn wakes_all_waiters_and_late_subscribers() {
        let shutdown = Shutdown::default();
        let first = shutdown.stopped();
        let second = shutdown.stopped();
        tokio::pin!(first, second);
        // Poll both once, like persistent connection read loops do.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(1), &mut first)
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(1), &mut second)
                .await
                .is_err()
        );
        shutdown.begin();
        shutdown.begin();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            first.await;
            second.await;
            shutdown.stopped().await;
        })
        .await
        .unwrap();
    }
}
