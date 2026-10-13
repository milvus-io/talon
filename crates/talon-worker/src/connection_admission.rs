//! Worker-global data-plane connection admission.
//!
//! Both server implementations acquire from this budget **before** accepting a
//! socket. When the budget is full, the accept loop waits and excess peers stay
//! in the kernel listen backlog: the worker does not reject them, accept their
//! file descriptors, or spawn parked per-connection tasks. Because there is one
//! accept loop per listener, waiting inside the worker is bounded by the number
//! of listeners.

use talon_transport::ConnectionLimit;
use tokio::sync::OwnedSemaphorePermit;

use crate::WorkerMetrics;

/// A cloneable worker-global connection budget with admission observability.
#[derive(Clone)]
pub struct ConnectionAdmission {
    limit: ConnectionLimit,
    metrics: WorkerMetrics,
}

impl ConnectionAdmission {
    /// Size the shared budget from the actual number of data-plane threads.
    /// Reject invalid capacities before constructing the underlying semaphore.
    pub fn for_threads(
        threads: usize,
        connections_per_thread: usize,
        metrics: WorkerMetrics,
    ) -> anyhow::Result<Self> {
        let capacity = threads
            .checked_mul(connections_per_thread)
            .filter(|&n| n > 0 && n <= tokio::sync::Semaphore::MAX_PERMITS)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "invalid data-plane connection capacity: {threads} threads * \
                     data_plane_connections_per_thread={connections_per_thread}; \
                     total must be in 1..={}",
                    tokio::sync::Semaphore::MAX_PERMITS
                )
            })?;
        Ok(Self::new(capacity, metrics))
    }

    /// Create one admission budget and publish its configured capacity.
    pub fn new(capacity: usize, metrics: WorkerMetrics) -> Self {
        let limit = ConnectionLimit::new(capacity);
        metrics.set_connection_capacity(capacity.max(1));
        Self { limit, metrics }
    }

    /// Acquire capacity before accepting a socket.
    ///
    /// Saturation is recorded at the failed immediate acquisition, before this
    /// future waits. The returned RAII permit must be held until the accepted
    /// connection task exits.
    pub async fn acquire(&self) -> OwnedSemaphorePermit {
        if let Some(permit) = self.limit.try_acquire() {
            return permit;
        }
        self.metrics.record_connection_admission_saturation();
        self.limit.acquire().await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn default_budget_scales_with_threads_and_updates_metric() {
        let per_thread = talon_core::WorkerConfig::default().data_plane_connections_per_thread;
        for (threads, expected) in [(1, 8192), (16, 131072)] {
            let metrics = WorkerMetrics::new(0);
            let admission =
                ConnectionAdmission::for_threads(threads, per_thread, metrics.clone()).unwrap();
            assert_eq!(admission.limit.available(), expected);
            assert!(metrics
                .render()
                .contains(&format!("talon_worker_connection_capacity {expected}\n")));
        }
    }

    #[test]
    fn invalid_scaled_budgets_return_errors_instead_of_panicking() {
        let max = tokio::sync::Semaphore::MAX_PERMITS;
        for (threads, per_thread) in [(0, 8192), (1, 0), (usize::MAX, 2), (1, max + 1), (2, max)] {
            assert!(
                ConnectionAdmission::for_threads(threads, per_thread, WorkerMetrics::new(0))
                    .is_err()
            );
        }
        assert!(ConnectionAdmission::for_threads(1, max, WorkerMetrics::new(0)).is_ok());
    }

    #[tokio::test]
    async fn scaled_budget_remains_shared_across_acceptors() {
        let metrics = WorkerMetrics::new(0);
        let first = ConnectionAdmission::for_threads(2, 3, metrics.clone()).unwrap();
        let second = first.clone();
        let mut held = Vec::new();
        for _ in 0..3 {
            held.push(first.acquire().await);
            held.push(second.acquire().await);
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(10), first.acquire())
                .await
                .is_err()
        );
        assert!(metrics
            .render()
            .contains("talon_worker_connection_admission_saturated_total 1\n"));
        drop(held.pop());
        let _replacement = tokio::time::timeout(Duration::from_secs(1), second.acquire())
            .await
            .expect("releasing a connection restores shared capacity");
        assert_eq!(first.limit.available(), 0);
    }

    #[tokio::test]
    async fn cancelled_permit_holder_releases_capacity() {
        let metrics = WorkerMetrics::new(0);
        let admission = ConnectionAdmission::new(1, metrics);
        let permit = admission.acquire().await;
        let holder = tokio::spawn(async move {
            let _permit = permit;
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;
        holder.abort();
        holder.await.unwrap_err();

        let replacement = tokio::time::timeout(Duration::from_secs(1), admission.acquire())
            .await
            .expect("cancelling a connection task must release its permit");
        drop(replacement);
    }

    #[tokio::test]
    async fn clones_share_capacity_and_cancelled_waiters_do_not_leak() {
        let metrics = WorkerMetrics::new(0);
        let admission = ConnectionAdmission::new(1, metrics.clone());
        let permit = admission.acquire().await;

        let waiting_admission = admission.clone();
        let waiter = tokio::spawn(async move { waiting_admission.acquire().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if metrics
                    .render()
                    .contains("talon_worker_connection_admission_saturated_total 1")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocked clone should report saturation");

        waiter.abort();
        waiter.await.unwrap_err();
        drop(permit);

        let replacement = tokio::time::timeout(Duration::from_secs(1), admission.acquire())
            .await
            .expect("cancelled waiter must not retain capacity");
        drop(replacement);
        let rendered = metrics.render();
        assert!(rendered.contains("talon_worker_connection_capacity 1"));
        assert!(rendered.contains("talon_worker_connection_admission_saturated_total 1"));
    }
}
