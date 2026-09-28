//! Admission and graceful shutdown shared by completion and readiness runtimes.
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::watch;

const CLOSED: usize = 1 << (usize::BITS - 1);

pub struct DrainGate {
    state: AtomicUsize,
    changed: watch::Sender<()>,
}

impl Default for DrainGate {
    fn default() -> Self {
        Self {
            state: AtomicUsize::new(0),
            changed: watch::channel(()).0,
        }
    }
}

impl DrainGate {
    pub fn admit(self: &Arc<Self>) -> Option<Admission> {
        self.state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < CLOSED - 1).then_some(n + 1)
            })
            .ok()
            .map(|_| Admission(self.clone()))
    }

    /// Linearizes before any subsequent request admission, including pooled sockets.
    pub fn begin(&self) {
        self.state.fetch_or(CLOSED, Ordering::AcqRel);
        self.changed.send_replace(());
    }

    pub async fn stopped(&self) {
        let mut changed = self.changed.subscribe();
        while self.state.load(Ordering::Acquire) & CLOSED == 0 {
            let _ = changed.changed().await;
        }
    }

    pub async fn drained(&self) {
        let mut changed = self.changed.subscribe();
        while self.state.load(Ordering::Acquire) != CLOSED {
            let _ = changed.changed().await;
        }
    }
}

pub struct Admission(Arc<DrainGate>);
impl Drop for Admission {
    fn drop(&mut self) {
        if self.0.state.fetch_sub(1, Ordering::AcqRel) == CLOSED + 1 {
            self.0.changed.send_replace(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn stops_admission_but_preserves_accepted_work() {
        let gate = Arc::new(DrainGate::default());
        let held = gate.admit().unwrap();
        gate.begin();
        gate.stopped().await;
        assert!(gate.admit().is_none());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), gate.drained())
                .await
                .is_err()
        );
        drop(held);
        gate.drained().await;
        // Late subscribers cannot miss either notification.
        gate.stopped().await;
        gate.drained().await;
    }
}
