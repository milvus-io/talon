//! Submit GC HEADs to an existing data-plane ring. The GC scheduler owns rate
//! and concurrency limits; the ring owns HTTP futures, sockets and cancellation.
use std::sync::Arc;
use talon_core::{BackendStore, Error, ObjectId, ObjectStat, Result};
use tokio::sync::{mpsc, oneshot, watch};

#[derive(Clone)]
enum State {
    Starting,
    Ready(mpsc::Sender<Request>),
    Stopped,
}

struct Request {
    backend: Arc<dyn BackendStore>,
    object: ObjectId,
    result: oneshot::Sender<Result<ObjectStat>>,
}

pub(super) struct OriginHeads {
    state: watch::Sender<State>,
    capacity: usize,
}

fn stopped() -> Error {
    Error::Backend("background origin HEAD ring stopped".into())
}

impl OriginHeads {
    pub(super) fn new(concurrency: usize) -> Arc<Self> {
        Arc::new(Self {
            state: watch::channel(State::Starting).0,
            capacity: concurrency.max(1),
        })
    }

    pub(super) async fn head(
        &self,
        backend: Arc<dyn BackendStore>,
        object: ObjectId,
    ) -> Result<ObjectStat> {
        let mut state = self.state.subscribe();
        let sender = loop {
            let current = state.borrow_and_update().clone();
            match current {
                State::Starting => state.changed().await.map_err(|_| stopped())?,
                State::Ready(sender) => break sender,
                State::Stopped => return Err(stopped()),
            }
        };
        let (result, wait) = oneshot::channel();
        // The caller's timeout includes waiting for readiness and queue space.
        // Dropping `wait` also cancels an accepted request on its owning ring.
        tokio::select! {
            biased;
            _ = state.changed() => Err(stopped()),
            result = async {
                sender.send(Request { backend, object, result }).await.map_err(|_| stopped())?;
                wait.await.map_err(|_| stopped())?
            } => result,
        }
    }

    /// Called after bind, within LocalRing. Low-QPS GC work uses the first ready
    /// ring; all namespaces share this queue and the existing GC admission limit.
    #[cfg(target_os = "linux")]
    pub(super) fn start_on_ring(self: &Arc<Self>) {
        let (sender, mut requests) = mpsc::channel::<Request>(self.capacity);
        if !self.state.send_if_modified(|state| {
            if !matches!(state, State::Starting) {
                return false;
            }
            *state = State::Ready(sender.clone());
            true
        }) {
            return;
        }
        // Construct before spawning so even an unpolled receiver stops waiters
        // if the ring drains or exits. Stopped can never become Ready again.
        struct Stop(Arc<OriginHeads>);
        impl Drop for Stop {
            fn drop(&mut self) {
                self.0.stop();
            }
        }
        let guard = Stop(self.clone());
        let mut state = self.state.subscribe();
        talon_uring::spawn_background(async move {
            let _guard = guard;
            loop {
                if matches!(*state.borrow(), State::Stopped) {
                    return;
                }
                let request = tokio::select! {
                    biased;
                    _ = state.changed() => return,
                    request = requests.recv() => match request {
                        Some(request) => request,
                        None => return,
                    },
                };
                if request.result.is_closed() {
                    continue;
                }
                let mut state = state.clone();
                talon_uring::spawn_background(async move {
                    let Request {
                        backend,
                        object,
                        mut result,
                    } = request;
                    if matches!(*state.borrow(), State::Stopped) {
                        return;
                    }
                    tokio::select! {
                        biased;
                        _ = state.changed() => {},
                        _ = result.closed() => {},
                        value = backend.head(&object) => { let _ = result.send(value); },
                    }
                });
            }
        });
    }

    pub(super) fn stop(&self) {
        self.state.send_replace(State::Stopped);
    }
}

impl super::WorkerRuntime {
    pub(super) async fn background_head(&self, object: &ObjectId) -> Result<ObjectStat> {
        match &self.background_heads {
            Some(heads) => heads.head(self.backend.clone(), object.clone()).await,
            None => self.backend.head(object).await,
        }
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn start_background_heads(&self) {
        if let Some(heads) = &self.background_heads {
            heads.start_on_ring();
        }
    }

    pub(crate) fn stop_background_heads(&self) {
        if let Some(heads) = &self.background_heads {
            heads.stop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    struct UnusedBackend;
    #[async_trait::async_trait]
    impl BackendStore for UnusedBackend {
        async fn fetch_range(&self, _: &ObjectId, _: u64, _: u64) -> Result<bytes::Bytes> {
            panic!("HEAD dispatcher must not download payloads")
        }
        async fn head(&self, _: &ObjectId) -> Result<ObjectStat> {
            panic!("cancelled or unstarted request must not reach the backend")
        }
    }

    fn object() -> ObjectId {
        ObjectId::new(talon_core::Backend::S3, "bucket", "key")
    }

    #[tokio::test]
    async fn startup_wait_and_shutdown_are_bounded_by_the_caller() {
        let heads = OriginHeads::new(1);
        assert!(tokio::time::timeout(
            Duration::from_millis(10),
            heads.head(Arc::new(UnusedBackend), object()),
        )
        .await
        .is_err());
        let waiting = heads.head(Arc::new(UnusedBackend), object());
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());
        heads.stop();
        assert!(waiting.await.is_err());
        assert!(heads.head(Arc::new(UnusedBackend), object()).await.is_err());
    }

    #[tokio::test]
    async fn cancelled_queued_heads_and_full_queue_release_their_callers() {
        let heads = OriginHeads::new(1);
        let (send, mut requests) = mpsc::channel(1);
        heads.state.send_replace(State::Ready(send));
        {
            let queued = heads.head(Arc::new(UnusedBackend), object());
            tokio::pin!(queued);
            assert!(futures::poll!(&mut queued).is_pending());
        }
        let blocked = heads.head(Arc::new(UnusedBackend), object());
        tokio::pin!(blocked);
        assert!(futures::poll!(&mut blocked).is_pending());
        heads.stop();
        assert!(blocked.await.is_err());
        let cancelled = requests.recv().await.unwrap();
        assert!(cancelled.result.is_closed());
        assert!(requests.recv().await.is_none());
    }
}
