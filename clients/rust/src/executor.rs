//! Fixed Tokio I/O threads shared by the Rust, C and Python clients.
use crate::{client::Core, Error};
use std::{
    cell::RefCell,
    future::Future,
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread::JoinHandle,
};
use tokio::{runtime::Handle, sync::oneshot};

static NEXT_ID: AtomicUsize = AtomicUsize::new(1);
thread_local! {
    static CURRENT: RefCell<Option<Context>> = const { RefCell::new(None) };
}

struct Context {
    id: usize,
    index: usize,
    core: Arc<Core>,
}

// Each Handle belongs to a separate current-thread runtime. No socket or
// operation future is driven by the submitting caller.
pub(crate) struct Executor {
    id: usize,
    lanes: Vec<Handle>,
    stops: Vec<oneshot::Sender<()>>,
    threads: Vec<JoinHandle<()>>,
    next: AtomicUsize,
}

impl Executor {
    /// Use TOKIO_WORKER_THREADS, or the available CPU count when unset.
    pub(crate) fn new(core: Core, explicit: Option<usize>) -> Result<Self, Error> {
        let threads = if let Some(threads) = explicit {
            threads
        } else {
            match std::env::var("TOKIO_WORKER_THREADS") {
                Ok(value) => value
                    .parse()
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "TOKIO_WORKER_THREADS must be positive",
                        )
                    })
                    .map_err(runtime_error)?,
                Err(std::env::VarError::NotPresent) => std::thread::available_parallelism()
                    .map_err(runtime_error)?
                    .get(),
                Err(error) => {
                    return Err(runtime_error(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        error,
                    )))
                }
            }
        };
        if threads == 0 {
            return Err(Error::InvalidArgument(
                "thread count must be positive".into(),
            ));
        }
        let mut pool = Self {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            lanes: Vec::new(),
            stops: Vec::new(),
            threads: Vec::new(),
            next: AtomicUsize::new(0),
        };
        for index in 0..threads {
            let (stop, stopped) = oneshot::channel();
            let (ready, started) = std::sync::mpsc::sync_channel(1);
            let id = pool.id;
            let core = core.clone();
            pool.stops.push(stop);
            pool.threads.push(
                std::thread::Builder::new()
                    .name(format!("talon-sdk-tokio-{index}"))
                    .spawn(move || {
                        let runtime = match tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                        {
                            Ok(runtime) => runtime,
                            Err(error) => {
                                let _ = ready.send(Err(error));
                                return;
                            }
                        };
                        let local = Arc::new(core.with_sharded_connections());
                        drop(core);
                        CURRENT.with(|slot| {
                            *slot.borrow_mut() = Some(Context {
                                id,
                                index,
                                core: local,
                            })
                        });
                        if ready.send(Ok(runtime.handle().clone())).is_ok() {
                            runtime.block_on(async {
                                let _ = stopped.await;
                            });
                        }
                        // Drop every socket/task on its owning OS thread.
                        drop(runtime);
                        CURRENT.with(|slot| *slot.borrow_mut() = None);
                    })
                    .map_err(runtime_error)?,
            );
            let handle = started
                .recv()
                .map_err(|e| runtime_error(io::Error::other(e)))?
                .map_err(runtime_error)?;
            pool.lanes.push(handle);
        }
        Ok(pool)
    }

    /// Submit an operation. Construct and poll it on the selected Tokio thread.
    /// Explicit tracing context can be attached to the future as with Tokio spawn.
    pub(crate) fn spawn<F, Fut>(&self, operation: F) -> tokio::task::JoinHandle<Fut::Output>
    where
        F: FnOnce(Arc<Core>) -> Fut + Send + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let index = CURRENT
            .with(|slot| {
                slot.borrow()
                    .as_ref()
                    .filter(|lane| lane.id == self.id)
                    .map(|lane| lane.index)
            })
            .unwrap_or_else(|| self.next.fetch_add(1, Ordering::Relaxed) % self.lanes.len());
        let dispatch = tracing::dispatcher::get_default(|d| {
            (!d.is::<tracing::subscriber::NoSubscriber>()).then(|| d.clone())
        });
        self.lanes[index].spawn(async move {
            let core = CURRENT.with(|slot| {
                slot.borrow()
                    .as_ref()
                    .expect("executor thread")
                    .core
                    .clone()
            });
            let operation = operation(core);
            tokio::pin!(operation);
            std::future::poll_fn(|cx| match &dispatch {
                Some(dispatch) => {
                    tracing::dispatcher::with_default(dispatch, || operation.as_mut().poll(cx))
                }
                None => operation.as_mut().poll(cx),
            })
            .await
        })
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        self.stops.clear();
        // A callback may free its client. Never join an executor thread from
        // another executor thread: concurrent callbacks could otherwise deadlock.
        if CURRENT.with(|slot| slot.borrow().is_none()) {
            for thread in self.threads.drain(..) {
                let _ = thread.join();
            }
        }
    }
}

pub(crate) fn runtime_error(error: io::Error) -> Error {
    crate::CoordinatorError::Io(error).into()
}

/// Unlike a bare JoinHandle, dropping an SDK request cancels the operation.
pub(crate) struct Request<T>(pub(crate) tokio::task::JoinHandle<T>);
impl<T> Future for Request<T> {
    type Output = Result<T, Error>;
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.0)
            .poll(cx)
            .map(|result| result.map_err(|e| runtime_error(io::Error::other(e))))
    }
}
impl<T> Drop for Request<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Tag;
    impl tracing::Subscriber for Tag {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            false
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, _: &tracing::Event<'_>) {}
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }
    #[test]
    fn submitted_tasks_preserve_the_subscriber_without_leaking_it() {
        let core = crate::ClientBuilder::default()
            .with_coordinator("localhost:1")
            .build_core()
            .unwrap();
        let pool = Executor::new(core, Some(1)).unwrap();
        let dispatch = tracing::Dispatch::new(Tag);
        let task = tracing::dispatcher::with_default(&dispatch, || {
            pool.spawn(|_| async {
                tokio::task::yield_now().await;
                tracing::dispatcher::get_default(|d| d.is::<Tag>())
            })
        });
        assert!(futures::executor::block_on(task).unwrap());
        let task = pool.spawn(|_| async { tracing::dispatcher::get_default(|d| d.is::<Tag>()) });
        assert!(!futures::executor::block_on(task).unwrap());
    }
}
