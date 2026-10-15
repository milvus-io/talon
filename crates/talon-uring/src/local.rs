//! Ring-local task ownership. Channels keep the public future Send while the
//! actual I/O future and its buffers never leave the current Monoio thread.
use std::{cell::RefCell, future::Future, rc::Rc, time::Duration};
use tokio::sync::{oneshot, Notify};

#[derive(Default)]
struct Tasks {
    active: std::cell::Cell<usize>,
    idle: Notify,
}
thread_local! {
    static CURRENT: RefCell<Option<Rc<Tasks>>> = const { RefCell::new(None) };
}

/// Install inside a native Monoio runtime; drain after stopping request admission
/// and before dropping the runtime. Deliberately !Send, like the ring it owns.
pub struct LocalRing(Rc<Tasks>);
impl LocalRing {
    pub fn enter() -> Self {
        assert!(!monoio::utils::is_legacy(), "native io_uring required");
        let tasks = Rc::new(Tasks::default());
        CURRENT.with(|current| {
            let mut current = current.borrow_mut();
            assert!(current.is_none(), "nested ring scope");
            *current = Some(tasks.clone());
        });
        Self(tasks)
    }

    pub async fn drain(&self) {
        while self.0.active.get() != 0 {
            self.0.idle.notified().await;
        }
    }
}
impl Drop for LocalRing {
    fn drop(&mut self) {
        CURRENT.with(|current| current.borrow_mut().take());
    }
}

pub fn on_ring() -> bool {
    CURRENT.with(|current| current.borrow().is_some())
}

/// Reject native I/O outside the worker's existing ring; never create a runtime.
pub fn require_ring() -> std::io::Result<()> {
    if on_ring() {
        Ok(())
    } else {
        Err(std::io::Error::other(
            "native I/O requires an existing Monoio ring",
        ))
    }
}

/// Bridge a ring-owned !Send future to a Send result without moving its I/O.
/// Dropping the receiver cancels the work, unlike detached disk mutations.
pub fn spawn_cancellable<F: Future + 'static>(work: F) -> oneshot::Receiver<F::Output> {
    let (mut result, wait) = oneshot::channel();
    spawn_local(async move {
        tokio::select! {
            biased;
            _ = result.closed() => {},
            value = work => { let _ = result.send(value); }
        }
    });
    wait
}

/// Detach a task from its caller, retaining it in the ring's shutdown drain.
/// Call only within a LocalRing scope. Awaiting the returned channel never
/// migrates the task; dropping it does not cancel accepted mutations.
pub fn spawn_local<F: Future + 'static>(work: F) -> oneshot::Receiver<F::Output>
where
    F::Output: 'static,
{
    let tasks = CURRENT.with(|current| current.borrow().as_ref().expect("ring scope").clone());
    tasks.active.set(tasks.active.get() + 1);
    struct Finished(Rc<Tasks>);
    impl Drop for Finished {
        fn drop(&mut self) {
            self.0.active.set(self.0.active.get() - 1);
            self.0.idle.notify_one();
        }
    }
    let finished = Finished(tasks);
    let (result, wait) = oneshot::channel();
    monoio::spawn(async move {
        let _finished = finished;
        let _ = result.send(work.await);
    });
    wait
}

/// Use the current ring's timer without putting a !Send Sleep in caller futures.
/// Cancellation drops the native timer too; it cannot delay ring shutdown.
pub async fn sleep(duration: Duration) {
    if on_ring() {
        let (mut done, wait) = oneshot::channel();
        spawn_local(async move {
            tokio::select! {
                _ = monoio::time::sleep(duration) => { let _ = done.send(()); }
                _ = done.closed() => {}
            }
        });
        let _ = wait.await;
    } else {
        tokio::time::sleep(duration).await;
    }
}

pub async fn timeout<F: Future>(duration: Duration, work: F) -> Result<F::Output, ()> {
    tokio::select! {
        biased;
        value = work => Ok(value),
        _ = sleep(duration) => Err(()),
    }
}
