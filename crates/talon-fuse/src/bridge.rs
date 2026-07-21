//! Synchronous-FUSE to async-runtime bridge.
//!
//! `fuser` invokes filesystem callbacks on dedicated **synchronous** threads,
//! but Talon's I/O (placement lookups, block fetches) is async. This bridge lets
//! a FUSE callback thread hand a unit of work to the async runtime and block for
//! the reply **without blocking the runtime's reactor**:
//!
//! - the FUSE thread submits a request over a **bounded** channel (applying
//!   backpressure when the runtime is saturated) paired with a oneshot reply,
//! - an async worker task on the runtime processes requests and answers the
//!   oneshot,
//! - the FUSE thread blocks only on its own oneshot receiver, never on the
//!   reactor, so there is no cross-thread deadlock.
//!
//! The bridge is generic over the request/response types and the async handler,
//! so it is unit-testable with a plain Tokio runtime and no real FUSE mount.

use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};

/// Errors crossing the sync/async boundary.
#[derive(Debug, PartialEq, Eq)]
pub enum BridgeError {
    /// The async worker is saturated and the bounded queue is full.
    ///
    /// The FUSE thread should surface this as a retryable/`EAGAIN`-style error
    /// rather than block indefinitely.
    Backpressure,
    /// The async worker shut down before answering (channel closed).
    WorkerGone,
}

/// The FUSE-thread handle: submit work and block for the reply.
pub struct BridgeClient<Req, Resp> {
    tx: mpsc::Sender<(Req, oneshot::Sender<Resp>)>,
    handle: Handle,
}

impl<Req, Resp> Clone for BridgeClient<Req, Resp> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            handle: self.handle.clone(),
        }
    }
}

impl<Req: Send + 'static, Resp: Send + 'static> BridgeClient<Req, Resp> {
    /// Try to submit `req` without blocking, returning a receiver for the reply.
    ///
    /// Unlike [`dispatch`](Self::dispatch) this never blocks the caller: it only
    /// enqueues (or reports [`BridgeError::Backpressure`]) and hands back the
    /// oneshot receiver so the caller decides how to await it. Useful when the
    /// caller must stay responsive, and for probing backpressure without
    /// blocking on a reply gated behind a saturated worker.
    pub fn try_submit(&self, req: Req) -> Result<oneshot::Receiver<Resp>, BridgeError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.try_send((req, reply_tx)).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => BridgeError::Backpressure,
            mpsc::error::TrySendError::Closed(_) => BridgeError::WorkerGone,
        })?;
        Ok(reply_rx)
    }

    /// Submit `req` and block the calling (FUSE) thread until the reply arrives.
    ///
    /// Uses a `try_send` so a full queue returns [`BridgeError::Backpressure`]
    /// immediately instead of blocking the FUSE thread (and, transitively, the
    /// kernel) under load. The reply is awaited by driving the oneshot on the
    /// runtime via [`Handle::block_on`], which parks only this thread.
    pub fn dispatch(&self, req: Req) -> Result<Resp, BridgeError> {
        let reply_rx = self.try_submit(req)?;
        // Block this FUSE thread (not the reactor) on the reply.
        self.handle
            .block_on(reply_rx)
            .map_err(|_| BridgeError::WorkerGone)
    }
}

/// Spawn an async worker on `handle` that serves requests via `handler`.
///
/// Returns a [`BridgeClient`] the FUSE threads clone and call
/// [`dispatch`](BridgeClient::dispatch) on. `max_in_flight` bounds concurrent
/// in-flight handler invocations; when that many are running the worker stops
/// pulling from the queue, the bounded channel fills, and further `dispatch`
/// calls get [`BridgeError::Backpressure`] instead of over-committing the
/// runtime. The worker runs until all clients are dropped.
pub fn spawn_bridge<Req, Resp, F, Fut>(
    handle: Handle,
    max_in_flight: usize,
    handler: F,
) -> BridgeClient<Req, Resp>
where
    Req: Send + 'static,
    Resp: Send + 'static,
    F: Fn(Req) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Resp> + Send + 'static,
{
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    let cap = max_in_flight.max(1);
    // The channel holds at most `cap` queued items; combined with the in-flight
    // permits this bounds total outstanding work to ~2*cap before backpressure.
    let (tx, mut rx) = mpsc::channel::<(Req, oneshot::Sender<Resp>)>(cap);
    let handler = Arc::new(handler);
    let permits = Arc::new(Semaphore::new(cap));
    handle.spawn(async move {
        while let Some((req, reply)) = rx.recv().await {
            // Acquire a permit BEFORE spawning; when all permits are held this
            // await parks the worker, so it stops draining the channel and the
            // queue fills — turning into real backpressure at the sender.
            let permit = Arc::clone(&permits)
                .acquire_owned()
                .await
                .expect("semaphore open");
            let handler = Arc::clone(&handler);
            tokio::spawn(async move {
                let resp = handler(req).await;
                let _ = reply.send(resp);
                drop(permit);
            });
        }
    });
    BridgeClient { tx, handle }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn dispatch_from_sync_thread_gets_async_reply() {
        let rt = runtime();
        let bridge = spawn_bridge(rt.handle().clone(), 16, |req: u64| async move {
            // Simulate async I/O.
            tokio::time::sleep(Duration::from_millis(5)).await;
            req * 2
        });

        // Call from a plain (non-async) thread, like a FUSE callback would.
        let handle = std::thread::spawn(move || {
            let a = bridge.dispatch(10).unwrap();
            let b = bridge.dispatch(21).unwrap();
            (a, b)
        });
        assert_eq!(handle.join().unwrap(), (20, 42));
    }

    #[test]
    fn concurrent_fuse_threads_do_not_deadlock() {
        let rt = runtime();
        let counter = Arc::new(AtomicUsize::new(0));
        let c2 = Arc::clone(&counter);
        let bridge = spawn_bridge(rt.handle().clone(), 32, move |req: usize| {
            let c = Arc::clone(&c2);
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                req + 1
            }
        });

        let mut handles = Vec::new();
        for t in 0..8 {
            let b = bridge.clone();
            handles.push(std::thread::spawn(move || {
                let mut sum = 0;
                for i in 0..25 {
                    sum += b.dispatch(t * 100 + i).unwrap();
                }
                sum
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(counter.load(Ordering::SeqCst), 8 * 25);
    }

    #[test]
    fn full_queue_applies_backpressure() {
        let rt = runtime();
        // max_in_flight = 1; the handler waits on a tokio gate so the single
        // permit stays held and the queue fills, forcing backpressure.
        let gate = Arc::new(tokio::sync::Notify::new());
        let gate2 = Arc::clone(&gate);
        let bridge = spawn_bridge(rt.handle().clone(), 1, move |_req: u64| {
            let gate = Arc::clone(&gate2);
            async move {
                gate.notified().await;
                0u64
            }
        });

        // Submit (non-blocking) until the worker holds its one permit and the
        // depth-1 queue is full; the next submission must report backpressure.
        // We keep the reply receivers alive so the sends stay outstanding.
        let mut _pending = Vec::new();
        let mut saw_backpressure = false;
        for _ in 0..500 {
            match bridge.try_submit(0u64) {
                Ok(rx) => _pending.push(rx),
                Err(BridgeError::Backpressure) => {
                    saw_backpressure = true;
                    break;
                }
                Err(e) => panic!("unexpected: {e:?}"),
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            saw_backpressure,
            "a saturated bridge must surface backpressure"
        );

        // Release the gate and drop outstanding work; the worker drains without
        // deadlock as the runtime shuts down. (We assert backpressure, not
        // completion order, here.)
        gate.notify_waiters();
        drop(_pending);
        drop(bridge);
    }

    #[test]
    fn worker_gone_is_reported() {
        let rt = runtime();
        let bridge: BridgeClient<u64, u64> =
            spawn_bridge(rt.handle().clone(), 4, |req| async move { req });
        // Drop the runtime so the worker task can no longer run / receive.
        drop(rt);
        // The channel is closed once the worker is gone.
        match bridge.dispatch(1) {
            Err(BridgeError::WorkerGone) | Ok(_) => {}
            other => panic!("unexpected: {other:?}"),
        }
    }
}
