//! Loader thread pool for off-ring backend range fetches.
//!
//! On a cache miss the data-plane ring must not block on backend HTTP (blocking
//! client + TLS breaks zero-copy and stalls the ring). Instead it submits a
//! [`LoadTask`] to this pool, which runs fetches on Tokio's blocking-friendly
//! runtime with **bounded concurrency** — a semaphore caps in-flight fetches so
//! a burst of misses applies backpressure rather than growing unbounded.
//!
//! Each task fetches a byte range via a [`BackendStore`], and the fetched bytes
//! are delivered back through a completion channel ([`LoadOutcome`]) so a
//! ring-side watcher can checksum, stage-commit, and update the index. Delivery
//! is lossless: every submitted task yields exactly one outcome.

use std::sync::Arc;

use talon_core::{BackendStore, BlockId, Error, ObjectId};
use tokio::sync::{mpsc, Semaphore};

/// A unit of work for the loader pool: fetch a byte range for a block.
#[derive(Debug, Clone)]
pub struct LoadTask {
    /// The block this load is materializing (identity for the completion).
    pub block: BlockId,
    /// Source object to fetch from.
    pub object: ObjectId,
    /// Byte offset of the range to fetch.
    pub offset: u64,
    /// Length of the range to fetch.
    pub len: u64,
}

/// The result of a completed [`LoadTask`].
#[derive(Debug)]
pub struct LoadOutcome {
    /// The block the load was for.
    pub block: BlockId,
    /// Fetched bytes on success, or the error on failure.
    pub result: Result<bytes::Bytes, Error>,
}

/// A bounded-concurrency pool that runs backend fetches off the ring.
pub struct LoaderPool {
    backend: Arc<dyn BackendStore>,
    permits: Arc<Semaphore>,
    completions_tx: mpsc::UnboundedSender<LoadOutcome>,
}

impl LoaderPool {
    /// Create a pool over `backend` allowing `max_concurrency` in-flight fetches.
    ///
    /// Returns the pool and the receiving end of the completion channel; the
    /// ring-side watcher drains outcomes from the receiver.
    pub fn new(
        backend: Arc<dyn BackendStore>,
        max_concurrency: usize,
    ) -> (Self, mpsc::UnboundedReceiver<LoadOutcome>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let pool = Self {
            backend,
            permits: Arc::new(Semaphore::new(max_concurrency.max(1))),
            completions_tx: tx,
        };
        (pool, rx)
    }

    /// Number of fetch permits currently available (in-flight = max - this).
    pub fn available_permits(&self) -> usize {
        self.permits.available_permits()
    }

    /// Submit a task; spawns a bounded fetch that reports via the completion
    /// channel.
    ///
    /// Acquiring a permit is async and applies backpressure: when all permits
    /// are held the returned future waits rather than over-committing the
    /// backend. Each submission produces exactly one [`LoadOutcome`].
    pub async fn submit(&self, task: LoadTask) {
        // Acquire a permit before spawning so concurrency stays bounded; hold it
        // for the fetch duration.
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .expect("semaphore open");
        let backend = Arc::clone(&self.backend);
        let tx = self.completions_tx.clone();
        tokio::spawn(async move {
            let result = backend
                .fetch_range(&task.object, task.offset, task.len)
                .await;
            // Permit released when `permit` drops at end of scope.
            drop(permit);
            // Send is lossless as long as the receiver lives; if the watcher is
            // gone the whole worker is shutting down, so dropping is fine.
            let _ = tx.send(LoadOutcome {
                block: task.block,
                result,
            });
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use talon_core::{Backend, ObjectStat, Result, Version};

    /// A backend that records peak concurrency and can fail selected objects.
    struct MockBackend {
        in_flight: AtomicUsize,
        peak: AtomicUsize,
        delay_ms: u64,
    }

    impl MockBackend {
        fn new(delay_ms: u64) -> Arc<Self> {
            Arc::new(Self {
                in_flight: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                delay_ms,
            })
        }
    }

    #[async_trait]
    impl BackendStore for MockBackend {
        async fn fetch_range(&self, obj: &ObjectId, offset: u64, len: u64) -> Result<bytes::Bytes> {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            if obj.object_path == "boom" {
                return Err(Error::Backend("simulated failure".into()));
            }
            // Echo a deterministic payload derived from the range.
            Ok(bytes::Bytes::from(format!(
                "{}:{offset}+{len}",
                obj.object_path
            )))
        }

        async fn head(&self, _obj: &ObjectId) -> Result<ObjectStat> {
            Ok(ObjectStat {
                len: 0,
                version: Version::new("v"),
            })
        }
    }

    fn block(name: &str) -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "b", name),
            0,
            256 << 20,
            Version::new("v1"),
        )
    }

    fn task(name: &str) -> LoadTask {
        LoadTask {
            block: block(name),
            object: ObjectId::new(Backend::S3, "b", name),
            offset: 0,
            len: 8,
        }
    }

    #[tokio::test]
    async fn every_task_yields_exactly_one_outcome() {
        let backend = MockBackend::new(0);
        let (pool, mut rx) = LoaderPool::new(backend, 4);
        for i in 0..10 {
            pool.submit(task(&format!("obj{i}"))).await;
        }
        let mut got = 0;
        for _ in 0..10 {
            let outcome = rx.recv().await.unwrap();
            assert!(outcome.result.is_ok());
            got += 1;
        }
        assert_eq!(got, 10);
    }

    #[tokio::test]
    async fn concurrency_is_bounded() {
        let backend = MockBackend::new(20);
        let dyn_backend: Arc<dyn BackendStore> = Arc::clone(&backend) as Arc<dyn BackendStore>;
        let (pool, mut rx) = LoaderPool::new(dyn_backend, 3);
        for i in 0..12 {
            pool.submit(task(&format!("obj{i}"))).await;
        }
        for _ in 0..12 {
            rx.recv().await.unwrap();
        }
        // Peak in-flight at the backend never exceeded the configured limit.
        assert!(backend.peak.load(Ordering::SeqCst) <= 3);
    }

    #[tokio::test]
    async fn backend_errors_are_reported_not_lost() {
        let backend = MockBackend::new(0);
        let (pool, mut rx) = LoaderPool::new(backend, 2);
        pool.submit(task("ok")).await;
        pool.submit(task("boom")).await;

        let mut oks = 0;
        let mut errs = 0;
        for _ in 0..2 {
            match rx.recv().await.unwrap().result {
                Ok(_) => oks += 1,
                Err(Error::Backend(_)) => errs += 1,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        assert_eq!((oks, errs), (1, 1));
    }
}
