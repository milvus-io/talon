//! Flush driver for the write-back [`WriteCache`] (#364).
//!
//! [`WriteCache`] holds a queue of staged objects but nothing that drains it.
//! This is the driver: take the next staged generation, upload it to the origin,
//! and retire, retry, or park it.
//!
//! # Not reachable in production, deliberately
//!
//! ADR 0002 decided Talon promises **write-through only** — a write is durable
//! when the origin acknowledges it, never before — and rejected shipping
//! write-back behind an experimental flag, on the grounds that a flag is a
//! support commitment with the safety properties removed. So there is no
//! configuration key that constructs a [`Flusher`], and nothing on the serve
//! path references one. It is built and tested so that a future write-back
//! decision (which ADR 0002 §2 gates on replication before acknowledgement) is a
//! wiring problem rather than a design problem.
//!
//! # Bounded retries and the terminal state
//!
//! ADR 0002 §5 requires a defined answer to permanent flush failure and rules
//! out "retry forever", because unbounded retry converts a write failure into an
//! unbounded capacity leak: the staged bytes are never released, and the node
//! fills with data it cannot deliver.
//!
//! So each object gets an attempt budget. When it runs out, the entry is
//! **parked**, not deleted — it is acknowledged data the origin refused, and
//! discarding it would turn a delivery failure into silent data loss. Parked
//! entries stay counted in [`WriteCache::dirty_bytes`], are enumerable via
//! [`WriteCache::failed_objects`], and are re-armed by
//! [`WriteCache::retry_failed`] once the cause is fixed. The invariant is that a
//! failure is always *visible and actionable*, never silent in either direction.
//!
//! Attempts are per object, not global: one object the origin refuses (a bad
//! key, a bucket policy) must not consume the budget of every other object
//! behind it in the queue.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use talon_core::{BackendStore, ObjectId};

use crate::write_cache::WriteCache;

/// Default number of upload attempts per object before it is parked.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 5;

/// Default base delay for the exponential backoff between attempts.
pub const DEFAULT_BASE_BACKOFF: Duration = Duration::from_millis(200);

/// Default ceiling on the backoff delay.
pub const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(30);

/// What one [`Flusher::flush_next`] call did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushOutcome {
    /// Nothing was pending.
    Idle,
    /// An object was uploaded and retired; carries the byte count.
    Flushed(u64),
    /// The upload failed and the object was requeued for another attempt.
    Retrying,
    /// The upload failed and the object exhausted its attempt budget; it is
    /// parked and will not be retried without operator action.
    Parked,
}

/// Tunables for a [`Flusher`].
#[derive(Debug, Clone, Copy)]
pub struct FlushPolicy {
    /// Upload attempts per object before parking it.
    pub max_attempts: u32,
    /// Base delay for exponential backoff between attempts.
    pub base_backoff: Duration,
    /// Ceiling on the backoff delay.
    pub max_backoff: Duration,
}

impl Default for FlushPolicy {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            base_backoff: DEFAULT_BASE_BACKOFF,
            max_backoff: DEFAULT_MAX_BACKOFF,
        }
    }
}

/// Counters describing what a flusher has done, for metrics and tests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FlushStats {
    /// Objects successfully uploaded and retired.
    pub flushed: u64,
    /// Bytes successfully uploaded.
    pub bytes_flushed: u64,
    /// Failed upload attempts, including ones that were later retried
    /// successfully.
    pub attempts_failed: u64,
    /// Objects parked after exhausting their attempt budget.
    pub parked: u64,
}

/// Drains a [`WriteCache`] into a [`BackendStore`].
pub struct Flusher {
    cache: Arc<WriteCache>,
    backend: Arc<dyn BackendStore>,
    policy: FlushPolicy,
    /// Consecutive failed attempts per object, reset on success. Keyed by object
    /// rather than counted globally so one poisonous object cannot exhaust the
    /// budget of everything queued behind it.
    attempts: std::sync::Mutex<HashMap<ObjectId, u32>>,
    stats: std::sync::Mutex<FlushStats>,
}

impl Flusher {
    /// Create a flusher over a write cache and an origin backend.
    pub fn new(cache: Arc<WriteCache>, backend: Arc<dyn BackendStore>) -> Self {
        Self::with_policy(cache, backend, FlushPolicy::default())
    }

    /// Create a flusher with an explicit retry policy.
    pub fn with_policy(
        cache: Arc<WriteCache>,
        backend: Arc<dyn BackendStore>,
        policy: FlushPolicy,
    ) -> Self {
        Self {
            cache,
            backend,
            policy,
            attempts: std::sync::Mutex::new(HashMap::new()),
            stats: std::sync::Mutex::new(FlushStats::default()),
        }
    }

    /// The counters accumulated so far.
    pub fn stats(&self) -> FlushStats {
        *self.stats.lock().unwrap()
    }

    /// How long to wait before the attempt numbered `failures` (1-based).
    ///
    /// Exponential with a ceiling, so a sick origin is backed off from rather
    /// than hammered, and the delay never grows without bound.
    pub fn backoff_for(&self, failures: u32) -> Duration {
        let shift = failures.saturating_sub(1).min(16);
        self.policy
            .base_backoff
            .saturating_mul(1u32 << shift)
            .min(self.policy.max_backoff)
    }

    /// Take one staged object and try to upload it.
    ///
    /// Returns [`FlushOutcome::Idle`] when the queue is empty. On success the
    /// entry and its staged files are retired; on failure it is requeued, or
    /// parked once its attempt budget is spent.
    pub async fn flush_next(&self) -> FlushOutcome {
        let item = match self.cache.take_next() {
            Ok(Some(item)) => item,
            Ok(None) => return FlushOutcome::Idle,
            Err(error) => {
                // `take_next` already dropped the unreadable entry and logged
                // it; there is nothing here to retry.
                tracing::warn!(%error, "write cache could not produce a flush item");
                return FlushOutcome::Idle;
            }
        };

        let len = item.bytes.len() as u64;
        match self.backend.put(&item.object, item.bytes).await {
            Ok(version) => {
                self.cache.complete(&item.object, item.seq);
                self.attempts.lock().unwrap().remove(&item.object);
                let mut stats = self.stats.lock().unwrap();
                stats.flushed += 1;
                stats.bytes_flushed += len;
                tracing::info!(
                    object = %item.object.to_path(),
                    seq = item.seq,
                    bytes = len,
                    %version,
                    "flushed staged write to origin"
                );
                FlushOutcome::Flushed(len)
            }
            Err(error) => {
                let failures = {
                    let mut attempts = self.attempts.lock().unwrap();
                    let counter = attempts.entry(item.object.clone()).or_insert(0);
                    *counter += 1;
                    *counter
                };
                self.stats.lock().unwrap().attempts_failed += 1;

                if failures >= self.policy.max_attempts {
                    self.attempts.lock().unwrap().remove(&item.object);
                    self.stats.lock().unwrap().parked += 1;
                    tracing::error!(
                        object = %item.object.to_path(),
                        seq = item.seq,
                        attempts = failures,
                        %error,
                        "flush failed permanently; parking staged write"
                    );
                    self.cache.park_failed(&item.object, item.seq);
                    FlushOutcome::Parked
                } else {
                    tracing::warn!(
                        object = %item.object.to_path(),
                        seq = item.seq,
                        attempt = failures,
                        %error,
                        "flush attempt failed; will retry"
                    );
                    self.cache.requeue(&item.object, item.seq);
                    FlushOutcome::Retrying
                }
            }
        }
    }

    /// Flush until the queue is empty, sleeping the backoff after each failure.
    ///
    /// Returns the number of objects successfully flushed. Stops when nothing is
    /// pending — parked entries are not pending, so a permanently-failing object
    /// ends the drain rather than spinning on it.
    pub async fn drain(&self) -> u64 {
        let mut flushed = 0;
        loop {
            match self.flush_next().await {
                FlushOutcome::Idle => return flushed,
                FlushOutcome::Flushed(_) => flushed += 1,
                FlushOutcome::Parked => {}
                FlushOutcome::Retrying => {
                    // Back off before the next take, which may well be this same
                    // object; without this a failing origin is hammered at the
                    // speed of the loop.
                    let failures = self.max_recorded_failures();
                    tokio::time::sleep(self.backoff_for(failures)).await;
                }
            }
        }
    }

    /// The highest consecutive-failure count currently recorded, used to pace
    /// the drain loop's backoff.
    fn max_recorded_failures(&self) -> u32 {
        self.attempts
            .lock()
            .unwrap()
            .values()
            .copied()
            .max()
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU64, Ordering};
    use talon_core::{Backend, Error, ObjectStat, Result, Version};

    fn object(name: &str) -> ObjectId {
        ObjectId::new(Backend::S3, "bkt", name)
    }

    /// A backend that records puts and can be told to fail the first N attempts,
    /// or every attempt for a particular object.
    #[derive(Default)]
    struct FakeBackend {
        puts: std::sync::Mutex<Vec<(ObjectId, Bytes)>>,
        fail_first: AtomicU64,
        always_fail: std::sync::Mutex<Option<String>>,
    }

    impl FakeBackend {
        fn failing_first(n: u64) -> Self {
            Self {
                fail_first: AtomicU64::new(n),
                ..Default::default()
            }
        }

        fn always_failing_for(name: &str) -> Self {
            Self {
                always_fail: std::sync::Mutex::new(Some(name.to_string())),
                ..Default::default()
            }
        }

        fn puts(&self) -> Vec<(ObjectId, Bytes)> {
            self.puts.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl BackendStore for FakeBackend {
        async fn fetch_range(&self, obj: &ObjectId, _offset: u64, _len: u64) -> Result<Bytes> {
            Err(Error::NotFound(obj.to_path()))
        }

        async fn head(&self, obj: &ObjectId) -> Result<ObjectStat> {
            Err(Error::NotFound(obj.to_path()))
        }

        async fn put(&self, obj: &ObjectId, body: Bytes) -> Result<Version> {
            if self
                .always_fail
                .lock()
                .unwrap()
                .as_deref()
                .is_some_and(|n| n == obj.object_path)
            {
                return Err(Error::Backend("permanently rejected".into()));
            }
            if self
                .fail_first
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(Error::Backend("transient".into()));
            }
            self.puts.lock().unwrap().push((obj.clone(), body));
            Ok(Version::new("v-flushed"))
        }
    }

    /// Zero backoff so tests do not sleep.
    fn fast(max_attempts: u32) -> FlushPolicy {
        FlushPolicy {
            max_attempts,
            base_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        }
    }

    fn setup(
        backend: FakeBackend,
        policy: FlushPolicy,
    ) -> (tempdir::TempDir, Arc<WriteCache>, Arc<FakeBackend>, Flusher) {
        let dir = tempdir::TempDir::new();
        let cache = Arc::new(WriteCache::open(dir.path()).unwrap());
        let backend = Arc::new(backend);
        let flusher = Flusher::with_policy(cache.clone(), backend.clone(), policy);
        (dir, cache, backend, flusher)
    }

    #[tokio::test]
    async fn an_empty_queue_is_idle() {
        let (_d, _c, _b, flusher) = setup(FakeBackend::default(), fast(3));
        assert_eq!(flusher.flush_next().await, FlushOutcome::Idle);
        assert_eq!(flusher.stats(), FlushStats::default());
    }

    #[tokio::test]
    async fn a_successful_flush_uploads_and_retires_the_entry() {
        let (_d, cache, backend, flusher) = setup(FakeBackend::default(), fast(3));
        cache.stage(&object("a.bin"), b"hello").unwrap();

        assert_eq!(flusher.flush_next().await, FlushOutcome::Flushed(5));

        let puts = backend.puts();
        assert_eq!(puts.len(), 1);
        assert_eq!(puts[0].0, object("a.bin"));
        assert_eq!(&puts[0].1[..], b"hello");

        // Retired: no dirty bytes, no files, nothing left to flush.
        assert_eq!(cache.dirty_bytes(), 0);
        assert_eq!(cache.pending_len(), 0);
        assert_eq!(cache.inflight_len(), 0);
        assert_eq!(flusher.flush_next().await, FlushOutcome::Idle);

        let stats = flusher.stats();
        assert_eq!(stats.flushed, 1);
        assert_eq!(stats.bytes_flushed, 5);
        assert_eq!(stats.attempts_failed, 0);
    }

    #[tokio::test]
    async fn a_transient_failure_is_retried_and_then_succeeds() {
        let (_d, cache, backend, flusher) = setup(FakeBackend::failing_first(2), fast(5));
        cache.stage(&object("a.bin"), b"hello").unwrap();

        assert_eq!(flusher.flush_next().await, FlushOutcome::Retrying);
        assert_eq!(flusher.flush_next().await, FlushOutcome::Retrying);
        assert_eq!(flusher.flush_next().await, FlushOutcome::Flushed(5));

        assert_eq!(backend.puts().len(), 1, "only the successful put lands");
        assert_eq!(cache.dirty_bytes(), 0);
        let stats = flusher.stats();
        assert_eq!(stats.attempts_failed, 2);
        assert_eq!(stats.flushed, 1);
        assert_eq!(stats.parked, 0);
    }

    #[tokio::test]
    async fn a_permanent_failure_parks_the_entry_and_keeps_its_bytes() {
        let (_d, cache, _b, flusher) = setup(FakeBackend::always_failing_for("bad.bin"), fast(3));
        cache.stage(&object("bad.bin"), b"hello").unwrap();

        assert_eq!(flusher.flush_next().await, FlushOutcome::Retrying);
        assert_eq!(flusher.flush_next().await, FlushOutcome::Retrying);
        assert_eq!(flusher.flush_next().await, FlushOutcome::Parked);

        // Out of the queue, but the data is kept and stays visible: dropping it
        // would turn a delivery failure into silent data loss.
        assert_eq!(flusher.flush_next().await, FlushOutcome::Idle);
        assert_eq!(cache.failed_len(), 1);
        assert_eq!(cache.dirty_bytes(), 5);
        assert_eq!(cache.failed_objects(), vec![(object("bad.bin"), 5)]);
        assert_eq!(flusher.stats().parked, 1);
    }

    #[tokio::test]
    async fn a_parked_entry_is_reflushable_after_the_cause_is_fixed() {
        let dir = tempdir::TempDir::new();
        let cache = Arc::new(WriteCache::open(dir.path()).unwrap());
        let backend = Arc::new(FakeBackend::always_failing_for("bad.bin"));
        let flusher = Flusher::with_policy(cache.clone(), backend.clone(), fast(2));
        cache.stage(&object("bad.bin"), b"hello").unwrap();

        flusher.flush_next().await;
        assert_eq!(flusher.flush_next().await, FlushOutcome::Parked);

        // Operator fixes the cause and re-arms.
        *backend.always_fail.lock().unwrap() = None;
        assert_eq!(cache.retry_failed(), 1);
        assert_eq!(cache.failed_len(), 0);

        assert_eq!(flusher.flush_next().await, FlushOutcome::Flushed(5));
        assert_eq!(&backend.puts()[0].1[..], b"hello");
        assert_eq!(cache.dirty_bytes(), 0);
    }

    #[tokio::test]
    async fn a_poisonous_object_does_not_block_the_others() {
        let (_d, cache, backend, flusher) =
            setup(FakeBackend::always_failing_for("bad.bin"), fast(2));
        cache.stage(&object("bad.bin"), b"nope").unwrap();
        cache.stage(&object("good.bin"), b"fine").unwrap();

        let flushed = flusher.drain().await;

        assert_eq!(flushed, 1, "the healthy object drained");
        assert_eq!(backend.puts().len(), 1);
        assert_eq!(backend.puts()[0].0, object("good.bin"));
        assert_eq!(cache.failed_len(), 1, "only the poisonous one is parked");
        assert_eq!(cache.pending_len(), 0);
    }

    #[tokio::test]
    async fn the_attempt_budget_is_per_object() {
        // Two objects each fail once; with a budget of 2, neither should park,
        // which only holds if attempts are counted per object.
        let (_d, cache, _b, flusher) = setup(FakeBackend::failing_first(2), fast(2));
        cache.stage(&object("a.bin"), b"aa").unwrap();
        cache.stage(&object("b.bin"), b"bb").unwrap();

        let flushed = flusher.drain().await;

        assert_eq!(flushed, 2);
        assert_eq!(cache.failed_len(), 0);
        assert_eq!(flusher.stats().attempts_failed, 2);
    }

    #[tokio::test]
    async fn drain_empties_the_queue_and_counts_flushes() {
        let (_d, cache, backend, flusher) = setup(FakeBackend::default(), fast(3));
        for name in ["a.bin", "b.bin", "c.bin"] {
            cache.stage(&object(name), name.as_bytes()).unwrap();
        }

        assert_eq!(flusher.drain().await, 3);
        assert_eq!(backend.puts().len(), 3);
        assert_eq!(cache.dirty_bytes(), 0);
        assert_eq!(flusher.stats().flushed, 3);
    }

    #[tokio::test]
    async fn only_the_coalesced_generation_is_uploaded() {
        let (_d, cache, backend, flusher) = setup(FakeBackend::default(), fast(3));
        for body in [b"v1", b"v2", b"v3"] {
            cache.stage(&object("a.bin"), body).unwrap();
        }

        assert_eq!(flusher.drain().await, 1, "three writes, one upload");
        let puts = backend.puts();
        assert_eq!(puts.len(), 1);
        assert_eq!(&puts[0].1[..], b"v3");
    }

    #[tokio::test]
    async fn a_rewrite_after_parking_supersedes_the_parked_generation() {
        let dir = tempdir::TempDir::new();
        let cache = Arc::new(WriteCache::open(dir.path()).unwrap());
        let backend = Arc::new(FakeBackend::always_failing_for("a.bin"));
        let flusher = Flusher::with_policy(cache.clone(), backend.clone(), fast(1));

        // Budget of 1: the first failure parks v1.
        cache.stage(&object("a.bin"), b"v1").unwrap();
        assert_eq!(flusher.flush_next().await, FlushOutcome::Parked);
        assert_eq!(cache.failed_len(), 1);

        // The application rewrites the object. v2 is newer than the parked v1,
        // so staging it releases v1 rather than leaving it pinned forever.
        cache.stage(&object("a.bin"), b"v2").unwrap();
        assert_eq!(cache.failed_len(), 0);
        assert_eq!(cache.dirty_bytes(), 2, "only v2 remains staged");

        // Nothing left to re-arm; v1's bytes are gone.
        assert_eq!(cache.retry_failed(), 0);

        *backend.always_fail.lock().unwrap() = None;
        assert_eq!(flusher.flush_next().await, FlushOutcome::Flushed(2));
        let puts = backend.puts();
        assert_eq!(puts.len(), 1, "the superseded v1 is never uploaded");
        assert_eq!(&puts[0].1[..], b"v2");
    }

    #[tokio::test]
    async fn backoff_grows_exponentially_and_is_capped() {
        let (_d, _c, _b, flusher) = setup(
            FakeBackend::default(),
            FlushPolicy {
                max_attempts: 10,
                base_backoff: Duration::from_millis(100),
                max_backoff: Duration::from_millis(500),
            },
        );

        assert_eq!(flusher.backoff_for(1), Duration::from_millis(100));
        assert_eq!(flusher.backoff_for(2), Duration::from_millis(200));
        assert_eq!(flusher.backoff_for(3), Duration::from_millis(400));
        // Capped, and stays capped rather than overflowing at large counts.
        assert_eq!(flusher.backoff_for(4), Duration::from_millis(500));
        assert_eq!(flusher.backoff_for(u32::MAX), Duration::from_millis(500));
    }

    #[tokio::test]
    async fn recovered_entries_flush_after_a_restart() {
        let dir = tempdir::TempDir::new();
        {
            let cache = WriteCache::open(dir.path()).unwrap();
            cache.stage(&object("a.bin"), b"survives").unwrap();
        }

        let cache = Arc::new(WriteCache::open(dir.path()).unwrap());
        let backend = Arc::new(FakeBackend::default());
        let flusher = Flusher::new(cache.clone(), backend.clone());

        assert_eq!(flusher.flush_next().await, FlushOutcome::Flushed(8));
        assert_eq!(&backend.puts()[0].1[..], b"survives");
        assert_eq!(cache.dirty_bytes(), 0);
    }

    /// A minimal scoped temp directory (the worker crate has no tempfile dep).
    mod tempdir {
        use std::path::{Path, PathBuf};

        pub struct TempDir(PathBuf);

        impl TempDir {
            pub fn new() -> Self {
                static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let path =
                    std::env::temp_dir().join(format!("talon-flush-{}-{n}", std::process::id()));
                std::fs::create_dir_all(&path).unwrap();
                Self(path)
            }

            pub fn path(&self) -> &Path {
                &self.0
            }
        }

        impl Drop for TempDir {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
    }
}
