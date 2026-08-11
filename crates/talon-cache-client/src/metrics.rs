//! Read-path metrics.
//!
//! [`ReadStats`] is a small bundle of atomic counters the read path bumps as it
//! serves blocks: placement-cache hits and misses, membership refreshes,
//! per-replica worker fetch attempts and failures, and bytes served. It is
//! cheap to share (all `Relaxed` atomics behind an `Arc`) and lets the mount
//! layer expose live read-path health without threading a metrics facade
//! through every call.
//!
//! Counters are monotonic; a caller samples them into a [`ReadStatsSnapshot`]
//! (e.g. for a `/metrics` line or a periodic `tracing` log) and diffs snapshots
//! over time for rates.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Shared, cheaply-clonable read-path counters.
#[derive(Debug, Clone, Default)]
pub struct ReadStats {
    inner: Arc<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    coordinator_refreshes: AtomicU64,
    worker_fetches: AtomicU64,
    worker_failures: AtomicU64,
    bytes_served: AtomicU64,
}

impl ReadStats {
    /// Create a fresh, zeroed counter set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a placement-cache hit (owners served from cache).
    pub fn record_cache_hit(&self) {
        self.inner.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a placement-cache miss (local ranking was needed).
    pub fn record_cache_miss(&self) {
        self.inner.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a forced membership refresh after all cached replicas failed.
    ///
    /// The method name is retained for metrics API compatibility.
    pub fn record_coordinator_refresh(&self) {
        self.inner
            .coordinator_refreshes
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a single worker fetch attempt (one replica dial).
    pub fn record_worker_fetch(&self) {
        self.inner.worker_fetches.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed worker fetch attempt.
    pub fn record_worker_failure(&self) {
        self.inner.worker_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Add to the count of bytes successfully served to the caller.
    pub fn add_bytes_served(&self, n: u64) {
        self.inner.bytes_served.fetch_add(n, Ordering::Relaxed);
    }

    /// Take a consistent-enough snapshot of the current counter values.
    pub fn snapshot(&self) -> ReadStatsSnapshot {
        ReadStatsSnapshot {
            cache_hits: self.inner.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.inner.cache_misses.load(Ordering::Relaxed),
            coordinator_refreshes: self.inner.coordinator_refreshes.load(Ordering::Relaxed),
            worker_fetches: self.inner.worker_fetches.load(Ordering::Relaxed),
            worker_failures: self.inner.worker_failures.load(Ordering::Relaxed),
            bytes_served: self.inner.bytes_served.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time copy of [`ReadStats`] counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReadStatsSnapshot {
    /// Placement-cache hits.
    pub cache_hits: u64,
    /// Placement-cache misses (local rankings triggered).
    pub cache_misses: u64,
    /// Forced membership refreshes after all cached replicas failed.
    ///
    /// The field name is retained for metrics API compatibility.
    pub coordinator_refreshes: u64,
    /// Worker fetch attempts (per-replica dials).
    pub worker_fetches: u64,
    /// Failed worker fetch attempts.
    pub worker_failures: u64,
    /// Bytes successfully served to callers.
    pub bytes_served: u64,
}

impl ReadStatsSnapshot {
    /// Cache hit ratio in `[0.0, 1.0]`, or `0.0` if there were no lookups.
    pub fn hit_ratio(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }
}

/// Zone relationship of one served worker read (ADR 0006).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZoneMatch {
    /// The worker shares the reader's zone.
    Same,
    /// The worker is in a different zone (billable transfer).
    Cross,
    /// The reader or the worker has no known zone.
    Unknown,
}

impl ZoneMatch {
    /// Bounded label value for metrics.
    pub fn label(self) -> &'static str {
        match self {
            Self::Same => "same",
            Self::Cross => "cross",
            Self::Unknown => "unknown",
        }
    }
}

/// Observer for zone-classified reads, so each binary counts them into its
/// own registry. Implementations must not block.
pub trait ZoneReadObserver: Send + Sync {
    /// One successfully served worker read of `bytes` bytes.
    fn worker_read(&self, matched: ZoneMatch, bytes: u64) {
        let _ = (matched, bytes);
    }

    /// Zone affinity was requested but the same-zone worker set was empty, so
    /// this placement resolution used the full membership.
    fn affinity_fallback(&self) {}
}

/// Observer that drops every event.
pub struct NoopZoneReadObserver;

impl ZoneReadObserver for NoopZoneReadObserver {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_accumulate_and_snapshot() {
        let s = ReadStats::new();
        s.record_cache_hit();
        s.record_cache_hit();
        s.record_cache_miss();
        s.record_coordinator_refresh();
        s.record_worker_fetch();
        s.record_worker_fetch();
        s.record_worker_failure();
        s.add_bytes_served(4096);
        s.add_bytes_served(96);

        let snap = s.snapshot();
        assert_eq!(snap.cache_hits, 2);
        assert_eq!(snap.cache_misses, 1);
        assert_eq!(snap.coordinator_refreshes, 1);
        assert_eq!(snap.worker_fetches, 2);
        assert_eq!(snap.worker_failures, 1);
        assert_eq!(snap.bytes_served, 4192);
    }

    #[test]
    fn hit_ratio_handles_empty_and_typical() {
        assert_eq!(ReadStatsSnapshot::default().hit_ratio(), 0.0);
        let snap = ReadStatsSnapshot {
            cache_hits: 3,
            cache_misses: 1,
            ..Default::default()
        };
        assert_eq!(snap.hit_ratio(), 0.75);
    }

    #[test]
    fn clones_share_the_same_counters() {
        let a = ReadStats::new();
        let b = a.clone();
        a.record_cache_hit();
        b.record_cache_hit();
        // Both handles point at the same underlying counters.
        assert_eq!(a.snapshot().cache_hits, 2);
        assert_eq!(b.snapshot().cache_hits, 2);
    }
}
