// SPDX-License-Identifier: Apache-2.0
//! Prometheus metrics for the async worker.
//!
//! Deliberately a separate namespace from `talon_worker_*`. The two workers
//! answer the same requests with different cost models, so summing them into
//! one series would average a 4KB fetch with a 256MB one and hide exactly the
//! difference this worker exists to create. A dashboard that wants both plots
//! both.
//!
//! # The metric that matters
//!
//! `talon_async_worker_origin_bytes_fetched_total` against
//! `talon_async_worker_bytes_served_total` is the whole thesis: their ratio is
//! how much the cache saved, and on a selective-read workload the block worker's
//! equivalent ratio is far worse. Everything else here is supporting detail.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use talon_core::metrics::{labels, Counter, Gauge, Histogram, Metrics};

use crate::cache::tiered::TieredExtentCache;
use crate::runtime::ServeStats;

/// Counters, gauges and histograms for one async worker process.
#[derive(Clone)]
pub struct AsyncWorkerMetrics {
    registry: Metrics,

    requests_total: Counter,
    request_errors_total: Counter,
    writes_rejected_total: Counter,
    bytes_served_total: Counter,
    sendfile_responses_total: Counter,
    request_duration: Histogram,

    origin_bytes_fetched_total: Counter,
    version_lookups_total: Counter,
    version_cache_hits_total: Counter,
    version_mismatch_retries_total: Counter,
    reads_clamped_total: Counter,

    l1_hits_total: Counter,
    l1_misses_total: Counter,
    l1_evictions_total: Counter,
    l1_bytes: Gauge,

    l2_hits_total: Counter,
    l2_bytes_written_total: Counter,
    l2_short_misses_total: Counter,
    l2_extents_evicted_total: Counter,
    l2_extents: Gauge,
    l2_bytes: Gauge,

    admissions_rejected_total: Counter,
    admissions_dropped_total: Counter,

    active_connections: Gauge,
    active_connection_count: Arc<AtomicU64>,
}

impl std::fmt::Debug for AsyncWorkerMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncWorkerMetrics")
            .field("requests", &self.requests_total.get())
            .field("origin_bytes", &self.origin_bytes_fetched_total.get())
            .finish()
    }
}

impl AsyncWorkerMetrics {
    /// Create a registry labelled with the configured object-store backend.
    pub fn new(backend: &str) -> Self {
        let registry = Metrics::new();
        registry
            .gauge(
                "talon_async_worker_build_info",
                "Async worker build information.",
                labels(&[("version", env!("CARGO_PKG_VERSION")), ("backend", backend)]),
            )
            .set(1.0);

        let c = |name: &str, help: &str| registry.counter(name, help, BTreeMap::new());
        let g = |name: &str, help: &str| registry.gauge(name, help, BTreeMap::new());

        Self {
            requests_total: c(
                "talon_async_worker_requests_total",
                "Range reads completed.",
            ),
            request_errors_total: c(
                "talon_async_worker_request_errors_total",
                "Range reads completed with an error.",
            ),
            writes_rejected_total: c(
                "talon_async_worker_writes_rejected_total",
                "Write or delete requests refused; this worker is read-only.",
            ),
            bytes_served_total: c(
                "talon_async_worker_bytes_served_total",
                "Bytes returned to clients.",
            ),
            sendfile_responses_total: c(
                "talon_async_worker_sendfile_responses_total",
                "Reads answered zero-copy from a pinned NVMe extent.",
            ),
            request_duration: registry.histogram(
                "talon_async_worker_request_duration_seconds",
                "Range read latency.",
                BTreeMap::new(),
            ),

            origin_bytes_fetched_total: c(
                "talon_async_worker_origin_bytes_fetched_total",
                "Bytes fetched from the origin store. Against bytes_served_total, \
                 this is what the cache saved.",
            ),
            version_lookups_total: c(
                "talon_async_worker_version_lookups_total",
                "Object versions resolved by a backend HEAD.",
            ),
            version_cache_hits_total: c(
                "talon_async_worker_version_cache_hits_total",
                "Version resolutions answered from the TTL cache.",
            ),
            version_mismatch_retries_total: c(
                "talon_async_worker_version_mismatch_retries_total",
                "Reads retried after the origin reported a version mismatch.",
            ),
            reads_clamped_total: c(
                "talon_async_worker_reads_clamped_total",
                "Reads shortened because they ran past the end of the object.",
            ),

            l1_hits_total: c("talon_async_worker_l1_hits_total", "DRAM tier hits."),
            l1_misses_total: c("talon_async_worker_l1_misses_total", "DRAM tier misses."),
            l1_evictions_total: c(
                "talon_async_worker_l1_evictions_total",
                "Extents reclaimed from the DRAM tier.",
            ),
            l1_bytes: g(
                "talon_async_worker_l1_bytes",
                "Bytes currently held in the DRAM tier.",
            ),

            l2_hits_total: c("talon_async_worker_l2_hits_total", "NVMe tier hits."),
            l2_bytes_written_total: c(
                "talon_async_worker_l2_bytes_written_total",
                "Bytes written to the NVMe tier.",
            ),
            l2_short_misses_total: c(
                "talon_async_worker_l2_short_misses_total",
                "NVMe lookups where the stored extent was shorter than the read.",
            ),
            l2_extents_evicted_total: c(
                "talon_async_worker_l2_extents_evicted_total",
                "Extents discarded by NVMe region reclamation.",
            ),
            l2_extents: g(
                "talon_async_worker_l2_extents",
                "Extents currently addressable on the NVMe tier.",
            ),
            l2_bytes: g(
                "talon_async_worker_l2_bytes",
                "Bytes currently packed into NVMe regions.",
            ),

            admissions_rejected_total: c(
                "talon_async_worker_admissions_rejected_total",
                "Extents not written to NVMe for lack of DRAM hits.",
            ),
            admissions_dropped_total: c(
                "talon_async_worker_admissions_dropped_total",
                "Extents dropped because the admission staging buffer was full.",
            ),

            active_connections: g(
                "talon_async_worker_active_connections",
                "Data-plane connections currently open.",
            ),
            active_connection_count: Arc::new(AtomicU64::new(0)),
            registry,
        }
    }

    /// Record a successful read.
    pub fn record_success(&self, bytes: u64, zero_copy: bool, elapsed: Duration) {
        self.requests_total.inc();
        self.bytes_served_total.add(bytes);
        if zero_copy {
            self.sendfile_responses_total.inc();
        }
        self.request_duration.observe(elapsed.as_secs_f64());
    }

    /// Record a failed read.
    pub fn record_error(&self, elapsed: Duration) {
        self.request_errors_total.inc();
        self.request_duration.observe(elapsed.as_secs_f64());
    }

    /// Record a refused write or delete.
    pub fn record_write_rejected(&self) {
        self.writes_rejected_total.inc();
    }

    /// Track an open connection; the returned guard decrements on drop.
    pub fn track_connection(&self) -> ConnectionGuard {
        let n = self.active_connection_count.fetch_add(1, Ordering::Relaxed) + 1;
        self.active_connections.set(n as f64);
        ConnectionGuard {
            count: Arc::clone(&self.active_connection_count),
            gauge: self.active_connections.clone(),
        }
    }

    /// Copy the cache's and runtime's counters into the registry.
    ///
    /// Both keep their own atomics rather than holding metric handles, so that
    /// neither depends on this module. Call before rendering.
    pub fn sync(&self, cache: &TieredExtentCache, serve: &ServeStats) {
        let c = cache.stats();

        set_counter(&self.origin_bytes_fetched_total, serve.origin_bytes_fetched);
        set_counter(&self.version_lookups_total, serve.version_lookups);
        set_counter(&self.version_cache_hits_total, serve.version_cache_hits);
        set_counter(
            &self.version_mismatch_retries_total,
            serve.version_mismatch_retries,
        );
        set_counter(&self.reads_clamped_total, serve.reads_clamped);

        set_counter(&self.l1_hits_total, c.memory_hits);
        set_counter(&self.l1_misses_total, c.memory_misses);
        set_counter(&self.l1_evictions_total, c.memory_evictions);
        self.l1_bytes.set(c.memory_bytes as f64);

        set_counter(&self.l2_hits_total, c.disk_hits);
        set_counter(&self.l2_bytes_written_total, c.disk_bytes_written);
        set_counter(&self.l2_short_misses_total, c.disk_short_misses);
        set_counter(&self.l2_extents_evicted_total, c.disk_extents_evicted);
        set_counter(&self.admissions_rejected_total, c.admissions_rejected);
        set_counter(&self.admissions_dropped_total, c.admissions_dropped);

        if let Some(disk) = cache.disk() {
            self.l2_extents.set(disk.extent_count() as f64);
            self.l2_bytes.set(disk.allocated_bytes() as f64);
        }
    }

    /// Render the registry in Prometheus text format.
    pub fn render(&self) -> String {
        self.registry.render()
    }
}

/// Move a counter forward to `target`.
///
/// The source counters are monotonic, so this only ever adds; a counter that
/// somehow went backwards is left alone rather than made to decrease, which
/// Prometheus would read as a reset.
fn set_counter(counter: &Counter, target: u64) {
    let current = counter.get();
    if target > current {
        counter.add(target - current);
    }
}

/// Decrements the active-connection gauge when dropped.
pub struct ConnectionGuard {
    count: Arc<AtomicU64>,
    gauge: Gauge,
}

impl std::fmt::Debug for ConnectionGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionGuard").finish()
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        let n = self.count.fetch_sub(1, Ordering::Relaxed).saturating_sub(1);
        self.gauge.set(n as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats_with_origin_bytes(n: u64) -> ServeStats {
        ServeStats {
            origin_bytes_fetched: n,
            ..Default::default()
        }
    }

    #[test]
    fn the_registry_renders_the_headline_series() {
        let m = AsyncWorkerMetrics::new("s3");
        m.record_success(4096, false, Duration::from_millis(2));
        let text = m.render();

        assert!(text.contains("talon_async_worker_bytes_served_total 4096"));
        assert!(text.contains("talon_async_worker_origin_bytes_fetched_total"));
        assert!(text.contains("talon_async_worker_build_info"));
        assert!(text.contains("backend=\"s3\""));
    }

    #[test]
    fn the_namespace_never_collides_with_the_block_worker() {
        // Summing the two workers' series would average a 4KB fetch with a
        // 256MB one and hide the difference this worker exists to create.
        let text = AsyncWorkerMetrics::new("s3").render();
        for line in text.lines().filter(|l| !l.starts_with('#')) {
            assert!(
                !line.starts_with("talon_worker_"),
                "leaked into the block worker's namespace: {line}"
            );
        }
    }

    #[test]
    fn a_zero_copy_response_is_counted_separately() {
        let m = AsyncWorkerMetrics::new("s3");
        m.record_success(100, true, Duration::from_millis(1));
        m.record_success(100, false, Duration::from_millis(1));

        let text = m.render();
        assert!(text.contains("talon_async_worker_sendfile_responses_total 1"));
        assert!(text.contains("talon_async_worker_requests_total 2"));
    }

    #[test]
    fn errors_are_counted_apart_from_successes() {
        let m = AsyncWorkerMetrics::new("s3");
        m.record_error(Duration::from_millis(5));
        m.record_write_rejected();

        let text = m.render();
        assert!(text.contains("talon_async_worker_request_errors_total 1"));
        assert!(text.contains("talon_async_worker_writes_rejected_total 1"));
        assert!(
            text.contains("talon_async_worker_requests_total 0"),
            "an error must not count as a served request"
        );
    }

    #[test]
    fn syncing_moves_counters_forward_but_never_back() {
        // A counter that decreases reads as a process restart in Prometheus.
        let m = AsyncWorkerMetrics::new("s3");
        set_counter(&m.origin_bytes_fetched_total, 5000);
        assert_eq!(m.origin_bytes_fetched_total.get(), 5000);

        set_counter(&m.origin_bytes_fetched_total, 4000);
        assert_eq!(m.origin_bytes_fetched_total.get(), 5000, "went backwards");

        set_counter(&m.origin_bytes_fetched_total, 6000);
        assert_eq!(m.origin_bytes_fetched_total.get(), 6000);
    }

    #[tokio::test]
    async fn sync_copies_runtime_and_cache_counters() {
        use crate::cache::tiered::ExtentCacheConfig;

        let cache = TieredExtentCache::new(&ExtentCacheConfig {
            memory_bytes: 1 << 20,
            memory_shards: 1,
            ..Default::default()
        })
        .await
        .unwrap();

        let m = AsyncWorkerMetrics::new("s3");
        m.sync(&cache, &stats_with_origin_bytes(8192));

        let text = m.render();
        assert!(text.contains("talon_async_worker_origin_bytes_fetched_total 8192"));
        assert!(text.contains("talon_async_worker_l1_bytes 0"));
    }

    #[test]
    fn the_connection_gauge_returns_to_zero() {
        let m = AsyncWorkerMetrics::new("s3");
        {
            let _a = m.track_connection();
            let _b = m.track_connection();
            assert!(m
                .render()
                .contains("talon_async_worker_active_connections 2"));
        }
        assert!(m
            .render()
            .contains("talon_async_worker_active_connections 0"));
    }
}
