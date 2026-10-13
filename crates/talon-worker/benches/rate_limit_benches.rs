//! Microbenchmarks for the per-namespace rate limiter's hot-path overhead.
//!
//! [`NamespaceRateLimiter::admit`] runs on the data plane once per read, so its
//! cost is pure overhead on every request. These measure that overhead —
//! disabled vs enabled, one metric vs two, one namespace vs many — and the
//! same-namespace contention behaviour, since the design's claim is that the
//! io_uring rings never serialize on a global QoS lock (the limiter is one
//! lock-free GCRA cell per `(namespace, metric)`, so concurrent admits are a single
//! atomic CAS, and worst-case contention is a CAS retry, never a blocked thread).
//!
//! To isolate the admitted-path cost from the (cheaper) throttle path, the
//! enabled limiters use an enormous burst so they always admit; the throttle
//! path does strictly less work (an atomic load and compare, no successful CAS).
//!
//! Run: `cargo bench -p talon-worker --bench rate_limit_benches`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use talon_core::{MetricLimits, NamespaceId, RateLimit, RateLimitPolicy};
use talon_worker::NamespaceRateLimiter;

fn main() {
    divan::main();
}

/// A representative response size charged against read_throughput.
const READ_BYTES: u64 = 64 * 1024;

/// A limiter whose burst is so large it always admits, so a bench measures the
/// admitted path (map lookup + GCRA CAS per configured metric).
fn always_admits(iops: bool, throughput: bool) -> NamespaceRateLimiter {
    // A huge burst_ratio saturates the GCRA tolerance, so no charge ever throttles.
    let limit = RateLimit::new(1_000_000, 1.0e15).unwrap();
    NamespaceRateLimiter::new(RateLimitPolicy {
        enabled: true,
        default: MetricLimits {
            read_iops: iops.then_some(limit),
            read_throughput: throughput.then_some(limit),
        },
        overrides: HashMap::new(),
    })
}

/// Baseline: a disabled limiter returns immediately after the enabled check.
#[divan::bench]
fn admit_disabled(bencher: divan::Bencher) {
    let limiter = NamespaceRateLimiter::disabled();
    let namespace = NamespaceId::named("t");
    bencher.bench(|| limiter.admit(divan::black_box(&namespace), divan::black_box(READ_BYTES)));
}

/// Enabled with only a read_iops limit: one warm map lookup + one GCRA charge.
#[divan::bench]
fn admit_iops_only(bencher: divan::Bencher) {
    let limiter = always_admits(true, false);
    let namespace = NamespaceId::named("t");
    let _ = limiter.admit(&namespace, READ_BYTES); // warm the namespace entry
    bencher.bench(|| limiter.admit(divan::black_box(&namespace), divan::black_box(READ_BYTES)));
}

/// The full hot path: both read_iops and read_throughput limited (two charges).
#[divan::bench]
fn admit_iops_and_throughput(bencher: divan::Bencher) {
    let limiter = always_admits(true, true);
    let namespace = NamespaceId::named("t");
    let _ = limiter.admit(&namespace, READ_BYTES);
    bencher.bench(|| limiter.admit(divan::black_box(&namespace), divan::black_box(READ_BYTES)));
}

/// Steady-state cost when the map holds many namespaces and requests rotate over
/// them (measures lookup + cache behaviour, not first-insert).
#[divan::bench(args = [64, 1024])]
fn admit_across_namespaces(bencher: divan::Bencher, namespaces: usize) {
    let limiter = always_admits(true, true);
    let ids: Vec<NamespaceId> = (0..namespaces)
        .map(|i| NamespaceId::named(format!("namespace-{i}")))
        .collect();
    for id in &ids {
        let _ = limiter.admit(id, READ_BYTES); // warm every entry
    }
    let cursor = AtomicUsize::new(0);
    bencher.bench(|| {
        let idx = cursor.fetch_add(1, Ordering::Relaxed) % ids.len();
        limiter.admit(divan::black_box(&ids[idx]), divan::black_box(READ_BYTES))
    });
}

/// Contention: every thread hammers ONE namespace, so all admits contend on a
/// single GCRA atomic. Lock-free — the worst case is a CAS retry, never a
/// blocked thread — so this shows how the single-atomic cell degrades under
/// concurrency rather than whether it serializes.
#[divan::bench(threads = [1, 4, 8, 16])]
fn admit_same_namespace_contended(bencher: divan::Bencher) {
    let limiter = always_admits(true, true);
    let namespace = NamespaceId::named("hot");
    let _ = limiter.admit(&namespace, READ_BYTES);
    bencher.bench(|| limiter.admit(divan::black_box(&namespace), divan::black_box(READ_BYTES)));
}

/// Contention control: each thread uses a distinct namespace (a thread-local id),
/// so admits touch independent atoms and should scale without cross-thread
/// interference — the counterpoint to the single-namespace contention above.
#[divan::bench(threads = [1, 4, 8, 16])]
fn admit_distinct_namespaces_scaled(bencher: divan::Bencher) {
    thread_local! {
        static NAMESPACE: NamespaceId =
            NamespaceId::named(format!("t-{:?}", std::thread::current().id()));
    }
    let limiter = always_admits(true, true);
    bencher.bench(|| {
        NAMESPACE.with(|t| limiter.admit(divan::black_box(t), divan::black_box(READ_BYTES)))
    });
}
