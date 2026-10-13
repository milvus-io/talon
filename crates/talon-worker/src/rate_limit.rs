//! Worker-side per-namespace rate limiting.
//!
//! A [`NamespaceRateLimiter`] enforces a static, per-worker local policy: for each
//! namespace it holds one lock-free GCRA cell per limited metric and admits or
//! throttles a request with a single atomic op, so the io_uring rings never
//! serialize on a global QoS lock — only same-namespace requests briefly touch the
//! same cell.
//!
//! This is the local, single-worker stage of the eventual global design: limits
//! are per worker and per process, sized so the fleet aggregate stays bounded.
//! Cross-worker owners and snapshot propagation are a later stage, as is
//! enforcing [`RateMetric::OriginReadBytes`] (which needs the namespace threaded
//! into the cache-miss path).

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use talon_core::{Gcra, MetricLimits, NamespaceId, RateDecision, RateLimitPolicy, RateMetric};

/// One namespace's GCRA cells, one per limited metric.
#[derive(Debug)]
struct NamespaceCells {
    read_iops: Option<Gcra>,
    read_throughput: Option<Gcra>,
}

impl NamespaceCells {
    fn from_limits(limits: &MetricLimits) -> Self {
        Self {
            read_iops: limits.read_iops.map(Gcra::new),
            read_throughput: limits.read_throughput.map(Gcra::new),
        }
    }

    /// Charge one read of `read_bytes` against this namespace's cells.
    fn charge(&self, read_bytes: u64, now_nanos: u64) -> Result<(), Throttled> {
        if let Some(cell) = &self.read_iops {
            if let RateDecision::Throttled { retry_after } = cell.charge_at(1, now_nanos) {
                return Err(Throttled {
                    metric: RateMetric::ReadIops,
                    retry_after,
                });
            }
        }
        if let Some(cell) = &self.read_throughput {
            if let RateDecision::Throttled { retry_after } = cell.charge_at(read_bytes, now_nanos) {
                return Err(Throttled {
                    metric: RateMetric::ReadThroughput,
                    retry_after,
                });
            }
        }
        Ok(())
    }
}

/// A rejected admission: which metric was exceeded and when to retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Throttled {
    /// The metric whose limit the request would exceed.
    pub metric: RateMetric,
    /// How long the caller should wait before retrying the same request.
    pub retry_after: Duration,
}

/// The maximum number of distinct namespaces that get their own dedicated GCRA
/// cells on one worker.
///
/// The direct data plane is unauthenticated, so a client can declare
/// arbitrarily many distinct namespace names; without a ceiling the cell map would
/// grow without bound — a memory-exhaustion vector. Namespaces first seen beyond
/// this cap share a single overflow cell governed by the default limit, so
/// memory stays bounded while the (almost certainly abusive or misconfigured)
/// excess is still paced. Configured overrides are pre-created and never lost to
/// the cap.
const MAX_NAMESPACE_CELLS: usize = 4096;

/// Worker-global, `Arc`-shared per-namespace limiter.
#[derive(Debug)]
pub struct NamespaceRateLimiter {
    policy: RateLimitPolicy,
    /// Monotonic clock origin; GCRA cells advance in nanoseconds from here.
    origin: Instant,
    /// Per-namespace cells: created eagerly for configured overrides and lazily for
    /// default-governed namespaces. A read lock covers the common warm lookup; the
    /// write lock is taken only the first time a default-governed namespace is
    /// seen. Bounded at `max_cells` entries (see [`MAX_NAMESPACE_CELLS`]).
    cells: RwLock<HashMap<NamespaceId, NamespaceCells>>,
    /// Shared fallback cell for namespaces first seen after the cap is reached,
    /// governed by the default limit. `None` when the default limits nothing
    /// (then default-governed namespaces are admitted without a cell at all).
    overflow: Option<NamespaceCells>,
    /// Ceiling on the number of distinct namespace cells (see [`MAX_NAMESPACE_CELLS`]).
    max_cells: usize,
}

impl NamespaceRateLimiter {
    /// Build a limiter from a static local policy.
    pub fn new(policy: RateLimitPolicy) -> Self {
        Self::with_max_cells(policy, MAX_NAMESPACE_CELLS)
    }

    /// Build a limiter with an explicit cardinality cap (see
    /// [`MAX_NAMESPACE_CELLS`]); [`new`](Self::new) uses the default.
    ///
    /// Overrides are pre-created so each is honored regardless of arrival order
    /// and never lost to the cap, and a shared overflow cell is prepared when
    /// the default limits anything.
    fn with_max_cells(policy: RateLimitPolicy, max_cells: usize) -> Self {
        let mut cells = HashMap::new();
        for (name, limits) in &policy.overrides {
            if !limits.is_empty() {
                cells.insert(
                    NamespaceId::named(name.clone()),
                    NamespaceCells::from_limits(limits),
                );
            }
        }
        let overflow =
            (!policy.default.is_empty()).then(|| NamespaceCells::from_limits(&policy.default));
        Self {
            policy,
            origin: Instant::now(),
            cells: RwLock::new(cells),
            overflow,
            max_cells,
        }
    }

    /// A limiter that admits every request (the feature disabled).
    pub fn disabled() -> Self {
        Self::new(RateLimitPolicy::default())
    }

    /// Whether the limiter enforces anything.
    pub fn is_enabled(&self) -> bool {
        self.policy.enabled
    }

    /// Charge one read request expected to return `read_bytes` bytes against
    /// `namespace`, returning [`Throttled`] if it would exceed a configured limit.
    pub fn admit(&self, namespace: &NamespaceId, read_bytes: u64) -> Result<(), Throttled> {
        // Skip the clock read entirely when the feature is off.
        if !self.policy.enabled {
            return Ok(());
        }
        self.admit_at(namespace, read_bytes, self.now_nanos())
    }

    /// Time-injected core of [`admit`](Self::admit).
    ///
    /// read_iops is charged before read_throughput; a request rejected on
    /// throughput has already consumed its one read-iops credit, which only
    /// makes the limiter more conservative under overload.
    fn admit_at(
        &self,
        namespace: &NamespaceId,
        read_bytes: u64,
        now_nanos: u64,
    ) -> Result<(), Throttled> {
        if !self.policy.enabled {
            return Ok(());
        }
        let default = namespace.is_unattributed().then(NamespaceId::default);
        let namespace = default.as_ref().unwrap_or(namespace);
        // Fast path: a warm namespace (an override or a previously-seen
        // default-governed namespace) is charged in place under a shared read lock,
        // so admits for different namespaces never block each other and the
        // same-namespace case is a single atomic CAS with no allocation or clone.
        if let Some(cells) = self.cells.read().unwrap().get(namespace) {
            return cells.charge(read_bytes, now_nanos);
        }
        // Unseen namespace. If nothing limits it, admit without allocating a cell;
        // this also keeps an empty default from ever growing the map.
        let limits = self.policy.limits_for(namespace);
        if limits.is_empty() {
            return Ok(());
        }
        // Otherwise give it a dedicated cell — unless the cardinality cap is
        // reached, in which case fall back to the shared overflow cell so the
        // unauthenticated map cannot grow without bound.
        let mut map = self.cells.write().unwrap();
        // Re-check under the write lock: another thread may have inserted.
        if let Some(cells) = map.get(namespace) {
            return cells.charge(read_bytes, now_nanos);
        }
        if map.len() < self.max_cells {
            return map
                .entry(namespace.clone())
                .or_insert_with(|| NamespaceCells::from_limits(&limits))
                .charge(read_bytes, now_nanos);
        }
        drop(map);
        match &self.overflow {
            Some(overflow) => overflow.charge(read_bytes, now_nanos),
            None => Ok(()),
        }
    }

    #[cfg(test)]
    fn cell_count(&self) -> usize {
        self.cells.read().unwrap().len()
    }

    fn now_nanos(&self) -> u64 {
        Instant::now()
            .saturating_duration_since(self.origin)
            .as_nanos()
            .min(u64::MAX as u128) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::RateLimit;

    fn iops_policy(rate: u64, burst_ratio: f64) -> RateLimitPolicy {
        RateLimitPolicy {
            enabled: true,
            default: MetricLimits {
                read_iops: Some(RateLimit::new(rate, burst_ratio).unwrap()),
                read_throughput: None,
            },
            overrides: HashMap::new(),
        }
    }

    #[test]
    fn disabled_admits_everything() {
        let limiter = NamespaceRateLimiter::disabled();
        for _ in 0..1000 {
            assert!(limiter.admit(&NamespaceId::named("a"), u64::MAX).is_ok());
        }
    }

    #[test]
    fn read_iops_admits_a_burst_then_throttles() {
        // rate 10, burst_ratio 1.0 => 10 units of burst.
        let limiter = NamespaceRateLimiter::new(iops_policy(10, 1.0));
        let namespace = NamespaceId::named("a");
        for _ in 0..10 {
            assert!(limiter.admit_at(&namespace, 0, 0).is_ok());
        }
        let throttled = limiter.admit_at(&namespace, 0, 0).unwrap_err();
        assert_eq!(throttled.metric, RateMetric::ReadIops);
        assert_eq!(throttled.retry_after, Duration::from_millis(100));
    }

    #[test]
    fn namespaces_are_limited_independently() {
        // rate 1, burst_ratio 1.0 => one request then pacing.
        let limiter = NamespaceRateLimiter::new(iops_policy(1, 1.0));
        assert!(limiter.admit_at(&NamespaceId::named("a"), 0, 0).is_ok());
        // A different namespace has its own fresh burst.
        assert!(limiter.admit_at(&NamespaceId::named("b"), 0, 0).is_ok());
        // ...but the first namespace is now paced.
        assert!(limiter.admit_at(&NamespaceId::named("a"), 0, 0).is_err());
    }

    #[test]
    fn unattributed_uses_the_default_limit() {
        let limiter = NamespaceRateLimiter::new(iops_policy(1, 1.0));
        assert!(limiter.admit_at(&NamespaceId::unattributed(), 0, 0).is_ok());
        assert!(limiter
            .admit_at(&NamespaceId::unattributed(), 0, 0)
            .is_err());
        assert!(limiter.admit_at(&NamespaceId::default(), 0, 0).is_err());
    }

    #[test]
    fn overrides_take_precedence_over_the_default() {
        let mut policy = iops_policy(1, 1.0);
        policy.overrides.insert(
            "vip".to_string(),
            MetricLimits {
                read_iops: Some(RateLimit::new(10, 1.0).unwrap()),
                read_throughput: None,
            },
        );
        let limiter = NamespaceRateLimiter::new(policy);
        // The default namespace is paced after one request...
        assert!(limiter.admit_at(&NamespaceId::named("a"), 0, 0).is_ok());
        assert!(limiter.admit_at(&NamespaceId::named("a"), 0, 0).is_err());
        // ...while the override grants a larger burst.
        for _ in 0..10 {
            assert!(limiter.admit_at(&NamespaceId::named("vip"), 0, 0).is_ok());
        }
        assert!(limiter.admit_at(&NamespaceId::named("vip"), 0, 0).is_err());
    }

    #[test]
    fn throughput_limit_throttles_on_bytes() {
        let policy = RateLimitPolicy {
            enabled: true,
            default: MetricLimits {
                read_iops: None,
                read_throughput: Some(RateLimit::new(1_000_000, 1.0).unwrap()),
            },
            overrides: HashMap::new(),
        };
        let limiter = NamespaceRateLimiter::new(policy);
        let namespace = NamespaceId::named("a");
        // One request draining the whole burst is admitted...
        assert!(limiter.admit_at(&namespace, 1_000_000, 0).is_ok());
        // ...the next byte is throttled on throughput, not iops.
        let throttled = limiter.admit_at(&namespace, 1, 0).unwrap_err();
        assert_eq!(throttled.metric, RateMetric::ReadThroughput);
    }

    #[test]
    fn caps_the_sustained_rate_under_overload() {
        // Capability: at read_iops rate 1000/s (burst 1000), offer 10x the rate
        // across a simulated second and confirm the admitted count is the burst
        // plus one second of the sustained rate (~2000), not the 10_000 offered.
        let limiter = NamespaceRateLimiter::new(iops_policy(1000, 1.0));
        let namespace = NamespaceId::named("a");
        let offered = 10_000u64;
        let mut admitted = 0u64;
        // 1000 steps of 1ms across ~1s; 10 requests offered per step.
        for step in 0..1000u64 {
            let now = step * 1_000_000; // ns
            for _ in 0..10 {
                if limiter.admit_at(&namespace, 0, now).is_ok() {
                    admitted += 1;
                }
            }
        }
        // The rate is capped far below the offered load...
        assert!(
            admitted < offered / 2,
            "admitted {admitted} of {offered} offered"
        );
        // ...and close to burst (1000) + one second of rate (1000) = ~2000.
        assert!(
            (1800..=2200).contains(&admitted),
            "admitted {admitted}, expected ~2000 (burst + 1s of rate)"
        );
    }

    // Build an iops-only override for a single namespace tier.
    fn iops_override(rate: u64, burst_ratio: f64) -> MetricLimits {
        MetricLimits {
            read_iops: Some(RateLimit::new(rate, burst_ratio).unwrap()),
            read_throughput: None,
        }
    }

    #[test]
    fn caps_each_namespace_independently_under_concurrent_overload() {
        // Multi-namespace capability: three namespaces with *different* read_iops
        // limits all offer far more than their share at every simulated instant.
        // Each must be capped at its own rate — a greedy namespace never borrows
        // another's budget, and a small namespace is not dragged up by a large one.
        let mut policy = iops_policy(1000, 1.0); // "a" falls back to this default
        policy
            .overrides
            .insert("vip".to_string(), iops_override(5000, 1.0));
        policy
            .overrides
            .insert("tiny".to_string(), iops_override(100, 1.0));
        let limiter = NamespaceRateLimiter::new(policy);

        // (namespace, configured rate/s).
        let namespaces = [
            (NamespaceId::named("a"), 1000u64),
            (NamespaceId::named("vip"), 5000),
            (NamespaceId::named("tiny"), 100),
        ];
        let mut admitted = [0u64; 3];
        // 1000 steps of 1ms across ~1s. At every step *each* namespace offers 20
        // requests (>> even vip's per-ms share), so all three are overloaded at
        // the same instants — the interleaving models simultaneous callers.
        let offered_per_namespace = 1000u64 * 20;
        for step in 0..1000u64 {
            let now = step * 1_000_000; // ns
            for (i, (namespace, _)) in namespaces.iter().enumerate() {
                for _ in 0..20 {
                    if limiter.admit_at(namespace, 0, now).is_ok() {
                        admitted[i] += 1;
                    }
                }
            }
        }

        for (i, (namespace, rate)) in namespaces.iter().enumerate() {
            // Each namespace is capped near burst (1x rate) + one second of its own
            // rate (1x rate) = ~2x rate, independent of the others.
            let expected = 2 * rate;
            let (lo, hi) = (expected * 9 / 10, expected * 11 / 10);
            assert!(
                (lo..=hi).contains(&admitted[i]),
                "namespace {namespace} admitted {} of {offered_per_namespace} offered, \
                 expected ~{expected} ({lo}..={hi})",
                admitted[i],
            );
        }
    }

    #[test]
    fn isolates_namespace_rates_under_real_thread_contention() {
        // Same guarantee, but under genuine concurrency: one OS thread per
        // namespace hammers the *shared* limiter with its own id for a fixed
        // window. This exercises the lock-free cells under contention and
        // confirms each namespace is throttled near its own rate — nowhere near the
        // millions of calls each thread actually offers.
        let mut policy = iops_policy(2000, 1.0);
        policy
            .overrides
            .insert("vip".to_string(), iops_override(8000, 1.0));
        let limiter = std::sync::Arc::new(NamespaceRateLimiter::new(policy));

        let window = Duration::from_millis(300);
        let namespaces = [("a", 2000u64), ("vip", 8000u64)];
        let handles: Vec<_> = namespaces
            .iter()
            .map(|&(name, rate)| {
                let limiter = std::sync::Arc::clone(&limiter);
                let namespace = NamespaceId::named(name);
                std::thread::spawn(move || {
                    let start = Instant::now();
                    let mut admitted = 0u64;
                    // Tight batches so the offered rate dwarfs the limit; count
                    // only what the limiter lets through.
                    while start.elapsed() < window {
                        for _ in 0..64 {
                            if limiter.admit(&namespace, 0).is_ok() {
                                admitted += 1;
                            }
                        }
                    }
                    (rate, admitted, start.elapsed())
                })
            })
            .collect();

        let mut results = Vec::new();
        for h in handles {
            results.push(h.join().unwrap());
        }

        for &(rate, admitted, elapsed) in &results {
            // Expected ~ burst (1x rate) + rate * elapsed. Loose absolute bounds
            // absorb scheduler jitter while still proving the cap holds: raw
            // throughput over 300ms is in the millions, so [0.5x, 3x] rate can
            // only be reached by real limiting.
            let secs = elapsed.as_secs_f64();
            let expected = rate as f64 * (1.0 + secs);
            let (lo, hi) = ((rate / 2), (rate * 3));
            assert!(
                admitted >= lo && admitted <= hi,
                "rate {rate}: admitted {admitted}, expected ~{expected:.0} \
                 ({lo}..={hi}) over {secs:.3}s",
            );
        }

        // Isolation is also visible in the ratio: both tiers ran the same
        // window, so the 8000 namespace admits ~4x the 2000 namespace (burst and rate
        // both scale with the tier), never the same amount.
        let a = results.iter().find(|r| r.0 == 2000).unwrap().1;
        let vip = results.iter().find(|r| r.0 == 8000).unwrap().1;
        assert!(
            vip > a * 2,
            "vip (8000/s) admitted {vip} should far exceed a (2000/s) {a}",
        );
    }

    #[test]
    fn empty_limits_admit_without_allocating_a_cell() {
        // Enabled, but nothing configured (empty default, no overrides): every
        // namespace is admitted and no cell is ever created, so distinct namespace
        // names cannot grow worker memory.
        let limiter = NamespaceRateLimiter::new(RateLimitPolicy {
            enabled: true,
            default: MetricLimits::default(),
            overrides: HashMap::new(),
        });
        for i in 0..1000 {
            assert!(limiter
                .admit_at(&NamespaceId::named(format!("t{i}")), 0, 0)
                .is_ok());
        }
        assert_eq!(limiter.cell_count(), 0);
    }

    #[test]
    fn bounds_cardinality_and_overflows_to_a_shared_cell() {
        // A non-empty default with a tiny cap: only `max_cells` distinct namespaces
        // get their own cell; further namespaces share the overflow cell, so the
        // unauthenticated map cannot grow past the cap.
        let limiter = NamespaceRateLimiter::with_max_cells(iops_policy(1, 1.0), 2);
        // Two distinct namespaces each get a dedicated cell (fresh burst each).
        assert!(limiter.admit_at(&NamespaceId::named("t0"), 0, 0).is_ok());
        assert!(limiter.admit_at(&NamespaceId::named("t1"), 0, 0).is_ok());
        assert_eq!(limiter.cell_count(), 2);
        // A third distinct namespace does not grow the map — it shares the overflow
        // cell, and is admitted on the overflow cell's own fresh burst...
        assert!(limiter.admit_at(&NamespaceId::named("t2"), 0, 0).is_ok());
        assert_eq!(limiter.cell_count(), 2);
        // ...while a fourth overflow namespace is throttled, because it shares that
        // now-drained overflow cell (rate 1) rather than getting its own burst.
        assert!(limiter.admit_at(&NamespaceId::named("t3"), 0, 0).is_err());
        assert_eq!(limiter.cell_count(), 2);
    }

    #[test]
    fn overrides_are_pre_created_and_exempt_from_the_cap() {
        // With the cap already reached by dynamic namespaces, a configured override
        // is still honored (it was pre-created), not shunted to the overflow.
        let mut policy = iops_policy(1, 1.0);
        policy.overrides.insert(
            "vip".to_string(),
            MetricLimits {
                read_iops: Some(talon_core::RateLimit::new(10, 1.0).unwrap()),
                read_throughput: None,
            },
        );
        // Cap of 1, already consumed by the pre-created override cell.
        let limiter = NamespaceRateLimiter::with_max_cells(policy, 1);
        assert_eq!(limiter.cell_count(), 1);
        // A dynamic default namespace is already over the cap, so it shares the
        // overflow cell rather than getting its own (the map does not grow)...
        assert!(limiter.admit_at(&NamespaceId::named("t0"), 0, 0).is_ok());
        assert_eq!(limiter.cell_count(), 1);
        // ...while the override keeps its full rate-10 burst on its own cell.
        for _ in 0..10 {
            assert!(limiter.admit_at(&NamespaceId::named("vip"), 0, 0).is_ok());
        }
        assert!(limiter.admit_at(&NamespaceId::named("vip"), 0, 0).is_err());
    }
}
