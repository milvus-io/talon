//! Worker-side per-tenant rate limiting.
//!
//! A [`TenantRateLimiter`] enforces a static, per-worker local policy: for each
//! tenant it holds one lock-free GCRA cell per limited metric and admits or
//! throttles a request with a single atomic op, so the io_uring rings never
//! serialize on a global QoS lock — only same-tenant requests briefly touch the
//! same cell.
//!
//! This is the local, single-worker stage of the eventual global design: limits
//! are per worker and per process, sized so the fleet aggregate stays bounded.
//! Cross-worker owners and snapshot propagation are a later stage, as is
//! enforcing [`RateMetric::OriginReadBytes`] (which needs the tenant threaded
//! into the cache-miss path).

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use talon_core::{Gcra, MetricLimits, RateDecision, RateLimitPolicy, RateMetric, TenantId};

/// One tenant's GCRA cells, one per limited metric.
#[derive(Debug)]
struct TenantCells {
    read_iops: Option<Gcra>,
    read_throughput: Option<Gcra>,
}

impl TenantCells {
    fn from_limits(limits: &MetricLimits) -> Self {
        Self {
            read_iops: limits.read_iops.map(Gcra::new),
            read_throughput: limits.read_throughput.map(Gcra::new),
        }
    }

    /// Charge one read of `read_bytes` against this tenant's cells.
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

/// The maximum number of distinct tenants that get their own dedicated GCRA
/// cells on one worker.
///
/// The direct data plane is unauthenticated, so a client can declare
/// arbitrarily many distinct tenant names; without a ceiling the cell map would
/// grow without bound — a memory-exhaustion vector. Tenants first seen beyond
/// this cap share a single overflow cell governed by the default limit, so
/// memory stays bounded while the (almost certainly abusive or misconfigured)
/// excess is still paced. Configured overrides are pre-created and never lost to
/// the cap.
const MAX_TENANT_CELLS: usize = 4096;

/// Worker-global, `Arc`-shared per-tenant limiter.
#[derive(Debug)]
pub struct TenantRateLimiter {
    policy: RateLimitPolicy,
    /// Monotonic clock origin; GCRA cells advance in nanoseconds from here.
    origin: Instant,
    /// Per-tenant cells: created eagerly for configured overrides and lazily for
    /// default-governed tenants. A read lock covers the common warm lookup; the
    /// write lock is taken only the first time a default-governed tenant is
    /// seen. Bounded at `max_cells` entries (see [`MAX_TENANT_CELLS`]).
    cells: RwLock<HashMap<TenantId, TenantCells>>,
    /// Shared fallback cell for tenants first seen after the cap is reached,
    /// governed by the default limit. `None` when the default limits nothing
    /// (then default-governed tenants are admitted without a cell at all).
    overflow: Option<TenantCells>,
    /// Ceiling on the number of distinct tenant cells (see [`MAX_TENANT_CELLS`]).
    max_cells: usize,
}

impl TenantRateLimiter {
    /// Build a limiter from a static local policy.
    pub fn new(policy: RateLimitPolicy) -> Self {
        Self::with_max_cells(policy, MAX_TENANT_CELLS)
    }

    /// Build a limiter with an explicit cardinality cap (see
    /// [`MAX_TENANT_CELLS`]); [`new`](Self::new) uses the default.
    ///
    /// Overrides are pre-created so each is honored regardless of arrival order
    /// and never lost to the cap, and a shared overflow cell is prepared when
    /// the default limits anything.
    fn with_max_cells(policy: RateLimitPolicy, max_cells: usize) -> Self {
        let mut cells = HashMap::new();
        for (name, limits) in &policy.overrides {
            if !limits.is_empty() {
                cells.insert(
                    TenantId::named(name.clone()),
                    TenantCells::from_limits(limits),
                );
            }
        }
        let overflow =
            (!policy.default.is_empty()).then(|| TenantCells::from_limits(&policy.default));
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
    /// `tenant`, returning [`Throttled`] if it would exceed a configured limit.
    pub fn admit(&self, tenant: &TenantId, read_bytes: u64) -> Result<(), Throttled> {
        // Skip the clock read entirely when the feature is off.
        if !self.policy.enabled {
            return Ok(());
        }
        self.admit_at(tenant, read_bytes, self.now_nanos())
    }

    /// Time-injected core of [`admit`](Self::admit).
    ///
    /// read_iops is charged before read_throughput; a request rejected on
    /// throughput has already consumed its one read-iops credit, which only
    /// makes the limiter more conservative under overload.
    fn admit_at(
        &self,
        tenant: &TenantId,
        read_bytes: u64,
        now_nanos: u64,
    ) -> Result<(), Throttled> {
        if !self.policy.enabled {
            return Ok(());
        }
        // Fast path: a warm tenant (an override or a previously-seen
        // default-governed tenant) is charged in place under a shared read lock,
        // so admits for different tenants never block each other and the
        // same-tenant case is a single atomic CAS with no allocation or clone.
        if let Some(cells) = self.cells.read().unwrap().get(tenant) {
            return cells.charge(read_bytes, now_nanos);
        }
        // Unseen tenant. If nothing limits it, admit without allocating a cell;
        // this also keeps an empty default from ever growing the map.
        let limits = self.policy.limits_for(tenant);
        if limits.is_empty() {
            return Ok(());
        }
        // Otherwise give it a dedicated cell — unless the cardinality cap is
        // reached, in which case fall back to the shared overflow cell so the
        // unauthenticated map cannot grow without bound.
        let mut map = self.cells.write().unwrap();
        // Re-check under the write lock: another thread may have inserted.
        if let Some(cells) = map.get(tenant) {
            return cells.charge(read_bytes, now_nanos);
        }
        if map.len() < self.max_cells {
            return map
                .entry(tenant.clone())
                .or_insert_with(|| TenantCells::from_limits(&limits))
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
        let limiter = TenantRateLimiter::disabled();
        for _ in 0..1000 {
            assert!(limiter.admit(&TenantId::named("a"), u64::MAX).is_ok());
        }
    }

    #[test]
    fn read_iops_admits_a_burst_then_throttles() {
        // rate 10, burst_ratio 1.0 => 10 units of burst.
        let limiter = TenantRateLimiter::new(iops_policy(10, 1.0));
        let tenant = TenantId::named("a");
        for _ in 0..10 {
            assert!(limiter.admit_at(&tenant, 0, 0).is_ok());
        }
        let throttled = limiter.admit_at(&tenant, 0, 0).unwrap_err();
        assert_eq!(throttled.metric, RateMetric::ReadIops);
        assert_eq!(throttled.retry_after, Duration::from_millis(100));
    }

    #[test]
    fn tenants_are_limited_independently() {
        // rate 1, burst_ratio 1.0 => one request then pacing.
        let limiter = TenantRateLimiter::new(iops_policy(1, 1.0));
        assert!(limiter.admit_at(&TenantId::named("a"), 0, 0).is_ok());
        // A different tenant has its own fresh burst.
        assert!(limiter.admit_at(&TenantId::named("b"), 0, 0).is_ok());
        // ...but the first tenant is now paced.
        assert!(limiter.admit_at(&TenantId::named("a"), 0, 0).is_err());
    }

    #[test]
    fn unattributed_uses_the_default_limit() {
        let limiter = TenantRateLimiter::new(iops_policy(1, 1.0));
        assert!(limiter.admit_at(&TenantId::unattributed(), 0, 0).is_ok());
        assert!(limiter.admit_at(&TenantId::unattributed(), 0, 0).is_err());
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
        let limiter = TenantRateLimiter::new(policy);
        // The default tenant is paced after one request...
        assert!(limiter.admit_at(&TenantId::named("a"), 0, 0).is_ok());
        assert!(limiter.admit_at(&TenantId::named("a"), 0, 0).is_err());
        // ...while the override grants a larger burst.
        for _ in 0..10 {
            assert!(limiter.admit_at(&TenantId::named("vip"), 0, 0).is_ok());
        }
        assert!(limiter.admit_at(&TenantId::named("vip"), 0, 0).is_err());
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
        let limiter = TenantRateLimiter::new(policy);
        let tenant = TenantId::named("a");
        // One request draining the whole burst is admitted...
        assert!(limiter.admit_at(&tenant, 1_000_000, 0).is_ok());
        // ...the next byte is throttled on throughput, not iops.
        let throttled = limiter.admit_at(&tenant, 1, 0).unwrap_err();
        assert_eq!(throttled.metric, RateMetric::ReadThroughput);
    }

    #[test]
    fn caps_the_sustained_rate_under_overload() {
        // Capability: at read_iops rate 1000/s (burst 1000), offer 10x the rate
        // across a simulated second and confirm the admitted count is the burst
        // plus one second of the sustained rate (~2000), not the 10_000 offered.
        let limiter = TenantRateLimiter::new(iops_policy(1000, 1.0));
        let tenant = TenantId::named("a");
        let offered = 10_000u64;
        let mut admitted = 0u64;
        // 1000 steps of 1ms across ~1s; 10 requests offered per step.
        for step in 0..1000u64 {
            let now = step * 1_000_000; // ns
            for _ in 0..10 {
                if limiter.admit_at(&tenant, 0, now).is_ok() {
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

    #[test]
    fn empty_limits_admit_without_allocating_a_cell() {
        // Enabled, but nothing configured (empty default, no overrides): every
        // tenant is admitted and no cell is ever created, so distinct tenant
        // names cannot grow worker memory.
        let limiter = TenantRateLimiter::new(RateLimitPolicy {
            enabled: true,
            default: MetricLimits::default(),
            overrides: HashMap::new(),
        });
        for i in 0..1000 {
            assert!(limiter
                .admit_at(&TenantId::named(format!("t{i}")), 0, 0)
                .is_ok());
        }
        assert_eq!(limiter.cell_count(), 0);
    }

    #[test]
    fn bounds_cardinality_and_overflows_to_a_shared_cell() {
        // A non-empty default with a tiny cap: only `max_cells` distinct tenants
        // get their own cell; further tenants share the overflow cell, so the
        // unauthenticated map cannot grow past the cap.
        let limiter = TenantRateLimiter::with_max_cells(iops_policy(1, 1.0), 2);
        // Two distinct tenants each get a dedicated cell (fresh burst each).
        assert!(limiter.admit_at(&TenantId::named("t0"), 0, 0).is_ok());
        assert!(limiter.admit_at(&TenantId::named("t1"), 0, 0).is_ok());
        assert_eq!(limiter.cell_count(), 2);
        // A third distinct tenant does not grow the map — it shares the overflow
        // cell, and is admitted on the overflow cell's own fresh burst...
        assert!(limiter.admit_at(&TenantId::named("t2"), 0, 0).is_ok());
        assert_eq!(limiter.cell_count(), 2);
        // ...while a fourth overflow tenant is throttled, because it shares that
        // now-drained overflow cell (rate 1) rather than getting its own burst.
        assert!(limiter.admit_at(&TenantId::named("t3"), 0, 0).is_err());
        assert_eq!(limiter.cell_count(), 2);
    }

    #[test]
    fn overrides_are_pre_created_and_exempt_from_the_cap() {
        // With the cap already reached by dynamic tenants, a configured override
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
        let limiter = TenantRateLimiter::with_max_cells(policy, 1);
        assert_eq!(limiter.cell_count(), 1);
        // A dynamic default tenant is already over the cap, so it shares the
        // overflow cell rather than getting its own (the map does not grow)...
        assert!(limiter.admit_at(&TenantId::named("t0"), 0, 0).is_ok());
        assert_eq!(limiter.cell_count(), 1);
        // ...while the override keeps its full rate-10 burst on its own cell.
        for _ in 0..10 {
            assert!(limiter.admit_at(&TenantId::named("vip"), 0, 0).is_ok());
        }
        assert!(limiter.admit_at(&TenantId::named("vip"), 0, 0).is_err());
    }
}
