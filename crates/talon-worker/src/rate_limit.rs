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

/// Worker-global, `Arc`-shared per-tenant limiter.
#[derive(Debug)]
pub struct TenantRateLimiter {
    policy: RateLimitPolicy,
    /// Monotonic clock origin; GCRA cells advance in nanoseconds from here.
    origin: Instant,
    /// Lazily-created per-tenant cells. A read lock covers the common warm
    /// lookup; the write lock is taken only the first time a tenant is seen.
    cells: RwLock<HashMap<TenantId, TenantCells>>,
}

impl TenantRateLimiter {
    /// Build a limiter from a static local policy.
    pub fn new(policy: RateLimitPolicy) -> Self {
        Self {
            policy,
            origin: Instant::now(),
            cells: RwLock::new(HashMap::new()),
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
        // Fast path: a warm tenant is charged in place under a shared read lock,
        // so admits for different tenants never block each other and the
        // same-tenant case is a single atomic CAS with no allocation or clone.
        if let Some(cells) = self.cells.read().unwrap().get(tenant) {
            return cells.charge(read_bytes, now_nanos);
        }
        // Slow path (first time this tenant is seen): insert, then charge. The
        // brief write lock is paid once per tenant, not per request.
        let mut map = self.cells.write().unwrap();
        map.entry(tenant.clone())
            .or_insert_with(|| TenantCells::from_limits(&self.policy.limits_for(tenant)))
            .charge(read_bytes, now_nanos)
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
}
