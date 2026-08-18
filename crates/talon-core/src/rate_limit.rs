//! Per-tenant rate-limit primitives shared by every enforcement tier.
//!
//! This module holds the tier-agnostic building blocks — the metrics a policy
//! is expressed over, a single metric's `(rate, burst_ratio)` limit, and a
//! lock-free GCRA cell that admits or throttles one stream of requests. Where
//! the limit is *enforced* (the worker for direct clients, the object-store
//! gateway for its own traffic) is a property of the caller, not of these
//! primitives.
//!
//! The cell is a Generic Cell Rate Algorithm meter: its entire state is one
//! "theoretical arrival time" (TAT) timestamp held in a single [`AtomicU64`], so
//! concurrent callers admit or throttle with a single compare-and-swap and never
//! serialize on a lock. That property is what lets the data-plane rings charge a
//! shared per-tenant limit without contention.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::TenantId;

/// A rate-limited traffic dimension for one tenant.
///
/// Costs are expressed in the metric's natural unit: one request for
/// [`RateMetric::ReadIops`], one byte for the byte-rate metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RateMetric {
    /// Client read requests admitted per second (cost is one request).
    ReadIops,
    /// Bytes per second served to the client — the read throughput / egress
    /// bandwidth (cost is bytes).
    ReadThroughput,
    /// Bytes per second fetched from the object-store origin on a cache miss
    /// (cost is bytes).
    OriginReadBytes,
}

impl RateMetric {
    /// Every metric, in a stable order for iteration and per-metric tables.
    pub const ALL: [RateMetric; 3] = [
        RateMetric::ReadIops,
        RateMetric::ReadThroughput,
        RateMetric::OriginReadBytes,
    ];

    /// Stable, low-cardinality label for telemetry and configuration keys.
    pub const fn label(self) -> &'static str {
        match self {
            RateMetric::ReadIops => "read_iops",
            RateMetric::ReadThroughput => "read_throughput",
            RateMetric::OriginReadBytes => "origin_read_bytes",
        }
    }
}

impl std::fmt::Display for RateMetric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// The default burst window, in seconds, when a limit does not set its own.
///
/// A ratio of 1.0 lets a tenant burst up to one second's worth of its sustained
/// rate before pacing takes over.
pub const DEFAULT_BURST_RATIO: f64 = 1.0;

fn default_burst_ratio() -> f64 {
    DEFAULT_BURST_RATIO
}

/// A single metric's sustained rate and relative burst allowance.
///
/// `rate` is the long-run ceiling in units per second. `burst_ratio` sizes the
/// burst *relative* to that rate: the largest instantaneous backlog admitted
/// before pacing is `rate * burst_ratio` units, i.e. `burst_ratio` seconds of
/// accumulation. It defaults to [`DEFAULT_BURST_RATIO`], so a config may set
/// only `rate`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimit {
    /// Sustained ceiling in units per second.
    pub rate: u64,
    /// Burst window in seconds: burst size is `rate * burst_ratio` units.
    #[serde(default = "default_burst_ratio")]
    pub burst_ratio: f64,
}

impl RateLimit {
    /// Construct a limit, returning an error for a non-positive rate or an
    /// invalid burst ratio (non-finite or not strictly positive).
    pub fn new(rate: u64, burst_ratio: f64) -> Result<Self, RateLimitError> {
        if rate == 0 {
            return Err(RateLimitError::ZeroRate);
        }
        if !burst_ratio.is_finite() || burst_ratio <= 0.0 {
            return Err(RateLimitError::InvalidBurstRatio(burst_ratio));
        }
        Ok(Self { rate, burst_ratio })
    }
}

/// Why a [`RateLimit`] is invalid.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum RateLimitError {
    /// The sustained rate was zero; use "no limit configured" instead.
    #[error("rate limit rate must be non-zero")]
    ZeroRate,
    /// The burst ratio was not a finite, strictly-positive number.
    #[error("rate limit burst_ratio must be finite and positive, got {0}")]
    InvalidBurstRatio(f64),
}

/// The per-metric limits applied to one tenant. A `None` leaves that metric
/// unlimited. Deserialized from a config table such as
/// `read_iops = { rate = 1000, burst_ratio = 2.0 }`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricLimits {
    /// Admitted client read requests per second.
    pub read_iops: Option<RateLimit>,
    /// Read throughput (bytes per second served to the client).
    pub read_throughput: Option<RateLimit>,
}

impl MetricLimits {
    /// Whether any metric is limited.
    pub fn is_empty(&self) -> bool {
        self.read_iops.is_none() && self.read_throughput.is_none()
    }

    /// Validate every configured limit.
    pub fn validate(&self) -> Result<(), RateLimitError> {
        for limit in [self.read_iops, self.read_throughput].into_iter().flatten() {
            RateLimit::new(limit.rate, limit.burst_ratio)?;
        }
        Ok(())
    }
}

/// A static per-worker rate-limit policy: one default applied to every tenant
/// plus optional per-tenant overrides, keyed by tenant name.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RateLimitPolicy {
    /// When false the limiter admits everything (the feature is off).
    pub enabled: bool,
    /// Limits applied to a tenant with no explicit override.
    pub default: MetricLimits,
    /// Per-tenant overrides, keyed by [`TenantId`] name.
    pub overrides: HashMap<String, MetricLimits>,
}

impl RateLimitPolicy {
    /// The limits that apply to `tenant`: its override if present, else the
    /// default. The unattributed tenant always uses the default.
    pub fn limits_for(&self, tenant: &TenantId) -> MetricLimits {
        tenant
            .name()
            .and_then(|name| self.overrides.get(name).copied())
            .unwrap_or(self.default)
    }

    /// Validate every limit in the policy.
    pub fn validate(&self) -> Result<(), RateLimitError> {
        self.default.validate()?;
        for limits in self.overrides.values() {
            limits.validate()?;
        }
        Ok(())
    }
}

/// The outcome of charging a [`Gcra`] cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateDecision {
    /// The request is admitted and the cell has been advanced.
    Admitted,
    /// The request would exceed the limit; retry after this delay. The cell is
    /// unchanged, so a caller that waits and retries is charged exactly once.
    Throttled {
        /// How long until enough capacity exists to admit the same cost.
        retry_after: Duration,
    },
}

impl RateDecision {
    /// Whether the request was admitted.
    pub fn is_admitted(self) -> bool {
        matches!(self, RateDecision::Admitted)
    }
}

const NANOS_PER_SEC: u64 = 1_000_000_000;

/// A lock-free GCRA rate-limit cell for one `(tenant, metric)`.
///
/// The cell admits a burst of up to `rate * burst_ratio` units instantaneously
/// and then paces admissions at `rate` units per second. All state is one atomic
/// TAT, so charging is a single compare-and-swap; time is supplied by the caller
/// (as monotonic nanoseconds from a fixed origin) so the algorithm is
/// deterministic and testable.
#[derive(Debug)]
pub struct Gcra {
    /// Theoretical arrival time, in nanoseconds from the caller's clock origin.
    tat_nanos: AtomicU64,
    /// Emission interval: nanoseconds of TAT advance per unit of cost.
    interval_nanos: u64,
    /// Tolerance: how far ahead of "now" the TAT may sit — `burst_ratio`
    /// seconds, i.e. `rate * burst_ratio` units of credit.
    tolerance_nanos: u64,
}

impl Gcra {
    /// Build a fresh, empty cell for the given limit.
    pub fn new(limit: RateLimit) -> Self {
        // Nanoseconds of credit per unit. Clamped to at least 1ns so a single
        // unit always advances the TAT and an admitted cost is never a no-op.
        let interval_nanos = NANOS_PER_SEC
            .checked_div(limit.rate)
            .unwrap_or(NANOS_PER_SEC)
            .max(1);
        // The burst window is `burst_ratio` seconds of accumulation, which works
        // out to `rate * burst_ratio` units. Clamp to at least one emission
        // interval so a single unit is always admittable, even for a tiny ratio.
        let tolerance_nanos = ((limit.burst_ratio.max(0.0)) * NANOS_PER_SEC as f64) as u64;
        let tolerance_nanos = tolerance_nanos.max(interval_nanos);
        Self {
            tat_nanos: AtomicU64::new(0),
            interval_nanos,
            tolerance_nanos,
        }
    }

    /// Charge `cost` units against the cell at monotonic time `now_nanos`.
    ///
    /// A zero cost is always admitted without touching the cell. On success the
    /// TAT advances by `cost` emission intervals; on throttle the cell is left
    /// unchanged and the returned delay is when the same cost would next fit.
    pub fn charge_at(&self, cost: u64, now_nanos: u64) -> RateDecision {
        if cost == 0 {
            return RateDecision::Admitted;
        }
        // Total TAT advance for this cost, saturating rather than wrapping for
        // pathological (huge cost, tiny rate) combinations.
        let increment = (cost as u128 * self.interval_nanos as u128).min(u64::MAX as u128) as u64;
        loop {
            let tat = self.tat_nanos.load(Ordering::Relaxed);
            let base = tat.max(now_nanos);
            let new_tat = base.saturating_add(increment);
            // Backlog after admitting = new_tat - now. Admit while it fits the
            // burst tolerance.
            if new_tat.saturating_sub(now_nanos) <= self.tolerance_nanos {
                match self.tat_nanos.compare_exchange_weak(
                    tat,
                    new_tat,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return RateDecision::Admitted,
                    // Lost the race; another charge advanced the TAT. Retry.
                    Err(_) => continue,
                }
            }
            // Earliest time the cost fits: when backlog drains to the tolerance.
            let retry = new_tat
                .saturating_sub(self.tolerance_nanos)
                .saturating_sub(now_nanos);
            return RateDecision::Throttled {
                retry_after: Duration::from_nanos(retry),
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit(rate: u64, burst_ratio: f64) -> RateLimit {
        RateLimit::new(rate, burst_ratio).unwrap()
    }

    #[test]
    fn metric_labels_are_stable_and_unique() {
        let labels: Vec<_> = RateMetric::ALL.iter().map(|m| m.label()).collect();
        assert_eq!(
            labels,
            ["read_iops", "read_throughput", "origin_read_bytes"]
        );
        assert_eq!(RateMetric::ReadThroughput.to_string(), "read_throughput");
    }

    #[test]
    fn rate_limit_rejects_invalid_values() {
        assert_eq!(RateLimit::new(0, 1.0), Err(RateLimitError::ZeroRate));
        assert!(matches!(
            RateLimit::new(1, 0.0),
            Err(RateLimitError::InvalidBurstRatio(_))
        ));
        assert!(matches!(
            RateLimit::new(1, f64::NAN),
            Err(RateLimitError::InvalidBurstRatio(_))
        ));
        assert!(RateLimit::new(100, 2.0).is_ok());
    }

    #[test]
    fn burst_ratio_defaults_when_absent_in_config() {
        let limit: RateLimit = toml::from_str("rate = 1000").unwrap();
        assert_eq!(limit.rate, 1000);
        assert_eq!(limit.burst_ratio, DEFAULT_BURST_RATIO);
    }

    #[test]
    fn policy_deserializes_from_a_config_table() {
        let toml = r#"
            enabled = true
            [default]
            read_iops = { rate = 1000, burst_ratio = 2.0 }
            [overrides.acme]
            read_throughput = { rate = 1048576 }
        "#;
        let policy: RateLimitPolicy = toml::from_str(toml).unwrap();
        assert!(policy.enabled);
        assert_eq!(policy.default.read_iops, Some(limit(1000, 2.0)));
        // A named override wins over the default for that tenant, and its
        // throughput limit uses the default burst ratio.
        let acme = policy.limits_for(&TenantId::named("acme"));
        assert_eq!(
            acme.read_throughput,
            Some(limit(1_048_576, DEFAULT_BURST_RATIO))
        );
        assert_eq!(acme.read_iops, None);
        // Any other tenant falls back to the default.
        assert_eq!(policy.limits_for(&TenantId::named("other")), policy.default);
        policy.validate().unwrap();
    }

    #[test]
    fn admits_a_full_burst_then_throttles() {
        // 10 units/sec, burst_ratio 1.0 => 1s window => 10 units of burst.
        let cell = Gcra::new(limit(10, 1.0));
        for _ in 0..10 {
            assert_eq!(cell.charge_at(1, 0), RateDecision::Admitted);
        }
        match cell.charge_at(1, 0) {
            RateDecision::Throttled { retry_after } => {
                // One emission interval = 1/10 s = 100ms.
                assert_eq!(retry_after, Duration::from_millis(100));
            }
            other => panic!("expected throttle, got {other:?}"),
        }
    }

    #[test]
    fn refills_at_the_configured_rate() {
        let cell = Gcra::new(limit(10, 1.0));
        for _ in 0..10 {
            assert!(cell.charge_at(1, 0).is_admitted());
        }
        // Still throttled just before one interval (100ms) elapses...
        assert!(!cell.charge_at(1, 99_999_999).is_admitted());
        // ...and admitted once a full 100ms interval has passed.
        assert!(cell.charge_at(1, 100_000_000).is_admitted());
    }

    #[test]
    fn burst_ratio_scales_the_burst_window() {
        // burst_ratio 2.0 at 10/sec => 20 units of burst before pacing.
        let cell = Gcra::new(limit(10, 2.0));
        for _ in 0..20 {
            assert!(cell.charge_at(1, 0).is_admitted());
        }
        assert!(!cell.charge_at(1, 0).is_admitted());
    }

    #[test]
    fn cost_scales_with_units_for_byte_metrics() {
        // 1000 bytes/sec, burst_ratio 1.0 => 1000 bytes of burst.
        let cell = Gcra::new(limit(1000, 1.0));
        assert!(cell.charge_at(1000, 0).is_admitted());
        assert!(!cell.charge_at(1, 0).is_admitted());
        // After 1s the whole burst has refilled.
        assert!(cell.charge_at(1000, NANOS_PER_SEC).is_admitted());
    }

    #[test]
    fn zero_cost_never_throttles_or_advances() {
        let cell = Gcra::new(limit(1, 1.0));
        assert_eq!(cell.charge_at(0, 0), RateDecision::Admitted);
        // The one unit of burst is still available afterwards.
        assert!(cell.charge_at(1, 0).is_admitted());
    }
}
