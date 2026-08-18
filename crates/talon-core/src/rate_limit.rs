//! Per-tenant rate-limit primitives shared by every enforcement tier.
//!
//! This module holds the tier-agnostic building blocks — the metrics a policy
//! is expressed over, a single metric's `(rate, burst)` limit, and a lock-free
//! GCRA cell that admits or throttles one stream of requests. Where the limit is
//! *enforced* (the object-store gateway for authenticated tenants, the worker
//! for direct clients) is a property of the caller, not of these primitives.
//!
//! The cell is a Generic Cell Rate Algorithm meter: its entire state is one
//! "theoretical arrival time" (TAT) timestamp held in a single [`AtomicU64`], so
//! concurrent callers admit or throttle with a single compare-and-swap and never
//! serialize on a lock. That property is what lets the data-plane rings (and the
//! gateway's request tasks) charge a shared per-tenant limit without contention.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// A rate-limited traffic dimension for one tenant.
///
/// Costs are expressed in the metric's natural unit: one request for
/// [`RateMetric::ReadIops`], one byte for the byte metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RateMetric {
    /// Client read requests admitted (cost is one request).
    ReadIops,
    /// Bytes sent from the serving tier to the client (cost is bytes).
    ClientEgressBytes,
    /// Bytes fetched from the object-store origin on a cache miss (cost is bytes).
    OriginReadBytes,
}

impl RateMetric {
    /// Every metric, in a stable order for iteration and per-metric tables.
    pub const ALL: [RateMetric; 3] = [
        RateMetric::ReadIops,
        RateMetric::ClientEgressBytes,
        RateMetric::OriginReadBytes,
    ];

    /// Stable, low-cardinality label for telemetry and configuration keys.
    pub const fn label(self) -> &'static str {
        match self {
            RateMetric::ReadIops => "read_iops",
            RateMetric::ClientEgressBytes => "client_egress_bytes",
            RateMetric::OriginReadBytes => "origin_read_bytes",
        }
    }
}

impl std::fmt::Display for RateMetric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// A single metric's sustained rate and burst allowance.
///
/// `rate` is the long-run ceiling in units per second; `burst` is the largest
/// instantaneous backlog, in the same units, admitted before pacing kicks in.
/// Both must be non-zero — an absent limit is modelled by the caller as "no
/// [`RateLimit`] configured for this metric", not by a zero here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateLimit {
    /// Sustained ceiling in units per second.
    pub rate: u64,
    /// Maximum burst, in units, admitted ahead of the sustained rate.
    pub burst: u64,
}

impl RateLimit {
    /// Construct a limit, returning an error for a non-positive rate or burst.
    pub fn new(rate: u64, burst: u64) -> Result<Self, RateLimitError> {
        if rate == 0 {
            return Err(RateLimitError::ZeroRate);
        }
        if burst == 0 {
            return Err(RateLimitError::ZeroBurst);
        }
        Ok(Self { rate, burst })
    }
}

/// Why a [`RateLimit`] is invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RateLimitError {
    /// The sustained rate was zero; use "no limit configured" instead.
    #[error("rate limit rate must be non-zero")]
    ZeroRate,
    /// The burst allowance was zero; a limit must admit at least one unit.
    #[error("rate limit burst must be non-zero")]
    ZeroBurst,
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
/// The cell admits a burst of up to `burst` units instantaneously and then
/// paces admissions at `rate` units per second. All state is one atomic TAT, so
/// charging is a single compare-and-swap; time is supplied by the caller (as
/// monotonic nanoseconds from a fixed origin) so the algorithm is deterministic
/// and testable.
#[derive(Debug)]
pub struct Gcra {
    /// Theoretical arrival time, in nanoseconds from the caller's clock origin.
    tat_nanos: AtomicU64,
    /// Emission interval: nanoseconds of TAT advance per unit of cost.
    interval_nanos: u64,
    /// Tolerance: how far ahead of "now" the TAT may sit (i.e. `burst` units).
    tolerance_nanos: u64,
}

impl Gcra {
    /// Build a fresh, empty cell for the given limit.
    pub fn new(limit: RateLimit) -> Self {
        // Nanoseconds of credit per unit, and the burst window in those units.
        // Both are clamped to at least 1ns so a single unit always advances the
        // TAT and an admitted cost is never a no-op.
        let interval_nanos = (NANOS_PER_SEC / limit.rate).max(1);
        let tolerance_nanos =
            (limit.burst as u128 * interval_nanos as u128).min(u64::MAX as u128) as u64;
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

    #[test]
    fn metric_labels_are_stable_and_unique() {
        let labels: Vec<_> = RateMetric::ALL.iter().map(|m| m.label()).collect();
        assert_eq!(
            labels,
            ["read_iops", "client_egress_bytes", "origin_read_bytes"]
        );
        assert_eq!(RateMetric::ReadIops.to_string(), "read_iops");
    }

    #[test]
    fn rate_limit_rejects_zero() {
        assert_eq!(RateLimit::new(0, 1), Err(RateLimitError::ZeroRate));
        assert_eq!(RateLimit::new(1, 0), Err(RateLimitError::ZeroBurst));
        assert!(RateLimit::new(100, 10).is_ok());
    }

    #[test]
    fn admits_a_full_burst_then_throttles() {
        // 100 units/sec, burst 10: ten immediate admits at t=0, eleventh paces.
        let cell = Gcra::new(RateLimit::new(100, 10).unwrap());
        for _ in 0..10 {
            assert_eq!(cell.charge_at(1, 0), RateDecision::Admitted);
        }
        match cell.charge_at(1, 0) {
            RateDecision::Throttled { retry_after } => {
                // One emission interval = 1/100 s = 10ms.
                assert_eq!(retry_after, Duration::from_millis(10));
            }
            other => panic!("expected throttle, got {other:?}"),
        }
    }

    #[test]
    fn refills_at_the_configured_rate() {
        let cell = Gcra::new(RateLimit::new(100, 10).unwrap());
        for _ in 0..10 {
            assert!(cell.charge_at(1, 0).is_admitted());
        }
        // Still throttled just before one interval elapses...
        assert!(!cell.charge_at(1, 9_999_999).is_admitted());
        // ...and admitted once a full 10ms interval has passed.
        assert!(cell.charge_at(1, 10_000_000).is_admitted());
    }

    #[test]
    fn cost_scales_with_units_for_byte_metrics() {
        // 1000 bytes/sec, burst 1000 bytes: one 1000-byte admit drains the burst.
        let cell = Gcra::new(RateLimit::new(1000, 1000).unwrap());
        assert!(cell.charge_at(1000, 0).is_admitted());
        assert!(!cell.charge_at(1, 0).is_admitted());
        // After 1s, the whole burst has refilled.
        assert!(cell.charge_at(1000, NANOS_PER_SEC).is_admitted());
    }

    #[test]
    fn zero_cost_never_throttles_or_advances() {
        let cell = Gcra::new(RateLimit::new(1, 1).unwrap());
        assert_eq!(cell.charge_at(0, 0), RateDecision::Admitted);
        // The one unit of burst is still available afterwards.
        assert!(cell.charge_at(1, 0).is_admitted());
    }

    #[test]
    fn throttle_delay_grows_with_backlog() {
        // burst 1 so the cell paces immediately; a 5-unit cost needs ~5 intervals.
        let cell = Gcra::new(RateLimit::new(100, 1).unwrap());
        assert!(cell.charge_at(1, 0).is_admitted());
        match cell.charge_at(5, 0) {
            RateDecision::Throttled { retry_after } => {
                // Need 5 more units of credit beyond the 1-unit tolerance.
                assert_eq!(retry_after, Duration::from_millis(50));
            }
            other => panic!("expected throttle, got {other:?}"),
        }
    }
}
