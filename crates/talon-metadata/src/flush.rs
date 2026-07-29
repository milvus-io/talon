//! Origin flush: conflict policy and the retry budget (ADR 0003 §9.9).
//!
//! # No policy discards an acknowledged write
//!
//! > There is **no automatic `origin_wins` mode**, exported or otherwise:
//! > automatically making the acknowledged Talon version disappear from its path
//! > would contradict ADR 0002 §3's rejection of configurable read-your-writes.
//!
//! All three policies preserve read-your-writes. `export_then_manual` is the one
//! that looks like an exception and is not: it copies the Talon payload to a
//! protected prefix *and keeps serving it*. The export is evidence for an
//! operator, not a resolution.
//!
//! Discarding acknowledged data is reachable only through an explicit operator
//! action carrying the exact mutation id and a data-loss confirmation flag —
//! never through a policy that a namespace could be configured into.
//!
//! # The retry budget is replicated, not local
//!
//! > Attempt count and classified error are replicated mutation state, so owner
//! > failover or process restart cannot reset the budget and create an
//! > accidental retry-forever loop.
//!
//! A local counter would reset on every failover — and a shard whose owner keeps
//! failing is exactly when failovers happen, so the loop would be unbounded
//! precisely in the case the budget exists to bound.
//!
//! The ADR adds one more constraint that rules out the obvious shortcut:
//!
//! > A new owner recomputes the bounded delay from the persisted attempt count
//! > instead of trusting another worker's wall clock.
//!
//! So the delay is a pure function of the attempt number. Persisting a
//! "next attempt at" timestamp would import the old owner's clock skew.

use core::time::Duration;

/// What to do when the origin rejects a conditional PUT.
///
/// Configured per namespace. Changing it "affects new conflicts only; already
/// parked conflicts require an explicit management operation" — an operator who
/// switches policy is stating an intent for future conflicts, not silently
/// re-deciding ones a human may already be looking at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum ConflictPolicy {
    /// Park the mutation and wait for an operator.
    #[default]
    Manual,
    /// Re-read the origin version and retry one conditional PUT against it.
    TalonWins,
    /// Copy to the conflict prefix for inspection, but keep serving the Talon
    /// version.
    ExportThenManual,
}

impl ConflictPolicy {
    /// Stable identifier for configuration, metrics, and audit records.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::TalonWins => "talon_wins",
            Self::ExportThenManual => "export_then_manual",
        }
    }

    /// Whether the Talon version keeps being served under this policy.
    ///
    /// True for all three. The method exists to make that a testable claim
    /// rather than a comment: a fourth policy that returned false here would be
    /// the `origin_wins` mode the ADR forbids, and the test below would catch
    /// it.
    pub const fn preserves_read_your_writes(self) -> bool {
        match self {
            Self::Manual | Self::TalonWins | Self::ExportThenManual => true,
        }
    }
}

/// What the flusher should do about a conflict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictAction {
    /// Park in `CONFLICT` and alert.
    Park,
    /// Retry one conditional PUT against `observed_version`.
    ///
    /// Exactly one. §9.9: "another concurrent change returns to `CONFLICT`" —
    /// a retry loop here would fight an external writer indefinitely and could
    /// overwrite a version it never saw.
    RetryAgainst {
        /// Origin version the retry is conditioned on.
        observed_version: String,
    },
    /// Export to the conflict prefix, then park. The Talon version keeps serving.
    ExportThenPark,
}

/// Decide what to do about an origin conflict.
///
/// `already_retried` tracks whether this conflict has already consumed its
/// single `talon_wins` retry, which is what stops that policy looping.
pub fn on_conflict(
    policy: ConflictPolicy,
    observed_version: &str,
    already_retried: bool,
) -> ConflictAction {
    match policy {
        ConflictPolicy::Manual => ConflictAction::Park,
        ConflictPolicy::ExportThenManual => ConflictAction::ExportThenPark,
        ConflictPolicy::TalonWins if already_retried => ConflictAction::Park,
        ConflictPolicy::TalonWins => ConflictAction::RetryAgainst {
            observed_version: observed_version.to_owned(),
        },
    }
}

/// Total attempts allowed against the origin for one mutation.
pub const MAX_FLUSH_ATTEMPTS: u32 = 5;

/// First backoff delay.
pub const INITIAL_BACKOFF: Duration = Duration::from_millis(200);

/// Ceiling on any single backoff delay.
pub const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Delay before `attempt`, computed from the persisted attempt count alone.
///
/// A pure function of the attempt number, deliberately. §9.9 requires a new
/// owner to recompute the delay "from the persisted attempt count instead of
/// trusting another worker's wall clock" — persisting a "next attempt at"
/// timestamp would carry the old owner's clock skew into a new process.
///
/// `attempt` is 1-based: attempt 1 is the first retry after the initial failure.
pub fn backoff_for(attempt: u32) -> Duration {
    if attempt <= 1 {
        return INITIAL_BACKOFF;
    }
    // Saturating shift: a corrupt or absurd attempt count must clamp to the cap,
    // not overflow into a short delay and hammer the origin.
    let factor = 1u64.checked_shl(attempt - 1).unwrap_or(u64::MAX);
    INITIAL_BACKOFF
        .checked_mul(factor.min(u32::MAX as u64) as u32)
        .unwrap_or(MAX_BACKOFF)
        .min(MAX_BACKOFF)
}

/// Whether the retry budget is exhausted.
///
/// > Exhausting the budget records `FAILED` on `W` replicas and parks the
/// > payload. Reads continue to return the committed Talon version.
///
/// Exhaustion is not data loss: the acknowledged version is still served, and
/// the operator decides what happens to it.
pub const fn budget_exhausted(attempts: u32) -> bool {
    attempts >= MAX_FLUSH_ATTEMPTS
}

/// An operator action on a parked conflict.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Resolution {
    /// Retry as Talon-wins, starting a fresh bounded budget.
    Retry,
    /// Export to the conflict prefix for inspection.
    Export,
    /// Discard the Talon version and accept the origin's.
    ///
    /// The only path that destroys acknowledged data, which is why it carries
    /// the mutation id and a confirmation flag rather than being a bare command.
    AbandonAcceptOrigin {
        /// Exact mutation being discarded.
        mutation_id: String,
        /// Explicit acknowledgement that data will be lost.
        confirm_data_loss: bool,
    },
}

impl Resolution {
    /// Whether this resolution destroys acknowledged data.
    pub const fn destroys_acknowledged_data(&self) -> bool {
        matches!(self, Self::AbandonAcceptOrigin { .. })
    }

    /// Whether the resolution may be applied.
    ///
    /// Abandonment without the confirmation flag is refused. §9.9 requires "the
    /// exact mutation ID plus a data-loss confirmation flag", so a mistyped or
    /// replayed command cannot silently discard a write.
    pub const fn is_permitted(&self) -> bool {
        match self {
            Self::Retry | Self::Export => true,
            Self::AbandonAcceptOrigin {
                confirm_data_loss, ..
            } => *confirm_data_loss,
        }
    }
}

/// One audited resolution.
///
/// > Every automatic and manual resolution is audited with namespace, object
/// > identity, mutation ID, origin version, policy, actor, and result.
///
/// Automatic ones are audited too: a `talon_wins` retry that succeeds silently
/// still overwrote something an external writer put there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRecord {
    /// Namespace the object belongs to.
    pub namespace: String,
    /// Object identity.
    pub object: String,
    /// Mutation involved.
    pub mutation_id: String,
    /// Origin version observed at the time.
    pub origin_version: String,
    /// Policy in force.
    pub policy: ConflictPolicy,
    /// Who acted: an operator identity, or the system for automatic actions.
    pub actor: String,
    /// Outcome.
    pub result: &'static str,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_policy_stops_serving_the_acknowledged_version() {
        // The rule the section is built on. A policy that returned false here
        // would be the origin_wins mode ADR 0003 §9.9 forbids, because it would
        // make an acknowledged write disappear from its path without anyone
        // asking.
        for policy in [
            ConflictPolicy::Manual,
            ConflictPolicy::TalonWins,
            ConflictPolicy::ExportThenManual,
        ] {
            assert!(
                policy.preserves_read_your_writes(),
                "{policy:?} must keep serving the Talon version"
            );
        }
    }

    #[test]
    fn the_default_policy_is_manual() {
        // The default must be the one that changes nothing without a human.
        assert_eq!(ConflictPolicy::default(), ConflictPolicy::Manual);
    }

    #[test]
    fn talon_wins_retries_exactly_once() {
        // §9.9: "another concurrent change returns to CONFLICT". Looping would
        // fight an external writer indefinitely and could overwrite a version
        // this flusher never observed.
        assert_eq!(
            on_conflict(ConflictPolicy::TalonWins, "v7", false),
            ConflictAction::RetryAgainst {
                observed_version: "v7".to_owned()
            }
        );
        assert_eq!(
            on_conflict(ConflictPolicy::TalonWins, "v8", true),
            ConflictAction::Park
        );
    }

    #[test]
    fn export_keeps_serving_rather_than_resolving() {
        // The policy that looks like an exception and is not: the export is
        // evidence for an operator, not a decision.
        assert_eq!(
            on_conflict(ConflictPolicy::ExportThenManual, "v1", false),
            ConflictAction::ExportThenPark
        );
        assert!(ConflictPolicy::ExportThenManual.preserves_read_your_writes());
    }

    #[test]
    fn manual_never_touches_the_origin() {
        assert_eq!(
            on_conflict(ConflictPolicy::Manual, "v1", false),
            ConflictAction::Park
        );
        assert_eq!(
            on_conflict(ConflictPolicy::Manual, "v1", true),
            ConflictAction::Park
        );
    }

    #[test]
    fn backoff_grows_from_the_attempt_count_alone() {
        // Pure in the attempt number, so a new owner computes the same delay
        // without inheriting the old owner's clock.
        assert_eq!(backoff_for(1), Duration::from_millis(200));
        assert_eq!(backoff_for(2), Duration::from_millis(400));
        assert_eq!(backoff_for(3), Duration::from_millis(800));
        assert_eq!(backoff_for(4), Duration::from_millis(1600));
    }

    #[test]
    fn backoff_is_capped() {
        // Without the cap, a handful of attempts would push the delay past any
        // useful bound and the mutation would appear stuck rather than failed.
        assert_eq!(backoff_for(20), MAX_BACKOFF);
        assert_eq!(backoff_for(u32::MAX), MAX_BACKOFF);
    }

    #[test]
    fn an_absurd_attempt_count_clamps_rather_than_wrapping() {
        // A corrupt persisted count must not overflow into a short delay and
        // hammer the origin. Every value clamps to the cap.
        for attempt in [31u32, 32, 33, 64, 65, 1000, u32::MAX - 1] {
            assert!(
                backoff_for(attempt) <= MAX_BACKOFF,
                "attempt {attempt} produced {:?}",
                backoff_for(attempt)
            );
        }
    }

    #[test]
    fn the_budget_is_five_attempts() {
        assert!(!budget_exhausted(4));
        assert!(budget_exhausted(5));
        assert!(budget_exhausted(6));
    }

    #[test]
    fn abandoning_without_confirmation_is_refused() {
        // The single path that destroys acknowledged data. Requiring the flag
        // means a mistyped or replayed command cannot discard a write.
        let unconfirmed = Resolution::AbandonAcceptOrigin {
            mutation_id: "m-1".to_owned(),
            confirm_data_loss: false,
        };
        assert!(!unconfirmed.is_permitted());
        assert!(unconfirmed.destroys_acknowledged_data());

        let confirmed = Resolution::AbandonAcceptOrigin {
            mutation_id: "m-1".to_owned(),
            confirm_data_loss: true,
        };
        assert!(confirmed.is_permitted());
    }

    #[test]
    fn only_abandonment_destroys_data() {
        // Guards the audit boundary: retry and export are recoverable, so a new
        // variant that destroys data must be a deliberate decision rather than
        // an accident.
        assert!(!Resolution::Retry.destroys_acknowledged_data());
        assert!(!Resolution::Export.destroys_acknowledged_data());
        assert!(Resolution::Retry.is_permitted());
        assert!(Resolution::Export.is_permitted());
    }
}
