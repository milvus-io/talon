//! Mapping cluster capabilities onto POSIX errnos.
//!
//! ADR 0003 §4 calls itself "the load-bearing clause", and the reason is #363:
//!
//! > Silent degradation is precisely the defect in #363: `link()` currently
//! > appears to succeed, produces copies that can diverge, cannot self-repair,
//! > and does not report which copies went stale. A feature that looks supported
//! > and is subtly wrong is worse than one that plainly refuses, because
//! > applications build on the former.
//!
//! So every operation needing a capability the cluster lacks fails with a
//! *distinct* errno, and the distinction an application can act on is between a
//! permanent property of the deployment and a transient outage:
//!
//! | Operation or condition | Result |
//! |---|---|
//! | `link()` without TMS | `EPERM` |
//! | `link()` capability configured but TMS unavailable | `EAGAIN` |
//! | `fcntl`/`flock` without the TMS locking capability | `EOPNOTSUPP` |
//! | TMS locking configured but unavailable | `ENOLCK` |
//!
//! > `EOPNOTSUPP` means that this cluster does not implement distributed
//! > locking. `ENOLCK` is reserved for the distinct case where the cluster
//! > offers locking but cannot currently reach or use the lock service. Talon
//! > never turns either case into a successful mount-local lock.
//!
//! The pairing is what makes the errnos useful: `EPERM` and `EOPNOTSUPP` tell an
//! application to stop asking, while `EAGAIN` and `ENOLCK` tell it to retry.
//! Collapsing them would make an outage look like a deployment choice.

use talon_metadata::{Capability, CapabilityState, ClusterCapabilities};

/// Errno values from §4's table.
///
/// Defined here rather than taken from `libc` so this contract is testable in
/// the default build: `libc` is an optional dependency behind the `mount`
/// feature, and the errno mapping is exactly the part that most needs a test
/// which always runs. `mount.rs` asserts these agree with `libc`.
pub mod errno {
    /// Operation not permitted. The cluster does not offer hard links.
    pub const EPERM: i32 = 1;
    /// Try again. The capability exists but its store is unreachable.
    pub const EAGAIN: i32 = 11;
    /// No locks available. The cluster offers locking but cannot reach it.
    pub const ENOLCK: i32 = 37;
    /// Operation not supported. This cluster does not implement locking.
    pub const EOPNOTSUPP: i32 = 95;
}

/// The errno for a `link()` that requires inode indirection.
///
/// `EPERM` when the cluster does not offer hard links, `EAGAIN` when it does but
/// the metadata store is unreachable. Returns `None` when the operation may
/// proceed.
///
/// The `EPERM` case preserves today's shipped behaviour (`mount.rs:949`), which
/// §5 singles out as the one part of the ADR that should exist before TMS does:
///
/// > Without TMS, `link()` returns `EPERM`. This is the only part of this ADR
/// > that should be implemented before TMS exists, because it is a correctness
/// > fix for shipped behaviour (#363).
pub fn hard_link_errno(capabilities: &ClusterCapabilities) -> Option<i32> {
    match capabilities.state_of(Capability::HardLinks) {
        CapabilityState::Available => None,
        CapabilityState::NotOffered => Some(errno::EPERM),
        CapabilityState::Unreachable => Some(errno::EAGAIN),
    }
}

/// The errno for a lock operation.
///
/// `EOPNOTSUPP` when the cluster does not implement distributed locking,
/// `ENOLCK` when it does but the lock service is unreachable. Returns `None`
/// when the operation may proceed.
///
/// Note that `None` is currently unreachable in practice: no backend advertises
/// [`Capability::Locks`], because shipping POSIX locks needs its own ADR
/// defining "the bounded byte-range representation, blocking and fairness rules,
/// waiter recovery, and cross-file deadlock detection". The branch exists so
/// that wiring the lock path later is a change to one call site rather than a
/// change to this contract.
pub fn lock_errno(capabilities: &ClusterCapabilities) -> Option<i32> {
    match capabilities.state_of(Capability::Locks) {
        CapabilityState::Available => None,
        CapabilityState::NotOffered => Some(errno::EOPNOTSUPP),
        CapabilityState::Unreachable => Some(errno::ENOLCK),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_metadata::{CapabilityRevision, CapabilitySet};

    fn offering(capability: Capability, reachable: bool) -> ClusterCapabilities {
        ClusterCapabilities {
            advertised: CapabilitySet::none().with(capability),
            revision: CapabilityRevision::new(1),
            store_reachable: reachable,
        }
    }

    #[test]
    fn a_cluster_without_tms_refuses_hard_links_with_eperm() {
        // Today's shipped behaviour, which §5 keeps as the correct answer for an
        // unconfigured cluster.
        assert_eq!(
            hard_link_errno(&ClusterCapabilities::none()),
            Some(errno::EPERM)
        );
    }

    #[test]
    fn an_unreachable_store_makes_hard_links_retryable_not_forbidden() {
        // EPERM would tell the application to stop asking, which is wrong when
        // the capability exists and the store is merely down. §4 pairs the two
        // errnos precisely so this case is distinguishable.
        assert_eq!(
            hard_link_errno(&offering(Capability::HardLinks, false)),
            Some(errno::EAGAIN)
        );
    }

    #[test]
    fn hard_links_proceed_when_offered_and_reachable() {
        assert_eq!(
            hard_link_errno(&offering(Capability::HardLinks, true)),
            None
        );
    }

    #[test]
    fn a_cluster_without_locking_reports_eopnotsupp_never_success() {
        // The critical case. §4: "Talon never turns either case into a
        // successful mount-local lock." A successful return here would let an
        // application believe it holds cluster-wide exclusion while only
        // coordinating processes on one mount.
        let errno = lock_errno(&ClusterCapabilities::none());
        assert_eq!(errno, Some(errno::EOPNOTSUPP));
        assert_ne!(errno, None, "a lock request must never silently succeed");
    }

    #[test]
    fn an_unreachable_lock_service_reports_enolck_not_eopnotsupp() {
        // §4: "ENOLCK is reserved for the distinct case where the cluster offers
        // locking but cannot currently reach or use the lock service."
        assert_eq!(
            lock_errno(&offering(Capability::Locks, false)),
            Some(errno::ENOLCK)
        );
    }

    #[test]
    fn the_two_lock_errnos_are_never_interchangeable() {
        // Guards the pairing itself: one says stop, the other says retry.
        assert_ne!(errno::EOPNOTSUPP, errno::ENOLCK);
        assert_ne!(
            lock_errno(&ClusterCapabilities::none()),
            lock_errno(&offering(Capability::Locks, false))
        );
    }

    #[test]
    fn an_unreachable_store_does_not_change_an_unoffered_capability() {
        // A cluster that offers hard links but not locks, with its store down,
        // must still report locks as unsupported rather than unreachable --
        // otherwise an application would retry forever against a feature the
        // cluster will never provide.
        let capabilities = offering(Capability::HardLinks, false);
        assert_eq!(lock_errno(&capabilities), Some(errno::EOPNOTSUPP));
        assert_eq!(hard_link_errno(&capabilities), Some(errno::EAGAIN));
    }
}
