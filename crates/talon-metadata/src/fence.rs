//! The namespace mapping fence (ADR 0003 §5).
//!
//! A client caches the fact that some path is an ordinary object at its visible
//! address — a "negative mapping". That cache is what keeps the read path fast,
//! and it is exactly what goes stale when a hard-link promotion moves the data
//! to an inode object. §5:
//!
//! > That write exclusion requires an explicit distributed fence; checking TMS
//! > only inside `link()` is insufficient because a client may hold a stale
//! > negative mapping.
//!
//! Without the fence such a client writes to the visible-path object after
//! promotion, and the write is silently lost — #363's divergence arriving
//! through the cache instead of through `link()`.
//!
//! # Why the check is local
//!
//! > Workers watch TMS and validate the revision against local state before
//! > resolving a path or accepting a mutation. **This is a local hot-path check,
//! > not a TMS round trip per operation.**
//!
//! A store round trip per request would put TMS on the data path, which §6
//! forbids. So a worker compares against the revision it already holds, and the
//! propagation of that revision is a separate, low-frequency concern.

use crate::revision::MappingRevision;

/// The outcome of comparing a request's revision against local state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceDecision {
    /// The revisions agree; the operation may proceed.
    Proceed,
    /// The client is behind. It must refresh and retry.
    ///
    /// This is the case §5 names: the client's cached mapping predates a
    /// transition, so acting on it could write to a path the data has left.
    ClientStale,
    /// The worker is behind the client.
    ///
    /// Refusing here is not obviously necessary, and it is the subtler half of
    /// the fence. A worker that has not yet observed a transition cannot know
    /// whether the path it is about to resolve was moved by it. Serving the
    /// request would mean answering from state the worker *knows* is out of
    /// date — the client just proved it. §5 requires a worker that cannot keep
    /// its guard current to stop accepting mutations, and this is the
    /// per-request expression of the same rule.
    WorkerStale,
}

impl FenceDecision {
    /// Whether the operation may proceed.
    pub fn is_proceed(self) -> bool {
        matches!(self, Self::Proceed)
    }
}

/// Compare a request's mapping revision against the locally held one.
///
/// Both directions of mismatch refuse. Equality is the only case that proceeds,
/// because it is the only case in which both sides agree on which paths are
/// inode-addressed.
pub fn check(request: MappingRevision, local: MappingRevision) -> FenceDecision {
    use core::cmp::Ordering;
    match request.cmp(&local) {
        Ordering::Equal => FenceDecision::Proceed,
        Ordering::Less => FenceDecision::ClientStale,
        Ordering::Greater => FenceDecision::WorkerStale,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_revisions_proceed() {
        assert_eq!(
            check(MappingRevision::new(4), MappingRevision::new(4)),
            FenceDecision::Proceed
        );
    }

    #[test]
    fn an_untouched_namespace_proceeds_without_a_stored_record() {
        // §3's sparsity claim covers the mapping guard: a namespace that never
        // had a transition stores nothing, and both sides derive INITIAL. If
        // this needed a record, every namespace would cost one.
        assert_eq!(
            check(MappingRevision::INITIAL, MappingRevision::INITIAL),
            FenceDecision::Proceed
        );
    }

    #[test]
    fn a_client_behind_the_worker_is_refused() {
        // The case §5 is about: the client's cached negative mapping predates a
        // promotion, so writing through it would land on an object the data has
        // already left.
        assert_eq!(
            check(MappingRevision::new(3), MappingRevision::new(4)),
            FenceDecision::ClientStale
        );
    }

    #[test]
    fn a_worker_behind_the_client_is_also_refused() {
        // The subtler half. The worker has not seen the transition, so it cannot
        // know whether the path it would resolve was moved by it. Serving would
        // mean answering from state the client just proved stale.
        assert_eq!(
            check(MappingRevision::new(5), MappingRevision::new(4)),
            FenceDecision::WorkerStale
        );
    }

    #[test]
    fn only_equality_proceeds() {
        // Guards against a comparison that treats "at least as new" as good
        // enough. A worker ahead of the client is still a disagreement about
        // which paths are inode-addressed, and one of the two is acting on a
        // view that no longer holds.
        let local = MappingRevision::new(10);
        for request in [8u64, 9, 11, 12] {
            assert!(
                !check(MappingRevision::new(request), local).is_proceed(),
                "revision {request} against local 10 must not proceed"
            );
        }
        assert!(check(local, local).is_proceed());
    }
}
