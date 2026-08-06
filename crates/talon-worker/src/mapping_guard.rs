//! The worker's local view of namespace mapping revisions (ADR 0003 §5).
//!
//! A worker validates every request's mapping revision before resolving a path
//! or accepting a mutation, and §5 is specific about how:
//!
//! > Workers watch TMS and validate the revision against local state before
//! > resolving a path or accepting a mutation. **This is a local hot-path check,
//! > not a TMS round trip per operation.**
//!
//! So this holds the revision in memory and the check is a map lookup and an
//! integer comparison. A store round trip per request would put TMS on the data
//! path, which §6 forbids.
//!
//! # What this deliberately does not do
//!
//! It does not fetch revisions. §5 says workers watch TMS, while §7 confines
//! "TMS credentials and network access to the management tier" — and a worker is
//! not the management tier. That contradiction is tracked in #403 and has to be
//! resolved by an ADR amendment, not by an implementation guess.
//!
//! This type is agnostic to the answer: whether the revision arrives from a TMS
//! watch, a coordinator heartbeat, or a pull, it lands through
//! [`MappingGuard::observe`] and the hot-path check is identical.
//!
//! # Guard staleness
//!
//! > A worker that cannot keep its guard current stops accepting mutations for
//! > that namespace.
//!
//! [`MappingGuard::is_current`] expresses that: a guard whose revision has not
//! been refreshed within its freshness window is not trustworthy, and the worker
//! must refuse rather than serve from a view it cannot vouch for.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use talon_metadata::fence::{check, FenceDecision};
use talon_metadata::MappingRevision;

/// Why a request was refused by the fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardRefusal {
    /// The client's revision is older than the worker's.
    ///
    /// The client refreshes through a coordinator and retries (§5).
    ClientStale,
    /// The client's revision is newer than the worker's.
    ///
    /// The worker has not observed a transition the client already knows about,
    /// so it cannot tell whether the path in question was moved by it.
    WorkerStale,
    /// The worker's guard has not been refreshed recently enough to trust.
    ///
    /// Distinct from [`GuardRefusal::WorkerStale`]: there the worker knows it is
    /// behind, here it cannot know either way. §5 requires it to stop accepting
    /// mutations for the namespace rather than assume its stale view still
    /// holds.
    GuardExpired,
}

/// A worker's locally held mapping revisions, one per namespace.
#[derive(Debug)]
pub struct MappingGuard {
    namespaces: Mutex<HashMap<String, Observed>>,
    freshness: Duration,
}

#[derive(Debug, Clone, Copy)]
struct Observed {
    revision: MappingRevision,
    at: Instant,
}

impl MappingGuard {
    /// A guard whose entries must be refreshed within `freshness`.
    pub fn new(freshness: Duration) -> Self {
        Self {
            namespaces: Mutex::new(HashMap::new()),
            freshness,
        }
    }

    /// Record a revision observed for `namespace`.
    ///
    /// Monotonic: an older revision is ignored rather than applied. Propagation
    /// may reorder — two coordinators can answer a heartbeat concurrently, and a
    /// retried message can arrive late — and moving the guard backwards would
    /// re-admit exactly the writes the fence just excluded.
    ///
    /// Observing the same revision again does refresh the timestamp, because
    /// "unchanged" is positive evidence that the guard is current.
    pub fn observe(&self, namespace: &str, revision: MappingRevision) {
        let mut namespaces = self
            .namespaces
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = Instant::now();
        namespaces
            .entry(namespace.to_owned())
            .and_modify(|entry| {
                if revision >= entry.revision {
                    entry.revision = revision;
                    entry.at = now;
                }
            })
            .or_insert(Observed { revision, at: now });
    }

    /// The revision currently held for `namespace`, if any is fresh.
    pub fn current(&self, namespace: &str) -> Option<MappingRevision> {
        let namespaces = self
            .namespaces
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        namespaces
            .get(namespace)
            .filter(|entry| entry.at.elapsed() <= self.freshness)
            .map(|entry| entry.revision)
    }

    /// Revision held for `namespace`, including an expired entry.
    ///
    /// Used for acknowledgements so a stale update receives the worker's higher
    /// actual revision without falsely refreshing the guard deadline.
    pub fn held(&self, namespace: &str) -> Option<MappingRevision> {
        self.namespaces
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(namespace)
            .map(|entry| entry.revision)
    }

    /// Whether the guard for `namespace` is fresh enough to serve from.
    pub fn is_current(&self, namespace: &str) -> bool {
        self.current(namespace).is_some()
    }

    /// Validate a request's revision against the locally held one.
    ///
    /// A namespace with no entry is treated as [`MappingRevision::INITIAL`]
    /// rather than as expired. §3's sparsity claim means a namespace that has
    /// never had a hard-link transition stores nothing, so "no entry" is the
    /// normal state for almost every namespace and must not refuse traffic.
    pub fn validate(&self, namespace: &str, request: MappingRevision) -> Result<(), GuardRefusal> {
        let namespaces = self
            .namespaces
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let local = match namespaces.get(namespace) {
            Some(entry) if entry.at.elapsed() > self.freshness => {
                return Err(GuardRefusal::GuardExpired)
            }
            Some(entry) => entry.revision,
            // Never observed: sparsity means this is an ordinary namespace with
            // no transitions, not a missing guard.
            None => MappingRevision::INITIAL,
        };
        match check(request, local) {
            FenceDecision::Proceed => Ok(()),
            FenceDecision::ClientStale => Err(GuardRefusal::ClientStale),
            FenceDecision::WorkerStale => Err(GuardRefusal::WorkerStale),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn guard() -> MappingGuard {
        MappingGuard::new(Duration::from_secs(30))
    }

    #[test]
    fn an_untouched_namespace_serves_without_any_stored_revision() {
        // §3: a namespace that never had a hard-link transition occupies zero
        // records. If "no entry" refused traffic, enabling the capability would
        // break every ordinary namespace until something populated a guard.
        assert_eq!(guard().validate("ns", MappingRevision::INITIAL), Ok(()));
    }

    #[test]
    fn a_matching_revision_proceeds() {
        let guard = guard();
        guard.observe("ns", MappingRevision::new(4));
        assert_eq!(guard.validate("ns", MappingRevision::new(4)), Ok(()));
    }

    #[test]
    fn a_stale_client_is_refused() {
        let guard = guard();
        guard.observe("ns", MappingRevision::new(4));
        assert_eq!(
            guard.validate("ns", MappingRevision::new(3)),
            Err(GuardRefusal::ClientStale)
        );
    }

    #[test]
    fn a_client_ahead_of_the_worker_is_refused_distinctly() {
        // Not the same as ClientStale: here the worker is behind, so telling the
        // client to refresh would send it into a retry loop against a worker
        // that cannot yet serve it.
        let guard = guard();
        guard.observe("ns", MappingRevision::new(4));
        assert_eq!(
            guard.validate("ns", MappingRevision::new(5)),
            Err(GuardRefusal::WorkerStale)
        );
    }

    #[test]
    fn an_expired_guard_refuses_rather_than_serving_a_stale_view() {
        // §5: "A worker that cannot keep its guard current stops accepting
        // mutations for that namespace." Serving from an unrefreshed guard would
        // mean vouching for a view the worker has no evidence still holds.
        let guard = MappingGuard::new(Duration::from_millis(1));
        guard.observe("ns", MappingRevision::new(4));
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(
            guard.validate("ns", MappingRevision::new(4)),
            Err(GuardRefusal::GuardExpired)
        );
        assert!(!guard.is_current("ns"));
    }

    #[test]
    fn observing_an_older_revision_does_not_move_the_guard_backwards() {
        // Propagation can reorder: two coordinators may answer concurrently, and
        // a retried message can arrive late. Applying the older value would
        // re-admit the writes the fence just excluded.
        let guard = guard();
        guard.observe("ns", MappingRevision::new(7));
        guard.observe("ns", MappingRevision::new(3));
        assert_eq!(guard.current("ns"), Some(MappingRevision::new(7)));
        assert_eq!(
            guard.validate("ns", MappingRevision::new(3)),
            Err(GuardRefusal::ClientStale)
        );
    }

    #[test]
    fn re_observing_the_same_revision_keeps_the_guard_fresh() {
        // "Unchanged" is positive evidence, not absence of evidence. A namespace
        // with no transitions would otherwise expire its guard and stop
        // accepting mutations for no reason.
        let guard = MappingGuard::new(Duration::from_millis(50));
        guard.observe("ns", MappingRevision::new(2));
        std::thread::sleep(Duration::from_millis(30));
        guard.observe("ns", MappingRevision::new(2));
        std::thread::sleep(Duration::from_millis(30));
        assert!(
            guard.is_current("ns"),
            "a repeated observation must refresh the guard"
        );
    }

    #[test]
    fn namespaces_are_independent() {
        // A transition in one namespace must not fence traffic in another; the
        // revision is namespace-wide, not cluster-wide (§5).
        let guard = guard();
        guard.observe("busy", MappingRevision::new(9));
        assert_eq!(guard.validate("quiet", MappingRevision::INITIAL), Ok(()));
        assert_eq!(
            guard.validate("busy", MappingRevision::INITIAL),
            Err(GuardRefusal::ClientStale)
        );
    }
}
