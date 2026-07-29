//! What a cluster advertises to clients and operators.
//!
//! ADR 0003 §4 requires capability discovery to be possible without attempting
//! an operation:
//!
//! > A client must be able to discover cluster capabilities before relying on
//! > them. Capabilities are reported by the coordinator with their own
//! > capability revision and exposed through the management API so an operator
//! > can see what a cluster offers without attempting an operation.
//!
//! Two distinctions in this module carry more weight than they might appear to.
//!
//! # The revision is its own thing
//!
//! > The capability revision and §9's write-routing revision are not ADR 0001's
//! > membership-derived `PlacementVersion`.
//!
//! Three revisions that advance for unrelated reasons. `PlacementVersion` is a
//! content hash of the node set, so it changes whenever a worker joins or
//! leaves — which happens constantly and says nothing about what the cluster can
//! do. A capability revision changes when the *configuration* changes, which is
//! rare. Folding them together would make every membership change look like a
//! capability change and hide the changes that matter.
//!
//! # Advertised is not the same as reachable
//!
//! [`ClusterCapabilities::advertised`] answers "what does this cluster offer",
//! and [`ClusterCapabilities::store_reachable`] answers "can it serve that right
//! now". §6 requires the two to stay separable:
//!
//! > Operations requiring TMS fail closed, with the same errno as an
//! > unconfigured cluster plus a distinct log and metric so "not configured" and
//! > "configured but unreachable" are separable in an incident.
//!
//! §4 then maps the pair to different errnos — `EOPNOTSUPP` for a cluster that
//! does not implement locking, `ENOLCK` for one that does but cannot reach its
//! lock service. Collapsing them would make an outage indistinguishable from a
//! deployment choice, in both the logs and the application's error handling.

use core::fmt;

use crate::capability::{Capability, CapabilitySet};

/// A monotonically increasing revision of a cluster's advertised capabilities.
///
/// Deliberately a distinct type from any placement or routing version so the
/// three cannot be compared or assigned to one another by accident.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct CapabilityRevision(u64);

impl CapabilityRevision {
    /// The revision of a cluster whose capabilities have never changed.
    pub const INITIAL: Self = Self(0);

    /// Construct from a raw counter value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The raw counter value, for the wire protocol and the management API.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The next revision.
    ///
    /// # Panics
    ///
    /// Panics on overflow, which would make a newer revision compare as older
    /// and leave clients pinned to a stale capability set.
    #[must_use]
    pub const fn next(self) -> Self {
        match self.0.checked_add(1) {
            Some(value) => Self(value),
            None => panic!("capability revision overflowed"),
        }
    }
}

impl fmt::Display for CapabilityRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// What a cluster advertises, and whether it can currently serve it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterCapabilities {
    /// Capabilities this cluster offers.
    ///
    /// Empty for a cluster with no metadata store configured, which §1 keeps a
    /// complete, supported deployment:
    ///
    /// > A cluster without one is a complete, supported Talon deployment
    /// > offering exactly today's feature set.
    pub advertised: CapabilitySet,

    /// Revision of the advertised set.
    ///
    /// Advances on configuration change, never on membership change.
    pub revision: CapabilityRevision,

    /// Whether the metadata store is currently reachable.
    ///
    /// `true` when no store is configured: an empty capability set is always
    /// serviceable, since there is nothing to fail. Distinguishing "offers
    /// nothing" from "offers something it cannot reach" is the point of
    /// [`ClusterCapabilities::state_of`].
    pub store_reachable: bool,
}

/// Why an operation requiring a capability cannot proceed.
///
/// Maps directly onto §4's errno table. Kept as an enum rather than a bool so
/// the two failure modes cannot collapse into one on the way to the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityState {
    /// The cluster offers this capability and can serve it now.
    Available,
    /// The cluster does not offer this capability at all.
    ///
    /// A permanent property of the deployment. Retrying cannot help, and the
    /// caller must report it as unsupported rather than as a transient fault.
    NotOffered,
    /// The cluster offers this capability but its store is unreachable.
    ///
    /// Transient. §6 requires the read path to be unaffected and the operation
    /// to fail closed — never to fall back to a local approximation.
    Unreachable,
}

impl ClusterCapabilities {
    /// A cluster with no metadata store.
    pub fn none() -> Self {
        Self {
            advertised: CapabilitySet::none(),
            revision: CapabilityRevision::INITIAL,
            store_reachable: true,
        }
    }

    /// How an operation requiring `capability` should be answered.
    pub fn state_of(&self, capability: Capability) -> CapabilityState {
        if !self.advertised.supports(capability) {
            return CapabilityState::NotOffered;
        }
        if self.store_reachable {
            CapabilityState::Available
        } else {
            CapabilityState::Unreachable
        }
    }

    /// Whether `capability` can be used right now.
    pub fn is_available(&self, capability: Capability) -> bool {
        self.state_of(capability) == CapabilityState::Available
    }
}

impl Default for ClusterCapabilities {
    fn default() -> Self {
        Self::none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cluster_without_a_metadata_store_offers_nothing_and_is_serviceable() {
        // §1: such a cluster "is a complete, supported Talon deployment offering
        // exactly today's feature set" -- not a degraded one.
        let capabilities = ClusterCapabilities::none();
        assert!(capabilities.advertised.is_empty());
        assert!(capabilities.store_reachable);
        assert_eq!(
            capabilities.state_of(Capability::HardLinks),
            CapabilityState::NotOffered
        );
    }

    #[test]
    fn not_offered_and_unreachable_are_distinct_states() {
        // The distinction §4 maps to EOPNOTSUPP vs ENOLCK and §6 requires to
        // stay separable in an incident. Collapsing them would make an outage
        // indistinguishable from a deployment choice.
        let offered_and_down = ClusterCapabilities {
            advertised: CapabilitySet::none().with(Capability::Locks),
            revision: CapabilityRevision::new(3),
            store_reachable: false,
        };
        assert_eq!(
            offered_and_down.state_of(Capability::Locks),
            CapabilityState::Unreachable
        );
        assert_eq!(
            offered_and_down.state_of(Capability::HardLinks),
            CapabilityState::NotOffered,
            "an unreachable store does not make an unoffered capability unreachable"
        );
        assert!(!offered_and_down.is_available(Capability::Locks));
    }

    #[test]
    fn an_advertised_capability_with_a_reachable_store_is_available() {
        let capabilities = ClusterCapabilities {
            advertised: CapabilitySet::none().with(Capability::HardLinks),
            revision: CapabilityRevision::new(1),
            store_reachable: true,
        };
        assert_eq!(
            capabilities.state_of(Capability::HardLinks),
            CapabilityState::Available
        );
        assert!(capabilities.is_available(Capability::HardLinks));
    }

    #[test]
    fn capability_revisions_advance_monotonically() {
        let first = CapabilityRevision::INITIAL;
        assert!(first.next() > first);
        assert_eq!(first.next().next().get(), 2);
    }

    #[test]
    fn capability_revision_ordering_is_numeric_not_lexicographic() {
        // Guards against encoding the counter as a string on the wire or in the
        // API: "10" sorts before "9" lexicographically, which would make a newer
        // capability set look stale and pin clients to an old view.
        assert!(CapabilityRevision::new(10) > CapabilityRevision::new(9));
    }
}
