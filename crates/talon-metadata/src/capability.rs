//! Per-backend capability declaration.
//!
//! ADR 0003 §7 requires capabilities to be *declared* by each backend rather
//! than inferred from the presence of a generic compare-and-swap call:
//!
//! > Capabilities are advertised per TMS backend, not inferred from the
//! > existence of a generic compare-and-swap call.
//!
//! The distinction matters because the underlying primitives look similar while
//! the guarantees differ. A store offering single-key compare-and-swap can back
//! a lock, but hard links need one transaction to span the transition record,
//! the inode record, and every path index entry. A backend that cannot do that
//! must refuse the capability rather than approximate it with a sequence of
//! single-key writes that a crash can interleave.
//!
//! This is why the ADR names a backend that must not claim the richer
//! capabilities:
//!
//! > In particular, the Kubernetes Lease representation from ADR 0001 is not a
//! > multi-record transaction service and cannot provide hard links or
//! > write-back by itself.

use core::fmt;

/// A capability a [`MetadataStore`](crate::MetadataStore) backend may advertise.
///
/// Each variant names the guarantees ADR 0003 §7 requires of a backend before
/// it may claim support. A backend advertises only what it can honour; callers
/// check with [`CapabilitySet::supports`] before attempting an operation, and
/// the store rejects unsupported operations with
/// [`MetadataError::CapabilityUnsupported`](crate::MetadataError::CapabilityUnsupported).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Capability {
    /// Distributed locking.
    ///
    /// Requires server-observed expiring sessions and atomic compare-and-swap.
    /// "Server-observed" is the load-bearing word: expiry must be judged by the
    /// store, not by a client comparing wall-clock timestamps, because a
    /// partitioned client cannot be trusted to notice that it lost its session.
    ///
    /// Advertising this capability does not make POSIX locks available. Per the
    /// ADR's validation gates, a separate locking ADR must first define the
    /// bounded byte-range representation, blocking and fairness rules, waiter
    /// recovery, and cross-file deadlock detection.
    Locks,

    /// Hard links through inode indirection (ADR 0003 §5).
    ///
    /// Requires atomic transactions across transition, inode, and path index
    /// records. A backend limited to single-record compare-and-swap must not
    /// advertise this: the promotion commit in §5 updates two path indexes, one
    /// inode record, and removes a transition record, and a crash partway
    /// through a non-atomic emulation leaves exactly the divergence that #363
    /// is about.
    HardLinks,

    /// Write-back shard ownership (ADR 0003 §9).
    ///
    /// Requires everything [`Capability::Locks`] does, plus persistent fencing
    /// terms and atomic shard-state transitions. "Persistent" excludes stores
    /// where the term lives only in an ephemeral session key, because §9.3
    /// requires the last committed term to outlive owner lease expiry:
    ///
    /// > `term` is a monotonically increasing fencing number. It must survive
    /// > owner lease expiry.
    ///
    /// Advertising this capability does **not** enable write-back. ADR 0003
    /// §9.11 keeps write-back unreachable until a superseding ADR is accepted;
    /// this capability only says the store *could* back it.
    WriteBack,
}

impl Capability {
    /// Every capability, in declaration order.
    pub const ALL: [Self; 3] = [Self::Locks, Self::HardLinks, Self::WriteBack];

    /// Stable lowercase identifier for logs, metrics, and the management API.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Locks => "locks",
            Self::HardLinks => "hard_links",
            Self::WriteBack => "write_back",
        }
    }
}

impl fmt::Display for Capability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The set of capabilities a backend advertises.
///
/// Stored as a bitmask so that it is `Copy` and cheap to pass alongside every
/// operation. Construct with [`CapabilitySet::none`] and add only what the
/// backend can actually honour — the default is deliberately empty so that a
/// new backend advertises nothing until someone states otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CapabilitySet(u8);

impl CapabilitySet {
    /// A backend that advertises nothing.
    ///
    /// This is the correct starting point for every backend. A store with no
    /// capabilities is still useful: it can hold records for features that need
    /// no cross-record atomicity, and it keeps the "cluster without TMS"
    /// behaviour testable.
    pub const fn none() -> Self {
        Self(0)
    }

    /// Add a capability.
    #[must_use]
    pub const fn with(self, capability: Capability) -> Self {
        Self(self.0 | Self::bit(capability))
    }

    /// Whether the backend advertises `capability`.
    pub const fn supports(self, capability: Capability) -> bool {
        self.0 & Self::bit(capability) != 0
    }

    /// Whether the backend advertises nothing at all.
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Iterate the advertised capabilities in [`Capability::ALL`] order.
    pub fn iter(self) -> impl Iterator<Item = Capability> {
        Capability::ALL
            .into_iter()
            .filter(move |capability| self.supports(*capability))
    }

    const fn bit(capability: Capability) -> u8 {
        match capability {
            Capability::Locks => 1 << 0,
            Capability::HardLinks => 1 << 1,
            Capability::WriteBack => 1 << 2,
        }
    }
}

impl FromIterator<Capability> for CapabilitySet {
    fn from_iter<I: IntoIterator<Item = Capability>>(iter: I) -> Self {
        iter.into_iter()
            .fold(Self::none(), |set, capability| set.with(capability))
    }
}

impl fmt::Display for CapabilitySet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return f.write_str("none");
        }
        let mut first = true;
        for capability in self.iter() {
            if !first {
                f.write_str(",")?;
            }
            f.write_str(capability.as_str())?;
            first = false;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_capability_set_advertises_nothing() {
        let set = CapabilitySet::none();
        assert!(set.is_empty());
        for capability in Capability::ALL {
            assert!(
                !set.supports(capability),
                "{capability} must not be advertised by default"
            );
        }
    }

    #[test]
    fn adding_one_capability_does_not_imply_another() {
        // Guards ADR 0003 §7: a store with single-key compare-and-swap can back
        // locks without being able to back hard links. Nothing here may infer
        // one capability from another.
        let set = CapabilitySet::none().with(Capability::Locks);
        assert!(set.supports(Capability::Locks));
        assert!(!set.supports(Capability::HardLinks));
        assert!(!set.supports(Capability::WriteBack));
    }

    #[test]
    fn capabilities_round_trip_through_iteration() {
        let set = CapabilitySet::none()
            .with(Capability::WriteBack)
            .with(Capability::Locks);
        let collected: CapabilitySet = set.iter().collect();
        assert_eq!(set, collected);
        assert_eq!(
            set.iter().collect::<Vec<_>>(),
            [Capability::Locks, Capability::WriteBack]
        );
    }

    #[test]
    fn display_lists_capabilities_and_names_the_empty_set() {
        assert_eq!(CapabilitySet::none().to_string(), "none");
        assert_eq!(
            CapabilitySet::none()
                .with(Capability::Locks)
                .with(Capability::HardLinks)
                .to_string(),
            "locks,hard_links"
        );
    }
}
