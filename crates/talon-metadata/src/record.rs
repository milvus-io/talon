//! Record types admitted to TMS.
//!
//! Every type here must satisfy ADR 0003 §2's admission rule:
//!
//! > If a fact can be rebuilt by listing the object store, it does not go in TMS.
//!
//! The rule is not advisory. §3 turns it into a quantity claim that decides
//! whether an etcd-class backend is viable at all:
//!
//! > a file with a single link occupies zero TMS records; [...] an unlocked file
//! > occupies zero.
//!
//! etcd holds its keyspace in memory with a practical ceiling in the
//! single-digit GB, so per-object records for a billion-object bucket would not
//! fit. Sparsity is what makes the backend choice work, which is why
//! [`LinkCount`] cannot represent 1: a singly-linked file must be
//! *unrepresentable* here, not merely absent by convention.
//!
//! Deliberately **not** modelled, because the object store is the truth:
//! file existence, directory structure, size, mtime, ETag, desired shard
//! placement, and per-object dirty inventory.

use core::fmt;
use core::num::NonZeroU64;

use crate::error::{MetadataError, MetadataResult};
use crate::revision::MappingRevision;

/// Identifies a namespace within a cluster.
///
/// Namespaces scope mapping revisions and write shards. §9.1 includes the
/// namespace in the shard hash so that two namespaces sharing a bucket do not
/// contend for the same shard ownership.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NamespaceId(String);

impl NamespaceId {
    /// Construct from a non-empty identifier.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataError::InvalidRecord`] when empty.
    pub fn new(value: impl Into<String>) -> MetadataResult<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(MetadataError::InvalidRecord {
                detail: "namespace id must not be empty".to_owned(),
            });
        }
        Ok(Self(value))
    }

    /// Borrow the identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NamespaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// An inode number within a namespace.
///
/// Inode-addressed storage uses the `<ns>/<ino>` convention that already exists
/// for unlink-while-open. §5 notes the convention is reusable but its
/// persistence model is not: that mechanism's mapping "only has to survive
/// inside one process for one open handle's lifetime, so in-memory state
/// suffices", whereas hard links "must hold across processes, clients, and
/// restarts".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InodeNumber(NonZeroU64);

impl InodeNumber {
    /// Construct from a non-zero inode number.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataError::InvalidRecord`] for zero, which POSIX reserves.
    pub fn new(value: u64) -> MetadataResult<Self> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or_else(|| MetadataError::InvalidRecord {
                detail: "inode number must be non-zero".to_owned(),
            })
    }

    /// The raw inode number.
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl fmt::Display for InodeNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A link count of at least two.
///
/// This type is the admission rule expressed in Rust. §3 requires a singly
/// linked file to occupy **zero** TMS records, and §5 confines indirection to
/// files that actually use the feature:
///
/// > Indirection applies **only to multiply-linked files**. A file with one name
/// > remains a plain object at its visible path, directly readable by any S3
/// > client. When a link count returns to one, the object moves back.
///
/// Making 1 unrepresentable means a future change cannot quietly start writing
/// a record per ordinary file — the code would not compile. That matters more
/// than it might appear: per-object records are precisely what would make etcd
/// non-viable (§3) and turn Talon into the object-store-backed filesystem that
/// the ADR's context section rules out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LinkCount(NonZeroU64);

impl LinkCount {
    /// The count at which a file first enters inode-addressed storage.
    pub const PROMOTED: Self = match NonZeroU64::new(2) {
        Some(value) => Self(value),
        None => unreachable!(),
    };

    /// Construct from a count of at least two.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataError::InvalidRecord`] for 0 or 1. A count of 1 is not
    /// an error in the filesystem — it is the ordinary case — but it must not be
    /// *stored*, because storing it would mean a record per ordinary file.
    pub fn new(value: u64) -> MetadataResult<Self> {
        if value < 2 {
            return Err(MetadataError::InvalidRecord {
                detail: format!(
                    "link count {value} does not belong in TMS: a file with fewer than two links \
                     occupies zero records (ADR 0003 §3)"
                ),
            });
        }
        NonZeroU64::new(value)
            .map(Self)
            .ok_or_else(|| MetadataError::InvalidRecord {
                detail: "link count must be non-zero".to_owned(),
            })
    }

    /// The raw count.
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// One more link.
    ///
    /// § 5: adding a third or later link "needs no object copy: one TMS
    /// transaction adds the new path index, increments the bounded link count,
    /// and advances the mapping revision."
    ///
    /// # Panics
    ///
    /// Panics on overflow, which would otherwise wrap a large count to a small
    /// one and make the inode look ready for demotion.
    #[must_use]
    pub fn increment(self) -> Self {
        let raw = self.0.get().checked_add(1).expect("link count overflowed");
        Self(NonZeroU64::new(raw).expect("incremented count is non-zero"))
    }

    /// One fewer link, or `None` when the file should demote to path-addressed
    /// storage.
    ///
    /// Returning `None` at 1 rather than a `LinkCount` of 1 is what forces the
    /// caller to handle demotion. §5 calls that "the required inverse of
    /// promotion, not an optional space optimization" — leaving a
    /// singly-referenced inode record behind would violate §3's sparsity claim
    /// and strand an object outside its visible path.
    #[must_use]
    pub fn decrement(self) -> Option<Self> {
        NonZeroU64::new(self.0.get() - 1)
            .filter(|raw| raw.get() >= 2)
            .map(Self)
    }
}

impl fmt::Display for LinkCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// An inode record for a multiply-linked file.
///
/// Admitted by §2 as "inode reference counts". Exists only while the file has
/// two or more links; see [`LinkCount`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InodeRecord {
    /// Namespace owning the inode.
    pub namespace: NamespaceId,
    /// Inode number, addressing the internal object.
    pub inode: InodeNumber,
    /// Number of visible paths referencing this inode.
    pub link_count: LinkCount,
    /// Set when the inode object is missing or failed verification.
    ///
    /// §5's repair policy is deliberately asymmetric: an unreferenced object is
    /// quarantined and eventually deleted, but a reference to a *missing* object
    /// is marked corrupt and alerted — "never fabricate data or delete the
    /// references automatically".
    pub corrupt: bool,
}

/// A visible path referencing an inode.
///
/// Admitted by §2 as "`path -> inode` for files with more than one link".
/// Ordinary single-path files have no such record: their path *is* their
/// address in the object store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathIndexEntry {
    /// Namespace owning the path.
    pub namespace: NamespaceId,
    /// Visible path, as seen through the mount.
    pub path: String,
    /// Inode this path resolves to.
    pub inode: InodeNumber,
}

/// State of a hard-link transition (ADR 0003 §5).
///
/// The transition record is what makes promotion crash-safe. It is durable
/// before any object copy begins, so recovery can always tell which of the
/// copy, the commit, and the cleanup have happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LinkTransitionState {
    /// The copy to inode-addressed storage has not been committed.
    ///
    /// § 5: while `PREPARING` exists, reads may continue from the original
    /// object, and writes receive a retryable error rather than landing on an
    /// object that is about to stop being authoritative.
    Preparing,
    /// The TMS commit succeeded; obsolete path objects may still exist.
    ///
    /// The commit — not the cleanup — is the linearization point of `link()`.
    /// A crash here loses nothing: the inode object is authoritative and the
    /// stale path object is deleted on recovery.
    Committed,
    /// The transition was abandoned before commit.
    Aborted,
}

impl LinkTransitionState {
    /// Whether the original path object is still authoritative.
    ///
    /// Drives §5's crash-recovery table: before the commit the original object
    /// holds the data, after it the inode object does.
    pub const fn original_is_authoritative(self) -> bool {
        matches!(self, Self::Preparing | Self::Aborted)
    }
}

/// A durable hard-link transition record (ADR 0003 §5).
///
/// Admitted by §2 as an "active link and namespace transition record". Unlike
/// the inode and path records, this one is short-lived: it exists only while a
/// transition is in progress, and §3 notes that "an identity-changing operation
/// occupies one transition record only while it is in progress".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkTransition {
    /// Namespace owning the transition.
    pub namespace: NamespaceId,
    /// Unique operation identifier, used to fence a resumed transition.
    pub operation_id: String,
    /// Current state.
    pub state: LinkTransitionState,
    /// Path holding the data before promotion.
    pub source_path: String,
    /// Path being added as a second link.
    pub new_path: String,
    /// Object version captured before the copy.
    ///
    /// Used as a precondition for both the copy and the post-commit delete, so
    /// a concurrent overwrite cannot be silently discarded.
    pub source_version: String,
    /// Mapping revision this transition was formed against.
    pub expected_mapping_revision: MappingRevision,
    /// Inode the paths will resolve to.
    pub inode: InodeNumber,
    /// Worker executing the object-store copy.
    ///
    /// §7: coordinators "do not receive object-store credentials solely for TMS
    /// features and do not proxy payload bytes". The worker already authorized
    /// for the namespace does the copy under a fenced operation token.
    pub operation_worker: String,
    /// Process incarnation of that worker, so a restarted worker cannot resume
    /// a transition its predecessor owned.
    pub operation_worker_incarnation: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_single_link_cannot_be_represented() {
        // The admission rule as a compile-and-runtime boundary: §3 requires a
        // singly-linked file to occupy zero TMS records, so a count of 1 must
        // not be storable at all.
        for value in [0, 1] {
            let error = LinkCount::new(value).expect_err("counts below two must not construct");
            assert!(matches!(error, MetadataError::InvalidRecord { .. }));
        }
        assert_eq!(LinkCount::new(2).expect("two links").get(), 2);
    }

    #[test]
    fn decrementing_to_one_link_signals_demotion_rather_than_storing_one() {
        // §5 calls demotion "the required inverse of promotion, not an optional
        // space optimization". Returning None forces the caller to perform it
        // instead of leaving a singly-referenced inode record behind.
        assert_eq!(LinkCount::PROMOTED.decrement(), None);

        let three = LinkCount::new(3).expect("three links");
        assert_eq!(three.decrement(), Some(LinkCount::PROMOTED));
    }

    #[test]
    fn link_counts_increment_for_additional_paths() {
        let promoted = LinkCount::PROMOTED;
        assert_eq!(promoted.increment().get(), 3);
        assert_eq!(promoted.increment().increment().get(), 4);
    }

    #[test]
    fn inode_zero_is_rejected() {
        let error = InodeNumber::new(0).expect_err("zero is reserved");
        assert!(matches!(error, MetadataError::InvalidRecord { .. }));
        assert_eq!(InodeNumber::new(7).expect("non-zero").get(), 7);
    }

    #[test]
    fn an_empty_namespace_id_is_rejected() {
        assert!(NamespaceId::new("").is_err());
        assert_eq!(
            NamespaceId::new("ns-a").expect("non-empty").as_str(),
            "ns-a"
        );
    }

    #[test]
    fn authority_follows_the_commit_not_the_cleanup() {
        // §5's crash-recovery table: the TMS commit is the linearization point,
        // so the original object stops being authoritative there — not when the
        // obsolete object is finally deleted.
        assert!(LinkTransitionState::Preparing.original_is_authoritative());
        assert!(LinkTransitionState::Aborted.original_is_authoritative());
        assert!(!LinkTransitionState::Committed.original_is_authoritative());
    }
}
