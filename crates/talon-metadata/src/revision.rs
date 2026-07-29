//! Opaque revision tokens.
//!
//! A revision is a backend-issued token, never a number Talon computes. ADR
//! 0003 §7 requires watch-from-revision with an explicit `Compacted` variant,
//! which only works if the token round-trips through Talon unmodified.
//!
//! Treating it as opaque also keeps three unrelated revisions from being
//! confused. §4 and §9.3 are explicit that they are distinct:
//!
//! > The capability revision and §9's write-routing revision are not ADR 0001's
//! > membership-derived `PlacementVersion`.
//!
//! They advance for different reasons — membership changes immediately alter
//! the desired HRW ranking, while effective write ownership changes only after
//! a handoff or recovery commits — so a value from one is meaningless to the
//! others.

use core::fmt;

use crate::error::{MetadataError, MetadataResult};

/// An opaque, backend-issued revision token.
///
/// Talon compares these for equality and hands them back to the backend. It
/// never parses, orders, or arithmetically advances one: etcd revisions are
/// monotonic integers, but that is a backend detail and other backends need not
/// share it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StoreRevision(String);

impl StoreRevision {
    /// Construct a revision from a non-empty backend token.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataError::InvalidRevision`] when the token is empty. An
    /// empty token would silently behave like "from the beginning" in a
    /// watch resume, which is exactly the case that must surface as an error
    /// rather than a full replay.
    pub fn new(value: impl Into<String>) -> MetadataResult<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(MetadataError::InvalidRevision {
                backend: None,
                revision: value,
                detail: "revision must not be empty",
            });
        }
        Ok(Self(value))
    }

    /// Borrow the backend token without interpreting it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StoreRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A namespace-scoped mapping revision (ADR 0003 §5).
///
/// Distinct from [`StoreRevision`]: this one Talon *does* own and advance.
///
/// > A namespace with the hard-link capability therefore has one monotonically
/// > increasing `MappingRevision` and a TMS-backed mutation guard.
///
/// Every write and namespace mutation carries the revision it resolved against,
/// and workers reject an older one with a stale-mapping error. That is what
/// stops a client holding a cached path mapping from writing to an object that
/// a concurrent hard-link promotion has already moved to inode-addressed
/// storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct MappingRevision(u64);

impl MappingRevision {
    /// The revision of a namespace that has never had a mapping transition.
    pub const INITIAL: Self = Self(0);

    /// Construct from a raw counter value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The raw counter value, for durable encoding and the wire protocol.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The next revision.
    ///
    /// # Panics
    ///
    /// Panics on overflow. At one transition per nanosecond this would take
    /// roughly 585 years; a wrap would silently make a stale mapping look
    /// current, so aborting is the safer failure.
    #[must_use]
    pub const fn next(self) -> Self {
        match self.0.checked_add(1) {
            Some(value) => Self(value),
            None => panic!("mapping revision overflowed"),
        }
    }
}

impl fmt::Display for MappingRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_revision_token_is_rejected() {
        // An empty token would read as "resume from the beginning" instead of
        // failing, turning a lost position into a silent full replay.
        let error = StoreRevision::new("").expect_err("empty token must not construct");
        assert!(matches!(error, MetadataError::InvalidRevision { .. }));
    }

    #[test]
    fn a_revision_token_round_trips_unmodified() {
        // The backend must get back exactly what it issued; Talon never
        // normalises or reformats the token.
        let token = "\"etcd-rev-9007199254740993\"";
        let revision = StoreRevision::new(token).expect("non-empty token");
        assert_eq!(revision.as_str(), token);
        assert_eq!(revision.to_string(), token);
    }

    #[test]
    fn mapping_revisions_advance_monotonically_from_initial() {
        let first = MappingRevision::INITIAL;
        let second = first.next();
        assert!(second > first);
        assert_eq!(second.get(), 1);
        assert_eq!(second.next().get(), 2);
    }

    #[test]
    fn mapping_revision_ordering_is_numeric_not_lexicographic() {
        // Guards against encoding the counter as a string somewhere along the
        // way: "10" sorts before "9" lexicographically, which would make a
        // newer mapping look stale and admit a write that should be fenced.
        assert!(MappingRevision::new(10) > MappingRevision::new(9));
    }
}
