//! Atomic multi-record transactions.
//!
//! ADR 0003 §5's promotion commit is a single compare-and-swap that must apply
//! four changes together:
//!
//! ```text
//! source_path -> inode
//! new_path    -> inode
//! inode.link_count = 2
//! remove LinkTransition
//! ```
//!
//! > That TMS transaction is the linearization point of `link()`. Before it,
//! > `source_path` is authoritative and `new_path` does not exist. After it, the
//! > inode object is authoritative for both paths.
//!
//! Partial application has no valid interpretation. A crash between the two path
//! writes leaves one name resolving to an inode and the other to a
//! path-addressed object that post-commit cleanup is about to delete — the
//! divergence #363 is about, reintroduced one layer down.
//!
//! This is also why the capability exists. §7 requires hard links to have
//! "atomic transactions across transition, inode, and path index records", and a
//! backend that can only do single-key compare-and-swap must refuse
//! [`Capability::HardLinks`](crate::Capability::HardLinks) rather than emulate
//! the transaction with a sequence of writes.

use crate::record::{InodeNumber, InodeRecord, NamespaceId, PathIndexEntry};
use crate::revision::MappingRevision;

/// A precondition evaluated atomically before a transaction applies.
///
/// All conditions are checked against one linearizable view. If any fails, no
/// operation in the transaction is applied.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Precondition {
    /// The namespace's mapping revision equals this value.
    ///
    /// §5 requires the promotion commit to verify the mapping revision it was
    /// formed against. Without this, a transition prepared against one view of
    /// the namespace could commit after a concurrent transition already moved
    /// the same path.
    MappingRevisionIs {
        /// Namespace being fenced.
        namespace: NamespaceId,
        /// Revision the caller resolved against.
        expected: MappingRevision,
    },
    /// The path has no index entry — it is an ordinary single-path file.
    ///
    /// §5: "A current negative mapping proves that an ordinary unlinked object
    /// still uses its visible path."
    PathIsUnmapped {
        /// Namespace owning the path.
        namespace: NamespaceId,
        /// Path that must not resolve to an inode.
        path: String,
    },
    /// The path resolves to this inode.
    PathResolvesTo {
        /// Namespace owning the path.
        namespace: NamespaceId,
        /// Path being checked.
        path: String,
        /// Inode the path must resolve to.
        inode: InodeNumber,
    },
    /// A link transition with this operation id exists and is unchanged.
    ///
    /// Fences a resumed transition against its own predecessor: a recovering
    /// coordinator must not commit a transition that another has already
    /// aborted.
    TransitionExists {
        /// Namespace owning the transition.
        namespace: NamespaceId,
        /// Operation identifier.
        operation_id: String,
    },
    /// No link transition exists for this operation id.
    TransitionAbsent {
        /// Namespace owning the transition.
        namespace: NamespaceId,
        /// Operation identifier.
        operation_id: String,
    },
}

/// A single record mutation within a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Operation {
    /// Create or replace a path index entry.
    PutPathIndex(PathIndexEntry),
    /// Remove a path index entry.
    RemovePathIndex {
        /// Namespace owning the path.
        namespace: NamespaceId,
        /// Path to unmap.
        path: String,
    },
    /// Create or replace an inode record.
    PutInode(InodeRecord),
    /// Remove an inode record.
    ///
    /// Used by demotion, which §5 requires as "the required inverse of
    /// promotion, not an optional space optimization" — leaving the record
    /// behind would violate §3's sparsity claim.
    RemoveInode {
        /// Namespace owning the inode.
        namespace: NamespaceId,
        /// Inode to remove.
        inode: InodeNumber,
    },
    /// Create or replace a link transition record.
    PutTransition(crate::record::LinkTransition),
    /// Remove a link transition record.
    RemoveTransition {
        /// Namespace owning the transition.
        namespace: NamespaceId,
        /// Operation identifier.
        operation_id: String,
    },
    /// Advance the namespace mapping revision.
    ///
    /// §5: "Creating a transition advances the revision." Clients carrying an
    /// older revision are rejected with a stale-mapping error and refresh,
    /// which is what closes the race where a client holding a stale negative
    /// mapping writes to a visible path after promotion moved it.
    AdvanceMappingRevision {
        /// Namespace whose revision advances.
        namespace: NamespaceId,
    },
}

/// An all-or-nothing set of preconditions and operations.
///
/// Build with [`Transaction::new`], add preconditions with
/// [`Transaction::when`], and operations with [`Transaction::then`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Transaction {
    preconditions: Vec<Precondition>,
    operations: Vec<Operation>,
}

impl Transaction {
    /// An empty transaction.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a precondition.
    #[must_use]
    pub fn when(mut self, precondition: Precondition) -> Self {
        self.preconditions.push(precondition);
        self
    }

    /// Add an operation.
    #[must_use]
    pub fn then(mut self, operation: Operation) -> Self {
        self.operations.push(operation);
        self
    }

    /// Preconditions, in declaration order.
    pub fn preconditions(&self) -> &[Precondition] {
        &self.preconditions
    }

    /// Operations, in declaration order.
    pub fn operations(&self) -> &[Operation] {
        &self.operations
    }

    /// Whether the transaction would change nothing.
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }
}

/// Outcome of a committed transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionOutcome {
    /// Revision at which the transaction committed.
    pub revision: crate::revision::StoreRevision,
    /// Namespace mapping revision after any advance, when one was requested.
    pub mapping_revision: Option<MappingRevision>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn namespace() -> NamespaceId {
        NamespaceId::new("ns").expect("valid namespace")
    }

    #[test]
    fn an_empty_transaction_changes_nothing() {
        assert!(Transaction::new().is_empty());
    }

    #[test]
    fn preconditions_and_operations_keep_declaration_order() {
        // Order matters for diagnostics: when a commit fails, the first failing
        // precondition is the one worth reporting.
        let transaction = Transaction::new()
            .when(Precondition::MappingRevisionIs {
                namespace: namespace(),
                expected: MappingRevision::INITIAL,
            })
            .when(Precondition::PathIsUnmapped {
                namespace: namespace(),
                path: "b.bin".to_owned(),
            })
            .then(Operation::AdvanceMappingRevision {
                namespace: namespace(),
            });

        assert_eq!(transaction.preconditions().len(), 2);
        assert!(matches!(
            transaction.preconditions()[0],
            Precondition::MappingRevisionIs { .. }
        ));
        assert!(matches!(
            transaction.preconditions()[1],
            Precondition::PathIsUnmapped { .. }
        ));
        assert_eq!(transaction.operations().len(), 1);
        assert!(!transaction.is_empty());
    }
}
