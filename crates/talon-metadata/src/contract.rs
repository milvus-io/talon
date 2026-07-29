//! Shared contract suite for [`MetadataStore`](crate::MetadataStore) backends.
//!
//! Every backend runs these identical cases. The point is that ADR 0003's
//! correctness arguments rest on *contract* properties, not on any one
//! implementation — so a backend that quietly weakens one must fail here rather
//! than surface later as a hard-link divergence in production.
//!
//! The suite covers the refusal paths as deliberately as the happy paths. §7
//! requires a backend that cannot honour a capability to reject the operation,
//! and §4 calls approximating it "precisely the defect in #363". A backend that
//! silently half-applied a transaction would pass a happy-path-only suite.
//!
//! Exported rather than `#[cfg(test)]`-private so the etcd backend can run the
//! same cases against a real server.

use crate::capability::Capability;
use crate::error::MetadataError;
use crate::memory::MemoryMetadataStore;
use crate::record::{InodeRecord, LinkCount, NamespaceId, PathIndexEntry};
use crate::revision::MappingRevision;
use crate::transaction::{Operation, Precondition, Transaction};
use crate::{InodeNumber, MetadataStore};

fn namespace() -> NamespaceId {
    NamespaceId::new("contract-ns").expect("valid namespace")
}

fn inode(value: u64) -> InodeNumber {
    InodeNumber::new(value).expect("non-zero inode")
}

/// A promotion commit, as specified by ADR 0003 §5.
///
/// Mirrors the four changes the ADR requires to apply together:
///
/// ```text
/// source_path -> inode
/// new_path    -> inode
/// inode.link_count = 2
/// remove LinkTransition
/// ```
fn promotion(
    ns: &NamespaceId,
    source_path: &str,
    new_path: &str,
    ino: InodeNumber,
    expected_revision: MappingRevision,
) -> Transaction {
    Transaction::new()
        .when(Precondition::MappingRevisionIs {
            namespace: ns.clone(),
            expected: expected_revision,
        })
        .then(Operation::PutPathIndex(PathIndexEntry {
            namespace: ns.clone(),
            path: source_path.to_owned(),
            inode: ino,
        }))
        .then(Operation::PutPathIndex(PathIndexEntry {
            namespace: ns.clone(),
            path: new_path.to_owned(),
            inode: ino,
        }))
        .then(Operation::PutInode(InodeRecord {
            namespace: ns.clone(),
            inode: ino,
            link_count: LinkCount::PROMOTED,
            corrupt: false,
        }))
        .then(Operation::AdvanceMappingRevision {
            namespace: ns.clone(),
        })
}

/// An ordinary single-path file has no TMS record.
///
/// This is §3's sparsity claim at the store boundary, and it is what makes an
/// etcd-class backend viable: per-object records for a billion-object bucket
/// would exceed etcd's in-memory keyspace. `resolve_path` returning `None` is
/// the normal case, not an error.
pub async fn unmapped_paths_resolve_to_nothing(store: &impl MetadataStore) {
    let ns = namespace();
    let resolved = store
        .resolve_path(&ns, "ordinary/file.bin")
        .await
        .expect("resolving an unmapped path is not an error");
    assert_eq!(
        resolved, None,
        "a single-path file must occupy zero records (ADR 0003 §3)"
    );
}

/// A namespace with no transitions reports the initial mapping revision.
///
/// Sparsity covers the mapping guard too: an untouched namespace stores nothing.
pub async fn untouched_namespaces_report_the_initial_revision(store: &impl MetadataStore) {
    let ns = NamespaceId::new("never-touched").expect("valid namespace");
    let revision = store
        .mapping_revision(&ns)
        .await
        .expect("an untouched namespace has a revision without storing one");
    assert_eq!(revision, MappingRevision::INITIAL);
}

/// A backend refuses operations whose capability it does not advertise.
///
/// §7: "A backend that cannot satisfy one of these contracts must not advertise
/// that capability." The refusal must be a capability gap, not a transient
/// error, so callers can map it to the errno for "this cluster does not offer
/// the feature" rather than retrying.
pub async fn unsupported_capabilities_are_refused_not_approximated(store: &impl MetadataStore) {
    if store.supports(Capability::HardLinks) {
        return;
    }
    let ns = namespace();
    let error = store
        .resolve_path(&ns, "a.bin")
        .await
        .expect_err("a backend without hard links must refuse to resolve paths");
    assert!(
        error.is_capability_gap(),
        "refusal must be a capability gap, got {error}"
    );
    assert!(
        !error.is_retryable(),
        "a missing capability is permanent; retrying would spin"
    );
}

/// A failed precondition applies none of the transaction's operations.
///
/// The property §5's promotion commit depends on. A partially applied promotion
/// would leave one path resolving to an inode and the other to a path-addressed
/// object that cleanup is about to delete — reintroducing #363's divergence one
/// layer down.
pub fn a_failed_precondition_applies_nothing(store: &MemoryMetadataStore) {
    let ns = namespace();
    let ino = inode(42);

    let stale = promotion(&ns, "a.bin", "b.bin", ino, MappingRevision::new(99));
    let error = store
        .commit(&stale)
        .expect_err("a stale mapping revision must not commit");
    assert!(matches!(error, MetadataError::CompareAndSwapFailed { .. }));

    let state = store.state_snapshot(&ns);
    assert!(
        state.paths.is_empty(),
        "no path index entry may survive a failed transaction"
    );
    assert!(
        state.inodes.is_empty(),
        "no inode record may survive a failed transaction"
    );
    assert_eq!(
        state.mapping_revision,
        MappingRevision::INITIAL,
        "the mapping revision must not advance on a failed transaction"
    );
}

/// A promotion commit applies all four changes together.
pub fn a_promotion_commits_atomically(store: &MemoryMetadataStore) {
    let ns = namespace();
    let ino = inode(7);

    let outcome = store
        .commit(&promotion(
            &ns,
            "data/a.bin",
            "data/b.bin",
            ino,
            MappingRevision::INITIAL,
        ))
        .expect("promotion commits against the current revision");

    assert_eq!(
        outcome.mapping_revision,
        Some(MappingRevision::new(1)),
        "creating a transition advances the namespace revision (§5)"
    );

    let state = store.state_snapshot(&ns);
    assert_eq!(state.paths.get("data/a.bin"), Some(&ino));
    assert_eq!(
        state.paths.get("data/b.bin"),
        Some(&ino),
        "both names must resolve to the inode after the linearization point"
    );
    assert_eq!(
        state.inodes.get(&ino.get()).map(|record| record.link_count),
        Some(LinkCount::PROMOTED)
    );
}

/// A second commit against a consumed revision fails.
///
/// The race §5's mapping revision exists to close: a transition prepared
/// against one view of the namespace must not commit after a concurrent
/// transition already moved the same path.
pub fn a_consumed_mapping_revision_cannot_commit_twice(store: &MemoryMetadataStore) {
    let ns = namespace();

    store
        .commit(&promotion(
            &ns,
            "x.bin",
            "y.bin",
            inode(1),
            MappingRevision::INITIAL,
        ))
        .expect("first promotion commits");

    let error = store
        .commit(&promotion(
            &ns,
            "x.bin",
            "z.bin",
            inode(2),
            MappingRevision::INITIAL,
        ))
        .expect_err("a replayed revision must not commit");
    assert!(matches!(error, MetadataError::CompareAndSwapFailed { .. }));
}

/// A negative mapping precondition rejects an already-mapped path.
///
/// §5: "A current negative mapping proves that an ordinary unlinked object
/// still uses its visible path."
pub fn an_already_mapped_path_fails_the_unmapped_precondition(store: &MemoryMetadataStore) {
    let ns = namespace();
    let ino = inode(5);

    store
        .commit(&promotion(
            &ns,
            "p.bin",
            "q.bin",
            ino,
            MappingRevision::INITIAL,
        ))
        .expect("promotion commits");

    let transaction = Transaction::new()
        .when(Precondition::PathIsUnmapped {
            namespace: ns.clone(),
            path: "p.bin".to_owned(),
        })
        .then(Operation::AdvanceMappingRevision {
            namespace: ns.clone(),
        });

    store
        .commit(&transaction)
        .expect_err("a mapped path must fail the unmapped precondition");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_backend_satisfies_the_async_contract() {
        let store = MemoryMetadataStore::new();
        unmapped_paths_resolve_to_nothing(&store).await;
        untouched_namespaces_report_the_initial_revision(&store).await;
    }

    #[tokio::test]
    async fn a_store_without_capabilities_refuses_rather_than_approximating() {
        let store = MemoryMetadataStore::without_capabilities();
        unsupported_capabilities_are_refused_not_approximated(&store).await;
    }

    #[test]
    fn memory_backend_satisfies_the_transaction_contract() {
        a_failed_precondition_applies_nothing(&MemoryMetadataStore::new());
        a_promotion_commits_atomically(&MemoryMetadataStore::new());
        a_consumed_mapping_revision_cannot_commit_twice(&MemoryMetadataStore::new());
        an_already_mapped_path_fails_the_unmapped_precondition(&MemoryMetadataStore::new());
    }
}
