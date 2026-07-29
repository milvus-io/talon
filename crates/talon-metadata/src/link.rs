//! The hard-link promotion and demotion state machine (ADR 0003 §5).
//!
//! Promotion spans two systems that cannot share a transaction:
//!
//! > The copy, TMS mutation, and old-object deletion cannot be one cross-system
//! > transaction, so the transition is an explicit durable state machine.
//!
//! The durable [`LinkTransition`] record is what makes recovery decidable. Without
//! it, a crash leaves an inode object and a path object with no way to tell which
//! one holds the current data.
//!
//! # The pivot is the commit, not the cleanup
//!
//! > That TMS transaction is the linearization point of `link()`. Before it,
//! > `source_path` is authoritative and `new_path` does not exist. After it, the
//! > inode object is authoritative for both paths.
//!
//! Deleting the obsolete path object afterwards is cleanup. A crash between the
//! commit and that delete loses nothing — the inode object already holds the
//! data, and recovery just finishes the tidying.
//!
//! # Demotion is required
//!
//! > This is the required inverse of promotion, not an optional space
//! > optimization.
//!
//! Leaving a singly-referenced inode record behind would violate §3's sparsity
//! claim and strand an object outside its visible path, where no plain S3 client
//! could find it. [`LinkCount::decrement`] returning `None` at one link is what
//! forces callers here.

use crate::error::{MetadataError, MetadataResult};
use crate::record::{
    InodeNumber, InodeRecord, LinkCount, LinkTransition, LinkTransitionState, NamespaceId,
    PathIndexEntry,
};
use crate::revision::MappingRevision;
use crate::transaction::{Operation, Precondition, Transaction};

/// Where a crashed transition left the world, and what to do about it.
///
/// Mirrors §5's recovery table one variant per row. The variants are named for
/// the *action*, not the crash point, because that is what a recovering
/// coordinator needs to decide.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recovery {
    /// The copy never completed. Abort; the original path object still holds the
    /// data and nothing else was published.
    Abort,
    /// The copy completed but the commit did not. Either finish the commit or
    /// delete the orphaned inode object — both are correct, because the original
    /// path object is still authoritative and the inode object is not yet
    /// referenced.
    ResumeOrDiscard,
    /// The commit succeeded. Delete the obsolete path object; the inode object
    /// is authoritative.
    FinishCleanup,
}

/// Whether the inode object exists in the object store.
///
/// A recovering coordinator establishes this by asking a worker to stat the
/// inode object; §7 keeps object-store credentials out of the coordinator, so it
/// never looks itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InodeObject {
    /// The copy has not been observed to complete.
    Absent,
    /// The copy completed and verified.
    Present,
}

/// Decide how to recover a transition found after a crash.
///
/// Implements §5's recovery table:
///
/// | Crash point | Authoritative data | Recovery |
/// |---|---|---|
/// | Before inode copy | Original path object | Abort the transition |
/// | After copy, before TMS commit | Original path object | Resume commit or delete the orphan inode object |
/// | After TMS commit, before old-object deletion | Inode object | Delete the obsolete path object |
/// | After deletion | Inode object | No action |
///
/// The last row needs no entry here: once the transition record is gone there is
/// nothing to recover, which is why this takes a transition rather than an
/// `Option`.
pub fn recovery_for(transition: &LinkTransition, inode_object: InodeObject) -> Recovery {
    match (transition.state, inode_object) {
        // Committed: the inode object is authoritative regardless of what the
        // old path object still contains.
        (LinkTransitionState::Committed, _) => Recovery::FinishCleanup,
        // Aborted, or preparing with no copy: nothing was published.
        (LinkTransitionState::Aborted, _)
        | (LinkTransitionState::Preparing, InodeObject::Absent) => Recovery::Abort,
        // The ambiguous row. The copy exists but no reference to it does, so the
        // original is still authoritative and either direction is safe.
        (LinkTransitionState::Preparing, InodeObject::Present) => Recovery::ResumeOrDiscard,
    }
}

/// The transaction that opens a promotion.
///
/// Creates the `PREPARING` record and advances the namespace mapping revision.
/// The revision bump is not bookkeeping — it is the fence:
///
/// > Creating a transition advances the revision. [...] Stale clients receive
/// > `STALE_MAPPING`, refresh through any coordinator, and retry. A current
/// > negative mapping proves that an ordinary unlinked object still uses its
/// > visible path.
///
/// Without it, a client holding a stale negative mapping could write to the
/// visible-path object after promotion moved the data to the inode object, and
/// that write would be silently lost.
pub fn begin_promotion(transition: &LinkTransition) -> MetadataResult<Transaction> {
    if transition.state != LinkTransitionState::Preparing {
        return Err(MetadataError::InvalidRecord {
            detail: format!("a promotion opens in Preparing, not {:?}", transition.state),
        });
    }
    Ok(Transaction::new()
        .when(Precondition::MappingRevisionIs {
            namespace: transition.namespace.clone(),
            expected: transition.expected_mapping_revision,
        })
        // Both paths must be unmapped: the source because promoting an
        // already-promoted file would overwrite its inode reference, the target
        // because linking onto an existing link is a different operation.
        .when(Precondition::PathIsUnmapped {
            namespace: transition.namespace.clone(),
            path: transition.source_path.clone(),
        })
        .when(Precondition::PathIsUnmapped {
            namespace: transition.namespace.clone(),
            path: transition.new_path.clone(),
        })
        // Fences a resumed transition against a concurrent one for the same
        // operation: a coordinator that lost its lease must not create a second
        // record under the same id.
        .when(Precondition::TransitionAbsent {
            namespace: transition.namespace.clone(),
            operation_id: transition.operation_id.clone(),
        })
        .then(Operation::PutTransition(transition.clone()))
        .then(Operation::AdvanceMappingRevision {
            namespace: transition.namespace.clone(),
        }))
}

/// The transaction that commits a promotion.
///
/// This is the linearization point of `link()`. It applies §5's four changes
/// together:
///
/// ```text
/// source_path -> inode
/// new_path    -> inode
/// inode.link_count = 2
/// remove LinkTransition
/// ```
///
/// The preconditions verify the operation id and the mapping revision, so a
/// transition prepared against one view of the namespace cannot commit after a
/// concurrent transition already moved the same path.
pub fn commit_promotion(
    transition: &LinkTransition,
    observed_revision: MappingRevision,
) -> Transaction {
    Transaction::new()
        .when(Precondition::TransitionExists {
            namespace: transition.namespace.clone(),
            operation_id: transition.operation_id.clone(),
        })
        .when(Precondition::MappingRevisionIs {
            namespace: transition.namespace.clone(),
            expected: observed_revision,
        })
        .then(Operation::PutPathIndex(PathIndexEntry {
            namespace: transition.namespace.clone(),
            path: transition.source_path.clone(),
            inode: transition.inode,
        }))
        .then(Operation::PutPathIndex(PathIndexEntry {
            namespace: transition.namespace.clone(),
            path: transition.new_path.clone(),
            inode: transition.inode,
        }))
        .then(Operation::PutInode(InodeRecord {
            namespace: transition.namespace.clone(),
            inode: transition.inode,
            link_count: LinkCount::PROMOTED,
            corrupt: false,
        }))
        .then(Operation::RemoveTransition {
            namespace: transition.namespace.clone(),
            operation_id: transition.operation_id.clone(),
        })
        .then(Operation::AdvanceMappingRevision {
            namespace: transition.namespace.clone(),
        })
}

/// The transaction that abandons a promotion.
///
/// Removes the transition record and advances the revision so clients stop
/// receiving the retryable error the `PREPARING` record caused.
pub fn abort_promotion(transition: &LinkTransition) -> Transaction {
    Transaction::new()
        .when(Precondition::TransitionExists {
            namespace: transition.namespace.clone(),
            operation_id: transition.operation_id.clone(),
        })
        .then(Operation::RemoveTransition {
            namespace: transition.namespace.clone(),
            operation_id: transition.operation_id.clone(),
        })
        .then(Operation::AdvanceMappingRevision {
            namespace: transition.namespace.clone(),
        })
}

/// The transaction that adds a third or later link.
///
/// > Adding a third or later link needs no object copy: one TMS transaction adds
/// > the new path index, increments the bounded link count, and advances the
/// > mapping revision.
///
/// No transition record and no object-store work: the data is already
/// inode-addressed, so a new name is purely a new reference.
pub fn add_link(
    namespace: &NamespaceId,
    new_path: &str,
    inode: InodeNumber,
    current: LinkCount,
    observed_revision: MappingRevision,
) -> Transaction {
    Transaction::new()
        .when(Precondition::MappingRevisionIs {
            namespace: namespace.clone(),
            expected: observed_revision,
        })
        .when(Precondition::PathIsUnmapped {
            namespace: namespace.clone(),
            path: new_path.to_owned(),
        })
        .then(Operation::PutPathIndex(PathIndexEntry {
            namespace: namespace.clone(),
            path: new_path.to_owned(),
            inode,
        }))
        .then(Operation::PutInode(InodeRecord {
            namespace: namespace.clone(),
            inode,
            link_count: current.increment(),
            corrupt: false,
        }))
        .then(Operation::AdvanceMappingRevision {
            namespace: namespace.clone(),
        })
}

/// The transaction that removes one link while at least two will remain.
///
/// > Removing a link while at least two paths remain is the inverse transaction.
///
/// # Errors
///
/// Returns [`MetadataError::InvalidRecord`] when the removal would leave one
/// link. That case is a demotion, not a link removal, and must go through
/// [`commit_demotion`] so the object returns to its visible path.
pub fn remove_link(
    namespace: &NamespaceId,
    path: &str,
    inode: InodeNumber,
    current: LinkCount,
    observed_revision: MappingRevision,
) -> MetadataResult<Transaction> {
    let remaining = current
        .decrement()
        .ok_or_else(|| MetadataError::InvalidRecord {
            detail: "removing this link leaves one: use commit_demotion (ADR 0003 §5)".to_owned(),
        })?;
    Ok(Transaction::new()
        .when(Precondition::MappingRevisionIs {
            namespace: namespace.clone(),
            expected: observed_revision,
        })
        .when(Precondition::PathResolvesTo {
            namespace: namespace.clone(),
            path: path.to_owned(),
            inode,
        })
        .then(Operation::RemovePathIndex {
            namespace: namespace.clone(),
            path: path.to_owned(),
        })
        .then(Operation::PutInode(InodeRecord {
            namespace: namespace.clone(),
            inode,
            link_count: remaining,
            corrupt: false,
        }))
        .then(Operation::AdvanceMappingRevision {
            namespace: namespace.clone(),
        }))
}

/// The transaction that demotes back to path-addressed storage.
///
/// The linearization point of the `unlink()` that removes one of the final two
/// links. §5 step 3: "atomically remove both path indexes and the inode record
/// from TMS, recording the new mapping revision".
///
/// Removing the inode record is not optional. Leaving it would keep a
/// singly-referenced record in TMS, violating §3's sparsity claim, and would
/// leave the data at an internal address that no plain S3 client can reach.
pub fn commit_demotion(
    namespace: &NamespaceId,
    removed_path: &str,
    remaining_path: &str,
    inode: InodeNumber,
    observed_revision: MappingRevision,
) -> Transaction {
    Transaction::new()
        .when(Precondition::MappingRevisionIs {
            namespace: namespace.clone(),
            expected: observed_revision,
        })
        .when(Precondition::PathResolvesTo {
            namespace: namespace.clone(),
            path: removed_path.to_owned(),
            inode,
        })
        .when(Precondition::PathResolvesTo {
            namespace: namespace.clone(),
            path: remaining_path.to_owned(),
            inode,
        })
        .then(Operation::RemovePathIndex {
            namespace: namespace.clone(),
            path: removed_path.to_owned(),
        })
        .then(Operation::RemovePathIndex {
            namespace: namespace.clone(),
            path: remaining_path.to_owned(),
        })
        .then(Operation::RemoveInode {
            namespace: namespace.clone(),
            inode,
        })
        .then(Operation::AdvanceMappingRevision {
            namespace: namespace.clone(),
        })
}

/// Whether `resumer` may take over a transition owned by another worker.
///
/// > If that worker fails, any coordinator may fence it and assign another
/// > worker to resume the durable transition.
///
/// The incarnation is what makes this safe: a worker that restarts comes back
/// with a new incarnation, so it cannot resume its predecessor's work as though
/// no crash had occurred. Same worker and same incarnation means the original
/// owner is still alive and the transition is not orphaned.
pub fn may_resume(transition: &LinkTransition, resumer: &str, resumer_incarnation: &str) -> bool {
    transition.operation_worker != resumer
        || transition.operation_worker_incarnation != resumer_incarnation
}

#[cfg(test)]
mod tests {
    use super::*;

    fn namespace() -> NamespaceId {
        NamespaceId::new("ns").expect("valid namespace")
    }

    fn inode() -> InodeNumber {
        InodeNumber::new(9).expect("non-zero inode")
    }

    fn transition(state: LinkTransitionState) -> LinkTransition {
        LinkTransition {
            namespace: namespace(),
            operation_id: "op-1".to_owned(),
            state,
            source_path: "data/a.bin".to_owned(),
            new_path: "data/b.bin".to_owned(),
            source_version: "v1".to_owned(),
            expected_mapping_revision: MappingRevision::INITIAL,
            inode: inode(),
            operation_worker: "w0".to_owned(),
            operation_worker_incarnation: "inc-0".to_owned(),
        }
    }

    #[test]
    fn a_crash_before_the_copy_aborts() {
        // Row 1 of §5's recovery table. Nothing was published, so the original
        // path object is untouched and the transition is simply dropped.
        assert_eq!(
            recovery_for(
                &transition(LinkTransitionState::Preparing),
                InodeObject::Absent
            ),
            Recovery::Abort
        );
    }

    #[test]
    fn a_crash_after_the_copy_but_before_the_commit_may_go_either_way() {
        // Row 2. The inode object exists but nothing references it, so the
        // original is still authoritative and both finishing and discarding are
        // correct. The ADR says as much: "Resume commit or delete the orphan
        // inode object".
        assert_eq!(
            recovery_for(
                &transition(LinkTransitionState::Preparing),
                InodeObject::Present
            ),
            Recovery::ResumeOrDiscard
        );
    }

    #[test]
    fn a_crash_after_the_commit_only_needs_cleanup() {
        // Row 3. The commit is the linearization point, so the inode object is
        // authoritative and the obsolete path object is just garbage to remove.
        assert_eq!(
            recovery_for(
                &transition(LinkTransitionState::Committed),
                InodeObject::Present
            ),
            Recovery::FinishCleanup
        );
    }

    #[test]
    fn a_committed_transition_recovers_the_same_way_whatever_the_object_store_shows() {
        // The commit decides authority, not the object store's contents. If a
        // committed transition were treated as recoverable-either-way when the
        // inode object appeared absent, recovery could discard data that
        // link() already reported as durable.
        for object in [InodeObject::Absent, InodeObject::Present] {
            assert_eq!(
                recovery_for(&transition(LinkTransitionState::Committed), object),
                Recovery::FinishCleanup
            );
        }
    }

    #[test]
    fn a_promotion_must_open_in_preparing() {
        let error = begin_promotion(&transition(LinkTransitionState::Committed))
            .expect_err("only Preparing opens a promotion");
        assert!(matches!(error, MetadataError::InvalidRecord { .. }));
    }

    #[test]
    fn opening_a_promotion_fences_both_paths_and_advances_the_revision() {
        // The revision bump is the fence that stops a client with a stale
        // negative mapping from writing to the visible-path object after the
        // data moves.
        let transaction =
            begin_promotion(&transition(LinkTransitionState::Preparing)).expect("valid");
        assert!(transaction.preconditions().iter().any(|p| matches!(
            p,
            Precondition::PathIsUnmapped { path, .. } if path == "data/a.bin"
        )));
        assert!(transaction.preconditions().iter().any(|p| matches!(
            p,
            Precondition::PathIsUnmapped { path, .. } if path == "data/b.bin"
        )));
        assert!(transaction
            .operations()
            .iter()
            .any(|op| matches!(op, Operation::AdvanceMappingRevision { .. })));
    }

    #[test]
    fn the_commit_applies_all_four_changes_together() {
        // §5's linearization point. Any subset of these is an inconsistent
        // namespace, which is why they share one transaction.
        let transaction = commit_promotion(
            &transition(LinkTransitionState::Preparing),
            MappingRevision::new(1),
        );
        let paths = transaction
            .operations()
            .iter()
            .filter(|op| matches!(op, Operation::PutPathIndex(_)))
            .count();
        assert_eq!(paths, 2, "both names must map to the inode");
        assert!(transaction
            .operations()
            .iter()
            .any(|op| matches!(op, Operation::PutInode(_))));
        assert!(transaction
            .operations()
            .iter()
            .any(|op| matches!(op, Operation::RemoveTransition { .. })));
    }

    #[test]
    fn adding_a_third_link_copies_nothing() {
        // The data is already inode-addressed, so a new name is purely a new
        // reference. An object copy here would be pure waste and a new source of
        // divergence.
        let transaction = add_link(
            &namespace(),
            "data/c.bin",
            inode(),
            LinkCount::PROMOTED,
            MappingRevision::new(2),
        );
        assert!(
            !transaction
                .operations()
                .iter()
                .any(|op| matches!(op, Operation::PutTransition(_))),
            "no transition record: there is no object work to fence"
        );
        let count = transaction.operations().iter().find_map(|op| match op {
            Operation::PutInode(record) => Some(record.link_count),
            _ => None,
        });
        assert_eq!(count.map(|c| c.get()), Some(3));
    }

    #[test]
    fn removing_the_second_to_last_link_refuses_and_points_at_demotion() {
        // Dropping to one link is a demotion, not a link removal: the object has
        // to move back to its visible path. Allowing it here would strand the
        // data at an internal address and leave a singly-referenced record in
        // TMS, violating §3's sparsity claim.
        let error = remove_link(
            &namespace(),
            "data/b.bin",
            inode(),
            LinkCount::PROMOTED,
            MappingRevision::new(2),
        )
        .expect_err("dropping to one link must not be an ordinary removal");
        assert!(matches!(error, MetadataError::InvalidRecord { .. }));
    }

    #[test]
    fn removing_a_link_from_three_leaves_two() {
        let transaction = remove_link(
            &namespace(),
            "data/c.bin",
            inode(),
            LinkCount::new(3).expect("three links"),
            MappingRevision::new(3),
        )
        .expect("three links may drop to two");
        let count = transaction.operations().iter().find_map(|op| match op {
            Operation::PutInode(record) => Some(record.link_count),
            _ => None,
        });
        assert_eq!(count, Some(LinkCount::PROMOTED));
    }

    #[test]
    fn demotion_removes_both_paths_and_the_inode_record() {
        // Leaving the inode record would keep a singly-referenced entry in TMS
        // and leave the data where no plain S3 client can read it -- the
        // property §2 exists to protect.
        let transaction = commit_demotion(
            &namespace(),
            "data/b.bin",
            "data/a.bin",
            inode(),
            MappingRevision::new(4),
        );
        let removals = transaction
            .operations()
            .iter()
            .filter(|op| matches!(op, Operation::RemovePathIndex { .. }))
            .count();
        assert_eq!(removals, 2);
        assert!(transaction
            .operations()
            .iter()
            .any(|op| matches!(op, Operation::RemoveInode { .. })));
    }

    #[test]
    fn a_restarted_worker_may_resume_its_own_orphaned_transition() {
        // The incarnation is the discriminator: a worker that restarts comes
        // back with a new one, so this is a genuine takeover rather than a
        // process pretending no crash occurred.
        let transition = transition(LinkTransitionState::Preparing);
        assert!(may_resume(&transition, "w0", "inc-1"));
        assert!(may_resume(&transition, "w1", "inc-0"));
    }

    #[test]
    fn a_live_owner_is_not_resumable_by_itself() {
        // Same worker, same incarnation means nothing crashed. Treating that as
        // resumable would let a slow-but-alive worker's transition be taken over
        // and duplicated.
        let transition = transition(LinkTransitionState::Preparing);
        assert!(!may_resume(&transition, "w0", "inc-0"));
    }
}
