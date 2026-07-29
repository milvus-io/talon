//! The §5 promotion lifecycle executed against a real store.
//!
//! The unit tests in `link.rs` check the *shape* of each transaction. These run
//! them, which is what proves the invariants actually hold: that a crash at each
//! point in ADR 0003 §5's recovery table leaves a recoverable world, and that a
//! stale participant cannot commit over a concurrent one.
//!
//! Everything here uses the memory backend, so a crash is simulated by simply
//! not issuing the next transaction — which is exactly what a crash is from the
//! store's point of view.

use talon_metadata::link::{
    abort_promotion, add_link, begin_promotion, commit_demotion, commit_promotion, recovery_for,
    remove_link, InodeObject, Recovery,
};
use talon_metadata::{
    InodeNumber, LinkCount, LinkTransition, LinkTransitionState, MappingRevision,
    MemoryMetadataStore, MetadataStore, NamespaceId,
};

fn namespace() -> NamespaceId {
    NamespaceId::new("link-ns").expect("valid namespace")
}

fn inode(value: u64) -> InodeNumber {
    InodeNumber::new(value).expect("non-zero inode")
}

fn transition(revision: MappingRevision) -> LinkTransition {
    LinkTransition {
        namespace: namespace(),
        operation_id: "op-1".to_owned(),
        state: LinkTransitionState::Preparing,
        source_path: "data/a.bin".to_owned(),
        new_path: "data/b.bin".to_owned(),
        source_version: "v1".to_owned(),
        expected_mapping_revision: revision,
        inode: inode(1),
        operation_worker: "w0".to_owned(),
        operation_worker_incarnation: "inc-0".to_owned(),
    }
}

#[tokio::test]
async fn a_full_promotion_makes_both_names_resolve_to_the_inode() {
    let store = MemoryMetadataStore::new();
    let ns = namespace();
    let transition = transition(MappingRevision::INITIAL);

    let opened = store
        .commit(&begin_promotion(&transition).expect("valid promotion"))
        .await
        .expect("promotion opens");
    let after_open = opened.mapping_revision.expect("revision advanced");

    // Before the commit, neither name resolves: the source is still an ordinary
    // path-addressed object and the target does not exist yet.
    assert_eq!(
        store
            .resolve_path(&ns, "data/a.bin")
            .await
            .expect("resolve"),
        None,
        "the source stays path-addressed until the commit"
    );

    store
        .commit(&commit_promotion(&transition, after_open))
        .await
        .expect("promotion commits");

    for path in ["data/a.bin", "data/b.bin"] {
        assert_eq!(
            store
                .resolve_path(&ns, path)
                .await
                .expect("resolve")
                .map(|entry| entry.inode),
            Some(inode(1)),
            "{path} must resolve to the inode after the linearization point"
        );
    }
    assert_eq!(
        store
            .load_inode(&ns, inode(1))
            .await
            .expect("inode record")
            .link_count,
        LinkCount::PROMOTED
    );
}

#[tokio::test]
async fn a_crash_before_the_copy_leaves_the_source_readable() {
    // Row 1 of §5's recovery table. The transition exists but nothing was
    // published, so the original object is untouched and abort is the answer.
    let store = MemoryMetadataStore::new();
    let ns = namespace();
    let transition = transition(MappingRevision::INITIAL);

    store
        .commit(&begin_promotion(&transition).expect("valid"))
        .await
        .expect("promotion opens");

    assert_eq!(
        recovery_for(&transition, InodeObject::Absent),
        Recovery::Abort
    );

    store
        .commit(&abort_promotion(&transition))
        .await
        .expect("abort commits");

    assert_eq!(
        store
            .resolve_path(&ns, "data/a.bin")
            .await
            .expect("resolve"),
        None,
        "aborting leaves the source an ordinary path-addressed object"
    );
    assert!(
        store
            .load_transition(&ns, "op-1")
            .expect("transition lookup")
            .is_none(),
        "the transition record must not survive its own abort"
    );
}

#[tokio::test]
async fn a_stale_participant_cannot_commit_after_a_concurrent_transition() {
    // The race §5's mapping revision exists to close. A coordinator that
    // prepared against one view of the namespace, then stalled, must not commit
    // over whatever happened meanwhile.
    let store = MemoryMetadataStore::new();
    let transition = transition(MappingRevision::INITIAL);

    let opened = store
        .commit(&begin_promotion(&transition).expect("valid"))
        .await
        .expect("promotion opens");
    let after_open = opened.mapping_revision.expect("revision advanced");

    // Something else advances the namespace -- another transition, an unrelated
    // link removal, anything that bumps the revision.
    store
        .commit(&abort_promotion(&transition))
        .await
        .expect("concurrent operation commits");

    store
        .commit(&commit_promotion(&transition, after_open))
        .await
        .expect_err("a stale mapping revision must not commit");
}

#[tokio::test]
async fn a_commit_is_fenced_on_the_revision_not_just_the_transition_record() {
    // Regression guard found by mutation testing: removing the commit's
    // mapping-revision precondition left every other test in this file passing,
    // because they all fail for a different reason first (usually the missing
    // transition record).
    //
    // The revision fence is what §5 relies on to stop a stalled coordinator
    // committing over concurrent work. This isolates it: the transition record
    // is still present and valid, only the namespace has moved on.
    let store = MemoryMetadataStore::new();
    let ns = namespace();
    let transition = transition(MappingRevision::INITIAL);

    let opened = store
        .commit(&begin_promotion(&transition).expect("valid"))
        .await
        .expect("promotion opens");
    let stale_revision = opened.mapping_revision.expect("revision advanced");

    // An unrelated namespace mutation advances the revision. The transition
    // record is untouched, so only the revision check can catch this.
    store
        .commit(&add_link(
            &ns,
            "unrelated/other.bin",
            inode(7),
            LinkCount::PROMOTED,
            stale_revision,
        ))
        .await
        .expect("unrelated mutation commits");

    assert!(
        store
            .load_transition(&ns, "op-1")
            .expect("transition lookup")
            .is_some(),
        "the transition record must still exist, so this test isolates the revision fence"
    );

    store
        .commit(&commit_promotion(&transition, stale_revision))
        .await
        .expect_err("a commit against a superseded revision must be refused");
}

#[tokio::test]
async fn adding_a_third_link_needs_no_transition_record() {
    // §5: "Adding a third or later link needs no object copy: one TMS
    // transaction adds the new path index, increments the bounded link count,
    // and advances the mapping revision."
    let store = MemoryMetadataStore::new();
    let ns = namespace();
    let transition = transition(MappingRevision::INITIAL);

    let opened = store
        .commit(&begin_promotion(&transition).expect("valid"))
        .await
        .expect("promotion opens");
    let committed = store
        .commit(&commit_promotion(
            &transition,
            opened.mapping_revision.expect("revision"),
        ))
        .await
        .expect("promotion commits");

    let after = store
        .commit(&add_link(
            &ns,
            "data/c.bin",
            inode(1),
            LinkCount::PROMOTED,
            committed.mapping_revision.expect("revision"),
        ))
        .await
        .expect("third link commits");

    assert_eq!(
        store
            .resolve_path(&ns, "data/c.bin")
            .await
            .expect("resolve")
            .map(|entry| entry.inode),
        Some(inode(1))
    );
    assert_eq!(
        store
            .load_inode(&ns, inode(1))
            .await
            .expect("inode")
            .link_count
            .get(),
        3
    );
    assert!(after.mapping_revision.is_some(), "the fence still advances");
}

#[tokio::test]
async fn demotion_returns_the_object_to_a_plain_path() {
    // §5 calls this "the required inverse of promotion, not an optional space
    // optimization". After it, no TMS record remains: the file is an ordinary
    // object again, readable by any S3 client, which is the property §2 exists
    // to protect.
    let store = MemoryMetadataStore::new();
    let ns = namespace();
    let transition = transition(MappingRevision::INITIAL);

    let opened = store
        .commit(&begin_promotion(&transition).expect("valid"))
        .await
        .expect("promotion opens");
    let committed = store
        .commit(&commit_promotion(
            &transition,
            opened.mapping_revision.expect("revision"),
        ))
        .await
        .expect("promotion commits");

    store
        .commit(&commit_demotion(
            &ns,
            "data/b.bin",
            "data/a.bin",
            inode(1),
            committed.mapping_revision.expect("revision"),
        ))
        .await
        .expect("demotion commits");

    for path in ["data/a.bin", "data/b.bin"] {
        assert_eq!(
            store.resolve_path(&ns, path).await.expect("resolve"),
            None,
            "{path} must be path-addressed again after demotion"
        );
    }
    assert!(
        store.load_inode(&ns, inode(1)).await.is_err(),
        "the inode record must not outlive the last link (ADR 0003 §3)"
    );
}

#[tokio::test]
async fn a_link_removal_that_would_leave_one_link_is_refused() {
    // Dropping to one link has to move the object back to its visible path, so
    // it cannot be an ordinary index removal. Allowing it here would strand the
    // data at an internal address that no plain S3 client can reach.
    let store = MemoryMetadataStore::new();
    let ns = namespace();
    let transition = transition(MappingRevision::INITIAL);

    let opened = store
        .commit(&begin_promotion(&transition).expect("valid"))
        .await
        .expect("promotion opens");
    let committed = store
        .commit(&commit_promotion(
            &transition,
            opened.mapping_revision.expect("revision"),
        ))
        .await
        .expect("promotion commits");

    remove_link(
        &ns,
        "data/b.bin",
        inode(1),
        LinkCount::PROMOTED,
        committed.mapping_revision.expect("revision"),
    )
    .expect_err("dropping to one link must go through demotion");
}

#[tokio::test]
async fn two_promotions_of_the_same_source_cannot_both_commit() {
    // Two coordinators racing to promote the same file. Whichever opens second
    // must fail, or the loser would overwrite the winner's inode reference and
    // leave one of the two inode objects orphaned.
    let store = MemoryMetadataStore::new();
    let first = transition(MappingRevision::INITIAL);

    store
        .commit(&begin_promotion(&first).expect("valid"))
        .await
        .expect("first promotion opens");

    let mut second = transition(MappingRevision::INITIAL);
    second.operation_id = "op-2".to_owned();
    second.new_path = "data/z.bin".to_owned();
    second.inode = inode(2);

    store
        .commit(&begin_promotion(&second).expect("valid"))
        .await
        .expect_err("the second promotion must not open against a consumed revision");
}
