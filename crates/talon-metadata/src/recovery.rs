//! Shard handoff and quorum recovery (ADR 0003 §9.8).
//!
//! Two paths, and the difference is not cosmetic.
//!
//! **Planned handoff** happens while the old owner is alive and cooperating, so
//! its dirty set can be copied before ownership moves. Ownership changes *last*,
//! once the data is already in place.
//!
//! **Quorum recovery** happens when the old owner is gone, so the dirty set has
//! to be reconstructed from whoever else acknowledged those writes — and
//! ownership changes *first*, so nothing can mutate the shard while that
//! happens.
//!
//! Conflating them would mean either stalling a graceful drain behind a recovery
//! quorum, or assuming an absent owner will hand over data it can no longer
//! serve.
//!
//! # Why the quorum arithmetic works
//!
//! > `W + Q > R` guarantees that the recovery responses intersect every write
//! > that could have been acknowledged.
//!
//! A write needed `W` durable copies; recovery hears from `Q` replicas out of
//! `R`. If `W + Q > R` the two sets must overlap, so every acknowledged write is
//! visible to recovery. With `R=3, W=2` that gives `Q=2`.
//!
//! When it cannot be satisfied:
//!
//! > the shard remains `INCOMPLETE`; writes are refused and **reads do not fall
//! > back to the origin**.
//!
//! Falling back would serve a stale object as though it were current, which is
//! the failure this whole section exists to prevent.

use core::time::Duration;

use crate::assignment::{FencingTerm, ShardState};

/// Deadlines from ADR 0003 §9.8.
///
/// Fixed rather than configurable: they are load-bearing for the guarantee that
/// a shard either returns to service or is declared `INCOMPLETE` in bounded
/// time. An operator raising them would silently extend the window in which a
/// client sees retryable errors without any promise of resolution.
pub mod deadlines {
    use core::time::Duration;

    /// Time without an accepted heartbeat before a worker is unhealthy.
    ///
    /// Deliberately shorter than ADR 0001's 30-second membership lease. §9.8:
    /// "the persistent TMS term, not deletion of the membership record, fences
    /// the old owner" — so recovery may begin while the old membership record
    /// still exists, and that is safe precisely because the term fences it.
    pub const UNHEALTHY_AFTER: Duration = Duration::from_secs(15);

    /// Time to fence the previous acting set and collect `Q` manifests.
    pub const MANIFEST_COLLECTION: Duration = Duration::from_secs(10);

    /// Maximum gap between observable repair progress.
    pub const REPAIR_PROGRESS: Duration = Duration::from_secs(30);

    /// Time from entering `RECOVERING` to returning to `ACTIVE`.
    pub const RECOVERY_TOTAL: Duration = Duration::from_secs(120);

    /// How long a client retries a `RECOVERING` shard before surfacing `EAGAIN`.
    pub const CLIENT_RETRY: Duration = Duration::from_secs(30);
}

/// Identifies one mutation within a shard.
///
/// Ordered lexicographically by `(term, sequence)`, which is what makes
/// cross-node comparison meaningful:
///
/// > Mutation IDs are ordered lexicographically, so `(13, 1)` is newer than
/// > every mutation from term 12, regardless of the old owner's process-local
/// > counter.
///
/// This is the answer to a problem the ADR calls out explicitly: a worker's
/// local sequence counter is process-monotonic and reseeded from a local scan,
/// so two workers' sequence numbers are unrelated. Leading with the term — which
/// only a TMS compare-and-swap can advance — gives a total order that survives
/// restarts and partitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MutationId {
    /// Shard term under which the mutation was accepted.
    pub term: FencingTerm,
    /// Position within that term, monotonic per shard.
    pub sequence: u64,
}

impl MutationId {
    /// Construct a mutation id.
    pub const fn new(term: FencingTerm, sequence: u64) -> Self {
        Self { term, sequence }
    }
}

/// Durability state of one mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MutationState {
    /// Payload and manifest are durable on this replica, but the write was not
    /// acknowledged to the client.
    Prepared,
    /// Acknowledged: the commit record reached `W` replicas.
    Committed,
    /// Written through to the origin.
    Flushed,
}

impl MutationState {
    /// Whether recovery may keep this mutation.
    ///
    /// Only committed and flushed mutations survive. §9.8 step 5: "discard
    /// uncommitted prepares".
    ///
    /// The asymmetry is deliberate. A `PREPARED` record means some replicas hold
    /// the bytes but the client was never told the write succeeded, so
    /// discarding it cannot break a promise. Keeping it could: a write the
    /// client believes failed would reappear, which is worse than losing a write
    /// nobody was told about.
    pub const fn survives_recovery(self) -> bool {
        matches!(self, Self::Committed | Self::Flushed)
    }
}

/// One replica's report about one object during recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestEntry {
    /// Object the mutation applies to.
    pub object: String,
    /// Mutation identity.
    pub mutation: MutationId,
    /// Durability state on the reporting replica.
    pub state: MutationState,
    /// Whether the payload verified against its recorded checksum.
    pub payload_verified: bool,
}

/// What a replica returned in response to `FenceAndSnapshot`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestResponse {
    /// Replica that answered.
    pub replica: String,
    /// Its view of the shard's dirty set.
    pub entries: Vec<ManifestEntry>,
}

/// Why a recovery attempt cannot complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryFailure {
    /// Fewer than `Q` replicas answered.
    ///
    /// Without the quorum, recovery cannot prove it has seen every acknowledged
    /// write — so proceeding would risk silently dropping one.
    QuorumNotMet {
        /// Responses received.
        received: usize,
        /// Responses required.
        required: usize,
    },
    /// A selected mutation's payload failed verification everywhere.
    PayloadUnavailable {
        /// Object whose payload could not be obtained.
        object: String,
    },
    /// A deadline elapsed.
    DeadlineExceeded {
        /// Which deadline.
        stage: &'static str,
    },
}

/// The outcome of a recovery attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryOutcome {
    /// The shard can return to service with this dirty set.
    Recovered {
        /// Winning mutation per object, after the merge.
        selected: Vec<ManifestEntry>,
    },
    /// The shard becomes `INCOMPLETE`.
    ///
    /// > New evidence, such as a previous replica returning, may start a new
    /// > fenced recovery attempt under a higher term; it does not silently make
    /// > the old attempt active.
    Incomplete {
        /// Why the attempt failed.
        failure: RecoveryFailure,
    },
}

impl RecoveryOutcome {
    /// The shard state this outcome commits.
    pub const fn resulting_state(&self) -> ShardState {
        match self {
            Self::Recovered { .. } => ShardState::Active,
            Self::Incomplete { .. } => ShardState::Incomplete,
        }
    }
}

/// Responses required from the previous acting set.
///
/// `Q = R - W + 1`, the smallest value satisfying `W + Q > R`.
pub const fn recovery_quorum(replication_factor: u8, write_quorum: u8) -> u8 {
    replication_factor - write_quorum + 1
}

/// Merge manifests and decide whether the shard can return to service.
///
/// Implements §9.8 steps 4–6: require `Q` responses, discard uncommitted
/// prepares, merge by object taking the greatest committed `(term, sequence)`,
/// and require a verified payload for each winner.
pub fn recover(
    responses: &[ManifestResponse],
    replication_factor: u8,
    write_quorum: u8,
) -> RecoveryOutcome {
    let required = recovery_quorum(replication_factor, write_quorum) as usize;
    if responses.len() < required {
        return RecoveryOutcome::Incomplete {
            failure: RecoveryFailure::QuorumNotMet {
                received: responses.len(),
                required,
            },
        };
    }

    // Winner per object: the greatest surviving mutation id. Ties on id are the
    // same mutation reported by different replicas, so any copy will do -- but
    // prefer one whose payload verified, since an unverified copy elsewhere does
    // not mean the mutation is unrecoverable.
    let mut winners: Vec<ManifestEntry> = Vec::new();
    for response in responses {
        for entry in &response.entries {
            if !entry.state.survives_recovery() {
                continue;
            }
            match winners.iter_mut().find(|w| w.object == entry.object) {
                None => winners.push(entry.clone()),
                Some(current) => {
                    if entry.mutation > current.mutation
                        || (entry.mutation == current.mutation
                            && entry.payload_verified
                            && !current.payload_verified)
                    {
                        *current = entry.clone();
                    }
                }
            }
        }
    }

    if let Some(missing) = winners.iter().find(|entry| !entry.payload_verified) {
        return RecoveryOutcome::Incomplete {
            failure: RecoveryFailure::PayloadUnavailable {
                object: missing.object.clone(),
            },
        };
    }

    winners.sort_by(|a, b| a.object.cmp(&b.object));
    RecoveryOutcome::Recovered { selected: winners }
}

/// Whether an elapsed duration has blown a recovery deadline.
///
/// Returns the stage that failed, for the `INCOMPLETE` reason.
pub fn deadline_exceeded(
    since_recovering: Duration,
    since_progress: Duration,
    manifests_collected: bool,
) -> Option<&'static str> {
    if since_recovering > deadlines::RECOVERY_TOTAL {
        return Some("recovery_total");
    }
    if !manifests_collected && since_recovering > deadlines::MANIFEST_COLLECTION {
        return Some("manifest_collection");
    }
    if since_progress > deadlines::REPAIR_PROGRESS {
        return Some("repair_progress");
    }
    None
}

/// Whether a coordinator may begin a new recovery.
///
/// §9.8: "If a coordinator cannot obtain a fresh `ClusterStateStore` snapshot
/// under ADR 0001 §8, it starts no new recovery."
///
/// Acting on a stale membership view could fence a healthy owner: the snapshot
/// is the only evidence that the worker is actually gone rather than merely
/// unobserved.
pub const fn may_start_recovery(has_fresh_membership_snapshot: bool) -> bool {
    has_fresh_membership_snapshot
}

/// Steps of a planned handoff (ADR 0003 §9.8).
///
/// Ordering is the correctness argument: ownership moves *after* the data, so
/// there is no window in which the new owner is authoritative for bytes it does
/// not hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum HandoffStep {
    /// Mark the shard `DRAINING`.
    MarkDraining,
    /// Old owner keeps serving while the new acting set receives the manifest
    /// and payloads.
    TransferDirtySet,
    /// Briefly stop new writes and copy the final mutations.
    QuiesceAndCopyTail,
    /// Atomically increment the term and commit the new owner in TMS.
    CommitOwnership,
    /// Activate the new owner and retire the old assignment.
    Activate,
}

impl HandoffStep {
    /// The steps in order.
    pub const ORDER: [Self; 5] = [
        Self::MarkDraining,
        Self::TransferDirtySet,
        Self::QuiesceAndCopyTail,
        Self::CommitOwnership,
        Self::Activate,
    ];

    /// Whether the old owner still serves writes at this step.
    ///
    /// It does until the tail copy, which is what makes a handoff graceful
    /// rather than an outage.
    pub const fn old_owner_still_serves(self) -> bool {
        matches!(self, Self::MarkDraining | Self::TransferDirtySet)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(object: &str, term: u64, seq: u64, state: MutationState) -> ManifestEntry {
        ManifestEntry {
            object: object.to_owned(),
            mutation: MutationId::new(FencingTerm::new(term), seq),
            state,
            payload_verified: true,
        }
    }

    fn response(replica: &str, entries: Vec<ManifestEntry>) -> ManifestResponse {
        ManifestResponse {
            replica: replica.to_owned(),
            entries,
        }
    }

    #[test]
    fn mutation_ids_order_by_term_before_sequence() {
        // The property that makes cross-node comparison possible. A worker's
        // local sequence is process-monotonic and reseeded from a local scan, so
        // two workers' sequences are unrelated -- but only a TMS compare-and-swap
        // can advance a term, so leading with it gives a total order that
        // survives restarts.
        let old_term_high_seq = MutationId::new(FencingTerm::new(12), 9_999);
        let new_term_low_seq = MutationId::new(FencingTerm::new(13), 1);
        assert!(
            new_term_low_seq > old_term_high_seq,
            "(13,1) must beat (12,9999)"
        );
    }

    #[test]
    fn the_quorum_intersects_every_acknowledged_write() {
        // W + Q > R is the whole argument. R=3, W=2 gives Q=2: any two
        // responders must include a replica that acknowledged any given write.
        assert_eq!(recovery_quorum(3, 2), 2);
        assert!(2 + recovery_quorum(3, 2) > 3);
        // A stricter write quorum needs fewer recovery responses.
        assert_eq!(recovery_quorum(5, 4), 2);
        assert!(4 + recovery_quorum(5, 4) > 5);
    }

    #[test]
    fn too_few_responses_leaves_the_shard_incomplete() {
        // Proceeding without the quorum could silently drop an acknowledged
        // write, which is exactly what INCOMPLETE exists to prevent.
        let outcome = recover(
            &[response(
                "w1",
                vec![entry("a", 1, 1, MutationState::Committed)],
            )],
            3,
            2,
        );
        assert_eq!(
            outcome,
            RecoveryOutcome::Incomplete {
                failure: RecoveryFailure::QuorumNotMet {
                    received: 1,
                    required: 2
                }
            }
        );
        assert_eq!(outcome.resulting_state(), ShardState::Incomplete);
    }

    #[test]
    fn uncommitted_prepares_are_discarded() {
        // §9.8 step 5. A PREPARED record means the client was never told the
        // write succeeded, so discarding cannot break a promise -- while keeping
        // it could resurrect a write the client believes failed.
        let outcome = recover(
            &[
                response("w1", vec![entry("a", 1, 5, MutationState::Prepared)]),
                response("w2", vec![entry("a", 1, 5, MutationState::Prepared)]),
            ],
            3,
            2,
        );
        assert_eq!(outcome, RecoveryOutcome::Recovered { selected: vec![] });
    }

    #[test]
    fn the_greatest_committed_mutation_wins_per_object() {
        let outcome = recover(
            &[
                response(
                    "w1",
                    vec![
                        entry("a", 12, 900, MutationState::Committed),
                        entry("b", 13, 1, MutationState::Committed),
                    ],
                ),
                response(
                    "w2",
                    vec![
                        // Newer term beats a much higher sequence in an older one.
                        entry("a", 13, 2, MutationState::Committed),
                        entry("b", 13, 1, MutationState::Committed),
                    ],
                ),
            ],
            3,
            2,
        );
        let RecoveryOutcome::Recovered { selected } = outcome else {
            panic!("expected recovery");
        };
        assert_eq!(selected.len(), 2);
        assert_eq!(
            selected[0].mutation,
            MutationId::new(FencingTerm::new(13), 2)
        );
    }

    #[test]
    fn a_committed_mutation_beats_a_later_prepare() {
        // A prepare with a higher id is still not acknowledged data. Preferring
        // it would replace a write the client was told succeeded with one it was
        // not.
        let outcome = recover(
            &[
                response("w1", vec![entry("a", 5, 1, MutationState::Committed)]),
                response("w2", vec![entry("a", 5, 9, MutationState::Prepared)]),
            ],
            3,
            2,
        );
        let RecoveryOutcome::Recovered { selected } = outcome else {
            panic!("expected recovery");
        };
        assert_eq!(
            selected[0].mutation,
            MutationId::new(FencingTerm::new(5), 1)
        );
    }

    #[test]
    fn an_unverifiable_payload_leaves_the_shard_incomplete() {
        // §9.8: reads must not fall back to the origin here. Serving the origin
        // version would present stale data as current -- the failure this
        // section exists to prevent.
        let mut bad = entry("a", 1, 1, MutationState::Committed);
        bad.payload_verified = false;
        let outcome = recover(
            &[response("w1", vec![bad.clone()]), response("w2", vec![bad])],
            3,
            2,
        );
        assert!(matches!(
            outcome,
            RecoveryOutcome::Incomplete {
                failure: RecoveryFailure::PayloadUnavailable { .. }
            }
        ));
    }

    #[test]
    fn a_verified_copy_is_preferred_over_an_unverified_one() {
        // The same mutation reported twice: one replica's copy is corrupt. That
        // does not make the mutation unrecoverable, so recovery must take the
        // good copy rather than declaring INCOMPLETE.
        let mut corrupt = entry("a", 2, 3, MutationState::Committed);
        corrupt.payload_verified = false;
        let good = entry("a", 2, 3, MutationState::Committed);
        let outcome = recover(
            &[
                response("w1", vec![corrupt]),
                response("w2", vec![good.clone()]),
            ],
            3,
            2,
        );
        assert_eq!(
            outcome,
            RecoveryOutcome::Recovered {
                selected: vec![good]
            }
        );
    }

    #[test]
    fn deadlines_are_classified_by_stage() {
        assert_eq!(
            deadline_exceeded(Duration::from_secs(121), Duration::ZERO, true),
            Some("recovery_total")
        );
        assert_eq!(
            deadline_exceeded(Duration::from_secs(11), Duration::ZERO, false),
            Some("manifest_collection")
        );
        assert_eq!(
            deadline_exceeded(Duration::from_secs(20), Duration::from_secs(31), true),
            Some("repair_progress")
        );
        assert_eq!(
            deadline_exceeded(Duration::from_secs(20), Duration::from_secs(5), true),
            None
        );
    }

    #[test]
    fn collecting_manifests_stops_the_collection_deadline_applying() {
        // Once Q manifests are in, the collection clock is irrelevant; only the
        // repair and total clocks still run. Otherwise a slow repair would be
        // reported as a collection failure and mislead an operator.
        assert_eq!(
            deadline_exceeded(Duration::from_secs(60), Duration::from_secs(5), true),
            None
        );
    }

    #[test]
    fn the_unhealthy_threshold_fires_before_the_membership_lease_expires() {
        // Deliberate, per §9.8: "the persistent TMS term, not deletion of the
        // membership record, fences the old owner". Recovery may begin while the
        // membership record still exists.
        assert!(deadlines::UNHEALTHY_AFTER < Duration::from_secs(30));
    }

    #[test]
    fn recovery_needs_a_fresh_membership_snapshot() {
        // Acting on a stale view could fence a healthy owner: the snapshot is
        // the only evidence the worker is actually gone rather than unobserved.
        assert!(may_start_recovery(true));
        assert!(!may_start_recovery(false));
    }

    #[test]
    fn a_handoff_moves_ownership_after_the_data() {
        // The ordering is the correctness argument: there is no window in which
        // the new owner is authoritative for bytes it does not hold.
        let commit = HandoffStep::ORDER
            .iter()
            .position(|s| *s == HandoffStep::CommitOwnership)
            .expect("present");
        let transfer = HandoffStep::ORDER
            .iter()
            .position(|s| *s == HandoffStep::TransferDirtySet)
            .expect("present");
        assert!(transfer < commit, "data must move before ownership");
    }

    #[test]
    fn the_old_owner_serves_until_the_tail_copy() {
        // What makes a planned handoff graceful rather than an outage.
        assert!(HandoffStep::MarkDraining.old_owner_still_serves());
        assert!(HandoffStep::TransferDirtySet.old_owner_still_serves());
        assert!(!HandoffStep::QuiesceAndCopyTail.old_owner_still_serves());
        assert!(!HandoffStep::CommitOwnership.old_owner_still_serves());
    }
}
