//! WAL checkpointing, compaction, and replay (ADR 0003 §9.4).
//!
//! # The ordering is the protocol
//!
//! > The worker `fsync`s the temporary snapshot, renames it, `fsync`s the
//! > directory, appends and `fsync`s a `CHECKPOINT` record naming that snapshot,
//! > and only then deletes covered WAL segments and `fsync`s the directory
//! > again.
//!
//! Each boundary defines a recoverable state:
//!
//! > A crash before `CHECKPOINT` leaves an ignorable snapshot; a crash after it
//! > can replay the snapshot plus later WAL.
//!
//! The `CHECKPOINT` record is the commit point. Before it the snapshot is
//! garbage that costs disk and nothing else. After it the snapshot is
//! authoritative and the covered segments are redundant.
//!
//! Deleting segments earlier would destroy the only copy of records the
//! snapshot has not yet been *declared* to cover — the snapshot may be on disk
//! and even correct, but nothing durable says so, so recovery would not use it.
//!
//! # Why two compaction triggers
//!
//! > Compaction runs when WAL metadata reaches 2 GiB or more than 50 percent of
//! > its records are obsolete.
//!
//! Size bounds disk; obsolescence bounds replay time. A WAL that is small but
//! almost entirely superseded still replays every record it contains, so size
//! alone would let recovery time grow without the size trigger ever firing.

use crate::wal_commit::WalPosition;

/// WAL metadata size that triggers compaction.
pub const COMPACT_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Fraction of obsolete records that triggers compaction.
pub const COMPACT_OBSOLETE_RATIO: f64 = 0.5;

/// Steps of the checkpoint protocol, in the order they must occur.
///
/// Named for what each step makes true, because the reason they are ordered is
/// what each crash boundary leaves behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CheckpointStep {
    /// Write the snapshot to a temporary name.
    WriteTemp,
    /// `fsync` the temporary file, so its bytes survive a crash.
    SyncTemp,
    /// Rename it into place.
    Rename,
    /// `fsync` the directory, so the rename survives a crash.
    SyncDirAfterRename,
    /// Append and `fsync` the `CHECKPOINT` record.
    ///
    /// The commit point. Before this the snapshot is ignorable; after it, it is
    /// authoritative.
    CommitRecord,
    /// Delete covered segments and `fsync` the directory again.
    DeleteSegments,
}

impl CheckpointStep {
    /// The steps in protocol order.
    pub const ORDER: [Self; 6] = [
        Self::WriteTemp,
        Self::SyncTemp,
        Self::Rename,
        Self::SyncDirAfterRename,
        Self::CommitRecord,
        Self::DeleteSegments,
    ];

    /// Whether the snapshot is authoritative once this step has completed.
    pub const fn snapshot_is_authoritative(self) -> bool {
        matches!(self, Self::CommitRecord | Self::DeleteSegments)
    }

    /// Whether covered segments may be deleted once this step has completed.
    ///
    /// Only after the record is durable. This is the same condition as
    /// [`snapshot_is_authoritative`](Self::snapshot_is_authoritative) and is
    /// kept separate because the two are easy to conflate and the consequence
    /// of getting this one wrong is data loss rather than a slower recovery.
    pub const fn segments_may_be_deleted(self) -> bool {
        matches!(self, Self::CommitRecord | Self::DeleteSegments)
    }
}

/// What recovery should do after a crash at a given step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashOutcome {
    /// The snapshot is not usable; replay from the previous checkpoint.
    ///
    /// The partial or uncommitted snapshot is garbage to be cleaned up. Nothing
    /// is lost: every record it would have covered is still in the WAL.
    IgnoreSnapshot,
    /// The snapshot is usable; replay it plus any later WAL records.
    UseSnapshot,
}

/// What a crash immediately after `step` leaves recoverable.
pub const fn crash_outcome(step: CheckpointStep) -> CrashOutcome {
    if step.snapshot_is_authoritative() {
        CrashOutcome::UseSnapshot
    } else {
        CrashOutcome::IgnoreSnapshot
    }
}

/// Why compaction should run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactTrigger {
    /// WAL metadata reached [`COMPACT_BYTES`].
    Size,
    /// More than [`COMPACT_OBSOLETE_RATIO`] of records are obsolete.
    Obsolescence,
}

/// Whether compaction should run now.
///
/// Either trigger suffices. Size is checked first only so a WAL that satisfies
/// both reports the cheaper-to-explain reason.
pub fn should_compact(
    metadata_bytes: u64,
    live_records: u64,
    total_records: u64,
) -> Option<CompactTrigger> {
    if metadata_bytes >= COMPACT_BYTES {
        return Some(CompactTrigger::Size);
    }
    if total_records == 0 {
        return None;
    }
    let obsolete = total_records.saturating_sub(live_records);
    // More than half is equivalent to `obsolete / total > 0.5`, without
    // converting counters to f64 and losing integer precision above 2^53.
    if obsolete > total_records / 2 {
        return Some(CompactTrigger::Obsolescence);
    }
    None
}

/// A checkpoint found on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointCandidate {
    /// Snapshot file this checkpoint names.
    pub snapshot_file: String,
    /// WAL position the snapshot is current as of.
    pub position: WalPosition,
    /// Whether the snapshot's checksum verified.
    pub checksum_valid: bool,
}

/// Choose the checkpoint to recover from.
///
/// > Recovery loads the latest valid checkpoint and replays later WAL records.
///
/// "Latest *valid*" is the operative word. A snapshot whose checksum fails is
/// skipped in favour of an older one plus more WAL — slower, but correct.
/// Failing recovery outright would turn one bad file into an unavailable shard
/// when the data to rebuild it is still present.
///
/// Returns `None` when no checkpoint verifies, meaning replay starts from the
/// beginning of the retained WAL.
pub fn select_checkpoint(candidates: &[CheckpointCandidate]) -> Option<&CheckpointCandidate> {
    candidates
        .iter()
        .filter(|candidate| candidate.checksum_valid)
        .max_by_key(|candidate| candidate.position)
}

/// Where replay begins.
///
/// A checkpoint covers everything up to its position, so replay resumes from
/// there. Without one it starts at the beginning of the retained WAL.
pub fn replay_from(checkpoint: Option<&CheckpointCandidate>) -> WalPosition {
    checkpoint.map_or(WalPosition::START, |candidate| candidate.position)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(file: &str, position: u64, valid: bool) -> CheckpointCandidate {
        CheckpointCandidate {
            snapshot_file: file.to_owned(),
            position: WalPosition::new(position),
            checksum_valid: valid,
        }
    }

    #[test]
    fn the_commit_record_is_the_boundary_between_ignorable_and_authoritative() {
        // §9.4: "A crash before CHECKPOINT leaves an ignorable snapshot; a
        // crash after it can replay the snapshot plus later WAL."
        for step in [
            CheckpointStep::WriteTemp,
            CheckpointStep::SyncTemp,
            CheckpointStep::Rename,
            CheckpointStep::SyncDirAfterRename,
        ] {
            assert_eq!(
                crash_outcome(step),
                CrashOutcome::IgnoreSnapshot,
                "{step:?} precedes the commit record"
            );
        }
        for step in [CheckpointStep::CommitRecord, CheckpointStep::DeleteSegments] {
            assert_eq!(
                crash_outcome(step),
                CrashOutcome::UseSnapshot,
                "{step:?} is at or after the commit record"
            );
        }
    }

    #[test]
    fn segments_are_never_deletable_before_the_record_is_durable() {
        // The step whose ordering costs data rather than time. Deleting earlier
        // would destroy the only copy of records the snapshot has not yet been
        // declared to cover -- the snapshot may be on disk and correct, but
        // nothing durable says so, so recovery would not use it.
        for step in [
            CheckpointStep::WriteTemp,
            CheckpointStep::SyncTemp,
            CheckpointStep::Rename,
            CheckpointStep::SyncDirAfterRename,
        ] {
            assert!(
                !step.segments_may_be_deleted(),
                "{step:?} must not permit segment deletion"
            );
        }
        assert!(CheckpointStep::CommitRecord.segments_may_be_deleted());
    }

    #[test]
    fn the_protocol_order_puts_the_record_after_the_rename_sync() {
        // A rename that is not directory-synced can vanish on a crash, so a
        // record naming it would point at a file that is not there. Recovery
        // would then find a committed checkpoint with no snapshot -- worse than
        // no checkpoint, because it looks usable.
        let order = CheckpointStep::ORDER;
        let sync_dir = order
            .iter()
            .position(|s| *s == CheckpointStep::SyncDirAfterRename)
            .expect("present");
        let commit = order
            .iter()
            .position(|s| *s == CheckpointStep::CommitRecord)
            .expect("present");
        let delete = order
            .iter()
            .position(|s| *s == CheckpointStep::DeleteSegments)
            .expect("present");
        assert!(
            sync_dir < commit,
            "the rename must be durable before the record"
        );
        assert!(
            commit < delete,
            "the record must be durable before deletion"
        );
    }

    #[test]
    fn size_triggers_compaction() {
        assert_eq!(
            should_compact(COMPACT_BYTES, 100, 100),
            Some(CompactTrigger::Size)
        );
        assert_eq!(should_compact(COMPACT_BYTES - 1, 100, 100), None);
    }

    #[test]
    fn obsolescence_triggers_compaction_independently_of_size() {
        // A small WAL that is almost entirely superseded still replays every
        // record. Without this trigger, recovery time would grow while the size
        // threshold never fired.
        assert_eq!(
            should_compact(1024, 10, 100),
            Some(CompactTrigger::Obsolescence),
            "90% obsolete must compact regardless of size"
        );
    }

    #[test]
    fn exactly_half_obsolete_does_not_trigger() {
        // §9.4 says "more than 50 percent". The boundary is worth pinning
        // because a >= would compact a steady-state WAL forever.
        assert_eq!(should_compact(1024, 50, 100), None);
        assert_eq!(
            should_compact(1024, 49, 100),
            Some(CompactTrigger::Obsolescence)
        );
    }

    #[test]
    fn obsolescence_keeps_exact_integer_boundaries_at_large_counts() {
        // f64 rounds these adjacent u64 values together and would miss that the
        // obsolete count is one record above half.
        let total = u64::MAX;
        let obsolete = total / 2 + 1;
        let live = total - obsolete;
        assert_eq!(
            should_compact(0, live, total),
            Some(CompactTrigger::Obsolescence)
        );
    }

    #[test]
    fn an_empty_wal_never_compacts() {
        // Guards against a division by zero producing NaN, which compares false
        // against everything and would silently disable the trigger.
        assert_eq!(should_compact(0, 0, 0), None);
    }

    #[test]
    fn recovery_picks_the_latest_valid_checkpoint() {
        let candidates = [
            candidate("a", 100, true),
            candidate("b", 300, true),
            candidate("c", 200, true),
        ];
        let chosen = select_checkpoint(&candidates).expect("a valid checkpoint");
        assert_eq!(chosen.snapshot_file, "b");
        assert_eq!(replay_from(Some(chosen)), WalPosition::new(300));
    }

    #[test]
    fn a_corrupt_snapshot_falls_back_rather_than_failing_recovery() {
        // Failing outright would turn one bad file into an unavailable shard
        // while the data to rebuild it is still in the WAL. Slower and correct
        // beats faster and unavailable.
        let candidates = [candidate("old", 100, true), candidate("new", 300, false)];
        let chosen = select_checkpoint(&candidates).expect("the older checkpoint");
        assert_eq!(chosen.snapshot_file, "old");
        assert_eq!(replay_from(Some(chosen)), WalPosition::new(100));
    }

    #[test]
    fn no_valid_checkpoint_replays_from_the_start() {
        let candidates = [candidate("bad", 900, false)];
        assert_eq!(select_checkpoint(&candidates), None);
        assert_eq!(replay_from(None), WalPosition::START);
    }

    #[test]
    fn the_thresholds_match_the_adr() {
        assert_eq!(COMPACT_BYTES, 2 * 1024 * 1024 * 1024);
        assert!((COMPACT_OBSOLETE_RATIO - 0.5).abs() < f64::EPSILON);
    }
}
