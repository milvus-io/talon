//! Reconciling TMS records against inode objects (ADR 0003 §5).
//!
//! The transition state machine in [`crate::link`] makes crashes unambiguous,
//! but it cannot prevent everything:
//!
//! > it does not eliminate abandoned copies, external deletion, storage
//! > corruption, or operator mistakes.
//!
//! This module is the comparison engine behind both the background scrubber and
//! `talon fsck`. It decides what a discrepancy *means* and what may be done
//! about it automatically; it does not list objects, hold leases, or mutate
//! anything.
//!
//! # The asymmetry is the design
//!
//! An inode object with no TMS reference and a TMS reference to a missing inode
//! object look like mirror images. They are not.
//!
//! Unreferenced garbage is recoverable: quarantining it loses nothing a
//! reference could have found, and the object is still there if someone was
//! wrong. A missing object is the opposite — deleting its references would erase
//! the only surviving record that the data was supposed to exist, converting a
//! recoverable incident into silent data loss.
//!
//! So §5 permits the first automatically and forbids the second:
//!
//! > never fabricate data or delete the references automatically
//!
//! > The background scrubber only applies repairs whose safety can be proved
//! > from two observations and the absence of a live transition. **Missing
//! > authoritative data always requires operator action or restoration from an
//! > external backup.**

use crate::record::{InodeNumber, LinkCount};

/// What the scrubber observed about one inode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observation {
    /// Inode under inspection.
    pub inode: InodeNumber,
    /// Whether the inode object exists in the object store.
    pub object_present: bool,
    /// Link count recorded in TMS, if an inode record exists.
    pub recorded_link_count: Option<LinkCount>,
    /// Number of TMS path entries actually referencing this inode.
    pub path_references: u64,
    /// Whether a live `LinkTransition` covers this inode.
    ///
    /// A transition in flight legitimately produces states that look like
    /// discrepancies — an inode object with no references yet is exactly what
    /// `PREPARING` means. Those belong to the transition, not the scrubber.
    pub live_transition: bool,
    /// Whether this inode was already observed unreferenced in an earlier pass.
    ///
    /// §5 requires a *second* unreferenced check before deletion, so one pass
    /// can never delete on its own.
    pub previously_unreferenced: bool,
    /// Whether the configured grace period has elapsed since quarantine.
    pub grace_elapsed: bool,
}

/// What the scrubber may do about an observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Repair {
    /// Nothing to do.
    None,
    /// Leave it alone: a live transition owns this state.
    ///
    /// Distinct from [`Repair::None`] so an operator reading an audit log can
    /// tell "consistent" from "deliberately not touched".
    DeferToTransition,
    /// Move an unreferenced inode object to quarantine.
    Quarantine {
        /// Inode whose object is unreferenced.
        inode: InodeNumber,
    },
    /// Delete a quarantined object.
    ///
    /// Only after a second unreferenced observation *and* the grace period.
    DeleteQuarantined {
        /// Inode whose object is being removed.
        inode: InodeNumber,
    },
    /// Repair a link count that disagrees with the path map.
    ///
    /// The path map is authoritative: it is what resolution actually consults,
    /// so a stale count is a derived value that drifted, not evidence that a
    /// path is missing.
    RepairLinkCount {
        /// Inode to repair.
        inode: InodeNumber,
        /// Count recomputed from the path map.
        recomputed: LinkCount,
    },
    /// Mark an inode corrupt because its object is missing.
    ///
    /// Never accompanied by deleting the references. §5: "never fabricate data
    /// or delete the references automatically". Access fails and an operator is
    /// alerted; recovery is a restore, not a cleanup.
    MarkCorrupt {
        /// Inode whose object is missing.
        inode: InodeNumber,
        /// Paths left dangling, for the alert.
        dangling_references: u64,
    },
    /// The state needs an operator: the scrubber cannot prove a safe action.
    RequiresOperator {
        /// Inode in question.
        inode: InodeNumber,
        /// Why automatic repair is refused.
        reason: &'static str,
    },
}

impl Repair {
    /// Whether applying this repair destroys data.
    ///
    /// `talon fsck` audits every destructive action, and the scrubber's grace
    /// period exists solely to bound these.
    pub fn is_destructive(&self) -> bool {
        matches!(self, Self::DeleteQuarantined { .. })
    }
}

/// Decide what to do about one observation.
///
/// Ordering matters: a live transition short-circuits everything, because every
/// other rule assumes the world is at rest.
pub fn decide(observation: &Observation) -> Repair {
    if observation.live_transition {
        return Repair::DeferToTransition;
    }

    match (observation.object_present, observation.path_references) {
        // A reference with no object. The dangerous case, and the one where
        // doing nothing destructive is the whole point.
        (false, refs) if refs > 0 => Repair::MarkCorrupt {
            inode: observation.inode,
            dangling_references: refs,
        },

        // Neither object nor references: a demotion that finished. Nothing to
        // reconcile, and nothing to alert about.
        (false, _) => Repair::None,

        // An object nothing points at.
        (true, 0) => {
            if observation.previously_unreferenced && observation.grace_elapsed {
                Repair::DeleteQuarantined {
                    inode: observation.inode,
                }
            } else {
                // First sighting, or the grace period is still running. §5
                // requires two observations *and* the grace period, so a single
                // pass can never delete -- a scrubber racing a slow transition
                // would otherwise delete an object about to be referenced.
                Repair::Quarantine {
                    inode: observation.inode,
                }
            }
        }

        // An object with references: check the derived count.
        (true, refs) => match observation.recorded_link_count {
            None => Repair::RequiresOperator {
                inode: observation.inode,
                reason: "paths reference an inode with no TMS record",
            },
            Some(recorded) if recorded.get() == refs => Repair::None,
            Some(_) => match LinkCount::new(refs) {
                Ok(recomputed) => Repair::RepairLinkCount {
                    inode: observation.inode,
                    recomputed,
                },
                // Exactly one path remains, so the file should have been
                // demoted back to path-addressed storage. Finishing that means
                // copying an object and rewriting the namespace, which is a
                // transition, not a repair -- the scrubber must not attempt it.
                Err(_) => Repair::RequiresOperator {
                    inode: observation.inode,
                    reason: "inode has fewer than two links and needs demotion, not repair",
                },
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inode() -> InodeNumber {
        InodeNumber::new(3).expect("non-zero inode")
    }

    fn observation() -> Observation {
        Observation {
            inode: inode(),
            object_present: true,
            recorded_link_count: Some(LinkCount::PROMOTED),
            path_references: 2,
            live_transition: false,
            previously_unreferenced: false,
            grace_elapsed: false,
        }
    }

    #[test]
    fn a_consistent_inode_needs_no_repair() {
        assert_eq!(decide(&observation()), Repair::None);
    }

    #[test]
    fn a_live_transition_owns_its_own_mess() {
        // An inode object with no references is exactly what PREPARING looks
        // like. Quarantining it would fight the transition that is about to
        // reference it.
        let observed = Observation {
            object_present: true,
            path_references: 0,
            live_transition: true,
            ..observation()
        };
        assert_eq!(decide(&observed), Repair::DeferToTransition);
    }

    #[test]
    fn an_unreferenced_object_is_quarantined_not_deleted_on_first_sight() {
        // §5 requires a second unreferenced check *and* the grace period. One
        // pass must never delete: a scrubber racing a slow transition would
        // otherwise remove an object that is about to be referenced.
        let observed = Observation {
            path_references: 0,
            ..observation()
        };
        assert_eq!(decide(&observed), Repair::Quarantine { inode: inode() });
        assert!(!decide(&observed).is_destructive());
    }

    #[test]
    fn deletion_needs_both_a_second_observation_and_the_grace_period() {
        // Either alone is insufficient, and the test asserts both halves rather
        // than only the success case -- otherwise dropping one condition would
        // still pass.
        let second_only = Observation {
            path_references: 0,
            previously_unreferenced: true,
            grace_elapsed: false,
            ..observation()
        };
        assert_eq!(decide(&second_only), Repair::Quarantine { inode: inode() });

        let grace_only = Observation {
            path_references: 0,
            previously_unreferenced: false,
            grace_elapsed: true,
            ..observation()
        };
        assert_eq!(decide(&grace_only), Repair::Quarantine { inode: inode() });

        let both = Observation {
            path_references: 0,
            previously_unreferenced: true,
            grace_elapsed: true,
            ..observation()
        };
        assert_eq!(decide(&both), Repair::DeleteQuarantined { inode: inode() });
        assert!(decide(&both).is_destructive());
    }

    #[test]
    fn a_missing_object_is_never_repaired_by_deleting_its_references() {
        // The asymmetry that matters. Deleting the references would erase the
        // only surviving evidence that this data was supposed to exist, turning
        // a recoverable incident into silent loss. §5: "never fabricate data or
        // delete the references automatically".
        let observed = Observation {
            object_present: false,
            path_references: 2,
            ..observation()
        };
        let repair = decide(&observed);
        assert_eq!(
            repair,
            Repair::MarkCorrupt {
                inode: inode(),
                dangling_references: 2
            }
        );
        assert!(
            !repair.is_destructive(),
            "marking corrupt must not destroy anything"
        );
    }

    #[test]
    fn a_drifted_link_count_is_recomputed_from_the_path_map() {
        // The path map is authoritative because it is what resolution consults.
        // A stale count is a derived value that drifted, not evidence that a
        // path vanished.
        let observed = Observation {
            recorded_link_count: Some(LinkCount::new(5).expect("five")),
            path_references: 3,
            ..observation()
        };
        assert_eq!(
            decide(&observed),
            Repair::RepairLinkCount {
                inode: inode(),
                recomputed: LinkCount::new(3).expect("three"),
            }
        );
    }

    #[test]
    fn a_single_remaining_link_needs_demotion_and_refuses_automatic_repair() {
        // Recomputing to 1 is not representable, and rightly so: one link means
        // the object belongs back at its visible path, which is a transition
        // (copy plus namespace rewrite), not a count fix.
        let observed = Observation {
            recorded_link_count: Some(LinkCount::PROMOTED),
            path_references: 1,
            ..observation()
        };
        assert!(matches!(decide(&observed), Repair::RequiresOperator { .. }));
    }

    #[test]
    fn paths_referencing_an_inode_with_no_record_need_an_operator() {
        // The scrubber cannot tell whether the record was lost or the paths are
        // spurious, and the two call for opposite actions. §5 puts that decision
        // with an operator.
        let observed = Observation {
            recorded_link_count: None,
            path_references: 2,
            ..observation()
        };
        assert!(matches!(decide(&observed), Repair::RequiresOperator { .. }));
    }

    #[test]
    fn a_completed_demotion_leaves_nothing_to_reconcile() {
        // No object and no references is the normal end state of demotion, not
        // a fault. Alerting here would make every demotion look like damage.
        let observed = Observation {
            object_present: false,
            path_references: 0,
            recorded_link_count: None,
            ..observation()
        };
        assert_eq!(decide(&observed), Repair::None);
    }

    #[test]
    fn only_quarantine_deletion_is_destructive() {
        // Guards the audit boundary: `talon fsck` records every destructive
        // action, so anything newly destructive must be a deliberate decision
        // rather than an accident of a new variant.
        assert!(!Repair::None.is_destructive());
        assert!(!Repair::DeferToTransition.is_destructive());
        assert!(!Repair::Quarantine { inode: inode() }.is_destructive());
        assert!(!Repair::MarkCorrupt {
            inode: inode(),
            dangling_references: 1
        }
        .is_destructive());
        assert!(Repair::DeleteQuarantined { inode: inode() }.is_destructive());
    }
}
