//! LOAD (prewarm) orchestration.
//!
//! Given a request to prewarm an object range, the coordinator:
//!
//! 1. **Splits** the range into fixed-size 256MB blocks ([`split_into_blocks`]).
//! 2. **Assigns** each block a primary worker via the [`Placement`] strategy
//!    over the current membership.
//! 3. Emits a [`LoadAssignment`] per block carrying the whole-vs-paged
//!    [`LoadHint`], which the coordinator sends to workers as `LoadBlobs`;
//!    workers then pull the bytes from the backend themselves.
//!
//! Blob *listing* (enumerating objects/sizes from a backend) is expected to run
//! on a background thread so a large listing never blocks the control ring —
//! this module takes already-known object sizes and does the CPU-cheap
//! split/assign, and tracks prewarm progress via [`LoadProgress`].

use talon_core::{BlockId, LoadHint, NodeId, NodeInfo, ObjectId, Version};

use crate::Placement;

/// One block's prewarm assignment: which block, on which primary worker, in
/// which physical form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadAssignment {
    /// The block to load.
    pub block: BlockId,
    /// Primary worker chosen to hold it (None if the cluster is empty).
    pub primary: Option<NodeId>,
    /// Whole-vs-paged hint selecting the physical form.
    pub hint: LoadHint,
}

/// Split an object range into fixed-size block ids.
///
/// Covers `[offset, offset + len)` with `block_size`-aligned blocks (the first
/// block starts at the `block_size` boundary at or below `offset`). The final
/// block may be short; every returned [`BlockId`] uses the same `block_size` and
/// `version`.
pub fn split_into_blocks(
    object: &ObjectId,
    offset: u64,
    len: u64,
    block_size: u32,
    version: &Version,
) -> Vec<BlockId> {
    if block_size == 0 || len == 0 {
        return Vec::new();
    }
    let bs = block_size as u64;
    let start = (offset / bs) * bs;
    let end = offset + len; // exclusive
    let mut blocks = Vec::new();
    let mut pos = start;
    while pos < end {
        blocks.push(BlockId::new(
            object.clone(),
            pos,
            block_size,
            version.clone(),
        ));
        pos += bs;
    }
    blocks
}

/// Progress of an in-flight prewarm.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LoadProgress {
    /// Total blocks in the prewarm plan.
    pub total: u64,
    /// Blocks confirmed loaded so far.
    pub completed: u64,
}

impl LoadProgress {
    /// Whether every planned block has completed.
    pub fn is_done(&self) -> bool {
        self.completed >= self.total
    }

    /// Fraction complete in `[0.0, 1.0]` (1.0 when there is nothing to do).
    pub fn fraction(&self) -> f64 {
        if self.total == 0 {
            1.0
        } else {
            self.completed as f64 / self.total as f64
        }
    }
}

/// Build the full set of per-block assignments for a prewarm request.
///
/// Splits the range, assigns each block's primary via `placement` over `nodes`,
/// and attaches `hint`. The returned plan is what the coordinator turns into
/// `LoadBlobs` messages; pair it with a [`LoadProgress`] initialized to
/// `total = plan.len()`.
#[allow(clippy::too_many_arguments)]
pub fn plan_load<P: Placement>(
    placement: &P,
    nodes: &[NodeInfo],
    object: &ObjectId,
    offset: u64,
    len: u64,
    block_size: u32,
    version: &Version,
    hint: LoadHint,
) -> Vec<LoadAssignment> {
    split_into_blocks(object, offset, len, block_size, version)
        .into_iter()
        .map(|block| {
            let primary = placement.locate(&block, nodes);
            LoadAssignment {
                block,
                primary,
                hint,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RendezvousPlacement;
    use talon_core::{Backend, NodeRole};

    fn object() -> ObjectId {
        ObjectId::new(Backend::S3, "bucket", "big/object.bin")
    }

    fn nodes(ids: &[&str]) -> Vec<NodeInfo> {
        ids.iter()
            .map(|id| NodeInfo {
                id: NodeId::new(*id),
                address: format!("{id}:7001"),
                role: NodeRole::Worker,
            })
            .collect()
    }

    #[test]
    fn split_covers_range_with_aligned_blocks() {
        let bs: u32 = 256 << 20;
        let v = Version::new("v1");
        // 2.5 blocks starting mid-first-block -> blocks at 0, bs, 2*bs.
        let blocks = split_into_blocks(&object(), bs as u64 / 2, bs as u64 * 2, bs, &v);
        let offs: Vec<u64> = blocks.iter().map(|b| b.offset).collect();
        assert_eq!(offs, vec![0, bs as u64, 2 * bs as u64]);
        assert!(blocks.iter().all(|b| b.block_size == bs));
    }

    #[test]
    fn split_edge_cases() {
        let v = Version::new("v1");
        assert!(split_into_blocks(&object(), 0, 0, 256 << 20, &v).is_empty());
        assert!(split_into_blocks(&object(), 0, 100, 0, &v).is_empty());
        // Exactly one block.
        assert_eq!(split_into_blocks(&object(), 0, 10, 256 << 20, &v).len(), 1);
    }

    #[test]
    fn plan_assigns_primary_and_hint() {
        let bs: u32 = 256 << 20;
        let v = Version::new("v1");
        let ns = nodes(&["a", "b", "c"]);
        let hint = LoadHint::Paged {
            page_size: 256 * 1024,
        };
        let plan = plan_load(
            &RendezvousPlacement,
            &ns,
            &object(),
            0,
            bs as u64 * 3,
            bs,
            &v,
            hint,
        );
        assert_eq!(plan.len(), 3);
        for a in &plan {
            assert_eq!(a.hint, hint);
            let primary = a.primary.as_ref().unwrap();
            // Assigned primary matches the raw placement decision.
            assert_eq!(
                Some(primary.clone()),
                RendezvousPlacement.locate(&a.block, &ns)
            );
        }
    }

    #[test]
    fn plan_with_empty_cluster_has_no_primary() {
        let bs: u32 = 256 << 20;
        let v = Version::new("v1");
        let plan = plan_load(
            &RendezvousPlacement,
            &[],
            &object(),
            0,
            bs as u64,
            bs,
            &v,
            LoadHint::Whole,
        );
        assert_eq!(plan.len(), 1);
        assert!(plan[0].primary.is_none());
    }

    #[test]
    fn progress_tracking() {
        let mut p = LoadProgress {
            total: 4,
            completed: 0,
        };
        assert!(!p.is_done());
        assert_eq!(p.fraction(), 0.0);
        p.completed = 4;
        assert!(p.is_done());
        assert_eq!(p.fraction(), 1.0);
        // Empty plan is trivially done.
        assert!(LoadProgress::default().is_done());
        assert_eq!(LoadProgress::default().fraction(), 1.0);
    }
}
