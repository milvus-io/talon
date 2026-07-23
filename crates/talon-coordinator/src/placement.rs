//! Object placement strategy.
//!
//! Placement uses rendezvous (highest-random-weight, HRW) hashing so that the
//! set of blocks that must move when a node joins or leaves is minimized. On
//! top of the single-owner `locate`, [`Placement::locate_top_k`] returns an
//! ordered replica list; RF stays 1 in v1, but the ordering reserves a stable
//! replica sequence for a future RF=2.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use talon_core::{BlockId, NodeId, NodeInfo};

/// A monotonically increasing version of the placement/node set.
///
/// The coordinator bumps the epoch whenever membership changes so that clients
/// and workers can detect stale placement decisions and refresh.
///
/// # Monotonicity across restarts
///
/// Clients only refresh cached placement when they observe an epoch **strictly
/// greater** than the one they hold, so the epoch must never move backwards —
/// including across a coordinator process restart. A plain in-memory counter
/// from `0` breaks this: a restarted coordinator would re-advertise low epochs
/// that clients (holding a higher pre-restart epoch) silently ignore, pinning
/// them to a stale replica list forever (see issue #69).
///
/// To stay monotonic without external state, the epoch is seeded from the
/// process **start time**: the wall-clock second occupies the high 32 bits and
/// an in-process counter the low 32 bits. A later process always starts from a
/// larger seed than any earlier one produced, so its epochs outrank them.
///
/// This keeps the coordinator free of unrebuildable persistent state (a v1
/// design invariant). The residual edge — two restarts within the *same* second
/// where the later process observes fewer membership changes — is negligible in
/// practice (pod restarts take seconds). A future revision may instead derive
/// the epoch from the Kubernetes `resourceVersion` of the worker endpoints,
/// which is monotonic by construction (tracked for v1.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Epoch(pub u64);

impl Epoch {
    /// Return the next epoch (this + 1).
    pub fn next(self) -> Self {
        Epoch(self.0 + 1)
    }

    /// A fresh epoch seeded from the current wall-clock second in the high 32
    /// bits, with a zero low counter. Used as the base for a newly started
    /// coordinator so its epochs outrank any earlier process's (see the type
    /// docs for the monotonicity rationale).
    pub fn seeded_now() -> Self {
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        // Clamp to 32 bits (good past year 2106) and shift into the high half.
        Epoch((secs & 0xFFFF_FFFF) << 32)
    }
}

/// Decides which node(s) should hold a given block.
pub trait Placement {
    /// Return the node responsible for `block`, given the current node set.
    ///
    /// Equivalent to the first element of [`Placement::locate_top_k`] with
    /// `k = 1`.
    fn locate(&self, block: &BlockId, nodes: &[NodeInfo]) -> Option<NodeId>;

    /// Return up to `k` nodes for `block`, ordered by descending HRW weight.
    ///
    /// The ordering is deterministic and stable under membership changes: the
    /// relative order of any two surviving nodes never changes when a third
    /// node is added or removed (the HRW property). `k = 1` yields the same
    /// result as [`Placement::locate`].
    fn locate_top_k(&self, block: &BlockId, nodes: &[NodeInfo], k: usize) -> Vec<NodeId>;
}

/// Rendezvous (highest random weight) hashing placement.
///
/// Minimizes reassignment when nodes join or leave the cluster.
#[derive(Default)]
pub struct RendezvousPlacement;

impl RendezvousPlacement {
    fn weight(block: &BlockId, node: &NodeId) -> u64 {
        let mut hasher = DefaultHasher::new();
        block.hash(&mut hasher);
        node.0.hash(&mut hasher);
        hasher.finish()
    }
}

impl Placement for RendezvousPlacement {
    fn locate(&self, block: &BlockId, nodes: &[NodeInfo]) -> Option<NodeId> {
        nodes
            .iter()
            .max_by_key(|n| Self::weight(block, &n.id))
            .map(|n| n.id.clone())
    }

    fn locate_top_k(&self, block: &BlockId, nodes: &[NodeInfo], k: usize) -> Vec<NodeId> {
        if k == 0 {
            return Vec::new();
        }
        let mut ranked: Vec<(u64, &NodeId)> = nodes
            .iter()
            .map(|n| (Self::weight(block, &n.id), &n.id))
            .collect();
        // Sort by descending weight; break ties on the node id so the order is
        // fully deterministic even when two nodes hash to the same weight.
        ranked.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1 .0.cmp(&b.1 .0)));
        ranked
            .into_iter()
            .take(k)
            .map(|(_, id)| id.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{Backend, ObjectId, Version};

    fn block(n: u64) -> BlockId {
        BlockId {
            object: ObjectId {
                backend: Backend::S3,
                bucket: "b".into(),
                object_path: format!("obj/{n}"),
            },
            offset: 0,
            block_size: 256 << 20,
            version: Version("v1".into()),
        }
    }

    fn nodes(ids: &[&str]) -> Vec<NodeInfo> {
        ids.iter()
            .map(|id| NodeInfo {
                id: NodeId::new(*id),
                address: "127.0.0.1:0".into(),
                role: talon_core::NodeRole::Worker,
            })
            .collect()
    }

    #[test]
    fn locate_matches_top_k_first() {
        let p = RendezvousPlacement;
        let ns = nodes(&["a", "b", "c", "d"]);
        for i in 0..50 {
            let blk = block(i);
            let single = p.locate(&blk, &ns);
            let topk = p.locate_top_k(&blk, &ns, 1);
            assert_eq!(single.as_ref(), topk.first());
        }
    }

    #[test]
    fn top_k_len_and_uniqueness() {
        let p = RendezvousPlacement;
        let ns = nodes(&["a", "b", "c", "d", "e"]);
        let ranked = p.locate_top_k(&block(7), &ns, 3);
        assert_eq!(ranked.len(), 3);
        let mut sorted: Vec<String> = ranked.iter().map(|n| n.0.clone()).collect();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 3, "no duplicates in top-k");
    }

    #[test]
    fn top_k_stable_under_membership_change() {
        // Removing a node never reorders the two surviving nodes (HRW property).
        let p = RendezvousPlacement;
        let full = nodes(&["a", "b", "c", "d", "e"]);
        for i in 0..100 {
            let blk = block(i);
            let full_rank = p.locate_top_k(&blk, &full, full.len());
            // Drop the top-ranked node and re-rank.
            let dropped = &full_rank[0];
            let survivors: Vec<NodeInfo> =
                full.iter().filter(|n| &n.id != dropped).cloned().collect();
            let sub_rank = p.locate_top_k(&blk, &survivors, survivors.len());
            let expected: Vec<&NodeId> = full_rank.iter().skip(1).collect();
            let got: Vec<&NodeId> = sub_rank.iter().collect();
            assert_eq!(expected, got, "surviving order changed for block {i}");
        }
    }

    #[test]
    fn epoch_increments() {
        let e = Epoch::default();
        assert_eq!(e.0, 0);
        assert_eq!(e.next().0, 1);
        assert!(e < e.next());
    }

    #[test]
    fn seeded_now_puts_time_in_high_bits() {
        let e = Epoch::seeded_now();
        // High 32 bits carry a nonzero wall-clock second; low 32 start clear.
        assert!(e.0 >> 32 > 0, "seed must occupy the high bits");
        assert_eq!(e.0 & 0xFFFF_FFFF, 0, "low counter starts at 0");
        // A seed leaves ample low-bit headroom before it could collide with the
        // next second's seed (2^32 counter increments per second).
        assert!(e.next().0 > e.0);
    }
}
