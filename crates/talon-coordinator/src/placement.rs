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
use xxhash_rust::xxh3::xxh3_64;

/// A content-derived version of the placement/node set.
///
/// The coordinator publishes this version alongside every placement answer so
/// clients and workers can detect when their cached placement predates a
/// membership change and refresh.
///
/// # Deterministic across coordinators
///
/// Talon runs coordinators **active-active**: several stateless processes serve
/// placement for the same cluster behind a load balancer. A client's successive
/// lookups can land on different coordinators, so the version a client caches
/// must depend only on the *observable membership*, never on which process
/// answered or when it started.
///
/// Earlier revisions seeded the version from the process start time plus a
/// process-local counter (issue #69/#71). That is fine for a single coordinator
/// but breaks under active-active: two processes holding the **same** healthy
/// worker set would advertise **different** counters, so a load-balanced client
/// would see the version flip on every other request and refresh its cache
/// continuously — or, worse, treat a peer's legitimately different value as
/// "older" and ignore it.
///
/// Instead the version is a stable 64-bit hash of the placement-relevant fields
/// of the healthy node set (each node's id, address, and role), computed over a
/// canonical, id-sorted encoding. Two coordinators with identical membership
/// therefore compute the **identical** version, and any placement-relevant
/// change (a node joining, leaving, or changing address) changes it. The value
/// carries no ordering meaning: clients compare versions for **equality**, not
/// magnitude (see the FUSE placement cache).
///
/// The hash keeps coordinators free of unrebuildable persistent state (a v1
/// design invariant) while remaining backend-neutral: a future revision can map
/// an opaque Kubernetes `resourceVersion` or etcd revision onto the same token
/// type without changing clients.
/// `Ord`/`PartialOrd` are intentionally NOT derived: the epoch is an
/// equality-only content hash, so magnitude comparison is meaningless (there is
/// no "newer > older" since the #80 monotonic→hash change). Omitting the derives
/// makes a stray `>`/`<` on an epoch fail to compile rather than silently do the
/// wrong thing; clients compare with `==`/`!=` only. `StoreRevision` models the
/// same constraint by not deriving `Ord` (#167).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Epoch(pub u64);

impl Epoch {
    /// The version of an empty node set.
    ///
    /// Distinct from any non-empty membership's version so a client caching a
    /// placement against a populated cluster still refreshes if the cluster
    /// drains to zero nodes.
    pub const EMPTY: Epoch = Epoch(0);

    /// Compute the deterministic placement version for a node set.
    ///
    /// The result depends only on the placement-relevant fields (id, address,
    /// role) of the nodes, is independent of their input order, and is identical
    /// on any coordinator observing the same membership. An empty set maps to
    /// [`Epoch::EMPTY`].
    pub fn for_nodes(nodes: &[NodeInfo]) -> Self {
        if nodes.is_empty() {
            return Epoch::EMPTY;
        }
        // Canonicalize: sort by node id so input order never affects the hash,
        // and encode each placement-relevant field with an unambiguous length
        // delimiter so distinct field boundaries can never collide (e.g.
        // id="ab",addr="c" must not hash like id="a",addr="bc").
        let mut ids: Vec<&NodeInfo> = nodes.iter().collect();
        ids.sort_unstable_by(|a, b| a.id.0.cmp(&b.id.0));
        let mut buf: Vec<u8> = Vec::with_capacity(nodes.len() * 48);
        for node in ids {
            let role = match node.role {
                talon_core::NodeRole::Coordinator => 0u8,
                talon_core::NodeRole::Worker => 1u8,
            };
            push_field(&mut buf, node.id.0.as_bytes());
            push_field(&mut buf, node.address.as_bytes());
            buf.push(role);
        }
        // A non-empty set must never hash to the reserved empty sentinel, so a
        // populated cluster is always distinguishable from a drained one.
        let raw = xxh3_64(&buf);
        Epoch(if raw == Epoch::EMPTY.0 { 1 } else { raw })
    }
}

/// Append a length-prefixed field to the canonical membership encoding.
fn push_field(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    buf.extend_from_slice(bytes);
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
    fn version_is_deterministic_and_order_independent() {
        let a = nodes(&["a", "b", "c"]);
        let mut shuffled = a.clone();
        shuffled.reverse();
        // Same membership, different input order -> identical version.
        assert_eq!(Epoch::for_nodes(&a), Epoch::for_nodes(&shuffled));
        // Recomputing on a fresh process (simulated by a fresh call) is stable.
        assert_eq!(Epoch::for_nodes(&a), Epoch::for_nodes(&a));
    }

    #[test]
    fn version_changes_on_placement_relevant_change() {
        let base = Epoch::for_nodes(&nodes(&["a", "b"]));
        // Adding a node changes the version.
        assert_ne!(base, Epoch::for_nodes(&nodes(&["a", "b", "c"])));
        // Removing a node changes the version.
        assert_ne!(base, Epoch::for_nodes(&nodes(&["a"])));
        // An address change on the same id changes the version.
        let mut moved = nodes(&["a", "b"]);
        moved[0].address = "moved:9999".into();
        assert_ne!(base, Epoch::for_nodes(&moved));
    }

    #[test]
    fn empty_set_is_reserved_sentinel() {
        assert_eq!(Epoch::for_nodes(&[]), Epoch::EMPTY);
        assert_eq!(Epoch::EMPTY.0, 0);
        // A populated cluster is always distinguishable from a drained one.
        assert_ne!(Epoch::for_nodes(&nodes(&["a"])), Epoch::EMPTY);
    }

    #[test]
    fn field_boundaries_do_not_collide() {
        // Length-delimited encoding: shifting a byte across the id/address
        // boundary must produce a different version.
        let mut left = nodes(&["ab"]);
        left[0].address = "c".into();
        let mut right = nodes(&["a"]);
        right[0].address = "bc".into();
        assert_ne!(Epoch::for_nodes(&left), Epoch::for_nodes(&right));
    }
}
