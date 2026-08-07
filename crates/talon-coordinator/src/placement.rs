//! Object placement strategy.
//!
//! Current clients prebuild a deterministic Maglev table when membership
//! changes, making per-block primary lookup O(1). This module retains the
//! coordinator lookup adapter for older clients.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use talon_core::{rank_cache_workers, BlockId, ClusterType, NodeId, NodeInfo};
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
                talon_core::NodeRole::AsyncWorker => 2u8,
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
///
/// # One ring per cluster
///
/// Two strategies exist, and a cluster runs exactly one of them:
///
/// | Strategy | Cluster type | Node set | Hash key |
/// |---|---|---|---|
/// | [`RendezvousPlacement`] | [`ClusterType::Block`] | [`NodeRole::Worker`](talon_core::NodeRole::Worker) | the whole [`BlockId`] (offset included) |
/// | [`ObjectPlacement`] | [`ClusterType::Async`] | [`NodeRole::AsyncWorker`](talon_core::NodeRole::AsyncWorker) | the object identity alone |
///
/// [`ClusterPlacement`] pairs a strategy with its cluster type so the two can
/// never be picked independently. Both used to live in one coordinator,
/// selected per request; ADR 0006 says why that changed.
pub trait Placement {
    /// Return the node responsible for `block`, given the node set.
    ///
    /// Equivalent to the first element of [`Placement::locate_top_k`] with
    /// `k = 1`.
    fn locate(&self, block: &BlockId, nodes: &[NodeInfo]) -> Option<NodeId>;

    /// Return up to `k` deterministic nodes for `block`, primary first.
    ///
    /// `k = 1` yields the same result as [`Placement::locate`].
    fn locate_top_k(&self, block: &BlockId, nodes: &[NodeInfo], k: usize) -> Vec<NodeId>;
}

/// Rank `nodes` by descending HRW weight under `weight`, taking the top `k`.
///
/// Used by the async ring only. The block ring resolves through
/// [`talon_core::CachePlacementTable`]'s Maglev mapping instead, so that a
/// client can compute the same answer locally without asking the coordinator
/// (#455). The async ring has no client-side equivalent yet.
fn rank_top_k<W>(nodes: &[NodeInfo], k: usize, weight: W) -> Vec<NodeId>
where
    W: Fn(&NodeId) -> u64,
{
    if k == 0 {
        return Vec::new();
    }
    let mut ranked: Vec<(u64, &NodeId)> = nodes.iter().map(|n| (weight(&n.id), &n.id)).collect();
    // Sort by descending weight; break ties on the node id so the order is
    // fully deterministic even when two nodes hash to the same weight.
    ranked.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1 .0.cmp(&b.1 .0)));
    ranked
        .into_iter()
        .take(k)
        .map(|(_, id)| id.clone())
        .collect()
}

/// Compatibility adapter using the same deterministic Maglev mapping as clients.
///
/// The historical type name is retained to avoid breaking coordinator callers.
///
/// Because the offset is part of the key, consecutive blocks of one object land
/// on different nodes and a parallel scan fans out across the fleet — which is
/// what a block worker wants, and exactly what an extent cache does not.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RendezvousPlacement;

impl Placement for RendezvousPlacement {
    fn locate(&self, block: &BlockId, nodes: &[NodeInfo]) -> Option<NodeId> {
        rank_cache_workers(block, nodes, 1).into_iter().next()
    }

    fn locate_top_k(&self, block: &BlockId, nodes: &[NodeInfo], k: usize) -> Vec<NodeId> {
        rank_cache_workers(block, nodes, k)
    }
}

/// The async worker ring: rendezvous hashing over the object identity alone.
///
/// # What is in the key
///
/// [`BlockId`] has four fields — object, offset, block size, and version. This
/// ring hashes only the object identity (backend, bucket, path) and drops the
/// other three, so the key is "which name is being read", with no notion of
/// position and no notion of which revision.
///
/// **Why the offset is dropped.** A columnar reader fetches a footer and then
/// cherry-picks column chunks at unrelated offsets. If those hash apart, the
/// footer fetch warms a cache nobody else reads and every chunk pays a cold
/// miss. Whole-object affinity is the entire reason a footer is worth caching,
/// and dropping the offset is what produces it.
///
/// **Why the version is dropped.** With the version in the key, republishing an
/// object under a new ETag relocates it: the new owner starts cold while the
/// previous owner still holds every warm extent of the old revision. Nothing is
/// gained by the move, because the worker's own cache does not distinguish
/// revisions either — it interns the `ObjectId` alone, and assumes the objects
/// it serves are immutable (ADR 0005 §3). Relocating would buy a cold cache and
/// no coherence.
///
/// The tradeoff is that one very large object is served by one async worker
/// rather than spread across the fleet. That is the right trade for
/// many-small-objects columnar workloads and an argument for sizing this
/// cluster separately — not for putting both rings in one cluster.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ObjectPlacement;

impl ObjectPlacement {
    fn weight(block: &BlockId, node: &NodeId) -> u64 {
        let mut hasher = DefaultHasher::new();
        block.object.hash(&mut hasher);
        node.0.hash(&mut hasher);
        hasher.finish()
    }
}

impl Placement for ObjectPlacement {
    fn locate(&self, block: &BlockId, nodes: &[NodeInfo]) -> Option<NodeId> {
        nodes
            .iter()
            .max_by_key(|n| Self::weight(block, &n.id))
            .map(|n| n.id.clone())
    }

    fn locate_top_k(&self, block: &BlockId, nodes: &[NodeInfo], k: usize) -> Vec<NodeId> {
        rank_top_k(nodes, k, |id| Self::weight(block, id))
    }
}

/// The one ring a cluster serves, chosen once from its [`ClusterType`].
///
/// An enum rather than a `Box<dyn Placement>` or a pair of fields: both
/// variants are unit structs, so this costs nothing at runtime, and it makes
/// "a cluster has exactly one ring" a fact the type system holds rather than an
/// invariant every call site has to remember. The previous shape — a service
/// carrying both strategies and picking per request — is what ADR 0006
/// replaced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterPlacement {
    /// The block ring: hashes the whole [`BlockId`], so a parallel scan fans
    /// out across the fleet.
    Block(RendezvousPlacement),
    /// The object ring: hashes the object identity, so every range of one
    /// object lands on one worker.
    Async(ObjectPlacement),
}

impl ClusterPlacement {
    /// The ring a cluster of this type places on.
    pub fn for_type(cluster: ClusterType) -> Self {
        match cluster {
            ClusterType::Block => ClusterPlacement::Block(RendezvousPlacement),
            ClusterType::Async => ClusterPlacement::Async(ObjectPlacement),
        }
    }

    /// The cluster type this ring belongs to.
    pub fn cluster_type(&self) -> ClusterType {
        match self {
            ClusterPlacement::Block(_) => ClusterType::Block,
            ClusterPlacement::Async(_) => ClusterType::Async,
        }
    }
}

impl Placement for ClusterPlacement {
    fn locate(&self, block: &BlockId, nodes: &[NodeInfo]) -> Option<NodeId> {
        match self {
            ClusterPlacement::Block(p) => p.locate(block, nodes),
            ClusterPlacement::Async(p) => p.locate(block, nodes),
        }
    }

    fn locate_top_k(&self, block: &BlockId, nodes: &[NodeInfo], k: usize) -> Vec<NodeId> {
        match self {
            ClusterPlacement::Block(p) => p.locate_top_k(block, nodes, k),
            ClusterPlacement::Async(p) => p.locate_top_k(block, nodes, k),
        }
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
    fn membership_change_never_returns_a_removed_worker() {
        let p = RendezvousPlacement;
        let full = nodes(&["a", "b", "c", "d", "e"]);
        for i in 0..100 {
            let blk = block(i);
            let full_rank = p.locate_top_k(&blk, &full, full.len());
            let dropped = &full_rank[0];
            let survivors: Vec<NodeInfo> =
                full.iter().filter(|n| &n.id != dropped).cloned().collect();
            let sub_rank = p.locate_top_k(&blk, &survivors, survivors.len());
            assert!(!sub_rank.contains(dropped));
            assert_eq!(sub_rank.len(), survivors.len());
        }
    }

    /// The negative control: the block ring must still fan out.
    ///
    /// Object affinity is right for an extent cache and wrong for a block
    /// worker, where spreading consecutive blocks is what lets a parallel scan
    /// use the whole fleet. If this ever starts behaving like the test above,
    /// the two rings have collapsed into one.
    #[test]
    fn the_block_ring_still_spreads_offsets_across_the_fleet() {
        let p = RendezvousPlacement;
        let ns = nodes(&["a", "b", "c", "d", "e"]);
        let base = block(1);

        let mut owners = std::collections::HashSet::new();
        for i in 0..32u64 {
            let mut at = base.clone();
            at.offset = i * (256 << 20);
            owners.insert(p.locate(&at, &ns).unwrap().0);
        }
        assert!(
            owners.len() > 1,
            "the block ring collapsed to one node: {owners:?}"
        );
    }

    /// Every range of one object must resolve to a single async worker.
    ///
    /// Dropping the offset is what buys this. A Parquet reader fetches the
    /// footer, then cherry-picks column chunks at unrelated offsets; if those
    /// hash to different nodes, the footer fetch warms a cache nobody else
    /// reads and each chunk pays a cold miss.
    #[test]
    fn the_object_ring_pins_a_whole_object_to_one_node() {
        let p = ObjectPlacement;
        let ns = nodes(&["a", "b", "c", "d", "e"]);
        let base = block(1);

        let owner = p.locate(&base, &ns).unwrap();
        for offset in [0, 4 << 10, 1 << 20, 256 << 20, 900 << 20] {
            let mut at = base.clone();
            at.offset = offset;
            // The block size travels with the offset in a real BlockId, so
            // vary it too: neither may reach the key.
            at.block_size = 64 << 20;
            assert_eq!(
                p.locate(&at, &ns),
                Some(owner.clone()),
                "offset {offset} left the object's node"
            );
        }
    }

    /// Republishing an object must not move it.
    ///
    /// `BlockId` derives `Hash` over all four fields, version included, so
    /// hashing the whole id would relocate an object on every overwrite and
    /// hand it to a worker with a cold cache. Nothing is bought by that: the
    /// async worker's own cache does not distinguish revisions either — it
    /// interns the `ObjectId` alone and assumes immutability (ADR 0005 §3) —
    /// so the move would cost a cold cache and buy no coherence.
    #[test]
    fn the_object_ring_survives_a_republish() {
        let p = ObjectPlacement;
        let ns = nodes(&["a", "b", "c", "d", "e"]);

        for i in 0..50 {
            let mut v1 = block(i);
            let mut v2 = v1.clone();
            v2.version = Version("etag-after-overwrite".into());
            // Offset differs too: neither field may reach the key.
            v1.offset = 0;
            v2.offset = 512 << 20;

            assert_eq!(
                p.locate(&v1, &ns),
                p.locate(&v2, &ns),
                "object {i} migrated on republish"
            );
        }
    }
    /// The block ring must agree with what clients compute for themselves.
    ///
    /// Since #455 a client rebuilds the Maglev table from its own membership
    /// snapshot instead of asking the coordinator, so these are two
    /// implementations of one mapping. If they drift, a client fetches from a
    /// worker the coordinator does not consider the owner — the read still
    /// succeeds, by fetching from origin and caching a second copy, so the
    /// symptom is a quietly halved hit rate rather than an error.
    ///
    /// This also pins that adding the async ring did not perturb the block
    /// ring, which is what this test originally existed to catch.
    #[test]
    fn the_block_ring_agrees_with_client_side_placement() {
        let ns = nodes(&["a", "b", "c", "d", "e"]);
        for i in 0..50 {
            let blk = block(i);
            let expected = talon_core::rank_cache_workers(&blk, &ns, 1)
                .into_iter()
                .next();
            assert_eq!(
                RendezvousPlacement.locate(&blk, &ns),
                expected,
                "block {i} moved"
            );
        }
    }

    /// The two rings must disagree, or the object ring is doing nothing.
    #[test]
    fn the_rings_are_genuinely_different() {
        let ns = nodes(&["a", "b", "c", "d", "e"]);
        let differs = (0..200u64).any(|i| {
            let mut blk = block(i);
            blk.offset = 256 << 20;
            RendezvousPlacement.locate(&blk, &ns) != ObjectPlacement.locate(&blk, &ns)
        });
        assert!(
            differs,
            "both rings produced identical placement everywhere"
        );
    }

    /// A cluster type selects its ring, and only its ring.
    ///
    /// The pairing used to be made by hand at each call site — a strategy here,
    /// a role filter there — which is how a lookup could come to cross pools.
    #[test]
    fn a_cluster_type_selects_its_own_ring() {
        let ns = nodes(&["a", "b", "c", "d", "e"]);
        let blocks = ClusterPlacement::for_type(ClusterType::Block);
        let extents = ClusterPlacement::for_type(ClusterType::Async);

        assert_eq!(blocks.cluster_type(), ClusterType::Block);
        assert_eq!(extents.cluster_type(), ClusterType::Async);

        for i in 0..50 {
            let blk = block(i);
            assert_eq!(
                blocks.locate(&blk, &ns),
                RendezvousPlacement.locate(&blk, &ns)
            );
            assert_eq!(extents.locate(&blk, &ns), ObjectPlacement.locate(&blk, &ns));
            assert_eq!(
                blocks.locate_top_k(&blk, &ns, 3),
                RendezvousPlacement.locate_top_k(&blk, &ns, 3)
            );
            assert_eq!(
                extents.locate_top_k(&blk, &ns, 3),
                ObjectPlacement.locate_top_k(&blk, &ns, 3)
            );
        }
    }

    /// Each cluster type keeps the behaviour its ring exists for.
    #[test]
    fn the_block_cluster_spreads_offsets_and_the_async_cluster_does_not() {
        let ns = nodes(&["a", "b", "c", "d", "e"]);
        let offsets = |p: &ClusterPlacement| -> std::collections::HashSet<String> {
            (0..32u64)
                .map(|i| {
                    let mut at = block(1);
                    at.offset = i * (256 << 20);
                    p.locate(&at, &ns).unwrap().0
                })
                .collect()
        };

        assert!(offsets(&ClusterPlacement::for_type(ClusterType::Block)).len() > 1);
        assert_eq!(
            offsets(&ClusterPlacement::for_type(ClusterType::Async)).len(),
            1,
            "the async cluster must pin a whole object to one worker"
        );
    }

    /// Distinct objects should still spread across the async pool.
    #[test]
    fn the_object_ring_spreads_distinct_objects() {
        let p = ObjectPlacement;
        let ns = nodes(&["a", "b", "c", "d", "e"]);
        let owners: std::collections::HashSet<String> = (0..200)
            .map(|i| p.locate(&block(i), &ns).unwrap().0)
            .collect();
        assert_eq!(owners.len(), ns.len(), "an async worker got no objects");
    }

    /// HRW stability must hold on the object ring too.
    #[test]
    fn the_object_ring_is_stable_under_membership_change() {
        let p = ObjectPlacement;
        let full = nodes(&["a", "b", "c", "d", "e"]);
        for i in 0..100 {
            let blk = block(i);
            let full_rank = p.locate_top_k(&blk, &full, full.len());
            let dropped = &full_rank[0];
            let survivors: Vec<NodeInfo> =
                full.iter().filter(|n| &n.id != dropped).cloned().collect();
            let sub_rank = p.locate_top_k(&blk, &survivors, survivors.len());
            let expected: Vec<&NodeId> = full_rank.iter().skip(1).collect();
            assert_eq!(expected, sub_rank.iter().collect::<Vec<_>>(), "object {i}");
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
