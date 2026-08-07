//! Placement lookup service.
//!
//! [`PlacementService`] ties [`Membership`] and a [`Placement`] strategy
//! together to answer the client-facing "where does this block live?" query. A
//! lookup returns the ordered replica list *plus the current epoch*, so a
//! client can cache the answer and detect staleness (epoch mismatch, wrong
//! owner) on a later request and refresh.
//!
//! [`PlacementService::handle`] adapts the transport-level
//! [`ControlMessage::PlacementLookup`] into a [`ControlMessage::PlacementResponse`],
//! keeping the service usable directly (via [`lookup`](PlacementService::lookup))
//! or over the wire.
//!
//! There is one ring, fixed by the cluster's [`ClusterType`] at startup. A
//! client does not choose it — it is a property of which cluster it connected
//! to. See ADR 0006.

use talon_core::BlockId;
use talon_transport::ControlMessage;

use crate::{Epoch, Membership, Placement};

/// The result of a placement lookup: ordered owners at a given epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementResult {
    /// Ordered replica node ids (primary first). Empty if no nodes.
    pub owners: Vec<String>,
    /// The placement epoch these owners were computed at.
    pub epoch: Epoch,
}

/// Answers placement lookups from the current membership + strategy.
///
/// Holds exactly one ring. Which one is decided once, from the cluster type,
/// when the coordinator starts — see [`ClusterPlacement::for_type`].
///
/// [`ClusterPlacement::for_type`]: crate::ClusterPlacement::for_type
pub struct PlacementService<P: Placement> {
    membership: Membership,
    placement: P,
}

impl<P: Placement> PlacementService<P> {
    /// Create a service over the given membership registry and strategy.
    ///
    /// The two must agree about which pool this cluster serves: `membership`
    /// admits one worker role and `placement` hashes for that role's ring.
    /// Build both from the same [`ClusterType`] rather than pairing them by
    /// hand.
    ///
    /// [`ClusterType`]: talon_core::ClusterType
    pub fn new(membership: Membership, placement: P) -> Self {
        Self {
            membership,
            placement,
        }
    }

    /// Access the underlying membership registry.
    pub fn membership(&self) -> &Membership {
        &self.membership
    }

    /// Locate up to `k` ordered owners for `block`, at the current epoch.
    ///
    /// The returned epoch is read together with the node snapshot so the
    /// answer is internally consistent for the client to cache.
    pub fn lookup(&self, block: &BlockId, k: usize) -> PlacementResult {
        // Read the epoch first, then the nodes; membership only advances the
        // epoch, so a concurrent change can only make the epoch we return
        // conservatively old, prompting a harmless client refresh.
        let epoch = self.membership.epoch();
        let nodes = self.membership.worker_snapshot();
        let owners = self
            .placement
            .locate_top_k(block, &nodes, k)
            .into_iter()
            .map(|n| n.0)
            .collect();
        PlacementResult { owners, epoch }
    }

    /// Handle a transport [`ControlMessage`].
    ///
    /// Answers [`ControlMessage::PlacementLookup`] with a
    /// [`ControlMessage::PlacementResponse`]; any other message yields an
    /// [`ControlMessage::Ack`] with `ok: false` describing the mismatch.
    pub fn handle(&self, msg: ControlMessage) -> ControlMessage {
        match msg {
            ControlMessage::PlacementLookup { block, k } => {
                response(self.lookup(&block, k as usize))
            }
            // The ring is no longer the client's to name. Reject rather than
            // resolve: a client sending this believes it is choosing a pool,
            // and answering anyway would confirm a belief that is now wrong.
            // Removed from the wire entirely in the next change.
            ControlMessage::RingPlacementLookup { ring, .. } => ControlMessage::Ack {
                ok: false,
                detail: Some(format!(
                    "this cluster serves the {} ring only; a lookup cannot name a ring \
                     (requested {ring})",
                    self.membership.cluster_type()
                )),
            },
            other => ControlMessage::Ack {
                ok: false,
                detail: Some(format!("unexpected control message: {other:?}")),
            },
        }
    }
}

fn response(res: PlacementResult) -> ControlMessage {
    ControlMessage::PlacementResponse {
        owners: res.owners.into_iter().map(talon_core::NodeId).collect(),
        epoch: res.epoch.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClusterPlacement, RendezvousPlacement};
    use talon_core::{Backend, ClusterType, NodeId, NodeInfo, NodeRole, ObjectId, Version};
    use talon_transport::Ring;

    fn block(n: u64) -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "b", format!("o/{n}")),
            0,
            256 << 20,
            Version::new("v1"),
        )
    }

    fn svc(ids: &[&str]) -> PlacementService<ClusterPlacement> {
        cluster(ClusterType::Block, ids, &[])
    }

    /// A service for one cluster type, given block-worker and async-worker ids.
    ///
    /// Both lists are offered to every cluster on purpose: whichever role the
    /// cluster does not admit must be refused, and passing only the "right"
    /// ones would never exercise that.
    fn cluster(
        cluster_type: ClusterType,
        block_ids: &[&str],
        async_ids: &[&str],
    ) -> PlacementService<ClusterPlacement> {
        let m = Membership::for_cluster(cluster_type);
        for (ids, role) in [
            (block_ids, NodeRole::Worker),
            (async_ids, NodeRole::AsyncWorker),
        ] {
            for id in ids {
                m.register(NodeInfo {
                    id: NodeId::new(*id),
                    address: format!("{id}:7001"),
                    role,
                });
            }
        }
        PlacementService::new(m, ClusterPlacement::for_type(cluster_type))
    }

    #[test]
    fn lookup_returns_owners_and_version() {
        let s = svc(&["a", "b", "c"]);
        let res = s.lookup(&block(1), 2);
        assert_eq!(res.owners.len(), 2);
        // The version is the deterministic hash of the node set.
        assert_eq!(res.epoch, Epoch::for_nodes(&s.membership().snapshot()));

        // Matches the raw placement's top-K ordering.
        let nodes = s.membership().snapshot();
        let expect: Vec<String> = RendezvousPlacement
            .locate_top_k(&block(1), &nodes, 2)
            .into_iter()
            .map(|n| n.0)
            .collect();
        assert_eq!(res.owners, expect);
    }

    #[test]
    fn version_changes_on_membership_change() {
        let s = svc(&["a", "b"]);
        let e0 = s.lookup(&block(9), 1).epoch;
        s.membership().register(NodeInfo {
            id: NodeId::new("c"),
            address: "c:1".into(),
            role: NodeRole::Worker,
        });
        let e1 = s.lookup(&block(9), 1).epoch;
        assert_ne!(e1, e0, "version must change on membership change");
    }

    #[test]
    fn handle_placement_lookup_message() {
        let s = svc(&["a", "b", "c"]);
        let expected = s.lookup(&block(2), 1).epoch.0;
        let req = ControlMessage::PlacementLookup {
            block: block(2),
            k: 1,
        };
        match s.handle(req) {
            ControlMessage::PlacementResponse { owners, epoch } => {
                assert_eq!(owners.len(), 1);
                assert_eq!(epoch, expected);
            }
            other => panic!("expected PlacementResponse, got {other:?}"),
        }
    }

    /// A lookup can only ever name this cluster's own workers.
    ///
    /// It used to be possible to hold both pools in one registry and pick
    /// between them per request. Now the other pool is not merely unselected —
    /// it was refused at registration and is not present to select.
    #[test]
    fn a_lookup_only_ever_names_this_clusters_workers() {
        let blocks = cluster(ClusterType::Block, &["blk-a", "blk-b"], &["ext-a", "ext-b"]);
        let extents = cluster(ClusterType::Async, &["blk-a", "blk-b"], &["ext-a", "ext-b"]);

        for i in 0..50 {
            let owners = blocks.lookup(&block(i), 2).owners;
            assert!(
                !owners.is_empty() && owners.iter().all(|o| o.starts_with("blk-")),
                "block cluster named an async worker: {owners:?}"
            );
            let owners = extents.lookup(&block(i), 2).owners;
            assert!(
                !owners.is_empty() && owners.iter().all(|o| o.starts_with("ext-")),
                "async cluster named a block worker: {owners:?}"
            );
        }
    }

    /// The two cluster types must place differently, or one of the rings is
    /// doing nothing.
    #[test]
    fn each_cluster_type_places_on_its_own_ring() {
        let blocks = cluster(ClusterType::Block, &["a", "b", "c", "d", "e"], &[]);
        let extents = cluster(ClusterType::Async, &[], &["a", "b", "c", "d", "e"]);

        // Same node names in both, so any difference is the hash key, not the
        // node set: the block ring includes the offset and the object ring
        // does not.
        let mut at_offset = block(1);
        at_offset.offset = 256 << 20;
        let differs = (0..200u64).any(|i| {
            let mut b = block(i);
            b.offset = 256 << 20;
            blocks.lookup(&b, 1).owners != extents.lookup(&b, 1).owners
        });
        assert!(differs, "both cluster types placed identically everywhere");
    }

    /// An async cluster with no workers yet answers "nobody".
    #[test]
    fn a_cluster_with_no_workers_has_no_owners_but_a_real_epoch() {
        let s = cluster(ClusterType::Async, &["blk-a"], &[]);
        let res = s.lookup(&block(1), 2);
        assert!(res.owners.is_empty());
        // The refused block worker is not in the registry, so there is nothing
        // for the epoch to cover either.
        assert_eq!(res.epoch, Epoch::EMPTY);
    }

    /// A client naming a ring is told it cannot, and which cluster it reached.
    ///
    /// Silently resolving would confirm a belief that is now wrong; answering
    /// with an empty owner list would look like "no workers yet".
    #[test]
    fn a_lookup_that_names_a_ring_is_refused_with_the_clusters_type() {
        let s = cluster(ClusterType::Async, &[], &["ext-a"]);
        match s.handle(ControlMessage::RingPlacementLookup {
            block: block(3),
            ring: Ring::Block,
            k: 2,
        }) {
            ControlMessage::Ack {
                ok: false,
                detail: Some(detail),
            } => {
                assert!(detail.contains("async"), "must name the cluster: {detail}");
                assert!(detail.contains("block"), "must name the request: {detail}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    #[test]
    fn handle_rejects_unexpected_message() {
        let s = svc(&["a"]);
        let resp = s.handle(ControlMessage::EpochBump { epoch: 5 });
        assert!(matches!(resp, ControlMessage::Ack { ok: false, .. }));
    }

    #[test]
    fn empty_cluster_yields_no_owners() {
        let s = svc(&[]);
        let res = s.lookup(&block(1), 3);
        assert!(res.owners.is_empty());
        assert_eq!(res.epoch, Epoch::EMPTY);
    }
}
