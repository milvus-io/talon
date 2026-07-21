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

use talon_core::BlockId;
use talon_transport::ControlMessage;

use crate::{Epoch, Membership, Placement};

/// The result of a placement lookup: ordered owners at a given epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementResult {
    /// Ordered replica node ids (highest HRW weight first). Empty if no nodes.
    pub owners: Vec<String>,
    /// The placement epoch these owners were computed at.
    pub epoch: Epoch,
}

/// Answers placement lookups from the current membership + strategy.
pub struct PlacementService<P: Placement> {
    membership: Membership,
    placement: P,
}

impl<P: Placement> PlacementService<P> {
    /// Create a service over the given membership registry and strategy.
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

    /// Locate up to `k` ordered owners for `block` at the current epoch.
    ///
    /// The returned epoch is read together with the node snapshot so the
    /// answer is internally consistent for the client to cache.
    pub fn lookup(&self, block: &BlockId, k: usize) -> PlacementResult {
        // Read the epoch first, then the nodes; membership only advances the
        // epoch, so a concurrent change can only make the epoch we return
        // conservatively old, prompting a harmless client refresh.
        let epoch = self.membership.epoch();
        let nodes = self.membership.snapshot();
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
                let res = self.lookup(&block, k as usize);
                let owners = res.owners.into_iter().map(talon_core::NodeId).collect();
                ControlMessage::PlacementResponse {
                    owners,
                    epoch: res.epoch.0,
                }
            }
            other => ControlMessage::Ack {
                ok: false,
                detail: Some(format!("unexpected control message: {other:?}")),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RendezvousPlacement;
    use talon_core::{Backend, NodeId, NodeInfo, NodeRole, ObjectId, Version};

    fn block(n: u64) -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "b", format!("o/{n}")),
            0,
            256 << 20,
            Version::new("v1"),
        )
    }

    fn svc(ids: &[&str]) -> PlacementService<RendezvousPlacement> {
        let m = Membership::new();
        for id in ids {
            m.register(NodeInfo {
                id: NodeId::new(*id),
                address: format!("{id}:7001"),
                role: NodeRole::Worker,
            });
        }
        PlacementService::new(m, RendezvousPlacement)
    }

    #[test]
    fn lookup_returns_owners_and_epoch() {
        let s = svc(&["a", "b", "c"]);
        let res = s.lookup(&block(1), 2);
        assert_eq!(res.owners.len(), 2);
        assert_eq!(res.epoch, Epoch(3)); // three registrations

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
    fn epoch_advances_and_is_visible_to_clients() {
        let s = svc(&["a", "b"]);
        let e0 = s.lookup(&block(9), 1).epoch;
        s.membership().register(NodeInfo {
            id: NodeId::new("c"),
            address: "c:1".into(),
            role: NodeRole::Worker,
        });
        let e1 = s.lookup(&block(9), 1).epoch;
        assert!(e1 > e0, "epoch must advance on membership change");
    }

    #[test]
    fn handle_placement_lookup_message() {
        let s = svc(&["a", "b", "c"]);
        let req = ControlMessage::PlacementLookup {
            block: block(2),
            k: 1,
        };
        match s.handle(req) {
            ControlMessage::PlacementResponse { owners, epoch } => {
                assert_eq!(owners.len(), 1);
                assert_eq!(epoch, 3);
            }
            other => panic!("expected PlacementResponse, got {other:?}"),
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
        assert_eq!(res.epoch, Epoch(0));
    }
}
