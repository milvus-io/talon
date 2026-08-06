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
//! There are two rings. Which one a lookup resolves against is the client's
//! choice, carried as a [`Ring`] on the request: the coordinator sees only a
//! byte range and would have to guess, while the caller knows what it is about
//! to read.

use talon_core::BlockId;
use talon_transport::{ControlMessage, Ring};

use crate::{Epoch, Membership, ObjectPlacement, Placement};

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
/// Holds both rings. The generic `P` is the block ring, so existing
/// constructions are unchanged; the async ring is always [`ObjectPlacement`],
/// which is a unit struct and costs nothing to carry.
pub struct PlacementService<P: Placement> {
    membership: Membership,
    placement: P,
    async_placement: ObjectPlacement,
}

impl<P: Placement> PlacementService<P> {
    /// Create a service over the given membership registry and strategy.
    pub fn new(membership: Membership, placement: P) -> Self {
        Self {
            membership,
            placement,
            async_placement: ObjectPlacement,
        }
    }

    /// Access the underlying membership registry.
    pub fn membership(&self) -> &Membership {
        &self.membership
    }

    /// Locate up to `k` ordered block workers for `block`, at the current epoch.
    ///
    /// The returned epoch is read together with the node snapshot so the
    /// answer is internally consistent for the client to cache.
    ///
    /// Equivalent to [`lookup_on`](Self::lookup_on) with [`Ring::Block`], which
    /// is what an unqualified "lookup" has always meant.
    pub fn lookup(&self, block: &BlockId, k: usize) -> PlacementResult {
        self.lookup_on(Ring::Block, block, k)
    }

    /// Locate up to `k` ordered owners for `block` on `ring`.
    ///
    /// [`Ring::Block`] hashes the whole [`BlockId`], spreading a large object's
    /// ranges across the fleet. [`Ring::Async`] hashes the object identity
    /// alone against a disjoint node set, so every range of one object resolves
    /// to the same worker and stays there across a republish.
    ///
    /// A lookup never crosses rings. An async lookup against a cluster with no
    /// async workers answers with no owners rather than substituting a block
    /// worker, which holds no extents and would refuse the read a round trip
    /// later.
    pub fn lookup_on(&self, ring: Ring, block: &BlockId, k: usize) -> PlacementResult {
        // The strategy and the node set move together — that pairing *is* the
        // ring, and splitting it is how a lookup would come to cross pools.
        match ring {
            Ring::Block => self.resolve(&self.placement, ring, block, k),
            Ring::Async => self.resolve(&self.async_placement, ring, block, k),
            // `Ring` is `#[non_exhaustive]`: a ring this coordinator predates
            // must answer "nobody" rather than fall through to the block pool.
            _ => PlacementResult {
                owners: Vec::new(),
                epoch: self.membership.epoch(),
            },
        }
    }

    fn resolve<R: Placement>(
        &self,
        strategy: &R,
        ring: Ring,
        block: &BlockId,
        k: usize,
    ) -> PlacementResult {
        // Read the epoch first, then the nodes; membership only advances the
        // epoch, so a concurrent change can only make the epoch we return
        // conservatively old, prompting a harmless client refresh.
        //
        // The epoch covers the *whole* membership, not just this ring: a
        // client caches one epoch and compares it for equality, so scoping it
        // per ring would leave clients of different rings disagreeing about
        // which epoch is current. The cost is a spurious refresh when the
        // other ring changes, which is exactly the harmless case above.
        let epoch = self.membership.epoch();
        let nodes = self.membership.snapshot_for_role(ring.role());
        let owners = strategy
            .locate_top_k(block, &nodes, k)
            .into_iter()
            .map(|n| n.0)
            .collect();
        PlacementResult { owners, epoch }
    }

    /// Handle a transport [`ControlMessage`].
    ///
    /// Answers [`ControlMessage::PlacementLookup`] and
    /// [`ControlMessage::RingPlacementLookup`] with a
    /// [`ControlMessage::PlacementResponse`]; any other message yields an
    /// [`ControlMessage::Ack`] with `ok: false` describing the mismatch.
    pub fn handle(&self, msg: ControlMessage) -> ControlMessage {
        match msg {
            // This message predates the async worker and has always meant the
            // block ring. Keeping that mapping is what makes a pre-schema-4
            // client see no behaviour change.
            ControlMessage::PlacementLookup { block, k } => {
                response(self.lookup_on(Ring::Block, &block, k as usize))
            }
            ControlMessage::RingPlacementLookup { block, ring, k } => {
                response(self.lookup_on(ring, &block, k as usize))
            }
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

    /// A lookup must never cross pools.
    ///
    /// This is the guarantee the role split exists to provide. Without it an
    /// extent lookup can return a block worker, which holds no extents, and
    /// the mistake surfaces as a failed read one round trip later instead of
    /// as an empty owner list here.
    #[test]
    fn a_lookup_never_crosses_rings() {
        let m = Membership::new();
        for id in ["blk-a", "blk-b"] {
            m.register(NodeInfo {
                id: NodeId::new(id),
                address: format!("{id}:7001"),
                role: NodeRole::Worker,
            });
        }
        for id in ["ext-a", "ext-b"] {
            m.register(NodeInfo {
                id: NodeId::new(id),
                address: format!("{id}:7001"),
                role: NodeRole::AsyncWorker,
            });
        }
        let s = PlacementService::new(m, RendezvousPlacement);

        for i in 0..50 {
            let blocks = s.lookup(&block(i), 2).owners;
            assert!(
                blocks.iter().all(|o| o.starts_with("blk-")),
                "block lookup returned an async worker: {blocks:?}"
            );
            let extents = s.lookup_on(Ring::Async, &block(i), 2).owners;
            assert!(
                extents.iter().all(|o| o.starts_with("ext-")),
                "async lookup returned a block worker: {extents:?}"
            );
        }
    }

    /// A cluster with no async workers answers "nobody", not "a block worker".
    #[test]
    fn an_async_lookup_against_a_block_only_cluster_has_no_owners() {
        let s = svc(&["a", "b", "c"]);
        let res = s.lookup_on(Ring::Async, &block(1), 2);
        assert!(res.owners.is_empty());
        // The epoch still reflects the real membership, so the client caches a
        // valid epoch and refreshes when async workers actually arrive.
        assert_ne!(res.epoch, Epoch::EMPTY);
    }

    /// The wire path: an unclassed lookup still means the block pool.
    #[test]
    fn each_lookup_message_resolves_against_its_own_ring() {
        let m = Membership::new();
        m.register(NodeInfo {
            id: NodeId::new("blk-a"),
            address: "blk-a:7001".into(),
            role: NodeRole::Worker,
        });
        m.register(NodeInfo {
            id: NodeId::new("ext-a"),
            address: "ext-a:7001".into(),
            role: NodeRole::AsyncWorker,
        });
        let s = PlacementService::new(m, RendezvousPlacement);

        match s.handle(ControlMessage::PlacementLookup {
            block: block(3),
            k: 2,
        }) {
            ControlMessage::PlacementResponse { owners, .. } => {
                assert_eq!(owners, vec![NodeId::new("blk-a")]);
            }
            other => panic!("expected PlacementResponse, got {other:?}"),
        }

        for ring in [Ring::Block, Ring::Async] {
            let expected = match ring {
                Ring::Async => NodeId::new("ext-a"),
                _ => NodeId::new("blk-a"),
            };
            match s.handle(ControlMessage::RingPlacementLookup {
                block: block(3),
                ring,
                k: 2,
            }) {
                ControlMessage::PlacementResponse { owners, .. } => {
                    assert_eq!(owners, vec![expected], "{ring} ring");
                }
                other => panic!("expected PlacementResponse, got {other:?}"),
            }
        }
    }

    /// The explicit block ring and the legacy unqualified lookup must be the
    /// same answer, or a client that upgrades to the ring-aware message would
    /// silently start reading from different workers.
    #[test]
    fn naming_the_block_ring_matches_the_legacy_lookup() {
        let s = svc(&["a", "b", "c", "d"]);
        for i in 0..50 {
            assert_eq!(
                s.handle(ControlMessage::PlacementLookup {
                    block: block(i),
                    k: 2
                }),
                s.handle(ControlMessage::RingPlacementLookup {
                    block: block(i),
                    ring: Ring::Block,
                    k: 2
                }),
            );
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
