//! Object placement strategy.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use talon_core::{BlockId, NodeId, NodeInfo};

/// Decides which node should hold a given block.
pub trait Placement {
    /// Return the node responsible for `block`, given the current node set.
    fn locate(&self, block: &BlockId, nodes: &[NodeInfo]) -> Option<NodeId>;
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
}
