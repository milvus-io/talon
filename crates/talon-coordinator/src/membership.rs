//! Cluster membership tracking.

use std::collections::HashMap;
use std::sync::RwLock;
use talon_core::{NodeId, NodeInfo};

/// An in-memory registry of known cluster nodes.
#[derive(Default)]
pub struct Membership {
    nodes: RwLock<HashMap<NodeId, NodeInfo>>,
}

impl Membership {
    /// Create an empty membership registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register or update a node.
    pub fn register(&self, info: NodeInfo) {
        self.nodes.write().unwrap().insert(info.id.clone(), info);
    }

    /// Remove a node from the registry.
    pub fn remove(&self, id: &NodeId) {
        self.nodes.write().unwrap().remove(id);
    }

    /// Return a snapshot of all currently known nodes.
    pub fn snapshot(&self) -> Vec<NodeInfo> {
        self.nodes.read().unwrap().values().cloned().collect()
    }
}
