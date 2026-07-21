//! Cluster node identity and metadata.

use serde::{Deserialize, Serialize};
use std::fmt;

/// A unique identifier for a node in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

impl NodeId {
    /// Create a new node id.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The role a node plays in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    /// Coordinates metadata and object placement.
    Coordinator,
    /// Stores cached object data.
    Worker,
}

/// Metadata describing a cluster node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Unique identifier of the node.
    pub id: NodeId,
    /// Network address (host:port) of the node.
    pub address: String,
    /// The role this node plays.
    pub role: NodeRole,
}
