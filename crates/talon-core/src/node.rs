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
///
/// Variants are appended, never reordered: the discriminant is serialized in
/// [`NodeInfo`] and read by the Java and Python clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    /// Coordinates metadata and object placement.
    Coordinator,
    /// Stores cached object data as fixed-size blocks.
    Worker,
    /// Serves reads from an extent cache, caching exactly the byte ranges
    /// asked for rather than whole blocks.
    ///
    /// Placed on its own rendezvous ring, keyed on the object identity alone,
    /// and kept out of the block ring entirely. Read-only: it refuses writes,
    /// so a coordinator must not hand it to a client that intends to write.
    /// See ADR 0005.
    AsyncWorker,
}

impl NodeRole {
    /// True for the data-plane roles — the ones that hold backend credentials
    /// and serve reads.
    ///
    /// Use this wherever the question is "is this a data node?" rather than
    /// "which pool does it belong to?". An `== NodeRole::Worker` comparison
    /// answers the second question while looking like it answers the first,
    /// which is how an async worker silently disappears from a membership
    /// feed.
    pub fn is_worker(self) -> bool {
        matches!(self, NodeRole::Worker | NodeRole::AsyncWorker)
    }

    /// The Prometheus/JSON label for this role.
    pub fn as_str(self) -> &'static str {
        match self {
            NodeRole::Coordinator => "coordinator",
            NodeRole::Worker => "worker",
            NodeRole::AsyncWorker => "async_worker",
        }
    }
}

impl fmt::Display for NodeRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_worker_roles_are_data_nodes() {
        assert!(NodeRole::Worker.is_worker());
        assert!(NodeRole::AsyncWorker.is_worker());
        assert!(!NodeRole::Coordinator.is_worker());
    }

    #[test]
    fn role_labels_are_distinct_and_stable() {
        // These strings land in Prometheus label values and management JSON;
        // renaming one silently breaks every dashboard built on it.
        assert_eq!(NodeRole::Coordinator.as_str(), "coordinator");
        assert_eq!(NodeRole::Worker.as_str(), "worker");
        assert_eq!(NodeRole::AsyncWorker.as_str(), "async_worker");
    }
}
