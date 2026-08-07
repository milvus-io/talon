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
    /// Lives in its own cluster, never alongside a [`NodeRole::Worker`]: the
    /// ring is a property of the cluster and the two rings are incompatible.
    /// Read-only — it refuses writes, so a write must not be routed to a
    /// cluster made of these. See ADR 0005 and ADR 0006.
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

/// Which kind of cache a cluster is, and therefore which ring it places on.
///
/// A cluster runs exactly one rendezvous ring over exactly one worker role. The
/// two rings hash different keys against different node sets and cannot
/// meaningfully coexist: a block worker holds no extents and an async worker
/// holds no blocks, so a lookup answered from the wrong pool fails a round trip
/// later at read time.
///
/// This used to be a per-*request* choice, carried as a `Ring` on the wire and
/// selected by the client. Making it a property of the cluster moves the
/// decision to the only party that can be sure of it, and turns "wrong pool"
/// from an empty owner list into a refused registration. See ADR 0006.
///
/// A node's [`NodeRole`] and a cluster's type are different questions —
/// coordinators belong to a cluster of either type — so this is a separate
/// enum rather than a reuse of `NodeRole`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterType {
    /// Fixed-size block cache, hashed on the whole `BlockId` so consecutive
    /// ranges of one object spread across the fleet. The default, and the only
    /// kind that existed before ADR 0005.
    #[default]
    Block,
    /// Extent cache, hashed on the object identity alone so every range of one
    /// object resolves to the same worker. Read-only.
    Async,
}

impl ClusterType {
    /// The one worker role this cluster admits.
    ///
    /// A node reporting any other worker role is refused at registration
    /// rather than filtered out at lookup, which is the difference between a
    /// boundary and a convention.
    pub fn worker_role(self) -> NodeRole {
        match self {
            ClusterType::Block => NodeRole::Worker,
            ClusterType::Async => NodeRole::AsyncWorker,
        }
    }

    /// Whether a node of `role` belongs in this cluster.
    ///
    /// Coordinators belong to both — the type constrains the *worker* pool.
    pub fn admits(self, role: NodeRole) -> bool {
        !role.is_worker() || role == self.worker_role()
    }

    /// The Prometheus/JSON/config label for this cluster type.
    pub fn as_str(self) -> &'static str {
        match self {
            ClusterType::Block => "block",
            ClusterType::Async => "async",
        }
    }
}

impl fmt::Display for ClusterType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ClusterType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "block" => Ok(ClusterType::Block),
            "async" => Ok(ClusterType::Async),
            other => Err(format!(
                "unknown cluster type {other:?}; expected \"block\" or \"async\""
            )),
        }
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

    #[test]
    fn cluster_type_labels_are_distinct_and_stable() {
        // Same contract as the role labels, plus one more: these are also
        // config values, so renaming one breaks deployed TOML and env.
        assert_eq!(ClusterType::Block.as_str(), "block");
        assert_eq!(ClusterType::Async.as_str(), "async");
    }

    #[test]
    fn each_cluster_type_admits_exactly_one_worker_role() {
        assert_eq!(ClusterType::Block.worker_role(), NodeRole::Worker);
        assert_eq!(ClusterType::Async.worker_role(), NodeRole::AsyncWorker);

        assert!(ClusterType::Block.admits(NodeRole::Worker));
        assert!(!ClusterType::Block.admits(NodeRole::AsyncWorker));
        assert!(ClusterType::Async.admits(NodeRole::AsyncWorker));
        assert!(!ClusterType::Async.admits(NodeRole::Worker));
    }

    #[test]
    fn a_coordinator_belongs_to_a_cluster_of_either_type() {
        // The type constrains the worker pool, not the control plane.
        assert!(ClusterType::Block.admits(NodeRole::Coordinator));
        assert!(ClusterType::Async.admits(NodeRole::Coordinator));
    }

    #[test]
    fn cluster_type_parses_from_its_own_label() {
        // Round-trips config: whatever `as_str` writes, `from_str` must read.
        for t in [ClusterType::Block, ClusterType::Async] {
            assert_eq!(t.as_str().parse::<ClusterType>(), Ok(t));
        }
        assert_eq!("  ASYNC ".parse::<ClusterType>(), Ok(ClusterType::Async));
    }

    #[test]
    fn an_unknown_cluster_type_names_the_valid_ones() {
        // A typo in config must say what was expected, not just fail.
        let err = "extent".parse::<ClusterType>().unwrap_err();
        assert!(err.contains("block"), "{err}");
        assert!(err.contains("async"), "{err}");
    }

    #[test]
    fn the_default_cluster_type_is_block() {
        // Matches CoordinatorConfig's default, so an existing deployment that
        // sets nothing keeps the behaviour it already had.
        assert_eq!(ClusterType::default(), ClusterType::Block);
    }
}
