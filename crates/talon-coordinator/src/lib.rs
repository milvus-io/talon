//! # talon-coordinator
//!
//! The coordinator tracks cluster membership and decides which worker holds
//! each object. It exposes a routing layer that clients use to locate data.

pub mod config;
pub mod heartbeat;
pub mod load;
pub mod membership;
pub mod observability;
pub mod placement;
pub mod service;
pub mod state_store;

pub use config::{CoordinatorConfig, CoordinatorConfigPatch};
pub use heartbeat::{HeartbeatConfig, HeartbeatTracker, Inventory};
pub use load::{plan_load, split_into_blocks, LoadAssignment, LoadProgress};
pub use membership::{K8sSelector, KubernetesMembership, Membership, MembershipSource};
pub use observability::{
    serve_admin as serve_coordinator_admin, ControlOperation, CoordinatorMetrics,
    CoordinatorObservability,
};
pub use placement::{Epoch, Placement, RendezvousPlacement};
pub use service::{PlacementResult, PlacementService};
pub use state_store::{
    BackendHealth, ClusterSnapshot, ClusterStateConfig, ClusterStateStore, ClusterStateWatch,
    ConfigError as ClusterStateConfigError, MemoryStateStore, NodeEvent, NodeEventKind,
    StateBackend, StateStoreError, StateStoreResult, StoreRevision, TimeSource, WriteDisposition,
    WriteResult,
};
