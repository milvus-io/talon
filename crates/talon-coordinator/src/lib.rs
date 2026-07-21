//! # talon-coordinator
//!
//! The coordinator tracks cluster membership and decides which worker holds
//! each object. It exposes a routing layer that clients use to locate data.

pub mod heartbeat;
pub mod load;
pub mod membership;
pub mod placement;
pub mod service;

pub use heartbeat::{HeartbeatConfig, HeartbeatTracker, Inventory};
pub use load::{plan_load, split_into_blocks, LoadAssignment, LoadProgress};
pub use membership::{K8sSelector, KubernetesMembership, Membership, MembershipSource};
pub use placement::{Epoch, Placement, RendezvousPlacement};
pub use service::{PlacementResult, PlacementService};
