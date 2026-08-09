//! Reusable Talon cache data-plane client.
//!
//! This crate owns coordinator discovery, local placement, pooled worker
//! connections, range planning, and read/write exchanges. Protocol frontends
//! use it without depending on a particular filesystem or HTTP adapter.

pub mod block_reader;
pub mod coordinator_client;
mod lock;
pub mod membership_cache;
pub mod metrics;
pub mod placement_cache;
pub mod pool;
pub mod read_plan;
pub mod worker_client;

pub use block_reader::{BlockReadError, BlockReader, FileView};
pub use coordinator_client::{
    CoordinatorClient, CoordinatorError, ObjectStat, Placement, ResolvedPlacement,
};
pub use metrics::{ReadStats, ReadStatsSnapshot};
pub use placement_cache::{Cached, PlacementCache, RefreshReason};
pub use pool::{ConnectionPool, DEFAULT_CONNECT_TIMEOUT, DEFAULT_REQUEST_TIMEOUT};
pub use read_plan::{plan_read, BlockSegment};
pub use worker_client::{WorkerClient, WorkerError, WriteClient};
