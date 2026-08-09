//! # talon-fuse
//!
//! A FUSE filesystem client that exposes the Talon distributed cache as a
//! mountable POSIX filesystem. Reads and writes to the mount are translated
//! into object store operations against the cluster.

/// Compatibility exports for the shared cache client read path.
pub mod block_reader {
    pub use talon_cache_client::block_reader::*;
}
pub mod capability;
/// Compatibility exports for the shared coordinator client.
pub mod coordinator_client {
    pub use talon_cache_client::coordinator_client::*;
}
pub(crate) mod lock;
pub mod mapping;
/// Compatibility exports for the shared membership cache.
pub mod membership_cache {
    pub use talon_cache_client::membership_cache::*;
}
/// Compatibility exports for shared cache-client metrics.
pub mod metrics {
    pub use talon_cache_client::metrics::*;
}
#[cfg(feature = "mount")]
pub mod mount;
pub mod ops;
/// Compatibility exports for the shared placement cache.
pub mod placement_cache {
    pub use talon_cache_client::placement_cache::*;
}
/// Compatibility exports for the shared connection pool.
pub mod pool {
    pub use talon_cache_client::pool::*;
}
pub mod prefetch;
/// Compatibility exports for shared range planning.
pub mod read_plan {
    pub use talon_cache_client::read_plan::*;
}
pub mod readahead;
/// Compatibility exports for shared worker clients.
pub mod worker_client {
    pub use talon_cache_client::worker_client::*;
}

pub use block_reader::{BlockReadError, BlockReader, FileView};
pub use coordinator_client::{
    CoordinatorClient, CoordinatorError, ObjectStat, Placement, ResolvedPlacement,
};
pub use mapping::{path_to_object, resolve_read, ReadTarget};
pub use metrics::{ReadStats, ReadStatsSnapshot};
#[cfg(feature = "mount")]
pub use mount::{TalonFuse, CANONICAL_MOUNT_VERSION};
pub use ops::{
    Attr, DirEntry, FileKind, FsError, ReadOnlyFs, WritebackSource,
    DEFAULT_MAX_LOGICAL_OBJECT_BYTES, DEFAULT_MAX_OBJECT_BYTES, ROOT_INO,
};
pub use placement_cache::{Cached, PlacementCache, RefreshReason};
pub use pool::{ConnectionPool, DEFAULT_CONNECT_TIMEOUT, DEFAULT_REQUEST_TIMEOUT};
pub use prefetch::Prefetcher;
pub use read_plan::{plan_read, BlockSegment};
pub use readahead::{ReadaheadConfig, ReadaheadState};
pub use worker_client::{WorkerClient, WorkerError, WriteClient};
