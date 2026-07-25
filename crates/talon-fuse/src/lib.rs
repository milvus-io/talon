//! # talon-fuse
//!
//! A FUSE filesystem client that exposes the Talon distributed cache as a
//! mountable POSIX filesystem. Reads and writes to the mount are translated
//! into object store operations against the cluster.

pub mod block_reader;
pub mod coordinator_client;
pub mod mapping;
pub mod metrics;
#[cfg(feature = "mount")]
pub mod mount;
pub mod ops;
pub mod placement_cache;
pub mod pool;
pub mod prefetch;
pub mod read_plan;
pub mod readahead;
pub mod worker_client;

pub use block_reader::{BlockReadError, BlockReader, FileView};
pub use coordinator_client::{
    CoordinatorClient, CoordinatorError, ObjectStat, Placement, ResolvedPlacement,
};
pub use mapping::{path_to_object, resolve_read, ReadTarget};
pub use metrics::{ReadStats, ReadStatsSnapshot};
#[cfg(feature = "mount")]
pub use mount::{TalonFuse, CANONICAL_MOUNT_VERSION};
pub use ops::{Attr, DirEntry, FileKind, FsError, ReadOnlyFs, ROOT_INO};
pub use placement_cache::{Cached, PlacementCache, RefreshReason};
pub use pool::ConnectionPool;
pub use prefetch::Prefetcher;
pub use read_plan::{plan_read, BlockSegment};
pub use readahead::{ReadaheadConfig, ReadaheadState};
pub use worker_client::{WorkerClient, WorkerError};
