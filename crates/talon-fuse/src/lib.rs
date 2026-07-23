//! # talon-fuse
//!
//! A FUSE filesystem client that exposes the Talon distributed cache as a
//! mountable POSIX filesystem. Reads and writes to the mount are translated
//! into object store operations against the cluster.

pub mod block_reader;
pub mod bridge;
pub mod coordinator_client;
pub mod fs;
pub mod mapping;
pub mod ops;
pub mod placement_cache;
pub mod prefetch;
pub mod read_plan;
pub mod readahead;
pub mod worker_client;

pub use block_reader::{BlockReadError, BlockReader, FileView};
pub use bridge::{spawn_bridge, BridgeClient, BridgeError};
pub use coordinator_client::{CoordinatorClient, CoordinatorError, Placement, ResolvedPlacement};
pub use fs::TalonFs;
pub use mapping::{object_to_path, path_to_object, resolve_read, ReadTarget};
pub use ops::{Attr, DirEntry, FileKind, FsError, ReadOnlyFs, ROOT_INO};
pub use placement_cache::{Cached, PlacementCache, RefreshReason};
pub use prefetch::Prefetcher;
pub use read_plan::{plan_read, BlockSegment};
pub use readahead::{ReadaheadConfig, ReadaheadState};
pub use worker_client::{WorkerClient, WorkerError};
