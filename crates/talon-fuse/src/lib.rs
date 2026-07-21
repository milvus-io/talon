//! # talon-fuse
//!
//! A FUSE filesystem client that exposes the Talon distributed cache as a
//! mountable POSIX filesystem. Reads and writes to the mount are translated
//! into object store operations against the cluster.

pub mod fs;
pub mod mapping;
pub mod placement_cache;

pub use fs::TalonFs;
pub use mapping::{object_to_path, path_to_object, resolve_read, ReadTarget};
pub use placement_cache::{Cached, PlacementCache, RefreshReason};
