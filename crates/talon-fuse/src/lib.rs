//! # talon-fuse
//!
//! A FUSE filesystem client that exposes the Talon distributed cache as a
//! mountable POSIX filesystem. Reads and writes to the mount are translated
//! into object store operations against the cluster.

pub mod fs;

pub use fs::TalonFs;
