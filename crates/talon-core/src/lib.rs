//! # talon-core
//!
//! Shared types, traits, and protocol definitions for the Talon distributed
//! object store cache. All other Talon crates depend on this crate.

pub mod error;
pub mod key;
pub mod node;
pub mod store;

pub use error::{Error, Result};
pub use key::CacheKey;
pub use node::{NodeId, NodeInfo, NodeRole};
pub use store::ObjectStore;
