//! # talon-core
//!
//! Shared types, traits, and protocol definitions for the Talon distributed
//! object store cache. All other Talon crates depend on this crate.

pub mod backend;
pub mod block;
pub mod config;
pub mod error;
pub mod key;
pub mod metrics;
pub mod node;
pub mod store;
pub mod trace;

pub use backend::{BackendStore, ObjectStat};
pub use block::{BlockForm, BlockMeta, LoadHint, PresentBitmap};
pub use config::{Patch, WorkerConfig, WorkerConfigPatch};
pub use error::{Error, Result};
pub use key::{Backend, BlockId, ObjectId, PageIndex, Version};
pub use metrics::{Counter, Gauge, Histogram, Metrics};
pub use node::{NodeId, NodeInfo, NodeRole};
pub use store::{BlockHandle, ObjectStore};
pub use trace::{init_tracing, RequestId};
