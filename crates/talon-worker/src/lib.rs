//! # talon-worker
//!
//! A worker node stores cached object data and serves it to clients. It
//! provides an in-memory [`ObjectStore`](talon_core::ObjectStore)
//! implementation, with room to add tiered/persistent backends later.

pub mod block_store;
pub mod eviction;
pub mod index;
pub mod memory_store;
pub mod paged_store;

pub use block_store::WholeBlockStore;
pub use eviction::{CacheUnit, Lru};
pub use index::{BlockIndex, Presence};
pub use memory_store::MemoryStore;
pub use paged_store::PagedBlockStore;
