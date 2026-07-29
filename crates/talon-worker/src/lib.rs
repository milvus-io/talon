//! # talon-worker
//!
//! A worker node stores cached object data and serves it to clients. It
//! provides an in-memory [`ObjectStore`](talon_core::ObjectStore)
//! implementation, with room to add tiered/persistent backends later.

pub mod block_store;
pub mod capacity;
pub mod eviction;
pub mod flusher;
pub mod index;
pub mod loader;
pub mod mapping_guard;
pub mod memory_store;
pub mod miss;
pub mod observability;
pub mod paged_store;
pub mod runtime;
pub mod sendfile;
pub mod splice;
pub mod staging;
pub mod tokio_conn;
pub mod uring_conn;
pub mod uring_serve;
pub mod wal;
pub mod write_cache;

pub use block_store::WholeBlockStore;
pub use capacity::{CacheDirConfig, CacheDirs};
pub use eviction::{CacheUnit, Lru};
pub use flusher::{FlushOutcome, FlushPolicy, FlushStats, Flusher};
pub use index::{BlockIndex, Presence};
pub use loader::{LoadOutcome, LoadTask, LoaderPool};
pub use memory_store::{MemoryInsert, MemoryPageKey, MemoryStore};
pub use miss::{touched_pages, Admission, InFlightGuard, InFlightLoads, LoadKey};
pub use observability::{serve_admin, WorkerMetrics, WorkerObservability, WorkerReadiness};
pub use paged_store::PagedBlockStore;
pub use runtime::{ServeOutcome, WorkerRuntime};
pub use sendfile::{send_file_range, DEFAULT_CHUNK};
pub use splice::{ingest_put, splice_to_file};
pub use staging::{Checksum, Stager};
pub use write_cache::{FlushItem, WriteCache};
