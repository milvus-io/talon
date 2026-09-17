//! # talon-worker
//!
//! A worker node stores cached object data and serves it to clients. It
//! provides an in-memory [`ObjectStore`](talon_core::ObjectStore)
//! implementation, with room to add tiered/persistent backends later.

pub mod block_store;
pub mod capacity;
pub mod connection_admission;
mod data_error;
pub mod eviction;
mod fd_cache;
pub mod flusher;
pub mod index;
pub mod loader;
pub mod mapping_guard;
pub mod memory_store;
pub mod miss;
pub mod observability;
pub mod page_access_store;
mod page_cleanup;
pub mod page_gc;
mod page_lifecycle;
pub mod paged_store;
pub mod rate_limit;
pub mod runtime;
pub mod sendfile;
#[cfg(target_os = "linux")]
pub mod splice;
pub mod staging;
pub mod tokio_conn;
#[cfg(target_os = "linux")]
pub mod uring_conn;
#[cfg(target_os = "linux")]
pub mod uring_serve;
pub mod wal;
pub mod wal_checkpoint;
pub mod wal_commit;
pub mod write_cache;

pub use block_store::WholeBlockStore;
pub use capacity::{CacheDirConfig, CacheDirs};
pub use connection_admission::ConnectionAdmission;
pub use eviction::{CacheUnit, Lru};
pub use flusher::{FlushOutcome, FlushPolicy, FlushStats, Flusher};
pub use index::{BlockIndex, Presence};
pub use loader::{LoadOutcome, LoadTask, LoaderPool};
pub use memory_store::{MemoryInsert, MemoryPageKey, MemoryStore};
pub use miss::{touched_pages, Admission, InFlightGuard, InFlightLoads, LoadKey};
pub use observability::{serve_admin, WorkerMetrics, WorkerObservability, WorkerReadiness};
pub use paged_store::PagedBlockStore;
pub use rate_limit::{TenantRateLimiter, Throttled};
pub use runtime::{ServeOutcome, WorkerRuntime};
pub use sendfile::{
    send_file_range, send_header_and_file_range, send_header_and_file_ranges, DEFAULT_CHUNK,
};
#[cfg(target_os = "linux")]
pub use splice::{ingest_put, splice_to_file};
pub use staging::{Checksum, Stager};
pub use write_cache::{FlushItem, WriteCache};
