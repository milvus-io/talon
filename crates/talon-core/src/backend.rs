//! The backend (origin) store abstraction.
//!
//! [`BackendStore`] is the durable source a worker fetches from on a cache miss:
//! S3, GCS, or Azure Blob. It is deliberately separate from
//! [`ObjectStore`](crate::ObjectStore) (the local cache): different lifecycle,
//! different failure modes, and it is driven off the data-plane ring by the
//! loader thread pool (see `DESIGN.md`).

use crate::{ObjectId, Result, Version};
use async_trait::async_trait;
use bytes::Bytes;

/// Metadata about a source object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectStat {
    /// Total size of the object in bytes.
    pub len: u64,
    /// Current version/etag of the object.
    pub version: Version,
}

/// A durable blob backend that workers load blocks/pages from on cache miss.
#[async_trait]
pub trait BackendStore: Send + Sync {
    /// Fetch a byte range `[offset, offset + len)` of a source object.
    ///
    /// Used for both whole-block and page-level loads; the caller chooses the
    /// range. Bytes land in a `Vec`/`Bytes` (unlike the cache read path), so a
    /// checksum can be computed here.
    async fn fetch_range(&self, obj: &ObjectId, offset: u64, len: u64) -> Result<Bytes>;

    /// Fetch object metadata (size + version) without transferring data.
    async fn head(&self, obj: &ObjectId) -> Result<ObjectStat>;
}
