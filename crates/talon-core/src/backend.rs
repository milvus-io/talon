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
use std::path::Path;

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

    /// Fetch a byte range, optionally guarded by an `If-Match` precondition.
    ///
    /// When `if_match` is `Some`, the fetch is conditioned on the object still
    /// being at that version (S3/Azure `If-Match`, GCS `x-goog-if-generation-match`),
    /// so a source overwrite between the version resolution and the GET is
    /// rejected with [`Error::VersionMismatch`](crate::Error::VersionMismatch)
    /// instead of silently committing
    /// the newer bytes under the older version's key (the HEAD→GET TOCTOU,
    /// issue #163). The worker keys cache blocks by the resolved version, so a
    /// precondition failure means the caller must re-resolve and refetch.
    ///
    /// The default implementation ignores the precondition and delegates to
    /// [`fetch_range`](Self::fetch_range); real backends override it to carry the
    /// precondition into the request. This keeps in-memory/test backends that
    /// have no notion of preconditions working unchanged.
    async fn fetch_range_if_match(
        &self,
        obj: &ObjectId,
        offset: u64,
        len: u64,
        if_match: Option<&Version>,
    ) -> Result<Bytes> {
        let _ = if_match;
        self.fetch_range(obj, offset, len).await
    }

    /// Fetch object metadata (size + version) without transferring data.
    async fn head(&self, obj: &ObjectId) -> Result<ObjectStat>;

    /// Upload the whole object, replacing any existing one.
    ///
    /// Returns the version/etag the store assigns to the newly-written object, so
    /// the caller (the write-through worker) can cache the bytes under the same
    /// version the read path would resolve via [`head`](Self::head), keeping
    /// read-after-write consistent with the #163 versioning.
    ///
    /// The default implementation refuses with [`Error::Backend`](crate::Error::Backend); real backends
    /// (S3/GCS/Azure) override it. This keeps in-memory/test backends that only
    /// serve reads compiling unchanged (write support, #226/#227).
    async fn put(&self, obj: &ObjectId, body: Bytes) -> Result<Version> {
        let _ = body;
        Err(crate::Error::Backend(format!(
            "backend does not support PUT for {}",
            obj.to_path()
        )))
    }

    /// Upload the whole object, optionally guarded by an `If-Match` precondition.
    ///
    /// When `if_match` is `Some`, the write is conditioned on the object still
    /// being at that version (S3/Azure `If-Match`, GCS `x-goog-if-generation-match`),
    /// so a concurrent overwrite between the client's read and this write is
    /// rejected with [`Error::VersionMismatch`](crate::Error::VersionMismatch)
    /// rather than clobbering newer bytes (the write analogue of
    /// [`fetch_range_if_match`](Self::fetch_range_if_match)).
    ///
    /// The default implementation ignores the precondition and delegates to
    /// [`put`](Self::put).
    async fn put_if_match(
        &self,
        obj: &ObjectId,
        body: Bytes,
        if_match: Option<&Version>,
    ) -> Result<Version> {
        let _ = if_match;
        self.put(obj, body).await
    }

    /// Upload a file as a whole object without materializing it in memory.
    ///
    /// `len` is the exact number of bytes to read from `path`. Real backends
    /// override this with a streaming request. The default returns an explicit
    /// error so unsupported backends cannot silently allocate the whole file.
    async fn put_file(&self, obj: &ObjectId, path: &Path, len: u64) -> Result<Version> {
        let _ = (path, len);
        Err(crate::Error::Backend(format!(
            "backend does not support streamed PUT for {}",
            obj.to_path()
        )))
    }

    /// Delete the object. Idempotent: a missing object is `Ok(())`.
    ///
    /// The default implementation refuses with [`Error::Backend`](crate::Error::Backend); real backends
    /// override it.
    async fn delete(&self, obj: &ObjectId) -> Result<()> {
        Err(crate::Error::Backend(format!(
            "backend does not support DELETE for {}",
            obj.to_path()
        )))
    }
}
