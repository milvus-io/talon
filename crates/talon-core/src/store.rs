//! The cache-side object store abstraction.
//!
//! [`ObjectStore`] is the worker's local cache surface. Hot reads return a
//! [`BlockHandle`] — an fd plus offset/len — so the transport layer can serve
//! bytes with `sendfile` without copying them through userspace. A small
//! `get_bytes` path exists for the optional in-memory (L1) tier.

use crate::{BlockId, PageIndex, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::os::fd::OwnedFd;

/// A zero-copy read source: a file descriptor plus the byte range to serve.
///
/// The transport layer performs `sendfile(fd, socket, offset, len)`. Keeping
/// this to `std` fd types means `talon-core` carries no transport or syscall
/// dependency.
#[derive(Debug)]
pub struct BlockHandle {
    /// File descriptor backing the cached block or page.
    pub fd: OwnedFd,
    /// Byte offset within the file at which the requested data starts.
    pub offset: u64,
    /// Number of bytes to serve from `offset`.
    pub len: u64,
}

impl BlockHandle {
    /// Create a new handle.
    pub fn new(fd: OwnedFd, offset: u64, len: u64) -> Self {
        Self { fd, offset, len }
    }
}

/// An asynchronous, block-addressed local cache store.
///
/// Reads are keyed by [`BlockId`]. Absent whole blocks or pages surface
/// [`Error::NotFound`](crate::Error::NotFound); callers translate that into a
/// block- or page-level miss and trigger a backend load.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Fetch a whole block as a zero-copy handle.
    async fn get_block(&self, id: &BlockId) -> Result<BlockHandle>;

    /// Fetch a single page of a paged block as a zero-copy handle.
    async fn get_page(&self, id: &BlockId, page: PageIndex) -> Result<BlockHandle>;

    /// Fetch a sub-range of a block as one or more zero-copy handles.
    ///
    /// For a whole block this is a single handle over `[offset, offset + len)`.
    /// For a paged block this returns one handle per covered present page
    /// (contiguous present pages may be coalesced). If any covered page is
    /// absent, returns [`Error::NotFound`](crate::Error::NotFound) so the caller
    /// can perform a page-level load.
    async fn get_range(&self, id: &BlockId, offset: u64, len: u64) -> Result<Vec<BlockHandle>>;

    /// Fetch a small object's bytes directly (optional L1 memory path).
    async fn get_bytes(&self, id: &BlockId) -> Result<Bytes>;

    /// Insert or overwrite a block's bytes.
    async fn put(&self, id: &BlockId, value: Bytes) -> Result<()>;

    /// Remove a block. Succeeds even if absent.
    async fn delete(&self, id: &BlockId) -> Result<()>;

    /// Whether a whole block is present in the cache.
    async fn contains(&self, id: &BlockId) -> Result<bool> {
        match self.get_bytes(id).await {
            Ok(_) => Ok(true),
            Err(crate::Error::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }
}
