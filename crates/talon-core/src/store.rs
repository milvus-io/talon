//! The core object store abstraction.

use crate::{CacheKey, Result};
use async_trait::async_trait;
use bytes::Bytes;

/// An asynchronous key/value object store.
///
/// Implementations back the Talon cache; the worker provides the primary
/// implementation while the coordinator routes requests to workers.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Fetch an object by key. Returns [`Error::NotFound`](crate::Error::NotFound)
    /// if the object is absent.
    async fn get(&self, key: &CacheKey) -> Result<Bytes>;

    /// Insert or overwrite an object.
    async fn put(&self, key: &CacheKey, value: Bytes) -> Result<()>;

    /// Remove an object. Succeeds even if the key is absent.
    async fn delete(&self, key: &CacheKey) -> Result<()>;

    /// Return whether an object exists.
    async fn contains(&self, key: &CacheKey) -> Result<bool> {
        match self.get(key).await {
            Ok(_) => Ok(true),
            Err(crate::Error::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }
}
