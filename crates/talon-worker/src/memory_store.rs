//! A simple in-memory [`ObjectStore`] implementation.
//!
//! This backs the optional L1 memory path and tests. It fully implements the
//! byte-oriented methods (`get_bytes`/`put`/`delete`/`contains`); the zero-copy
//! fd methods (`get_block`/`get_page`/`get_range`) are wired in the dedicated
//! worker-store PR and currently report the block/page as absent.

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::RwLock;
use talon_core::{BlockHandle, BlockId, Error, ObjectStore, PageIndex, Result};

/// An in-memory object store backed by a hash map.
#[derive(Default)]
pub struct MemoryStore {
    inner: RwLock<HashMap<BlockId, Bytes>>,
}

impl MemoryStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the number of stored blocks.
    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    /// Return whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl ObjectStore for MemoryStore {
    async fn get_block(&self, id: &BlockId) -> Result<BlockHandle> {
        // Zero-copy fd path is provided by the NVMe-backed worker store.
        Err(Error::NotFound(id.to_string()))
    }

    async fn get_page(&self, id: &BlockId, page: PageIndex) -> Result<BlockHandle> {
        Err(Error::NotFound(format!("{id} page {}", page.0)))
    }

    async fn get_range(&self, id: &BlockId, _offset: u64, _len: u64) -> Result<Vec<BlockHandle>> {
        Err(Error::NotFound(id.to_string()))
    }

    async fn get_bytes(&self, id: &BlockId) -> Result<Bytes> {
        self.inner
            .read()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| Error::NotFound(id.to_string()))
    }

    async fn put(&self, id: &BlockId, value: Bytes) -> Result<()> {
        self.inner.write().unwrap().insert(id.clone(), value);
        Ok(())
    }

    async fn delete(&self, id: &BlockId) -> Result<()> {
        self.inner.write().unwrap().remove(id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{Backend, ObjectId, Version};

    fn block(name: &str) -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "bucket", name),
            0,
            256 * 1024 * 1024,
            Version::new("v1"),
        )
    }

    #[tokio::test]
    async fn put_get_delete_roundtrip() {
        let store = MemoryStore::new();
        let id = block("hello");

        store.put(&id, Bytes::from_static(b"world")).await.unwrap();
        assert_eq!(
            store.get_bytes(&id).await.unwrap(),
            Bytes::from_static(b"world")
        );
        assert!(store.contains(&id).await.unwrap());

        store.delete(&id).await.unwrap();
        assert!(!store.contains(&id).await.unwrap());
    }
}
