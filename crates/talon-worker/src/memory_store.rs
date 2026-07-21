//! A simple in-memory [`ObjectStore`] implementation.

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::RwLock;
use talon_core::{CacheKey, Error, ObjectStore, Result};

/// An in-memory object store backed by a hash map.
#[derive(Default)]
pub struct MemoryStore {
    inner: RwLock<HashMap<CacheKey, Bytes>>,
}

impl MemoryStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the number of stored objects.
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
    async fn get(&self, key: &CacheKey) -> Result<Bytes> {
        self.inner
            .read()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| Error::NotFound(key.to_string()))
    }

    async fn put(&self, key: &CacheKey, value: Bytes) -> Result<()> {
        self.inner.write().unwrap().insert(key.clone(), value);
        Ok(())
    }

    async fn delete(&self, key: &CacheKey) -> Result<()> {
        self.inner.write().unwrap().remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_get_delete_roundtrip() {
        let store = MemoryStore::new();
        let key = CacheKey::new("hello");

        store.put(&key, Bytes::from_static(b"world")).await.unwrap();
        assert_eq!(store.get(&key).await.unwrap(), Bytes::from_static(b"world"));
        assert!(store.contains(&key).await.unwrap());

        store.delete(&key).await.unwrap();
        assert!(!store.contains(&key).await.unwrap());
    }
}
