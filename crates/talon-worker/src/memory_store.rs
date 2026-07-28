//! Byte-accounted in-memory L1 block store.
//!
//! The store is intentionally scoped to small whole blocks. Large blocks stay in
//! the NVMe-backed L2 where the data plane can serve them with `sendfile(2)`.
//! Reads clone [`Bytes`], which is reference-counted and does not copy payload
//! bytes, and atomically mark the entry most-recently-used.

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Mutex;
use talon_core::{BlockHandle, BlockId, Error, ObjectStore, PageIndex, Result};

/// Outcome of attempting to admit a block into L1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryInsert {
    /// L1 is disabled (`capacity_bytes == 0`).
    Disabled,
    /// The block exceeds the configured per-entry limit or total L1 capacity.
    TooLarge,
    /// The block was admitted; these colder entries were evicted to make room.
    Inserted {
        /// Evicted L1 block identities, coldest first.
        evicted: Vec<BlockId>,
    },
}

struct Entry {
    value: Bytes,
    last_used: u64,
}

struct Inner {
    entries: HashMap<BlockId, Entry>,
    resident_bytes: u64,
    clock: u64,
}

/// An in-memory L1 store with byte capacity and LRU eviction.
pub struct MemoryStore {
    inner: Mutex<Inner>,
    capacity_bytes: u64,
    max_entry_bytes: u64,
}

impl MemoryStore {
    /// Create an empty store without a practical payload limit.
    ///
    /// This preserves the original `MemoryStore::new()` behavior for callers
    /// using it as a standalone in-memory [`ObjectStore`]. Worker L1 instances
    /// should use [`MemoryStore::with_limits`].
    pub fn new() -> Self {
        Self::with_limits(u64::MAX, u64::MAX)
    }

    /// Create a store with L1 admission disabled.
    pub fn disabled() -> Self {
        Self::with_limits(0, 0)
    }

    /// Create an L1 store bounded by total and per-entry byte limits.
    ///
    /// A zero capacity disables admission. `max_entry_bytes` is clamped to the
    /// total capacity so an entry can never evict the cache and then fail to fit.
    pub fn with_limits(capacity_bytes: u64, max_entry_bytes: u64) -> Self {
        Self {
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                resident_bytes: 0,
                clock: 0,
            }),
            capacity_bytes,
            max_entry_bytes: max_entry_bytes.min(capacity_bytes),
        }
    }

    /// Whether L1 admission is enabled.
    pub fn is_enabled(&self) -> bool {
        self.capacity_bytes > 0 && self.max_entry_bytes > 0
    }

    /// Configured total L1 capacity.
    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    /// Maximum size of one admitted L1 entry.
    pub fn max_entry_bytes(&self) -> u64 {
        self.max_entry_bytes
    }

    /// Whether a block of `len` bytes is eligible for L1.
    pub fn is_eligible(&self, len: u64) -> bool {
        self.is_enabled() && len <= self.max_entry_bytes && len <= self.capacity_bytes
    }

    /// Return the number of resident L1 blocks.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().entries.len()
    }

    /// Return whether L1 contains no blocks.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return resident L1 payload bytes.
    pub fn resident_bytes(&self) -> u64 {
        self.inner.lock().unwrap().resident_bytes
    }

    /// Fetch and mark a block most-recently-used.
    pub fn get(&self, id: &BlockId) -> Option<Bytes> {
        let mut inner = self.inner.lock().unwrap();
        inner.clock = inner.clock.saturating_add(1);
        let tick = inner.clock;
        let entry = inner.entries.get_mut(id)?;
        entry.last_used = tick;
        Some(entry.value.clone())
    }

    /// Admit or replace a block and enforce the configured L1 capacity.
    pub fn insert(&self, id: BlockId, value: Bytes) -> MemoryInsert {
        let len = value.len() as u64;
        if !self.is_enabled() {
            return MemoryInsert::Disabled;
        }
        if !self.is_eligible(len) {
            return MemoryInsert::TooLarge;
        }

        let mut inner = self.inner.lock().unwrap();
        inner.clock = inner.clock.saturating_add(1);
        let tick = inner.clock;
        if let Some(previous) = inner.entries.remove(&id) {
            inner.resident_bytes = inner
                .resident_bytes
                .saturating_sub(previous.value.len() as u64);
        }
        inner.resident_bytes = inner.resident_bytes.saturating_add(len);
        inner.entries.insert(
            id,
            Entry {
                value,
                last_used: tick,
            },
        );

        let mut evicted = Vec::new();
        while inner.resident_bytes > self.capacity_bytes {
            let victim = inner
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(id, _)| id.clone());
            let Some(victim) = victim else {
                break;
            };
            if let Some(entry) = inner.entries.remove(&victim) {
                inner.resident_bytes = inner
                    .resident_bytes
                    .saturating_sub(entry.value.len() as u64);
                evicted.push(victim);
            }
        }
        MemoryInsert::Inserted { evicted }
    }

    /// Remove one block, returning whether it was resident.
    pub fn remove(&self, id: &BlockId) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let Some(entry) = inner.entries.remove(id) else {
            return false;
        };
        inner.resident_bytes = inner
            .resident_bytes
            .saturating_sub(entry.value.len() as u64);
        true
    }

    /// Remove every old version of the same logical block as `keep`.
    pub fn remove_superseded(&self, keep: &BlockId) -> Vec<BlockId> {
        let mut inner = self.inner.lock().unwrap();
        let victims: Vec<BlockId> = inner
            .entries
            .keys()
            .filter(|id| {
                id.object == keep.object
                    && id.offset == keep.offset
                    && id.block_size == keep.block_size
                    && id.version != keep.version
            })
            .cloned()
            .collect();
        for victim in &victims {
            if let Some(entry) = inner.entries.remove(victim) {
                inner.resident_bytes = inner
                    .resident_bytes
                    .saturating_sub(entry.value.len() as u64);
            }
        }
        victims
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ObjectStore for MemoryStore {
    async fn get_block(&self, id: &BlockId) -> Result<BlockHandle> {
        // A DRAM entry has no file descriptor; callers use `get_bytes`.
        Err(Error::NotFound(id.to_string()))
    }

    async fn get_page(&self, id: &BlockId, page: PageIndex) -> Result<BlockHandle> {
        Err(Error::NotFound(format!("{id} page {}", page.0)))
    }

    async fn get_range(&self, id: &BlockId, _offset: u64, _len: u64) -> Result<Vec<BlockHandle>> {
        Err(Error::NotFound(id.to_string()))
    }

    async fn get_bytes(&self, id: &BlockId) -> Result<Bytes> {
        self.get(id).ok_or_else(|| Error::NotFound(id.to_string()))
    }

    async fn put(&self, id: &BlockId, value: Bytes) -> Result<()> {
        match self.insert(id.clone(), value) {
            MemoryInsert::Inserted { .. } => Ok(()),
            MemoryInsert::Disabled => Err(Error::Other("L1 memory store is disabled".into())),
            MemoryInsert::TooLarge => Err(Error::Other(format!(
                "block {id} exceeds L1 entry/capacity limit"
            ))),
        }
    }

    async fn delete(&self, id: &BlockId) -> Result<()> {
        self.remove(id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{Backend, ObjectId, Version};

    fn block(name: &str, version: &str) -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "bucket", name),
            0,
            256 * 1024 * 1024,
            Version::new(version),
        )
    }

    #[tokio::test]
    async fn put_get_delete_roundtrip() {
        let store = MemoryStore::with_limits(64, 64);
        let id = block("hello", "v1");

        store.put(&id, Bytes::from_static(b"world")).await.unwrap();
        assert_eq!(
            store.get_bytes(&id).await.unwrap(),
            Bytes::from_static(b"world")
        );
        assert!(store.contains(&id).await.unwrap());
        assert_eq!(store.resident_bytes(), 5);

        store.delete(&id).await.unwrap();
        assert!(!store.contains(&id).await.unwrap());
        assert_eq!(store.resident_bytes(), 0);
    }

    #[test]
    fn disabled_and_oversized_entries_are_not_admitted() {
        let disabled = MemoryStore::disabled();
        assert_eq!(
            disabled.insert(block("a", "v1"), Bytes::from_static(b"a")),
            MemoryInsert::Disabled
        );
        assert!(disabled.is_empty());

        let store = MemoryStore::with_limits(8, 4);
        assert_eq!(
            store.insert(block("a", "v1"), Bytes::from_static(b"12345")),
            MemoryInsert::TooLarge
        );
        assert!(store.is_empty());
    }

    #[tokio::test]
    async fn default_store_remains_writable_and_disabled_put_is_an_error() {
        let store = MemoryStore::new();
        let id = block("default", "v1");
        store
            .put(&id, Bytes::from_static(b"payload"))
            .await
            .unwrap();
        assert_eq!(
            store.get_bytes(&id).await.unwrap(),
            Bytes::from_static(b"payload")
        );

        let disabled = MemoryStore::disabled();
        let error = disabled
            .put(&id, Bytes::from_static(b"payload"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("disabled"));
        assert!(disabled.is_empty());
    }

    #[test]
    fn max_entry_limit_is_clamped_to_capacity() {
        let store = MemoryStore::with_limits(4, 8);
        assert_eq!(store.capacity_bytes(), 4);
        assert_eq!(store.max_entry_bytes(), 4);
        assert!(store.is_eligible(4));
        assert!(!store.is_eligible(5));
    }

    #[test]
    fn lru_eviction_is_byte_accounted_and_touch_sensitive() {
        let store = MemoryStore::with_limits(8, 4);
        let a = block("a", "v1");
        let b = block("b", "v1");
        let c = block("c", "v1");
        assert_eq!(
            store.insert(a.clone(), Bytes::from_static(b"aaaa")),
            MemoryInsert::Inserted { evicted: vec![] }
        );
        assert_eq!(
            store.insert(b.clone(), Bytes::from_static(b"bbbb")),
            MemoryInsert::Inserted { evicted: vec![] }
        );
        assert_eq!(store.get(&a), Some(Bytes::from_static(b"aaaa")));

        assert_eq!(
            store.insert(c.clone(), Bytes::from_static(b"cccc")),
            MemoryInsert::Inserted {
                evicted: vec![b.clone()]
            }
        );
        assert!(store.get(&b).is_none());
        assert!(store.get(&a).is_some());
        assert!(store.get(&c).is_some());
        assert_eq!(store.resident_bytes(), 8);
    }

    #[test]
    fn replacing_an_entry_updates_accounting_without_self_eviction() {
        let store = MemoryStore::with_limits(8, 8);
        let id = block("a", "v1");
        store.insert(id.clone(), Bytes::from_static(b"123456"));
        assert_eq!(store.resident_bytes(), 6);
        assert_eq!(
            store.insert(id.clone(), Bytes::from_static(b"xy")),
            MemoryInsert::Inserted { evicted: vec![] }
        );
        assert_eq!(store.resident_bytes(), 2);
        assert_eq!(store.get(&id), Some(Bytes::from_static(b"xy")));
    }

    #[test]
    fn superseded_versions_are_removed_without_touching_other_blocks() {
        let store = MemoryStore::with_limits(64, 16);
        let old = block("same", "v1");
        let keep = block("same", "v2");
        let other = block("other", "v1");
        for id in [&old, &keep, &other] {
            store.insert(id.clone(), Bytes::from_static(b"data"));
        }

        assert_eq!(store.remove_superseded(&keep), vec![old.clone()]);
        assert!(store.get(&old).is_none());
        assert!(store.get(&keep).is_some());
        assert!(store.get(&other).is_some());
        assert_eq!(store.resident_bytes(), 8);
    }

    #[test]
    fn concurrent_reads_and_inserts_stay_within_capacity() {
        let store = std::sync::Arc::new(MemoryStore::with_limits(1024, 32));
        let mut threads = Vec::new();
        for worker in 0..8 {
            let store = std::sync::Arc::clone(&store);
            threads.push(std::thread::spawn(move || {
                for n in 0..500 {
                    let id = block(&format!("{worker}-{n}"), "v1");
                    store.insert(id.clone(), Bytes::from(vec![worker as u8; 16]));
                    let _ = store.get(&id);
                }
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        assert!(store.resident_bytes() <= store.capacity_bytes());
        assert!(store.len() <= 64);
    }
}
