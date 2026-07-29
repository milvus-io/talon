//! Byte-accounted in-memory L1 page cache.
//!
//! L2 stores large whole blocks on local disk. L1 independently caches fixed
//! size pages addressed by `(BlockId, PageIndex)`, so a small hot region of a
//! large block can stay in DRAM without promoting the whole block.

use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::sync::Mutex;
use talon_core::{BlockId, PageIndex};

/// Identity of one L1 page.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MemoryPageKey {
    pub block: BlockId,
    pub page: PageIndex,
}

/// Outcome of attempting to admit a page into L1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryInsert {
    /// L1 is disabled (`capacity_bytes == 0` or `page_size_bytes == 0`).
    Disabled,
    /// The value is empty, larger than one page, or cannot fit in L1.
    TooLarge,
    /// The page was admitted; these colder pages were evicted to make room.
    Inserted {
        /// Evicted L1 page identities, coldest first.
        evicted: Vec<MemoryPageKey>,
    },
}

struct Entry {
    value: Bytes,
    last_used: u64,
}

struct Inner {
    entries: HashMap<MemoryPageKey, Entry>,
    resident_bytes: u64,
    clock: u64,
}

/// An in-memory, byte-capacity LRU over fixed-size block pages.
pub struct MemoryStore {
    inner: Mutex<Inner>,
    capacity_bytes: u64,
    page_size_bytes: u64,
}

impl MemoryStore {
    /// Create an empty disabled store.
    pub fn new() -> Self {
        Self::disabled()
    }

    /// Create a store with L1 admission disabled.
    pub fn disabled() -> Self {
        Self::with_limits(0, 0)
    }

    /// Create an L1 store bounded by total bytes with fixed-size pages.
    pub fn with_limits(capacity_bytes: u64, page_size_bytes: u64) -> Self {
        Self {
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                resident_bytes: 0,
                clock: 0,
            }),
            capacity_bytes,
            page_size_bytes,
        }
    }

    /// Whether L1 admission is enabled.
    pub fn is_enabled(&self) -> bool {
        self.capacity_bytes > 0
            && self.page_size_bytes > 0
            && self.page_size_bytes <= self.capacity_bytes
    }

    /// Configured total L1 capacity.
    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    /// Configured L1 page size.
    pub fn page_size_bytes(&self) -> u64 {
        self.page_size_bytes
    }

    /// Return the number of resident L1 pages.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().entries.len()
    }

    /// Return whether L1 contains no pages.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return resident L1 payload bytes.
    pub fn resident_bytes(&self) -> u64 {
        self.inner.lock().unwrap().resident_bytes
    }

    /// Fetch one page and mark it most-recently-used.
    pub fn get_page(&self, block: &BlockId, page: PageIndex) -> Option<Bytes> {
        let mut inner = self.inner.lock().unwrap();
        inner.clock = inner.clock.saturating_add(1);
        let tick = inner.clock;
        let entry = inner.entries.get_mut(&MemoryPageKey {
            block: block.clone(),
            page,
        })?;
        entry.last_used = tick;
        Some(entry.value.clone())
    }

    /// Fetch an exact block-relative range when every covering page is present.
    ///
    /// A range within one page returns a zero-copy `Bytes::slice`. Multi-page
    /// ranges are stitched into one contiguous buffer.
    pub fn get_range(&self, block: &BlockId, offset: u64, len: u64) -> Option<Bytes> {
        if len == 0 {
            return Some(Bytes::new());
        }
        let end = offset.checked_add(len)?;
        let page_size = self.page_size_bytes;
        if !self.is_enabled() || end > block.block_size as u64 {
            return None;
        }
        let first = offset / page_size;
        let last = (end - 1) / page_size;

        let mut inner = self.inner.lock().unwrap();
        for page in first..=last {
            let page = PageIndex(u32::try_from(page).ok()?);
            let entry = inner.entries.get(&MemoryPageKey {
                block: block.clone(),
                page,
            })?;
            let page_start = u64::from(page.0) * page_size;
            let required_end = end.min(page_start + page_size) - page_start;
            if entry.value.len() < usize::try_from(required_end).ok()? {
                return None;
            }
        }

        inner.clock = inner.clock.saturating_add(1);
        let tick = inner.clock;
        if first == last {
            let page = PageIndex(u32::try_from(first).ok()?);
            let entry = inner.entries.get_mut(&MemoryPageKey {
                block: block.clone(),
                page,
            })?;
            entry.last_used = tick;
            let start = usize::try_from(offset % page_size).ok()?;
            return Some(entry.value.slice(start..start + usize::try_from(len).ok()?));
        }

        let mut out = BytesMut::with_capacity(usize::try_from(len).ok()?);
        for page_number in first..=last {
            let page = PageIndex(u32::try_from(page_number).ok()?);
            let entry = inner.entries.get_mut(&MemoryPageKey {
                block: block.clone(),
                page,
            })?;
            entry.last_used = tick;
            let page_start = page_number * page_size;
            let start = usize::try_from(offset.saturating_sub(page_start)).ok()?;
            let take_end = end.min(page_start + page_size) - page_start;
            let take_end = usize::try_from(take_end).ok()?;
            out.extend_from_slice(&entry.value[start..take_end]);
        }
        Some(out.freeze())
    }

    /// Admit or replace one page and enforce the configured L1 capacity.
    pub fn insert_page(&self, block: BlockId, page: PageIndex, value: Bytes) -> MemoryInsert {
        let len = value.len() as u64;
        if !self.is_enabled() {
            return MemoryInsert::Disabled;
        }
        let page_start = u64::from(page.0).saturating_mul(self.page_size_bytes);
        if len == 0
            || len > self.page_size_bytes
            || len > self.capacity_bytes
            || page_start >= block.block_size as u64
        {
            return MemoryInsert::TooLarge;
        }

        let key = MemoryPageKey { block, page };
        let mut inner = self.inner.lock().unwrap();
        inner.clock = inner.clock.saturating_add(1);
        let tick = inner.clock;
        if let Some(previous) = inner.entries.remove(&key) {
            inner.resident_bytes = inner
                .resident_bytes
                .saturating_sub(previous.value.len() as u64);
        }
        inner.resident_bytes = inner.resident_bytes.saturating_add(len);
        inner.entries.insert(
            key,
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
                .map(|(key, _)| key.clone());
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

    /// Remove every page belonging to one block.
    pub fn remove_block(&self, block: &BlockId) -> Vec<MemoryPageKey> {
        let mut inner = self.inner.lock().unwrap();
        let victims: Vec<_> = inner
            .entries
            .keys()
            .filter(|key| &key.block == block)
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

    /// Remove all pages for old versions of the same logical block as `keep`.
    pub fn remove_superseded(&self, keep: &BlockId) -> Vec<MemoryPageKey> {
        let mut inner = self.inner.lock().unwrap();
        let victims: Vec<_> = inner
            .entries
            .keys()
            .filter(|key| {
                let id = &key.block;
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

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{Backend, ObjectId, Version};

    fn block(name: &str, version: &str) -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "bucket", name),
            0,
            16,
            Version::new(version),
        )
    }

    #[test]
    fn pages_are_independent_and_ranges_require_all_pages() {
        let store = MemoryStore::with_limits(16, 4);
        let id = block("hot", "v1");
        store.insert_page(id.clone(), PageIndex(0), Bytes::from_static(b"abcd"));
        store.insert_page(id.clone(), PageIndex(2), Bytes::from_static(b"ijkl"));

        assert_eq!(store.get_range(&id, 1, 2), Some(Bytes::from_static(b"bc")));
        assert_eq!(
            store.get_range(&id, 8, 4),
            Some(Bytes::from_static(b"ijkl"))
        );
        assert_eq!(store.get_range(&id, 3, 6), None);
    }

    #[test]
    fn multi_page_range_is_stitched_exactly() {
        let store = MemoryStore::with_limits(16, 4);
        let id = block("range", "v1");
        for (page, bytes) in [b"abcd", b"efgh", b"ijkl"].into_iter().enumerate() {
            store.insert_page(
                id.clone(),
                PageIndex(page as u32),
                Bytes::copy_from_slice(bytes),
            );
        }
        assert_eq!(
            store.get_range(&id, 2, 9),
            Some(Bytes::from_static(b"cdefghijk"))
        );
    }

    #[test]
    fn short_final_page_supports_in_bounds_ranges_only() {
        let store = MemoryStore::with_limits(8, 4);
        let id = block("tail", "v1");
        store.insert_page(id.clone(), PageIndex(3), Bytes::from_static(b"xy"));
        assert_eq!(store.get_range(&id, 12, 2), Some(Bytes::from_static(b"xy")));
        assert_eq!(store.get_range(&id, 12, 3), None);
    }

    #[test]
    fn lru_eviction_is_page_granular_and_touch_sensitive() {
        let store = MemoryStore::with_limits(8, 4);
        let id = block("lru", "v1");
        store.insert_page(id.clone(), PageIndex(0), Bytes::from_static(b"aaaa"));
        store.insert_page(id.clone(), PageIndex(1), Bytes::from_static(b"bbbb"));
        assert!(store.get_page(&id, PageIndex(0)).is_some());

        assert_eq!(
            store.insert_page(id.clone(), PageIndex(2), Bytes::from_static(b"cccc")),
            MemoryInsert::Inserted {
                evicted: vec![MemoryPageKey {
                    block: id.clone(),
                    page: PageIndex(1),
                }]
            }
        );
        assert!(store.get_page(&id, PageIndex(0)).is_some());
        assert!(store.get_page(&id, PageIndex(1)).is_none());
        assert!(store.get_page(&id, PageIndex(2)).is_some());
        assert_eq!(store.resident_bytes(), 8);
    }

    #[test]
    fn replacing_page_updates_byte_accounting() {
        let store = MemoryStore::with_limits(8, 4);
        let id = block("replace", "v1");
        store.insert_page(id.clone(), PageIndex(0), Bytes::from_static(b"abcd"));
        store.insert_page(id.clone(), PageIndex(0), Bytes::from_static(b"xy"));
        assert_eq!(store.resident_bytes(), 2);
        assert_eq!(
            store.get_page(&id, PageIndex(0)),
            Some(Bytes::from_static(b"xy"))
        );
    }

    #[test]
    fn block_and_version_invalidation_remove_all_matching_pages() {
        let store = MemoryStore::with_limits(32, 4);
        let old = block("same", "v1");
        let keep = block("same", "v2");
        let other = block("other", "v1");
        for page in 0..2 {
            store.insert_page(old.clone(), PageIndex(page), Bytes::from_static(b"old!"));
            store.insert_page(keep.clone(), PageIndex(page), Bytes::from_static(b"keep"));
        }
        store.insert_page(other.clone(), PageIndex(0), Bytes::from_static(b"data"));

        assert_eq!(store.remove_superseded(&keep).len(), 2);
        assert!(store.get_page(&old, PageIndex(0)).is_none());
        assert!(store.get_page(&keep, PageIndex(0)).is_some());
        assert_eq!(store.remove_block(&keep).len(), 2);
        assert!(store.get_page(&other, PageIndex(0)).is_some());
    }

    #[test]
    fn disabled_and_invalid_pages_are_rejected() {
        let id = block("invalid", "v1");
        assert_eq!(
            MemoryStore::disabled().insert_page(id.clone(), PageIndex(0), Bytes::from_static(b"a")),
            MemoryInsert::Disabled
        );
        let store = MemoryStore::with_limits(4, 4);
        assert_eq!(
            store.insert_page(id.clone(), PageIndex(0), Bytes::from_static(b"12345")),
            MemoryInsert::TooLarge
        );
        assert_eq!(
            store.insert_page(id, PageIndex(4), Bytes::from_static(b"x")),
            MemoryInsert::TooLarge
        );
    }

    #[test]
    fn concurrent_reads_and_inserts_stay_within_capacity() {
        let store = std::sync::Arc::new(MemoryStore::with_limits(1024, 16));
        let mut threads = Vec::new();
        for worker in 0..8 {
            let store = std::sync::Arc::clone(&store);
            threads.push(std::thread::spawn(move || {
                for n in 0..500 {
                    let id = block(&format!("{worker}-{n}"), "v1");
                    store.insert_page(id.clone(), PageIndex(0), Bytes::from(vec![worker; 16]));
                    let _ = store.get_page(&id, PageIndex(0));
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
