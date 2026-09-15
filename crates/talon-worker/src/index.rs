//! In-memory block index.
//!
//! The [`BlockIndex`] maps each resident [`BlockId`] to its [`BlockMeta`]
//! (physical [`BlockForm`] + length) and tracks total resident bytes for
//! eviction. It is the central structure shared by the data path (hit/miss
//! decisions), the miss/loader path (commit on completion), and eviction
//! (byte accounting).
//!
//! Access is synchronized with a single [`RwLock`]; reads (presence/lookup)
//! take a shared lock, mutations (commit/remove/page updates) take an exclusive
//! one. This is deliberately simple for v1; a sharded map can replace it later
//! if the lock becomes hot.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::eviction::{AccessHandle, CacheUnit};

use talon_core::{BlockForm, BlockId, BlockMeta, PageIndex, PresentBitmap};

/// The result of a presence query — how a read should be served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Presence {
    /// The whole block is resident; serve directly.
    Whole,
    /// The block is paged and every requested page is resident.
    PageHit,
    /// The block is paged but at least one requested page is absent.
    PageMiss,
    /// The block is not in the index at all.
    Miss,
}

/// A thread-safe index of resident blocks.
#[derive(Default)]
pub struct BlockIndex {
    inner: RwLock<Inner>,
}

#[derive(Default)]
struct Inner {
    map: HashMap<BlockId, IndexedBlock>,
    resident_bytes: u64,
}

struct IndexedBlock {
    meta: BlockMeta,
    // Sparse: allocating a token for every possible page would dwarf residency
    // metadata when each large block has only one or two cached pages.
    access: HashMap<Option<PageIndex>, AccessHandle>,
}

impl From<BlockMeta> for IndexedBlock {
    fn from(meta: BlockMeta) -> Self {
        Self {
            meta,
            access: HashMap::new(),
        }
    }
}

impl BlockIndex {
    /// Create an empty index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of blocks currently tracked.
    pub fn len(&self) -> usize {
        self.inner.read().unwrap().map.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.read().unwrap().map.is_empty()
    }

    /// Total resident bytes across all tracked blocks (for eviction).
    pub fn resident_bytes(&self) -> u64 {
        self.inner.read().unwrap().resident_bytes
    }

    /// Number of materialized pages across all paged blocks.
    pub fn page_count(&self) -> u64 {
        self.inner
            .read()
            .unwrap()
            .map
            .values()
            .map(|entry| match &entry.meta.form {
                BlockForm::Whole => 0,
                BlockForm::Paged { present, .. } => u64::from(present.count()),
            })
            .sum()
    }

    /// Look up a copy of a block's metadata.
    pub fn get(&self, id: &BlockId) -> Option<BlockMeta> {
        self.inner
            .read()
            .unwrap()
            .map
            .get(id)
            .map(|e| e.meta.clone())
    }

    /// Resolve metadata and mark a whole-block read in the same index lookup.
    pub(crate) fn get_and_touch(&self, id: &BlockId) -> Option<BlockMeta> {
        let g = self.inner.read().unwrap();
        let entry = g.map.get(id)?;
        if let Some(access) = entry.access.get(&None) {
            access.touch();
        }
        Some(entry.meta.clone())
    }

    /// Attach the policy token after admission. The index owns only recency;
    /// pinning and byte accounting remain exclusively owned by the policy.
    pub(crate) fn set_access(&self, unit: &CacheUnit, access: AccessHandle) {
        let (block, page) = match unit {
            CacheUnit::Whole(block) => (block, None),
            CacheUnit::Page(block, page) => (block, Some(*page)),
        };
        let mut g = self.inner.write().unwrap();
        if let Some(entry) = g.map.get_mut(block) {
            let resident = match (&entry.meta.form, page) {
                (BlockForm::Whole, None) => true,
                (BlockForm::Paged { present, .. }, Some(page)) => present.is_present(page),
                _ => false,
            };
            if resident {
                entry.access.insert(page, access);
            }
        }
    }

    /// Snapshot the `(BlockId, len)` of every currently-tracked block.
    ///
    /// Used to seed the eviction tracker at startup from the index rebuilt off
    /// on-disk cache, so already-resident blocks count against capacity from the
    /// first request (issue #159).
    pub fn snapshot_lens(&self) -> Vec<(BlockId, u64)> {
        self.inner
            .read()
            .unwrap()
            .map
            .values()
            .map(|e| (e.meta.id.clone(), e.meta.len))
            .collect()
    }

    /// Snapshot every resident cache *unit* and its byte cost: one entry per
    /// whole block, one per present page of a paged block.
    ///
    /// Seeds the eviction tracker at startup so a rebuilt paged block charges
    /// capacity per resident page rather than for its full logical length.
    pub fn snapshot_units(&self) -> Vec<(BlockId, Option<PageIndex>, u64)> {
        let g = self.inner.read().unwrap();
        let mut out = Vec::new();
        for entry in g.map.values() {
            let meta = &entry.meta;
            match &meta.form {
                BlockForm::Whole => out.push((meta.id.clone(), None, meta.len)),
                BlockForm::Paged { page_size, present } => {
                    for p in 0..present.len() {
                        let page = PageIndex(p);
                        if present.is_present(page) {
                            out.push((
                                meta.id.clone(),
                                Some(page),
                                talon_core::page_len(meta.len, *page_size, page),
                            ));
                        }
                    }
                }
            }
        }
        out
    }

    /// Commit a fully-materialized block (whole or a complete paged set).
    ///
    /// Called by PUT and by loader completion. Replaces any existing entry;
    /// byte accounting is adjusted by the delta between the old and new entry's
    /// *resident* bytes — a paged block only charges for its present pages.
    pub fn commit(&self, meta: BlockMeta) {
        let mut g = self.inner.write().unwrap();
        let added = meta.resident_bytes();
        if let Some(prev) = g.map.insert(meta.id.clone(), meta.into()) {
            g.resident_bytes = g.resident_bytes.saturating_sub(prev.meta.resident_bytes());
        }
        g.resident_bytes = g.resident_bytes.saturating_add(added);
    }

    /// Insert a paged block with an empty presence bitmap, unless one is already
    /// tracked.
    ///
    /// Used when a paged load begins: pages are then filled in with
    /// [`mark_page`](Self::mark_page). `len` is the block's logical length.
    /// Idempotent by design — a second concurrent page miss on the same block
    /// must not reset the bitmap and lose the pages the first one materialized.
    /// Returns `true` if a new entry was created.
    pub fn init_paged(&self, id: BlockId, page_size: u32, len: u64) -> bool {
        let mut g = self.inner.write().unwrap();
        if g.map.contains_key(&id) {
            // Already tracked — either paged (keep its bitmap) or whole (fully
            // resident, nothing to page in). Either way, do not reset it.
            return false;
        }
        let page_count = id.page_count(page_size);
        g.map.insert(
            id.clone(),
            BlockMeta {
                id,
                form: BlockForm::Paged {
                    page_size,
                    present: PresentBitmap::new(page_count),
                },
                len,
            }
            .into(),
        );
        // A fresh paged entry has no present pages, so it adds no resident bytes.
        true
    }

    /// Mark a page present on an existing paged block, charging its bytes.
    ///
    /// Returns `true` if the block exists, is paged, and the page went from
    /// absent to present; `false` otherwise (whole blocks, unknown blocks, and
    /// already-present pages), so byte accounting is never double-counted.
    pub fn mark_page(&self, id: &BlockId, page: PageIndex) -> bool {
        let mut g = self.inner.write().unwrap();
        let Some(entry) = g.map.get_mut(id) else {
            return false;
        };
        let meta = &mut entry.meta;
        let BlockForm::Paged { page_size, present } = &mut meta.form else {
            return false;
        };
        if present.is_present(page) {
            return false;
        }
        present.set(page);
        let bytes = talon_core::page_len(meta.len, *page_size, page);
        g.resident_bytes = g.resident_bytes.saturating_add(bytes);
        true
    }

    /// Mark a page absent on a paged block, refunding its bytes.
    ///
    /// Used by page-level eviction: the block entry (and its other pages) stays
    /// intact. Returns `true` if the page was present and is now cleared.
    pub fn clear_page(&self, id: &BlockId, page: PageIndex) -> bool {
        let mut g = self.inner.write().unwrap();
        let Some(entry) = g.map.get_mut(id) else {
            return false;
        };
        let meta = &mut entry.meta;
        let BlockForm::Paged { page_size, present } = &mut meta.form else {
            return false;
        };
        if !present.is_present(page) {
            return false;
        }
        present.clear(page);
        entry.access.remove(&Some(page));
        if entry.access.capacity() > 32 && entry.access.len() < entry.access.capacity() / 4 {
            entry.access.shrink_to(entry.access.len() * 2);
        }
        let bytes = talon_core::page_len(meta.len, *page_size, page);
        g.resident_bytes = g.resident_bytes.saturating_sub(bytes);
        true
    }

    /// Decide how a read over `[start_page, end_page)` should be served.
    ///
    /// `start_page`/`end_page` are only consulted for paged blocks; for a whole
    /// block the answer is always [`Presence::Whole`].
    pub fn presence(&self, id: &BlockId, start_page: PageIndex, end_page: PageIndex) -> Presence {
        self.lookup_presence(id, start_page, end_page, false)
    }

    /// Mark resident read ranges without a second block-key lookup, key clone,
    /// or acquisition of the capacity policy's mutex.
    pub(crate) fn presence_and_touch(
        &self,
        id: &BlockId,
        start: PageIndex,
        end: PageIndex,
    ) -> Presence {
        self.lookup_presence(id, start, end, true)
    }

    fn lookup_presence(
        &self,
        id: &BlockId,
        start: PageIndex,
        end: PageIndex,
        touch: bool,
    ) -> Presence {
        let g = self.inner.read().unwrap();
        let Some(entry) = g.map.get(id) else {
            return Presence::Miss;
        };
        match &entry.meta.form {
            BlockForm::Whole => {
                if touch {
                    if let Some(access) = entry.access.get(&None) {
                        access.touch();
                    }
                }
                Presence::Whole
            }
            BlockForm::Paged { present, .. } => {
                if !present.range_present(start, end) {
                    return Presence::PageMiss;
                }
                if touch {
                    for page in start.0..end.0 {
                        if let Some(access) = entry.access.get(&Some(PageIndex(page))) {
                            access.touch();
                        }
                    }
                }
                Presence::PageHit
            }
        }
    }

    /// Remove a block from the index, returning its metadata if present.
    ///
    /// Byte accounting is decremented by the removed block's resident bytes.
    pub fn remove(&self, id: &BlockId) -> Option<BlockMeta> {
        let mut g = self.inner.write().unwrap();
        let removed = g.map.remove(id).map(|entry| entry.meta);
        if let Some(meta) = &removed {
            g.resident_bytes = g.resident_bytes.saturating_sub(meta.resident_bytes());
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use talon_core::{Backend, ObjectId, Version};

    fn block(n: u64) -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "b", format!("o/{n}")),
            0,
            256 << 20,
            Version::new("v1"),
        )
    }

    fn whole_meta(id: BlockId, len: u64) -> BlockMeta {
        BlockMeta {
            id,
            form: BlockForm::Whole,
            len,
        }
    }

    #[test]
    fn read_lookup_marks_only_the_requested_page_and_drops_retired_tokens() {
        use crate::eviction::Lru;
        let idx = BlockIndex::new();
        let lru = Lru::new();
        let id = block(1);
        idx.init_paged(id.clone(), 4096, 8192);
        let p0 = CacheUnit::Page(id.clone(), PageIndex(0));
        let p1 = CacheUnit::Page(id.clone(), PageIndex(1));
        for (unit, page) in [(&p0, PageIndex(0)), (&p1, PageIndex(1))] {
            idx.mark_page(&id, page);
            idx.set_access(unit, lru.insert(unit.clone(), 4096));
        }
        assert_eq!(
            idx.presence_and_touch(&id, PageIndex(0), PageIndex(1)),
            Presence::PageHit
        );
        assert_eq!(lru.evict_to_fit(4096), vec![p1.clone()]);
        idx.clear_page(&id, PageIndex(1));
        assert!(!idx.inner.read().unwrap().map[&id]
            .access
            .contains_key(&Some(PageIndex(1))));
        lru.remove(&p0);
        idx.clear_page(&id, PageIndex(0));
        idx.mark_page(&id, PageIndex(0));
        idx.set_access(&p0, lru.insert(p0.clone(), 4096));
        idx.mark_page(&id, PageIndex(1));
        idx.set_access(&p1, lru.insert(p1.clone(), 4096));
        // Ordinary metadata probes must not bias eviction.
        idx.presence(&id, PageIndex(0), PageIndex(1));
        assert_eq!(lru.evict_to_fit(4096), vec![p0]);
    }

    #[test]
    fn commit_lookup_and_byte_accounting() {
        let idx = BlockIndex::new();
        assert!(idx.is_empty());

        idx.commit(whole_meta(block(1), 1000));
        idx.commit(whole_meta(block(2), 2000));
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.resident_bytes(), 3000);

        // Re-commit with a different length adjusts accounting by the delta.
        idx.commit(whole_meta(block(1), 1500));
        assert_eq!(idx.resident_bytes(), 3500);

        let m = idx.get(&block(2)).unwrap();
        assert_eq!(m.len, 2000);

        let removed = idx.remove(&block(1)).unwrap();
        assert_eq!(removed.len, 1500);
        assert_eq!(idx.resident_bytes(), 2000);
        assert!(idx.remove(&block(1)).is_none());
    }

    #[test]
    fn presence_drives_hit_vs_miss() {
        let idx = BlockIndex::new();
        let id = block(7);

        // Unknown block.
        assert_eq!(
            idx.presence(&id, PageIndex(0), PageIndex(1)),
            Presence::Miss
        );

        // Whole block always a whole hit.
        idx.commit(whole_meta(id.clone(), 10));
        assert_eq!(
            idx.presence(&id, PageIndex(0), PageIndex(1)),
            Presence::Whole
        );

        // Paged block: pages absent -> miss, then present -> hit. Uses a fresh
        // id — `init_paged` deliberately does not downgrade an existing whole
        // entry, which is already fully resident.
        let id = block(8);
        let page_size = 256 * 1024;
        idx.init_paged(id.clone(), page_size, 256 << 20);
        assert_eq!(
            idx.presence(&id, PageIndex(0), PageIndex(2)),
            Presence::PageMiss
        );

        assert!(idx.mark_page(&id, PageIndex(0)));
        assert!(idx.mark_page(&id, PageIndex(1)));
        assert_eq!(idx.page_count(), 2);
        assert_eq!(
            idx.presence(&id, PageIndex(0), PageIndex(2)),
            Presence::PageHit
        );
        assert_eq!(
            idx.presence(&id, PageIndex(0), PageIndex(3)),
            Presence::PageMiss
        );

        // mark_page on a whole block is a no-op returning false.
        idx.commit(whole_meta(id.clone(), 10));
        assert!(!idx.mark_page(&id, PageIndex(0)));
    }

    #[test]
    fn concurrent_update_and_read() {
        let idx = Arc::new(BlockIndex::new());
        let mut handles = Vec::new();
        for t in 0..8u64 {
            let idx = Arc::clone(&idx);
            handles.push(std::thread::spawn(move || {
                for n in 0..500u64 {
                    let id = block(t * 1000 + n);
                    idx.commit(whole_meta(id.clone(), 8));
                    let _ = idx.presence(&id, PageIndex(0), PageIndex(1));
                    let _ = idx.get(&id);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(idx.len(), 8 * 500);
        assert_eq!(idx.resident_bytes(), 8 * 500 * 8);
    }
}
