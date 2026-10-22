//! In-memory block index.
//!
//! The [`BlockIndex`] maps each resident [`BlockId`] to its [`BlockMeta`]
//! (physical [`BlockForm`] + length) and tracks total resident bytes for
//! eviction. It is the central structure shared by the data path (hit/miss
//! decisions), the miss/loader path (commit on completion), and eviction
//! (byte accounting).
//!
//! The directory lock protects membership; each block has its own [`RwLock`]
//! for metadata and page updates. Readers borrow the entry while holding the
//! directory read lock. Writers retain an Arc and release the directory before
//! locking the entry. Removal/replacement retires the old entry under its write
//! lock, so writers racing with retirement retry their lookup.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard};

use crate::eviction::{AccessHandle, CacheUnit};
use crate::page_lifecycle::{PageReadGuard, PageState};

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
    entries: RwLock<HashMap<BlockId, Arc<Entry>>>,
    resident_bytes: AtomicU64,
    resident_pages: AtomicU64,
}

struct Entry {
    // None means retired: retained Arcs must never mutate or charge it again.
    block: RwLock<Option<IndexedBlock>>,
}

impl Entry {
    fn new(block: IndexedBlock) -> Self {
        Self {
            block: RwLock::new(Some(block)),
        }
    }
}

fn materialized_pages(meta: &BlockMeta) -> u64 {
    match &meta.form {
        BlockForm::Whole => 0,
        BlockForm::Paged { present, .. } => u64::from(present.count()),
    }
}

/// Data-file allocation discovered during startup; never persisted in sidecars.
pub(crate) struct StoredBlock {
    pub meta: BlockMeta,
    pub allocated: Vec<(Option<PageIndex>, u64)>,
}

struct IndexedBlock {
    meta: BlockMeta,
    allocated: HashMap<Option<PageIndex>, u64>,
    // Sparse: allocating a token for every possible page would dwarf residency
    // metadata when each large block has only one or two cached pages.
    access: HashMap<Option<PageIndex>, IndexedAccess>,
}

struct IndexedAccess {
    policy: AccessHandle,
    page: Option<Arc<PageState>>,
}

impl From<BlockMeta> for IndexedBlock {
    fn from(meta: BlockMeta) -> Self {
        Self {
            meta,
            allocated: HashMap::new(),
            access: HashMap::new(),
        }
    }
}

impl BlockIndex {
    fn entry(&self, id: &BlockId) -> Option<Arc<Entry>> {
        self.entries.read().unwrap().get(id).cloned()
    }

    fn read_entry<T>(&self, id: &BlockId, read: impl FnOnce(&IndexedBlock) -> T) -> Option<T> {
        let entries = self.entries.read().unwrap();
        let entry = entries.get(id)?;
        let guard = entry.block.read().unwrap();
        // Keeping membership stable also prevents this entry from retiring.
        Some(read(guard.as_ref().expect("live directory entry")))
    }

    fn write_entry<T>(
        &self,
        id: &BlockId,
        write: impl FnOnce(&mut IndexedBlock) -> T,
    ) -> Option<T> {
        loop {
            let entry = self.entry(id)?;
            let mut guard = entry.block.write().unwrap();
            if let Some(block) = guard.as_mut() {
                return Some(write(block));
            }
        }
    }

    // Called while the affected entry is exclusively locked (or, for a new
    // entry, before it is visible). Counters are observations, not a transaction
    // spanning the directory, both counters, and all entries.
    fn replace_bytes(&self, old: u64, new: u64) {
        self.resident_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |bytes| {
                Some(bytes.saturating_sub(old).saturating_add(new))
            })
            .unwrap();
    }

    fn with_snapshot<T>(
        &self,
        read: impl FnOnce(&[RwLockReadGuard<'_, Option<IndexedBlock>>]) -> T,
    ) -> T {
        // Startup snapshots retain the original point-in-time semantics. Keep
        // membership stable and acquire all entry read locks before collecting
        // data. Request-path reads only take one entry lock instead.
        let entries = self.entries.read().unwrap();
        let guards: Vec<_> = entries
            .values()
            .map(|entry| entry.block.read().unwrap())
            .collect();
        read(&guards)
    }

    /// Page protection is acquired through the existing index/token lookup.
    /// No block lifecycle lock or second page-table lookup is needed. The single
    /// page case keeps its guard inline and does not allocate a vector buffer.
    pub(crate) fn page_reads_and_touch(
        &self,
        id: &BlockId,
        start: PageIndex,
        end: PageIndex,
    ) -> Option<(PageReadGuard, Vec<PageReadGuard>)> {
        if start.0 >= end.0 {
            return None;
        }
        self.read_entry(id, |entry| {
            let BlockForm::Paged { present, .. } = &entry.meta.form else {
                return None;
            };
            if !present.range_present(start, end) {
                return None;
            }
            let acquire = |page| {
                let access = entry.access.get(&Some(PageIndex(page)))?;
                let guard = access.page.as_ref()?.acquire()?;
                access.policy.touch();
                Some(guard)
            };
            let first = acquire(start.0)?;
            let tail: Option<Vec<_>> = (start.0 + 1..end.0).map(acquire).collect();
            Some((first, tail?))
        })?
    }

    pub(crate) fn set_page_state(&self, id: &BlockId, page: PageIndex, state: Arc<PageState>) {
        self.write_entry(id, |entry| {
            if let Some(access) = entry.access.get_mut(&Some(page)) {
                access.page = Some(state);
            }
        });
    }

    /// Create an empty index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of blocks currently tracked.
    pub fn len(&self) -> usize {
        self.entries.read().unwrap().len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.read().unwrap().is_empty()
    }

    /// Allocated disk bytes of indexed data files, also charged to eviction.
    pub fn resident_bytes(&self) -> u64 {
        self.resident_bytes.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn logical_resident_bytes(&self) -> u64 {
        self.with_snapshot(|entries| {
            entries
                .iter()
                .map(|entry| {
                    entry
                        .as_ref()
                        .expect("live directory entry")
                        .meta
                        .resident_bytes()
                })
                .sum()
        })
    }

    /// Number of materialized pages across all paged blocks.
    pub fn page_count(&self) -> u64 {
        self.resident_pages.load(Ordering::Relaxed)
    }

    /// Look up a copy of a block's metadata.
    pub fn get(&self, id: &BlockId) -> Option<BlockMeta> {
        self.read_entry(id, |entry| entry.meta.clone())
    }

    /// Resolve metadata and mark a whole-block read in the same index lookup.
    pub(crate) fn get_and_touch(&self, id: &BlockId) -> Option<BlockMeta> {
        self.read_entry(id, |entry| {
            if let Some(access) = entry.access.get(&None) {
                access.policy.touch();
            }
            entry.meta.clone()
        })
    }

    /// Probe the whole-block fast path without touching any paged entries.
    pub(crate) fn is_whole_and_touch(&self, id: &BlockId) -> bool {
        self.read_entry(id, |entry| {
            if !matches!(entry.meta.form, BlockForm::Whole) {
                return false;
            }
            if let Some(access) = entry.access.get(&None) {
                access.policy.touch();
            }
            true
        })
        .unwrap_or(false)
    }

    /// Attach the policy token after admission, retaining any page lifecycle
    /// handle. Capacity pinning and byte accounting remain owned by the policy.
    pub(crate) fn set_access(&self, unit: &CacheUnit, access: AccessHandle) {
        let (block, page) = match unit {
            CacheUnit::Whole(block) => (block, None),
            CacheUnit::Page(block, page) => (block, Some(*page)),
        };
        self.write_entry(block, |entry| {
            let resident = match (&entry.meta.form, page) {
                (BlockForm::Whole, None) => true,
                (BlockForm::Paged { present, .. }, Some(page)) => present.is_present(page),
                _ => false,
            };
            if resident {
                let old_page = entry.access.get(&page).and_then(|old| old.page.clone());
                entry.access.insert(
                    page,
                    IndexedAccess {
                        policy: access,
                        page: old_page,
                    },
                );
            }
        });
    }

    /// Snapshot the `(BlockId, len)` of every currently-tracked block.
    ///
    /// Used to seed the eviction tracker at startup from the index rebuilt off
    /// on-disk cache, so already-resident blocks count against capacity from the
    /// first request (issue #159).
    pub fn snapshot_lens(&self) -> Vec<(BlockId, u64)> {
        self.with_snapshot(|entries| {
            entries
                .iter()
                .map(|entry| {
                    let e = entry.as_ref().expect("live directory entry");
                    (e.meta.id.clone(), e.meta.len)
                })
                .collect()
        })
    }

    /// Snapshot every resident cache *unit* and its byte cost: one entry per
    /// whole block, one per present page of a paged block.
    ///
    /// Seeds the eviction tracker at startup so a rebuilt paged block charges
    /// capacity per resident page rather than for its full logical length.
    pub fn snapshot_units(&self) -> Vec<(BlockId, Option<PageIndex>, u64)> {
        self.with_snapshot(|entries| {
            let mut out = Vec::new();
            for guard in entries {
                let entry = guard.as_ref().expect("live directory entry");
                let meta = &entry.meta;
                match &meta.form {
                    BlockForm::Whole => out.push((meta.id.clone(), None, entry.allocated[&None])),
                    BlockForm::Paged { present, .. } => {
                        for p in 0..present.len() {
                            let page = PageIndex(p);
                            if present.is_present(page) {
                                out.push((
                                    meta.id.clone(),
                                    Some(page),
                                    entry.allocated[&Some(page)],
                                ));
                            }
                        }
                    }
                }
            }
            out
        })
    }

    /// Commit metadata with measured allocated bytes for each resident data file.
    ///
    /// Called by PUT and by loader completion. Replaces any existing entry;
    /// byte accounting is adjusted by the delta between the old and new entry's
    /// *resident* bytes — a paged block only charges for its present pages.
    pub fn commit_allocated(
        &self,
        meta: BlockMeta,
        allocated: impl IntoIterator<Item = (Option<PageIndex>, u64)>,
    ) {
        let allocated: HashMap<_, _> = allocated.into_iter().collect();
        let added: u64 = allocated.values().sum();
        let pages = materialized_pages(&meta);
        let id = meta.id.clone();
        let entry = IndexedBlock {
            meta,
            allocated,
            access: HashMap::new(),
        };
        let mut entries = self.entries.write().unwrap();
        // Structural mutations take directory -> entry. Ordinary operations
        // never reacquire the directory while holding an entry lock.
        let previous = entries.get(&id).map(|old| {
            old.block
                .write()
                .unwrap()
                .take()
                .expect("live directory entry")
        });
        let old_bytes = previous
            .as_ref()
            .map_or(0, |old| old.allocated.values().sum());
        let old_pages = previous
            .as_ref()
            .map_or(0, |old| materialized_pages(&old.meta));
        self.replace_bytes(old_bytes, added);
        if pages >= old_pages {
            self.resident_pages
                .fetch_add(pages - old_pages, Ordering::Relaxed);
        } else {
            self.resident_pages
                .fetch_sub(old_pages - pages, Ordering::Relaxed);
        }
        entries.insert(id, Arc::new(Entry::new(entry)));
        drop(entries);
        // Potentially large maps and bitmaps are freed outside directory locks.
        drop(previous);
    }

    // Synthetic index fixtures specify byte costs without creating disk files.
    #[cfg(test)]
    pub(crate) fn commit(&self, meta: BlockMeta) {
        let allocated = match &meta.form {
            BlockForm::Whole => vec![(None, meta.len)],
            BlockForm::Paged { page_size, present } => (0..present.len())
                .map(PageIndex)
                .filter(|p| present.is_present(*p))
                .map(|p| (Some(p), talon_core::page_len(meta.len, *page_size, p)))
                .collect(),
        };
        self.commit_allocated(meta, allocated);
    }

    /// Insert a paged block with an empty presence bitmap, unless one is already
    /// tracked.
    ///
    /// Used when a paged load begins: pages are then filled in with
    /// [`mark_page_allocated`](Self::mark_page_allocated). `len` is the block's logical length.
    /// Idempotent by design — a second concurrent page miss on the same block
    /// must not reset the bitmap and lose the pages the first one materialized.
    /// Returns `true` if a new entry was created.
    pub fn init_paged(&self, id: BlockId, page_size: u32, len: u64) -> bool {
        if self.entries.read().unwrap().contains_key(&id) {
            // Already tracked — either paged (keep its bitmap) or whole (fully
            // resident, nothing to page in). Either way, do not reset it.
            return false;
        }
        let page_count = id.page_count(page_size);
        let entry = Entry::new(
            BlockMeta {
                id: id.clone(),
                form: BlockForm::Paged {
                    page_size,
                    present: PresentBitmap::new(page_count),
                },
                len,
            }
            .into(),
        );
        let mut entries = self.entries.write().unwrap();
        let std::collections::hash_map::Entry::Vacant(slot) = entries.entry(id) else {
            return false;
        };
        slot.insert(Arc::new(entry));
        // A fresh paged entry has no present pages, so it adds no resident bytes.
        true
    }

    /// Publish a page and its measured allocation, replacing any prior cost.
    ///
    /// Returns `true` if the block exists, is paged, and the page went from
    /// absent to present; `false` otherwise (whole blocks, unknown blocks, and
    /// already-present pages), so byte accounting is never double-counted.
    pub fn mark_page_allocated(&self, id: &BlockId, page: PageIndex, allocated: u64) -> bool {
        self.write_entry(id, |entry| {
            let BlockForm::Paged { present, .. } = &mut entry.meta.form else {
                return false;
            };
            if page.0 >= present.len() {
                return false;
            }
            let fresh = !present.is_present(page);
            present.set(page);
            let old = entry.allocated.insert(Some(page), allocated).unwrap_or(0);
            self.replace_bytes(old, allocated);
            self.resident_pages
                .fetch_add(u64::from(fresh), Ordering::Relaxed);
            fresh
        })
        .unwrap_or(false)
    }

    #[cfg(test)]
    pub(crate) fn mark_page(&self, id: &BlockId, page: PageIndex) -> bool {
        let allocated = self.get(id).map_or(0, |meta| meta.page_len(page));
        self.mark_page_allocated(id, page, allocated)
    }

    /// Mark a page absent on a paged block, refunding its bytes.
    ///
    /// Used by page-level eviction: the block entry (and its other pages) stays
    /// intact. Returns `true` if the page was present and is now cleared.
    pub fn clear_page(&self, id: &BlockId, page: PageIndex) -> bool {
        self.write_entry(id, |entry| {
            let meta = &mut entry.meta;
            let BlockForm::Paged { present, .. } = &mut meta.form else {
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
            let bytes = entry.allocated.remove(&Some(page)).unwrap_or(0);
            self.replace_bytes(bytes, 0);
            self.resident_pages.fetch_sub(1, Ordering::Relaxed);
            true
        })
        .unwrap_or(false)
    }

    /// Decide how a read over `[start_page, end_page)` should be served.
    ///
    /// `start_page`/`end_page` are only consulted for paged blocks; for a whole
    /// block the answer is always [`Presence::Whole`].
    pub fn presence(&self, id: &BlockId, start_page: PageIndex, end_page: PageIndex) -> Presence {
        self.read_entry(id, |entry| match &entry.meta.form {
            BlockForm::Whole => Presence::Whole,
            BlockForm::Paged { present, .. } => {
                if !present.range_present(start_page, end_page) {
                    return Presence::PageMiss;
                }
                Presence::PageHit
            }
        })
        .unwrap_or(Presence::Miss)
    }

    /// Remove a block from the index, returning its metadata if present.
    ///
    /// Byte accounting is decremented by the removed block's resident bytes.
    /// The directory stops exposing the block before its retirement and
    /// accounting finish; retained operations may complete in that interval.
    pub fn remove(&self, id: &BlockId) -> Option<BlockMeta> {
        let mut entries = self.entries.write().unwrap();
        let entry = entries.remove(id)?;
        // Retiring this detached entry must not block unrelated lookups or a
        // new entry with the same key. Its Arc keeps the old state alive.
        drop(entries);
        let removed = entry
            .block
            .write()
            .unwrap()
            .take()
            .expect("removed entry is not retired");
        self.replace_bytes(removed.allocated.values().sum(), 0);
        self.resident_pages
            .fetch_sub(materialized_pages(&removed.meta), Ordering::Relaxed);
        Some(removed.meta)
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
    fn allocation_cost_is_independent_of_logical_length_and_replacement() {
        let index = BlockIndex::new();
        let id = block(1);
        index.commit_allocated(whole_meta(id.clone(), 6000), [(None, 8192)]);
        assert_eq!(index.resident_bytes(), 8192);
        assert_eq!(index.get(&id).unwrap().len, 6000);
        // Sparse replacement: a nonempty logical file may have no allocated blocks.
        index.commit_allocated(whole_meta(id.clone(), 6000), [(None, 0)]);
        assert_eq!(index.resident_bytes(), 0);
        index.remove(&id);
        index.init_paged(id.clone(), 512, 600);
        assert!(index.mark_page_allocated(&id, PageIndex(0), 4096));
        assert!(index.mark_page_allocated(&id, PageIndex(1), 8192));
        assert_eq!(index.resident_bytes(), 12288);
        assert!(!index.mark_page_allocated(&id, PageIndex(1), 4096));
        assert_eq!(index.resident_bytes(), 8192);
        index.clear_page(&id, PageIndex(1));
        assert_eq!(index.resident_bytes(), 4096);
        index.remove(&id);
        assert_eq!(index.resident_bytes(), 0);
    }

    #[test]
    fn page_accounting_survives_recovery_replacement_and_duplicate_mutations() {
        let index = BlockIndex::new();
        let id = block(1);
        let mut present = PresentBitmap::new(id.page_count(4096));
        present.set(PageIndex(0));
        present.set(PageIndex(65));
        let recovered = BlockMeta {
            id: id.clone(),
            form: BlockForm::Paged {
                page_size: 4096,
                present,
            },
            len: u64::from(id.block_size),
        };
        index.commit(recovered.clone());
        index.commit(recovered);
        assert_eq!(index.page_count(), 2);
        assert!(!index.init_paged(id.clone(), 4096, u64::from(id.block_size)));
        assert!(!index.mark_page(&id, PageIndex(65)));
        assert!(!index.mark_page(&id, PageIndex(u32::MAX)));
        assert!(index.mark_page(&id, PageIndex(100)));
        assert_eq!(index.page_count(), 3);
        assert!(index.clear_page(&id, PageIndex(0)));
        assert!(!index.clear_page(&id, PageIndex(0)));
        assert_eq!(index.page_count(), 2);
        index.commit(whole_meta(id.clone(), u64::from(id.block_size)));
        assert_eq!(index.page_count(), 0);
        index.remove(&id);
        index.init_paged(id.clone(), 4096, u64::from(id.block_size));
        index.mark_page(&id, PageIndex(1));
        assert_eq!(index.page_count(), 1);
        index.remove(&id);
        index.remove(&id);
        assert_eq!(index.page_count(), 0);
    }

    #[test]
    fn page_lookup_does_not_lock_block_and_partial_pin_failure_releases_readers() {
        use crate::eviction::Lru;
        let idx = BlockIndex::new();
        let lru = Lru::new();
        let id = block(1);
        let life = crate::page_lifecycle::PageLifecycle::new();
        let block = life.block(&id);
        idx.init_paged(id.clone(), 4096, 8192);
        for p in 0..2 {
            let page = PageIndex(p);
            let unit = CacheUnit::Page(id.clone(), page);
            idx.mark_page(&id, page);
            idx.set_access(&unit, lru.insert(unit.clone(), 4096));
            idx.set_page_state(&id, page, block.register(page, Some(10), 10, false));
        }
        let locked = block.inner.lock().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                tx.send(
                    idx.page_reads_and_touch(&id, PageIndex(0), PageIndex(2))
                        .is_some(),
                )
                .unwrap();
            });
            let result = rx.recv_timeout(std::time::Duration::from_secs(5));
            // Release before asserting so a regression cannot strand the thread.
            drop(locked);
            assert!(result.unwrap());
        });
        let second = block.candidate(PageIndex(1)).unwrap();
        assert!(block.claim(&second, 100, None));
        assert!(idx
            .page_reads_and_touch(&id, PageIndex(0), PageIndex(2))
            .is_none());
        let first = block.candidate(PageIndex(0)).unwrap();
        assert!(
            block.claim(&first, 100, None),
            "partial range acquisition leaked the first pin"
        );
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
        let life = crate::page_lifecycle::PageLifecycle::new();
        let page = life.block(&id).register(PageIndex(0), None, 0, false);
        idx.set_page_state(&id, PageIndex(0), page);
        assert!(idx
            .page_reads_and_touch(&id, PageIndex(0), PageIndex(1))
            .is_some());
        assert_eq!(lru.evict_to_fit(4096), vec![p1.clone()]);
        idx.clear_page(&id, PageIndex(1));
        assert!(idx
            .read_entry(&id, |entry| !entry.access.contains_key(&Some(PageIndex(1))))
            .unwrap());
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
    fn whole_probe_preserves_whole_recency_without_heating_pages() {
        use crate::eviction::Lru;
        let idx = BlockIndex::new();
        let lru = Lru::new();
        assert!(!idx.is_whole_and_touch(&block(0)));
        let whole = CacheUnit::Whole(block(1));
        idx.commit(whole_meta(block(1), 4096));
        idx.set_access(&whole, lru.insert(whole.clone(), 4096));
        let paged = block(2);
        idx.init_paged(paged.clone(), 4096, 8192);
        let page = CacheUnit::Page(paged.clone(), PageIndex(0));
        idx.mark_page(&paged, PageIndex(0));
        idx.set_access(&page, lru.insert(page.clone(), 4096));

        assert!(idx.is_whole_and_touch(&block(1)));
        assert!(!idx.is_whole_and_touch(&paged));
        // The older whole block received a real touch; the newer page did not.
        assert_eq!(lru.evict_to_fit(4096), vec![page]);
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

    #[test]
    fn page_updates_only_lock_their_entry() {
        use crate::eviction::Lru;
        use std::sync::mpsc;
        use std::time::Duration;

        let index = BlockIndex::new();
        let busy = block(1);
        let other = block(2);
        index.init_paged(busy.clone(), 4096, 8192);
        index.init_paged(other.clone(), 4096, 8192);
        let busy_entry = index.entry(&busy).unwrap();
        let busy_guard = busy_entry.block.write().unwrap();
        // A directory reader must not prevent mutations to existing entries.
        let directory_guard = index.entries.read().unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let lru = Lru::new();
                let lifecycle = crate::page_lifecycle::PageLifecycle::new();
                let page = PageIndex(0);
                let unit = CacheUnit::Page(other.clone(), page);
                assert!(!index.init_paged(other.clone(), 4096, 8192));
                assert!(index.mark_page_allocated(&other, page, 4096));
                index.set_access(&unit, lru.insert(unit.clone(), 4096));
                index.set_page_state(
                    &other,
                    page,
                    lifecycle.block(&other).register(page, None, 0, false),
                );
                assert!(index
                    .page_reads_and_touch(&other, page, PageIndex(1))
                    .is_some());
                assert_eq!(
                    index.presence(&other, page, PageIndex(1)),
                    Presence::PageHit
                );
                assert!(index.clear_page(&other, page));
                tx.send(()).unwrap();
            });
            let result = rx.recv_timeout(Duration::from_secs(5));
            drop(directory_guard);
            drop(busy_guard);
            result.expect("an unrelated entry or directory reader blocked page updates");
        });
        assert_eq!(index.resident_bytes(), 0);
        assert_eq!(index.page_count(), 0);
    }

    #[test]
    fn entry_write_waiters_release_the_directory() {
        use std::sync::mpsc;
        use std::time::{Duration, Instant};

        let index = BlockIndex::new();
        let id = block(1);
        index.init_paged(id.clone(), 4096, 4096);
        let entry = index.entry(&id).unwrap();
        let guard = entry.block.write().unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| index.mark_page_allocated(&id, PageIndex(0), 4096));
            // Directory + this test own two Arcs; the third proves the waiter
            // has completed its lookup before we try a structural mutation.
            let deadline = Instant::now() + Duration::from_secs(5);
            while Arc::strong_count(&entry) < 3 && Instant::now() < deadline {
                std::thread::yield_now();
            }
            let looked_up = Arc::strong_count(&entry) >= 3;
            scope.spawn(|| {
                index.commit_allocated(whole_meta(block(2), 8192), [(None, 8192)]);
                tx.send(index.remove(&block(2)).is_some()).unwrap();
            });
            let result = rx.recv_timeout(Duration::from_secs(5));
            drop(guard);
            assert!(looked_up, "page updater did not reach the entry lookup");
            assert!(result.expect("entry waiter retained the directory lock"));
        });
        assert_eq!(index.resident_bytes(), 4096);
        assert_eq!(index.page_count(), 1);
    }

    #[test]
    fn removal_releases_directory_before_retiring_and_allows_reinsertion() {
        use std::sync::mpsc;
        use std::time::{Duration, Instant};

        let index = BlockIndex::new();
        let id = block(1);
        index.init_paged(id.clone(), 4096, 8192);
        index.mark_page_allocated(&id, PageIndex(0), 4096);
        let old = index.entry(&id).unwrap();
        let read_guard = old.block.read().unwrap();
        let (removed_tx, removed_rx) = mpsc::channel();
        let (inserted_tx, inserted_rx) = mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| removed_tx.send(index.remove(&id)).unwrap());
            let deadline = Instant::now() + Duration::from_secs(5);
            let detached = loop {
                if let Ok(entries) = index.entries.try_read() {
                    if !entries.contains_key(&id) {
                        break true;
                    }
                }
                if Instant::now() >= deadline {
                    break false;
                }
                std::thread::yield_now();
            };
            let inserted = if detached {
                scope.spawn(|| {
                    index.commit_allocated(whole_meta(id.clone(), 8192), [(None, 8192)]);
                    inserted_tx
                        .send((
                            index.get(&id).unwrap(),
                            index.resident_bytes(),
                            index.page_count(),
                        ))
                        .unwrap();
                });
                Some(inserted_rx.recv_timeout(Duration::from_secs(5)))
            } else {
                None
            };
            // Release the entry before assertions: on failure the remover must
            // still be able to finish so scope can join its threads.
            drop(read_guard);
            assert!(
                detached,
                "removal held the directory while waiting for an entry"
            );
            let (meta, bytes, pages) = inserted.unwrap().expect("same-key insertion was blocked");
            assert!(matches!(meta.form, BlockForm::Whole));
            // The detached old page is still charged until retirement finishes.
            assert_eq!((bytes, pages), (4096 + 8192, 1));
            let removed = removed_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap();
            assert!(matches!(removed.form, BlockForm::Paged { .. }));
        });
        assert!(old.block.read().unwrap().is_none());
        assert!(matches!(index.get(&id).unwrap().form, BlockForm::Whole));
        assert_eq!(
            (index.len(), index.resident_bytes(), index.page_count()),
            (1, 8192, 0)
        );
        assert!(index.remove(&id).is_some());
        assert_eq!(
            (index.len(), index.resident_bytes(), index.page_count()),
            (0, 0, 0)
        );
    }

    #[test]
    fn replacement_and_removal_retire_retained_entries() {
        let index = BlockIndex::new();
        let id = block(1);
        index.init_paged(id.clone(), 4096, 8192);
        index.mark_page_allocated(&id, PageIndex(0), 4096);
        let old = index.entry(&id).unwrap();
        index.commit_allocated(whole_meta(id.clone(), 8192), [(None, 8192)]);
        assert!(old.block.read().unwrap().is_none());
        assert_eq!(index.resident_bytes(), 8192);
        assert_eq!(index.page_count(), 0);
        let replacement = index.entry(&id).unwrap();
        assert!(!Arc::ptr_eq(&old, &replacement));
        index.remove(&id);
        assert!(replacement.block.read().unwrap().is_none());
        index.init_paged(id.clone(), 4096, 8192);
        assert!(index.mark_page_allocated(&id, PageIndex(1), 4096));
        assert_eq!(index.resident_bytes(), 4096);
        assert_eq!(index.page_count(), 1);
    }

    #[test]
    fn concurrent_page_updates_replacements_and_removals_preserve_accounting() {
        use std::sync::Barrier;

        let index = BlockIndex::new();
        let start = Barrier::new(10);
        std::thread::scope(|scope| {
            for worker in 0..10 {
                let index = &index;
                let start = &start;
                scope.spawn(move || {
                    start.wait();
                    for round in 0..500 {
                        // All threads contend on the same small set, including
                        // directory removal/recreation while page updates wait.
                        let id = block(round % 4);
                        if worker >= 8 {
                            // Borrowed readers keep membership stable while
                            // racing with entry updates and directory changes.
                            let _ = index.get(&id);
                            let _ = index.presence(&id, PageIndex(0), PageIndex(2));
                            let _ = index.is_whole_and_touch(&id);
                            continue;
                        }
                        match worker % 4 {
                            0 => {
                                index.init_paged(id.clone(), 256 * 1024, 256 << 20);
                                index.mark_page_allocated(&id, PageIndex(0), 4096);
                                index.mark_page_allocated(&id, PageIndex(1), 8192);
                            }
                            1 => {
                                index.clear_page(&id, PageIndex(0));
                                index.mark_page_allocated(&id, PageIndex(1), 4096);
                            }
                            2 => index.commit_allocated(whole_meta(id, 512), [(None, 4096)]),
                            _ => {
                                index.remove(&id);
                            }
                        }
                    }
                });
            }
        });
        let units = index.snapshot_units();
        assert_eq!(
            index.resident_bytes(),
            units.iter().map(|(_, _, bytes)| bytes).sum::<u64>()
        );
        assert_eq!(
            index.page_count(),
            units.iter().filter(|(_, page, _)| page.is_some()).count() as u64
        );
        for n in 0..4 {
            index.remove(&block(n));
        }
        assert!(index.is_empty());
        assert_eq!(index.resident_bytes(), 0);
        assert_eq!(index.page_count(), 0);
    }
}
