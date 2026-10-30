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
    units: IndexedUnits,
}

enum IndexedUnits {
    Whole(UnitSlot),
    Paged(PageSlots),
}

enum PageSlots {
    Compact(CompactPages),
    // Large geometries keep sparse addressing instead of allocating a large
    // redirect table. The common 256-page geometry uses one-byte indices.
    Sparse(HashMap<PageIndex, UnitSlot>),
}

struct CompactPages {
    page_count: u16,
    // Allocated on first insertion. Missing pages may leave stale redirects;
    // the reverse mapping validates them, so all 256 u8 values remain usable.
    redirect: Box<[u8]>,
    page_ids: Vec<u8>,
    slots: Vec<UnitSlot>,
}

impl CompactPages {
    fn position(&self, page: PageIndex) -> Option<usize> {
        let position = usize::from(*self.redirect.get(page.0 as usize)?);
        (self.page_ids.get(position).copied() == Some(page.0 as u8)).then_some(position)
    }

    fn get_or_insert(&mut self, page: PageIndex) -> &mut UnitSlot {
        assert!(
            page.0 < u32::from(self.page_count),
            "page outside block geometry"
        );
        if let Some(position) = self.position(page) {
            return &mut self.slots[position];
        }
        if self.redirect.is_empty() {
            self.redirect = vec![0; usize::from(self.page_count)].into_boxed_slice();
        }
        // Bound geometric growth by the known page count, including non-power
        // of two geometries. Both vectors share the same logical length.
        if self.slots.len() == self.slots.capacity() {
            let capacity = (self.slots.capacity().max(2) * 2).min(usize::from(self.page_count));
            self.slots.reserve_exact(capacity - self.slots.len());
            self.page_ids.reserve_exact(capacity - self.page_ids.len());
        }
        let position = self.slots.len();
        self.slots.push(UnitSlot::default());
        self.page_ids.push(page.0 as u8);
        self.redirect[page.0 as usize] = position as u8;
        &mut self.slots[position]
    }

    fn remove(&mut self, page: PageIndex) -> Option<UnitSlot> {
        let position = self.position(page)?;
        let removed = self.slots.swap_remove(position);
        self.page_ids.swap_remove(position);
        if position < self.slots.len() {
            self.redirect[usize::from(self.page_ids[position])] = position as u8;
        }
        if self.slots.capacity() > 32 && self.slots.len() < self.slots.capacity() / 4 {
            self.slots.shrink_to(self.slots.len() * 2);
            self.page_ids.shrink_to(self.page_ids.len() * 2);
        }
        Some(removed)
    }
}

impl PageSlots {
    fn new(page_count: u32) -> Self {
        if page_count <= 256 {
            Self::Compact(CompactPages {
                page_count: page_count as u16,
                redirect: Box::default(),
                page_ids: Vec::new(),
                slots: Vec::new(),
            })
        } else {
            Self::Sparse(HashMap::new())
        }
    }

    fn get(&self, page: PageIndex) -> Option<&UnitSlot> {
        match self {
            Self::Compact(pages) => Some(&pages.slots[pages.position(page)?]),
            Self::Sparse(pages) => pages.get(&page),
        }
    }

    fn get_mut(&mut self, page: PageIndex) -> Option<&mut UnitSlot> {
        match self {
            Self::Compact(pages) => {
                let position = pages.position(page)?;
                Some(&mut pages.slots[position])
            }
            Self::Sparse(pages) => pages.get_mut(&page),
        }
    }

    fn get_or_insert(&mut self, page: PageIndex) -> &mut UnitSlot {
        match self {
            Self::Compact(pages) => pages.get_or_insert(page),
            Self::Sparse(pages) => pages.entry(page).or_default(),
        }
    }

    fn remove(&mut self, page: PageIndex) -> Option<UnitSlot> {
        match self {
            Self::Compact(pages) => pages.remove(page),
            Self::Sparse(pages) => {
                let removed = pages.remove(&page);
                if pages.capacity() > 32 && pages.len() < pages.capacity() / 4 {
                    pages.shrink_to(pages.len() * 2);
                }
                removed
            }
        }
    }

    fn allocated_bytes(&self) -> u64 {
        match self {
            Self::Compact(pages) => pages.slots.iter().map(|slot| slot.allocated).sum(),
            Self::Sparse(pages) => pages.values().map(|slot| slot.allocated).sum(),
        }
    }
}

#[derive(Default)]
struct UnitSlot {
    allocated: u64,
    // Recovery and admission attach access state after allocation is recorded.
    access: Option<IndexedAccess>,
}

impl IndexedUnits {
    fn get(&self, page: Option<PageIndex>) -> Option<&UnitSlot> {
        match (self, page) {
            (Self::Whole(slot), None) => Some(slot),
            (Self::Paged(pages), Some(page)) => pages.get(page),
            _ => None,
        }
    }

    fn get_mut(&mut self, page: Option<PageIndex>) -> Option<&mut UnitSlot> {
        match (self, page) {
            (Self::Whole(slot), None) => Some(slot),
            (Self::Paged(pages), Some(page)) => pages.get_mut(page),
            _ => None,
        }
    }

    fn allocated_bytes(&self) -> u64 {
        match self {
            Self::Whole(slot) => slot.allocated,
            Self::Paged(pages) => pages.allocated_bytes(),
        }
    }
}

struct IndexedAccess {
    policy: AccessHandle,
    page: Option<Arc<PageState>>,
}

impl From<BlockMeta> for IndexedBlock {
    fn from(meta: BlockMeta) -> Self {
        let units = match &meta.form {
            BlockForm::Whole => IndexedUnits::Whole(UnitSlot::default()),
            BlockForm::Paged { present, .. } => IndexedUnits::Paged(PageSlots::new(present.len())),
        };
        Self { meta, units }
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
                let access = entry.units.get(Some(PageIndex(page)))?.access.as_ref()?;
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
            if let Some(access) = entry
                .units
                .get_mut(Some(page))
                .and_then(|slot| slot.access.as_mut())
            {
                access.page = Some(state);
            }
        });
    }

    /// Startup only, before serving or starting maintenance. Attach policy
    /// tokens in place rather than cloning a full snapshot and looking every
    /// unit up again. Membership stays stable throughout the traversal.
    pub(crate) fn restore_lru(&self) -> crate::eviction::Lru {
        use crate::eviction::RecoveredUnit;
        let entries = self.entries.read().unwrap();
        let mut units = Vec::with_capacity(entries.len());
        for entry in entries.values() {
            let mut guard = entry.block.write().unwrap();
            let entry = guard.as_mut().expect("live directory entry");
            let shared = AccessHandle::default();
            let mut add = |page, slot: &mut UnitSlot| {
                let access = shared.sibling();
                match &mut slot.access {
                    Some(old) => old.policy = access.clone(),
                    None => {
                        slot.access = Some(IndexedAccess {
                            policy: access.clone(),
                            page: None,
                        })
                    }
                }
                units.push(RecoveredUnit {
                    unit: Arc::new(match page {
                        Some(page) => CacheUnit::Page(entry.meta.id.clone(), page),
                        None => CacheUnit::Whole(entry.meta.id.clone()),
                    }),
                    bytes: slot.allocated,
                    access,
                });
            };
            match &entry.meta.form {
                BlockForm::Whole => add(None, entry.units.get_mut(None).unwrap()),
                BlockForm::Paged { present, .. } => {
                    for p in 0..present.len() {
                        let page = PageIndex(p);
                        if present.is_present(page) {
                            add(Some(page), entry.units.get_mut(Some(page)).unwrap());
                        }
                    }
                }
            }
        }
        let blocks = entries.len();
        drop(entries);
        crate::eviction::Lru::from_recovered(units, blocks)
    }

    /// Startup only. Whole blocks have no page lifecycle: skip them without
    /// allocating or cloning their identities into a second full snapshot.
    pub(crate) fn restore_page_states(
        &self,
        lifecycle: &crate::page_lifecycle::PageLifecycle,
        now: u64,
    ) {
        let entries = self.entries.read().unwrap();
        for entry in entries.values() {
            let mut guard = entry.block.write().unwrap();
            let entry = guard.as_mut().expect("live directory entry");
            if let BlockForm::Paged { present, .. } = &entry.meta.form {
                if present.count() == 0 {
                    continue;
                }
                let block = lifecycle.block(&entry.meta.id);
                for p in 0..present.len() {
                    let page = PageIndex(p);
                    if present.is_present(page) {
                        if let Some(access) = entry
                            .units
                            .get_mut(Some(page))
                            .and_then(|s| s.access.as_mut())
                        {
                            access.page = Some(block.register(page, None, now, false));
                        }
                    }
                }
            }
        }
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
            if let Some(access) = entry.units.get(None).and_then(|slot| slot.access.as_ref()) {
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
            if let Some(access) = entry.units.get(None).and_then(|slot| slot.access.as_ref()) {
                access.policy.touch();
            }
            true
        })
        .unwrap_or(false)
    }

    /// Access metadata only; does not count as a hit or establish residency.
    pub(crate) fn whole_access(&self, id: &BlockId) -> Option<AccessHandle> {
        self.read_entry(id, |entry| {
            entry
                .units
                .get(None)
                .and_then(|slot| slot.access.as_ref())
                .map(|a| a.policy.clone())
        })
        .flatten()
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
                let slot = entry.units.get_mut(page).expect("resident unit has a slot");
                if let Some(old) = slot.access.as_mut() {
                    old.policy = access;
                } else {
                    slot.access = Some(IndexedAccess {
                        policy: access,
                        page: None,
                    });
                }
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
                    BlockForm::Whole => out.push((
                        meta.id.clone(),
                        None,
                        entry
                            .units
                            .get(None)
                            .expect("whole unit has a slot")
                            .allocated,
                    )),
                    BlockForm::Paged { present, .. } => {
                        for p in 0..present.len() {
                            let page = PageIndex(p);
                            if present.is_present(page) {
                                out.push((
                                    meta.id.clone(),
                                    Some(page),
                                    entry
                                        .units
                                        .get(Some(page))
                                        .expect("resident page has a slot")
                                        .allocated,
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
        let pages = materialized_pages(&meta);
        let id = meta.id.clone();
        let mut entry = IndexedBlock::from(meta);
        match &mut entry.units {
            IndexedUnits::Whole(slot) => {
                for (page, bytes) in allocated {
                    assert!(
                        page.is_none(),
                        "whole allocation must not have a page index"
                    );
                    slot.allocated = bytes;
                }
            }
            IndexedUnits::Paged(slots) => {
                for (page, allocated) in allocated {
                    slots
                        .get_or_insert(page.expect("paged allocation must have a page index"))
                        .allocated = allocated;
                }
            }
        }
        let added = entry.units.allocated_bytes();
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
            .map_or(0, |old| old.units.allocated_bytes());
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
        // Potentially large page tables and bitmaps are freed outside directory locks.
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
            let IndexedUnits::Paged(pages) = &mut entry.units else {
                unreachable!("paged metadata has a page table");
            };
            let old = std::mem::replace(&mut pages.get_or_insert(page).allocated, allocated);
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
            let IndexedUnits::Paged(pages) = &mut entry.units else {
                unreachable!("paged metadata has a page table");
            };
            let bytes = pages
                .remove(page)
                .expect("resident page has a slot")
                .allocated;
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
        self.replace_bytes(removed.units.allocated_bytes(), 0);
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
    fn sparse_page_slots_preserve_partial_admission_and_clear_all_state() {
        let index = BlockIndex::new();
        let id = block(1);
        let page = PageIndex(255);
        let end = PageIndex(256);
        let unit = CacheUnit::Page(id.clone(), page);
        let life = crate::page_lifecycle::PageLifecycle::new();
        let state = life.block(&id).register(page, None, 0, false);
        index.init_paged(id.clone(), 1 << 20, u64::from(id.block_size));
        // Attaching state to an absent page must not allocate a sparse slot.
        index.set_access(&unit, AccessHandle::default());
        index.set_page_state(&id, page, state.clone());
        index.read_entry(&id, |entry| {
            let IndexedUnits::Paged(PageSlots::Compact(pages)) = &entry.units else {
                panic!("expected paged entry");
            };
            assert!(pages.redirect.is_empty());
            assert_eq!(pages.slots.capacity(), 0);
            assert_eq!(pages.page_ids.capacity(), 0);
        });

        // A resident sparse file can have zero allocation. Presence and access
        // publication must not be inferred from its byte cost.
        assert!(index.mark_page_allocated(&id, page, 0));
        assert_eq!(index.presence(&id, page, end), Presence::PageHit);
        assert_eq!(index.snapshot_units(), vec![(id.clone(), Some(page), 0)]);
        assert!(index.page_reads_and_touch(&id, page, end).is_none());
        index.set_page_state(&id, page, state.clone()); // No policy attached yet.
        index.set_access(&unit, AccessHandle::default());
        assert!(index.page_reads_and_touch(&id, page, end).is_none());
        index.set_page_state(&id, page, state);
        assert!(index.page_reads_and_touch(&id, page, end).is_some());

        // Replacing either allocation or policy preserves the lifecycle handle.
        let bytes = u64::from(u32::MAX) + 1;
        assert!(!index.mark_page_allocated(&id, page, bytes));
        index.set_access(&unit, AccessHandle::default());
        assert!(index.page_reads_and_touch(&id, page, end).is_some());
        assert_eq!(index.resident_bytes(), bytes);
        assert_eq!(index.page_count(), 1);
        assert!(index.clear_page(&id, page));
        assert_eq!(index.resident_bytes(), 0);
        assert!(index.snapshot_units().is_empty());
        // Readmission must not inherit the removed page's access/state.
        assert!(index.mark_page_allocated(&id, page, 4096));
        assert!(index.page_reads_and_touch(&id, page, end).is_none());
        index.remove(&id);
        assert_eq!((index.resident_bytes(), index.page_count()), (0, 0));
    }

    #[test]
    fn compact_full_block_moves_last_slot_without_losing_access_state() {
        let index = BlockIndex::new();
        let id = block(1);
        index.init_paged(id.clone(), 1 << 20, u64::from(id.block_size));
        for page in 0..256 {
            assert!(index.mark_page_allocated(&id, PageIndex(page), u64::from(page) + 1));
        }
        let last = PageIndex(255);
        let life = crate::page_lifecycle::PageLifecycle::new();
        index.set_access(&CacheUnit::Page(id.clone(), last), AccessHandle::default());
        index.set_page_state(&id, last, life.block(&id).register(last, None, 0, false));
        assert!(index.clear_page(&id, PageIndex(0)));
        assert!(index
            .page_reads_and_touch(&id, last, PageIndex(256))
            .is_some());
        assert_eq!(index.snapshot_units().last().unwrap().2, 256);
        assert_eq!(
            index.presence(&id, PageIndex(0), PageIndex(1)),
            Presence::PageMiss
        );
        // A removed page's stale redirect must not target the moved page.
        index.set_access(
            &CacheUnit::Page(id.clone(), PageIndex(0)),
            AccessHandle::default(),
        );
        assert!(!index.clear_page(&id, PageIndex(0)));
        assert!(index.mark_page_allocated(&id, PageIndex(0), 0));
        assert!(index
            .page_reads_and_touch(&id, PageIndex(0), PageIndex(1))
            .is_none());
        assert!(index
            .page_reads_and_touch(&id, last, PageIndex(256))
            .is_some());
        assert!(index.clear_page(&id, last));
        assert!(index.mark_page_allocated(&id, last, 4096));
        assert!(index
            .page_reads_and_touch(&id, last, PageIndex(256))
            .is_none());
        assert_eq!(index.page_count(), 256);
        index.remove(&id);
        assert_eq!(index.resident_bytes(), 0);
    }

    #[test]
    fn page_slot_churn_matches_residency_across_geometry_boundaries() {
        for count in [0, 1, 17, 256, 257, 1024] {
            let index = BlockIndex::new();
            let mut id = block(1);
            id.block_size = count * 4096;
            index.init_paged(id.clone(), 4096, u64::from(id.block_size));
            assert!(!index.mark_page_allocated(&id, PageIndex(count), 4096));
            assert!(!index.clear_page(&id, PageIndex(u32::MAX)));
            let mut expected = vec![None; count as usize];
            let mut rng = 0x123456789abcdefu64;
            for round in 0..(count * 8) {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                let page = (rng % u64::from(count)) as u32;
                let slot = &mut expected[page as usize];
                if rng & 1 == 0 {
                    let bytes = if round % 7 == 0 {
                        0
                    } else {
                        u64::from(round) << 24
                    };
                    assert_eq!(
                        index.mark_page_allocated(&id, PageIndex(page), bytes),
                        slot.is_none()
                    );
                    *slot = Some(bytes);
                } else {
                    assert_eq!(
                        index.clear_page(&id, PageIndex(page)),
                        slot.take().is_some()
                    );
                }
                if round % 31 == 0 {
                    let units: Vec<_> = expected
                        .iter()
                        .enumerate()
                        .filter_map(|(p, bytes)| {
                            bytes.map(|bytes| (id.clone(), Some(PageIndex(p as u32)), bytes))
                        })
                        .collect();
                    assert_eq!(index.snapshot_units(), units);
                    assert_eq!(
                        index.resident_bytes(),
                        expected.iter().flatten().sum::<u64>()
                    );
                    assert_eq!(index.page_count(), units.len() as u64);
                }
            }
            for page in 0..count {
                assert_eq!(
                    index.clear_page(&id, PageIndex(page)),
                    expected[page as usize].is_some()
                );
            }
            assert_eq!((index.resident_bytes(), index.page_count()), (0, 0));
            if count > 0 {
                // Refill after shrinking the vectors and invalidating redirects.
                assert!(index.mark_page_allocated(&id, PageIndex(count - 1), 4096));
                assert_eq!(
                    index.snapshot_units(),
                    vec![(id.clone(), Some(PageIndex(count - 1)), 4096)]
                );
            }
            index.remove(&id);
            assert_eq!((index.resident_bytes(), index.page_count()), (0, 0));
        }
    }

    #[test]
    fn recovered_compact_slots_accept_unordered_and_duplicate_allocations() {
        let index = BlockIndex::new();
        let id = block(1);
        let mut present = PresentBitmap::new(256);
        present.set(PageIndex(7));
        present.set(PageIndex(255));
        index.commit_allocated(
            BlockMeta {
                id: id.clone(),
                form: BlockForm::Paged {
                    page_size: 1 << 20,
                    present,
                },
                len: u64::from(id.block_size),
            },
            [
                (Some(PageIndex(255)), 4096),
                (Some(PageIndex(7)), 0),
                (Some(PageIndex(255)), 8192),
            ],
        );
        assert_eq!(
            index.snapshot_units(),
            vec![
                (id.clone(), Some(PageIndex(7)), 0),
                (id.clone(), Some(PageIndex(255)), 8192)
            ]
        );
        assert_eq!((index.resident_bytes(), index.page_count()), (8192, 2));
        assert!(index.clear_page(&id, PageIndex(255)));
        assert_eq!(
            index.snapshot_units(),
            vec![(id.clone(), Some(PageIndex(7)), 0)]
        );
        assert!(index.clear_page(&id, PageIndex(7)));
        assert_eq!((index.resident_bytes(), index.page_count()), (0, 0));
    }

    #[test]
    fn startup_attaches_policy_and_lifecycle_to_sparse_residents() {
        // Exercise both compact and sparse page tables, including zero-byte
        // allocations, absent pages, whole blocks, and data-path token touches.
        for pages in [256, 512] {
            let index = BlockIndex::new();
            let whole = block(1);
            index.commit_allocated(
                BlockMeta {
                    id: whole.clone(),
                    form: BlockForm::Whole,
                    len: 17,
                },
                [(None, 4096)],
            );
            let paged = block(2);
            index.init_paged(
                paged.clone(),
                paged.block_size / pages,
                u64::from(paged.block_size),
            );
            index.mark_page_allocated(&paged, PageIndex(0), 0);
            index.mark_page_allocated(&paged, PageIndex(pages - 1), 8192);
            let lru = index.restore_lru();
            let lifecycle = crate::page_lifecycle::PageLifecycle::new();
            index.restore_page_states(&lifecycle, 10);
            assert_eq!(lru.len(), 3);
            assert_eq!(lru.total_bytes(), index.resident_bytes());
            assert_eq!(index.page_count(), 2);
            assert!(index
                .page_reads_and_touch(&paged, PageIndex(1), PageIndex(2))
                .is_none());
            let candidates = lru.block_candidates(&paged);
            assert_eq!(candidates.len(), 2);
            assert!(index
                .page_reads_and_touch(&paged, PageIndex(0), PageIndex(1))
                .is_some());
            assert!(index
                .page_reads_and_touch(&paged, PageIndex(pages - 1), PageIndex(pages))
                .is_some());
            assert!(candidates.iter().all(|c| !lru.candidate_is_current(c)));
            let candidate = lru.block_candidates(&whole).pop().unwrap();
            assert!(index.get_and_touch(&whole).is_some());
            assert!(!lru.candidate_is_current(&candidate));
        }
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
            .read_entry(&id, |entry| entry.units.get(Some(PageIndex(1))).is_none())
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
