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

use crate::eviction::{AccessHandle, CacheUnit, PolicyEntry, PolicyState};
use crate::page_lifecycle::{BlockState, PageEntry, PageReadGuard, PageTable};

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
    entries: RwLock<HashMap<Arc<BlockId>, DirectoryEntry>>,
    resident_blocks: AtomicU64,
    resident_bytes: AtomicU64,
    resident_pages: AtomicU64,
}

#[derive(Default)]
struct DirectoryEntry {
    resident: Option<Arc<Entry>>,
    lifecycle: Option<Arc<BlockState>>,
    policy: PolicyUnits,
}

#[derive(Default)]
struct PolicyUnits {
    whole: Option<Arc<RwLock<UnitSlot>>>,
    pages: Option<PageTable>,
    len: usize,
}

impl DirectoryEntry {
    fn is_empty(&self) -> bool {
        self.resident.is_none() && self.lifecycle.is_none() && self.policy.len == 0
    }
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
    Whole(Arc<RwLock<UnitSlot>>),
    Paged(PageTable),
}

#[derive(Default)]
pub(crate) struct UnitSlot {
    allocated: u64,
    access: Option<AccessHandle>,
    policy: Option<PolicyState>,
}

impl IndexedUnits {
    fn read<T>(&self, page: Option<PageIndex>, f: impl FnOnce(&UnitSlot) -> T) -> Option<T> {
        match (self, page) {
            (Self::Whole(slot), None) => Some(f(&slot.read().unwrap())),
            (Self::Paged(pages), Some(page)) => pages
                .read()
                .unwrap()
                .get(&page.0)
                .map(|entry| f(&entry.unit)),
            _ => None,
        }
    }
    fn write<T>(
        &mut self,
        page: Option<PageIndex>,
        f: impl FnOnce(&mut UnitSlot) -> T,
    ) -> Option<T> {
        match (self, page) {
            (Self::Whole(slot), None) => Some(f(&mut slot.write().unwrap())),
            (Self::Paged(pages), Some(page)) => pages
                .write()
                .unwrap()
                .get_mut(&page.0)
                .map(|entry| f(&mut entry.unit)),
            _ => None,
        }
    }
    fn allocated_bytes(&self) -> u64 {
        match self {
            Self::Whole(slot) => slot.read().unwrap().allocated,
            Self::Paged(pages) => pages
                .read()
                .unwrap()
                .values()
                .map(|entry| entry.unit.allocated)
                .sum(),
        }
    }
    fn page_table(&self) -> Option<PageTable> {
        match self {
            Self::Paged(pages) => Some(pages.clone()),
            _ => None,
        }
    }
}

impl From<BlockMeta> for IndexedBlock {
    fn from(meta: BlockMeta) -> Self {
        let units = match &meta.form {
            BlockForm::Whole => IndexedUnits::Whole(Arc::default()),
            BlockForm::Paged { .. } => IndexedUnits::Paged(Arc::default()),
        };
        Self { meta, units }
    }
}

impl IndexedBlock {
    fn allocated(
        meta: BlockMeta,
        allocated: impl IntoIterator<Item = (Option<PageIndex>, u64)>,
    ) -> Self {
        let mut entry = Self::from(meta);
        match &mut entry.units {
            IndexedUnits::Whole(slot) => {
                for (page, bytes) in allocated {
                    assert!(
                        page.is_none(),
                        "whole allocation must not have a page index"
                    );
                    slot.write().unwrap().allocated = bytes;
                }
            }
            IndexedUnits::Paged(slots) => {
                for (page, allocated) in allocated {
                    let page = page.expect("paged allocation must have a page index");
                    let BlockForm::Paged { present, .. } = &entry.meta.form else {
                        unreachable!()
                    };
                    assert!(page.0 < present.len(), "page outside block geometry");
                    slots
                        .write()
                        .unwrap()
                        .get_or_insert_with(page.0, PageEntry::unbound)
                        .unit
                        .allocated = allocated;
                }
            }
        }
        entry
    }
}

impl BlockIndex {
    pub(crate) fn policy_get(&self, unit: &CacheUnit) -> Option<PolicyEntry> {
        let entries = self.entries.read().unwrap();
        let policy = &entries.get(unit.block())?.policy;
        let read = |slot: &UnitSlot| {
            Some(PolicyEntry {
                state: slot.policy?,
                access: slot.access.as_ref()?.clone(),
            })
        };
        match unit.page() {
            None => read(&policy.whole.as_ref()?.read().unwrap()),
            Some(page) => read(&policy.pages.as_ref()?.read().unwrap().get(&page.0)?.unit),
        }
    }

    pub(crate) fn policy_insert(&self, unit: &CacheUnit, entry: PolicyEntry) {
        let mut entries = self.entries.write().unwrap();
        let directory = if let Some(directory) = entries.get_mut(unit.block()) {
            directory
        } else {
            entries.entry(Arc::new(unit.block().clone())).or_default()
        };
        let resident = directory
            .resident
            .as_ref()
            .and_then(|resident| {
                let block = resident.block.read().unwrap();
                block.as_ref().map(|block| match &block.units {
                    IndexedUnits::Whole(slot) => (Some(slot.clone()), None),
                    IndexedUnits::Paged(pages) => (None, Some(pages.clone())),
                })
            })
            .unwrap_or_default();
        let put = |slot: &mut UnitSlot| {
            let fresh = slot.policy.replace(entry.state).is_none();
            slot.access = Some(entry.access);
            fresh
        };
        let fresh = match unit.page() {
            None => {
                let slot = directory
                    .policy
                    .whole
                    .get_or_insert_with(|| resident.0.unwrap_or_default());
                put(&mut slot.write().unwrap())
            }
            Some(page) => {
                let pages = directory
                    .policy
                    .pages
                    .get_or_insert_with(|| resident.1.unwrap_or_default());
                put(&mut pages
                    .write()
                    .unwrap()
                    .get_or_insert_with(page.0, PageEntry::unbound)
                    .unit)
            }
        };
        directory.policy.len += usize::from(fresh);
    }

    pub(crate) fn policy_update<T>(
        &self,
        unit: &CacheUnit,
        update: impl FnOnce(&mut PolicyState) -> T,
    ) -> Option<T> {
        let entries = self.entries.read().unwrap();
        let policy = &entries.get(unit.block())?.policy;
        match unit.page() {
            None => Some(update(
                policy.whole.as_ref()?.write().unwrap().policy.as_mut()?,
            )),
            Some(page) => Some(update(
                policy
                    .pages
                    .as_ref()?
                    .write()
                    .unwrap()
                    .get_mut(&page.0)?
                    .unit
                    .policy
                    .as_mut()?,
            )),
        }
    }

    pub(crate) fn policy_remove(&self, unit: &CacheUnit) -> Option<PolicyEntry> {
        let mut entries = self.entries.write().unwrap();
        let directory = entries.get_mut(unit.block())?;
        let take = |slot: &mut UnitSlot| {
            Some(PolicyEntry {
                state: slot.policy.take()?,
                access: slot.access.as_ref().expect("policy access token").clone(),
            })
        };
        let removed = match unit.page() {
            None => take(&mut directory.policy.whole.as_ref()?.write().unwrap())?,
            Some(page) => {
                let resident = directory
                    .resident
                    .as_ref()
                    .map(|entry| entry.block.read().unwrap());
                let present = resident.as_ref().and_then(|entry| entry.as_ref()).is_some_and(|entry| {
                    matches!(&entry.meta.form, BlockForm::Paged { present, .. } if present.is_present(page))
                });
                let mut pages = directory.policy.pages.as_ref()?.write().unwrap();
                let entry = pages.get_mut(&page.0)?;
                let removed = take(&mut entry.unit)?;
                if !present && entry.unit.allocated == 0 && !entry.handle.is_bound() {
                    pages.remove(&page.0);
                }
                removed
            }
        };
        directory.policy.len -= 1;
        if directory.policy.len == 0 {
            directory.policy = PolicyUnits::default();
        }
        if directory.is_empty() {
            entries.remove(unit.block());
        }
        Some(removed)
    }
    /// Startup-only bulk restoration. Exclusive ownership prevents publication
    /// to readers while workers build page tables without the directory lock.
    pub(crate) fn restore_allocated(&mut self, blocks: Vec<StoredBlock>, threads: usize) {
        self.try_restore_allocated(blocks, threads, |block| {
            Ok::<_, std::convert::Infallible>((block.meta, block.allocated))
        })
        .unwrap();
    }

    /// Decode and construct entries in parallel, then publish with exclusive
    /// ownership. A decode error leaves the existing index unchanged. Small
    /// whole objects can supply inline allocations instead of a Vec per object.
    pub(crate) fn try_restore_allocated<T: Send, A, E: Send>(
        &mut self,
        input: Vec<T>,
        threads: usize,
        decode: impl Fn(T) -> Result<(BlockMeta, A), E> + Sync,
    ) -> Result<(), E>
    where
        A: IntoIterator<Item = (Option<PageIndex>, u64)>,
    {
        let built = crate::recovery::map(input, threads, |item| {
            let (meta, allocated) = decode(item)?;
            let id = meta.id.clone();
            let entry = IndexedBlock::allocated(meta, allocated);
            let bytes = entry.units.allocated_bytes();
            let pages = materialized_pages(&entry.meta);
            Ok((id, Arc::new(Entry::new(entry)), bytes, pages))
        })
        .into_iter()
        .collect::<Result<Vec<_>, E>>()?;
        let entries = self.entries.get_mut().unwrap();
        entries.reserve(built.len());
        let bytes = self.resident_bytes.get_mut();
        let pages = self.resident_pages.get_mut();
        for (id, entry, added_bytes, added_pages) in built {
            let slot = entries.entry(Arc::new(id)).or_default();
            if let Some(old) = slot.resident.replace(entry) {
                let old = old
                    .block
                    .write()
                    .unwrap()
                    .take()
                    .expect("live recovery entry");
                *bytes -= old.units.allocated_bytes();
                *pages -= materialized_pages(&old.meta);
            } else {
                *self.resident_blocks.get_mut() += 1;
            }
            *bytes += added_bytes;
            *pages += added_pages;
        }
        Ok(())
    }

    pub(crate) fn lifecycle(&self, id: &BlockId) -> Option<Arc<BlockState>> {
        self.entries.read().unwrap().get(id)?.lifecycle.clone()
    }

    pub(crate) fn lifecycle_or_insert(
        &self,
        id: &BlockId,
        create: impl FnOnce(Arc<BlockId>, Option<PageTable>) -> Arc<BlockState>,
    ) -> Arc<BlockState> {
        if let Some(block) = self.lifecycle(id) {
            return block;
        }
        let mut entries = self.entries.write().unwrap();
        let key = entries
            .get_key_value(id)
            .map(|(id, _)| id.clone())
            .unwrap_or_else(|| Arc::new(id.clone()));
        let slot = entries.entry(key.clone()).or_default();
        if let Some(block) = &slot.lifecycle {
            return block.clone();
        }
        let pages = slot.resident.as_ref().and_then(|entry| {
            entry
                .block
                .read()
                .unwrap()
                .as_ref()
                .and_then(|block| block.units.page_table())
        });
        let block = create(key, pages);
        slot.lifecycle = Some(block.clone());
        block
    }

    // Caller owns the lifecycle registry and has checked the empty block under
    // its lock. The directory and registry are its only remaining owners.
    pub(crate) fn retire_lifecycle(&self, id: &BlockId, block: &Arc<BlockState>) -> bool {
        let mut entries = self.entries.write().unwrap();
        let Some(slot) = entries.get_mut(id) else {
            return false;
        };
        if !slot
            .lifecycle
            .as_ref()
            .is_some_and(|old| Arc::ptr_eq(old, block))
            || Arc::strong_count(block) != 2
            || !block.retire_if_empty()
        {
            return false;
        }
        slot.lifecycle = None;
        if slot.is_empty() {
            entries.remove(id);
        }
        true
    }

    fn entry(&self, id: &BlockId) -> Option<Arc<Entry>> {
        self.entries.read().unwrap().get(id)?.resident.clone()
    }

    fn read_entry<T>(&self, id: &BlockId, read: impl FnOnce(&IndexedBlock) -> T) -> Option<T> {
        let entries = self.entries.read().unwrap();
        let entry = entries.get(id)?.resident.as_ref()?;
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
            .filter_map(|entry| entry.resident.as_ref())
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
            let IndexedUnits::Paged(pages) = &entry.units else {
                unreachable!()
            };
            let pages = pages.read().unwrap();
            let acquire = |page| {
                let state = pages.get(&page)?;
                let access = state.unit.access.as_ref()?;
                if !state.handle.is_bound() {
                    return None;
                }
                let guard = state.handle.acquire()?;
                access.touch();
                Some(guard)
            };
            let first = acquire(start.0)?;
            let tail: Option<Vec<_>> = (start.0 + 1..end.0).map(acquire).collect();
            Some((first, tail?))
        })?
    }

    /// Startup only, before serving or starting maintenance. Attach policy
    /// tokens in place rather than cloning a full snapshot and looking every
    /// unit up again. Membership stays stable throughout the traversal.
    #[cfg(test)]
    pub(crate) fn restore_lru(self: &Arc<Self>) -> crate::eviction::Lru {
        crate::eviction::Lru::from_recovered(
            self.clone(),
            self.restore_policy_units(),
            self.len(),
            true,
        )
    }

    pub(crate) fn restore_policy_units(&self) -> Vec<crate::eviction::RecoveredUnit> {
        use crate::eviction::RecoveredUnit;
        let started = std::time::Instant::now();
        let entries = self.entries.read().unwrap();
        let mut units = Vec::with_capacity(entries.len());
        for entry in entries.values().filter_map(|entry| entry.resident.as_ref()) {
            let mut guard = entry.block.write().unwrap();
            let entry = guard.as_mut().expect("live directory entry");
            let shared = AccessHandle::default();
            let mut first = true;
            let mut add = |page: Option<PageIndex>, slot: &mut UnitSlot| {
                let access = if first {
                    first = false;
                    shared.clone()
                } else {
                    shared.sibling()
                };
                slot.access = Some(access.clone());
                units.push(RecoveredUnit {
                    independent: false,
                    unit: Arc::new(match page {
                        Some(page) => CacheUnit::Page(entry.meta.id.clone(), page),
                        None => CacheUnit::Whole(entry.meta.id.clone()),
                    }),
                    bytes: slot.allocated,
                    access,
                });
            };
            match &entry.meta.form {
                BlockForm::Whole => {
                    entry.units.write(None, |slot| add(None, slot));
                }
                BlockForm::Paged { present, .. } => {
                    let IndexedUnits::Paged(pages) = &entry.units else {
                        unreachable!()
                    };
                    for (&page, state) in pages.write().unwrap().iter_mut() {
                        let page = PageIndex(page);
                        if present.is_present(page) {
                            add(Some(page), &mut state.unit);
                        }
                    }
                }
            }
        }
        tracing::info!(
            blocks = entries.len(),
            units = units.len(),
            tokens_ms = started.elapsed().as_secs_f64() * 1000.0,
            "ordinary cache policy token recovery"
        );
        units
    }

    /// Startup only. Whole blocks have no page lifecycle: skip them without
    /// allocating or cloning their identities into a second full snapshot.
    pub(crate) fn restore_page_states(&self, lifecycle: &crate::page_lifecycle::PageLifecycle) {
        let entries: Vec<_> = self
            .entries
            .read()
            .unwrap()
            .iter()
            .filter_map(|(id, entry)| {
                entry
                    .resident
                    .as_ref()
                    .map(|entry| (id.clone(), entry.clone()))
            })
            .collect();
        for (id, entry) in entries {
            let paged = {
                let guard = entry.block.read().unwrap();
                let entry = guard.as_ref().expect("live directory entry");
                matches!(&entry.meta.form, BlockForm::Paged { present, .. } if present.count() > 0)
            };
            if paged {
                lifecycle.block(&id);
            }
        }
    }

    /// Create an empty index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of blocks currently tracked.
    pub fn len(&self) -> usize {
        self.resident_blocks.load(Ordering::Relaxed) as usize
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
            entry.units.read(None, |slot| {
                if let Some(access) = &slot.access {
                    access.touch();
                }
            });
            entry.meta.clone()
        })
    }

    /// Probe the whole-block fast path without touching any paged entries.
    pub(crate) fn is_whole_and_touch(&self, id: &BlockId) -> bool {
        self.read_entry(id, |entry| {
            if !matches!(entry.meta.form, BlockForm::Whole) {
                return false;
            }
            entry.units.read(None, |slot| {
                if let Some(access) = &slot.access {
                    access.touch();
                }
            });
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
                entry
                    .units
                    .write(page, |slot| slot.access = Some(access))
                    .expect("resident unit has a slot");
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
                            .read(None, |slot| slot.allocated)
                            .expect("whole unit has a slot"),
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
                                        .read(Some(page), |slot| slot.allocated)
                                        .expect("resident page has a slot"),
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
        let mut entry = IndexedBlock::allocated(meta, allocated);
        let added = entry.units.allocated_bytes();
        let mut entries = self.entries.write().unwrap();
        // Structural mutations take directory -> entry. Ordinary operations
        // never reacquire the directory while holding an entry lock.
        let slot = entries.entry(Arc::new(id)).or_default();
        let previous = slot.resident.as_ref().map(|old| {
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
        if let (Some(life), IndexedUnits::Paged(pages)) = (&slot.lifecycle, &mut entry.units) {
            let mut state = life.inner.lock().unwrap();
            for page in pages.read().unwrap().values() {
                page.handle.bind_work(state.work.clone());
            }
            state.pages = pages.clone();
        }
        if slot.resident.replace(Arc::new(Entry::new(entry))).is_none() {
            self.resident_blocks.fetch_add(1, Ordering::Relaxed);
        }
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
        if self.entry(&id).is_some() {
            // Already tracked — either paged (keep its bitmap) or whole (fully
            // resident, nothing to page in). Either way, do not reset it.
            return false;
        }
        let page_count = id.page_count(page_size);
        let mut entry = Entry::new(
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
        let slot = entries.entry(Arc::new(id)).or_default();
        if slot.resident.is_some() {
            return false;
        }
        if let Some(life) = &slot.lifecycle {
            life.inner.lock().unwrap().pages = entry
                .block
                .get_mut()
                .unwrap()
                .as_ref()
                .unwrap()
                .units
                .page_table()
                .unwrap();
        }
        slot.resident = Some(Arc::new(entry));
        self.resident_blocks.fetch_add(1, Ordering::Relaxed);
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
            let mut pages = pages.write().unwrap();
            let slot = pages.get_or_insert_with(page.0, PageEntry::unbound);
            let old = std::mem::replace(&mut slot.unit.allocated, allocated);
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
            let mut pages = pages.write().unwrap();
            let slot = pages.get_mut(&page.0).expect("resident page has a slot");
            let bytes = std::mem::take(&mut slot.unit.allocated);
            if slot.unit.policy.is_none() {
                slot.unit.access = None;
            }
            if !slot.handle.is_bound() && slot.unit.policy.is_none() {
                pages.remove(&page.0);
            }
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
        let slot = entries.get_mut(id)?;
        let entry = slot.resident.take()?;
        if slot.is_empty() {
            entries.remove(id);
        }
        self.resident_blocks.fetch_sub(1, Ordering::Relaxed);
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
    fn lifecycle_directory_preserves_gates_across_residency_changes() {
        let index = Arc::new(BlockIndex::new());
        let life = crate::page_lifecycle::PageLifecycle::with_index(index.clone());
        let id = block(1);
        let pending = life.block(&id);
        assert_eq!(index.len(), 0);
        assert!(index.snapshot_lens().is_empty());
        assert!(index.get(&id).is_none());

        index.init_paged(id.clone(), 4096, 4096);
        index.mark_page_allocated(&id, PageIndex(0), 4096);
        assert!(Arc::ptr_eq(&pending, &life.block(&id)));
        pending.register(PageIndex(0), None, 0, false);
        let candidate = pending.candidate(PageIndex(0)).unwrap();
        assert!(pending.claim(&candidate, 0, None));
        index.clear_page(&id, PageIndex(0));
        pending.finish(&candidate, 0);
        pending.inner.lock().unwrap().cleanup_pending = false;
        drop(candidate);
        index.remove(&id).unwrap();
        assert_eq!(index.len(), 0);
        life.retire_empty(&id);
        assert!(Arc::ptr_eq(&pending, &life.block(&id)));
        index.commit(whole_meta(id.clone(), 4096));
        assert!(Arc::ptr_eq(&pending, &life.block(&id)));
        drop(pending);
        life.retire_empty(&id);
        assert!(life.get(&id).is_none());
        assert_eq!(index.len(), 1);
        assert_eq!(index.resident_bytes(), 4096);
        index.remove(&id).unwrap();
        assert!(index.entries.read().unwrap().is_empty());
    }

    #[test]
    fn lifecycle_scan_retires_unpublished_blocks_from_the_shared_directory() {
        let index = Arc::new(BlockIndex::new());
        let life = crate::page_lifecycle::PageLifecycle::with_index(index.clone());
        for n in 0..100 {
            drop(life.block(&block(n)));
        }
        assert!(index.is_empty());
        let (_, report) = life.scan(
            &mut crate::page_lifecycle::ScanCursor::default(),
            1000,
            100,
            None,
            1000,
        );
        assert!(report.completed);
        assert!(index.entries.read().unwrap().is_empty());
    }

    #[test]
    fn shared_pages_keep_retry_and_read_generations_across_storage_transitions() {
        let index = Arc::new(BlockIndex::new());
        let life = crate::page_lifecycle::PageLifecycle::with_index(index.clone());
        let id = block(1);
        index.init_paged(id.clone(), 4096, 8192);
        let block = life.block(&id);
        let p0 = PageIndex(0);
        let old = block.register(p0, Some(10), 10, false);
        index.mark_page_allocated(&id, p0, 4096);
        let read = old.acquire().unwrap();
        let candidate = block.candidate(p0).unwrap();
        assert!(!block.claim(&candidate, 100, None));

        block.register(PageIndex(1), Some(10), 10, false);
        index.mark_page_allocated(&id, PageIndex(1), 8192);
        drop(read);
        assert!(block.claim(&candidate, 100, None));
        block.abort(&candidate, 1);
        assert_eq!(index.resident_bytes(), 12288);
        assert!(old.acquire().is_some());
        let candidate = block.candidate(p0).unwrap();
        assert!(block.claim(&candidate, 100, None));
        assert!(index.clear_page(&id, p0));
        block.finish(&candidate, 100);
        assert_eq!(index.resident_bytes(), 8192);
        assert!(old.acquire().is_none());

        let fresh = block.register(p0, Some(101), 101, false);
        index.mark_page_allocated(&id, p0, 16384);
        assert!(!block.claim(&candidate, 102, None));
        block.abort(&candidate, 1);
        block.finish(&candidate, 102);
        assert!(fresh.acquire().is_some());
        assert_eq!(index.resident_bytes(), 24576);
        assert_eq!(index.page_count(), 2);
    }

    #[test]
    fn exclusive_parallel_recovery_matches_commits_and_retires_replacements() {
        fn input() -> Vec<StoredBlock> {
            (0..5000)
                .map(|n| {
                    let mut present = PresentBitmap::new(256);
                    present.set(PageIndex(0));
                    present.set(PageIndex(7));
                    StoredBlock {
                        meta: BlockMeta {
                            id: block(n),
                            len: 256 << 20,
                            form: BlockForm::Paged {
                                page_size: 1 << 20,
                                present,
                            },
                        },
                        allocated: vec![(Some(PageIndex(7)), 8192), (Some(PageIndex(0)), 0)],
                    }
                })
                .chain(std::iter::once(StoredBlock {
                    meta: whole_meta(block(3), 10),
                    allocated: vec![(None, 4096)],
                }))
                .collect()
        }
        let serial = BlockIndex::new();
        let mut parallel = BlockIndex::new();
        for index in [&serial, &parallel] {
            index.commit_allocated(whole_meta(block(9000), 20), [(None, 8192)]);
            index.commit_allocated(whole_meta(block(0), 20), [(None, 4096)]);
        }
        let retired = parallel.entry(&block(0)).unwrap();
        for block in input() {
            serial.commit_allocated(block.meta, block.allocated);
        }
        parallel.restore_allocated(input(), 4);
        assert!(retired.block.read().unwrap().is_none());
        assert_eq!(parallel.resident_bytes(), serial.resident_bytes());
        assert_eq!(parallel.page_count(), serial.page_count());
        assert_eq!(parallel.len(), serial.len());
        for n in (0..5000).chain(std::iter::once(9000)) {
            assert_eq!(parallel.get(&block(n)), serial.get(&block(n)));
        }
    }

    #[test]
    fn parallel_decode_failure_does_not_publish_partial_index() {
        let mut index = BlockIndex::new();
        let original = whole_meta(block(9), 17);
        index.commit_allocated(original.clone(), [(None, 4096)]);
        let error = index.try_restore_allocated((0..4096).collect(), 2, |n| {
            if n == 2048 {
                return Err("invalid recovery record");
            }
            Ok((whole_meta(block(n), 32), [(None, 8192)]))
        });
        assert_eq!(error, Err("invalid recovery record"));
        assert_eq!(index.len(), 1);
        assert_eq!(index.get(&block(9)), Some(original));
        assert_eq!(index.resident_bytes(), 4096);
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
        let index = Arc::new(BlockIndex::new());
        let id = block(1);
        let page = PageIndex(255);
        let end = PageIndex(256);
        let unit = CacheUnit::Page(id.clone(), page);
        let life = crate::page_lifecycle::PageLifecycle::with_index(index.clone());
        index.init_paged(id.clone(), 1 << 20, u64::from(id.block_size));
        // Attaching state to an absent page must not allocate a sparse slot.
        index.set_access(&unit, AccessHandle::default());
        index.read_entry(&id, |entry| {
            let IndexedUnits::Paged(pages) = &entry.units else {
                panic!("expected paged entry");
            };
            assert!(pages.read().unwrap().is_empty());
        });

        // A resident sparse file can have zero allocation. Presence and access
        // publication must not be inferred from its byte cost.
        assert!(index.mark_page_allocated(&id, page, 0));
        assert_eq!(index.presence(&id, page, end), Presence::PageHit);
        assert_eq!(index.snapshot_units(), vec![(id.clone(), Some(page), 0)]);
        assert!(index.page_reads_and_touch(&id, page, end).is_none());
        life.block(&id).register(page, None, 0, false);
        assert!(index.page_reads_and_touch(&id, page, end).is_none());
        index.set_access(&unit, AccessHandle::default());
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
    fn shared_page_removal_preserves_sibling_access_state() {
        let index = Arc::new(BlockIndex::new());
        let id = block(1);
        index.init_paged(id.clone(), 1 << 20, u64::from(id.block_size));
        for page in 0..256 {
            assert!(index.mark_page_allocated(&id, PageIndex(page), u64::from(page) + 1));
        }
        let last = PageIndex(255);
        let life = crate::page_lifecycle::PageLifecycle::with_index(index.clone());
        index.set_access(&CacheUnit::Page(id.clone(), last), AccessHandle::default());
        life.block(&id).register(last, None, 0, false);
        assert!(index.clear_page(&id, PageIndex(0)));
        assert!(index
            .page_reads_and_touch(&id, last, PageIndex(256))
            .is_some());
        assert_eq!(index.snapshot_units().last().unwrap().2, 256);
        assert_eq!(
            index.presence(&id, PageIndex(0), PageIndex(1)),
            Presence::PageMiss
        );
        // A removed page must not inherit a sibling's access state.
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
                // Refill after removing every resident page.
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
    fn recovered_pages_accept_unordered_and_duplicate_allocations() {
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
            let index = Arc::new(BlockIndex::new());
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
            let lifecycle = crate::page_lifecycle::PageLifecycle::with_index(index.clone());
            index.restore_page_states(&lifecycle);
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
            // The first page reuses the template token, but its second-chance
            // bit must remain independent of every sibling's bit.
            for candidate in &candidates {
                assert_eq!(
                    lru.candidate_is_current(candidate),
                    candidate.unit == CacheUnit::Page(paged.clone(), PageIndex(pages - 1))
                );
            }
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
        let idx = Arc::new(BlockIndex::new());
        let lru = Lru::new();
        let id = block(1);
        let life = crate::page_lifecycle::PageLifecycle::with_index(idx.clone());
        let block = life.block(&id);
        idx.init_paged(id.clone(), 4096, 8192);
        for p in 0..2 {
            let page = PageIndex(p);
            let unit = CacheUnit::Page(id.clone(), page);
            idx.mark_page(&id, page);
            idx.set_access(&unit, lru.insert(unit.clone(), 4096));
            block.register(page, Some(10), 10, false);
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
        let idx = Arc::new(BlockIndex::new());
        let lru = Lru::new();
        let id = block(1);
        idx.init_paged(id.clone(), 4096, 8192);
        let p0 = CacheUnit::Page(id.clone(), PageIndex(0));
        let p1 = CacheUnit::Page(id.clone(), PageIndex(1));
        for (unit, page) in [(&p0, PageIndex(0)), (&p1, PageIndex(1))] {
            idx.mark_page(&id, page);
            idx.set_access(unit, lru.insert(unit.clone(), 4096));
        }
        let life = crate::page_lifecycle::PageLifecycle::with_index(idx.clone());
        life.block(&id).register(PageIndex(0), None, 0, false);
        assert!(idx
            .page_reads_and_touch(&id, PageIndex(0), PageIndex(1))
            .is_some());
        assert_eq!(lru.evict_to_fit(4096), vec![p1.clone()]);
        idx.clear_page(&id, PageIndex(1));
        assert!(idx
            .read_entry(&id, |entry| entry
                .units
                .read(Some(PageIndex(1)), |slot| slot.access.is_none())
                .unwrap_or(true))
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

        let index = Arc::new(BlockIndex::new());
        let busy = block(1);
        let other = block(2);
        index.init_paged(busy.clone(), 4096, 8192);
        index.init_paged(other.clone(), 4096, 8192);
        let busy_entry = index.entry(&busy).unwrap();
        let busy_guard = busy_entry.block.write().unwrap();
        // A directory reader must not prevent mutations to existing entries.
        let lifecycle = crate::page_lifecycle::PageLifecycle::with_index(index.clone());
        let _other_lifecycle = lifecycle.block(&other);
        let directory_guard = index.entries.read().unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let lru = Lru::new();
                let page = PageIndex(0);
                let unit = CacheUnit::Page(other.clone(), page);
                assert!(!index.init_paged(other.clone(), 4096, 8192));
                assert!(index.mark_page_allocated(&other, page, 4096));
                index.set_access(&unit, lru.insert(unit.clone(), 4096));
                lifecycle.block(&other).register(page, None, 0, false);
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

        let index = Arc::new(BlockIndex::new());
        let id = block(1);
        index.init_paged(id.clone(), 4096, 8192);
        index.mark_page_allocated(&id, PageIndex(0), 4096);
        let lru = index.restore_lru();
        let unit = CacheUnit::Page(id.clone(), PageIndex(0));
        assert!(lru.pin(&unit));
        let old = index.entry(&id).unwrap();
        let read_guard = old.block.read().unwrap();
        let (removed_tx, removed_rx) = mpsc::channel();
        let (inserted_tx, inserted_rx) = mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| removed_tx.send(index.remove(&id)).unwrap());
            let deadline = Instant::now() + Duration::from_secs(5);
            let detached = loop {
                if let Ok(entries) = index.entries.try_read() {
                    if entries
                        .get(&id)
                        .is_some_and(|entry| entry.resident.is_none())
                    {
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
        assert_eq!(lru.total_bytes(), 4096);
        assert!(lru.evict_to_fit(0).is_empty());
        lru.unpin(&unit);
        assert_eq!(lru.remove(&unit), Some(4096));
        assert!(lru.is_empty());
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
    fn shared_policy_survives_clear_and_whole_replacement() {
        let index = Arc::new(BlockIndex::new());
        let id = block(1);
        index.init_paged(id.clone(), 4096, 8192);
        index.mark_page_allocated(&id, PageIndex(0), 4096);
        let lru = index.restore_lru();
        let unit = CacheUnit::Page(id.clone(), PageIndex(0));
        assert!(lru.pin(&unit));
        assert!(index.clear_page(&id, PageIndex(0)));
        assert_eq!(index.resident_bytes(), 0);
        assert_eq!(lru.total_bytes(), 4096);
        assert!(lru.evict_to_fit(0).is_empty());
        lru.unpin(&unit);
        assert_eq!(lru.remove(&unit), Some(4096));
        assert!(index
            .read_entry(&id, |entry| entry
                .units
                .page_table()
                .unwrap()
                .read()
                .unwrap()
                .is_empty())
            .unwrap());
        index.remove(&id);
        assert!(index.entries.read().unwrap().is_empty());

        index.commit_allocated(whole_meta(id.clone(), 4096), [(None, 4096)]);
        let unit = CacheUnit::Whole(id.clone());
        let old = lru.insert(unit.clone(), 4096);
        assert!(lru.pin(&unit));
        index.commit_allocated(whole_meta(id.clone(), 8192), [(None, 8192)]);
        let new = lru.insert(unit.clone(), 8192);
        assert!(Arc::ptr_eq(old.clock(), new.clock()));
        index.set_access(&unit, new);
        assert_eq!(index.resident_bytes(), 8192);
        assert_eq!(lru.total_bytes(), 8192);
        assert!(lru.evict_to_fit(0).is_empty());
        index.remove(&id);
        lru.unpin(&unit);
        assert_eq!(lru.remove(&unit), Some(8192));
        assert!(index.entries.read().unwrap().is_empty());
    }

    #[test]
    fn policy_only_page_churn_releases_removed_nodes() {
        let index = Arc::new(BlockIndex::new());
        let lru = index.restore_lru();
        let id = block(1);
        lru.insert(CacheUnit::Page(id.clone(), PageIndex(0)), 4096);
        for page in 1..256 {
            let unit = CacheUnit::Page(id.clone(), PageIndex(page));
            lru.insert(unit.clone(), 4096);
            assert_eq!(lru.remove(&unit), Some(4096));
        }
        assert_eq!(lru.len(), 1);
        assert_eq!(
            index.entries.read().unwrap()[&id]
                .policy
                .pages
                .as_ref()
                .unwrap()
                .read()
                .unwrap()
                .len(),
            1
        );
        lru.remove_block(&id);
        assert!(index.entries.read().unwrap().is_empty());
    }

    #[test]
    fn policy_only_units_do_not_appear_in_residency() {
        let index = Arc::new(BlockIndex::new());
        let lru = index.restore_lru();
        let id = block(1);
        lru.insert_with_policy(CacheUnit::Whole(id.clone()), 4096, true);
        assert_eq!(
            (index.len(), index.resident_bytes(), index.page_count()),
            (0, 0, 0)
        );
        assert!(index.get(&id).is_none());
        let lifecycle = crate::page_lifecycle::PageLifecycle::with_index(index.clone());
        let block = lifecycle.block(&id);
        drop(block);
        // A lifecycle sweep must not discard the small-store admission.
        lifecycle.scan(&mut Default::default(), 1024, 100, Some(1), 1024);
        assert_eq!(lru.total_bytes(), 4096);
        assert_eq!(lru.block_candidates(&id).len(), 1);
        lru.remove_block(&id);
        assert!(lru.is_empty());
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
