//! In-memory block index.
//!
//! The [`BlockIndex`] maps each resident [`BlockId`] to its [`BlockMeta`]
//! (physical [`BlockForm`] + length) and tracks total resident bytes for
//! eviction. It is the central structure shared by the data path (hit/miss
//! decisions), the miss/loader path (commit on completion), and eviction
//! (byte accounting).
//!
//! Journal-backed blocks use the catalog directory for membership, geometry,
//! and allocation. Standalone entries keep an individual [`RwLock`] for
//! metadata and page updates. Readers borrow the entry while holding the
//! directory read lock. Writers retain an Arc and release the directory before
//! locking the entry. Removal/replacement retires the old entry under its write
//! lock, so writers racing with retirement retry their lookup.
use crate::block_key::{BlockIdentity, BlockKey};
use crate::catalog::{DiskId, StoreKind};
use crate::small_store::key::{SmallIdentity, SmallRef};

mod catalog;
mod compact_policy;
mod directory;
pub(crate) use directory::Directory;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard};

use crate::eviction::compact::{CompactPolicy, Pin, Selection, UnitState};
#[cfg(test)]
use crate::eviction::CacheUnit;
use crate::eviction::{
    AccessHandle, BlockActivity, LegacyBlockActivity, PolicyEntry, PolicyState, SharedUnit,
    UnitKey, UnitKind,
};
use crate::page_lifecycle::{PageEntry, PageReadGuard, PageRows, PageSet, PageTable, PageWork};

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
    entries: Arc<RwLock<Directory>>,
    catalog: bool,
    small: RwLock<Vec<Arc<RwLock<crate::small_store::directory::Directory>>>>,
    resident_blocks: AtomicU64,
    resident_bytes: AtomicU64,
    resident_pages: AtomicU64,
    pub(crate) page_work: Arc<PageWork>,
    pub(crate) compact: Arc<CompactPolicy>,
    compact_enabled: AtomicBool,
}

#[derive(Default)]
pub(crate) struct DirectoryEntry {
    resident: Option<Arc<Entry>>,
    legacy: Option<Box<LegacyPolicy>>,
}

#[derive(Default)]
struct LegacyPolicy {
    units: BTreeMap<u64, PolicyEntry>,
    small: u8,
    activity: LegacyBlockActivity,
    #[cfg(test)]
    attached: BTreeMap<u64, AccessHandle>,
}

impl LegacyPolicy {
    fn access(&self, kind: UnitKind) -> Option<&AccessHandle> {
        #[cfg(test)]
        if let Some(access) = self.attached.get(&kind.order()) {
            return Some(access);
        }
        self.units.get(&kind.order()).map(|entry| &entry.access)
    }

    fn len(&self) -> usize {
        self.units.len() + self.small.count_ones() as usize
    }

    fn has_units(&self) -> bool {
        !self.units.is_empty() || self.small != 0
    }

    fn is_empty(&self) -> bool {
        #[cfg(test)]
        if !self.attached.is_empty() {
            return false;
        }
        !self.has_units() && self.activity.access.is_none()
    }
}

impl DirectoryEntry {
    fn has_paged_resident(&self) -> bool {
        self.resident.as_ref().is_some_and(|entry| entry.paged)
    }

    fn legacy_mut(&mut self) -> &mut LegacyPolicy {
        self.legacy.get_or_insert_with(Default::default)
    }

    fn prune_legacy(&mut self) {
        if self.legacy.as_ref().is_some_and(|p| p.is_empty()) {
            self.legacy = None;
        }
    }

    fn is_empty(&self) -> bool {
        self.resident.is_none() && self.legacy.is_none()
    }
}

struct Entry {
    // None means retired: retained Arcs must never mutate or charge it again.
    block: RwLock<Option<IndexedBlock>>,
    paged: bool,
    // Directory detachment precedes retirement. Only the attached occupant
    // can publish slot-indexed maintenance updates.
    attached: AtomicBool,
}

impl Entry {
    fn new(block: IndexedBlock) -> Self {
        Self {
            paged: matches!(block.units, IndexedUnits::Paged(_)),
            block: RwLock::new(Some(block)),
            attached: AtomicBool::new(true),
        }
    }
}

/// Data-file allocation discovered during startup; never persisted in sidecars.
pub(crate) struct StoredBlock {
    pub meta: StoredMeta,
    pub allocated: Vec<(Option<PageIndex>, u64)>,
}

#[derive(Clone)]
pub(crate) struct StoredMeta {
    pub id: BlockKey,
    pub disk_id: Option<DiskId>,
    pub form: BlockForm,
    pub len: u64,
}

impl From<BlockMeta> for StoredMeta {
    fn from(meta: BlockMeta) -> Self {
        Self {
            id: BlockKey::new(&meta.id),
            disk_id: None,
            form: meta.form,
            len: meta.len,
        }
    }
}

impl From<StoredMeta> for BlockMeta {
    fn from(meta: StoredMeta) -> Self {
        Self {
            id: meta.id.materialize(),
            form: meta.form,
            len: meta.len,
        }
    }
}

struct IndexedBlock {
    meta: ResidentMeta,
    units: IndexedUnits,
    // Retained policy/lifecycle tables can change after directory detachment.
    // Retirement refunds this incarnation's charges, independently of them.
    charged_bytes: u64,
    charged_pages: u64,
    policy_revision: u64,
    policy_bytes: u64,
    policy_units: u32,
}

struct ResidentMeta {
    id: BlockKey,
    disk_id: Option<DiskId>,
    len: u64,
    page_size: u32,
    page_count: u32,
}

enum IndexedUnits {
    Whole(UnitSlot),
    Paged(PageTable),
}

#[derive(Default)]
struct UnitSlot {
    allocated: u64,
    access: UnitState,
}

#[derive(Default)]
pub(crate) struct PageUnit {
    allocated: u64,
    access: UnitState,
}

impl PageUnit {
    pub(crate) fn resident(&self) -> bool {
        self.access.resident()
    }
}

impl IndexedUnits {
    fn allocated_bytes(&self) -> u64 {
        match self {
            Self::Whole(slot) => slot.allocated,
            Self::Paged(pages) => pages
                .read()
                .unwrap()
                .values()
                .map(|entry| entry.unit.allocated)
                .sum(),
        }
    }
}

impl From<StoredMeta> for IndexedBlock {
    fn from(meta: StoredMeta) -> Self {
        let charged_pages = match &meta.form {
            BlockForm::Whole => 0,
            BlockForm::Paged { present, .. } => u64::from(present.count()),
        };
        let (page_size, page_count, units) = match meta.form {
            BlockForm::Whole => (
                0,
                0,
                IndexedUnits::Whole({
                    let mut slot = UnitSlot::default();
                    slot.access.set_resident(true);
                    slot
                }),
            ),
            BlockForm::Paged { page_size, present } => {
                let mut pages = PageRows::default();
                for page in 0..present.len() {
                    if present.is_present(PageIndex(page)) {
                        let mut entry = PageEntry::default();
                        entry.unit.access.set_resident(true);
                        pages.insert(page, entry);
                    }
                }
                (
                    page_size,
                    present.len(),
                    IndexedUnits::Paged(Arc::new(RwLock::new(pages))),
                )
            }
        };
        Self {
            meta: ResidentMeta {
                id: meta.id,
                disk_id: meta.disk_id,
                len: meta.len,
                page_size,
                page_count,
            },
            units,
            charged_bytes: 0,
            charged_pages,
            policy_revision: 0,
            policy_bytes: 0,
            policy_units: 0,
        }
    }
}

impl IndexedBlock {
    fn allocated(
        meta: StoredMeta,
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
                    slot.allocated = bytes;
                }
            }
            IndexedUnits::Paged(slots) => {
                for (page, allocated) in allocated {
                    let page = page.expect("paged allocation must have a page index");
                    assert!(
                        page.0 < entry.meta.page_count,
                        "page outside block geometry"
                    );
                    slots
                        .write()
                        .unwrap()
                        .get_or_insert_with(page.0, PageEntry::default)
                        .unit
                        .allocated = allocated;
                }
            }
        }
        entry.charged_bytes = entry.units.allocated_bytes();
        entry
    }

    fn materialized_pages(&self) -> u64 {
        self.charged_pages
    }

    fn metadata(&self) -> StoredMeta {
        let form = match &self.units {
            IndexedUnits::Whole(_) => BlockForm::Whole,
            IndexedUnits::Paged(pages) => {
                let mut present = PresentBitmap::new(self.meta.page_count);
                for (&page, slot) in pages.read().unwrap().iter() {
                    if slot.unit.access.resident() {
                        present.set(PageIndex(page));
                    }
                }
                BlockForm::Paged {
                    page_size: self.meta.page_size,
                    present,
                }
            }
        };
        StoredMeta {
            id: self.meta.id.clone(),
            disk_id: self.meta.disk_id,
            len: self.meta.len,
            form,
        }
    }
}

fn with_catalog_pages<T>(
    entries: &Directory,
    slot: usize,
    f: impl FnOnce(&BlockKey, PageSet<'_>) -> T,
) -> Option<T> {
    let (key, geometry, _) = entries.resident_at(slot)?;
    (geometry.kind() == Some(StoreKind::Paged))
        .then(|| f(key, PageSet::Catalog(geometry, entries.inline_age(slot))))
}

fn with_legacy_pages<T>(
    entry: &DirectoryEntry,
    exclusive: bool,
    f: impl FnOnce(PageSet<'_>) -> T,
) -> Option<T> {
    let block = entry.resident.as_ref()?.block.read().unwrap();
    let IndexedUnits::Paged(table) = &block.as_ref()?.units else {
        return None;
    };
    if exclusive {
        let rows = table.write().unwrap();
        Some(f(PageSet::Legacy(&rows)))
    } else {
        let rows = table.read().unwrap();
        Some(f(PageSet::Legacy(&rows)))
    }
}

fn paged_key_at(entries: &Directory, catalog: bool, slot: usize) -> Option<&BlockKey> {
    if catalog {
        let (key, geometry, _) = entries.resident_at(slot)?;
        (geometry.kind() == Some(StoreKind::Paged)).then_some(key)
    } else {
        let (key, entry) = entries.at(slot)?;
        entry.has_paged_resident().then_some(key)
    }
}

impl BlockIndex {
    /// Startup only, before serving. Reusing a recovered index starts a new
    /// runtime generation without retaining the previous policy or lifecycle.
    pub(crate) fn prepare_runtime_recovery(&self) {
        self.small.write().unwrap().clear();
        let mut entries = self.entries.write().unwrap();
        self.compact_enabled.store(false, Ordering::Relaxed);
        entries.reset_clocks(false);
        self.compact.reset();
        let tracked = self.page_work.tracking();
        self.page_work.reset();
        if self.catalog {
            entries.reset_catalog_policy();
        }
        entries.retain(|_, directory| {
            if let Some(entry) = &directory.resident {
                let mut guard = entry.block.write().unwrap();
                let block = guard.as_mut().expect("live directory entry");
                if tracked || directory.legacy.is_some() || block.policy_units > 0 {
                    let allocated = match &block.units {
                        IndexedUnits::Whole(slot) => vec![(None, slot.allocated)],
                        IndexedUnits::Paged(pages) => pages
                            .read()
                            .unwrap()
                            .iter()
                            .filter_map(|(&page, entry)| {
                                entry
                                    .unit
                                    .access
                                    .resident()
                                    .then_some((Some(PageIndex(page)), entry.unit.allocated))
                            })
                            .collect(),
                    };
                    *block = IndexedBlock::allocated(block.metadata(), allocated);
                }
            }
            directory.legacy = None;
            directory.resident.is_some()
        });
    }

    pub(crate) fn legacy_block_get(&self, id: &dyn BlockIdentity) -> Option<BlockActivity> {
        let entries = self.entries.read().unwrap();
        let legacy = entries.get(id)?.legacy.as_ref()?;
        legacy.activity.snapshot(legacy.len())
    }

    pub(crate) fn policy_block_get(&self, id: &dyn BlockIdentity) -> Option<BlockActivity> {
        let entries = self.entries.read().unwrap();
        self.block_activity(&entries, entries.slot(id)?)
    }

    pub(crate) fn policy_scan_ceiling(&self) -> usize {
        self.entries.read().unwrap().slot_count()
    }

    pub(crate) fn policy_block_at(&self, slot: usize) -> Option<(BlockKey, BlockActivity)> {
        let entries = self.entries.read().unwrap();
        let (id, _) = entries.at(slot)?;
        Some((id.clone(), self.block_activity(&entries, slot)?))
    }

    fn block_activity(&self, entries: &Directory, slot: usize) -> Option<BlockActivity> {
        let (_, entry) = entries.at(slot)?;
        let legacy = entry
            .legacy
            .as_ref()
            .and_then(|p| p.activity.snapshot(p.len()));
        let catalog = entries.resident_at(slot).and_then(|(_, geometry, _)| {
            let last_access = entries.clock(slot)?.load(Ordering::Relaxed);
            (last_access != 0).then(|| BlockActivity {
                units: geometry.page_count(),
                pins: self.compact.block_pins(slot),
                revision: entries.revision(slot).unwrap(),
                last_access,
            })
        });
        let compact = catalog.or_else(|| {
            entry.resident.as_ref().and_then(|entry| {
                let block = entry.block.read().unwrap();
                let block = block.as_ref()?;
                let last_access = entries.clock(slot)?.load(Ordering::Relaxed);
                (block.policy_units > 0 && last_access != 0).then(|| BlockActivity {
                    units: block.policy_units as usize,
                    pins: self.compact.block_pins(slot),
                    revision: block.policy_revision,
                    last_access,
                })
            })
        });
        match (compact, legacy) {
            (Some(a), Some(b)) => Some(BlockActivity {
                units: a.units + b.units,
                pins: a.pins + b.pins,
                revision: a.revision.max(b.revision),
                last_access: a.last_access.max(b.last_access),
            }),
            (compact, legacy) => compact.or(legacy),
        }
    }

    /// Advance by stable directory slot and then by unit key within the block.
    pub(crate) fn policy_unit_at(
        &self,
        cursor: &mut (usize, u64),
        end: (usize, u64),
    ) -> Option<SharedUnit> {
        let entries = self.entries.read().unwrap();
        let (position, start) = *cursor;
        let mut next = None;
        let mut last = None;
        if let Some((block, entry)) = entries.at(position) {
            let mut consider = |first, final_key| {
                next = next.into_iter().chain(first).min();
                last = last.into_iter().chain(final_key).max();
            };
            if let Some(legacy) = &entry.legacy {
                consider(
                    legacy.units.range(start..).next().map(|(&key, _)| key),
                    legacy.units.last_key_value().map(|(&key, _)| key),
                );
                for key in 0..=1 {
                    if legacy.small & (1 << key) != 0 {
                        consider((start <= key).then_some(key), Some(key));
                    }
                }
            }
            if let Some((_, geometry, _)) = entries.resident_at(position) {
                let first = geometry
                    .policy_units_from(start)
                    .next()
                    .map(|(kind, _)| kind.order());
                let last = match geometry.kind().unwrap() {
                    StoreKind::Whole => 0,
                    StoreKind::Paged => u64::from(geometry.last_page()) + 2,
                };
                consider(first, Some(last));
            }
            if let Some(entry) = &entry.resident {
                let entry = entry.block.read().unwrap();
                if let Some(block) = entry.as_ref().filter(|block| block.policy_units != 0) {
                    match &block.units {
                        IndexedUnits::Whole(unit) if unit.access.admitted() => {
                            consider((start == 0).then_some(0), Some(0));
                        }
                        IndexedUnits::Paged(pages) => {
                            if let Ok(page) = u32::try_from(start.saturating_sub(2)) {
                                let pages = pages.read().unwrap();
                                consider(
                                    pages
                                        .range(page..)
                                        .next()
                                        .map(|(&page, _)| u64::from(page) + 2),
                                    pages.last_key_value().map(|(&page, _)| u64::from(page) + 2),
                                );
                            }
                        }
                        IndexedUnits::Whole(_) => {}
                    }
                }
            }
            if let Some(key) = next.filter(|&key| (position, key) < end) {
                *cursor = if Some(key) == last {
                    (position + 1, 0)
                } else {
                    (position, key + 1)
                };
                return Some(SharedUnit {
                    block: block.clone(),
                    kind: UnitKind::from_order(key),
                });
            }
        }
        *cursor = (position + 1, 0).min(end);
        None
    }

    pub(crate) fn policy_block_update<T>(
        &self,
        id: &dyn BlockIdentity,
        update: impl FnOnce(&mut LegacyBlockActivity) -> T,
    ) -> Option<T> {
        let mut entries = self.entries.write().unwrap();
        let activity = &mut entries.get_mut(id)?.legacy.as_mut()?.activity;
        activity.access.as_ref()?;
        Some(update(activity))
    }

    pub(crate) fn policy_block_insert(&self, id: BlockKey, activity: LegacyBlockActivity) {
        self.entries
            .write()
            .unwrap()
            .get_or_insert(id)
            .1
            .legacy_mut()
            .activity = activity;
    }

    pub(crate) fn policy_block_remove(&self, id: &dyn BlockIdentity) {
        let mut entries = self.entries.write().unwrap();
        let Some(entry) = entries.get_mut(id) else {
            return;
        };
        if let Some(legacy) = &mut entry.legacy {
            legacy.activity = LegacyBlockActivity::default();
        }
        entry.prune_legacy();
        if entry.is_empty() {
            entries.remove(id);
        }
    }

    pub(crate) fn policy_get(&self, unit: &impl UnitKey) -> Option<PolicyEntry> {
        if !matches!(unit.kind(), UnitKind::Page(_)) {
            if let Some(policy) = self.small_policy(&SmallRef::from_unit(unit)) {
                return Some(policy);
            }
        }
        self.entries
            .read()
            .unwrap()
            .get(unit.block())?
            .legacy
            .as_ref()?
            .units
            .get(&unit.kind().order())
            .cloned()
    }

    pub(crate) fn policy_units(&self, id: &dyn BlockIdentity) -> Vec<(UnitKind, PolicyState)> {
        let mut units: Vec<_> = [UnitKind::Whole, UnitKind::ExactRange]
            .into_iter()
            .filter_map(|kind| {
                self.small_policy(&SmallRef {
                    block: id.view(),
                    kind,
                })
                .map(|p| (kind, p.state))
            })
            .collect();
        let entries = self.entries.read().unwrap();
        let Some(slot) = entries.slot(id) else {
            return units;
        };
        let (_, entry) = entries.at(slot).unwrap();
        if let Some(legacy) = &entry.legacy {
            units.extend(
                legacy
                    .units
                    .iter()
                    .map(|(&key, unit)| (UnitKind::from_order(key), unit.state)),
            );
        }
        let mut add = |kind, bytes| {
            units.push((
                kind,
                PolicyState {
                    bytes,
                    pins: self.compact.pins(slot, kind),
                    independent: false,
                    revision: 0,
                },
            ));
        };
        if let Some((_, geometry, _)) = entries.resident_at(slot) {
            if self.compact_enabled.load(Ordering::Relaxed) {
                geometry
                    .policy_units_from(0)
                    .for_each(|(kind, bytes)| add(kind, bytes));
            }
        } else if let Some(entry) = &entry.resident {
            let block = entry.block.read().unwrap();
            block
                .as_ref()
                .expect("live directory entry")
                .for_each_compact_unit(add);
        }
        units
    }

    pub(crate) fn shared_identity(&self, id: &dyn BlockIdentity) -> Option<BlockKey> {
        if let Some((id, _)) = self.entries.read().unwrap().get_key_value(id) {
            return Some(id.clone());
        }
        for kind in [UnitKind::Whole, UnitKind::ExactRange] {
            let key = SmallRef {
                block: id.view(),
                kind,
            };
            if let Some(directory) = self.small_directory(&key) {
                if let Some(key) = directory.read().unwrap().identity(&key) {
                    return Some(key.0.block);
                }
            }
        }
        None
    }

    pub(crate) fn policy_version_blocks(&self, id: &dyn BlockIdentity) -> Vec<BlockKey> {
        let mut blocks = Vec::new();
        let entries = self.entries.read().unwrap();
        entries.versions(id, |slot, key, entry| {
            if (self.compact_enabled.load(Ordering::Relaxed) && entries.resident_at(slot).is_some())
                || entry.legacy.as_deref().is_some_and(LegacyPolicy::has_units)
                || entry.resident.as_ref().is_some_and(|entry| {
                    entry.block.read().unwrap().as_ref().unwrap().policy_units > 0
                })
            {
                blocks.push(key.clone());
            }
        });
        blocks
    }

    #[cfg(test)]
    pub(crate) fn policy_versioned_count(&self, id: &dyn BlockIdentity) -> usize {
        self.policy_version_blocks(id)
            .iter()
            .filter(|block| {
                self.policy_units(*block)
                    .iter()
                    .any(|(_, unit)| !unit.independent)
            })
            .count()
    }

    pub(crate) fn policy_insert(&self, unit: &SharedUnit, entry: PolicyEntry) -> AccessHandle {
        if entry.independent && !matches!(unit.kind, UnitKind::Page(_)) {
            if let Some(directory) = self.small_directory(&SmallRef::from_unit(unit)) {
                if let Some(access) = directory
                    .write()
                    .unwrap()
                    .policy_insert(&SmallRef::from_unit(unit), entry.clone())
                {
                    let mut entries = self.entries.write().unwrap();
                    let (_, directory) = entries.get_or_insert(unit.block.clone());
                    directory.legacy_mut().small |= 1 << unit.kind.order();
                    return access;
                }
            }
        }
        let mut entries = self.entries.write().unwrap();
        let (_, directory) = entries.get_or_insert(unit.block.clone());
        let access = entry.access.clone();
        directory
            .legacy_mut()
            .units
            .insert(unit.kind.order(), entry);
        access
    }

    /// Return the update result and whether the unit participates in block tracking.
    pub(crate) fn policy_update<T>(
        &self,
        unit: &impl UnitKey,
        update: impl FnOnce(&mut PolicyState) -> T,
    ) -> Option<(T, bool)> {
        if !matches!(unit.kind(), UnitKind::Page(_)) {
            let id = SmallRef::from_unit(unit);
            if let Some(directory) = self.small_directory(&id) {
                let mut directory = directory.write().unwrap();
                if let Some(entry) = directory.policy_get(&id) {
                    let tracked = entry.access.small().is_none();
                    return directory
                        .policy_update(&id, update)
                        .map(|result| (result, tracked));
                }
            }
        }
        let mut entries = self.entries.write().unwrap();
        let policy = entries.get_mut(unit.block())?.legacy.as_mut()?;
        Some((
            update(&mut policy.units.get_mut(&unit.kind().order())?.state),
            true,
        ))
    }

    pub(crate) fn policy_remove(&self, unit: &impl UnitKey) -> Option<PolicyEntry> {
        if !matches!(unit.kind(), UnitKind::Page(_)) {
            if let Some(directory) = self.small_directory(&SmallRef::from_unit(unit)) {
                if let Some(entry) = directory
                    .write()
                    .unwrap()
                    .policy_remove(&SmallRef::from_unit(unit))
                {
                    if entry.access.small().is_none() {
                        let mut entries = self.entries.write().unwrap();
                        if let Some(directory) = entries.get_mut(unit.block()) {
                            if let Some(legacy) = &mut directory.legacy {
                                legacy.small &= !(1 << unit.kind().order());
                            }
                            directory.prune_legacy();
                            if directory.is_empty() {
                                entries.remove(unit.block());
                            }
                        }
                    }
                    return Some(entry);
                }
            }
        }
        let mut entries = self.entries.write().unwrap();
        let directory = entries.get_mut(unit.block())?;
        let removed = directory
            .legacy
            .as_mut()?
            .units
            .remove(&unit.kind().order())?;
        removed.access.retire();
        directory.prune_legacy();
        if directory.is_empty() {
            entries.remove(unit.block());
        }
        Some(removed)
    }

    pub(crate) fn register_small_directory(
        &self,
        directory: &Arc<RwLock<crate::small_store::directory::Directory>>,
    ) {
        let mut small = self.small.write().unwrap();
        if !small.iter().any(|entry| Arc::ptr_eq(entry, directory)) {
            small.push(directory.clone());
        }
    }

    fn small_directory(
        &self,
        id: &dyn SmallIdentity,
    ) -> Option<Arc<RwLock<crate::small_store::directory::Directory>>> {
        self.small
            .read()
            .unwrap()
            .iter()
            .find(|directory| directory.read().unwrap().identity(id).is_some())
            .cloned()
    }

    fn small_policy(&self, id: &dyn SmallIdentity) -> Option<PolicyEntry> {
        self.small_directory(id)?.read().unwrap().policy_get(id)
    }

    pub(crate) fn compact_small_totals(&self) -> (u64, usize) {
        self.small
            .read()
            .unwrap()
            .iter()
            .fold((0, 0), |total, directory| {
                let next = directory.read().unwrap().compact_totals();
                (total.0 + next.0, total.1 + next.1)
            })
    }

    pub(crate) fn compact_small_admit(
        &self,
        unit: &impl crate::eviction::UnitKey,
        bytes: u64,
        clock: bool,
        allow_uninitialized: bool,
    ) -> Option<AccessHandle> {
        let key = SmallRef::from_unit(unit);
        let directory = self.small_directory(&key)?;
        let mut directory = directory.write().unwrap();
        if !allow_uninitialized && !directory.has_recency() {
            return None;
        }
        directory.compact_admit(&key, bytes, clock)
    }

    pub(crate) fn compact_small_directories(
        &self,
    ) -> Vec<Arc<std::sync::RwLock<crate::small_store::directory::Directory>>> {
        self.small.read().unwrap().clone()
    }
    /// Startup-only bulk restoration. Exclusive ownership prevents publication
    /// to readers while workers build page tables without the directory lock.
    #[cfg(test)]
    pub(crate) fn restore_allocated(&mut self, blocks: Vec<StoredBlock>, threads: usize) {
        self.try_restore_allocated(blocks, threads, |block| {
            Ok::<_, std::convert::Infallible>((block.meta, block.allocated))
        })
        .unwrap();
    }

    /// Decode and construct entries in parallel, then publish with exclusive
    /// ownership. A decode error leaves the existing index unchanged. Small
    /// whole objects can supply inline allocations instead of a Vec per object.
    #[cfg(test)]
    pub(crate) fn try_restore_allocated<T: Send, A, E: Send>(
        &mut self,
        input: Vec<T>,
        threads: usize,
        decode: impl Fn(T) -> Result<(StoredMeta, A), E> + Sync,
    ) -> Result<(), E>
    where
        A: IntoIterator<Item = (Option<PageIndex>, u64)>,
    {
        let built = crate::recovery::map(input, threads, |item| {
            let (meta, allocated) = decode(item)?;
            let id = meta.id.clone();
            let entry = IndexedBlock::allocated(meta, allocated);
            let bytes = entry.charged_bytes;
            let pages = entry.materialized_pages();
            Ok((id, Arc::new(Entry::new(entry)), bytes, pages))
        })
        .into_iter()
        .collect::<Result<Vec<_>, E>>()?;
        let entries = Arc::get_mut(&mut self.entries).unwrap().get_mut().unwrap();
        entries.reserve(built.len());
        let bytes = self.resident_bytes.get_mut();
        let pages = self.resident_pages.get_mut();
        for (id, mut entry, added_bytes, added_pages) in built {
            let (key, slot) = entries.get_or_insert(id);
            Arc::get_mut(&mut entry)
                .unwrap()
                .block
                .get_mut()
                .unwrap()
                .as_mut()
                .unwrap()
                .meta
                .id = key;
            if let Some(old) = slot.resident.replace(entry) {
                let old = old
                    .block
                    .write()
                    .unwrap()
                    .take()
                    .expect("live recovery entry");
                *bytes -= old.charged_bytes;
                *pages -= old.materialized_pages();
            } else {
                *self.resident_blocks.get_mut() += 1;
            }
            *bytes += added_bytes;
            *pages += added_pages;
        }
        Ok(())
    }

    pub(crate) fn canonical_key(&self, id: &BlockId) -> Option<BlockKey> {
        self.entries
            .read()
            .unwrap()
            .get_key_value(id)
            .map(|(id, _)| id.clone())
    }

    pub(crate) fn enable_page_ages(&self) {
        let mut entries = self.entries.write().unwrap();
        if self.page_work.tracking() {
            return;
        }
        if self.catalog {
            entries.enable_catalog_ages();
        } else {
            for entry in entries.values().filter_map(|entry| entry.resident.as_ref()) {
                let block = entry.block.read().unwrap();
                if let IndexedUnits::Paged(table) = &block.as_ref().unwrap().units {
                    table.write().unwrap().track_resident_ages();
                }
            }
        }
        self.page_work.enable_tracking();
    }

    /// Keep directory membership stable across all accesses to a page row.
    pub(crate) fn with_page_slot<T>(
        &self,
        slot: usize,
        exclusive: bool,
        f: impl FnOnce(&BlockKey, PageSet<'_>) -> T,
    ) -> Option<T> {
        if self.catalog && exclusive {
            return with_catalog_pages(&self.entries.write().unwrap(), slot, f);
        }
        let entries = self.entries.read().unwrap();
        if self.catalog {
            with_catalog_pages(&entries, slot, f)
        } else {
            let (key, entry) = entries.at(slot)?;
            with_legacy_pages(entry, exclusive, |rows| f(key, rows))
        }
    }

    pub(crate) fn with_pages<T>(
        &self,
        id: &dyn BlockIdentity,
        exclusive: bool,
        f: impl FnOnce(usize, PageSet<'_>) -> T,
    ) -> Option<T> {
        if self.catalog && exclusive {
            let entries = self.entries.write().unwrap();
            let slot = entries.slot(id)?;
            return with_catalog_pages(&entries, slot, |_, rows| f(slot, rows));
        }
        let entries = self.entries.read().unwrap();
        let slot = entries.slot(id)?;
        if self.catalog {
            with_catalog_pages(&entries, slot, |_, rows| f(slot, rows))
        } else {
            with_legacy_pages(entries.get(id)?, exclusive, |rows| f(slot, rows))
        }
    }

    #[cfg(test)]
    pub(crate) fn legacy_pages(&self, id: &dyn BlockIdentity) -> Option<PageTable> {
        let entries = self.entries.read().unwrap();
        let block = entries.get(id)?.resident.as_ref()?.block.read().unwrap();
        let IndexedUnits::Paged(table) = &block.as_ref()?.units else {
            return None;
        };
        Some(table.clone())
    }

    pub(crate) fn visit_paged_slots(&self, mut visit: impl FnMut(usize, &BlockKey)) {
        let entries = self.entries.read().unwrap();
        for slot in 0..entries.slot_count() {
            if let Some(id) = paged_key_at(&entries, self.catalog, slot) {
                visit(slot, id);
            }
        }
    }

    pub(crate) fn paged_slots_in(
        &self,
        slots: std::ops::Range<usize>,
        matches_shard: impl Fn(&BlockKey) -> bool,
    ) -> Vec<usize> {
        let entries = self.entries.read().unwrap();
        slots
            .filter(|&slot| paged_key_at(&entries, self.catalog, slot).is_some_and(&matches_shard))
            .collect()
    }

    fn entry(&self, id: &BlockId) -> Option<Arc<Entry>> {
        self.entries.read().unwrap().get(id)?.resident.clone()
    }

    fn read_entry<T>(&self, id: &BlockId, read: impl FnOnce(&IndexedBlock) -> T) -> Option<T> {
        self.read_entry_with_policy(id, |block, _, _| read(block))
    }

    fn read_entry_with_policy<T>(
        &self,
        id: &BlockId,
        read: impl FnOnce(&IndexedBlock, Option<&AtomicU64>, Option<&LegacyPolicy>) -> T,
    ) -> Option<T> {
        let entries = self.entries.read().unwrap();
        let slot = entries.slot(id)?;
        let (_, directory) = entries.at(slot)?;
        let guard = directory.resident.as_ref()?.block.read().unwrap();
        // Keeping membership stable also prevents the slot's clock from being reused.
        Some(read(
            guard.as_ref().expect("live directory entry"),
            entries.clock(slot),
            directory.legacy.as_deref(),
        ))
    }

    fn write_paged_entry<T>(
        &self,
        id: &BlockId,
        write: impl FnOnce(&mut IndexedBlock, usize, Option<&AtomicU64>, &AtomicBool) -> T,
    ) -> Option<T> {
        self.with_compact_block_write(id, write)
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
        self: &Arc<Self>,
        id: &BlockId,
        start: PageIndex,
        end: PageIndex,
    ) -> Option<(PageReadGuard, Vec<PageReadGuard>)> {
        self.page_reads_and_touch_at(id, start, end)
            .map(|(_, first, tail)| (first, tail))
    }

    pub(crate) fn page_reads_and_touch_at(
        self: &Arc<Self>,
        id: &BlockId,
        start: PageIndex,
        end: PageIndex,
    ) -> Option<(Option<DiskId>, PageReadGuard, Vec<PageReadGuard>)> {
        if start.0 >= end.0 {
            return None;
        }
        if self.catalog {
            return self.catalog_read_pages(id, start, end);
        }
        // On a partial range miss, release acquired guards only after all
        // directory, entry and page locks have gone out of scope.
        let first;
        let mut tail = Vec::new();
        let disk_id = {
            let entries = self.entries.read().unwrap();
            let slot = entries.slot(id)?;
            let (_, directory) = entries.at(slot)?;
            let block = directory.resident.as_ref()?.block.read().unwrap();
            let block = block.as_ref()?;
            let IndexedUnits::Paged(pages) = &block.units else {
                return None;
            };
            let pages = pages.read().unwrap();
            let acquire = |page| {
                let row = pages.get(&page)?;
                let access = directory
                    .legacy
                    .as_ref()
                    .and_then(|p| p.access(UnitKind::Page(PageIndex(page))));
                if !row.unit.access.resident() || (!row.unit.access.admitted() && access.is_none())
                {
                    return None;
                }
                let guard = PageReadGuard::acquire(self, slot, page, &row.state)?;
                row.unit.access.touch(entries.clock(slot));
                if let Some(access) = access {
                    access.touch();
                }
                Some(guard)
            };
            first = acquire(start.0)?;
            for page in start.0 + 1..end.0 {
                tail.push(acquire(page)?);
            }
            block.meta.disk_id
        };
        Some((disk_id, first, tail))
    }

    /// Startup only, before serving or starting maintenance. Attach policy
    /// tokens in place rather than cloning a full snapshot and looking every
    /// unit up again. Membership stays stable throughout the traversal.
    #[cfg(test)]
    pub(crate) fn restore_lru(self: &Arc<Self>) -> crate::eviction::Lru {
        if self.catalog {
            return crate::eviction::Lru::from_runtime(self.clone(), Vec::new(), self.len(), true);
        }
        crate::eviction::Lru::from_recovered(
            self.clone(),
            self.restore_policy_units(true),
            self.len(),
            true,
        )
    }

    #[cfg(test)]
    pub(crate) fn restore_policy_units(
        &self,
        track_clock: bool,
    ) -> Vec<crate::eviction::RecoveredUnit> {
        use crate::eviction::RecoveredUnit;
        let started = std::time::Instant::now();
        let entries = self.entries.read().unwrap();
        let mut units = Vec::with_capacity(entries.len());
        for entry in entries.values().filter_map(|entry| entry.resident.as_ref()) {
            let guard = entry.block.read().unwrap();
            let entry = guard.as_ref().expect("live directory entry");
            let clock =
                track_clock.then(|| Arc::new(AtomicU64::new(crate::eviction::access_millis())));
            let mut add = |page: Option<PageIndex>, bytes, access| {
                units.push(RecoveredUnit {
                    independent: false,
                    unit: crate::eviction::SharedUnit {
                        block: entry.meta.id.clone(),
                        kind: page.map_or(UnitKind::Whole, UnitKind::Page),
                    },
                    bytes,
                    access,
                });
            };
            match &entry.units {
                IndexedUnits::Whole(slot) => {
                    let access = AccessHandle::with_state(Arc::default(), clock.clone());
                    add(None, slot.allocated, access);
                }
                IndexedUnits::Paged(pages) => {
                    for (&page, state) in pages.read().unwrap().iter() {
                        if state.unit.access.resident() {
                            let access = AccessHandle::with_state(Arc::default(), clock.clone());
                            add(Some(PageIndex(page)), state.unit.allocated, access);
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

    /// Create an empty index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of blocks currently tracked.
    pub fn len(&self) -> usize {
        if self.catalog {
            return self.entries.read().unwrap().totals().0 as usize;
        }
        self.resident_blocks.load(Ordering::Relaxed) as usize
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Allocated disk bytes of indexed data files, also charged to eviction.
    pub fn resident_bytes(&self) -> u64 {
        if self.catalog {
            return self.entries.read().unwrap().totals().2;
        }
        self.resident_bytes.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn logical_resident_bytes(&self) -> u64 {
        if self.catalog {
            let entries = self.entries.read().unwrap();
            return entries
                .residents()
                .map(|(key, geometry)| {
                    catalog::metadata(key, geometry, entries.page_size()).resident_bytes()
                })
                .sum();
        }
        self.with_snapshot(|entries| {
            entries
                .iter()
                .map(|entry| {
                    BlockMeta::from(entry.as_ref().expect("live directory entry").metadata())
                        .resident_bytes()
                })
                .sum()
        })
    }

    /// Number of materialized pages across all paged blocks.
    pub fn page_count(&self) -> u64 {
        if self.catalog {
            return self.entries.read().unwrap().totals().1;
        }
        self.resident_pages.load(Ordering::Relaxed)
    }

    /// Look up a copy of a block's metadata.
    pub fn get(&self, id: &BlockId) -> Option<BlockMeta> {
        if self.catalog {
            let entries = self.entries.read().unwrap();
            let (key, geometry, _) = entries.resident(id)?;
            return Some(catalog::metadata(key, geometry, entries.page_size()));
        }
        self.read_entry(id, |entry| entry.metadata().into())
    }

    /// Resolve metadata and mark a whole-block read in the same index lookup.
    pub(crate) fn get_and_touch(&self, id: &BlockId) -> Option<BlockMeta> {
        self.get_and_touch_at(id).map(|(meta, _)| meta)
    }

    pub(crate) fn get_and_touch_at(&self, id: &BlockId) -> Option<(BlockMeta, Option<DiskId>)> {
        if self.catalog {
            let entries = self.entries.read().unwrap();
            let (key, geometry, _) = entries.resident(id)?;
            if geometry.kind() == Some(StoreKind::Whole) {
                geometry
                    .policy_state(UnitKind::Whole)?
                    .touch(entries.clock(entries.slot(id)?));
            }
            return Some((
                catalog::metadata(key, geometry, entries.page_size()),
                geometry.disk_id,
            ));
        }
        self.read_entry_with_policy(id, |entry, clock, legacy| {
            if let IndexedUnits::Whole(slot) = &entry.units {
                slot.access.touch(clock);
                if let Some(access) = legacy.and_then(|p| p.access(UnitKind::Whole)) {
                    access.touch();
                }
            }
            (entry.metadata().into(), entry.meta.disk_id)
        })
    }

    /// Probe the whole-block fast path without touching any paged entries.
    pub(crate) fn is_whole_and_touch(&self, id: &BlockId) -> bool {
        self.whole_and_touch(id).is_some()
    }

    pub(crate) fn whole_and_touch(&self, id: &BlockId) -> Option<Option<DiskId>> {
        if self.catalog {
            let entries = self.entries.read().unwrap();
            let (_, geometry, _) = entries.resident(id)?;
            if geometry.kind() != Some(StoreKind::Whole) {
                return None;
            }
            geometry
                .policy_state(UnitKind::Whole)?
                .touch(entries.clock(entries.slot(id)?));
            return Some(geometry.disk_id);
        }
        self.read_entry_with_policy(id, |entry, clock, legacy| {
            let IndexedUnits::Whole(slot) = &entry.units else {
                return None;
            };
            slot.access.touch(clock);
            if let Some(access) = legacy.and_then(|p| p.access(UnitKind::Whole)) {
                access.touch();
            }
            Some(entry.meta.disk_id)
        })?
    }

    #[cfg(test)]
    pub(crate) fn set_access(&self, unit: &CacheUnit, access: AccessHandle) {
        let mut entries = self.entries.write().unwrap();
        let Some(directory) = entries.get_mut(unit.block()) else {
            return;
        };
        let Some(resident) = &directory.resident else {
            return;
        };
        let present = {
            let entry = resident.block.read().unwrap();
            match (&entry.as_ref().unwrap().units, unit.kind()) {
                (IndexedUnits::Whole(_), UnitKind::Whole) => true,
                (IndexedUnits::Paged(pages), UnitKind::Page(page)) => pages
                    .read()
                    .unwrap()
                    .get(&page.0)
                    .is_some_and(|slot| slot.unit.access.resident()),
                _ => false,
            }
        };
        if present {
            directory
                .legacy_mut()
                .attached
                .insert(unit.kind().order(), access);
        }
    }

    /// Snapshot the `(BlockId, len)` of every currently-tracked block.
    ///
    /// Used to seed the eviction tracker at startup from the index rebuilt off
    /// on-disk cache, so already-resident blocks count against capacity from the
    /// first request (issue #159).
    pub fn snapshot_lens(&self) -> Vec<(BlockId, u64)> {
        if self.catalog {
            return self
                .entries
                .read()
                .unwrap()
                .residents()
                .map(|(key, geometry)| (key.materialize(), geometry.len()))
                .collect();
        }
        self.with_snapshot(|entries| {
            entries
                .iter()
                .map(|entry| {
                    let e = entry.as_ref().expect("live directory entry");
                    (e.meta.id.materialize(), e.meta.len)
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
        if self.catalog {
            let entries = self.entries.read().unwrap();
            let mut units = Vec::new();
            for (key, geometry) in entries.residents() {
                let paged = geometry.kind() == Some(StoreKind::Paged);
                units.extend(geometry.pages_after(None).map(|(page, bytes)| {
                    (key.materialize(), paged.then_some(PageIndex(page)), bytes)
                }));
            }
            return units;
        }
        self.with_snapshot(|entries| {
            let mut out = Vec::new();
            for guard in entries {
                let entry = guard.as_ref().expect("live directory entry");
                let meta = &entry.meta;
                match &entry.units {
                    IndexedUnits::Whole(slot) => {
                        out.push((meta.id.materialize(), None, slot.allocated))
                    }
                    IndexedUnits::Paged(pages) => {
                        for (&page, slot) in pages.read().unwrap().iter() {
                            if slot.unit.access.resident() {
                                out.push((
                                    meta.id.materialize(),
                                    Some(PageIndex(page)),
                                    slot.unit.allocated,
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
        self.commit_allocated_at(meta, None, allocated);
    }

    pub(crate) fn commit_allocated_at(
        &self,
        meta: BlockMeta,
        disk_id: Option<DiskId>,
        allocated: impl IntoIterator<Item = (Option<PageIndex>, u64)>,
    ) {
        let mut meta = StoredMeta::from(meta);
        meta.disk_id = disk_id;
        self.commit_shared(meta, allocated);
    }

    pub(crate) fn commit_stored(&self, block: StoredBlock) {
        self.commit_shared(block.meta, block.allocated);
    }

    fn commit_shared(
        &self,
        meta: StoredMeta,
        allocated: impl IntoIterator<Item = (Option<PageIndex>, u64)>,
    ) {
        let id = meta.id.clone();
        let mut entry = IndexedBlock::allocated(meta, allocated);
        let pages = entry.materialized_pages();
        let added = entry.charged_bytes;
        if self.page_work.tracking() {
            if let IndexedUnits::Paged(table) = &mut entry.units {
                let rows = Arc::get_mut(table).unwrap().get_mut().unwrap();
                rows.track_resident_ages();
            }
        }
        let mut entries = self.entries.write().unwrap();
        // Structural mutations take directory -> entry. Ordinary operations
        // never reacquire the directory while holding an entry lock.
        let (key, slot) = entries.get_or_insert(id.clone());
        entry.meta.id = key;
        let previous = slot.resident.as_ref().map(|old| {
            old.block
                .write()
                .unwrap()
                .take()
                .expect("live directory entry")
        });
        let old_bytes = previous.as_ref().map_or(0, |old| old.charged_bytes);
        let old_pages = previous.as_ref().map_or(0, |old| old.materialized_pages());
        self.replace_bytes(old_bytes, added);
        if pages >= old_pages {
            self.resident_pages
                .fetch_add(pages - old_pages, Ordering::Relaxed);
        } else {
            self.resident_pages
                .fetch_sub(old_pages - pages, Ordering::Relaxed);
        }
        let key = entry.meta.id.clone();
        let replaced = slot.resident.replace(Arc::new(Entry::new(entry)));
        if replaced.is_none() {
            self.resident_blocks.fetch_add(1, Ordering::Relaxed);
        }
        let cursor = entries.slot(&key).unwrap();
        if let Some(old) = replaced.filter(|old| old.paged) {
            self.page_work.detached(&key, cursor, Some(&old.attached));
        }
        if pages != 0 {
            self.page_work.mark_structural(&key, cursor);
        }
        let position = entries.slot(&id).expect("committed directory slot");
        let clock = entries.clock(position);
        self.compact.invalidate_block(position);
        if let Some(clock) = clock {
            clock.store(0, Ordering::Relaxed);
        }
        if let Some(previous) = &previous {
            self.refund_block(previous);
        }
        if self.compact_enabled.load(Ordering::Relaxed) {
            let (_, slot) = entries.at(position).unwrap();
            let mut guard = slot.resident.as_ref().unwrap().block.write().unwrap();
            self.admit_block(guard.as_mut().unwrap(), clock);
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
        let id = BlockKey::new(&id);
        let mut entry = Entry::new(
            StoredMeta {
                id: id.clone(),
                disk_id: None,
                form: BlockForm::Paged {
                    page_size,
                    present: PresentBitmap::new(page_count),
                },
                len,
            }
            .into(),
        );
        let mut entries = self.entries.write().unwrap();
        let (key, slot) = entries.get_or_insert(id);
        entry.block.get_mut().unwrap().as_mut().unwrap().meta.id = key;
        if slot.resident.is_some() {
            return false;
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
        self.mark_page_allocated_at(id, page, allocated, None)
    }

    pub(crate) fn mark_page_allocated_at(
        &self,
        id: &BlockId,
        page: PageIndex,
        allocated: u64,
        disk_id: Option<DiskId>,
    ) -> bool {
        self.write_paged_entry(id, |entry, cursor, clock, attached| {
            let IndexedUnits::Paged(pages) = &mut entry.units else {
                return false;
            };
            if let Some(disk_id) = disk_id {
                debug_assert!(entry
                    .meta
                    .disk_id
                    .map_or(true, |current| current == disk_id));
                entry.meta.disk_id = Some(disk_id);
            }
            if page.0 >= entry.meta.page_count {
                return false;
            }
            let mut pages = pages.write().unwrap();
            let slot = pages.get_or_insert_with(page.0, PageEntry::default);
            let fresh = !slot.unit.access.set_resident(true);
            let old = std::mem::replace(&mut slot.unit.allocated, allocated);
            if self.compact_enabled.load(Ordering::Relaxed) {
                let position = cursor;
                // A replacement is a distinct incarnation even if the row survives.
                self.compact.invalidate(position, UnitKind::Page(page));
                if slot.unit.access.admit() {
                    entry.policy_units += 1;
                    entry.policy_bytes += allocated;
                    self.compact.add(allocated, 1);
                } else {
                    entry.policy_bytes = entry.policy_bytes - old + allocated;
                    self.compact.replace_bytes(old, allocated);
                }
                slot.unit.access.reset_reference();
                entry.policy_revision = self.record_admission(clock);
            }
            if fresh {
                if self.page_work.tracking() {
                    pages.track_age(page.0);
                }
                self.page_work.added_page(&entry.meta.id, cursor, attached);
            }
            entry.charged_bytes = entry
                .charged_bytes
                .saturating_sub(old)
                .saturating_add(allocated);
            entry.charged_pages += u64::from(fresh);
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
        let cleared = self
            .write_paged_entry(id, |entry, cursor, clock, attached| {
                let IndexedUnits::Paged(pages) = &mut entry.units else {
                    return false;
                };
                let mut pages = pages.write().unwrap();
                let Some(slot) = pages.get_mut(&page.0) else {
                    return false;
                };
                let admitted = slot.unit.access.admitted();
                if !slot.unit.access.set_resident(false) {
                    return false;
                }
                let bytes = std::mem::take(&mut slot.unit.allocated);
                entry.charged_bytes = entry.charged_bytes.saturating_sub(bytes);
                entry.charged_pages -= 1;
                if entry.charged_pages == 0 {
                    entry.meta.disk_id = None;
                }
                if admitted {
                    self.compact.invalidate(cursor, UnitKind::Page(page));
                    self.compact.subtract(bytes, 1);
                    entry.policy_units -= 1;
                    entry.policy_bytes -= bytes;
                    if let Some(clock) = clock.filter(|_| entry.policy_units == 0) {
                        clock.store(0, Ordering::Relaxed);
                    }
                }
                let retry = pages.forget_page(page.0);
                pages.remove(&page.0);
                self.page_work.removed_page(
                    &entry.meta.id,
                    cursor,
                    page.0,
                    entry.charged_pages == 0,
                    retry,
                    attached,
                );
                self.replace_bytes(bytes, 0);
                self.resident_pages.fetch_sub(1, Ordering::Relaxed);
                true
            })
            .unwrap_or(false);
        #[cfg(test)]
        if cleared && !self.compact_enabled.load(Ordering::Relaxed) {
            if let Some(entry) = self.entries.write().unwrap().get_mut(id) {
                if let Some(legacy) = &mut entry.legacy {
                    legacy.attached.remove(&UnitKind::Page(page).order());
                }
                entry.prune_legacy();
            }
        }
        cleared
    }

    /// Decide how a read over `[start_page, end_page)` should be served.
    ///
    /// `start_page`/`end_page` are only consulted for paged blocks; for a whole
    /// block the answer is always [`Presence::Whole`].
    pub fn presence(&self, id: &BlockId, start_page: PageIndex, end_page: PageIndex) -> Presence {
        if self.catalog {
            let entries = self.entries.read().unwrap();
            let Some((_, geometry, _)) = entries.resident(id) else {
                return Presence::Miss;
            };
            if geometry.kind() == Some(StoreKind::Whole) {
                return Presence::Whole;
            }
            let mut present = geometry.pages_after(start_page.0.checked_sub(1));
            return if (start_page.0..end_page.0)
                .all(|expected| present.next().is_some_and(|(page, _)| page == expected))
            {
                Presence::PageHit
            } else {
                Presence::PageMiss
            };
        }
        self.read_entry(id, |entry| match &entry.units {
            IndexedUnits::Whole(_) => Presence::Whole,
            IndexedUnits::Paged(pages) => {
                let pages = pages.read().unwrap();
                if (start_page.0..end_page.0).all(|page| {
                    pages
                        .get(&page)
                        .is_some_and(|slot| slot.unit.access.resident())
                }) {
                    Presence::PageHit
                } else {
                    Presence::PageMiss
                }
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
        let cursor = entries.slot(id)?;
        let key = entries.at(cursor)?.0.clone();
        let slot = entries.get_mut(id)?;
        let entry = slot.resident.take()?;
        if entry.paged {
            self.page_work.detached(&key, cursor, Some(&entry.attached));
        } else {
            entry.attached.store(false, Ordering::Release);
        }
        let position = cursor;
        #[cfg(test)]
        if let Some(legacy) = &mut slot.legacy {
            legacy.attached.clear();
        }
        slot.prune_legacy();
        if slot.is_empty() {
            entries.remove(id);
        }
        self.compact.invalidate_block(position);
        if let Some(clock) = entries.clock(position) {
            clock.store(0, Ordering::Relaxed);
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
        self.refund_block(&removed);
        self.replace_bytes(removed.charged_bytes, 0);
        self.resident_pages
            .fetch_sub(removed.materialized_pages(), Ordering::Relaxed);
        Some(removed.metadata().into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn small_locators_own_policy_through_delete_and_readmission() {
        use crate::{small_store::SmallObjectStore, Lru};
        let root = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SmallObjectStore::open(root.path().to_owned(), 128 << 10, 256 << 10, 4 << 20, None)
                .unwrap(),
        );
        let index = Arc::new(BlockIndex::new());
        let mut units = Vec::new();
        store.restore_policy_units(&index, &mut units, false);
        let lru = Arc::new(Lru::from_recovered(index.clone(), units, 0, false));
        store.bind_policy(lru.clone());
        let id = block(0);
        let unit = CacheUnit::Whole(id.clone());
        for _ in 0..20 {
            store
                .put(&id, bytes::Bytes::from_static(b"payload"))
                .await
                .unwrap();
            assert!(index.entries.read().unwrap().is_empty());
            assert_eq!(lru.len(), 1);
            assert_eq!(index.policy_units(&id).len(), 1);
            assert!(lru.pin(&unit));
            let old = index.policy_get(&unit).unwrap().access;
            store.delete(&id).await.unwrap();
            assert!(!store.contains(&id));
            assert!(index.policy_get(&unit).is_some());
            lru.unpin(&unit);
            lru.remove(&unit);
            assert!(index.small_directory(&id).is_none());
            store
                .put(&id, bytes::Bytes::from_static(b"payload"))
                .await
                .unwrap();
            let new = index.policy_get(&unit).unwrap().access;
            assert!(!Arc::ptr_eq(old.small().unwrap(), new.small().unwrap()));
            old.touch();
            assert_eq!(store.read(&id).await.unwrap().unwrap(), b"payload"[..]);
            lru.remove(&unit);
            assert!(store.contains(&id));
            store.delete(&id).await.unwrap();
            assert!(index.small_directory(&id).is_none());
        }
        assert_eq!(lru.total_bytes(), 0);
        assert!(index.entries.read().unwrap().is_empty());
    }
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

    fn with_directory_reader(index: &BlockIndex, scan: impl FnOnce() + Send) {
        std::thread::scope(|scope| {
            let reader = index.entries.read().unwrap();
            let (done, finished) = std::sync::mpsc::channel();
            let task = scope.spawn(move || {
                scan();
                done.send(()).unwrap();
            });
            let result = finished.recv_timeout(std::time::Duration::from_secs(5));
            drop(reader);
            task.join().unwrap();
            assert!(result.is_ok(), "scan waited for the directory reader");
        });
    }

    #[test]
    fn tti_scan_preserves_concurrent_directory_readers() {
        let index = Arc::new(BlockIndex::new());
        let life = crate::page_lifecycle::PageLifecycle::with_index(index.clone(), true);
        let id = block(1);
        index.init_paged(id.clone(), 4096, 4096);
        index.mark_page_allocated(&id, PageIndex(0), 4096);
        life.block(&id)
            .register(PageIndex(0), Some(100), 100, false);

        with_directory_reader(&index, || {
            let (candidates, report) = life.scan(
                &mut crate::page_lifecycle::ScanCursor::default(),
                100,
                200,
                Some(50),
                100,
            );
            assert!(report.completed);
            assert_eq!(report.checked, 1);
            assert_eq!(candidates.len(), 1);
        });
        assert!(life.get(&id).is_some());
        assert_eq!(index.page_count(), 1);
    }

    #[test]
    fn pending_lifecycle_cleanup_preserves_concurrent_directory_readers() {
        let index = Arc::new(BlockIndex::new());
        let life = crate::page_lifecycle::PageLifecycle::with_index(index.clone(), false);
        let id = block(1);
        index.init_paged(id.clone(), 4096, 4096);
        index.mark_page_allocated(&id, PageIndex(0), 4096);
        index.clear_page(&id, PageIndex(0));

        with_directory_reader(&index, || {
            let (_, report) = life.scan(
                &mut crate::page_lifecycle::ScanCursor::default(),
                100,
                200,
                None,
                100,
            );
            assert!(report.completed);
            assert_eq!(report.empty.len(), 1);
        });
        assert!(life.get(&id).is_some());
        life.block(&id).cleanup_finished();
        index.remove(&id);
        assert!(life.get(&id).is_none());
    }

    #[test]
    fn idle_scan_preserves_concurrent_directory_readers() {
        let index = Arc::new(BlockIndex::new());
        let id = block(1);
        index.commit(whole_meta(id.clone(), 4096));
        let lru = index.restore_lru();
        let access = lru.insert(CacheUnit::Whole(id.clone()), 4096);
        access.clock().store(100, Ordering::Relaxed);

        with_directory_reader(&index, || {
            let candidates = lru.idle_candidates(&mut (0, 0), 200, 50, 100, 100);
            assert_eq!(candidates.len(), 1);
            assert!(lru.stale_candidate_is_current(&candidates[0]));
            access.touch();
            assert!(!lru.stale_candidate_is_current(&candidates[0]));
        });
    }

    #[test]
    fn ownership_scan_and_deferral_preserve_concurrent_directory_readers() {
        let index = Arc::new(BlockIndex::new());
        let id = block(1);
        index.commit(whole_meta(id.clone(), 4096));
        let lru = index.restore_lru();
        let limit = lru.ownership_admission_ceiling();
        lru.insert(CacheUnit::Whole(id.clone()), 4096);
        with_directory_reader(&index, || {
            assert!(lru.first_ownership_candidate(0..limit).is_none());
            let next_limit = lru.ownership_admission_ceiling();
            let candidate = lru.first_ownership_candidate(limit..next_limit).unwrap();
            assert_eq!(candidate.block, id);
            assert!(lru.stale_candidate_is_current(&candidate));
            assert!(lru.refresh_ownership_candidate(&candidate).is_some());
        });
    }

    #[tokio::test]
    async fn mutation_gate_survives_residency_changes_without_an_index_row() {
        let index = Arc::new(BlockIndex::new());
        let life = crate::page_lifecycle::PageLifecycle::with_index(index.clone(), true);
        let id = block(1);
        let pending = life.block(&id);
        let held = pending.lock().await;
        assert!(index.entries.read().unwrap().is_empty());
        index.init_paged(id.clone(), 4096, 4096);
        index.mark_page_allocated(&id, PageIndex(0), 4096);
        index.remove(&id).unwrap();
        assert!(index.entries.read().unwrap().is_empty());
        index.commit(whole_meta(id.clone(), 4096));
        let current = life.block(&id);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), current.lock())
                .await
                .is_err()
        );
        drop(held);
        let _held = current.lock().await;
        assert_eq!(index.resident_bytes(), 4096);
        index.remove(&id).unwrap();
        assert!(index.entries.read().unwrap().is_empty());
    }

    #[test]
    fn lifecycle_scan_retires_unpublished_blocks_from_the_shared_directory() {
        let index = Arc::new(BlockIndex::new());
        let life = crate::page_lifecycle::PageLifecycle::with_index(index.clone(), true);
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
        let life = crate::page_lifecycle::PageLifecycle::with_index(index.clone(), true);
        let id = block(1);
        index.init_paged(id.clone(), 4096, 8192);
        let block = life.block(&id);
        let p0 = PageIndex(0);
        index.mark_page_allocated(&id, p0, 4096);
        block.register(p0, Some(10), 10, false);
        let candidate = block.candidate(p0).unwrap();
        let read = block.acquire(p0).unwrap();
        assert!(!block.claim(&candidate, 100, None));

        index.mark_page_allocated(&id, PageIndex(1), 8192);
        block.register(PageIndex(1), Some(10), 10, false);
        drop(read);
        assert!(block.claim(&candidate, 100, None));
        block.abort(&candidate, 1);
        assert_eq!(index.resident_bytes(), 12288);
        assert!(block.acquire(p0).is_some());
        let candidate = block.candidate(p0).unwrap();
        assert!(block.claim(&candidate, 100, None));
        assert!(index.clear_page(&id, p0));
        assert_eq!(index.resident_bytes(), 8192);
        assert!(block.acquire(p0).is_none());

        index.mark_page_allocated(&id, p0, 16384);
        block.register(p0, Some(101), 101, false);
        assert!(!block.claim(&candidate, 102, None));
        block.abort(&candidate, 1);
        assert!(block.acquire(p0).is_some());
        assert_eq!(index.resident_bytes(), 24576);
        assert_eq!(index.page_count(), 2);
    }

    #[test]
    fn recovery_and_readmission_share_the_catalog_identity() {
        let id = BlockKey::new(&block(1));
        let mut index = BlockIndex::new();
        index.restore_allocated(
            vec![StoredBlock {
                meta: StoredMeta {
                    id: id.clone(),
                    disk_id: None,
                    len: 4096,
                    form: BlockForm::Whole,
                },
                allocated: vec![(None, 4096)],
            }],
            1,
        );
        for bytes in [8192, 16384] {
            let entries = index.entries.read().unwrap();
            let (key, slot) = entries.get_key_value(&id).unwrap();
            assert!(key.ptr_eq(&id));
            assert!(BlockKey::ptr_eq(
                &slot
                    .resident
                    .as_ref()
                    .unwrap()
                    .block
                    .read()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .meta
                    .id,
                &id
            ));
            drop(entries);
            index.commit_allocated(whole_meta(id.materialize(), bytes), [(None, bytes)]);
        }
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
                        }
                        .into(),
                        allocated: vec![(Some(PageIndex(7)), 8192), (Some(PageIndex(0)), 0)],
                    }
                })
                .chain(std::iter::once(StoredBlock {
                    meta: whole_meta(block(3), 10).into(),
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
            serial.commit_stored(block);
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
            Ok((whole_meta(block(n), 32).into(), [(None, 8192)]))
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
        let life = crate::page_lifecycle::PageLifecycle::with_index(index.clone(), true);
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
        let life = crate::page_lifecycle::PageLifecycle::with_index(index.clone(), true);
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
    fn compact_recovery_and_admission_leave_idle_units_inline() {
        use crate::eviction::Lru;
        use std::mem::size_of;

        assert_eq!(size_of::<UnitState>(), 4);
        assert_eq!(size_of::<UnitSlot>(), 16);
        assert_eq!(size_of::<PageUnit>(), 16);
        for track in [false, true] {
            let index = Arc::new(BlockIndex::new());
            let whole = block(1);
            let paged = block(2);
            index.commit(whole_meta(whole.clone(), 4096));
            index.init_paged(paged.clone(), 4096, 8192);
            assert!(index.mark_page_allocated(&paged, PageIndex(0), 0));
            let lru = Lru::from_runtime(index.clone(), Vec::new(), 2, track);
            let lifecycle = crate::page_lifecycle::PageLifecycle::with_index(index.clone(), false);

            let life = lifecycle.block(&paged);
            life.register(PageIndex(1), None, 0, false);
            assert!(index.mark_page_allocated(&paged, PageIndex(1), 8192));
            assert_eq!((lru.len(), lru.total_bytes()), (3, 12288));
            assert_eq!(index.compact.active_units(), 0);
            let entries = index.entries.read().unwrap();
            for id in [&whole, &paged] {
                let position = entries.slot(id).unwrap();
                assert!(entries.at(position).unwrap().1.legacy.is_none());
                assert_eq!(
                    entries
                        .clock(position)
                        .map(|clock| clock.load(Ordering::Relaxed) != 0),
                    track.then_some(true)
                );
            }
            drop(entries);

            let mut candidates = lru.block_candidates(&paged);
            assert_eq!(index.compact.active_units(), 2);
            let (reader, _) = index
                .page_reads_and_touch(&paged, PageIndex(0), PageIndex(1))
                .unwrap();
            assert!(!lru.candidate_is_current(&candidates[0]));
            assert!(lru.candidate_is_current(&candidates[1]));
            drop(reader);
            candidates.clear();
            assert_eq!(index.compact.active_units(), 0);
            assert!(index.clear_page(&paged, PageIndex(0)));
            assert_eq!((lru.len(), lru.total_bytes()), (2, 12288));
            index.remove(&whole);
            index.remove(&paged);
            assert!(lru.is_empty());
        }
    }

    #[test]
    fn compact_tickets_and_pins_cannot_cross_unit_or_directory_reuse() {
        use crate::eviction::Lru;

        let index = Arc::new(BlockIndex::new());
        let id = block(8);
        let page = PageIndex(0);
        let unit = CacheUnit::Page(id.clone(), page);
        let lru = Arc::new(Lru::from_runtime(index.clone(), Vec::new(), 0, true));
        assert!(index.init_paged(id.clone(), 4096, 4096));
        assert!(index.mark_page_allocated(&id, page, 4096));
        let old = lru.block_candidates(&id).pop().unwrap();
        lru.touch(&unit);
        assert!(!lru.candidate_is_current(&old));
        // A CLOCK pass may clear the reference bit without selecting this unit.
        assert!(lru
            .candidates_to_fit_bounded(0, &Default::default(), 1, 1)
            .is_empty());
        assert!(!lru.candidate_is_current(&old));
        let newer = lru.candidates_to_fit(0, &Default::default()).pop().unwrap();
        assert!(lru.candidate_is_current(&newer));
        assert!(!lru.candidate_is_current(&old));
        drop(old);
        assert!(lru.candidate_is_current(&newer));

        let previous = lru
            .first_ownership_candidate(0..lru.ownership_admission_ceiling())
            .unwrap();
        let pin = lru.pin_guard(unit.clone()).unwrap();
        assert!(index.clear_page(&id, page));
        assert!(index.mark_page_allocated(&id, page, 8192));
        let replacement_pin = lru.pin_guard(unit.clone()).unwrap();
        drop(pin);
        assert!(!lru.candidate_is_current(&newer));
        assert!(!lru.stale_candidate_is_current(&previous));
        assert!(lru.candidates_to_fit(0, &Default::default()).is_empty());
        drop(replacement_pin);

        let last = lru.block_candidates(&id).pop().unwrap();
        let pin = lru.pin_guard(unit.clone()).unwrap();
        let position = index.entries.read().unwrap().slot(&id).unwrap();
        index.remove(&id);
        index.init_paged(id.clone(), 4096, 4096);
        index.mark_page_allocated(&id, page, 4096);
        assert_eq!(index.entries.read().unwrap().slot(&id), Some(position));
        let replacement_pin = lru.pin_guard(unit).unwrap();
        drop(pin);
        assert!(lru.candidates_to_fit(0, &Default::default()).is_empty());
        drop(replacement_pin);
        assert!(!lru.candidate_is_current(&last));
        let current = lru.block_candidates(&id).pop().unwrap();
        drop(last);
        assert!(lru.candidate_is_current(&current));
        drop(current);
        assert_eq!(index.compact.active_units(), 0);
        assert_eq!((lru.len(), lru.total_bytes()), (1, 4096));
    }

    #[test]
    fn retained_page_tokens_do_not_touch_a_new_policy_admission() {
        let index = Arc::new(BlockIndex::new());
        let id = block(1);
        index.init_paged(id.clone(), 4096, 4096);
        index.mark_page_allocated(&id, PageIndex(0), 4096);
        let lru = index.restore_lru();
        let unit = CacheUnit::Page(id.clone(), PageIndex(0));
        let old = lru.insert(unit.clone(), 4096);
        let (reader, _) = index
            .page_reads_and_touch(&id, PageIndex(0), PageIndex(1))
            .unwrap();
        let life = crate::page_lifecycle::PageLifecycle::with_index(index.clone(), false);
        let block = life.block(&id);
        for _ in 0..3 {
            lru.remove(&unit);
            let current = lru.insert(unit.clone(), 4096);
            let candidate = lru.block_candidates(&id).pop().unwrap();
            old.touch();
            assert!(
                lru.candidate_is_current(&candidate),
                "retired token reached a later admission"
            );
            current.touch();
            assert!(!lru.candidate_is_current(&candidate));
            assert!(
                block.claim_all_pages().is_none(),
                "policy generations must preserve existing page readers"
            );
        }
        reader.record_access_and_release(None);
        assert!(block.claim_all_pages().is_some());
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
            let lifecycle = crate::page_lifecycle::PageLifecycle::with_index(index.clone(), true);
            for page in [0, pages - 1] {
                lifecycle
                    .block(&paged)
                    .register(PageIndex(page), None, 0, false);
            }
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
    fn page_lookup_partial_pin_failure_releases_readers() {
        use crate::eviction::Lru;
        let idx = Arc::new(BlockIndex::new());
        let lru = Lru::new();
        let id = block(1);
        let life = crate::page_lifecycle::PageLifecycle::with_index(idx.clone(), true);
        let block = life.block(&id);
        idx.init_paged(id.clone(), 4096, 8192);
        for p in 0..2 {
            let page = PageIndex(p);
            let unit = CacheUnit::Page(id.clone(), page);
            idx.mark_page(&id, page);
            idx.set_access(&unit, lru.insert(unit.clone(), 4096));
            block.register(page, Some(10), 10, false);
        }
        assert!(idx
            .page_reads_and_touch(&id, PageIndex(0), PageIndex(2))
            .is_some());
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
        let life = crate::page_lifecycle::PageLifecycle::with_index(idx.clone(), true);
        life.block(&id).register(PageIndex(0), None, 0, false);
        assert!(idx
            .page_reads_and_touch(&id, PageIndex(0), PageIndex(1))
            .is_some());
        assert_eq!(lru.evict_to_fit(4096), vec![p1.clone()]);
        idx.clear_page(&id, PageIndex(1));
        assert!(idx
            .read_entry(&id, |entry| {
                let IndexedUnits::Paged(pages) = &entry.units else {
                    panic!("paged block")
                };
                pages
                    .read()
                    .unwrap()
                    .get(&1)
                    .is_none_or(|slot| !slot.unit.access.resident())
            })
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
        use std::sync::mpsc;
        use std::time::Duration;

        let index = Arc::new(BlockIndex::new());
        let busy = block(1);
        let other = block(2);
        index.init_paged(busy.clone(), 4096, 8192);
        index.init_paged(other.clone(), 4096, 8192);
        let lifecycle = crate::page_lifecycle::PageLifecycle::with_index(index.clone(), true);
        index.enable_compact_policy(true);
        let busy_entry = index.entry(&busy).unwrap();
        let busy_guard = busy_entry.block.write().unwrap();
        // A directory reader must not prevent mutations to existing entries.
        let directory_guard = index.entries.read().unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let page = PageIndex(0);
                assert!(!index.init_paged(other.clone(), 4096, 8192));
                assert!(index.mark_page_allocated(&other, page, 4096));
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

        for compact in [false, true] {
            let index = BlockIndex::new();
            let id = block(1);
            index.init_paged(id.clone(), 4096, 4096);
            if compact {
                index.enable_compact_policy(true);
            }
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
            assert_eq!(
                index.compact.totals(),
                if compact { (4096, 1) } else { (0, 0) }
            );
        }
    }

    #[test]
    fn zero_allocation_presence_is_distinct_from_pending_and_policy_only_pages() {
        let index = Arc::new(BlockIndex::new());
        let id = block(1);
        index.init_paged(id.clone(), 4096, 4096);
        let life = crate::page_lifecycle::PageLifecycle::with_index(index.clone(), false);
        let state = life.block(&id);
        state.register(PageIndex(0), None, 0, false);
        let lru = index.restore_lru();
        lru.insert(CacheUnit::Page(id.clone(), PageIndex(0)), 0);
        assert_eq!(
            index.presence(&id, PageIndex(0), PageIndex(1)),
            Presence::PageMiss
        );
        assert!(index.snapshot_units().is_empty());
        assert!(index
            .page_reads_and_touch(&id, PageIndex(0), PageIndex(1))
            .is_none());
        assert!(index.mark_page_allocated(&id, PageIndex(0), 0));
        assert!(index
            .page_reads_and_touch(&id, PageIndex(0), PageIndex(1))
            .is_some());
        assert_eq!(
            index.snapshot_units(),
            [(id.clone(), Some(PageIndex(0)), 0)]
        );
        assert!(index.clear_page(&id, PageIndex(0)));
        assert_eq!(
            index.presence(&id, PageIndex(0), PageIndex(1)),
            Presence::PageMiss
        );
        assert_eq!((index.page_count(), index.resident_bytes()), (0, 0));
        let BlockForm::Paged { present, .. } = index.get(&id).unwrap().form else {
            panic!()
        };
        assert_eq!(present.count(), 0);
    }

    #[test]
    fn detached_retirement_refunds_charges_after_shared_pages_are_removed() {
        use std::time::{Duration, Instant};
        for (allocated, bind) in [(0, false), (0, true), (4096, true)] {
            let index = Arc::new(BlockIndex::new());
            let id = block(1);
            index.init_paged(id.clone(), 4096, 4096);
            index.mark_page_allocated(&id, PageIndex(0), allocated);
            let lru = index.restore_lru();
            let life = crate::page_lifecycle::PageLifecycle::with_index(index.clone(), false);
            let _candidate = bind.then(|| {
                let state = life.block(&id);
                let candidate = state.candidate(PageIndex(0)).unwrap();
                assert!(state.claim(&candidate, 0, None));
                candidate
            });
            let entry = index.entry(&id).unwrap();
            let held = entry.block.read().unwrap();
            std::thread::scope(|scope| {
                let removal = scope.spawn(|| index.remove(&id));
                let deadline = Instant::now() + Duration::from_secs(5);
                while index.entry(&id).is_some() && Instant::now() < deadline {
                    std::thread::yield_now();
                }
                let detached = index.entry(&id).is_none();
                if detached {
                    lru.remove(&CacheUnit::Page(id.clone(), PageIndex(0)));
                }
                drop(held);
                assert!(detached);
                assert!(removal.join().unwrap().is_some());
            });
            assert_eq!(
                (index.len(), index.page_count(), index.resident_bytes()),
                (0, 0, 0)
            );
        }
    }

    #[test]
    fn removal_releases_directory_before_retiring_and_allows_reinsertion() {
        use std::sync::mpsc;
        use std::time::{Duration, Instant};

        let index = Arc::new(BlockIndex::new());
        let id = block(1);
        let lifecycle = crate::page_lifecycle::PageLifecycle::with_index(index.clone(), true);
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
            let writer = scope.spawn(|| index.mark_page_allocated(&id, PageIndex(1), 4096));
            let deadline = Instant::now() + Duration::from_secs(5);
            while Arc::strong_count(&old) < 3 && Instant::now() < deadline {
                std::thread::yield_now();
            }
            let writer_waiting = Arc::strong_count(&old) >= 3;
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
            assert!(writer_waiting, "page writer did not retain the old entry");
            assert!(
                detached,
                "removal held the directory while waiting for an entry"
            );
            assert!(!writer.join().unwrap());
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
        assert_eq!(lifecycle.dirty_summary(), (0, None));
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
            .legacy_pages(&id)
            .map(|pages| pages.read().unwrap().is_empty())
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
    fn runtime_recovery_rebinds_a_reused_index_without_stale_policy_or_work() {
        let index = Arc::new(BlockIndex::new());
        let id = block(1);
        let unit = CacheUnit::Page(id.clone(), PageIndex(0));
        index.init_paged(id.clone(), 4096, 4096);
        index.mark_page_allocated(&id, PageIndex(0), 4096);
        let mut previous_clock = None;
        for _ in 0..2 {
            index.prepare_runtime_recovery();
            let lru = index.restore_lru();
            let life = crate::page_lifecycle::PageLifecycle::with_index(index.clone(), true);
            life.block(&id).register(PageIndex(0), Some(10), 10, false);
            let access = index.policy_get(&unit).unwrap().access;
            if let Some(previous) = previous_clock {
                assert!(!Arc::ptr_eq(&previous, access.clock()));
            }
            previous_clock = Some(access.clock().clone());
            assert_eq!(lru.block_candidates(&id).len(), 1);
            assert_eq!(index.policy_block_get(&id).unwrap().units, 1);
            assert!(index
                .page_reads_and_touch(&id, PageIndex(0), PageIndex(1))
                .is_some());
            assert_eq!((index.resident_bytes(), index.page_count()), (4096, 1));
            lru.insert_with_policy(CacheUnit::Whole(block(2)), 8192, true);
        }
        index.prepare_runtime_recovery();
        assert!(index.policy_get(&CacheUnit::Whole(block(2))).is_none());
        assert!(!index.page_work.tracking());
        assert_eq!(index.len(), 1);
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
                .legacy
                .as_ref()
                .unwrap()
                .units
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
        let lifecycle = crate::page_lifecycle::PageLifecycle::with_index(index.clone(), true);
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
