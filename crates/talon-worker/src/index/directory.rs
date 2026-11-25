use super::DirectoryEntry;
use crate::block_key::{BlockIdentity, BlockKey};
use crate::catalog::{BlockRow, BlockRows, Geometry, Operation, StoreKind};
use crate::eviction::{access_millis, compact::CompactPolicy, UnitKind};
use crate::page_lifecycle::{PageAge, PageWork};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, LazyLock};
#[cfg(test)]
use talon_core::BlockId;
use talon_core::PageIndex;

static EMPTY_RUNTIME_ROW: LazyLock<DirectoryEntry> = LazyLock::new(DirectoryEntry::default);

/// The journal and the index share these rows. Runtime policy and lifecycle
/// are attached by slot, and may keep an identity alive without any files.
#[derive(Default)]
pub(crate) struct Directory {
    pub(crate) rows: BlockRows,
    // Legacy and external policy only allocate an entry while they use a slot.
    runtime: BTreeMap<usize, DirectoryEntry>,
    clocks: Option<Vec<AtomicU64>>,
    revisions: CatalogRevisions,
    inline_ages: Option<Vec<PageAge>>,
    catalog_policy: bool,
    pub(super) compact: Arc<CompactPolicy>,
    page_size: Option<u32>,
    pub(super) page_work: Arc<PageWork>,
    blocks: u64,
    pages: u64,
    bytes: u128,
}

impl Directory {
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[cfg(test)]
    pub fn reserve(&mut self, additional: usize) {
        self.rows.reserve(additional);
    }

    pub fn get(&self, id: &dyn BlockIdentity) -> Option<&DirectoryEntry> {
        self.get_key_value(id).map(|(_, entry)| entry)
    }

    pub fn get_mut(&mut self, id: &dyn BlockIdentity) -> Option<&mut DirectoryEntry> {
        self.runtime.get_mut(&self.slot(id)?)
    }

    pub fn get_key_value(&self, id: &dyn BlockIdentity) -> Option<(&BlockKey, &DirectoryEntry)> {
        self.at(self.slot(id)?)
    }

    pub fn get_or_insert(&mut self, id: BlockKey) -> (BlockKey, &mut DirectoryEntry) {
        let slot = self.rows.get_or_insert(id) as usize - 1;
        self.ensure_clock(slot);
        let key = self.rows.at(slot).unwrap().id.clone();
        (key, self.runtime.entry(slot).or_default())
    }

    pub fn remove(&mut self, id: &dyn BlockIdentity) -> Option<DirectoryEntry> {
        let slot = self.rows.handle(id)? as usize - 1;
        let runtime = self.runtime.remove(&slot);
        self.release(id, slot);
        runtime
    }

    pub fn slot(&self, id: &dyn BlockIdentity) -> Option<usize> {
        Some(self.rows.handle(id)? as usize - 1)
    }

    pub fn slot_count(&self) -> usize {
        self.rows.slot_count()
    }

    pub fn at(&self, cursor: usize) -> Option<(&BlockKey, &DirectoryEntry)> {
        let row = self.rows.at(cursor)?;
        Some((&row.id, self.runtime_at(cursor)))
    }

    pub(super) fn at_or_after(&self, start: usize) -> Option<(usize, &BlockKey, &DirectoryEntry)> {
        let (slot, row) = self.rows.at_or_after(start)?;
        Some((slot, &row.id, self.runtime_at(slot)))
    }

    fn runtime_at(&self, slot: usize) -> &DirectoryEntry {
        self.runtime.get(&slot).unwrap_or(&EMPTY_RUNTIME_ROW)
    }

    pub fn versions(
        &self,
        id: &dyn BlockIdentity,
        mut visit: impl FnMut(usize, &BlockKey, &DirectoryEntry),
    ) {
        self.rows.versions(id, |handle, row| {
            visit(
                handle as usize - 1,
                &row.id,
                self.runtime_at(handle as usize - 1),
            )
        });
    }

    pub fn iter(&self) -> impl Iterator<Item = (&BlockKey, &DirectoryEntry)> {
        self.rows
            .iter()
            .map(|(handle, row)| (&row.id, self.runtime_at(handle as usize - 1)))
    }

    pub fn values(&self) -> impl Iterator<Item = &DirectoryEntry> {
        self.iter().map(|(_, entry)| entry)
    }

    pub fn retain(&mut self, mut keep: impl FnMut(&BlockKey, &mut DirectoryEntry) -> bool) {
        let mut removed = Vec::new();
        self.runtime.retain(|&slot, entry| {
            let row = self.rows.at(slot).unwrap();
            let retained = keep(&row.id, entry);
            if !retained {
                removed.push((row.id.clone(), slot));
            }
            retained
        });
        for (id, slot) in removed {
            self.release(&id, slot);
        }
    }

    fn release(&mut self, id: &dyn BlockIdentity, slot: usize) {
        self.rows.release(id);
        if self.rows.at(slot).is_none() {
            self.compact.invalidate_block(slot);
            self.clear_clock(slot);
        }
    }

    fn ensure_clock(&mut self, slot: usize) {
        if let Some(clocks) = &mut self.clocks {
            if slot >= clocks.len() {
                clocks.resize_with(slot + 1, AtomicU64::default);
                self.revisions.resize(slot + 1);
            }
        }
    }

    fn ensure_inline_age(&mut self, slot: usize) {
        if let Some(ages) = &mut self.inline_ages {
            if slot >= ages.len() {
                ages.resize_with(slot + 1, PageAge::unknown);
            }
        }
    }

    pub(super) fn inline_age(&self, slot: usize) -> Option<&PageAge> {
        self.inline_ages.as_ref()?.get(slot)
    }

    pub(super) fn enable_catalog_ages(&mut self) {
        self.inline_ages = Some((0..self.slot_count()).map(|_| PageAge::unknown()).collect());
        self.rows.for_each_paged_mut(Geometry::enable_page_ages);
    }

    pub(super) fn clock(&self, slot: usize) -> Option<&AtomicU64> {
        self.clocks.as_ref()?.get(slot)
    }

    pub(super) fn revision(&self, slot: usize) -> Option<u64> {
        self.revisions.get(slot)
    }

    pub(super) fn reset_clocks(&mut self, enabled: bool) {
        self.clocks = enabled.then(|| {
            (0..self.slot_count())
                .map(|_| AtomicU64::default())
                .collect()
        });
        self.revisions = CatalogRevisions::Off;
    }

    fn clear_clock(&mut self, slot: usize) {
        if let Some(clock) = self.clock(slot) {
            clock.store(0, Relaxed);
            self.revisions.set(slot, 0);
        }
    }

    fn record_catalog_admission(&mut self, slot: usize) {
        let revision = self.compact.next_revision();
        self.compact.admitted_at(revision);
        if let Some(clock) = self.clock(slot) {
            clock.fetch_max(access_millis().max(1), Relaxed);
            self.revisions.set(slot, revision);
        }
    }

    pub(super) fn enable_catalog_policy(&mut self) {
        self.catalog_policy = true;
        if self.clocks.is_some() {
            self.revisions = CatalogRevisions::Compact(vec![0; self.slot_count()]);
        }
        for slot in 0..self.slot_count() {
            if let Some((_, geometry, _)) = self.resident_at(slot) {
                let mut bytes = 0;
                let mut units = 0;
                for (kind, allocated) in geometry.policy_units_from(0) {
                    geometry.policy_state(kind).unwrap().publish();
                    bytes += u128::from(allocated);
                    units += 1;
                }
                self.compact.replace(0, 0, bytes, units);
                self.record_catalog_admission(slot);
            }
        }
    }

    pub(super) fn reset_catalog_policy(&mut self) {
        self.catalog_policy = false;
        self.inline_ages = None;
        self.rows.for_each_paged_mut(Geometry::reset_page_states);
        for (_, geometry) in self.residents() {
            if let Some(state) = geometry.policy_state(UnitKind::Whole) {
                state.retire();
            }
        }
    }

    pub(crate) fn attach(&mut self, page_size: u32) {
        if let Some(current) = self.page_size {
            assert_eq!(current, page_size, "shared catalog page geometry changed");
            return;
        }
        self.rows.restore_shadows();
        self.page_size = Some(page_size);
        let total = self
            .rows
            .iter()
            .fold(Usage::default(), |mut total, (handle, _)| {
                let usage = self.usage(handle, None);
                total.blocks += usage.blocks;
                total.pages += usage.pages;
                total.bytes += usage.bytes;
                total
            });
        self.blocks = total.blocks;
        self.pages = total.pages;
        self.bytes = total.bytes;
    }

    pub fn page_size(&self) -> u32 {
        self.page_size.unwrap_or_default()
    }

    pub fn totals(&self) -> (u64, u64, u64) {
        (
            self.blocks,
            self.pages,
            u64::try_from(self.bytes).unwrap_or(u64::MAX),
        )
    }

    fn visible<'a>(&'a self, handle: u32, row: &'a BlockRow) -> Option<&'a Geometry> {
        if self.page_size? == 0 {
            return self.rows.geometry(handle, StoreKind::Whole);
        }
        row.geometry.kind().map(|_| &row.geometry)
    }

    pub fn resident(
        &self,
        id: &dyn BlockIdentity,
    ) -> Option<(&BlockKey, &Geometry, &DirectoryEntry)> {
        self.resident_at(self.slot(id)?)
    }

    pub(super) fn resident_at(
        &self,
        slot: usize,
    ) -> Option<(&BlockKey, &Geometry, &DirectoryEntry)> {
        let row = self.rows.at(slot)?;
        Some((
            &row.id,
            self.visible(slot as u32 + 1, row)?,
            self.runtime_at(slot),
        ))
    }

    pub fn residents(&self) -> impl Iterator<Item = (&BlockKey, &Geometry)> {
        self.rows
            .iter()
            .filter_map(|(handle, row)| Some((&row.id, self.visible(handle, row)?)))
    }

    pub(crate) fn store_geometry(
        &self,
        kind: StoreKind,
        id: &dyn BlockIdentity,
    ) -> Option<&Geometry> {
        self.rows.geometry(self.rows.handle(id)?, kind)
    }

    fn usage(&self, handle: u32, page: Option<u32>) -> Usage {
        let Some(geometry) = self
            .rows
            .at(handle as usize - 1)
            .and_then(|r| self.visible(handle, r))
        else {
            return Usage::default();
        };
        let paged = geometry.kind() == Some(StoreKind::Paged);
        let bytes = if let Some(page) = page.filter(|_| paged) {
            u128::from(geometry.allocation(page).unwrap_or(0))
        } else {
            geometry
                .pages_after(None)
                .map(|(_, bytes)| u128::from(bytes))
                .sum()
        };
        Usage {
            blocks: 1,
            pages: if paged {
                geometry.page_count() as u64
            } else {
                0
            },
            bytes,
        }
    }

    fn policy_usage(&self, handle: u32, page: Option<u32>) -> (u128, u64) {
        let Some((_, geometry, _)) = self.resident_at(handle as usize - 1) else {
            return (0, 0);
        };
        match page.filter(|_| geometry.kind() == Some(StoreKind::Paged)) {
            Some(page) => geometry
                .allocation(page)
                .map_or((0, 0), |bytes| (u128::from(bytes), 1)),
            None => (
                geometry
                    .pages_after(None)
                    .map(|(_, bytes)| u128::from(bytes))
                    .sum(),
                geometry.page_count() as u64,
            ),
        }
    }

    pub(crate) fn apply(
        &mut self,
        id: &talon_core::BlockId,
        kind: StoreKind,
        operation: &Operation,
        disk_id: Option<crate::catalog::DiskId>,
        allocated: Option<u64>,
    ) {
        let handle = match self.rows.handle(id) {
            Some(handle) => handle,
            None if allocated.is_some()
                && matches!(
                    operation,
                    Operation::Put { .. } | Operation::PutWhole { .. }
                ) =>
            {
                self.rows.get_or_insert(BlockKey::new(id))
            }
            None => return,
        };
        let page = match operation {
            Operation::Put { page, .. } | Operation::DeletePage { page } => Some(*page),
            _ => None,
        };
        let before = self.usage(handle, page);
        let slot = handle as usize - 1;
        self.ensure_clock(slot);
        self.ensure_inline_age(slot);
        let old_kind = self.resident_at(slot).and_then(|(_, row, _)| row.kind());
        let before_policy = self.catalog_policy.then(|| self.policy_usage(handle, page));
        let putting = allocated.is_some()
            && matches!(
                operation,
                Operation::Put { .. } | Operation::PutWhole { .. }
            );
        let live_paged = kind == StoreKind::Paged && self.page_size.is_some_and(|size| size != 0);
        let key = live_paged.then(|| self.rows.at(slot).unwrap().id.clone());
        match (operation, allocated) {
            (Operation::PutWhole { len }, Some(bytes)) => {
                self.rows
                    .put(handle, Geometry::new(kind, disk_id, *len, 0, bytes));
            }
            (Operation::Put { page, len }, Some(bytes)) => {
                if let Some(row) = self.rows.geometry_mut(handle, kind) {
                    debug_assert_eq!(row.disk_id, disk_id);
                    let age = self.inline_ages.as_ref().and_then(|ages| ages.get(slot));
                    row.put(*len, *page, bytes, age);
                } else {
                    if let Some(age) = self
                        .inline_ages
                        .as_mut()
                        .and_then(|ages| ages.get_mut(slot))
                    {
                        *age = PageAge::unknown();
                    }
                    let mut geometry = Geometry::new(kind, disk_id, *len, *page, bytes);
                    if live_paged && self.page_work.tracking() {
                        geometry.enable_page_ages();
                    }
                    self.rows.put(handle, geometry);
                }
                if let Some(key) = &key {
                    if self.rows.geometry(handle, kind).unwrap().page_count() as u64 > before.pages
                    {
                        self.page_work.mark_structural(key, slot);
                    }
                }
            }
            _ => {
                if let Some(key) = &key {
                    if let Some(row) = self.rows.geometry(handle, kind) {
                        match page {
                            None => self.page_work.detached(key, slot, None),
                            Some(page) => {
                                if let Some(state) = row.page_row(page, None) {
                                    if row.page_count() == 1 {
                                        // The final unlink also retires the disk directory.
                                        self.page_work.detached(key, slot, None);
                                    } else {
                                        // Runtime single-page deletion claims
                                        // the page before unlinking it.
                                        debug_assert!(!state.pinned());
                                        self.page_work.remove_selection(slot, page);
                                        if state.retry_pending() {
                                            self.page_work.cancel_retry(slot, page);
                                        }
                                        self.page_work.mark_structural(key, slot);
                                    }
                                }
                            }
                        }
                    }
                }
                if let Some(page) = page {
                    let age = self
                        .inline_ages
                        .as_mut()
                        .and_then(|ages| ages.get_mut(slot));
                    if self
                        .rows
                        .geometry_mut(handle, kind)
                        .is_some_and(|row| row.remove_page(page, age))
                    {
                        self.rows.clear(handle, kind);
                    }
                } else {
                    self.rows.clear(handle, kind);
                }
            }
        }
        if let Some(before_policy) = before_policy {
            let new_kind = self.resident_at(slot).and_then(|(_, row, _)| row.kind());
            if old_kind != new_kind || old_kind == Some(kind) {
                let after_policy = self.policy_usage(handle, page);
                let unit = page.map(|page| UnitKind::Page(PageIndex(page)));
                if let Some(unit) = unit.filter(|_| old_kind == new_kind) {
                    self.compact.invalidate(slot, unit);
                } else {
                    self.compact.invalidate_block(slot);
                }
                if new_kind == Some(kind) && putting {
                    let geometry = self.resident_at(slot).unwrap().1;
                    geometry
                        .policy_state(unit.unwrap_or(UnitKind::Whole))
                        .unwrap()
                        .publish();
                    self.record_catalog_admission(slot);
                } else if new_kind.is_none() {
                    self.clear_clock(slot);
                }
                self.compact.replace(
                    before_policy.0,
                    before_policy.1,
                    after_policy.0,
                    after_policy.1,
                );
            }
        }
        if !putting && self.runtime_at(slot).is_empty() {
            self.runtime.remove(&slot);
            self.release(id, slot);
        }
        let after = self.usage(handle, page);
        self.blocks = self.blocks - before.blocks + after.blocks;
        self.pages = self.pages - before.pages + after.pages;
        self.bytes = self.bytes - before.bytes + after.bytes;
    }
}

/// Global revisions remain monotonic and widen before a compact slot could
/// wrap. The widening allocation is retained only by a tracked catalog.
#[derive(Default)]
enum CatalogRevisions {
    #[default]
    Off,
    Compact(Vec<u32>),
    Wide(Vec<u64>),
}

impl CatalogRevisions {
    fn get(&self, slot: usize) -> Option<u64> {
        match self {
            Self::Off => None,
            Self::Compact(revisions) => revisions.get(slot).copied().map(u64::from),
            Self::Wide(revisions) => revisions.get(slot).copied(),
        }
    }

    fn set(&mut self, slot: usize, revision: u64) {
        if let Self::Compact(revisions) = self {
            if let Ok(revision) = u32::try_from(revision) {
                revisions[slot] = revision;
                return;
            }
            *self = Self::Wide(revisions.iter().copied().map(u64::from).collect());
        }
        if let Self::Wide(revisions) = self {
            revisions[slot] = revision;
        }
    }

    fn resize(&mut self, len: usize) {
        match self {
            Self::Off => {}
            Self::Compact(revisions) => revisions.resize(len, 0),
            Self::Wide(revisions) => revisions.resize(len, 0),
        }
    }
}

#[derive(Default)]
struct Usage {
    blocks: u64,
    pages: u64,
    bytes: u128,
}

#[cfg(test)]
impl std::ops::Index<&BlockId> for Directory {
    type Output = DirectoryEntry;
    fn index(&self, id: &BlockId) -> &Self::Output {
        self.get(id).expect("directory entry")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{Backend, ObjectId, Version};

    #[test]
    fn admission_revisions_widen_without_changing_existing_slots() {
        let mut revisions = CatalogRevisions::Compact(vec![0; 2]);
        revisions.set(0, u64::from(u32::MAX));
        revisions.set(1, u64::from(u32::MAX) + 1);
        revisions.resize(3);
        assert_eq!(revisions.get(0), Some(u64::from(u32::MAX)));
        assert_eq!(revisions.get(1), Some(u64::from(u32::MAX) + 1));
        assert_eq!(revisions.get(2), Some(0));
    }

    #[test]
    fn removal_and_recovery_reuse_only_retired_slots() {
        let id = |n| {
            BlockKey::new(&BlockId::new(
                ObjectId::new(Backend::S3, "b", format!("o/{n}")),
                0,
                256,
                Version::new("v1"),
            ))
        };
        let mut directory = Directory::default();
        for n in 0..100 {
            directory.get_or_insert(id(n)).1.legacy_mut().small = if n % 3 == 0 { 1 } else { 0 };
        }
        let canonical = directory.get_key_value(&id(30)).unwrap().0.clone();
        let replacement = directory.get_or_insert(id(30)).0;
        assert!(canonical.ptr_eq(&replacement));
        for n in (0..100).step_by(2) {
            assert!(directory.remove(&id(n)).is_some());
        }
        directory.retain(|_, entry| entry.legacy.as_ref().unwrap().small == 0);
        let retained: Vec<_> = directory
            .iter()
            .map(|(id, entry)| (id.clone(), entry.legacy.as_ref().unwrap().small))
            .collect();
        let free = directory.slot_count() - directory.len();
        for n in 100..100 + free {
            let entry = directory.get_or_insert(id(n)).1;
            assert!(
                entry.is_empty(),
                "reused slots must not inherit prior state"
            );
            entry.legacy_mut().small = (n % 2) as u8;
        }
        assert_eq!(directory.slot_count(), 100);
        for (id, value) in retained {
            assert_eq!(
                directory.get(&id).unwrap().legacy.as_ref().unwrap().small,
                value
            );
        }
        directory.retain(|_, _| false);
        assert!(directory.is_empty());
        assert_eq!(directory.slot_count() - directory.len(), 100);
    }
}
