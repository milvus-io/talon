use super::DirectoryEntry;
use crate::block_key::{BlockIdentity, BlockKey};
use crate::catalog::{BlockRow, BlockRows, Geometry, Operation, StoreKind};
use std::sync::LazyLock;
#[cfg(test)]
use talon_core::BlockId;

static EMPTY_RUNTIME_ROW: LazyLock<DirectoryEntry> = LazyLock::new(DirectoryEntry::default);

/// The journal and the index share these rows. Runtime policy and lifecycle
/// are attached by slot, and may keep an identity alive without any files.
#[derive(Default)]
pub(crate) struct Directory {
    pub(crate) rows: BlockRows,
    runtime: Vec<DirectoryEntry>,
    page_size: Option<u32>,
    blocks: u64,
    pages: u64,
    bytes: u128,
}

impl Directory {
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
        self.runtime.reserve(additional);
    }

    pub fn get(&self, id: &dyn BlockIdentity) -> Option<&DirectoryEntry> {
        self.get_key_value(id).map(|(_, entry)| entry)
    }

    pub fn get_mut(&mut self, id: &dyn BlockIdentity) -> Option<&mut DirectoryEntry> {
        self.runtime.get_mut(self.rows.handle(id)? as usize - 1)
    }

    pub fn get_key_value(&self, id: &dyn BlockIdentity) -> Option<(&BlockKey, &DirectoryEntry)> {
        self.at(self.rows.handle(id)? as usize - 1)
    }

    pub fn get_or_insert(&mut self, id: BlockKey) -> (BlockKey, &mut DirectoryEntry) {
        let slot = self.rows.get_or_insert(id) as usize - 1;
        if slot >= self.runtime.len() {
            self.runtime.resize_with(slot + 1, DirectoryEntry::default);
        }
        let key = self.rows.at(slot).unwrap().id.clone();
        (key, &mut self.runtime[slot])
    }

    pub fn remove(&mut self, id: &dyn BlockIdentity) -> Option<DirectoryEntry> {
        let slot = self.rows.handle(id)? as usize - 1;
        let runtime = self
            .runtime
            .get_mut(slot)
            .map(std::mem::take)
            .unwrap_or_default();
        self.rows.release(id);
        Some(runtime)
    }

    pub fn slot_count(&self) -> usize {
        self.rows.slot_count()
    }

    pub fn at(&self, cursor: usize) -> Option<(&BlockKey, &DirectoryEntry)> {
        let row = self.rows.at(cursor)?;
        Some((&row.id, self.runtime_at(cursor)))
    }

    fn runtime_at(&self, slot: usize) -> &DirectoryEntry {
        self.runtime.get(slot).unwrap_or(&EMPTY_RUNTIME_ROW)
    }

    pub fn versions(
        &self,
        id: &dyn BlockIdentity,
        mut visit: impl FnMut(&BlockKey, &DirectoryEntry),
    ) {
        self.rows.versions(id, |handle, row| {
            visit(&row.id, self.runtime_at(handle as usize - 1))
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
        for (cursor, entry) in self.runtime.iter_mut().enumerate() {
            if let Some(row) = self.rows.at(cursor) {
                if !keep(&row.id, entry) {
                    let id = row.id.clone();
                    *entry = DirectoryEntry::default();
                    self.rows.release(&id);
                }
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
        let handle = self.rows.handle(id)?;
        let slot = handle as usize - 1;
        let row = self.rows.at(slot).unwrap();
        Some((&row.id, self.visible(handle, row)?, self.runtime_at(slot)))
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
        match (operation, allocated) {
            (Operation::PutWhole { len }, Some(bytes)) => {
                self.rows
                    .put(handle, Geometry::new(kind, disk_id, *len, 0, bytes));
            }
            (Operation::Put { page, len }, Some(bytes)) => {
                if let Some(row) = self.rows.geometry_mut(handle, kind) {
                    debug_assert_eq!(row.disk_id, disk_id);
                    row.put(*len, *page, bytes);
                } else {
                    self.rows
                        .put(handle, Geometry::new(kind, disk_id, *len, *page, bytes));
                }
            }
            _ => {
                if let Some(page) = page {
                    if self
                        .rows
                        .geometry_mut(handle, kind)
                        .is_some_and(|row| row.remove_page(page))
                    {
                        self.rows.clear(handle, kind);
                    }
                } else {
                    self.rows.clear(handle, kind);
                }
                if self.runtime_at(handle as usize - 1).is_empty() {
                    self.rows.release(id);
                }
            }
        }
        let after = self.usage(handle, page);
        self.blocks = self.blocks - before.blocks + after.blocks;
        self.pages = self.pages - before.pages + after.pages;
        self.bytes = self.bytes - before.bytes + after.bytes;
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
            directory.get_or_insert(id(n)).1.policy.len = n;
        }
        let canonical = directory.get_key_value(&id(30)).unwrap().0.clone();
        let replacement = directory.get_or_insert(id(30)).0;
        assert!(canonical.ptr_eq(&replacement));
        for n in (0..100).step_by(2) {
            assert_eq!(directory.remove(&id(n)).unwrap().policy.len, n);
        }
        directory.retain(|_, entry| entry.policy.len % 3 != 0);
        let retained: Vec<_> = directory
            .iter()
            .map(|(id, entry)| (id.clone(), entry.policy.len))
            .collect();
        let free = directory.slot_count() - directory.len();
        for n in 100..100 + free {
            let entry = directory.get_or_insert(id(n)).1;
            assert!(
                entry.is_empty(),
                "reused slots must not inherit prior state"
            );
            entry.policy.len = n;
        }
        assert_eq!(directory.slot_count(), 100);
        for (id, value) in retained {
            assert_eq!(directory.get(&id).unwrap().policy.len, value);
        }
        directory.retain(|_, _| false);
        assert!(directory.is_empty());
        assert_eq!(directory.slot_count() - directory.len(), 100);
    }
}
