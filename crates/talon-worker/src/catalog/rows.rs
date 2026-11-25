//! Snapshot-friendly slabs for durable file residency.
use super::DiskId;
use crate::block_key::{BlockIdentity, BlockKey};
use crate::small_store::lookup::Lookup;
use std::hash::{BuildHasher, RandomState};
use std::sync::Arc;

const SLAB_ROWS: usize = 256;
const SECTOR: u64 = 512;

#[derive(Clone)]
pub(super) struct BlockRow {
    pub id: BlockKey,
    pub disk_id: Option<DiskId>,
    pages: Pages,
}

#[derive(Clone)]
enum Pages {
    Inline { len: u32, page: CompactPage },
    Multiple(Arc<SharedPages>),
}

#[derive(Clone, Copy)]
struct CompactPage {
    index: u32,
    sectors: u32,
}

impl CompactPage {
    fn new(index: u32, bytes: u64) -> Option<Self> {
        (bytes % SECTOR == 0).then_some(())?;
        Some(Self {
            index,
            sectors: u32::try_from(bytes / SECTOR).ok()?,
        })
    }

    fn unpack(self) -> (u32, u64) {
        (self.index, u64::from(self.sectors) * SECTOR)
    }
}

#[derive(Clone)]
struct SharedPages {
    len: u32,
    rows: PageRows,
}

#[derive(Clone)]
enum PageRows {
    Compact(Vec<CompactPage>),
    Wide(Vec<(u32, u64)>),
}

impl BlockRow {
    pub fn new(id: BlockKey, disk_id: Option<DiskId>, len: u64, index: u32, bytes: u64) -> Self {
        let len = u32::try_from(len).expect("validated block length");
        let pages = match CompactPage::new(index, bytes) {
            Some(page) => Pages::Inline { len, page },
            None => Pages::Multiple(Arc::new(SharedPages {
                len,
                rows: PageRows::Wide(vec![(index, bytes)]),
            })),
        };
        Self { id, disk_id, pages }
    }

    pub fn len(&self) -> u64 {
        u64::from(match &self.pages {
            Pages::Inline { len, .. } => *len,
            Pages::Multiple(pages) => pages.len,
        })
    }

    pub fn page_count(&self) -> usize {
        match &self.pages {
            Pages::Inline { .. } => 1,
            Pages::Multiple(pages) => match &pages.rows {
                PageRows::Compact(rows) => rows.len(),
                PageRows::Wide(rows) => rows.len(),
            },
        }
    }

    pub fn pages_after(&self, after: Option<u32>) -> impl Iterator<Item = (u32, u64)> + '_ {
        let (one, compact, wide) = match &self.pages {
            Pages::Inline { page, .. } => (
                after
                    .map_or(true, |last| page.index > last)
                    .then(|| page.unpack()),
                &[][..],
                &[][..],
            ),
            Pages::Multiple(pages) => match &pages.rows {
                PageRows::Compact(rows) => {
                    let start = after.map_or(0, |last| rows.partition_point(|p| p.index <= last));
                    (None, &rows[start..], &[][..])
                }
                PageRows::Wide(rows) => {
                    let start = after.map_or(0, |last| rows.partition_point(|p| p.0 <= last));
                    (None, &[][..], &rows[start..])
                }
            },
        };
        one.into_iter()
            .chain(compact.iter().map(|page| page.unpack()))
            .chain(wide.iter().copied())
    }

    pub fn last_page(&self) -> u32 {
        match &self.pages {
            Pages::Inline { page, .. } => page.index,
            Pages::Multiple(pages) => match &pages.rows {
                PageRows::Compact(rows) => rows.last().unwrap().index,
                PageRows::Wide(rows) => rows.last().unwrap().0,
            },
        }
    }

    pub fn put(&mut self, len: u64, index: u32, bytes: u64) {
        let len = u32::try_from(len).expect("validated block length");
        let page = CompactPage::new(index, bytes);
        if let Pages::Inline { page: old, .. } = self.pages {
            if old.index == index {
                *self = Self::new(self.id.clone(), self.disk_id, u64::from(len), index, bytes);
                return;
            }
            let mut rows = Vec::with_capacity(2);
            rows.push(old);
            self.pages = Pages::Multiple(Arc::new(SharedPages {
                len,
                rows: PageRows::Compact(rows),
            }));
        }
        let Pages::Multiple(pages) = &mut self.pages else {
            unreachable!()
        };
        let pages = Arc::make_mut(pages);
        pages.len = len;
        if let PageRows::Compact(rows) = &mut pages.rows {
            if let Some(page) = page {
                match rows.binary_search_by_key(&index, |page| page.index) {
                    Ok(at) => rows[at] = page,
                    Err(at) => rows.insert(at, page),
                }
                return;
            }
            pages.rows = PageRows::Wide(rows.iter().map(|page| page.unpack()).collect());
        }
        let PageRows::Wide(rows) = &mut pages.rows else {
            unreachable!()
        };
        match rows.binary_search_by_key(&index, |page| page.0) {
            Ok(at) => rows[at].1 = bytes,
            Err(at) => rows.insert(at, (index, bytes)),
        }
    }

    /// Return true when the block has no remaining pages.
    fn remove_page(&mut self, index: u32) -> bool {
        if let Pages::Inline { page, .. } = &self.pages {
            return page.index == index;
        }
        let Pages::Multiple(pages) = &mut self.pages else {
            unreachable!()
        };
        let pages = Arc::make_mut(pages);
        let single = match &mut pages.rows {
            PageRows::Compact(rows) => {
                if let Ok(at) = rows.binary_search_by_key(&index, |page| page.index) {
                    rows.remove(at);
                    if rows.len() >= 2 && rows.len() <= rows.capacity() / 4 {
                        rows.shrink_to_fit();
                    }
                }
                match rows.as_slice() {
                    [] => return true,
                    [page] => Some(*page),
                    _ => None,
                }
            }
            PageRows::Wide(rows) => {
                if let Ok(at) = rows.binary_search_by_key(&index, |page| page.0) {
                    rows.remove(at);
                    if rows.len() >= 2 && rows.len() <= rows.capacity() / 4 {
                        rows.shrink_to_fit();
                    }
                }
                match rows.as_slice() {
                    [] => return true,
                    &[(index, bytes)] => CompactPage::new(index, bytes),
                    _ => None,
                }
            }
        };
        if let Some(page) = single {
            self.pages = Pages::Inline {
                len: pages.len,
                page,
            };
        }
        false
    }
}

pub(super) type Slab = Arc<[Option<BlockRow>]>;

#[derive(Default)]
pub(super) struct BlockRows {
    lookup: Lookup,
    hash: RandomState,
    slabs: Vec<Slab>,
    slots: usize,
    free: Vec<u32>,
}

impl BlockRows {
    fn hash(&self, id: &dyn BlockIdentity) -> u32 {
        let id = id.view();
        self.hash.hash_one((id.object, id.offset, id.block_size)) as u32
    }

    fn at(&self, handle: u32) -> &Option<BlockRow> {
        let slot = handle as usize - 1;
        &self.slabs[slot / SLAB_ROWS][slot % SLAB_ROWS]
    }

    fn at_mut(&mut self, handle: u32) -> &mut Option<BlockRow> {
        let slot = handle as usize - 1;
        &mut Arc::make_mut(&mut self.slabs[slot / SLAB_ROWS])[slot % SLAB_ROWS]
    }

    fn find(&self, id: &dyn BlockIdentity) -> Option<usize> {
        let view = id.view();
        self.lookup.find(self.hash(id), |handle, _| {
            self.at(handle).as_ref().unwrap().id.view() == view
        })
    }

    pub fn get(&self, id: &dyn BlockIdentity) -> Option<&BlockRow> {
        self.at(self.lookup.handle(self.find(id)?)).as_ref()
    }

    pub fn get_mut(&mut self, id: &dyn BlockIdentity) -> Option<&mut BlockRow> {
        let slot = self.find(id)?;
        self.at_mut(self.lookup.handle(slot)).as_mut()
    }

    pub fn insert(&mut self, mut row: BlockRow) {
        let handle = match self.find(&row.id) {
            Some(slot) => {
                let handle = self.lookup.handle(slot);
                row.id = self.at(handle).as_ref().unwrap().id.clone();
                handle
            }
            None => {
                if self.lookup.needs_reserve(1) {
                    let mut lookup = std::mem::take(&mut self.lookup);
                    let growth = (lookup.len() / 8).max(1);
                    lookup.rebuild(growth, |handle| {
                        self.hash(&self.at(handle).as_ref().unwrap().id)
                    });
                    self.lookup = lookup;
                }
                let handle = self.free.pop().unwrap_or_else(|| {
                    if self.slots % SLAB_ROWS == 0 {
                        self.slabs.push((0..SLAB_ROWS).map(|_| None).collect());
                    }
                    self.slots += 1;
                    let handle = u32::try_from(self.slots).expect("catalog handles exhausted");
                    assert!(handle < u32::MAX);
                    handle
                });
                self.lookup.insert(self.hash(&row.id), handle, 0);
                handle
            }
        };
        *self.at_mut(handle) = Some(row);
    }

    pub fn remove(&mut self, id: &dyn BlockIdentity) {
        if let Some(slot) = self.find(id) {
            let handle = self.lookup.handle(slot);
            self.lookup.remove(slot);
            self.free.push(handle);
            *self.at_mut(handle) = None;
        }
    }

    pub fn remove_page(&mut self, id: &dyn BlockIdentity, page: u32) {
        if self.get_mut(id).is_some_and(|row| row.remove_page(page)) {
            self.remove(id);
        }
    }

    pub fn values(&self) -> impl Iterator<Item = &BlockRow> {
        self.slabs.iter().flat_map(|slab| slab.iter().flatten())
    }

    pub fn slabs(&self) -> impl Iterator<Item = Slab> + '_ {
        self.slabs.iter().cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{Backend, BlockId, ObjectId, Version};

    #[test]
    fn inline_layout_and_sparse_wide_page_roundtrip() {
        assert_eq!(size_of::<BlockRow>(), 32);
        assert_eq!(size_of::<CompactPage>(), 8);
        let id = BlockKey::new(&BlockId::new(
            ObjectId::new(Backend::S3, "bucket", "object"),
            0,
            u32::MAX,
            Version::new("v1"),
        ));
        let mut rows = BlockRows::default();
        rows.insert(BlockRow::new(id.clone(), None, u64::from(u32::MAX), 1, 512));
        let frozen = rows.slabs().next().unwrap();
        let row = rows.get_mut(&id).unwrap();
        row.put(u64::from(u32::MAX), 1 << 20, 4096);
        row.put(u64::from(u32::MAX), 3, u64::MAX);
        assert!(row
            .pages_after(None)
            .eq([(1, 512), (3, u64::MAX), (1 << 20, 4096)]));
        assert!(frozen[0].as_ref().unwrap().pages_after(None).eq([(1, 512)]));
        rows.remove_page(&id, 3);
        rows.remove_page(&id, 1);
        assert!(matches!(rows.get(&id).unwrap().pages, Pages::Inline { .. }));
        rows.remove_page(&id, 1 << 20);
        assert!(rows.get(&id).is_none());
        rows.insert(BlockRow::new(id.clone(), None, 16, 2, 0));
        assert!(rows.get(&id).unwrap().pages_after(None).eq([(2, 0)]));
        assert!(frozen[0].as_ref().unwrap().pages_after(None).eq([(1, 512)]));
    }
}
