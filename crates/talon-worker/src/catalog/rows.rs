//! Snapshot-friendly slabs for durable file residency.
use super::DiskId;
use crate::block_key::{BlockIdentity, BlockKey};
use crate::small_store::lookup::Lookup;
use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::sync::Arc;

const SLAB_ROWS: usize = 256;
const SECTOR: u64 = 512;

#[derive(Clone)]
pub(crate) struct BlockRow {
    pub id: BlockKey,
    pub geometry: Geometry,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StoreKind {
    Whole,
    Paged,
}

impl StoreKind {
    pub fn path(self) -> &'static str {
        match self {
            Self::Whole => "whole",
            Self::Paged => "paged",
        }
    }

    pub fn from_path(path: &str) -> Option<Self> {
        match path {
            "whole" => Some(Self::Whole),
            "paged" => Some(Self::Paged),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub(crate) struct Geometry {
    pub disk_id: Option<DiskId>,
    pages: Pages,
}

#[derive(Clone)]
enum Pages {
    Empty,
    Whole { len: u32, sectors: u32 },
    WholeWide(Arc<(u32, u64)>),
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

impl Geometry {
    fn empty() -> Self {
        Self {
            disk_id: None,
            pages: Pages::Empty,
        }
    }

    pub fn new(kind: StoreKind, disk_id: Option<DiskId>, len: u64, index: u32, bytes: u64) -> Self {
        let len = u32::try_from(len).expect("validated block length");
        let pages = match (kind, CompactPage::new(index, bytes)) {
            (StoreKind::Whole, Some(page)) => Pages::Whole {
                len,
                sectors: page.sectors,
            },
            (StoreKind::Whole, None) => Pages::WholeWide(Arc::new((len, bytes))),
            (StoreKind::Paged, Some(page)) => Pages::Inline { len, page },
            (StoreKind::Paged, None) => Pages::Multiple(Arc::new(SharedPages {
                len,
                rows: PageRows::Wide(vec![(index, bytes)]),
            })),
        };
        Self { disk_id, pages }
    }

    pub fn kind(&self) -> Option<StoreKind> {
        match self.pages {
            Pages::Empty => None,
            Pages::Whole { .. } | Pages::WholeWide(_) => Some(StoreKind::Whole),
            _ => Some(StoreKind::Paged),
        }
    }

    pub fn len(&self) -> u64 {
        u64::from(match &self.pages {
            Pages::Empty => 0,
            Pages::Whole { len, .. } | Pages::Inline { len, .. } => *len,
            Pages::WholeWide(pair) => pair.0,
            Pages::Multiple(pages) => pages.len,
        })
    }

    pub fn page_count(&self) -> usize {
        match &self.pages {
            Pages::Empty => 0,
            Pages::Whole { .. } | Pages::WholeWide(_) | Pages::Inline { .. } => 1,
            Pages::Multiple(pages) => match &pages.rows {
                PageRows::Compact(rows) => rows.len(),
                PageRows::Wide(rows) => rows.len(),
            },
        }
    }

    pub fn pages_after(&self, after: Option<u32>) -> impl Iterator<Item = (u32, u64)> + '_ {
        let (one, compact, wide) = match &self.pages {
            Pages::Empty => (None, &[][..], &[][..]),
            Pages::Whole { sectors, .. } => (
                after.is_none().then(|| (0, u64::from(*sectors) * SECTOR)),
                &[][..],
                &[][..],
            ),
            Pages::WholeWide(pair) => (after.is_none().then(|| (0, pair.1)), &[][..], &[][..]),
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
            Pages::Empty => unreachable!("empty block has no last page"),
            Pages::Whole { .. } | Pages::WholeWide(_) => 0,
            Pages::Inline { page, .. } => page.index,
            Pages::Multiple(pages) => match &pages.rows {
                PageRows::Compact(rows) => rows.last().unwrap().index,
                PageRows::Wide(rows) => rows.last().unwrap().0,
            },
        }
    }

    pub fn allocation(&self, page: u32) -> Option<u64> {
        self.pages_after(page.checked_sub(1))
            .next()
            .and_then(|(index, bytes)| (index == page).then_some(bytes))
    }

    pub fn put(&mut self, len: u64, index: u32, bytes: u64) {
        let len = u32::try_from(len).expect("validated block length");
        let page = CompactPage::new(index, bytes);
        if let Pages::Inline { page: old, .. } = self.pages {
            if old.index == index {
                *self = Self::new(StoreKind::Paged, self.disk_id, u64::from(len), index, bytes);
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
    pub fn remove_page(&mut self, index: u32) -> bool {
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

pub(crate) type Slab = Arc<[Option<BlockRow>]>;

/// Handles are valid under the directory lock. A new admission gets its
/// durable DiskId from its Begin sequence, even when it reuses a slab slot.
#[derive(Default)]
pub(crate) struct BlockRows {
    lookup: Lookup,
    hash: RandomState,
    slabs: Vec<Slab>,
    slots: usize,
    free: Vec<u32>,
    // A logical block normally has only one address. Retain both file families
    // if an existing catalog contains both. The paged row has startup priority.
    whole_overlap: HashMap<u32, Geometry>,
}

impl BlockRows {
    fn hash(&self, id: &dyn BlockIdentity) -> u32 {
        let id = id.view();
        self.hash.hash_one((id.object, id.offset, id.block_size)) as u32
    }

    pub fn at(&self, slot: usize) -> Option<&BlockRow> {
        self.slabs
            .get(slot / SLAB_ROWS)?
            .get(slot % SLAB_ROWS)?
            .as_ref()
    }

    fn at_mut(&mut self, handle: u32) -> &mut Option<BlockRow> {
        let slot = handle as usize - 1;
        &mut Arc::make_mut(&mut self.slabs[slot / SLAB_ROWS])[slot % SLAB_ROWS]
    }

    fn find(&self, id: &dyn BlockIdentity) -> Option<usize> {
        let view = id.view();
        self.lookup.find(self.hash(id), |handle, _| {
            self.at(handle as usize - 1).unwrap().id.view() == view
        })
    }

    pub fn handle(&self, id: &dyn BlockIdentity) -> Option<u32> {
        self.find(id).map(|slot| self.lookup.handle(slot))
    }

    pub fn len(&self) -> usize {
        self.lookup.len()
    }

    pub fn slot_count(&self) -> usize {
        self.slots
    }

    pub fn reserve(&mut self, additional: usize) {
        if self.lookup.needs_reserve(additional) {
            let mut lookup = std::mem::take(&mut self.lookup);
            lookup.rebuild(additional.max(lookup.len() / 8), |handle| {
                self.hash(&self.at(handle as usize - 1).unwrap().id)
            });
            self.lookup = lookup;
        }
    }

    pub fn get_or_insert(&mut self, id: BlockKey) -> u32 {
        if let Some(handle) = self.handle(&id) {
            return handle;
        }
        self.reserve(1);
        let handle = self.free.pop().unwrap_or_else(|| {
            if self.slots % SLAB_ROWS == 0 {
                self.slabs.push((0..SLAB_ROWS).map(|_| None).collect());
            }
            self.slots += 1;
            let handle = u32::try_from(self.slots).expect("catalog handles exhausted");
            assert!(handle < u32::MAX);
            handle
        });
        self.lookup.insert(self.hash(&id), handle, 0);
        *self.at_mut(handle) = Some(BlockRow {
            id,
            geometry: Geometry::empty(),
        });
        handle
    }

    pub fn geometry(&self, handle: u32, kind: StoreKind) -> Option<&Geometry> {
        let row = self.at(handle as usize - 1)?;
        if row.geometry.kind() == Some(kind) {
            Some(&row.geometry)
        } else if kind == StoreKind::Whole {
            self.whole_overlap.get(&handle)
        } else {
            None
        }
    }

    pub fn geometry_mut(&mut self, handle: u32, kind: StoreKind) -> Option<&mut Geometry> {
        if self.at(handle as usize - 1)?.geometry.kind() == Some(kind) {
            Some(&mut self.at_mut(handle).as_mut().unwrap().geometry)
        } else if kind == StoreKind::Whole {
            self.whole_overlap.get_mut(&handle)
        } else {
            None
        }
    }

    pub fn put(&mut self, handle: u32, geometry: Geometry) {
        let old_kind = self.at(handle as usize - 1).unwrap().geometry.kind();
        match (old_kind, geometry.kind()) {
            (Some(StoreKind::Paged), Some(StoreKind::Whole)) => {
                self.whole_overlap.insert(handle, geometry);
            }
            (Some(StoreKind::Whole), Some(StoreKind::Paged)) => {
                let row = self.at_mut(handle).as_mut().unwrap();
                let whole = std::mem::replace(&mut row.geometry, geometry);
                self.whole_overlap.insert(handle, whole);
            }
            (_, kind) => {
                if kind == Some(StoreKind::Whole) {
                    self.whole_overlap.remove(&handle);
                }
                self.at_mut(handle).as_mut().unwrap().geometry = geometry;
            }
        }
    }

    pub fn clear(&mut self, handle: u32, kind: StoreKind) {
        if self.at(handle as usize - 1).unwrap().geometry.kind() == Some(kind) {
            self.at_mut(handle).as_mut().unwrap().geometry = Geometry::empty();
        } else if kind == StoreKind::Whole {
            self.whole_overlap.remove(&handle);
        }
    }

    /// Recover a whole block shadowed by paged residency that was deleted
    /// before restart. Runtime eviction otherwise leaves the shadow hidden.
    pub fn restore_shadows(&mut self) {
        let shadows: Vec<_> = self
            .whole_overlap
            .keys()
            .copied()
            .filter(|&handle| {
                self.at(handle as usize - 1)
                    .unwrap()
                    .geometry
                    .kind()
                    .is_none()
            })
            .collect();
        for handle in shadows {
            let geometry = self.whole_overlap.remove(&handle).unwrap();
            self.put(handle, geometry);
        }
    }

    pub fn release(&mut self, id: &dyn BlockIdentity) {
        if let Some(slot) = self.find(id) {
            let handle = self.lookup.handle(slot);
            if self
                .at(handle as usize - 1)
                .unwrap()
                .geometry
                .kind()
                .is_none()
                && !self.whole_overlap.contains_key(&handle)
            {
                self.lookup.remove(slot);
                self.free.push(handle);
                *self.at_mut(handle) = None;
            }
        }
    }

    pub fn versions(&self, id: &dyn BlockIdentity, mut visit: impl FnMut(u32, &BlockRow)) {
        let view = id.view();
        self.lookup.find_all(self.hash(id), |_, handle| {
            let row = self.at(handle as usize - 1).unwrap();
            let candidate = row.id.view();
            if (candidate.object, candidate.offset, candidate.block_size)
                == (view.object, view.offset, view.block_size)
            {
                visit(handle, row);
            }
            false
        });
    }

    pub fn iter(&self) -> impl Iterator<Item = (u32, &BlockRow)> {
        self.slabs
            .iter()
            .flat_map(|slab| slab.iter())
            .enumerate()
            .filter_map(|(slot, row)| Some(((slot + 1) as u32, row.as_ref()?)))
    }

    pub fn slabs(&self) -> impl Iterator<Item = Slab> + '_ {
        self.slabs.iter().cloned()
    }

    pub fn overlap(&self) -> impl Iterator<Item = BlockRow> + '_ {
        self.whole_overlap
            .iter()
            .map(|(&handle, geometry)| BlockRow {
                id: self.at(handle as usize - 1).unwrap().id.clone(),
                geometry: geometry.clone(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{Backend, BlockId, ObjectId, Version};

    #[test]
    fn inline_layout_and_sparse_wide_page_roundtrip() {
        assert_eq!(size_of::<BlockRow>(), 32);
        assert_eq!(size_of::<Option<BlockRow>>(), 32);
        assert_eq!(size_of::<CompactPage>(), 8);
        let id = BlockKey::new(&BlockId::new(
            ObjectId::new(Backend::S3, "bucket", "object"),
            0,
            u32::MAX,
            Version::new("v1"),
        ));
        let mut rows = BlockRows::default();
        let handle = rows.get_or_insert(id.clone());
        rows.put(
            handle,
            Geometry::new(StoreKind::Paged, None, u64::from(u32::MAX), 1, 512),
        );
        let frozen = rows.slabs().next().unwrap();
        let row = rows.geometry_mut(handle, StoreKind::Paged).unwrap();
        row.put(u64::from(u32::MAX), 1 << 20, 4096);
        row.put(u64::from(u32::MAX), 3, u64::MAX);
        assert!(row
            .pages_after(None)
            .eq([(1, 512), (3, u64::MAX), (1 << 20, 4096)]));
        assert!(frozen[0]
            .as_ref()
            .unwrap()
            .geometry
            .pages_after(None)
            .eq([(1, 512)]));
        row.remove_page(3);
        row.remove_page(1);
        assert!(matches!(row.pages, Pages::Inline { .. }));
        assert!(row.remove_page(1 << 20));
        rows.clear(handle, StoreKind::Paged);
        rows.release(&id);
        assert!(rows.handle(&id).is_none());
        assert_eq!(rows.get_or_insert(id), handle);
        rows.put(handle, Geometry::new(StoreKind::Paged, None, 16, 2, 0));
        assert!(rows
            .geometry(handle, StoreKind::Paged)
            .unwrap()
            .pages_after(None)
            .eq([(2, 0)]));
        assert!(frozen[0]
            .as_ref()
            .unwrap()
            .geometry
            .pages_after(None)
            .eq([(1, 512)]));
    }
}
