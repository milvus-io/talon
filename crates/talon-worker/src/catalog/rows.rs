//! Snapshot-friendly slabs for durable file residency.
use super::DiskId;
use crate::block_key::{BlockIdentity, BlockKey};
use crate::eviction::UnitKind;
use crate::page_lifecycle::{PageAge, PageRow, PageWord};
use crate::small_store::lookup::Lookup;
use std::collections::{BTreeMap, HashMap};
use std::hash::{BuildHasher, RandomState};
use std::sync::Arc;
use talon_core::PageIndex;

const SLAB_ROWS: usize = 256;
const OCCUPANCY_WORDS: usize = SLAB_ROWS / u64::BITS as usize;
const SECTOR: u64 = 512;
// A large block allocates only populated chunks. An insertion or removal
// shifts at most PAGE_CHUNK - 1 rows, independent of the block's page count.
const PAGE_CHUNK: usize = 256;

fn page_chunk(index: u32) -> (u32, u32) {
    let local = index % PAGE_CHUNK as u32;
    (index - local, local)
}

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
    disk_id: Option<DiskId>,
    pages: Pages,
}

#[cfg(test)]
thread_local! {
    static PAGE_ROWS_VISITED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn take_page_rows_visited() -> usize {
    PAGE_ROWS_VISITED.replace(0)
}

#[derive(Clone)]
enum Pages {
    Empty,
    Whole {
        len: u32,
        sectors: u32,
        state: PageWord,
    },
    WholeWide(Arc<(u32, u64, PageWord)>),
    Inline {
        len: u32,
        page: InlinePage,
        state: PageWord,
    },
    Multiple(Arc<SharedPages>),
    Chunked(Arc<ChunkedPages>),
}

#[derive(Clone, Copy)]
struct InlinePage(u32);

impl InlinePage {
    const SECTOR_BITS: u32 = 20;
    const SECTOR_MASK: u32 = (1 << Self::SECTOR_BITS) - 1;

    fn new(index: u32, bytes: u64) -> Option<Self> {
        let page = CompactPage::new(index, bytes)?;
        let sectors = page.sectors();
        (index <= u32::MAX >> Self::SECTOR_BITS && sectors <= Self::SECTOR_MASK)
            .then_some(Self((index << Self::SECTOR_BITS) | sectors))
    }

    fn index(self) -> u32 {
        self.0 >> Self::SECTOR_BITS
    }

    fn unpack(self) -> (u32, u64) {
        (self.index(), u64::from(self.0 & Self::SECTOR_MASK) * SECTOR)
    }
}

#[derive(Clone, Copy)]
struct CompactPage {
    index: u16,
    sectors: [u16; 2],
}

impl CompactPage {
    fn new(index: u32, bytes: u64) -> Option<Self> {
        (bytes % SECTOR == 0).then_some(())?;
        let sectors = u32::try_from(bytes / SECTOR).ok()?;
        Some(Self {
            index: u16::try_from(index).ok()?,
            sectors: [sectors as u16, (sectors >> 16) as u16],
        })
    }

    fn sectors(self) -> u32 {
        u32::from(self.sectors[0]) | (u32::from(self.sectors[1]) << 16)
    }

    fn unpack(self) -> (u32, u64) {
        (u32::from(self.index), u64::from(self.sectors()) * SECTOR)
    }
}

#[derive(Clone)]
struct SharedPages {
    len: u32,
    rows: PageRows,
    // Moves and COW under the directory write lock keep all arrays aligned.
    // Snapshot serialization reads only the geometry.
    states: Vec<PageWord>,
    ages: Vec<PageAge>,
}

#[derive(Clone)]
struct ChunkedPages {
    len: u32,
    count: usize,
    track_ages: bool,
    chunks: BTreeMap<u32, Arc<SharedPages>>,
}

impl ChunkedPages {
    fn new(pages: &SharedPages) -> Self {
        let mut result = Self {
            len: pages.len,
            count: pages.states.len(),
            track_ages: !pages.ages.is_empty(),
            chunks: BTreeMap::new(),
        };
        for at in 0..pages.states.len() {
            let (index, bytes) = pages.rows.at(at);
            let (base, local) = page_chunk(index);
            let chunk = result
                .chunks
                .entry(base)
                .or_insert_with(|| Arc::new(SharedPages::empty(0)));
            Arc::get_mut(chunk)
                .unwrap()
                .push(local, bytes, pages.row(at));
        }
        result
    }

    fn chunks_from(&self, start: u32) -> impl Iterator<Item = (u32, &SharedPages, usize)> {
        let (start_base, local) = page_chunk(start);
        self.chunks.range(start_base..).map(move |(&base, chunk)| {
            let first = if base == start_base {
                chunk.lower_bound(local)
            } else {
                0
            };
            (base, chunk.as_ref(), first)
        })
    }
}

impl SharedPages {
    fn empty(len: u32) -> Self {
        Self {
            len,
            rows: PageRows::Compact(Vec::new()),
            states: Vec::new(),
            ages: Vec::new(),
        }
    }

    fn row(&self, at: usize) -> PageRow<'_> {
        PageRow {
            state: &self.states[at],
            age: self.ages.get(at),
        }
    }

    fn lower_bound(&self, start: u32) -> usize {
        self.rows.position(start).unwrap_or_else(|at| at)
    }

    fn push(&mut self, index: u32, bytes: u64, row: PageRow<'_>) {
        self.rows.put(Err(self.states.len()), index, bytes);
        self.states.push(row.state.clone());
        if let Some(age) = row.age {
            self.ages.push(age.clone());
        }
    }

    fn put(&mut self, index: u32, bytes: u64, track_ages: bool) -> bool {
        let position = self.rows.position(index);
        self.rows.put(position, index, bytes);
        if let Err(at) = position {
            self.states.insert(at, PageWord::default());
            if track_ages {
                self.ages.insert(at, PageAge::unknown());
            }
        }
        position.is_err()
    }

    fn remove(&mut self, at: usize) {
        self.rows.remove(at);
        remove_row(&mut self.states, at);
        if !self.ages.is_empty() {
            remove_row(&mut self.ages, at);
        }
    }

    fn enable_ages(&mut self) {
        if self.ages.is_empty() {
            self.ages = (0..self.states.len()).map(|_| PageAge::unknown()).collect();
        }
    }

    fn reset_states(&mut self) {
        self.states.fill_with(PageWord::default);
        self.ages = Vec::new();
    }
}

#[derive(Clone)]
enum PageRows {
    Compact(Vec<CompactPage>),
    Wide(Vec<(u32, u64)>),
}

impl PageRows {
    fn position(&self, index: u32) -> Result<usize, usize> {
        match self {
            Self::Compact(rows) => rows.binary_search_by_key(&index, |page| u32::from(page.index)),
            Self::Wide(rows) => rows.binary_search_by_key(&index, |page| page.0),
        }
    }

    fn at(&self, position: usize) -> (u32, u64) {
        #[cfg(test)]
        PAGE_ROWS_VISITED.set(PAGE_ROWS_VISITED.get() + 1);
        match self {
            Self::Compact(rows) => rows[position].unpack(),
            Self::Wide(rows) => rows[position],
        }
    }

    fn put(&mut self, position: Result<usize, usize>, index: u32, bytes: u64) {
        if let Self::Compact(rows) = self {
            if let Some(page) = CompactPage::new(index, bytes) {
                match position {
                    Ok(at) => rows[at] = page,
                    Err(at) => rows.insert(at, page),
                }
                return;
            }
            let mut wide = Vec::with_capacity(rows.len() + usize::from(position.is_err()));
            wide.extend(rows.iter().map(|page| page.unpack()));
            *self = Self::Wide(wide);
        }
        let Self::Wide(rows) = self else {
            unreachable!()
        };
        match position {
            Ok(at) => rows[at].1 = bytes,
            Err(at) => rows.insert(at, (index, bytes)),
        }
    }

    fn remove(&mut self, at: usize) {
        match self {
            Self::Compact(rows) => remove_row(rows, at),
            Self::Wide(rows) => remove_row(rows, at),
        }
    }
}

fn start_pair<T>(first: T) -> Vec<T> {
    let mut rows = Vec::with_capacity(2);
    rows.push(first);
    rows
}

fn remove_row<T>(rows: &mut Vec<T>, at: usize) {
    rows.remove(at);
    if rows.len() >= 2 && rows.len() <= rows.capacity() / 4 {
        rows.shrink_to_fit();
    }
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
        let pages = match kind {
            StoreKind::Whole => match CompactPage::new(0, bytes) {
                Some(page) => Pages::Whole {
                    len,
                    sectors: page.sectors(),
                    state: PageWord::default(),
                },
                None => Pages::WholeWide(Arc::new((len, bytes, PageWord::default()))),
            },
            StoreKind::Paged => match InlinePage::new(index, bytes) {
                Some(page) => Pages::Inline {
                    len,
                    page,
                    state: PageWord::default(),
                },
                None => Pages::Multiple(Arc::new(SharedPages {
                    len,
                    rows: match CompactPage::new(index, bytes) {
                        Some(page) => PageRows::Compact(vec![page]),
                        None => PageRows::Wide(vec![(index, bytes)]),
                    },
                    states: vec![PageWord::default()],
                    ages: Vec::new(),
                })),
            },
        };
        Self { disk_id, pages }
    }

    pub fn disk_id(&self) -> Option<DiskId> {
        self.disk_id
    }

    pub fn is_persisted(&self) -> bool {
        self.disk_id.is_some()
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
            Pages::Chunked(pages) => pages.len,
        })
    }

    pub fn page_count(&self) -> usize {
        match &self.pages {
            Pages::Empty => 0,
            Pages::Whole { .. } | Pages::WholeWide(_) | Pages::Inline { .. } => 1,
            Pages::Multiple(pages) => pages.states.len(),
            Pages::Chunked(pages) => pages.count,
        }
    }

    pub fn pages_after(&self, after: Option<u32>) -> impl Iterator<Item = (u32, u64)> + '_ {
        let start = after.map_or(Some(0), |page| page.checked_add(1));
        let (one, multiple, chunked) = match &self.pages {
            Pages::Empty => (None, None, None),
            Pages::Whole { sectors, .. } => (
                after.is_none().then_some((0, u64::from(*sectors) * SECTOR)),
                None,
                None,
            ),
            Pages::WholeWide(pair) => (after.is_none().then_some((0, pair.1)), None, None),
            Pages::Inline { page, .. } => (
                start
                    .filter(|&start| page.index() >= start)
                    .map(|_| page.unpack()),
                None,
                None,
            ),
            Pages::Multiple(pages) => (None, start.map(|start| (pages.as_ref(), start)), None),
            Pages::Chunked(pages) => (None, None, start.map(|start| (pages.as_ref(), start))),
        };
        one.into_iter()
            .chain(multiple.into_iter().flat_map(|(pages, start)| {
                (pages.lower_bound(start)..pages.states.len()).map(|at| pages.rows.at(at))
            }))
            .chain(chunked.into_iter().flat_map(|(pages, start)| {
                pages.chunks_from(start).flat_map(|(base, chunk, first)| {
                    (first..chunk.states.len()).map(move |at| {
                        let (local, bytes) = chunk.rows.at(at);
                        (base + local, bytes)
                    })
                })
            }))
    }

    pub fn last_page(&self) -> u32 {
        match &self.pages {
            Pages::Empty => unreachable!("empty block has no last page"),
            Pages::Whole { .. } | Pages::WholeWide(_) => 0,
            Pages::Inline { page, .. } => page.index(),
            Pages::Multiple(pages) => pages.rows.at(pages.states.len() - 1).0,
            Pages::Chunked(pages) => {
                let (&base, chunk) = pages.chunks.last_key_value().unwrap();
                base + chunk.rows.at(chunk.states.len() - 1).0
            }
        }
    }

    pub fn allocation(&self, page: u32) -> Option<u64> {
        match &self.pages {
            Pages::Empty => None,
            Pages::Whole { sectors, .. } => (page == 0).then_some(u64::from(*sectors) * SECTOR),
            Pages::WholeWide(pair) => (page == 0).then_some(pair.1),
            Pages::Inline { page: resident, .. } => {
                (page == resident.index()).then(|| resident.unpack().1)
            }
            Pages::Multiple(pages) => Some(pages.rows.at(pages.rows.position(page).ok()?).1),
            Pages::Chunked(pages) => {
                let (base, local) = page_chunk(page);
                let chunk = pages.chunks.get(&base)?;
                Some(chunk.rows.at(chunk.rows.position(local).ok()?).1)
            }
        }
    }

    pub(crate) fn policy_state(&self, kind: UnitKind) -> Option<&PageWord> {
        match (kind, &self.pages) {
            (UnitKind::Whole, Pages::Whole { state, .. }) => Some(state),
            (UnitKind::Whole, Pages::WholeWide(value)) => Some(&value.2),
            (UnitKind::Page(page), _) => self.page_row(page.0, None).map(|row| row.state),
            _ => None,
        }
    }

    pub(crate) fn policy_units_from(
        &self,
        start: u64,
    ) -> impl Iterator<Item = (UnitKind, u64)> + '_ {
        let whole = (start == 0 && self.kind() == Some(StoreKind::Whole))
            .then(|| (UnitKind::Whole, self.allocation(0).unwrap()));
        let first_page = u32::try_from(start.saturating_sub(2)).ok();
        let pages = first_page.filter(|_| self.kind() == Some(StoreKind::Paged));
        whole.into_iter().chain(pages.into_iter().flat_map(|page| {
            self.pages_after(page.checked_sub(1))
                .map(|(page, bytes)| (UnitKind::Page(PageIndex(page)), bytes))
        }))
    }

    pub(crate) fn page_row<'a>(
        &'a self,
        page: u32,
        inline_age: Option<&'a PageAge>,
    ) -> Option<PageRow<'a>> {
        match &self.pages {
            Pages::Inline {
                page: resident,
                state,
                ..
            } if resident.index() == page => Some(PageRow {
                state,
                age: inline_age,
            }),
            Pages::Multiple(pages) => Some(pages.row(pages.rows.position(page).ok()?)),
            Pages::Chunked(pages) => {
                let (base, local) = page_chunk(page);
                let chunk = pages.chunks.get(&base)?;
                Some(chunk.row(chunk.rows.position(local).ok()?))
            }
            _ => None,
        }
    }

    pub(crate) fn page_rows_from<'a>(
        &'a self,
        start: u32,
        inline_age: Option<&'a PageAge>,
    ) -> impl Iterator<Item = (u32, PageRow<'a>)> {
        let (one, many, chunked) = match &self.pages {
            Pages::Inline { page, state, .. } if page.index() >= start => (
                Some((
                    page.index(),
                    PageRow {
                        state,
                        age: inline_age,
                    },
                )),
                None,
                None,
            ),
            Pages::Multiple(pages) => (None, Some(pages), None),
            Pages::Chunked(pages) => (None, None, Some(pages)),
            _ => (None, None, None),
        };
        one.into_iter()
            .chain(many.into_iter().flat_map(move |pages| {
                (pages.lower_bound(start)..pages.states.len())
                    .map(|at| (pages.rows.at(at).0, pages.row(at)))
            }))
            .chain(chunked.into_iter().flat_map(move |pages| {
                pages.chunks_from(start).flat_map(|(base, chunk, first)| {
                    (first..chunk.states.len())
                        .map(move |at| (base + chunk.rows.at(at).0, chunk.row(at)))
                })
            }))
    }

    pub(crate) fn enable_page_ages(&mut self) {
        match &mut self.pages {
            Pages::Multiple(pages) if pages.ages.is_empty() => Arc::make_mut(pages).enable_ages(),
            Pages::Chunked(pages) if !pages.track_ages => {
                let pages = Arc::make_mut(pages);
                pages.track_ages = true;
                for chunk in pages.chunks.values_mut() {
                    Arc::make_mut(chunk).enable_ages();
                }
            }
            _ => {}
        }
    }

    pub(crate) fn reset_page_states(&mut self) {
        match &mut self.pages {
            Pages::Inline { state, .. } => *state = PageWord::default(),
            Pages::Multiple(pages) => Arc::make_mut(pages).reset_states(),
            Pages::Chunked(pages) => {
                let pages = Arc::make_mut(pages);
                pages.track_ages = false;
                for chunk in pages.chunks.values_mut() {
                    Arc::make_mut(chunk).reset_states();
                }
            }
            _ => {}
        }
    }

    pub fn put(&mut self, len: u64, index: u32, bytes: u64, inline_age: Option<&PageAge>) {
        let len = u32::try_from(len).expect("validated block length");
        if let Pages::Inline {
            len: old_len,
            page,
            state,
        } = &mut self.pages
        {
            if page.index() == index {
                if let Some(replacement) = InlinePage::new(index, bytes) {
                    *old_len = len;
                    *page = replacement;
                    return;
                }
            }
            let (old_index, old_bytes) = page.unpack();
            let old = CompactPage::new(old_index, old_bytes).expect("inline page fits compact row");
            self.pages = Pages::Multiple(Arc::new(SharedPages {
                len,
                rows: PageRows::Compact(start_pair(old)),
                states: start_pair(std::mem::take(state)),
                ages: inline_age.map_or_else(Vec::new, |age| start_pair(age.clone())),
            }));
        }
        if let Pages::Multiple(pages) = &self.pages {
            if pages.states.len() == PAGE_CHUNK && pages.rows.position(index).is_err() {
                self.pages = Pages::Chunked(Arc::new(ChunkedPages::new(pages)));
            }
        }
        match &mut self.pages {
            Pages::Multiple(pages) => {
                let pages = Arc::make_mut(pages);
                pages.len = len;
                pages.put(index, bytes, !pages.ages.is_empty());
            }
            Pages::Chunked(pages) => {
                let pages = Arc::make_mut(pages);
                let (base, local) = page_chunk(index);
                let chunk = pages
                    .chunks
                    .entry(base)
                    .or_insert_with(|| Arc::new(SharedPages::empty(0)));
                pages.len = len;
                pages.count +=
                    usize::from(Arc::make_mut(chunk).put(local, bytes, pages.track_ages));
            }
            _ => unreachable!(),
        }
    }

    /// Return true when the block has no remaining pages.
    pub fn remove_page(&mut self, index: u32, inline_age: Option<&mut PageAge>) -> bool {
        if let Pages::Chunked(pages) = &mut self.pages {
            let pages = Arc::make_mut(pages);
            let (base, local) = page_chunk(index);
            let std::collections::btree_map::Entry::Occupied(mut entry) = pages.chunks.entry(base)
            else {
                return false;
            };
            let Ok(at) = entry.get().rows.position(local) else {
                return false;
            };
            let chunk = Arc::make_mut(entry.get_mut());
            chunk.remove(at);
            if chunk.states.is_empty() {
                entry.remove();
            }
            pages.count -= 1;
            // Hysteresis prevents a fill/delete near the promotion threshold
            // from repeatedly converting the entire block.
            if pages.count == PAGE_CHUNK / 2 {
                let mut compact = SharedPages::empty(pages.len);
                for (base, chunk, first) in pages.chunks_from(0) {
                    for at in first..chunk.states.len() {
                        let (local, bytes) = chunk.rows.at(at);
                        compact.push(base + local, bytes, chunk.row(at));
                    }
                }
                self.pages = Pages::Multiple(Arc::new(compact));
            }
            return false;
        }
        if let Pages::Inline { page, .. } = self.pages {
            return page.index() == index;
        }
        let Pages::Multiple(pages) = &mut self.pages else {
            unreachable!()
        };
        let Ok(at) = pages.rows.position(index) else {
            return false;
        };
        if pages.states.len() == 1 {
            return true;
        }
        let pages = Arc::make_mut(pages);
        pages.remove(at);
        if pages.states.len() == 1 {
            let (index, bytes) = pages.rows.at(0);
            if let Some(page) = InlinePage::new(index, bytes) {
                if let Some(inline_age) = inline_age {
                    *inline_age = pages.ages.pop().unwrap_or_else(PageAge::unknown);
                }
                self.pages = Pages::Inline {
                    len: pages.len,
                    page,
                    state: pages.states.pop().unwrap(),
                };
            }
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
    // Only occupied slabs need a bitmap. Keep it outside the snapshot slabs.
    occupied: BTreeMap<usize, [u64; OCCUPANCY_WORDS]>,
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

    pub fn at_or_after(&self, start: usize) -> Option<(usize, &BlockRow)> {
        if let Some(row) = self.at(start) {
            return Some((start, row));
        }
        let start_slab = start / SLAB_ROWS;
        let word_bits = u64::BITS as usize;
        // At most the starting slab needs a partial search. Later slabs in
        // this map are nonempty, even after a long run of retired slabs.
        for (&slab, words) in self.occupied.range(start_slab..) {
            let offset = if slab == start_slab {
                start % SLAB_ROWS
            } else {
                0
            };
            for (word, &bits) in words.iter().enumerate().skip(offset / word_bits) {
                let base = word * word_bits;
                let bits = bits & (u64::MAX << offset.saturating_sub(base));
                if bits != 0 {
                    let slot = slab * SLAB_ROWS + base + bits.trailing_zeros() as usize;
                    return Some((slot, self.at(slot).expect("occupied catalog slot")));
                }
            }
        }
        None
    }

    fn occupancy_address(handle: u32) -> (usize, usize, u64) {
        let slot = handle as usize - 1;
        let offset = slot % SLAB_ROWS;
        let word_bits = u64::BITS as usize;
        (
            slot / SLAB_ROWS,
            offset / word_bits,
            1 << (offset % word_bits),
        )
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

    #[cfg(test)]
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
        let (slab, word, bit) = Self::occupancy_address(handle);
        self.occupied.entry(slab).or_default()[word] |= bit;
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
                let (slab, word, bit) = Self::occupancy_address(handle);
                let words = self.occupied.get_mut(&slab).unwrap();
                words[word] &= !bit;
                if words.iter().all(|&bits| bits == 0) {
                    self.occupied.remove(&slab);
                }
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

    pub(crate) fn for_each_paged_mut(&mut self, mut visit: impl FnMut(&mut Geometry)) {
        for slab in &mut self.slabs {
            if slab
                .iter()
                .flatten()
                .any(|row| row.geometry.kind() == Some(StoreKind::Paged))
            {
                for row in Arc::make_mut(slab).iter_mut().flatten() {
                    if row.geometry.kind() == Some(StoreKind::Paged) {
                        visit(&mut row.geometry);
                    }
                }
            }
        }
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
    fn descending_fills_keep_edits_within_one_bounded_chunk() {
        const COUNT: u32 = 65_536;
        const LEN: u64 = COUNT as u64 * 4096;
        let mut row = Geometry::new(StoreKind::Paged, None, LEN, COUNT - 1, 4096);
        for page in (0..COUNT - 1).rev() {
            row.put(LEN, page, 4096, None);
        }
        let Pages::Chunked(chunks) = &row.pages else {
            panic!("large block stayed flat")
        };
        assert_eq!(chunks.chunks.len(), COUNT as usize / PAGE_CHUNK);
        for chunk in chunks.chunks.values() {
            let PageRows::Compact(rows) = &chunk.rows else {
                panic!("dense chunk widened")
            };
            assert_eq!(rows.len(), PAGE_CHUNK);
            assert_eq!(rows.capacity(), PAGE_CHUNK);
            assert_eq!(chunk.states.capacity(), PAGE_CHUNK);
            assert!(chunk.ages.is_empty());
        }
        for page in 0..COUNT - (PAGE_CHUNK / 2) as u32 {
            assert!(!row.remove_page(page, None));
        }
        assert!(matches!(row.pages, Pages::Multiple(_)));
        assert_eq!(row.page_count(), PAGE_CHUNK / 2);
        assert_eq!(row.allocation(COUNT - 1), Some(4096));
    }

    #[test]
    fn inline_layout_and_sparse_wide_page_roundtrip() {
        assert_eq!(size_of::<BlockRow>(), 40);
        assert_eq!(size_of::<Option<BlockRow>>(), 40);
        assert_eq!(size_of::<PageWord>(), 4);
        assert_eq!(size_of::<CompactPage>(), 6);
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
        row.put(u64::from(u32::MAX), 1 << 20, 4096, None);
        row.put(u64::from(u32::MAX), 3, u64::MAX, None);
        assert!(row
            .pages_after(None)
            .eq([(1, 512), (3, u64::MAX), (1 << 20, 4096)]));
        assert!(frozen[0]
            .as_ref()
            .unwrap()
            .geometry
            .pages_after(None)
            .eq([(1, 512)]));
        row.remove_page(3, None);
        row.remove_page(1 << 20, None);
        assert!(matches!(row.pages, Pages::Inline { .. }));
        assert!(row.remove_page(1, None));
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
