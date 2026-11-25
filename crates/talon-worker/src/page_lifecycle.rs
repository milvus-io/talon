//! Page residency and idle age. Disk operations never run under these locks.
use crate::block_key::{BlockIdentity, BlockKey};
use crate::catalog::{DiskId, Geometry};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use talon_core::{BlockId, PageIndex};

// Match the existing two-hex-digit disk directories. This also lets checkpoint
// traversal retain only one directory's state at a time.
pub(crate) const SHARDS: usize = 256;
pub(crate) fn disk_shard(id: &BlockId) -> usize {
    let mut hash = DefaultHasher::new();
    id.hash(&mut hash);
    (hash.finish() >> 56) as usize
}
const DIRECTORY_SHARDS: usize = 256;

/// Active mutations share a gate for the exact block identity. The pool
/// retains no gates or key references after the last operation releases one.
struct MutationGates {
    blocks: Mutex<HashMap<BlockKey, Weak<PageMutationGate>>>,
    directories: Vec<Arc<tokio::sync::RwLock<()>>>,
}

impl MutationGates {
    fn gate(self: &Arc<Self>, id: &BlockKey, stripe: usize) -> Arc<PageMutationGate> {
        let mut active = self.blocks.lock().unwrap();
        if let Some(gate) = active.get(id).and_then(Weak::upgrade) {
            return gate;
        }
        let gate = Arc::new(PageMutationGate {
            owner: Arc::downgrade(self),
            id: id.clone(),
            directory: self.directories[stripe].clone(),
            block: Arc::default(),
        });
        active.insert(id.clone(), Arc::downgrade(&gate));
        gate
    }
}

/// The directory stripe is shared with orphan cleanup. The block lock allows
/// page fills with disjoint InFlightGuards to run concurrently.
struct PageMutationGate {
    owner: Weak<MutationGates>,
    id: BlockKey,
    directory: Arc<tokio::sync::RwLock<()>>,
    block: Arc<tokio::sync::RwLock<()>>,
}

impl Drop for PageMutationGate {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            let mut active = owner.blocks.lock().unwrap();
            if active
                .get(&self.id)
                .is_some_and(|gate| std::ptr::eq(gate.as_ptr(), self))
            {
                active.remove(&self.id);
            }
        }
    }
}

pub(crate) struct PageMutationGuard {
    _block: futures::future::Either<
        tokio::sync::OwnedRwLockReadGuard<()>,
        tokio::sync::OwnedRwLockWriteGuard<()>,
    >,
    _directory: tokio::sync::OwnedRwLockReadGuard<()>,
    _gate: Arc<PageMutationGate>,
}
/// Anchored wall time: monotonic during a process, portable across restarts.
pub(crate) struct AccessClock {
    started: Instant,
    unix_ms: u64,
    #[cfg(test)]
    test_now: std::sync::atomic::AtomicU64,
}
impl AccessClock {
    pub fn new() -> Self {
        Self {
            #[cfg(test)]
            test_now: std::sync::atomic::AtomicU64::new(0),
            started: Instant::now(),
            unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(u64::MAX as u128) as u64,
        }
    }
    pub fn now(&self) -> u64 {
        #[cfg(test)]
        {
            let now = self.test_now.load(std::sync::atomic::Ordering::Relaxed);
            if now != 0 {
                return now;
            }
        }
        self.unix_ms
            .saturating_add(self.started.elapsed().as_millis().min(u64::MAX as u128) as u64)
    }
    #[cfg(test)]
    pub(crate) fn set(&self, now: u64) {
        self.test_now
            .store(now, std::sync::atomic::Ordering::Relaxed);
    }
}

// Pin counts move with these flags when the resident row is copied or moved.
// Bits 6 through 8 belong to the eviction policy.
const ONE_READER: u32 = 1 << 9;
const PINNED: u32 = (u16::MAX as u32) * ONE_READER;
const CLOSED: u32 = 1 << 1;
const ACCESSED: u32 = 1 << 2;
const DIRTY: u32 = 1 << 3;
const RETRY_SHIFT: u32 = 4;
const RETRY: u32 = 3 << RETRY_SHIFT;
const UNKNOWN: u64 = u64::MAX;
const UNTRACKED: u64 = u64::MAX - 1;

#[derive(Default)]
pub(crate) struct PageWord(pub(crate) AtomicU32);

impl Clone for PageWord {
    fn clone(&self) -> Self {
        Self(AtomicU32::new(self.load(Ordering::Relaxed)))
    }
}

impl std::ops::Deref for PageWord {
    type Target = AtomicU32;
    fn deref(&self) -> &AtomicU32 {
        &self.0
    }
}

pub(crate) struct PageAge(AtomicU64);

impl Clone for PageAge {
    fn clone(&self) -> Self {
        Self(AtomicU64::new(self.0.load(Ordering::Relaxed)))
    }
}

impl PageAge {
    pub(crate) fn unknown() -> Self {
        Self(AtomicU64::new(UNKNOWN))
    }

    pub(crate) fn tracked(&self) -> bool {
        self.0.load(Ordering::Relaxed) != UNTRACKED
    }
}

#[derive(Default)]
pub(crate) struct PageEntry {
    pub(crate) unit: crate::index::PageUnit,
    pub(crate) state: PageWord,
}

/// In-memory index fixtures use these rows. The journal-backed index borrows
/// its geometry and lifecycle state directly from the catalog.
#[derive(Default)]
pub(crate) struct PageRows {
    rows: crate::page_table::PageMap<PageEntry>,
    ages: crate::page_table::PageMap<PageAge>,
}
impl std::ops::Deref for PageRows {
    type Target = crate::page_table::PageMap<PageEntry>;

    fn deref(&self) -> &Self::Target {
        &self.rows
    }
}
impl std::ops::DerefMut for PageRows {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rows
    }
}
impl PageRows {
    pub(crate) fn track_age(&mut self, page: u32) {
        self.ages.get_or_insert_with(page, PageAge::unknown);
    }

    pub(crate) fn track_resident_ages(&mut self) {
        for (&page, row) in self.rows.iter() {
            if row.unit.resident() {
                self.ages.get_or_insert_with(page, PageAge::unknown);
            }
        }
    }

    pub(crate) fn forget_page(&mut self, page: u32) -> u32 {
        self.ages.remove(&page);
        let row = self.rows.get_mut(&page).expect("resident page");
        std::mem::take(row.state.0.get_mut())
    }
}

#[derive(Clone, Copy)]
pub(crate) struct PageRow<'a> {
    pub state: &'a PageWord,
    pub age: Option<&'a PageAge>,
}

impl PageRow<'_> {
    fn age(self) -> Option<u64> {
        let time = self.age?.0.load(Ordering::Relaxed);
        (time < UNTRACKED).then_some(time)
    }

    fn restore(self, access: Option<u64>) {
        if let Some(age) = self.age.filter(|age| age.tracked()) {
            age.0.store(
                access.map_or(UNKNOWN, |t| t.min(UNTRACKED - 1)),
                Ordering::Relaxed,
            );
        }
    }

    fn touch(self, now: u64) -> bool {
        let Some(age) = self.age else { return false };
        let mut old = age.0.load(Ordering::Relaxed);
        let now = now.min(UNTRACKED - 1);
        while old != UNTRACKED && (old == UNKNOWN || old < now) {
            match age
                .0
                .compare_exchange_weak(old, now, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return self.state.fetch_or(DIRTY, Ordering::Relaxed) & DIRTY == 0,
                Err(actual) => old = actual,
            }
        }
        false
    }

    pub(crate) fn pinned(self) -> bool {
        self.state.load(Ordering::Relaxed) & PINNED != 0
    }

    pub(crate) fn retry_pending(self) -> bool {
        self.state.load(Ordering::Relaxed) & RETRY != 0
    }
}

pub(crate) enum PageSet<'a> {
    Catalog(&'a Geometry, Option<&'a PageAge>),
    Legacy(&'a PageRows, &'a PageTable, usize),
}

impl PageSet<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Catalog(geometry, _) => geometry.page_count(),
            Self::Legacy(_, _, count) => *count,
        }
    }

    fn get(&self, page: u32) -> Option<PageRow<'_>> {
        match self {
            Self::Catalog(geometry, age) => geometry.page_row(page, *age),
            Self::Legacy(rows, _, _) => {
                let row = rows.get(&page)?;
                row.unit.resident().then_some(PageRow {
                    state: &row.state,
                    age: rows.ages.get(&page),
                })
            }
        }
    }

    fn iter_from(&self, start: u32) -> impl Iterator<Item = (u32, PageRow<'_>)> {
        let (catalog, legacy) = match self {
            Self::Catalog(geometry, age) => (Some(geometry.page_rows_from(start, *age)), None),
            Self::Legacy(rows, _, _) => (
                None,
                Some(rows.range(start..).filter_map(|(&page, row)| {
                    row.unit.resident().then_some((
                        page,
                        PageRow {
                            state: &row.state,
                            age: rows.ages.get(&page),
                        },
                    ))
                })),
            ),
        };
        catalog
            .into_iter()
            .flatten()
            .chain(legacy.into_iter().flatten())
    }
}

pub(crate) type PageTable = Arc<RwLock<PageRows>>;

// A slot can be reused between batches. The catalog's durable address and
// the standalone table allocation identify an incarnation without pinning it.
enum PageIncarnation {
    Catalog(Option<DiskId>),
    Legacy(Weak<RwLock<PageRows>>),
}

impl PageIncarnation {
    fn of(rows: &PageSet<'_>) -> Self {
        match rows {
            PageSet::Catalog(geometry, _) => Self::Catalog(geometry.disk_id),
            PageSet::Legacy(_, table, _) => Self::Legacy(Arc::downgrade(table)),
        }
    }

    fn matches(&self, rows: &PageSet<'_>) -> bool {
        match (self, rows) {
            (Self::Catalog(disk), PageSet::Catalog(geometry, _)) => *disk == geometry.disk_id,
            (Self::Legacy(old), PageSet::Legacy(_, table, _)) => old.as_ptr() == Arc::as_ptr(table),
            _ => false,
        }
    }
}

pub(crate) struct PageReadGuard {
    index: Arc<crate::BlockIndex>,
    slot: usize,
    ticket: u64,
    len: u32,
    accessed_at: Option<u64>,
    successful: bool,
}

impl PageReadGuard {
    fn pin(page: u32, state: &PageWord) -> Option<()> {
        match state.fetch_update(Ordering::AcqRel, Ordering::Relaxed, |flags| {
            (flags & CLOSED == 0 && flags & PINNED != PINNED).then(|| flags + ONE_READER)
        }) {
            Ok(_) => Some(()),
            Err(flags) => {
                if flags & PINNED == PINNED {
                    static REPORTED: AtomicBool = AtomicBool::new(false);
                    if !REPORTED.swap(true, Ordering::Relaxed) {
                        tracing::warn!(page, "resident page reader limit reached");
                    }
                }
                None
            }
        }
    }

    /// Pin the resident prefix while the caller holds the directory and page
    /// rows stable. The caller drops a partial guard outside those locks.
    pub(crate) fn acquire<'a, T>(
        index: &Arc<crate::BlockIndex>,
        slot: usize,
        start: u32,
        mut rows: impl Iterator<Item = (&'a PageWord, T)>,
        mut touch: impl FnMut(&PageWord, T),
    ) -> Option<Self> {
        let (first, access) = rows.next()?;
        let work = &index.page_work;
        let ticket = {
            let mut active = work.operations(slot).lock().unwrap();
            if active.get((slot, BLOCK_CLAIM)).is_some() {
                return None;
            }
            Self::pin(start, first)?;
            let ticket = work.next_identity();
            active.insert((slot, ticket), u64::from(start));
            ticket
        };
        // The directory (or standalone page table) remains borrowed until
        // the range is pinned. Other slots can use this operation stripe.
        touch(first, access);
        let mut len = 1;
        for (state, access) in rows {
            if Self::pin(start + len, state).is_none() {
                break;
            }
            touch(state, access);
            len += 1;
        }
        Some(Self {
            index: index.clone(),
            slot,
            ticket,
            len,
            accessed_at: None,
            successful: false,
        })
    }

    pub(crate) fn covers(&self, len: u32) -> bool {
        self.len == len
    }

    pub fn record_access_and_release(mut self, now: Option<u64>) {
        self.accessed_at = now;
        self.successful = true;
    }

    #[cfg(test)]
    pub fn record_access(&self, now: u64) {
        assert_eq!(self.len, 1);
        self.index.with_page_slot(self.slot, false, |id, rows| {
            let work = &self.index.page_work;
            let active = work.operations(self.slot).lock().unwrap();
            let Some(page) = active
                .get((self.slot, self.ticket))
                .map(|start| start as u32)
            else {
                return;
            };
            drop(active);
            if let Some(row) = rows.get(page) {
                let changed = row.touch(now);
                let flags = row
                    .state
                    .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |flags| {
                        Some((flags & !RETRY) | ACCESSED)
                    })
                    .unwrap();
                if changed {
                    work.mark(id, self.slot, now, false);
                }
                if flags & RETRY != 0 {
                    work.cancel_retry(self.slot, page);
                }
            }
        });
    }
}

impl Drop for PageReadGuard {
    fn drop(&mut self) {
        self.index.with_page_slot(self.slot, false, |id, rows| {
            let work = &self.index.page_work;
            let start = work
                .operations(self.slot)
                .lock()
                .unwrap()
                .remove((self.slot, self.ticket));
            let Some(start) = start.map(|start| start as u32) else {
                // A detached block may already have reused this slot.
                return;
            };
            let mut changed_age = false;
            let mut release = |page, row: PageRow<'_>| {
                if self.successful {
                    changed_age |= self.accessed_at.is_some_and(|now| row.touch(now));
                    let flags = row
                        .state
                        .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |flags| {
                            debug_assert!(flags & PINNED != 0);
                            Some(((flags - ONE_READER) & !RETRY) | ACCESSED)
                        })
                        .unwrap();
                    if flags & RETRY != 0 {
                        work.cancel_retry(self.slot, page);
                    }
                } else {
                    let flags = row.state.fetch_sub(ONE_READER, Ordering::Release);
                    debug_assert!(flags & PINNED != 0);
                }
            };
            if self.len == 1 {
                if let Some(row) = rows.get(start) {
                    release(start, row);
                }
            } else {
                for (page, row) in rows.iter_from(start) {
                    if page - start >= self.len {
                        break;
                    }
                    release(page, row);
                }
            }
            if changed_age {
                work.mark(id, self.slot, self.accessed_at.unwrap(), false);
            }
        });
    }
}

fn retry(flags: u32) -> Option<usize> {
    let value = (flags & RETRY) >> RETRY_SHIFT;
    (value != 0).then(|| value as usize - 1)
}

pub(crate) fn expired(last: Option<u64>, now: u64, tti: u64) -> bool {
    last.map_or(true, |last| now.saturating_sub(last) > tti)
}

/// A lookup key and the mutation gate pool. Creating this value does not
/// publish an index row. Only async mutations allocate a gate.
#[derive(Clone)]
pub(crate) struct PagedMutation {
    pub id: BlockKey,
    index: Arc<crate::BlockIndex>,
    gates: Arc<MutationGates>,
}

impl PagedMutation {
    fn gate(&self) -> Arc<PageMutationGate> {
        let mut hash = DefaultHasher::new();
        self.id.hash(&mut hash);
        self.gates
            .gate(&self.id, hash.finish() as usize % DIRECTORY_SHARDS)
    }

    pub async fn lock(&self) -> PageMutationGuard {
        let gate = self.gate();
        let directory = gate.directory.clone().read_owned().await;
        let block = futures::future::Either::Right(gate.block.clone().write_owned().await);
        PageMutationGuard {
            _block: block,
            _directory: directory,
            _gate: gate,
        }
    }

    pub async fn lock_page_fill(&self) -> PageMutationGuard {
        let gate = self.gate();
        let directory = gate.directory.clone().read_owned().await;
        let block = futures::future::Either::Left(gate.block.clone().read_owned().await);
        PageMutationGuard {
            _block: block,
            _directory: directory,
            _gate: gate,
        }
    }

    pub fn has_pages(&self) -> bool {
        self.index
            .with_pages(&self.id, false, |_, rows| {
                rows.iter_from(0).next().is_some()
            })
            .unwrap_or(false)
    }

    pub fn cleanup_finished(&self) {
        self.index.with_pages(&self.id, true, |slot, _| {
            self.index.page_work.clear_pending(slot)
        });
    }

    #[cfg(test)]
    pub fn page_keys(&self) -> Vec<u32> {
        self.index
            .with_pages(&self.id, false, |_, rows| {
                rows.iter_from(0).map(|(page, _)| page).collect()
            })
            .unwrap_or_default()
    }

    pub fn restore_ages(&self, records: &BTreeMap<u32, u64>) -> (u64, u64) {
        self.index
            .with_pages(&self.id, true, |_, rows| {
                let mut resident = 0;
                let mut missing = 0;
                for (page, row) in rows.iter_from(0) {
                    let access = records.get(&page).copied();
                    row.restore(access);
                    resident += 1;
                    missing += u64::from(access.is_none());
                }
                (resident, missing)
            })
            .unwrap_or_default()
    }

    /// A fill preserves any newer read age recorded after the durable row
    /// became visible. Startup restores the age before serving reads.
    pub fn register(&self, page: PageIndex, access: Option<u64>, now: u64, fill: bool) {
        self.index.with_pages(&self.id, true, |slot, rows| {
            let Some(row) = rows.get(page.0) else { return };
            if fill {
                if access.is_some_and(|access| row.touch(access)) {
                    self.index.page_work.mark(&self.id, slot, now, false);
                }
                let state = row.state.load(Ordering::Relaxed);
                row.state
                    .store((state & !RETRY) | ACCESSED, Ordering::Release);
                if state & RETRY != 0 {
                    self.index.page_work.cancel_retry(slot, page.0);
                }
            } else {
                row.restore(access);
            }
        });
    }

    #[cfg(test)]
    pub fn register_fixture(&self, page: PageIndex, access: Option<u64>, now: u64, fill: bool) {
        let id = self.id.materialize();
        self.index
            .init_paged(id.clone(), 1, u64::from(id.block_size));
        self.index.mark_page_allocated(&id, page, 0);
        self.register(page, access, now, fill);
    }

    pub fn acquire(&self, page: PageIndex) -> Option<PageReadGuard> {
        self.index.with_pages(&self.id, false, |slot, rows| {
            let row = rows.get(page.0)?;
            PageReadGuard::acquire(
                &self.index,
                slot,
                page.0,
                std::iter::once((row.state, ())),
                |_, ()| {},
            )
        })?
    }

    pub fn candidate(&self, page: PageIndex) -> Option<GcCandidate> {
        self.index.with_pages(&self.id, false, |slot, rows| {
            let row = rows.get(page.0)?;
            let flags = row.state.load(Ordering::Acquire);
            self.select(slot, page.0, row.state, 0, flags)
        })?
    }

    fn select(
        &self,
        slot: usize,
        page: u32,
        state: &PageWord,
        reason: usize,
        flags: u32,
    ) -> Option<GcCandidate> {
        if flags & (CLOSED | PINNED) != 0 {
            return None;
        }
        let work = &self.index.page_work;
        let mut active = work.operations(slot).lock().unwrap();
        if active.get((slot, BLOCK_CLAIM)).is_some() {
            return None;
        }
        state
            .compare_exchange(
                flags,
                flags & !ACCESSED,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .ok()?;
        let identity = work.next_identity();
        active.insert((slot, u64::from(page)), identity);
        Some(GcCandidate {
            block: self.clone(),
            page: PageIndex(page),
            lease: Arc::new(PageSelection {
                work: work.clone(),
                slot,
                page,
                identity,
            }),
            reason,
        })
    }

    pub fn claim(&self, c: &GcCandidate, now: u64, tti: Option<u64>) -> bool {
        let work = &self.index.page_work;
        if !Arc::ptr_eq(work, &c.lease.work) {
            return false;
        }
        self.index
            .with_pages(&self.id, false, |slot, rows| {
                let Some(row) = rows.get(c.page.0) else {
                    return false;
                };
                let mut active = work.operations(slot).lock().unwrap();
                let flags = row.state.load(Ordering::Acquire);
                if active.get((slot, BLOCK_CLAIM)).is_some()
                    || !c.lease.current(&active, slot)
                    || flags & (CLOSED | PINNED | ACCESSED) != 0
                    || tti.is_some_and(|tti| !expired(row.age(), now, tti))
                {
                    return false;
                }
                if row
                    .state
                    .compare_exchange(flags, flags | CLOSED, Ordering::AcqRel, Ordering::Relaxed)
                    .is_err()
                {
                    return false;
                }
                active.remove((slot, u64::from(c.page.0)));
                active.insert(
                    (slot, FIRST_CLOSED_PAGE + u64::from(c.page.0)),
                    c.lease.identity,
                );
                true
            })
            .unwrap_or(false)
    }

    pub fn abort(&self, c: &GcCandidate, reason: usize) {
        let work = &self.index.page_work;
        if !Arc::ptr_eq(work, &c.lease.work) {
            return;
        }
        self.index.with_pages(&self.id, true, |slot, rows| {
            let Some(row) = rows.get(c.page.0) else {
                return;
            };
            let mut active = work.operations(slot).lock().unwrap();
            let key = (slot, FIRST_CLOSED_PAGE + u64::from(c.page.0));
            if slot == c.lease.slot && active.get(key) == Some(c.lease.identity) {
                row.state
                    .fetch_update(Ordering::Release, Ordering::Relaxed, |flags| {
                        Some((flags & !(CLOSED | RETRY)) | ((reason as u32 + 1) << RETRY_SHIFT))
                    })
                    .unwrap();
                active.remove(key);
                active.insert((slot, u64::from(c.page.0)), c.lease.identity);
                drop(active);
                work.schedule_retry(slot, c.page.0);
            }
        });
    }

    pub fn claim_all_pages(&self) -> Option<BlockPageClaim> {
        let work = &self.index.page_work;
        let mut claim = BlockPageClaim {
            work: work.clone(),
            operation: None,
            len: 0,
        };
        // The exclusive directory (standalone: page table) borrow serializes
        // this check with complete range pin and release, while the stripe
        // separates pending GC selections from reads and claimed deletions.
        let valid = self.index.with_pages(&self.id, true, |slot, rows| {
            let mut active = work.operations(slot).lock().unwrap();
            if active.blocks_claim(slot) {
                return false;
            }
            let identity = work.next_identity();
            active.insert((slot, BLOCK_CLAIM), identity);
            claim.operation = Some((slot, identity));
            claim.len = rows.len();
            true
        });
        valid.unwrap_or(true).then_some(claim)
    }

    #[cfg(test)]
    pub fn last_access(&self, page: u32) -> Option<u64> {
        self.index
            .with_pages(&self.id, false, |_, rows| rows.get(page)?.age())?
    }

    #[cfg(test)]
    pub fn dirty(&self) -> Option<(u64, bool)> {
        self.index.with_pages(&self.id, false, |slot, _| {
            self.index
                .page_work
                .shard(disk_shard_key(&self.id))
                .lock()
                .unwrap()
                .get(slot)
        })?
    }

    #[cfg(test)]
    pub fn hold_rows(&self) -> PageTable {
        self.index.legacy_pages(&self.id).unwrap()
    }
}

pub(crate) struct BlockPageClaim {
    work: Arc<PageWork>,
    operation: Option<(usize, u64)>,
    len: usize,
}
impl BlockPageClaim {
    pub fn len(&self) -> usize {
        self.len
    }
}
impl Drop for BlockPageClaim {
    fn drop(&mut self) {
        if let Some((slot, identity)) = self.operation {
            let mut active = self.work.operations(slot).lock().unwrap();
            if active.get((slot, BLOCK_CLAIM)) == Some(identity) {
                active.remove((slot, BLOCK_CLAIM));
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct GcCandidate {
    pub block: PagedMutation,
    pub page: PageIndex,
    lease: Arc<PageSelection>,
    pub reason: usize,
}

const OPERATION_SHARDS: usize = 64;
// Each slot orders pending selections before operations that block a whole
// claim. Page keys store a selection identity, read tickets store their start
// page, and the last key stores the whole-block claim's identity.
const FIRST_CLOSED_PAGE: u64 = 1 << 32;
const FIRST_READ_TICKET: u64 = 2 << 32;
const BLOCK_CLAIM: u64 = u64::MAX;
type OperationKey = (usize, u64);

// One operation fits in a stripe without allocating a tree.
#[derive(Default)]
enum ActivePages {
    #[default]
    Empty,
    One(OperationKey, u64),
    Tree(BTreeMap<OperationKey, u64>),
}

impl ActivePages {
    fn insert(&mut self, key: OperationKey, value: u64) {
        match self {
            Self::Empty => *self = Self::One(key, value),
            Self::One(current, old) if *current == key => *old = value,
            Self::One(current, old) => {
                *self = Self::Tree(BTreeMap::from([(*current, *old), (key, value)]));
            }
            Self::Tree(tree) => {
                tree.insert(key, value);
            }
        }
    }

    fn get(&self, key: OperationKey) -> Option<u64> {
        match self {
            Self::One(current, value) if *current == key => Some(*value),
            Self::Tree(tree) => tree.get(&key).copied(),
            _ => None,
        }
    }

    fn remove(&mut self, key: OperationKey) -> Option<u64> {
        match self {
            Self::One(current, value) if *current == key => {
                let value = *value;
                *self = Self::Empty;
                Some(value)
            }
            Self::Tree(tree) => {
                let removed = tree.remove(&key);
                if tree.is_empty() {
                    *self = Self::Empty;
                }
                removed
            }
            _ => None,
        }
    }

    fn blocks_claim(&self, slot: usize) -> bool {
        match self {
            Self::One((current, key), _) => *current == slot && *key >= FIRST_CLOSED_PAGE,
            Self::Tree(tree) => tree
                .range((slot, FIRST_CLOSED_PAGE)..=(slot, BLOCK_CLAIM))
                .next()
                .is_some(),
            Self::Empty => false,
        }
    }

    fn remove_selection(&mut self, slot: usize, page: u32) {
        if self.remove((slot, u64::from(page))).is_none() {
            self.remove((slot, FIRST_CLOSED_PAGE + u64::from(page)));
        }
    }

    fn invalidate_legacy_page(&mut self, slot: usize, page: u32, pinned: bool) {
        self.remove_selection(slot, page);
        if !pinned {
            return;
        }
        // Standalone readers use one ticket per page because public
        // clear_page may forcibly retire that page while others stay pinned.
        let retired = |&(current, key): &OperationKey, &start: &u64| {
            current == slot
                && (FIRST_READ_TICKET..BLOCK_CLAIM).contains(&key)
                && start == u64::from(page)
        };
        match self {
            Self::One(key, start) if retired(key, start) => *self = Self::Empty,
            Self::Tree(tree) => {
                tree.retain(|key, start| !retired(key, start));
                if tree.is_empty() {
                    *self = Self::Empty;
                }
            }
            _ => {}
        }
    }

    fn invalidate_block(&mut self, slot: usize) {
        match self {
            Self::One((current, _), _) if *current == slot => *self = Self::Empty,
            Self::Tree(tree) => {
                while let Some((&key, _)) = tree.range((slot, 0)..=(slot, BLOCK_CLAIM)).next() {
                    tree.remove(&key);
                }
                if tree.is_empty() {
                    *self = Self::Empty;
                }
            }
            _ => {}
        }
    }
}

struct PageSelection {
    work: Arc<PageWork>,
    slot: usize,
    page: u32,
    identity: u64,
}

impl PageSelection {
    fn current(&self, active: &ActivePages, slot: usize) -> bool {
        self.slot == slot && active.get((slot, u64::from(self.page))) == Some(self.identity)
    }
}

impl Drop for PageSelection {
    fn drop(&mut self) {
        let mut active = self.work.operations(self.slot).lock().unwrap();
        if self.current(&active, self.slot) {
            active.remove((self.slot, u64::from(self.page)));
        }
    }
}

#[derive(Default)]
struct Pending {
    cleanup: bool,
    retries: BTreeSet<u32>,
}

// One record covers 64 directory slots. Clean resident blocks own no
// checkpoint or retry records. Oldest is conservative within a dirty group.
struct DirtyGroup {
    slots: u64,
    replace: u64,
    oldest: u64,
}

#[derive(Default)]
struct CheckpointShard {
    dirty: BTreeMap<usize, DirtyGroup>,
    membership: u64,
    checkpointed_membership: u64,
    removed: BTreeMap<u64, BlockId>,
    removed_bytes: usize,
    removal_overflow: u64,
    journal: crate::page_access_shard::JournalState,
}
impl CheckpointShard {
    fn mark(&mut self, slot: usize, now: u64, replace: bool) {
        let mask = 1 << (slot % 64);
        let group = self.dirty.entry(slot / 64).or_insert(DirtyGroup {
            slots: 0,
            replace: 0,
            oldest: now,
        });
        group.slots |= mask;
        group.oldest = group.oldest.min(now);
        if replace {
            group.replace |= mask;
        }
    }

    fn get(&self, slot: usize) -> Option<(u64, bool)> {
        let mask = 1 << (slot % 64);
        let group = self.dirty.get(&(slot / 64))?;
        (group.slots & mask != 0).then_some((group.oldest, group.replace & mask != 0))
    }

    fn take(&mut self, slot: usize) -> Option<(u64, bool)> {
        let mask = 1 << (slot % 64);
        let group = self.dirty.get_mut(&(slot / 64))?;
        if group.slots & mask == 0 {
            return None;
        }
        let result = (group.oldest, group.replace & mask != 0);
        group.slots &= !mask;
        group.replace &= !mask;
        if group.slots == 0 {
            self.dirty.remove(&(slot / 64));
        }
        Some(result)
    }

    fn next_dirty(&self, start: usize, ceiling: usize) -> Option<usize> {
        if start >= ceiling {
            return None;
        }
        let group_start = start / 64;
        let slot = self
            .dirty
            .range(group_start..)
            .find_map(|(&group, dirty)| {
                let slots = if group == group_start {
                    dirty.slots & (u64::MAX << (start % 64))
                } else {
                    dirty.slots
                };
                (slots != 0).then(|| group * 64 + slots.trailing_zeros() as usize)
            })?;
        (slot < ceiling).then_some(slot)
    }

    fn removed(&mut self, id: &BlockKey) {
        self.membership += 1;
        if self.journal.generation == 0 {
            // A first base may already be encoding this block. If this
            // removal is newer than that base, force the next checkpoint full.
            self.removal_overflow = self.membership;
            return;
        }
        let view = id.view();
        let bytes =
            view.object.bucket.len() + view.object.object_path.len() + view.version.len() + 128;
        if self.removed_bytes + bytes > (1 << 20) {
            self.removal_overflow = self.membership;
        } else {
            self.removed_bytes += bytes;
            self.removed.insert(self.membership, view.materialize());
        }
    }
}

pub(crate) struct PageWork {
    track_access: AtomicBool,
    next_identity: AtomicU64,
    active: OnceLock<Box<[Mutex<ActivePages>; OPERATION_SHARDS]>>,
    shards: OnceLock<Box<[Mutex<CheckpointShard>; SHARDS]>>,
    pending: Mutex<BTreeMap<usize, Pending>>,
    has_pending: AtomicBool,
}
impl Default for PageWork {
    fn default() -> Self {
        Self {
            track_access: AtomicBool::new(false),
            next_identity: AtomicU64::new(FIRST_READ_TICKET),
            active: OnceLock::new(),
            shards: OnceLock::new(),
            pending: Mutex::default(),
            has_pending: AtomicBool::new(false),
        }
    }
}
impl PageWork {
    pub(crate) fn tracking(&self) -> bool {
        self.track_access.load(Ordering::Acquire)
    }

    pub(crate) fn enable_tracking(&self) {
        self.shards
            .get_or_init(|| Box::new(std::array::from_fn(|_| Mutex::default())));
        self.track_access.store(true, Ordering::Release);
    }

    fn shard(&self, shard: usize) -> &Mutex<CheckpointShard> {
        &self.shards.get().expect("access tracking configured")[shard]
    }

    fn next_identity(&self) -> u64 {
        self.next_identity
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                old.checked_add(1)
            })
            .expect("page identity exhausted")
    }

    fn operations(&self, slot: usize) -> &Mutex<ActivePages> {
        &self
            .active
            .get_or_init(|| Box::new(std::array::from_fn(|_| Mutex::default())))
            [slot % OPERATION_SHARDS]
    }

    #[cfg(test)]
    pub(crate) fn operation_counts(&self, slot: usize) -> (usize, usize) {
        let mut pages = 0;
        let mut blocks = 0;
        let mut count = |key| {
            if key == BLOCK_CLAIM {
                blocks += 1;
            } else if key < FIRST_READ_TICKET {
                pages += 1;
            }
        };
        match &*self.operations(slot).lock().unwrap() {
            ActivePages::One((current, key), _) if *current == slot => count(*key),
            ActivePages::Tree(tree) => {
                for (&(_, key), _) in tree.range((slot, 0)..=(slot, BLOCK_CLAIM)) {
                    count(key);
                }
            }
            _ => {}
        }
        (blocks, pages)
    }

    pub(crate) fn remove_selection(&self, slot: usize, page: u32) {
        if let Some(shards) = self.active.get() {
            shards[slot % OPERATION_SHARDS]
                .lock()
                .unwrap()
                .remove_selection(slot, page);
        }
    }

    fn invalidate_block(&self, slot: usize) {
        if let Some(shards) = self.active.get() {
            shards[slot % OPERATION_SHARDS]
                .lock()
                .unwrap()
                .invalidate_block(slot);
        }
    }

    pub(crate) fn mark(&self, id: &BlockKey, slot: usize, now: u64, replace: bool) {
        if self.tracking() {
            self.shard(disk_shard_key(id))
                .lock()
                .unwrap()
                .mark(slot, now, replace);
        }
    }

    fn schedule_retry(&self, slot: usize, page: u32) {
        let mut pending = self.pending.lock().unwrap();
        pending.entry(slot).or_default().retries.insert(page);
        self.has_pending.store(true, Ordering::Relaxed);
    }

    pub(crate) fn cancel_retry(&self, slot: usize, page: u32) {
        let mut pending = self.pending.lock().unwrap();
        if let Some(work) = pending.get_mut(&slot) {
            work.retries.remove(&page);
            if work.retries.is_empty() && !work.cleanup {
                pending.remove(&slot);
                self.has_pending
                    .store(!pending.is_empty(), Ordering::Relaxed);
            }
        }
    }

    fn clear_pending(&self, slot: usize) {
        if self.has_pending.load(Ordering::Relaxed) {
            let mut pending = self.pending.lock().unwrap();
            pending.remove(&slot);
            self.has_pending
                .store(!pending.is_empty(), Ordering::Relaxed);
        }
    }

    pub(crate) fn added_page(&self, id: &BlockKey, slot: usize, attached: &AtomicBool) {
        if !self.tracking() && !self.has_pending.load(Ordering::Relaxed) {
            return;
        }
        let mut pending = self.pending.lock().unwrap();
        if !attached.load(Ordering::Relaxed) {
            return;
        }
        if pending.get(&slot).is_some_and(|work| work.cleanup) {
            pending.remove(&slot);
            self.has_pending
                .store(!pending.is_empty(), Ordering::Relaxed);
        }
        self.mark_structural(id, slot);
    }

    pub(crate) fn mark_structural(&self, id: &BlockKey, slot: usize) {
        if self.tracking() {
            self.mark(id, slot, crate::eviction::access_millis(), true);
        }
    }

    pub(crate) fn removed_page(
        &self,
        id: &BlockKey,
        slot: usize,
        page: u32,
        empty: bool,
        flags: u32,
        attached: &AtomicBool,
    ) {
        if let Some(shards) = self.active.get() {
            shards[slot % OPERATION_SHARDS]
                .lock()
                .unwrap()
                .invalidate_legacy_page(slot, page, flags & PINNED != 0);
        }
        let retry = flags & RETRY != 0;
        if !retry && !empty && !self.tracking() {
            return;
        }
        let mut pending = self.pending.lock().unwrap();
        if !attached.load(Ordering::Relaxed) {
            return;
        }
        if retry || empty {
            if empty {
                pending.insert(
                    slot,
                    Pending {
                        cleanup: true,
                        retries: BTreeSet::new(),
                    },
                );
            } else if let Some(work) = pending.get_mut(&slot) {
                work.retries.remove(&page);
                if work.retries.is_empty() {
                    pending.remove(&slot);
                }
            }
            self.has_pending
                .store(!pending.is_empty(), Ordering::Relaxed);
        }
        self.mark_structural(id, slot);
    }

    pub(crate) fn detached(&self, id: &BlockKey, slot: usize, attached: Option<&AtomicBool>) {
        // Writers can retain an old entry without holding the directory.
        // Serialize their work publication with this slot's detachment.
        let mut pending = self.pending.lock().unwrap();
        if let Some(attached) = attached {
            attached.store(false, Ordering::Release);
        }
        self.invalidate_block(slot);
        pending.remove(&slot);
        self.has_pending
            .store(!pending.is_empty(), Ordering::Relaxed);
        if self.tracking() {
            let mut work = self.shard(disk_shard_key(id)).lock().unwrap();
            work.take(slot);
            // An empty table may have a removal snapshot in flight. Its
            // tombstone must survive even if that snapshot ultimately fails.
            work.removed(id);
        }
    }

    pub(crate) fn reset(&self) {
        self.track_access.store(false, Ordering::Relaxed);
        for shard in self
            .active
            .get()
            .into_iter()
            .flat_map(|shards| shards.iter())
        {
            *shard.lock().unwrap() = ActivePages::default();
        }
        for shard in self
            .shards
            .get()
            .into_iter()
            .flat_map(|shards| shards.iter())
        {
            *shard.lock().unwrap() = CheckpointShard::default();
        }
        self.pending.lock().unwrap().clear();
        self.has_pending.store(false, Ordering::Relaxed);
    }
}

#[derive(Default)]
pub(crate) struct ScanCursor {
    slot: usize,
    page: u64,
    ceiling: Option<usize>,
    scan_seconds: f64,
    scan_retries: usize,
}
impl ScanCursor {
    pub fn record_batch(
        &mut self,
        seconds: f64,
        retries: usize,
        complete: bool,
    ) -> Option<(f64, usize)> {
        self.scan_seconds += seconds;
        self.scan_retries += retries;
        complete.then(|| {
            (
                std::mem::take(&mut self.scan_seconds),
                std::mem::take(&mut self.scan_retries),
            )
        })
    }
}

#[derive(Default)]
pub(crate) struct ScanReport {
    pub checked: usize,
    pub completed: bool,
    pub retries: usize,
    pub empty: Vec<PagedMutation>,
}

pub(crate) struct PageLifecycle {
    index: Arc<crate::BlockIndex>,
    gates: Arc<MutationGates>,
    checkpoints: Vec<Arc<tokio::sync::Mutex<()>>>,
    #[cfg(test)]
    checkpoint_inventory_work: [std::sync::atomic::AtomicUsize; 2],
}

fn disk_shard_key(id: &BlockKey) -> usize {
    let mut hash = DefaultHasher::new();
    id.hash(&mut hash);
    (hash.finish() >> 56) as usize
}

impl PageLifecycle {
    #[cfg(test)]
    pub fn new() -> Self {
        Self::with_index(Arc::new(crate::BlockIndex::new()), true)
    }

    pub fn with_index(index: Arc<crate::BlockIndex>, track_access: bool) -> Self {
        if track_access {
            index.enable_page_ages();
        }
        Self {
            index,
            checkpoints: (0..SHARDS).map(|_| Arc::default()).collect(),
            #[cfg(test)]
            checkpoint_inventory_work: Default::default(),
            gates: Arc::new(MutationGates {
                blocks: Mutex::default(),
                directories: (0..DIRECTORY_SHARDS).map(|_| Arc::default()).collect(),
            }),
        }
    }

    /// Startup, before durable ages are restored and reads can begin.
    pub fn enable_access_tracking(&self) {
        self.index.enable_page_ages();
    }

    pub fn tracks_access(&self) -> bool {
        self.index.page_work.tracking()
    }

    pub fn directory_gate(&self, digest: u64) -> Arc<tokio::sync::RwLock<()>> {
        self.gates.directories[digest as usize % DIRECTORY_SHARDS].clone()
    }

    pub fn block(&self, id: &BlockId) -> PagedMutation {
        self.mutation(
            self.index
                .canonical_key(id)
                .unwrap_or_else(|| BlockKey::new(id)),
        )
    }

    fn mutation(&self, id: BlockKey) -> PagedMutation {
        PagedMutation {
            id,
            index: self.index.clone(),
            gates: self.gates.clone(),
        }
    }

    #[cfg(test)]
    pub fn get(&self, id: &BlockId) -> Option<PagedMutation> {
        let block = self.block(id);
        self.index.with_pages(id, false, |_, _| ())?;
        Some(block)
    }

    pub fn scan(
        &self,
        cursor: &mut ScanCursor,
        limit: usize,
        now: u64,
        tti: Option<u64>,
        delete_limit: usize,
    ) -> (Vec<GcCandidate>, ScanReport) {
        let mut out = Vec::new();
        let mut report = ScanReport::default();
        let ceiling = *cursor
            .ceiling
            .get_or_insert_with(|| self.index.policy_scan_ceiling());
        let mut work = 0;
        while work < limit && out.len() + report.empty.len() < delete_limit {
            let start_slot = cursor.slot;
            let mut examined = 0;
            let budget = (limit - work).min(64);
            let mut inspect_slot = |slot, id: &BlockKey, rows: PageSet<'_>| {
                if slot != start_slot {
                    cursor.page = 0;
                }
                let (cleanup, retries) = {
                    let pending = self.index.page_work.pending.lock().unwrap();
                    pending.get(&slot).map_or((false, Vec::new()), |pending| {
                        let retries = if tti.is_none() {
                            u32::try_from(cursor.page).ok().map_or(Vec::new(), |start| {
                                pending
                                    .retries
                                    .range(start..)
                                    .copied()
                                    .take(budget + 1)
                                    .collect()
                            })
                        } else {
                            Vec::new()
                        };
                        (pending.cleanup, retries)
                    })
                };
                if cleanup {
                    report.empty.push(self.mutation(id.clone()));
                    return false;
                }
                let Ok(start) = u32::try_from(cursor.page) else {
                    return false;
                };
                let inspect = |(page, row): (u32, PageRow<'_>)| {
                    examined += 1;
                    cursor.page = u64::from(page) + 1;
                    report.checked += 1;
                    let flags = row.state.load(Ordering::Acquire);
                    let retry = retry(flags);
                    report.retries += usize::from(retry.is_some());
                    let idle = tti.is_some_and(|tti| expired(row.age(), now, tti));
                    let retryable = retry.is_some_and(|reason| reason != 0 || tti.is_some());
                    if (idle || retryable) && flags & (CLOSED | PINNED) == 0 {
                        let block = self.mutation(id.clone());
                        if let Some(candidate) = block.select(
                            slot,
                            page,
                            row.state,
                            if idle { 0 } else { retry.unwrap() },
                            flags,
                        ) {
                            out.push(candidate);
                        }
                    }
                    out.len() + report.empty.len() < delete_limit
                };
                let pages = rows.iter_from(start);
                fn scan_pages<'a>(
                    mut pages: impl Iterator<Item = (u32, PageRow<'a>)>,
                    budget: usize,
                    mut inspect: impl FnMut((u32, PageRow<'a>)) -> bool,
                ) -> bool {
                    for _ in 0..budget {
                        let Some(page) = pages.next() else {
                            return false;
                        };
                        if !inspect(page) {
                            return true;
                        }
                    }
                    pages.next().is_some()
                }
                if tti.is_some() {
                    scan_pages(pages, budget, inspect)
                } else {
                    scan_pages(
                        retries
                            .into_iter()
                            .filter_map(|page| Some((page, rows.get(page)?))),
                        budget,
                        inspect,
                    )
                }
            };
            let scanned = if tti.is_some() {
                self.index.scan_page_slot(start_slot..ceiling, inspect_slot)
            } else if start_slot < ceiling {
                let slot = self
                    .index
                    .page_work
                    .pending
                    .lock()
                    .unwrap()
                    .range(start_slot..ceiling)
                    .next()
                    .map(|(&slot, _)| slot);
                slot.map(|slot| {
                    let more = self
                        .index
                        .with_page_slot(slot, false, |id, rows| inspect_slot(slot, id, rows));
                    (slot, more)
                })
            } else {
                None
            };
            let Some((slot, more)) = scanned else {
                cursor.slot = 0;
                cursor.page = 0;
                cursor.ceiling = None;
                report.completed = true;
                break;
            };
            cursor.slot = slot;
            work += examined.max(1);
            if more != Some(true) {
                cursor.slot += 1;
                cursor.page = 0;
            }
        }
        (out, report)
    }

    pub fn checkpoint_gate(&self, shard: usize) -> Arc<tokio::sync::Mutex<()>> {
        self.checkpoints[shard].clone()
    }

    #[cfg(test)]
    fn count_inventory_batch(&self, visits: usize) {
        self.checkpoint_inventory_work[0].fetch_add(visits, Ordering::Relaxed);
        self.checkpoint_inventory_work[1].fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub fn checkpoint_inventory_work(&self) -> (usize, usize) {
        let [visits, locks] = &self.checkpoint_inventory_work;
        (
            visits.load(Ordering::Relaxed),
            locks.load(Ordering::Relaxed),
        )
    }

    pub fn checkpoint_start(&self, shard: usize) -> (usize, u64, bool) {
        self.index.with_slot_ceiling(|ceiling| {
            let work = self.index.page_work.shard(shard).lock().unwrap();
            (
                ceiling,
                work.membership,
                work.membership != work.checkpointed_membership || !work.dirty.is_empty(),
            )
        })
    }

    /// Startup groups compact slab indices once, so restoring many shards does
    /// not rescan the directory 256 times or retain a second set of block keys.
    pub fn recovery_slots(&self) -> [Vec<usize>; SHARDS] {
        let mut shards = std::array::from_fn(|_| Vec::new());
        self.index.visit_paged_slots(
            0,
            self.index.policy_scan_ceiling(),
            usize::MAX,
            |slot, id| shards[disk_shard_key(id)].push(slot),
        );
        shards
    }

    pub fn recovery_block(&self, slot: usize) -> Option<PagedMutation> {
        self.index
            .with_page_slot(slot, false, |id, _| self.mutation(id.clone()))
    }

    /// Delta checkpoints jump directly between dirty group bits.
    pub fn checkpoint_dirty_slots(
        &self,
        shard: usize,
        cursor: &mut usize,
        ceiling: usize,
    ) -> Vec<usize> {
        let work = self.index.page_work.shard(shard).lock().unwrap();
        let mut slots = Vec::new();
        while slots.len() < 64 {
            let Some(slot) = work.next_dirty(*cursor, ceiling) else {
                *cursor = ceiling;
                break;
            };
            slots.push(slot);
            *cursor = slot + 1;
        }
        slots
    }

    pub fn checkpoint_snapshot(
        &self,
        shard: usize,
        slot: usize,
        full: bool,
        membership: u64,
        clock: &AccessClock,
        size: &mut usize,
    ) -> anyhow::Result<Option<PageSnapshot>> {
        self.sample_checkpoint((shard, membership), slot, full, clock, size, || {})
    }

    fn sample_checkpoint(
        &self,
        (shard, membership): (usize, u64),
        slot: usize,
        full: bool,
        clock: &AccessClock,
        size: &mut usize,
        mut between_batches: impl FnMut(),
    ) -> anyhow::Result<Option<PageSnapshot>> {
        let mut batch = CheckpointBatch::default();
        let Some((mut sample, incarnation, consume, mut next_page)) = self
            .index
            .with_page_slot(slot, true, |id, rows| {
                if disk_shard_key(id) != shard {
                    return None;
                }
                let mut work = self.index.page_work.shard(shard).lock().unwrap();
                let dirty = work.get(slot);
                if !full && dirty.is_none() {
                    return None;
                }
                // A later tombstone must be followed by the replacement of any
                // reincarnated block. Leave dirty notifications scheduled when
                // the deletion wasn't included in this checkpoint's tombstones.
                let consume = work.membership == membership;
                let touched = if consume { work.take(slot) } else { None };
                drop(work);
                let replace = full || dirty.is_some_and(|(_, replace)| replace);
                let incarnation = PageIncarnation::of(&rows);
                let next_page = batch.read(&rows, 0, replace, consume);
                let snapshot = PageSnapshot {
                    block: id.clone(),
                    resident: batch.visited != 0,
                    replace,
                    snapshot: crate::page_access_store::AccessSnapshot {
                        revision: 0,
                        sampled_at: 0,
                        records: Vec::new(),
                    },
                    rollback: touched.map(|(oldest, _)| CheckpointRetry {
                        index: self.index.clone(),
                        slot,
                        oldest,
                    }),
                };
                Some((snapshot, incarnation, consume, next_page))
            })
            .flatten()
        else {
            return Ok(None);
        };

        // Budget for identity, record header, and the largest frame header.
        // The encoder also checks the limit, including any tombstones.
        let overhead = if full { 52 } else { 61 };
        let mut bytes = *size + serde_json::to_vec(&sample.block)?.len() + overhead;
        loop {
            bytes += batch.len * 12;
            anyhow::ensure!(
                bytes <= crate::page_access_shard::MAX_BYTES,
                "access shard exceeds 64 MiB limit"
            );
            sample
                .snapshot
                .records
                .extend_from_slice(&batch.records[..batch.len]);
            let Some(page) = next_page else { break };
            between_batches();
            // A late detach has its own tombstone. Never clear the dirty bits
            // of a replacement or combine ages from two incarnations.
            let Some(Some(next)) = self.index.with_page_slot(slot, true, |id, rows| {
                (id == &sample.block && incarnation.matches(&rows))
                    .then(|| batch.read(&rows, page, sample.replace, consume))
            }) else {
                return Ok(None);
            };
            next_page = next;
        }
        // Recovery rejects ages later than the sample timestamp.
        sample.snapshot.sampled_at = clock.now();
        *size = bytes;
        Ok(Some(sample))
    }

    pub fn checkpoint_journal(
        &self,
        shard: usize,
        membership: u64,
    ) -> (crate::page_access_shard::JournalState, Vec<BlockId>) {
        let work = self.index.page_work.shard(shard).lock().unwrap();
        let mut journal = work.journal;
        journal.full |= work.removal_overflow != 0;
        (
            journal,
            work.removed
                .range(..=membership)
                .map(|(_, id)| id.clone())
                .collect(),
        )
    }

    pub fn set_checkpoint_journal(
        &self,
        shard: usize,
        journal: crate::page_access_shard::JournalState,
    ) {
        self.index.page_work.shard(shard).lock().unwrap().journal = journal;
    }

    pub fn checkpoint_finished(&self, shard: usize, membership: u64) {
        let mut work = self.index.page_work.shard(shard).lock().unwrap();
        work.checkpointed_membership = membership;
        // Taking the last dirty group can leave an empty B-tree root. Release
        // its allocation once the checkpoint owns no dirty work to retry.
        if work.dirty.is_empty() {
            work.dirty.clear();
        }
        work.removed.retain(|&revision, _| revision > membership);
        work.removed_bytes = work
            .removed
            .values()
            .map(|id| {
                id.object.bucket.len() + id.object.object_path.len() + id.version.0.len() + 128
            })
            .sum();
        if work.removal_overflow <= membership {
            work.removal_overflow = 0;
        }
    }

    pub fn checkpoint_recovered(&self, shard: usize) {
        self.index.page_work.shard(shard).lock().unwrap().membership += 1;
    }

    pub fn dirty_summary(&self) -> (usize, Option<u64>) {
        let mut count = 0;
        let mut oldest: Option<u64> = None;
        for shard in self
            .index
            .page_work
            .shards
            .get()
            .into_iter()
            .flat_map(|shards| shards.iter())
        {
            for dirty in shard.lock().unwrap().dirty.values() {
                count += dirty.slots.count_ones() as usize;
                oldest = Some(oldest.map_or(dirty.oldest, |old| old.min(dirty.oldest)));
            }
        }
        (count, oldest)
    }
}

const CHECKPOINT_BATCH: usize = 64;

/// A round's inventory lets compaction find clean members. It only holds
/// physical slots, which can be reused between inventory and sampling.
pub(crate) struct CheckpointInventory(Box<[Vec<u32>; SHARDS]>);

impl CheckpointInventory {
    pub fn gather(life: &PageLifecycle, first_shard: usize, remaining: usize) -> Self {
        let mut groups = Box::new(std::array::from_fn(|_| Vec::new()));
        let ceiling = life.index.policy_scan_ceiling();
        let mut cursor = 0;
        while cursor < ceiling {
            let (next, _visited) =
                life.index
                    .visit_paged_slots(cursor, ceiling, CHECKPOINT_BATCH, |slot, id| {
                        let shard = disk_shard_key(id);
                        if (shard + SHARDS - first_shard) % SHARDS < remaining {
                            groups[shard]
                                .push(u32::try_from(slot).expect("directory slot fits in u32"));
                        }
                    });
            cursor = next;
            #[cfg(test)]
            life.count_inventory_batch(_visited);
        }
        Self(groups)
    }

    pub fn take_shard(&mut self, shard: usize) -> Vec<u32> {
        std::mem::take(&mut self.0[shard])
    }
}

struct CheckpointBatch {
    records: [(u32, u64); CHECKPOINT_BATCH],
    len: usize,
    visited: usize,
}

impl Default for CheckpointBatch {
    fn default() -> Self {
        Self {
            records: [(0, 0); CHECKPOINT_BATCH],
            len: 0,
            visited: 0,
        }
    }
}

impl CheckpointBatch {
    fn read(
        &mut self,
        rows: &PageSet<'_>,
        start: u32,
        replace: bool,
        consume: bool,
    ) -> Option<u32> {
        self.len = 0;
        self.visited = 0;
        let mut pages = rows.iter_from(start);
        for (page, row) in pages.by_ref().take(CHECKPOINT_BATCH) {
            self.visited += 1;
            let state = if consume {
                row.state.fetch_and(!DIRTY, Ordering::Relaxed)
            } else {
                row.state.load(Ordering::Relaxed)
            };
            if replace || state & DIRTY != 0 {
                if let Some(age) = row.age() {
                    self.records[self.len] = (page, age);
                    self.len += 1;
                }
            }
        }
        pages.next().map(|(page, _)| page)
    }
}

pub(crate) struct PageSnapshot {
    pub block: BlockKey,
    pub snapshot: crate::page_access_store::AccessSnapshot,
    pub resident: bool,
    pub replace: bool,
    rollback: Option<CheckpointRetry>,
}
impl PageSnapshot {
    /// An earlier slot and a later reused slot may both have sampled the
    /// same logical block. Keep the last sample, including empty replacements.
    pub fn keep_latest(samples: &mut Vec<Self>) {
        let mut seen = HashSet::new();
        samples.reverse();
        samples.retain(|sample| seen.insert(sample.block.clone()));
        samples.reverse();
    }

    pub fn commit(mut self) {
        self.rollback = None;
    }
}

struct CheckpointRetry {
    index: Arc<crate::BlockIndex>,
    slot: usize,
    oldest: u64,
}

impl Drop for PageSnapshot {
    fn drop(&mut self) {
        let Some(retry) = self.rollback.take() else {
            return;
        };
        // A removed identity has its own tombstone. Retry a live identity's
        // entire block, using its current ages even if it was recreated.
        retry.index.with_page_slot(retry.slot, false, |id, _| {
            if id == &self.block {
                retry
                    .index
                    .page_work
                    .mark(id, retry.slot, retry.oldest, true);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use talon_core::{Backend, ObjectId, Version};

    fn id() -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "bucket", "key"),
            0,
            131_072,
            Version::new("v1"),
        )
    }

    fn sample(life: &PageLifecycle, id: &BlockId, membership: u64, full: bool) -> PageSnapshot {
        let slot = life.recovery_slots()[disk_shard(id)][0];
        let clock = AccessClock::new();
        clock.set(100);
        life.checkpoint_snapshot(disk_shard(id), slot, full, membership, &clock, &mut 0)
            .unwrap()
            .unwrap()
    }

    #[test]
    fn scan_metrics_accumulate_until_the_round_completes() {
        let mut cursor = ScanCursor::default();
        assert_eq!(cursor.record_batch(1.0, 7, false), None);
        assert_eq!(cursor.record_batch(2.0, 0, true), Some((3.0, 7)));
        assert_eq!(cursor.record_batch(0.5, 0, true), Some((0.5, 0)));
    }

    #[test]
    fn packed_keys_preserve_on_disk_shards() {
        for offset in [0, 1, u64::MAX] {
            let mut id = id();
            id.offset = offset;
            assert_eq!(disk_shard(&id), disk_shard_key(&BlockKey::new(&id)));
        }
    }

    #[test]
    fn read_pin_and_delete_claim_have_one_winner() {
        let life = PageLifecycle::new();
        let id = id();
        let block = life.block(&id);
        for _ in 0..128 {
            block.register_fixture(PageIndex(0), Some(10), 10, true);
            let candidate = block.candidate(PageIndex(0)).unwrap();
            let barrier = Barrier::new(2);
            std::thread::scope(|scope| {
                let read = scope.spawn(|| {
                    barrier.wait();
                    let held = block.acquire(PageIndex(0));
                    barrier.wait();
                    held
                });
                barrier.wait();
                let closed = block.claim(&candidate, 100, Some(10));
                barrier.wait();
                let held = read.join().unwrap();
                assert_ne!(closed, held.is_some());
                drop(held);
                if !closed {
                    assert!(block.claim(&candidate, 100, Some(10)));
                }
                assert!(block.acquire(PageIndex(0)).is_none());
                assert!(life.index.clear_page(&id, PageIndex(0)));
                assert!(block.acquire(PageIndex(0)).is_none());
            });
        }
    }

    #[test]
    fn sparse_pin_survives_row_moves_and_does_not_touch_reused_slots() {
        let life = PageLifecycle::new();
        let id = id();
        let block = life.block(&id);
        let page = PageIndex(100_000);
        block.register_fixture(page, Some(10), 10, false);
        let stale = block.candidate(page).unwrap();
        let old_read = block.acquire(page).unwrap();
        for index in 0..700 {
            block.register_fixture(PageIndex(index), Some(10), 10, false);
        }
        assert!(!block.claim(&stale, 100, None));
        for index in 0..700 {
            assert!(life.index.clear_page(&id, PageIndex(index)));
        }
        assert!(!block.claim(&stale, 100, None));
        assert!(life.index.clear_page(&id, page));
        block.register_fixture(page, Some(20), 20, false);
        let reincarnated = block.acquire(page).unwrap();
        old_read.record_access_and_release(Some(99));
        assert_eq!(block.last_access(page.0), Some(20));
        assert!(block.claim_all_pages().is_none());
        life.index.remove(&id);
        block.register_fixture(page, Some(30), 30, false);
        reincarnated.record_access_and_release(Some(98));
        assert_eq!(block.last_access(page.0), Some(30));
        let fresh = block.candidate(page).unwrap();
        assert!(!block.claim(&stale, 100, None));
        assert!(block.claim(&fresh, 100, None));
    }

    #[test]
    fn same_millisecond_success_invalidates_a_candidate_without_dirtying_ages() {
        let life = PageLifecycle::new();
        let id = id();
        let block = life.block(&id);
        let page = PageIndex(0);
        block.register_fixture(page, Some(10), 10, true);
        sample(&life, &id, 0, true).commit();
        let stale = block.candidate(page).unwrap();
        block
            .acquire(page)
            .unwrap()
            .record_access_and_release(Some(10));
        assert!(!block.claim(&stale, 100, None));
        assert_eq!(life.dirty_summary(), (0, None));
        let current = block.candidate(page).unwrap();
        assert!(!block.claim(&stale, 100, None));
        drop(block.acquire(page).unwrap());
        assert!(block.claim(&current, 100, None));
    }

    #[test]
    fn checkpoint_recovers_access_completed_after_slot_selection() {
        use crate::page_access_store::PageAccessStore;

        for full in [true, false] {
            let life = PageLifecycle::new();
            let id = id();
            let block = life.block(&id);
            let shard = disk_shard(&id);
            let page = PageIndex(0);
            block.register_fixture(page, Some(10), 10, true);
            let clock = AccessClock::new();
            clock.set(100);
            let (_, generation, _) = life.checkpoint_start(shard);
            sample(&life, &id, generation, true).commit();
            block
                .acquire(page)
                .unwrap()
                .record_access_and_release(Some(clock.now()));
            let (ceiling, membership, _) = life.checkpoint_start(shard);
            let slot = if full {
                CheckpointInventory::gather(&life, shard, 1).take_shard(shard)[0] as usize
            } else {
                life.checkpoint_dirty_slots(shard, &mut 0, ceiling)[0]
            };

            // A reader completes after the checkpointer selects the slot but
            // before it acquires the row lock and consumes the dirty age.
            clock.set(101);
            block
                .acquire(page)
                .unwrap()
                .record_access_and_release(Some(clock.now()));
            let snapshot = life
                .checkpoint_snapshot(shard, slot, full, membership, &clock, &mut 0)
                .unwrap()
                .unwrap();
            assert_eq!(snapshot.replace, full);
            let bytes = PageAccessStore::encode(&id, 16, &snapshot.snapshot).unwrap();
            snapshot.commit();

            let restored = PageAccessStore::decode_bytes(&bytes, &id, 16, 102).unwrap();
            assert_eq!(restored.records.get(&page.0), Some(&101));
            assert_eq!(restored.future, 0);
        }
    }

    #[tokio::test]
    async fn full_checkpoint_replays_after_a_block_moves_into_a_later_slot() {
        use crate::page_access_shard::{self, JournalState};

        let life = PageLifecycle::new();
        let first = id();
        let shard = disk_shard(&first);
        let mut siblings = (1..).map(|offset| BlockId { offset, ..id() });
        let removed = siblings.find(|id| disk_shard(id) == shard).unwrap();
        let neighbor = siblings.find(|id| disk_shard(id) == shard).unwrap();
        let page = PageIndex(0);
        let block = life.block(&first);
        for (id, age) in [(&first, 10), (&removed, 11), (&neighbor, 12)] {
            life.block(id).register_fixture(page, Some(age), age, true);
        }
        let (_, membership, _) = life.checkpoint_start(shard);
        let slots = CheckpointInventory::gather(&life, shard, 1).take_shard(shard);
        assert_eq!(slots, &[0, 1, 2]);
        let clock = AccessClock::new();
        clock.set(100);
        let mut size = 24;
        let mut sample = |slot| {
            life.checkpoint_snapshot(shard, slot as usize, true, membership, &clock, &mut size)
                .unwrap()
                .unwrap()
        };
        let before = sample(slots[0]);
        life.index.remove(&first).unwrap();
        life.index.remove(&removed).unwrap();
        block.register_fixture(page, Some(30), 30, true);
        let after = sample(slots[1]);
        assert_eq!(after.block, before.block);
        let mut samples = vec![before, after, sample(slots[2])];
        let encode = |samples: &[PageSnapshot]| {
            page_access_shard::encode(
                16,
                samples
                    .iter()
                    .map(|sample| (sample.block.view(), &sample.snapshot)),
            )
            .unwrap()
        };
        let dir = tempfile::tempdir().unwrap();
        let replay = || page_access_shard::load(dir.path(), shard, 16, clock.now()).unwrap();
        // Reproduce the old traversal's output, including both incarnations.
        page_access_shard::publish(dir.path(), encode(&samples), None)
            .await
            .unwrap();
        assert!(replay().corrupt);

        PageSnapshot::keep_latest(&mut samples);
        let (journal, _) = page_access_shard::flush(
            dir.path(),
            JournalState::default(),
            true,
            encode(&samples),
            None,
        )
        .await
        .unwrap();
        life.set_checkpoint_journal(shard, journal);
        for sample in samples {
            sample.commit();
        }
        life.checkpoint_finished(shard, membership);
        let recovered = replay();
        assert!(!recovered.corrupt);
        assert_eq!(recovered.records.len(), 2);
        assert_eq!(recovered.records[&first].records[&0], 30);
        assert_eq!(recovered.records[&neighbor].records[&0], 12);
        assert!(
            life.checkpoint_start(shard).2,
            "late changes must stay dirty"
        );
    }

    #[test]
    fn full_checkpoint_round_visits_the_directory_once() {
        const SLOT_COUNT: usize = 16_384;
        let life = PageLifecycle::new();
        for offset in 0..SLOT_COUNT {
            let id = BlockId {
                offset: offset as u64,
                block_size: 1,
                ..id()
            };
            life.block(&id)
                .register_fixture(PageIndex(0), Some(10), 10, true);
        }
        assert_eq!(life.checkpoint_inventory_work(), (0, 0));
        let mut inventory = CheckpointInventory::gather(&life, 0, SHARDS);
        let mut slots = BTreeSet::new();
        for shard in 0..SHARDS {
            for slot in inventory.take_shard(shard) {
                let block = life.recovery_block(slot as usize).unwrap();
                assert_eq!(disk_shard_key(&block.id), shard);
                assert!(slots.insert(slot));
            }
        }
        assert_eq!(slots.len(), SLOT_COUNT);
        let (visits, locks) = life.checkpoint_inventory_work();
        assert_eq!(locks, SLOT_COUNT / CHECKPOINT_BATCH);
        assert_eq!(visits, SLOT_COUNT);
        eprintln!(
            "full-round directory visits: old={}, current={visits}",
            SLOT_COUNT * SHARDS
        );
    }

    #[test]
    fn catalog_delta_sampling_allows_independent_reads_between_batches() {
        use crate::catalog::{Operation, StoreKind};

        const PAGES: u32 = 65_536;
        const PAGE_SIZE: u32 = 16;
        let mut id = id();
        id.block_size = PAGES * PAGE_SIZE;
        let neighbor = BlockId {
            offset: 1,
            ..id.clone()
        };
        let directory = Arc::new(RwLock::new(crate::index::Directory::default()));
        directory.write().unwrap().attach(PAGE_SIZE);
        let life = PageLifecycle::with_index(
            Arc::new(crate::BlockIndex::from_catalog(directory.clone())),
            true,
        );
        let block = life.block(&id);
        let cold = life.block(&neighbor);
        let shard = disk_shard(&id);
        {
            let mut directory = directory.write().unwrap();
            for (id, count) in [(&id, PAGES), (&neighbor, 1)] {
                let len = u64::from(id.block_size);
                for page in 0..count {
                    directory.apply(
                        id,
                        StoreKind::Paged,
                        &Operation::Put { page, len },
                        None,
                        Some(512),
                    );
                }
            }
        }
        let (ceiling, membership, _) = life.checkpoint_start(shard);
        let slot = life.checkpoint_dirty_slots(shard, &mut 0, ceiling)[0];
        let clock = AccessClock::new();
        clock.set(10);
        let sample =
            |full| life.checkpoint_snapshot(shard, slot, full, membership, &clock, &mut 24);
        sample(true).unwrap().unwrap().commit();
        block
            .acquire(PageIndex(0))
            .unwrap()
            .record_access_and_release(Some(10));

        let mut boundaries = 0;
        let delta = std::thread::scope(|scope| {
            let (start, wait) = std::sync::mpsc::channel();
            let (done, completed) = std::sync::mpsc::channel();
            scope.spawn(move || {
                wait.recv().unwrap();
                cold.acquire(PageIndex(0))
                    .unwrap()
                    .record_access_and_release(Some(11));
                done.send(()).unwrap();
            });
            life.sample_checkpoint((shard, membership), slot, false, &clock, &mut 24, || {
                boundaries += 1;
                if boundaries == 1 {
                    start.send(()).unwrap();
                    completed
                        .recv_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
                    clock.set(12);
                    block
                        .acquire(PageIndex(PAGES - 1))
                        .unwrap()
                        .record_access_and_release(Some(12));
                    clock.set(13);
                    block
                        .acquire(PageIndex(0))
                        .unwrap()
                        .record_access_and_release(Some(13));
                }
            })
            .unwrap()
            .unwrap()
        });
        assert_eq!(boundaries, PAGES as usize / CHECKPOINT_BATCH - 1);
        assert!(!delta.replace);
        assert_eq!(delta.snapshot.records, vec![(0, 10), (PAGES - 1, 12)]);
        assert_eq!(delta.snapshot.sampled_at, 13);
        delta.commit();
        let next = sample(false).unwrap().unwrap();
        assert_eq!(next.snapshot.records, vec![(0, 13)]);
        next.commit();
        eprintln!("{PAGES}-page delta: reader completed at first of {boundaries} lock boundaries");
    }

    #[test]
    fn checkpoint_never_combines_pages_from_reused_slot_incarnations() {
        let life = PageLifecycle::new();
        let id = id();
        let block = life.block(&id);
        let shard = disk_shard(&id);
        let last = PageIndex(CHECKPOINT_BATCH as u32);
        for page in 0..=last.0 {
            block.register_fixture(PageIndex(page), Some(10), 10, true);
        }
        let (ceiling, membership, _) = life.checkpoint_start(shard);
        let slot = life.checkpoint_dirty_slots(shard, &mut 0, ceiling)[0];
        let clock = AccessClock::new();
        clock.set(100);
        let sampled = life
            .sample_checkpoint((shard, membership), slot, true, &clock, &mut 24, || {
                life.index.remove(&id).unwrap();
                block.register_fixture(last, Some(30), 30, true);
            })
            .unwrap();
        assert!(sampled.is_none());
        let (_, next, dirty) = life.checkpoint_start(shard);
        assert!(dirty);
        let replacement = sample(&life, &id, next, false);
        assert!(replacement.replace);
        assert_eq!(replacement.snapshot.records, vec![(last.0, 30)]);
        replacement.commit();
    }

    #[test]
    fn checkpoint_racing_read_and_fill_keeps_the_newest_ages() {
        for _ in 0..64 {
            let life = PageLifecycle::new();
            let id = id();
            let block = life.block(&id);
            let first = PageIndex(0);
            let second = PageIndex(100_000);
            let shard = disk_shard(&id);
            block.register_fixture(first, Some(10), 10, true);
            let (_, generation, _) = life.checkpoint_start(shard);
            sample(&life, &id, generation, true).commit();
            block
                .acquire(first)
                .unwrap()
                .record_access_and_release(Some(20));
            let reader = block.acquire(first).unwrap();
            let barrier = Barrier::new(2);
            std::thread::scope(|scope| {
                let update = scope.spawn(|| {
                    barrier.wait();
                    block.register_fixture(second, Some(40), 40, true);
                    reader.record_access_and_release(Some(30));
                });
                barrier.wait();
                let snapshot = sample(&life, &id, generation, false);
                let records = snapshot.snapshot.records.clone();
                snapshot.commit();
                update.join().unwrap();
                let mut persisted: BTreeMap<_, _> = records.into_iter().collect();
                if life.checkpoint_start(shard).2 {
                    let retry = sample(&life, &id, generation, false);
                    persisted.extend(retry.snapshot.records.iter().copied());
                    retry.commit();
                }
                assert_eq!(persisted.get(&first.0), Some(&30));
                assert_eq!(persisted.get(&second.0), Some(&40));
            });
        }
    }

    #[test]
    fn failed_snapshot_retries_sampled_ages_without_regressing_a_new_touch() {
        let life = PageLifecycle::new();
        let id = id();
        let block = life.block(&id);
        let shard = disk_shard(&id);
        block.register_fixture(PageIndex(0), Some(10), 10, true);
        let (_, generation, _) = life.checkpoint_start(shard);
        sample(&life, &id, generation, true).commit();
        block
            .acquire(PageIndex(0))
            .unwrap()
            .record_access_and_release(Some(20));
        let failed = sample(&life, &id, generation, false);
        assert!(!failed.replace);
        assert_eq!(failed.snapshot.records, vec![(0, 20)]);
        block
            .acquire(PageIndex(0))
            .unwrap()
            .record_access_and_release(Some(30));
        drop(failed);
        assert_eq!(life.dirty_summary(), (1, Some(20)));
        let retry = sample(&life, &id, generation, false);
        assert!(retry.replace);
        assert_eq!(retry.snapshot.records, vec![(0, 30)]);
        retry.commit();
        block
            .acquire(PageIndex(0))
            .unwrap()
            .record_access_and_release(Some(25));
        block
            .acquire(PageIndex(0))
            .unwrap()
            .record_access_and_release(Some(30));
        assert_eq!(life.dirty_summary(), (0, None));

        block
            .acquire(PageIndex(0))
            .unwrap()
            .record_access_and_release(Some(40));
        let slot = life.recovery_slots()[shard][0];
        let mut size = crate::page_access_shard::MAX_BYTES;
        assert!(life
            .checkpoint_snapshot(
                shard,
                slot,
                false,
                generation,
                &AccessClock::new(),
                &mut size
            )
            .is_err());
        assert_eq!(life.dirty_summary(), (1, Some(40)));
        let retry = sample(&life, &id, generation, false);
        assert!(retry.replace);
        assert_eq!(retry.snapshot.records, vec![(0, 40)]);
        retry.commit();
    }

    #[test]
    fn late_tombstone_does_not_erase_a_recreated_blocks_checkpoint() {
        let life = PageLifecycle::new();
        let id = id();
        let shard = disk_shard(&id);
        let block = life.block(&id);
        let page = PageIndex(0);
        block.register_fixture(page, Some(10), 10, true);
        let (_, old_membership, _) = life.checkpoint_start(shard);
        sample(&life, &id, old_membership, true).commit();
        life.set_checkpoint_journal(
            shard,
            crate::page_access_shard::JournalState {
                generation: 1,
                full: false,
                ..Default::default()
            },
        );
        life.index.remove(&id);
        block.register_fixture(page, Some(30), 30, true);
        let concurrent = sample(&life, &id, old_membership, false);
        assert_eq!(concurrent.snapshot.records, vec![(0, 30)]);
        concurrent.commit();
        life.checkpoint_finished(shard, old_membership);
        let (_, membership, dirty) = life.checkpoint_start(shard);
        assert!(dirty);
        assert_eq!(
            life.checkpoint_journal(shard, membership).1,
            vec![id.clone()]
        );
        let latest = sample(&life, &id, membership, false);
        assert!(latest.replace);
        assert_eq!(latest.snapshot.records, vec![(0, 30)]);
        latest.commit();
        life.checkpoint_finished(shard, membership);
        assert!(!life.checkpoint_start(shard).2);
    }

    #[test]
    fn failed_empty_snapshot_and_detach_leave_a_tombstone() {
        let life = PageLifecycle::new();
        let id = id();
        let shard = disk_shard(&id);
        let block = life.block(&id);
        let page = PageIndex(0);
        block.register_fixture(page, Some(10), 10, true);
        let (_, membership, _) = life.checkpoint_start(shard);
        sample(&life, &id, membership, true).commit();
        life.set_checkpoint_journal(
            shard,
            crate::page_access_shard::JournalState {
                generation: 1,
                full: false,
                ..Default::default()
            },
        );
        life.index.clear_page(&id, page);
        let empty = sample(&life, &id, membership, false);
        assert!(empty.replace);
        assert!(!empty.resident);
        life.index.remove(&id);
        drop(empty);
        let (_, next, dirty) = life.checkpoint_start(shard);
        assert!(dirty);
        assert_eq!(life.checkpoint_journal(shard, next).1, vec![id]);
    }

    #[test]
    fn first_checkpoint_removal_and_bounded_tombstones_preserve_later_changes() {
        let life = PageLifecycle::new();
        let id = id();
        let shard = disk_shard(&id);
        let block = life.block(&id);
        let page = PageIndex(0);
        block.register_fixture(page, None, 1, false);
        let (_, old, _) = life.checkpoint_start(shard);
        let in_flight = sample(&life, &id, old, true);
        life.index.remove(&id);
        life.set_checkpoint_journal(
            shard,
            crate::page_access_shard::JournalState {
                generation: 1,
                full: false,
                ..Default::default()
            },
        );
        in_flight.commit();
        life.checkpoint_finished(shard, old);
        let (_, next, dirty) = life.checkpoint_start(shard);
        assert!(dirty);
        assert!(life.checkpoint_journal(shard, next).0.full);
        for _ in 0..9000 {
            block.register_fixture(page, None, 1, false);
            life.index.remove(&id);
        }
        let (_, flushed, _) = life.checkpoint_start(shard);
        assert!(life.checkpoint_journal(shard, flushed).0.full);
        assert!(
            life.index
                .page_work
                .shard(shard)
                .lock()
                .unwrap()
                .removed_bytes
                <= 1 << 20
        );
        life.checkpoint_finished(shard, flushed);
        block.register_fixture(page, None, 1, false);
        life.index.remove(&id);
        life.checkpoint_finished(shard, flushed);
        let (_, latest, dirty) = life.checkpoint_start(shard);
        assert!(dirty);
        let (journal, removed) = life.checkpoint_journal(shard, latest);
        assert!(!journal.full);
        assert_eq!(removed, vec![id]);
    }

    #[test]
    fn untracked_pages_scan_only_retries_and_pending_cleanup() {
        let life = PageLifecycle::with_index(Arc::new(crate::BlockIndex::new()), false);
        let id = id();
        let block = life.block(&id);
        for page in (0..10_000).map(PageIndex) {
            block.register_fixture(page, Some(10), 10, false);
        }
        assert!(block.hold_rows().read().unwrap().ages.is_empty());
        let (_, clean) = life.scan(&mut ScanCursor::default(), 10, 100, None, 10);
        assert!(clean.completed);
        assert_eq!(clean.checked, 0);
        let page = PageIndex(9000);
        let candidate = block.candidate(page).unwrap();
        assert!(block.claim(&candidate, 100, None));
        block.abort(&candidate, 1);
        let (batch, report) = life.scan(&mut ScanCursor::default(), 10, 100, None, 10);
        assert_eq!(report.checked, 1);
        assert_eq!(batch[0].reason, 1);
        block
            .acquire(page)
            .unwrap()
            .record_access_and_release(Some(30));
        let (batch, clean) = life.scan(&mut ScanCursor::default(), 10, 100, None, 10);
        assert!(batch.is_empty());
        assert_eq!(clean.checked, 0);
        for page in (0..10_000).map(PageIndex) {
            assert!(life.index.clear_page(&id, page));
        }
        let (_, cleanup) = life.scan(&mut ScanCursor::default(), 10, 100, None, 10);
        assert_eq!(cleanup.empty.len(), 1);
        life.index.remove(&id);
        let (_, clean) = life.scan(&mut ScanCursor::default(), 10, 100, None, 10);
        assert!(clean.empty.is_empty());
        assert_eq!(life.dirty_summary(), (0, None));
    }

    #[test]
    fn saturated_reader_count_never_produces_an_uncounted_guard() {
        let life = PageLifecycle::new();
        let block = life.block(&id());
        let page = PageIndex(0);
        block.register_fixture(page, None, 0, false);
        let mut readers: Vec<_> = (0..u16::MAX)
            .map(|_| block.acquire(page).unwrap())
            .collect();
        assert!(block.acquire(page).is_none());
        assert!(block.candidate(page).is_none());
        drop(readers.pop());
        readers.push(block.acquire(page).unwrap());
        drop(readers);
        assert!(block.candidate(page).is_some());
    }

    #[test]
    fn retry_scan_continues_past_a_full_batch_of_pinned_pages() {
        let life = PageLifecycle::with_index(Arc::new(crate::BlockIndex::new()), false);
        let block = life.block(&id());
        for page in (0..65).map(PageIndex) {
            block.register_fixture(page, None, 0, false);
            let candidate = block.candidate(page).unwrap();
            assert!(block.claim(&candidate, 0, None));
            block.abort(&candidate, 1);
        }
        let _readers: Vec<_> = (0..64)
            .map(|page| block.acquire(PageIndex(page)).unwrap())
            .collect();
        let mut cursor = ScanCursor::default();
        let (first, report) = life.scan(&mut cursor, 64, 0, None, 64);
        assert!(first.is_empty());
        assert_eq!(report.checked, 64);
        let (second, report) = life.scan(&mut cursor, 64, 0, None, 64);
        assert_eq!(report.checked, 1);
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].page, PageIndex(64));
        assert!(report.completed);
    }
}
