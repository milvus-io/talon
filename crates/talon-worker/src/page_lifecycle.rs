//! Page residency and idle age. Disk operations never run under these locks.
use crate::block_key::{BlockIdentity, BlockKey};
use crate::catalog::Geometry;
use std::collections::{BTreeMap, BTreeSet, HashMap};
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

// Only the flags needed by scans live in the resident row. Full-width
// generations and pin counts live in the active operation table.
const PINNED: u32 = 1;
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

    pub(crate) fn forget_page(&mut self, page: u32) -> bool {
        self.ages.remove(&page);
        let row = self.rows.get_mut(&page).expect("resident page");
        std::mem::take(row.state.0.get_mut()) & RETRY != 0
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

    pub(crate) fn retry_pending(self) -> bool {
        self.state.load(Ordering::Relaxed) & RETRY != 0
    }
}

pub(crate) enum PageSet<'a> {
    Catalog(&'a Geometry, Option<&'a PageAge>),
    Legacy(&'a PageRows),
}

impl PageSet<'_> {
    fn get(&self, page: u32) -> Option<PageRow<'_>> {
        match self {
            Self::Catalog(geometry, age) => geometry.page_row(page, *age),
            Self::Legacy(rows) => {
                let row = rows.get(&page)?;
                row.unit.resident().then_some(PageRow {
                    state: &row.state,
                    age: rows.ages.get(&page),
                })
            }
        }
    }

    fn count(&self) -> usize {
        match self {
            Self::Catalog(geometry, _) => geometry.page_count(),
            Self::Legacy(rows) => rows.len(),
        }
    }

    fn iter_from(&self, start: u32) -> impl Iterator<Item = (u32, PageRow<'_>)> {
        let (catalog, legacy) = match self {
            Self::Catalog(geometry, age) => (Some(geometry.page_rows_from(start, *age)), None),
            Self::Legacy(rows) => (
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

pub(crate) struct PageReadGuard {
    index: Arc<crate::BlockIndex>,
    slot: usize,
    page: u32,
    identity: u64,
    accessed_at: Option<u64>,
    successful: bool,
}

impl PageReadGuard {
    pub(crate) fn acquire(
        index: &Arc<crate::BlockIndex>,
        slot: usize,
        page: u32,
        entry: &PageWord,
    ) -> Option<Self> {
        let work = &index.page_work;
        let mut active = work.operations(slot).lock().unwrap();
        if entry.load(Ordering::Acquire) & CLOSED != 0 {
            return None;
        }
        let cohort = active.entry((slot, page)).or_insert_with(|| ActivePage {
            identity: work.next_identity(),
            ..Default::default()
        });
        let Some(readers) = cohort.readers.checked_add(1) else {
            // Fall back to a miss without handing out an uncounted pin.
            static REPORTED: AtomicBool = AtomicBool::new(false);
            if !REPORTED.swap(true, Ordering::Relaxed) {
                tracing::warn!(page, "resident page reader limit reached");
            }
            return None;
        };
        cohort.readers = readers;
        entry.fetch_or(PINNED, Ordering::Release);
        Some(Self {
            index: index.clone(),
            slot,
            page,
            identity: cohort.identity,
            accessed_at: None,
            successful: false,
        })
    }

    pub fn record_access_and_release(mut self, now: Option<u64>) {
        self.accessed_at = now;
        self.successful = true;
    }

    #[cfg(test)]
    pub fn record_access(&self, now: u64) {
        self.update(Some(now), true, false);
    }

    fn update(&self, now: Option<u64>, successful: bool, release: bool) {
        self.index.with_page_slot(self.slot, false, |id, rows| {
            let Some(row) = rows.get(self.page) else {
                return;
            };
            let work = &self.index.page_work;
            let mut active = work.operations(self.slot).lock().unwrap();
            let address = (self.slot, self.page);
            let Some(cohort) = active
                .get_mut(&address)
                .filter(|op| op.identity == self.identity)
            else {
                return;
            };
            let changed_age = now.is_some_and(|now| row.touch(now));
            if release {
                cohort.readers -= 1;
            }
            let unpinned = cohort.readers == 0;
            let flags = row
                .state
                .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |mut flags| {
                    if successful {
                        flags = (flags & !RETRY) | ACCESSED;
                    }
                    if unpinned {
                        flags &= !PINNED;
                    }
                    Some(flags)
                })
                .unwrap();
            if unpinned && !cohort.selected {
                active.remove(&address);
            }
            drop(active);
            if changed_age {
                work.mark(id, self.slot, now.unwrap(), false);
            }
            if successful && flags & RETRY != 0 {
                work.cancel_retry(self.slot, self.page);
            }
        });
    }
}

impl Drop for PageReadGuard {
    fn drop(&mut self) {
        self.update(self.accessed_at, self.successful, true);
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
            PageReadGuard::acquire(&self.index, slot, page.0, rows.get(page.0)?.state)
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
        state
            .compare_exchange(
                flags,
                flags & !ACCESSED,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .ok()?;
        let identity = work.next_identity();
        active.insert((slot, page), ActivePage::selected(identity));
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
                let active = work.operations(slot).lock().unwrap();
                let flags = row.state.load(Ordering::Acquire);
                if !c.lease.current(&active, slot)
                    || flags & (CLOSED | PINNED | ACCESSED) != 0
                    || tti.is_some_and(|tti| !expired(row.age(), now, tti))
                {
                    return false;
                }
                row.state
                    .compare_exchange(flags, flags | CLOSED, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
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
            let active = work.operations(slot).lock().unwrap();
            let old = row.state.load(Ordering::Relaxed);
            if c.lease.current(&active, slot) && old & CLOSED != 0 {
                row.state
                    .fetch_update(Ordering::Release, Ordering::Relaxed, |flags| {
                        Some((flags & !(CLOSED | RETRY)) | ((reason as u32 + 1) << RETRY_SHIFT))
                    })
                    .unwrap();
                drop(active);
                work.schedule_retry(slot, c.page.0);
            }
        });
    }

    pub fn claim_all_pages(&self) -> Option<BlockPageClaim> {
        let mut claim = BlockPageClaim {
            block: self.clone(),
            identity: 0,
            len: 0,
            committed: false,
        };
        let valid = self.index.with_pages(&self.id, true, |slot, rows| {
            let work = &self.index.page_work;
            let mut active = work.operations(slot).lock().unwrap();
            if rows
                .iter_from(0)
                .any(|(_, row)| row.state.load(Ordering::Relaxed) & (CLOSED | PINNED) != 0)
            {
                return false;
            }
            claim.identity = work.next_identity();
            for (page, row) in rows.iter_from(0) {
                row.state.fetch_or(CLOSED, Ordering::Release);
                active.insert((slot, page), ActivePage::selected(claim.identity));
                claim.len += 1;
            }
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
    block: PagedMutation,
    identity: u64,
    len: usize,
    committed: bool,
}
impl BlockPageClaim {
    pub fn len(&self) -> usize {
        self.len
    }

    /// The caller already removed the exact claimed residency from the index.
    pub fn finish(mut self) {
        self.committed = true;
    }
}
impl Drop for BlockPageClaim {
    fn drop(&mut self) {
        if !self.committed && self.len != 0 {
            self.block
                .index
                .with_pages(&self.block.id, true, |slot, rows| {
                    let mut active = self.block.index.page_work.operations(slot).lock().unwrap();
                    for (page, row) in rows.iter_from(0) {
                        let address = (slot, page);
                        if active
                            .get(&address)
                            .is_some_and(|op| op.identity == self.identity)
                        {
                            row.state.fetch_and(!CLOSED, Ordering::Release);
                            active.remove(&address);
                        }
                    }
                });
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
type ActivePages = BTreeMap<(usize, u32), ActivePage>;

#[derive(Default)]
struct ActivePage {
    identity: u64,
    readers: u16,
    selected: bool,
}

impl ActivePage {
    fn selected(identity: u64) -> Self {
        Self {
            identity,
            selected: true,
            readers: 0,
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
        self.slot == slot
            && active
                .get(&(slot, self.page))
                .is_some_and(|op| op.identity == self.identity)
    }
}

impl Drop for PageSelection {
    fn drop(&mut self) {
        let mut active = self.work.operations(self.slot).lock().unwrap();
        if self.current(&active, self.slot) {
            let address = (self.slot, self.page);
            let op = active.get_mut(&address).unwrap();
            op.selected = false;
            if op.readers == 0 {
                active.remove(&address);
            }
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
            next_identity: AtomicU64::new(0),
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

    pub(crate) fn invalidate_page(&self, slot: usize, page: u32) {
        if let Some(shards) = self.active.get() {
            shards[slot % OPERATION_SHARDS]
                .lock()
                .unwrap()
                .remove(&(slot, page));
        }
    }

    fn invalidate_block(&self, slot: usize) {
        if let Some(shards) = self.active.get() {
            let mut active = shards[slot % OPERATION_SHARDS].lock().unwrap();
            while let Some((&address, _)) = active.range((slot, 0)..=(slot, u32::MAX)).next() {
                active.remove(&address);
            }
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
        retry: bool,
        attached: &AtomicBool,
    ) {
        self.invalidate_page(slot, page);
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
            shard.lock().unwrap().clear();
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
            let slot = if tti.is_some() {
                (cursor.slot < ceiling).then_some(cursor.slot)
            } else if cursor.slot < ceiling {
                self.index
                    .page_work
                    .pending
                    .lock()
                    .unwrap()
                    .range(cursor.slot..ceiling)
                    .next()
                    .map(|(&slot, _)| slot)
            } else {
                None
            };
            let Some(slot) = slot else {
                cursor.slot = 0;
                cursor.page = 0;
                cursor.ceiling = None;
                report.completed = true;
                break;
            };
            if slot != cursor.slot {
                cursor.slot = slot;
                cursor.page = 0;
            }
            let mut examined = 0;
            let budget = (limit - work).min(64);
            let more = self
                .index
                .with_page_slot(slot, false, |id, rows| {
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
                })
                .unwrap_or(false);
            work += examined.max(1);
            if !more {
                cursor.slot += 1;
                cursor.page = 0;
            }
        }
        (out, report)
    }

    pub fn checkpoint_gate(&self, shard: usize) -> Arc<tokio::sync::Mutex<()>> {
        self.checkpoints[shard].clone()
    }

    pub fn checkpoint_start(&self, shard: usize) -> (usize, u64, bool) {
        let ceiling = self.index.policy_scan_ceiling();
        let work = self.index.page_work.shard(shard).lock().unwrap();
        (
            ceiling,
            work.membership,
            work.membership != work.checkpointed_membership || !work.dirty.is_empty(),
        )
    }

    /// Startup groups compact slab indices once, so restoring many shards does
    /// not rescan the directory 256 times or retain a second set of block keys.
    pub fn recovery_slots(&self) -> [Vec<usize>; SHARDS] {
        let mut shards = std::array::from_fn(|_| Vec::new());
        self.index
            .visit_paged_slots(|slot, id| shards[disk_shard_key(id)].push(slot));
        shards
    }

    pub fn recovery_block(&self, slot: usize) -> Option<PagedMutation> {
        self.index
            .with_page_slot(slot, false, |id, _| self.mutation(id.clone()))
    }

    /// Full checkpoints statically bound the number of slab slots examined per
    /// call. Deltas jump directly between dirty group bits.
    pub fn checkpoint_slots(
        &self,
        shard: usize,
        cursor: &mut usize,
        ceiling: usize,
        full: bool,
    ) -> Vec<usize> {
        if full {
            let end = (*cursor + 64).min(ceiling);
            let slots = self
                .index
                .paged_slots_in(*cursor..end, |id| disk_shard_key(id) == shard);
            *cursor = end;
            return slots;
        }
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
        now: u64,
        size: &mut usize,
    ) -> anyhow::Result<Option<PageSnapshot>> {
        self.index
            .with_page_slot(slot, true, |id, rows| {
                if disk_shard_key(id) != shard {
                    return Ok(None);
                }
                let mut work = self.index.page_work.shard(shard).lock().unwrap();
                let dirty = work.get(slot);
                if !full && dirty.is_none() {
                    return Ok(None);
                }
                *size += serde_json::to_vec(id)?.len() + rows.count() * 12 + 52;
                anyhow::ensure!(
                    *size <= crate::page_access_shard::MAX_BYTES,
                    "access shard exceeds 64 MiB limit"
                );
                // A later tombstone must be followed by the replacement of any
                // reincarnated block. Leave dirty notifications scheduled when
                // the deletion wasn't included in this checkpoint's tombstones.
                let consume = work.membership == membership;
                let touched = if consume { work.take(slot) } else { None };
                drop(work);
                let replace = full || dirty.is_some_and(|(_, replace)| replace);
                let resident = rows.iter_from(0).next().is_some();
                let records = rows
                    .iter_from(0)
                    .filter_map(|(page, row)| {
                        let state = if consume {
                            row.state.fetch_and(!DIRTY, Ordering::Relaxed)
                        } else {
                            row.state.load(Ordering::Relaxed)
                        };
                        if replace || state & DIRTY != 0 {
                            return row.age().map(|age| (page, age));
                        }
                        None
                    })
                    .collect();
                Ok(Some(PageSnapshot {
                    block: id.clone(),
                    resident,
                    replace,
                    snapshot: crate::page_access_store::AccessSnapshot {
                        revision: 0,
                        sampled_at: now,
                        records,
                    },
                    rollback: touched.map(|(oldest, _)| CheckpointRetry {
                        index: self.index.clone(),
                        slot,
                        oldest,
                    }),
                }))
            })
            .transpose()
            .map(Option::flatten)
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

pub(crate) struct PageSnapshot {
    pub block: BlockKey,
    pub snapshot: crate::page_access_store::AccessSnapshot,
    pub resident: bool,
    pub replace: bool,
    rollback: Option<CheckpointRetry>,
}
impl PageSnapshot {
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
        life.checkpoint_snapshot(disk_shard(id), slot, full, membership, 100, &mut 0)
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
