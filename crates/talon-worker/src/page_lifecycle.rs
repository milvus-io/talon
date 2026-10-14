//! Page residency and idle age. Disk operations never run under these locks.
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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

/// Mutations in different blocks share the directory stripe in read mode, so
/// only orphan cleanup serializes them. Physical directory names suffice for
/// cleanup even if block.meta is missing or corrupt.
pub(crate) struct PageMutationGate {
    directory: Arc<tokio::sync::RwLock<()>>,
    block: tokio::sync::RwLock<()>,
}
pub(crate) struct PageMutationGuard<'a> {
    _directory: tokio::sync::OwnedRwLockReadGuard<()>,
    _block: futures::future::Either<
        tokio::sync::RwLockReadGuard<'a, ()>,
        tokio::sync::RwLockWriteGuard<'a, ()>,
    >,
}
impl PageMutationGate {
    /// Only for fills owning disjoint page-level InFlightGuards. GC, whole-block
    /// promotion and ordinary commits continue to take the exclusive block lock.
    pub async fn lock_page_fill(&self) -> PageMutationGuard<'_> {
        let directory = self.directory.clone().read_owned().await;
        let block = futures::future::Either::Left(self.block.read().await);
        PageMutationGuard {
            _directory: directory,
            _block: block,
        }
    }

    pub async fn lock(&self) -> PageMutationGuard<'_> {
        let directory = self.directory.clone().read_owned().await;
        let block = futures::future::Either::Right(self.block.write().await);
        PageMutationGuard {
            _directory: directory,
            _block: block,
        }
    }
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

// One word arbitrates readers and deletion. A successful read sets ACCESSED;
// selection consumes that bit under the block lock, never on the hot path.
const READERS: u64 = u32::MAX as u64;
const RETRY_SHIFT: u32 = 32;
const RETRY: u64 = 3 << RETRY_SHIFT;
const CLOSED: u64 = 1 << 62;
const ACCESSED: u64 = 1 << 63;
const UNKNOWN: u64 = u64::MAX;

#[derive(Default)]
struct ShardWork {
    dirty: Mutex<BTreeMap<u64, u64>>,
    pending: Mutex<BTreeSet<u64>>,
}

pub(crate) struct BlockWork {
    shard: Arc<ShardWork>,
    slot: u64,
    retired: AtomicBool,
    dirty_since: AtomicU64,
}
impl Default for BlockWork {
    fn default() -> Self {
        Self {
            shard: Arc::default(),
            slot: 0,
            retired: AtomicBool::new(false),
            dirty_since: AtomicU64::new(UNKNOWN),
        }
    }
}
impl BlockWork {
    fn dirty(&self, now: u64) {
        // Coalesce different pages of an already dirty block without taking the
        // shard lock. An RMW (not a load/skip) orders this publication against
        // checkpoint's reset before it samples pages. Older timestamps still
        // update the summary, notably when failed checkpoints restore dirtiness.
        if self.dirty_since.fetch_min(now, Ordering::Release) <= now {
            return;
        }
        let mut dirty = self.shard.dirty.lock().unwrap();
        if self.retired.load(Ordering::Relaxed) {
            return;
        }
        dirty
            .entry(self.slot)
            .and_modify(|old| *old = (*old).min(now))
            .or_insert(now);
    }
    fn pending(&self) {
        self.shard.pending.lock().unwrap().insert(self.slot);
    }
    fn retire(&self) {
        // Serialize retirement with late checkpoint rollback. A snapshot can
        // release its BlockState before its page/summary guards are dropped.
        let mut dirty = self.shard.dirty.lock().unwrap();
        self.retired.store(true, Ordering::Relaxed);
        dirty.remove(&self.slot);
        self.shard.pending.lock().unwrap().remove(&self.slot);
    }
    pub fn checkpoint(self: &Arc<Self>) -> BlockCheckpoint {
        let mut dirty = self.shard.dirty.lock().unwrap();
        let since = dirty.remove(&self.slot);
        self.dirty_since.swap(UNKNOWN, Ordering::AcqRel);
        // A publisher waiting for this lock can conservatively reinsert an old
        // summary. That may cause an extra checkpoint, but cannot lose a touch.
        drop(dirty);
        BlockCheckpoint {
            work: self.clone(),
            since,
        }
    }
}

/// Removing the summary before sampling pages lets concurrent changes register
/// a later checkpoint. Failure and cancellation restore the consumed summary.
pub(crate) struct BlockCheckpoint {
    work: Arc<BlockWork>,
    since: Option<u64>,
}
impl BlockCheckpoint {
    pub fn commit(mut self) {
        self.since = None;
    }
}
impl Drop for BlockCheckpoint {
    fn drop(&mut self) {
        if let Some(since) = self.since {
            self.work.dirty(since);
        }
    }
}

pub(crate) struct PageState {
    state: AtomicU64,
    last_access: AtomicU64,
    dirty_since: AtomicU64,
    work: Arc<BlockWork>,
}
impl PageState {
    #[cfg(test)]
    fn new(access: Option<u64>) -> Self {
        Self::with_work(access, Arc::default())
    }
    fn with_work(access: Option<u64>, work: Arc<BlockWork>) -> Self {
        Self {
            state: AtomicU64::new(0),
            last_access: AtomicU64::new(access.map_or(UNKNOWN, |t| t.min(UNKNOWN - 1))),
            dirty_since: AtomicU64::new(UNKNOWN),
            work,
        }
    }
    pub fn last_access(&self) -> Option<u64> {
        let t = self.last_access.load(Ordering::Acquire);
        (t != UNKNOWN).then_some(t)
    }
    #[cfg(test)]
    fn dirty_since(&self) -> Option<u64> {
        let t = self.dirty_since.load(Ordering::Acquire);
        (t != UNKNOWN).then_some(t)
    }
    fn touch(&self, now: u64) {
        let now = now.min(UNKNOWN - 1);
        // Out-of-order readers must not move access time backwards. Coalesce
        // same-millisecond hits without another timestamp or dirty-state write.
        let mut old = self.last_access.load(Ordering::Relaxed);
        while old == UNKNOWN || old < now {
            match self.last_access.compare_exchange_weak(
                old,
                now,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // Always a release RMW: a checkpoint consuming this marker
                    // must also see this timestamp. Only the first dirty touch
                    // per page/checkpoint notifies the coalesced block summary.
                    if self.dirty_since.fetch_min(now, Ordering::Release) == UNKNOWN {
                        self.work.dirty(now);
                    }
                    break;
                }
                Err(actual) => old = actual,
            }
        }
    }
    pub fn acquire(self: &Arc<Self>) -> Option<PageReadGuard> {
        let mut old = self.state.load(Ordering::Relaxed);
        loop {
            if old & CLOSED != 0 || old & READERS == READERS {
                return None;
            }
            match self.state.compare_exchange_weak(
                old,
                old + 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(PageReadGuard {
                        handle: self.clone(),
                        successful: false,
                    })
                }
                Err(actual) => old = actual,
            }
        }
    }
    fn release(&self, successful: bool) {
        if !successful {
            let old = self.state.fetch_sub(1, Ordering::Release);
            debug_assert_ne!(old & READERS, 0);
            return;
        }
        let mut old = self.state.load(Ordering::Relaxed);
        loop {
            debug_assert_ne!(old & READERS, 0);
            let next = ((old - 1) & !RETRY) | ACCESSED;
            match self
                .state
                .compare_exchange_weak(old, next, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => old = actual,
            }
        }
    }
    fn retry(state: u64) -> Option<usize> {
        let value = (state & RETRY) >> RETRY_SHIFT;
        (value != 0).then(|| value as usize - 1)
    }
}

pub(crate) struct PageEntry {
    pub handle: Arc<PageState>,
    // Only candidate selection changes this counter. It prevents one selection
    // from making an older candidate valid by consuming the ACCESSED bit again.
    selection: u64,
}
#[derive(Default)]
pub(crate) struct BlockPages {
    pub pages: BTreeMap<u32, PageEntry>,
    // Structural changes only: hot reads never lock or modify their block.
    pub revision: u64,
    pub cleanup_pending: bool,
    retries: BTreeSet<u32>,
    pub work: Arc<BlockWork>,
}
impl BlockPages {
    pub fn changed(&mut self, now: u64) {
        self.revision = self
            .revision
            .checked_add(1)
            .expect("access revision exhausted");
        self.work.dirty(now);
    }
    #[cfg(test)]
    pub fn dirty_since(&self) -> Option<u64> {
        self.work
            .shard
            .dirty
            .lock()
            .unwrap()
            .get(&self.work.slot)
            .copied()
    }
}
pub(crate) struct BlockState {
    pub id: BlockId,
    pub gate: Arc<PageMutationGate>,
    pub inner: Mutex<BlockPages>,
}
/// Holds every page closed until the owning block mutation either commits or
/// fails. A failed claim or unlink reopens all pages, without scheduling page GC.
pub(crate) struct BlockPageClaim {
    block: Arc<BlockState>,
    handles: Vec<Arc<PageState>>,
    committed: bool,
}
impl BlockPageClaim {
    pub fn len(&self) -> usize {
        self.handles.len()
    }

    pub fn finish(mut self, now: u64) {
        let mut g = self.block.inner.lock().unwrap();
        g.pages.clear();
        g.retries.clear();
        g.cleanup_pending = false;
        g.work.pending();
        g.changed(now);
        self.committed = true;
    }
}
impl Drop for BlockPageClaim {
    fn drop(&mut self) {
        if !self.committed {
            for handle in &self.handles {
                handle.state.fetch_and(!CLOSED, Ordering::Release);
            }
        }
    }
}

impl BlockState {
    /// Caller holds the block mutation gate and rechecks block activity after
    /// claiming. CAS with each reader counter prevents partial block eviction.
    pub fn claim_all_pages(self: &Arc<Self>) -> Option<BlockPageClaim> {
        let g = self.inner.lock().unwrap();
        let mut claim = BlockPageClaim {
            block: self.clone(),
            handles: Vec::with_capacity(g.pages.len()),
            committed: false,
        };
        for entry in g.pages.values() {
            let flags = entry.handle.state.load(Ordering::Acquire);
            if flags & (READERS | CLOSED) != 0
                || entry
                    .handle
                    .state
                    .compare_exchange(flags, flags | CLOSED, Ordering::AcqRel, Ordering::Relaxed)
                    .is_err()
            {
                return None;
            }
            claim.handles.push(entry.handle.clone());
        }
        Some(claim)
    }
    pub fn register(
        &self,
        page: PageIndex,
        access: Option<u64>,
        now: u64,
        dirty: bool,
    ) -> Arc<PageState> {
        let mut g = self.inner.lock().unwrap();
        g.cleanup_pending = false;
        let work = g.work.clone();
        // A duplicate same-version fill preserves existing guards and identity.
        let e = g.pages.entry(page.0).or_insert_with(|| PageEntry {
            handle: Arc::new(PageState::with_work(access, work)),
            selection: 0,
        });
        if dirty {
            if let Some(access) = access {
                e.handle.touch(access);
            }
            e.handle.state.fetch_or(ACCESSED, Ordering::Release);
        } else {
            // Startup recovery runs before publication to serving threads.
            e.handle.last_access.store(
                access.map_or(UNKNOWN, |t| t.min(UNKNOWN - 1)),
                Ordering::Relaxed,
            );
        }
        let handle = e.handle.clone();
        if dirty {
            g.changed(now);
        }
        handle
    }
    pub fn acquire(self: &Arc<Self>, page: PageIndex) -> Option<PageReadGuard> {
        self.inner
            .lock()
            .unwrap()
            .pages
            .get(&page.0)?
            .handle
            .acquire()
    }
    pub fn candidate(self: &Arc<Self>, page: PageIndex) -> Option<GcCandidate> {
        let mut g = self.inner.lock().unwrap();
        let e = g.pages.get_mut(&page.0)?;
        let flags = e.handle.state.load(Ordering::Acquire);
        if flags & CLOSED != 0 {
            return None;
        }
        self.select(page, e, 0, flags)
    }
    fn select(
        self: &Arc<Self>,
        page: PageIndex,
        entry: &mut PageEntry,
        reason: usize,
        flags: u64,
    ) -> Option<GcCandidate> {
        // A read can cancel an unlink retry without taking the block lock.
        // Consume ACCESSED only if the state used for selection is still valid;
        // otherwise a scan could revive a retry that the read just cancelled.
        entry
            .handle
            .state
            .compare_exchange(
                flags,
                flags & !ACCESSED,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .ok()?;
        entry.selection = entry
            .selection
            .checked_add(1)
            .expect("page selection exhausted");
        Some(GcCandidate {
            block: self.clone(),
            page,
            handle: entry.handle.clone(),
            selection: entry.selection,
            reason,
        })
    }
    /// Called under the mutation gate. The CAS is the read/delete arbitration
    /// point; timestamp validation follows successful exclusive ownership.
    pub fn claim(&self, c: &GcCandidate, now: u64, tti: Option<u64>) -> bool {
        let g = self.inner.lock().unwrap();
        let Some(e) = g.pages.get(&c.page.0) else {
            return false;
        };
        if !Arc::ptr_eq(&e.handle, &c.handle) || e.selection != c.selection {
            return false;
        }
        let state = e.handle.state.load(Ordering::Acquire);
        if state & (READERS | CLOSED | ACCESSED) != 0
            || e.handle
                .state
                .compare_exchange(state, state | CLOSED, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
        {
            return false;
        }
        if tti.is_some_and(|tti| !expired(e.handle.last_access(), now, tti)) {
            e.handle.state.fetch_and(!CLOSED, Ordering::Release);
            return false;
        }
        true
    }
    pub fn finish(&self, c: &GcCandidate, now: u64) {
        let mut g = self.inner.lock().unwrap();
        if g.pages.get(&c.page.0).is_some_and(|e| {
            Arc::ptr_eq(&e.handle, &c.handle)
                && e.selection == c.selection
                && e.handle.state.load(Ordering::Acquire) & CLOSED != 0
        }) {
            // Removed handles stay closed forever, even after the same page is
            // admitted again. Outstanding old handles cannot pin a new file.
            g.pages.remove(&c.page.0);
            g.retries.remove(&c.page.0);
            g.cleanup_pending = g.pages.is_empty();
            if g.cleanup_pending {
                g.work.pending();
            }
            g.changed(now);
        }
    }
    pub fn abort(&self, c: &GcCandidate, reason: usize) {
        let mut g = self.inner.lock().unwrap();
        if let Some(e) = g.pages.get(&c.page.0) {
            if Arc::ptr_eq(&e.handle, &c.handle) && e.selection == c.selection {
                e.handle
                    .state
                    .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |old| {
                        Some((old & !(CLOSED | RETRY)) | ((reason as u64 + 1) << RETRY_SHIFT))
                    })
                    .unwrap();
                g.retries.insert(c.page.0);
                g.work.pending();
            }
        }
    }
}

pub(crate) fn expired(last: Option<u64>, now: u64, tti: u64) -> bool {
    last.map_or(true, |last| now.saturating_sub(last) > tti)
}
pub(crate) struct PageReadGuard {
    handle: Arc<PageState>,
    successful: bool,
}
impl PageReadGuard {
    pub fn record_access_and_release(mut self, now: Option<u64>) {
        if let Some(now) = now {
            self.handle.touch(now);
        }
        self.successful = true;
    }
    #[cfg(test)]
    pub fn record_access(&self, now: u64) {
        self.handle.touch(now);
        self.handle
            .state
            .fetch_update(Ordering::Release, Ordering::Relaxed, |old| {
                Some((old & !RETRY) | ACCESSED)
            })
            .unwrap();
    }
}
impl Drop for PageReadGuard {
    fn drop(&mut self) {
        self.handle.release(self.successful);
    }
}

/// Consumes dirtiness before sampling. New touches mark a later checkpoint;
/// failed/cancelled snapshot construction restores the consumed marker on Drop.
pub(crate) struct PageCheckpoint {
    pub page: u32,
    pub access: Option<u64>,
    handle: Arc<PageState>,
    dirty_since: u64,
}
impl PageCheckpoint {
    pub fn new(page: u32, handle: &Arc<PageState>) -> Self {
        let dirty_since = handle.dirty_since.swap(UNKNOWN, Ordering::AcqRel);
        Self {
            page,
            access: handle.last_access(),
            handle: handle.clone(),
            dirty_since,
        }
    }
    #[cfg(test)]
    pub fn dirty(&self) -> bool {
        self.dirty_since != UNKNOWN
    }
    pub fn commit(mut self) {
        self.dirty_since = UNKNOWN;
    }
}
impl Drop for PageCheckpoint {
    fn drop(&mut self) {
        if self.dirty_since != UNKNOWN {
            self.handle
                .dirty_since
                .fetch_min(self.dirty_since, Ordering::Release);
            self.handle.work.dirty(self.dirty_since);
        }
    }
}
#[derive(Clone)]
pub(crate) struct GcCandidate {
    pub block: Arc<BlockState>,
    pub page: PageIndex,
    handle: Arc<PageState>,
    selection: u64,
    pub reason: usize,
}

#[derive(Default)]
struct Registry {
    by_id: HashMap<BlockId, u64>,
    ordered: BTreeMap<u64, Arc<BlockState>>,
    next: u64,
    membership: u64,
    checkpointed_membership: u64,
}
#[derive(Default)]
pub(crate) struct ScanCursor {
    shard: usize,
    block: u64,
    page: u64,
    ceiling: Option<u64>,
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
    pub empty: Vec<Arc<BlockState>>,
}
pub(crate) struct PageLifecycle {
    shards: Vec<Mutex<Registry>>,
    work: Vec<Arc<ShardWork>>,
    directories: Vec<Arc<tokio::sync::RwLock<()>>>,
    checkpoints: Vec<Arc<tokio::sync::Mutex<()>>>,
}
impl PageLifecycle {
    pub fn new() -> Self {
        Self {
            work: (0..SHARDS).map(|_| Arc::default()).collect(),
            shards: (0..SHARDS)
                .map(|_| Mutex::new(Registry::default()))
                .collect(),
            checkpoints: (0..SHARDS)
                .map(|_| Arc::new(tokio::sync::Mutex::new(())))
                .collect(),
            directories: (0..DIRECTORY_SHARDS)
                .map(|_| Arc::new(tokio::sync::RwLock::new(())))
                .collect(),
        }
    }
    pub fn directory_gate(&self, digest: u64) -> Arc<tokio::sync::RwLock<()>> {
        self.directories[digest as usize % DIRECTORY_SHARDS].clone()
    }
    #[cfg(test)]
    pub fn get(&self, id: &BlockId) -> Option<Arc<BlockState>> {
        let g = self.shards[disk_shard(id)].lock().unwrap();
        g.by_id.get(id).and_then(|n| g.ordered.get(n)).cloned()
    }
    pub fn retire_empty(&self, id: &BlockId) {
        let mut g = self.shards[disk_shard(id)].lock().unwrap();
        if let Some(&n) = g.by_id.get(id) {
            let block = &g.ordered[&n];
            let state = block.inner.lock().unwrap();
            let empty =
                state.pages.is_empty() && !state.cleanup_pending && Arc::strong_count(block) == 1;
            if empty {
                state.work.retire();
            }
            drop(state);
            if empty {
                g.ordered.remove(&n);
                g.by_id.remove(id);
                g.membership += 1;
            }
        }
    }
    pub fn block(&self, id: &BlockId) -> Arc<BlockState> {
        let mut hash = DefaultHasher::new();
        id.hash(&mut hash);
        let mut g = self.shards[disk_shard(id)].lock().unwrap();
        if let Some(n) = g.by_id.get(id) {
            return g.ordered[n].clone();
        }
        g.next += 1;
        g.membership += 1;
        let n = g.next;
        let block = Arc::new(BlockState {
            id: id.clone(),
            gate: Arc::new(PageMutationGate {
                directory: self.directory_gate(hash.finish()),
                block: tokio::sync::RwLock::new(()),
            }),
            inner: Mutex::new(BlockPages {
                work: Arc::new(BlockWork {
                    shard: self.work[disk_shard(id)].clone(),
                    slot: n,
                    retired: AtomicBool::new(false),
                    dirty_since: AtomicU64::new(UNKNOWN),
                }),
                ..Default::default()
            }),
        });
        g.by_id.insert(id.clone(), n);
        g.ordered.insert(n, block.clone());
        // Failed fills and whole-block operations can leave empty registry state.
        self.work[disk_shard(id)].pending.lock().unwrap().insert(n);
        block
    }
    /// Ordered block slots and page keys let a batch resume without copying the registry.
    /// Empty blocks are retired only without outside references, preserving gate identity.
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
        let mut work = 0;
        while work < limit && out.len() + report.empty.len() < delete_limit {
            let mut registry = self.shards[cursor.shard].lock().unwrap();
            let ceiling = *cursor.ceiling.get_or_insert(registry.next);
            let next = if cursor.block <= ceiling {
                if tti.is_some() {
                    registry.ordered.range(cursor.block..=ceiling).next()
                } else {
                    let slot = self.work[cursor.shard]
                        .pending
                        .lock()
                        .unwrap()
                        .range(cursor.block..=ceiling)
                        .next()
                        .copied();
                    slot.and_then(|slot| registry.ordered.get_key_value(&slot))
                }
            } else {
                None
            };
            let Some((&slot, block)) = next else {
                cursor.shard += 1;
                cursor.block = 0;
                cursor.page = 0;
                cursor.ceiling = None;
                if cursor.shard == SHARDS {
                    cursor.shard = 0;
                    report.completed = true;
                    break;
                }
                continue;
            };
            if slot != cursor.block {
                cursor.block = slot;
                cursor.page = 0;
            }
            let mut state = block.inner.lock().unwrap();
            if state.pages.is_empty() && state.cleanup_pending {
                report.empty.push(block.clone());
            }
            if state.pages.is_empty() && !state.cleanup_pending && Arc::strong_count(block) == 1 {
                let id = block.id.clone();
                state.work.retire();
                drop(state);
                registry.ordered.remove(&slot);
                registry.by_id.remove(&id);
                registry.membership += 1;
                cursor.block = slot + 1;
                cursor.page = 0;
                work += 1;
                continue;
            }
            let mut exhausted = true;
            let mut in_block = 0;
            let start = cursor.page.min(u32::MAX as u64) as u32;
            let keys: Vec<_> = if tti.is_some() {
                state
                    .pages
                    .range(start..)
                    .map(|(&page, _)| page)
                    .take(64)
                    .collect()
            } else {
                state.retries.range(start..).copied().take(64).collect()
            };
            for page in keys {
                if (page as u64) < cursor.page {
                    continue;
                }
                work += 1;
                in_block += 1;
                report.checked += 1;
                cursor.page = u64::from(page) + 1;
                let flags = state.pages[&page].handle.state.load(Ordering::Acquire);
                let retry = PageState::retry(flags);
                if retry.is_none() {
                    state.retries.remove(&page);
                }
                let entry = state.pages.get_mut(&page).unwrap();
                if retry.is_some() {
                    report.retries += 1;
                }
                let tti_expired =
                    tti.is_some_and(|tti| expired(entry.handle.last_access(), now, tti));
                let retryable = retry.is_some_and(|reason| reason != 0 || tti.is_some());
                if flags & (CLOSED | READERS) == 0 && (tti_expired || retryable) {
                    if let Some(candidate) = block.select(
                        PageIndex(page),
                        entry,
                        if tti_expired { 0 } else { retry.unwrap() },
                        flags,
                    ) {
                        out.push(candidate);
                    }
                }
                if work == limit || in_block == 64 || out.len() + report.empty.len() == delete_limit
                {
                    // Release registry and page locks even for a very large block.
                    exhausted = false;
                    break;
                }
            }
            if !state.pages.is_empty() && state.retries.is_empty() && !state.cleanup_pending {
                self.work[cursor.shard]
                    .pending
                    .lock()
                    .unwrap()
                    .remove(&slot);
            }
            if exhausted {
                cursor.block = slot + 1;
                cursor.page = 0;
                work += 1;
            }
        }
        (out, report)
    }
    /// Only checkpoint publication and cleanup of shard-level temporary files
    /// take this gate. Foreground page I/O never waits for a shard checkpoint.
    pub fn checkpoint_gate(&self, shard: usize) -> Arc<tokio::sync::Mutex<()>> {
        self.checkpoints[shard].clone()
    }
    pub fn checkpoint_start(&self, shard: usize) -> (u64, u64, bool) {
        let g = self.shards[shard].lock().unwrap();
        (
            g.next,
            g.membership,
            g.membership != g.checkpointed_membership
                || !self.work[shard].dirty.lock().unwrap().is_empty(),
        )
    }
    pub fn checkpoint_batch(
        &self,
        shard: usize,
        slot: &mut u64,
        ceiling: u64,
    ) -> Vec<Arc<BlockState>> {
        if *slot > ceiling {
            return Vec::new();
        }
        let g = self.shards[shard].lock().unwrap();
        let batch: Vec<_> = g
            .ordered
            .range(*slot..=ceiling)
            .take(64)
            .map(|(&n, block)| {
                *slot = n + 1;
                block.clone()
            })
            .collect();
        batch
    }
    pub fn checkpoint_finished(&self, shard: usize, membership: u64) {
        self.shards[shard].lock().unwrap().checkpointed_membership = membership;
    }
    pub fn checkpoint_recovered(&self, shard: usize) {
        // Also rewrite a recovered snapshot whose last pages disappeared in a
        // crash. There may be no live BlockState left to carry a dirty bit.
        self.shards[shard].lock().unwrap().membership += 1;
    }
    /// Summaries contain only dirty blocks, never a traversal of resident pages.
    pub fn dirty_summary(&self) -> (usize, Option<u64>) {
        let mut count = 0;
        let mut oldest: Option<u64> = None;
        for shard in &self.work {
            let dirty = shard.dirty.lock().unwrap();
            count += dirty.len();
            if let Some(since) = dirty.values().copied().min() {
                oldest = Some(oldest.map_or(since, |old| old.min(since)));
            }
        }
        (count, oldest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{Backend, ObjectId, Version};
    #[test]
    fn scan_metrics_accumulate_until_the_round_completes() {
        let mut cursor = ScanCursor::default();
        assert_eq!(cursor.record_batch(1.0, 7, false), None);
        assert_eq!(cursor.record_batch(2.0, 0, true), Some((3.0, 7)));
        assert_eq!(cursor.record_batch(0.5, 0, true), Some((0.5, 0)));
    }
    pub(super) fn id() -> BlockId {
        BlockId {
            object: ObjectId::new(Backend::S3, "bucket", "key"),
            version: Version("v1".into()),
            offset: 0,
            block_size: 64,
        }
    }

    #[test]
    fn read_pin_and_delete_claim_have_one_winner() {
        let life = PageLifecycle::new();
        let block = life.block(&id());
        for _ in 0..128 {
            let handle = block.register(PageIndex(0), Some(10), 10, true);
            let candidate = block.candidate(PageIndex(0)).unwrap();
            let barrier = std::sync::Barrier::new(2);
            std::thread::scope(|scope| {
                let reader = scope.spawn(|| {
                    barrier.wait();
                    let read = handle.acquire();
                    // Retain the pin until both contenders have decided.
                    barrier.wait();
                    read
                });
                barrier.wait();
                let claimed = block.claim(&candidate, 100, Some(10));
                barrier.wait();
                let read = reader.join().unwrap();
                assert_ne!(claimed, read.is_some());
                drop(read);
                if !claimed {
                    assert!(block.claim(&candidate, 100, Some(10)));
                }
                assert!(handle.acquire().is_none());
                assert!(block.candidate(PageIndex(0)).is_none());
                block.finish(&candidate, 100);
                assert!(handle.acquire().is_none());
            });
        }
    }

    #[test]
    fn scan_cannot_revive_retry_cancelled_by_a_concurrent_successful_read() {
        let life = PageLifecycle::new();
        let block = life.block(&id());
        let handle = block.register(PageIndex(0), Some(10), 10, false);
        for reason in [1, 2] {
            let candidate = block.candidate(PageIndex(0)).unwrap();
            assert!(block.claim(&candidate, 100, None));
            block.abort(&candidate, reason);
            let mut state = block.inner.lock().unwrap();
            let entry = state.pages.get_mut(&0).unwrap();
            let observed = entry.handle.state.load(Ordering::Acquire);
            assert_eq!(PageState::retry(observed), Some(reason));
            // Pause selection after inspecting eligibility. Completion through
            // the stable handle must work even while this block lock is held.
            handle.acquire().unwrap().record_access_and_release(None);
            assert!(block
                .select(PageIndex(0), entry, reason, observed)
                .is_none());
            drop(state);
            let (candidates, _) = life.scan(&mut ScanCursor::default(), 100, 100, None, 10);
            assert!(candidates.is_empty());
        }
    }

    #[test]
    fn coalesced_access_invalidates_selection_without_changing_block_revision() {
        let life = PageLifecycle::new();
        let block = life.block(&id());
        let handle = block.register(PageIndex(0), Some(10), 10, false);
        let before = block.inner.lock().unwrap().revision;
        let old = block.candidate(PageIndex(0)).unwrap();
        handle
            .acquire()
            .unwrap()
            .record_access_and_release(Some(10));
        assert!(!block.claim(&old, 100, None));
        let current = block.candidate(PageIndex(0)).unwrap();
        assert!(
            !block.claim(&old, 100, None),
            "reselection must not revive an old candidate"
        );
        // A failed read neither heats the page nor invalidates the selection.
        drop(handle.acquire().unwrap());
        assert_eq!(handle.last_access(), Some(10));
        assert_eq!(handle.dirty_since(), None);
        assert_eq!(block.inner.lock().unwrap().revision, before);
        assert!(block.claim(&current, 100, None));
    }

    #[test]
    fn checkpoint_commit_retains_new_access_and_failure_restores_old_dirtiness() {
        let handle = Arc::new(PageState::new(Some(10)));
        handle
            .acquire()
            .unwrap()
            .record_access_and_release(Some(20));
        let snapshot = PageCheckpoint::new(0, &handle);
        assert_eq!(snapshot.access, Some(20));
        assert!(snapshot.dirty());
        handle
            .acquire()
            .unwrap()
            .record_access_and_release(Some(30));
        snapshot.commit();
        assert_eq!(handle.dirty_since(), Some(30));
        let failed = PageCheckpoint::new(0, &handle);
        handle
            .acquire()
            .unwrap()
            .record_access_and_release(Some(40));
        drop(failed);
        assert_eq!(handle.dirty_since(), Some(30));
        let retry = PageCheckpoint::new(0, &handle);
        assert_eq!(retry.access, Some(40));
        retry.commit();
        // Same-millisecond and late readers do not regress the clock or dirty
        // an already durable timestamp.
        handle
            .acquire()
            .unwrap()
            .record_access_and_release(Some(40));
        handle
            .acquire()
            .unwrap()
            .record_access_and_release(Some(35));
        assert_eq!(handle.last_access(), Some(40));
        assert_eq!(handle.dirty_since(), None);
    }

    #[test]
    fn checkpoint_racing_a_touch_either_captures_it_or_keeps_it_dirty() {
        for _ in 0..128 {
            let handle = Arc::new(PageState::new(Some(10)));
            let barrier = std::sync::Barrier::new(2);
            std::thread::scope(|scope| {
                let writer = scope.spawn(|| {
                    barrier.wait();
                    handle
                        .acquire()
                        .unwrap()
                        .record_access_and_release(Some(20));
                });
                barrier.wait();
                let snapshot = PageCheckpoint::new(0, &handle);
                let persisted = snapshot.access;
                snapshot.commit();
                writer.join().unwrap();
                assert!(persisted == Some(20) || handle.dirty_since() == Some(20));
            });
        }
    }
    #[test]
    fn consumed_access_releases_once_and_still_invalidates_candidates() {
        let life = PageLifecycle::new();
        let block = life.block(&id());
        block.register(PageIndex(0), Some(10), 10, true);
        let stale = block.candidate(PageIndex(0)).unwrap();
        let a = block.acquire(PageIndex(0)).unwrap();
        let b = block.acquire(PageIndex(0)).unwrap();
        a.record_access_and_release(Some(20));
        {
            let state = block.inner.lock().unwrap();
            assert_eq!(
                state.pages[&0].handle.state.load(Ordering::Acquire) & READERS,
                1
            );
            assert_eq!(state.pages[&0].handle.last_access(), Some(20));
        }
        assert!(!block.claim(&stale, 100, None));
        let current = block.candidate(PageIndex(0)).unwrap();
        assert!(!block.claim(&current, 100, None));
        drop(b);
        assert_eq!(
            block.inner.lock().unwrap().pages[&0]
                .handle
                .state
                .load(Ordering::Acquire)
                & READERS,
            0
        );
        assert!(block.claim(&current, 100, None));
    }

    #[tokio::test]
    async fn directory_stripes_do_not_serialize_foreground_blocks() {
        let life = PageLifecycle::new();
        let first = id();
        let digest = |id: &BlockId| {
            let mut hash = DefaultHasher::new();
            id.hash(&mut hash);
            hash.finish()
        };
        let second = (1..10000)
            .map(|offset| {
                let mut id = first.clone();
                id.offset = offset * 64;
                id
            })
            .find(|id| {
                digest(id) as usize % DIRECTORY_SHARDS == digest(&first) as usize % DIRECTORY_SHARDS
            })
            .unwrap();
        let a = life.block(&first);
        let b = life.block(&second);
        let _a = a.gate.lock().await;
        let _b = tokio::time::timeout(std::time::Duration::from_secs(1), b.gate.lock())
            .await
            .unwrap();
        assert!(life.directory_gate(digest(&first)).try_write().is_err());
    }

    #[tokio::test]
    async fn disjoint_fills_overlap_but_exclude_gc_and_whole_commits() {
        let life = PageLifecycle::new();
        let state = life.block(&id());
        let a = state.gate.lock_page_fill().await;
        let b = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            state.gate.lock_page_fill(),
        )
        .await
        .unwrap();
        let exclusive = state.gate.lock();
        tokio::pin!(exclusive);
        assert!(futures::poll!(&mut exclusive).is_pending());
        assert!(state.gate.directory.try_write().is_err());
        drop(a);
        assert!(futures::poll!(&mut exclusive).is_pending());
        drop(b);
        let _exclusive = exclusive.await;
        assert!(futures::poll!(std::pin::pin!(state.gate.lock_page_fill())).is_pending());
    }

    #[test]
    fn checkpoint_rollback_cannot_resurrect_retired_dirty_summary() {
        let life = PageLifecycle::new();
        let block = life.block(&id());
        let page = block.register(PageIndex(0), Some(10), 10, true);
        page.acquire().unwrap().record_access_and_release(Some(20));
        let summary = block.inner.lock().unwrap().work.checkpoint();
        let snapshot = PageCheckpoint::new(0, &page);
        block.claim_all_pages().unwrap().finish(30);
        drop(block);
        life.retire_empty(&id());
        assert!(life.get(&id()).is_none());
        // Model failed publication dropping its retained guards after retirement.
        drop(snapshot);
        drop(summary);
        assert_eq!(life.dirty_summary(), (0, None));
    }

    #[test]
    fn disabled_tti_retires_empty_states_after_external_references_release() {
        let life = PageLifecycle::new();
        let block = life.block(&id());
        // An incomplete fill's empty state stays pending while a caller owns it.
        life.scan(&mut ScanCursor::default(), 100, 100, None, 10);
        drop(block);
        life.scan(&mut ScanCursor::default(), 100, 100, None, 10);
        assert!(life.get(&id()).is_none());

        let block = life.block(&id());
        block.register(PageIndex(0), Some(1), 1, true);
        life.scan(&mut ScanCursor::default(), 100, 100, None, 10);
        block.claim_all_pages().unwrap().finish(100);
        life.retire_empty(&id()); // Still protected by this caller's Arc.
        life.scan(&mut ScanCursor::default(), 100, 100, None, 10);
        drop(block);
        life.scan(&mut ScanCursor::default(), 100, 100, None, 10);
        assert!(life.get(&id()).is_none());
        assert_eq!(life.dirty_summary(), (0, None));
    }

    #[test]
    fn disabled_tti_examines_only_retry_pages() {
        let life = PageLifecycle::new();
        let block = life.block(&id());
        for page in 0..10000 {
            block.register(PageIndex(page), None, 1, false);
        }
        let (_, clean) = life.scan(&mut ScanCursor::default(), 100, 10000, None, 10);
        assert!(clean.completed);
        assert_eq!(clean.checked, 0);
        let retry = block.candidate(PageIndex(9000)).unwrap();
        block.abort(&retry, 1);
        let (candidates, retry_scan) = life.scan(&mut ScanCursor::default(), 100, 10000, None, 10);
        assert_eq!(retry_scan.checked, 1);
        assert_eq!(candidates.len(), 1);
        block
            .acquire(PageIndex(9000))
            .unwrap()
            .record_access_and_release(None);
        let (candidates, _) = life.scan(&mut ScanCursor::default(), 100, 10000, None, 10);
        assert!(candidates.is_empty());
        let (_, clean) = life.scan(&mut ScanCursor::default(), 100, 10000, None, 10);
        assert_eq!(
            clean.checked, 0,
            "successful reads retire cancelled retries"
        );
    }

    #[test]
    fn dirty_block_reads_do_not_wait_for_the_shard_summary_lock() {
        let life = PageLifecycle::new();
        let block = life.block(&id());
        let handles: Vec<_> = (0..256)
            .map(|page| block.register(PageIndex(page), Some(10), 10, false))
            .collect();
        handles[0]
            .acquire()
            .unwrap()
            .record_access_and_release(Some(20));
        let work = block.inner.lock().unwrap().work.clone();
        let held = work.shard.dirty.lock().unwrap();
        let completed = std::thread::scope(|scope| {
            let (tx, rx) = std::sync::mpsc::channel();
            scope.spawn(move || {
                for handle in handles {
                    handle
                        .acquire()
                        .unwrap()
                        .record_access_and_release(Some(30));
                }
                tx.send(()).unwrap();
            });
            let completed = rx.recv_timeout(std::time::Duration::from_secs(2)).is_ok();
            // Always release before joining, including a regression's timeout.
            drop(held);
            completed
        });
        assert!(
            completed,
            "other pages of a dirty block waited for the shard lock"
        );
        assert_eq!(life.dirty_summary(), (1, Some(20)));
    }

    #[test]
    fn failed_checkpoint_restores_older_block_summary_after_a_new_touch() {
        let life = PageLifecycle::new();
        let block = life.block(&id());
        let handle = block.register(PageIndex(0), Some(10), 10, false);
        let work = block.inner.lock().unwrap().work.clone();
        handle
            .acquire()
            .unwrap()
            .record_access_and_release(Some(20));
        let summary = work.checkpoint();
        let page = PageCheckpoint::new(0, &handle);
        handle
            .acquire()
            .unwrap()
            .record_access_and_release(Some(30));
        assert_eq!(life.dirty_summary(), (1, Some(30)));
        drop(page);
        drop(summary);
        assert_eq!(life.dirty_summary(), (1, Some(20)));
        let summary = work.checkpoint();
        let page = PageCheckpoint::new(0, &handle);
        assert_eq!(page.access, Some(30));
        page.commit();
        summary.commit();
        assert_eq!(life.dirty_summary(), (0, None));
    }

    #[test]
    fn coalesced_block_touch_racing_checkpoint_is_captured_or_stays_scheduled() {
        for _ in 0..128 {
            let life = PageLifecycle::new();
            let block = life.block(&id());
            let first = block.register(PageIndex(0), Some(10), 10, false);
            let second = block.register(PageIndex(1), Some(10), 10, false);
            first.acquire().unwrap().record_access_and_release(Some(20));
            let work = block.inner.lock().unwrap().work.clone();
            let barrier = std::sync::Barrier::new(2);
            std::thread::scope(|scope| {
                let reader = scope.spawn(|| {
                    barrier.wait();
                    second
                        .acquire()
                        .unwrap()
                        .record_access_and_release(Some(30));
                });
                barrier.wait();
                let summary = work.checkpoint();
                PageCheckpoint::new(0, &first).commit();
                let page = PageCheckpoint::new(1, &second);
                let captured = page.access == Some(30);
                page.commit();
                summary.commit();
                reader.join().unwrap();
                assert!(captured || life.dirty_summary().0 == 1);
            });
            let summary = work.checkpoint();
            PageCheckpoint::new(0, &first).commit();
            PageCheckpoint::new(1, &second).commit();
            summary.commit();
            assert_eq!(life.dirty_summary(), (0, None));
        }
    }

    #[test]
    fn checkpoint_summary_preserves_changes_and_restores_failed_snapshots() {
        let life = PageLifecycle::new();
        let block = life.block(&id());
        let handle = block.register(PageIndex(0), Some(10), 10, false);
        let shard = disk_shard(&id());
        let (_, membership, _) = life.checkpoint_start(shard);
        life.checkpoint_finished(shard, membership);
        assert!(!life.checkpoint_start(shard).2);
        handle
            .acquire()
            .unwrap()
            .record_access_and_release(Some(20));
        assert_eq!(life.dirty_summary(), (1, Some(20)));
        let work = block.inner.lock().unwrap().work.clone();
        let summary = work.checkpoint();
        let page = PageCheckpoint::new(0, &handle);
        handle
            .acquire()
            .unwrap()
            .record_access_and_release(Some(30));
        page.commit();
        summary.commit();
        assert_eq!(life.dirty_summary(), (1, Some(30)));
        let summary = work.checkpoint();
        let page = PageCheckpoint::new(0, &handle);
        drop(page);
        drop(summary);
        assert!(life.checkpoint_start(shard).2);
        assert_eq!(life.dirty_summary(), (1, Some(30)));
        let summary = work.checkpoint();
        PageCheckpoint::new(0, &handle).commit();
        summary.commit();
        assert_eq!(life.dirty_summary(), (0, None));
        assert!(!life.checkpoint_start(shard).2);
    }

    #[test]
    fn disabled_tti_keeps_unknown_pages_but_discovers_cleanup_retries() {
        let life = PageLifecycle::new();
        let block = life.block(&id());
        block.register(PageIndex(0), None, 1, false);
        block.register(PageIndex(1), None, 1, false);
        let retry = block.candidate(PageIndex(1)).unwrap();
        block.abort(&retry, 1);
        let (candidates, _) = life.scan(&mut ScanCursor::default(), 100, 10000, None, 10);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].page, PageIndex(1));
        assert_eq!(candidates[0].reason, 1);
    }

    #[test]
    fn boundary_pin_touch_and_generation() {
        let life = PageLifecycle::new();
        let b = life.block(&id());
        b.register(PageIndex(0), Some(10), 10, true);
        let c = b.candidate(PageIndex(0)).unwrap();
        assert!(!b.claim(&c, 20, Some(10)));
        let read = b.acquire(PageIndex(0)).unwrap();
        assert!(!b.claim(&c, 21, Some(10)));
        read.record_access(21);
        drop(read);
        assert!(!b.claim(&c, 40, Some(10)));
        let c = b.candidate(PageIndex(0)).unwrap();
        assert!(b.claim(&c, 40, Some(10)));
        assert!(b.acquire(PageIndex(0)).is_none());
        b.abort(&c, 0);
        assert!(b.acquire(PageIndex(0)).is_some());
        assert!(b.claim(&c, 40, Some(10)));
        b.finish(&c, 40);
        b.register(PageIndex(0), Some(50), 50, true);
        assert!(!b.claim(&c, 100, None));
    }
    #[test]
    fn deletion_budget_resumes_across_empty_cleanup_candidates() {
        let life = PageLifecycle::new();
        for offset in 0..8 {
            let mut id = id();
            id.offset = offset * 64;
            let state = life.block(&id);
            state.register(PageIndex(0), Some(1), 1, false);
            let candidate = state.candidate(PageIndex(0)).unwrap();
            assert!(state.claim(&candidate, 100, None));
            state.finish(&candidate, 100);
        }
        let mut cursor = ScanCursor::default();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..16 {
            let (pages, report) = life.scan(&mut cursor, 100, 100, None, 1);
            assert!(pages.is_empty());
            assert!(report.empty.len() <= 1);
            for block in report.empty {
                assert!(seen.insert(block.id.clone()));
            }
            if report.completed {
                assert_eq!(seen.len(), 8);
                return;
            }
        }
        panic!("cleanup candidates prevented scan completion");
    }

    #[test]
    fn unknown_is_expired_and_scan_resumes() {
        let life = PageLifecycle::new();
        let b = life.block(&id());
        for p in 0..10 {
            b.register(PageIndex(p), None, 50, false);
        }
        let mut cursor = ScanCursor::default();
        let mut seen = Vec::new();
        loop {
            let (batch, r) = life.scan(&mut cursor, 3, 50, Some(100), usize::MAX);
            assert!(r.checked <= 3);
            seen.extend(batch.iter().map(|c| c.page.0));
            if r.completed {
                break;
            }
        }
        seen.sort();
        assert_eq!(seen, (0..10).collect::<Vec<_>>());
    }
}
