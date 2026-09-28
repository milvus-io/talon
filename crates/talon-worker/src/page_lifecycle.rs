//! Page residency and idle age. Disk operations never run under these locks.
use std::collections::{BTreeMap, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
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
    block: tokio::sync::Mutex<()>,
}
pub(crate) struct PageMutationGuard<'a> {
    _directory: tokio::sync::OwnedRwLockReadGuard<()>,
    _block: tokio::sync::MutexGuard<'a, ()>,
}
impl PageMutationGate {
    pub async fn lock(&self) -> PageMutationGuard<'_> {
        let directory = self.directory.clone().read_owned().await;
        let block = self.block.lock().await;
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

pub(crate) struct PageState {
    state: AtomicU64,
    last_access: AtomicU64,
    dirty_since: AtomicU64,
}
impl PageState {
    fn new(access: Option<u64>) -> Self {
        Self {
            state: AtomicU64::new(0),
            last_access: AtomicU64::new(access.map_or(UNKNOWN, |t| t.min(UNKNOWN - 1))),
            dirty_since: AtomicU64::new(UNKNOWN),
        }
    }
    pub fn last_access(&self) -> Option<u64> {
        let t = self.last_access.load(Ordering::Acquire);
        (t != UNKNOWN).then_some(t)
    }
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
                    // must also see this timestamp. No block-shared cache line.
                    self.dirty_since.fetch_min(now, Ordering::Release);
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
    pub dirty_since: Option<u64>,
    pub cleanup_pending: bool,
}
impl BlockPages {
    pub fn changed(&mut self, now: u64) {
        self.revision = self
            .revision
            .checked_add(1)
            .expect("access revision exhausted");
        self.dirty_since.get_or_insert(now);
    }
    pub fn dirty_since(&self) -> Option<u64> {
        self.dirty_since
            .into_iter()
            .chain(self.pages.values().filter_map(|p| p.handle.dirty_since()))
            .min()
    }
}
pub(crate) struct BlockState {
    pub id: BlockId,
    pub gate: Arc<PageMutationGate>,
    pub inner: Mutex<BlockPages>,
}
impl BlockState {
    pub fn register(
        &self,
        page: PageIndex,
        access: Option<u64>,
        now: u64,
        dirty: bool,
    ) -> Arc<PageState> {
        let mut g = self.inner.lock().unwrap();
        g.cleanup_pending = false;
        // A duplicate same-version fill preserves existing guards and identity.
        let e = g.pages.entry(page.0).or_insert_with(|| PageEntry {
            handle: Arc::new(PageState::new(access)),
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
    pub fn claim(&self, c: &GcCandidate, now: u64, ttl: Option<u64>) -> bool {
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
        if ttl.is_some_and(|ttl| !expired(e.handle.last_access(), now, ttl)) {
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
            g.cleanup_pending = g.pages.is_empty();
            g.changed(now);
        }
    }
    pub fn abort(&self, c: &GcCandidate, reason: usize) {
        let g = self.inner.lock().unwrap();
        if let Some(e) = g.pages.get(&c.page.0) {
            if Arc::ptr_eq(&e.handle, &c.handle) && e.selection == c.selection {
                e.handle
                    .state
                    .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |old| {
                        Some((old & !(CLOSED | RETRY)) | ((reason as u64 + 1) << RETRY_SHIFT))
                    })
                    .unwrap();
            }
        }
    }
}

pub(crate) fn expired(last: Option<u64>, now: u64, ttl: u64) -> bool {
    last.map_or(true, |last| now.saturating_sub(last) > ttl)
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
    directories: Vec<Arc<tokio::sync::RwLock<()>>>,
    checkpoints: Vec<Arc<Mutex<()>>>,
}
impl PageLifecycle {
    pub fn new() -> Self {
        Self {
            shards: (0..SHARDS)
                .map(|_| Mutex::new(Registry::default()))
                .collect(),
            checkpoints: (0..SHARDS).map(|_| Arc::new(Mutex::new(()))).collect(),
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
                block: tokio::sync::Mutex::new(()),
            }),
            inner: Mutex::new(BlockPages::default()),
        });
        g.by_id.insert(id.clone(), n);
        g.ordered.insert(n, block.clone());
        block
    }
    /// Ordered block slots and page keys let a batch resume without copying the registry.
    /// Empty blocks are retired only without outside references, preserving gate identity.
    pub fn scan(
        &self,
        cursor: &mut ScanCursor,
        limit: usize,
        now: u64,
        ttl: Option<u64>,
        delete_limit: usize,
    ) -> (Vec<GcCandidate>, ScanReport) {
        let mut out = Vec::new();
        let mut report = ScanReport::default();
        let mut work = 0;
        while work < limit && out.len() + report.empty.len() < delete_limit {
            let mut registry = self.shards[cursor.shard].lock().unwrap();
            let ceiling = *cursor.ceiling.get_or_insert(registry.next);
            let next = if cursor.block <= ceiling {
                registry.ordered.range(cursor.block..=ceiling).next()
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
            for (&page, entry) in state
                .pages
                .range_mut((cursor.page.min(u32::MAX as u64) as u32)..)
            {
                if (page as u64) < cursor.page {
                    continue;
                }
                work += 1;
                in_block += 1;
                report.checked += 1;
                cursor.page = u64::from(page) + 1;
                let flags = entry.handle.state.load(Ordering::Acquire);
                let retry = PageState::retry(flags);
                if retry.is_some() {
                    report.retries += 1;
                }
                let ttl_expired =
                    ttl.is_some_and(|ttl| expired(entry.handle.last_access(), now, ttl));
                let retryable = retry.is_some_and(|reason| reason != 0 || ttl.is_some());
                if flags & (CLOSED | READERS) == 0 && (ttl_expired || retryable) {
                    if let Some(candidate) = block.select(
                        PageIndex(page),
                        entry,
                        if ttl_expired { 0 } else { retry.unwrap() },
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
    pub fn checkpoint_gate(&self, shard: usize) -> Arc<Mutex<()>> {
        self.checkpoints[shard].clone()
    }
    pub fn checkpoint_start(&self, shard: usize) -> (u64, u64, bool) {
        let g = self.shards[shard].lock().unwrap();
        (
            g.next,
            g.membership,
            g.membership != g.checkpointed_membership,
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
    /// Checkpoint iteration is bounded by blocks examined, including clean blocks.
    pub fn dirty_batch(
        &self,
        cursor: &mut ScanCursor,
        limit: usize,
    ) -> (Vec<Arc<BlockState>>, bool) {
        let mut out = Vec::new();
        let mut examined = 0;
        while examined < limit {
            let g = self.shards[cursor.shard].lock().unwrap();
            let ceiling = *cursor.ceiling.get_or_insert(g.next);
            let next = if cursor.block <= ceiling {
                g.ordered.range(cursor.block..=ceiling).next()
            } else {
                None
            };
            let Some((&slot, block)) = next else {
                cursor.shard += 1;
                cursor.block = 0;
                cursor.ceiling = None;
                if cursor.shard == SHARDS {
                    cursor.shard = 0;
                    return (out, true);
                }
                continue;
            };
            cursor.block = slot + 1;
            examined += 1;
            let inner = block.inner.lock().unwrap();
            if inner.dirty_since().is_some() && !inner.pages.is_empty() {
                out.push(block.clone());
            }
        }
        (out, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{Backend, ObjectId, Version};
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

    #[test]
    fn disabled_ttl_keeps_unknown_pages_but_discovers_cleanup_retries() {
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
            state.inner.lock().unwrap().cleanup_pending = true;
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
