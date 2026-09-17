//! Page residency and idle age. Disk operations never run under these locks.
use std::collections::{BTreeMap, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use talon_core::{BlockId, PageIndex};

const SHARDS: usize = 32;
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

#[derive(Clone, Copy)]
pub(crate) struct PageEntry {
    pub last_access: Option<u64>,
    pub generation: u64,
    pub revision: u64,
    pub readers: u32,
    pub evicting: bool,
    pub retry: Option<u8>,
}
#[derive(Default)]
pub(crate) struct BlockPages {
    pub pages: BTreeMap<u32, PageEntry>,
    pub revision: u64,
    pub dirty_since: Option<u64>,
    next_generation: u64,
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
}
pub(crate) struct BlockState {
    pub id: BlockId,
    pub gate: Arc<PageMutationGate>,
    pub inner: Mutex<BlockPages>,
}
impl BlockState {
    pub fn register(&self, page: PageIndex, access: Option<u64>, now: u64, dirty: bool) {
        let mut g = self.inner.lock().unwrap();
        g.cleanup_pending = false;
        // A duplicate same-version fill must not invalidate an existing read guard.
        if let Some(e) = g.pages.get_mut(&page.0) {
            e.last_access = access;
            e.revision += 1;
        } else {
            g.next_generation = g
                .next_generation
                .checked_add(1)
                .expect("page generation exhausted");
            let generation = g.next_generation;
            g.pages.insert(
                page.0,
                PageEntry {
                    last_access: access,
                    generation,
                    revision: 0,
                    readers: 0,
                    evicting: false,
                    retry: None,
                },
            );
        }
        if dirty {
            g.changed(now);
        }
    }
    pub fn acquire(self: &Arc<Self>, page: PageIndex) -> Option<PageReadGuard> {
        let mut g = self.inner.lock().unwrap();
        let e = g.pages.get_mut(&page.0)?;
        if e.evicting {
            return None;
        }
        e.readers = e.readers.checked_add(1)?;
        Some(PageReadGuard {
            block: self.clone(),
            page,
            generation: e.generation,
        })
    }
    pub fn candidate(self: &Arc<Self>, page: PageIndex) -> Option<GcCandidate> {
        let g = self.inner.lock().unwrap();
        let e = g.pages.get(&page.0)?;
        Some(GcCandidate {
            block: self.clone(),
            page,
            generation: e.generation,
            revision: e.revision,
            reason: 0,
        })
    }
    /// Called with the mutation gate held; rechecks all scan-time assumptions.
    pub fn claim(&self, c: &GcCandidate, now: u64, ttl: Option<u64>) -> bool {
        let mut g = self.inner.lock().unwrap();
        let Some(e) = g.pages.get_mut(&c.page.0) else {
            return false;
        };
        if e.generation != c.generation || e.revision != c.revision || e.readers != 0 || e.evicting
        {
            return false;
        }
        if ttl.is_some_and(|ttl| !expired(e.last_access, now, ttl)) {
            return false;
        }
        e.evicting = true;
        true
    }
    pub fn finish(&self, c: &GcCandidate, now: u64) {
        let mut g = self.inner.lock().unwrap();
        if g.pages
            .get(&c.page.0)
            .is_some_and(|e| e.generation == c.generation && e.evicting)
        {
            g.pages.remove(&c.page.0);
            g.cleanup_pending = g.pages.is_empty();
            g.changed(now);
        }
    }
    pub fn abort(&self, c: &GcCandidate, reason: usize) {
        let mut g = self.inner.lock().unwrap();
        if let Some(e) = g.pages.get_mut(&c.page.0) {
            if e.generation == c.generation {
                e.evicting = false;
                e.retry = Some(reason as u8);
            }
        }
    }
}

pub(crate) fn expired(last: Option<u64>, now: u64, ttl: u64) -> bool {
    last.map_or(true, |last| now.saturating_sub(last) > ttl)
}
pub(crate) struct PageReadGuard {
    block: Arc<BlockState>,
    page: PageIndex,
    generation: u64,
}
impl PageReadGuard {
    pub fn record_access(&self, now: u64) {
        let mut g = self.block.inner.lock().unwrap();
        if let Some(e) = g.pages.get_mut(&self.page.0) {
            if e.generation == self.generation {
                e.last_access = Some(now);
                e.revision += 1;
                e.retry = None;
                g.changed(now);
            }
        }
    }
}
impl Drop for PageReadGuard {
    fn drop(&mut self) {
        let mut g = self.block.inner.lock().unwrap();
        if let Some(e) = g.pages.get_mut(&self.page.0) {
            if e.generation == self.generation {
                e.readers -= 1;
            }
        }
    }
}
#[derive(Clone)]
pub(crate) struct GcCandidate {
    pub block: Arc<BlockState>,
    pub page: PageIndex,
    generation: u64,
    revision: u64,
    pub reason: usize,
}

#[derive(Default)]
struct Registry {
    by_id: HashMap<BlockId, u64>,
    ordered: BTreeMap<u64, Arc<BlockState>>,
    next: u64,
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
}
impl PageLifecycle {
    pub fn new() -> Self {
        Self {
            shards: (0..SHARDS)
                .map(|_| Mutex::new(Registry::default()))
                .collect(),
            directories: (0..DIRECTORY_SHARDS)
                .map(|_| Arc::new(tokio::sync::RwLock::new(())))
                .collect(),
        }
    }
    pub fn directory_gate(&self, digest: u64) -> Arc<tokio::sync::RwLock<()>> {
        self.directories[digest as usize % DIRECTORY_SHARDS].clone()
    }
    pub fn get(&self, id: &BlockId) -> Option<Arc<BlockState>> {
        let mut hash = DefaultHasher::new();
        id.hash(&mut hash);
        let g = self.shards[hash.finish() as usize % SHARDS].lock().unwrap();
        g.by_id.get(id).and_then(|n| g.ordered.get(n)).cloned()
    }
    pub fn retire_empty(&self, id: &BlockId) {
        let mut hash = DefaultHasher::new();
        id.hash(&mut hash);
        let mut g = self.shards[hash.finish() as usize % SHARDS].lock().unwrap();
        if let Some(&n) = g.by_id.get(id) {
            let block = &g.ordered[&n];
            let state = block.inner.lock().unwrap();
            let empty =
                state.pages.is_empty() && !state.cleanup_pending && Arc::strong_count(block) == 1;
            drop(state);
            if empty {
                g.ordered.remove(&n);
                g.by_id.remove(id);
            }
        }
    }
    pub fn block(&self, id: &BlockId) -> Arc<BlockState> {
        let mut hash = DefaultHasher::new();
        id.hash(&mut hash);
        let mut g = self.shards[hash.finish() as usize % SHARDS].lock().unwrap();
        if let Some(n) = g.by_id.get(id) {
            return g.ordered[n].clone();
        }
        g.next += 1;
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
            let state = block.inner.lock().unwrap();
            if state.pages.is_empty() && state.cleanup_pending {
                report.empty.push(block.clone());
            }
            if state.pages.is_empty() && !state.cleanup_pending && Arc::strong_count(block) == 1 {
                let id = block.id.clone();
                drop(state);
                registry.ordered.remove(&slot);
                registry.by_id.remove(&id);
                cursor.block = slot + 1;
                cursor.page = 0;
                work += 1;
                continue;
            }
            let mut exhausted = true;
            let mut in_block = 0;
            for (&page, entry) in state
                .pages
                .range((cursor.page.min(u32::MAX as u64) as u32)..)
            {
                if (page as u64) < cursor.page {
                    continue;
                }
                work += 1;
                in_block += 1;
                report.checked += 1;
                cursor.page = u64::from(page) + 1;
                if entry.retry.is_some() {
                    report.retries += 1;
                }
                let ttl_expired = ttl.is_some_and(|ttl| expired(entry.last_access, now, ttl));
                let retryable = entry
                    .retry
                    .is_some_and(|reason| reason != 0 || ttl.is_some());
                if !entry.evicting && entry.readers == 0 && (ttl_expired || retryable) {
                    out.push(GcCandidate {
                        block: block.clone(),
                        page: PageIndex(page),
                        generation: entry.generation,
                        revision: entry.revision,
                        reason: if ttl_expired {
                            0
                        } else {
                            usize::from(entry.retry.unwrap())
                        },
                    });
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
            if inner.dirty_since.is_some() && !inner.pages.is_empty() {
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
