//! Byte-accounted approximate LRU (second-chance) eviction policy.
//!
//! Tracks cache *units* — a whole block, or a single `(block, page)` for paged
//! blocks — in a second-chance queue, keyed by their byte cost rather than
//! by count. When the tracked total exceeds capacity, [`Lru::evict_to_fit`]
//! returns reclamation candidates, skipping any unit currently *pinned*
//! by an in-flight reader (so a `sendfile` in progress is never evicted).
//!
//! This module is policy only: it decides *what* to evict and maintains byte
//! accounting. Unlinking files and updating the [`BlockIndex`](crate::BlockIndex)
//! is done by the caller with the returned unit list. Segmented-LRU / TinyLFU
//! are deferred per DESIGN.md.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use talon_core::{BlockId, ObjectId, PageIndex, Version};

/// A single evictable cache unit.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CacheUnit {
    /// A whole block, evicted as one unit.
    Whole(BlockId),
    /// One page of a paged block.
    Page(BlockId, PageIndex),
}

impl CacheUnit {
    pub(crate) fn block(&self) -> &BlockId {
        match self {
            Self::Whole(block) | Self::Page(block, _) => block,
        }
    }

    fn page(&self) -> Option<PageIndex> {
        match self {
            Self::Whole(_) => None,
            Self::Page(_, page) => Some(*page),
        }
    }
}

/// Version-independent identity; offsets and block sizes must remain distinct.
#[derive(PartialEq, Eq, Hash)]
struct LogicalBlock {
    object: ObjectId,
    offset: u64,
    block_size: u32,
}

impl From<&BlockId> for LogicalBlock {
    fn from(block: &BlockId) -> Self {
        Self {
            object: block.object.clone(),
            offset: block.offset,
            block_size: block.block_size,
        }
    }
}

/// Stable recency token for one resident unit. Keeping this token does not pin
/// data. After removal it cannot affect a new admission of the same identity.
#[derive(Clone)]
pub struct AccessHandle(Arc<AtomicBool>, Arc<AtomicU64>);

/// Process-local monotonic age. Restarted residents start a fresh idle window.
pub(crate) fn access_millis() -> u64 {
    static START: OnceLock<std::time::Instant> = OnceLock::new();
    START
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_millis() as u64
        + 1
}

impl Default for AccessHandle {
    fn default() -> Self {
        Self(
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicU64::new(access_millis())),
        )
    }
}

impl AccessHandle {
    /// Coalesce hits until the eviction hand consumes this second chance.
    /// This is advisory recency only, so relaxed ordering is sufficient.
    pub fn touch(&self) {
        let now = access_millis();
        if self.1.load(Ordering::Relaxed) < now {
            self.1.fetch_max(now, Ordering::Relaxed);
        }
        if !self.0.load(Ordering::Relaxed) {
            self.0.store(true, Ordering::Relaxed);
        }
    }
}

/// A policy snapshot invalidated by a later touch, selection, or admission.
#[derive(Clone, Debug)]
pub(crate) struct EvictionCandidate {
    pub unit: CacheUnit,
    revision: u64,
}

/// One complete resident block, independent of its whole/paged representation.
#[derive(Clone, Debug)]
pub(crate) struct StaleBlockCandidate {
    pub block: BlockId,
    pub units: usize,
    revision: u64,
    pub last_access: u64,
    pub position: u64,
}

struct BlockActivity {
    units: usize,
    pins: usize,
    position: u64,
    revision: u64,
    access: Arc<AtomicU64>,
}

struct Entry {
    bytes: u64,
    position: u64,
    revision: u64,
    access: AccessHandle,
    pins: u32,
}

/// Byte-accounted second-chance eviction with strict pinning. Reads use stable
/// access handles; only membership changes and eviction take the policy lock.
pub struct Lru {
    inner: Mutex<Inner>,
    eviction: Mutex<()>,
}

/// A cancellation-safe capacity-policy pin.
pub struct LruPin {
    lru: Arc<Lru>,
    unit: CacheUnit,
}
impl Drop for LruPin {
    fn drop(&mut self) {
        self.lru.unpin(&self.unit);
    }
}

struct Inner {
    entries: HashMap<Arc<CacheUnit>, Entry>,
    queue: BTreeMap<u64, Arc<CacheUnit>>,
    /// Only live units are indexed. `None` denotes a whole block; storing page
    /// indices avoids duplicating object paths and versions for every page.
    versions: HashMap<LogicalBlock, HashMap<Version, HashSet<Option<PageIndex>>>>,
    // Reference counts track resident units, not outstanding reader tokens.
    block_access: HashMap<BlockId, BlockActivity>,
    block_queue: BTreeMap<u64, BlockId>,
    object_checks: HashMap<ObjectId, (usize, Option<u64>)>,
    total_bytes: u64,
    clock: u64,
}

impl Inner {
    /// Move a resident unit to the back of the second-chance queue.
    fn rotate(&mut self, unit: &CacheUnit) -> u64 {
        let entry = self.entries.get_mut(unit).expect("tracked cache unit");
        let queued = self
            .queue
            .remove(&entry.position)
            .expect("queued cache unit");
        let position = self.clock;
        self.clock = self
            .clock
            .checked_add(1)
            .expect("eviction sequence exhausted");
        entry.position = position;
        self.queue.insert(position, queued);
        position
    }

    fn snapshot(&mut self, unit: CacheUnit) -> EvictionCandidate {
        self.entries[&unit].access.0.store(false, Ordering::Relaxed);
        let revision = self.rotate(&unit);
        self.entries
            .get_mut(&unit)
            .expect("tracked cache unit")
            .revision = revision;
        EvictionCandidate { unit, revision }
    }

    /// All removal paths must update both maps under the same lock.
    fn remove(&mut self, unit: &CacheUnit) -> Option<u64> {
        let entry = self.entries.remove(unit)?;
        self.queue.remove(&entry.position);
        let block = unit.block();
        let activity = self.block_access.get_mut(block).expect("tracked block");
        activity.units -= 1;
        activity.pins -= entry.pins as usize;
        if activity.units == 0 {
            let position = activity.position;
            self.block_access.remove(block);
            self.block_queue.remove(&position);
        }
        let check = self
            .object_checks
            .get_mut(&block.object)
            .expect("tracked object");
        check.0 -= 1;
        if check.0 == 0 {
            self.object_checks.remove(&block.object);
        }
        let key = LogicalBlock::from(block);
        let versions = self.versions.get_mut(&key).expect("tracked logical block");
        let units = versions.get_mut(&block.version).expect("tracked version");
        let removed = units.remove(&unit.page());
        debug_assert!(removed, "tracked cache unit missing from version index");
        if units.is_empty() {
            versions.remove(&block.version);
        } else if units.capacity() > 32 && units.len() < units.capacity() / 4 {
            // Release historical page capacity when a block becomes sparse.
            // Leave growth headroom and skip tiny sets to avoid reallocating
            // on every removal or when residency oscillates near a threshold.
            units.shrink_to(units.len() * 2);
        }
        if versions.is_empty() {
            self.versions.remove(&key);
        }
        Lru::subtract_bytes(&mut self.total_bytes, entry.bytes);
        Some(entry.bytes)
    }
}

impl Lru {
    fn add_bytes(total: &mut u64, bytes: u64) {
        debug_assert!(
            total.checked_add(bytes).is_some(),
            "LRU byte accounting overflow"
        );
        *total = total.saturating_add(bytes);
    }

    fn subtract_bytes(total: &mut u64, bytes: u64) {
        debug_assert!(*total >= bytes, "LRU byte accounting underflow");
        *total = total.saturating_sub(bytes);
    }

    /// Create an empty tracker.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                queue: BTreeMap::new(),
                versions: HashMap::new(),
                block_access: HashMap::new(),
                block_queue: BTreeMap::new(),
                object_checks: HashMap::new(),
                total_bytes: 0,
                clock: 0,
            }),
            eviction: Mutex::new(()),
        }
    }

    /// Total bytes currently tracked.
    pub fn total_bytes(&self) -> u64 {
        self.inner.lock().unwrap().total_bytes
    }

    /// Number of units currently tracked.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().entries.len()
    }

    /// Whether the tracker holds no units.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().entries.is_empty()
    }

    /// Insert or update a unit, returning its stable access token. Updating an
    /// existing admission preserves its pins and token.
    pub fn insert(&self, unit: CacheUnit, bytes: u64) -> AccessHandle {
        let mut g = self.inner.lock().unwrap();
        let position = g.clock;
        g.clock = g.clock.checked_add(1).expect("eviction sequence exhausted");
        if let Some(e) = g.entries.get_mut(&unit) {
            let old = e.bytes;
            e.bytes = bytes;
            e.access.touch();
            let access = e.access.clone();
            g.block_access
                .get_mut(unit.block())
                .expect("tracked block")
                .revision = position;
            Self::subtract_bytes(&mut g.total_bytes, old);
            Self::add_bytes(&mut g.total_bytes, bytes);
            return access;
        }
        Self::add_bytes(&mut g.total_bytes, bytes);
        let block = unit.block();
        g.versions
            .entry(LogicalBlock::from(block))
            .or_default()
            .entry(block.version.clone())
            .or_default()
            .insert(unit.page());
        if !g.block_access.contains_key(block) {
            g.block_queue.insert(position, block.clone());
            g.block_access.insert(
                block.clone(),
                BlockActivity {
                    units: 0,
                    pins: 0,
                    position,
                    revision: position,
                    access: Arc::new(AtomicU64::new(access_millis())),
                },
            );
        }
        let activity = g.block_access.get_mut(block).expect("tracked block");
        activity.units += 1;
        activity.revision = position;
        let access = AccessHandle(Arc::new(AtomicBool::new(false)), activity.access.clone());
        // New pages also refresh the shared block idle age.
        access.1.fetch_max(access_millis(), Ordering::Relaxed);
        g.object_checks
            .entry(block.object.clone())
            .or_insert((0, None))
            .0 += 1;
        let unit = Arc::new(unit);
        g.queue.insert(position, unit.clone());
        g.entries.insert(
            unit,
            Entry {
                bytes,
                position,
                revision: position,
                access: access.clone(),
                pins: 0,
            },
        );
        access
    }

    /// Compatibility lookup for callers without a resolved access token.
    /// Data-path reads should touch the token obtained with their index lookup.
    pub fn touch(&self, unit: &CacheUnit) {
        if let Some(e) = self.inner.lock().unwrap().entries.get(unit) {
            e.access.touch();
        }
    }

    /// Pin a unit so it cannot be evicted while an active reader holds it.
    ///
    /// Returns `true` if the unit exists.
    pub fn pin(&self, unit: &CacheUnit) -> bool {
        let mut g = self.inner.lock().unwrap();
        match g.entries.get_mut(unit) {
            Some(e) => {
                e.pins += 1;
                g.block_access
                    .get_mut(unit.block())
                    .expect("tracked block")
                    .pins += 1;
                true
            }
            None => false,
        }
    }

    pub fn pin_guard(self: &Arc<Self>, unit: CacheUnit) -> Option<LruPin> {
        self.pin(&unit).then(|| LruPin {
            lru: self.clone(),
            unit,
        })
    }

    /// Release one pin previously taken with [`pin`](Self::pin).
    pub fn unpin(&self, unit: &CacheUnit) {
        let mut g = self.inner.lock().unwrap();
        if let Some(e) = g.entries.get_mut(unit) {
            if e.pins > 0 {
                e.pins -= 1;
                g.block_access
                    .get_mut(unit.block())
                    .expect("tracked block")
                    .pins -= 1;
            }
        }
    }

    /// Remove a unit outright (e.g. explicit delete), returning its byte cost.
    pub fn remove(&self, unit: &CacheUnit) -> Option<u64> {
        self.inner.lock().unwrap().remove(unit)
    }

    /// Snapshot candidates without charging freed bytes before unlink succeeds.
    pub(crate) fn candidates_to_fit(
        &self,
        capacity: u64,
        excluded: &HashSet<CacheUnit>,
    ) -> Vec<EvictionCandidate> {
        self.candidates_to_fit_bounded(capacity, excluded, usize::MAX, usize::MAX)
    }

    /// Limit both policy walk work and retained candidates for background batches.
    pub(crate) fn candidates_to_fit_bounded(
        &self,
        capacity: u64,
        excluded: &HashSet<CacheUnit>,
        mut scan_budget: usize,
        delete_budget: usize,
    ) -> Vec<EvictionCandidate> {
        if self.total_bytes() <= capacity {
            return Vec::new();
        }
        let _eviction = self.eviction.lock().unwrap();
        let count = self.len();
        let mut selected = HashSet::new();
        let mut selected_bytes = 0_u64;
        let mut out = Vec::new();
        // Keep the upstream bounded second-chance walk and short lock batches.
        // Candidates remain resident and charged until unlink succeeds.
        for second_pass in [false, true] {
            let mut remaining = count;
            while remaining > 0 {
                let mut g = self.inner.lock().unwrap();
                let batch = remaining.min(64);
                for _ in 0..batch {
                    if scan_budget == 0
                        || out.len() == delete_budget
                        || g.total_bytes.saturating_sub(selected_bytes) <= capacity
                    {
                        return out;
                    }
                    scan_budget -= 1;
                    let Some((_, unit)) = g.queue.first_key_value() else {
                        return out;
                    };
                    let unit = unit.clone();
                    let entry = &g.entries[unit.as_ref()];
                    // Do not consume recency for an already selected unit:
                    // a touch after its snapshot must still invalidate it.
                    let eligible = entry.pins == 0
                        && !selected.contains(unit.as_ref())
                        && !excluded.contains(unit.as_ref());
                    let referenced = eligible && entry.access.0.swap(false, Ordering::Relaxed);
                    let victim = eligible && (second_pass || !referenced);
                    let bytes = entry.bytes;
                    let revision = g.rotate(&unit);
                    if eligible {
                        // Consuming recency must invalidate any older snapshot,
                        // even when this walk grants the unit another chance.
                        g.entries
                            .get_mut(unit.as_ref())
                            .expect("queued entry")
                            .revision = revision;
                    }
                    if victim {
                        selected_bytes = selected_bytes.saturating_add(bytes);
                        selected.insert((*unit).clone());
                        out.push(EvictionCandidate {
                            unit: (*unit).clone(),
                            revision,
                        });
                    }
                }
                remaining -= batch;
            }
        }
        out
    }

    /// Old-version candidates; removal is committed by the caller after I/O.
    pub(crate) fn superseded_candidates(&self, keep: &BlockId) -> Vec<EvictionCandidate> {
        let mut g = self.inner.lock().unwrap();
        let Some(versions) = g.versions.get(&LogicalBlock::from(keep)) else {
            return Vec::new();
        };
        let units: Vec<_> = versions
            .iter()
            .filter(|(version, _)| *version != &keep.version)
            .flat_map(|(version, units)| {
                units.iter().map(move |page| {
                    let block = BlockId::new(
                        keep.object.clone(),
                        keep.offset,
                        keep.block_size,
                        version.clone(),
                    );
                    match page {
                        None => CacheUnit::Whole(block),
                        Some(page) => CacheUnit::Page(block, *page),
                    }
                })
            })
            .filter(|unit| g.entries[unit].pins == 0)
            .collect();
        units.into_iter().map(|unit| g.snapshot(unit)).collect()
    }

    /// Snapshot resident units for an explicit block invalidation.
    pub(crate) fn block_candidates(&self, block: &BlockId) -> Vec<EvictionCandidate> {
        let mut g = self.inner.lock().unwrap();
        let Some(units) = g
            .versions
            .get(&LogicalBlock::from(block))
            .and_then(|versions| versions.get(&block.version))
        else {
            return Vec::new();
        };
        let units: Vec<_> = units
            .iter()
            .map(|page| match page {
                None => CacheUnit::Whole(block.clone()),
                Some(page) => CacheUnit::Page(block.clone(), *page),
            })
            .collect();
        units.into_iter().map(|unit| g.snapshot(unit)).collect()
    }

    /// Visit each block once, regardless of its number of cached pages. The
    /// admission-ordered block queue is separate from the capacity eviction hand.
    #[cfg(test)]
    pub(crate) fn stale_gc_candidates(
        &self,
        cursor: &mut (u64, u64),
        now: u64,
        age: u64,
        scan_budget: usize,
        delete_budget: usize,
    ) -> Vec<StaleBlockCandidate> {
        if cursor.0 >= cursor.1 {
            *cursor = (0, self.stale_scan_ceiling());
        }
        self.scan_stale_blocks(cursor, now, age, (scan_budget, delete_budget), false)
    }

    pub(crate) fn scan_stale_blocks(
        &self,
        cursor: &mut (u64, u64),
        now: u64,
        age: u64,
        limits: (usize, usize),
        ownership: bool,
    ) -> Vec<StaleBlockCandidate> {
        let (scan_budget, delete_budget) = limits;
        let mut out = Vec::new();
        for _ in 0..scan_budget {
            let g = self.inner.lock().unwrap();
            let Some((&position, block)) = g.block_queue.range(cursor.0..cursor.1).next() else {
                cursor.0 = cursor.1;
                break;
            };
            cursor.0 = position + 1;
            let activity = &g.block_access[block];
            let access = activity.access.load(Ordering::Relaxed);
            let verified = g.object_checks[&block.object].1;
            if !ownership
                && (activity.pins != 0
                    || now.saturating_sub(access) <= age
                    || verified.is_some_and(|at| now.saturating_sub(at) < age))
            {
                continue;
            }
            out.push(StaleBlockCandidate {
                block: block.clone(),
                units: activity.units,
                revision: activity.revision,
                last_access: access,
                position,
            });
            if out.len() >= delete_budget {
                break;
            }
        }
        out
    }

    /// Reuse the admission queue for deferred ownership work, outside this
    /// round's frozen boundary. There is no second per-block index.
    pub(crate) fn defer_stale_block(&self, block: &BlockId) {
        let mut g = self.inner.lock().unwrap();
        let position = g.clock;
        let Some(activity) = g.block_access.get_mut(block) else {
            return;
        };
        let old = std::mem::replace(&mut activity.position, position);
        g.clock = g.clock.checked_add(1).expect("eviction sequence exhausted");
        g.block_queue.remove(&old);
        g.block_queue.insert(position, block.clone());
    }

    pub(crate) fn stale_scan_ceiling(&self) -> u64 {
        self.inner.lock().unwrap().clock
    }

    pub(crate) fn stale_candidate_is_current(&self, candidate: &StaleBlockCandidate) -> bool {
        self.inner
            .lock()
            .unwrap()
            .block_access
            .get(&candidate.block)
            .is_some_and(|entry| {
                entry.pins == 0
                    && entry.revision == candidate.revision
                    && entry.access.load(Ordering::Relaxed) == candidate.last_access
            })
    }

    /// Called under the block mutation gate after its complete on-disk deletion.
    pub(crate) fn remove_block(&self, block: &BlockId) {
        let mut g = self.inner.lock().unwrap();
        let Some(pages) = g
            .versions
            .get(&LogicalBlock::from(block))
            .and_then(|versions| versions.get(&block.version))
        else {
            return;
        };
        let pages: Vec<_> = pages.iter().copied().collect();
        for page in pages {
            let unit = match page {
                Some(page) => CacheUnit::Page(block.clone(), page),
                None => CacheUnit::Whole(block.clone()),
            };
            g.remove(&unit);
        }
    }

    pub(crate) fn stale_check_due(&self, object: &ObjectId, now: u64, age: u64) -> bool {
        self.inner
            .lock()
            .unwrap()
            .object_checks
            .get(object)
            .is_some_and(|(_, at)| at.map_or(true, |at| now.saturating_sub(at) >= age))
    }

    pub(crate) fn stale_verified(&self, object: &ObjectId, now: u64) {
        if let Some(check) = self.inner.lock().unwrap().object_checks.get_mut(object) {
            check.1 = Some(now);
        }
    }

    /// Recheck with the block mutation gate held. Commits also pin under that gate.
    pub(crate) fn candidate_is_current(&self, candidate: &EvictionCandidate) -> bool {
        self.inner
            .lock()
            .unwrap()
            .entries
            .get(&candidate.unit)
            .is_some_and(|entry| {
                entry.pins == 0
                    && entry.revision == candidate.revision
                    && !entry.access.0.load(Ordering::Relaxed)
            })
    }

    /// Evict and return every *superseded* unit — whole block or page — for the
    /// same `(object, offset, block_size)` as `keep` but a different version.
    ///
    /// When an object is overwritten its new ETag yields a new [`BlockId`] and a
    /// new `.blk` file (or `.pages` directory), while the old version's files
    /// would otherwise stay resident forever (issue #159, compounding #119).
    /// Called on commit of a fresh version, this reclaims the stale sibling(s)
    /// immediately. Pinned units (an in-flight reader still serving the old
    /// bytes) are left alone.
    ///
    /// Looks up only this logical block's versions and visits units belonging
    /// to other versions. With only `keep` resident, no cache units are scanned,
    /// regardless of the number of pages in this block or the rest of the cache.
    pub fn evict_superseded(&self, keep: &BlockId) -> Vec<CacheUnit> {
        let mut g = self.inner.lock().unwrap();
        let Some(versions) = g.versions.get(&LogicalBlock::from(keep)) else {
            return Vec::new();
        };
        if versions.len() == 1 && versions.contains_key(&keep.version) {
            return Vec::new();
        }
        let victims: Vec<CacheUnit> = versions
            .iter()
            .filter(|(version, _)| *version != &keep.version)
            .flat_map(|(version, units)| {
                units.iter().map(move |page| {
                    let block = BlockId::new(
                        keep.object.clone(),
                        keep.offset,
                        keep.block_size,
                        version.clone(),
                    );
                    match page {
                        None => CacheUnit::Whole(block),
                        Some(page) => CacheUnit::Page(block, *page),
                    }
                })
            })
            .filter(|unit| g.entries.get(unit).expect("indexed cache unit").pins == 0)
            .collect();
        for unit in &victims {
            g.remove(unit);
        }
        victims
    }

    /// Reclaim capacity with a bounded second-chance walk. Queue membership is
    /// exact: explicit/version removals also erase their queue nodes, so churn
    /// cannot accumulate stale candidates. Each candidate costs O(log N), with
    /// no full-map search per victim. Hits never reorder this queue.
    ///
    /// Two bounded passes guarantee progress even under continuous touches;
    /// the second pass may reclaim recently touched units but never pinned ones.
    /// Policy locks are released every 64 candidates to bound mutation stalls.
    pub fn evict_to_fit(&self, capacity: u64) -> Vec<CacheUnit> {
        // Most admissions do not need eviction or its serialization lock.
        if self.total_bytes() <= capacity {
            return Vec::new();
        }
        let _eviction = self.eviction.lock().unwrap();
        let count = self.len();
        let mut evicted = Vec::new();
        for second_pass in [false, true] {
            let mut remaining = count;
            while remaining > 0 {
                let mut g = self.inner.lock().unwrap();
                let batch = remaining.min(64);
                for _ in 0..batch {
                    if g.total_bytes <= capacity {
                        return evicted;
                    }
                    let Some((position, unit)) = g.queue.first_key_value() else {
                        return evicted;
                    };
                    let position = *position;
                    let unit = unit.clone();
                    let e = g.entries.get(unit.as_ref()).expect("queued entry");
                    let referenced = e.access.0.swap(false, Ordering::Relaxed);
                    if e.pins == 0 && (second_pass || !referenced) {
                        g.remove(&unit);
                        evicted.push((*unit).clone());
                    } else {
                        g.queue.remove(&position);
                        let next = g.clock;
                        g.clock = g.clock.checked_add(1).expect("eviction sequence exhausted");
                        let entry = g.entries.get_mut(unit.as_ref()).expect("queued entry");
                        entry.position = next;
                        entry.revision = next;
                        g.queue.insert(next, unit);
                    }
                }
                remaining -= batch;
            }
        }
        evicted
    }
}

impl Default for Lru {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{Backend, ObjectId, Version};

    fn blk(n: u64) -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "b", format!("o/{n}")),
            0,
            256 << 20,
            Version::new("v1"),
        )
    }

    fn whole(n: u64) -> CacheUnit {
        CacheUnit::Whole(blk(n))
    }

    #[test]
    fn stale_gc_age_boundary_and_sibling_activity() {
        let lru = Lru::new();
        let first = CacheUnit::Page(blk(1), PageIndex(0));
        let second = CacheUnit::Page(blk(1), PageIndex(1));
        let access = lru.insert(first, 16);
        lru.insert(second.clone(), 16);
        access.1.store(100, Ordering::Relaxed);
        let mut cursor = (0, 0);
        assert!(lru
            .stale_gc_candidates(&mut cursor, 200, 100, 8, 8)
            .is_empty());
        let candidates = lru.stale_gc_candidates(&mut cursor, 201, 100, 8, 8);
        assert_eq!(candidates.len(), 1);
        assert!(candidates.iter().all(|c| lru.stale_candidate_is_current(c)));
        // A touch of either page invalidates the block candidate.
        access.1.store(201, Ordering::Relaxed);
        lru.touch(&second);
        assert!(candidates
            .iter()
            .all(|c| !lru.stale_candidate_is_current(c)));
    }

    #[test]
    fn stale_gc_admission_replacement_and_metadata_retirement() {
        let lru = Lru::new();
        let unit = whole(1);
        let old = lru.insert(unit.clone(), 16);
        old.1.store(1, Ordering::Relaxed);
        let candidates = lru.stale_gc_candidates(&mut (0, 0), 200, 100, 8, 8);
        assert_eq!(candidates.len(), 1);
        lru.stale_verified(&unit.block().object, 200);
        lru.remove(&unit);
        {
            let g = lru.inner.lock().unwrap();
            assert!(g.block_access.is_empty());
            assert!(g.block_queue.is_empty());
            assert!(g.object_checks.is_empty());
        }
        let new = lru.insert(unit, 16);
        assert!(!Arc::ptr_eq(&old.1, &new.1));
        assert!(!lru.stale_candidate_is_current(&candidates[0]));
        assert!(lru
            .inner
            .lock()
            .unwrap()
            .object_checks
            .values()
            .all(|(_, at)| at.is_none()));
    }

    #[test]
    fn deferred_candidates_keep_accounting_and_skip_protected_units() {
        let lru = Lru::new();
        let hot = lru.insert(whole(1), 10);
        lru.insert(whole(2), 10);
        lru.insert(whole(3), 10);
        lru.insert(whole(4), 10);
        hot.touch();
        lru.pin(&whole(3));
        let candidates = lru.candidates_to_fit(20, &HashSet::from([whole(4)]));
        assert_eq!(
            candidates
                .iter()
                .map(|c| c.unit.clone())
                .collect::<Vec<_>>(),
            vec![whole(2), whole(1)]
        );
        assert_eq!(lru.total_bytes(), 40);
        for candidate in candidates {
            assert!(lru.candidate_is_current(&candidate));
            lru.remove(&candidate.unit);
        }
        assert_eq!(lru.total_bytes(), 20);
    }

    #[test]
    fn deferred_candidates_survive_a_full_walk_past_pinned_units() {
        let lru = Lru::new();
        lru.insert(whole(1), 10);
        lru.insert(whole(2), 10);
        lru.pin(&whole(2));
        let candidates = lru.candidates_to_fit(0, &HashSet::new());
        assert_eq!(candidates.len(), 1);
        assert!(lru.candidate_is_current(&candidates[0]));
        assert_eq!(lru.total_bytes(), 20);
    }

    #[test]
    fn stable_handle_touch_invalidates_a_deferred_candidate_after_later_walks() {
        let lru = Lru::new();
        let handle = lru.insert(whole(1), 10);
        let candidate = lru.candidates_to_fit(0, &HashSet::new()).pop().unwrap();
        assert!(lru.candidate_is_current(&candidate));
        handle.touch();
        assert!(!lru.candidate_is_current(&candidate));
        // A second selection clears recency, but must not revive the old snapshot.
        let replacement = lru.candidates_to_fit(0, &HashSet::new()).pop().unwrap();
        assert!(!lru.candidate_is_current(&candidate));
        assert!(lru.candidate_is_current(&replacement));
        lru.remove(&whole(1));
        lru.insert(whole(1), 10);
        assert!(!lru.candidate_is_current(&replacement));
    }

    #[test]
    fn version_candidates_use_live_index_without_charging_before_unlink() {
        let lru = Lru::new();
        let old = blk(1);
        let mut keep = old.clone();
        keep.version = Version::new("v2");
        let page = CacheUnit::Page(old.clone(), PageIndex(0));
        let pinned = CacheUnit::Page(old.clone(), PageIndex(1));
        let current = CacheUnit::Page(keep.clone(), PageIndex(0));
        let access = lru.insert(page.clone(), 10);
        lru.insert(pinned.clone(), 10);
        lru.insert(current, 10);
        lru.insert(whole(2), 10);
        lru.pin(&pinned);
        let candidates = lru.superseded_candidates(&keep);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].unit, page);
        assert!(lru.candidate_is_current(&candidates[0]));
        assert_eq!(lru.total_bytes(), 40);
        access.touch();
        assert!(!lru.candidate_is_current(&candidates[0]));
        let refreshed = lru.block_candidates(&old);
        assert_eq!(refreshed.len(), 2);
        assert!(!lru.candidate_is_current(&candidates[0]));
        for candidate in refreshed {
            assert_eq!(lru.candidate_is_current(&candidate), candidate.unit == page);
        }
    }

    #[test]
    fn stable_handle_never_touches_a_replacement_admission() {
        let lru = Lru::new();
        let old = lru.insert(whole(1), 10);
        lru.remove(&whole(1));
        lru.insert(whole(1), 10);
        lru.insert(whole(2), 10);
        old.touch();
        assert_eq!(lru.evict_to_fit(10), vec![whole(1)]);
    }

    #[test]
    fn handle_hits_do_not_need_the_policy_lock() {
        let lru = Lru::new();
        let handle = lru.insert(whole(1), 10);
        lru.insert(whole(2), 10);
        let guard = lru.inner.lock().unwrap();
        // This would deadlock if a token lookup took the policy lock.
        handle.touch();
        drop(guard);
        assert_eq!(lru.evict_to_fit(10), vec![whole(2)]);
    }

    #[test]
    fn removal_churn_keeps_one_queue_node_per_resident_unit() {
        let lru = Lru::new();
        for i in 0..4096 {
            lru.insert(whole(i % 8), 10);
            lru.insert(whole(i % 8), 20);
            if i % 3 == 0 {
                lru.remove(&whole(i % 8));
            }
            let g = lru.inner.lock().unwrap();
            assert_eq!(g.queue.len(), g.entries.len());
            for (position, unit) in &g.queue {
                assert_eq!(g.entries[unit].position, *position);
            }
        }
        lru.evict_to_fit(0);
        let g = lru.inner.lock().unwrap();
        assert!(g.queue.is_empty());
        assert!(g.versions.is_empty());
    }

    #[test]
    fn continuously_touched_entries_do_not_prevent_capacity_reclamation() {
        let lru = Lru::new();
        let handles: Vec<_> = (0..2048).map(|i| lru.insert(whole(i), 10)).collect();
        for i in 0..16 {
            assert!(lru.pin(&whole(i)));
        }
        let stop = AtomicBool::new(false);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                while !stop.load(Ordering::Relaxed) {
                    for access in &handles {
                        access.touch();
                    }
                }
            });
            for access in &handles {
                access.touch();
            }
            let evicted = lru.evict_to_fit(160);
            stop.store(true, Ordering::Relaxed);
            assert_eq!(evicted.len(), 2032);
            assert_eq!(lru.total_bytes(), 160);
            for i in 0..16 {
                assert!(!evicted.contains(&whole(i)));
            }
        });
    }

    #[test]
    fn coldest_bytes_evicted_first() {
        let lru = Lru::new();
        lru.insert(whole(1), 100);
        lru.insert(whole(2), 100);
        lru.insert(whole(3), 100);
        assert_eq!(lru.total_bytes(), 300);

        // Touch 1 so 2 becomes the coldest.
        lru.touch(&whole(1));

        let evicted = lru.evict_to_fit(150);
        // Need to drop 150 bytes -> evict two coldest: 2 then 3.
        assert_eq!(evicted, vec![whole(2), whole(3)]);
        assert_eq!(lru.total_bytes(), 100);
        assert_eq!(lru.len(), 1);
    }

    #[test]
    fn pinned_units_are_not_evicted() {
        let lru = Lru::new();
        lru.insert(whole(1), 100);
        lru.insert(whole(2), 100);
        // Pin the coldest unit (1) — it must survive even under pressure.
        assert!(lru.pin(&whole(1)));

        let evicted = lru.evict_to_fit(0);
        assert_eq!(evicted, vec![whole(2)]);
        assert_eq!(lru.total_bytes(), 100); // pinned unit remains
        assert!(lru.len() == 1);

        // After unpinning, it can be evicted.
        lru.unpin(&whole(1));
        let evicted = lru.evict_to_fit(0);
        assert_eq!(evicted, vec![whole(1)]);
        assert!(lru.is_empty());
    }

    #[test]
    fn accounting_consistent_across_ops() {
        let lru = Lru::new();
        lru.insert(whole(1), 100);
        lru.insert(whole(1), 250); // update same unit
        assert_eq!(lru.total_bytes(), 250);
        lru.insert(whole(1), 50); // shrink same unit
        assert_eq!(lru.total_bytes(), 50);
        assert_eq!(lru.len(), 1);

        assert_eq!(lru.remove(&whole(1)), Some(50));
        assert_eq!(lru.total_bytes(), 0);
        assert_eq!(lru.remove(&whole(1)), None);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "LRU byte accounting underflow")]
    fn accounting_underflow_is_detected_in_debug_builds() {
        Lru::subtract_bytes(&mut 0, 1);
    }

    #[cfg(not(debug_assertions))]
    #[test]
    fn accounting_underflow_saturates_in_release_builds() {
        let mut total = 0;
        Lru::subtract_bytes(&mut total, 1);
        assert_eq!(total, 0);
    }

    #[test]
    fn page_units_evict_independently() {
        let lru = Lru::new();
        let b = blk(9);
        lru.insert(CacheUnit::Page(b.clone(), PageIndex(0)), 64);
        lru.insert(CacheUnit::Page(b.clone(), PageIndex(1)), 64);
        lru.touch(&CacheUnit::Page(b.clone(), PageIndex(1)));

        let evicted = lru.evict_to_fit(64);
        assert_eq!(evicted, vec![CacheUnit::Page(b.clone(), PageIndex(0))]);
        assert_eq!(lru.total_bytes(), 64);
    }

    #[test]
    fn evict_superseded_reclaims_only_other_versions() {
        // Two versions of the same (object, offset), a different offset of the
        // same object, and a pinned old version.
        let obj = ObjectId::new(Backend::S3, "b", "same");
        let v1 = CacheUnit::Whole(BlockId::new(obj.clone(), 0, 256 << 20, Version::new("v1")));
        let v2 = CacheUnit::Whole(BlockId::new(obj.clone(), 0, 256 << 20, Version::new("v2")));
        let other_offset = CacheUnit::Whole(BlockId::new(
            obj.clone(),
            256 << 20,
            256 << 20,
            Version::new("v1"),
        ));
        let other_obj = whole(42);
        let lru = Lru::new();
        for (u, b) in [
            (&v1, 100),
            (&v2, 100),
            (&other_offset, 100),
            (&other_obj, 100),
        ] {
            lru.insert(u.clone(), b);
        }

        let CacheUnit::Whole(keep) = v2.clone() else {
            unreachable!()
        };
        let evicted = lru.evict_superseded(&keep);
        // Only v1 (same object+offset+block_size, different version) is reclaimed.
        assert_eq!(evicted, vec![v1.clone()]);
        assert_eq!(lru.total_bytes(), 300);
        assert!(lru.remove(&v2).is_some());
        assert!(lru.remove(&other_offset).is_some());
        assert!(lru.remove(&other_obj).is_some());
        assert!(lru.remove(&v1).is_none());
    }

    #[test]
    fn evict_superseded_skips_pinned_old_version() {
        let obj = ObjectId::new(Backend::S3, "b", "same");
        let v1 = CacheUnit::Whole(BlockId::new(obj.clone(), 0, 256 << 20, Version::new("v1")));
        let v2 = CacheUnit::Whole(BlockId::new(obj.clone(), 0, 256 << 20, Version::new("v2")));
        let lru = Lru::new();
        lru.insert(v1.clone(), 100);
        lru.insert(v2.clone(), 100);
        // An in-flight reader still serving the old bytes pins v1.
        assert!(lru.pin(&v1));

        let CacheUnit::Whole(keep) = v2.clone() else {
            unreachable!()
        };
        assert!(lru.evict_superseded(&keep).is_empty());
        assert_eq!(lru.total_bytes(), 200);
        lru.unpin(&v1);
        assert_eq!(lru.evict_superseded(&keep), vec![v1]);
        assert_eq!(lru.total_bytes(), 100);
    }

    #[test]
    fn superseded_pages_and_whole_blocks_preserve_pins_and_current_pages() {
        let lru = Lru::new();
        let old = blk(1);
        let mut keep = old.clone();
        keep.version = Version::new("v2");
        let mut older = old.clone();
        older.version = Version::new("v0");
        let old_page = CacheUnit::Page(old.clone(), PageIndex(0));
        let pinned_page = CacheUnit::Page(old.clone(), PageIndex(1));
        let old_whole = CacheUnit::Whole(old);
        let older_page = CacheUnit::Page(older, PageIndex(0));
        let current_page = CacheUnit::Page(keep.clone(), PageIndex(0));
        for unit in [
            &old_page,
            &pinned_page,
            &old_whole,
            &older_page,
            &current_page,
        ] {
            lru.insert(unit.clone(), 10);
        }
        assert!(lru.pin(&pinned_page));
        assert!(lru.pin(&pinned_page));
        // Updating a page must neither duplicate its index entry nor reset pins.
        lru.insert(pinned_page.clone(), 20);
        lru.unpin(&pinned_page);

        let victims: HashSet<_> = lru.evict_superseded(&keep).into_iter().collect();
        assert_eq!(victims, HashSet::from([old_page, old_whole, older_page]));
        assert_eq!(lru.total_bytes(), 30);
        assert_eq!(lru.len(), 2);
        assert!(lru.evict_superseded(&keep).is_empty());

        lru.unpin(&pinned_page);
        assert_eq!(lru.evict_superseded(&keep), vec![pinned_page]);
        assert_eq!(lru.total_bytes(), 10);
        assert_eq!(lru.remove(&current_page), Some(10));
        assert!(lru.inner.lock().unwrap().versions.is_empty());
    }

    #[test]
    fn superseded_lookup_respects_all_logical_block_fields() {
        let lru = Lru::new();
        let old = blk(1);
        let mut keep = old.clone();
        keep.version = Version::new("v2");
        let mut different_blocks = vec![old.clone(); 5];
        different_blocks[0].object.backend = Backend::Gcs;
        different_blocks[1].object.bucket = "other-bucket".into();
        different_blocks[2].object.object_path = "other-path".into();
        different_blocks[3].offset += u64::from(old.block_size);
        different_blocks[4].block_size /= 2;
        let unrelated: Vec<_> = different_blocks
            .into_iter()
            .map(|block| CacheUnit::Page(block, PageIndex(0)))
            .collect();
        let victim = CacheUnit::Page(old, PageIndex(0));
        lru.insert(victim.clone(), 10);
        for unit in &unrelated {
            lru.insert(unit.clone(), 10);
        }
        // `keep` need not already be tracked to reclaim its other versions.
        assert_eq!(lru.evict_superseded(&keep), vec![victim]);
        assert!(lru.evict_superseded(&keep).is_empty());
        assert_eq!(lru.total_bytes(), 50);
        for unit in unrelated {
            assert_eq!(lru.remove(&unit), Some(10));
        }
        assert!(lru.inner.lock().unwrap().versions.is_empty());
    }

    #[test]
    fn version_index_tracks_explicit_and_capacity_removal_and_reinsertion() {
        let lru = Lru::new();
        let old = blk(1);
        let mut keep = old.clone();
        keep.version = Version::new("v2");
        let page0 = CacheUnit::Page(old.clone(), PageIndex(0));
        let page1 = CacheUnit::Page(old, PageIndex(1));
        let current = CacheUnit::Whole(keep.clone());
        lru.insert(page0.clone(), 10);
        lru.insert(page1.clone(), 20);
        lru.insert(current.clone(), 30);

        assert_eq!(lru.remove(&page0), Some(10));
        assert_eq!(lru.remove(&page0), None);
        assert_eq!(lru.evict_to_fit(30), vec![page1.clone()]);
        assert!(lru.evict_superseded(&keep).is_empty());
        {
            let g = lru.inner.lock().unwrap();
            let versions = &g.versions[&LogicalBlock::from(&keep)];
            assert_eq!(versions.len(), 1);
            assert_eq!(versions[&keep.version], HashSet::from([None]));
        }
        // The same identities can be admitted again after either removal path.
        lru.insert(page0.clone(), 40);
        lru.insert(page1.clone(), 50);
        let victims: HashSet<_> = lru.evict_superseded(&keep).into_iter().collect();
        assert_eq!(victims, HashSet::from([page0, page1]));
        assert_eq!(lru.total_bytes(), 30);
        assert_eq!(lru.evict_to_fit(0), vec![current.clone()]);
        assert!(lru.inner.lock().unwrap().versions.is_empty());
        lru.insert(current.clone(), 60);
        assert!(lru.evict_superseded(&keep).is_empty());
        assert_eq!(lru.remove(&current), Some(60));
        assert_eq!(lru.total_bytes(), 0);
        assert!(lru.inner.lock().unwrap().versions.is_empty());
    }

    #[test]
    fn sparse_version_index_releases_capacity_after_page_eviction() {
        let lru = Lru::new();
        let page_bytes = 64 << 10;
        // Fill and drain each block while earlier blocks retain a pinned hot
        // page. The index must follow residency rather than each block's peak.
        for n in 0..8 {
            let block = blk(n);
            let hot = CacheUnit::Page(block.clone(), PageIndex(0));
            for page in 0..512 {
                lru.insert(CacheUnit::Page(block.clone(), PageIndex(page)), page_bytes);
            }
            assert!(lru.pin(&hot));
            assert_eq!(lru.evict_to_fit((n + 1) * page_bytes).len(), 511,);
        }
        assert_eq!(lru.len(), 8);
        assert_eq!(lru.total_bytes(), 8 * page_bytes);
        let g = lru.inner.lock().unwrap();
        for versions in g.versions.values() {
            let pages = &versions[&Version::new("v1")];
            assert_eq!(pages, &HashSet::from([Some(PageIndex(0))]));
            assert!(pages.capacity() <= 64, "sparse set retains peak capacity");
        }
    }

    #[test]
    fn sparse_version_index_preserves_pins_and_regrows_after_shrinking() {
        let lru = Lru::new();
        let old = blk(1);
        let mut keep = old.clone();
        keep.version = Version::new("v2");
        let hot = CacheUnit::Page(old.clone(), PageIndex(0));
        let current = CacheUnit::Whole(keep.clone());
        lru.insert(hot.clone(), 1);
        assert!(lru.pin(&hot));
        lru.insert(current.clone(), 1);

        for explicit in [true, false] {
            for page in 1..512 {
                lru.insert(CacheUnit::Page(old.clone(), PageIndex(page)), 1);
            }
            if explicit {
                for page in 1..512 {
                    assert_eq!(
                        lru.remove(&CacheUnit::Page(old.clone(), PageIndex(page))),
                        Some(1),
                    );
                }
            } else {
                assert_eq!(lru.evict_superseded(&keep).len(), 511);
            }
            assert_eq!(lru.len(), 2);
            assert_eq!(lru.total_bytes(), 2);
            assert!(lru.evict_superseded(&keep).is_empty());
            let g = lru.inner.lock().unwrap();
            let pages = &g.versions[&LogicalBlock::from(&old)][&old.version];
            assert_eq!(pages, &HashSet::from([Some(PageIndex(0))]));
            assert!(pages.capacity() <= 64, "sparse set retains peak capacity");
        }

        lru.unpin(&hot);
        assert_eq!(lru.evict_superseded(&keep), vec![hot]);
        assert_eq!(lru.remove(&current), Some(1));
        assert!(lru.inner.lock().unwrap().versions.is_empty());
    }

    #[test]
    fn filling_current_pages_does_not_reclaim_them() {
        let lru = Lru::new();
        let block = blk(1);
        for page in 0..4096 {
            lru.insert(CacheUnit::Page(block.clone(), PageIndex(page)), 64);
            assert!(lru.evict_superseded(&block).is_empty());
        }
        assert_eq!(lru.len(), 4096);
        assert_eq!(lru.total_bytes(), 4096 * 64);
    }
}
