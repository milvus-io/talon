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

use talon_core::{BlockId, ObjectId, PageIndex};

mod recovery;
pub(crate) use recovery::RecoveredUnit;

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
}

/// Block identity borrowed from a resident unit. The block map and its scan
/// queues share that unit's immutable strings, even if this particular page is
/// removed before its siblings. This retains metadata only, never a data pin.
#[derive(Clone, Debug)]
struct BlockKey(Arc<CacheUnit>);

impl AsRef<BlockId> for BlockKey {
    fn as_ref(&self) -> &BlockId {
        self.0.block()
    }
}

impl std::borrow::Borrow<BlockId> for BlockKey {
    fn borrow(&self) -> &BlockId {
        self.as_ref()
    }
}

impl PartialEq for BlockKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for BlockKey {}

impl std::hash::Hash for BlockKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(self.as_ref(), state);
    }
}

/// Stable recency token for one resident unit. Keeping this token does not pin
/// data. After removal it cannot affect a new admission of the same identity.
#[derive(Clone)]
pub struct AccessHandle(
    Arc<AtomicBool>,
    Arc<AtomicU64>,
    Option<Arc<crate::small_store::DirtyAccess>>,
);

/// Anchored wall time: monotonic within a process and portable across restarts.
pub(crate) fn access_millis() -> u64 {
    static CLOCK: OnceLock<crate::page_lifecycle::AccessClock> = OnceLock::new();
    CLOCK
        .get_or_init(crate::page_lifecycle::AccessClock::new)
        .now()
}

impl Default for AccessHandle {
    fn default() -> Self {
        Self(
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicU64::new(access_millis())),
            None,
        )
    }
}

impl AccessHandle {
    pub(crate) fn clock(&self) -> &Arc<AtomicU64> {
        &self.1
    }

    pub(crate) fn track_small(mut self, dirty: Arc<crate::small_store::DirtyAccess>) -> Self {
        self.2 = Some(dirty);
        self
    }

    pub(crate) fn sample_small(&self) -> u64 {
        if let Some(dirty) = &self.2 {
            dirty.consume();
        }
        self.last_access()
    }

    pub(crate) fn last_access(&self) -> u64 {
        self.1.load(Ordering::Relaxed)
    }

    /// Startup only, before serving. A clock rollback must not postpone expiry
    /// beyond one fresh idle window; a missing/invalid age starts that window.
    pub(crate) fn restore(&self, last_access: u64) {
        let now = access_millis();
        self.1.store(
            if last_access == 0 {
                now
            } else {
                last_access.min(now)
            },
            Ordering::Relaxed,
        );
    }

    /// Separate second-chance bit, with the same block-wide idle clock.
    pub(crate) fn sibling(&self) -> Self {
        Self(Arc::new(AtomicBool::new(false)), self.1.clone(), None)
    }

    /// Coalesce hits until the eviction hand consumes this second chance.
    /// This is advisory recency only, so relaxed ordering is sufficient.
    pub fn touch(&self) {
        let now = access_millis();
        if self.1.load(Ordering::Relaxed) < now {
            self.1.fetch_max(now, Ordering::Relaxed);
        }
        if let Some(dirty) = &self.2 {
            dirty.touch();
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
    pub revision: u64,
    last_access: u64,
}

struct BlockActivity {
    units: usize,
    pins: usize,
    position: u64,
    revision: u64,
    // One notification coalesces admissions until ownership observes the block.
    ownership_position: u64,
    ownership_pending: bool,
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
    // Reference counts track resident units, not outstanding reader tokens.
    block_access: HashMap<BlockKey, BlockActivity>,
    block_queue: BTreeMap<u64, BlockKey>,
    ownership_queue: BTreeMap<u64, BlockKey>,
    object_checks: HashMap<ObjectId, (usize, Option<u64>)>,
    track_checks: bool,
    total_bytes: u64,
    clock: u64,
    // Unlike clock, this does not advance for capacity-policy rotations or reads.
    admission_ceiling: u64,
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
            let ownership_position = activity.ownership_position;
            self.ownership_queue.remove(&ownership_position);
            self.block_access.remove(block);
            self.block_queue.remove(&position);
        }
        if let Some(check) = self.object_checks.get_mut(&block.object) {
            check.0 -= 1;
            if check.0 == 0 {
                self.object_checks.remove(&block.object);
            }
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
        Self::with_object_checks(true)
    }

    pub(crate) fn with_object_checks(track_checks: bool) -> Self {
        Self {
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                queue: BTreeMap::new(),
                block_access: HashMap::new(),
                block_queue: BTreeMap::new(),
                ownership_queue: BTreeMap::new(),
                object_checks: HashMap::new(),
                track_checks,
                total_bytes: 0,
                clock: 0,
                admission_ceiling: 0,
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
        g.admission_ceiling = g.clock;
        if let Some(activity) = g.block_access.get_mut(unit.block()) {
            if !activity.ownership_pending {
                let previous = activity.ownership_position;
                activity.ownership_position = position;
                activity.ownership_pending = true;
                let block = g.ownership_queue.remove(&previous).expect("queued block");
                g.ownership_queue.insert(position, block);
            }
        }
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
        let unit = Arc::new(unit);
        Self::add_bytes(&mut g.total_bytes, bytes);
        let block = unit.block();
        if !g.block_access.contains_key(block) {
            let key = BlockKey(unit.clone());
            g.block_queue.insert(position, key.clone());
            g.ownership_queue.insert(position, key.clone());
            g.block_access.insert(
                key,
                BlockActivity {
                    units: 0,
                    pins: 0,
                    position,
                    revision: position,
                    ownership_position: position,
                    ownership_pending: true,
                    access: Arc::new(AtomicU64::new(access_millis())),
                },
            );
        }
        let activity = g.block_access.get_mut(block).expect("tracked block");
        activity.units += 1;
        activity.revision = position;
        let access = AccessHandle(
            Arc::new(AtomicBool::new(false)),
            activity.access.clone(),
            None,
        );
        // New pages also refresh the shared block idle age.
        access.1.fetch_max(access_millis(), Ordering::Relaxed);
        if g.track_checks {
            g.object_checks
                .entry(block.object.clone())
                .or_insert((0, None))
                .0 += 1;
        }
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

    /// Snapshot exact units supplied by the residency index; absent units may
    /// have been removed since the caller collected them.
    pub(crate) fn candidates(
        &self,
        units: impl IntoIterator<Item = CacheUnit>,
    ) -> Vec<EvictionCandidate> {
        let mut g = self.inner.lock().unwrap();
        units
            .into_iter()
            .filter_map(|unit| g.entries.contains_key(&unit).then(|| g.snapshot(unit)))
            .collect()
    }

    /// Visit each block once, regardless of its number of cached pages. The
    /// admission-ordered block queue is separate from the capacity eviction hand.
    pub(crate) fn stale_gc_candidates(
        &self,
        cursor: &mut (u64, u64),
        now: u64,
        age: u64,
        scan_budget: usize,
        delete_budget: usize,
    ) -> Vec<StaleBlockCandidate> {
        self.age_candidates(cursor, now, age, (scan_budget, delete_budget), true)
    }

    pub(crate) fn idle_candidates(
        &self,
        cursor: &mut (u64, u64),
        now: u64,
        age: u64,
        scan_budget: usize,
        delete_budget: usize,
    ) -> Vec<StaleBlockCandidate> {
        self.age_candidates(cursor, now, age, (scan_budget, delete_budget), false)
    }

    fn age_candidates(
        &self,
        cursor: &mut (u64, u64),
        now: u64,
        age: u64,
        (scan_budget, delete_budget): (usize, usize),
        consider_verified: bool,
    ) -> Vec<StaleBlockCandidate> {
        let mut out = Vec::new();
        if cursor.0 >= cursor.1 {
            *cursor = (0, self.inner.lock().unwrap().clock);
        }
        for _ in 0..scan_budget {
            let g = self.inner.lock().unwrap();
            let Some((&position, block)) = g.block_queue.range(cursor.0..cursor.1).next() else {
                cursor.0 = cursor.1;
                break;
            };
            cursor.0 = position + 1;
            let activity = &g.block_access[block];
            let access = activity.access.load(Ordering::Relaxed);
            let verified = g
                .object_checks
                .get(&block.as_ref().object)
                .and_then(|check| check.1);
            if activity.pins != 0
                || now.saturating_sub(access) <= age
                || (consider_verified && verified.is_some_and(|at| now.saturating_sub(at) < age))
            {
                continue;
            }
            out.push(StaleBlockCandidate {
                block: block.as_ref().clone(),
                units: activity.units,
                revision: activity.revision,
                last_access: access,
            });
            if out.len() >= delete_budget {
                break;
            }
        }
        out
    }

    /// Observe one coalesced notification. The queue key is independent of the
    /// latest admission revision, and is also the stable key for protected retries.
    /// Entries remain indexed after observation so membership events can scan all
    /// residents; the next admission moves an observed entry back to the tail.
    ///
    /// A post-boundary fill defers the entire block, moving its notification to
    /// the latest revision without consuming it. The caller must skip the returned
    /// candidate when its revision is at or beyond `admission_limit`. Returning it
    /// rather than looping here keeps each scheduler slice bounded even if every
    /// block in the prefix has received a post-boundary fill.
    pub(crate) fn next_ownership_block(
        &self,
        cursor: &mut (u64, u64),
        admission_limit: u64,
    ) -> Option<(u64, StaleBlockCandidate)> {
        let mut g = self.inner.lock().unwrap();
        let (&position, block) = g.ownership_queue.range(cursor.0..cursor.1).next()?;
        cursor.0 = position + 1;
        let block = block.clone();
        let activity = g.block_access.get_mut(&block).expect("tracked block");
        let revision = activity.revision;
        if revision >= admission_limit {
            activity.ownership_position = revision;
            activity.ownership_pending = true;
        } else {
            activity.ownership_pending = false;
        }
        let candidate = StaleBlockCandidate {
            block: block.as_ref().clone(),
            units: activity.units,
            revision,
            last_access: activity.access.load(Ordering::Relaxed),
        };
        if revision >= admission_limit && position != revision {
            let block = g.ownership_queue.remove(&position).expect("queued block");
            g.ownership_queue.insert(revision, block);
        }
        Some((position, candidate))
    }

    pub(crate) fn block_scan_ceiling(&self) -> u64 {
        self.inner.lock().unwrap().clock
    }

    /// Constant-time check for cache admissions since an ownership sweep.
    pub(crate) fn ownership_admission_ceiling(&self) -> u64 {
        self.inner.lock().unwrap().admission_ceiling
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

    /// Reclaim capacity with a bounded second-chance walk. Queue membership is
    /// exact: explicit removals also erase their queue nodes, so churn
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
    fn updating_whole_preserves_access_and_pins() {
        let lru = Arc::new(Lru::new());
        let unit = whole(1);
        let access = lru.insert(unit.clone(), 10);
        let pin = lru.pin_guard(unit.clone());
        let updated = lru.insert(unit.clone(), 20);
        assert!(Arc::ptr_eq(&access.0, &updated.0));
        assert_eq!(lru.inner.lock().unwrap().entries[&unit].pins, 1);
        let updated = lru.insert(unit.clone(), 30);
        assert!(Arc::ptr_eq(&access.0, &updated.0));
        assert_eq!(lru.inner.lock().unwrap().entries[&unit].pins, 1);
        drop(pin);
        lru.remove(&unit);
        assert!(lru.is_empty());
        assert_eq!(lru.total_bytes(), 0);
    }

    #[test]
    fn ownership_admissions_coalesce_until_observed_and_rearm_after_observation() {
        let lru = Lru::new();
        let block = blk(1);
        for page in 0..16 {
            lru.insert(CacheUnit::Page(block.clone(), PageIndex(page)), 4096);
        }
        {
            let g = lru.inner.lock().unwrap();
            assert_eq!(g.ownership_queue.len(), 1);
            let activity = &g.block_access[&block];
            assert_eq!(activity.ownership_position, 0);
            assert_eq!(activity.revision, 15);
        }
        let limit = lru.ownership_admission_ceiling();
        let (position, candidate) = lru.next_ownership_block(&mut (0, limit), limit).unwrap();
        assert_eq!(candidate.units, 16);
        assert!(lru.stale_candidate_is_current(&candidate));
        // A protected retry looks up the queue position, not the latest revision.
        assert_eq!(
            lru.next_ownership_block(&mut (position, position + 1), limit)
                .unwrap()
                .1
                .revision,
            candidate.revision
        );
        for page in 16..32 {
            lru.insert(CacheUnit::Page(block.clone(), PageIndex(page)), 4096);
        }
        assert!(!lru.stale_candidate_is_current(&candidate));
        assert!(lru
            .next_ownership_block(&mut (position, position + 1), limit)
            .is_none());
        let next_limit = lru.ownership_admission_ceiling();
        let (next_position, candidate) = lru
            .next_ownership_block(&mut (limit, next_limit), next_limit)
            .unwrap();
        assert_eq!(next_position, limit);
        assert_eq!(candidate.units, 32);
        assert_eq!(candidate.revision, next_limit - 1);
        // Retain observed entries for a later full membership sweep.
        assert!(lru
            .next_ownership_block(&mut (0, next_limit), next_limit)
            .is_some());
    }

    #[test]
    fn ownership_coalesced_post_boundary_fill_moves_to_next_pass() {
        let lru = Lru::new();
        let block = blk(1);
        lru.insert(CacheUnit::Page(block.clone(), PageIndex(0)), 4096);
        lru.insert(whole(2), 4096);
        let limit = lru.ownership_admission_ceiling();
        lru.insert(CacheUnit::Page(block.clone(), PageIndex(1)), 4096);
        lru.insert(CacheUnit::Page(block.clone(), PageIndex(2)), 4096);
        let mut cursor = (0, limit);
        let (_, deferred) = lru.next_ownership_block(&mut cursor, limit).unwrap();
        assert!(deferred.revision >= limit);
        assert_eq!(
            lru.next_ownership_block(&mut cursor, limit)
                .unwrap()
                .1
                .block,
            blk(2)
        );
        assert!(lru.next_ownership_block(&mut cursor, limit).is_none());
        // A further fill must stay coalesced with the deferred notification.
        lru.insert(CacheUnit::Page(block.clone(), PageIndex(3)), 4096);
        let next_limit = lru.ownership_admission_ceiling();
        let (position, candidate) = lru
            .next_ownership_block(&mut (limit, next_limit), next_limit)
            .unwrap();
        assert_eq!(position, deferred.revision);
        assert_eq!(candidate.block, block);
        assert_eq!(candidate.units, 4);
        assert!(lru.stale_candidate_is_current(&candidate));
    }

    #[test]
    fn ownership_removal_and_recreation_retire_coalesced_notification() {
        let lru = Lru::new();
        let block = blk(1);
        for page in 0..16 {
            lru.insert(CacheUnit::Page(block.clone(), PageIndex(page)), 4096);
        }
        let limit = lru.ownership_admission_ceiling();
        for page in 0..16 {
            lru.remove(&CacheUnit::Page(block.clone(), PageIndex(page)));
        }
        assert!(lru.inner.lock().unwrap().ownership_queue.is_empty());
        lru.insert(CacheUnit::Page(block.clone(), PageIndex(0)), 4096);
        assert!(lru.next_ownership_block(&mut (0, limit), limit).is_none());
        let next_limit = lru.ownership_admission_ceiling();
        assert_eq!(
            lru.next_ownership_block(&mut (limit, next_limit), next_limit)
                .unwrap()
                .1
                .block,
            block
        );
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
    fn exact_candidates_revalidate_access_and_pins_without_charging_before_unlink() {
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
        let candidates = lru.candidates([page.clone()]);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].unit, page);
        assert!(lru.candidate_is_current(&candidates[0]));
        assert_eq!(lru.total_bytes(), 40);
        access.touch();
        assert!(!lru.candidate_is_current(&candidates[0]));
        let refreshed = lru.candidates([page.clone(), pinned.clone()]);
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
    fn versions_compete_by_recency_for_capacity() {
        let old = blk(1);
        let mut new = old.clone();
        new.version = Version::new("v2");
        for (old, new) in [
            (CacheUnit::Whole(old.clone()), CacheUnit::Whole(new.clone())),
            (
                CacheUnit::Page(old, PageIndex(0)),
                CacheUnit::Page(new, PageIndex(0)),
            ),
        ] {
            let lru = Lru::new();
            lru.insert(old.clone(), 100);
            lru.insert(new.clone(), 100);
            assert!(lru.evict_to_fit(200).is_empty());
            assert_eq!(lru.len(), 2);
            lru.touch(&old);
            assert_eq!(lru.evict_to_fit(100), vec![new]);
            assert_eq!(lru.remove(&old), Some(100));
            assert!(lru.is_empty());
        }
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
}
