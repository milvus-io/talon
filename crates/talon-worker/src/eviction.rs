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

use crate::block_key::{ObjectIdentity, ObjectKey};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

#[cfg(test)]
use talon_core::Version;
use talon_core::{BlockId, ObjectId, PageIndex};

mod recovery;
mod versions;
pub(crate) use recovery::RecoveredUnit;
use versions::VersionIndex;

/// A single evictable cache unit.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CacheUnit {
    /// A whole block, evicted as one unit.
    Whole(BlockId),
    /// One exact byte range; block offset and size are the requested coordinates.
    ExactRange(BlockId),
    /// One page of a paged block.
    Page(BlockId, PageIndex),
}

impl CacheUnit {
    pub(crate) fn block(&self) -> &BlockId {
        match self {
            Self::Whole(block) | Self::ExactRange(block) | Self::Page(block, _) => block,
        }
    }
}

use crate::block_key::{BlockIdentity, BlockKey};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum UnitKind {
    Whole,
    ExactRange,
    Page(PageIndex),
}

pub(crate) trait UnitKey {
    fn block(&self) -> &dyn BlockIdentity;
    fn kind(&self) -> UnitKind;
    fn shared_block(&self) -> BlockKey {
        BlockKey::new(self.block())
    }
}
impl UnitKey for CacheUnit {
    fn block(&self) -> &dyn BlockIdentity {
        self.block()
    }
    fn kind(&self) -> UnitKind {
        match self {
            Self::Whole(_) => UnitKind::Whole,
            Self::ExactRange(_) => UnitKind::ExactRange,
            Self::Page(_, page) => UnitKind::Page(*page),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SharedUnit {
    pub block: BlockKey,
    pub kind: UnitKind,
}
impl UnitKey for SharedUnit {
    fn block(&self) -> &dyn BlockIdentity {
        &self.block
    }
    fn kind(&self) -> UnitKind {
        self.kind
    }
    fn shared_block(&self) -> BlockKey {
        self.block.clone()
    }
}
impl SharedUnit {
    pub(crate) fn materialize(&self) -> CacheUnit {
        match self.kind {
            UnitKind::Page(page) => CacheUnit::Page(self.block.materialize(), page),
            UnitKind::Whole => CacheUnit::Whole(self.block.materialize()),
            UnitKind::ExactRange => CacheUnit::ExactRange(self.block.materialize()),
        }
    }
}
impl From<CacheUnit> for SharedUnit {
    fn from(unit: CacheUnit) -> Self {
        Self {
            block: BlockKey::new(unit.block()),
            kind: unit.kind(),
        }
    }
}

/// Stable recency token for one resident unit. Keeping this token does not pin
/// data. After removal it cannot affect a new admission of the same identity.
#[derive(Clone)]
pub struct AccessHandle(Access);

#[derive(Clone)]
enum Access {
    Page(
        Arc<crate::page_lifecycle::PageState>,
        Option<Arc<AtomicU64>>,
        u64,
    ),
    Tracked(
        Arc<crate::page_lifecycle::PageState>,
        Arc<crate::small_store::DirtyAccess>,
        u64,
    ),
    Small(
        Arc<crate::small_store::policy::SmallAccess>,
        Option<Arc<AtomicU64>>,
        Option<Arc<crate::small_store::DirtyAccess>>,
    ),
}

/// Anchored wall time: monotonic within a process and portable across restarts.
pub(crate) fn access_millis() -> u64 {
    static CLOCK: OnceLock<crate::page_lifecycle::AccessClock> = OnceLock::new();
    CLOCK
        .get_or_init(crate::page_lifecycle::AccessClock::new)
        .now()
}

impl Default for AccessHandle {
    fn default() -> Self {
        Self::new(true)
    }
}

impl AccessHandle {
    pub(crate) fn with_state(
        state: Arc<crate::page_lifecycle::PageState>,
        clock: Option<Arc<AtomicU64>>,
    ) -> Self {
        let generation = state.policy_generation();
        Self(Access::Page(state, clock, generation))
    }
    pub(crate) fn bind_state(mut self, state: Arc<crate::page_lifecycle::PageState>) -> Self {
        match &mut self.0 {
            Access::Page(old, _, generation) | Access::Tracked(old, _, generation) => {
                if !Arc::ptr_eq(old, &state) {
                    *generation = state.policy_generation();
                    *old = state;
                }
            }
            Access::Small(..) => unreachable!("small-object token cannot bind a page"),
        }
        self
    }
    pub(crate) fn page_state(&self) -> &Arc<crate::page_lifecycle::PageState> {
        match &self.0 {
            Access::Page(state, ..) | Access::Tracked(state, ..) => state,
            Access::Small(..) => unreachable!("small-object token has no page state"),
        }
    }
    pub(crate) fn small(&self) -> Option<&Arc<crate::small_store::policy::SmallAccess>> {
        match &self.0 {
            Access::Small(small, ..) => Some(small),
            _ => None,
        }
    }
    pub(crate) fn for_small(small: Arc<crate::small_store::policy::SmallAccess>) -> Self {
        Self(Access::Small(small, None, None))
    }
    pub(crate) fn retire(&self) {
        match &self.0 {
            Access::Page(state, _, generation) | Access::Tracked(state, _, generation) => {
                state.retire_policy(*generation)
            }
            Access::Small(small, ..) => small.retire(),
        }
    }
    fn referenced(&self) -> bool {
        match &self.0 {
            Access::Page(state, _, generation) | Access::Tracked(state, _, generation) => {
                state.referenced(*generation)
            }
            Access::Small(small, ..) => small.referenced(),
        }
    }
    fn take_reference(&self) -> bool {
        match &self.0 {
            Access::Page(state, _, generation) | Access::Tracked(state, _, generation) => {
                state.take_reference(*generation)
            }
            Access::Small(small, ..) => small.take_reference(),
        }
    }
    pub(crate) fn new(track_clock: bool) -> Self {
        Self::with_state(
            Arc::default(),
            track_clock.then(|| Arc::new(AtomicU64::new(access_millis()))),
        )
    }
    fn clock_opt(&self) -> Option<&Arc<AtomicU64>> {
        match &self.0 {
            Access::Page(_, clock, _) | Access::Small(_, clock, _) => clock.as_ref(),
            Access::Tracked(_, dirty, _) => Some(dirty.clock()),
        }
    }
    fn dirty(&self) -> Option<&Arc<crate::small_store::DirtyAccess>> {
        match &self.0 {
            Access::Tracked(_, dirty, _) => Some(dirty),
            Access::Small(_, _, dirty) => dirty.as_ref(),
            _ => None,
        }
    }
    pub(crate) fn clock(&self) -> &Arc<AtomicU64> {
        self.clock_opt().expect("access tracking enabled")
    }
    pub(crate) fn tracks_clock(&self) -> bool {
        self.clock_opt().is_some()
    }
    pub(crate) fn enable_clock(mut self) -> Self {
        match &mut self.0 {
            Access::Page(_, clock, _) | Access::Small(_, clock, _) => {
                clock.get_or_insert_with(|| Arc::new(AtomicU64::new(access_millis())));
            }
            Access::Tracked(..) => {}
        }
        self
    }
    pub(crate) fn track_small(self, dirty: Arc<crate::small_store::DirtyAccess>) -> Self {
        Self(match self.0 {
            Access::Page(state, _, generation) | Access::Tracked(state, _, generation) => {
                Access::Tracked(state, dirty, generation)
            }
            Access::Small(small, clock, _) => Access::Small(small, clock, Some(dirty)),
        })
    }
    pub(crate) fn sample_small(&self) -> u64 {
        if let Some(dirty) = self.dirty() {
            dirty.consume();
        }
        self.last_access()
    }
    pub(crate) fn last_access(&self) -> u64 {
        self.clock_opt()
            .map_or(0, |clock| clock.load(Ordering::Relaxed))
    }
    pub(crate) fn restore(&self, last_access: u64) {
        let now = access_millis();
        self.clock().store(
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
        Self::with_state(
            Arc::default(),
            Some(
                self.clock_opt()
                    .cloned()
                    .unwrap_or_else(|| Arc::new(AtomicU64::new(access_millis()))),
            ),
        )
    }
    pub fn touch(&self) {
        touch_clock(self.clock_opt());
        if let Some(dirty) = self.dirty() {
            dirty.touch();
        }
        match &self.0 {
            Access::Page(state, _, generation) | Access::Tracked(state, _, generation) => {
                state.reference(*generation)
            }
            Access::Small(small, ..) => small.touch(),
        }
    }
}

/// A policy snapshot invalidated by a later touch, selection, or admission.
#[derive(Clone, Debug)]
pub(crate) struct EvictionCandidate {
    pub unit: CacheUnit,
    pub(crate) revision: u64,
    pub(crate) small: Option<Arc<crate::small_store::policy::SmallAccess>>,
}

/// One complete resident block, independent of its whole/paged representation.
#[derive(Clone, Debug)]
pub(crate) struct StaleBlockCandidate {
    pub block: BlockId,
    pub units: usize,
    pub revision: u64,
    last_access: u64,
}

impl StaleBlockCandidate {
    pub(crate) fn is_idle(&self, now: u64, age: u64) -> bool {
        age > 0 && now.saturating_sub(self.last_access) > age
    }
}

pub(crate) struct BlockPolicy {
    pins: usize,
    position: u64,
    revision: u64,
    // One notification coalesces admissions until ownership observes the block.
    ownership_position: AtomicU64,
    ownership_pending: AtomicBool,
    access: Arc<AtomicU64>,
}

pub(crate) struct BlockActivity {
    pub units: usize,
    pins: usize,
    position: u64,
    revision: u64,
    ownership_position: u64,
    #[cfg(test)]
    ownership_pending: bool,
    access: Arc<AtomicU64>,
}

impl BlockPolicy {
    pub(crate) fn snapshot(&self, units: usize) -> BlockActivity {
        BlockActivity {
            units,
            pins: self.pins,
            position: self.position,
            revision: self.revision,
            ownership_position: self.ownership_position.load(Ordering::Relaxed),
            #[cfg(test)]
            ownership_pending: self.ownership_pending.load(Ordering::Relaxed),
            access: self.access.clone(),
        }
    }

    // The LRU mutex serializes both fields with admissions and queue changes.
    // Atomics let observation retain a shared directory lock.
    pub(crate) fn observe_ownership(&self, admission_limit: u64) {
        let pending = self.revision >= admission_limit;
        if pending {
            self.ownership_position
                .store(self.revision, Ordering::Relaxed);
        }
        self.ownership_pending.store(pending, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy)]
pub(crate) struct PolicyState {
    pub(crate) independent: bool,
    pub(crate) bytes: u64,
    pub(crate) position: u64,
    pub(crate) revision: u64,
    pub(crate) pins: u32,
}

#[derive(Clone)]
pub(crate) struct PolicyEntry {
    pub state: PolicyState,
    pub access: AccessHandle,
}

#[derive(Default)]
pub(crate) struct PageAccessSlot {
    state: Option<Arc<crate::page_lifecycle::PageState>>,
    clock: Option<Arc<AtomicU64>>,
    // Policy generations are even; the low bit records residency independently.
    generation_and_resident: u64,
}

impl PageAccessSlot {
    pub(crate) fn is_none(&self) -> bool {
        self.state.is_none()
    }
    pub(crate) fn resident(&self) -> bool {
        self.generation_and_resident & 1 != 0
    }
    pub(crate) fn set_resident(&mut self, resident: bool) -> bool {
        let old = self.resident();
        self.generation_and_resident = (self.generation_and_resident & !1) | u64::from(resident);
        old
    }
    pub(crate) fn set(&mut self, access: AccessHandle) {
        let Access::Page(state, clock, generation) = access.0 else {
            unreachable!("paged entries require a page access token")
        };
        debug_assert_eq!(generation & 1, 0);
        self.generation_and_resident = generation | (self.generation_and_resident & 1);
        self.state = Some(state);
        self.clock = clock;
    }
    pub(crate) fn clear(&mut self) {
        self.state = None;
        self.clock = None;
        self.generation_and_resident &= 1;
    }
    pub(crate) fn get(&self) -> Option<AccessHandle> {
        Some(AccessHandle(Access::Page(
            self.state.as_ref()?.clone(),
            self.clock.clone(),
            self.generation_and_resident & !1,
        )))
    }
    pub(crate) fn touch(&self) -> Option<()> {
        let state = self.state.as_ref()?;
        touch_clock(self.clock.as_ref());
        state.reference(self.generation_and_resident & !1);
        Some(())
    }
}

fn touch_clock(clock: Option<&Arc<AtomicU64>>) {
    if let Some(clock) = clock {
        let now = access_millis();
        if clock.load(Ordering::Relaxed) < now {
            clock.fetch_max(now, Ordering::Relaxed);
        }
    }
}

impl std::ops::Deref for PolicyEntry {
    type Target = PolicyState;
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

/// Byte-accounted second-chance eviction with strict pinning. Reads use stable
/// access handles; only membership changes and eviction take the policy lock.
pub struct Lru {
    inner: Mutex<Inner>,
    eviction: Mutex<()>,
    #[cfg(test)]
    pub(crate) before_small_publish: Mutex<Option<Box<dyn FnOnce() + Send>>>,
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
    index: Arc<crate::BlockIndex>,
    queue: BTreeMap<u64, SharedUnit>,
    versions: VersionIndex,
    block_queue: BTreeMap<u64, BlockKey>,
    ownership_queue: BTreeMap<u64, BlockKey>,
    object_checks: HashMap<ObjectKey, (usize, Option<u64>)>,
    track_checks: bool,
    track_blocks: bool,
    track_small_clock: bool,
    total_bytes: u64,
    clock: u64,
    // Unlike clock, this does not advance for capacity-policy rotations or reads.
    admission_ceiling: u64,
    small_cursor: usize,
    small_first: bool,
}

impl Inner {
    /// Move a resident unit to the back of the second-chance queue.
    fn rotate(&mut self, unit: &CacheUnit) -> u64 {
        let entry = self.index.policy_get(unit).expect("tracked cache unit");
        let queued = self
            .queue
            .remove(&entry.position)
            .expect("queued cache unit");
        let position = self.clock;
        self.clock = self
            .clock
            .checked_add(1)
            .expect("eviction sequence exhausted");
        self.index
            .policy_update(unit, |entry| entry.position = position);
        self.queue.insert(position, queued);
        position
    }

    fn snapshot(&mut self, unit: CacheUnit) -> EvictionCandidate {
        let access = self
            .index
            .policy_get(&unit)
            .expect("tracked cache unit")
            .access;
        if let Some(ticket) = access.small() {
            return EvictionCandidate {
                revision: ticket.snapshot(),
                unit,
                small: Some(ticket.clone()),
            };
        }
        access.take_reference();
        let revision = self.rotate(&unit);
        self.index
            .policy_update(&unit, |entry| entry.revision = revision);
        EvictionCandidate {
            unit,
            revision,
            small: None,
        }
    }

    /// Remove membership, aggregate accounting and queues under the policy lock.
    fn remove(&mut self, unit: &CacheUnit) -> Option<u64> {
        let entry = self.index.policy_remove(unit)?;
        if entry.access.small().is_some() {
            return Some(entry.bytes);
        }
        self.queue.remove(&entry.position);
        let block = unit.block();
        if self.track_blocks {
            self.index
                .policy_block_update(block, |activity| {
                    activity.pins -= entry.pins as usize;
                })
                .expect("tracked block");
            let activity = self.index.policy_block_get(block).expect("tracked block");
            if activity.units == 0 {
                self.ownership_queue.remove(&activity.ownership_position);
                self.index.policy_block_remove(block);
                self.block_queue.remove(&activity.position);
            }
        }
        if let Some(check) = self
            .object_checks
            .get_mut(&block.object as &dyn ObjectIdentity)
        {
            check.0 -= 1;
            if check.0 == 0 {
                self.object_checks
                    .remove(&block.object as &dyn ObjectIdentity);
            }
        }
        if !entry.independent {
            self.remove_version(unit);
        }
        Lru::subtract_bytes(&mut self.total_bytes, entry.bytes);
        Some(entry.bytes)
    }

    fn remove_version(&mut self, unit: &impl UnitKey) {
        let block = unit.block();
        if !self.index.policy_has_versioned_units(block) {
            self.versions.remove(block);
        }
    }

    fn superseded_units(&self, keep: &BlockId) -> Vec<CacheUnit> {
        self.versions
            .blocks(keep)
            .filter(|block| block.view().version != keep.version.as_str())
            .flat_map(|block| {
                self.index
                    .policy_units(block)
                    .into_iter()
                    .filter(|(_, state)| !state.independent && state.pins == 0)
                    .map(|(kind, _)| {
                        SharedUnit {
                            block: block.clone(),
                            kind,
                        }
                        .materialize()
                    })
            })
            .collect()
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
        Self::with_tracking(true, true)
    }

    fn with_tracking(track_checks: bool, track_blocks: bool) -> Self {
        Self {
            inner: Mutex::new(Inner {
                index: Arc::new(crate::BlockIndex::new()),
                queue: BTreeMap::new(),
                versions: VersionIndex::default(),
                block_queue: BTreeMap::new(),
                ownership_queue: BTreeMap::new(),
                object_checks: HashMap::new(),
                track_checks,
                track_blocks,
                track_small_clock: track_blocks,
                total_bytes: 0,
                clock: 0,
                admission_ceiling: 0,
                small_cursor: 0,
                small_first: false,
            }),
            eviction: Mutex::new(()),
            #[cfg(test)]
            before_small_publish: Mutex::default(),
        }
    }

    /// Total bytes currently tracked.
    pub fn total_bytes(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.total_bytes + inner.index.compact_small_totals().0
    }

    /// Number of units currently tracked.
    pub fn len(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.queue.len() + inner.index.compact_small_totals().1
    }

    /// Whether the tracker holds no units.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Insert or update a unit, returning its stable access token. Updating an
    /// existing admission preserves its pins and token.
    pub fn insert(&self, unit: CacheUnit, bytes: u64) -> AccessHandle {
        self.insert_with_policy(unit, bytes, false)
    }

    pub(crate) fn shared_identity(&self, id: &dyn BlockIdentity) -> BlockKey {
        self.inner
            .lock()
            .unwrap()
            .index
            .shared_identity(id)
            .unwrap_or_else(|| BlockKey::new(id))
    }

    pub(crate) fn index(&self) -> Arc<crate::BlockIndex> {
        self.inner.lock().unwrap().index.clone()
    }

    pub(crate) fn tracks_small_clock(&self) -> bool {
        self.inner.lock().unwrap().track_small_clock
    }

    pub(crate) fn enable_small_clock(&self) -> bool {
        !std::mem::replace(&mut self.inner.lock().unwrap().track_small_clock, true)
    }

    /// Packed objects are independent full keys, with no cross-version eviction.
    pub(crate) fn insert_with_policy(
        &self,
        unit: impl UnitKey,
        bytes: u64,
        independent: bool,
    ) -> AccessHandle {
        let mut g = self.inner.lock().unwrap();
        Self::insert_locked(&mut g, unit, bytes, independent)
    }

    pub(crate) fn publish_small(
        &self,
        unit: SharedUnit,
        bytes: u64,
        directory: &RwLock<crate::small_store::directory::Directory>,
        location: crate::small_store::Location,
    ) {
        let mut g = self.inner.lock().unwrap();
        let key = crate::small_store::key::SmallKey(unit.clone());
        directory.write().unwrap().stage(key.clone(), location);
        let access = Self::insert_locked(&mut g, unit.clone(), bytes, true);
        #[cfg(test)]
        if let Some(pause) = self.before_small_publish.lock().unwrap().take() {
            pause();
        }
        let mut directory = directory.write().unwrap();
        directory.attach_access(&key, access);
        directory.publish(&key);
    }

    fn insert_locked(
        g: &mut Inner,
        unit: impl UnitKey,
        bytes: u64,
        independent: bool,
    ) -> AccessHandle {
        let independent = independent || unit.kind() == UnitKind::ExactRange;
        if independent && !g.track_blocks && !g.track_checks {
            if let Some(access) = g
                .index
                .compact_small_admit(&unit, bytes, g.track_small_clock)
            {
                return access;
            }
        }
        assert!(!independent || !matches!(unit.kind(), UnitKind::Page(_)));
        let position = g.clock;
        g.clock = g.clock.checked_add(1).expect("eviction sequence exhausted");
        g.admission_ceiling = g.clock;
        let requeue = g
            .index
            .policy_block_update(unit.block(), |activity| {
                if *activity.ownership_pending.get_mut() {
                    return None;
                }
                let previous = *activity.ownership_position.get_mut();
                *activity.ownership_position.get_mut() = position;
                *activity.ownership_pending.get_mut() = true;
                Some(previous)
            })
            .flatten();
        if let Some(previous) = requeue {
            let block = g.ownership_queue.remove(&previous).expect("queued block");
            g.ownership_queue.insert(position, block);
        }
        if let Some(e) = g.index.policy_get(&unit) {
            let old = e.bytes;
            let was_independent = e.independent;
            g.index.policy_update(&unit, |e| {
                e.independent = independent;
                e.bytes = bytes;
            });
            let mut access = e.access.clone();
            if independent && g.track_small_clock && !access.tracks_clock() {
                access = access.enable_clock();
                let unit = SharedUnit {
                    block: g
                        .index
                        .shared_identity(unit.block())
                        .expect("tracked block"),
                    kind: unit.kind(),
                };
                g.index.policy_insert(
                    &unit,
                    PolicyEntry {
                        state: PolicyState {
                            independent,
                            bytes,
                            ..e.state
                        },
                        access: access.clone(),
                    },
                );
            }
            access.touch();
            if was_independent != independent {
                if independent {
                    g.remove_version(&unit);
                } else {
                    let key = g
                        .index
                        .shared_identity(unit.block())
                        .expect("tracked block");
                    g.versions.insert(key);
                }
            }
            g.index
                .policy_block_update(unit.block(), |activity| activity.revision = position);
            Self::subtract_bytes(&mut g.total_bytes, old);
            Self::add_bytes(&mut g.total_bytes, bytes);
            return access;
        }
        Self::add_bytes(&mut g.total_bytes, bytes);
        let block = unit.block();
        if g.track_blocks && g.index.policy_block_get(block).is_none() {
            let key = g.index.policy_block_insert(
                unit.shared_block(),
                BlockPolicy {
                    pins: 0,
                    position,
                    revision: position,
                    ownership_position: AtomicU64::new(position),
                    ownership_pending: AtomicBool::new(true),
                    access: Arc::new(AtomicU64::new(access_millis())),
                },
            );
            g.block_queue.insert(position, key.clone());
            g.ownership_queue.insert(position, key);
        }
        let clock = g.index.policy_block_update(block, |activity| {
            activity.revision = position;
            activity.access.clone()
        });
        let clock = clock.or_else(|| {
            (independent && g.track_small_clock).then(|| Arc::new(AtomicU64::new(access_millis())))
        });
        let access = AccessHandle::with_state(Arc::default(), clock);
        // New pages also refresh the shared block idle age.
        if let Some(clock) = access.clock_opt() {
            clock.fetch_max(access_millis(), Ordering::Relaxed);
        }
        let unit = SharedUnit {
            block: g
                .index
                .shared_identity(block)
                .unwrap_or_else(|| unit.shared_block()),
            kind: unit.kind(),
        };
        if g.track_checks {
            g.object_checks
                .entry(ObjectKey(unit.block.clone()))
                .or_insert((0, None))
                .0 += 1;
        }
        if !independent {
            g.versions.insert(unit.block.clone());
        }
        g.queue.insert(position, unit.clone());
        g.index.policy_insert(
            &unit,
            PolicyEntry {
                state: PolicyState {
                    independent,
                    bytes,
                    position,
                    revision: position,
                    pins: 0,
                },
                access: access.clone(),
            },
        )
    }

    /// Compatibility lookup for callers without a resolved access token.
    /// Data-path reads should touch the token obtained with their index lookup.
    pub fn touch(&self, unit: &CacheUnit) {
        if let Some(e) = self.inner.lock().unwrap().index.policy_get(unit) {
            e.access.touch();
        }
    }

    /// Pin a unit so it cannot be evicted while an active reader holds it.
    ///
    /// Returns `true` if the unit exists.
    pub fn pin(&self, unit: &CacheUnit) -> bool {
        let g = self.inner.lock().unwrap();
        match g.index.policy_update(unit, |e| e.pins += 1) {
            Some(()) => {
                g.index
                    .policy_block_update(unit.block(), |activity| activity.pins += 1);
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
        let g = self.inner.lock().unwrap();
        if g.index.policy_update(unit, |e| {
            if e.pins == 0 {
                return false;
            }
            e.pins -= 1;
            true
        }) == Some(true)
        {
            g.index
                .policy_block_update(unit.block(), |activity| activity.pins -= 1);
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
        let (count, small_first) = {
            let mut g = self.inner.lock().unwrap();
            g.small_first = !g.small_first;
            (g.queue.len(), g.small_first)
        };
        let mut selected = HashSet::new();
        let mut selected_bytes = 0_u64;
        let mut out = Vec::new();
        for second_pass in [false, true] {
            for small in [small_first, !small_first] {
                if small {
                    let (index, directories, first) = {
                        let mut g = self.inner.lock().unwrap();
                        let directories = g.index.compact_small_directories();
                        let first = g.small_cursor;
                        g.small_cursor = first.wrapping_add(1);
                        (g.index.clone(), directories, first)
                    };
                    for n in 0..directories.len() {
                        let directory = &directories[(first + n) % directories.len()];
                        let mut remaining = directory.read().unwrap().compact_capacity();
                        while remaining > 0 {
                            let g = self.inner.lock().unwrap();
                            let mut needed = (g.total_bytes + index.compact_small_totals().0)
                                .saturating_sub(selected_bytes)
                                .saturating_sub(capacity);
                            if needed == 0 || scan_budget == 0 || out.len() == delete_budget {
                                return out;
                            }
                            let batch = remaining.min(scan_budget).min(64);
                            let mut scans = batch;
                            let candidates = directory.write().unwrap().compact_candidates(
                                &mut needed,
                                excluded,
                                &mut selected,
                                &mut scans,
                                delete_budget - out.len(),
                                second_pass,
                            );
                            for candidate in &candidates {
                                selected_bytes = selected_bytes.saturating_add(
                                    candidate
                                        .small
                                        .as_ref()
                                        .unwrap()
                                        .state
                                        .lock()
                                        .unwrap()
                                        .bytes,
                                );
                            }
                            out.extend(candidates);
                            scan_budget -= batch - scans;
                            remaining -= batch;
                        }
                    }
                    continue;
                }
                let mut remaining = count;
                while remaining > 0 {
                    let mut g = self.inner.lock().unwrap();
                    let batch = remaining.min(64);
                    for _ in 0..batch {
                        if scan_budget == 0
                            || out.len() == delete_budget
                            || (g.total_bytes + g.index.compact_small_totals().0)
                                .saturating_sub(selected_bytes)
                                <= capacity
                        {
                            return out;
                        }
                        scan_budget -= 1;
                        let Some((_, unit)) = g.queue.first_key_value() else {
                            break;
                        };
                        let unit = unit.clone();
                        let entry = g.index.policy_get(&unit).expect("queued entry");
                        let unit = unit.materialize();
                        let eligible = entry.pins == 0
                            && !selected.contains(&unit)
                            && !excluded.contains(&unit);
                        let referenced = eligible && entry.access.take_reference();
                        let victim = eligible && (second_pass || !referenced);
                        let bytes = entry.bytes;
                        let revision = g.rotate(&unit);
                        if eligible {
                            g.index
                                .policy_update(&unit, |entry| entry.revision = revision);
                        }
                        if victim {
                            selected_bytes = selected_bytes.saturating_add(bytes);
                            selected.insert(unit.clone());
                            out.push(EvictionCandidate {
                                unit,
                                revision,
                                small: None,
                            });
                        }
                    }
                    remaining -= batch;
                }
            }
        }
        out
    }

    /// Old-version candidates; removal is committed by the caller after I/O.
    pub(crate) fn superseded_candidates(&self, keep: &BlockId) -> Vec<EvictionCandidate> {
        let mut g = self.inner.lock().unwrap();
        if g.index
            .policy_get(&CacheUnit::Whole(keep.clone()))
            .is_some_and(|e| e.independent)
        {
            return Vec::new();
        }
        let units = g.superseded_units(keep);
        units.into_iter().map(|unit| g.snapshot(unit)).collect()
    }

    /// Snapshot resident units for an explicit block invalidation.
    pub(crate) fn block_candidates(&self, block: &BlockId) -> Vec<EvictionCandidate> {
        let mut g = self.inner.lock().unwrap();
        let units: Vec<_> = g
            .index
            .policy_units(block)
            .into_iter()
            .map(|(kind, _)| match kind {
                UnitKind::Whole => CacheUnit::Whole(block.clone()),
                UnitKind::ExactRange => CacheUnit::ExactRange(block.clone()),
                UnitKind::Page(page) => CacheUnit::Page(block.clone(), page),
            })
            .collect();
        units.into_iter().map(|unit| g.snapshot(unit)).collect()
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
            let activity = g.index.policy_block_get(block).expect("tracked block");
            let access = activity.access.load(Ordering::Relaxed);
            if activity.pins != 0 || now.saturating_sub(access) <= age {
                continue;
            }
            if consider_verified
                && g.object_checks
                    .get(&block.view().object as &dyn ObjectIdentity)
                    .and_then(|check| check.1)
                    .is_some_and(|at| now.saturating_sub(at) < age)
            {
                continue;
            }
            out.push(StaleBlockCandidate {
                block: block.materialize(),
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
        let activity = g
            .index
            .policy_block_observe_ownership(&block, admission_limit)
            .expect("tracked block");
        let candidate = StaleBlockCandidate {
            block: block.materialize(),
            units: activity.units,
            revision: activity.revision,
            last_access: activity.access.load(Ordering::Relaxed),
        };
        let revision = candidate.revision;
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
            .index
            .policy_block_get(&candidate.block)
            .is_some_and(|entry| {
                entry.pins == 0
                    && entry.revision == candidate.revision
                    && entry.access.load(Ordering::Relaxed) == candidate.last_access
            })
    }

    /// Called under the block mutation gate after its complete on-disk deletion.
    pub(crate) fn remove_block(&self, block: &BlockId) {
        let mut g = self.inner.lock().unwrap();
        for (kind, _) in g.index.policy_units(block) {
            let unit = match kind {
                UnitKind::Page(page) => CacheUnit::Page(block.clone(), page),
                UnitKind::Whole => CacheUnit::Whole(block.clone()),
                UnitKind::ExactRange => CacheUnit::ExactRange(block.clone()),
            };
            g.remove(&unit);
        }
    }

    pub(crate) fn stale_check_due(&self, object: &ObjectId, now: u64, age: u64) -> bool {
        self.inner
            .lock()
            .unwrap()
            .object_checks
            .get(object as &dyn ObjectIdentity)
            .is_some_and(|(_, at)| at.map_or(true, |at| now.saturating_sub(at) >= age))
    }

    pub(crate) fn stale_verified(&self, object: &ObjectId, now: u64) {
        if let Some(check) = self
            .inner
            .lock()
            .unwrap()
            .object_checks
            .get_mut(object as &dyn ObjectIdentity)
        {
            check.1 = Some(now);
        }
    }

    /// Recheck with the block mutation gate held. Commits also pin under that gate.
    pub(crate) fn candidate_is_current(&self, candidate: &EvictionCandidate) -> bool {
        if let Some(small) = &candidate.small {
            return small.is_current(candidate.revision);
        }
        self.inner
            .lock()
            .unwrap()
            .index
            .policy_get(&candidate.unit)
            .is_some_and(|entry| {
                entry.pins == 0
                    && entry.revision == candidate.revision
                    && !entry.access.referenced()
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
        if g.index
            .policy_get(&CacheUnit::Whole(keep.clone()))
            .is_some_and(|e| e.independent)
        {
            return Vec::new();
        }
        let victims = g.superseded_units(keep);
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
        let candidates = self.candidates_to_fit(capacity, &HashSet::new());
        let mut out = Vec::new();
        for batch in candidates.chunks(64) {
            let mut g = self.inner.lock().unwrap();
            for candidate in batch {
                if g.total_bytes + g.index.compact_small_totals().0 <= capacity {
                    return out;
                }
                let current = g
                    .index
                    .policy_get(&candidate.unit)
                    .is_some_and(|entry| entry.pins == 0 && entry.revision == candidate.revision);
                if current {
                    g.remove(&candidate.unit);
                    out.push(candidate.unit.clone());
                }
            }
        }
        out
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

    #[test]
    fn page_access_residency_preserves_generations_and_releases_tokens() {
        let mut slot = PageAccessSlot::default();
        let first = AccessHandle::new(true);
        let state = first.page_state().clone();
        let clock = Arc::downgrade(first.clock());
        assert!(!slot.set_resident(true));
        slot.set(first.clone());
        assert!(slot.resident());
        slot.touch().unwrap();
        assert!(first.take_reference());
        first.retire();
        let second = AccessHandle::with_state(state, None);
        slot.touch().unwrap();
        assert!(
            !second.referenced(),
            "retired token touched a later admission"
        );
        slot.set(second.clone());
        assert!(slot.set_resident(false));
        slot.touch().unwrap();
        assert!(
            second.take_reference(),
            "nonresident policy still owns its token"
        );
        slot.set_resident(true);
        slot.clear();
        assert!(
            slot.resident(),
            "access retirement must not clear residency"
        );
        assert!(slot.get().is_none());
        drop(first);
        assert!(clock.upgrade().is_none());
    }

    #[test]
    fn object_checks_share_identity_across_versions_and_release_the_last_unit() {
        let lru = Lru::new();
        let first = CacheUnit::Page(blk(9), PageIndex(0));
        lru.insert(first.clone(), 4096);
        let alive = lru
            .index()
            .shared_identity(first.block())
            .unwrap()
            .downgrade();
        lru.stale_verified(&first.block().object, 100);
        let mut other = blk(9);
        other.offset = 4096;
        other.version = Version::new("v2");
        let second = CacheUnit::Page(other, PageIndex(1));
        lru.insert(second.clone(), 0);
        {
            let g = lru.inner.lock().unwrap();
            assert_eq!(g.object_checks.len(), 1);
            assert_eq!(
                g.object_checks
                    .get(&first.block().object as &dyn ObjectIdentity),
                Some(&(2, Some(100)))
            );
        }
        lru.remove(&first);
        assert!(alive());
        assert!(!lru.stale_check_due(&second.block().object, 101, 10));
        lru.remove(&second);
        assert!(!alive());
        assert!(lru.inner.lock().unwrap().object_checks.is_empty());
    }

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
    fn exact_ranges_coexist_with_regular_units_and_skip_version_eviction() {
        for ordinary in [whole(1), CacheUnit::Page(blk(1), PageIndex(0))] {
            for independent in [false, true] {
                if independent && matches!(ordinary, CacheUnit::Page(..)) {
                    continue;
                }
                let lru = Lru::new();
                let block = blk(1);
                let exact = CacheUnit::ExactRange(block.clone());
                lru.insert(exact.clone(), 20);
                lru.insert_with_policy(ordinary.clone(), 10, independent);
                let candidates: HashSet<_> = lru
                    .block_candidates(&block)
                    .into_iter()
                    .map(|candidate| candidate.unit)
                    .collect();
                assert_eq!(candidates, HashSet::from([exact.clone(), ordinary.clone()]));
                let mut newer = block.clone();
                newer.version = Version::new("v2");
                assert!(lru
                    .superseded_candidates(&newer)
                    .iter()
                    .all(|candidate| candidate.unit == ordinary));
                lru.evict_superseded(&newer);
                assert!(lru.inner.lock().unwrap().index.policy_get(&exact).is_some());
                lru.remove_block(&block);
                assert!(lru.is_empty());
                assert_eq!(lru.total_bytes(), 0);
                assert!(lru
                    .inner
                    .lock()
                    .unwrap()
                    .index
                    .policy_block_get(&block)
                    .is_none());
            }
        }
    }

    #[test]
    fn disabled_block_tracking_keeps_capacity_pins_and_version_eviction() {
        let lru = Lru::with_tracking(false, false);
        let old = CacheUnit::Page(blk(0), PageIndex(0));
        let mut new_block = blk(0);
        new_block.version = Version::new("v2");
        let new = CacheUnit::Page(new_block.clone(), PageIndex(0));
        let token = lru.insert(old.clone(), 4096);
        assert!(!token.tracks_clock());
        token.touch();
        assert!(lru.pin(&old));
        lru.insert(new.clone(), 8192);
        assert!(lru.evict_superseded(&new_block).is_empty());
        assert_eq!(lru.evict_to_fit(0), [new]);
        lru.unpin(&old);
        assert_eq!(lru.evict_superseded(&new_block), [old]);
        assert!(lru.is_empty());
        let g = lru.inner.lock().unwrap();
        assert!(g.block_queue.is_empty());
        assert!(g.ownership_queue.is_empty());
        assert!(g.index.policy_block_get(&blk(0)).is_none());
        assert_eq!(g.total_bytes, 0);
    }

    #[test]
    fn switching_whole_storage_policy_preserves_access_and_pins() {
        for tracking in [false, true] {
            let lru = Arc::new(Lru::with_tracking(tracking, tracking));
            let unit = whole(1);
            let access = lru.insert(unit.clone(), 10);
            let pin = lru.pin_guard(unit.clone());
            let updated = lru.insert_with_policy(unit.clone(), 20, true);
            assert_eq!(updated.tracks_clock(), tracking);
            assert_eq!(
                lru.inner
                    .lock()
                    .unwrap()
                    .index
                    .policy_get(&unit)
                    .unwrap()
                    .access
                    .tracks_clock(),
                tracking
            );
            assert!(Arc::ptr_eq(access.page_state(), updated.page_state()));
            assert!(lru.inner.lock().unwrap().versions.is_empty());
            assert_eq!(
                lru.inner
                    .lock()
                    .unwrap()
                    .index
                    .policy_get(&unit)
                    .unwrap()
                    .pins,
                1
            );
            let updated = lru.insert(unit.clone(), 30);
            assert!(Arc::ptr_eq(access.page_state(), updated.page_state()));
            assert_eq!(lru.inner.lock().unwrap().versions.len(), 1);
            assert_eq!(
                lru.inner
                    .lock()
                    .unwrap()
                    .index
                    .policy_get(&unit)
                    .unwrap()
                    .pins,
                1
            );
            drop(pin);
            lru.remove_block(unit.block());
            assert!(lru.is_empty());
            assert_eq!(lru.total_bytes(), 0);
        }
    }

    #[test]
    fn independent_keys_skip_version_index_and_preserve_exact_deletion() {
        for track_checks in [false, true] {
            let lru = Lru::with_tracking(track_checks, true);
            let v1 = blk(1);
            let mut v2 = v1.clone();
            v2.version = Version::new("v2");
            for id in [&v1, &v2] {
                lru.insert_with_policy(CacheUnit::Whole(id.clone()), 4096, true);
            }
            assert!(lru.inner.lock().unwrap().versions.is_empty());
            assert_eq!(
                lru.inner.lock().unwrap().object_checks.len(),
                usize::from(track_checks)
            );
            assert!(lru.superseded_candidates(&v2).is_empty());
            assert!(lru.evict_superseded(&v2).is_empty());
            let mut paged = v1.clone();
            paged.version = Version::new("v3");
            lru.insert(CacheUnit::Page(paged.clone(), PageIndex(0)), 100);
            assert!(lru.superseded_candidates(&paged).is_empty());
            assert!(lru.superseded_candidates(&v2).is_empty());
            assert_eq!(lru.block_candidates(&v1).len(), 1);
            if track_checks {
                assert!(lru.stale_check_due(&v1.object, 100, 10));
                lru.stale_verified(&v1.object, 100);
                assert!(!lru.stale_check_due(&v2.object, 101, 10));
            }
            lru.remove_block(&v1);
            assert!(lru.block_candidates(&v1).is_empty());
            assert_eq!(lru.block_candidates(&v2).len(), 1);
            lru.remove_block(&v2);
            lru.remove_block(&paged);
            assert!(lru.is_empty());
            assert!(lru.inner.lock().unwrap().object_checks.is_empty());
        }
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
            let activity = g.index.policy_block_get(&block).unwrap();
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
        lru.remove_block(&block);
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
        access.clock().store(100, Ordering::Relaxed);
        let mut cursor = (0, 0);
        assert!(lru
            .stale_gc_candidates(&mut cursor, 200, 100, 8, 8)
            .is_empty());
        let candidates = lru.stale_gc_candidates(&mut cursor, 201, 100, 8, 8);
        assert_eq!(candidates.len(), 1);
        assert!(candidates.iter().all(|c| lru.stale_candidate_is_current(c)));
        // A touch of either page invalidates the block candidate.
        access.clock().store(201, Ordering::Relaxed);
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
        old.clock().store(1, Ordering::Relaxed);
        let candidates = lru.stale_gc_candidates(&mut (0, 0), 200, 100, 8, 8);
        assert_eq!(candidates.len(), 1);
        lru.stale_verified(&unit.block().object, 200);
        lru.remove(&unit);
        {
            let g = lru.inner.lock().unwrap();
            assert!(g.index.policy_block_get(unit.block()).is_none());
            assert!(g.block_queue.is_empty());
            assert!(g.object_checks.is_empty());
        }
        let new = lru.insert(unit, 16);
        assert!(!Arc::ptr_eq(old.clock(), new.clock()));
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
            assert!(g
                .queue
                .values()
                .all(|unit| g.index.policy_get(unit).is_some()));
            for (position, unit) in &g.queue {
                assert_eq!(g.index.policy_get(unit).unwrap().position, *position);
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
            assert_eq!(g.versions.blocks(&keep).count(), 1);
            assert_eq!(
                g.index
                    .policy_units(&keep)
                    .iter()
                    .map(|(kind, _)| *kind)
                    .collect::<Vec<_>>(),
                [UnitKind::Whole]
            );
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
        for n in 0..8 {
            assert_eq!(g.versions.blocks(&blk(n)).count(), 1);
            assert_eq!(
                g.index
                    .policy_units(&blk(n))
                    .iter()
                    .map(|(kind, _)| *kind)
                    .collect::<Vec<_>>(),
                [UnitKind::Page(PageIndex(0))]
            );
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
            assert_eq!(
                g.index
                    .policy_units(&old)
                    .iter()
                    .map(|(kind, _)| *kind)
                    .collect::<Vec<_>>(),
                [UnitKind::Page(PageIndex(0))]
            );
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
