//! Garbage collection over the published compact catalog.
use super::catalog::{Address, Catalog};
use super::key::SmallIdentity;
use super::lookup::VISIBLE;
use super::policy::SmallAccess;
use super::recency::Recency;
use super::{IdleRecencyCursor, SmallObjectStore};
use crate::eviction::CacheUnit;
use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::{atomic::Ordering::Relaxed, Arc};
use talon_core::BlockId;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct OwnershipLimit {
    pub epoch: u64,
    last_segment: u64,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct OwnershipCursor {
    segment: u64,
    physical: u64,
    offset: u64,
}

#[derive(Default)]
pub(super) struct Ownership {
    epoch: u64,
    dirty: bool,
    // A replacement keeps the original segment's place in the sweep.
    segments: BTreeMap<u64, Admissions>,
    relocated: HashMap<u64, u64>,
    // Destination epoch data is assembled while the old segment is still visible.
    moving: HashMap<u64, Admissions>,
}

struct Admissions {
    physical: u64,
    first: u64,
    latest: u64,
    // All rows normally have one epoch. Only segments that straddle a
    // boundary (or receive a later re-admission) need per-offset metadata.
    exceptions: HashMap<u64, u64>,
}

impl Admissions {
    fn new(physical: u64, epoch: u64) -> Self {
        Self {
            physical,
            first: epoch,
            latest: epoch,
            exceptions: HashMap::new(),
        }
    }

    fn epoch(&self, offset: u64) -> u64 {
        self.exceptions.get(&offset).copied().unwrap_or(self.first)
    }

    fn admit(&mut self, offset: u64, epoch: u64) {
        self.latest = self.latest.max(epoch);
        if epoch != self.first {
            self.exceptions.insert(offset, epoch);
        }
    }
}

impl Ownership {
    fn logical_segment(&self, physical: u64) -> u64 {
        self.relocated.get(&physical).copied().unwrap_or(physical)
    }

    fn epoch_at(&self, (segment, offset): Address) -> Option<u64> {
        self.moving
            .get(&segment)
            .or_else(|| self.segments.get(&self.logical_segment(segment)))
            .map(|admissions| admissions.epoch(offset))
    }

    pub fn admit(&mut self, (segment, offset): Address) {
        let logical = self.logical_segment(segment);
        self.segments
            .entry(logical)
            .or_insert_with(|| Admissions::new(segment, self.epoch))
            .admit(offset, self.epoch);
        self.dirty = true;
    }

    pub fn remove(&mut self, (segment, offset): Address) {
        let logical = self.logical_segment(segment);
        if let Some(admissions) = self.segments.get_mut(&logical) {
            admissions.exceptions.remove(&offset);
        }
    }

    pub fn relocate(&mut self, old: Address, new: Address) {
        let epoch = self.epoch_at(old).expect("published small admission");
        self.moving
            .entry(new.0)
            .or_insert_with(|| Admissions::new(new.0, epoch))
            .admit(new.1, epoch);
    }

    pub fn replace_segment(&mut self, old: u64, new: u64) {
        let logical = self.relocated.remove(&old).unwrap_or(old);
        let admissions = self.moving.remove(&new).expect("relocated small segment");
        self.segments.insert(logical, admissions);
        self.relocated.insert(new, logical);
    }

    pub fn remove_segment(&mut self, segment: u64) {
        let logical = self.relocated.remove(&segment).unwrap_or(segment);
        if self
            .segments
            .get(&logical)
            .is_some_and(|admissions| admissions.physical == segment)
        {
            self.segments.remove(&logical);
        }
    }

    fn capture(&mut self) -> OwnershipLimit {
        self.epoch = self
            .epoch
            .checked_add(1)
            .expect("small admission epoch exhausted");
        self.dirty = false;
        OwnershipLimit {
            epoch: self.epoch,
            last_segment: self.segments.last_key_value().map_or(0, |(&id, _)| id),
        }
    }
}

pub(crate) enum GcCheck<'a> {
    Ownership(&'a Range<u64>),
    Idle { now: u64, age: u64 },
}

#[derive(Clone)]
pub(crate) struct SmallGcCandidate {
    pub unit: CacheUnit,
    pub ticket: Arc<SmallAccess>,
}

impl Catalog {
    fn gc_candidate(&self, slot: usize, unit: CacheUnit) -> SmallGcCandidate {
        let (address, record) = self.metadata(self.lookup.handle(slot)).unwrap();
        let access = self.compact_access(slot, address, record.stored_len());
        SmallGcCandidate {
            unit,
            ticket: access.small().unwrap().clone(),
        }
    }
}

impl SmallAccess {
    /// Arm the watch under the catalog lock. Consecutive observations share
    /// the same revision until a hit or a re-admission clears the watch.
    pub(crate) fn observe_gc(&self, check: GcCheck<'_>) -> Option<u64> {
        let catalog = self.catalog.upgrade()?;
        let catalog = catalog.write().unwrap();
        let slot = self.slot(&catalog)?;
        if catalog.lookup.flags(slot) & VISIBLE == 0
            || self.state.lock().unwrap().pins != 0
            || !self.gc_eligible(&catalog, slot, check)
        {
            return None;
        }
        let (_, value) = catalog.recency_at(slot)?;
        if Recency::watch_gc(value) {
            self.gc_revision.store(catalog.next_revision(), Relaxed);
        }
        Some(self.gc_revision.load(Relaxed))
    }

    pub(crate) fn gc_is_current(&self, revision: u64) -> bool {
        let Some(catalog) = self.catalog.upgrade() else {
            return false;
        };
        let catalog = catalog.read().unwrap();
        self.slot(&catalog).is_some_and(|slot| {
            catalog.lookup.flags(slot) & VISIBLE != 0
                && self.state.lock().unwrap().pins == 0
                && self.gc_revision.load(Relaxed) == revision
                && catalog
                    .recency_at(slot)
                    .is_some_and(|(_, value)| Recency::gc_unchanged(value))
        })
    }

    pub(crate) fn admitted_in(&self, unit: &CacheUnit, epochs: &Range<u64>) -> bool {
        let Some(catalog) = self.catalog.upgrade() else {
            return false;
        };
        let catalog = catalog.read().unwrap();
        let visible = self
            .slot(&catalog)
            .is_some_and(|slot| catalog.lookup.flags(slot) & VISIBLE != 0);
        // A tombstone may be journaled before accounting completes. Keep a
        // failed deletion pending while the retired locator still accounts for
        // this same admission.
        let retained = || {
            catalog
                .retired
                .get(unit as &dyn SmallIdentity)
                .filter(|entry| entry.live)
                .and_then(|entry| entry.policy.as_ref()?.access.small())
                .is_some_and(|ticket| std::ptr::eq(ticket.as_ref(), self))
        };
        (visible || retained())
            && catalog.ownership.as_ref().is_some_and(|ownership| {
                ownership
                    .epoch_at(*self.address.lock().unwrap())
                    .is_some_and(|epoch| epochs.contains(&epoch))
            })
    }

    fn gc_eligible(&self, catalog: &Catalog, slot: usize, check: GcCheck<'_>) -> bool {
        match check {
            GcCheck::Ownership(epochs) => catalog.ownership.as_ref().is_some_and(|ownership| {
                ownership
                    .epoch_at(*self.address.lock().unwrap())
                    .is_some_and(|epoch| epochs.contains(&epoch))
            }),
            GcCheck::Idle { now, age } => {
                catalog.recency_at(slot).is_some_and(|(recency, value)| {
                    now.saturating_sub(recency.time(value.load(Relaxed))) > age
                })
            }
        }
    }
}

impl SmallObjectStore {
    pub(crate) fn init_ownership(&self) {
        let mut catalog = self.journal.model.0.write().unwrap();
        catalog.ownership = Some(Ownership {
            // A namespace opened after a membership event still needs an
            // hourly pass for any recovered admissions.
            dirty: catalog.live.values().any(|&count| count != 0),
            segments: catalog
                .model
                .segments
                .keys()
                .map(|&segment| (segment, Admissions::new(segment, 0)))
                .collect(),
            ..Ownership::default()
        });
    }

    pub(crate) fn ownership_limit(&self, event: bool) -> Option<OwnershipLimit> {
        let mut catalog = self.journal.model.0.write().unwrap();
        let ownership = catalog.ownership.as_mut()?;
        (event || ownership.dirty).then(|| ownership.capture())
    }

    pub(crate) async fn ownership_candidates(
        &self,
        cursor: &mut OwnershipCursor,
        first_epoch: u64,
        limit: OwnershipLimit,
        mut budget: usize,
        owns: impl Fn(&BlockId) -> bool,
    ) -> (bool, Vec<SmallGcCandidate>) {
        // In-memory relocation of a segment publishes rows one at a time.
        // A sweep must see the source or the completed replacement.
        let _publication = self.journal.readers.read().await;
        let catalog = self.journal.model.0.read().unwrap();
        let mut candidates = Vec::new();
        let ownership = catalog
            .ownership
            .as_ref()
            .expect("small ownership tracking");
        if cursor.segment > limit.last_segment {
            return (true, candidates);
        }
        let epochs = first_epoch..limit.epoch;
        for (&logical, admissions) in ownership
            .segments
            .range(cursor.segment..=limit.last_segment)
        {
            if budget == 0 {
                return (false, candidates);
            }
            let physical = admissions.physical;
            if cursor.segment != logical || cursor.physical != physical {
                budget -= 1;
                *cursor = OwnershipCursor {
                    segment: logical,
                    physical,
                    offset: 0,
                };
            }
            if admissions.latest >= first_epoch {
                let records = &catalog.segment(physical).unwrap().records;
                for row in records.position(cursor.offset)..records.slots() {
                    if budget == 0 {
                        return (false, candidates);
                    }
                    budget -= 1;
                    let offset = records.offset_at(row);
                    cursor.offset = offset + 1;
                    if !epochs.contains(&admissions.epoch(offset)) {
                        continue;
                    }
                    let Some(slot) = catalog.visible_policy_slot(physical, row) else {
                        continue;
                    };
                    let (_, record) = records.get(row).unwrap();
                    let unit = record.key.view().cache_unit();
                    if !owns(unit.block()) {
                        candidates.push(catalog.gc_candidate(slot, unit));
                    }
                }
            }
            cursor.segment = logical + 1;
        }
        (true, candidates)
    }

    pub(crate) fn stale_candidates(
        &self,
        cursor: &mut IdleRecencyCursor,
        now: u64,
        age: u64,
        scan: usize,
        deletes: usize,
    ) -> (bool, Vec<(SmallGcCandidate, u64)>) {
        let catalog = self.journal.model.0.read().unwrap();
        if catalog.recency.is_none() {
            return (true, Vec::new());
        }
        let Some(end) = cursor.end.or_else(|| {
            let (&id, segment) = catalog.model.segments.last_key_value()?;
            Some((id, segment.bytes))
        }) else {
            return (true, Vec::new());
        };
        cursor.end = Some(end);
        let mut candidates = Vec::new();
        let done = catalog.visit_recency(&mut cursor.next, end, scan, |slot, id, row| {
            let (recency, value) = catalog.recency_at(slot).unwrap();
            if now.saturating_sub(recency.time(value.load(Relaxed))) > age {
                let (_, record) = catalog.segment(id).unwrap().records.get(row).unwrap();
                candidates.push(catalog.gc_candidate(slot, record.key.view().cache_unit()));
            }
            candidates.len() < deletes
        });
        drop(catalog);
        let candidates = candidates
            .into_iter()
            .filter_map(|candidate| {
                let revision = candidate.ticket.observe_gc(GcCheck::Idle { now, age })?;
                Some((candidate, revision))
            })
            .collect();
        (done, candidates)
    }
}
