//! Garbage collection over the published compact catalog.
use super::catalog::{Address, Catalog};
use super::key::SmallIdentity;
use super::lookup::{POLICY, VISIBLE};
use super::policy::SmallAccess;
use super::recency::Recency;
use super::SmallObjectStore;
use crate::block_key::ObjectRef;
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

#[derive(Clone, Copy, Default)]
pub(crate) struct SmallStaleCursor {
    next: OwnershipCursor,
    end: Option<OwnershipCursor>,
}

#[derive(Default)]
pub(super) struct Ownership {
    epoch: u64,
    dirty: bool,
    segments: HashMap<u64, Admissions>,
    // Only deferred admissions need an ID. The reverse index follows their
    // physical offsets during publication, without retaining keys or tickets.
    retries: BTreeMap<u64, Address>,
    retry_ids: HashMap<Address, u64>,
    next_retry: u64,
}

struct Admissions {
    first: u64,
    latest: u64,
    end: u64,
    // Freeze the published extent lazily on the first post-boundary mutation.
    saved_end: Option<(u64, u64)>,
    // Only segments that straddle a boundary need per-offset epoch metadata.
    exceptions: HashMap<u64, u64>,
}

impl Admissions {
    fn new(epoch: u64) -> Self {
        Self {
            first: epoch,
            latest: epoch,
            end: 0,
            saved_end: None,
            exceptions: HashMap::new(),
        }
    }

    fn epoch(&self, offset: u64) -> u64 {
        self.exceptions.get(&offset).copied().unwrap_or(self.first)
    }

    fn end_at(&self, epoch: u64) -> u64 {
        self.saved_end
            .filter(|&(boundary, _)| boundary == epoch)
            .map_or(self.end, |(_, end)| end)
    }

    fn insert(&mut self, offset: u64, epoch: u64) {
        self.latest = self.latest.max(epoch);
        self.end = self.end.max(offset + 1);
        if epoch != self.first {
            self.exceptions.insert(offset, epoch);
        }
    }
}

impl Ownership {
    fn epoch_at(&self, (segment, offset): Address) -> Option<u64> {
        self.segments.get(&segment).map(|rows| rows.epoch(offset))
    }

    pub fn admit(&mut self, address @ (segment, offset): Address) {
        let rows = self
            .segments
            .entry(segment)
            .or_insert_with(|| Admissions::new(self.epoch));
        if rows.latest < self.epoch {
            rows.saved_end = Some((self.epoch, rows.end));
        }
        rows.insert(offset, self.epoch);
        self.remove_retry(address);
        self.dirty = true;
    }

    pub fn remove(&mut self, address @ (segment, offset): Address) {
        if let Some(rows) = self.segments.get_mut(&segment) {
            rows.exceptions.remove(&offset);
        }
        self.remove_retry(address);
    }

    pub fn relocate(&mut self, old: Address, new: Address) {
        let source = &self.segments[&old.0];
        let epoch = source.epoch(old.1);
        let prefix = source
            .saved_end
            .filter(|&(boundary, _)| boundary == self.epoch);
        let rows = self
            .segments
            .entry(new.0)
            .or_insert_with(|| Admissions::new(epoch));
        if let Some((boundary, end)) = prefix {
            let (_, moved_end) = rows.saved_end.get_or_insert((boundary, 0));
            if old.1 < end {
                *moved_end = (*moved_end).max(new.1 + 1);
            }
        }
        rows.insert(new.1, epoch);
        if let Some(id) = self.retry_ids.remove(&old) {
            self.retry_ids.insert(new, id);
            self.retries.insert(id, new);
        }
    }

    pub fn remove_segment(&mut self, segment: u64) {
        self.segments.remove(&segment);
    }

    fn remove_retry(&mut self, address: Address) {
        if let Some(id) = self.retry_ids.remove(&address) {
            self.retries.remove(&id);
            if self.retry_ids.capacity() > 64
                && self.retry_ids.len() < self.retry_ids.capacity() / 4
            {
                self.retry_ids.shrink_to_fit();
            }
        }
    }

    fn capture(&mut self, last_segment: u64) -> OwnershipLimit {
        self.epoch = self
            .epoch
            .checked_add(1)
            .expect("small admission epoch exhausted");
        self.dirty = false;
        self.retries = BTreeMap::new();
        self.retry_ids = HashMap::new();
        self.next_retry = 0;
        OwnershipLimit {
            epoch: self.epoch,
            last_segment,
        }
    }

    fn defer(&mut self, address: Address, epoch: u64) {
        if self.epoch == epoch {
            self.retry_ids.entry(address).or_insert_with(|| {
                let id = self.next_retry;
                self.next_retry = id.checked_add(1).expect("small retry ID exhausted");
                self.retries.insert(id, address);
                id
            });
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
    fn ownership_candidate_at(&self, address: Address) -> Option<SmallGcCandidate> {
        self.ownership_candidate_at_if(address, |_| true)
    }

    fn ownership_candidate_at_if(
        &self,
        address: Address,
        eligible: impl FnOnce(&CacheUnit) -> bool,
    ) -> Option<SmallGcCandidate> {
        if let Some(key) = self.retired_addresses.get(&address) {
            let unit = key.view().cache_unit();
            if !eligible(&unit) {
                return None;
            }
            let ticket = self
                .retired
                .get(key)?
                .policy
                .as_ref()?
                .access
                .small()?
                .clone();
            return Some(SmallGcCandidate { unit, ticket });
        }
        let slot = self.at(address)?;
        if self.lookup.flags(slot) & (VISIBLE | POLICY) != VISIBLE | POLICY {
            return None;
        }
        let (_, record) = self.record(self.lookup.handle(slot))?;
        let unit = record.key.view().cache_unit();
        eligible(&unit).then(|| self.gc_candidate(slot, unit))
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
    #[cfg(test)]
    pub(crate) fn ownership_retry_count(&self) -> usize {
        let catalog = self.journal.model.0.read().unwrap();
        catalog.ownership.as_ref().unwrap().retries.len()
    }

    #[cfg(test)]
    pub(crate) fn catalog_revision(&self) -> u64 {
        self.journal.model.0.read().unwrap().revision.load(Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn live_ticket_count(&self) -> usize {
        self.journal.model.0.read().unwrap().tickets.len()
    }

    #[cfg(test)]
    pub(crate) fn assert_no_resident_policy_allocations(&self) {
        let catalog = self.journal.model.0.read().unwrap();
        assert_eq!(catalog.policy.capacity(), 0);
        assert_eq!(catalog.access.capacity(), 0);
        assert_eq!(catalog.tickets.capacity(), 0);
    }

    pub(crate) fn init_ownership(&self) {
        let mut catalog = self.journal.model.0.write().unwrap();
        catalog.ownership = Some(Ownership {
            // A namespace opened after a membership event still needs an
            // hourly pass for any recovered admissions.
            dirty: catalog.live.values().any(|&count| count != 0),
            segments: catalog
                .published
                .values()
                .map(|&physical| {
                    let mut rows = Admissions::new(0);
                    rows.end = catalog.segment(physical).unwrap().bytes;
                    (physical, rows)
                })
                .collect(),
            ..Ownership::default()
        });
    }

    pub(crate) fn ownership_limit(&self, event: bool) -> Option<OwnershipLimit> {
        let mut catalog = self.journal.model.0.write().unwrap();
        let last = catalog.published.last_key_value().map_or(0, |(&id, _)| id);
        let ownership = catalog.ownership.as_mut()?;
        (event || ownership.dirty).then(|| ownership.capture(last))
    }

    pub(crate) fn defer_ownership(&self, candidate: &SmallGcCandidate, epochs: &Range<u64>) {
        let mut catalog = self.journal.model.0.write().unwrap();
        let address = *candidate.ticket.address.lock().unwrap();
        let ownership = catalog.ownership.as_ref().unwrap();
        if !ownership
            .epoch_at(address)
            .is_some_and(|epoch| epochs.contains(&epoch))
        {
            return;
        }
        let visible = candidate
            .ticket
            .slot(&catalog)
            .is_some_and(|slot| catalog.lookup.flags(slot) & VISIBLE != 0);
        let retained = || {
            catalog
                .retired
                .get(&candidate.unit as &dyn SmallIdentity)
                .filter(|entry| entry.live)
                .and_then(|entry| entry.policy.as_ref()?.access.small())
                .is_some_and(|ticket| Arc::ptr_eq(ticket, &candidate.ticket))
        };
        if visible || retained() {
            catalog
                .ownership
                .as_mut()
                .unwrap()
                .defer(address, epochs.end);
        }
    }

    pub(crate) fn has_ownership_retries(&self, limit: OwnershipLimit) -> bool {
        let catalog = self.journal.model.0.read().unwrap();
        let ownership = catalog.ownership.as_ref().unwrap();
        ownership.epoch == limit.epoch && !ownership.retries.is_empty()
    }

    pub(crate) fn ownership_retry_candidates(
        &self,
        cursor: &mut u64,
        limit: OwnershipLimit,
        budget: usize,
    ) -> (bool, Vec<SmallGcCandidate>) {
        let catalog = self.journal.model.0.read().unwrap();
        let ownership = catalog.ownership.as_ref().unwrap();
        if ownership.epoch != limit.epoch {
            return (true, Vec::new());
        }
        let mut candidates = Vec::new();
        let mut retries = ownership.retries.range(*cursor..);
        for (&id, &address) in retries.by_ref().take(budget) {
            *cursor = id + 1;
            if let Some(candidate) = catalog.ownership_candidate_at(address) {
                candidates.push(candidate);
            }
        }
        (retries.next().is_none(), candidates)
    }

    pub(crate) fn ownership_candidates(
        &self,
        cursor: &mut OwnershipCursor,
        first_epoch: u64,
        limit: OwnershipLimit,
        mut budget: usize,
        owns: impl Fn(&BlockId) -> bool,
    ) -> (bool, Vec<SmallGcCandidate>) {
        let catalog = self.journal.model.0.read().unwrap();
        let mut candidates = Vec::new();
        let ownership = catalog
            .ownership
            .as_ref()
            .expect("small ownership tracking");
        if cursor.segment > limit.last_segment || ownership.epoch != limit.epoch {
            return (true, candidates);
        }
        let epochs = first_epoch..limit.epoch;
        for (&logical, &physical) in catalog.published.range(cursor.segment..=limit.last_segment) {
            if budget == 0 {
                return (false, candidates);
            }
            if cursor.segment != logical || cursor.physical != physical {
                budget -= 1;
                *cursor = OwnershipCursor {
                    segment: logical,
                    physical,
                    offset: 0,
                };
            }
            let admissions = &ownership.segments[&physical];
            if admissions.latest >= first_epoch {
                let end = admissions.end_at(limit.epoch);
                let records = &catalog.segment(physical).unwrap().records;
                let mut rows = (records.position(cursor.offset)..records.position(end))
                    .map(|row| records.offset_at(row))
                    .peekable();
                let mut retired = catalog
                    .retired_addresses
                    .range((physical, cursor.offset.min(end))..(physical, end))
                    .map(|(&(_, offset), _)| offset)
                    .peekable();
                while let Some(offset) =
                    rows.peek().into_iter().chain(retired.peek()).min().copied()
                {
                    if budget == 0 {
                        return (false, candidates);
                    }
                    budget -= 1;
                    cursor.offset = offset + 1;
                    if rows.peek() == Some(&offset) {
                        rows.next();
                    }
                    if retired.peek() == Some(&offset) {
                        retired.next();
                    }
                    if epochs.contains(&admissions.epoch(offset)) {
                        if let Some(candidate) = catalog
                            .ownership_candidate_at_if((physical, offset), |unit| {
                                !owns(unit.block())
                            })
                        {
                            candidates.push(candidate);
                        }
                    }
                }
            }
            cursor.segment = logical + 1;
        }
        (true, candidates)
    }

    pub(crate) fn stale_candidates(
        &self,
        cursor: &mut SmallStaleCursor,
        now: u64,
        age: u64,
        scan: usize,
        deletes: usize,
        due: impl Fn(ObjectRef<'_>) -> bool,
    ) -> (bool, Vec<(SmallGcCandidate, u64)>) {
        let catalog = self.journal.model.0.read().unwrap();
        if catalog.recency.is_none() {
            return (true, Vec::new());
        }
        let mut candidates = Vec::new();
        let mut key = std::array::from_fn(|_| Vec::new());
        let done = cursor.visit(&catalog, scan, |slot, id, row| {
            let (recency, value) = catalog.recency_at(slot).unwrap();
            if now.saturating_sub(recency.time(value.load(Relaxed))) > age {
                let records = &catalog.segment(id).unwrap().records;
                if due(records.object(row, &mut key)) {
                    let (_, record) = records.get(row).unwrap();
                    candidates.push(catalog.gc_candidate(slot, record.key.view().cache_unit()));
                }
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

impl SmallStaleCursor {
    fn visit(
        &mut self,
        catalog: &Catalog,
        mut budget: usize,
        mut visit: impl FnMut(usize, u64, usize) -> bool,
    ) -> bool {
        let Some(end) = self.end.or_else(|| {
            let (&segment, &physical) = catalog.published.last_key_value()?;
            let offset = catalog.segment(physical)?.bytes;
            Some(OwnershipCursor {
                segment,
                physical,
                offset,
            })
        }) else {
            return true;
        };
        self.end = Some(end);
        if self.next.segment > end.segment {
            return true;
        }
        for (&segment, &physical) in catalog.published.range(self.next.segment..=end.segment) {
            if budget == 0 {
                return false;
            }
            if self.next.segment != segment || self.next.physical != physical {
                self.next = OwnershipCursor {
                    segment,
                    physical,
                    offset: 0,
                };
            }
            let records = &catalog.segment(physical).unwrap().records;
            let start = records.position(self.next.offset);
            // A replacement is sealed and kept the logical position of its
            // source. The fixed idle cutoff excludes fresh, late appends.
            let stop = if segment == end.segment && physical == end.physical {
                records.position(end.offset)
            } else {
                records.slots()
            };
            for row in start..stop {
                if budget == 0 {
                    return false;
                }
                budget -= 1;
                self.next.offset = records.offset_at(row) + 1;
                if let Some(slot) = catalog.visible_policy_slot(physical, row) {
                    if !visit(slot, physical, row) {
                        return false;
                    }
                }
            }
            budget -= usize::from(start >= stop);
            self.next.segment = segment + 1;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use talon_core::{Backend, ObjectId, Version};

    fn compact_store(root: &std::path::Path) -> Arc<SmallObjectStore> {
        let store = Arc::new(
            SmallObjectStore::open(root.to_owned(), 128 << 10, 256 << 10, 0, None).unwrap(),
        );
        let index = Arc::new(crate::BlockIndex::new());
        store.restore_policy_units(&index, &mut Vec::new(), false);
        store.init_recency();
        store.bind_policy(Arc::new(crate::Lru::from_recovered(
            index,
            Vec::new(),
            0,
            true,
        )));
        store
    }

    fn block(n: usize) -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "bucket", format!("key-{n}")),
            0,
            128 << 10,
            Version::new("v1"),
        )
    }

    async fn create_compaction_work(store: &Arc<SmallObjectStore>, survivors: usize) {
        let garbage = block(0);
        store
            .put(&garbage, Bytes::from(vec![2; 96 << 10]))
            .await
            .unwrap();
        for n in 1..=survivors {
            store
                .put(&block(n), Bytes::from_static(b"packed"))
                .await
                .unwrap();
        }
        store.delete(&garbage).await.unwrap();
        store.policy().unwrap().remove(&CacheUnit::Whole(garbage));
    }

    #[tokio::test]
    async fn compact_relocation_copies_payload_without_creating_survivor_tickets() {
        let root = tempfile::tempdir().unwrap();
        let store = compact_store(root.path());
        create_compaction_work(&store, 256).await;
        let now = crate::eviction::access_millis() + 1000;
        let (_, selected) =
            store.stale_candidates(&mut SmallStaleCursor::default(), now, 1, 4096, 1, |_| true);
        let (candidate, revision) = &selected[0];
        let before = store.catalog_revision();
        let source = store
            .lookup_entry(candidate.unit.block())
            .unwrap()
            .unwrap()
            .segment();
        assert_eq!(store.live_ticket_count(), 1);
        assert!(store.maintain(true).await.unwrap());
        assert_eq!(store.catalog_revision(), before);
        assert_eq!(store.live_ticket_count(), 1);
        assert_ne!(
            store
                .lookup_entry(candidate.unit.block())
                .unwrap()
                .unwrap()
                .segment(),
            source
        );
        assert!(candidate.ticket.gc_is_current(*revision));
        for n in 1..=256 {
            assert_eq!(
                store.read(&block(n)).await.unwrap(),
                Some(Bytes::from_static(b"packed"))
            );
        }
    }

    #[tokio::test]
    async fn active_stale_scan_uses_readable_source_until_replacement_publication() {
        let root = tempfile::tempdir().unwrap();
        let store = compact_store(root.path());
        create_compaction_work(&store, 3).await;

        let mut cursor = SmallStaleCursor::default();
        let now = crate::eviction::access_millis() + 1000;
        let (done, selected) = store.stale_candidates(&mut cursor, now, 1, 8, 1, |_| true);
        assert!(!done);
        assert_eq!(selected[0].0.unit, CacheUnit::Whole(block(1)));
        drop(selected);

        let pause = Arc::new(super::super::tests::AppendPause::default());
        *store.streams()[0].relocation_pause.lock().unwrap() = Some(pause.clone());
        let (reclaimed, ()) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(store.maintain(true), async {
                pause.entered.notified().await;
                // Replace is journaled, but the payload still resolves through
                // the published source until the locator swap.
                let source = store.lookup_entry(&block(2)).unwrap().unwrap().segment();
                assert!(!store
                    .journal
                    .model
                    .0
                    .read()
                    .unwrap()
                    .model
                    .segments
                    .contains_key(&source));
                let (done, selected) = store.stale_candidates(&mut cursor, now, 1, 8, 8, |_| true);
                assert!(done);
                assert_eq!(
                    selected
                        .into_iter()
                        .map(|(candidate, _)| candidate.unit)
                        .collect::<Vec<_>>(),
                    [CacheUnit::Whole(block(2)), CacheUnit::Whole(block(3))]
                );
                pause.resume.notify_one();
            })
        })
        .await
        .unwrap();
        assert!(reclaimed.unwrap());
    }

    #[tokio::test]
    async fn stale_scan_charges_empty_segments_and_resumes_with_a_budget_of_one() {
        const EMPTY_SEGMENTS: usize = 8;
        let root = tempfile::tempdir().unwrap();
        let store = compact_store(root.path());
        let payload = Bytes::from(vec![1; 96 << 10]);
        for n in 0..=EMPTY_SEGMENTS * 2 {
            store.put(&block(n), payload.clone()).await.unwrap();
        }
        for n in 0..EMPTY_SEGMENTS * 2 {
            store.delete(&block(n)).await.unwrap();
        }
        {
            let catalog = store.journal.model.0.read().unwrap();
            assert_eq!(catalog.model.segments.len(), EMPTY_SEGMENTS + 1);
            assert!(catalog
                .model
                .segments
                .values()
                .take(EMPTY_SEGMENTS)
                .all(|segment| segment.records.slots() == 0));
        }

        let now = crate::eviction::access_millis() + 2;
        let mut cursor = SmallStaleCursor::default();
        for _ in 0..EMPTY_SEGMENTS {
            let (done, candidates) = store.stale_candidates(&mut cursor, now, 1, 1, 1, |_| true);
            assert!(!done);
            assert!(candidates.is_empty());
        }
        let (_, candidates) = store.stale_candidates(&mut cursor, now, 1, 1, 1, |_| true);
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].0.unit,
            CacheUnit::Whole(block(EMPTY_SEGMENTS * 2))
        );
        assert!(
            store
                .stale_candidates(&mut cursor, now, 1, 1, 1, |_| true)
                .0
        );
    }
}
