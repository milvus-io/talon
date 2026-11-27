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
use std::sync::{
    atomic::{
        AtomicU64,
        Ordering::{AcqRel, Acquire, Relaxed, Release},
    },
    Arc,
};
use talon_core::BlockId;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct OwnershipLimit {
    pub epoch: u64,
    last_segment: u64,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct PublishedCursor {
    segment: u64,
    physical: u64,
    offset: u64,
}

impl PublishedCursor {
    fn enter_segment(&mut self, segment: u64, physical: u64) -> bool {
        if self.segment == segment && self.physical == physical {
            return false;
        }
        // Compaction keeps the logical position but replaces the row offsets.
        *self = Self {
            segment,
            physical,
            offset: 0,
        };
        true
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct SmallStaleCursor {
    next: PublishedCursor,
    end: Option<PublishedCursor>,
}

const DIRTY: u64 = 1 << 63;

#[derive(Default)]
pub(super) struct OwnershipEpoch(AtomicU64);

impl OwnershipEpoch {
    fn current(&self) -> u64 {
        self.0.load(Acquire) & !DIRTY
    }

    pub(super) fn admit(&self) -> u64 {
        self.0.fetch_or(DIRTY, AcqRel) & !DIRTY
    }

    fn capture(&self, event: bool) -> Option<u64> {
        // Tagging an admission and marking it dirty are one operation. An
        // admission consumed here has already published its segment position.
        let previous = self
            .0
            .fetch_update(AcqRel, Acquire, |state| {
                (state != 0 && (event || state & DIRTY != 0)).then(|| {
                    let epoch = (state & !DIRTY) + 1;
                    assert!(epoch < DIRTY, "small admission epoch exhausted");
                    epoch
                })
            })
            .ok()?;
        Some((previous & !DIRTY) + 1)
    }
}

#[derive(Default)]
pub(super) struct Ownership {
    segments: HashMap<u64, SegmentAdmission>,
    retries: OwnershipRetries,
}

#[derive(Default)]
struct SegmentAdmission {
    epoch: u64,
    cutoff: Option<u64>,
}

#[derive(Default)]
pub(crate) struct OwnershipRetries {
    epoch: u64,
    addresses: BTreeMap<u64, Address>,
    // The reverse index follows observed admissions through relocation without
    // retaining keys or tickets.
    ids: HashMap<Address, u64>,
    next: u64,
}

impl OwnershipRetries {
    fn register(&mut self, address: Address) -> u64 {
        *self.ids.entry(address).or_insert_with(|| {
            let id = self.next;
            self.next = id.checked_add(1).expect("small retry ID exhausted");
            self.addresses.insert(id, address);
            id
        })
    }

    fn remove(&mut self, address: Address) {
        if let Some(id) = self.ids.remove(&address) {
            self.addresses.remove(&id);
            if self.ids.capacity() > 64 && self.ids.len() < self.ids.capacity() / 4 {
                self.ids.shrink_to_fit();
            }
        }
    }

    fn relocate(&mut self, old: Address, new: Address) {
        if let Some(id) = self.ids.remove(&old) {
            self.ids.insert(new, id);
            self.addresses.insert(id, new);
        }
    }

    fn contains(&self, id: u64, address: Address) -> bool {
        self.addresses.get(&id) == Some(&address)
    }

    fn is_empty(&self) -> bool {
        self.addresses.is_empty()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.addresses.len()
    }

    fn iter_from(&self, id: u64) -> impl Iterator<Item = (u64, Address)> + '_ {
        self.addresses
            .range(id..)
            .map(|(&id, &address)| (id, address))
    }
}

impl Ownership {
    pub fn admit(&mut self, address: Address, epoch: u64, cutoff: Option<u64>) {
        let segment = self.segments.entry(address.0).or_default();
        if segment.epoch != epoch {
            *segment = SegmentAdmission { epoch, cutoff };
        }
        self.remove(address);
    }

    pub fn remove(&mut self, address: Address) {
        self.retries.remove(address);
    }

    pub fn relocate_retry(&mut self, old: Address, new: Address) {
        self.retries.relocate(old, new);
    }

    pub fn relocate_segment(&mut self, old: u64, new: u64) {
        if let Some(mut segment) = self.segments.remove(&old) {
            segment.cutoff = None;
            self.segments.insert(new, segment);
        }
    }

    pub fn remove_segment(&mut self, segment: u64) {
        self.segments.remove(&segment);
    }
}

#[derive(Clone)]
pub(crate) struct SmallGcCandidate {
    pub unit: CacheUnit,
    pub ticket: Arc<SmallAccess>,
    retry_id: Option<u64>,
}

impl SmallGcCandidate {
    pub(crate) fn observe_ownership(&mut self, epoch: u64) -> Option<u64> {
        let ticket = &self.ticket;
        let catalog = ticket.catalog.upgrade()?;
        let mut catalog = catalog.write().unwrap();
        let retries = &catalog.ownership.as_ref()?.retries;
        if catalog.shared.ownership.current() != epoch || retries.epoch != epoch {
            return None;
        }
        let address = *ticket.address.lock().unwrap();
        let slot = ticket.visible_gc_slot(&catalog);
        if let Some(id) = self.retry_id {
            if !retries.contains(id, address) {
                return None;
            }
        } else {
            if slot.is_none() && !catalog.has_retired_ticket(&self.unit, ticket) {
                return None;
            }
            // Register under the same lock as the first observation. Re-admission
            // can revoke the work even while this attempt waits for delete credit.
            let retries = &mut catalog.ownership.as_mut().unwrap().retries;
            self.retry_id = Some(retries.register(address));
        }
        ticket.watch_gc(&catalog, slot?)
    }
}

impl Catalog {
    fn gc_candidate(&self, slot: usize, unit: CacheUnit) -> SmallGcCandidate {
        let (address, record) = self.metadata(self.lookup.handle(slot)).unwrap();
        let access = self.compact_access(slot, address, record.stored_len());
        SmallGcCandidate {
            unit,
            ticket: access.small().unwrap().clone(),
            retry_id: None,
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
        if let Some(entry) = self.retired_at(address) {
            let unit = entry.key.view().cache_unit();
            if !eligible(&unit) {
                return None;
            }
            let ticket = entry.policy.as_ref()?.access.small()?.clone();
            return Some(SmallGcCandidate {
                unit,
                ticket,
                retry_id: None,
            });
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
    fn visible_gc_slot(&self, catalog: &Catalog) -> Option<usize> {
        self.slot(catalog)
            .filter(|&slot| catalog.lookup.flags(slot) & VISIBLE != 0)
    }

    fn observe_idle(&self, now: u64, age: u64) -> Option<u64> {
        let catalog = self.catalog.upgrade()?;
        let catalog = catalog.write().unwrap();
        let slot = self.visible_gc_slot(&catalog)?;
        let (recency, value) = catalog.recency_at(slot)?;
        if now.saturating_sub(recency.time(value.load(Relaxed))) <= age {
            return None;
        }
        self.watch_gc(&catalog, slot)
    }

    /// Arm the watch under the catalog lock. Consecutive observations share
    /// the same revision until a hit or a re-admission clears the watch.
    fn watch_gc(&self, catalog: &Catalog, slot: usize) -> Option<u64> {
        if self.state.lock().unwrap().pins != 0 {
            return None;
        }
        let (_, value) = catalog.recency_at(slot)?;
        if Recency::watch_gc(value) || self.gc_revision.load(Relaxed) == 0 {
            self.gc_revision.store(catalog.next_revision(), Relaxed);
        }
        Some(self.gc_revision.load(Relaxed))
    }

    pub(crate) fn gc_is_current(&self, revision: u64) -> bool {
        self.gc_is_current_for_unlink(revision, false)
    }

    pub(super) fn gc_is_current_for_unlink(&self, revision: u64, nonblocking: bool) -> bool {
        let Some(catalog) = self.catalog.upgrade() else {
            return false;
        };
        let Some(catalog) = super::read_for_unlink(&catalog, nonblocking) else {
            return false;
        };
        let Some(slot) = self.slot_for_unlink(&catalog, nonblocking) else {
            return false;
        };
        if catalog.lookup.flags(slot) & VISIBLE == 0 {
            return false;
        }
        let state = if nonblocking {
            crate::runtime::uncontended(self.state.try_lock())
        } else {
            Some(self.state.lock().unwrap())
        };
        state.is_some_and(|state| {
            state.pins == 0
                && self.gc_revision.load(Relaxed) == revision
                && catalog
                    .recency_at(slot)
                    .is_some_and(|(_, value)| Recency::gc_unchanged(value))
        })
    }
}

impl SmallObjectStore {
    #[cfg(test)]
    pub(crate) fn under_catalog_writer<T: Send>(&self, work: impl FnOnce() -> T + Send) -> T {
        std::thread::scope(|scope| {
            let (send, receive) = std::sync::mpsc::channel();
            let result = {
                let _catalog = self.journal.model.0.write().unwrap();
                scope.spawn(move || send.send(work()).ok());
                receive.recv_timeout(std::time::Duration::from_secs(2))
            };
            result.expect("operation waited for catalog writer")
        })
    }

    #[cfg(test)]
    pub(crate) fn packed_access_time(&self, key: &dyn SmallIdentity, set: Option<u64>) -> u64 {
        let catalog = self.journal.model.0.write().unwrap();
        let slot = catalog.find(key, VISIBLE).unwrap();
        let (recency, value) = catalog.recency_at(slot).unwrap();
        if let Some(timestamp) = set {
            value.store(timestamp, Relaxed);
        }
        recency.time(value.load(Relaxed))
    }

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
        catalog.ownership = Some(Ownership::default());
        // Recovered segments can need a supplement even without an event.
        let dirty = if self.len() != 0 { DIRTY } else { 0 };
        catalog.shared.ownership.0.store(1 | dirty, Release);
    }

    pub(crate) fn ownership_limit(&self, event: bool) -> Option<OwnershipLimit> {
        let shared = &self.journal.model.1;
        let epoch = shared.ownership.capture(event)?;
        Some(OwnershipLimit {
            epoch,
            last_segment: shared.last_publication.load(Acquire),
        })
    }

    pub(crate) fn ownership_is_current(&self, limit: OwnershipLimit) -> bool {
        self.journal.model.1.ownership.current() == limit.epoch
    }

    // The collector takes ownership of old maps after leaving the membership
    // lock. Return them so their destruction also happens outside the catalog.
    pub(crate) fn take_superseded_retries(
        &self,
        limit: OwnershipLimit,
    ) -> Option<OwnershipRetries> {
        let mut catalog = self.journal.model.0.write().unwrap();
        let retries = &mut catalog.ownership.as_mut()?.retries;
        if !self.ownership_is_current(limit) || retries.epoch == limit.epoch {
            return None;
        }
        let old = std::mem::take(retries);
        retries.epoch = limit.epoch;
        Some(old)
    }

    pub(crate) fn has_ownership_retries(&self, limit: OwnershipLimit) -> bool {
        let catalog = self.journal.model.0.read().unwrap();
        let retries = &catalog.ownership.as_ref().unwrap().retries;
        self.ownership_is_current(limit) && retries.epoch == limit.epoch && !retries.is_empty()
    }

    pub(crate) fn ownership_retry_candidates(
        &self,
        cursor: &mut u64,
        limit: OwnershipLimit,
        budget: usize,
    ) -> (bool, Vec<SmallGcCandidate>) {
        let catalog = self.journal.model.0.read().unwrap();
        let retries = &catalog.ownership.as_ref().unwrap().retries;
        if !self.ownership_is_current(limit) || retries.epoch != limit.epoch {
            return (true, Vec::new());
        }
        let mut candidates = Vec::new();
        let mut retries = retries.iter_from(*cursor);
        for (id, address) in retries.by_ref().take(budget) {
            *cursor = id + 1;
            if let Some(mut candidate) = catalog.ownership_candidate_at(address) {
                candidate.retry_id = Some(id);
                candidates.push(candidate);
            }
        }
        (retries.next().is_none(), candidates)
    }

    pub(crate) fn ownership_candidates(
        &self,
        cursor: &mut PublishedCursor,
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
        if cursor.segment > limit.last_segment || !self.ownership_is_current(limit) {
            return (true, candidates);
        }
        for (logical, physical) in catalog.published_segments(cursor.segment..=limit.last_segment) {
            if budget == 0 {
                return (false, candidates);
            }
            if cursor.enter_segment(logical, physical) {
                budget -= 1;
            }
            let default = SegmentAdmission::default();
            let admission = ownership.segments.get(&physical).unwrap_or(&default);
            if admission.epoch >= first_epoch {
                let records = &catalog.segment(physical).unwrap().records;
                let end = admission
                    .cutoff
                    .filter(|_| admission.epoch == limit.epoch)
                    .unwrap_or(u64::MAX)
                    .max(cursor.offset);
                let mut rows = (records.position(cursor.offset)..records.position(end))
                    .map(|row| records.offset_at(row))
                    .peekable();
                let mut retired = catalog
                    .retired_offsets(physical, cursor.offset..end)
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
                    if let Some(candidate) = catalog
                        .ownership_candidate_at_if((physical, offset), |unit| !owns(unit.block()))
                    {
                        candidates.push(candidate);
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
        if !catalog.has_recency() {
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
                let revision = candidate.ticket.observe_idle(now, age)?;
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
            let (segment, physical) = catalog.last_published_segment()?;
            let offset = catalog.segment(physical)?.bytes;
            Some(PublishedCursor {
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
        for (segment, physical) in catalog.published_segments(self.next.segment..=end.segment) {
            if budget == 0 {
                return false;
            }
            self.next.enter_segment(segment, physical);
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

    #[test]
    fn ownership_retry_id_tracks_relocation_and_removal() {
        let mut retries = OwnershipRetries::default();
        let old = (1, 0);
        let other = (1, 8);
        let new = (2, 0);
        let id = retries.register(old);
        let other_id = retries.register(other);

        retries.relocate(old, new);
        assert!(!retries.contains(id, old));
        assert_eq!(retries.register(new), id);
        retries.remove(old);
        assert!(retries.iter_from(id).eq([(id, new), (other_id, other)]));

        retries.remove(new);
        assert!(retries.iter_from(id).eq([(other_id, other)]));
        assert!(retries.register(new) > other_id);
    }

    #[tokio::test]
    async fn both_collectors_rescan_replaced_segment_from_start() {
        let root = tempfile::tempdir().unwrap();
        let store = compact_store(root.path());
        store.init_ownership();
        create_compaction_work(&store, 3).await;
        let limit = store.ownership_limit(true).unwrap();
        let now = crate::eviction::access_millis() + 1000;

        let mut ownership = PublishedCursor::default();
        let first = (0..8)
            .find_map(|_| {
                let (done, mut candidates) =
                    store.ownership_candidates(&mut ownership, 0, limit, 1, |_| false);
                assert!(!done);
                candidates.pop()
            })
            .unwrap();
        assert_eq!(first.unit, CacheUnit::Whole(block(1)));

        let mut stale = SmallStaleCursor::default();
        let (done, selected) = store.stale_candidates(&mut stale, now, 1, 8, 1, |_| true);
        assert!(!done);
        assert_eq!(selected[0].0.unit, first.unit);
        assert!(store.maintain(true).await.unwrap());

        let expected = [1, 2, 3].map(|n| CacheUnit::Whole(block(n)));
        let (done, selected) = store.ownership_candidates(&mut ownership, 0, limit, 8, |_| false);
        assert!(done);
        assert!(selected
            .into_iter()
            .map(|candidate| candidate.unit)
            .eq(expected.clone()));

        let (done, selected) = store.stale_candidates(&mut stale, now, 1, 8, 8, |_| true);
        assert!(done);
        assert!(selected
            .into_iter()
            .map(|(candidate, _)| candidate.unit)
            .eq(expected));
    }

    #[tokio::test]
    async fn ownership_scans_mixed_admissions_across_relocation() {
        let root = tempfile::tempdir().unwrap();
        let store = compact_store(root.path());
        store.init_ownership();
        create_compaction_work(&store, 1).await;
        let id = block(1);
        let limit = store.ownership_limit(true).unwrap();
        let scan = |first, limit| {
            store
                .ownership_candidates(&mut PublishedCursor::default(), first, limit, 8, |_| false)
                .1
        };
        drop(store.take_superseded_retries(limit));
        let mut candidate = scan(0, limit).pop().unwrap();
        let initial = candidate.observe_ownership(limit.epoch).unwrap();
        let ticket = &candidate.ticket;
        store
            .put(&block(2), Bytes::from_static(b"late"))
            .await
            .unwrap();
        assert!(store.maintain(true).await.unwrap());
        assert_eq!(scan(0, limit).len(), 2);
        assert!(ticket.gc_is_current(initial));
        let (done, mut retries) = store.ownership_retry_candidates(&mut 0, limit, 8);
        assert!(done);
        assert_eq!(retries.len(), 1);
        assert_eq!(retries[0].unit.block(), &id);
        assert_eq!(retries[0].observe_ownership(limit.epoch), Some(initial));
        assert_eq!(store.len(), 2);
        let supplement = store.ownership_limit(false).unwrap();
        assert!(scan(0, limit).is_empty());
        assert_eq!(scan(limit.epoch, supplement).len(), 2);

        drop(store.take_superseded_retries(supplement));
        store.put(&id, Bytes::from_static(b"packed")).await.unwrap();
        assert!(!ticket.gc_is_current(initial));
        let mut candidate = scan(limit.epoch, supplement)
            .into_iter()
            .find(|candidate| candidate.unit.block() == &id)
            .unwrap();
        let after_admission = candidate.observe_ownership(supplement.epoch).unwrap();
        drop(
            store
                .policy()
                .unwrap()
                .pin_guard(candidate.unit.clone())
                .unwrap(),
        );
        assert!(!ticket.gc_is_current(after_admission));
        let after_pin = candidate.observe_ownership(supplement.epoch).unwrap();
        assert!(store.read(&id).await.unwrap().is_some());
        assert!(!ticket.gc_is_current(after_pin));

        assert_eq!(store.len(), 2);
        let next = store.ownership_limit(false).unwrap();
        assert_eq!(scan(supplement.epoch, next).len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn packed_ownership_deadline_skips_registry_during_stream_wait() {
        let root = tempfile::tempdir().unwrap();
        let store = compact_store(root.path());
        store.init_ownership();
        let id = block(1);
        store.put(&id, Bytes::from_static(b"packed")).await.unwrap();
        let limit = store.ownership_limit(true).unwrap();
        drop(store.take_superseded_retries(limit));
        let (_, mut rows) =
            store.ownership_candidates(&mut Default::default(), 0, limit, 8, |_| false);
        let mut candidate = rows.pop().unwrap();
        let revision = candidate.observe_ownership(limit.epoch).unwrap();
        let streams = store.streams();
        use std::time::{Duration, Instant};
        let runtime = tokio::runtime::Handle::current();
        for publication in [true, false] {
            let (_publication, _state);
            if publication {
                _publication = streams[0].publication.lock().await;
            } else {
                _state = streams[0].state.lock().await;
            }
            store.under_mutation_registry(|| {
                runtime.block_on(async {
                    let deadline = Instant::now() + Duration::from_millis(150);
                    let deletion =
                        store.delete_gc(candidate.clone(), revision, Some(deadline), || {
                            panic!("authorization after a stalled wait")
                        });
                    tokio::pin!(deletion);
                    // Registry contention must not short-circuit the stream wait.
                    assert!(
                        tokio::time::timeout(Duration::from_millis(20), &mut deletion)
                            .await
                            .is_err()
                    );
                    assert!(!tokio::time::timeout(Duration::from_secs(2), &mut deletion)
                        .await
                        .expect("deletion must finish before the stream lock is released")
                        .unwrap());
                });
            });
        }
        assert!(store.contains(&id));
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
