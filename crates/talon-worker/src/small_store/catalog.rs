use super::journal::{Event, Model, SegmentMeta};
use super::lookup::{Lookup, POLICY, REFERENCED, VISIBLE};
use super::{Location, SegmentFile, StagedSegment};
use crate::eviction::{AccessHandle, PolicyEntry, PolicyState};
use crate::small_store::key::{SmallIdentity, SmallKey};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::sync::atomic::{
    AtomicU64, AtomicUsize,
    Ordering::{Relaxed, Release},
};
use std::sync::{Arc, RwLock, Weak};

pub(super) type Address = (u64, u64);

pub(super) struct Retired {
    slot: usize,
    pub key: SmallKey,
    location: Location,
    live: bool,
    pub policy: Option<PolicyEntry>,
}

#[derive(Default)]
pub(super) struct Catalog {
    pub owner: Weak<RwLock<Catalog>>,
    pub model: Model,
    pub lookup: Lookup,
    chunks: Vec<(u64, usize)>,
    free_chunks: Vec<u32>,
    segment_chunks: BTreeMap<u64, Vec<u32>>,
    pub files: BTreeMap<u64, Arc<SegmentFile>>,
    pub policy: HashMap<Address, PolicyEntry>,
    pub access: HashMap<Address, AccessHandle>,
    pub(super) recency: Option<super::recency::Recency>,
    pub(super) ownership: Option<super::gc::Ownership>,
    live: HashMap<usize, usize>,
    pub compact_streams: HashSet<usize>,
    pub compact_totals: HashMap<usize, (u64, usize)>,
    #[cfg(test)]
    pub compact_recovery_slots: usize,
    pub tickets: Arc<super::tickets::Tickets>,
    pub pins: HashMap<Address, Arc<super::policy::SmallAccess>>,
    pub charges: HashMap<Address, u64>,
    pub revision: AtomicU64,
    retired: HashMap<SmallKey, Retired>,
    retired_addresses: BTreeMap<Address, SmallKey>,
    retired_segments: BTreeMap<u64, Arc<SegmentMeta>>,
    // Streams reserve physical IDs before publication. GC orders segments by
    // first publication, and a replacement inherits its source's position.
    published: BTreeMap<u64, u64>,
    publication_positions: HashMap<u64, u64>,
    pub shared: Arc<CatalogState>,
    active: bool,
}

#[derive(Default)]
pub(super) struct CatalogState {
    live_count: AtomicUsize,
    pub last_publication: AtomicU64,
    pub ownership: super::gc::OwnershipEpoch,
}

#[derive(Clone)]
pub(super) struct LiveModel(pub Arc<RwLock<Catalog>>, pub Arc<CatalogState>);

impl Default for LiveModel {
    fn default() -> Self {
        let shared = Arc::new(CatalogState::default());
        let catalog = Arc::new_cyclic(|owner| {
            RwLock::new(Catalog {
                owner: owner.clone(),
                shared: shared.clone(),
                ..Default::default()
            })
        });
        Self(catalog, shared)
    }
}

impl LiveModel {
    pub(super) fn for_replay() -> Self {
        let model = Self::default();
        model.0.write().unwrap().model.next = 1;
        model
    }

    pub(super) fn activate(&self) -> io::Result<Model> {
        let mut catalog = self.0.write().unwrap();
        catalog.activate()?;
        Ok(catalog.model.clone())
    }

    pub(super) fn len(&self) -> usize {
        self.1.live_count.load(Relaxed)
    }

    pub fn lookup(&self, id: &dyn SmallIdentity) -> Option<Location> {
        let hash = super::records::key_hash(id);
        let encoded = super::key_codec::encode(id);
        self.0.read().unwrap().lookup_location(id, hash, &encoded)
    }

    pub fn lookup_read<T, E>(
        &self,
        id: &dyn SmallIdentity,
        validate: impl FnOnce(u64) -> Result<T, E>,
    ) -> Result<Option<(Location, T)>, E> {
        let hash = super::records::key_hash(id);
        let encoded = super::key_codec::encode(id);
        let catalog = self.0.read().unwrap();
        let Some((slot, record)) = catalog.find_record(hash, &encoded, VISIBLE) else {
            return catalog
                .retired_location(id)
                .map(|loc| validate(loc.len).map(|range| (loc, range)))
                .transpose();
        };
        let range = validate(record.len)?;
        let loc = if catalog.has_recency() && catalog.compact_policy(slot, record.stream) {
            let Some(loc) = catalog.payload_location(record, None) else {
                return Ok(None);
            };
            // This guard keeps the row and slot stable through both atomic updates.
            catalog.lookup.reference(slot);
            catalog.touch_recency(slot);
            loc
        } else {
            let Some(loc) = catalog.resolve_location(slot, record) else {
                return Ok(None);
            };
            loc
        };
        Ok(Some((loc, range)))
    }
}

struct ResolvedRecord {
    address: Address,
    len: u64,
    checksum: Option<u64>,
    stream: usize,
}

impl Catalog {
    pub fn hash(&self, key: &dyn SmallIdentity) -> u32 {
        super::records::key_hash(key)
    }

    pub fn segment(&self, id: u64) -> Option<&SegmentMeta> {
        self.model
            .segments
            .get(&id)
            .or_else(|| self.retired_segments.get(&id))
            .map(AsRef::as_ref)
    }

    pub fn record(&self, handle: u32) -> Option<(Address, super::journal::Record)> {
        let (segment, row) = self.row(handle)?;
        let (offset, record) = self.segment(segment)?.records.get(row)?;
        Some(((segment, offset), record))
    }

    pub(super) fn row(&self, handle: u32) -> Option<(u64, usize)> {
        let handle = handle.checked_sub(1)?;
        let (segment, base) = *self.chunks.get((handle >> 10) as usize)?;
        Some((segment, base + (handle & 1023) as usize))
    }

    pub fn metadata(&self, handle: u32) -> Option<(Address, super::records::Metadata)> {
        let (segment, row) = self.row(handle)?;
        let meta = self.segment(segment)?.records.metadata(row)?;
        Some(((segment, meta.offset), meta))
    }

    pub fn matches(&self, handle: u32, key: &[u8]) -> bool {
        let Some((segment, row)) = self.row(handle) else {
            return false;
        };
        self.segment(segment)
            .is_some_and(|meta| meta.records.matches(row, key))
    }

    pub(super) fn handle(&self, segment: u64, row: usize) -> u32 {
        (self.segment_chunks[&segment][row / 1024] << 10 | (row % 1024) as u32) + 1
    }

    pub fn find(&self, key: &dyn SmallIdentity, flags: u8) -> Option<usize> {
        let encoded = super::key_codec::encode(key);
        self.find_record(self.hash(key), &encoded, flags)
            .map(|(slot, _)| slot)
    }

    pub(super) fn find_in_stream(
        &self,
        stream: usize,
        id: &dyn SmallIdentity,
        flags: u8,
    ) -> Option<usize> {
        let encoded = super::key_codec::encode(id);
        self.lookup.find(self.hash(id), |handle, state| {
            state & flags == flags
                && self.row(handle).is_some_and(|(segment, _)| {
                    self.segment(segment)
                        .is_some_and(|meta| meta.slot == stream)
                })
                && self.matches(handle, &encoded)
        })
    }

    fn find_record(&self, hash: u32, encoded: &[u8], flags: u8) -> Option<(usize, ResolvedRecord)> {
        let mut resolved = None;
        let slot = self.lookup.find(hash, |handle, state| {
            if state & flags != flags {
                return false;
            }
            let Some((segment, row)) = self.row(handle) else {
                return false;
            };
            let Some(meta) = self.segment(segment) else {
                return false;
            };
            let Some(record) = meta.records.metadata(row) else {
                return false;
            };
            if record.hash != hash || !meta.records.matches(row, encoded) {
                return false;
            }
            resolved = Some(ResolvedRecord {
                address: (segment, record.offset),
                len: record.len,
                checksum: record.checksum,
                stream: meta.slot,
            });
            true
        })?;
        Some((slot, resolved.unwrap()))
    }

    pub fn at(&self, address: Address) -> Option<usize> {
        let records = &self.segment(address.0)?.records;
        let row = records.position(address.1);
        let record = records.metadata(row)?;
        if record.offset != address.1 {
            return None;
        }
        let handle = self.handle(address.0, row);
        self.lookup.find(record.hash, |found, _| found == handle)
    }

    pub(super) fn visible_policy_slot(&self, segment: u64, row: usize) -> Option<usize> {
        let record = self.segment(segment)?.records.metadata(row)?;
        let handle = self.handle(segment, row);
        self.lookup.find(record.hash, |found, flags| {
            found == handle && flags & (VISIBLE | POLICY) == VISIBLE | POLICY
        })
    }

    fn lookup_location(
        &self,
        id: &dyn SmallIdentity,
        hash: u32,
        encoded: &[u8],
    ) -> Option<Location> {
        if let Some((slot, record)) = self.find_record(hash, encoded, VISIBLE) {
            return self.resolve_location(slot, record);
        }
        self.retired_location(id)
    }

    fn retired_location(&self, id: &dyn SmallIdentity) -> Option<Location> {
        self.retired_location_in(id, None)
    }

    pub(super) fn retired_location_in(
        &self,
        id: &dyn SmallIdentity,
        stream: Option<usize>,
    ) -> Option<Location> {
        self.retired
            .get(id)
            .filter(|entry| entry.live && stream.map_or(true, |stream| entry.slot == stream))
            .map(|entry| entry.location.clone())
    }

    pub(super) fn retired_identity(&self, id: &dyn SmallIdentity) -> Option<SmallKey> {
        self.retired.get(id).map(|entry| entry.key.clone())
    }

    pub(super) fn retired_at(&self, address: Address) -> Option<&Retired> {
        self.retired_addresses
            .get(&address)
            .and_then(|key| self.retired.get(key))
    }

    pub(super) fn retired_offsets(
        &self,
        segment: u64,
        offsets: std::ops::Range<u64>,
    ) -> impl Iterator<Item = u64> + '_ {
        self.retired_addresses
            .range((segment, offsets.start)..(segment, offsets.end))
            .map(|(&(_, offset), _)| offset)
    }

    pub(super) fn has_retired_ticket(
        &self,
        id: &dyn SmallIdentity,
        ticket: &Arc<super::policy::SmallAccess>,
    ) -> bool {
        self.retired
            .get(id)
            .filter(|entry| entry.live)
            .and_then(|entry| entry.policy.as_ref()?.access.small())
            .is_some_and(|current| Arc::ptr_eq(current, ticket))
    }

    pub fn location(&self, slot: usize) -> Option<Location> {
        let resolved = self.resolve_record(slot)?;
        self.resolve_location(slot, resolved)
    }

    pub fn location_for_move(&self, slot: usize) -> Option<Location> {
        self.payload_location(self.resolve_record(slot)?, None)
    }

    fn resolve_record(&self, slot: usize) -> Option<ResolvedRecord> {
        let (address, record) = self.metadata(self.lookup.handle(slot))?;
        Some(ResolvedRecord {
            address,
            len: record.len,
            checksum: record.checksum,
            stream: self.segment(address.0)?.slot,
        })
    }

    fn compact_policy(&self, slot: usize, stream: usize) -> bool {
        self.compact_streams.contains(&stream)
            && self.lookup.flags(slot) & super::lookup::POLICY != 0
    }

    fn resolve_location(&self, slot: usize, record: ResolvedRecord) -> Option<Location> {
        let access = if self.compact_policy(slot, record.stream) {
            Some(self.compact_access(
                slot,
                record.address,
                record.len + super::checksum_len(record.checksum),
            ))
        } else {
            self.policy
                .get(&record.address)
                .map(|entry| entry.access.clone())
                .or_else(|| self.access.get(&record.address).cloned())
        };
        self.payload_location(record, access)
    }

    fn payload_location(
        &self,
        record: ResolvedRecord,
        access: Option<AccessHandle>,
    ) -> Option<Location> {
        Some(Location {
            file: self.files.get(&record.address.0)?.clone(),
            segment: record.address.0,
            offset: record.address.1,
            len: record.len,
            checksum: record.checksum,
            access,
        })
    }

    pub(super) fn stream_len(&self, stream: usize) -> usize {
        self.live.get(&stream).copied().unwrap_or_default()
    }

    pub(super) fn visible_count(&self) -> usize {
        self.shared.live_count.load(Relaxed)
    }

    fn count_visibility(&mut self, stream: usize, visible: bool) {
        let count = self.live.entry(stream).or_default();
        if visible {
            *count += 1;
            self.shared.live_count.fetch_add(1, Relaxed);
        } else {
            *count -= 1;
            self.shared.live_count.fetch_sub(1, Relaxed);
        }
    }

    fn set_visible(&mut self, stream: usize, slot: usize, visible: bool) {
        let flags = self.lookup.flags(slot);
        if (flags & VISIBLE != 0) != visible {
            self.lookup
                .set_flags(slot, (flags & !VISIBLE) | if visible { VISIBLE } else { 0 });
            self.count_visibility(stream, visible);
        }
    }

    pub(super) fn restore_row(
        &mut self,
        stream: usize,
        key: &dyn SmallIdentity,
        location: Location,
    ) -> io::Result<()> {
        if self.find(key, VISIBLE).is_some() {
            return Err(super::format::invalid("duplicate recovered object"));
        }
        let slot = self
            .at((location.segment, location.offset))
            .expect("journaled small object");
        self.files.insert(location.segment, location.file);
        self.publish_segment(location.segment);
        self.set_visible(stream, slot, true);
        Ok(())
    }

    fn prepare_row(&mut self, _stream: usize, _key: &SmallKey, location: &Location) -> usize {
        self.activate().expect("validated small catalog");
        let address = (location.segment, location.offset);
        self.files.insert(address.0, location.file.clone());
        #[cfg(test)]
        if self.at(address).is_none() {
            self.apply(Event::Insert {
                slot: _stream,
                segment: address.0,
                offset: address.1,
                record: super::journal::Record {
                    key: _key.clone(),
                    len: location.len,
                    checksum: location.checksum,
                },
            })
            .expect("test locator metadata");
        }
        self.at(address).expect("journaled small object")
    }

    pub(super) fn stage_row(
        &mut self,
        stream: usize,
        key: &SmallKey,
        location: Location,
    ) -> Option<Location> {
        let new = self.prepare_row(stream, key, &location);
        let old = self.find_in_stream(stream, key, VISIBLE);
        let previous = old.and_then(|slot| self.location_for_move(slot));
        if let Some(old) = old.filter(|&old| old != new) {
            self.set_visible(stream, old, false);
            self.transfer_row(
                old,
                new,
                self.address(old),
                (location.segment, location.offset),
            );
        }
        if let Some(access) = location.access.filter(Self::stores_access) {
            self.attach_access(new, access);
        }
        previous
    }

    pub(super) fn publish_row(
        &mut self,
        stream: usize,
        slot: usize,
        address: Address,
        stored_len: u64,
    ) {
        self.set_visible(stream, slot, true);
        self.publish_segment(address.0);
        if let Some(ownership) = &mut self.ownership {
            // A tail append can bound the current ownership sweep.
            let cutoff = (self.model.segments[&address.0].bytes == address.1 + stored_len)
                .then_some(address.1);
            ownership.admit(address, self.shared.ownership.admit(), cutoff);
        }
    }

    pub(super) fn relocate_row(
        &mut self,
        stream: usize,
        key: &SmallKey,
        location: Location,
        before_publish: impl FnOnce(),
    ) -> Option<Location> {
        let new = self.prepare_row(stream, key, &location);
        let address = (location.segment, location.offset);
        let old = self.find_in_stream(stream, key, VISIBLE);
        let previous = old.and_then(|slot| self.location_for_move(slot));
        let stored_len = location.stored_len();
        if let (Some(old), Some(previous)) = (old, &previous) {
            self.relocate_visible_row(
                old,
                new,
                (previous.segment, previous.offset),
                address,
                before_publish,
            );
        } else {
            before_publish();
        }
        if let Some(access) = location.access.filter(Self::stores_access) {
            self.attach_access(new, access);
        }
        if previous.is_none() {
            self.publish_row(stream, new, address, stored_len);
        }
        previous
    }

    pub(super) fn replace_segment(&mut self, old: u64, replacement: Option<&StagedSegment>) {
        if let Some(new) = replacement {
            self.files.insert(new.id, new.segment.file.clone());
            for (&old_offset, record) in &new.records {
                let old_address = (old, old_offset);
                let new_address = (new.id, record.new_offset);
                let old_slot = self.at(old_address).expect("reconciled source record");
                let new_slot = self.at(new_address).expect("staged replacement record");
                self.relocate_visible_row(old_slot, new_slot, old_address, new_address, || {});
            }
            self.replace_published_segment(old, new.id);
        }
        self.retire_segment(old);
    }

    fn relocate_visible_row(
        &mut self,
        old: usize,
        new: usize,
        old_address: Address,
        new_address: Address,
        before_publish: impl FnOnce(),
    ) {
        if old != new {
            self.transfer_row(old, new, old_address, new_address);
        }
        if let Some(ownership) = &mut self.ownership {
            ownership.relocate_retry(old_address, new_address);
        }
        before_publish();
        // Both rows belong to the same stream. Keep its count and the
        // lock-free total unchanged while moving visibility.
        self.lookup.set_flags(new, self.lookup.flags(new) | VISIBLE);
    }

    fn transfer_row(&mut self, old: usize, new: usize, old_address: Address, new_address: Address) {
        let flags = self.lookup.flags(old);
        self.lookup
            .set_flags(old, flags & !(VISIBLE | POLICY | REFERENCED));
        self.relocate_recency(old_address, new);
        self.relocate_policy(old_address, new_address, new);
        if let Some(policy) = self.policy.remove(&old_address) {
            self.policy.insert(new_address, policy);
        }
        if let Some(access) = self.access.remove(&old_address) {
            self.access.insert(new_address, access);
        }
        self.lookup
            .set_flags(new, self.lookup.flags(new) | flags & (POLICY | REFERENCED));
    }

    pub(super) fn remove_row(&mut self, stream: usize, id: &dyn SmallIdentity) -> Option<Location> {
        if let Some(slot) = self.find_in_stream(stream, id, VISIBLE) {
            let location = self.location(slot);
            self.set_visible(stream, slot, false);
            return location;
        }
        let retired = self.retired.get_mut(id)?;
        if !retired.live {
            return None;
        }
        retired.live = false;
        let location = retired.location.clone();
        self.retired_addresses
            .remove(&(location.segment, location.offset));
        self.prune_retired(id);
        self.count_visibility(stream, false);
        Some(location)
    }

    pub(super) fn policy_get(&self, stream: usize, id: &dyn SmallIdentity) -> Option<PolicyEntry> {
        if let Some(slot) = self.find_in_stream(stream, id, POLICY) {
            if self.compact_streams.contains(&stream) {
                return Some(self.compact_entry(slot));
            }
            return self.policy.get(&self.address(slot)).cloned();
        }
        let mut entry = self.retired.get(id)?.policy.clone()?;
        if let Some(ticket) = entry.access.small() {
            entry.state = *ticket.state.lock().unwrap();
        }
        Some(entry)
    }

    pub(super) fn policy_insert(
        &mut self,
        slot: usize,
        policy: PolicyEntry,
    ) -> Option<AccessHandle> {
        let address = self.metadata(self.lookup.handle(slot))?.0;
        self.lookup
            .set_flags(slot, self.lookup.flags(slot) | POLICY);
        self.access.remove(&address);
        let access = policy.access.clone();
        self.policy.insert(address, policy);
        Some(access)
    }

    pub(super) fn policy_update<T>(
        &mut self,
        stream: usize,
        id: &dyn SmallIdentity,
        update: impl FnOnce(&mut PolicyState) -> T,
    ) -> Option<T> {
        if let Some(slot) = self.find_in_stream(stream, id, POLICY) {
            if self.compact_streams.contains(&stream) {
                let entry = self.compact_entry(slot);
                return Some(self.update_ticket(entry.access.small().unwrap(), true, update));
            }
            let address = self.address(slot);
            return Some(update(&mut self.policy.get_mut(&address)?.state));
        }
        let policy = self.retired.get_mut(id)?.policy.as_mut()?;
        if let Some(ticket) = policy.access.small().cloned() {
            Some(self.update_ticket(&ticket, false, update))
        } else {
            Some(update(&mut policy.state))
        }
    }

    pub(super) fn policy_remove(
        &mut self,
        stream: usize,
        id: &dyn SmallIdentity,
    ) -> Option<PolicyEntry> {
        if let Some(slot) = self.find_in_stream(stream, id, POLICY) {
            if self.compact_streams.contains(&stream) {
                return Some(self.compact_remove(slot));
            }
            let address = self.address(slot);
            self.lookup
                .set_flags(slot, self.lookup.flags(slot) & !POLICY);
            let policy = self.policy.remove(&address)?;
            policy.access.retire();
            return Some(policy);
        }
        let retired = self.retired.get_mut(id)?;
        let mut policy = retired.policy.take()?;
        let stream = retired.slot;
        let address = (retired.location.segment, retired.location.offset);
        if let Some(ticket) = policy.access.small() {
            policy.state = *ticket.state.lock().unwrap();
            self.compact_forget(address, stream, &policy);
        } else {
            policy.access.retire();
        }
        self.prune_retired(id);
        Some(policy)
    }

    fn prune_retired(&mut self, id: &dyn SmallIdentity) {
        if self
            .retired
            .get(id)
            .is_some_and(|entry| !entry.live && entry.policy.is_none())
        {
            self.retired.remove(id);
        }
    }

    fn retire_row(&mut self, address: Address, slot: usize, location: Location) {
        let key = self.record(self.lookup.handle(slot)).unwrap().1.key;
        let live = self.lookup.flags(slot) & VISIBLE != 0;
        if live && self.ownership.is_some() {
            self.retired_addresses.insert(address, key.clone());
        }
        let policy = self.policy.remove(&address).or_else(|| {
            let access = location.access.as_ref()?;
            let state = *access.small()?.state.lock().unwrap();
            Some(PolicyEntry {
                state,
                access: access.clone(),
            })
        });
        // A journal delete removes the indexed row. Its logical visibility
        // and policy survive until the corresponding directory removals.
        self.retired.insert(
            key.clone(),
            Retired {
                slot: self.segment(address.0).unwrap().slot,
                key,
                location,
                live,
                policy,
            },
        );
        self.access.remove(&address);
    }

    pub(super) fn address(&self, slot: usize) -> Address {
        self.metadata(self.lookup.handle(slot))
            .expect("small catalog row")
            .0
    }

    fn reserve(&mut self, additional: usize) {
        if self.lookup.needs_reserve(additional) {
            let mut lookup = std::mem::take(&mut self.lookup);
            let growth = additional.max(lookup.len() / 16);
            lookup.rebuild(growth, |handle| {
                self.metadata(handle).expect("indexed small record").1.hash
            });
            self.lookup = lookup;
        }
    }

    fn allocate_chunks(&mut self, segment: u64, rows: usize) -> io::Result<()> {
        let chunks = self.segment_chunks.entry(segment).or_default();
        while chunks.len() < rows.div_ceil(1024) {
            let entry = (segment, chunks.len() * 1024);
            let chunk = if let Some(chunk) = self.free_chunks.pop() {
                self.chunks[chunk as usize] = entry;
                chunk
            } else {
                if self.chunks.len() >= (1 << 22) - 1 {
                    return Err(io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "small-object directory handles exhausted",
                    ));
                }
                let chunk = self.chunks.len() as u32;
                self.chunks.push(entry);
                chunk
            };
            chunks.push(chunk);
        }
        Ok(())
    }

    fn index_rows(
        &mut self,
        segment: u64,
        start: usize,
        limit: usize,
        flags: &[(u64, u8)],
    ) -> io::Result<()> {
        let Some(meta) = self.segment(segment) else {
            return Ok(());
        };
        let slots = meta.records.slots();
        self.allocate_chunks(segment, slots)?;
        let end = slots.min(limit);
        self.reserve(end.saturating_sub(start));
        for row in start..end {
            let Some(record) = self.segment(segment).unwrap().records.metadata(row) else {
                continue;
            };
            let offset = record.offset;
            let hash = record.hash;
            let state = flags
                .binary_search_by_key(&offset, |(offset, _)| *offset)
                .ok()
                .map_or(0, |i| flags[i].1);
            self.lookup.insert(hash, self.handle(segment, row), state);
        }
        Ok(())
    }

    fn unindex_rows(&mut self, segment: u64, start: usize, limit: usize) -> Vec<(u64, u8)> {
        let Some(meta) = self.segment(segment) else {
            return Vec::new();
        };
        let end = meta.records.slots().min(limit);
        let mut flags = Vec::with_capacity(end.saturating_sub(start));
        for row in start..end {
            let Some(record) = self.segment(segment).unwrap().records.metadata(row) else {
                continue;
            };
            let offset = record.offset;
            let hash = record.hash;
            let handle = self.handle(segment, row);
            if let Some(slot) = self.lookup.find(hash, |found, _| found == handle) {
                flags.push((offset, self.lookup.remove(slot)));
            }
        }
        flags
    }

    pub fn activate(&mut self) -> io::Result<()> {
        if self.active {
            return Ok(());
        }
        for meta in self.model.segments.values_mut() {
            let meta = Arc::make_mut(meta);
            if meta.sealed {
                meta.records.seal();
            } else {
                meta.records.shrink_to_fit();
            }
        }
        let count = self
            .model
            .segments
            .values()
            .map(|meta| meta.records.len())
            .sum();
        self.lookup.rebuild(count, |_| unreachable!());
        let segments: Vec<_> = self.model.segments.keys().copied().collect();
        for segment in segments {
            self.index_rows(segment, 0, usize::MAX, &[])?;
        }
        self.active = true;
        Ok(())
    }

    pub fn retire_segment(&mut self, segment: u64) {
        self.unindex_rows(segment, 0, usize::MAX);
        self.retired_segments.remove(&segment);
        self.files.remove(&segment);
        if let Some(position) = self.publication_positions.remove(&segment) {
            self.published.remove(&position);
        }
        if let Some(ownership) = &mut self.ownership {
            ownership.remove_segment(segment);
        }
        if let Some(recency) = &mut self.recency {
            recency.remove_segment(segment);
        }
        if let Some(chunks) = self.segment_chunks.remove(&segment) {
            self.free_chunks.extend(chunks);
        }
        self.trim_index();
    }

    pub(super) fn published_segments(
        &self,
        range: std::ops::RangeInclusive<u64>,
    ) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.published
            .range(range)
            .map(|(&logical, &physical)| (logical, physical))
    }

    pub(super) fn last_published_segment(&self) -> Option<(u64, u64)> {
        self.published
            .last_key_value()
            .map(|(&logical, &physical)| (logical, physical))
    }

    pub fn replace_published_segment(&mut self, old: u64, new: u64) {
        if let Some(ownership) = &mut self.ownership {
            ownership.relocate_segment(old, new);
        }
        if let Some(position) = self.publication_positions.remove(&old) {
            if let Some(previous) = self.publication_positions.insert(new, position) {
                self.published.remove(&previous);
            }
            self.published.insert(position, new);
        } else {
            self.publish_segment(new);
        }
    }

    pub fn publish_segment(&mut self, segment: u64) {
        self.publication_positions
            .entry(segment)
            .or_insert_with(|| {
                let last = &self.shared.last_publication;
                let position = last
                    .load(Relaxed)
                    .checked_add(1)
                    .expect("small segment publication sequence exhausted");
                self.published.insert(position, segment);
                last.store(position, Release);
                position
            });
    }

    fn trim_index(&mut self) {
        if self.lookup.capacity() > 64 && self.lookup.len() * 4 < self.lookup.capacity() * 3 {
            let mut lookup = std::mem::take(&mut self.lookup);
            lookup.rebuild(0, |handle| {
                self.metadata(handle).expect("indexed small record").1.hash
            });
            self.lookup = lookup;
        }
    }

    pub fn apply(&mut self, event: Event) -> io::Result<()> {
        if !self.active {
            return self.model.apply(event);
        }
        let removing = matches!(&event, Event::Delete { .. } | Event::Recover { .. });
        let replaced = match &event {
            Event::Replace { old, .. } => Some(*old),
            _ => None,
        };
        let changed = match &event {
            Event::Insert {
                segment, offset, ..
            } => Some((
                *segment,
                self.segment(*segment)
                    .map_or(0, |meta| meta.records.position(*offset)),
                usize::MAX,
            )),
            Event::Delete { segment, offset } => {
                let address = (*segment, *offset);
                if let Some(slot) = self.at(address) {
                    if let Some(location) = self.location(slot) {
                        self.retire_row(address, slot, location);
                    }
                }
                self.segment(*segment).map(|meta| {
                    if meta.records.compacts_after_remove() {
                        (*segment, 0, usize::MAX)
                    } else {
                        let row = meta.records.position(*offset);
                        (*segment, row, row + 1)
                    }
                })
            }
            Event::Recover { segment } => Some((*segment, 0, usize::MAX)),
            Event::Replace { old, .. } => {
                if let Some(meta) = self.model.segments.get(old) {
                    self.retired_segments.insert(*old, meta.clone());
                }
                None
            }
            _ => None,
        };
        let flags = changed.map_or_else(Vec::new, |(segment, row, limit)| {
            self.unindex_rows(segment, row, limit)
        });
        let recency_change = self.recency.as_ref().and_then(|_| match &event {
            Event::Insert {
                segment, offset, ..
            } => {
                let records = self.segment(*segment).map(|meta| &meta.records);
                Some(super::recency::Change::Insert {
                    segment: *segment,
                    row: records.map_or(0, |records| records.position(*offset)),
                    old_slots: records.map_or(0, |records| records.slots()),
                })
            }
            Event::Delete { segment, offset } => self.model.segments.get(segment).map(|meta| {
                let row = meta.records.position(*offset);
                if meta.records.compacts_after_remove() {
                    super::recency::Change::Retain {
                        segment: *segment,
                        live: (0..meta.records.slots())
                            .map(|r| r != row && meta.records.metadata(r).is_some())
                            .collect(),
                    }
                } else {
                    super::recency::Change::Delete {
                        segment: *segment,
                        row,
                    }
                }
            }),
            Event::Recover { segment } => {
                self.model
                    .segments
                    .get(segment)
                    .map(|meta| super::recency::Change::Retain {
                        segment: *segment,
                        live: (0..meta.records.position(meta.durable_end))
                            .map(|row| meta.records.metadata(row).is_some())
                            .collect(),
                    })
            }
            _ => None,
        });
        let result = self.model.apply(event);
        if result.is_ok() {
            if let Some(change) = recency_change {
                change.apply(&self.model, self.recency.as_mut().unwrap());
            }
        }
        if let Some((segment, row, limit)) = changed {
            self.index_rows(segment, row, limit, &flags)?;
            if removing {
                self.trim_index();
            }
        }
        if let Some(segment) = replaced {
            let published = self.segment(segment).is_some_and(|meta| {
                (0..meta.records.slots()).any(|row| {
                    let Some(record) = meta.records.metadata(row) else {
                        return false;
                    };
                    let handle = self.handle(segment, row);
                    self.lookup
                        .find(record.hash, |found, _| found == handle)
                        .is_some_and(|slot| {
                            self.lookup.flags(slot) & (VISIBLE | super::lookup::POLICY) != 0
                        })
                })
            });
            if !published {
                self.retire_segment(segment);
            }
        }
        result
    }
}

impl talon_journal::Machine for LiveModel {
    type Record = Event;
    type SnapshotRecord = <Model as talon_journal::Machine>::SnapshotRecord;
    type Snapshot = <Model as talon_journal::Machine>::Snapshot;
    fn encoded_len(record: &Event) -> io::Result<usize> {
        Model::encoded_len(record)
    }
    fn encode(record: &Event) -> io::Result<bytes::Bytes> {
        Model::encode(record)
    }
    fn decode(bytes: &[u8]) -> io::Result<Event> {
        Model::decode(bytes)
    }
    fn apply(&mut self, _: u64, record: Event) -> io::Result<()> {
        self.0.write().unwrap().apply(record)
    }
    fn snapshot(&self) -> io::Result<Self::Snapshot> {
        self.0.read().unwrap().model.snapshot()
    }
    fn decode_snapshot(bytes: &[u8]) -> io::Result<Self::SnapshotRecord> {
        Model::decode_snapshot(bytes)
    }
    fn restore(&mut self, record: Self::SnapshotRecord) -> io::Result<()> {
        self.0.write().unwrap().model.restore(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{Backend, BlockId, ObjectId, Version};

    #[test]
    fn continuous_appends_never_shrink_or_rehash_per_record() {
        let mut catalog = Catalog::default();
        catalog.activate().unwrap();
        let mut capacity = catalog.lookup.capacity();
        for offset in 0..100_000 {
            catalog
                .apply(Event::Insert {
                    slot: 1,
                    segment: 1,
                    offset,
                    record: super::super::journal::Record {
                        key: SmallKey::new(&BlockId::new(
                            ObjectId::new(Backend::S3, "b", offset.to_string()),
                            0,
                            1024,
                            Version::new("v"),
                        )),
                        len: 1,
                        checksum: Some(0),
                    },
                })
                .unwrap();
            assert!(catalog.lookup.capacity() >= capacity);
            capacity = catalog.lookup.capacity();
        }
        assert_eq!(catalog.lookup.len(), 100_000);
        assert!(
            catalog.lookup.rebuilds() < 200,
            "{} rebuilds",
            catalog.lookup.rebuilds()
        );
    }

    #[test]
    fn abandoned_unpublished_segment_releases_records_and_handles() {
        let mut catalog = Catalog::default();
        for offset in 0..100 {
            catalog
                .apply(Event::Insert {
                    slot: 1,
                    segment: 1,
                    offset,
                    record: super::super::journal::Record {
                        key: SmallKey::new(&BlockId::new(
                            ObjectId::new(Backend::S3, "b", offset.to_string()),
                            0,
                            1024,
                            Version::new("v"),
                        )),
                        len: 1,
                        checksum: Some(0),
                    },
                })
                .unwrap();
        }
        catalog.activate().unwrap();
        assert_eq!(catalog.lookup.len(), 100);
        catalog.apply(Event::Replace { old: 1, new: None }).unwrap();
        assert!(catalog.model.segments.is_empty());
        assert!(catalog.retired_segments.is_empty());
        assert!(catalog.segment_chunks.is_empty());
        assert_eq!(catalog.free_chunks.len(), catalog.chunks.len());
        assert_eq!(catalog.lookup.len(), 0);
        assert!(catalog.lookup.capacity() <= 64);
    }
}
