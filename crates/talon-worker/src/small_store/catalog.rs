use super::journal::{Event, Model, SegmentMeta};
use super::lookup::{Lookup, VISIBLE};
use super::{Location, SegmentFile};
use crate::eviction::{AccessHandle, PolicyEntry};
use crate::small_store::key::{SmallIdentity, SmallKey};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock, Weak};

pub(super) type Address = (u64, u64);

pub(super) struct Retired {
    pub slot: usize,
    pub key: SmallKey,
    pub location: Location,
    pub live: bool,
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
    pub live: HashMap<usize, usize>,
    pub compact_streams: HashSet<usize>,
    pub compact_totals: HashMap<usize, (u64, usize)>,
    pub tickets: Arc<super::tickets::Tickets>,
    pub pins: HashMap<Address, Arc<super::policy::SmallAccess>>,
    pub charges: HashMap<Address, u64>,
    pub revision: AtomicU64,
    pub retired: HashMap<SmallKey, Retired>,
    retired_segments: BTreeMap<u64, Arc<SegmentMeta>>,
    active: bool,
}

#[derive(Clone)]
pub(super) struct LiveModel(pub Arc<RwLock<Catalog>>);

impl Default for LiveModel {
    fn default() -> Self {
        Self(Arc::new_cyclic(|owner| {
            RwLock::new(Catalog {
                owner: owner.clone(),
                ..Default::default()
            })
        }))
    }
}

impl LiveModel {
    pub fn lookup(&self, id: &dyn SmallIdentity) -> Option<Location> {
        let hash = super::records::key_hash(id);
        let mut encoded = super::key_codec::encode(id);
        self.0
            .read()
            .unwrap()
            .lookup_location(id, hash, &mut encoded)
    }
}

struct ResolvedRecord {
    address: Address,
    len: u64,
    checksum: u64,
    charge: u64,
    stream: usize,
}

impl Catalog {
    pub fn hash(&self, key: &dyn SmallIdentity) -> u64 {
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

    fn row(&self, handle: u32) -> Option<(u64, usize)> {
        let handle = handle.checked_sub(1)?;
        let (segment, base) = *self.chunks.get((handle >> 10) as usize)?;
        Some((segment, base + (handle & 1023) as usize))
    }

    pub fn metadata(&self, handle: u32) -> Option<(Address, super::records::Metadata)> {
        let (segment, row) = self.row(handle)?;
        let meta = self.segment(segment)?.records.metadata(row)?;
        Some(((segment, meta.offset), meta))
    }

    pub fn matches(&self, handle: u32, key: &mut Vec<u8>) -> bool {
        let Some((segment, row)) = self.row(handle) else {
            return false;
        };
        self.segment(segment)
            .is_some_and(|meta| meta.records.matches(row, key))
    }

    fn handle(&self, segment: u64, row: usize) -> u32 {
        (self.segment_chunks[&segment][row / 1024] << 10 | (row % 1024) as u32) + 1
    }

    pub fn find(&self, key: &dyn SmallIdentity, flags: u8) -> Option<usize> {
        let mut encoded = super::key_codec::encode(key);
        self.find_record(self.hash(key), &mut encoded, flags)
            .map(|(slot, _)| slot)
    }

    fn find_record(
        &self,
        hash: u64,
        encoded: &mut Vec<u8>,
        flags: u8,
    ) -> Option<(usize, ResolvedRecord)> {
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
                charge: record.charge,
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

    fn lookup_location(
        &self,
        id: &dyn SmallIdentity,
        hash: u64,
        encoded: &mut Vec<u8>,
    ) -> Option<Location> {
        if let Some((slot, record)) = self.find_record(hash, encoded, VISIBLE) {
            return self.resolve_location(slot, record);
        }
        self.retired
            .get(id)
            .filter(|entry| entry.live)
            .map(|entry| entry.location.clone())
    }

    pub fn location(&self, slot: usize) -> Option<Location> {
        let (address, record) = self.metadata(self.lookup.handle(slot))?;
        let resolved = ResolvedRecord {
            address,
            len: record.len,
            checksum: record.checksum,
            charge: record.charge,
            stream: self.segment(address.0)?.slot,
        };
        self.resolve_location(slot, resolved)
    }

    fn resolve_location(&self, slot: usize, record: ResolvedRecord) -> Option<Location> {
        let access = if self.compact_streams.contains(&record.stream)
            && self.lookup.flags(slot) & super::lookup::POLICY != 0
        {
            Some(self.compact_access(slot, record.address, record.charge))
        } else {
            self.policy
                .get(&record.address)
                .map(|entry| entry.access.clone())
                .or_else(|| self.access.get(&record.address).cloned())
        };
        Some(Location {
            file: self.files.get(&record.address.0)?.clone(),
            segment: record.address.0,
            offset: record.address.1,
            len: record.len,
            checksum: record.checksum,
            access,
        })
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
            Arc::make_mut(meta).records.shrink_to_fit();
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
        if let Some(chunks) = self.segment_chunks.remove(&segment) {
            self.free_chunks.extend(chunks);
        }
        self.trim_index();
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
                        let key = self.record(self.lookup.handle(slot)).unwrap().1.key;
                        let policy = self.policy.remove(&address).or_else(|| {
                            let access = location.access.as_ref()?;
                            let state = *access.small()?.state.lock().unwrap();
                            Some(PolicyEntry {
                                state,
                                access: access.clone(),
                            })
                        });
                        self.retired.insert(
                            key.clone(),
                            Retired {
                                slot: self.segment(address.0).unwrap().slot,
                                key,
                                location,
                                live: self.lookup.flags(slot) & VISIBLE != 0,
                                policy,
                            },
                        );
                        self.access.remove(&address);
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
        let result = self.model.apply(event);
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
                        checksum: 0,
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
                        checksum: 0,
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
