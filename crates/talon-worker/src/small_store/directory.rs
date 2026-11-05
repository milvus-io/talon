use super::catalog::{Catalog, LiveModel};
use super::lookup::{POLICY, REFERENCED, VISIBLE};
use super::Location;
use crate::block_key::{BlockIdentity, BlockKey};
use crate::eviction::{AccessHandle, PolicyEntry, PolicyState};

#[derive(Default)]
pub(crate) struct Directory {
    pub(super) catalog: LiveModel,
    slot: usize,
    staged: Option<super::catalog::Address>,
    clock: usize,
    #[cfg(test)]
    before_relocation_publish: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl Directory {
    pub(crate) fn enable_compact(&mut self) {
        self.catalog.0.write().unwrap().enable_compact(self.slot);
    }

    pub(crate) fn compact_totals(&self) -> (u64, usize) {
        self.catalog
            .0
            .read()
            .unwrap()
            .compact_totals
            .get(&self.slot)
            .copied()
            .unwrap_or_default()
    }

    pub(crate) fn compact_capacity(&self) -> usize {
        let catalog = self.catalog.0.read().unwrap();
        if catalog
            .compact_totals
            .get(&self.slot)
            .is_some_and(|total| total.1 != 0)
        {
            catalog.lookup.capacity()
        } else {
            0
        }
    }

    pub(crate) fn compact_admit(
        &mut self,
        id: &dyn BlockIdentity,
        bytes: u64,
        clock: bool,
    ) -> Option<AccessHandle> {
        let mut catalog = self.catalog.0.write().unwrap();
        let slot = self
            .pending(&catalog, id)
            .or_else(|| self.find(&catalog, id, VISIBLE))?;
        catalog.enable_compact(self.slot);
        Some(catalog.compact_admit(slot, bytes, clock))
    }

    pub(crate) fn compact_candidates(
        &mut self,
        bytes: &mut u64,
        excluded: &std::collections::HashSet<crate::eviction::CacheUnit>,
        selected: &mut std::collections::HashSet<crate::eviction::CacheUnit>,
        scans: &mut usize,
        limit: usize,
        second_pass: bool,
    ) -> Vec<crate::eviction::EvictionCandidate> {
        let mut catalog = self.catalog.0.write().unwrap();
        if catalog
            .compact_totals
            .get(&self.slot)
            .map_or(true, |total| total.1 == 0)
        {
            return Vec::new();
        }
        let mut out = Vec::new();
        let capacity = catalog.lookup.capacity();
        let mut walked = 0;
        while walked < capacity && *scans > 0 && out.len() < limit && *bytes > 0 {
            let slot = self.clock % capacity;
            self.clock = slot + 1;
            walked += 1;
            *scans -= 1;
            let Some((handle, flags)) = catalog.lookup.entry(slot) else {
                continue;
            };
            if flags & (POLICY | VISIBLE) != POLICY | VISIBLE {
                continue;
            }
            let (address, record) = catalog.record(handle).unwrap();
            if catalog.segment(address.0).unwrap().slot != self.slot {
                continue;
            }
            let unit = crate::eviction::CacheUnit::Whole(record.key.materialize());
            if excluded.contains(&unit) || selected.contains(&unit) {
                continue;
            }
            let entry = catalog.compact_entry(slot);
            if entry.pins != 0 {
                continue;
            }
            let ticket = entry.access.small().cloned().unwrap();
            let revision = catalog.next_revision();
            ticket.state.lock().unwrap().revision = revision;
            catalog.lookup.set_flags(slot, flags & !REFERENCED);
            if flags & REFERENCED != 0 && !second_pass {
                continue;
            }
            *bytes = bytes.saturating_sub(entry.bytes);
            selected.insert(unit.clone());
            out.push(crate::eviction::EvictionCandidate {
                unit,
                revision,
                small: Some(ticket),
            });
        }
        out
    }
    pub(super) fn restore(&mut self, key: BlockKey, location: Location) -> std::io::Result<()> {
        if self.catalog.0.read().unwrap().find(&key, VISIBLE).is_some() {
            return Err(super::format::invalid("duplicate recovered object"));
        }
        self.insert(key, location);
        Ok(())
    }

    pub(super) fn segment_entries(&self, segment: u64) -> Vec<(BlockKey, Location)> {
        let mut catalog = self.catalog.0.write().unwrap();
        let entries: Vec<_> = catalog
            .segment(segment)
            .map(|meta| {
                meta.records
                    .iter()
                    .map(|(offset, record)| (offset, BlockKey::new(&record.key)))
                    .collect()
            })
            .unwrap_or_default();
        entries
            .into_iter()
            .filter_map(|(offset, key)| {
                let slot = catalog.at((segment, offset))?;
                if catalog.lookup.flags(slot) & VISIBLE == 0 {
                    return None;
                }
                Some((key, catalog.location(slot)?))
            })
            .collect()
    }

    pub(super) fn new(catalog: LiveModel, slot: usize) -> Self {
        Self {
            catalog,
            slot,
            staged: None,
            clock: 0,
            #[cfg(test)]
            before_relocation_publish: None,
        }
    }
    pub fn reserve(&mut self, _: usize) {
        self.catalog
            .0
            .write()
            .unwrap()
            .activate()
            .expect("validated small catalog");
    }
    fn find(&self, catalog: &Catalog, id: &dyn BlockIdentity, flags: u8) -> Option<usize> {
        catalog.lookup.find(catalog.hash(id), |handle, state| {
            state & flags == flags
                && catalog.record(handle).is_some_and(|(address, record)| {
                    record.key == id.view()
                        && catalog
                            .segment(address.0)
                            .is_some_and(|segment| segment.slot == self.slot)
                })
        })
    }
    fn pending(&self, catalog: &Catalog, id: &dyn BlockIdentity) -> Option<usize> {
        let slot = catalog.at(self.staged?)?;
        (catalog.record(catalog.lookup.handle(slot))?.1.key == id.view()).then_some(slot)
    }
    pub fn len(&self) -> usize {
        self.catalog
            .0
            .read()
            .unwrap()
            .live
            .get(&self.slot)
            .copied()
            .unwrap_or(0)
    }

    pub fn identity(&self, id: &dyn BlockIdentity) -> Option<BlockKey> {
        let catalog = self.catalog.0.read().unwrap();
        if let Some(slot) = self
            .find(&catalog, id, VISIBLE)
            .or_else(|| self.find(&catalog, id, POLICY))
            .or_else(|| self.pending(&catalog, id))
        {
            return Some(BlockKey::new(
                &catalog.record(catalog.lookup.handle(slot))?.1.key,
            ));
        }
        catalog.retired.get(id).map(|entry| entry.key.clone())
    }
    pub fn get(&self, id: &dyn BlockIdentity) -> Option<Location> {
        let mut catalog = self.catalog.0.write().unwrap();
        if let Some(slot) = self.find(&catalog, id, VISIBLE) {
            return catalog.location(slot);
        }
        catalog
            .retired
            .get(id)
            .filter(|entry| entry.live && entry.slot == self.slot)
            .map(|entry| entry.location.clone())
    }
    pub fn get_key_value(&self, id: &dyn BlockIdentity) -> Option<(BlockKey, Location)> {
        self.get(id).map(|location| (BlockKey::new(id), location))
    }
    pub fn contains_key(&self, id: &dyn BlockIdentity) -> bool {
        self.get(id).is_some()
    }
    pub fn iter(&self) -> std::vec::IntoIter<(BlockKey, Location)> {
        let mut catalog = self.catalog.0.write().unwrap();
        let slots: Vec<_> = catalog
            .lookup
            .iter()
            .filter_map(|(slot, handle, flags)| {
                if flags & VISIBLE == 0 {
                    return None;
                }
                let (address, _) = catalog.record(handle)?;
                (catalog.segment(address.0)?.slot == self.slot).then_some(slot)
            })
            .collect();
        slots
            .into_iter()
            .filter_map(|slot| {
                let key = BlockKey::new(&catalog.record(catalog.lookup.handle(slot))?.1.key);
                Some((key, catalog.location(slot)?))
            })
            .collect::<Vec<_>>()
            .into_iter()
    }
    #[cfg(test)]
    pub fn values(&self) -> impl Iterator<Item = Location> + '_ {
        self.iter().map(|(_, location)| location)
    }
    pub fn insert(&mut self, key: BlockKey, location: Location) -> Option<Location> {
        let model = self.catalog.clone();
        let mut catalog = model.0.write().unwrap();
        let old = self.stage_in(&mut catalog, key.clone(), location);
        #[cfg(test)]
        if let Some(pause) = self.before_relocation_publish.take() {
            pause();
        }
        self.publish_in(&mut catalog, &key);
        old
    }
    pub fn stage(&mut self, key: BlockKey, location: Location) -> Option<Location> {
        let model = self.catalog.clone();
        let mut catalog = model.0.write().unwrap();
        self.stage_in(&mut catalog, key, location)
    }
    fn stage_in(
        &mut self,
        catalog: &mut Catalog,
        key: BlockKey,
        location: Location,
    ) -> Option<Location> {
        catalog.activate().expect("validated small catalog");
        catalog
            .files
            .insert(location.segment, location.file.clone());
        let address = (location.segment, location.offset);
        self.staged = Some(address);
        #[cfg(test)]
        if catalog.at(address).is_none() {
            catalog
                .apply(super::journal::Event::Insert {
                    slot: self.slot,
                    segment: address.0,
                    offset: address.1,
                    record: super::journal::Record {
                        key: key.clone(),
                        len: location.len,
                        checksum: location.checksum,
                    },
                })
                .expect("test locator metadata");
        }
        let slot = catalog.at(address).expect("journaled small object");
        let old_slot = self.find(catalog, &key, VISIBLE);
        let old = old_slot.and_then(|slot| catalog.location(slot));
        if let Some(old_slot) = old_slot.filter(|old| *old != slot) {
            let old_address = catalog.record(catalog.lookup.handle(old_slot)).unwrap().0;
            let flags = catalog.lookup.flags(old_slot);
            catalog
                .lookup
                .set_flags(old_slot, flags & !(VISIBLE | POLICY | REFERENCED));
            *catalog.live.entry(self.slot).or_default() -= 1;
            catalog.relocate_policy(old_address, address, slot);
            if let Some(policy) = catalog.policy.remove(&old_address) {
                catalog.policy.insert(address, policy);
            }
            if let Some(access) = catalog.access.remove(&old_address) {
                catalog.access.insert(address, access);
            }
            let flags = catalog.lookup.flags(slot) | flags & (POLICY | REFERENCED);
            catalog.lookup.set_flags(slot, flags);
        }
        if let Some(access) = location
            .access
            .filter(|access| access.small().is_none() || access.tracks_clock())
        {
            if let Some(policy) = catalog.policy.get_mut(&address) {
                policy.access = access;
            } else {
                catalog.access.insert(address, access);
            }
        }
        old
    }
    pub fn publish(&mut self, id: &dyn BlockIdentity) {
        let model = self.catalog.clone();
        let mut catalog = model.0.write().unwrap();
        self.publish_in(&mut catalog, id);
    }
    fn publish_in(&mut self, catalog: &mut Catalog, id: &dyn BlockIdentity) {
        let slot = self.pending(catalog, id).expect("staged small object");
        let flags = catalog.lookup.flags(slot);
        if flags & VISIBLE == 0 {
            *catalog.live.entry(self.slot).or_default() += 1;
        }
        catalog.lookup.set_flags(slot, flags | VISIBLE);
        self.staged = None;
    }
    pub fn attach_access(&mut self, id: &dyn BlockIdentity, access: AccessHandle) {
        if access.small().is_some() && !access.tracks_clock() {
            return;
        }
        let mut catalog = self.catalog.0.write().unwrap();
        let slot = self
            .pending(&catalog, id)
            .or_else(|| self.find(&catalog, id, VISIBLE))
            .expect("staged small object");
        let address = catalog.record(catalog.lookup.handle(slot)).unwrap().0;
        if let Some(policy) = catalog.policy.get_mut(&address) {
            policy.access = access;
        } else {
            catalog.access.insert(address, access);
        }
    }
    pub fn remove(&mut self, id: &dyn BlockIdentity) -> Option<Location> {
        let mut catalog = self.catalog.0.write().unwrap();
        if let Some(slot) = self.find(&catalog, id, VISIBLE) {
            let location = catalog.location(slot);
            let flags = catalog.lookup.flags(slot) & !VISIBLE;
            catalog.lookup.set_flags(slot, flags);
            *catalog.live.entry(self.slot).or_default() -= 1;
            return location;
        }
        let retired = catalog.retired.get_mut(id)?;
        if !retired.live {
            return None;
        }
        retired.live = false;
        let location = retired.location.clone();
        if retired.policy.is_none() {
            catalog.retired.remove(id);
        }
        *catalog.live.entry(self.slot).or_default() -= 1;
        Some(location)
    }
    pub fn policy_get(&self, id: &dyn BlockIdentity) -> Option<PolicyEntry> {
        let mut catalog = self.catalog.0.write().unwrap();
        if let Some(slot) = self.find(&catalog, id, POLICY) {
            if catalog.compact_streams.contains(&self.slot) {
                return Some(catalog.compact_entry(slot));
            }
            let address = catalog.record(catalog.lookup.handle(slot))?.0;
            return catalog.policy.get(&address).cloned();
        }
        let mut entry = catalog.retired.get(id)?.policy.clone()?;
        if let Some(ticket) = entry.access.small() {
            entry.state = *ticket.state.lock().unwrap();
        }
        Some(entry)
    }
    pub fn policy_insert(
        &mut self,
        id: &dyn BlockIdentity,
        policy: PolicyEntry,
    ) -> Option<AccessHandle> {
        let mut catalog = self.catalog.0.write().unwrap();
        let slot = self
            .pending(&catalog, id)
            .or_else(|| self.find(&catalog, id, VISIBLE))?;
        let address = catalog.record(catalog.lookup.handle(slot))?.0;
        let flags = catalog.lookup.flags(slot) | POLICY;
        catalog.lookup.set_flags(slot, flags);
        catalog.access.remove(&address);
        let access = policy.access.clone();
        catalog.policy.insert(address, policy);
        Some(access)
    }
    pub fn policy_update<T>(
        &mut self,
        id: &dyn BlockIdentity,
        update: impl FnOnce(&mut PolicyState) -> T,
    ) -> Option<T> {
        let mut catalog = self.catalog.0.write().unwrap();
        if let Some(slot) = self.find(&catalog, id, POLICY) {
            if catalog.compact_streams.contains(&self.slot) {
                return Some(catalog.compact_update(slot, update));
            }
            let address = catalog.record(catalog.lookup.handle(slot))?.0;
            return Some(update(&mut catalog.policy.get_mut(&address)?.state));
        }
        if let Some(ticket) = catalog
            .retired
            .get(id)?
            .policy
            .as_ref()?
            .access
            .small()
            .cloned()
        {
            let address = *ticket.address.lock().unwrap();
            let mut state = ticket.state.lock().unwrap();
            let result = update(&mut state);
            if state.pins == 0 {
                catalog.pins.remove(&address);
            } else {
                catalog.pins.insert(address, ticket.clone());
            }
            Some(result)
        } else {
            Some(update(
                &mut catalog.retired.get_mut(id)?.policy.as_mut()?.state,
            ))
        }
    }
    pub fn policy_remove(&mut self, id: &dyn BlockIdentity) -> Option<PolicyEntry> {
        let mut catalog = self.catalog.0.write().unwrap();
        if let Some(slot) = self.find(&catalog, id, POLICY) {
            if catalog.compact_streams.contains(&self.slot) {
                return Some(catalog.compact_remove(slot));
            }
            let address = catalog.record(catalog.lookup.handle(slot))?.0;
            let flags = catalog.lookup.flags(slot) & !POLICY;
            catalog.lookup.set_flags(slot, flags);
            let policy = catalog.policy.remove(&address)?;
            policy.access.retire();
            return Some(policy);
        }
        let retired = catalog.retired.get_mut(id)?;
        let mut policy = retired.policy.take()?;
        let stream = retired.slot;
        let address = (retired.location.segment, retired.location.offset);
        let live = retired.live;
        if let Some(ticket) = policy.access.small() {
            policy.state = *ticket.state.lock().unwrap();
            catalog.compact_forget(address, stream, &policy);
        } else {
            policy.access.retire();
        }
        if !live {
            catalog.retired.remove(id);
        }
        Some(policy)
    }
    pub fn retire_segment(&mut self, segment: u64) {
        self.catalog.0.write().unwrap().retire_segment(segment);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eviction::{CacheUnit, SharedUnit};
    use crate::{BlockIndex, Lru};
    use std::sync::{mpsc, Arc, RwLock};
    use std::time::Duration;
    use talon_core::{Backend, BlockId, ObjectId, Version};

    fn fixture(count: u64) -> (Directory, impl Fn(u64) -> BlockId) {
        let mut directory = Directory::default();
        let key = |n: u64| {
            BlockId::new(
                ObjectId::new(Backend::S3, "b", n.to_string()),
                0,
                1024,
                Version::new("v"),
            )
        };
        let file = Arc::new(super::super::SegmentFile {
            path: "unused".into(),
            fd: tokio::sync::Mutex::default(),
        });
        for n in 0..count {
            directory.insert(
                BlockKey::new(&key(n)),
                Location {
                    file: file.clone(),
                    segment: 1,
                    offset: n,
                    len: 1,
                    checksum: 0,
                    access: None,
                },
            );
        }
        directory.enable_compact();
        (directory, key)
    }

    #[test]
    fn compact_tickets_release_after_bulk_reads_and_repeated_hits() {
        let (directory, key) = fixture(1025);
        let tokens: Vec<_> = (0..1025).map(|n| directory.get(&key(n)).unwrap()).collect();
        drop(tokens);
        for _ in 0..512 {
            directory.get(&key(0)).unwrap().access.unwrap().touch();
        }
        let catalog = directory.catalog.0.read().unwrap();
        assert!(catalog.tickets.len() <= 256);
        assert!(catalog.tickets.capacity() < 2048);
        assert!(catalog.policy.is_empty());
        assert!(catalog.access.is_empty());
        assert_eq!(std::mem::size_of::<AccessHandle>(), 32);
    }

    #[test]
    fn compact_reference_pin_revision_and_relocation_survive_ticket_lifetimes() {
        let (mut directory, key) = fixture(1);
        let id = key(0);
        directory.get(&id).unwrap().access.unwrap().touch();
        let access = directory.get(&id).unwrap().access.unwrap();
        assert!(access.small().unwrap().referenced());
        let first = access.small().unwrap().snapshot();
        assert!(access.small().unwrap().is_current(first));
        access.touch();
        assert!(!access.small().unwrap().is_current(first));
        let second = access.small().unwrap().snapshot();
        assert!(!access.small().unwrap().is_current(first));
        assert!(access.small().unwrap().is_current(second));
        directory
            .policy_update(&id, |state| state.pins += 2)
            .unwrap();
        drop(access);
        assert_eq!(directory.policy_get(&id).unwrap().pins, 2);
        let old = directory.get(&id).unwrap();
        let access = old.access.clone().unwrap();
        access.touch();
        directory.insert(
            BlockKey::new(&id),
            Location {
                segment: 2,
                offset: 0,
                ..old
            },
        );
        assert!(access.small().unwrap().referenced());
        assert_eq!(directory.policy_get(&id).unwrap().pins, 2);
        directory
            .policy_update(&id, |state| state.pins -= 1)
            .unwrap();
        assert!(!access.small().unwrap().is_current(second));
        directory
            .policy_update(&id, |state| state.pins -= 1)
            .unwrap();
        assert!(directory.catalog.0.read().unwrap().pins.is_empty());
        directory.policy_remove(&id).unwrap();
        let new = directory.compact_admit(&id, 1, false).unwrap();
        access.touch();
        assert!(!new.small().unwrap().referenced());
        assert!(!access.small().unwrap().is_current(second));
    }

    #[test]
    fn compact_token_refreshes_after_rehash_and_record_compaction() {
        let (mut directory, key) = fixture(128);
        let token = directory.get(&key(127)).unwrap().access.unwrap();
        let file = directory.get(&key(0)).unwrap().file;
        for n in 128..512 {
            directory.insert(
                BlockKey::new(&key(n)),
                Location {
                    file: file.clone(),
                    segment: 1,
                    offset: n,
                    len: 1,
                    checksum: 0,
                    access: None,
                },
            );
        }
        token.touch();
        assert!(directory
            .get(&key(127))
            .unwrap()
            .access
            .unwrap()
            .small()
            .unwrap()
            .referenced());
        for n in 0..6 {
            directory
                .catalog
                .0
                .write()
                .unwrap()
                .apply(super::super::journal::Event::Delete {
                    segment: 1,
                    offset: n,
                })
                .unwrap();
        }
        let candidate = token.small().unwrap().snapshot();
        assert!(token.small().unwrap().is_current(candidate));
        token.touch();
        assert!(!token.small().unwrap().is_current(candidate));
        assert!(directory
            .get(&key(127))
            .unwrap()
            .access
            .unwrap()
            .small()
            .unwrap()
            .referenced());
        assert!(!directory
            .get(&key(126))
            .unwrap()
            .access
            .unwrap()
            .small()
            .unwrap()
            .referenced());
    }

    #[test]
    fn compact_relocation_updates_a_token_at_the_current_epoch() {
        let (mut directory, key) = fixture(1);
        let id = key(0);
        let old = directory.get(&id).unwrap();
        let token = old.access.clone().unwrap();
        directory
            .catalog
            .0
            .write()
            .unwrap()
            .apply(super::super::journal::Event::Insert {
                slot: 0,
                segment: 2,
                offset: 0,
                record: super::super::journal::Record {
                    key: BlockKey::new(&id),
                    len: 1,
                    checksum: 0,
                },
            })
            .unwrap();
        token.touch();
        directory.insert(
            BlockKey::new(&id),
            Location {
                segment: 2,
                offset: 0,
                ..old
            },
        );
        let candidate = token.small().unwrap().snapshot();
        assert!(token.small().unwrap().is_current(candidate));
        token.touch();
        assert!(!token.small().unwrap().is_current(candidate));
    }

    #[test]
    fn shared_lookup_cannot_observe_partial_relocation() {
        let (mut directory, key) = fixture(1);
        let id = key(0);
        let old = directory.get(&id).unwrap();
        let model = directory.catalog.clone();
        let (entered, paused) = mpsc::channel();
        let (resume, resumed) = mpsc::channel();
        let resumed = std::sync::Mutex::new(resumed);
        directory.before_relocation_publish = Some(Box::new(move || {
            entered.send(()).unwrap();
            resumed.lock().unwrap().recv().unwrap();
        }));
        let publish_id = id.clone();
        let writer = std::thread::spawn(move || {
            directory.insert(
                BlockKey::new(&publish_id),
                Location {
                    segment: 2,
                    offset: 0,
                    ..old
                },
            )
        });
        paused.recv_timeout(Duration::from_secs(5)).unwrap();
        let (attempt, attempted) = mpsc::channel();
        let (found, result) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            attempt.send(()).unwrap();
            found
                .send(model.lookup(&id).map(|location| location.segment))
                .unwrap();
        });
        attempted.recv_timeout(Duration::from_secs(5)).unwrap();
        let blocked = result.recv_timeout(Duration::from_millis(20)).is_err();
        resume.send(()).unwrap();
        writer.join().unwrap();
        reader.join().unwrap();
        assert!(blocked, "a lookup must not observe stage without publish");
        assert_eq!(result.recv().unwrap(), Some(2));
    }

    #[test]
    fn compact_scans_count_empty_and_pinned_slots() {
        let (mut directory, key) = fixture(1);
        directory
            .policy_update(&key(0), |state| state.pins = 1)
            .unwrap();
        let mut scans = 7;
        let mut bytes = u64::MAX;
        let candidates = directory.compact_candidates(
            &mut bytes,
            &Default::default(),
            &mut Default::default(),
            &mut scans,
            10,
            false,
        );
        assert!(candidates.is_empty());
        assert_eq!(scans, 0);
        assert_eq!(directory.clock, 7);
    }

    #[test]
    fn traversal_is_a_stable_cut_across_concurrent_rehash() {
        let mut directory = Directory::default();
        let file = Arc::new(super::super::SegmentFile {
            path: "unused".into(),
            fd: tokio::sync::Mutex::default(),
        });
        let key = |n: u64| {
            BlockKey::new(&BlockId::new(
                ObjectId::new(Backend::S3, "b", n.to_string()),
                0,
                1024,
                Version::new("v"),
            ))
        };
        let insert = |directory: &mut Directory, n| {
            directory.insert(
                key(n),
                Location {
                    file: file.clone(),
                    segment: 1,
                    offset: n,
                    len: 1,
                    checksum: 0,
                    access: None,
                },
            );
        };
        for n in 0..8 {
            insert(&mut directory, n);
        }
        let capacity = directory.catalog.0.read().unwrap().lookup.capacity();
        let mut snapshot = directory.iter();
        let mut old = vec![snapshot.next().unwrap().0];
        for n in 8..200 {
            insert(&mut directory, n);
        }
        assert!(directory.catalog.0.read().unwrap().lookup.capacity() > capacity);
        old.extend(snapshot.map(|(key, _)| key));
        assert_eq!(old.len(), 8);
        for n in 0..8 {
            assert!(old.contains(&key(n)));
        }
    }

    #[test]
    fn late_policy_removal_cannot_release_an_unpublished_locator() {
        let id = BlockId::new(
            ObjectId::new(Backend::S3, "b", "o"),
            0,
            256,
            Version::new("v"),
        );
        let unit = SharedUnit {
            block: BlockKey::new(&id),
            page: None,
        };
        let directory = Arc::new(RwLock::new(Directory::default()));
        let index = Arc::new(BlockIndex::new());
        index.register_small_directory(&directory);
        let lru = Arc::new(Lru::from_recovered(
            index.clone(),
            Vec::new(),
            0,
            false,
            false,
        ));
        let (entered, paused) = mpsc::channel();
        let (resume, resumed) = mpsc::channel();
        *lru.before_small_publish.lock().unwrap() = Some(Box::new(move || {
            entered.send(()).unwrap();
            resumed.recv().unwrap();
        }));
        let publisher = {
            let lru = lru.clone();
            let directory = directory.clone();
            std::thread::spawn(move || {
                lru.publish_small(
                    unit,
                    10,
                    &directory,
                    Location {
                        file: Arc::new(super::super::SegmentFile {
                            path: "unused".into(),
                            fd: tokio::sync::Mutex::default(),
                        }),
                        segment: 1,
                        offset: 0,
                        len: 1,
                        checksum: 0,
                        access: None,
                    },
                )
            })
        };
        paused.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(index.policy_get(&CacheUnit::Whole(id.clone())).is_some());
        assert!(directory.read().unwrap().get(&id).is_none());
        let (attempt, attempted) = mpsc::channel();
        let (removed, removal) = mpsc::channel();
        let remover = {
            let lru = lru.clone();
            let id = id.clone();
            std::thread::spawn(move || {
                attempt.send(()).unwrap();
                lru.remove(&CacheUnit::Whole(id));
                removed.send(()).unwrap();
            })
        };
        attempted.recv_timeout(Duration::from_secs(5)).unwrap();
        let blocked = removal.recv_timeout(Duration::from_millis(20)).is_err();
        resume.send(()).unwrap();
        publisher.join().unwrap();
        remover.join().unwrap();
        assert!(blocked, "removal must wait for locator publication");
        let mut directory = directory.write().unwrap();
        assert!(directory.get(&id).is_some());
        assert!(directory.policy_get(&id).is_none());
        directory.remove(&id);
        assert!(directory.identity(&id).is_none());
    }
}
