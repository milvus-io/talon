use super::catalog::{Address, Catalog, LiveModel};
use super::lookup::{POLICY, REFERENCED, VISIBLE};
use super::Location;
use crate::eviction::{AccessHandle, PolicyEntry, PolicyState};
use crate::small_store::key::{SmallIdentity, SmallKey};

#[derive(Default)]
pub(crate) struct Directory {
    pub(super) catalog: LiveModel,
    slot: usize,
    staged: Option<(Address, u64)>,
    clock: usize,
    #[cfg(test)]
    before_relocation_publish: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl Directory {
    #[cfg(test)]
    pub(crate) fn enable_compact(&mut self) {
        self.catalog.0.write().unwrap().enable_compact([self.slot]);
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
        id: &dyn SmallIdentity,
        bytes: u64,
        clock: bool,
    ) -> Option<AccessHandle> {
        let mut catalog = self.catalog.0.write().unwrap();
        let slot = self
            .pending(&catalog, id)
            .or_else(|| self.find(&catalog, id, VISIBLE))?;
        Some(catalog.compact_admit(slot, bytes, clock))
    }

    pub(crate) fn has_recency(&self) -> bool {
        self.catalog.has_recency()
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
            let unit = record.key.view().cache_unit();
            if excluded.contains(&unit) || selected.contains(&unit) {
                continue;
            }
            let entry = catalog.compact_entry(slot);
            if entry.pins != 0 {
                continue;
            }
            let ticket = entry.access.small().cloned().unwrap();
            let revision = catalog.observe_policy(&ticket, Some(slot));
            if flags & REFERENCED != 0 && !second_pass {
                continue;
            }
            *bytes = bytes.saturating_sub(entry.bytes);
            selected.insert(unit.clone());
            out.push(crate::eviction::EvictionCandidate {
                unit,
                revision,
                small: Some(ticket),
                compact: None,
            });
        }
        out
    }
    pub(super) fn restore(
        &mut self,
        key: &dyn SmallIdentity,
        location: Location,
    ) -> std::io::Result<()> {
        self.catalog
            .0
            .write()
            .unwrap()
            .restore_row(self.slot, key, location)
    }

    pub(super) fn segment_entries(&self, segment: u64) -> Vec<(SmallKey, Location)> {
        let catalog = self.catalog.0.read().unwrap();
        let Some(meta) = catalog.segment(segment) else {
            return Vec::new();
        };
        meta.records
            .iter()
            .filter_map(|(offset, key)| {
                let slot = catalog.at((segment, offset))?;
                if catalog.lookup.flags(slot) & VISIBLE == 0 {
                    return None;
                }
                Some((key.key, catalog.location_for_move(slot)?))
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
    fn find(&self, catalog: &Catalog, id: &dyn SmallIdentity, flags: u8) -> Option<usize> {
        catalog.find_in_stream(self.slot, id, flags)
    }

    fn pending(&self, catalog: &Catalog, id: &dyn SmallIdentity) -> Option<usize> {
        let slot = catalog.at(self.staged?.0)?;
        catalog
            .matches(catalog.lookup.handle(slot), &super::key_codec::encode(id))
            .then_some(slot)
    }
    pub fn len(&self) -> usize {
        self.catalog.0.read().unwrap().stream_len(self.slot)
    }

    pub fn identity(&self, id: &dyn SmallIdentity) -> Option<SmallKey> {
        let catalog = self.catalog.0.read().unwrap();
        if self
            .find(&catalog, id, VISIBLE)
            .or_else(|| self.find(&catalog, id, POLICY))
            .or_else(|| self.pending(&catalog, id))
            .is_some()
        {
            return Some(SmallKey::new(id));
        }
        catalog.retired_identity(id)
    }
    pub fn get(&self, id: &dyn SmallIdentity) -> Option<Location> {
        self.get_in(&self.catalog.0.read().unwrap(), id)
    }
    pub(super) fn get_for_unlink(
        &self,
        id: &dyn SmallIdentity,
        nonblocking: bool,
    ) -> Option<Option<Location>> {
        let catalog = super::read_for_unlink(&self.catalog.0, nonblocking)?;
        Some(self.get_in(&catalog, id))
    }
    fn get_in(&self, catalog: &Catalog, id: &dyn SmallIdentity) -> Option<Location> {
        if let Some(slot) = self.find(catalog, id, VISIBLE) {
            return catalog.location(slot);
        }
        catalog.retired_location_in(id, Some(self.slot))
    }

    pub fn get_key_value(&self, id: &dyn SmallIdentity) -> Option<(SmallKey, Location)> {
        self.get(id).map(|location| (SmallKey::new(id), location))
    }
    pub fn contains_key(&self, id: &dyn SmallIdentity) -> bool {
        self.get(id).is_some()
    }
    pub fn iter(&self) -> std::vec::IntoIter<(SmallKey, Location)> {
        let catalog = self.catalog.0.read().unwrap();
        let entries: Vec<_> = catalog
            .files
            .keys()
            .filter_map(|&segment| {
                let meta = catalog.segment(segment)?;
                (meta.slot == self.slot).then_some((segment, meta))
            })
            .flat_map(|(segment, meta)| {
                meta.records
                    .iter()
                    .map(move |(offset, record)| ((segment, offset), record.key))
            })
            .collect();
        entries
            .into_iter()
            .filter_map(|(address, key)| {
                let slot = catalog.at(address)?;
                (catalog.lookup.flags(slot) & VISIBLE != 0).then_some(())?;
                Some((key, catalog.location(slot)?))
            })
            .collect::<Vec<_>>()
            .into_iter()
    }
    #[cfg(test)]
    pub fn values(&self) -> impl Iterator<Item = Location> + '_ {
        self.iter().map(|(_, location)| location)
    }
    pub fn insert(&mut self, key: SmallKey, location: Location) -> Option<Location> {
        #[cfg(test)]
        let pause = self.before_relocation_publish.take();
        self.catalog
            .0
            .write()
            .unwrap()
            .relocate_row(self.slot, &key, location, || {
                #[cfg(test)]
                if let Some(pause) = pause {
                    pause();
                }
            })
    }

    pub(super) fn replace_segment(&mut self, old: u64, replacement: Option<&super::StagedSegment>) {
        self.catalog
            .0
            .write()
            .unwrap()
            .replace_segment(old, replacement);
    }

    pub fn stage(&mut self, key: SmallKey, location: Location) -> Option<Location> {
        self.staged = Some(((location.segment, location.offset), location.stored_len()));
        self.catalog
            .0
            .write()
            .unwrap()
            .stage_row(self.slot, &key, location)
    }

    pub fn publish(&mut self, id: &dyn SmallIdentity) {
        let mut catalog = self.catalog.0.write().unwrap();
        let slot = self.pending(&catalog, id).expect("staged small object");
        let (address, stored_len) = self.staged.take().unwrap();
        catalog.publish_row(self.slot, slot, address, stored_len);
    }

    pub fn attach_access(&mut self, id: &dyn SmallIdentity, access: AccessHandle) {
        if !Catalog::stores_access(&access) {
            return;
        }
        let mut catalog = self.catalog.0.write().unwrap();
        let slot = self
            .pending(&catalog, id)
            .or_else(|| self.find(&catalog, id, VISIBLE))
            .expect("staged small object");
        catalog.attach_access(slot, access);
    }

    pub fn remove(&mut self, id: &dyn SmallIdentity) -> Option<Location> {
        self.catalog.0.write().unwrap().remove_row(self.slot, id)
    }

    pub fn policy_get(&self, id: &dyn SmallIdentity) -> Option<PolicyEntry> {
        self.catalog.0.read().unwrap().policy_get(self.slot, id)
    }

    pub fn policy_insert(
        &mut self,
        id: &dyn SmallIdentity,
        policy: PolicyEntry,
    ) -> Option<AccessHandle> {
        let mut catalog = self.catalog.0.write().unwrap();
        let slot = self
            .pending(&catalog, id)
            .or_else(|| self.find(&catalog, id, VISIBLE))?;
        catalog.policy_insert(slot, policy)
    }

    pub fn policy_update<T>(
        &mut self,
        id: &dyn SmallIdentity,
        update: impl FnOnce(&mut PolicyState) -> T,
    ) -> Option<T> {
        self.catalog
            .0
            .write()
            .unwrap()
            .policy_update(self.slot, id, update)
    }

    pub fn policy_remove(&mut self, id: &dyn SmallIdentity) -> Option<PolicyEntry> {
        self.catalog.0.write().unwrap().policy_remove(self.slot, id)
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
                SmallKey::new(&key(n)),
                Location {
                    file: file.clone(),
                    segment: 1,
                    offset: n,
                    len: 1,
                    checksum: Some(0),
                    access: None,
                },
            );
        }
        directory.enable_compact();
        (directory, key)
    }

    #[test]
    fn lookups_touches_and_token_reclamation_share_the_catalog_read_lock() {
        use std::sync::mpsc;
        use std::time::Duration;

        let (directory, key) = fixture(2);
        let model = directory.catalog.clone();
        let held = model.0.read().unwrap();
        let (done, completed) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let location = directory.catalog.lookup(&key(0)).unwrap();
            location.touch();
            assert!(location
                .access
                .as_ref()
                .unwrap()
                .small()
                .unwrap()
                .referenced());
            assert!(directory
                .policy_get(&key(0))
                .unwrap()
                .access
                .small()
                .is_some());
            let other = directory.get(&key(1)).unwrap();
            other.touch();
            assert!(directory.catalog.lookup(&key(2)).is_none());
            drop(location);
            drop(other);
            assert_eq!(directory.catalog.0.read().unwrap().tickets.len(), 0);
            done.send(()).unwrap();
        });
        let result = completed.recv_timeout(Duration::from_secs(5));
        drop(held);
        worker.join().unwrap();
        result.expect("reads and access updates must not require the catalog write lock");
    }

    #[test]
    fn concurrent_same_key_readers_share_one_token_and_preserve_references() {
        use std::sync::{mpsc, Barrier};
        use std::time::Duration;

        let (directory, key) = fixture(1);
        let model = directory.catalog.clone();
        let held = model.0.read().unwrap();
        let start = Arc::new(Barrier::new(17));
        let (send, receive) = mpsc::channel();
        let workers: Vec<_> = (0..16)
            .map(|_| {
                let model = model.clone();
                let id = key(0);
                let start = start.clone();
                let send = send.clone();
                std::thread::spawn(move || {
                    start.wait();
                    let access = model.lookup(&id).unwrap().access.unwrap();
                    for _ in 0..128 {
                        access.touch();
                    }
                    send.send(access).unwrap();
                })
            })
            .collect();
        start.wait();
        let mut tokens = Vec::new();
        for _ in 0..16 {
            match receive.recv_timeout(Duration::from_secs(5)) {
                Ok(token) => tokens.push(token),
                Err(_) => break,
            }
        }
        drop(held);
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(tokens.len(), 16);
        let first = tokens[0].small().unwrap();
        for token in &tokens {
            assert!(Arc::ptr_eq(first, token.small().unwrap()));
        }
        assert!(first.referenced());
        let revision = first.snapshot();
        assert!(first.is_current(revision));
        tokens[15].touch();
        assert!(!first.is_current(revision));
        drop(tokens);
        assert_eq!(model.0.read().unwrap().tickets.len(), 0);
    }

    #[test]
    fn concurrent_token_churn_preserves_other_live_tokens() {
        let (directory, key) = fixture(1025);
        let held = directory.get(&key(0)).unwrap().access.unwrap();
        let model = directory.catalog.clone();
        let keys: Vec<_> = (1..1025).map(key).collect();
        std::thread::scope(|scope| {
            for batch in keys.chunks(64) {
                let model = &model;
                let held = &held;
                scope.spawn(move || {
                    for _ in 0..16 {
                        let tokens: Vec<_> = batch
                            .iter()
                            .map(|id| model.lookup(id).unwrap().access.unwrap())
                            .collect();
                        for (id, token) in batch.iter().zip(&tokens) {
                            let again = model.lookup(id).unwrap().access.unwrap();
                            assert!(Arc::ptr_eq(token.small().unwrap(), again.small().unwrap()));
                            token.touch();
                        }
                        held.touch();
                    }
                });
            }
        });
        assert!(held.small().unwrap().referenced());
        assert_eq!(model.0.read().unwrap().tickets.len(), 1);
        drop(held);
        assert_eq!(model.0.read().unwrap().tickets.len(), 0);
    }

    #[test]
    fn last_reader_releases_its_token_while_the_catalog_is_write_locked() {
        let (directory, key) = fixture(1);
        let access = directory.get(&key(0)).unwrap().access.unwrap();
        let catalog = directory.catalog.0.write().unwrap();
        assert_eq!(catalog.tickets.len(), 1);
        drop(access);
        assert_eq!(catalog.tickets.len(), 0);
        assert_eq!(catalog.tickets.capacity(), 0);
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
        assert_eq!(catalog.tickets.len(), 0);
        assert_eq!(catalog.tickets.capacity(), 0);
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
            SmallKey::new(&id),
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
                SmallKey::new(&key(n)),
                Location {
                    file: file.clone(),
                    segment: 1,
                    offset: n,
                    len: 1,
                    checksum: Some(0),
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
                    key: SmallKey::new(&id),
                    len: 1,
                    checksum: Some(0),
                },
            })
            .unwrap();
        token.touch();
        directory.insert(
            SmallKey::new(&id),
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
    fn journal_delete_releases_retired_visibility_and_policy_in_either_order() {
        let (mut directory, key) = fixture(2);
        let model = directory.catalog.clone();
        model.0.write().unwrap().ownership = Some(Default::default());

        for (offset, visibility_first) in [(0, true), (1, false)] {
            let id = key(offset);
            let address = (1, offset);
            directory
                .policy_update(&id, |state| state.pins += 1)
                .unwrap();
            {
                let mut catalog = model.0.write().unwrap();
                catalog
                    .apply(super::super::journal::Event::Delete { segment: 1, offset })
                    .unwrap();
                assert!(catalog.retired_at(address).is_some());
            }
            assert_eq!(directory.len(), 2 - offset as usize);
            assert_eq!(model.len(), directory.len());
            assert_eq!(directory.policy_get(&id).unwrap().pins, 1);
            assert!(model.lookup(&id).is_some());

            if visibility_first {
                directory.remove(&id).unwrap();
                assert!(model.0.read().unwrap().retired_at(address).is_none());
                assert!(directory.identity(&id).is_some());
                assert!(model.lookup(&id).is_none());
                directory
                    .policy_update(&id, |state| state.pins -= 1)
                    .unwrap();
                assert_eq!(directory.policy_remove(&id).unwrap().pins, 0);
            } else {
                assert_eq!(directory.policy_remove(&id).unwrap().pins, 1);
                assert!(model.0.read().unwrap().retired_at(address).is_some());
                assert!(model.lookup(&id).is_some());
                directory.remove(&id).unwrap();
            }
            let catalog = model.0.read().unwrap();
            assert!(catalog.retired_identity(&id).is_none());
            assert!(catalog.retired_at(address).is_none());
            assert!(catalog.pins.is_empty());
            drop(catalog);
            assert_eq!(directory.len(), 1 - offset as usize);
            assert_eq!(model.len(), directory.len());
        }
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
                SmallKey::new(&publish_id),
                Location {
                    segment: 2,
                    offset: 0,
                    ..old
                },
            )
        });
        paused.recv_timeout(Duration::from_secs(5)).unwrap();
        let live_while_moving = model.len();
        let reader_model = model.clone();
        let (attempt, attempted) = mpsc::channel();
        let (found, result) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            attempt.send(()).unwrap();
            found
                .send(reader_model.lookup(&id).map(|location| location.segment))
                .unwrap();
        });
        attempted.recv_timeout(Duration::from_secs(5)).unwrap();
        let blocked = result.recv_timeout(Duration::from_millis(20)).is_err();
        resume.send(()).unwrap();
        writer.join().unwrap();
        reader.join().unwrap();
        assert!(blocked, "a lookup must not observe stage without publish");
        assert_eq!(result.recv().unwrap(), Some(2));
        assert_eq!(live_while_moving, 1);
        assert_eq!(model.len(), 1);
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
            SmallKey::new(&BlockId::new(
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
                    checksum: Some(0),
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
            block: crate::block_key::BlockKey::new(&id),
            kind: crate::eviction::UnitKind::Whole,
        };
        let directory = Arc::new(RwLock::new(Directory::default()));
        let index = Arc::new(BlockIndex::new());
        index.register_small_directory(&directory);
        let lru = Arc::new(Lru::from_recovered(index.clone(), Vec::new(), 0, false));
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
                        checksum: Some(0),
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
