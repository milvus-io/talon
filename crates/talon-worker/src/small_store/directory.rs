use super::catalog::{Catalog, LiveModel};
use super::lookup::{POLICY, VISIBLE};
use super::Location;
use crate::block_key::{BlockIdentity, BlockKey};
use crate::eviction::{AccessHandle, PolicyEntry, PolicyState};

#[derive(Default)]
pub(crate) struct Directory {
    pub(super) catalog: LiveModel,
    slot: usize,
    staged: Option<super::catalog::Address>,
}

impl Directory {
    pub(super) fn restore(&mut self, key: BlockKey, location: Location) -> std::io::Result<()> {
        if self.catalog.0.read().unwrap().find(&key, VISIBLE).is_some() {
            return Err(super::format::invalid("duplicate recovered object"));
        }
        self.insert(key, location);
        Ok(())
    }

    pub(super) fn segment_entries(&self, segment: u64) -> Vec<(BlockKey, Location)> {
        let catalog = self.catalog.0.read().unwrap();
        let Some(meta) = catalog.segment(segment) else {
            return Vec::new();
        };
        meta.records
            .iter()
            .filter_map(|(offset, record)| {
                let slot = catalog.at((segment, offset))?;
                if catalog.lookup.flags(slot) & VISIBLE == 0 {
                    return None;
                }
                Some((BlockKey::new(&record.key), catalog.location(slot)?))
            })
            .collect()
    }
    pub(super) fn new(catalog: LiveModel, slot: usize) -> Self {
        Self {
            catalog,
            slot,
            staged: None,
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
        let catalog = self.catalog.0.read().unwrap();
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
        let catalog = self.catalog.0.read().unwrap();
        catalog
            .lookup
            .iter()
            .filter_map(|(slot, handle, flags)| {
                if flags & VISIBLE == 0 {
                    return None;
                }
                let (address, record) = catalog.record(handle)?;
                if catalog.segment(address.0)?.slot != self.slot {
                    return None;
                }
                Some((BlockKey::new(&record.key), catalog.location(slot)?))
            })
            .collect::<Vec<_>>()
            .into_iter()
    }
    #[cfg(test)]
    pub fn values(&self) -> impl Iterator<Item = Location> + '_ {
        self.iter().map(|(_, location)| location)
    }
    pub fn insert(&mut self, key: BlockKey, location: Location) -> Option<Location> {
        let old = self.stage(key.clone(), location);
        self.publish(&key);
        old
    }
    pub fn stage(&mut self, key: BlockKey, location: Location) -> Option<Location> {
        let mut catalog = self.catalog.0.write().unwrap();
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
        let old_slot = self.find(&catalog, &key, VISIBLE);
        let old = old_slot.and_then(|slot| catalog.location(slot));
        if let Some(old_slot) = old_slot.filter(|old| *old != slot) {
            let old_address = catalog.record(catalog.lookup.handle(old_slot)).unwrap().0;
            let flags = catalog.lookup.flags(old_slot);
            catalog
                .lookup
                .set_flags(old_slot, flags & !(VISIBLE | POLICY));
            *catalog.live.entry(self.slot).or_default() -= 1;
            if let Some(policy) = catalog.policy.remove(&old_address) {
                catalog.policy.insert(address, policy);
            }
            if let Some(access) = catalog.access.remove(&old_address) {
                catalog.access.insert(address, access);
            }
            let flags = catalog.lookup.flags(slot) | flags & POLICY;
            catalog.lookup.set_flags(slot, flags);
        }
        if let Some(access) = location.access {
            if let Some(policy) = catalog.policy.get_mut(&address) {
                policy.access = access;
            } else {
                catalog.access.insert(address, access);
            }
        }
        old
    }
    pub fn publish(&mut self, id: &dyn BlockIdentity) {
        let mut catalog = self.catalog.0.write().unwrap();
        let slot = self.pending(&catalog, id).expect("staged small object");
        let flags = catalog.lookup.flags(slot);
        if flags & VISIBLE == 0 {
            *catalog.live.entry(self.slot).or_default() += 1;
        }
        catalog.lookup.set_flags(slot, flags | VISIBLE);
        self.staged = None;
    }
    pub fn attach_access(&mut self, id: &dyn BlockIdentity, access: AccessHandle) {
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
        let catalog = self.catalog.0.read().unwrap();
        if let Some(slot) = self.find(&catalog, id, POLICY) {
            let address = catalog.record(catalog.lookup.handle(slot))?.0;
            return catalog.policy.get(&address).cloned();
        }
        catalog.retired.get(id)?.policy.clone()
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
            let address = catalog.record(catalog.lookup.handle(slot))?.0;
            return Some(update(&mut catalog.policy.get_mut(&address)?.state));
        }
        Some(update(
            &mut catalog.retired.get_mut(id)?.policy.as_mut()?.state,
        ))
    }
    pub fn policy_remove(&mut self, id: &dyn BlockIdentity) -> Option<PolicyEntry> {
        let mut catalog = self.catalog.0.write().unwrap();
        if let Some(slot) = self.find(&catalog, id, POLICY) {
            let address = catalog.record(catalog.lookup.handle(slot))?.0;
            let flags = catalog.lookup.flags(slot) & !POLICY;
            catalog.lookup.set_flags(slot, flags);
            let policy = catalog.policy.remove(&address)?;
            policy.access.retire();
            return Some(policy);
        }
        let retired = catalog.retired.get_mut(id)?;
        let policy = retired.policy.take()?;
        policy.access.retire();
        if !retired.live {
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
