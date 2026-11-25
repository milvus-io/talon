use super::DirectoryEntry;
use crate::block_key::{BlockIdentity, BlockKey};
use crate::small_store::lookup::Lookup;
use std::hash::{BuildHasher, RandomState};
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use talon_core::BlockId;

#[derive(Default)]
struct Slot {
    key: Option<BlockKey>,
    entry: DirectoryEntry,
}

/// The lookup stores only stable slab handles. Hashing omits the source
/// version so exact lookups and superseded-version walks share one probe chain.
#[derive(Default)]
pub(super) struct Directory {
    lookup: Lookup,
    hash: RandomState,
    slots: Vec<Slot>,
    clocks: Option<Vec<AtomicU64>>,
    free: Vec<u32>,
}

impl Directory {
    fn hash(&self, id: &dyn BlockIdentity) -> u32 {
        let id = id.view();
        self.hash.hash_one((id.object, id.offset, id.block_size)) as u32
    }

    fn find(&self, id: &dyn BlockIdentity) -> Option<usize> {
        let view = id.view();
        self.lookup.find(self.hash(id), |handle, _| {
            self.slots[handle as usize - 1].key.as_ref().unwrap().view() == view
        })
    }

    pub fn position(&self, id: &dyn BlockIdentity) -> Option<usize> {
        Some(self.lookup.handle(self.find(id)?) as usize - 1)
    }

    /// A clock is addressed only while the directory protects its slot.
    pub fn clock(&self, slot: usize) -> Option<&AtomicU64> {
        Some(&self.clocks.as_ref()?[slot])
    }

    pub fn reset_clocks(&mut self, enabled: bool) {
        self.clocks = enabled.then(|| (0..self.slots.len()).map(|_| AtomicU64::new(0)).collect());
    }

    pub fn len(&self) -> usize {
        self.lookup.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn reserve(&mut self, additional: usize) {
        if self.lookup.needs_reserve(additional) {
            let growth = additional.max(self.lookup.len() / 8);
            self.lookup.rebuild(growth, |handle| {
                let id = self.slots[handle as usize - 1].key.as_ref().unwrap().view();
                self.hash.hash_one((id.object, id.offset, id.block_size)) as u32
            });
        }
        let new_slots = additional.saturating_sub(self.free.len());
        self.slots.reserve(new_slots);
        if let Some(clocks) = &mut self.clocks {
            clocks.reserve(new_slots);
        }
    }

    pub fn get(&self, id: &dyn BlockIdentity) -> Option<&DirectoryEntry> {
        self.get_key_value(id).map(|(_, entry)| entry)
    }

    pub fn get_mut(&mut self, id: &dyn BlockIdentity) -> Option<&mut DirectoryEntry> {
        let handle = self.lookup.handle(self.find(id)?);
        Some(&mut self.slots[handle as usize - 1].entry)
    }

    pub fn get_key_value(&self, id: &dyn BlockIdentity) -> Option<(&BlockKey, &DirectoryEntry)> {
        self.at(self.lookup.handle(self.find(id)?) as usize - 1)
    }

    pub fn get_or_insert(&mut self, id: BlockKey) -> (BlockKey, &mut DirectoryEntry) {
        let handle = match self.find(&id) {
            Some(i) => self.lookup.handle(i),
            None => {
                self.reserve(1);
                let handle = self.free.pop().unwrap_or_else(|| {
                    let handle = u32::try_from(self.slots.len() + 1)
                        .expect("block directory handle exhausted");
                    assert!(handle < u32::MAX);
                    self.slots.push(Slot::default());
                    if let Some(clocks) = &mut self.clocks {
                        clocks.push(AtomicU64::new(0));
                    }
                    handle
                });
                self.lookup.insert(self.hash(&id), handle, 0);
                self.slots[handle as usize - 1].key = Some(id);
                handle
            }
        };
        let slot = &mut self.slots[handle as usize - 1];
        (slot.key.as_ref().unwrap().clone(), &mut slot.entry)
    }

    pub fn remove(&mut self, id: &dyn BlockIdentity) -> Option<DirectoryEntry> {
        let i = self.find(id)?;
        let handle = self.lookup.handle(i);
        self.lookup.remove(i);
        self.free.push(handle);
        if let Some(clock) = self.clock(handle as usize - 1) {
            clock.store(0, Ordering::Relaxed);
        }
        Some(std::mem::take(&mut self.slots[handle as usize - 1]).entry)
    }

    pub fn slot_count(&self) -> usize {
        self.slots.len()
    }

    pub fn at(&self, cursor: usize) -> Option<(&BlockKey, &DirectoryEntry)> {
        let slot = self.slots.get(cursor)?;
        Some((slot.key.as_ref()?, &slot.entry))
    }

    pub fn versions(
        &self,
        id: &dyn BlockIdentity,
        mut visit: impl FnMut(&BlockKey, &DirectoryEntry),
    ) {
        let view = id.view();
        self.lookup.find_all(self.hash(id), |_, handle| {
            let (key, entry) = self.at(handle as usize - 1).unwrap();
            let candidate = key.view();
            if (candidate.object, candidate.offset, candidate.block_size)
                == (view.object, view.offset, view.block_size)
            {
                visit(key, entry);
            }
            false
        });
    }

    pub fn iter(&self) -> impl Iterator<Item = (&BlockKey, &DirectoryEntry)> {
        self.slots
            .iter()
            .filter_map(|slot| Some((slot.key.as_ref()?, &slot.entry)))
    }

    pub fn values(&self) -> impl Iterator<Item = &DirectoryEntry> {
        self.iter().map(|(_, entry)| entry)
    }

    pub fn retain(&mut self, mut keep: impl FnMut(&BlockKey, &mut DirectoryEntry) -> bool) {
        for cursor in 0..self.slots.len() {
            let slot = &mut self.slots[cursor];
            if let Some(key) = &slot.key {
                if !keep(key, &mut slot.entry) {
                    let id = key.clone();
                    self.remove(&id);
                }
            }
        }
    }
}

#[cfg(test)]
impl std::ops::Index<&BlockId> for Directory {
    type Output = DirectoryEntry;
    fn index(&self, id: &BlockId) -> &Self::Output {
        self.get(id).expect("directory entry")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{Backend, ObjectId, Version};

    #[test]
    fn removal_and_recovery_reuse_only_retired_slots() {
        let id = |n| {
            BlockKey::new(&BlockId::new(
                ObjectId::new(Backend::S3, "b", format!("o/{n}")),
                0,
                256,
                Version::new("v1"),
            ))
        };
        let mut directory = Directory::default();
        for n in 0..100 {
            directory.get_or_insert(id(n)).1.legacy_mut().small = if n % 3 == 0 { 1 } else { 0 };
        }
        let canonical = directory.get_key_value(&id(30)).unwrap().0.clone();
        let replacement = directory.get_or_insert(id(30)).0;
        assert!(canonical.ptr_eq(&replacement));
        for n in (0..100).step_by(2) {
            assert!(directory.remove(&id(n)).is_some());
        }
        directory.retain(|_, entry| entry.legacy.as_ref().unwrap().small == 0);
        let retained: Vec<_> = directory
            .iter()
            .map(|(id, entry)| (id.clone(), entry.legacy.as_ref().unwrap().small))
            .collect();
        let free = directory.slots.len() - directory.len();
        for n in 100..100 + free {
            let entry = directory.get_or_insert(id(n)).1;
            assert!(
                entry.is_empty(),
                "reused slots must not inherit prior state"
            );
            entry.legacy_mut().small = (n % 2) as u8;
        }
        assert_eq!(directory.slots.len(), 100);
        for (id, value) in retained {
            assert_eq!(
                directory.get(&id).unwrap().legacy.as_ref().unwrap().small,
                value
            );
        }
        directory.retain(|_, _| false);
        assert!(directory.is_empty());
        assert_eq!(directory.slots.len() - directory.len(), 100);
    }
}
