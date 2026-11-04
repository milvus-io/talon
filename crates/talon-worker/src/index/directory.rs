use super::{BlockId, DirectoryEntry};
use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;

/// Keep large records out of spare hash buckets. Slots never escape the
/// directory lock; externally retained resident/lifecycle objects use Arcs.
#[derive(Default)]
pub(super) struct Directory {
    keys: HashMap<Arc<BlockId>, usize>,
    slots: Vec<DirectoryEntry>,
    free: Vec<usize>,
}

impl Directory {
    pub fn len(&self) -> usize {
        self.keys.len()
    }
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
    pub fn reserve(&mut self, additional: usize) {
        self.keys.reserve(additional);
        self.slots
            .reserve(additional.saturating_sub(self.free.len()));
    }
    pub fn get(&self, id: &BlockId) -> Option<&DirectoryEntry> {
        Some(&self.slots[*self.keys.get(id)?])
    }
    pub fn get_mut(&mut self, id: &BlockId) -> Option<&mut DirectoryEntry> {
        Some(&mut self.slots[*self.keys.get(id)?])
    }
    pub fn get_key_value(&self, id: &BlockId) -> Option<(&Arc<BlockId>, &DirectoryEntry)> {
        let (key, slot) = self.keys.get_key_value(id)?;
        Some((key, &self.slots[*slot]))
    }
    pub fn get_or_insert(&mut self, id: Arc<BlockId>) -> (Arc<BlockId>, &mut DirectoryEntry) {
        let (key, slot) = match self.keys.entry(id) {
            Entry::Occupied(entry) => (entry.key().clone(), *entry.get()),
            Entry::Vacant(entry) => {
                let slot = self.free.pop().unwrap_or_else(|| {
                    self.slots.push(DirectoryEntry::default());
                    self.slots.len() - 1
                });
                let key = entry.key().clone();
                entry.insert(slot);
                (key, slot)
            }
        };
        (key, &mut self.slots[slot])
    }
    pub fn remove(&mut self, id: &BlockId) -> Option<DirectoryEntry> {
        let slot = self.keys.remove(id)?;
        self.free.push(slot);
        Some(std::mem::take(&mut self.slots[slot]))
    }
    pub fn iter(&self) -> impl Iterator<Item = (&Arc<BlockId>, &DirectoryEntry)> {
        self.keys
            .iter()
            .map(|(key, slot)| (key, &self.slots[*slot]))
    }
    pub fn values(&self) -> impl Iterator<Item = &DirectoryEntry> {
        self.keys.values().map(|slot| &self.slots[*slot])
    }
    pub fn retain(&mut self, mut keep: impl FnMut(&Arc<BlockId>, &mut DirectoryEntry) -> bool) {
        self.keys.retain(|key, slot| {
            if keep(key, &mut self.slots[*slot]) {
                return true;
            }
            self.slots[*slot] = DirectoryEntry::default();
            self.free.push(*slot);
            false
        });
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
            Arc::new(BlockId::new(
                ObjectId::new(Backend::S3, "b", format!("o/{n}")),
                0,
                256,
                Version::new("v1"),
            ))
        };
        let mut directory = Directory::default();
        for n in 0..100 {
            directory.get_or_insert(id(n)).1.policy.len = n;
        }
        let canonical = directory.get_key_value(&id(30)).unwrap().0.clone();
        let replacement = directory.get_or_insert(id(30)).0;
        assert!(Arc::ptr_eq(&canonical, &replacement));
        for n in (0..100).step_by(2) {
            assert_eq!(directory.remove(&id(n)).unwrap().policy.len, n);
        }
        directory.retain(|_, entry| entry.policy.len % 3 != 0);
        let retained: Vec<_> = directory
            .iter()
            .map(|(id, entry)| (id.clone(), entry.policy.len))
            .collect();
        let free = directory.free.len();
        for n in 100..100 + free {
            let entry = directory.get_or_insert(id(n)).1;
            assert!(
                entry.is_empty(),
                "reused slots must not inherit prior state"
            );
            entry.policy.len = n;
        }
        assert_eq!(directory.slots.len(), 100);
        for (id, value) in retained {
            assert_eq!(directory.get(&id).unwrap().policy.len, value);
        }
        directory.retain(|_, _| false);
        assert!(directory.is_empty());
        assert_eq!(directory.free.len(), 100);
    }
}
