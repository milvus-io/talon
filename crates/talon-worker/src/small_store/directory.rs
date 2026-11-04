use super::Location;
use crate::block_key::{BlockIdentity, BlockKey};
use crate::eviction::{AccessHandle, PolicyEntry, PolicyState};
use std::collections::HashMap;

#[derive(Default)]
pub(crate) struct Directory {
    keys: HashMap<BlockKey, usize>,
    slots: Vec<Option<Entry>>,
    free: Vec<usize>,
    live: usize,
}

struct Entry {
    key: BlockKey,
    location: Location,
    live: bool,
    policy: Option<PolicyState>,
}

impl Directory {
    pub fn reserve(&mut self, additional: usize) {
        self.keys.reserve(additional);
        self.slots
            .reserve(additional.saturating_sub(self.free.len()));
    }

    pub fn len(&self) -> usize {
        self.live
    }

    fn entry(&self, id: &dyn BlockIdentity) -> Option<&Entry> {
        self.slots[*self.keys.get(id)?].as_ref()
    }

    fn entry_mut(&mut self, id: &dyn BlockIdentity) -> Option<&mut Entry> {
        self.slots[*self.keys.get(id)?].as_mut()
    }

    pub fn identity(&self, id: &dyn BlockIdentity) -> Option<BlockKey> {
        self.keys.get_key_value(id).map(|(key, _)| key.clone())
    }

    pub fn get(&self, id: &dyn BlockIdentity) -> Option<&Location> {
        let entry = self.entry(id)?;
        entry.live.then_some(&entry.location)
    }

    pub fn get_mut(&mut self, id: &dyn BlockIdentity) -> Option<&mut Location> {
        let entry = self.entry_mut(id)?;
        entry.live.then_some(&mut entry.location)
    }

    pub fn get_key_value(&self, id: &dyn BlockIdentity) -> Option<(&BlockKey, &Location)> {
        let entry = self.entry(id)?;
        entry.live.then_some((&entry.key, &entry.location))
    }

    pub fn contains_key(&self, id: &dyn BlockIdentity) -> bool {
        self.get(id).is_some()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&BlockKey, &Location)> {
        self.slots
            .iter()
            .flatten()
            .filter(|e| e.live)
            .map(|e| (&e.key, &e.location))
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&BlockKey, &mut Location)> {
        self.slots
            .iter_mut()
            .flatten()
            .filter(|e| e.live)
            .map(|e| (&e.key, &mut e.location))
    }

    #[cfg(test)]
    pub fn values(&self) -> impl Iterator<Item = &Location> {
        self.iter().map(|(_, loc)| loc)
    }

    pub fn insert(&mut self, key: BlockKey, location: Location) -> Option<Location> {
        let old = self.stage(key.clone(), location);
        self.publish(&key);
        old
    }

    /// Stage an unpublished locator before attaching policy under the LRU lock.
    /// No directory lock may be held while entering the LRU.
    pub fn stage(&mut self, key: BlockKey, mut location: Location) -> Option<Location> {
        if let Some(entry) = self.entry_mut(&key) {
            if entry.policy.is_some() && location.access.is_none() {
                location.access = entry.location.access.clone();
            }
            let old = std::mem::replace(&mut entry.location, location);
            return entry.live.then_some(old);
        }
        let slot = self.free.pop().unwrap_or_else(|| {
            self.slots.push(None);
            self.slots.len() - 1
        });
        self.keys.insert(key.clone(), slot);
        self.slots[slot] = Some(Entry {
            key,
            location,
            live: false,
            policy: None,
        });
        None
    }

    pub fn publish(&mut self, id: &dyn BlockIdentity) {
        let entry = self.entry_mut(id).expect("staged small object");
        if !entry.live {
            entry.live = true;
            self.live += 1;
        }
    }

    pub fn attach_access(&mut self, id: &dyn BlockIdentity, access: AccessHandle) {
        self.entry_mut(id)
            .expect("staged small object")
            .location
            .access = Some(access);
    }

    pub fn remove(&mut self, id: &dyn BlockIdentity) -> Option<Location> {
        let entry = self.entry_mut(id)?;
        if !entry.live {
            return None;
        }
        entry.live = false;
        let location = entry.location.clone();
        let empty = entry.policy.is_none();
        self.live -= 1;
        if empty {
            self.release(id);
        }
        Some(location)
    }

    fn release(&mut self, id: &dyn BlockIdentity) {
        let slot = self.keys.remove(id).expect("retired small object");
        self.slots[slot] = None;
        self.free.push(slot);
    }

    pub fn policy_get(&self, id: &dyn BlockIdentity) -> Option<PolicyEntry> {
        let entry = self.entry(id)?;
        Some(PolicyEntry {
            state: entry.policy?,
            access: entry.location.access.as_ref()?.clone(),
        })
    }

    pub fn policy_insert(
        &mut self,
        id: &dyn BlockIdentity,
        policy: PolicyEntry,
    ) -> Option<AccessHandle> {
        let entry = self.entry_mut(id)?;
        entry.policy = Some(policy.state);
        entry.location.access = Some(policy.access.clone());
        Some(policy.access)
    }

    pub fn policy_update<T>(
        &mut self,
        id: &dyn BlockIdentity,
        update: impl FnOnce(&mut PolicyState) -> T,
    ) -> Option<T> {
        Some(update(self.entry_mut(id)?.policy.as_mut()?))
    }

    pub fn policy_remove(&mut self, id: &dyn BlockIdentity) -> Option<PolicyEntry> {
        let entry = self.entry_mut(id)?;
        let policy = PolicyEntry {
            state: entry.policy.take()?,
            access: entry.location.access.as_ref()?.clone(),
        };
        policy.access.retire();
        if !entry.live {
            self.release(id);
        }
        Some(policy)
    }
}
