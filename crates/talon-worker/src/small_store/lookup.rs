use std::sync::atomic::{AtomicU8, Ordering};

const EMPTY: u32 = 0;
const DELETED: u32 = u32::MAX;
const FINGERPRINT: u8 = 31;
pub(super) const VISIBLE: u8 = 32;
pub(super) const POLICY: u8 = 64;
pub(super) const REFERENCED: u8 = 128;

#[derive(Default)]
pub(super) struct Lookup {
    handles: Vec<u32>,
    control: Vec<AtomicU8>,
    len: usize,
    deleted: usize,
    epoch: u64,
    #[cfg(test)]
    rebuilds: usize,
}

impl Lookup {
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    fn changed(&mut self) {
        self.epoch = self
            .epoch
            .checked_add(1)
            .expect("small lookup epoch exhausted");
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn capacity(&self) -> usize {
        self.handles.len()
    }

    pub fn needs_reserve(&self, additional: usize) -> bool {
        (self.len + self.deleted + additional) * 5 > self.capacity() * 4
    }

    pub fn rebuild(&mut self, additional: usize, hash: impl Fn(u32) -> u64) {
        let target = (self.len + additional).max(16);
        let capacity = (target * 5).div_ceil(4).div_ceil(64) * 64;
        let mut next = Self {
            handles: vec![EMPTY; capacity],
            control: (0..capacity).map(|_| AtomicU8::new(0)).collect(),
            len: 0,
            deleted: 0,
            epoch: self.epoch,
            #[cfg(test)]
            rebuilds: self.rebuilds + 1,
        };
        next.changed();
        for (_, handle, flags) in self.iter() {
            next.insert(hash(handle), handle, flags);
        }
        *self = next;
    }

    pub fn find(&self, hash: u64, mut equal: impl FnMut(u32, u8) -> bool) -> Option<usize> {
        if self.capacity() == 0 {
            return None;
        }
        let fingerprint = (hash >> 59) as u8;
        let mut slot = (hash % self.capacity() as u64) as usize;
        for _ in 0..self.capacity() {
            let handle = self.handles[slot];
            if handle == EMPTY {
                return None;
            }
            if handle != DELETED
                && self.control[slot].load(Ordering::Relaxed) & FINGERPRINT == fingerprint
                && equal(handle, self.flags(slot))
            {
                return Some(slot);
            }
            slot += 1;
            if slot == self.capacity() {
                slot = 0;
            }
        }
        None
    }

    pub fn insert(&mut self, hash: u64, handle: u32, flags: u8) -> usize {
        assert!(handle != EMPTY && handle != DELETED);
        let mut slot = (hash % self.capacity() as u64) as usize;
        let mut tombstone = None;
        for _ in 0..self.capacity() {
            match self.handles[slot] {
                EMPTY => {
                    let slot = tombstone.unwrap_or(slot);
                    self.deleted -= usize::from(tombstone.is_some());
                    self.handles[slot] = handle;
                    *self.control[slot].get_mut() = (hash >> 59) as u8 | (flags & !FINGERPRINT);
                    self.len += 1;
                    self.changed();
                    return slot;
                }
                DELETED => {
                    tombstone.get_or_insert(slot);
                }
                _ => {}
            }
            slot += 1;
            if slot == self.capacity() {
                slot = 0;
            }
        }
        panic!("small-object lookup reservation exhausted")
    }

    pub fn remove(&mut self, slot: usize) -> u8 {
        assert!(self.handles[slot] != EMPTY && self.handles[slot] != DELETED);
        self.handles[slot] = DELETED;
        self.len -= 1;
        self.deleted += 1;
        self.changed();
        self.flags(slot)
    }

    pub fn handle(&self, slot: usize) -> u32 {
        self.handles[slot]
    }

    pub fn entry(&self, slot: usize) -> Option<(u32, u8)> {
        let handle = *self.handles.get(slot)?;
        (handle != EMPTY && handle != DELETED).then_some((handle, self.flags(slot)))
    }

    #[cfg(test)]
    pub fn rebuilds(&self) -> usize {
        self.rebuilds
    }

    pub fn flags(&self, slot: usize) -> u8 {
        self.control[slot].load(Ordering::Relaxed) & !FINGERPRINT
    }

    pub fn set_flags(&mut self, slot: usize, flags: u8) {
        let control = self.control[slot].get_mut();
        *control = *control & FINGERPRINT | flags & !FINGERPRINT;
    }

    pub fn reference(&self, slot: usize) {
        self.control[slot].fetch_or(REFERENCED, Ordering::Relaxed);
    }

    pub fn take_reference(&self, slot: usize) -> bool {
        self.control[slot].fetch_and(!REFERENCED, Ordering::Relaxed) & REFERENCED != 0
    }

    pub fn iter(&self) -> impl Iterator<Item = (usize, u32, u8)> + '_ {
        self.handles
            .iter()
            .enumerate()
            .filter_map(|(slot, &handle)| {
                (handle != EMPTY && handle != DELETED).then_some((slot, handle, self.flags(slot)))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn colliding_keys_survive_deletion_and_rehash_without_losing_flags() {
        let mut lookup = Lookup::default();
        lookup.rebuild(100, |_| 7);
        for handle in 1..=100 {
            lookup.insert(7, handle, if handle % 2 == 0 { VISIBLE } else { POLICY });
        }
        for handle in (1..=100).step_by(3) {
            let slot = lookup.find(7, |actual, _| actual == handle).unwrap();
            lookup.remove(slot);
        }
        lookup.rebuild(0, |_| 7);
        for handle in 1..=100 {
            let slot = lookup.find(7, |actual, _| actual == handle);
            if (handle - 1) % 3 == 0 {
                assert!(slot.is_none());
            } else {
                assert_eq!(
                    lookup.flags(slot.unwrap()),
                    if handle % 2 == 0 { VISIBLE } else { POLICY }
                );
            }
        }
        assert!(lookup.find(7, |actual, _| actual == 101).is_none());
    }
}
