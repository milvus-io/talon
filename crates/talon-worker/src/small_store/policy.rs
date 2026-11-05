use super::catalog::{Address, Catalog};
use super::lookup::{POLICY, REFERENCED, VISIBLE};
use crate::eviction::{AccessHandle, PolicyEntry, PolicyState};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};

pub(crate) struct SmallAccess {
    catalog: Weak<RwLock<Catalog>>,
    pub(super) address: Mutex<Address>,
    position: Mutex<(u64, usize)>,
    pub(crate) state: Mutex<PolicyState>,
    retired: AtomicBool,
}

impl std::fmt::Debug for SmallAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SmallAccess").finish_non_exhaustive()
    }
}

impl Drop for SmallAccess {
    fn drop(&mut self) {
        let Some(catalog) = self.catalog.upgrade() else {
            return;
        };
        let Ok(mut catalog) = catalog.try_write() else {
            return;
        };
        let address = *self.address.get_mut().unwrap();
        if catalog
            .tickets
            .get(&address)
            .is_some_and(|ticket| std::ptr::eq(ticket.as_ptr(), self))
        {
            catalog.tickets.remove(&address);
            if catalog.tickets.capacity() > 4 * catalog.tickets.len().max(256) {
                catalog.tickets.shrink_to(256);
            }
        }
    }
}

impl SmallAccess {
    pub fn retire(&self) {
        self.retired.store(true, Ordering::Release);
    }

    fn slot(&self, catalog: &Catalog) -> Option<usize> {
        if self.retired.load(Ordering::Acquire) {
            return None;
        }
        let mut position = self.position.lock().unwrap();
        let slot = if position.0 == catalog.lookup.epoch() {
            position.1
        } else {
            let slot = catalog.at(*self.address.lock().unwrap())?;
            *position = (catalog.lookup.epoch(), slot);
            slot
        };
        (catalog.lookup.flags(slot) & POLICY != 0).then_some(slot)
    }

    pub fn touch(&self) {
        let Some(catalog) = self.catalog.upgrade() else {
            return;
        };
        let mut catalog = catalog.write().unwrap();
        if let Some(slot) = self.slot(&catalog) {
            let flags = catalog.lookup.flags(slot) | REFERENCED;
            catalog.lookup.set_flags(slot, flags);
        }
    }

    pub fn referenced(&self) -> bool {
        let Some(catalog) = self.catalog.upgrade() else {
            return false;
        };
        let catalog = catalog.read().unwrap();
        self.slot(&catalog)
            .is_some_and(|slot| catalog.lookup.flags(slot) & REFERENCED != 0)
    }

    pub fn take_reference(&self) -> bool {
        let Some(catalog) = self.catalog.upgrade() else {
            return false;
        };
        let mut catalog = catalog.write().unwrap();
        let Some(slot) = self.slot(&catalog) else {
            return false;
        };
        let flags = catalog.lookup.flags(slot);
        catalog.lookup.set_flags(slot, flags & !REFERENCED);
        flags & REFERENCED != 0
    }

    pub fn snapshot(&self) -> u64 {
        let catalog = self.catalog.upgrade().expect("live small catalog");
        let mut catalog = catalog.write().unwrap();
        let revision = catalog.next_revision();
        self.state.lock().unwrap().revision = revision;
        if let Some(slot) = self.slot(&catalog) {
            let flags = catalog.lookup.flags(slot);
            catalog.lookup.set_flags(slot, flags & !REFERENCED);
        }
        revision
    }

    pub fn is_current(&self, revision: u64) -> bool {
        let Some(catalog) = self.catalog.upgrade() else {
            return false;
        };
        let catalog = catalog.read().unwrap();
        let Some(slot) = self.slot(&catalog) else {
            return false;
        };
        let state = self.state.lock().unwrap();
        state.pins == 0
            && state.revision == revision
            && catalog.lookup.flags(slot) & REFERENCED == 0
    }
}

impl Catalog {
    pub(super) fn next_revision(&mut self) -> u64 {
        self.revision = self
            .revision
            .checked_add(1)
            .expect("small policy revision exhausted");
        self.revision
    }

    pub(super) fn compact_entry(&mut self, slot: usize) -> PolicyEntry {
        let (address, record) = self
            .record(self.lookup.handle(slot))
            .expect("small policy record");
        let charge = super::charge(&record.key, record.len);
        let access = self.compact_access(slot, address, charge);
        let state = *access
            .small()
            .expect("compact access token")
            .state
            .lock()
            .unwrap();
        PolicyEntry { state, access }
    }

    pub(super) fn compact_access(
        &mut self,
        slot: usize,
        address: Address,
        charge: u64,
    ) -> AccessHandle {
        let bytes = self.charges.get(&address).copied().unwrap_or(charge);
        let ticket = self
            .tickets
            .get(&address)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                self.ticket_creations += 1;
                if self.ticket_creations >= self.tickets.len().max(256) {
                    self.ticket_creations = 0;
                    self.tickets.retain(|_, ticket| ticket.strong_count() != 0);
                    if self.tickets.capacity() > 4 * self.tickets.len().max(256) {
                        self.tickets.shrink_to(256);
                    }
                }
                let ticket = Arc::new(SmallAccess {
                    catalog: self.owner.clone(),
                    address: Mutex::new(address),
                    position: Mutex::new((self.lookup.epoch(), slot)),
                    state: Mutex::new(PolicyState {
                        independent: true,
                        bytes,
                        position: 0,
                        revision: self.next_revision(),
                        pins: 0,
                    }),
                    retired: AtomicBool::new(false),
                });
                self.tickets.insert(address, Arc::downgrade(&ticket));
                ticket
            });
        self.access
            .get(&address)
            .cloned()
            .unwrap_or_else(|| AccessHandle::for_small(ticket))
    }

    pub(super) fn enable_compact(&mut self, stream: usize) {
        if !self.compact_streams.insert(stream) {
            return;
        }
        let mut total = (0_u64, 0_usize);
        for slot in 0..self.lookup.capacity() {
            let Some((handle, flags)) = self.lookup.entry(slot) else {
                continue;
            };
            if flags & VISIBLE == 0 {
                continue;
            }
            let (address, record) = self.record(handle).unwrap();
            if self.segment(address.0).unwrap().slot != stream {
                continue;
            }
            assert!(
                !self.policy.contains_key(&address),
                "generic policy already installed"
            );
            total.0 += super::charge(&record.key, record.len);
            total.1 += 1;
            self.lookup.set_flags(slot, flags | POLICY);
        }
        self.compact_totals.insert(stream, total);
    }

    pub(super) fn compact_admit(&mut self, slot: usize, bytes: u64, clock: bool) -> AccessHandle {
        let (address, record) = self.record(self.lookup.handle(slot)).unwrap();
        let stream = self.segment(address.0).unwrap().slot;
        let charge = super::charge(&record.key, record.len);
        let flags = self.lookup.flags(slot);
        let old = if flags & POLICY != 0 {
            self.charges.get(&address).copied().unwrap_or(charge)
        } else {
            0
        };
        let total = self.compact_totals.entry(stream).or_default();
        total.0 = total.0 - old + bytes;
        total.1 += usize::from(flags & POLICY == 0);
        if bytes == charge {
            self.charges.remove(&address);
        } else {
            self.charges.insert(address, bytes);
        }
        self.lookup.set_flags(slot, flags | POLICY);
        let mut entry = self.compact_entry(slot);
        let revision = self.next_revision();
        let ticket = entry.access.small().unwrap();
        {
            let mut state = ticket.state.lock().unwrap();
            state.bytes = bytes;
            state.revision = revision;
        }
        if clock {
            entry.access = entry.access.enable_clock();
            self.access.insert(address, entry.access.clone());
        }
        entry.access
    }

    pub(super) fn compact_update<T>(
        &mut self,
        slot: usize,
        update: impl FnOnce(&mut PolicyState) -> T,
    ) -> T {
        let entry = self.compact_entry(slot);
        let ticket = entry.access.small().cloned().unwrap();
        let address = *ticket.address.lock().unwrap();
        let mut state = ticket.state.lock().unwrap();
        let result = update(&mut state);
        if state.pins == 0 {
            self.pins.remove(&address);
        } else {
            self.pins.insert(address, ticket.clone());
        }
        result
    }

    pub(super) fn compact_remove(&mut self, slot: usize) -> PolicyEntry {
        let entry = self.compact_entry(slot);
        let (address, _) = self.record(self.lookup.handle(slot)).unwrap();
        let stream = self.segment(address.0).unwrap().slot;
        self.compact_forget(address, stream, &entry);
        let flags = self.lookup.flags(slot) & !(POLICY | REFERENCED);
        self.lookup.set_flags(slot, flags);
        entry
    }

    pub(super) fn compact_forget(&mut self, address: Address, stream: usize, entry: &PolicyEntry) {
        let total = self
            .compact_totals
            .get_mut(&stream)
            .expect("small policy totals");
        total.0 -= entry.bytes;
        total.1 -= 1;
        entry.access.retire();
        self.tickets.remove(&address);
        self.pins.remove(&address);
        self.charges.remove(&address);
        self.access.remove(&address);
    }

    pub(super) fn relocate_policy(&mut self, old: Address, new: Address, slot: usize) {
        if let Some(ticket) = self.tickets.remove(&old) {
            if let Some(ticket) = ticket.upgrade() {
                *ticket.address.lock().unwrap() = new;
                *ticket.position.lock().unwrap() = (self.lookup.epoch(), slot);
            }
            self.tickets.insert(new, ticket);
        }
        if let Some(pin) = self.pins.remove(&old) {
            self.pins.insert(new, pin);
        }
        if let Some(charge) = self.charges.remove(&old) {
            self.charges.insert(new, charge);
        }
    }
}
