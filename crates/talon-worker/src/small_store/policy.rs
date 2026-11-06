use super::catalog::{Address, Catalog};
use super::lookup::{POLICY, REFERENCED, VISIBLE};
use crate::eviction::{AccessHandle, PolicyEntry, PolicyState};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};

pub(crate) struct SmallAccess {
    catalog: Weak<RwLock<Catalog>>,
    tickets: Weak<super::tickets::Tickets>,
    pub(super) address: Mutex<Address>,
    position: AtomicUsize,
    epoch: AtomicU64,
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
        let Some(tickets) = self.tickets.upgrade() else {
            return;
        };
        let address = *self.address.get_mut().unwrap();
        tickets.remove_if(address, |ticket| std::ptr::eq(ticket.as_ptr(), self));
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
        let slot = if self.epoch.load(Ordering::Acquire) == catalog.lookup.epoch() {
            self.position.load(Ordering::Relaxed)
        } else {
            let slot = catalog.at(*self.address.lock().unwrap())?;
            // The catalog read lock keeps the epoch fixed; concurrent refreshes publish the same slot.
            self.position.store(slot, Ordering::Relaxed);
            self.epoch.store(catalog.lookup.epoch(), Ordering::Release);
            slot
        };
        (catalog.lookup.flags(slot) & POLICY != 0).then_some(slot)
    }

    pub fn touch(&self) {
        let Some(catalog) = self.catalog.upgrade() else {
            return;
        };
        let catalog = catalog.read().unwrap();
        if let Some(slot) = self.slot(&catalog) {
            catalog.lookup.reference(slot);
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
        let catalog = catalog.read().unwrap();
        let Some(slot) = self.slot(&catalog) else {
            return false;
        };
        catalog.lookup.take_reference(slot)
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
    pub(super) fn next_revision(&self) -> u64 {
        self.revision
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |revision| {
                revision.checked_add(1)
            })
            .expect("small policy revision exhausted")
            + 1
    }

    pub(super) fn compact_entry(&self, slot: usize) -> PolicyEntry {
        let (address, record) = self
            .metadata(self.lookup.handle(slot))
            .expect("small policy record");
        let charge = record.charge;
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
        &self,
        slot: usize,
        address: Address,
        charge: u64,
    ) -> AccessHandle {
        let bytes = self.charges.get(&address).copied().unwrap_or(charge);
        let ticket = self.tickets.get_or_insert(address, || SmallAccess {
            catalog: self.owner.clone(),
            tickets: Arc::downgrade(&self.tickets),
            address: Mutex::new(address),
            position: AtomicUsize::new(slot),
            epoch: AtomicU64::new(self.lookup.epoch()),
            state: Mutex::new(PolicyState {
                independent: true,
                bytes,
                position: 0,
                revision: self.next_revision(),
                pins: 0,
            }),
            retired: AtomicBool::new(false),
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
        if self.live.get(&stream).copied().unwrap_or(0) == 0 {
            self.compact_totals.insert(stream, (0, 0));
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
            let (address, record) = self.metadata(handle).unwrap();
            if self.segment(address.0).unwrap().slot != stream {
                continue;
            }
            assert!(
                !self.policy.contains_key(&address),
                "generic policy already installed"
            );
            total.0 += record.charge;
            total.1 += 1;
            self.lookup.set_flags(slot, flags | POLICY);
        }
        self.compact_totals.insert(stream, total);
    }

    pub(super) fn compact_admit(&mut self, slot: usize, bytes: u64, clock: bool) -> AccessHandle {
        let (address, record) = self.metadata(self.lookup.handle(slot)).unwrap();
        let stream = self.segment(address.0).unwrap().slot;
        let charge = record.charge;
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
        let (address, _) = self.metadata(self.lookup.handle(slot)).unwrap();
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
        if let Some(ticket) = self
            .tickets
            .remove(&old)
            .and_then(|ticket| ticket.upgrade())
        {
            *ticket.address.lock().unwrap() = new;
            ticket.position.store(slot, Ordering::Relaxed);
            ticket.epoch.store(self.lookup.epoch(), Ordering::Release);
            self.tickets.insert(new, Arc::downgrade(&ticket));
        }
        if let Some(pin) = self.pins.remove(&old) {
            self.pins.insert(new, pin);
        }
        if let Some(charge) = self.charges.remove(&old) {
            self.charges.insert(new, charge);
        }
    }
}
