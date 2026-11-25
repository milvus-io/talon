//! Policy state for ordinary resident units. The catalog guards row identity;
//! the sparse table contains only outstanding selections and pins.
use super::{access_millis, UnitKind};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

const RESIDENT: u8 = 1;
const ADMITTED: u8 = 2;
const REFERENCED: u8 = 4;

#[derive(Default, Debug)]
pub(crate) struct UnitState(AtomicU8);

impl UnitState {
    pub fn resident(&self) -> bool {
        self.0.load(Relaxed) & RESIDENT != 0
    }

    pub fn set_resident(&mut self, resident: bool) -> bool {
        let old = self.0.get_mut();
        let present = *old & RESIDENT != 0;
        if resident {
            *old |= RESIDENT;
        } else {
            *old = 0;
        }
        present
    }

    pub fn admitted(&self) -> bool {
        self.0.load(Relaxed) & ADMITTED != 0
    }

    pub fn admit(&self) -> bool {
        debug_assert!(self.resident());
        self.0.fetch_or(ADMITTED, Relaxed) & ADMITTED == 0
    }

    pub fn retire(&self) {
        self.0.fetch_and(RESIDENT, Relaxed);
    }

    pub fn touch(&self, clock: Option<&AtomicU64>) {
        if self.admitted() {
            self.0.fetch_or(REFERENCED, Relaxed);
            if let Some(clock) = clock {
                touch_clock(clock);
            }
        }
    }

    fn referenced(&self) -> bool {
        self.0.load(Relaxed) & REFERENCED != 0
    }

    pub fn reset_reference(&self) {
        self.take_reference();
    }

    fn take_reference(&self) -> bool {
        self.0.fetch_and(!REFERENCED, Relaxed) & REFERENCED != 0
    }
}

fn touch_clock(clock: &AtomicU64) {
    let previous = clock.load(Relaxed);
    if previous != 0 {
        let now = access_millis();
        if previous < now {
            clock.fetch_max(now, Relaxed);
        }
    }
}

#[derive(Default, Debug)]
pub(crate) struct CompactPolicy {
    sequence: AtomicU64,
    admissions: AtomicU64,
    bytes: AtomicU64,
    units: AtomicU64,
    active: Arc<Mutex<Active>>,
}

type Address = (usize, u64);
type Active = BTreeMap<Address, Operation>;

fn block_addresses(slot: usize) -> std::ops::RangeInclusive<Address> {
    (slot, 0)..=(slot, u64::MAX)
}

#[derive(Default, Debug)]
struct Operation {
    selection: Option<u64>,
    // Only the first pin for an incarnation needs a sequence number.
    pin_generation: u64,
    pins: u32,
}

fn update_active(
    active: &mut Active,
    address: Address,
    update: impl FnOnce(&mut Operation),
) {
    if let Some(operation) = active.get_mut(&address) {
        update(operation);
        if operation.pins == 0 && operation.selection.is_none() {
            active.remove(&address);
        }
    }
}

#[derive(Debug)]
pub(crate) struct Selection {
    active: Arc<Mutex<Active>>,
    address: Address,
    generation: u64,
}

impl Drop for Selection {
    fn drop(&mut self) {
        update_active(&mut self.active.lock().unwrap(), self.address, |op| {
            if op.selection == Some(self.generation) {
                op.selection = None;
            }
        });
    }
}

pub(crate) struct Pin {
    active: Arc<Mutex<Active>>,
    address: Address,
    generation: u64,
}

impl Drop for Pin {
    fn drop(&mut self) {
        release(&mut self.active.lock().unwrap(), self.address, Some(self.generation));
    }
}

fn release(active: &mut Active, address: Address, generation: Option<u64>) {
    update_active(active, address, |op| {
        if op.pins > 0 && generation.is_none_or(|g| g == op.pin_generation) {
            op.pins -= 1;
        }
    });
}

impl CompactPolicy {
    pub fn next_revision(&self) -> u64 {
        self.sequence
            .fetch_update(Relaxed, Relaxed, |n| n.checked_add(1))
            .expect("eviction sequence exhausted")
    }

    pub fn admitted_at(&self, revision: u64) {
        self.admissions.fetch_max(revision + 1, Relaxed);
    }

    pub fn admission_ceiling(&self) -> u64 {
        self.admissions.load(Relaxed)
    }

    pub fn totals(&self) -> (u64, usize) {
        (self.bytes.load(Relaxed), self.units.load(Relaxed) as usize)
    }

    pub fn add(&self, bytes: u64, units: u64) {
        self.bytes.fetch_add(bytes, Relaxed);
        self.units.fetch_add(units, Relaxed);
    }

    pub fn subtract(&self, bytes: u64, units: u64) {
        self.bytes.fetch_sub(bytes, Relaxed);
        self.units.fetch_sub(units, Relaxed);
    }

    pub fn replace_bytes(&self, old: u64, new: u64) {
        self.bytes
            .fetch_update(Relaxed, Relaxed, |total| {
                Some(total.saturating_sub(old).saturating_add(new))
            })
            .unwrap();
    }

    pub fn reset(&self) {
        self.active.lock().unwrap().clear();
        self.bytes.store(0, Relaxed);
        self.units.store(0, Relaxed);
    }

    pub fn pins(&self, slot: usize, kind: UnitKind) -> u32 {
        self.active.lock().unwrap()
            .get(&(slot, kind.order()))
            .map_or(0, |op| op.pins)
    }

    pub fn block_pins(&self, slot: usize) -> usize {
        self.active.lock().unwrap()
            .range(block_addresses(slot))
            .map(|(_, op)| op.pins as usize)
            .sum()
    }

    pub fn pin(&self, slot: usize, kind: UnitKind) -> u64 {
        let mut active = self.active.lock().unwrap();
        let op = active.entry((slot, kind.order())).or_default();
        if op.pins == 0 {
            op.pin_generation = self.next_revision();
        }
        op.pins = op.pins.checked_add(1).expect("eviction pins exhausted");
        op.pin_generation
    }

    pub fn guard(&self, slot: usize, kind: UnitKind) -> Pin {
        Pin {
            active: self.active.clone(),
            address: (slot, kind.order()),
            generation: self.pin(slot, kind),
        }
    }

    pub fn unpin(&self, slot: usize, kind: UnitKind) {
        release(&mut self.active.lock().unwrap(), (slot, kind.order()), None);
    }

    pub fn invalidate(&self, slot: usize, kind: UnitKind) {
        self.active.lock().unwrap().remove(&(slot, kind.order()));
    }

    pub fn invalidate_block(&self, slot: usize) {
        self.active.lock().unwrap().extract_if(block_addresses(slot), |_, _| true).for_each(drop);
    }

    pub fn select(
        &self,
        slot: usize,
        kind: UnitKind,
        state: &UnitState,
        force: bool,
    ) -> Option<Arc<Selection>> {
        let address = (slot, kind.order());
        let mut active = self.active.lock().unwrap();
        if active.get(&address).is_some_and(|op| op.pins > 0) {
            return None;
        }
        // Consuming a reference always retires the previous selection. Keep
        // the table locked so that no validator can see its bit cleared first.
        if state.take_reference() && !force {
            active.remove(&address);
            return None;
        }
        let generation = self.next_revision();
        active.entry(address).or_default().selection = Some(generation);
        Some(Arc::new(Selection {
            active: self.active.clone(),
            address,
            generation,
        }))
    }

    pub fn is_current(
        &self,
        slot: usize,
        kind: UnitKind,
        state: &UnitState,
        ticket: &Selection,
    ) -> bool {
        let address = (slot, kind.order());
        if address != ticket.address || !Arc::ptr_eq(&self.active, &ticket.active) {
            return false;
        }
        self.active.lock().unwrap().get(&address).is_some_and(|op| {
            op.selection == Some(ticket.generation) && op.pins == 0 && !state.referenced()
        })
    }

    #[cfg(test)]
    pub fn active_units(&self) -> usize {
        self.active.lock().unwrap().len()
    }
}
