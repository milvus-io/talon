use super::*;

impl IndexedBlock {
    pub(super) fn for_each_compact_unit(&self, mut visit: impl FnMut(UnitKind, u64)) {
        if self.policy_units == 0 {
            return;
        }
        match &self.units {
            IndexedUnits::Whole(slot) => {
                if slot.access.admitted() {
                    visit(UnitKind::Whole, slot.allocated);
                }
            }
            IndexedUnits::Paged(pages) => {
                for (&page, entry) in pages.read().unwrap().iter() {
                    if entry.unit.access.admitted() {
                        visit(
                            UnitKind::Page(PageIndex(page)),
                            entry.unit.allocated,
                        );
                    }
                }
            }
        }
    }

    fn with_compact_unit<T>(
        &self,
        kind: UnitKind,
        visit: impl FnOnce(&UnitState, u64) -> T,
    ) -> Option<T> {
        match (kind, &self.units) {
            (UnitKind::Whole, IndexedUnits::Whole(slot)) if slot.access.admitted() => {
                Some(visit(&slot.access, slot.allocated))
            }
            (UnitKind::Page(page), IndexedUnits::Paged(pages)) => {
                let pages = pages.read().unwrap();
                let slot = &pages.get(&page.0)?.unit;
                slot.access
                    .admitted()
                    .then(|| visit(&slot.access, slot.allocated))
            }
            _ => None,
        }
    }
}

impl BlockIndex {
    pub(crate) fn enable_compact_policy(&self, track_blocks: bool) {
        let mut entries = self.entries.write().unwrap();
        entries.reset_clocks(track_blocks);
        for slot in 0..entries.slot_count() {
            let Some((_, entry)) = entries.at(slot) else {
                continue;
            };
            if let Some(entry) = &entry.resident {
                let mut entry = entry.block.write().unwrap();
                self.admit_block(entry.as_mut().unwrap(), entries.clock(slot));
            }
        }
        self.compact_enabled.store(true, Ordering::Relaxed);
    }

    pub(super) fn admit_block(&self, block: &mut IndexedBlock, clock: Option<&AtomicU64>) {
        let mut units = 0;
        let mut bytes = 0;
        let mut admit = |access: &UnitState, allocated| {
            if access.resident() && access.admit() {
                units += 1;
                bytes += allocated;
            }
        };
        match &block.units {
            IndexedUnits::Whole(slot) => admit(&slot.access, slot.allocated),
            IndexedUnits::Paged(pages) => {
                for page in pages.read().unwrap().values() {
                    admit(&page.unit.access, page.unit.allocated);
                }
            }
        }
        if units > 0 {
            block.policy_units += units;
            block.policy_bytes += bytes;
            self.compact.add(bytes, u64::from(units));
            block.policy_revision = self.record_admission(clock);
        }
    }

    pub(super) fn record_admission(&self, clock: Option<&AtomicU64>) -> u64 {
        if let Some(clock) = clock {
            clock.fetch_max(crate::eviction::access_millis().max(1), Ordering::Relaxed);
        }
        let revision = self.compact.next_revision();
        self.compact.admitted_at(revision);
        revision
    }

    pub(super) fn refund_block(&self, block: &IndexedBlock) {
        // The detached block may still share its page table with lifecycle
        // work. Refund its recorded charges without consulting those rows.
        self.compact
            .subtract(block.policy_bytes, u64::from(block.policy_units));
    }

    fn with_compact_unit<T>(
        &self,
        unit: &impl UnitKey,
        visit: impl FnOnce(usize, Option<&AtomicU64>, &UnitState, u64) -> T,
    ) -> Option<T> {
        if !self.compact_enabled.load(Ordering::Relaxed) {
            return None;
        }
        let entries = self.entries.read().unwrap();
        let slot = entries.position(unit.block())?;
        let block = entries.at(slot)?.1.resident.as_ref()?.block.read().unwrap();
        block
            .as_ref()?
            .with_compact_unit(unit.kind(), |state, bytes| {
                visit(slot, entries.clock(slot), state, bytes)
            })
    }

    pub(crate) fn compact_touch(&self, unit: &impl UnitKey) -> Option<()> {
        self.with_compact_unit(unit, |_, clock, state, _| state.touch(clock))
    }

    pub(crate) fn compact_pin(&self, unit: &impl UnitKey) -> Option<()> {
        self.with_compact_unit(unit, |slot, _, _, _| {
            self.compact.pin(slot, unit.kind());
        })
    }

    pub(crate) fn compact_pin_guard(&self, unit: &impl UnitKey) -> Option<Pin> {
        self.with_compact_unit(unit, |slot, _, _, _| self.compact.guard(slot, unit.kind()))
    }

    pub(crate) fn compact_unpin(&self, unit: &impl UnitKey) -> Option<()> {
        self.with_compact_unit(unit, |slot, _, _, _| self.compact.unpin(slot, unit.kind()))
    }

    pub(super) fn with_compact_block_write<T>(
        &self,
        id: &dyn BlockIdentity,
        write: impl FnOnce(&mut IndexedBlock, usize, Option<&AtomicU64>) -> T,
    ) -> Option<T> {
        loop {
            let entries = self.entries.read().unwrap();
            let slot = entries.position(id)?;
            let entry = entries.at(slot)?.1.resident.as_ref()?.clone();
            if let Ok(mut block) = entry.block.try_write() {
                return Some(write(block.as_mut()?, slot, entries.clock(slot)));
            }
            // A waiting writer must leave unrelated directory slots available.
            // Resolve the slot again once the contended entry lock is released.
            drop(entries);
            drop(entry.block.write().unwrap());
        }
    }

    pub(crate) fn compact_remove(&self, unit: &impl UnitKey) -> Option<u64> {
        if !self.compact_enabled.load(Ordering::Relaxed) {
            return None;
        }
        self.with_compact_block_write(unit.block(), |block, slot, clock| {
            let bytes = block.with_compact_unit(unit.kind(), |state, bytes| {
                self.compact.invalidate(slot, unit.kind());
                state.retire();
                bytes
            })?;
            block.policy_units -= 1;
            block.policy_bytes -= bytes;
            if let Some(clock) = clock.filter(|_| block.policy_units == 0) {
                clock.store(0, Ordering::Relaxed);
            }
            self.compact.subtract(bytes, 1);
            Some(bytes)
        })?
    }

    // Outer Option distinguishes a compact resident from a legacy unit; the
    // inner Option records whether the resident got a second chance or a pin.
    pub(crate) fn compact_select(
        &self,
        unit: &impl UnitKey,
        force: bool,
    ) -> Option<Option<(u64, Arc<Selection>)>> {
        self.with_compact_unit(unit, |slot, _, state, bytes| {
            self.compact
                .select(slot, unit.kind(), state, force)
                .map(|ticket| (bytes, ticket))
        })
    }

    pub(crate) fn compact_is_current(&self, unit: &impl UnitKey, ticket: &Selection) -> bool {
        self.with_compact_unit(unit, |slot, _, state, _| {
            self.compact.is_current(slot, unit.kind(), state, ticket)
        })
        .unwrap_or(false)
    }
}
