//! Access times live beside catalog rows. Journal snapshots share the packed
//! records without retaining a copy of the live access times.

use super::catalog::{Address, Catalog};
use super::journal::Model;
use crate::eviction::access_millis;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{
    AtomicU64,
    Ordering::{AcqRel, Acquire, Relaxed},
};
use std::sync::Mutex;

const DIRTY: u64 = 1 << 63;
// A hit clears this bit even when its millisecond timestamp is unchanged.
const GC_WATCH: u64 = 1 << 62;
const GROUP_ROWS: usize = u64::BITS as usize;
type DirtyGroup = (u64, usize);

#[derive(Clone, Copy, Default)]
pub(crate) struct IdleRecencyCursor {
    pub(super) next: Address,
    pub(super) end: Option<Address>,
}

pub(super) struct Recency {
    rows: BTreeMap<u64, SegmentRecency>,
    dirty_groups: Mutex<BTreeSet<DirtyGroup>>,
    pub started: u64,
}

#[derive(Default)]
struct SegmentRecency {
    times: Vec<AtomicU64>,
    dirty: Vec<AtomicU64>,
}

impl SegmentRecency {
    fn with_slots(slots: usize) -> Self {
        let mut rows = Self {
            times: (0..slots).map(|_| AtomicU64::new(0)).collect(),
            dirty: Vec::new(),
        };
        rows.resize_dirty();
        rows
    }

    fn resize_dirty(&mut self) {
        self.dirty
            .resize_with(self.times.len().div_ceil(GROUP_ROWS), AtomicU64::default);
    }
}

impl Recency {
    pub fn new(model: &Model) -> Self {
        Self {
            rows: model
                .segments
                .iter()
                .map(|(&id, meta)| (id, SegmentRecency::with_slots(meta.records.slots())))
                .collect(),
            dirty_groups: Mutex::default(),
            started: access_millis(),
        }
    }

    pub fn time(&self, value: u64) -> u64 {
        match value & !(DIRTY | GC_WATCH) {
            0 => self.started,
            time => time,
        }
    }

    pub fn watch_gc(value: &AtomicU64) -> bool {
        value.fetch_or(GC_WATCH, Relaxed) & GC_WATCH == 0
    }

    pub fn gc_unchanged(value: &AtomicU64) -> bool {
        value.load(Relaxed) & GC_WATCH != 0
    }

    pub fn restore(&self, value: &AtomicU64, time: u64) -> bool {
        time != 0 && value.fetch_max(time.min(self.started), Relaxed) == 0
    }

    fn touch(&self, segment: u64, row: usize) {
        let value = &self.rows[&segment].times[row];
        let now = access_millis();
        let changed = value.fetch_update(Relaxed, Relaxed, |old| {
            let updated = self.time(old).max(now) | DIRTY;
            (old != updated).then_some(updated)
        });
        if changed.is_ok_and(|old| old & DIRTY == 0) {
            self.mark_dirty(segment, row);
        }
    }

    fn mark_dirty(&self, segment: u64, row: usize) {
        let group = row / GROUP_ROWS;
        let word = &self.rows[&segment].dirty[group];
        let bit = 1 << (row % GROUP_ROWS);
        if word.fetch_or(bit, AcqRel) == 0 {
            let mut dirty_groups = self.dirty_groups.lock().unwrap();
            if word.load(Acquire) != 0 {
                dirty_groups.insert((segment, group));
            }
        }
    }

    fn clear_dirty(&self, segment: u64, row: usize) {
        let group = row / GROUP_ROWS;
        let word = &self.rows[&segment].dirty[group];
        let bit = 1 << (row % GROUP_ROWS);
        if word.fetch_and(!bit, AcqRel) == bit {
            let mut dirty_groups = self.dirty_groups.lock().unwrap();
            // A concurrent hit can dirty this group again before we remove it.
            if word.load(Acquire) == 0 {
                dirty_groups.remove(&(segment, group));
            }
        }
    }

    pub fn sample(&self, segment: u64, row: usize, full: bool) -> Option<u64> {
        // Clear the index before sampling. A hit in between is either included
        // in this timestamp or marks the group for the next checkpoint.
        self.clear_dirty(segment, row);
        let value = self.rows[&segment].times[row].fetch_and(!DIRTY, Relaxed);
        (full || value & DIRTY != 0).then(|| self.time(value))
    }

    fn next_dirty_group(&self, start: DirtyGroup) -> Option<DirtyGroup> {
        self.dirty_groups
            .lock()
            .unwrap()
            .range(start..)
            .next()
            .copied()
    }

    fn clear_groups(&mut self, segment: u64, first: usize) {
        let groups = self.dirty_groups.get_mut().unwrap();
        let range = (segment, first)..=(segment, usize::MAX);
        while let Some(group) = groups.range(range.clone()).next().copied() {
            groups.remove(&group);
        }
    }

    pub fn remove_segment(&mut self, segment: u64) {
        self.clear_groups(segment, 0);
        self.rows.remove(&segment);
    }

    fn rebuild_groups(&mut self, segment: u64, first: usize) {
        self.clear_groups(segment, first);
        let groups = self.dirty_groups.get_mut().unwrap();
        let rows = self.rows.get_mut(&segment).unwrap();
        rows.resize_dirty();
        for (group, chunk) in rows.times.chunks_mut(GROUP_ROWS).enumerate().skip(first) {
            let mut bits = 0;
            for (bit, value) in chunk.iter_mut().enumerate() {
                if *value.get_mut() & DIRTY != 0 {
                    bits |= 1 << bit;
                }
            }
            *rows.dirty[group].get_mut() = bits;
            if bits != 0 {
                groups.insert((segment, group));
            }
        }
    }

    pub fn reset(&self) {
        for segment in self.rows.values() {
            for value in &segment.times {
                value.store(0, Relaxed);
            }
            for word in &segment.dirty {
                word.store(0, Relaxed);
            }
        }
        self.dirty_groups.lock().unwrap().clear();
    }
}

pub(super) enum Change {
    Insert {
        segment: u64,
        row: usize,
        old_slots: usize,
    },
    Delete {
        segment: u64,
        row: usize,
    },
    Retain {
        segment: u64,
        live: Vec<bool>,
    },
}

impl Change {
    pub fn apply(self, model: &Model, recency: &mut Recency) {
        match self {
            Self::Insert {
                segment,
                row,
                old_slots,
            } => {
                let rows = recency.rows.entry(segment).or_default();
                let times = &mut rows.times;
                if model.segments[&segment].records.slots() == old_slots {
                    times[row].store(0, Relaxed);
                    recency.clear_dirty(segment, row);
                } else {
                    if times.len() == times.capacity() {
                        times.reserve_exact(64);
                    }
                    times.insert(row, AtomicU64::new(0));
                    if row == old_slots {
                        rows.resize_dirty();
                    } else {
                        recency.rebuild_groups(segment, row / GROUP_ROWS);
                    }
                }
            }
            Self::Delete { segment, row } => {
                recency.rows[&segment].times[row].store(0, Relaxed);
                recency.clear_dirty(segment, row);
            }
            Self::Retain { segment, live } => {
                let times = &mut recency.rows.get_mut(&segment).unwrap().times;
                let mut live = live.into_iter();
                times.retain(|_| live.next().unwrap_or(false));
                times.shrink_to_fit();
                recency.rebuild_groups(segment, 0);
            }
        }
    }
}

impl Catalog {
    pub(super) fn recency_at(&self, slot: usize) -> Option<(&Recency, &AtomicU64)> {
        let (segment, row) = self.row(self.lookup.handle(slot))?;
        let recency = self.recency.as_ref()?;
        Some((recency, recency.rows.get(&segment)?.times.get(row)?))
    }

    pub(super) fn touch_recency(&self, slot: usize) {
        if let Some(recency) = &self.recency {
            let (segment, row) = self.row(self.lookup.handle(slot)).unwrap();
            recency.touch(segment, row);
        }
    }

    pub(super) fn relocate_recency(&self, old: Address, new_slot: usize) {
        if let Some(recency) = &self.recency {
            let (segment, row) = self.row(self.lookup.handle(new_slot)).unwrap();
            let old_row = self.segment(old.0).unwrap().records.position(old.1);
            let time = recency.rows[&old.0].times[old_row].load(Relaxed);
            recency.rows[&segment].times[row].store(time | DIRTY, Relaxed);
            recency.mark_dirty(segment, row);
        }
    }

    /// Traverse only pending bitmap words, using the stable record offset to
    /// resume even if a journal mutation moves the row's index between batches.
    pub(super) fn visit_dirty_recency(
        &self,
        cursor: &mut Address,
        ceiling: Address,
        mut limit: usize,
        mut visit: impl FnMut(u64, usize) -> bool,
    ) -> bool {
        let recency = self.recency.as_ref().unwrap();
        let (start_segment, start_offset) = *cursor;
        let start_row = self
            .segment(start_segment)
            .map_or(0, |segment| segment.records.position(start_offset));
        let mut start = (start_segment, start_row / GROUP_ROWS);
        while let Some((id, group)) = recency.next_dirty_group(start) {
            if id > ceiling.0 {
                break;
            }
            let records = &self.segment(id).unwrap().records;
            let base = group * GROUP_ROWS;
            let skip = if id == start_segment {
                start_row.saturating_sub(base)
            } else {
                0
            };
            let mut pending = recency.rows[&id].dirty[group].load(Acquire) & (u64::MAX << skip);
            while pending != 0 {
                let bit = pending.trailing_zeros() as usize;
                pending &= pending - 1;
                let row = base + bit;
                let offset = records.offset_at(row);
                if (id, offset) >= ceiling {
                    return true;
                }
                if limit == 0 {
                    return false;
                }
                limit -= 1;
                *cursor = (id, offset + 1);
                if self.visible_policy_slot(id, row).is_some() && !visit(id, row) {
                    return false;
                }
            }
            start = (id, group + 1);
        }
        true
    }

    /// Charge one step per row, and charge a segment transition when no rows
    /// were visited. Offsets remain meaningful if deletion compacts the rows.
    pub(super) fn visit_recency(
        &self,
        cursor: &mut Address,
        end: Address,
        mut limit: usize,
        mut visit: impl FnMut(usize, u64, usize) -> bool,
    ) -> bool {
        if *cursor >= end {
            return true;
        }
        for (&id, segment) in self.model.segments.range(cursor.0..=end.0) {
            if limit == 0 {
                return false;
            }
            let records = &segment.records;
            let start = if id == cursor.0 {
                records.position(cursor.1)
            } else {
                0
            };
            let stop = if id == end.0 {
                records.position(end.1)
            } else {
                records.slots()
            };
            for row in start..stop {
                if limit == 0 {
                    return false;
                }
                limit -= 1;
                *cursor = (id, records.offset_at(row) + 1);
                let Some(slot) = self.visible_policy_slot(id, row) else {
                    continue;
                };
                if !visit(slot, id, row) {
                    return false;
                }
            }
            limit -= usize::from(start == stop);
            *cursor = (id + 1, 0);
        }
        true
    }
}
