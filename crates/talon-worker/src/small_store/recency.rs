//! Access times live beside catalog rows. Journal snapshots share the packed
//! records without retaining a copy of the live access times.

use super::catalog::{Address, Catalog};
use super::journal::{Model, SegmentMeta};
use super::lookup::{POLICY, VISIBLE};
use crate::eviction::access_millis;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

const DIRTY: u64 = 1 << 63;

pub(super) struct Recency {
    pub rows: BTreeMap<u64, Vec<AtomicU64>>,
    pub started: u64,
}

impl Recency {
    pub fn new(model: &Model) -> Self {
        Self {
            rows: model
                .segments
                .iter()
                .map(|(&id, meta)| {
                    (
                        id,
                        (0..meta.records.slots())
                            .map(|_| AtomicU64::new(0))
                            .collect(),
                    )
                })
                .collect(),
            started: access_millis(),
        }
    }

    pub fn time(&self, value: u64) -> u64 {
        match value & !DIRTY {
            0 => self.started,
            time => time,
        }
    }

    pub fn restore(&self, value: &AtomicU64, time: u64) -> bool {
        time != 0 && value.fetch_max(time.min(self.started), Relaxed) == 0
    }

    pub fn touch(&self, value: &AtomicU64) {
        let now = access_millis();
        let _ = value.fetch_update(Relaxed, Relaxed, |old| {
            let updated = self.time(old).max(now) | DIRTY;
            (old != updated).then_some(updated)
        });
    }

    pub fn sample(&self, value: &AtomicU64, full: bool) -> Option<u64> {
        let old = value.load(Relaxed);
        if !full && old & DIRTY == 0 {
            return None;
        }
        Some(self.time(value.fetch_and(!DIRTY, Relaxed)))
    }

    pub fn reset(&self) {
        for value in self.rows.values().flatten() {
            value.store(0, Relaxed);
        }
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
        old: Option<Arc<SegmentMeta>>,
    },
    Recover {
        segment: u64,
        old: Arc<SegmentMeta>,
        end: u64,
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
                let times = recency.rows.entry(segment).or_default();
                if model.segments[&segment].records.slots() == old_slots {
                    times[row].store(0, Relaxed);
                } else {
                    if times.len() == times.capacity() {
                        times.reserve_exact(64);
                    }
                    times.insert(row, AtomicU64::new(0));
                }
            }
            Self::Delete { segment, row, old } => {
                let times = recency.rows.get_mut(&segment).unwrap();
                if let Some(old) = old {
                    let mut rows = 0..old.records.slots();
                    times.retain(|_| {
                        let index = rows.next().unwrap();
                        index != row && old.records.metadata(index).is_some()
                    });
                    times.shrink_to_fit();
                } else {
                    times[row].store(0, Relaxed);
                }
            }
            Self::Recover { segment, old, end } => {
                let times = recency.rows.get_mut(&segment).unwrap();
                times.truncate(old.records.position(end));
                let mut rows = 0..old.records.slots();
                times.retain(|_| old.records.metadata(rows.next().unwrap()).is_some());
                times.shrink_to_fit();
            }
        }
    }
}

impl Catalog {
    pub(super) fn recency_at(&self, slot: usize) -> Option<(&Recency, &AtomicU64)> {
        let (segment, row) = self.row(self.lookup.handle(slot))?;
        let recency = self.recency.as_ref()?;
        Some((recency, recency.rows.get(&segment)?.get(row)?))
    }

    pub(super) fn touch_recency(&self, slot: usize) {
        if let Some((recency, value)) = self.recency_at(slot) {
            recency.touch(value);
        }
    }

    pub(super) fn relocate_recency(&self, old: Address, new_slot: usize) {
        if let Some((recency, value)) = self.recency_at(new_slot) {
            let row = self.segment(old.0).unwrap().records.position(old.1);
            value.store(recency.rows[&old.0][row].load(Relaxed) | DIRTY, Relaxed);
        }
    }

    /// Visit bounded slices of segment rows. The offset remains meaningful if
    /// deletion compacts the row array between slices.
    pub(super) fn visit_recency(
        &self,
        cursor: &mut Address,
        ceiling: u64,
        mut limit: usize,
        mut visit: impl FnMut(usize, u64, usize) -> bool,
    ) -> bool {
        if cursor.0 > ceiling {
            return true;
        }
        for (&id, segment) in self.model.segments.range(cursor.0..=ceiling) {
            let records = &segment.records;
            let start = if id == cursor.0 {
                records.position(cursor.1)
            } else {
                0
            };
            for row in start..records.slots() {
                if limit == 0 {
                    return false;
                }
                limit -= 1;
                *cursor = (id, records.offset_at(row) + 1);
                let Some(record) = records.metadata(row) else {
                    continue;
                };
                let handle = self.handle(id, row);
                let Some(slot) = self.lookup.find(record.hash, |found, flags| {
                    found == handle && flags & (VISIBLE | POLICY) == VISIBLE | POLICY
                }) else {
                    continue;
                };
                if !visit(slot, id, row) {
                    return false;
                }
            }
            *cursor = (id + 1, 0);
        }
        true
    }
}
