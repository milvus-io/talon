//! Byte-accounted LRU eviction policy.
//!
//! Tracks cache *units* — a whole block, or a single `(block, page)` for paged
//! blocks — in least-recently-used order, keyed by their byte cost rather than
//! by count. When the tracked total exceeds capacity, [`Lru::evict_to_fit`]
//! returns the coldest units to reclaim, skipping any unit currently *pinned*
//! by an in-flight reader (so a `sendfile` in progress is never evicted).
//!
//! This module is policy only: it decides *what* to evict and maintains byte
//! accounting. Unlinking files and updating the [`BlockIndex`](crate::BlockIndex)
//! is done by the caller with the returned unit list. Segmented-LRU / TinyLFU
//! are deferred per DESIGN.md.

use std::collections::HashMap;
use std::sync::Mutex;

use talon_core::{BlockId, PageIndex};

/// A single evictable cache unit.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CacheUnit {
    /// A whole block, evicted as one unit.
    Whole(BlockId),
    /// One page of a paged block.
    Page(BlockId, PageIndex),
}

/// Internal per-unit bookkeeping.
struct Entry {
    bytes: u64,
    /// Monotonic tick of last access; higher = more recently used.
    last_used: u64,
    /// Active readers; a unit with `pins > 0` is never evicted.
    pins: u32,
}

/// A byte-accounted LRU tracker with reader pinning.
pub struct Lru {
    inner: Mutex<Inner>,
}

struct Inner {
    entries: HashMap<CacheUnit, Entry>,
    total_bytes: u64,
    clock: u64,
}

impl Lru {
    /// Create an empty tracker.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                total_bytes: 0,
                clock: 0,
            }),
        }
    }

    /// Total bytes currently tracked.
    pub fn total_bytes(&self) -> u64 {
        self.inner.lock().unwrap().total_bytes
    }

    /// Number of units currently tracked.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().entries.len()
    }

    /// Whether the tracker holds no units.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().entries.is_empty()
    }

    /// Insert or update a unit with its byte cost, marking it most-recently-used.
    pub fn insert(&self, unit: CacheUnit, bytes: u64) {
        let mut g = self.inner.lock().unwrap();
        g.clock += 1;
        let tick = g.clock;
        if let Some(e) = g.entries.get_mut(&unit) {
            let old = e.bytes;
            e.bytes = bytes;
            e.last_used = tick;
            g.total_bytes = g.total_bytes - old + bytes;
        } else {
            g.total_bytes += bytes;
            g.entries.insert(
                unit,
                Entry {
                    bytes,
                    last_used: tick,
                    pins: 0,
                },
            );
        }
    }

    /// Record an access, moving the unit to most-recently-used. No-op if absent.
    pub fn touch(&self, unit: &CacheUnit) {
        let mut g = self.inner.lock().unwrap();
        g.clock += 1;
        let tick = g.clock;
        if let Some(e) = g.entries.get_mut(unit) {
            e.last_used = tick;
        }
    }

    /// Pin a unit so it cannot be evicted while an active reader holds it.
    ///
    /// Returns `true` if the unit exists.
    pub fn pin(&self, unit: &CacheUnit) -> bool {
        let mut g = self.inner.lock().unwrap();
        match g.entries.get_mut(unit) {
            Some(e) => {
                e.pins += 1;
                true
            }
            None => false,
        }
    }

    /// Release one pin previously taken with [`pin`](Self::pin).
    pub fn unpin(&self, unit: &CacheUnit) {
        let mut g = self.inner.lock().unwrap();
        if let Some(e) = g.entries.get_mut(unit) {
            e.pins = e.pins.saturating_sub(1);
        }
    }

    /// Remove a unit outright (e.g. explicit delete), returning its byte cost.
    pub fn remove(&self, unit: &CacheUnit) -> Option<u64> {
        let mut g = self.inner.lock().unwrap();
        let e = g.entries.remove(unit)?;
        g.total_bytes -= e.bytes;
        Some(e.bytes)
    }

    /// Evict coldest unpinned units until `total_bytes <= capacity`.
    ///
    /// Returns the evicted units (coldest first) so the caller can unlink files
    /// and update the index. Pinned units are skipped; if only pinned units
    /// remain, eviction stops even if still over capacity.
    pub fn evict_to_fit(&self, capacity: u64) -> Vec<CacheUnit> {
        let mut g = self.inner.lock().unwrap();
        let mut evicted = Vec::new();
        while g.total_bytes > capacity {
            // Find the coldest unpinned unit.
            let victim = g
                .entries
                .iter()
                .filter(|(_, e)| e.pins == 0)
                .min_by_key(|(_, e)| e.last_used)
                .map(|(u, _)| u.clone());
            match victim {
                Some(unit) => {
                    if let Some(e) = g.entries.remove(&unit) {
                        g.total_bytes -= e.bytes;
                    }
                    evicted.push(unit);
                }
                None => break, // everything left is pinned
            }
        }
        evicted
    }
}

impl Default for Lru {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{Backend, ObjectId, Version};

    fn blk(n: u64) -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "b", format!("o/{n}")),
            0,
            256 << 20,
            Version::new("v1"),
        )
    }

    fn whole(n: u64) -> CacheUnit {
        CacheUnit::Whole(blk(n))
    }

    #[test]
    fn coldest_bytes_evicted_first() {
        let lru = Lru::new();
        lru.insert(whole(1), 100);
        lru.insert(whole(2), 100);
        lru.insert(whole(3), 100);
        assert_eq!(lru.total_bytes(), 300);

        // Touch 1 so 2 becomes the coldest.
        lru.touch(&whole(1));

        let evicted = lru.evict_to_fit(150);
        // Need to drop 150 bytes -> evict two coldest: 2 then 3.
        assert_eq!(evicted, vec![whole(2), whole(3)]);
        assert_eq!(lru.total_bytes(), 100);
        assert_eq!(lru.len(), 1);
    }

    #[test]
    fn pinned_units_are_not_evicted() {
        let lru = Lru::new();
        lru.insert(whole(1), 100);
        lru.insert(whole(2), 100);
        // Pin the coldest unit (1) — it must survive even under pressure.
        assert!(lru.pin(&whole(1)));

        let evicted = lru.evict_to_fit(0);
        assert_eq!(evicted, vec![whole(2)]);
        assert_eq!(lru.total_bytes(), 100); // pinned unit remains
        assert!(lru.len() == 1);

        // After unpinning, it can be evicted.
        lru.unpin(&whole(1));
        let evicted = lru.evict_to_fit(0);
        assert_eq!(evicted, vec![whole(1)]);
        assert!(lru.is_empty());
    }

    #[test]
    fn accounting_consistent_across_ops() {
        let lru = Lru::new();
        lru.insert(whole(1), 100);
        lru.insert(whole(1), 250); // update same unit
        assert_eq!(lru.total_bytes(), 250);
        assert_eq!(lru.len(), 1);

        assert_eq!(lru.remove(&whole(1)), Some(250));
        assert_eq!(lru.total_bytes(), 0);
        assert_eq!(lru.remove(&whole(1)), None);
    }

    #[test]
    fn page_units_evict_independently() {
        let lru = Lru::new();
        let b = blk(9);
        lru.insert(CacheUnit::Page(b.clone(), PageIndex(0)), 64);
        lru.insert(CacheUnit::Page(b.clone(), PageIndex(1)), 64);
        lru.touch(&CacheUnit::Page(b.clone(), PageIndex(1)));

        let evicted = lru.evict_to_fit(64);
        assert_eq!(evicted, vec![CacheUnit::Page(b.clone(), PageIndex(0))]);
        assert_eq!(lru.total_bytes(), 64);
    }
}
