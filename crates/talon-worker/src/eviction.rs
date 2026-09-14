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

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use talon_core::{BlockId, ObjectId, PageIndex, Version};

/// A single evictable cache unit.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CacheUnit {
    /// A whole block, evicted as one unit.
    Whole(BlockId),
    /// One page of a paged block.
    Page(BlockId, PageIndex),
}

impl CacheUnit {
    fn block(&self) -> &BlockId {
        match self {
            Self::Whole(block) | Self::Page(block, _) => block,
        }
    }

    fn page(&self) -> Option<PageIndex> {
        match self {
            Self::Whole(_) => None,
            Self::Page(_, page) => Some(*page),
        }
    }
}

/// Version-independent identity; offsets and block sizes must remain distinct.
#[derive(PartialEq, Eq, Hash)]
struct LogicalBlock {
    object: ObjectId,
    offset: u64,
    block_size: u32,
}

impl From<&BlockId> for LogicalBlock {
    fn from(block: &BlockId) -> Self {
        Self {
            object: block.object.clone(),
            offset: block.offset,
            block_size: block.block_size,
        }
    }
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
    /// Only live units are indexed. `None` denotes a whole block; storing page
    /// indices avoids duplicating object paths and versions for every page.
    versions: HashMap<LogicalBlock, HashMap<Version, HashSet<Option<PageIndex>>>>,
    total_bytes: u64,
    clock: u64,
}

impl Inner {
    /// All removal paths must update both maps under the same lock.
    fn remove(&mut self, unit: &CacheUnit) -> Option<u64> {
        let entry = self.entries.remove(unit)?;
        let block = unit.block();
        let key = LogicalBlock::from(block);
        let versions = self.versions.get_mut(&key).expect("tracked logical block");
        let units = versions.get_mut(&block.version).expect("tracked version");
        let removed = units.remove(&unit.page());
        debug_assert!(removed, "tracked cache unit missing from version index");
        if units.is_empty() {
            versions.remove(&block.version);
        } else if units.capacity() > 32 && units.len() < units.capacity() / 4 {
            // Release historical page capacity when a block becomes sparse.
            // Leave growth headroom and skip tiny sets to avoid reallocating
            // on every removal or when residency oscillates near a threshold.
            units.shrink_to(units.len() * 2);
        }
        if versions.is_empty() {
            self.versions.remove(&key);
        }
        Lru::subtract_bytes(&mut self.total_bytes, entry.bytes);
        Some(entry.bytes)
    }
}

impl Lru {
    fn add_bytes(total: &mut u64, bytes: u64) {
        debug_assert!(
            total.checked_add(bytes).is_some(),
            "LRU byte accounting overflow"
        );
        *total = total.saturating_add(bytes);
    }

    fn subtract_bytes(total: &mut u64, bytes: u64) {
        debug_assert!(*total >= bytes, "LRU byte accounting underflow");
        *total = total.saturating_sub(bytes);
    }

    /// Create an empty tracker.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                versions: HashMap::new(),
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
            Self::subtract_bytes(&mut g.total_bytes, old);
            Self::add_bytes(&mut g.total_bytes, bytes);
        } else {
            Self::add_bytes(&mut g.total_bytes, bytes);
            let block = unit.block();
            g.versions
                .entry(LogicalBlock::from(block))
                .or_default()
                .entry(block.version.clone())
                .or_default()
                .insert(unit.page());
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
        self.inner.lock().unwrap().remove(unit)
    }

    /// Evict and return every *superseded* unit — whole block or page — for the
    /// same `(object, offset, block_size)` as `keep` but a different version.
    ///
    /// When an object is overwritten its new ETag yields a new [`BlockId`] and a
    /// new `.blk` file (or `.pages` directory), while the old version's files
    /// would otherwise stay resident forever (issue #159, compounding #119).
    /// Called on commit of a fresh version, this reclaims the stale sibling(s)
    /// immediately. Pinned units (an in-flight reader still serving the old
    /// bytes) are left alone.
    ///
    /// Looks up only this logical block's versions and visits units belonging
    /// to other versions. With only `keep` resident, no cache units are scanned,
    /// regardless of the number of pages in this block or the rest of the cache.
    pub fn evict_superseded(&self, keep: &BlockId) -> Vec<CacheUnit> {
        let mut g = self.inner.lock().unwrap();
        let Some(versions) = g.versions.get(&LogicalBlock::from(keep)) else {
            return Vec::new();
        };
        if versions.len() == 1 && versions.contains_key(&keep.version) {
            return Vec::new();
        }
        let victims: Vec<CacheUnit> = versions
            .iter()
            .filter(|(version, _)| *version != &keep.version)
            .flat_map(|(version, units)| {
                units.iter().map(move |page| {
                    let block = BlockId::new(
                        keep.object.clone(),
                        keep.offset,
                        keep.block_size,
                        version.clone(),
                    );
                    match page {
                        None => CacheUnit::Whole(block),
                        Some(page) => CacheUnit::Page(block, *page),
                    }
                })
            })
            .filter(|unit| g.entries.get(unit).expect("indexed cache unit").pins == 0)
            .collect();
        for unit in &victims {
            g.remove(unit);
        }
        victims
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
                    g.remove(&unit);
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
        lru.insert(whole(1), 50); // shrink same unit
        assert_eq!(lru.total_bytes(), 50);
        assert_eq!(lru.len(), 1);

        assert_eq!(lru.remove(&whole(1)), Some(50));
        assert_eq!(lru.total_bytes(), 0);
        assert_eq!(lru.remove(&whole(1)), None);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "LRU byte accounting underflow")]
    fn accounting_underflow_is_detected_in_debug_builds() {
        Lru::subtract_bytes(&mut 0, 1);
    }

    #[cfg(not(debug_assertions))]
    #[test]
    fn accounting_underflow_saturates_in_release_builds() {
        let mut total = 0;
        Lru::subtract_bytes(&mut total, 1);
        assert_eq!(total, 0);
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

    #[test]
    fn evict_superseded_reclaims_only_other_versions() {
        // Two versions of the same (object, offset), a different offset of the
        // same object, and a pinned old version.
        let obj = ObjectId::new(Backend::S3, "b", "same");
        let v1 = CacheUnit::Whole(BlockId::new(obj.clone(), 0, 256 << 20, Version::new("v1")));
        let v2 = CacheUnit::Whole(BlockId::new(obj.clone(), 0, 256 << 20, Version::new("v2")));
        let other_offset = CacheUnit::Whole(BlockId::new(
            obj.clone(),
            256 << 20,
            256 << 20,
            Version::new("v1"),
        ));
        let other_obj = whole(42);
        let lru = Lru::new();
        for (u, b) in [
            (&v1, 100),
            (&v2, 100),
            (&other_offset, 100),
            (&other_obj, 100),
        ] {
            lru.insert(u.clone(), b);
        }

        let CacheUnit::Whole(keep) = v2.clone() else {
            unreachable!()
        };
        let evicted = lru.evict_superseded(&keep);
        // Only v1 (same object+offset+block_size, different version) is reclaimed.
        assert_eq!(evicted, vec![v1.clone()]);
        assert_eq!(lru.total_bytes(), 300);
        assert!(lru.remove(&v2).is_some());
        assert!(lru.remove(&other_offset).is_some());
        assert!(lru.remove(&other_obj).is_some());
        assert!(lru.remove(&v1).is_none());
    }

    #[test]
    fn evict_superseded_skips_pinned_old_version() {
        let obj = ObjectId::new(Backend::S3, "b", "same");
        let v1 = CacheUnit::Whole(BlockId::new(obj.clone(), 0, 256 << 20, Version::new("v1")));
        let v2 = CacheUnit::Whole(BlockId::new(obj.clone(), 0, 256 << 20, Version::new("v2")));
        let lru = Lru::new();
        lru.insert(v1.clone(), 100);
        lru.insert(v2.clone(), 100);
        // An in-flight reader still serving the old bytes pins v1.
        assert!(lru.pin(&v1));

        let CacheUnit::Whole(keep) = v2.clone() else {
            unreachable!()
        };
        assert!(lru.evict_superseded(&keep).is_empty());
        assert_eq!(lru.total_bytes(), 200);
        lru.unpin(&v1);
        assert_eq!(lru.evict_superseded(&keep), vec![v1]);
        assert_eq!(lru.total_bytes(), 100);
    }

    #[test]
    fn superseded_pages_and_whole_blocks_preserve_pins_and_current_pages() {
        let lru = Lru::new();
        let old = blk(1);
        let mut keep = old.clone();
        keep.version = Version::new("v2");
        let mut older = old.clone();
        older.version = Version::new("v0");
        let old_page = CacheUnit::Page(old.clone(), PageIndex(0));
        let pinned_page = CacheUnit::Page(old.clone(), PageIndex(1));
        let old_whole = CacheUnit::Whole(old);
        let older_page = CacheUnit::Page(older, PageIndex(0));
        let current_page = CacheUnit::Page(keep.clone(), PageIndex(0));
        for unit in [
            &old_page,
            &pinned_page,
            &old_whole,
            &older_page,
            &current_page,
        ] {
            lru.insert(unit.clone(), 10);
        }
        assert!(lru.pin(&pinned_page));
        assert!(lru.pin(&pinned_page));
        // Updating a page must neither duplicate its index entry nor reset pins.
        lru.insert(pinned_page.clone(), 20);
        lru.unpin(&pinned_page);

        let victims: HashSet<_> = lru.evict_superseded(&keep).into_iter().collect();
        assert_eq!(victims, HashSet::from([old_page, old_whole, older_page]));
        assert_eq!(lru.total_bytes(), 30);
        assert_eq!(lru.len(), 2);
        assert!(lru.evict_superseded(&keep).is_empty());

        lru.unpin(&pinned_page);
        assert_eq!(lru.evict_superseded(&keep), vec![pinned_page]);
        assert_eq!(lru.total_bytes(), 10);
        assert_eq!(lru.remove(&current_page), Some(10));
        assert!(lru.inner.lock().unwrap().versions.is_empty());
    }

    #[test]
    fn superseded_lookup_respects_all_logical_block_fields() {
        let lru = Lru::new();
        let old = blk(1);
        let mut keep = old.clone();
        keep.version = Version::new("v2");
        let mut different_blocks = vec![old.clone(); 5];
        different_blocks[0].object.backend = Backend::Gcs;
        different_blocks[1].object.bucket = "other-bucket".into();
        different_blocks[2].object.object_path = "other-path".into();
        different_blocks[3].offset += u64::from(old.block_size);
        different_blocks[4].block_size /= 2;
        let unrelated: Vec<_> = different_blocks
            .into_iter()
            .map(|block| CacheUnit::Page(block, PageIndex(0)))
            .collect();
        let victim = CacheUnit::Page(old, PageIndex(0));
        lru.insert(victim.clone(), 10);
        for unit in &unrelated {
            lru.insert(unit.clone(), 10);
        }
        // `keep` need not already be tracked to reclaim its other versions.
        assert_eq!(lru.evict_superseded(&keep), vec![victim]);
        assert!(lru.evict_superseded(&keep).is_empty());
        assert_eq!(lru.total_bytes(), 50);
        for unit in unrelated {
            assert_eq!(lru.remove(&unit), Some(10));
        }
        assert!(lru.inner.lock().unwrap().versions.is_empty());
    }

    #[test]
    fn version_index_tracks_explicit_and_capacity_removal_and_reinsertion() {
        let lru = Lru::new();
        let old = blk(1);
        let mut keep = old.clone();
        keep.version = Version::new("v2");
        let page0 = CacheUnit::Page(old.clone(), PageIndex(0));
        let page1 = CacheUnit::Page(old, PageIndex(1));
        let current = CacheUnit::Whole(keep.clone());
        lru.insert(page0.clone(), 10);
        lru.insert(page1.clone(), 20);
        lru.insert(current.clone(), 30);

        assert_eq!(lru.remove(&page0), Some(10));
        assert_eq!(lru.remove(&page0), None);
        assert_eq!(lru.evict_to_fit(30), vec![page1.clone()]);
        assert!(lru.evict_superseded(&keep).is_empty());
        {
            let g = lru.inner.lock().unwrap();
            let versions = &g.versions[&LogicalBlock::from(&keep)];
            assert_eq!(versions.len(), 1);
            assert_eq!(versions[&keep.version], HashSet::from([None]));
        }
        // The same identities can be admitted again after either removal path.
        lru.insert(page0.clone(), 40);
        lru.insert(page1.clone(), 50);
        let victims: HashSet<_> = lru.evict_superseded(&keep).into_iter().collect();
        assert_eq!(victims, HashSet::from([page0, page1]));
        assert_eq!(lru.total_bytes(), 30);
        assert_eq!(lru.evict_to_fit(0), vec![current.clone()]);
        assert!(lru.inner.lock().unwrap().versions.is_empty());
        lru.insert(current.clone(), 60);
        assert!(lru.evict_superseded(&keep).is_empty());
        assert_eq!(lru.remove(&current), Some(60));
        assert_eq!(lru.total_bytes(), 0);
        assert!(lru.inner.lock().unwrap().versions.is_empty());
    }

    #[test]
    fn sparse_version_index_releases_capacity_after_page_eviction() {
        let lru = Lru::new();
        let page_bytes = 64 << 10;
        // Fill and drain each block while earlier blocks retain a pinned hot
        // page. The index must follow residency rather than each block's peak.
        for n in 0..8 {
            let block = blk(n);
            let hot = CacheUnit::Page(block.clone(), PageIndex(0));
            for page in 0..512 {
                lru.insert(CacheUnit::Page(block.clone(), PageIndex(page)), page_bytes);
            }
            assert!(lru.pin(&hot));
            assert_eq!(lru.evict_to_fit((n + 1) * page_bytes).len(), 511,);
        }
        assert_eq!(lru.len(), 8);
        assert_eq!(lru.total_bytes(), 8 * page_bytes);
        let g = lru.inner.lock().unwrap();
        for versions in g.versions.values() {
            let pages = &versions[&Version::new("v1")];
            assert_eq!(pages, &HashSet::from([Some(PageIndex(0))]));
            assert!(pages.capacity() <= 64, "sparse set retains peak capacity");
        }
    }

    #[test]
    fn sparse_version_index_preserves_pins_and_regrows_after_shrinking() {
        let lru = Lru::new();
        let old = blk(1);
        let mut keep = old.clone();
        keep.version = Version::new("v2");
        let hot = CacheUnit::Page(old.clone(), PageIndex(0));
        let current = CacheUnit::Whole(keep.clone());
        lru.insert(hot.clone(), 1);
        assert!(lru.pin(&hot));
        lru.insert(current.clone(), 1);

        for explicit in [true, false] {
            for page in 1..512 {
                lru.insert(CacheUnit::Page(old.clone(), PageIndex(page)), 1);
            }
            if explicit {
                for page in 1..512 {
                    assert_eq!(
                        lru.remove(&CacheUnit::Page(old.clone(), PageIndex(page))),
                        Some(1),
                    );
                }
            } else {
                assert_eq!(lru.evict_superseded(&keep).len(), 511);
            }
            assert_eq!(lru.len(), 2);
            assert_eq!(lru.total_bytes(), 2);
            assert!(lru.evict_superseded(&keep).is_empty());
            let g = lru.inner.lock().unwrap();
            let pages = &g.versions[&LogicalBlock::from(&old)][&old.version];
            assert_eq!(pages, &HashSet::from([Some(PageIndex(0))]));
            assert!(pages.capacity() <= 64, "sparse set retains peak capacity");
        }

        lru.unpin(&hot);
        assert_eq!(lru.evict_superseded(&keep), vec![hot]);
        assert_eq!(lru.remove(&current), Some(1));
        assert!(lru.inner.lock().unwrap().versions.is_empty());
    }

    #[test]
    fn filling_current_pages_does_not_reclaim_them() {
        let lru = Lru::new();
        let block = blk(1);
        for page in 0..4096 {
            lru.insert(CacheUnit::Page(block.clone(), PageIndex(page)), 64);
            assert!(lru.evict_superseded(&block).is_empty());
        }
        assert_eq!(lru.len(), 4096);
        assert_eq!(lru.total_bytes(), 4096 * 64);
    }
}
