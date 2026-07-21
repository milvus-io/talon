//! Miss-path in-flight dedup and load admission.
//!
//! When the data-plane ring finds a block (or page) absent, it must return
//! `LOADING` promptly and kick a backend fetch **off** the ring — never block on
//! HTTP. To avoid a thundering herd, concurrent misses for the same target must
//! trigger exactly one load. [`InFlightLoads`] tracks demand loads keyed at two
//! granularities:
//!
//! - whole-block misses keyed by [`BlockId`], and
//! - page-level misses keyed by `(BlockId, PageIndex)` so a paged miss loads
//!   only the touched pages, never the whole 256MB block.
//!
//! [`InFlightLoads::admit`] returns an [`Admission`]: `Started` for the first
//! caller (which submits a [`LoadTask`](crate::LoadTask) to the loader pool) or
//! `AlreadyLoading` for the rest (which just return `LOADING`). When a load
//! completes, [`InFlightLoads::complete`] clears the key so a later refetch can
//! proceed if needed.

use std::collections::HashSet;
use std::sync::Mutex;

use talon_core::{BlockId, PageIndex};

/// A demand-load target: a whole block or a single page of a paged block.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LoadKey {
    /// A whole-block load.
    Whole(BlockId),
    /// A single page of a paged block.
    Page(BlockId, PageIndex),
}

/// Outcome of trying to admit a demand load.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// This caller is the first; it owns submitting the load.
    Started,
    /// A load for this key is already in flight; caller returns `LOADING`.
    AlreadyLoading,
}

/// Tracks demand loads currently in flight to deduplicate concurrent misses.
#[derive(Default)]
pub struct InFlightLoads {
    inner: Mutex<HashSet<LoadKey>>,
}

impl InFlightLoads {
    /// Create an empty tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Try to admit a load for `key`.
    ///
    /// Returns [`Admission::Started`] exactly once per key until the matching
    /// [`complete`](Self::complete); all overlapping callers get
    /// [`Admission::AlreadyLoading`].
    pub fn admit(&self, key: LoadKey) -> Admission {
        let mut g = self.inner.lock().unwrap();
        if g.insert(key) {
            Admission::Started
        } else {
            Admission::AlreadyLoading
        }
    }

    /// Mark a load complete (success or failure), clearing its key.
    ///
    /// Returns `true` if the key was in flight. After this a subsequent miss for
    /// the same key can start a fresh load.
    pub fn complete(&self, key: &LoadKey) -> bool {
        self.inner.lock().unwrap().remove(key)
    }

    /// Whether a load for `key` is currently in flight.
    pub fn is_in_flight(&self, key: &LoadKey) -> bool {
        self.inner.lock().unwrap().contains(key)
    }

    /// Number of loads currently in flight.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    /// Whether no loads are in flight.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }
}

/// Compute the set of page indices a read over `[offset, offset+len)` touches,
/// so a paged miss fetches only those pages rather than the whole block.
///
/// Returns an empty vec for a zero-length read or zero page size.
pub fn touched_pages(offset: u64, len: u64, page_size: u32) -> Vec<PageIndex> {
    if len == 0 || page_size == 0 {
        return Vec::new();
    }
    let ps = page_size as u64;
    let first = offset / ps;
    let last = (offset + len - 1) / ps;
    (first..=last).map(|p| PageIndex(p as u32)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{Backend, ObjectId, Version};

    fn block(n: u64) -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "b", format!("o/{n}")),
            0,
            256 << 20,
            Version::new("v1"),
        )
    }

    #[test]
    fn concurrent_misses_trigger_exactly_one_load() {
        let f = InFlightLoads::new();
        let key = LoadKey::Whole(block(1));
        assert_eq!(f.admit(key.clone()), Admission::Started);
        // All subsequent callers see AlreadyLoading.
        for _ in 0..5 {
            assert_eq!(f.admit(key.clone()), Admission::AlreadyLoading);
        }
        assert!(f.is_in_flight(&key));
        assert_eq!(f.len(), 1);
    }

    #[test]
    fn completion_allows_refetch() {
        let f = InFlightLoads::new();
        let key = LoadKey::Whole(block(2));
        assert_eq!(f.admit(key.clone()), Admission::Started);
        assert!(f.complete(&key));
        assert!(!f.is_in_flight(&key));
        // A fresh miss after completion starts a new load.
        assert_eq!(f.admit(key.clone()), Admission::Started);
        // Completing an unknown key is a no-op.
        assert!(!f.complete(&LoadKey::Whole(block(999))));
    }

    #[test]
    fn page_and_whole_keys_are_independent() {
        let f = InFlightLoads::new();
        let b = block(3);
        assert_eq!(f.admit(LoadKey::Whole(b.clone())), Admission::Started);
        // A page load of the same block is a distinct key.
        assert_eq!(
            f.admit(LoadKey::Page(b.clone(), PageIndex(0))),
            Admission::Started
        );
        assert_eq!(
            f.admit(LoadKey::Page(b.clone(), PageIndex(1))),
            Admission::Started
        );
        // Same page again dedups.
        assert_eq!(
            f.admit(LoadKey::Page(b.clone(), PageIndex(0))),
            Admission::AlreadyLoading
        );
        assert_eq!(f.len(), 3);
    }

    #[test]
    fn touched_pages_covers_only_the_range() {
        // page_size 4: read [2, 10) touches bytes in pages 0,1,2.
        assert_eq!(
            touched_pages(2, 8, 4),
            vec![PageIndex(0), PageIndex(1), PageIndex(2)]
        );
        // A read fully inside one page touches just that page.
        assert_eq!(touched_pages(5, 2, 4), vec![PageIndex(1)]);
        // Never the whole block: a 1-byte read is one page.
        assert_eq!(touched_pages(0, 1, 256 * 1024), vec![PageIndex(0)]);
        // Edge cases.
        assert!(touched_pages(0, 0, 4).is_empty());
        assert!(touched_pages(0, 4, 0).is_empty());
    }
}
