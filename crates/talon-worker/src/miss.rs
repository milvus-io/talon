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
//! caller (which performs the backend load) or `AlreadyLoading` for the rest.
//! An `AlreadyLoading` caller can [`InFlightLoads::wait`] for the leader's load
//! to finish and then serve from the now-warm cache, so N concurrent misses for
//! the same block trigger exactly **one** backend fetch. When a load completes,
//! [`InFlightLoads::complete`] clears the key and wakes all waiters.
//!
//! Prefer [`InFlightLoads::admit_owned`], which hands the leader an
//! [`InFlightGuard`] that clears the marker on drop — so a cancelled or
//! panicking leader can never orphan the key and hang the waiters (#162).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

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
    /// This caller is the first; it owns performing the load.
    Started,
    /// A load for this key is already in flight; the caller can [`wait`] for it.
    ///
    /// [`wait`]: InFlightLoads::wait
    AlreadyLoading,
}

/// Tracks demand loads currently in flight to deduplicate concurrent misses.
///
/// Each in-flight key carries a [`Notify`] the leader signals on completion, so
/// followers can await the leader instead of each issuing a redundant fetch.
#[derive(Default)]
pub struct InFlightLoads {
    inner: Mutex<HashMap<LoadKey, Arc<Notify>>>,
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
        use std::collections::hash_map::Entry;
        let mut g = self.inner.lock().unwrap();
        match g.entry(key) {
            Entry::Occupied(_) => Admission::AlreadyLoading,
            Entry::Vacant(slot) => {
                slot.insert(Arc::new(Notify::new()));
                Admission::Started
            }
        }
    }

    /// Like [`admit`](Self::admit) but, when this caller becomes the leader,
    /// returns an RAII [`InFlightGuard`] that calls [`complete`](Self::complete)
    /// on drop.
    ///
    /// This is cancellation- and panic-safe: if the leader's fetch task is
    /// dropped (client disconnect) or panics, the guard's `Drop` still clears
    /// the in-flight marker and wakes waiters, so a follower's
    /// [`wait`](Self::wait) can never hang forever on an orphaned key (issue
    /// #162). `None` means a load is already in flight (the caller should
    /// [`wait`](Self::wait)).
    pub fn admit_owned(self: &Arc<Self>, key: LoadKey) -> Option<InFlightGuard> {
        use std::collections::hash_map::Entry;
        let mut g = self.inner.lock().unwrap();
        match g.entry(key.clone()) {
            Entry::Occupied(_) => None,
            Entry::Vacant(slot) => {
                slot.insert(Arc::new(Notify::new()));
                Some(InFlightGuard {
                    inflight: Arc::clone(self),
                    key: Some(key),
                })
            }
        }
    }

    /// Wait until the in-flight load for `key` completes.
    ///
    /// Returns immediately if no load is in flight (it already finished). Uses
    /// the register-then-recheck pattern so a `complete` racing between the
    /// lookup and the await cannot cause a missed wakeup.
    pub async fn wait(&self, key: &LoadKey) {
        loop {
            let notify = {
                let g = self.inner.lock().unwrap();
                match g.get(key) {
                    Some(n) => Arc::clone(n),
                    None => return, // load already completed
                }
            };
            let notified = notify.notified();
            tokio::pin!(notified);
            // Register interest before re-checking, so a completion after this
            // point is guaranteed to wake us.
            notified.as_mut().enable();
            // Re-check: if the leader completed between the lookup and enable(),
            // the key is gone and we must not wait for a signal that won't come.
            if !self.is_in_flight(key) {
                return;
            }
            notified.await;
            // Woken; loop to confirm the key is actually gone (guards spurious
            // wakeups and re-admitted keys).
        }
    }

    /// Mark a load complete (success or failure), clearing its key and waking
    /// any waiters.
    ///
    /// Returns `true` if the key was in flight. After this a subsequent miss for
    /// the same key can start a fresh load.
    pub fn complete(&self, key: &LoadKey) -> bool {
        let notify = self.inner.lock().unwrap().remove(key);
        match notify {
            Some(n) => {
                n.notify_waiters();
                true
            }
            None => false,
        }
    }

    /// Whether a load for `key` is currently in flight.
    pub fn is_in_flight(&self, key: &LoadKey) -> bool {
        self.inner.lock().unwrap().contains_key(key)
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

/// RAII marker for an in-flight load owned by the leader caller.
///
/// Dropping the guard calls [`InFlightLoads::complete`], clearing the key and
/// waking any waiters — even if the owning task is cancelled or panics — so an
/// interrupted fetch can never orphan the key and hang future readers (#162).
pub struct InFlightGuard {
    inflight: Arc<InFlightLoads>,
    key: Option<LoadKey>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.inflight.complete(&key);
        }
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

    #[tokio::test]
    async fn wait_returns_immediately_when_not_in_flight() {
        let f = InFlightLoads::new();
        // No load in flight -> wait returns at once.
        f.wait(&LoadKey::Whole(block(10))).await;
    }

    #[tokio::test]
    async fn waiters_wake_on_completion() {
        use std::sync::Arc;
        let f = Arc::new(InFlightLoads::new());
        let key = LoadKey::Whole(block(11));
        assert_eq!(f.admit(key.clone()), Admission::Started);

        // Spawn several followers that wait for the leader.
        let mut waiters = Vec::new();
        for _ in 0..4 {
            let f = Arc::clone(&f);
            let key = key.clone();
            waiters.push(tokio::spawn(async move {
                f.wait(&key).await;
                // After waking, the key must be gone.
                assert!(!f.is_in_flight(&key));
            }));
        }

        // Give the waiters a moment to register interest, then complete.
        tokio::task::yield_now().await;
        assert!(f.complete(&key));
        for w in waiters {
            w.await.unwrap();
        }
    }

    #[tokio::test]
    async fn guard_drop_clears_marker_and_wakes_waiters() {
        // The RAII guard must clear the in-flight key and wake waiters when it is
        // dropped WITHOUT an explicit complete() -- e.g. the leader task was
        // cancelled or panicked mid-fetch (issue #162). Otherwise waiters hang
        // forever on an orphaned key.
        use std::sync::Arc;
        let f = Arc::new(InFlightLoads::new());
        let key = LoadKey::Whole(block(12));
        let guard = f
            .admit_owned(key.clone())
            .expect("first caller is the leader");
        // A second caller is not the leader.
        assert!(f.admit_owned(key.clone()).is_none());
        assert!(f.is_in_flight(&key));

        let waiter = {
            let f = Arc::clone(&f);
            let key = key.clone();
            tokio::spawn(async move {
                f.wait(&key).await;
                assert!(!f.is_in_flight(&key));
            })
        };
        tokio::task::yield_now().await;

        // Simulate the leader being dropped (cancellation/panic) with no explicit
        // complete(): the guard's Drop must release the marker and wake waiters.
        drop(guard);
        assert!(!f.is_in_flight(&key));
        waiter.await.unwrap();

        // After release a fresh caller can become the leader again.
        assert!(f.admit_owned(key).is_some());
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
