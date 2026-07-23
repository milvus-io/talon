//! Client-side placement cache with refresh and replica fallback.
//!
//! The FUSE client caches each block's ordered replica list + epoch for a short
//! TTL, so most reads skip a coordinator round-trip. A cached entry is refreshed
//! (evicted, forcing a re-lookup) on any staleness trigger:
//!
//! - **TTL expiry** — the cached entry aged out.
//! - **Version mismatch** — a response carried a different placement version.
//! - **Wrong owner / NOT_FOUND** — the contacted worker doesn't have the block.
//! - **Connect failure** — the contacted worker is unreachable.
//!
//! On `LOADING` / unavailable, the client walks the ordered replica list via
//! [`Cached::next_replica`] before giving up and refreshing. Time is injected as
//! a monotonic millisecond value for deterministic tests.

use std::collections::HashMap;
use std::sync::RwLock;

use talon_core::BlockId;

/// Why a cached placement entry was (or should be) invalidated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshReason {
    /// The entry's TTL elapsed.
    Expired,
    /// A placement version token different from the cached entry's was observed.
    EpochMismatch,
    /// The contacted worker did not own / have the block.
    WrongOwner,
    /// The contacted worker was unreachable.
    ConnectFailure,
}

/// A cached placement: ordered replicas + the version they were computed at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cached {
    /// Ordered replica node ids (primary first).
    pub replicas: Vec<String>,
    /// Placement version token these replicas were computed against.
    ///
    /// This is an opaque coordinator-issued token (a content hash of the node
    /// set), **not** a monotonically increasing counter. Clients compare it for
    /// equality only — see [`PlacementCache::observe_epoch`].
    pub epoch: u64,
}

impl Cached {
    /// The primary (first) replica, if any.
    pub fn primary(&self) -> Option<&str> {
        self.replicas.first().map(String::as_str)
    }

    /// The replica after `current` in the ordered list, for fallback.
    ///
    /// Returns `None` once the list is exhausted (caller should refresh).
    pub fn next_replica(&self, current: &str) -> Option<&str> {
        let pos = self.replicas.iter().position(|r| r == current)?;
        self.replicas.get(pos + 1).map(String::as_str)
    }
}

struct Entry {
    cached: Cached,
    inserted_ms: u64,
}

/// A short-TTL placement cache keyed by [`BlockId`].
pub struct PlacementCache {
    ttl_ms: u64,
    entries: RwLock<HashMap<BlockId, Entry>>,
}

impl PlacementCache {
    /// Create a cache with the given entry TTL in milliseconds.
    pub fn new(ttl_ms: u64) -> Self {
        Self {
            ttl_ms,
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Insert/replace a fresh placement for `block` observed at `now_ms`.
    pub fn insert(&self, block: BlockId, cached: Cached, now_ms: u64) {
        self.entries.write().unwrap().insert(
            block,
            Entry {
                cached,
                inserted_ms: now_ms,
            },
        );
    }

    /// Look up a non-expired placement for `block` as of `now_ms`.
    ///
    /// An expired entry is treated as a miss (and lazily dropped).
    pub fn get(&self, block: &BlockId, now_ms: u64) -> Option<Cached> {
        // Fast path: shared lock.
        {
            let g = self.entries.read().unwrap();
            if let Some(e) = g.get(block) {
                if now_ms.saturating_sub(e.inserted_ms) <= self.ttl_ms {
                    return Some(e.cached.clone());
                }
            } else {
                return None;
            }
        }
        // Slow path: expired -> drop under a write lock.
        self.entries.write().unwrap().remove(block);
        None
    }

    /// Invalidate a block's cached placement for the given reason.
    ///
    /// Returns `true` if an entry was present and removed.
    pub fn invalidate(&self, block: &BlockId, _reason: RefreshReason) -> bool {
        self.entries.write().unwrap().remove(block).is_some()
    }

    /// Reconcile against an observed placement version: if the cached entry's
    /// version differs from `observed_epoch`, drop it so the next access
    /// re-looks-up.
    ///
    /// The version is an opaque token (a content hash of the coordinator's node
    /// set), not an ordered counter, so this compares for **inequality** rather
    /// than `<`. Any difference — a membership change on one coordinator, or a
    /// response served by a peer that observed a different set — invalidates the
    /// entry. Because identical membership always hashes to the identical token
    /// (issue #80), a client load-balanced across active-active coordinators
    /// with a stable cluster sees no spurious invalidations.
    ///
    /// Returns `true` if the entry was invalidated by the version check.
    pub fn observe_epoch(&self, block: &BlockId, observed_epoch: u64) -> bool {
        let mut g = self.entries.write().unwrap();
        if let Some(e) = g.get(block) {
            if e.cached.epoch != observed_epoch {
                g.remove(block);
                return true;
            }
        }
        false
    }

    /// Number of entries currently held (including not-yet-collected expired).
    pub fn len(&self) -> usize {
        self.entries.read().unwrap().len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.read().unwrap().is_empty()
    }
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

    fn cached(epoch: u64, replicas: &[&str]) -> Cached {
        Cached {
            replicas: replicas.iter().map(|s| s.to_string()).collect(),
            epoch,
        }
    }

    #[test]
    fn hit_within_ttl_miss_after() {
        let c = PlacementCache::new(1000);
        c.insert(block(1), cached(1, &["a", "b"]), 0);
        assert_eq!(c.get(&block(1), 500).unwrap().primary(), Some("a"));
        // Past TTL -> miss, and the stale entry is dropped.
        assert!(c.get(&block(1), 1001).is_none());
        assert!(c.is_empty());
    }

    #[test]
    fn replica_fallback_walks_ordered_list() {
        let cc = cached(1, &["a", "b", "c"]);
        assert_eq!(cc.primary(), Some("a"));
        assert_eq!(cc.next_replica("a"), Some("b"));
        assert_eq!(cc.next_replica("b"), Some("c"));
        assert_eq!(cc.next_replica("c"), None); // exhausted -> refresh
        assert_eq!(cc.next_replica("unknown"), None);
    }

    #[test]
    fn invalidate_on_triggers() {
        let c = PlacementCache::new(1000);
        for reason in [
            RefreshReason::WrongOwner,
            RefreshReason::ConnectFailure,
            RefreshReason::Expired,
            RefreshReason::EpochMismatch,
        ] {
            c.insert(block(2), cached(1, &["a"]), 0);
            assert!(c.invalidate(&block(2), reason));
            assert!(c.get(&block(2), 0).is_none());
            // Second invalidate is a no-op.
            assert!(!c.invalidate(&block(2), reason));
        }
    }

    #[test]
    fn version_mismatch_self_heals() {
        let c = PlacementCache::new(10_000);
        c.insert(block(3), cached(5, &["a"]), 0);
        // Identical version token does not invalidate.
        assert!(!c.observe_epoch(&block(3), 5));
        assert!(c.get(&block(3), 0).is_some());
        // Any different token drops the stale entry — the token is a content
        // hash, not an ordered counter, so "different" (not "greater") is the
        // trigger.
        assert!(c.observe_epoch(&block(3), 6));
        assert!(c.get(&block(3), 0).is_none());
    }

    #[test]
    fn membership_change_invalidates_regardless_of_token_order() {
        // Regression for issue #80: the version is a content hash, so a newer
        // membership can hash to a *numerically smaller* token. Invalidation
        // must still fire on any difference, not only on an increase.
        let c = PlacementCache::new(10_000);
        c.insert(block(7), cached(9_000, &["w3"]), 0);
        // A different membership hashes to a smaller value here.
        let different_but_smaller = 42u64;
        assert!(different_but_smaller < 9_000);
        assert!(c.observe_epoch(&block(7), different_but_smaller));
        assert!(c.get(&block(7), 0).is_none());
    }

    #[test]
    fn stable_membership_across_coordinators_does_not_thrash() {
        // A client load-balanced between two active-active coordinators that
        // observe the *same* membership sees the *same* token, so its cache is
        // never spuriously invalidated (issue #80).
        let c = PlacementCache::new(10_000);
        let token = 0xABCD_1234u64; // both coordinators compute this same hash.
        c.insert(block(4), cached(token, &["a", "b"]), 0);
        // Repeated observations from either coordinator, same token -> no drop.
        assert!(!c.observe_epoch(&block(4), token));
        assert!(!c.observe_epoch(&block(4), token));
        assert!(c.get(&block(4), 0).is_some());
    }
}
