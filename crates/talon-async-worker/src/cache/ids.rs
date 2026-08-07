// SPDX-License-Identifier: Apache-2.0
//! Interning of [`ObjectId`]s to compact integer stream ids.
//!
//! An extent key has to identify an object, and carrying that identity
//! literally is expensive: an [`ObjectId`] holds a backend discriminant plus a
//! bucket `String` plus an object-path `String`. Hashing all of it on every
//! extent lookup, for a map that may hold hundreds of thousands of extents, is
//! waste.
//!
//! [`StreamIds`] allocates a stable `u64` per object on first sight, so an
//! extent key is two integers instead of three heap allocations.
//!
//! # Why the version is *not* part of the identity
//!
//! It used to be. Keying on `(ObjectId, Version)` made a republish allocate a
//! new id, which made every extent of the superseded version unreachable for
//! free — invalidation that could not be missed because there was no
//! invalidation step.
//!
//! That is traded away deliberately (ADR 0005 §3): the objects this worker
//! caches are read-only, so a republish is the rare case rather than the
//! common one, and paying a `Version` clone on every lookup plus a `Version`
//! string per stream in every checkpoint to guard against it is the wrong
//! trade.
//!
//! The consequence is that a republish is **not** self-healing here. Nothing
//! about a same-path overwrite changes the id, so the stale extents stay
//! reachable until something purges them. `AsyncWorkerRuntime::resolve` does
//! that: when the version-TTL refresh observes an ETag it has not seen, it
//! calls [`release_object`](Self::release_object) before serving. Staleness is
//! therefore bounded by the version TTL rather than eliminated.
//!
//! `talon-worker` keeps the version in its `BlockId`, so the two workers differ
//! here on purpose. See ADR 0005 §3.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use talon_core::ObjectId;

/// A registry mapping objects to stable numeric ids.
///
/// Ids are never reused within a process. Releasing an object drops its map
/// entry, but the id itself is retired rather than recycled — a stale
/// [`ExtentKey`] holding a released id can then never alias a live one.
///
/// [`ExtentKey`]: super::ExtentKey
pub struct StreamIds {
    map: Mutex<HashMap<ObjectId, u64>>,
    next_id: AtomicU64,
}

impl std::fmt::Debug for StreamIds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamIds")
            .field("live", &self.len())
            .field("allocated", &self.next_id.load(Ordering::Relaxed))
            .finish()
    }
}

impl StreamIds {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
        }
    }

    /// Return the stable id for `object`, allocating on first sight.
    pub fn get_or_intern(&self, object: &ObjectId) -> u64 {
        let mut map = self.map.lock().unwrap();
        // Borrow-first so the common (already-interned) path does not clone the
        // bucket or object path.
        if let Some(&id) = map.get(object) {
            return id;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        map.insert(object.clone(), id);
        id
    }

    /// Look up an existing id without allocating one.
    pub fn peek(&self, object: &ObjectId) -> Option<u64> {
        self.map.lock().unwrap().get(object).copied()
    }

    /// Drop `object`'s interned id, returning it if it was interned.
    ///
    /// The caller purges the returned id's extents. Called on an outright
    /// delete, and on a republish detected at version-TTL refresh — which is
    /// what bounds staleness now that the version is not in the key.
    pub fn release_object(&self, object: &ObjectId) -> Vec<u64> {
        let mut map = self.map.lock().unwrap();
        map.remove(object).into_iter().collect()
    }

    /// Number of live interned objects.
    pub fn len(&self) -> usize {
        self.map.lock().unwrap().len()
    }

    /// Whether nothing is interned.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for StreamIds {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::Backend;

    fn object(path: &str) -> ObjectId {
        ObjectId::new(Backend::S3, "bucket", path)
    }

    #[test]
    fn interning_is_stable_and_unique() {
        let ids = StreamIds::new();
        let a = ids.get_or_intern(&object("a.parquet"));
        let b = ids.get_or_intern(&object("b.parquet"));

        assert_eq!(a, ids.get_or_intern(&object("a.parquet")));
        assert_ne!(a, b, "distinct objects must not share an id");
        assert_eq!(ids.len(), 2);
    }

    /// The deliberate reversal: an overwrite at the same path resolves to the
    /// *same* id, so the cached extents stay reachable.
    ///
    /// This is what makes a republish non-self-healing, and it is why
    /// `AsyncWorkerRuntime::resolve` must purge on a version change. If this
    /// test ever flips back to `assert_ne!`, that purge is dead code and the
    /// checkpoint format is carrying a `Version` again.
    #[test]
    fn a_republish_reuses_the_objects_id() {
        let ids = StreamIds::new();
        let first = ids.get_or_intern(&object("a.parquet"));
        let after_republish = ids.get_or_intern(&object("a.parquet"));
        assert_eq!(first, after_republish);
        assert_eq!(ids.len(), 1, "one object, one id, regardless of version");
    }

    #[test]
    fn released_ids_are_retired_not_recycled() {
        // An extent key holding a released id must never alias a later object.
        let ids = StreamIds::new();
        let released = ids.get_or_intern(&object("a.parquet"));
        ids.release_object(&object("a.parquet"));
        let fresh = ids.get_or_intern(&object("c.parquet"));
        assert_ne!(released, fresh);
    }

    #[test]
    fn release_object_drops_only_that_object() {
        let ids = StreamIds::new();
        let a = ids.get_or_intern(&object("a.parquet"));
        let b = ids.get_or_intern(&object("b.parquet"));

        assert_eq!(ids.release_object(&object("a.parquet")), vec![a]);
        assert_eq!(ids.len(), 1);
        assert!(ids.peek(&object("a.parquet")).is_none());
        assert_eq!(ids.peek(&object("b.parquet")), Some(b));
    }

    #[test]
    fn releasing_an_unknown_object_is_a_no_op() {
        // The purge path fires on every observed version change, including for
        // objects this worker has never cached.
        let ids = StreamIds::new();
        assert!(ids.release_object(&object("never-seen.parquet")).is_empty());
        assert!(ids.is_empty());
    }

    #[test]
    fn concurrent_interning_allocates_one_id_per_object() {
        use std::sync::Arc;
        let ids = Arc::new(StreamIds::new());
        let mut handles = Vec::new();
        for _ in 0..8 {
            let ids = Arc::clone(&ids);
            handles.push(std::thread::spawn(move || {
                for n in 0..100u64 {
                    ids.get_or_intern(&object(&format!("o/{n}")));
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(ids.len(), 100, "8 threads racing must not double-allocate");
    }
}
