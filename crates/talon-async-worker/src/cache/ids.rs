// SPDX-License-Identifier: Apache-2.0
//! Interning of `(ObjectId, Version)` pairs to compact integer stream ids.
//!
//! An extent key has to identify an object *version* — Talon's cache coherence
//! rests on the origin ETag being part of the key, so that republishing an
//! object at the same path yields a different key and stale bytes are
//! unreachable without any invalidation protocol.
//!
//! Carrying that identity literally is expensive: an [`ObjectId`] holds a
//! backend discriminant plus a bucket `String` plus an object-path `String`,
//! and a [`Version`] is another `String`. Hashing all of it on every extent
//! lookup, for a map that may hold hundreds of thousands of extents, is waste.
//!
//! [`StreamIds`] allocates a stable `u64` per `(object, version)` pair on first
//! sight. A new ETag allocates a *new* id, so every extent of the superseded
//! version becomes unreachable — which is exactly the invalidation behaviour
//! the full key gave us, at 8 bytes instead of three heap allocations.
//!
//! See ADR 0005 §3.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use talon_core::{ObjectId, Version};

/// A registry mapping `(object, version)` pairs to stable numeric ids.
///
/// Ids are never reused within a process. Releasing a superseded version drops
/// its map entry so version churn does not grow the table without bound, but
/// the id itself is retired rather than recycled — a stale [`ExtentKey`]
/// holding a released id can then never alias a live one.
///
/// [`ExtentKey`]: super::ExtentKey
pub struct StreamIds {
    map: Mutex<HashMap<(ObjectId, Version), u64>>,
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

    /// Return the stable id for `(object, version)`, allocating on first sight.
    pub fn get_or_intern(&self, object: &ObjectId, version: &Version) -> u64 {
        let mut map = self.map.lock().unwrap();
        // Borrow-first so the common (already-interned) path does not clone the
        // object path or the etag.
        if let Some(&id) = map.get(&(object.clone(), version.clone())) {
            return id;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        map.insert((object.clone(), version.clone()), id);
        id
    }

    /// Look up an existing id without allocating one.
    pub fn peek(&self, object: &ObjectId, version: &Version) -> Option<u64> {
        self.map
            .lock()
            .unwrap()
            .get(&(object.clone(), version.clone()))
            .copied()
    }

    /// Drop every interned version of `object` except `keep`, returning the
    /// released ids.
    ///
    /// The caller purges the returned ids' extents. Called when a commit
    /// supersedes an older version, so the table tracks live versions rather
    /// than every version ever seen.
    pub fn release_superseded(&self, object: &ObjectId, keep: &Version) -> Vec<u64> {
        let mut map = self.map.lock().unwrap();
        let mut released = Vec::new();
        map.retain(|(obj, version), id| {
            let supersede = obj == object && version != keep;
            if supersede {
                released.push(*id);
            }
            !supersede
        });
        released
    }

    /// Drop every interned version of `object`, returning the released ids.
    ///
    /// Used when an object is deleted outright rather than overwritten.
    pub fn release_object(&self, object: &ObjectId) -> Vec<u64> {
        let mut map = self.map.lock().unwrap();
        let mut released = Vec::new();
        map.retain(|(obj, _), id| {
            let drop_it = obj == object;
            if drop_it {
                released.push(*id);
            }
            !drop_it
        });
        released
    }

    /// Number of live interned pairs.
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
        let a = ids.get_or_intern(&object("a.parquet"), &Version::new("v1"));
        let b = ids.get_or_intern(&object("b.parquet"), &Version::new("v1"));

        assert_eq!(
            a,
            ids.get_or_intern(&object("a.parquet"), &Version::new("v1"))
        );
        assert_ne!(a, b, "distinct objects must not share an id");
        assert_eq!(ids.len(), 2);
    }

    #[test]
    fn a_new_version_gets_a_new_id() {
        // This is the invalidation guarantee: an overwrite must not resolve to
        // the previous version's extents.
        let ids = StreamIds::new();
        let v1 = ids.get_or_intern(&object("a.parquet"), &Version::new("v1"));
        let v2 = ids.get_or_intern(&object("a.parquet"), &Version::new("v2"));
        assert_ne!(v1, v2);
        assert_eq!(ids.len(), 2, "both versions interned until one is released");
    }

    #[test]
    fn release_superseded_keeps_only_the_live_version() {
        let ids = StreamIds::new();
        let v1 = ids.get_or_intern(&object("a.parquet"), &Version::new("v1"));
        let v2 = ids.get_or_intern(&object("a.parquet"), &Version::new("v2"));
        let other = ids.get_or_intern(&object("b.parquet"), &Version::new("v1"));

        let released = ids.release_superseded(&object("a.parquet"), &Version::new("v2"));
        assert_eq!(released, vec![v1]);
        assert_eq!(ids.len(), 2, "v2 and the unrelated object survive");
        assert_eq!(
            ids.peek(&object("a.parquet"), &Version::new("v2")),
            Some(v2)
        );
        assert_eq!(
            ids.peek(&object("b.parquet"), &Version::new("v1")),
            Some(other)
        );
        assert!(ids
            .peek(&object("a.parquet"), &Version::new("v1"))
            .is_none());
    }

    #[test]
    fn released_ids_are_retired_not_recycled() {
        // An extent key holding a released id must never alias a later object.
        let ids = StreamIds::new();
        let v1 = ids.get_or_intern(&object("a.parquet"), &Version::new("v1"));
        ids.release_superseded(&object("a.parquet"), &Version::new("v2"));
        let fresh = ids.get_or_intern(&object("c.parquet"), &Version::new("v1"));
        assert_ne!(v1, fresh);
    }

    #[test]
    fn release_object_drops_every_version() {
        let ids = StreamIds::new();
        ids.get_or_intern(&object("a.parquet"), &Version::new("v1"));
        ids.get_or_intern(&object("a.parquet"), &Version::new("v2"));
        ids.get_or_intern(&object("b.parquet"), &Version::new("v1"));

        let released = ids.release_object(&object("a.parquet"));
        assert_eq!(released.len(), 2);
        assert_eq!(ids.len(), 1);
    }

    #[test]
    fn concurrent_interning_allocates_one_id_per_pair() {
        use std::sync::Arc;
        let ids = Arc::new(StreamIds::new());
        let mut handles = Vec::new();
        for _ in 0..8 {
            let ids = Arc::clone(&ids);
            handles.push(std::thread::spawn(move || {
                for n in 0..100u64 {
                    ids.get_or_intern(&object(&format!("o/{n}")), &Version::new("v1"));
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(ids.len(), 100, "8 threads racing must not double-allocate");
    }
}
