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
//!
//! # Ids outlive the process
//!
//! An id is meaningless on its own — it names a row in this table — so a
//! checkpointed extent map is unreadable without the table that produced it.
//! [`Recovery`] rebinds the `(id, object)` pairs a checkpoint carries, and
//! pushes the allocator past the highest of them so a freshly interned object
//! can never alias a recovered one. See [`checkpoint`](super::checkpoint).

use std::collections::{HashMap, HashSet};
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

    /// Resolve interned ids back to their objects, for a checkpoint.
    ///
    /// One pass over the table rather than a lookup per id, because the table
    /// is keyed the other way round. A reverse index would make this O(1) but
    /// would double the memory of a structure that exists to *save* memory, for
    /// a caller that runs once per 64 MiB written.
    ///
    /// Ids with no live entry are simply absent from the result; the caller
    /// drops their extents rather than checkpointing keys nothing can name.
    pub fn names_of(&self, ids: &[u64]) -> Vec<(u64, ObjectId)> {
        let wanted: HashSet<u64> = ids.iter().copied().collect();
        self.map
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, id)| wanted.contains(id))
            .map(|(object, id)| (*id, object.clone()))
            .collect()
    }

    /// Bind a recovered id to its object, reserving every id up to it.
    ///
    /// Returns `false` when `object` is already interned under a *different*
    /// id, which means two checkpoints disagree; the caller drops that shard's
    /// entries for the id rather than letting an extent key resolve to the
    /// wrong object.
    ///
    /// Reached only through [`Recovery`], which additionally rejects one id
    /// claimed by two objects.
    fn bind(&self, id: u64, object: &ObjectId) -> bool {
        let mut map = self.map.lock().unwrap();
        match map.get(object) {
            Some(&existing) => return existing == id,
            None => {
                map.insert(object.clone(), id);
            }
        }
        // Ids allocated after recovery must not collide with a recovered one.
        // `fetch_max` rather than a store: several shards recover in sequence
        // and a later one may carry a lower high-water mark.
        self.next_id.fetch_max(id + 1, Ordering::Relaxed);
        true
    }
}

/// Rebinds checkpointed stream ids to their objects across a whole recovery.
///
/// Held for the duration of one [`ExtentStore`] recovery, spanning every shard,
/// because the invariant it enforces is global: **one id names one object**. A
/// per-shard check could not see a second shard claiming the same id, and an id
/// that resolves to two objects is a cross-object read — the one failure this
/// cache must never have. The reverse map exists only for that check and is
/// dropped when recovery ends, so it costs nothing at steady state.
///
/// [`ExtentStore`]: super::store::ExtentStore
pub struct Recovery<'a> {
    ids: &'a StreamIds,
    claimed: HashMap<u64, ObjectId>,
}

impl<'a> Recovery<'a> {
    /// Begin recovering into `ids`.
    pub fn new(ids: &'a StreamIds) -> Self {
        Self {
            ids,
            claimed: HashMap::new(),
        }
    }

    /// Bind a shard's recovered streams, returning the ids that were accepted.
    ///
    /// Entries whose `stream_id` is absent from the returned set must be
    /// discarded. Rejection is not an error: a rejected stream costs a refetch,
    /// while a wrongly accepted one serves another object's bytes.
    pub fn accept(&mut self, streams: &[(u64, ObjectId)]) -> HashSet<u64> {
        let mut accepted = HashSet::with_capacity(streams.len());
        for (id, object) in streams {
            match self.claimed.get(id) {
                // Idempotent: the same pair seen twice is fine, and lets a
                // shard be recovered more than once without special-casing.
                Some(previous) if previous == object => {
                    accepted.insert(*id);
                }
                Some(previous) => {
                    tracing::warn!(
                        id,
                        first = %previous.to_path(),
                        second = %object.to_path(),
                        "checkpoint recovery: one stream id claimed by two objects; dropping"
                    );
                }
                None => {
                    if self.ids.bind(*id, object) {
                        self.claimed.insert(*id, object.clone());
                        accepted.insert(*id);
                    } else {
                        tracing::warn!(
                            id,
                            object = %object.to_path(),
                            "checkpoint recovery: object already interned under another id; dropping"
                        );
                    }
                }
            }
        }
        accepted
    }

    /// Streams bound so far.
    pub fn len(&self) -> usize {
        self.claimed.len()
    }

    /// Whether nothing has been bound.
    pub fn is_empty(&self) -> bool {
        self.claimed.is_empty()
    }
}

impl std::fmt::Debug for Recovery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Recovery")
            .field("bound", &self.claimed.len())
            .finish()
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

    #[test]
    fn recovered_ids_resolve_to_the_object_that_was_checkpointed() {
        let ids = StreamIds::new();
        let streams = vec![(4, object("a.parquet")), (9, object("b.parquet"))];
        let accepted = Recovery::new(&ids).accept(&streams);

        assert_eq!(accepted, [4, 9].into_iter().collect());
        assert_eq!(ids.peek(&object("a.parquet")), Some(4));
        assert_eq!(ids.peek(&object("b.parquet")), Some(9));
    }

    #[test]
    fn interning_after_recovery_never_reissues_a_recovered_id() {
        // The whole point of persisting ids: a fresh allocation that collided
        // with a recovered one would make a new object read the recovered
        // object's extents.
        let ids = StreamIds::new();
        Recovery::new(&ids).accept(&[(41, object("recovered.parquet"))]);

        let fresh = ids.get_or_intern(&object("new.parquet"));
        assert!(
            fresh > 41,
            "allocated {fresh}, which aliases a recovered id"
        );
    }

    #[test]
    fn a_lower_high_water_mark_from_a_later_shard_does_not_lower_the_counter() {
        // Shards recover in sequence and carry unrelated id ranges; the counter
        // has to end up past the highest of them, not the last of them.
        let ids = StreamIds::new();
        let mut recovery = Recovery::new(&ids);
        recovery.accept(&[(500, object("high.parquet"))]);
        recovery.accept(&[(2, object("low.parquet"))]);

        assert!(ids.get_or_intern(&object("new.parquet")) > 500);
    }

    #[test]
    fn recovering_the_same_stream_twice_is_idempotent() {
        let ids = StreamIds::new();
        let mut recovery = Recovery::new(&ids);
        let streams = vec![(4, object("a.parquet"))];

        assert_eq!(recovery.accept(&streams), [4].into_iter().collect());
        assert_eq!(recovery.accept(&streams), [4].into_iter().collect());
        assert_eq!(ids.len(), 1);
    }

    #[test]
    fn one_id_claimed_by_two_objects_is_rejected_not_overwritten() {
        // A cross-object read is the one failure this cache must never have, so
        // a checkpoint that would cause one loses rather than wins.
        let ids = StreamIds::new();
        let mut recovery = Recovery::new(&ids);
        recovery.accept(&[(4, object("first.parquet"))]);

        let accepted = recovery.accept(&[(4, object("second.parquet"))]);
        assert!(accepted.is_empty(), "the second claim must be dropped");
        assert_eq!(ids.peek(&object("first.parquet")), Some(4));
        assert_eq!(ids.peek(&object("second.parquet")), None);
    }

    #[test]
    fn one_object_claimed_under_two_ids_is_rejected() {
        let ids = StreamIds::new();
        let mut recovery = Recovery::new(&ids);
        recovery.accept(&[(4, object("a.parquet"))]);

        assert!(recovery.accept(&[(5, object("a.parquet"))]).is_empty());
        assert_eq!(ids.peek(&object("a.parquet")), Some(4));
    }

    #[test]
    fn a_released_object_reinterns_above_its_recovered_id() {
        // `released_ids_are_retired_not_recycled` has to keep holding after a
        // recovery seeded the table from disk.
        let ids = StreamIds::new();
        Recovery::new(&ids).accept(&[(8, object("a.parquet"))]);
        assert_eq!(ids.release_object(&object("a.parquet")), vec![8]);

        assert!(ids.get_or_intern(&object("a.parquet")) > 8);
    }
}
