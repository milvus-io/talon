//! Durable dirty staging and flush queue — the write-back skeleton (#358).
//!
//! Talon's write path is write-through today: the client PUTs a whole object to
//! its owning worker, the worker uploads it to the origin, and only then caches
//! the bytes. The backend upload *is* the durability point, so there is no
//! worker-side dirty state to lose.
//!
//! Write-back inverts that: the worker acknowledges a write once the bytes are
//! durable **locally**, and uploads to the origin afterwards. That requires
//! machinery write-through never needed — durable staging, crash recovery, a
//! queue of un-flushed objects, and a way to read back data that exists only
//! locally. This module is that machinery, built and tested on its own.
//!
//! It is deliberately **not** wired into [`WorkerRuntime`](crate::WorkerRuntime)
//! or the serve path. ADR 0002 (`docs/adr/0002-write-cache-durability.md`)
//! settled the question this module exists to serve: Talon promises
//! **write-through only** — a write is durable when the origin acknowledges it,
//! never before — and write-back stays deferred behind explicit entry
//! conditions, chief among them replication before acknowledgement, since
//! acknowledging from one node's NVMe is not durability.
//!
//! So this module is the mechanism a future write-back would need, kept built
//! and tested so that decision is a wiring and replication problem rather than a
//! from-scratch design problem. Enabling it requires its own ADR superseding
//! 0002.
//!
//! # On-disk layout
//!
//! Each staged object writes two files under `<root>/writeback/`:
//!
//! ```text
//! <hash>-<seq>.data   the object bytes
//! <hash>-<seq>.meta   JSON sidecar: object path, length, checksum, seq
//! ```
//!
//! `<hash>` is an xxh3 of the object's namespace path (so an arbitrary object
//! key becomes a safe filename) and `<seq>` is a process-monotonic counter that
//! makes each staged generation distinct — a new write to an object being
//! flushed must not overwrite the bytes the flusher is reading.
//!
//! Both files are staged-then-renamed via [`Stager`], so a present file is
//! always complete. **The sidecar is written after the data and is the commit
//! marker**: recovery only trusts an entry whose `.meta` exists, so an interrupt
//! at any point leaves either a complete entry or reclaimable garbage, never a
//! pending entry pointing at a truncated body.
//!
//! # Coalescing and the in-flight boundary
//!
//! An object rewritten repeatedly before its flush starts collapses to a single
//! pending entry — the newer bytes supersede the older, whose files are removed.
//! Once [`take_next`](WriteCache::take_next) hands an entry to the flusher it
//! becomes *in flight* and is immune to coalescing; a concurrent write creates a
//! new pending entry alongside it. That separation is what keeps a hot object
//! from either accumulating N uploads or having its bytes pulled out from under
//! an upload in progress.
//!
//! A flush that fails is requeued rather than dropped — a write acknowledged
//! locally must not be lost because the origin was briefly unavailable.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use talon_core::{Error, ObjectId, Result};

use crate::staging::{Checksum, Stager};

/// The subdirectory of the cache root holding dirty (un-flushed) objects.
const WRITEBACK_DIR: &str = "writeback";

/// A staged generation of one object's bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    /// Monotonic generation number; higher supersedes lower.
    seq: u64,
    /// Length of the staged body in bytes.
    len: u64,
    /// xxh3 checksum of the staged body, verified on read-back.
    checksum: Checksum,
}

/// The sidecar record persisted next to a staged body.
#[derive(serde::Serialize, serde::Deserialize)]
struct Sidecar {
    /// The object's namespace path (`/s3/<bucket>/<key>`), so recovery can
    /// reconstruct the [`ObjectId`] without a separate index.
    path: String,
    seq: u64,
    len: u64,
    checksum: u64,
}

/// One object handed to the flusher by [`WriteCache::take_next`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushItem {
    /// The object to upload to the origin.
    pub object: ObjectId,
    /// The generation being flushed; pass it back to
    /// [`complete`](WriteCache::complete)/[`requeue`](WriteCache::requeue) so a
    /// stale completion cannot retire a newer generation.
    pub seq: u64,
    /// The staged bytes, checksum-verified on read-back.
    pub bytes: bytes::Bytes,
}

#[derive(Default)]
struct Inner {
    /// Staged generations waiting to be flushed, one per object (the newest).
    pending: HashMap<ObjectId, Entry>,
    /// Generations currently being uploaded; immune to coalescing.
    inflight: HashMap<ObjectId, Entry>,
    /// FIFO of objects with a pending entry. May hold stale duplicates, which
    /// `take_next` skips.
    queue: VecDeque<ObjectId>,
    /// Generations whose flush failed permanently, parked out of the queue.
    ///
    /// Their files stay on disk: this is acknowledged data that could not be
    /// delivered to the origin, so dropping it would be silent data loss. It is
    /// held for an operator to drain or abandon deliberately (ADR 0002 §5).
    failed: HashMap<ObjectId, Entry>,
}

/// Durable staging area and flush queue for un-flushed (dirty) objects.
pub struct WriteCache {
    dir: PathBuf,
    stager: Stager,
    next_seq: AtomicU64,
    inner: Mutex<Inner>,
}

impl WriteCache {
    /// Open (creating if needed) the write cache under `<root>/writeback`,
    /// recovering any entries left by a previous process.
    ///
    /// Returns the cache; use [`pending_len`](Self::pending_len) to observe how
    /// many entries recovery restored.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let dir = root.as_ref().join(WRITEBACK_DIR);
        std::fs::create_dir_all(&dir)?;
        let stager = Stager::new(&dir)?;
        stager.reclaim_orphans()?;
        let cache = Self {
            dir,
            stager,
            next_seq: AtomicU64::new(0),
            inner: Mutex::new(Inner::default()),
        };
        cache.recover()?;
        Ok(cache)
    }

    /// The directory holding staged objects.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Number of objects with a pending (not yet in flight) generation.
    pub fn pending_len(&self) -> usize {
        self.inner.lock().unwrap().pending.len()
    }

    /// Number of objects currently being flushed.
    pub fn inflight_len(&self) -> usize {
        self.inner.lock().unwrap().inflight.len()
    }

    /// Number of objects parked after a permanently-failed flush.
    ///
    /// A non-zero value means the node is holding acknowledged data the origin
    /// has refused to accept. It requires operator action and does not resolve
    /// on its own.
    pub fn failed_len(&self) -> usize {
        self.inner.lock().unwrap().failed.len()
    }

    /// The objects parked after a permanently-failed flush, with their staged
    /// byte counts — the enumeration an operator drain procedure works from.
    pub fn failed_objects(&self) -> Vec<(ObjectId, u64)> {
        let inner = self.inner.lock().unwrap();
        inner
            .failed
            .iter()
            .map(|(object, entry)| (object.clone(), entry.len))
            .collect()
    }

    /// Total staged bytes across pending, in-flight, and failed generations.
    ///
    /// This is the amount of acknowledged-but-not-yet-durable-at-the-origin data
    /// the node is holding — the number a write-back deployment must alarm on.
    /// Parked failures count: they are the part of it that will not drain by
    /// itself.
    pub fn dirty_bytes(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.pending.values().map(|e| e.len).sum::<u64>()
            + inner.inflight.values().map(|e| e.len).sum::<u64>()
            + inner.failed.values().map(|e| e.len).sum::<u64>()
    }

    /// Durably stage `body` as the newest generation of `object` and enqueue it
    /// for flushing.
    ///
    /// Returns once the bytes and their commit marker are `fsync`ed — this is
    /// the point a write-back path would be entitled to acknowledge the write.
    /// A pending generation of the same object that has not started flushing is
    /// superseded and its files removed; an in-flight generation is left alone.
    pub fn stage(&self, object: &ObjectId, body: &[u8]) -> Result<u64> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let checksum = self
            .stager
            .commit(&self.data_path(object, seq), body, None)?;
        let sidecar = Sidecar {
            path: object.to_path(),
            seq,
            len: body.len() as u64,
            checksum: checksum.0,
        };
        let encoded = serde_json::to_vec(&sidecar)
            .map_err(|e| Error::Serialization(format!("encode writeback sidecar: {e}")))?;
        // The sidecar lands *after* the body: it is the commit marker, so an
        // entry is only recoverable once its bytes are already complete.
        self.stager
            .commit(&self.meta_path(object, seq), &encoded, None)?;
        // Both renames are only durable once the directory entry is; without
        // this a crash can lose a file the caller was told was staged.
        fsync_dir(&self.dir)?;

        let entry = Entry {
            seq,
            len: body.len() as u64,
            checksum,
        };
        let superseded = {
            let mut inner = self.inner.lock().unwrap();
            let superseded = inner.pending.insert(object.clone(), entry);
            if superseded.is_none() {
                inner.queue.push_back(object.clone());
            }
            // A rewrite also resolves a parked failure: the newer bytes replace
            // whatever the origin refused, so holding the old generation would
            // pin capacity and keep alarming for data nobody wants delivered.
            let parked = inner.failed.remove(object);
            superseded.into_iter().chain(parked).collect::<Vec<_>>()
        };
        // Drop superseded generations' files outside the lock; neither was in
        // flight, so nobody is reading them.
        for old in superseded {
            self.remove_files(object, old.seq);
        }
        Ok(seq)
    }

    /// Return the newest staged bytes for `object`, if any — the read-your-writes
    /// lookup.
    ///
    /// Returns the newest staged generation across the pending, in-flight, and
    /// parked-failure sets. Parked entries are included deliberately: a flush
    /// that failed does not make the bytes wrong, and dropping them from this
    /// lookup would break read-your-writes exactly when the origin is unable to
    /// serve the data either.
    ///
    /// The body is checksum-verified, so a corrupted staged file surfaces as an
    /// error rather than as silently wrong data served in place of the origin's.
    pub fn staged_bytes(&self, object: &ObjectId) -> Result<Option<bytes::Bytes>> {
        let entry = {
            let inner = self.inner.lock().unwrap();
            let newest = [
                inner.pending.get(object),
                inner.inflight.get(object),
                inner.failed.get(object),
            ]
            .into_iter()
            .flatten()
            .max_by_key(|entry| entry.seq);
            match newest {
                Some(entry) => entry.clone(),
                None => return Ok(None),
            }
        };
        self.read_verified(object, &entry).map(Some)
    }

    /// Take the next object due for flushing, moving it from pending to
    /// in-flight, or `None` when nothing is pending.
    ///
    /// The returned bytes are read from the staged file and checksum-verified.
    /// An object that is already in flight is skipped (its newer pending
    /// generation waits for the current upload to retire), so a single hot
    /// object can never be uploaded twice concurrently.
    pub fn take_next(&self) -> Result<Option<FlushItem>> {
        loop {
            let (object, entry) = {
                let mut inner = self.inner.lock().unwrap();
                let mut skipped = Vec::new();
                let found = loop {
                    let Some(object) = inner.queue.pop_front() else {
                        break None;
                    };
                    // Stale duplicate, or superseded/retired since it was queued.
                    let Some(entry) = inner.pending.get(&object).cloned() else {
                        continue;
                    };
                    if inner.inflight.contains_key(&object) {
                        // Already uploading; hold this generation back rather
                        // than racing two uploads of the same object.
                        skipped.push(object);
                        continue;
                    }
                    break Some((object, entry));
                };
                for object in skipped {
                    inner.queue.push_back(object);
                }
                match found {
                    Some((object, entry)) => {
                        inner.pending.remove(&object);
                        inner.inflight.insert(object.clone(), entry.clone());
                        (object, entry)
                    }
                    None => return Ok(None),
                }
            };

            match self.read_verified(&object, &entry) {
                Ok(bytes) => {
                    return Ok(Some(FlushItem {
                        object,
                        seq: entry.seq,
                        bytes,
                    }))
                }
                Err(error) => {
                    // A staged file that cannot be read back is unflushable; drop
                    // it rather than wedging the queue behind it forever, and be
                    // loud, because this is acknowledged data being lost.
                    tracing::error!(
                        object = %object.to_path(),
                        seq = entry.seq,
                        %error,
                        "unreadable staged write dropped"
                    );
                    let mut inner = self.inner.lock().unwrap();
                    if inner.inflight.get(&object).map(|e| e.seq) == Some(entry.seq) {
                        inner.inflight.remove(&object);
                    }
                    drop(inner);
                    self.remove_files(&object, entry.seq);
                    continue;
                }
            }
        }
    }

    /// Retire a successfully-flushed generation, deleting its staged files.
    ///
    /// A `seq` that no longer matches the in-flight generation is ignored, so a
    /// late completion cannot retire data staged after it.
    pub fn complete(&self, object: &ObjectId, seq: u64) {
        let retire = {
            let mut inner = self.inner.lock().unwrap();
            match inner.inflight.get(object) {
                Some(entry) if entry.seq == seq => {
                    inner.inflight.remove(object);
                    true
                }
                _ => false,
            }
        };
        if retire {
            self.remove_files(object, seq);
        }
    }

    /// Return a failed flush to the queue so it is retried.
    ///
    /// If a newer generation was staged while this one was in flight, the failed
    /// generation is discarded instead — the newer bytes supersede it, and
    /// re-uploading the older ones would move the origin backwards.
    pub fn requeue(&self, object: &ObjectId, seq: u64) {
        let stale = {
            let mut inner = self.inner.lock().unwrap();
            match inner.inflight.get(object) {
                Some(entry) if entry.seq == seq => {
                    let entry = inner.inflight.remove(object).expect("just matched");
                    match inner.pending.get(object) {
                        // A newer generation is already queued; this one is dead.
                        Some(newer) if newer.seq > entry.seq => true,
                        _ => {
                            inner.pending.insert(object.clone(), entry);
                            inner.queue.push_back(object.clone());
                            false
                        }
                    }
                }
                _ => false,
            }
        };
        if stale {
            self.remove_files(object, seq);
        }
    }

    /// Park a generation whose flush failed permanently, out of the retry queue.
    ///
    /// Called when a driver exhausts its retry budget. The staged files are
    /// **kept**: this is acknowledged data the origin would not accept, so
    /// deleting it would turn a delivery failure into silent data loss. It stays
    /// counted in [`dirty_bytes`](Self::dirty_bytes) and enumerable via
    /// [`failed_objects`](Self::failed_objects) so an operator can drain or
    /// abandon it deliberately (ADR 0002 §5).
    ///
    /// If a newer generation was staged while this one was in flight, the failed
    /// generation is discarded instead — the newer bytes supersede it, and the
    /// newer flush may well succeed.
    pub fn park_failed(&self, object: &ObjectId, seq: u64) {
        let stale = {
            let mut inner = self.inner.lock().unwrap();
            match inner.inflight.get(object) {
                Some(entry) if entry.seq == seq => {
                    let entry = inner.inflight.remove(object).expect("just matched");
                    match inner.pending.get(object) {
                        Some(newer) if newer.seq > entry.seq => true,
                        _ => {
                            tracing::error!(
                                object = %object.to_path(),
                                seq,
                                bytes = entry.len,
                                "staged write parked after permanent flush failure; \
                                 it will not drain without operator action"
                            );
                            inner.failed.insert(object.clone(), entry);
                            false
                        }
                    }
                }
                _ => false,
            }
        };
        if stale {
            self.remove_files(object, seq);
        }
    }

    /// Move every parked failure back into the flush queue.
    ///
    /// The operator-facing half of [`park_failed`](Self::park_failed): after the
    /// cause is fixed (credentials, quota, a bucket policy), this re-arms them
    /// without restarting the process. Returns how many were requeued. A parked
    /// object that has since been rewritten is dropped rather than requeued —
    /// the newer generation already covers it.
    pub fn retry_failed(&self) -> usize {
        let (requeued, superseded) = {
            let mut inner = self.inner.lock().unwrap();
            let parked: Vec<_> = inner.failed.drain().collect();
            let mut requeued = 0;
            let mut superseded = Vec::new();
            for (object, entry) in parked {
                match inner.pending.get(&object) {
                    Some(newer) if newer.seq > entry.seq => superseded.push((object, entry.seq)),
                    _ => {
                        inner.pending.insert(object.clone(), entry);
                        inner.queue.push_back(object);
                        requeued += 1;
                    }
                }
            }
            (requeued, superseded)
        };
        for (object, seq) in superseded {
            self.remove_files(&object, seq);
        }
        requeued
    }

    /// Read a staged body and verify it against the entry's checksum.
    fn read_verified(&self, object: &ObjectId, entry: &Entry) -> Result<bytes::Bytes> {
        let bytes = std::fs::read(self.data_path(object, entry.seq))?;
        if bytes.len() as u64 != entry.len {
            return Err(Error::Backend(format!(
                "staged write for {} is {} bytes, expected {}",
                object.to_path(),
                bytes.len(),
                entry.len
            )));
        }
        Stager::verify(&bytes, entry.checksum)?;
        Ok(bytes::Bytes::from(bytes))
    }

    /// Rebuild the pending set from committed sidecars and reclaim everything
    /// else.
    ///
    /// A `.data` file with no `.meta` was interrupted before commit and is
    /// dropped. Where several generations of one object survived (a crash
    /// between staging a newer generation and removing the older), the highest
    /// `seq` wins and the rest are removed.
    fn recover(&self) -> Result<()> {
        let mut newest: HashMap<ObjectId, Entry> = HashMap::new();
        let mut seen_seq: HashMap<(ObjectId, u64), ()> = HashMap::new();
        let mut max_seq = 0u64;

        for dirent in std::fs::read_dir(&self.dir)? {
            let path = dirent?.path();
            if path.extension().is_some_and(|e| e == "meta") {
                let raw = match std::fs::read(&path) {
                    Ok(raw) => raw,
                    Err(error) => {
                        tracing::warn!(?path, %error, "unreadable writeback sidecar reclaimed");
                        let _ = std::fs::remove_file(&path);
                        continue;
                    }
                };
                let sidecar: Sidecar = match serde_json::from_slice(&raw) {
                    Ok(s) => s,
                    Err(error) => {
                        tracing::warn!(?path, %error, "corrupt writeback sidecar reclaimed");
                        let _ = std::fs::remove_file(&path);
                        continue;
                    }
                };
                let object = match ObjectId::from_path(&sidecar.path) {
                    Ok(o) => o,
                    Err(error) => {
                        tracing::warn!(?path, %error, "writeback sidecar has an unusable path");
                        let _ = std::fs::remove_file(&path);
                        continue;
                    }
                };
                max_seq = max_seq.max(sidecar.seq);
                seen_seq.insert((object.clone(), sidecar.seq), ());
                let entry = Entry {
                    seq: sidecar.seq,
                    len: sidecar.len,
                    checksum: Checksum(sidecar.checksum),
                };
                match newest.get(&object) {
                    Some(existing) if existing.seq >= entry.seq => {
                        // An older generation survived alongside a newer one.
                        self.remove_files(&object, entry.seq);
                    }
                    Some(existing) => {
                        let superseded = existing.seq;
                        newest.insert(object.clone(), entry);
                        self.remove_files(&object, superseded);
                    }
                    None => {
                        newest.insert(object, entry);
                    }
                }
            }
        }

        // Any body whose sidecar never landed was never committed; drop it.
        for dirent in std::fs::read_dir(&self.dir)? {
            let path = dirent?.path();
            if !path.extension().is_some_and(|e| e == "data") {
                continue;
            }
            let committed = path
                .file_stem()
                .and_then(|s| s.to_str())
                .map(|stem| self.dir.join(format!("{stem}.meta")))
                .is_some_and(|meta| meta.exists());
            if !committed {
                tracing::warn!(?path, "uncommitted staged write reclaimed");
                let _ = std::fs::remove_file(&path);
            }
        }

        self.next_seq
            .store(max_seq.saturating_add(1), Ordering::Relaxed);
        let mut inner = self.inner.lock().unwrap();
        for (object, entry) in newest {
            inner.queue.push_back(object.clone());
            inner.pending.insert(object, entry);
        }
        Ok(())
    }

    /// Best-effort removal of one generation's data and sidecar.
    ///
    /// The sidecar goes first: with the body still present, a crash mid-removal
    /// leaves an uncommitted body that recovery reclaims — the reverse order
    /// would leave a pending entry pointing at nothing.
    fn remove_files(&self, object: &ObjectId, seq: u64) {
        let _ = std::fs::remove_file(self.meta_path(object, seq));
        let _ = std::fs::remove_file(self.data_path(object, seq));
    }

    fn data_path(&self, object: &ObjectId, seq: u64) -> PathBuf {
        self.dir.join(format!("{}.data", file_stem(object, seq)))
    }

    fn meta_path(&self, object: &ObjectId, seq: u64) -> PathBuf {
        self.dir.join(format!("{}.meta", file_stem(object, seq)))
    }
}

/// A filesystem-safe, collision-resistant stem for one staged generation.
///
/// The object path is hashed rather than escaped: object keys may contain any
/// byte and are effectively unbounded in length, so neither is safe as a
/// filename. The sidecar carries the real path, so the hash never needs to be
/// reversed.
fn file_stem(object: &ObjectId, seq: u64) -> String {
    format!(
        "{:016x}-{seq}",
        xxhash_rust::xxh3::xxh3_64(object.to_path().as_bytes())
    )
}

/// `fsync` a directory so renames into it are durable.
fn fsync_dir(dir: &Path) -> Result<()> {
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::Backend;

    fn object(name: &str) -> ObjectId {
        ObjectId::new(Backend::S3, "bkt", name)
    }

    fn cache() -> (tempdir::TempDir, WriteCache) {
        let dir = tempdir::TempDir::new();
        let cache = WriteCache::open(dir.path()).unwrap();
        (dir, cache)
    }

    #[test]
    fn stage_then_take_returns_the_bytes_and_moves_to_inflight() {
        let (_dir, cache) = cache();
        let seq = cache.stage(&object("a.bin"), b"hello").unwrap();
        assert_eq!(cache.pending_len(), 1);
        assert_eq!(cache.dirty_bytes(), 5);

        let item = cache.take_next().unwrap().expect("one pending");
        assert_eq!(item.object, object("a.bin"));
        assert_eq!(item.seq, seq);
        assert_eq!(&item.bytes[..], b"hello");
        assert_eq!(cache.pending_len(), 0);
        assert_eq!(cache.inflight_len(), 1);
        // Still dirty: in flight is not yet durable at the origin.
        assert_eq!(cache.dirty_bytes(), 5);

        assert!(cache.take_next().unwrap().is_none());
    }

    #[test]
    fn complete_retires_the_entry_and_deletes_its_files() {
        let (_dir, cache) = cache();
        cache.stage(&object("a.bin"), b"hello").unwrap();
        let item = cache.take_next().unwrap().unwrap();
        cache.complete(&item.object, item.seq);

        assert_eq!(cache.inflight_len(), 0);
        assert_eq!(cache.dirty_bytes(), 0);
        assert!(cache.staged_bytes(&object("a.bin")).unwrap().is_none());
        let left: Vec<_> = std::fs::read_dir(cache.dir())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .filter(|n| n != "staging")
            .collect();
        assert!(left.is_empty(), "files left behind: {left:?}");
    }

    #[test]
    fn a_stale_completion_does_not_retire_a_newer_generation() {
        let (_dir, cache) = cache();
        cache.stage(&object("a.bin"), b"v1").unwrap();
        let first = cache.take_next().unwrap().unwrap();
        cache.stage(&object("a.bin"), b"v2").unwrap();
        // The flusher for v1 reports success late, after v2 was staged.
        cache.complete(&first.object, first.seq + 100);
        assert_eq!(cache.inflight_len(), 1);
        cache.complete(&first.object, first.seq);

        // v2 survives and is still flushable.
        let next = cache.take_next().unwrap().expect("v2 pending");
        assert_eq!(&next.bytes[..], b"v2");
    }

    #[test]
    fn repeat_writes_coalesce_into_one_pending_entry() {
        let (_dir, cache) = cache();
        for body in [b"v1", b"v2", b"v3"] {
            cache.stage(&object("a.bin"), body).unwrap();
        }
        assert_eq!(cache.pending_len(), 1);
        assert_eq!(cache.dirty_bytes(), 2);

        let item = cache.take_next().unwrap().unwrap();
        assert_eq!(&item.bytes[..], b"v3");
        assert!(cache.take_next().unwrap().is_none(), "no stale duplicates");
    }

    #[test]
    fn a_write_during_flush_does_not_disturb_the_inflight_generation() {
        let (_dir, cache) = cache();
        cache.stage(&object("a.bin"), b"v1").unwrap();
        let inflight = cache.take_next().unwrap().unwrap();
        cache.stage(&object("a.bin"), b"v2").unwrap();

        // The in-flight bytes are still readable and unchanged.
        assert_eq!(
            std::fs::read(cache.data_path(&inflight.object, inflight.seq)).unwrap(),
            b"v1"
        );
        // And v2 is held back rather than uploaded concurrently.
        assert!(cache.take_next().unwrap().is_none());

        cache.complete(&inflight.object, inflight.seq);
        let next = cache.take_next().unwrap().expect("v2 released");
        assert_eq!(&next.bytes[..], b"v2");
    }

    #[test]
    fn a_failed_flush_is_requeued_and_retried() {
        let (_dir, cache) = cache();
        cache.stage(&object("a.bin"), b"hello").unwrap();
        let item = cache.take_next().unwrap().unwrap();
        cache.requeue(&item.object, item.seq);

        assert_eq!(cache.inflight_len(), 0);
        assert_eq!(cache.pending_len(), 1);
        let retry = cache.take_next().unwrap().expect("retried");
        assert_eq!(retry.seq, item.seq);
        assert_eq!(&retry.bytes[..], b"hello");
    }

    #[test]
    fn requeue_discards_a_generation_superseded_while_in_flight() {
        let (_dir, cache) = cache();
        cache.stage(&object("a.bin"), b"v1").unwrap();
        let inflight = cache.take_next().unwrap().unwrap();
        cache.stage(&object("a.bin"), b"v2").unwrap();
        cache.requeue(&inflight.object, inflight.seq);

        // Only v2 remains; re-uploading v1 would move the origin backwards.
        assert_eq!(cache.pending_len(), 1);
        let next = cache.take_next().unwrap().unwrap();
        assert_eq!(&next.bytes[..], b"v2");
        assert!(cache.take_next().unwrap().is_none());
    }

    #[test]
    fn staged_bytes_serves_read_your_writes_for_pending_and_inflight() {
        let (_dir, cache) = cache();
        assert!(cache.staged_bytes(&object("a.bin")).unwrap().is_none());

        cache.stage(&object("a.bin"), b"v1").unwrap();
        assert_eq!(
            &cache.staged_bytes(&object("a.bin")).unwrap().unwrap()[..],
            b"v1"
        );

        // Readable while in flight...
        let item = cache.take_next().unwrap().unwrap();
        assert_eq!(
            &cache.staged_bytes(&object("a.bin")).unwrap().unwrap()[..],
            b"v1"
        );

        // ...and a newer pending generation wins over the in-flight one.
        cache.stage(&object("a.bin"), b"v2").unwrap();
        assert_eq!(
            &cache.staged_bytes(&object("a.bin")).unwrap().unwrap()[..],
            b"v2"
        );
        cache.complete(&item.object, item.seq);
        assert_eq!(
            &cache.staged_bytes(&object("a.bin")).unwrap().unwrap()[..],
            b"v2"
        );
    }

    #[test]
    fn staged_bytes_rejects_a_corrupted_body() {
        let (_dir, cache) = cache();
        let seq = cache.stage(&object("a.bin"), b"hello").unwrap();
        std::fs::write(cache.data_path(&object("a.bin"), seq), b"world").unwrap();

        let err = cache.staged_bytes(&object("a.bin")).unwrap_err();
        assert!(
            err.to_string().contains("checksum mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn recovery_restores_pending_entries_across_a_restart() {
        let dir = tempdir::TempDir::new();
        {
            let cache = WriteCache::open(dir.path()).unwrap();
            cache.stage(&object("a.bin"), b"aaa").unwrap();
            cache.stage(&object("b.bin"), b"bb").unwrap();
            // b is in flight when the process dies — un-flushed either way.
            cache.take_next().unwrap().unwrap();
        }

        let cache = WriteCache::open(dir.path()).unwrap();
        assert_eq!(cache.pending_len(), 2);
        assert_eq!(cache.dirty_bytes(), 5);
        let mut recovered: Vec<_> = std::iter::from_fn(|| cache.take_next().unwrap())
            .map(|i| (i.object.object_path.clone(), i.bytes))
            .collect();
        recovered.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(recovered[0].0, "a.bin");
        assert_eq!(&recovered[0].1[..], b"aaa");
        assert_eq!(recovered[1].0, "b.bin");
        assert_eq!(&recovered[1].1[..], b"bb");
    }

    #[test]
    fn recovery_drops_a_body_whose_commit_marker_never_landed() {
        let dir = tempdir::TempDir::new();
        let seq = {
            let cache = WriteCache::open(dir.path()).unwrap();
            let seq = cache.stage(&object("a.bin"), b"hello").unwrap();
            // Simulate a crash between the body rename and the sidecar rename.
            std::fs::remove_file(cache.meta_path(&object("a.bin"), seq)).unwrap();
            seq
        };

        let cache = WriteCache::open(dir.path()).unwrap();
        assert_eq!(cache.pending_len(), 0);
        assert!(cache.take_next().unwrap().is_none());
        assert!(!cache.data_path(&object("a.bin"), seq).exists());
    }

    #[test]
    fn recovery_keeps_only_the_newest_surviving_generation() {
        let dir = tempdir::TempDir::new();
        {
            let cache = WriteCache::open(dir.path()).unwrap();
            let old = cache.stage(&object("a.bin"), b"v1").unwrap();
            // Take it in flight so the next stage does not remove its files,
            // leaving two committed generations on disk as a crash would.
            cache.take_next().unwrap().unwrap();
            cache.stage(&object("a.bin"), b"v222").unwrap();
            assert!(cache.data_path(&object("a.bin"), old).exists());
        }

        let cache = WriteCache::open(dir.path()).unwrap();
        assert_eq!(cache.pending_len(), 1);
        assert_eq!(cache.dirty_bytes(), 4);
        let item = cache.take_next().unwrap().unwrap();
        assert_eq!(&item.bytes[..], b"v222");
        assert!(cache.take_next().unwrap().is_none());
    }

    #[test]
    fn recovery_reclaims_a_corrupt_sidecar() {
        let dir = tempdir::TempDir::new();
        let seq = {
            let cache = WriteCache::open(dir.path()).unwrap();
            let seq = cache.stage(&object("a.bin"), b"hello").unwrap();
            std::fs::write(cache.meta_path(&object("a.bin"), seq), b"{not json").unwrap();
            seq
        };

        let cache = WriteCache::open(dir.path()).unwrap();
        assert_eq!(cache.pending_len(), 0);
        assert!(!cache.data_path(&object("a.bin"), seq).exists());
    }

    #[test]
    fn sequence_numbers_do_not_repeat_across_a_restart() {
        let dir = tempdir::TempDir::new();
        let first = {
            let cache = WriteCache::open(dir.path()).unwrap();
            cache.stage(&object("a.bin"), b"v1").unwrap()
        };
        let cache = WriteCache::open(dir.path()).unwrap();
        let second = cache.stage(&object("b.bin"), b"v2").unwrap();
        assert!(
            second > first,
            "seq went backwards across restart: {first} then {second}"
        );
    }

    #[test]
    fn an_unreadable_staged_file_is_dropped_rather_than_wedging_the_queue() {
        let (_dir, cache) = cache();
        let bad = cache.stage(&object("bad.bin"), b"hello").unwrap();
        std::fs::remove_file(cache.data_path(&object("bad.bin"), bad)).unwrap();
        cache.stage(&object("good.bin"), b"fine").unwrap();

        let item = cache.take_next().unwrap().expect("queue not wedged");
        assert_eq!(item.object, object("good.bin"));
        assert_eq!(&item.bytes[..], b"fine");
        assert_eq!(cache.pending_len(), 0);
    }

    #[test]
    fn distinct_objects_flush_in_fifo_order() {
        let (_dir, cache) = cache();
        for name in ["a.bin", "b.bin", "c.bin"] {
            cache.stage(&object(name), name.as_bytes()).unwrap();
        }
        let order: Vec<_> = std::iter::from_fn(|| cache.take_next().unwrap())
            .map(|i| i.object.object_path)
            .collect();
        assert_eq!(order, ["a.bin", "b.bin", "c.bin"]);
    }

    #[test]
    fn park_failed_keeps_the_bytes_out_of_the_queue_but_still_accounted() {
        let (_dir, cache) = cache();
        let seq = cache.stage(&object("a.bin"), b"hello").unwrap();
        let item = cache.take_next().unwrap().unwrap();
        cache.park_failed(&item.object, item.seq);

        // Not flushable, but not lost: the files stay and the bytes still count
        // as dirty, because this is acknowledged data the origin refused.
        assert!(cache.take_next().unwrap().is_none());
        assert_eq!(cache.pending_len(), 0);
        assert_eq!(cache.inflight_len(), 0);
        assert_eq!(cache.failed_len(), 1);
        assert_eq!(cache.dirty_bytes(), 5);
        assert_eq!(cache.failed_objects(), vec![(object("a.bin"), 5)]);
        assert!(cache.data_path(&object("a.bin"), seq).exists());
        // And still readable, so read-your-writes survives a failed flush.
        assert_eq!(
            &cache.staged_bytes(&object("a.bin")).unwrap().unwrap()[..],
            b"hello"
        );
    }

    #[test]
    fn retry_failed_returns_parked_entries_to_the_queue() {
        let (_dir, cache) = cache();
        for name in ["a.bin", "b.bin"] {
            cache.stage(&object(name), name.as_bytes()).unwrap();
            let item = cache.take_next().unwrap().unwrap();
            cache.park_failed(&item.object, item.seq);
        }
        assert_eq!(cache.failed_len(), 2);

        assert_eq!(cache.retry_failed(), 2);
        assert_eq!(cache.failed_len(), 0);
        assert_eq!(cache.pending_len(), 2);

        let mut names: Vec<_> = std::iter::from_fn(|| cache.take_next().unwrap())
            .map(|i| i.object.object_path)
            .collect();
        names.sort();
        assert_eq!(names, ["a.bin", "b.bin"]);
    }

    #[test]
    fn a_rewrite_releases_a_parked_generation() {
        let (_dir, cache) = cache();
        let old = cache.stage(&object("a.bin"), b"v1").unwrap();
        let item = cache.take_next().unwrap().unwrap();
        cache.park_failed(&item.object, item.seq);

        cache.stage(&object("a.bin"), b"v2").unwrap();

        // The parked generation is released rather than pinning capacity and
        // alarming forever for bytes nobody wants delivered any more.
        assert_eq!(cache.failed_len(), 0);
        assert_eq!(cache.dirty_bytes(), 2);
        assert!(!cache.data_path(&object("a.bin"), old).exists());
        let next = cache.take_next().unwrap().unwrap();
        assert_eq!(&next.bytes[..], b"v2");
    }

    #[test]
    fn parking_a_generation_superseded_in_flight_discards_it() {
        let (_dir, cache) = cache();
        cache.stage(&object("a.bin"), b"v1").unwrap();
        let inflight = cache.take_next().unwrap().unwrap();
        cache.stage(&object("a.bin"), b"v2").unwrap();
        cache.park_failed(&inflight.object, inflight.seq);

        // v2 supersedes v1 and may well succeed, so v1 is not parked.
        assert_eq!(cache.failed_len(), 0);
        assert_eq!(cache.pending_len(), 1);
        assert!(!cache.data_path(&object("a.bin"), inflight.seq).exists());
    }

    #[test]
    fn parked_entries_survive_a_restart_as_pending() {
        let dir = tempdir::TempDir::new();
        {
            let cache = WriteCache::open(dir.path()).unwrap();
            cache.stage(&object("a.bin"), b"hello").unwrap();
            let item = cache.take_next().unwrap().unwrap();
            cache.park_failed(&item.object, item.seq);
        }

        // Recovery does not distinguish parked from pending — a restart is a
        // legitimate reason to try again, and the bytes are still owed to the
        // origin either way.
        let cache = WriteCache::open(dir.path()).unwrap();
        assert_eq!(cache.pending_len(), 1);
        assert_eq!(cache.dirty_bytes(), 5);
        let item = cache.take_next().unwrap().unwrap();
        assert_eq!(&item.bytes[..], b"hello");
    }

    /// A minimal scoped temp directory (the worker crate has no tempfile dep).
    mod tempdir {
        use std::path::{Path, PathBuf};

        pub struct TempDir(PathBuf);

        impl TempDir {
            pub fn new() -> Self {
                static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let path =
                    std::env::temp_dir().join(format!("talon-wc-{}-{n}", std::process::id()));
                std::fs::create_dir_all(&path).unwrap();
                Self(path)
            }

            pub fn path(&self) -> &Path {
                &self.0
            }
        }

        impl Drop for TempDir {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
    }
}
