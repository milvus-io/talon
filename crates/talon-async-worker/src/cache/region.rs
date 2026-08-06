// SPDX-License-Identifier: Apache-2.0
//! L2: region-packed shard files, the on-disk layout for cached extents.
//!
//! An extent is not its own file. Each shard is one pre-allocated file divided
//! into fixed [`REGION_SIZE`] regions; extents are packed contiguously into a
//! region as they are admitted and addressed by an [`ExtentRun`]. Reads are
//! `pread` at the run's file offset, writes are one `pwrite` per packed region.
//!
//! Nothing here is aligned or padded. A run is exactly as long as the bytes
//! that were fetched, so a 4KB footer occupies 4KB of a region. That is the
//! whole point of this worker — see ADR 0005 §2.
//!
//! # Why regions rather than one file per extent
//!
//! One file per cached range is up to thousands of inodes for a single object,
//! and this worker deliberately caches many small ranges. Packing instead:
//!
//! - bounds inode and open-file count by shard count, not resident extent count;
//! - keeps zero-copy — `sendfile(region_fd, offset, len)` works here exactly as
//!   it does on a whole-block file, and extents that are contiguous inside a
//!   region coalesce into *one* call rather than one per extent;
//! - makes reclamation O(regions) instead of O(extents).
//!
//! The cost is that a region is the unit of reclamation: evicting one discards
//! every extent packed into it. [`RegionTracker`] scores regions by decayed
//! read volume to make that choice well, but the imprecision is real and
//! accepted.
//!
//! # Eviction cannot race a read
//!
//! Each region carries a pin count. [`ShardFile::get_bytes`] and
//! [`ShardFile::pin`] take a pin while still holding the state read lock, and
//! [`grow_or_evict`](ShardState::grow_or_evict) — which needs the write lock —
//! skips any region with a live pin. A `pwrite` packing a new extent therefore
//! can never land on a region an in-flight `pread` or `sendfile` is reading.
//!
//! See ADR 0005 §4.

use std::collections::HashMap;
use std::os::fd::OwnedFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use talon_core::{BlockHandle, Error, Result};

use super::ExtentKey;

/// Size of one region within a shard file.
pub const REGION_SIZE: u64 = 64 * 1024 * 1024;

/// Ceiling on how many of the coldest regions a single reclamation pass
/// considers. Scaled down on small shards — see
/// [`grow_or_evict`](ShardState::grow_or_evict).
const NUM_EVICTION_CANDIDATES: usize = 3;

/// Read events between successive score decays.
const DECAY_INTERVAL: u64 = 1_000;

/// Multiplier applied to every region score at each decay tick, so a region
/// that was hot an hour ago loses to one that is hot now.
const DECAY_FACTOR: f64 = 0.9;

/// Where one extent's bytes live inside a shard file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtentRun {
    /// Region index within the shard file.
    pub region: u32,
    /// Byte offset of the extent within its region.
    pub offset_in_region: u32,
    /// Length of the extent in bytes. Whatever was fetched — not a page size.
    pub size: u32,
    /// xxh3 digest of the extent bytes, truncated to 32 bits. Zero when
    /// checksums are disabled, which is why verification also tests for
    /// non-zero.
    pub checksum: u32,
}

impl ExtentRun {
    /// Absolute byte offset of this run within the shard file.
    #[inline]
    pub fn file_offset(&self) -> u64 {
        self.region as u64 * REGION_SIZE + self.offset_in_region as u64
    }
}

/// Truncated xxh3 digest used for optional extent integrity checks.
///
/// xxh3 rather than crc32 because `xxhash-rust` is already a workspace
/// dependency; this is corruption detection, not a security boundary.
#[inline]
fn digest(bytes: &[u8]) -> u32 {
    xxhash_rust::xxh3::xxh3_64(bytes) as u32
}

/// Decayed read-volume scores, one per region, driving reclamation choice.
#[derive(Debug, Default)]
struct RegionTracker {
    scores: Vec<f64>,
    event_count: u64,
}

impl RegionTracker {
    fn new() -> Self {
        Self::default()
    }

    fn ensure_capacity(&mut self, regions: usize) {
        if self.scores.len() < regions {
            self.scores.resize(regions, 0.0);
        }
    }

    /// Credit a region for bytes served from it.
    fn region_read(&mut self, region: u32, bytes: u64) {
        let idx = region as usize;
        self.ensure_capacity(idx + 1);
        self.scores[idx] += bytes as f64;
    }

    /// Credit a region for being filled, so a freshly packed region is not
    /// immediately the coldest candidate before anything has read from it.
    fn region_filled(&mut self, region: u32) {
        let idx = region as usize;
        self.ensure_capacity(idx + 1);
        self.scores[idx] += REGION_SIZE as f64 * 0.1;
    }

    /// Advance the decay clock, scaling every score down at each interval.
    fn tick(&mut self) {
        self.event_count += 1;
        if self.event_count % DECAY_INTERVAL == 0 {
            for s in self.scores.iter_mut() {
                *s *= DECAY_FACTOR;
            }
        }
    }

    /// The `n` coldest regions, excluding `pinned`.
    fn coldest(&self, n: usize, pinned: &[u32]) -> Vec<u32> {
        let mut indexed: Vec<(u32, f64)> = self
            .scores
            .iter()
            .enumerate()
            .filter(|(i, _)| !pinned.contains(&(*i as u32)))
            .map(|(i, &s)| (i as u32, s))
            .collect();
        indexed.sort_by(|a, b| a.1.total_cmp(&b.1));
        indexed.truncate(n);
        indexed.into_iter().map(|(r, _)| r).collect()
    }
}

/// A region's worth of packed extents, ready to be written in one `pwrite`.
struct PackedRegion {
    file_offset: u64,
    buf: Vec<u8>,
    /// `(index into the caller's entry slice, where that entry landed)`.
    runs: Vec<(usize, ExtentRun)>,
    /// Index of the first entry that did not fit in this region.
    next: usize,
}

/// Mutable shard state behind a single `RwLock`.
struct ShardState {
    entries: HashMap<ExtentKey, ExtentRun>,
    region_sizes: Vec<u32>,
    writable_regions: Vec<u32>,
    num_regions: u32,
    tracker: RegionTracker,
    stats: ShardStats,
}

impl ShardState {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            region_sizes: Vec::new(),
            writable_regions: Vec::new(),
            num_regions: 0,
            tracker: RegionTracker::new(),
            stats: ShardStats::default(),
        }
    }

    /// Make a region available to pack into, by growing the file if the shard
    /// is below its cap, or by reclaiming the coldest unpinned regions if not.
    ///
    /// Returns `false` when every region is pinned and the file cannot grow, in
    /// which case the write is dropped — a cache miss later beats blocking a
    /// reader now.
    fn grow_or_evict(
        &mut self,
        file: &std::fs::File,
        max_regions: u32,
        region_pins: &[AtomicU32],
    ) -> std::io::Result<bool> {
        if self.num_regions < max_regions {
            let new_region = self.num_regions;
            file.set_len((new_region + 1) as u64 * REGION_SIZE)?;
            self.region_sizes.push(0);
            self.tracker.ensure_capacity(new_region as usize + 1);
            self.writable_regions.push(new_region);
            self.num_regions += 1;
            tracing::debug!(
                regions = self.num_regions,
                max = max_regions,
                "extent store shard grew"
            );
            return Ok(true);
        }

        let pinned: Vec<u32> = (0..self.num_regions)
            .filter(|r| {
                region_pins
                    .get(*r as usize)
                    .is_some_and(|p| p.load(Ordering::Acquire) > 0)
            })
            .collect();
        // Reclaiming a batch amortizes the sort over several admissions, but
        // never take more than a quarter of the shard in one pass: a flat batch
        // of three would discard the entire cache on a shard of three regions,
        // to make room for one extent.
        let budget = NUM_EVICTION_CANDIDATES.min((self.num_regions as usize / 4).max(1));
        let candidates = self.tracker.coldest(budget, &pinned);
        if candidates.is_empty() {
            tracing::warn!("extent store shard: every region pinned, dropping write");
            return Ok(false);
        }

        let before = self.entries.len();
        self.entries
            .retain(|_, run| !candidates.contains(&run.region));
        self.stats.extents_evicted += (before - self.entries.len()) as u64;
        self.stats.regions_evicted += candidates.len() as u64;

        for &r in &candidates {
            self.region_sizes[r as usize] = 0;
            self.tracker.scores[r as usize] = 0.0;
        }
        self.writable_regions.clone_from(&candidates);

        tracing::debug!(?candidates, "extent store shard reclaimed regions");
        Ok(true)
    }

    /// Fill one region with as many of `entries[from..]` as will fit.
    fn pack_region(
        &mut self,
        entries: &[(ExtentKey, Bytes)],
        from: usize,
        file: &std::fs::File,
        max_regions: u32,
        region_pins: &[AtomicU32],
    ) -> std::io::Result<Option<PackedRegion>> {
        loop {
            while self.writable_regions.is_empty() {
                if !self.grow_or_evict(file, max_regions, region_pins)? {
                    return Ok(None);
                }
            }

            let region = self.writable_regions[0];
            let region_start = self.region_sizes[region as usize];
            let available = REGION_SIZE as u32 - region_start;

            let mut buf = Vec::new();
            let mut runs = Vec::new();
            let mut written = 0u32;
            let mut j = from;

            while j < entries.len() {
                let size = entries[j].1.len() as u32;
                if written + size > available {
                    break;
                }
                runs.push((
                    j,
                    ExtentRun {
                        region,
                        offset_in_region: region_start + written,
                        size,
                        checksum: 0,
                    },
                ));
                buf.extend_from_slice(&entries[j].1);
                written += size;
                j += 1;
            }

            // Nothing fit in the tail of this region — retire it and try the next.
            if runs.is_empty() {
                self.tracker.region_filled(region);
                self.writable_regions.remove(0);
                continue;
            }

            self.region_sizes[region as usize] += written;
            return Ok(Some(PackedRegion {
                file_offset: region as u64 * REGION_SIZE + region_start as u64,
                buf,
                runs,
                next: j,
            }));
        }
    }
}

/// Counters for one shard.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ShardStats {
    /// Bytes written into this shard.
    pub bytes_written: u64,
    /// Bytes served out of this shard.
    pub bytes_read: u64,
    /// Extents admitted.
    pub extents_written: u64,
    /// Extents served.
    pub extents_read: u64,
    /// Lookups that found a run too short for the requested length.
    pub short_misses: u64,
    /// Writes skipped because a longer extent was already stored at the key.
    pub short_writes_skipped: u64,
    /// Extents discarded by region reclamation.
    pub extents_evicted: u64,
    /// Regions reclaimed.
    pub regions_evicted: u64,
    /// Reads rejected because the stored digest did not match.
    pub checksum_failures: u64,
}

/// A pinned, zero-copy view of one extent, safe to `sendfile` from.
///
/// Holding this keeps the extent's region pinned, so reclamation cannot
/// overwrite the bytes underneath an in-flight transfer. The pin is released on
/// drop — which is why the [`BlockHandle`] is borrowed rather than moved out:
/// handing the fd away while dropping the pin would reintroduce exactly the
/// race the pin exists to prevent.
pub struct PinnedExtent {
    handle: BlockHandle,
    shard: Arc<ShardFile>,
    region: u32,
}

impl PinnedExtent {
    /// The zero-copy handle to serve from. Valid for this guard's lifetime.
    pub fn handle(&self) -> &BlockHandle {
        &self.handle
    }

    /// Bytes this handle will serve.
    pub fn len(&self) -> u64 {
        self.handle.len
    }

    /// Whether the pinned range is empty.
    pub fn is_empty(&self) -> bool {
        self.handle.len == 0
    }
}

impl std::fmt::Debug for PinnedExtent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinnedExtent")
            .field("region", &self.region)
            .field("offset", &self.handle.offset)
            .field("len", &self.handle.len)
            .finish()
    }
}

impl Drop for PinnedExtent {
    fn drop(&mut self) {
        self.shard.unpin(self.region);
    }
}

/// One pre-allocated, region-divided shard file.
pub struct ShardFile {
    path: PathBuf,
    file: Arc<std::fs::File>,
    max_regions: u32,
    checksums_enabled: bool,
    state: RwLock<ShardState>,
    /// One pin counter per region, pre-allocated to `max_regions`.
    region_pins: Vec<AtomicU32>,
}

impl std::fmt::Debug for ShardFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.read().unwrap();
        f.debug_struct("ShardFile")
            .field("path", &self.path)
            .field("regions", &state.num_regions)
            .field("extents", &state.entries.len())
            .finish()
    }
}

impl ShardFile {
    /// Create (truncating any previous content) a shard file at `path`.
    ///
    /// Truncating is deliberate: run descriptors live only in memory, so a
    /// pre-existing file's bytes are unaddressable. This tier is cold after
    /// restart — see ADR 0005 §7.
    pub fn create(path: PathBuf, max_regions: u32, checksums_enabled: bool) -> Result<Arc<Self>> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        Ok(Arc::new(Self {
            path,
            file: Arc::new(file),
            max_regions,
            checksums_enabled,
            state: RwLock::new(ShardState::new()),
            region_pins: (0..max_regions).map(|_| AtomicU32::new(0)).collect(),
        }))
    }

    fn unpin(&self, region: u32) {
        if let Some(pin) = self.region_pins.get(region as usize) {
            pin.fetch_sub(1, Ordering::Release);
        }
    }

    /// Resolve a key to its run, taking a pin on the run's region.
    ///
    /// The pin is taken while the read lock is still held. Reclamation needs
    /// the write lock, so it cannot start until this returns — by which point
    /// the pin is visible and reclamation will skip the region.
    ///
    /// Returns `None` for an absent key, or for a run shorter than `len`. A
    /// short run means the extent was admitted from a smaller read and cannot
    /// satisfy this one; the caller refetches at the larger size and the longer
    /// extent supersedes it. This is the largest-wins rule at the disk tier.
    fn resolve_pinned(&self, key: &ExtentKey, len: u64) -> Option<ExtentRun> {
        let state = self.state.read().unwrap();
        let run = state.entries.get(key).copied()?;
        if (run.size as u64) < len {
            drop(state);
            self.state.write().unwrap().stats.short_misses += 1;
            return None;
        }
        self.region_pins[run.region as usize].fetch_add(1, Ordering::Relaxed);
        Some(run)
    }

    /// Credit a completed read against the region's score.
    fn record_read(&self, region: u32, bytes: u64) {
        let mut state = self.state.write().unwrap();
        state.tracker.region_read(region, bytes);
        state.tracker.tick();
        state.stats.bytes_read += bytes;
        state.stats.extents_read += 1;
    }

    /// Read an extent's bytes into userspace.
    ///
    /// Returns the **whole stored extent**, which may be longer than `len` —
    /// the caller slices to what it needs, and a tier above can promote the
    /// full range so its own copy is the largest one too. Contrast [`pin`],
    /// which yields exactly `len` because a `sendfile` writes straight to the
    /// socket.
    ///
    /// Blocking; callers reach it through the store, which runs it on the
    /// blocking pool.
    ///
    /// [`pin`]: ShardFile::pin
    pub fn get_bytes(&self, key: &ExtentKey, len: u64) -> Result<Option<Bytes>> {
        let Some(run) = self.resolve_pinned(key, len) else {
            return Ok(None);
        };

        let size = run.size as usize;
        let mut buf = vec![0u8; size];
        let read = self.file.read_exact_at(&mut buf, run.file_offset());
        self.unpin(run.region);

        if read.is_err() {
            return Ok(None);
        }

        if self.checksums_enabled && run.checksum != 0 {
            let actual = digest(&buf);
            if actual != run.checksum {
                self.state.write().unwrap().stats.checksum_failures += 1;
                tracing::error!(
                    path = %self.path.display(),
                    offset = run.file_offset(),
                    size,
                    stored = format_args!("{:#010x}", run.checksum),
                    actual = format_args!("{actual:#010x}"),
                    "extent store digest mismatch — treating as a miss"
                );
                // A miss, not an error: the caller refetches from origin and
                // the bad bytes are overwritten when the region is reclaimed.
                return Ok(None);
            }
        }

        self.record_read(run.region, size as u64);
        Ok(Some(Bytes::from(buf)))
    }

    /// Take a pinned zero-copy handle over `len` bytes of an extent, for
    /// `sendfile`.
    ///
    /// The returned guard must be held for the duration of the transfer. The fd
    /// is a `dup` of the shard file, so the guard owns its own descriptor.
    ///
    /// Checksums cannot be verified on this path — the bytes never enter
    /// userspace, which is the entire point. A deployment that wants
    /// verification on every read must use the bytes path.
    pub fn pin(self: &Arc<Self>, key: &ExtentKey, len: u64) -> Result<Option<PinnedExtent>> {
        let Some(run) = self.resolve_pinned(key, len) else {
            return Ok(None);
        };

        let serve = len.min(run.size as u64);
        let dup = match self.file.try_clone() {
            Ok(f) => f,
            Err(e) => {
                self.unpin(run.region);
                return Err(Error::Io(e));
            }
        };

        self.record_read(run.region, serve);
        Ok(Some(PinnedExtent {
            handle: BlockHandle::new(OwnedFd::from(dup), run.file_offset(), serve),
            shard: Arc::clone(self),
            region: run.region,
        }))
    }

    /// Pack and write a batch of extents.
    ///
    /// Entries are sorted by key first so extents that are adjacent in the
    /// object land adjacent in a region, which is what lets a multi-extent read
    /// coalesce into a single `sendfile` later.
    ///
    /// An entry whose key already holds an equal or longer extent is dropped
    /// before any bytes are written: under largest-wins a short read must never
    /// displace the longer range a big read already paid for.
    pub fn insert_many(&self, mut entries: Vec<(ExtentKey, Bytes)>) -> Result<()> {
        entries.retain(|(_, b)| !b.is_empty() && b.len() as u64 <= REGION_SIZE);
        {
            let mut state = self.state.write().unwrap();
            let before = entries.len();
            entries.retain(|(k, b)| {
                !state
                    .entries
                    .get(k)
                    .is_some_and(|old| old.size as usize >= b.len())
            });
            state.stats.short_writes_skipped += (before - entries.len()) as u64;
        }
        if entries.is_empty() {
            return Ok(());
        }
        entries.sort_by_key(|(k, _)| *k);

        let mut i = 0;
        while i < entries.len() {
            let packed = {
                let mut state = self.state.write().unwrap();
                match state.pack_region(
                    &entries,
                    i,
                    &self.file,
                    self.max_regions,
                    &self.region_pins,
                )? {
                    Some(p) => p,
                    None => return Ok(()),
                }
            };

            self.file.write_all_at(&packed.buf, packed.file_offset)?;

            {
                let mut state = self.state.write().unwrap();
                let mut bytes = 0u64;
                let count = packed.runs.len() as u64;
                for (idx, mut run) in packed.runs {
                    if self.checksums_enabled {
                        run.checksum = digest(&entries[idx].1);
                    }
                    bytes += entries[idx].1.len() as u64;
                    state.entries.insert(entries[idx].0, run);
                }
                state.stats.bytes_written += bytes;
                state.stats.extents_written += count;
            }

            i = packed.next;
        }
        Ok(())
    }

    /// Drop every extent belonging to any of `stream_ids`.
    ///
    /// The bytes stay on disk until the region is reclaimed; making them
    /// unaddressable is what matters for correctness.
    pub fn forget_streams(&self, stream_ids: &[u64]) -> u64 {
        if stream_ids.is_empty() {
            return 0;
        }
        let mut state = self.state.write().unwrap();
        let before = state.entries.len();
        state
            .entries
            .retain(|key, _| !stream_ids.contains(&key.stream_id));
        (before - state.entries.len()) as u64
    }

    /// Whether a key is currently addressable.
    pub fn contains(&self, key: &ExtentKey) -> bool {
        self.state.read().unwrap().entries.contains_key(key)
    }

    /// Number of addressable extents.
    pub fn extent_count(&self) -> u64 {
        self.state.read().unwrap().entries.len() as u64
    }

    /// Bytes currently allocated to regions in this shard.
    pub fn allocated_bytes(&self) -> u64 {
        let state = self.state.read().unwrap();
        state.region_sizes.iter().map(|&s| s as u64).sum()
    }

    /// Snapshot this shard's counters.
    pub fn stats(&self) -> ShardStats {
        self.state.read().unwrap().stats
    }

    /// Path of the backing file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn tmp_dir(tag: &str) -> PathBuf {
        let mut h = DefaultHasher::new();
        std::time::SystemTime::now().hash(&mut h);
        std::thread::current().id().hash(&mut h);
        tag.hash(&mut h);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "talon-extent-{}-{:x}",
            std::process::id(),
            h.finish()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn shard(dir: &Path, max_regions: u32, checksums: bool) -> Arc<ShardFile> {
        ShardFile::create(dir.join("extents_0.bin"), max_regions, checksums).unwrap()
    }

    fn key(stream: u64, offset: u64) -> ExtentKey {
        ExtentKey::new(stream, offset)
    }

    fn extent(byte: u8, len: usize) -> Bytes {
        Bytes::from(vec![byte; len])
    }

    #[test]
    fn insert_then_read_round_trips() {
        let dir = tmp_dir("roundtrip");
        let s = shard(&dir, 2, false);

        s.insert_many(vec![(key(0, 0), extent(0xAB, 4096))])
            .unwrap();
        let got = s.get_bytes(&key(0, 0), 4096).unwrap().unwrap();
        assert_eq!(got.len(), 4096);
        assert!(got.iter().all(|&b| b == 0xAB));

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn absent_key_is_a_miss() {
        let dir = tmp_dir("absent");
        let s = shard(&dir, 2, false);
        assert!(s.get_bytes(&key(9, 0), 16).unwrap().is_none());
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn extents_are_stored_at_their_exact_length() {
        // Nothing is rounded up to a page or a block: a 100-byte read costs
        // 100 bytes of a region. This is the reason the crate exists.
        let dir = tmp_dir("exact");
        let s = shard(&dir, 1, false);
        s.insert_many(vec![
            (key(0, 0), extent(1, 100)),
            (key(0, 100), extent(2, 7)),
            (key(0, 107), extent(3, 65_537)),
        ])
        .unwrap();

        assert_eq!(s.stats().bytes_written, 100 + 7 + 65_537);
        assert_eq!(s.allocated_bytes(), 100 + 7 + 65_537);
        assert_eq!(s.get_bytes(&key(0, 100), 7).unwrap().unwrap().len(), 7);

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_run_shorter_than_the_request_is_a_miss() {
        let dir = tmp_dir("short");
        let s = shard(&dir, 2, false);
        s.insert_many(vec![(key(0, 0), extent(1, 512))]).unwrap();

        assert!(s.get_bytes(&key(0, 0), 1024).unwrap().is_none());
        assert_eq!(s.stats().short_misses, 1);
        assert!(s.get_bytes(&key(0, 0), 512).unwrap().is_some());

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_longer_stored_extent_satisfies_a_shorter_read() {
        // The other half of largest-wins: one big fetch backs every smaller
        // read at the same offset.
        let dir = tmp_dir("prefix");
        let s = shard(&dir, 1, false);
        s.insert_many(vec![(key(0, 0), extent(0x7F, 8192))])
            .unwrap();

        let full = s.get_bytes(&key(0, 0), 1024).unwrap().unwrap();
        assert_eq!(
            full.len(),
            8192,
            "get_bytes returns the whole stored extent"
        );

        let pinned = s.pin(&key(0, 0), 1024).unwrap().unwrap();
        assert_eq!(pinned.len(), 1024, "pin serves exactly what was asked");

        drop(pinned);
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_shorter_write_never_displaces_a_longer_stored_extent() {
        let dir = tmp_dir("nodowngrade");
        let s = shard(&dir, 1, false);
        s.insert_many(vec![(key(0, 0), extent(0xAA, 4096))])
            .unwrap();
        s.insert_many(vec![(key(0, 0), extent(0xBB, 64))]).unwrap();

        assert_eq!(s.stats().short_writes_skipped, 1);
        let got = s.get_bytes(&key(0, 0), 4096).unwrap().unwrap();
        assert_eq!(got.len(), 4096);
        assert!(got.iter().all(|&b| b == 0xAA), "the long extent survived");

        // A longer write does replace it.
        s.insert_many(vec![(key(0, 0), extent(0xCC, 8192))])
            .unwrap();
        let bigger = s.get_bytes(&key(0, 0), 8192).unwrap().unwrap();
        assert_eq!(bigger.len(), 8192);
        assert!(bigger.iter().all(|&b| b == 0xCC));

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn many_extents_pack_into_one_region_and_all_read_back() {
        let dir = tmp_dir("pack");
        let s = shard(&dir, 1, false);
        let n = 64u64;
        let size = 64 * 1024usize;

        let entries: Vec<_> = (0..n)
            .map(|i| (key(0, i * size as u64), extent((i % 251) as u8, size)))
            .collect();
        s.insert_many(entries).unwrap();

        assert_eq!(s.stats().extents_written, n);
        assert_eq!(s.extent_count(), n);
        for i in 0..n {
            let got = s.get_bytes(&key(0, i * size as u64), size as u64).unwrap();
            let bytes = got.unwrap_or_else(|| panic!("extent {i} missing"));
            assert_eq!(bytes[0], (i % 251) as u8, "extent {i} wrong content");
        }

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn adjacent_extents_are_packed_adjacently() {
        // This is what lets a multi-extent read coalesce into one sendfile.
        let dir = tmp_dir("adjacent");
        let s = shard(&dir, 1, false);
        let size = 4096u64;

        // Insert out of order; insert_many sorts before packing.
        s.insert_many(vec![
            (key(0, 2 * size), extent(3, size as usize)),
            (key(0, 0), extent(1, size as usize)),
            (key(0, size), extent(2, size as usize)),
        ])
        .unwrap();

        let state = s.state.read().unwrap();
        let a = state.entries[&key(0, 0)];
        let b = state.entries[&key(0, size)];
        let c = state.entries[&key(0, 2 * size)];
        assert_eq!(a.region, b.region);
        assert_eq!(b.region, c.region);
        assert_eq!(b.file_offset(), a.file_offset() + size);
        assert_eq!(c.file_offset(), b.file_offset() + size);

        drop(state);
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn the_file_grows_a_region_at_a_time_up_to_the_cap() {
        let dir = tmp_dir("grow");
        let s = shard(&dir, 3, false);
        let size = (REGION_SIZE / 2) as usize;

        for i in 0..4u64 {
            s.insert_many(vec![(key(0, i * size as u64), extent(i as u8, size))])
                .unwrap();
        }

        let len = std::fs::metadata(s.path()).unwrap().len();
        assert!(len <= 3 * REGION_SIZE, "shard grew past its cap: {len}");
        assert!(len >= 2 * REGION_SIZE, "shard did not grow: {len}");

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn reclamation_frees_the_coldest_region() {
        let dir = tmp_dir("reclaim");
        let s = shard(&dir, 2, false);
        let size = REGION_SIZE as usize;

        // Fill both regions, one extent each.
        s.insert_many(vec![(key(0, 0), extent(1, size))]).unwrap();
        s.insert_many(vec![(key(0, 1), extent(2, size))]).unwrap();
        assert_eq!(s.extent_count(), 2);

        // Read region 0 repeatedly so region 1 is the colder candidate.
        for _ in 0..5 {
            s.get_bytes(&key(0, 0), size as u64).unwrap();
        }

        // A third extent has nowhere to go without reclaiming.
        s.insert_many(vec![(key(0, 2), extent(3, size))]).unwrap();

        assert!(s.stats().regions_evicted > 0, "expected a reclamation");
        assert!(
            s.contains(&key(0, 0)),
            "the hot region must survive reclamation"
        );

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn reclamation_never_empties_a_small_shard() {
        // A fixed batch of three candidates would discard every region of a
        // two-region shard to admit one extent, turning a cache into a
        // write-only device.
        let dir = tmp_dir("smallshard");
        let s = shard(&dir, 2, false);
        let size = REGION_SIZE as usize;

        for i in 0..6u64 {
            s.insert_many(vec![(key(0, i), extent(i as u8, size))])
                .unwrap();
        }

        assert!(s.stats().regions_evicted > 0, "expected reclamation");
        assert!(
            s.extent_count() > 0,
            "reclamation emptied the whole shard instead of its coldest region"
        );

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_pinned_region_is_never_reclaimed() {
        let dir = tmp_dir("pinned");
        let s = shard(&dir, 1, false);
        let size = (REGION_SIZE / 2) as usize;
        s.insert_many(vec![(key(0, 0), extent(0x5A, size))])
            .unwrap();

        // Hold a pin across a write that would otherwise reclaim this region.
        let pinned = s.pin(&key(0, 0), size as u64).unwrap().unwrap();
        s.insert_many(vec![(key(0, 1), extent(0xFF, size))])
            .unwrap();
        s.insert_many(vec![(key(0, 2), extent(0xEE, size))])
            .unwrap();

        // The pinned extent's bytes must be intact.
        let mut buf = vec![0u8; size];
        let h = pinned.handle();
        std::fs::File::from(h.fd.try_clone().unwrap())
            .read_exact_at(&mut buf, h.offset)
            .unwrap();
        assert!(
            buf.iter().all(|&b| b == 0x5A),
            "a pinned region was overwritten underneath a reader"
        );

        drop(pinned);
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn pin_yields_a_handle_at_the_run_offset() {
        let dir = tmp_dir("handle");
        let s = shard(&dir, 1, false);
        s.insert_many(vec![
            (key(0, 0), extent(1, 4096)),
            (key(0, 4096), extent(2, 4096)),
        ])
        .unwrap();

        let pinned = s.pin(&key(0, 4096), 4096).unwrap().unwrap();
        assert_eq!(pinned.len(), 4096);
        assert_eq!(
            pinned.handle().offset,
            4096,
            "the second extent follows the first"
        );

        drop(pinned);
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn dropping_a_pin_releases_the_region() {
        let dir = tmp_dir("unpin");
        let s = shard(&dir, 1, false);
        s.insert_many(vec![(key(0, 0), extent(1, 4096))]).unwrap();

        let pinned = s.pin(&key(0, 0), 4096).unwrap().unwrap();
        assert_eq!(s.region_pins[0].load(Ordering::Acquire), 1);
        drop(pinned);
        assert_eq!(s.region_pins[0].load(Ordering::Acquire), 0);

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_corrupt_extent_is_a_miss_when_checksums_are_on() {
        let dir = tmp_dir("crc");
        let s = shard(&dir, 1, true);
        s.insert_many(vec![(key(0, 0), extent(0xAB, 4096))])
            .unwrap();
        assert!(s.get_bytes(&key(0, 0), 4096).unwrap().is_some());

        // Corrupt the bytes on disk behind the store's back.
        std::fs::OpenOptions::new()
            .write(true)
            .open(s.path())
            .unwrap()
            .write_all_at(&vec![0xFF; 4096], 0)
            .unwrap();

        assert!(
            s.get_bytes(&key(0, 0), 4096).unwrap().is_none(),
            "corruption must surface as a miss, not as bad bytes"
        );
        assert_eq!(s.stats().checksum_failures, 1);

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn corruption_is_undetected_without_checksums() {
        // Documents the cost of the default: this is why the knob exists.
        let dir = tmp_dir("nocrc");
        let s = shard(&dir, 1, false);
        s.insert_many(vec![(key(0, 0), extent(0xAB, 4096))])
            .unwrap();

        std::fs::OpenOptions::new()
            .write(true)
            .open(s.path())
            .unwrap()
            .write_all_at(&vec![0xFF; 4096], 0)
            .unwrap();

        let got = s.get_bytes(&key(0, 0), 4096).unwrap().unwrap();
        assert!(got.iter().all(|&b| b == 0xFF));

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn forget_streams_makes_extents_unaddressable() {
        // How a republished object's bytes go away: the new version interns a
        // new stream id, and the old id's extents are forgotten wholesale.
        let dir = tmp_dir("forget");
        let s = shard(&dir, 1, false);
        s.insert_many(vec![
            (key(1, 0), extent(1, 4096)),
            (key(1, 4096), extent(2, 4096)),
            (key(2, 0), extent(3, 4096)),
        ])
        .unwrap();

        assert_eq!(s.forget_streams(&[1]), 2);
        assert!(!s.contains(&key(1, 0)));
        assert!(s.contains(&key(2, 0)), "unrelated streams survive");

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn empty_and_oversized_extents_are_skipped() {
        let dir = tmp_dir("bounds");
        let s = shard(&dir, 1, false);
        s.insert_many(vec![
            (key(0, 0), Bytes::new()),
            (key(0, 1), Bytes::from(vec![0u8; REGION_SIZE as usize + 1])),
        ])
        .unwrap();

        assert_eq!(s.stats().extents_written, 0);
        assert_eq!(s.extent_count(), 0);

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn stats_account_reads_and_writes_exactly() {
        let dir = tmp_dir("stats");
        let s = shard(&dir, 1, false);
        let size = 8192u64;
        let n = 5u64;

        let entries: Vec<_> = (0..n)
            .map(|i| (key(0, i * size), extent(i as u8, size as usize)))
            .collect();
        s.insert_many(entries).unwrap();

        let w = s.stats();
        assert_eq!(w.extents_written, n);
        assert_eq!(w.bytes_written, n * size);
        assert_eq!(w.extents_read, 0);

        for i in 0..n {
            s.get_bytes(&key(0, i * size), size).unwrap().unwrap();
        }
        let r = s.stats();
        assert_eq!(r.extents_read, n);
        assert_eq!(r.bytes_read, n * size);

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn tracker_picks_the_coldest_regions_and_skips_pinned() {
        let mut t = RegionTracker::new();
        t.ensure_capacity(5);
        t.region_read(0, 1_000_000);
        t.region_read(1, 1_000);
        t.region_read(3, 50_000);
        t.region_read(4, 500);
        // region 2 is untouched, so it is coldest.

        assert_eq!(t.coldest(3, &[]), vec![2, 4, 1]);
        assert_eq!(t.coldest(3, &[2, 4]), vec![1, 3, 0]);
    }

    #[test]
    fn tracker_decays_scores_on_schedule() {
        let mut t = RegionTracker::new();
        t.ensure_capacity(1);
        t.region_read(0, 1_000_000);
        for _ in 0..DECAY_INTERVAL {
            t.tick();
        }
        let expected = 1_000_000.0_f64 * DECAY_FACTOR;
        assert!((t.scores[0] - expected).abs() < 1.0);
    }

    #[test]
    fn run_file_offset_spans_regions() {
        let run = ExtentRun {
            region: 2,
            offset_in_region: 1024,
            size: 4096,
            checksum: 0,
        };
        assert_eq!(run.file_offset(), 2 * REGION_SIZE + 1024);
    }

    /// A shard file whose path cannot be created must report the error, not
    /// panic and not silently degrade to a cache that drops everything.
    #[test]
    fn creating_a_shard_on_an_unusable_path_fails() {
        let dir = tmp_dir("badpath");
        // A regular file, then a path *inside* it: ENOTDIR on open.
        let blocker = dir.join("not-a-directory");
        std::fs::write(&blocker, b"x").unwrap();

        let result = ShardFile::create(blocker.join("extents_0.bin"), 1, false);
        assert!(result.is_err(), "expected an error for an unusable path");

        std::fs::remove_dir_all(dir).ok();
    }

    /// Extents are variable length by design, so the packing arithmetic has to
    /// hold across sizes that differ by orders of magnitude — a 2KB footer and
    /// a 128KB column chunk land in the same region.
    #[test]
    fn extents_of_very_different_sizes_all_round_trip() {
        let dir = tmp_dir("sizes");
        let s = shard(&dir, 4, true);

        let small = 2048u64;
        let large = 128u64 << 10;
        for i in 0..8u64 {
            s.insert_many(vec![(
                key(1, i * small),
                extent((i * 17 % 256) as u8, small as usize),
            )])
            .unwrap();
        }
        for i in 0..4u64 {
            s.insert_many(vec![(
                key(2, i * large),
                extent((i * 31 % 256) as u8, large as usize),
            )])
            .unwrap();
        }

        for i in 0..8u64 {
            let got = s.get_bytes(&key(1, i * small), small).unwrap().unwrap();
            assert_eq!(got.len(), small as usize, "small extent {i} length");
            assert!(
                got.iter().all(|&b| b == (i * 17 % 256) as u8),
                "small extent {i} contents"
            );
        }
        for i in 0..4u64 {
            let got = s.get_bytes(&key(2, i * large), large).unwrap().unwrap();
            assert_eq!(got.len(), large as usize, "large extent {i} length");
            assert!(
                got.iter().all(|&b| b == (i * 31 % 256) as u8),
                "large extent {i} contents"
            );
        }

        std::fs::remove_dir_all(dir).ok();
    }

    /// Readers and writers run concurrently against one shard file. A reader
    /// must see either the whole extent or nothing — never a half-written one,
    /// and never bytes belonging to a different extent.
    ///
    /// This is the pread/pwrite race the region pin counts exist to prevent.
    #[test]
    fn concurrent_writers_and_readers_never_see_a_torn_extent() {
        let dir = tmp_dir("concurrent");
        let s = shard(&dir, 8, true);
        let size = 4096u64;
        let n = 64u64;
        let rounds = 8;

        std::thread::scope(|scope| {
            let writer = &s;
            scope.spawn(move || {
                for _ in 0..rounds {
                    for i in 0..n {
                        writer
                            .insert_many(vec![(
                                key(0, i * size),
                                extent((i % 256) as u8, size as usize),
                            )])
                            .unwrap();
                    }
                }
            });

            for _ in 0..3 {
                let reader = &s;
                scope.spawn(move || {
                    for _ in 0..rounds {
                        for i in 0..n {
                            if let Some(got) = reader.get_bytes(&key(0, i * size), size).unwrap() {
                                assert_eq!(got.len(), size as usize, "extent {i} torn length");
                                let want = (i % 256) as u8;
                                assert!(
                                    got.iter().all(|&b| b == want),
                                    "extent {i} contains bytes from another extent"
                                );
                            }
                        }
                    }
                });
            }
        });

        let st = s.stats();
        assert!(st.extents_written > 0, "the writer wrote nothing");
        assert_eq!(st.checksum_failures, 0, "a read observed a torn write");

        std::fs::remove_dir_all(dir).ok();
    }
}
