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
//! # Surviving a restart
//!
//! Run descriptors are what make packed bytes addressable, so a shard opened
//! without them holds 64 MiB regions of unreachable data. [`ShardFile::open`]
//! recovers them from a checkpoint written alongside the shard, and
//! [`ShardFile::checkpoint`] writes one. Three files per shard, as in Velox:
//!
//! ```text
//! extents_0.bin       the region file
//! extents_0.bin.cpt   the entry map, stream names, and region bookkeeping
//! extents_0.bin.log   regions reclaimed since that checkpoint
//! ```
//!
//! Recovery never fails a startup. A checkpoint that is absent, torn, or
//! written under a different configuration means a cold shard, which costs
//! refetches; refusing to start costs the whole cache.
//!
//! See ADR 0005 §4 and §7.

use std::collections::HashMap;
use std::os::fd::OwnedFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use talon_core::{BlockHandle, Error, Result};

use super::checkpoint::{self, CheckpointData, EvictionLog};
use super::ids::{Recovery, StreamIds};
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
    /// Regions reclaimed since the last checkpoint. `None` when checkpointing
    /// is disabled, in which case reclamation has nothing to record.
    log: Option<EvictionLog>,
    /// Bytes packed since the last checkpoint, against
    /// [`ShardFile::checkpoint_interval_bytes`].
    bytes_since_checkpoint: u64,
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
            log: None,
            bytes_since_checkpoint: 0,
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

        // Durable *before* the region is reused. The checkpoint on disk still
        // names entries in these regions; if the bytes were overwritten and the
        // process died before the next checkpoint, recovery would resurrect
        // descriptors pointing at unrelated data. The extent digest usually
        // catches that, but only when checksums are on and only off the
        // zero-copy path — not a guarantee to rest correctness on.
        //
        // One `fsync` of a few bytes per 64 MiB reclaimed. An `fsync` under the
        // write lock is a stall, and this is the deliberate place to take one:
        // reclamation already holds the lock and already discards a region.
        if let Some(log) = self.log.as_mut() {
            if let Err(error) = log.append(&candidates) {
                // Reusing the region anyway would be the unsafe direction: the
                // stale checkpoint would outlive the eviction unrecorded. Drop
                // the write instead and let the next admission try again.
                tracing::error!(
                    %error,
                    "extent store shard: eviction log append failed; refusing to reuse the region"
                );
                self.stats.checkpoint_errors += 1;
                return Ok(false);
            }
        }

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
    /// Checkpoints written successfully.
    pub checkpoints_written: u64,
    /// Checkpoints recovered at startup. Zero or one per shard.
    pub checkpoints_read: u64,
    /// Extents made addressable again by a recovered checkpoint.
    pub extents_recovered: u64,
    /// Checkpoint writes, reads, or eviction-log appends that failed.
    ///
    /// None of these are fatal — the worst case is a cold shard — but a
    /// sustained non-zero value means warm restart is not actually working.
    pub checkpoint_errors: u64,
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
    /// Bytes packed between checkpoints. Zero disables checkpointing, and with
    /// it the eviction log — a shard that never writes a checkpoint has nothing
    /// for a log to protect.
    checkpoint_interval_bytes: u64,
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

/// Checkpoint path for a shard file.
fn checkpoint_path(shard: &Path) -> PathBuf {
    let mut p = shard.as_os_str().to_os_string();
    p.push(".cpt");
    PathBuf::from(p)
}

/// Eviction-log path for a shard file.
fn log_path(shard: &Path) -> PathBuf {
    let mut p = shard.as_os_str().to_os_string();
    p.push(".log");
    PathBuf::from(p)
}

impl ShardFile {
    /// Create (truncating any previous content) a shard file at `path`.
    ///
    /// The cold-start constructor: run descriptors start empty, so any
    /// pre-existing bytes are unaddressable and keeping them would consume the
    /// capacity budget for nothing.
    pub fn create(path: PathBuf, max_regions: u32, checksums_enabled: bool) -> Result<Arc<Self>> {
        Self::open(path, max_regions, checksums_enabled, 0, None)
    }

    /// Open a shard file, recovering its extent map from a checkpoint when one
    /// is usable.
    ///
    /// Passing a [`Recovery`] opts into warm restart. Recovery is abandoned —
    /// and the shard truncated to a cold start — when the checkpoint is absent,
    /// torn, digest-mismatched, or was written under a configuration this shard
    /// does not share. Never an error: a cache that refuses to start is worse
    /// than a cache that starts empty.
    ///
    /// `checkpoint_interval_bytes` of zero disables checkpoint writing, which
    /// also means no eviction log. Recovery is still attempted if a `Recovery`
    /// is supplied, so the interval can be lowered to zero without discarding
    /// what a previous run left behind.
    pub fn open(
        path: PathBuf,
        max_regions: u32,
        checksums_enabled: bool,
        checkpoint_interval_bytes: u64,
        recovery: Option<&mut Recovery<'_>>,
    ) -> Result<Arc<Self>> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let mut state = ShardState::new();
        let mut recovered = false;
        if let Some(recovery) = recovery {
            match Self::recover(&path, &file, max_regions, checksums_enabled, recovery) {
                Ok(Some(from_checkpoint)) => {
                    state = from_checkpoint;
                    recovered = true;
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(
                        path = %path.display(),
                        %error,
                        "extent store shard: checkpoint unusable; starting cold"
                    );
                    state.stats.checkpoint_errors += 1;
                }
            }
        }

        // A shard that recovered nothing must not inherit the previous run's
        // bytes: nothing can address them, and they would count against the
        // capacity budget until reclamation happened to reach them.
        //
        // Keyed on whether recovery *ran*, not on whether it produced regions:
        // an empty-but-valid checkpoint is a successful recovery of a shard that
        // held nothing, and deleting it would make the next start report a
        // missing checkpoint instead.
        if !recovered {
            file.set_len(0)?;
            let _ = std::fs::remove_file(checkpoint_path(&path));
            let _ = std::fs::remove_file(log_path(&path));
        }

        if checkpoint_interval_bytes > 0 {
            match EvictionLog::open(log_path(&path)) {
                Ok(log) => state.log = Some(log),
                Err(error) => {
                    // Without a log, reclamation cannot invalidate the entries a
                    // stale checkpoint names, so checkpointing has to be off
                    // rather than merely unprotected.
                    tracing::error!(
                        path = %path.display(),
                        %error,
                        "extent store shard: cannot open eviction log; \
                         checkpointing disabled for this shard"
                    );
                    state.stats.checkpoint_errors += 1;
                }
            }
        }
        let checkpoint_interval_bytes = if state.log.is_some() {
            checkpoint_interval_bytes
        } else {
            0
        };

        Ok(Arc::new(Self {
            path,
            file: Arc::new(file),
            max_regions,
            checksums_enabled,
            checkpoint_interval_bytes,
            state: RwLock::new(state),
            region_pins: (0..max_regions).map(|_| AtomicU32::new(0)).collect(),
        }))
    }

    /// Rebuild shard state from a checkpoint, or `None` when there is none.
    fn recover(
        path: &Path,
        file: &std::fs::File,
        max_regions: u32,
        checksums_enabled: bool,
        recovery: &mut Recovery<'_>,
    ) -> Result<Option<ShardState>> {
        let cpt_path = checkpoint_path(path);
        let bytes = match std::fs::read(&cpt_path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(Error::Io(e)),
        };
        let data = checkpoint::decode(&bytes)
            .map_err(|e| Error::Other(format!("{}: {e}", cpt_path.display())))?;

        // Configuration checks, before any entry is trusted. Region indices only
        // mean something under the capacity they were written with, and a
        // checkpoint from the other checksum mode carries digests this shard
        // would either ignore or compare against nothing.
        if data.max_regions != max_regions {
            return Err(Error::Other(format!(
                "capacity changed: checkpoint holds {} regions per shard, configured for {}",
                data.max_regions, max_regions
            )));
        }
        if data.checksums_enabled != checksums_enabled {
            return Err(Error::Other(format!(
                "checksum mode changed: checkpoint was written with checksums {}",
                if data.checksums_enabled { "on" } else { "off" }
            )));
        }

        let num_regions = data.num_regions() as u32;
        let needed = num_regions as u64 * REGION_SIZE;
        if file.metadata()?.len() < needed {
            return Err(Error::Other(format!(
                "shard file is {} bytes, shorter than the {needed} the checkpoint describes",
                file.metadata()?.len()
            )));
        }

        // Regions reclaimed after the checkpoint was written. Their bytes have
        // been overwritten, so every entry naming one is a dangling descriptor.
        let evicted = EvictionLog::read(&log_path(path))?;
        let accepted = recovery.accept(&data.streams);

        let CheckpointData {
            mut region_sizes,
            region_scores,
            entries,
            ..
        } = data;

        let total = entries.len();
        let live: HashMap<ExtentKey, ExtentRun> = entries
            .into_iter()
            .filter(|(key, run)| {
                !evicted.contains(&run.region) && accepted.contains(&key.stream_id)
            })
            .collect();

        let mut tracker = RegionTracker::new();
        tracker.scores = region_scores;
        tracker.ensure_capacity(num_regions as usize);

        // Only the reclaimed regions are writable, as in Velox. The tail of a
        // partially filled region is given up rather than packed into: a
        // partially filled region may hold post-checkpoint extents this
        // recovery does not know about, and writing over them would corrupt
        // nothing but would make the region's high-water mark a lie.
        let mut writable: Vec<u32> = evicted
            .iter()
            .copied()
            .filter(|r| *r < num_regions)
            .collect();
        writable.sort_unstable();
        writable.dedup();
        for &r in &writable {
            region_sizes[r as usize] = 0;
            tracker.scores[r as usize] = 0.0;
        }

        tracing::info!(
            path = %path.display(),
            regions = num_regions,
            extents = live.len(),
            dropped = total - live.len(),
            reclaimed_regions = writable.len(),
            "extent store shard recovered from checkpoint"
        );

        Ok(Some(ShardState {
            stats: ShardStats {
                checkpoints_read: 1,
                extents_recovered: live.len() as u64,
                ..ShardStats::default()
            },
            entries: live,
            region_sizes,
            writable_regions: writable,
            num_regions,
            tracker,
            log: None,
            bytes_since_checkpoint: 0,
        }))
    }

    /// Write a checkpoint, making everything currently addressable recoverable.
    ///
    /// The ordering is Velox's, with one change — the checkpoint is written to a
    /// temporary name and renamed, rather than truncated in place, so the
    /// previous checkpoint stays valid until the new one is complete:
    ///
    /// 1. `fsync` the region file, so the checkpoint never names bytes that are
    ///    not durable;
    /// 2. write, `fsync`, and rename the checkpoint;
    /// 3. clear the eviction log, whose records the new checkpoint supersedes.
    ///
    /// Both crash windows leave a consistent pair: the old checkpoint with a
    /// full log, or the new checkpoint with a stale-but-harmless one — replaying
    /// evictions of regions the new checkpoint already excludes is idempotent.
    ///
    /// Runs under the write lock for its whole duration, which is what makes
    /// step 3 safe: an eviction slipping in between the snapshot and the clear
    /// would have its log record erased while the new checkpoint still named its
    /// entries.
    ///
    /// A no-op when checkpointing is disabled.
    pub fn checkpoint(&self, ids: &StreamIds) -> Result<()> {
        let mut state = self.state.write().unwrap();
        if state.log.is_none() {
            return Ok(());
        }

        self.file.sync_data()?;

        // The id table is the only thing that can turn a `stream_id` back into
        // an object, so it travels with the entry map. Only the streams this
        // shard actually refers to: the table is process-wide and most of it
        // belongs to other shards.
        let mut referenced: Vec<u64> = state.entries.keys().map(|k| k.stream_id).collect();
        referenced.sort_unstable();
        referenced.dedup();
        let streams = ids.names_of(&referenced);

        // An entry whose stream is no longer interned — released between the
        // two steps above — would recover into a key nothing can name. Drop it
        // here rather than write a checkpoint that recovery has to repair.
        let named: std::collections::HashSet<u64> = streams.iter().map(|(id, _)| *id).collect();
        let mut region_scores = state.tracker.scores.clone();
        region_scores.resize(state.region_sizes.len(), 0.0);

        let data = CheckpointData {
            max_regions: self.max_regions,
            checksums_enabled: self.checksums_enabled,
            region_sizes: state.region_sizes.clone(),
            region_scores,
            streams,
            entries: state
                .entries
                .iter()
                .filter(|(k, _)| named.contains(&k.stream_id))
                .map(|(k, v)| (*k, *v))
                .collect(),
        };

        let encoded = checkpoint::encode(&data);
        let cpt = checkpoint_path(&self.path);
        let tmp = {
            let mut p = cpt.as_os_str().to_os_string();
            p.push(".tmp");
            PathBuf::from(p)
        };

        let result = (|| -> std::io::Result<()> {
            let mut f = std::fs::File::create(&tmp)?;
            std::io::Write::write_all(&mut f, &encoded)?;
            f.sync_data()?;
            drop(f);
            std::fs::rename(&tmp, &cpt)
        })();
        if let Err(error) = result {
            let _ = std::fs::remove_file(&tmp);
            state.stats.checkpoint_errors += 1;
            return Err(Error::Io(error));
        }

        if let Some(log) = state.log.as_mut() {
            if let Err(error) = log.clear() {
                // The checkpoint is already durable and correct; a log that
                // still names those regions only costs recoverable extents next
                // time, so this is worth counting but not worth failing.
                tracing::warn!(
                    path = %self.path.display(),
                    %error,
                    "extent store shard: could not clear the eviction log"
                );
                state.stats.checkpoint_errors += 1;
            }
        }

        state.bytes_since_checkpoint = 0;
        state.stats.checkpoints_written += 1;
        tracing::debug!(
            path = %self.path.display(),
            extents = data.entries.len(),
            bytes = encoded.len(),
            "extent store shard checkpointed"
        );
        Ok(())
    }

    /// Checkpoint if enough has been written since the last one.
    ///
    /// Called after every admission batch. Byte-triggered rather than
    /// time-triggered so the cost tracks how much there is to lose: an idle
    /// shard never rewrites a checkpoint it has not invalidated.
    pub fn checkpoint_if_due(&self, ids: &StreamIds) -> Result<bool> {
        if self.checkpoint_interval_bytes == 0 {
            return Ok(false);
        }
        {
            let state = self.state.read().unwrap();
            if state.bytes_since_checkpoint < self.checkpoint_interval_bytes {
                return Ok(false);
            }
        }
        self.checkpoint(ids)?;
        Ok(true)
    }

    /// Whether this shard writes checkpoints.
    pub fn checkpointing(&self) -> bool {
        self.checkpoint_interval_bytes > 0
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
                state.bytes_since_checkpoint += bytes;
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
        // How an invalidated object's bytes go away: its stream id is released
        // and that id's extents are forgotten wholesale.
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

    // -----------------------------------------------------------------
    // Warm restart
    //
    // A restart is modelled by dropping a `ShardFile` and reopening the same
    // path, which is the whole of what a process boundary changes here — the
    // file survives, everything in memory does not.
    // -----------------------------------------------------------------

    const CPT_INTERVAL: u64 = REGION_SIZE;

    fn warm_shard(
        dir: &Path,
        max_regions: u32,
        checksums: bool,
        ids: &StreamIds,
    ) -> Arc<ShardFile> {
        let mut recovery = Recovery::new(ids);
        ShardFile::open(
            dir.join("extents_0.bin"),
            max_regions,
            checksums,
            CPT_INTERVAL,
            Some(&mut recovery),
        )
        .unwrap()
    }

    fn object(path: &str) -> talon_core::ObjectId {
        talon_core::ObjectId::new(talon_core::Backend::S3, "wh", path)
    }

    #[test]
    fn a_checkpointed_extent_reads_back_byte_exact_after_a_reopen() {
        let dir = tmp_dir("cpt-roundtrip");
        let ids = StreamIds::new();
        let id = ids.get_or_intern(&object("part-0.parquet"));

        {
            let s = warm_shard(&dir, 2, false, &ids);
            s.insert_many(vec![(key(id, 4096), extent(0x7E, 8192))])
                .unwrap();
            s.checkpoint(&ids).unwrap();
            assert_eq!(s.stats().checkpoints_written, 1);
        }

        let s = warm_shard(&dir, 2, false, &ids);
        let got = s.get_bytes(&key(id, 4096), 8192).unwrap().unwrap();
        assert_eq!(got.len(), 8192);
        assert!(got.iter().all(|&b| b == 0x7E), "recovered bytes differ");
        assert_eq!(s.stats().extents_recovered, 1);
        assert_eq!(s.stats().checkpoints_read, 1);

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn writes_after_the_last_checkpoint_are_not_recovered() {
        // The accepted loss, pinned so it is a decision rather than a surprise:
        // a crash discards everything written since the last checkpoint, which
        // is what bounds the checkpoint interval's cost.
        let dir = tmp_dir("cpt-window");
        let ids = StreamIds::new();
        let id = ids.get_or_intern(&object("part-0.parquet"));

        {
            let s = warm_shard(&dir, 2, false, &ids);
            s.insert_many(vec![(key(id, 0), extent(1, 512))]).unwrap();
            s.checkpoint(&ids).unwrap();
            s.insert_many(vec![(key(id, 512), extent(2, 512))]).unwrap();
        }

        let s = warm_shard(&dir, 2, false, &ids);
        assert!(
            s.contains(&key(id, 0)),
            "the checkpointed extent must survive"
        );
        assert!(!s.contains(&key(id, 512)), "the later write must be gone");

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn an_extent_in_a_logged_evicted_region_is_not_recovered() {
        // The eviction log's reason for existing. Region 0's bytes were
        // overwritten after the checkpoint named them, so recovering that
        // descriptor would read whatever landed there instead.
        let dir = tmp_dir("cpt-evicted");
        let ids = StreamIds::new();
        let id = ids.get_or_intern(&object("part-0.parquet"));

        {
            let s = warm_shard(&dir, 2, false, &ids);
            s.insert_many(vec![(key(id, 0), extent(1, 512))]).unwrap();
            s.checkpoint(&ids).unwrap();
        }
        // Reclamation of region 0, as the shard would have recorded it.
        {
            let mut log = EvictionLog::open(log_path(&dir.join("extents_0.bin"))).unwrap();
            log.append(&[0]).unwrap();
        }

        let s = warm_shard(&dir, 2, false, &ids);
        assert!(
            !s.contains(&key(id, 0)),
            "a reclaimed region must not recover"
        );
        assert_eq!(s.stats().extents_recovered, 0);

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_checkpoint_truncated_at_any_length_yields_a_cold_shard_not_a_bad_read() {
        // The crash-safety test at the file level: every prefix of a real
        // checkpoint must either be rejected outright or, if some prefix ever
        // did parse, never make an extent readable that is not byte-exact.
        let dir = tmp_dir("cpt-torn");
        let ids = StreamIds::new();
        let id = ids.get_or_intern(&object("part-0.parquet"));
        let cpt = {
            let s = warm_shard(&dir, 2, false, &ids);
            s.insert_many(vec![(key(id, 0), extent(0x33, 1024))])
                .unwrap();
            s.checkpoint(&ids).unwrap();
            std::fs::read(checkpoint_path(&dir.join("extents_0.bin"))).unwrap()
        };
        let region_file = std::fs::read(dir.join("extents_0.bin")).unwrap();
        let cpt_path = checkpoint_path(&dir.join("extents_0.bin"));

        for cut in 0..cpt.len() {
            std::fs::write(dir.join("extents_0.bin"), &region_file).unwrap();
            std::fs::write(&cpt_path, &cpt[..cut]).unwrap();

            let s = warm_shard(&dir, 2, false, &ids);
            match s.get_bytes(&key(id, 0), 1024).unwrap() {
                None => {}
                Some(got) => panic!(
                    "a checkpoint cut at {cut} bytes served {} bytes; \
                     a partial checkpoint must never be believed",
                    got.len()
                ),
            }
        }

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_checkpoint_written_with_checksums_off_is_rejected_when_they_are_on() {
        // The digests in the entry map mean different things in the two modes,
        // so adopting the map across a mode change would either verify against
        // zeros or skip verification silently.
        let dir = tmp_dir("cpt-checksum-mode");
        let ids = StreamIds::new();
        let id = ids.get_or_intern(&object("part-0.parquet"));

        {
            let s = warm_shard(&dir, 2, false, &ids);
            s.insert_many(vec![(key(id, 0), extent(4, 512))]).unwrap();
            s.checkpoint(&ids).unwrap();
        }

        let s = warm_shard(&dir, 2, true, &ids);
        assert!(!s.contains(&key(id, 0)));
        assert_eq!(s.stats().checkpoint_errors, 1);

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_capacity_change_is_rejected() {
        let dir = tmp_dir("cpt-capacity");
        let ids = StreamIds::new();
        let id = ids.get_or_intern(&object("part-0.parquet"));

        {
            let s = warm_shard(&dir, 2, false, &ids);
            s.insert_many(vec![(key(id, 0), extent(4, 512))]).unwrap();
            s.checkpoint(&ids).unwrap();
        }

        let s = warm_shard(&dir, 4, false, &ids);
        assert!(!s.contains(&key(id, 0)));
        assert_eq!(s.stats().checkpoints_read, 0);

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn region_scores_survive_a_checkpoint() {
        // Reclamation restarting from a flat distribution would evict a region
        // that is hot but young, which is the choice the decay scoring exists
        // to avoid.
        let dir = tmp_dir("cpt-scores");
        let ids = StreamIds::new();
        let id = ids.get_or_intern(&object("part-0.parquet"));

        {
            let s = warm_shard(&dir, 2, false, &ids);
            s.insert_many(vec![(key(id, 0), extent(1, 4096))]).unwrap();
            for _ in 0..20 {
                s.get_bytes(&key(id, 0), 4096).unwrap().unwrap();
            }
            s.checkpoint(&ids).unwrap();
        }

        let s = warm_shard(&dir, 2, false, &ids);
        let score = s.state.read().unwrap().tracker.scores[0];
        assert!(score > 0.0, "region 0 recovered with a score of {score}");

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_shard_with_no_checkpoint_starts_cold_and_truncates() {
        // Bytes with no descriptor are unaddressable; leaving them would charge
        // the capacity budget for a region nothing can read.
        let dir = tmp_dir("cpt-absent");
        let ids = StreamIds::new();
        let id = ids.get_or_intern(&object("part-0.parquet"));

        {
            let s = warm_shard(&dir, 2, false, &ids);
            s.insert_many(vec![(key(id, 0), extent(1, 4096))]).unwrap();
            // No checkpoint call.
        }

        let s = warm_shard(&dir, 2, false, &ids);
        assert!(!s.contains(&key(id, 0)));
        assert_eq!(s.allocated_bytes(), 0);
        assert_eq!(
            std::fs::metadata(dir.join("extents_0.bin")).unwrap().len(),
            0,
            "an unrecoverable shard file must be truncated"
        );

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn checkpointing_is_a_no_op_when_the_interval_is_zero() {
        let dir = tmp_dir("cpt-disabled");
        let ids = StreamIds::new();
        let id = ids.get_or_intern(&object("part-0.parquet"));

        let s = ShardFile::create(dir.join("extents_0.bin"), 2, false).unwrap();
        assert!(!s.checkpointing());
        s.insert_many(vec![(key(id, 0), extent(1, 512))]).unwrap();
        s.checkpoint(&ids).unwrap();

        assert_eq!(s.stats().checkpoints_written, 0);
        assert!(!checkpoint_path(&dir.join("extents_0.bin")).exists());
        assert!(!log_path(&dir.join("extents_0.bin")).exists());

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_checkpoint_is_written_once_the_interval_is_passed() {
        let dir = tmp_dir("cpt-due");
        let ids = StreamIds::new();
        let id = ids.get_or_intern(&object("part-0.parquet"));
        let mut recovery = Recovery::new(&ids);
        let s = ShardFile::open(
            dir.join("extents_0.bin"),
            2,
            false,
            4096,
            Some(&mut recovery),
        )
        .unwrap();

        s.insert_many(vec![(key(id, 0), extent(1, 2048))]).unwrap();
        assert!(!s.checkpoint_if_due(&ids).unwrap(), "2 KiB is under 4 KiB");

        s.insert_many(vec![(key(id, 2048), extent(2, 2048))])
            .unwrap();
        assert!(
            s.checkpoint_if_due(&ids).unwrap(),
            "4 KiB reaches the interval"
        );
        assert_eq!(s.stats().checkpoints_written, 1);

        // And the counter restarts, rather than checkpointing on every write
        // from here on.
        assert!(!s.checkpoint_if_due(&ids).unwrap());

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn an_empty_but_valid_checkpoint_is_a_successful_recovery() {
        // A shard that held nothing still recovers: deleting its checkpoint
        // would make the next start report a *missing* one, which reads as
        // damage rather than as an empty cache.
        let dir = tmp_dir("cpt-empty");
        let ids = StreamIds::new();

        {
            let s = warm_shard(&dir, 2, false, &ids);
            s.checkpoint(&ids).unwrap();
        }

        let s = warm_shard(&dir, 2, false, &ids);
        assert_eq!(s.stats().checkpoints_read, 1);
        assert_eq!(s.stats().checkpoint_errors, 0);
        assert!(checkpoint_path(&dir.join("extents_0.bin")).exists());

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn an_extent_whose_stream_was_released_is_not_checkpointed() {
        // A checkpointed key whose id is no longer interned would recover into
        // an entry nothing can name — dead weight against the capacity budget.
        let dir = tmp_dir("cpt-released");
        let ids = StreamIds::new();
        let live = ids.get_or_intern(&object("live.parquet"));
        let gone = ids.get_or_intern(&object("gone.parquet"));

        {
            let s = warm_shard(&dir, 2, false, &ids);
            s.insert_many(vec![
                (key(live, 0), extent(1, 512)),
                (key(gone, 0), extent(2, 512)),
            ])
            .unwrap();
            ids.release_object(&object("gone.parquet"));
            s.checkpoint(&ids).unwrap();
        }

        let s = warm_shard(&dir, 2, false, &ids);
        assert!(s.contains(&key(live, 0)));
        assert!(!s.contains(&key(gone, 0)));

        std::fs::remove_dir_all(dir).ok();
    }
}
