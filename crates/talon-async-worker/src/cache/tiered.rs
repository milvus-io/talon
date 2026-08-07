// SPDX-License-Identifier: Apache-2.0
//! The two-tier cache: DRAM over NVMe.
//!
//! A read consults L1; on a miss L1 runs a loader that consults L2; on a miss
//! there it goes to the origin. The result lands in L1, and reaches L2 only if
//! it earns admission.
//!
//! # Admission
//!
//! Writing every extent to NVMe would let one full scan evict a hot working set
//! it will never read again, and burn write endurance doing it. So L2 is fed
//! from L1's *evictions*, and only entries that accumulated at least
//! [`MIN_L1_HITS`] reads are admitted — a one-hit wonder is dropped.
//!
//! That policy has a hole in Talon specifically: L1 defaults to disabled
//! (`l1_capacity_bytes = 0`), and a disabled L1 never evicts, so a literal port
//! of the frequency gate would mean nothing ever reaches disk on a default
//! deployment. When L1 is off, extents are therefore admitted directly on first
//! miss — with no DRAM tier there is no hit count to gate on, and an unfiltered
//! NVMe cache is still far better than none. See ADR 0005 §5.
//!
//! # Writes never block reads
//!
//! "Write" here means a cache fill onto local NVMe — this worker is read-only
//! and never touches the origin. Admissions are staged into a buffer and
//! written by a spawned task, so the read returns immediately. Staging matters
//! because under direct admission each miss offers a single small extent, and
//! writing those one at a time would be a `pwrite` per extent instead of one
//! per packed region.
//!
//! A single in-flight write slot keeps concurrent admissions from stampeding
//! the disk; anything offered while a write runs stays staged for the next one.
//! Load is shed at the buffer's ceiling: past it, admissions are dropped and
//! counted, because a dropped admission costs one later miss while an unbounded
//! queue costs the memory this cache exists to bound.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use talon_core::{ObjectId, Result};

use super::ids::StreamIds;
use super::memory::{EvictionSink, MemoryCache, MemoryCacheStats};
use super::store::{ExtentStore, ExtentStoreConfig, ExtentStoreStats};
use super::ExtentKey;

/// A boxed loader future.
pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// L1 reads an extent must accumulate before it is worth a disk write.
///
/// Below this an entry is a one-hit wonder: admitting it would let a single
/// scan displace a hot working set on NVMe.
pub const MIN_L1_HITS: u32 = 3;

/// Bytes of staged admissions that trigger a flush to disk.
///
/// Direct admission offers one extent at a time, often a few KB. Writing each
/// straight through would be a `pwrite` per extent; staging into a batch means
/// one `pwrite` per packed region instead, which is what the region layout is
/// for.
const FLUSH_BYTES: u64 = 4 * 1024 * 1024;

/// Ceiling on the staging buffer.
///
/// This is where load is shed. If admissions arrive faster than the disk
/// retires them the excess is dropped and counted, rather than queued: a
/// dropped admission costs one later miss, while an unbounded queue costs the
/// memory this cache exists to bound.
const MAX_PENDING_BYTES: u64 = 64 * 1024 * 1024;

/// How to build a [`TieredExtentCache`].
#[derive(Debug, Clone)]
pub struct ExtentCacheConfig {
    /// L1 ceiling in bytes. Zero disables the DRAM tier, which switches L2 to
    /// direct admission.
    pub memory_bytes: u64,
    /// L1 shard count. Must be a power of two.
    pub memory_shards: usize,
    /// Directory for the L2 tier. `None` disables it.
    pub disk_dir: Option<PathBuf>,
    /// L2 ceiling in bytes.
    pub disk_bytes: u64,
    /// L2 shard count. Must be a power of two.
    pub disk_shards: usize,
    /// Verify a digest on every L2 read that passes through userspace.
    pub disk_checksums: bool,
    /// Bytes an L2 shard writes between checkpoints. Zero disables warm
    /// restart; the tier is then wiped at every start.
    pub checkpoint_interval_bytes: u64,
}

impl Default for ExtentCacheConfig {
    fn default() -> Self {
        Self {
            memory_bytes: 0,
            memory_shards: super::memory::DEFAULT_NUM_SHARDS,
            disk_dir: None,
            disk_bytes: 0,
            disk_shards: super::store::DEFAULT_NUM_SHARDS,
            disk_checksums: false,
            checkpoint_interval_bytes: 0,
        }
    }
}

/// Combined counters for both tiers.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ExtentCacheStats {
    /// Reads served from DRAM.
    pub memory_hits: u64,
    /// Reads that fell through DRAM.
    pub memory_misses: u64,
    /// DRAM entries reclaimed.
    pub memory_evictions: u64,
    /// DRAM entries dropped because a longer read superseded them.
    pub memory_stale_evictions: u64,
    /// Bytes currently in DRAM.
    pub memory_bytes: u64,
    /// Reads served from NVMe.
    pub disk_hits: u64,
    /// Bytes written to NVMe.
    pub disk_bytes_written: u64,
    /// NVMe lookups where the stored extent was too short.
    pub disk_short_misses: u64,
    /// Extents dropped by NVMe region reclamation.
    pub disk_extents_evicted: u64,
    /// Extents dropped on admission for not reaching [`MIN_L1_HITS`].
    pub admissions_rejected: u64,
    /// Extents dropped because the staging buffer was full.
    pub admissions_dropped: u64,
    /// Checkpoints written across every NVMe shard.
    pub checkpoints_written: u64,
    /// NVMe shards that recovered a checkpoint at startup.
    pub checkpoints_read: u64,
    /// Extents made addressable again by a recovered checkpoint.
    pub extents_recovered: u64,
    /// Checkpoint or eviction-log operations that failed.
    pub checkpoint_errors: u64,
}

/// Feeds extents to L2, applying the admission policy.
///
/// Every field the background write needs is its own `Arc`, so `admit` can
/// take `&self` — which is all [`EvictionSink::on_evicted`] provides — and
/// still hand owned handles to a spawned task.
#[derive(Debug)]
struct DiskWriter {
    disk: Arc<ExtentStore>,
    pending: Arc<Mutex<Vec<(ExtentKey, Bytes)>>>,
    pending_bytes: Arc<AtomicU64>,
    write_in_progress: Arc<AtomicBool>,
    rejected: AtomicU64,
    dropped: AtomicU64,
}

impl DiskWriter {
    fn new(disk: Arc<ExtentStore>) -> Arc<Self> {
        Arc::new(Self {
            disk,
            pending: Arc::new(Mutex::new(Vec::new())),
            pending_bytes: Arc::new(AtomicU64::new(0)),
            write_in_progress: Arc::new(AtomicBool::new(false)),
            rejected: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        })
    }

    /// Stage a batch for admission, flushing once enough has accumulated.
    fn admit(&self, entries: Vec<(ExtentKey, Bytes, u32)>) {
        let total = entries.len();
        let admitted: Vec<(ExtentKey, Bytes)> = entries
            .into_iter()
            .filter(|(_, _, num_uses)| *num_uses >= MIN_L1_HITS)
            .map(|(key, bytes, _)| (key, bytes))
            .collect();
        self.rejected
            .fetch_add((total - admitted.len()) as u64, Ordering::Relaxed);
        if admitted.is_empty() {
            return;
        }

        {
            let mut pending = self.pending.lock().unwrap();
            for (key, bytes) in admitted {
                if self.pending_bytes.load(Ordering::Relaxed) >= MAX_PENDING_BYTES {
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                self.pending_bytes
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                pending.push((key, bytes));
            }
        }

        self.flush_if(FLUSH_BYTES);
    }

    /// Write everything staged, if at least `threshold` bytes are waiting and no
    /// write is already in flight.
    fn flush_if(&self, threshold: u64) {
        if self.pending_bytes.load(Ordering::Relaxed) < threshold {
            return;
        }
        if self
            .write_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            // A write is running. This batch stays staged and goes out with the
            // next flush rather than being dropped.
            return;
        }

        let batch = std::mem::take(&mut *self.pending.lock().unwrap());
        if batch.is_empty() {
            self.write_in_progress.store(false, Ordering::Release);
            return;
        }
        let staged: u64 = batch.iter().map(|(_, b)| b.len() as u64).sum();
        self.pending_bytes.fetch_sub(staged, Ordering::Relaxed);

        let disk = Arc::clone(&self.disk);
        let flag = Arc::clone(&self.write_in_progress);
        tokio::spawn(async move {
            disk.insert_many(batch).await;
            flag.store(false, Ordering::Release);
        });
    }

    /// Write everything staged and park until it has landed.
    ///
    /// Each pass either drains the whole buffer or waits out the write holding
    /// it, so the loop makes progress and terminates.
    async fn drain(&self) {
        loop {
            while self.write_in_progress.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
            if self.pending_bytes.load(Ordering::Relaxed) == 0 {
                return;
            }
            self.flush_if(1);
        }
    }
}

impl EvictionSink for DiskWriter {
    fn on_evicted(&self, entries: Vec<(ExtentKey, Bytes, u32)>, _total_cache_bytes: u64) {
        self.admit(entries);
    }
}

/// DRAM over NVMe, keyed by [`ExtentKey`].
#[derive(Debug)]
pub struct TieredExtentCache {
    memory: Arc<MemoryCache>,
    disk: Option<Arc<ExtentStore>>,
    writer: Option<Arc<DiskWriter>>,
    /// True when L1 is off and L2 must admit on first miss instead of on
    /// eviction.
    direct_admission: bool,
    streams: Arc<StreamIds>,
}

impl TieredExtentCache {
    /// Build both tiers.
    ///
    /// # Errors
    /// If the L2 directory cannot be prepared.
    pub async fn new(config: &ExtentCacheConfig) -> Result<Arc<Self>> {
        // Built before the disk tier, not after: recovery binds checkpointed
        // stream ids into this table, and a table created afterwards would
        // allocate fresh ids that no recovered extent key refers to.
        let streams = Arc::new(StreamIds::new());

        let disk = match &config.disk_dir {
            Some(dir) if config.disk_bytes > 0 => Some(
                ExtentStore::new(
                    ExtentStoreConfig {
                        dir: dir.clone(),
                        max_bytes: config.disk_bytes,
                        num_shards: config.disk_shards,
                        checksums_enabled: config.disk_checksums,
                        checkpoint_interval_bytes: config.checkpoint_interval_bytes,
                    },
                    Arc::clone(&streams),
                )
                .await?,
            ),
            Some(_) => {
                tracing::warn!("extent cache disk dir set but disk_bytes is 0 — L2 disabled");
                None
            }
            None => None,
        };

        let writer = disk.as_ref().map(|d| DiskWriter::new(Arc::clone(d)));
        let direct_admission = config.memory_bytes == 0;

        // A disabled L1 never evicts, so hanging the sink off it would silently
        // starve L2. Direct admission takes over instead.
        let sink: Option<Arc<dyn EvictionSink>> = if direct_admission {
            None
        } else {
            writer.clone().map(|w| w as Arc<dyn EvictionSink>)
        };

        let memory =
            MemoryCache::with_eviction_sink(config.memory_bytes, config.memory_shards, sink);

        tracing::info!(
            memory_bytes = config.memory_bytes,
            disk_bytes = config.disk_bytes,
            direct_admission,
            "extent cache ready"
        );

        Ok(Arc::new(Self {
            memory,
            disk,
            writer,
            direct_admission,
            streams,
        }))
    }

    /// Resolve `object` to the stream id its extents are keyed by.
    ///
    /// The version is deliberately not part of this: see [`StreamIds`]. A
    /// republish therefore reuses the id and its cached extents keep
    /// answering, which is only correct because this worker assumes the
    /// objects it caches are immutable.
    pub fn intern(&self, object: &ObjectId) -> u64 {
        self.streams.get_or_intern(object)
    }

    /// Serve `len` bytes at `offset` of `stream_id`, loading on a miss.
    ///
    /// The loader runs only if neither tier has an extent at least `len` long
    /// at this offset. Concurrent readers of one key coalesce onto a single
    /// loader run.
    pub async fn get_or_load(
        &self,
        stream_id: u64,
        offset: u64,
        len: u64,
        loader: BoxFuture<'_, Result<Bytes>>,
    ) -> Result<Bytes> {
        let key = ExtentKey::new(stream_id, offset);

        let Some(disk) = self.disk.clone() else {
            return self.memory.get_or_load(key, len, loader).await;
        };

        let writer = self.writer.clone();
        let direct = self.direct_admission;
        let through_disk: BoxFuture<'_, Result<Bytes>> = Box::pin(async move {
            if let Some(bytes) = disk.get(key, len).await? {
                return Ok(bytes);
            }
            let bytes = loader.await?;
            if direct {
                // No L1 means no hit count to gate on. Synthesize one at the
                // threshold so the batch passes the same admission path.
                if let Some(w) = writer {
                    w.admit(vec![(key, bytes.clone(), MIN_L1_HITS)]);
                }
            }
            Ok(bytes)
        });

        self.memory.get_or_load(key, len, through_disk).await
    }

    /// Make every extent of `object` unreachable, returning how many were
    /// dropped.
    ///
    /// Nothing on the read path calls this. Under the immutability assumption
    /// there is no republish to react to, so this is the explicit escape
    /// hatch — for an outright delete, or for an operator who knows an object
    /// was overwritten and wants the worker to forget it. Without such a call
    /// a same-path overwrite is served from cache indefinitely; that is the
    /// documented behaviour, not an accident. See ADR 0005 §3.
    pub fn invalidate_object(&self, object: &ObjectId) -> u64 {
        let released = self.streams.release_object(object);
        self.forget(&released)
    }

    fn forget(&self, stream_ids: &[u64]) -> u64 {
        if stream_ids.is_empty() {
            return 0;
        }
        let dropped = self
            .disk
            .as_ref()
            .map(|d| d.forget_streams(stream_ids))
            .unwrap_or(0);
        tracing::debug!(streams = stream_ids.len(), dropped, "extents invalidated");
        dropped
    }

    /// The DRAM tier, for callers that need it directly.
    pub fn memory(&self) -> &Arc<MemoryCache> {
        &self.memory
    }

    /// The NVMe tier, if enabled.
    pub fn disk(&self) -> Option<&Arc<ExtentStore>> {
        self.disk.as_ref()
    }

    /// L1 counters alone.
    pub fn memory_stats(&self) -> MemoryCacheStats {
        self.memory.stats()
    }

    /// L2 counters alone.
    pub fn disk_stats(&self) -> ExtentStoreStats {
        self.disk.as_ref().map(|d| d.stats()).unwrap_or_default()
    }

    /// Both tiers' counters.
    pub fn stats(&self) -> ExtentCacheStats {
        let mem = self.memory.stats();
        let disk = self.disk_stats();
        let (rejected, dropped) = self
            .writer
            .as_ref()
            .map(|w| {
                (
                    w.rejected.load(Ordering::Relaxed),
                    w.dropped.load(Ordering::Relaxed),
                )
            })
            .unwrap_or((0, 0));

        ExtentCacheStats {
            memory_hits: mem.hits,
            memory_misses: mem.misses,
            memory_evictions: mem.evictions,
            memory_stale_evictions: mem.stale_evictions,
            memory_bytes: mem.current_bytes,
            disk_hits: disk.extents_read,
            disk_bytes_written: disk.bytes_written,
            disk_short_misses: disk.short_misses,
            disk_extents_evicted: disk.extents_evicted,
            admissions_rejected: rejected,
            admissions_dropped: dropped,
            checkpoints_written: disk.checkpoints_written,
            checkpoints_read: disk.checkpoints_read,
            extents_recovered: disk.extents_recovered,
            checkpoint_errors: disk.checkpoint_errors,
        }
    }

    /// Write out any staged admissions and park until they have landed.
    ///
    /// Admission is deliberately asynchronous and batched, so a caller that
    /// reads L2 right after filling L1 needs a barrier. Also useful before
    /// shutdown.
    pub async fn flush(&self) {
        if let Some(w) = &self.writer {
            w.drain().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::path::Path;
    use std::sync::atomic::AtomicUsize;
    use talon_core::Backend;

    fn tmp_root(tag: &str) -> PathBuf {
        let mut h = DefaultHasher::new();
        std::time::SystemTime::now().hash(&mut h);
        std::thread::current().id().hash(&mut h);
        tag.hash(&mut h);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "talon-tiered-{}-{:x}",
            std::process::id(),
            h.finish()
        ));
        p
    }

    fn config(root: &Path, memory_bytes: u64) -> ExtentCacheConfig {
        ExtentCacheConfig {
            memory_bytes,
            memory_shards: 1,
            disk_dir: Some(root.to_path_buf()),
            disk_bytes: super::super::region::REGION_SIZE * 4,
            disk_shards: 1,
            disk_checksums: false,
            checkpoint_interval_bytes: 0,
        }
    }

    /// A loader that counts how many times it actually ran.
    fn counting(calls: &Arc<AtomicUsize>, bytes: Bytes) -> BoxFuture<'static, Result<Bytes>> {
        let c = Arc::clone(calls);
        Box::pin(async move {
            c.fetch_add(1, Ordering::Relaxed);
            Ok(bytes)
        })
    }

    #[tokio::test]
    async fn a_miss_loads_and_a_hit_does_not() {
        let root = tmp_root("basic");
        let cache = TieredExtentCache::new(&config(&root, 1 << 20))
            .await
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));

        assert_eq!(
            cache
                .get_or_load(0, 0, 5, counting(&calls, Bytes::from_static(b"hello")))
                .await
                .unwrap(),
            Bytes::from_static(b"hello")
        );
        assert_eq!(
            cache
                .get_or_load(0, 0, 5, counting(&calls, Bytes::from_static(b"WRONG")))
                .await
                .unwrap(),
            Bytes::from_static(b"hello")
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_one_hit_wonder_is_not_written_to_disk() {
        // The whole point of the frequency gate: a scan that touches an extent
        // once must not displace a hot working set on NVMe.
        let root = tmp_root("onehit");
        let mut cfg = config(&root, 512 * 1024);
        cfg.memory_shards = 1;
        let cache = TieredExtentCache::new(&cfg).await.unwrap();

        let extent = 64 * 1024usize;
        for i in 0..32u64 {
            let data = Bytes::from(vec![i as u8; extent]);
            cache
                .get_or_load(
                    0,
                    i * extent as u64,
                    extent as u64,
                    Box::pin(async move { Ok(data) }),
                )
                .await
                .unwrap();
        }
        cache.flush().await;

        let s = cache.stats();
        assert!(s.memory_evictions > 0, "expected L1 pressure");
        assert!(
            s.admissions_rejected > 0,
            "one-hit wonders must be rejected"
        );
        assert_eq!(
            s.disk_bytes_written, 0,
            "nothing read more than once should have reached disk"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn an_extent_read_often_enough_is_written_to_disk() {
        let root = tmp_root("promote");
        let mut cfg = config(&root, 512 * 1024);
        cfg.memory_shards = 1;
        let cache = TieredExtentCache::new(&cfg).await.unwrap();

        let extent = 64 * 1024usize;
        let hot = Bytes::from(vec![0xAAu8; extent]);
        let data = hot.clone();
        cache
            .get_or_load(0, 0, extent as u64, Box::pin(async move { Ok(data) }))
            .await
            .unwrap();
        for _ in 0..MIN_L1_HITS + 2 {
            cache
                .get_or_load(
                    0,
                    0,
                    extent as u64,
                    Box::pin(async { panic!("must hit L1") }),
                )
                .await
                .unwrap();
        }

        // Now force it out of L1.
        for i in 1..32u64 {
            let data = Bytes::from(vec![i as u8; extent]);
            cache
                .get_or_load(
                    0,
                    i * extent as u64,
                    extent as u64,
                    Box::pin(async move { Ok(data) }),
                )
                .await
                .unwrap();
        }
        cache.flush().await;

        let disk = cache.disk().unwrap();
        assert!(
            disk.contains(&ExtentKey::new(0, 0)),
            "an extent read {} times should have been admitted",
            MIN_L1_HITS + 3
        );
        assert_eq!(
            disk.get(ExtentKey::new(0, 0), extent as u64).await.unwrap(),
            Some(hot)
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn with_l1_disabled_extents_are_admitted_on_first_miss() {
        // Talon's default. A disabled L1 never evicts, so gating admission on
        // L1 hits would leave NVMe permanently empty.
        let root = tmp_root("direct");
        let cache = TieredExtentCache::new(&config(&root, 0)).await.unwrap();

        let data = Bytes::from(vec![0x5Au8; 4096]);
        let want = data.clone();
        cache
            .get_or_load(0, 0, 4096, Box::pin(async move { Ok(data) }))
            .await
            .unwrap();
        cache.flush().await;

        let disk = cache.disk().unwrap();
        assert!(
            disk.contains(&ExtentKey::new(0, 0)),
            "with no L1, the first miss must reach disk"
        );

        // And the next read is served from disk without touching the loader.
        let got = cache
            .get_or_load(0, 0, 4096, Box::pin(async { panic!("must hit L2") }))
            .await
            .unwrap();
        assert_eq!(got, want);
        assert_eq!(cache.stats().disk_hits, 1);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_disk_hit_repopulates_l1() {
        let root = tmp_root("promote-back");
        let cache = TieredExtentCache::new(&config(&root, 1 << 20))
            .await
            .unwrap();

        // Seed L2 behind the cache's back.
        let data = Bytes::from(vec![0x33u8; 2048]);
        cache
            .disk()
            .unwrap()
            .insert_many(vec![(ExtentKey::new(4, 0), data.clone())])
            .await;

        let first = cache
            .get_or_load(4, 0, 2048, Box::pin(async { panic!("must hit L2") }))
            .await
            .unwrap();
        assert_eq!(first, data);

        // Second read comes from L1, so L2 is not touched again.
        let before = cache.stats().disk_hits;
        cache
            .get_or_load(4, 0, 2048, Box::pin(async { panic!("must hit L1") }))
            .await
            .unwrap();
        assert_eq!(
            cache.stats().disk_hits,
            before,
            "L1 should have absorbed it"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_longer_read_supersedes_both_tiers() {
        let root = tmp_root("largest");
        let cache = TieredExtentCache::new(&config(&root, 1 << 20))
            .await
            .unwrap();

        cache
            .disk()
            .unwrap()
            .insert_many(vec![(ExtentKey::new(5, 0), Bytes::from(vec![1u8; 512]))])
            .await;

        // 1024 cannot be served by the stored 512, so the loader runs.
        let calls = Arc::new(AtomicUsize::new(0));
        let got = cache
            .get_or_load(5, 0, 1024, counting(&calls, Bytes::from(vec![2u8; 1024])))
            .await
            .unwrap();

        assert_eq!(got.len(), 1024);
        assert!(got.iter().all(|&b| b == 2));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(cache.stats().disk_short_misses, 1);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_republish_shares_the_streams_extents_until_it_is_invalidated() {
        // The shape of the immutability assumption, end to end through
        // interning: nothing about an overwrite makes the old extents
        // unreachable, so the only way back to fresh bytes is an explicit
        // `invalidate_object`. No read-path caller makes that call.
        let root = tmp_root("versions");
        let cache = TieredExtentCache::new(&config(&root, 0)).await.unwrap();
        let obj = ObjectId::new(Backend::S3, "bucket", "part.parquet");

        let stream = cache.intern(&obj);
        cache
            .get_or_load(
                stream,
                0,
                3,
                Box::pin(async { Ok(Bytes::from_static(b"old")) }),
            )
            .await
            .unwrap();
        cache.flush().await;

        assert_eq!(cache.intern(&obj), stream, "a republish reuses the id");

        // The loader here would return "new", but it never runs: the extent
        // cached before the overwrite answers first. Served indefinitely, by
        // design — ADR 0005 §3.
        let stale = cache
            .get_or_load(
                stream,
                0,
                3,
                Box::pin(async { Ok(Bytes::from_static(b"new")) }),
            )
            .await
            .unwrap();
        assert_eq!(stale, Bytes::from_static(b"old"));

        cache.invalidate_object(&obj);
        let fresh = cache
            .get_or_load(
                cache.intern(&obj),
                0,
                3,
                Box::pin(async { Ok(Bytes::from_static(b"new")) }),
            )
            .await
            .unwrap();
        assert_eq!(fresh, Bytes::from_static(b"new"));

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn invalidating_an_object_frees_its_extents() {
        let root = tmp_root("invalidate");
        let cache = TieredExtentCache::new(&config(&root, 0)).await.unwrap();
        let obj = ObjectId::new(Backend::S3, "bucket", "part.parquet");

        let stream = cache.intern(&obj);
        for i in 0..3u64 {
            cache
                .get_or_load(
                    stream,
                    i * 64,
                    64,
                    Box::pin(async move { Ok(Bytes::from(vec![1u8; 64])) }),
                )
                .await
                .unwrap();
        }
        cache.flush().await;
        assert_eq!(cache.disk().unwrap().extent_count(), 3);

        let dropped = cache.invalidate_object(&obj);
        assert_eq!(dropped, 3);
        assert_eq!(cache.disk().unwrap().extent_count(), 0);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_cache_with_no_disk_still_serves() {
        let cache = TieredExtentCache::new(&ExtentCacheConfig {
            memory_bytes: 1 << 20,
            memory_shards: 1,
            ..Default::default()
        })
        .await
        .unwrap();

        let got = cache
            .get_or_load(0, 0, 2, Box::pin(async { Ok(Bytes::from_static(b"hi")) }))
            .await
            .unwrap();
        assert_eq!(got, Bytes::from_static(b"hi"));
        assert!(cache.disk().is_none());
        assert_eq!(cache.stats().disk_bytes_written, 0);
    }

    #[tokio::test]
    async fn both_tiers_disabled_is_a_pass_through() {
        let cache = TieredExtentCache::new(&ExtentCacheConfig::default())
            .await
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        for _ in 0..3 {
            cache
                .get_or_load(0, 0, 1, counting(&calls, Bytes::from_static(b"x")))
                .await
                .unwrap();
        }
        assert_eq!(calls.load(Ordering::Relaxed), 3, "nothing should be cached");
    }

    #[tokio::test]
    async fn concurrent_readers_of_one_extent_load_once() {
        let root = tmp_root("coalesce");
        let cache = TieredExtentCache::new(&config(&root, 1 << 20))
            .await
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            handles.push(tokio::spawn(async move {
                cache
                    .get_or_load(
                        9,
                        0,
                        4,
                        Box::pin(async move {
                            calls.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                            Ok(Bytes::from_static(b"data"))
                        }),
                    )
                    .await
            }));
        }
        for h in handles {
            assert_eq!(h.await.unwrap().unwrap(), Bytes::from_static(b"data"));
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn many_small_direct_admissions_all_reach_disk() {
        // With L1 off every miss is its own tiny admission. A single in-flight
        // write slot would drop most of them; staging batches them instead.
        let root = tmp_root("batched");
        let cache = TieredExtentCache::new(&config(&root, 0)).await.unwrap();

        let n = 200u64;
        for i in 0..n {
            cache
                .get_or_load(
                    i,
                    0,
                    128,
                    Box::pin(async move { Ok(Bytes::from(vec![i as u8; 128])) }),
                )
                .await
                .unwrap();
        }
        cache.flush().await;

        let disk = cache.disk().unwrap();
        assert_eq!(disk.extent_count(), n, "admissions were dropped");
        assert_eq!(cache.stats().disk_bytes_written, n * 128);
        assert_eq!(cache.stats().admissions_dropped, 0);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn stats_cover_both_tiers() {
        let root = tmp_root("stats");
        let cache = TieredExtentCache::new(&config(&root, 0)).await.unwrap();

        for i in 0..4u64 {
            cache
                .get_or_load(
                    i,
                    0,
                    128,
                    Box::pin(async move { Ok(Bytes::from(vec![i as u8; 128])) }),
                )
                .await
                .unwrap();
        }
        cache.flush().await;

        let s = cache.stats();
        assert_eq!(s.disk_bytes_written, 4 * 128);
        assert_eq!(s.admissions_rejected, 0, "direct admission rejects nothing");

        std::fs::remove_dir_all(root).ok();
    }
}
