// SPDX-License-Identifier: Apache-2.0
//! The L2 tier as a whole: a set of [`ShardFile`]s behind an async API.
//!
//! Sharding is by `stream_id`, so every extent of one object lands in one shard
//! file and a multi-extent read touches one fd. All file I/O runs on the
//! blocking pool — a `pread` against NVMe is microseconds, but microseconds on a
//! reactor thread is still a stall, and this worker's whole point is serving
//! many small reads concurrently.
//!
//! # Warm restart
//!
//! Each shard checkpoints its extent map periodically, so a restart recovers
//! what was on disk instead of discarding it. The directory is wiped only when
//! recovery is off or produced nothing — see
//! [`checkpoint`](super::checkpoint) and ADR 0005 §7.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use talon_core::{Error, Result};

use super::ids::{Recovery, StreamIds};
use super::region::{PinnedExtent, ShardFile, ShardStats, REGION_SIZE};
use super::ExtentKey;

/// Default number of shard files.
pub const DEFAULT_NUM_SHARDS: usize = 4;

/// How to lay out the on-disk tier.
#[derive(Debug, Clone)]
pub struct ExtentStoreConfig {
    /// Directory holding the shard files.
    pub dir: PathBuf,
    /// Ceiling on total bytes across all shards, rounded down to whole regions.
    pub max_bytes: u64,
    /// Number of shard files. Must be a power of two.
    pub num_shards: usize,
    /// Verify an xxh3 digest on every read that passes through userspace.
    pub checksums_enabled: bool,
    /// Bytes a shard writes between checkpoints. Zero disables checkpointing,
    /// and with it warm restart.
    pub checkpoint_interval_bytes: u64,
}

impl ExtentStoreConfig {
    /// Config with the default shard count, checksums off, and no checkpointing.
    pub fn new(dir: PathBuf, max_bytes: u64) -> Self {
        Self {
            dir,
            max_bytes,
            num_shards: DEFAULT_NUM_SHARDS,
            checksums_enabled: false,
            checkpoint_interval_bytes: 0,
        }
    }
}

/// Aggregate counters across every shard.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ExtentStoreStats {
    /// Bytes written to disk.
    pub bytes_written: u64,
    /// Bytes served from disk.
    pub bytes_read: u64,
    /// Extents admitted.
    pub extents_written: u64,
    /// Extents served.
    pub extents_read: u64,
    /// Lookups that found a stored extent too short to satisfy the read.
    pub short_misses: u64,
    /// Writes dropped because a longer extent was already stored.
    pub short_writes_skipped: u64,
    /// Extents discarded by region reclamation.
    pub extents_evicted: u64,
    /// Regions reclaimed.
    pub regions_evicted: u64,
    /// Reads rejected by a digest mismatch.
    pub checksum_failures: u64,
    /// Checkpoints written.
    pub checkpoints_written: u64,
    /// Shards that recovered a checkpoint at startup.
    pub checkpoints_read: u64,
    /// Extents made addressable again by recovery.
    pub extents_recovered: u64,
    /// Checkpoint or eviction-log operations that failed.
    pub checkpoint_errors: u64,
}

/// The on-disk extent tier.
#[derive(Debug)]
pub struct ExtentStore {
    shards: Vec<Arc<ShardFile>>,
    shard_mask: u64,
    streams: Arc<StreamIds>,
}

impl ExtentStore {
    /// Open the tier, recovering each shard's extent map when
    /// `checkpoint_interval_bytes` is non-zero and a usable checkpoint exists.
    ///
    /// `streams` is the process-wide id table. It is a parameter rather than
    /// something this type owns because recovery has to bind checkpointed ids
    /// into the *same* table the serve path interns against — a table built
    /// afterwards would allocate fresh ids and orphan everything recovered.
    ///
    /// # Errors
    /// If the directory cannot be created or a shard file cannot be opened. A
    /// checkpoint that cannot be read is not an error; that shard starts cold.
    ///
    /// # Panics
    /// If `num_shards` is zero or not a power of two.
    pub async fn new(config: ExtentStoreConfig, streams: Arc<StreamIds>) -> Result<Arc<Self>> {
        assert!(
            config.num_shards > 0 && config.num_shards.is_power_of_two(),
            "num_shards must be a positive power of two, got {}",
            config.num_shards
        );

        let bytes_per_shard = config.max_bytes / config.num_shards as u64;
        let max_regions = ((bytes_per_shard / REGION_SIZE).max(1)) as u32;
        let recover = config.checkpoint_interval_bytes > 0;

        let shards = {
            let streams = Arc::clone(&streams);
            blocking(move || {
                std::fs::create_dir_all(&config.dir)?;
                // One `Recovery` for the whole tier, not one per shard: it
                // enforces that an id names one object across every shard, and
                // a per-shard instance could not see the conflict.
                let mut recovery = Recovery::new(&streams);
                (0..config.num_shards)
                    .map(|i| {
                        ShardFile::open(
                            config.dir.join(format!("extents_{i}.bin")),
                            max_regions,
                            config.checksums_enabled,
                            config.checkpoint_interval_bytes,
                            recover.then_some(&mut recovery),
                        )
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .await?
        };

        let shard_mask = (shards.len() as u64) - 1;
        let recovered: u64 = shards.iter().map(|s| s.stats().extents_recovered).sum();
        tracing::info!(
            shards = shards.len(),
            max_regions_per_shard = max_regions,
            extents_recovered = recovered,
            "extent store ready"
        );
        Ok(Arc::new(Self {
            shards,
            shard_mask,
            streams,
        }))
    }

    #[inline]
    fn shard(&self, stream_id: u64) -> &Arc<ShardFile> {
        &self.shards[(stream_id & self.shard_mask) as usize]
    }

    /// Read an extent, if one at least `len` long is stored at `key`.
    ///
    /// Returns the **whole stored extent**, which may be longer than `len`, so
    /// a tier above can cache the largest range rather than re-shrinking it.
    pub async fn get(&self, key: ExtentKey, len: u64) -> Result<Option<Bytes>> {
        let shard = Arc::clone(self.shard(key.stream_id));
        blocking(move || shard.get_bytes(&key, len)).await
    }

    /// Take a pinned zero-copy handle over `len` bytes at `key`, for `sendfile`.
    ///
    /// The guard must be held for the duration of the transfer; dropping it
    /// releases the region back to reclamation.
    pub async fn pin(&self, key: ExtentKey, len: u64) -> Result<Option<PinnedExtent>> {
        let shard = Arc::clone(self.shard(key.stream_id));
        blocking(move || shard.pin(&key, len)).await
    }

    /// Admit a batch of extents, one concurrent write per shard touched.
    ///
    /// Write failures are logged and swallowed: a cache that cannot admit is
    /// slow, but a read that fails because a cache write failed is broken.
    pub async fn insert_many(&self, entries: Vec<(ExtentKey, Bytes)>) {
        if entries.is_empty() {
            return;
        }

        let mut by_shard: Vec<Vec<(ExtentKey, Bytes)>> = vec![Vec::new(); self.shards.len()];
        for (key, data) in entries {
            by_shard[(key.stream_id & self.shard_mask) as usize].push((key, data));
        }

        let mut tasks = Vec::new();
        for (shard, batch) in self.shards.iter().zip(by_shard) {
            if batch.is_empty() {
                continue;
            }
            let shard = Arc::clone(shard);
            let streams = Arc::clone(&self.streams);
            tasks.push(tokio::task::spawn_blocking(move || {
                if let Err(e) = shard.insert_many(batch) {
                    tracing::warn!(error = %e, "extent store batch write failed");
                }
                // In the same blocking task as the write it accounts for, so a
                // shard cannot outrun its own checkpoint and there is no
                // separate scheduler to keep alive.
                if let Err(e) = shard.checkpoint_if_due(&streams) {
                    tracing::warn!(error = %e, "extent store checkpoint failed");
                }
            }));
        }
        for task in tasks {
            if let Err(e) = task.await {
                tracing::warn!(error = %e, "extent store write task panicked");
            }
        }
    }

    /// Checkpoint every shard, regardless of how much it has written.
    ///
    /// For a controlled shutdown or an explicit operator request; the periodic
    /// path is [`ShardFile::checkpoint_if_due`], driven from
    /// [`insert_many`](Self::insert_many).
    pub async fn checkpoint_all(&self) -> Result<()> {
        let mut tasks = Vec::new();
        for shard in &self.shards {
            let shard = Arc::clone(shard);
            let streams = Arc::clone(&self.streams);
            tasks.push(tokio::task::spawn_blocking(move || {
                shard.checkpoint(&streams)
            }));
        }
        let mut first_error = None;
        for task in tasks {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "extent store shard checkpoint failed");
                    first_error.get_or_insert(e);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "extent store checkpoint task panicked");
                }
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Make every extent of `stream_ids` unaddressable, returning how many were
    /// dropped.
    ///
    /// Called when an object is explicitly invalidated. The bytes stay on disk
    /// until their region is reclaimed; unreachability is what matters.
    pub fn forget_streams(&self, stream_ids: &[u64]) -> u64 {
        self.shards
            .iter()
            .map(|s| s.forget_streams(stream_ids))
            .sum()
    }

    /// Whether an extent is currently addressable at `key`.
    pub fn contains(&self, key: &ExtentKey) -> bool {
        self.shard(key.stream_id).contains(key)
    }

    /// Total addressable extents.
    pub fn extent_count(&self) -> u64 {
        self.shards.iter().map(|s| s.extent_count()).sum()
    }

    /// Bytes currently packed into regions.
    pub fn allocated_bytes(&self) -> u64 {
        self.shards.iter().map(|s| s.allocated_bytes()).sum()
    }

    /// Number of shard files.
    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }

    /// Sum the per-shard counters.
    pub fn stats(&self) -> ExtentStoreStats {
        self.shards.iter().map(|s| s.stats()).fold(
            ExtentStoreStats::default(),
            |mut a, s: ShardStats| {
                a.bytes_written += s.bytes_written;
                a.bytes_read += s.bytes_read;
                a.extents_written += s.extents_written;
                a.extents_read += s.extents_read;
                a.short_misses += s.short_misses;
                a.short_writes_skipped += s.short_writes_skipped;
                a.extents_evicted += s.extents_evicted;
                a.regions_evicted += s.regions_evicted;
                a.checksum_failures += s.checksum_failures;
                a.checkpoints_written += s.checkpoints_written;
                a.checkpoints_read += s.checkpoints_read;
                a.extents_recovered += s.extents_recovered;
                a.checkpoint_errors += s.checkpoint_errors;
                a
            },
        )
    }
}

/// Run blocking file I/O off the reactor, flattening the join error.
async fn blocking<T, F>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::Other(format!("extent store blocking task failed: {e}")))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::path::Path;
    use talon_core::{Backend, ObjectId};

    fn tmp_root(tag: &str) -> PathBuf {
        let mut h = DefaultHasher::new();
        std::time::SystemTime::now().hash(&mut h);
        std::thread::current().id().hash(&mut h);
        tag.hash(&mut h);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "talon-store-{}-{:x}",
            std::process::id(),
            h.finish()
        ));
        p
    }

    async fn store(root: &Path, shards: usize) -> Arc<ExtentStore> {
        let mut cfg = ExtentStoreConfig::new(root.to_path_buf(), REGION_SIZE * shards as u64);
        cfg.num_shards = shards;
        ExtentStore::new(cfg, Arc::new(StreamIds::new()))
            .await
            .unwrap()
    }

    /// A store that checkpoints, over an id table the caller keeps so it can be
    /// handed to the reopened store — which is what a restart looks like from
    /// here, since the table is process-wide and outlives no process.
    async fn warm_store(
        root: &Path,
        shards: usize,
        streams: Arc<StreamIds>,
        interval: u64,
    ) -> Arc<ExtentStore> {
        let mut cfg = ExtentStoreConfig::new(root.to_path_buf(), REGION_SIZE * shards as u64);
        cfg.num_shards = shards;
        cfg.checkpoint_interval_bytes = interval;
        ExtentStore::new(cfg, streams).await.unwrap()
    }

    fn key(stream: u64, offset: u64) -> ExtentKey {
        ExtentKey::new(stream, offset)
    }

    #[tokio::test]
    async fn insert_then_get_round_trips() {
        let root = tmp_root("roundtrip");
        let s = store(&root, 2).await;

        s.insert_many(vec![(key(0, 0), Bytes::from_static(b"hello"))])
            .await;
        let got = s.get(key(0, 0), 5).await.unwrap().unwrap();
        assert_eq!(got, Bytes::from_static(b"hello"));

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn an_absent_extent_is_a_miss_not_an_error() {
        let root = tmp_root("absent");
        let s = store(&root, 2).await;
        assert!(s.get(key(7, 0), 16).await.unwrap().is_none());
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_stream_always_lands_in_one_shard() {
        // Every extent of one object shares an fd, so a multi-extent
        // read touches a single file.
        let root = tmp_root("affinity");
        let s = store(&root, 4).await;

        let entries: Vec<_> = (0..16u64)
            .map(|i| (key(3, i * 128), Bytes::from(vec![i as u8; 128])))
            .collect();
        s.insert_many(entries).await;

        let occupied = s.shards.iter().filter(|sh| sh.extent_count() > 0).count();
        assert_eq!(occupied, 1, "one stream spread across {occupied} shards");
        assert_eq!(s.extent_count(), 16);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn writes_fan_out_across_shards() {
        let root = tmp_root("fanout");
        let s = store(&root, 4).await;

        let entries: Vec<_> = (0..64u64)
            .map(|i| (key(i, 0), Bytes::from(vec![i as u8; 256])))
            .collect();
        s.insert_many(entries).await;

        assert_eq!(s.extent_count(), 64);
        for i in 0..64u64 {
            assert!(s.contains(&key(i, 0)), "stream {i} missing");
        }
        let busy = s.shards.iter().filter(|sh| sh.extent_count() > 0).count();
        assert_eq!(busy, 4, "64 streams should reach every shard");

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn get_returns_the_whole_stored_extent() {
        // So the tier above caches the largest range rather than re-shrinking
        // it to whatever this particular reader asked for.
        let root = tmp_root("whole");
        let s = store(&root, 1).await;
        s.insert_many(vec![(key(0, 0), Bytes::from(vec![9u8; 4096]))])
            .await;

        let got = s.get(key(0, 0), 64).await.unwrap().unwrap();
        assert_eq!(got.len(), 4096);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_read_longer_than_the_stored_extent_misses() {
        let root = tmp_root("shortmiss");
        let s = store(&root, 1).await;
        s.insert_many(vec![(key(0, 0), Bytes::from(vec![1u8; 512]))])
            .await;

        assert!(s.get(key(0, 0), 1024).await.unwrap().is_none());
        assert_eq!(s.stats().short_misses, 1);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn forgetting_a_stream_leaves_other_streams_intact() {
        let root = tmp_root("forget");
        let s = store(&root, 2).await;
        s.insert_many(vec![
            (key(1, 0), Bytes::from(vec![1u8; 64])),
            (key(1, 64), Bytes::from(vec![2u8; 64])),
            (key(2, 0), Bytes::from(vec![3u8; 64])),
        ])
        .await;

        assert_eq!(s.forget_streams(&[1]), 2);
        assert!(!s.contains(&key(1, 0)));
        assert!(s.contains(&key(2, 0)));

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn without_checkpointing_a_stale_directory_is_wiped_at_startup() {
        // With checkpointing off nothing wrote the run descriptors down, so
        // leftover bytes are unaddressable — keeping them would just leak disk.
        let root = tmp_root("coldstart");
        {
            let s = store(&root, 1).await;
            s.insert_many(vec![(key(0, 0), Bytes::from(vec![7u8; 4096]))])
                .await;
            assert_eq!(s.extent_count(), 1);
        }

        let fresh = store(&root, 1).await;
        assert_eq!(fresh.extent_count(), 0, "the tier must start cold");
        assert!(fresh.get(key(0, 0), 4096).await.unwrap().is_none());

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_checkpointed_tier_comes_back_warm() {
        // The whole feature, end to end at the tier level: write, checkpoint,
        // drop, reopen, read the same bytes without touching an origin.
        let root = tmp_root("warm");
        let streams = Arc::new(StreamIds::new());
        let body = Bytes::from(vec![0x5Au8; 4096]);
        {
            let s = warm_store(&root, 2, Arc::clone(&streams), REGION_SIZE).await;
            let object = ObjectId::new(Backend::S3, "wh", "part-0.parquet");
            let id = streams.get_or_intern(&object);
            s.insert_many(vec![(key(id, 0), body.clone())]).await;
            s.checkpoint_all().await.unwrap();
            assert_eq!(s.stats().checkpoints_written, 2, "one per shard");
        }

        let reopened = warm_store(&root, 2, Arc::clone(&streams), REGION_SIZE).await;
        let id = streams
            .peek(&ObjectId::new(Backend::S3, "wh", "part-0.parquet"))
            .expect("the id table must survive with the process");
        assert_eq!(reopened.extent_count(), 1);
        assert_eq!(reopened.get(key(id, 0), 4096).await.unwrap(), Some(body));
        assert_eq!(reopened.stats().extents_recovered, 1);
        assert_eq!(reopened.stats().checkpoints_read, 2);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn recovery_rebuilds_the_id_table_for_a_fresh_process() {
        // The real restart: nothing in memory survives, so the id an extent was
        // written under has to come back from the checkpoint or the extent is
        // unreachable however intact its bytes are.
        let root = tmp_root("warm-fresh");
        let object = ObjectId::new(Backend::Gcs, "wh", "dt=2026-08-07/part.parquet");
        let body = Bytes::from(vec![0x21u8; 2048]);
        {
            let streams = Arc::new(StreamIds::new());
            let s = warm_store(&root, 1, Arc::clone(&streams), REGION_SIZE).await;
            // Burn some ids first so the object's id is not 0, which would let
            // a broken recovery pass by coincidence.
            for i in 0..5 {
                streams.get_or_intern(&ObjectId::new(Backend::S3, "wh", format!("filler{i}")));
            }
            let id = streams.get_or_intern(&object);
            s.insert_many(vec![(key(id, 0), body.clone())]).await;
            s.checkpoint_all().await.unwrap();
        }

        let streams = Arc::new(StreamIds::new());
        let reopened = warm_store(&root, 1, Arc::clone(&streams), REGION_SIZE).await;
        let id = streams
            .peek(&object)
            .expect("the object must be re-interned");
        assert_eq!(reopened.get(key(id, 0), 2048).await.unwrap(), Some(body));
        // And a newly seen object must not land on the recovered id.
        let other = streams.get_or_intern(&ObjectId::new(Backend::S3, "wh", "new.parquet"));
        assert_ne!(other, id);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn a_capacity_change_starts_cold_rather_than_misreading_regions() {
        // Region indices only mean something under the capacity they were
        // written with, so a resized shard must not adopt the old map.
        let root = tmp_root("resize");
        let streams = Arc::new(StreamIds::new());
        {
            let s = warm_store(&root, 1, Arc::clone(&streams), REGION_SIZE).await;
            let id = streams.get_or_intern(&ObjectId::new(Backend::S3, "wh", "p.parquet"));
            s.insert_many(vec![(key(id, 0), Bytes::from(vec![1u8; 512]))])
                .await;
            s.checkpoint_all().await.unwrap();
        }

        let mut cfg = ExtentStoreConfig::new(root.clone(), REGION_SIZE * 4);
        cfg.num_shards = 1;
        cfg.checkpoint_interval_bytes = REGION_SIZE;
        let reopened = ExtentStore::new(cfg, Arc::clone(&streams)).await.unwrap();

        assert_eq!(reopened.extent_count(), 0, "the old map must be discarded");
        assert_eq!(reopened.stats().checkpoints_read, 0);
        assert_eq!(reopened.stats().checkpoint_errors, 1);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn turning_checkpointing_off_wipes_what_a_previous_run_left() {
        let root = tmp_root("warm-then-cold");
        let streams = Arc::new(StreamIds::new());
        let id = {
            let s = warm_store(&root, 1, Arc::clone(&streams), REGION_SIZE).await;
            let id = streams.get_or_intern(&ObjectId::new(Backend::S3, "wh", "p.parquet"));
            s.insert_many(vec![(key(id, 0), Bytes::from(vec![3u8; 512]))])
                .await;
            s.checkpoint_all().await.unwrap();
            id
        };

        let cold = store(&root, 1).await;
        assert_eq!(cold.extent_count(), 0);
        assert!(cold.get(key(id, 0), 512).await.unwrap().is_none());

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn stats_aggregate_across_shards() {
        let root = tmp_root("stats");
        let s = store(&root, 4).await;

        let entries: Vec<_> = (0..32u64)
            .map(|i| (key(i, 0), Bytes::from(vec![i as u8; 1024])))
            .collect();
        s.insert_many(entries).await;
        for i in 0..32u64 {
            s.get(key(i, 0), 1024).await.unwrap().unwrap();
        }

        let st = s.stats();
        assert_eq!(st.extents_written, 32);
        assert_eq!(st.extents_read, 32);
        assert_eq!(st.bytes_written, 32 * 1024);
        assert_eq!(st.bytes_read, 32 * 1024);
        assert_eq!(s.allocated_bytes(), 32 * 1024);

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    #[should_panic(expected = "power of two")]
    async fn a_non_power_of_two_shard_count_is_rejected() {
        let root = tmp_root("badshards");
        let mut cfg = ExtentStoreConfig::new(root, REGION_SIZE);
        cfg.num_shards = 6;
        let _ = ExtentStore::new(cfg, Arc::new(StreamIds::new())).await;
    }
}
