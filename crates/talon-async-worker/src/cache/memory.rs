// SPDX-License-Identifier: Apache-2.0
//! L1: a sharded, byte-bounded DRAM cache of extents.
//!
//! # Largest-wins entries
//!
//! An entry holds whatever byte range was fetched, and a lookup carries the
//! length the caller needs. A stored extent shorter than the request is
//! *stale*: it is dropped and reloaded at the larger size. So for a given
//! `(object, offset)` the cache converges on the largest range any reader has
//! asked for, and a shorter read is served as a prefix slice of it. Nothing is
//! rounded up to a page or a block, so nothing is over-fetched.
//!
//! # Load coalescing
//!
//! An entry is inserted in the `Loading` state before its loader runs. Readers
//! arriving meanwhile subscribe to a watch channel and park, so N concurrent
//! readers missing the same extent produce exactly one origin fetch. A failed
//! load publishes `Failed` and removes the entry, so waiters see the error and
//! the next caller retries with a fresh loader rather than inheriting a
//! poisoned entry.
//!
//! # Eviction
//!
//! A CLOCK sweep over a dense ring, scoring each entry `age / (1 + uses)` so a
//! frequently-read extent survives a one-shot neighbour of the same age. "Age"
//! is measured in reads, not seconds: each shard keeps a logical tick that
//! advances on every hit, so what makes an extent cold is how many other reads
//! have gone past it. Wall-clock time cannot separate entries inside a burst —
//! and a burst is exactly when eviction runs.
//!
//! The threshold deciding whether a swept entry is cold enough is recalibrated
//! from a periodic *sample* of the ring rather than a full scan, which keeps
//! eviction sub-linear in entry count — this cache holds far more, far smaller
//! entries than a block cache does, so a linear victim search per eviction
//! would not hold up.
//!
//! Entries that leave carry their hit count to an optional [`EvictionSink`],
//! which is what lets the NVMe tier admit only extents that earned their place
//! rather than everything a single scan touched. See ADR 0005 §5.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures_lite_stub::BoxFuture;
use tokio::sync::watch;

use talon_core::Result;

use super::ExtentKey;

/// Default shard count. Must be a power of two.
pub const DEFAULT_NUM_SHARDS: usize = 16;

/// Ring positions sampled when recalibrating the eviction threshold.
const NUM_EVICTION_SAMPLES: usize = 10;

/// Percentile of the sampled scores an entry must exceed to be evicted, so a
/// sweep reclaims the coldest slice rather than the first entry it meets.
const EVICTION_PERCENTILE: usize = 80;

/// Fixed-point numerator for the eviction score, so that dividing an age by a
/// use count keeps resolution instead of truncating small ages to zero.
const SCORE_SCALE: u64 = 1024;

/// A boxed future, kept local so the crate does not depend on `futures`.
mod futures_lite_stub {
    /// A pinned, boxed, `Send` future — the shape a loader closure returns.
    pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
}

/// Mix the key into a shard index. Two multiplicative constants so neighbouring
/// offsets of one object do not all land in the same shard.
#[inline]
fn shard_idx(key: &ExtentKey, shard_mask: u64) -> usize {
    let h = key
        .stream_id
        .wrapping_mul(11_400_714_819_323_198_485_u64)
        .wrapping_add(key.offset.wrapping_mul(6_364_136_223_846_793_005_u64));
    (h & shard_mask) as usize
}

#[derive(Clone)]
enum LoadState {
    Loading,
    Loaded(Bytes),
    Failed,
}

struct CacheEntry {
    key: ExtentKey,
    state_tx: watch::Sender<LoadState>,
    /// Shard tick at the last read. Zero means never read.
    last_use: AtomicU64,
    num_uses: AtomicU32,
    data_size: AtomicU64,
}

impl CacheEntry {
    fn new(key: ExtentKey) -> Arc<Self> {
        let (state_tx, _rx) = watch::channel(LoadState::Loading);
        Arc::new(Self {
            key,
            state_tx,
            last_use: AtomicU64::new(0),
            num_uses: AtomicU32::new(0),
            data_size: AtomicU64::new(0),
        })
    }

    /// Coldness: reads-since-last-touch, discounted by how often this entry has
    /// been read. Two extents last touched equally long ago rank by use count,
    /// so a hot extent outlives a one-shot neighbour of the same age. A
    /// never-read entry scores `u64::MAX` so it is always a candidate.
    fn eviction_score(&self, now: u64) -> u64 {
        let last = self.last_use.load(Ordering::Relaxed);
        if last == 0 {
            return u64::MAX;
        }
        now.saturating_sub(last).saturating_mul(SCORE_SCALE)
            / (1 + self.num_uses.load(Ordering::Relaxed) as u64)
    }

    fn touch(&self, now: u64) {
        self.last_use.store(now, Ordering::Relaxed);
        self.num_uses.fetch_add(1, Ordering::Relaxed);
    }
}

/// `(entry, is_new, bytes_freed, evicted)`.
type FindOrCreate = (Arc<CacheEntry>, bool, u64, Vec<(ExtentKey, Bytes, u32)>);

struct ShardInner {
    entries: HashMap<ExtentKey, Arc<CacheEntry>>,
    /// Sweep order. `None` marks a reclaimed slot, reused before growing.
    dense_ring: Vec<Option<Arc<CacheEntry>>>,
    empty_slots: Vec<usize>,
    clock_hand: usize,
    eviction_threshold: u64,
    loaded_bytes: u64,
    stats: ShardStats,
}

#[derive(Debug, Default, Clone, Copy)]
struct ShardStats {
    hits: u64,
    misses: u64,
    evictions: u64,
    stale_evictions: u64,
}

impl ShardInner {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            dense_ring: Vec::new(),
            empty_slots: Vec::new(),
            clock_hand: 0,
            eviction_threshold: 0,
            loaded_bytes: 0,
            stats: ShardStats::default(),
        }
    }

    fn ring_insert(&mut self, entry: Arc<CacheEntry>) {
        match self.empty_slots.pop() {
            Some(idx) => self.dense_ring[idx] = Some(entry),
            None => self.dense_ring.push(Some(entry)),
        }
    }

    fn ring_clear(&mut self, idx: usize) {
        self.dense_ring[idx] = None;
        self.empty_slots.push(idx);
    }

    /// Re-derive the eviction threshold from a stride sample of the ring.
    ///
    /// Sampling rather than scanning is what keeps eviction sub-linear. Entries
    /// still loading have no size yet and are skipped; if the whole sample is
    /// loading, the threshold drops to zero so any completed entry qualifies
    /// and the sweep can still make progress.
    fn calibrate_threshold(&mut self, now: u64) {
        let n = self.dense_ring.len();
        if n == 0 {
            self.eviction_threshold = 0;
            return;
        }
        let samples = NUM_EVICTION_SAMPLES.min(n);
        let step = (n / samples).max(1);

        let mut scores: Vec<u64> = (0..samples)
            .filter_map(|i| {
                self.dense_ring[(self.clock_hand + i * step) % n]
                    .as_ref()
                    .filter(|e| e.data_size.load(Ordering::Relaxed) > 0)
                    .map(|e| e.eviction_score(now))
            })
            .collect();

        if scores.is_empty() {
            self.eviction_threshold = 0;
            return;
        }
        scores.sort_unstable();
        let idx = (scores.len() * EVICTION_PERCENTILE / 100).min(scores.len() - 1);
        self.eviction_threshold = scores[idx];
    }

    /// Sweep the ring reclaiming cold entries until `target_bytes` are freed or
    /// the hand has been all the way round.
    fn evict(&mut self, now: u64, target_bytes: u64) -> (u64, Vec<(ExtentKey, Bytes, u32)>) {
        let n = self.dense_ring.len();
        if n == 0 {
            return (0, Vec::new());
        }

        let mut freed = 0u64;
        let mut swept = 0usize;
        let mut since_calibration = 0usize;
        let mut evicted = Vec::new();

        while swept < n {
            let idx = self.clock_hand % n;
            self.clock_hand = self.clock_hand.wrapping_add(1);
            swept += 1;

            let Some(entry) = self.dense_ring[idx].clone() else {
                continue;
            };
            since_calibration += 1;

            if self.eviction_threshold == 0 || since_calibration > n / 8 {
                self.calibrate_threshold(now);
                since_calibration = 0;
            }

            // Three owners are the map, the ring, and the clone above. More
            // means a caller is holding this entry — skip rather than evict a
            // live read out from under it.
            if Arc::strong_count(&entry) > 3 {
                continue;
            }

            let size = entry.data_size.load(Ordering::Relaxed);
            if size == 0 {
                continue; // still loading, or already reclaimed
            }
            if entry.eviction_score(now) < self.eviction_threshold {
                continue; // warmer than the cutoff
            }

            if self.entries.remove(&entry.key).is_some() {
                if let LoadState::Loaded(bytes) = entry.state_tx.borrow().clone() {
                    evicted.push((entry.key, bytes, entry.num_uses.load(Ordering::Relaxed)));
                }
                entry.data_size.store(0, Ordering::Relaxed);
                self.ring_clear(idx);
                self.loaded_bytes = self.loaded_bytes.saturating_sub(size);
                self.stats.evictions += 1;
                freed += size;
                if freed >= target_bytes {
                    break;
                }
            }
        }
        (freed, evicted)
    }
}

struct Shard {
    inner: Mutex<ShardInner>,
    byte_limit: u64,
    /// Monotonic read counter, this shard's notion of "now".
    ///
    /// A wall clock would be both more expensive — a `SystemTime::now()` on
    /// every hit — and less meaningful: what makes an extent cold is how many
    /// other reads have gone past it, not how many seconds have elapsed. Under
    /// a burst, milliseconds cannot separate entries at all; ticks always can.
    /// Per-shard rather than global so eviction never contends across shards,
    /// and because a shard only ever compares its own entries.
    clock: AtomicU64,
}

impl Shard {
    fn new(byte_limit: u64) -> Self {
        Self {
            inner: Mutex::new(ShardInner::new()),
            byte_limit,
            clock: AtomicU64::new(0),
        }
    }

    /// Advance and return the clock. Ticks start at 1, leaving 0 as the
    /// never-read sentinel.
    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn now(&self) -> u64 {
        self.clock.load(Ordering::Relaxed)
    }

    /// Find a usable entry for `key`, or register a new one to load into.
    ///
    /// An entry whose stored extent is shorter than `min_size` cannot satisfy
    /// this read, so it is dropped as stale and treated as a miss — this is the
    /// largest-wins rule.
    fn find_or_create(&self, key: &ExtentKey, min_size: u64) -> FindOrCreate {
        let now = self.now();
        let mut inner = self.inner.lock().unwrap();

        if let Some(entry) = inner.entries.get(key).cloned() {
            let cached = entry.data_size.load(Ordering::Relaxed);
            if cached > 0 && cached < min_size {
                if let Some(stale) = inner.entries.remove(key) {
                    let size = stale.data_size.swap(0, Ordering::Relaxed);
                    inner.loaded_bytes = inner.loaded_bytes.saturating_sub(size);
                    inner.stats.stale_evictions += 1;
                    inner.stats.evictions += 1;
                }
            } else {
                inner.stats.hits += 1;
                return (entry, false, 0, Vec::new());
            }
        }

        // Reclaim in batches rather than exactly-enough, so a steady stream of
        // inserts does not pay a sweep every time.
        let (freed, evicted) = if inner.loaded_bytes >= self.byte_limit {
            let overage = inner.loaded_bytes - self.byte_limit;
            let batch = self.byte_limit / 5;
            inner.evict(now, overage.max(batch))
        } else {
            (0, Vec::new())
        };

        inner.stats.misses += 1;
        let entry = CacheEntry::new(*key);
        inner.entries.insert(*key, Arc::clone(&entry));
        inner.ring_insert(Arc::clone(&entry));
        (entry, true, freed, evicted)
    }

    fn record_loaded(&self, size: u64) {
        self.inner.lock().unwrap().loaded_bytes += size;
    }

    fn remove(&self, key: &ExtentKey) -> Option<u64> {
        let mut inner = self.inner.lock().unwrap();
        let entry = inner.entries.remove(key)?;
        let size = entry.data_size.swap(0, Ordering::Relaxed);
        inner.loaded_bytes = inner.loaded_bytes.saturating_sub(size);
        Some(size)
    }

    fn stats(&self) -> ShardStats {
        self.inner.lock().unwrap().stats
    }
}

/// Receives extents evicted from L1, with the hit count each accumulated.
///
/// The NVMe tier implements this to admit only extents that were read often
/// enough to be worth a write, so one scan over cold data cannot displace a hot
/// working set on disk.
pub trait EvictionSink: Send + Sync + std::fmt::Debug {
    /// Called with a batch of evicted entries and the cache's current size.
    fn on_evicted(&self, entries: Vec<(ExtentKey, Bytes, u32)>, total_cache_bytes: u64);
}

/// Snapshot of L1 counters.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MemoryCacheStats {
    /// Lookups served from DRAM.
    pub hits: u64,
    /// Lookups that had to load.
    pub misses: u64,
    /// Entries reclaimed, including stale ones.
    pub evictions: u64,
    /// Entries dropped because a longer read superseded them.
    pub stale_evictions: u64,
    /// Bytes currently held.
    pub current_bytes: u64,
    /// Configured ceiling.
    pub max_bytes: u64,
}

/// A sharded, byte-bounded DRAM cache of extents.
pub struct MemoryCache {
    shards: Vec<Shard>,
    shard_mask: u64,
    max_bytes: u64,
    total_bytes: AtomicU64,
    eviction_sink: Option<Arc<dyn EvictionSink>>,
}

impl std::fmt::Debug for MemoryCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryCache")
            .field("max_bytes", &self.max_bytes)
            .field("current_bytes", &self.total_bytes.load(Ordering::Relaxed))
            .field("shards", &self.shards.len())
            .finish()
    }
}

impl MemoryCache {
    /// Create a cache with the default shard count. `max_bytes == 0` disables it.
    pub fn new(max_bytes: u64) -> Arc<Self> {
        Self::with_shards(max_bytes, DEFAULT_NUM_SHARDS)
    }

    /// Create a cache with an explicit shard count.
    pub fn with_shards(max_bytes: u64, num_shards: usize) -> Arc<Self> {
        Self::with_eviction_sink(max_bytes, num_shards, None)
    }

    /// Create a cache that forwards evicted extents to `eviction_sink`.
    ///
    /// # Panics
    /// If `num_shards` is zero or not a power of two.
    pub fn with_eviction_sink(
        max_bytes: u64,
        num_shards: usize,
        eviction_sink: Option<Arc<dyn EvictionSink>>,
    ) -> Arc<Self> {
        assert!(num_shards > 0, "num_shards must be positive");
        assert!(
            num_shards.is_power_of_two(),
            "num_shards must be a power of two, got {num_shards}"
        );
        let per_shard = max_bytes / num_shards as u64;
        Arc::new(Self {
            shards: (0..num_shards).map(|_| Shard::new(per_shard)).collect(),
            shard_mask: (num_shards as u64) - 1,
            max_bytes,
            total_bytes: AtomicU64::new(0),
            eviction_sink,
        })
    }

    /// Whether this cache stores anything.
    pub fn is_enabled(&self) -> bool {
        self.max_bytes > 0
    }

    /// Snapshot the counters.
    pub fn stats(&self) -> MemoryCacheStats {
        let totals = self.shards.iter().fold(ShardStats::default(), |mut a, s| {
            let st = s.stats();
            a.hits += st.hits;
            a.misses += st.misses;
            a.evictions += st.evictions;
            a.stale_evictions += st.stale_evictions;
            a
        });
        MemoryCacheStats {
            hits: totals.hits,
            misses: totals.misses,
            evictions: totals.evictions,
            stale_evictions: totals.stale_evictions,
            current_bytes: self.total_bytes.load(Ordering::Relaxed),
            max_bytes: self.max_bytes,
        }
    }

    /// Serve `len` bytes at `key`, running `loader` if nothing usable is cached.
    ///
    /// Concurrent callers for the same key coalesce: the first runs `loader`,
    /// the rest wait and share its result. A cached extent longer than `len` is
    /// served as a prefix slice.
    pub async fn get_or_load(
        &self,
        key: ExtentKey,
        len: u64,
        loader: BoxFuture<'_, Result<Bytes>>,
    ) -> Result<Bytes> {
        if !self.is_enabled() {
            return loader.await;
        }
        let (entry, is_new) = self.find_or_create(&key, len);
        if is_new {
            return self.load_exclusive(&key, len, &entry, loader).await;
        }
        self.wait_for(&key, len, &entry).await.ok_or_else(|| {
            talon_core::Error::Other(format!("concurrent load of {key} failed; retry the read"))
        })
    }

    async fn load_exclusive(
        &self,
        key: &ExtentKey,
        len: u64,
        entry: &Arc<CacheEntry>,
        loader: BoxFuture<'_, Result<Bytes>>,
    ) -> Result<Bytes> {
        match loader.await {
            Ok(bytes) => {
                let shard = self.shard(key);
                let size = bytes.len() as u64;
                entry.data_size.store(size, Ordering::Release);
                entry.touch(shard.tick());
                entry
                    .state_tx
                    .send_replace(LoadState::Loaded(bytes.clone()));
                shard.record_loaded(size);
                self.total_bytes.fetch_add(size, Ordering::Relaxed);
                tracing::trace!(%key, size, "L1 miss loaded");
                Ok(bytes.slice(0..len.min(size) as usize))
            }
            Err(error) => {
                // Publish the failure before removing, so parked waiters wake
                // and see it rather than blocking on a dropped sender.
                entry.state_tx.send_replace(LoadState::Failed);
                self.remove_entry(key);
                Err(error)
            }
        }
    }

    async fn wait_for(&self, key: &ExtentKey, len: u64, entry: &Arc<CacheEntry>) -> Option<Bytes> {
        let mut rx = entry.state_tx.subscribe();
        loop {
            let state = rx.borrow_and_update().clone();
            match state {
                LoadState::Loaded(bytes) => {
                    entry.touch(self.shard(key).tick());
                    tracing::trace!(%key, size = bytes.len(), "L1 hit");
                    return Some(bytes.slice(0..len.min(bytes.len() as u64) as usize));
                }
                LoadState::Failed => return None,
                LoadState::Loading => rx.changed().await.ok()?,
            }
        }
    }

    #[inline]
    fn shard(&self, key: &ExtentKey) -> &Shard {
        &self.shards[shard_idx(key, self.shard_mask)]
    }

    fn find_or_create(&self, key: &ExtentKey, min_size: u64) -> (Arc<CacheEntry>, bool) {
        let (entry, is_new, freed, evicted) = self.shard(key).find_or_create(key, min_size);
        if freed > 0 {
            self.subtract(freed);
        }
        if !evicted.is_empty() {
            if let Some(sink) = &self.eviction_sink {
                sink.on_evicted(evicted, self.total_bytes.load(Ordering::Relaxed));
            }
        }
        (entry, is_new)
    }

    fn remove_entry(&self, key: &ExtentKey) {
        if let Some(size) = self.shard(key).remove(key) {
            if size > 0 {
                self.subtract(size);
            }
        }
    }

    fn subtract(&self, bytes: u64) {
        self.total_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(bytes))
            })
            .ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn key(stream: u64, offset: u64) -> ExtentKey {
        ExtentKey::new(stream, offset)
    }

    fn loader(bytes: &'static [u8]) -> BoxFuture<'static, Result<Bytes>> {
        Box::pin(async move { Ok(Bytes::from_static(bytes)) })
    }

    #[tokio::test]
    async fn miss_loads_then_hit_serves_without_the_loader() {
        let cache = MemoryCache::new(1 << 20);
        let k = key(0, 0);

        let first = cache.get_or_load(k, 5, loader(b"hello")).await.unwrap();
        assert_eq!(first, Bytes::from_static(b"hello"));

        let calls = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&calls);
        let second = cache
            .get_or_load(
                k,
                5,
                Box::pin(async move {
                    c.fetch_add(1, Ordering::Relaxed);
                    Ok(Bytes::from_static(b"WRONG"))
                }),
            )
            .await
            .unwrap();

        assert_eq!(second, Bytes::from_static(b"hello"));
        assert_eq!(calls.load(Ordering::Relaxed), 0, "hit must not load");
        let s = cache.stats();
        assert_eq!((s.hits, s.misses), (1, 1));
    }

    #[tokio::test]
    async fn a_shorter_read_is_served_as_a_prefix() {
        let cache = MemoryCache::new(1 << 20);
        let k = key(0, 0);
        cache.get_or_load(k, 8, loader(b"abcdefgh")).await.unwrap();

        let short = cache
            .get_or_load(k, 3, Box::pin(async { panic!("must hit") }))
            .await
            .unwrap();
        assert_eq!(short, Bytes::from_static(b"abc"));
    }

    #[tokio::test]
    async fn a_longer_read_supersedes_the_stored_extent() {
        // Largest-wins: the 4-byte entry cannot serve 8 bytes, so it is dropped
        // and reloaded bigger rather than returning a short read.
        let cache = MemoryCache::new(1 << 20);
        let k = key(0, 0);
        cache.get_or_load(k, 4, loader(b"abcd")).await.unwrap();

        let longer = cache.get_or_load(k, 8, loader(b"abcdefgh")).await.unwrap();
        assert_eq!(longer, Bytes::from_static(b"abcdefgh"));
        assert_eq!(cache.stats().stale_evictions, 1);

        // And the larger extent now satisfies both lengths.
        let again = cache
            .get_or_load(k, 4, Box::pin(async { panic!("must hit") }))
            .await
            .unwrap();
        assert_eq!(again, Bytes::from_static(b"abcd"));
    }

    #[tokio::test]
    async fn extents_at_different_offsets_are_independent() {
        let cache = MemoryCache::new(1 << 20);
        cache
            .get_or_load(key(0, 0), 4, loader(b"aaaa"))
            .await
            .unwrap();
        cache
            .get_or_load(key(0, 4), 4, loader(b"bbbb"))
            .await
            .unwrap();

        assert_eq!(
            cache
                .get_or_load(key(0, 0), 4, Box::pin(async { panic!("must hit") }))
                .await
                .unwrap(),
            Bytes::from_static(b"aaaa")
        );
        assert_eq!(
            cache
                .get_or_load(key(0, 4), 4, Box::pin(async { panic!("must hit") }))
                .await
                .unwrap(),
            Bytes::from_static(b"bbbb")
        );
    }

    #[tokio::test]
    async fn different_streams_never_collide() {
        // Same offset, different object version -> different entry. This is the
        // no-stale-reads guarantee at the L1 level.
        let cache = MemoryCache::new(1 << 20);
        cache
            .get_or_load(key(1, 0), 3, loader(b"old"))
            .await
            .unwrap();
        let fresh = cache
            .get_or_load(key(2, 0), 3, loader(b"new"))
            .await
            .unwrap();
        assert_eq!(fresh, Bytes::from_static(b"new"));
    }

    #[tokio::test]
    async fn concurrent_readers_of_one_key_load_once() {
        let cache = Arc::new(MemoryCache::new(1 << 20));
        let calls = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            handles.push(tokio::spawn(async move {
                cache
                    .get_or_load(
                        key(3, 0),
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
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "8 readers, 1 origin fetch"
        );
    }

    #[tokio::test]
    async fn waiters_see_a_failed_load_and_the_next_caller_retries() {
        let cache = Arc::new(MemoryCache::new(1 << 20));
        let k = key(4, 0);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();

        let leader = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move {
                cache
                    .get_or_load(
                        k,
                        2,
                        Box::pin(async move {
                            started_tx.send(()).ok();
                            release_rx.await.ok();
                            Err(talon_core::Error::Other("injected".into()))
                        }),
                    )
                    .await
            })
        };
        started_rx.await.ok();

        let mut waiters = Vec::new();
        for _ in 0..4 {
            let cache = Arc::clone(&cache);
            waiters.push(tokio::spawn(async move {
                cache
                    .get_or_load(k, 2, Box::pin(async { panic!("waiter must not load") }))
                    .await
            }));
        }
        tokio::task::yield_now().await;
        release_tx.send(()).ok();

        assert!(leader.await.unwrap().is_err());
        for w in waiters {
            assert!(w.await.unwrap().is_err(), "waiters must see the failure");
        }

        // The poisoned entry is gone, so a later read succeeds.
        let ok = cache.get_or_load(k, 5, loader(b"fresh")).await.unwrap();
        assert_eq!(ok, Bytes::from_static(b"fresh"));
    }

    #[tokio::test]
    async fn eviction_keeps_the_cache_within_its_ceiling() {
        let cap = 4 * 1024 * 1024u64;
        let extent = 512 * 1024usize;
        let cache = MemoryCache::with_shards(cap, 1);

        for i in 0..32u64 {
            let data = Bytes::from(vec![i as u8; extent]);
            cache
                .get_or_load(
                    key(0, i * extent as u64),
                    extent as u64,
                    Box::pin(async move { Ok(data) }),
                )
                .await
                .unwrap();
        }

        let s = cache.stats();
        assert!(s.evictions > 0, "8x the capacity must evict");
        assert!(
            s.current_bytes <= cap * 2,
            "cache did not converge: {} vs cap {cap}",
            s.current_bytes
        );
    }

    #[tokio::test]
    async fn a_frequently_read_extent_outlives_a_cold_one() {
        let cap = 1024 * 1024u64;
        let extent = 128 * 1024usize;
        let cache = MemoryCache::with_shards(cap, 1);

        let hot = key(0, 0);
        cache
            .get_or_load(
                hot,
                extent as u64,
                Box::pin(async move { Ok(Bytes::from(vec![1u8; extent])) }),
            )
            .await
            .unwrap();
        for _ in 0..20 {
            cache
                .get_or_load(hot, extent as u64, Box::pin(async { panic!("must hit") }))
                .await
                .unwrap();
        }

        // Push far past capacity with cold, once-touched extents.
        for i in 1..40u64 {
            let data = Bytes::from(vec![i as u8; extent]);
            cache
                .get_or_load(
                    key(0, i * extent as u64),
                    extent as u64,
                    Box::pin(async move { Ok(data) }),
                )
                .await
                .unwrap();
        }

        let loaded = Arc::new(AtomicUsize::new(0));
        let l = Arc::clone(&loaded);
        cache
            .get_or_load(
                hot,
                extent as u64,
                Box::pin(async move {
                    l.fetch_add(1, Ordering::Relaxed);
                    Ok(Bytes::from(vec![1u8; extent]))
                }),
            )
            .await
            .unwrap();
        assert_eq!(
            loaded.load(Ordering::Relaxed),
            0,
            "the hot extent should have survived the cold sweep"
        );
    }

    #[tokio::test]
    async fn the_eviction_sink_receives_victims_with_their_hit_counts() {
        #[derive(Debug, Default)]
        struct Recorder {
            seen: Mutex<Vec<(ExtentKey, u32)>>,
        }
        impl EvictionSink for Recorder {
            fn on_evicted(&self, entries: Vec<(ExtentKey, Bytes, u32)>, _total: u64) {
                self.seen
                    .lock()
                    .unwrap()
                    .extend(entries.into_iter().map(|(k, _, uses)| (k, uses)));
            }
        }

        let sink = Arc::new(Recorder::default());
        let cap = 512 * 1024u64;
        let extent = 128 * 1024usize;
        let cache = MemoryCache::with_eviction_sink(
            cap,
            1,
            Some(Arc::clone(&sink) as Arc<dyn EvictionSink>),
        );

        for i in 0..16u64 {
            let data = Bytes::from(vec![i as u8; extent]);
            cache
                .get_or_load(
                    key(0, i * extent as u64),
                    extent as u64,
                    Box::pin(async move { Ok(data) }),
                )
                .await
                .unwrap();
        }

        let seen = sink.seen.lock().unwrap();
        assert!(!seen.is_empty(), "sink should have received victims");
        assert!(
            seen.iter().all(|(_, uses)| *uses >= 1),
            "every victim carries the hit count admission needs"
        );
    }

    #[tokio::test]
    async fn a_disabled_cache_passes_every_read_through() {
        let cache = MemoryCache::new(0);
        assert!(!cache.is_enabled());
        let calls = Arc::new(AtomicUsize::new(0));
        for _ in 0..3 {
            let c = Arc::clone(&calls);
            cache
                .get_or_load(
                    key(0, 0),
                    1,
                    Box::pin(async move {
                        c.fetch_add(1, Ordering::Relaxed);
                        Ok(Bytes::from_static(b"x"))
                    }),
                )
                .await
                .unwrap();
        }
        assert_eq!(calls.load(Ordering::Relaxed), 3, "no caching, no dedup");
    }

    #[tokio::test]
    async fn a_hit_returns_the_cached_allocation_not_a_copy() {
        let cache = MemoryCache::new(1 << 20);
        let k = key(0, 0);
        let a = cache.get_or_load(k, 8, loader(b"abcdefgh")).await.unwrap();
        let b = cache
            .get_or_load(k, 8, Box::pin(async { panic!("must hit") }))
            .await
            .unwrap();
        assert_eq!(a.as_ptr(), b.as_ptr(), "hit must be zero-copy");
    }

    #[test]
    #[should_panic(expected = "power of two")]
    fn a_non_power_of_two_shard_count_is_rejected() {
        MemoryCache::with_shards(1 << 20, 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_pressure_never_underflows_the_byte_counter() {
        let cap = 512 * 1024u64;
        let extent = 64 * 1024usize;
        let cache = Arc::new(MemoryCache::new(cap));

        let mut handles = Vec::new();
        for t in 0..8u64 {
            let cache = Arc::clone(&cache);
            handles.push(tokio::spawn(async move {
                for i in 0..64u64 {
                    let data = Bytes::from(vec![0u8; extent]);
                    cache
                        .get_or_load(
                            key(t, i * extent as u64),
                            extent as u64,
                            Box::pin(async move { Ok(data) }),
                        )
                        .await
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let s = cache.stats();
        assert!(
            s.current_bytes < 1u64 << 40,
            "byte counter wrapped: {}",
            s.current_bytes
        );
        assert!(s.evictions > 0);
    }
}
