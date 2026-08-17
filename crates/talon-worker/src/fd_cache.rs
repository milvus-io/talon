//! A sharded, bounded cache of open descriptors for immutable cache files.
//!
//! Both cache stores serve reads by handing a file descriptor to `sendfile`.
//! The files behind those descriptors are immutable and content-addressed:
//! once written and committed, the bytes at a given path never change. That
//! makes the `openat` + `statx` pair on the serving path pure overhead — path
//! resolution, inode lookup, and `stx_size` all return the same answer every
//! time, while contending on the same inode and dentry across every worker
//! thread.
//!
//! Shared by [`WholeBlockStore`](crate::WholeBlockStore), which caches one
//! descriptor per `.blk` file, and [`PagedBlockStore`](crate::PagedBlockStore),
//! which caches one per `.page` file. The paged store needs it more: a
//! cross-page read opens *every* page it touches, so the per-request `openat`
//! count scales with the span rather than being one per request.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Number of shards in the open-fd cache. Sharding keeps the per-request
/// lookup off a single global mutex when many connections hit distinct files.
const FD_CACHE_SHARDS: usize = 16;

/// Per-shard capacity of the open-fd cache, so the process-wide ceiling is
/// `FD_CACHE_SHARDS * FD_CACHE_SHARD_CAPACITY` descriptors on top of the
/// connection and listener fds.
///
/// The bound matters because an unbounded cache would pin one fd per file ever
/// touched; capping it keeps worst-case fd usage predictable and independent of
/// cache size. Callers pick a multiplier appropriate to their file granularity
/// — see [`FdCache::new`] and [`FdCache::with_capacity`].
const FD_CACHE_SHARD_CAPACITY: usize = 64;

/// A cached, shared descriptor for an immutable cache file.
#[derive(Clone)]
pub(crate) struct CachedFd {
    pub(crate) fd: Arc<OwnedFd>,
    pub(crate) len: u64,
}

/// A sharded, bounded cache of open descriptors.
///
/// Eviction is a simple random-ish replacement within a shard (drop one
/// arbitrary entry when full). Entries are equally hot in the steady state and
/// the cost of a miss is exactly the old behaviour — one `openat` — so a
/// precise LRU is not worth the extra bookkeeping on this path.
pub(crate) struct FdCache {
    shards: Vec<Mutex<HashMap<PathBuf, CachedFd>>>,
    per_shard: usize,
}

impl FdCache {
    /// A cache sized for whole-block files: 1024 descriptors total.
    ///
    /// Blocks default to 256 MiB, so this already covers a ~256 GiB resident
    /// working set — well past the point where the per-request `openat` was
    /// measurable.
    pub(crate) fn new() -> Self {
        Self::with_capacity(FD_CACHE_SHARD_CAPACITY)
    }

    /// A cache with an explicit per-shard capacity, for callers whose files are
    /// far smaller than a whole block (a 1 MiB page vs a 256 MiB block) and
    /// which therefore need proportionally more descriptors to cover the same
    /// resident bytes.
    pub(crate) fn with_capacity(per_shard: usize) -> Self {
        Self {
            shards: (0..FD_CACHE_SHARDS)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
            per_shard,
        }
    }

    fn shard_for(&self, path: &Path) -> &Mutex<HashMap<PathBuf, CachedFd>> {
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        &self.shards[(hasher.finish() as usize) % FD_CACHE_SHARDS]
    }

    pub(crate) fn get(&self, path: &Path) -> Option<CachedFd> {
        let shard = self.shard_for(path);
        let guard = shard.lock().unwrap_or_else(|e| e.into_inner());
        guard.get(path).cloned()
    }

    pub(crate) fn insert(&self, path: PathBuf, entry: CachedFd) {
        let shard = self.shard_for(&path);
        let mut guard = shard.lock().unwrap_or_else(|e| e.into_inner());
        if guard.len() >= self.per_shard && !guard.contains_key(&path) {
            if let Some(victim) = guard.keys().next().cloned() {
                guard.remove(&victim);
            }
        }
        guard.insert(path, entry);
    }

    /// Drop any cached descriptor for `path`.
    ///
    /// Called when a file is removed or rewritten so the cache does not pin an
    /// unlinked inode or serve stale bytes. In-flight handles keep the file
    /// alive until they complete, which is exactly the pre-existing behaviour
    /// for a block evicted mid-request.
    pub(crate) fn invalidate(&self, path: &Path) {
        let shard = self.shard_for(path);
        let mut guard = shard.lock().unwrap_or_else(|e| e.into_inner());
        guard.remove(path);
    }

    /// Drop every cached descriptor whose path lies under `dir`.
    ///
    /// A paged block is a directory of page files; deleting the block unlinks
    /// all of them at once, so every descriptor beneath it must be dropped.
    pub(crate) fn invalidate_prefix(&self, dir: &Path) {
        for shard in &self.shards {
            let mut guard = shard.lock().unwrap_or_else(|e| e.into_inner());
            guard.retain(|path, _| !path.starts_with(dir));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, bytes: &[u8]) -> CachedFd {
        std::fs::write(path, bytes).unwrap();
        let f = std::fs::File::open(path).unwrap();
        let len = f.metadata().unwrap().len();
        CachedFd {
            fd: Arc::new(OwnedFd::from(f)),
            len,
        }
    }

    /// A directory unique to the calling test.
    ///
    /// Keyed on the thread id as well as the pid: these tests run concurrently
    /// in one process, and a shared directory let `invalidate_prefix`'s
    /// "unrelated paths survive" assertion race the capacity test's writes.
    fn tmp_dir() -> PathBuf {
        let mut h = DefaultHasher::new();
        std::thread::current().id().hash(&mut h);
        let dir = std::env::temp_dir().join(format!(
            "talon-fdcache-{}-{:016x}",
            std::process::id(),
            h.finish()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn a_cached_descriptor_round_trips() {
        let dir = tmp_dir();
        let path = dir.join("a.page");
        let cache = FdCache::new();
        cache.insert(path.clone(), write(&path, b"hello"));
        assert_eq!(cache.get(&path).unwrap().len, 5);
        cache.invalidate(&path);
        assert!(cache.get(&path).is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The bound is what keeps fd usage independent of cache size, so a shard
    /// must never grow past its capacity no matter how many distinct paths hit
    /// it.
    #[test]
    fn a_shard_never_exceeds_its_capacity() {
        let dir = tmp_dir();
        let cache = FdCache::with_capacity(2);
        for i in 0..64 {
            let path = dir.join(format!("cap-{i}.page"));
            cache.insert(path.clone(), write(&path, b"x"));
        }
        for shard in &cache.shards {
            assert!(shard.lock().unwrap().len() <= 2);
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Deleting a paged block unlinks a whole directory of page files at once.
    #[test]
    fn invalidate_prefix_drops_every_descriptor_under_a_directory() {
        let dir = tmp_dir();
        let block = dir.join("block.pages");
        std::fs::create_dir_all(&block).unwrap();
        let other = dir.join("other.page");
        let cache = FdCache::new();
        for i in 0..4 {
            let path = block.join(format!("{i}.page"));
            cache.insert(path.clone(), write(&path, b"x"));
        }
        cache.insert(other.clone(), write(&other, b"y"));

        cache.invalidate_prefix(&block);

        for i in 0..4 {
            assert!(cache.get(&block.join(format!("{i}.page"))).is_none());
        }
        assert!(cache.get(&other).is_some(), "unrelated paths must survive");
        std::fs::remove_dir_all(&dir).ok();
    }
}
