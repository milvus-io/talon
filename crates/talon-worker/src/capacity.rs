//! Multi-cache-directory capacity management.
//!
//! A worker may spread its cache across several directories (typically one per
//! NVMe device), each with its own byte capacity. [`CacheDirs`] selects a
//! directory for each new block and tracks per-dir usage so no directory
//! exceeds its cap, surfacing usage to metrics and eviction.
//!
//! Placement policy is **most-free-bytes-first**: a new block goes to the dir
//! with the most remaining capacity that can fit it. This fills devices evenly
//! and avoids hot-spotting a single disk. A block's home dir is chosen once and
//! remembered so reads/evictions address the right device.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use talon_core::{BlockId, Error, Result};

/// One cache directory and its byte capacity.
#[derive(Debug, Clone)]
pub struct CacheDirConfig {
    /// Filesystem path of the cache directory.
    pub path: PathBuf,
    /// Maximum bytes this directory may hold.
    pub capacity_bytes: u64,
}

#[derive(Debug, Default)]
struct DirState {
    used_bytes: u64,
}

/// Manages placement and usage across multiple capped cache directories.
pub struct CacheDirs {
    dirs: Vec<CacheDirConfig>,
    // Per-dir mutable usage, indexed parallel to `dirs`.
    state: RwLock<Vec<DirState>>,
    // Which dir index a block was placed into.
    placement: RwLock<HashMap<BlockId, usize>>,
}

impl CacheDirs {
    /// Validate and construct from a set of cache-dir configs.
    ///
    /// Fails fast if there are no dirs, a path is empty, a capacity is zero, or
    /// two dirs share a path.
    pub fn new(dirs: Vec<CacheDirConfig>) -> Result<Self> {
        if dirs.is_empty() {
            return Err(Error::Other("at least one cache dir is required".into()));
        }
        let mut seen = std::collections::HashSet::new();
        for d in &dirs {
            if d.path.as_os_str().is_empty() {
                return Err(Error::Other("cache dir path must not be empty".into()));
            }
            if d.capacity_bytes == 0 {
                return Err(Error::Other(format!(
                    "cache dir {:?} has zero capacity",
                    d.path
                )));
            }
            if !seen.insert(d.path.clone()) {
                return Err(Error::Other(format!("duplicate cache dir: {:?}", d.path)));
            }
        }
        let n = dirs.len();
        Ok(Self {
            dirs,
            state: RwLock::new((0..n).map(|_| DirState::default()).collect()),
            placement: RwLock::new(HashMap::new()),
        })
    }

    /// Number of configured cache directories.
    pub fn len(&self) -> usize {
        self.dirs.len()
    }

    /// Whether there are no cache directories (never true after `new`).
    pub fn is_empty(&self) -> bool {
        self.dirs.is_empty()
    }

    /// Total capacity across all dirs.
    pub fn total_capacity(&self) -> u64 {
        self.dirs.iter().map(|d| d.capacity_bytes).sum()
    }

    /// Total bytes used across all dirs.
    pub fn total_used(&self) -> u64 {
        self.state
            .read()
            .unwrap()
            .iter()
            .map(|s| s.used_bytes)
            .sum()
    }

    /// Per-dir `(path, used, capacity)` usage, for metrics.
    pub fn usage(&self) -> Vec<(PathBuf, u64, u64)> {
        let st = self.state.read().unwrap();
        self.dirs
            .iter()
            .zip(st.iter())
            .map(|(d, s)| (d.path.clone(), s.used_bytes, d.capacity_bytes))
            .collect()
    }

    /// Place a new block of `size` bytes, returning its assigned directory.
    ///
    /// Chooses the dir with the most free bytes that can still fit `size`.
    /// Returns [`Error::Other`] if no dir has room (caller should evict first).
    /// Idempotent for an already-placed block: returns its existing dir without
    /// double-counting.
    pub fn place(&self, id: &BlockId, size: u64) -> Result<PathBuf> {
        if let Some(&idx) = self.placement.read().unwrap().get(id) {
            return Ok(self.dirs[idx].path.clone());
        }
        let mut st = self.state.write().unwrap();
        // Pick the dir with the largest remaining free space that fits `size`.
        let choice = self
            .dirs
            .iter()
            .enumerate()
            .filter_map(|(i, d)| {
                let free = d.capacity_bytes.checked_sub(st[i].used_bytes)?;
                (free >= size).then_some((i, free))
            })
            .max_by_key(|&(_, free)| free);

        match choice {
            Some((idx, _)) => {
                st[idx].used_bytes += size;
                self.placement.write().unwrap().insert(id.clone(), idx);
                Ok(self.dirs[idx].path.clone())
            }
            None => Err(Error::Other(format!(
                "no cache dir has room for {size} bytes (evict first)"
            ))),
        }
    }

    /// Release a placed block of `size` bytes, freeing its directory's usage.
    ///
    /// No-op if the block was not placed here.
    pub fn release(&self, id: &BlockId, size: u64) {
        if let Some(idx) = self.placement.write().unwrap().remove(id) {
            let mut st = self.state.write().unwrap();
            st[idx].used_bytes = st[idx].used_bytes.saturating_sub(size);
        }
    }

    /// The directory a block was placed into, if any.
    pub fn dir_of(&self, id: &BlockId) -> Option<PathBuf> {
        self.placement
            .read()
            .unwrap()
            .get(id)
            .map(|&i| self.dirs[i].path.clone())
    }

    /// Directory path at index `i` (for tests / diagnostics).
    pub fn path(&self, i: usize) -> Option<&Path> {
        self.dirs.get(i).map(|d| d.path.as_path())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{Backend, ObjectId, Version};

    fn block(n: u64) -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "b", format!("o/{n}")),
            0,
            256 << 20,
            Version::new("v1"),
        )
    }

    fn dirs() -> CacheDirs {
        CacheDirs::new(vec![
            CacheDirConfig {
                path: "/nvme0".into(),
                capacity_bytes: 1000,
            },
            CacheDirConfig {
                path: "/nvme1".into(),
                capacity_bytes: 1000,
            },
        ])
        .unwrap()
    }

    #[test]
    fn validation_rejects_bad_config() {
        assert!(CacheDirs::new(vec![]).is_err());
        assert!(CacheDirs::new(vec![CacheDirConfig {
            path: "/a".into(),
            capacity_bytes: 0
        }])
        .is_err());
        assert!(CacheDirs::new(vec![
            CacheDirConfig {
                path: "/dup".into(),
                capacity_bytes: 1
            },
            CacheDirConfig {
                path: "/dup".into(),
                capacity_bytes: 1
            },
        ])
        .is_err());
    }

    #[test]
    fn blocks_distribute_without_exceeding_caps() {
        let cd = dirs();
        // Each dir holds 1000; place four 400-byte blocks -> balanced 2 per dir.
        let mut assigned = Vec::new();
        for n in 0..4 {
            assigned.push(cd.place(&block(n), 400).unwrap());
        }
        // With most-free-first, placement alternates between the two dirs.
        let count0 = assigned.iter().filter(|p| p.ends_with("nvme0")).count();
        let count1 = assigned.iter().filter(|p| p.ends_with("nvme1")).count();
        assert_eq!((count0, count1), (2, 2));
        assert_eq!(cd.total_used(), 1600);

        // A fifth 400-byte block would push some dir over 1000 -> only one dir
        // (each at 800) has 200 free, which is < 400, so it must fail.
        assert!(cd.place(&block(99), 400).is_err());
    }

    #[test]
    fn place_is_idempotent_and_release_frees() {
        let cd = dirs();
        let p1 = cd.place(&block(1), 500).unwrap();
        let p2 = cd.place(&block(1), 500).unwrap(); // idempotent, no double count
        assert_eq!(p1, p2);
        assert_eq!(cd.total_used(), 500);

        cd.release(&block(1), 500);
        assert_eq!(cd.total_used(), 0);
        assert!(cd.dir_of(&block(1)).is_none());
        // Releasing an unknown block is a no-op.
        cd.release(&block(2), 100);
        assert_eq!(cd.total_used(), 0);
    }

    #[test]
    fn usage_reflects_placement() {
        let cd = dirs();
        cd.place(&block(1), 300).unwrap();
        cd.place(&block(2), 700).unwrap();
        let total: u64 = cd.usage().iter().map(|(_, used, _)| *used).sum();
        assert_eq!(total, 1000);
        assert_eq!(cd.total_capacity(), 2000);
    }

    #[test]
    fn oversized_block_is_rejected() {
        let cd = dirs();
        assert!(cd.place(&block(1), 1001).is_err());
    }
}
