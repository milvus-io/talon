//! Staged write, rename-commit, and xxh3 checksum for the loader path.
//!
//! The loader downloads a block/page into memory, and this module commits it to
//! disk durably and atomically:
//!
//! 1. Compute an [`xxh3`](xxhash_rust::xxh3) checksum over the fetched bytes.
//! 2. Write the bytes to a uniquely-named staged file under a `staging/`
//!    directory, then `fsync` the file.
//! 3. `rename` the staged file onto its final path — an atomic commit, so a
//!    present file is always complete.
//!
//! A [`Checksum`] is returned to store alongside the block meta; a corrupted
//! download (checksum mismatch against an expected value) is rejected before
//! commit. Orphaned staged files from a previous crash are reclaimed by
//! [`Stager::reclaim_orphans`] at startup.
//!
//! Unlike this path, the zero-copy `splice` PUT path can't checksum in flight —
//! this is where integrity is enforced.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use talon_core::{Error, Result};
use xxhash_rust::xxh3::xxh3_64;

/// An xxh3-64 content checksum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Checksum(pub u64);

impl Checksum {
    /// Compute the checksum of `bytes`.
    pub fn of(bytes: &[u8]) -> Self {
        Checksum(xxh3_64(bytes))
    }
}

impl std::fmt::Display for Checksum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// Commits loader-fetched bytes to disk via staged write + atomic rename.
pub struct Stager {
    staging_dir: PathBuf,
    counter: AtomicU64,
}

impl Stager {
    /// Create a stager using `<root>/staging` as its scratch directory.
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        let staging_dir = root.as_ref().join("staging");
        std::fs::create_dir_all(&staging_dir)?;
        Ok(Self {
            staging_dir,
            counter: AtomicU64::new(0),
        })
    }

    /// The staging scratch directory.
    pub fn staging_dir(&self) -> &Path {
        &self.staging_dir
    }

    /// Checksum, stage, fsync, and atomically commit `bytes` to `final_path`.
    ///
    /// If `expected` is `Some` and does not match the computed checksum, the
    /// staged file is removed and [`Error::Backend`] is returned — the corrupt
    /// download is never committed. On success the [`Checksum`] is returned to
    /// persist with the block meta.
    pub fn commit(
        &self,
        final_path: &Path,
        bytes: &[u8],
        expected: Option<Checksum>,
    ) -> Result<Checksum> {
        let checksum = Checksum::of(bytes);
        if let Some(exp) = expected {
            if exp != checksum {
                return Err(Error::Backend(format!(
                    "checksum mismatch: expected {exp}, computed {checksum}"
                )));
            }
        }

        if let Some(parent) = final_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let id = self.counter.fetch_add(1, Ordering::Relaxed);
        let staged =
            self.staging_dir
                .join(format!("{}-{}-{}.staged", std::process::id(), id, checksum));

        {
            let mut f = std::fs::File::create(&staged)?;
            f.write_all(bytes)?;
            f.sync_all()?; // durable before rename
        }
        // Atomic commit: a present final file is always complete.
        std::fs::rename(&staged, final_path)?;
        Ok(checksum)
    }

    /// Verify that `bytes` match `expected`; used for read-time verification.
    pub fn verify(bytes: &[u8], expected: Checksum) -> Result<()> {
        let got = Checksum::of(bytes);
        if got == expected {
            Ok(())
        } else {
            Err(Error::Backend(format!(
                "checksum mismatch on read: expected {expected}, computed {got}"
            )))
        }
    }

    /// Delete any leftover `*.staged` files from a previous crash.
    ///
    /// Returns the number of orphaned files reclaimed. Called at startup before
    /// serving, since a staged file that was never renamed is by definition not
    /// committed and safe to drop.
    pub fn reclaim_orphans(&self) -> Result<usize> {
        let mut reclaimed = 0;
        for entry in std::fs::read_dir(&self.staging_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "staged") {
                std::fs::remove_file(&path)?;
                reclaimed += 1;
            }
        }
        Ok(reclaimed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_root() -> PathBuf {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        std::time::SystemTime::now().hash(&mut h);
        std::thread::current().id().hash(&mut h);
        std::env::temp_dir().join(format!("talon-stage-{}-{}", std::process::id(), h.finish()))
    }

    #[test]
    fn checksum_is_stable_and_content_sensitive() {
        assert_eq!(Checksum::of(b"hello"), Checksum::of(b"hello"));
        assert_ne!(Checksum::of(b"hello"), Checksum::of(b"hellp"));
    }

    #[test]
    fn commit_writes_and_returns_checksum() {
        let root = tmp_root();
        let stager = Stager::new(&root).unwrap();
        let final_path = root.join("ab/block.blk");
        let data = b"durable bytes";

        let cs = stager.commit(&final_path, data, None).unwrap();
        assert_eq!(cs, Checksum::of(data));
        assert_eq!(std::fs::read(&final_path).unwrap(), data);
        // Nothing left in staging after a successful commit.
        assert_eq!(std::fs::read_dir(stager.staging_dir()).unwrap().count(), 0);

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn corrupt_download_is_rejected_and_not_committed() {
        let root = tmp_root();
        let stager = Stager::new(&root).unwrap();
        let final_path = root.join("block.blk");
        let wrong = Checksum(0xdead_beef);

        let err = stager
            .commit(&final_path, b"payload", Some(wrong))
            .unwrap_err();
        assert!(matches!(err, Error::Backend(_)));
        assert!(!final_path.exists(), "corrupt block must not be committed");
        // The staged file was cleaned up (mismatch returns before staging write).
        assert_eq!(std::fs::read_dir(stager.staging_dir()).unwrap().count(), 0);

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn matching_expected_checksum_commits() {
        let root = tmp_root();
        let stager = Stager::new(&root).unwrap();
        let final_path = root.join("block.blk");
        let data = b"verified";
        let expected = Checksum::of(data);
        let cs = stager.commit(&final_path, data, Some(expected)).unwrap();
        assert_eq!(cs, expected);
        assert!(final_path.exists());
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn verify_detects_corruption() {
        let good = Checksum::of(b"abc");
        assert!(Stager::verify(b"abc", good).is_ok());
        assert!(Stager::verify(b"xyz", good).is_err());
    }

    #[test]
    fn reclaim_orphans_removes_staged_files() {
        let root = tmp_root();
        let stager = Stager::new(&root).unwrap();
        // Simulate crash-orphaned staged files.
        std::fs::write(stager.staging_dir().join("1.staged"), b"x").unwrap();
        std::fs::write(stager.staging_dir().join("2.staged"), b"y").unwrap();
        std::fs::write(stager.staging_dir().join("keep.txt"), b"z").unwrap();

        let n = stager.reclaim_orphans().unwrap();
        assert_eq!(n, 2);
        assert!(stager.staging_dir().join("keep.txt").exists());

        std::fs::remove_dir_all(&root).ok();
    }
}
