//! Startup recovery from incompatible local cache state.
use std::{fs::File, future::Future, io, sync::Arc};

use crate::page_access_store::CacheRootLock;

const RESET_MARKER: &str = ".worker-reset";

/// Marks a local startup failure eligible for cache reset.
#[derive(Debug)]
pub struct IncompatibleCache;

impl std::fmt::Display for IncompatibleCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("incompatible local cache")
    }
}

impl std::error::Error for IncompatibleCache {}

pub(crate) fn incompatible(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

/// Apply only to local identity/store opening, never to general startup errors.
pub fn classify_open_error(error: anyhow::Error) -> anyhow::Error {
    let incompatible = error.chain().any(|cause| {
        let io = cause.downcast_ref::<io::Error>().or_else(|| {
            match cause.downcast_ref::<talon_core::Error>() {
                Some(talon_core::Error::Io(error)) => Some(error),
                _ => None,
            }
        });
        io.is_some_and(|error| error.kind() == io::ErrorKind::InvalidData)
    });
    if incompatible {
        error.context(IncompatibleCache)
    } else {
        error
    }
}

/// Retry once from an empty root only for explicitly classified local-open failures.
pub async fn open_or_reset<T, F, Fut>(
    mut lock: Arc<CacheRootLock>,
    enabled: bool,
    mut open: F,
) -> anyhow::Result<T>
where
    F: FnMut(Arc<CacheRootLock>) -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    match open(lock.clone()).await {
        Err(error) if enabled && error.is::<IncompatibleCache>() => {
            tracing::warn!(root = %lock.root.display(), reason = %format!("{error:#}"),
                "resetting incompatible local cache; all worker data and metadata will be deleted");
            talon_journal::JournalService::wait_for_retiring(&lock.root)?;
            Arc::get_mut(&mut lock)
                .ok_or_else(|| {
                    anyhow::anyhow!("cannot reset cache while local stores are still in use")
                })?
                .reset()?;
            open(lock).await
        }
        result => result,
    }
}

impl CacheRootLock {
    pub(crate) fn check_reset_complete(&self) -> io::Result<()> {
        match std::fs::symlink_metadata(self.root.join(RESET_MARKER)) {
            Ok(_) => Err(incompatible(
                "interrupted cache reset; enable reset_incompatible_cache to finish",
            )),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Requires sole ownership after all store handles and retiring writers exit.
    /// Keep the root and lock inode throughout deletion, including on failure.
    pub fn reset(&mut self) -> anyhow::Result<()> {
        let marker = self.root.join(RESET_MARKER);
        // A previous interrupted reset already has a durable marker. Never
        // follow a marker symlink or truncate a file outside the cache root.
        match File::options().write(true).create_new(true).open(&marker) {
            Ok(file) => file.sync_all()?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        File::open(&self.root)?.sync_all()?;
        for entry in std::fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name();
            let kind = entry.file_type()?;
            if name == ".worker.lock"
                || name == RESET_MARKER
                || (name == "lost+found" && kind.is_dir())
            {
                continue;
            }
            if kind.is_dir() {
                std::fs::remove_dir_all(entry.path())?;
            } else {
                std::fs::remove_file(entry.path())?;
            }
        }
        // Do not publish an empty root until every removal is durable.
        File::open(&self.root)?.sync_all()?;
        std::fs::remove_file(marker)?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{symlink, MetadataExt};

    #[test]
    fn reset_clears_all_residue_without_replacing_lock_or_following_symlinks() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("keep"), b"outside").unwrap();
        let mut lock = CacheRootLock::acquire(root.path()).unwrap();
        lock.load_identity("old", None, 64, 16).unwrap();
        let inode = std::fs::metadata(root.path().join(".worker.lock"))
            .unwrap()
            .ino();
        for directory in [
            "a/shared/small/metadata",
            "b/scopes/old/paged",
            "lost+found",
        ] {
            let path = root.path().join(directory);
            std::fs::create_dir_all(&path).unwrap();
            std::fs::write(path.join("residue"), b"old").unwrap();
        }
        symlink(outside.path(), root.path().join("external")).unwrap();
        symlink(outside.path(), root.path().join("a/shared/nested-link")).unwrap();
        std::fs::write(root.path().join(".worker_identity.tmp.old"), b"old").unwrap();
        lock.reset().unwrap();
        let mut names: Vec<_> = std::fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        names.sort();
        assert_eq!(names, [".worker.lock", "lost+found"]);
        assert_eq!(
            std::fs::metadata(root.path().join(".worker.lock"))
                .unwrap()
                .ino(),
            inode
        );
        assert!(CacheRootLock::acquire(root.path()).is_err());
        assert_eq!(
            std::fs::read(outside.path().join("keep")).unwrap(),
            b"outside"
        );
        let identity = lock.load_identity("new", Some("worker"), 128, 32).unwrap();
        assert_eq!(identity.cluster_id, "new");
    }

    #[tokio::test]
    async fn interrupted_reset_requires_opt_in_and_finishes_before_identity() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join(RESET_MARKER), b"").unwrap();
        std::fs::write(root.path().join("old-data"), b"old").unwrap();
        for enabled in [false, true] {
            let lock = Arc::new(CacheRootLock::acquire(root.path()).unwrap());
            let result = open_or_reset(lock, enabled, |lock| async move {
                lock.load_identity("c", Some("w"), 64, 16)
                    .map_err(classify_open_error)
            })
            .await;
            assert_eq!(result.is_ok(), enabled);
            assert_eq!(root.path().join("old-data").exists(), !enabled);
            assert_eq!(root.path().join(RESET_MARKER).exists(), !enabled);
        }
    }

    #[tokio::test]
    async fn identity_mismatches_reset_only_with_opt_in() {
        for (field, value) in [
            ("cluster_id", serde_json::json!("other")),
            ("worker_id", serde_json::json!("other")),
            ("block_size", serde_json::json!(128)),
            ("page_size", serde_json::json!(32)),
            ("format_version", serde_json::json!(99)),
        ] {
            let root = tempfile::tempdir().unwrap();
            let lock = CacheRootLock::acquire(root.path()).unwrap();
            let original = lock.load_identity("c", Some("w"), 64, 16).unwrap();
            drop(lock);
            let mut stored = serde_json::to_value(&original).unwrap();
            stored[field] = value;
            let bytes = serde_json::to_vec(&stored).unwrap();
            let path = root.path().join("worker_identity");
            std::fs::write(&path, &bytes).unwrap();
            for enabled in [false, true] {
                let lock = Arc::new(CacheRootLock::acquire(root.path()).unwrap());
                let result = open_or_reset(lock, enabled, |lock| async move {
                    lock.load_identity("c", Some("w"), 64, 16)
                        .map_err(classify_open_error)
                })
                .await;
                if enabled {
                    assert_eq!(result.unwrap(), original, "{field}");
                } else {
                    assert!(result.unwrap_err().is::<IncompatibleCache>(), "{field}");
                    assert_eq!(std::fs::read(&path).unwrap(), bytes);
                }
            }
        }
    }

    #[tokio::test]
    async fn ordinary_errors_never_reset_and_incompatibility_retries_only_once() {
        for kind in [
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::Other,
            io::ErrorKind::NotFound,
            io::ErrorKind::InvalidData,
        ] {
            let root = tempfile::tempdir().unwrap();
            let lock = Arc::new(CacheRootLock::acquire(root.path()).unwrap());
            let sentinel = root.path().join("sentinel");
            std::fs::write(&sentinel, b"keep").unwrap();
            let mut attempts = 0;
            let result: anyhow::Result<()> = open_or_reset(lock, true, |_| {
                attempts += 1;
                async move {
                    Err(classify_open_error(
                        talon_core::Error::Io(io::Error::new(kind, "injected open error")).into(),
                    ))
                }
            })
            .await;
            assert!(result.is_err());
            let incompatible = kind == io::ErrorKind::InvalidData;
            assert_eq!(attempts, if incompatible { 2 } else { 1 });
            assert_eq!(sentinel.exists(), !incompatible);
        }
    }

    #[tokio::test]
    async fn reset_refuses_live_store_owners() {
        let root = tempfile::tempdir().unwrap();
        let lock = Arc::new(CacheRootLock::acquire(root.path()).unwrap());
        let held = lock.clone();
        let result: anyhow::Result<()> = open_or_reset(lock, true, |_| async {
            Err(classify_open_error(incompatible("old format").into()))
        })
        .await;
        assert!(result.unwrap_err().to_string().contains("still in use"));
        assert!(!held.root.join(RESET_MARKER).exists());
        assert!(CacheRootLock::acquire(root.path()).is_err());
    }
}
