//! Startup recovery from incompatible local cache state.
use std::{fs::File, future::Future, io, sync::Arc};
use talon_core::WorkerConfig;

use crate::{
    identity::{WorkerIdentity, IDENTITY_FILE},
    page_access_store::CacheRootLock,
};

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

/// Preserve the logical identity and retry once with an empty cache after a local-open failure.
pub async fn open_or_reset<T, F, Fut>(
    mut lock: Arc<CacheRootLock>,
    config: &WorkerConfig,
    mut open: F,
) -> anyhow::Result<T>
where
    F: FnMut(Arc<CacheRootLock>) -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    let mut identity = lock.load_identity_owner(
        &config.cluster_id,
        config.node_id.as_deref(),
        config.block_size,
        config.l2_page_size_bytes,
    )?;
    match open(lock.clone()).await {
        Err(error) if config.reset_incompatible_cache && error.is::<IncompatibleCache>() => {
            tracing::warn!(root = %lock.root.display(), reason = %format!("{error:#}"),
                "resetting incompatible local cache; preserving worker identity while deleting cached data and metadata");
            talon_journal::JournalService::wait_for_retiring(&lock.root)?;
            identity.block_size = config.block_size;
            identity.page_size = config.l2_page_size_bytes;
            Arc::get_mut(&mut lock)
                .ok_or_else(|| {
                    anyhow::anyhow!("cannot reset cache while local stores are still in use")
                })?
                .reset(&identity)?;
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
    fn reset(&mut self, identity: &WorkerIdentity) -> anyhow::Result<()> {
        let marker = self.root.join(RESET_MARKER);
        // A previous interrupted reset already has a durable marker. Never
        // follow a marker symlink or truncate a file outside the cache root.
        match File::options().write(true).create_new(true).open(&marker) {
            Ok(file) => file.sync_all()?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        File::open(&self.root)?.sync_all()?;
        #[cfg(test)]
        crate::page_access_store::crash_barrier("reset_marker")?;
        for entry in std::fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name();
            let kind = entry.file_type()?;
            if name == ".worker.lock"
                || name == IDENTITY_FILE
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
            #[cfg(test)]
            crate::page_access_store::crash_barrier("reset_delete")?;
        }
        // Do not publish an empty root until every removal is durable.
        File::open(&self.root)?.sync_all()?;
        self.persist_identity(identity, true)?;
        #[cfg(test)]
        crate::page_access_store::crash_barrier("reset_identity")?;
        std::fs::remove_file(marker)?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{symlink, MetadataExt};

    fn config() -> WorkerConfig {
        WorkerConfig {
            cluster_id: "c".into(),
            block_size: 64,
            l2_page_size_bytes: 16,
            ..Default::default()
        }
    }

    async fn open_identity(
        root: &std::path::Path,
        cfg: &WorkerConfig,
    ) -> anyhow::Result<WorkerIdentity> {
        let lock = Arc::new(CacheRootLock::acquire(root)?);
        open_or_reset(lock, cfg, |lock| async move {
            lock.load_identity(
                &cfg.cluster_id,
                cfg.node_id.as_deref(),
                cfg.block_size,
                cfg.l2_page_size_bytes,
            )
            .map_err(classify_open_error)
        })
        .await
    }

    #[test]
    fn reset_clears_all_residue_without_replacing_lock_or_following_symlinks() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("keep"), b"outside").unwrap();
        let mut lock = CacheRootLock::acquire(root.path()).unwrap();
        let identity = lock.load_identity("c", None, 64, 16).unwrap();
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
        lock.reset(&identity).unwrap();
        let mut names: Vec<_> = std::fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        names.sort();
        assert_eq!(names, [".worker.lock", "lost+found", IDENTITY_FILE]);
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
        assert_eq!(lock.load_identity("c", None, 64, 16).unwrap(), identity);
    }

    #[tokio::test]
    async fn interrupted_reset_requires_opt_in_and_finishes_before_identity() {
        let root = tempfile::tempdir().unwrap();
        let identity = open_identity(root.path(), &config()).await.unwrap();
        std::fs::write(root.path().join(RESET_MARKER), b"").unwrap();
        std::fs::write(root.path().join("old-data"), b"old").unwrap();
        for enabled in [false, true] {
            let cfg = WorkerConfig {
                reset_incompatible_cache: enabled,
                ..config()
            };
            let result = open_identity(root.path(), &cfg).await;
            assert_eq!(result.is_ok(), enabled);
            if enabled {
                assert_eq!(result.unwrap(), identity);
            }
            assert_eq!(root.path().join("old-data").exists(), !enabled);
            assert_eq!(root.path().join(RESET_MARKER).exists(), !enabled);
        }
    }

    #[tokio::test]
    async fn geometry_changes_preserve_identity_and_reset_only_with_opt_in() {
        for (field, value) in [
            ("block_size", serde_json::json!(128)),
            ("page_size", serde_json::json!(32)),
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
                let cfg = WorkerConfig {
                    reset_incompatible_cache: enabled,
                    ..config()
                };
                let result = open_identity(root.path(), &cfg).await;
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
    async fn invalid_or_conflicting_identity_never_resets() {
        for case in ["cluster", "node", "format", "empty", "corrupt", "missing"] {
            for interrupted in [false, true] {
                let root = tempfile::tempdir().unwrap();
                let original = open_identity(root.path(), &config()).await.unwrap();
                let mut stored = serde_json::to_value(&original).unwrap();
                let mut cfg = config();
                match case {
                    "cluster" => stored["cluster_id"] = serde_json::json!("other"),
                    "node" => cfg.node_id = Some("different-worker".into()),
                    "format" => stored["format_version"] = serde_json::json!(99),
                    "empty" => stored["worker_id"] = serde_json::json!(""),
                    _ => {}
                }
                let path = root.path().join(IDENTITY_FILE);
                let bytes = if case == "corrupt" {
                    b"{broken".to_vec()
                } else {
                    serde_json::to_vec(&stored).unwrap()
                };
                std::fs::write(&path, &bytes).unwrap();
                if case == "missing" {
                    std::fs::remove_file(&path).unwrap();
                }
                if interrupted {
                    std::fs::write(root.path().join(RESET_MARKER), b"").unwrap();
                }
                let sentinel = root.path().join("data");
                std::fs::write(&sentinel, b"keep").unwrap();
                assert!(open_identity(root.path(), &cfg).await.is_err(), "{case}");
                assert_eq!(std::fs::read(&sentinel).unwrap(), b"keep", "{case}");
                assert_eq!(
                    root.path().join(RESET_MARKER).exists(),
                    interrupted,
                    "{case}"
                );
                if case == "missing" {
                    assert!(!path.exists());
                } else {
                    assert_eq!(std::fs::read(path).unwrap(), bytes, "{case}");
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
            open_identity(root.path(), &config()).await.unwrap();
            let lock = Arc::new(CacheRootLock::acquire(root.path()).unwrap());
            let sentinel = root.path().join("sentinel");
            std::fs::write(&sentinel, b"keep").unwrap();
            let mut attempts = 0;
            let result: anyhow::Result<()> = open_or_reset(lock, &config(), |_| {
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
        let result: anyhow::Result<()> = open_or_reset(lock, &config(), |_| async {
            Err(classify_open_error(incompatible("old format").into()))
        })
        .await;
        assert!(result.unwrap_err().to_string().contains("still in use"));
        assert!(!held.root.join(RESET_MARKER).exists());
        assert!(CacheRootLock::acquire(root.path()).is_err());
    }

    #[tokio::test]
    #[ignore = "subprocess for interrupted_reset_preserves_identity_at_each_boundary"]
    async fn reset_crash_child() {
        let root = std::env::var_os("TALON_RESET_TEST_ROOT").unwrap();
        let cfg = WorkerConfig {
            block_size: 128,
            l2_page_size_bytes: 32,
            ..config()
        };
        open_identity(std::path::Path::new(&root), &cfg)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn interrupted_reset_preserves_identity_at_each_boundary() {
        use std::time::Duration;
        for stage in ["reset_marker", "reset_delete", "reset_identity"] {
            let root = tempfile::tempdir().unwrap();
            let control = tempfile::tempdir().unwrap();
            let ready = control.path().join("ready");
            let original = open_identity(root.path(), &config()).await.unwrap();
            std::fs::create_dir(root.path().join("cached")).unwrap();
            std::fs::write(root.path().join("cached/payload"), b"old").unwrap();
            std::fs::write(root.path().join("other-data"), b"old").unwrap();
            let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--ignored",
                    "--exact",
                    "cache_reset::tests::reset_crash_child",
                ])
                .env("TALON_RESET_TEST_ROOT", root.path())
                .env("TALON_CHECKPOINT_CRASH_STAGE", stage)
                .env("TALON_CHECKPOINT_CRASH_READY", &ready)
                .stdout(std::process::Stdio::null())
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            tokio::time::timeout(Duration::from_secs(10), async {
                while !ready.exists() {
                    assert!(child.try_wait().unwrap().is_none(), "{stage}");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("reset reached crash boundary");
            child.kill().await.unwrap();
            let path = root.path().join(IDENTITY_FILE);
            let bytes = std::fs::read(&path).unwrap();
            let retained: WorkerIdentity = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(retained.worker_id, original.worker_id, "{stage}");
            assert!(root.path().join(RESET_MARKER).exists(), "{stage}");
            let cfg = WorkerConfig {
                block_size: 128,
                l2_page_size_bytes: 32,
                ..config()
            };
            let conflict = WorkerConfig {
                node_id: Some("different-worker".into()),
                ..cfg.clone()
            };
            assert!(open_identity(root.path(), &conflict)
                .await
                .unwrap_err()
                .to_string()
                .contains("node_id conflicts"));
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
            assert!(root.path().join(RESET_MARKER).exists());
            let recovered = open_identity(root.path(), &cfg).await.unwrap();
            assert_eq!(recovered.worker_id, original.worker_id, "{stage}");
            assert_eq!((recovered.block_size, recovered.page_size), (128, 32));
            assert!(!root.path().join(RESET_MARKER).exists());
            assert!(!root.path().join("cached").exists());
            assert!(!root.path().join("other-data").exists());
        }
    }
}
