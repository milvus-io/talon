//! Durable directory identity. Only the directory lock holder may initialize it.
use std::fs::File;
use std::io::{Read, Write};

use anyhow::{ensure, Context};
use serde::{Deserialize, Serialize};

use crate::page_access_store::CacheRootLock;

const IDENTITY_FILE: &str = "worker_identity";

/// The identity and cache geometry travel with the disk, never with a Pod IP.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerIdentity {
    pub format_version: u32,
    pub cluster_id: String,
    pub worker_id: String,
    pub block_size: u32,
    pub page_size: u64,
}

impl CacheRootLock {
    /// Load an authoritative identity or import an explicitly configured old ID.
    /// A nonempty legacy cache must be imported; silently changing owners would
    /// strand its contents. Temporary identity files left by a crash are ignored.
    pub fn load_identity(
        &self,
        cluster: &str,
        configured_id: Option<&str>,
        block_size: u32,
        page_size: u64,
    ) -> anyhow::Result<WorkerIdentity> {
        ensure!(
            !cluster.trim().is_empty(),
            "identity cluster_id must not be empty"
        );
        ensure!(
            configured_id.map_or(true, |id| !id.trim().is_empty()),
            "identity worker_id must not be empty"
        );
        let path = self.root.join(IDENTITY_FILE);
        let identity = match File::open(&path) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take(16 * 1024 + 1).read_to_end(&mut bytes)?;
                ensure!(
                    bytes.len() <= 16 * 1024,
                    "worker identity exceeds size limit"
                );
                serde_json::from_slice::<WorkerIdentity>(&bytes)
                    .context("corrupt worker_identity; refusing to replace directory identity")?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let has_cache =
                    std::fs::read_dir(&self.root)?.try_fold(false, |found, entry| {
                        let name = entry?.file_name();
                        let name = name.to_string_lossy();
                        Ok::<_, std::io::Error>(
                            found
                                || (name != ".worker.lock"
                                    && !name.starts_with(".worker_identity.tmp.")),
                        )
                    })?;
                ensure!(
                    !has_cache || configured_id.is_some(),
                    "existing cache has no worker_identity; explicitly import its old node_id"
                );
                let worker_id = match configured_id {
                    Some(id) => id.to_owned(),
                    None => {
                        let mut random = [0u8; 16];
                        File::open("/dev/urandom")?.read_exact(&mut random)?;
                        random.iter().map(|b| format!("{b:02x}")).collect()
                    }
                };
                let identity = WorkerIdentity {
                    format_version: 1,
                    cluster_id: cluster.to_owned(),
                    worker_id,
                    block_size,
                    page_size,
                };
                let mut temporary = tempfile::Builder::new()
                    .prefix(".worker_identity.tmp.")
                    .tempfile_in(&self.root)?;
                temporary.write_all(&serde_json::to_vec(&identity)?)?;
                temporary.as_file().sync_all()?;
                temporary
                    .persist_noclobber(&path)
                    .context("publish worker identity")?;
                File::open(&self.root)?.sync_all()?;
                identity
            }
            Err(error) => return Err(error).context("read worker identity"),
        };
        ensure!(
            identity.format_version == 1,
            "unsupported worker identity format {}",
            identity.format_version
        );
        ensure!(
            identity.cluster_id == cluster,
            "worker identity belongs to a different cluster"
        );
        ensure!(
            !identity.worker_id.trim().is_empty(),
            "corrupt worker identity: empty worker_id"
        );
        ensure!(
            configured_id.map_or(true, |id| id == identity.worker_id),
            "configured node_id conflicts with directory identity"
        );
        ensure!(
            identity.block_size == block_size && identity.page_size == page_size,
            "cache geometry conflicts with directory identity"
        );
        Ok(identity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_survives_restart_and_rejects_conflicts() {
        let root = tempfile::tempdir().unwrap();
        let lock = CacheRootLock::acquire(root.path()).unwrap();
        let first = lock.load_identity("c", None, 64, 16).unwrap();
        assert!(CacheRootLock::acquire(root.path()).is_err());
        drop(lock);
        let lock = CacheRootLock::acquire(root.path()).unwrap();
        assert_eq!(first, lock.load_identity("c", None, 64, 16).unwrap());
        assert!(lock.load_identity("other", None, 64, 16).is_err());
        assert!(lock.load_identity("c", Some("other"), 64, 16).is_err());
        assert!(lock.load_identity("c", None, 128, 16).is_err());
        assert!(lock.load_identity("c", None, 64, 32).is_err());
    }

    #[test]
    fn legacy_cache_requires_explicit_import() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("blocks")).unwrap();
        let lock = CacheRootLock::acquire(root.path()).unwrap();
        assert!(lock.load_identity("c", None, 64, 0).is_err());
        assert_eq!(
            lock.load_identity("c", Some("old-id"), 64, 0)
                .unwrap()
                .worker_id,
            "old-id"
        );
        assert_eq!(
            lock.load_identity("c", None, 64, 0).unwrap().worker_id,
            "old-id"
        );
    }

    #[test]
    fn corrupt_identity_is_never_replaced() {
        let root = tempfile::tempdir().unwrap();
        let lock = CacheRootLock::acquire(root.path()).unwrap();
        for bytes in [b"{incomplete".as_slice(), b"{}", b""] {
            std::fs::write(root.path().join(IDENTITY_FILE), bytes).unwrap();
            assert!(lock.load_identity("c", Some("old-id"), 64, 0).is_err());
            assert_eq!(
                std::fs::read(root.path().join(IDENTITY_FILE)).unwrap(),
                bytes
            );
        }
    }

    #[test]
    fn interrupted_unpublished_identity_does_not_become_authoritative() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join(".worker_identity.tmp.crashed"), b"partial").unwrap();
        let lock = CacheRootLock::acquire(root.path()).unwrap();
        assert!(lock.load_identity("c", None, 64, 0).is_ok());
    }
}
