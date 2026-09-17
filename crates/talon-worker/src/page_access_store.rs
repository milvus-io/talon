//! Versioned, checksummed per-block access checkpoints; never the residency authority.
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::path::Path;
use talon_core::BlockId;
use xxhash_rust::xxh3::xxh3_64;

const MAGIC: &[u8; 8] = b"TLNACC01";

#[cfg(test)]
thread_local! { static FAIL_CHECKPOINT: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) }; }
#[cfg(test)]
pub(crate) fn crash_barrier(stage: &'static str) -> std::io::Result<()> {
    // A child-process regression stops at actual write/rename boundaries. The
    // parent uses SIGKILL so neither tempfile Drop nor runtime shutdown runs.
    if std::env::var("TALON_CHECKPOINT_CRASH_STAGE").as_deref() == Ok(stage) {
        if let Some(ready) = std::env::var_os("TALON_CHECKPOINT_CRASH_READY") {
            std::fs::write(ready, b"ready")?;
            loop {
                std::thread::park();
            }
        }
    }
    Ok(())
}
#[cfg(test)]
fn checkpoint_fault(stage: &'static str) -> anyhow::Result<()> {
    crash_barrier(stage)?;
    anyhow::ensure!(
        !FAIL_CHECKPOINT.with(|fault| fault.get() == Some(stage)),
        "injected checkpoint {stage} failure"
    );
    Ok(())
}

pub(crate) struct AccessSnapshot {
    pub revision: u64,
    pub sampled_at: u64,
    pub records: Vec<(u32, u64)>,
}
#[derive(Default)]
pub(crate) struct AccessRecovery {
    pub records: BTreeMap<u32, u64>,
    pub corrupt: bool,
    pub future: usize,
}
pub(crate) struct PageAccessStore;
impl PageAccessStore {
    pub fn checkpoint(
        dir: &Path,
        id: &BlockId,
        page_size: u32,
        snapshot: &AccessSnapshot,
    ) -> anyhow::Result<usize> {
        let identity = serde_json::to_vec(id)?;
        let mut bytes = Vec::with_capacity(identity.len() + snapshot.records.len() * 12 + 48);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&(u32::try_from(identity.len())?).to_le_bytes());
        bytes.extend_from_slice(&identity);
        bytes.extend_from_slice(&page_size.to_le_bytes());
        bytes.extend_from_slice(&snapshot.revision.to_le_bytes());
        bytes.extend_from_slice(&snapshot.sampled_at.to_le_bytes());
        bytes.extend_from_slice(&(u32::try_from(snapshot.records.len())?).to_le_bytes());
        for &(page, access) in &snapshot.records {
            bytes.extend_from_slice(&page.to_le_bytes());
            bytes.extend_from_slice(&access.to_le_bytes());
        }
        bytes.extend_from_slice(&xxh3_64(&bytes).to_le_bytes());
        // Do not create the directory: a checkpoint cannot resurrect a deleted block.
        let mut tmp = tempfile::Builder::new()
            .prefix("access.meta.tmp.")
            .tempfile_in(dir)?;
        tmp.write_all(&bytes)?;
        #[cfg(test)]
        checkpoint_fault("file_sync")?;
        tmp.as_file().sync_all()?;
        #[cfg(test)]
        checkpoint_fault("rename")?;
        tmp.persist(dir.join("access.meta"))?;
        #[cfg(test)]
        checkpoint_fault("directory_sync")?;
        File::open(dir)?.sync_all()?;
        Ok(bytes.len())
    }
    pub fn load(dir: &Path, id: &BlockId, page_size: u32, now: u64) -> AccessRecovery {
        let path = dir.join("access.meta");
        if !path.exists() {
            return AccessRecovery::default();
        }
        match Self::decode(&path, id, page_size, now) {
            Ok(r) => r,
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "invalid page access checkpoint; pages are eligible for GC");
                AccessRecovery {
                    corrupt: true,
                    ..Default::default()
                }
            }
        }
    }
    fn decode(
        path: &Path,
        id: &BlockId,
        page_size: u32,
        now: u64,
    ) -> anyhow::Result<AccessRecovery> {
        let identity = serde_json::to_vec(id)?;
        let max_len = identity.len() as u64 + u64::from(id.page_count(page_size)) * 12 + 48;
        let mut bytes = Vec::new();
        File::open(path)?
            .take(max_len + 1)
            .read_to_end(&mut bytes)?;
        anyhow::ensure!(
            bytes.len() as u64 <= max_len && bytes.len() >= 48,
            "invalid checkpoint length"
        );
        let checksum_at = bytes.len() - 8;
        anyhow::ensure!(
            xxh3_64(&bytes[..checksum_at]) == u64::from_le_bytes(bytes[checksum_at..].try_into()?),
            "checksum mismatch"
        );
        let mut input = &bytes[..checksum_at];
        fn take<'a>(input: &mut &'a [u8], n: usize) -> anyhow::Result<&'a [u8]> {
            anyhow::ensure!(input.len() >= n, "truncated checkpoint");
            let (out, tail) = input.split_at(n);
            *input = tail;
            Ok(out)
        }
        fn u32_from(input: &mut &[u8]) -> anyhow::Result<u32> {
            Ok(u32::from_le_bytes(take(input, 4)?.try_into()?))
        }
        fn u64_from(input: &mut &[u8]) -> anyhow::Result<u64> {
            Ok(u64::from_le_bytes(take(input, 8)?.try_into()?))
        }
        anyhow::ensure!(
            take(&mut input, 8)? == MAGIC && u32_from(&mut input)? == 1,
            "unknown checkpoint format"
        );
        let n = u32_from(&mut input)? as usize;
        let stored: BlockId = serde_json::from_slice(take(&mut input, n)?)?;
        anyhow::ensure!(
            &stored == id && u32_from(&mut input)? == page_size,
            "checkpoint identity mismatch"
        );
        let _revision = u64_from(&mut input)?;
        let sampled = u64_from(&mut input)?;
        let count = u32_from(&mut input)?;
        anyhow::ensure!(
            count <= id.page_count(page_size) && input.len() as u64 == u64::from(count) * 12,
            "invalid checkpoint record count"
        );
        let mut out = AccessRecovery::default();
        let mut previous = None;
        for _ in 0..count {
            let page = u32_from(&mut input)?;
            let access = u64_from(&mut input)?;
            anyhow::ensure!(
                page < id.page_count(page_size) && previous.map_or(true, |p| page > p),
                "invalid or duplicate page index"
            );
            previous = Some(page);
            if access > now || access > sampled || sampled > now {
                out.future += 1;
            } else {
                out.records.insert(page, access);
            }
        }
        Ok(out)
    }
}

/// Held for the entire worker lifetime, even when TTL is disabled.
pub struct CacheRootLock {
    _file: File,
}
impl CacheRootLock {
    pub fn acquire(root: &Path) -> anyhow::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join(".worker.lock"))?;
        // SAFETY: file owns a valid descriptor. flock is released on close/process exit.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(anyhow::anyhow!(
                "cache root {} is already in use or cannot be locked: {}",
                root.display(),
                std::io::Error::last_os_error()
            ));
        }
        Ok(Self { _file: file })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{Backend, ObjectId, Version};
    fn id() -> BlockId {
        BlockId {
            object: ObjectId {
                backend: Backend::S3,
                bucket: "bucket".into(),
                object_path: "key".into(),
            },
            version: Version("v1".into()),
            offset: 0,
            block_size: 64,
        }
    }
    #[test]
    fn roundtrip_corruption_and_future() {
        let dir = tempfile::tempdir().unwrap();
        let id = id();
        let snapshot = AccessSnapshot {
            revision: 5,
            sampled_at: 100,
            records: vec![(0, 10), (2, 90)],
        };
        PageAccessStore::checkpoint(dir.path(), &id, 16, &snapshot).unwrap();
        let r = PageAccessStore::load(dir.path(), &id, 16, 110);
        assert_eq!(r.records.len(), 2);
        assert!(!r.corrupt);
        assert_eq!(PageAccessStore::load(dir.path(), &id, 16, 80).future, 2);
        assert!(PageAccessStore::load(dir.path(), &id, 8, 110).corrupt);
        std::fs::write(dir.path().join("access.meta"), b"broken").unwrap();
        assert!(PageAccessStore::load(dir.path(), &id, 16, 110).corrupt);
    }
    #[test]
    fn root_is_exclusive() {
        let dir = tempfile::tempdir().unwrap();
        let lock = CacheRootLock::acquire(dir.path()).unwrap();
        assert!(CacheRootLock::acquire(dir.path()).is_err());
        drop(lock);
        assert!(CacheRootLock::acquire(dir.path()).is_ok());
    }
    #[test]
    fn checkpoint_failure_boundaries_keep_a_complete_recoverable_file() {
        let dir = tempfile::tempdir().unwrap();
        let id = id();
        for stage in ["file_sync", "rename", "directory_sync"] {
            let old = AccessSnapshot {
                revision: 1,
                sampled_at: 10,
                records: vec![(0, 10)],
            };
            PageAccessStore::checkpoint(dir.path(), &id, 16, &old).unwrap();
            FAIL_CHECKPOINT.with(|f| f.set(Some(stage)));
            let new = AccessSnapshot {
                revision: 2,
                sampled_at: 20,
                records: vec![(0, 20)],
            };
            let result = PageAccessStore::checkpoint(dir.path(), &id, 16, &new);
            FAIL_CHECKPOINT.with(|f| f.set(None));
            assert!(result.is_err());
            let recovery = PageAccessStore::load(dir.path(), &id, 16, 30);
            assert!(!recovery.corrupt);
            assert_eq!(
                recovery.records[&0],
                if stage == "directory_sync" { 20 } else { 10 }
            );
            assert_eq!(
                std::fs::read_dir(dir.path()).unwrap().count(),
                1,
                "temporary file must be cleaned on returned errors"
            );
        }
    }

    #[test]
    fn rejects_duplicate_out_of_range_and_wrong_identity_even_with_valid_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let id = id();
        for records in [
            vec![(0, 10), (0, 10)],
            vec![(1, 10), (0, 10)],
            vec![(4, 10)],
        ] {
            PageAccessStore::checkpoint(
                dir.path(),
                &id,
                16,
                &AccessSnapshot {
                    revision: 1,
                    sampled_at: 10,
                    records,
                },
            )
            .unwrap();
            assert!(PageAccessStore::load(dir.path(), &id, 16, 20).corrupt);
        }
        PageAccessStore::checkpoint(
            dir.path(),
            &id,
            16,
            &AccessSnapshot {
                revision: 1,
                sampled_at: 10,
                records: vec![(0, 10)],
            },
        )
        .unwrap();
        let mut other = id.clone();
        other.version = Version::new("v2");
        assert!(PageAccessStore::load(dir.path(), &other, 16, 20).corrupt);
        std::fs::write(dir.path().join("access.meta.tmp.crashed"), b"partial").unwrap();
        assert_eq!(
            PageAccessStore::load(dir.path(), &id, 16, 20).records[&0],
            10
        );
    }
}
