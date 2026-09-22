//! Atomic directory-shard access snapshots. Page files remain the residency authority.
use crate::page_access_store::{AccessRecovery, AccessSnapshot, PageAccessStore};
use crate::page_lifecycle::disk_shard;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::time::{Duration, Instant};
use talon_core::BlockId;
use xxhash_rust::xxh3::xxh3_64;

const MAGIC: &[u8; 8] = b"TLNSHR01";
pub(crate) const FILE_NAME: &str = "access.shard";
pub(crate) const TEMP_PREFIX: &str = "access.shard.tmp.";
// A malformed or exceptionally large shard must not allocate unbounded memory.
pub(crate) const MAX_BYTES: usize = 64 * 1024 * 1024;
// Per worker, with one checkpoint writer. Shards are also spread over the
// configured checkpoint interval by PageGcService. Shutdown retains this cap.
const BYTES_PER_SECOND: u64 = 8 * 1024 * 1024;

pub(crate) struct ShardRecovery {
    pub records: HashMap<BlockId, AccessRecovery>,
    pub corrupt: bool,
}

pub(crate) fn load(dir: &Path, shard: usize, page_size: u32, now: u64) -> Option<ShardRecovery> {
    let path = dir.join(FILE_NAME);
    let result = (|| -> anyhow::Result<_> {
        let mut bytes = Vec::new();
        match File::open(&path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            file => file?.take(MAX_BYTES as u64 + 1).read_to_end(&mut bytes)?,
        };
        anyhow::ensure!(
            bytes.len() >= 24 && bytes.len() <= MAX_BYTES,
            "invalid shard length"
        );
        let end = bytes.len() - 8;
        anyhow::ensure!(
            xxh3_64(&bytes[..end]) == u64::from_le_bytes(bytes[end..].try_into()?),
            "shard checksum mismatch"
        );
        anyhow::ensure!(
            &bytes[..8] == MAGIC && u32::from_le_bytes(bytes[8..12].try_into()?) == page_size,
            "shard format or page size mismatch"
        );
        let count = u32::from_le_bytes(bytes[12..16].try_into()?);
        let mut input = &bytes[16..end];
        let mut records = HashMap::new();
        for _ in 0..count {
            anyhow::ensure!(input.len() >= 4, "truncated shard record");
            let n = u32::from_le_bytes(input[..4].try_into()?) as usize;
            input = &input[4..];
            anyhow::ensure!(n >= 48 && n <= input.len(), "invalid shard record length");
            let record = &input[..n];
            input = &input[n..];
            let identity_len = u32::from_le_bytes(record[12..16].try_into()?) as usize;
            anyhow::ensure!(
                identity_len <= record.len() - 16,
                "invalid block identity length"
            );
            let id: BlockId = serde_json::from_slice(&record[16..16 + identity_len])?;
            anyhow::ensure!(disk_shard(&id) == shard, "block in wrong shard");
            let recovered = PageAccessStore::decode_bytes(record, &id, page_size, now)?;
            anyhow::ensure!(
                records.insert(id, recovered).is_none(),
                "duplicate block identity"
            );
        }
        anyhow::ensure!(input.is_empty(), "trailing shard data");
        Ok(Some(ShardRecovery {
            records,
            corrupt: false,
        }))
    })();
    match result {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(path = %path.display(), %error, "invalid shard checkpoint; ignoring legacy access metadata");
            Some(ShardRecovery {
                records: HashMap::new(),
                corrupt: true,
            })
        }
    }
}

/// Caller holds the shard checkpoint gate through publication and directory sync.
/// No block gate is held: a snapshot may become stale exactly as an unsaved access
/// can, but it neither publishes residency nor clears a newer dirty revision.
pub(crate) fn checkpoint<'a>(
    dir: &Path,
    page_size: u32,
    records: impl Iterator<Item = (&'a BlockId, &'a AccessSnapshot)>,
) -> anyhow::Result<usize> {
    let mut bytes = Vec::from(*MAGIC);
    bytes.extend_from_slice(&page_size.to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    let mut count = 0_u32;
    for (id, snapshot) in records {
        let record = PageAccessStore::encode(id, page_size, snapshot)?;
        anyhow::ensure!(
            bytes.len() + record.len() + 12 <= MAX_BYTES,
            "access shard exceeds 64 MiB limit"
        );
        bytes.extend_from_slice(&(u32::try_from(record.len())?).to_le_bytes());
        bytes.extend_from_slice(&record);
        count += 1;
    }
    bytes[12..16].copy_from_slice(&count.to_le_bytes());
    bytes.extend_from_slice(&xxh3_64(&bytes).to_le_bytes());
    // Shard directories are retained by cleanup. Never recreate a missing one.
    let mut tmp = tempfile::Builder::new()
        .prefix(TEMP_PREFIX)
        .tempfile_in(dir)?;
    let started = Instant::now();
    let mut written = 0_u64;
    for chunk in bytes.chunks(64 * 1024) {
        tmp.write_all(chunk)?;
        written += chunk.len() as u64;
        let due = Duration::from_secs_f64(written as f64 / BYTES_PER_SECOND as f64);
        if let Some(wait) = due.checked_sub(started.elapsed()) {
            std::thread::sleep(wait);
        }
    }
    #[cfg(test)]
    crate::page_access_store::checkpoint_fault("file_sync")?;
    tmp.as_file().sync_all()?;
    #[cfg(test)]
    crate::page_access_store::checkpoint_fault("rename")?;
    tmp.persist(dir.join(FILE_NAME))?;
    #[cfg(test)]
    crate::page_access_store::checkpoint_fault("directory_sync")?;
    File::open(dir)?.sync_all()?;
    Ok(bytes.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page_access_store::FAIL_CHECKPOINT;
    use talon_core::{Backend, ObjectId, Version};

    #[test]
    fn failures_retain_atomic_snapshot_and_decoder_rejects_invalid_shards() {
        let dir = tempfile::tempdir().unwrap();
        let id = BlockId::new(
            ObjectId::new(Backend::S3, "bucket", "key"),
            0,
            64,
            Version::new("v1"),
        );
        let shard = disk_shard(&id);
        let old = AccessSnapshot {
            revision: 1,
            sampled_at: 10,
            records: vec![(0, 10)],
        };
        let new = AccessSnapshot {
            revision: 2,
            sampled_at: 20,
            records: vec![(0, 20)],
        };
        for stage in ["file_sync", "rename", "directory_sync"] {
            checkpoint(dir.path(), 16, std::iter::once((&id, &old))).unwrap();
            FAIL_CHECKPOINT.with(|f| f.set(Some(stage)));
            let result = checkpoint(dir.path(), 16, std::iter::once((&id, &new)));
            FAIL_CHECKPOINT.with(|f| f.set(None));
            assert!(result.is_err());
            let r = load(dir.path(), shard, 16, 30).unwrap();
            assert!(!r.corrupt);
            assert_eq!(
                r.records[&id].records[&0],
                if stage == "directory_sync" { 20 } else { 10 }
            );
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
        }
        assert!(load(dir.path(), (shard + 1) % 256, 16, 30).unwrap().corrupt);
        assert!(load(dir.path(), shard, 32, 30).unwrap().corrupt);
        checkpoint(dir.path(), 16, [(&id, &old), (&id, &new)].into_iter()).unwrap();
        assert!(load(dir.path(), shard, 16, 30).unwrap().corrupt);
        let file = File::create(dir.path().join(FILE_NAME)).unwrap();
        file.set_len(MAX_BYTES as u64 + 1).unwrap();
        assert!(load(dir.path(), shard, 16, 30).unwrap().corrupt);
    }
}
