use super::*;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

const MAGIC: &[u8; 8] = b"TLNSHR02";
const LOG_MAGIC: &[u8; 8] = b"TLNPAL01";
const HEADER: u64 = 16;
const COMPACT_BYTES: u64 = 1 << 20;

#[derive(Clone, Copy)]
pub(crate) struct JournalState {
    pub generation: u64,
    pub snapshot_bytes: u64,
    pub log_bytes: u64,
    pub full: bool,
}
impl Default for JournalState {
    fn default() -> Self {
        Self {
            generation: 0,
            snapshot_bytes: 0,
            log_bytes: 0,
            full: true,
        }
    }
}
impl JournalState {
    pub fn compact(&self) -> bool {
        self.full || self.log_bytes >= COMPACT_BYTES.max(self.snapshot_bytes)
    }
}
fn header(magic: &[u8; 8], generation: u64) -> Vec<u8> {
    let mut bytes = magic.to_vec();
    bytes.extend_from_slice(&generation.to_le_bytes());
    bytes
}

pub(super) fn unwrap_snapshot(bytes: &[u8]) -> anyhow::Result<(&[u8], u64)> {
    if bytes.starts_with(MAGIC) {
        anyhow::ensure!(bytes.len() >= 40, "truncated access snapshot generation");
        let generation = u64::from_le_bytes(bytes[8..16].try_into()?);
        anyhow::ensure!(generation != 0, "invalid access snapshot generation");
        Ok((&bytes[16..], generation))
    } else {
        Ok((bytes, 0))
    }
}

/// Each frame replaces a block's access map after structural changes, or merges
/// just dirty page times after reads. Empty replacements are deletion markers.
pub(crate) fn encode_delta<'a>(
    page_size: u32,
    records: impl Iterator<Item = (&'a BlockId, &'a AccessSnapshot, bool)>,
) -> anyhow::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for (id, snapshot, replace) in records {
        let record = PageAccessStore::encode(id, page_size, snapshot)?;
        anyhow::ensure!(
            bytes.len() + record.len() + 13 <= MAX_BYTES,
            "access delta exceeds 64 MiB limit"
        );
        let mut body = vec![u8::from(replace)];
        body.extend_from_slice(&record);
        bytes.extend_from_slice(&(body.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&xxh3_64(&body).to_le_bytes());
        bytes.extend_from_slice(&body);
    }
    Ok(bytes)
}

pub(super) fn replay(
    dir: &Path,
    generation: u64,
    shard: usize,
    page_size: u32,
    now: u64,
    records: &mut HashMap<BlockId, AccessRecovery>,
    state: &mut JournalState,
) -> anyhow::Result<()> {
    let mut log = Vec::new();
    match File::open(dir.join(LOG_NAME)) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        file => {
            file?
                .take((2 * MAX_BYTES) as u64 + HEADER + 1)
                .read_to_end(&mut log)?;
        }
    }
    anyhow::ensure!(
        log.len() >= HEADER as usize && log.len() <= 2 * MAX_BYTES + HEADER as usize,
        "invalid access log length"
    );
    anyhow::ensure!(log.starts_with(LOG_MAGIC), "invalid access log format");
    if u64::from_le_bytes(log[8..16].try_into()?) != generation {
        return Ok(());
    }
    state.log_bytes = log.len() as u64;
    let mut input = &log[HEADER as usize..];
    while !input.is_empty() {
        // Only incomplete EOF frames are recoverable. A complete frame with
        // bad checksum/geometry invalidates this shard's advisory ages.
        if input.len() < 12 {
            break;
        }
        let len = u32::from_le_bytes(input[..4].try_into()?) as usize;
        anyhow::ensure!(
            (49..=MAX_BYTES).contains(&len),
            "invalid access log frame length"
        );
        if input.len() < 12 + len {
            break;
        }
        let checksum = u64::from_le_bytes(input[4..12].try_into()?);
        let body = &input[12..12 + len];
        anyhow::ensure!(
            xxh3_64(body) == checksum && body[0] <= 1,
            "corrupt access log frame"
        );
        let replace = body[0] == 1;
        let record = &body[1..];
        let identity_len = u32::from_le_bytes(record[12..16].try_into()?) as usize;
        anyhow::ensure!(
            identity_len <= record.len() - 16,
            "invalid access log identity"
        );
        let id: BlockId = serde_json::from_slice(&record[16..16 + identity_len])?;
        anyhow::ensure!(disk_shard(&id) == shard, "access log block in wrong shard");
        let recovered = PageAccessStore::decode_bytes(record, &id, page_size, now)?;
        if replace {
            if recovered.records.is_empty() && recovered.future == 0 {
                records.remove(&id);
            } else {
                records.insert(id, recovered);
            }
        } else {
            let old = records.entry(id).or_default();
            for page in &recovered.future_pages {
                old.records.remove(page);
            }
            old.future += recovered.future;
            old.records.extend(recovered.records);
        }
        input = &input[12 + len..];
    }
    // Startup deliberately rebuilds one base per populated shard. This also
    // removes retired entries, migrates old snapshots and repairs torn tails.
    Ok(())
}

/// Caller holds the shard checkpoint gate, including the state update after
/// success. On any error it keeps full=true and all consumed dirty guards.
pub(crate) async fn flush(
    dir: &Path,
    mut state: JournalState,
    full: bool,
    bytes: Vec<u8>,
    budget: Option<&crate::background::BackgroundBudget>,
) -> anyhow::Result<(JournalState, usize)> {
    let written;
    if full {
        state.generation = state
            .generation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("access generation exhausted"))?;
        let mut snapshot = header(MAGIC, state.generation);
        snapshot.extend_from_slice(&bytes);
        state.snapshot_bytes = snapshot.len() as u64;
        written = super::publish_named(dir, FILE_NAME, snapshot, budget).await?
            + super::publish_named(dir, LOG_NAME, header(LOG_MAGIC, state.generation), budget)
                .await?;
        state.log_bytes = HEADER;
    } else if bytes.is_empty() {
        written = 0;
    } else {
        let defaults;
        let budget = match budget {
            Some(b) => b,
            None => {
                defaults =
                    crate::background::BackgroundBudget::new(&talon_core::WorkerConfig::default());
                &defaults
            }
        };
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(dir.join(LOG_NAME))
            .await?;
        file.seek(std::io::SeekFrom::Start(state.log_bytes)).await?;
        for chunk in bytes.chunks(64 << 10) {
            budget.bytes(chunk.len()).await;
            file.write_all(chunk).await?;
        }
        #[cfg(test)]
        crate::page_access_store::checkpoint_fault("file_sync")?;
        file.flush().await?;
        file.sync_all().await?;
        state.log_bytes += bytes.len() as u64;
        written = bytes.len();
    }
    state.full = false;
    Ok((state, written))
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{Backend, ObjectId, Version};

    fn id() -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "bucket", "key"),
            0,
            64,
            Version::new("v1"),
        )
    }
    fn snapshot(records: &[(u32, u64)]) -> AccessSnapshot {
        AccessSnapshot {
            revision: 1,
            sampled_at: records.iter().map(|(_, at)| *at).max().unwrap_or(100),
            records: records.to_vec(),
        }
    }
    async fn baseline(dir: &Path) -> JournalState {
        let bytes = encode(16, [(&id(), &snapshot(&[(0, 100), (1, 100)]))].into_iter()).unwrap();
        flush(dir, JournalState::default(), true, bytes, None)
            .await
            .unwrap()
            .0
    }
    async fn delta(
        dir: &Path,
        state: JournalState,
        records: &[(u32, u64)],
        replace: bool,
    ) -> JournalState {
        let bytes = encode_delta(16, [(&id(), &snapshot(records), replace)].into_iter()).unwrap();
        flush(dir, state, false, bytes, None).await.unwrap().0
    }
    fn recover(dir: &Path, now: u64) -> ShardRecovery {
        load(dir, disk_shard(&id()), 16, now).unwrap()
    }

    #[tokio::test]
    async fn append_merges_pages_replaces_structure_and_recreates_deleted_block() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = baseline(dir.path()).await;
        let base = std::fs::read(dir.path().join(FILE_NAME)).unwrap();
        state = delta(dir.path(), state, &[(0, 110)], false).await;
        let prefix = std::fs::read(dir.path().join(LOG_NAME)).unwrap();
        state = delta(dir.path(), state, &[(1, 120)], false).await;
        assert!(std::fs::read(dir.path().join(LOG_NAME))
            .unwrap()
            .starts_with(&prefix));
        assert_eq!(
            recover(dir.path(), 200).records[&id()].records,
            [(0, 110), (1, 120)].into()
        );
        state = delta(dir.path(), state, &[(1, 120)], true).await;
        assert_eq!(
            recover(dir.path(), 200).records[&id()].records,
            [(1, 120)].into()
        );
        state = delta(dir.path(), state, &[], true).await;
        assert!(!recover(dir.path(), 200).records.contains_key(&id()));
        delta(dir.path(), state, &[(0, 130)], true).await;
        assert_eq!(
            recover(dir.path(), 200).records[&id()].records,
            [(0, 130)].into()
        );
        assert_eq!(std::fs::read(dir.path().join(FILE_NAME)).unwrap(), base);
    }

    #[tokio::test]
    async fn torn_tail_preserves_prefix_but_complete_corruption_discards_ages() {
        let dir = tempfile::tempdir().unwrap();
        let state = baseline(dir.path()).await;
        let state = delta(dir.path(), state, &[(0, 110)], false).await;
        let prefix = std::fs::read(dir.path().join(LOG_NAME)).unwrap();
        delta(dir.path(), state, &[(1, 120)], false).await;
        let log = std::fs::read(dir.path().join(LOG_NAME)).unwrap();
        for end in prefix.len() + 1..log.len() {
            std::fs::write(dir.path().join(LOG_NAME), &log[..end]).unwrap();
            let recovered = recover(dir.path(), 200);
            assert!(!recovered.corrupt, "torn at {end}");
            assert_eq!(
                recovered.records[&id()].records,
                [(0, 110), (1, 100)].into()
            );
            assert!(recovered.journal.full);
        }
        let mut bad = log;
        *bad.last_mut().unwrap() ^= 1;
        std::fs::write(dir.path().join(LOG_NAME), bad).unwrap();
        assert!(recover(dir.path(), 200).corrupt);
        assert!(recover(dir.path(), 200).records.is_empty());
    }

    #[tokio::test]
    async fn generation_gap_ignores_old_log_and_future_delta_invalidates_old_age() {
        let dir = tempfile::tempdir().unwrap();
        let state = baseline(dir.path()).await;
        let state = delta(dir.path(), state, &[(0, 300)], false).await;
        let recovered = recover(dir.path(), 200);
        assert!(!recovered.records[&id()].records.contains_key(&0));
        assert_eq!(recovered.records[&id()].records[&1], 100);
        assert_eq!(recovered.records[&id()].future, 1);
        let stale_log = std::fs::read(dir.path().join(LOG_NAME)).unwrap();
        let bytes = encode(16, [(&id(), &snapshot(&[(1, 150)]))].into_iter()).unwrap();
        let (state, _) = flush(dir.path(), state, true, bytes, None).await.unwrap();
        assert_eq!(state.generation, 2);
        assert_eq!(state.log_bytes, HEADER);
        // Crash after base publication but before log replacement.
        std::fs::write(dir.path().join(LOG_NAME), stale_log).unwrap();
        assert_eq!(
            recover(dir.path(), 400).records[&id()].records,
            [(1, 150)].into()
        );
    }

    #[test]
    fn compaction_has_a_floor_and_scales_with_base_size() {
        let mut state = JournalState {
            full: false,
            snapshot_bytes: 100,
            ..Default::default()
        };
        state.log_bytes = COMPACT_BYTES - 1;
        assert!(!state.compact());
        state.log_bytes += 1;
        assert!(state.compact());
        state.snapshot_bytes = 2 * COMPACT_BYTES;
        assert!(!state.compact());
        state.log_bytes = state.snapshot_bytes;
        assert!(state.compact());
    }
}
