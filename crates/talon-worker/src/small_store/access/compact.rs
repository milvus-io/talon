use super::*;
use crate::small_store::catalog::Address;
use std::io::{BufReader, Read};

const ROWS_PER_BATCH: usize = 512;
const BATCH_BYTES: usize = 64 << 10;

impl SmallObjectStore {
    pub(crate) fn init_recency(&self) {
        let mut catalog = self.journal.model.0.write().unwrap();
        catalog.recency = Some(crate::small_store::recency::Recency::new(&catalog.model));
    }

    pub(crate) fn has_recency(&self) -> bool {
        self.journal.model.0.read().unwrap().recency.is_some()
    }

    pub(crate) fn idle_recency_candidates(
        &self,
        cursor: &mut IdleRecencyCursor,
        now: u64,
        age: u64,
        scan: usize,
        deletes: usize,
    ) -> Vec<(CacheUnit, Arc<crate::small_store::policy::SmallAccess>)> {
        let catalog = self.journal.model.0.read().unwrap();
        let Some(end) = cursor.end.or_else(|| {
            let (&id, segment) = catalog.model.segments.last_key_value()?;
            Some((id, segment.bytes))
        }) else {
            return Vec::new();
        };
        // Freeze the active segment's extent as well as its ID. Otherwise a
        // busy stream can keep the sweep from returning to older rows.
        cursor.end = Some(end);
        let mut candidates = Vec::new();
        let done = catalog.visit_recency(&mut cursor.next, end, scan, |slot, id, row| {
            let (recency, value) = catalog.recency_at(slot).unwrap();
            if now.saturating_sub(recency.time(value.load(Ordering::Relaxed))) > age {
                let records = &catalog.segment(id).unwrap().records;
                let (offset, record) = records
                    .metadata(row)
                    .map(|record| (record.offset, record))
                    .unwrap();
                let ticket = catalog.compact_access(slot, (id, offset), record.stored_len());
                let ticket = ticket.small().unwrap();
                if ticket.state.lock().unwrap().pins == 0 {
                    let (_, record) = records.get(row).unwrap();
                    candidates.push((record.key.view().cache_unit(), ticket.clone()));
                }
            }
            candidates.len() < deletes
        });
        if done {
            *cursor = IdleRecencyCursor::default();
        }
        candidates
    }

    pub(super) fn restore_compact_access(&self) {
        let catalog = self.journal.model.0.read().unwrap();
        let recency = catalog.recency.as_ref().unwrap();
        let result = (|| -> io::Result<DiskState> {
            let Some(mut snapshot) = open_access(&self.root.join(FILE))? else {
                return Ok(DiskState::default());
            };
            let mut state = DiskState::default();
            let mut magic = [0; 8];
            snapshot.read_exact(&mut magic)?;
            match &magic {
                LEGACY => {}
                MAGIC => {
                    state.generation = read_generation(&mut snapshot)?;
                    state.snapshot_bytes = snapshot.get_ref().metadata()?.len();
                }
                _ => return Err(invalid("small access snapshot version")),
            }
            let mut matched = 0;
            let mut repaired = false;
            let mut apply = |key: CacheUnit, time: u64| {
                if let Some(slot) = catalog.find(&key.view(), super::super::lookup::VISIBLE) {
                    if let Some((recency, value)) = catalog.recency_at(slot) {
                        matched += usize::from(recency.restore(value, time));
                        repaired |= time == 0 || time > recency.started;
                    }
                }
            };
            replay_access(&mut snapshot, false, &mut apply)?;
            if &magic == MAGIC {
                if let Some(mut log) = open_access(&self.root.join(LOG))? {
                    log.read_exact(&mut magic)?;
                    if &magic != LOG_MAGIC {
                        return Err(invalid("small access log version"));
                    }
                    if read_generation(&mut log)? == state.generation {
                        state.full = replay_access(&mut log, true, &mut apply)?;
                        state.log_bytes = log.get_ref().metadata()?.len();
                    }
                }
            }
            state.full |= repaired || matched < catalog.live.values().sum();
            Ok(state)
        })();
        match result {
            Ok(state) => *self.access_disk.lock().unwrap() = state,
            Err(error) => {
                // Startup publishes no readers until the replay is complete.
                recency.reset();
                tracing::warn!(%error, root = %self.root.display(), "small object access recovery failed; starting fresh idle windows");
            }
        }
    }

    pub(super) async fn checkpoint_compact_access(
        &self,
        budget: Option<&crate::background::BackgroundBudget>,
    ) -> io::Result<()> {
        let mut state = {
            let mut disk = self.access_disk.lock().unwrap();
            let state = *disk;
            // An error after sampling dirty rows requires a fresh snapshot.
            disk.full = true;
            state
        };
        let full = state.full
            || state.log_bytes >= MIN_COMPACT_BYTES.max(state.snapshot_bytes.div_ceil(2));
        let access_relocation = if full {
            Some(self.journal.access_relocation.write().await)
        } else {
            None
        };
        let mut cursor: Address = (0, 0);
        let ceiling = self
            .journal
            .model
            .0
            .read()
            .unwrap()
            .model
            .segments
            .last_key_value()
            .map_or((0, 0), |(&id, segment)| (id, segment.bytes));
        let tmp = self.root.join("access.checkpoint.tmp");
        let mut fd = None;
        let mut offset = state.log_bytes;
        if full {
            state.generation = state
                .generation
                .checked_add(1)
                .ok_or_else(|| invalid("access generation exhausted"))?;
            let file = {
                let _io = match budget {
                    Some(b) => Some(b.io().await),
                    None => None,
                };
                disk_io::open(tmp.clone(), disk_io::Open::Replace).await?
            };
            self.write_access_chunks(&file, header(MAGIC, state.generation).into(), 0, budget)
                .await?;
            offset = HEADER;
            fd = Some(file);
        }
        #[cfg(test)]
        {
            let pause = self.access_pause.lock().unwrap().take();
            if let Some((entered, release)) = pause {
                entered.notify_one();
                release.notified().await;
            }
        }
        loop {
            let mut bytes = Vec::new();
            let mut result = Ok(());
            let done = {
                let catalog = self.journal.model.0.read().unwrap();
                let recency = catalog.recency.as_ref().unwrap();
                let mut sample = |id, row| {
                    if let Some(time) = recency.sample(id, row, full) {
                        let (_, record) = catalog.segment(id).unwrap().records.get(row).unwrap();
                        result = encode(&mut bytes, &record.key, time);
                    }
                    result.is_ok() && bytes.len() < BATCH_BYTES
                };
                if full {
                    catalog.visit_recency(&mut cursor, ceiling, ROWS_PER_BATCH, |_, id, row| {
                        sample(id, row)
                    })
                } else {
                    catalog.visit_dirty_recency(&mut cursor, ceiling, ROWS_PER_BATCH, sample)
                }
            };
            result?;
            if !bytes.is_empty() {
                if fd.is_none() {
                    fd = Some(disk_io::open(self.root.join(LOG), disk_io::Open::Update).await?);
                }
                let len = bytes.len() as u64;
                self.write_access_chunks(fd.as_ref().unwrap(), bytes.into(), offset, budget)
                    .await?;
                offset += len;
            }
            if done {
                break;
            }
            tokio::task::yield_now().await;
        }
        drop(access_relocation);
        if let Some(fd) = fd {
            self.sync_access(fd, budget).await?;
            if full {
                self.rename_access(tmp, FILE, budget).await?;
                self.publish_access(LOG, header(LOG_MAGIC, state.generation).into(), budget)
                    .await?;
                state.snapshot_bytes = offset;
                state.log_bytes = HEADER;
            } else {
                state.log_bytes = offset;
            }
            let root = self.root.clone();
            let size = tokio::task::spawn_blocking(move || allocated(&root))
                .await
                .map_err(io::Error::other)??;
            self.access_bytes.store(size, Ordering::Relaxed);
        }
        state.full = false;
        *self.access_disk.lock().unwrap() = state;
        Ok(())
    }
}

fn open_access(path: &std::path::Path) -> io::Result<Option<BufReader<std::fs::File>>> {
    match std::fs::File::open(path) {
        Ok(file) => Ok(Some(BufReader::new(file))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn read_generation(reader: &mut impl Read) -> io::Result<u64> {
    let mut generation = [0; 8];
    reader.read_exact(&mut generation)?;
    Ok(u64::from_le_bytes(generation))
}

fn replay_access(
    reader: &mut impl Read,
    allow_partial_tail: bool,
    mut apply: impl FnMut(CacheUnit, u64),
) -> io::Result<bool> {
    let mut header = [0; 12];
    let mut record = Vec::new();
    let read_tail = |reader: &mut dyn Read, target: &mut [u8]| -> io::Result<bool> {
        match reader.read_exact(target) {
            Ok(()) => Ok(true),
            Err(error) if allow_partial_tail && error.kind() == io::ErrorKind::UnexpectedEof => {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    };
    loop {
        match reader.read_exact(&mut header[..1]) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(error) => return Err(error),
        }
        if !read_tail(reader, &mut header[1..])? {
            return Ok(true);
        }
        let len = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        if len > format::MAX_KEY + 32 {
            return Err(invalid("small access frame length"));
        }
        record.resize(len, 0);
        if !read_tail(reader, &mut record)? {
            return Ok(true);
        }
        let expected = u64::from_le_bytes(header[4..].try_into().unwrap());
        if xxhash_rust::xxh3::xxh3_64(&record) != expected {
            return Err(invalid("small access frame checksum"));
        }
        let mut body = record.as_slice();
        let time = format::u64(&mut body)?;
        apply(format::id(body)?, time);
    }
}
