use super::*;
use crate::small_store::catalog::Address;
use std::io::{BufReader, Read};

const ROWS_PER_BATCH: usize = 512;
const SNAPSHOT_MAGIC: &[u8; 8] = b"TLSA0003";
const DELTA_MAGIC: &[u8; 8] = b"TLSL0002";
const ENTRY_BYTES: usize = 12;
const GROUP_HEADER_BYTES: usize = 12;
const FRAME_HEADER_BYTES: usize = 12;

impl SmallObjectStore {
    pub(crate) fn init_recency(&self) {
        let mut catalog = self.journal.model.0.write().unwrap();
        catalog.recency = Some(crate::small_store::recency::Recency::new(&catalog.model));
    }

    pub(crate) fn has_recency(&self) -> bool {
        self.journal.model.0.read().unwrap().recency.is_some()
    }

    pub(crate) fn recency_for_unlink(&self, nonblocking: bool) -> Option<bool> {
        let catalog = crate::small_store::read_for_unlink(&self.journal.model.0, nonblocking)?;
        Some(catalog.recency.is_some())
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
        let started = Instant::now();
        let catalog = self.journal.model.0.read().unwrap();
        let recency = catalog.recency.as_ref().unwrap();
        let result = (|| -> io::Result<(DiskState, usize)> {
            let Some(mut snapshot) = open_access(&self.root.join(FILE))? else {
                return Ok((reset_state(&self.root), 0));
            };
            let mut state = DiskState::default();
            state.generation = read_header(&mut snapshot, SNAPSHOT_MAGIC)?;
            state.snapshot_bytes = snapshot.get_ref().metadata()?.len();

            let mut matched = 0;
            let mut repaired = false;
            let mut apply = |id, entries: &[u8]| {
                let Some(segment) = catalog.model.segments.get(&id) else {
                    return;
                };
                let records = &segment.records;
                let mut row = 0;
                for (offset, time) in decode_entries(entries) {
                    let offset = u64::from(offset);
                    if offset >= segment.durable_end {
                        break;
                    }
                    row = records.position_from(row, offset);
                    if records.offset_matches(row, offset) {
                        matched += usize::from(recency.restore(recency.value(id, row), time));
                        repaired |= time == 0 || time > recency.started;
                        row += 1;
                    }
                }
            };
            replay_access(&mut snapshot, false, &mut apply)?;
            if let Some(mut log) = open_access(&self.root.join(LOG))? {
                if read_header(&mut log, DELTA_MAGIC)? == state.generation {
                    if !replay_access(&mut log, true, &mut apply)? {
                        state.full = false;
                        state.log_bytes = log.get_ref().metadata()?.len();
                    }
                }
            }
            state.full |= repaired || matched < catalog.live.values().sum();
            Ok((state, matched))
        })();
        match result {
            Ok((state, matched)) => {
                *self.journal.access_disk.lock().unwrap() = state;
                tracing::info!(root = %self.root.display(), matched,
                    access_ms = started.elapsed().as_secs_f64() * 1000.0,
                    snapshot_bytes = state.snapshot_bytes, log_bytes = state.log_bytes,
                    "small compact access restored");
            }
            Err(error) => {
                // Startup publishes no readers until the replay is complete.
                recency.reset();
                *self.journal.access_disk.lock().unwrap() = reset_state(&self.root);
                tracing::warn!(%error, root = %self.root.display(), "small object access recovery failed; starting fresh idle windows");
            }
        }
    }

    pub(super) async fn checkpoint_compact_access(
        &self,
        budget: Option<&crate::background::BackgroundBudget>,
    ) -> io::Result<()> {
        // A relocation persists destination ages in this same generation
        // before making the replacement visible in the metadata WAL.
        let _relocation = self.journal.access_relocation.write().await;
        let mut state = {
            let mut disk = self.journal.access_disk.lock().unwrap();
            let state = *disk;
            // An error after sampling dirty rows requires a fresh snapshot.
            disk.full = true;
            disk.log_bytes = 0;
            state
        };
        let full = state.full
            || state.log_bytes >= MIN_COMPACT_BYTES.max(state.snapshot_bytes.div_ceil(2));
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
            state.generation = state.generation.wrapping_add(1).max(1);
            let file = {
                let _io = match budget {
                    Some(b) => Some(b.io().await),
                    None => None,
                };
                disk_io::open(tmp.clone(), disk_io::Open::Replace).await?
            };
            self.write_access_chunks(
                &file,
                header(SNAPSHOT_MAGIC, state.generation).into(),
                0,
                budget,
            )
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
            let mut batch = AgeBatch::default();
            let done = {
                let catalog = self.journal.model.0.read().unwrap();
                let recency = catalog.recency.as_ref().unwrap();
                let mut sample = |id, row| {
                    if let Some(time) = recency.sample(id, row, full) {
                        let offset = catalog.segment(id).unwrap().records.offset_at(row);
                        batch.push(id, offset as u32, time);
                    }
                    true
                };
                if full {
                    catalog.visit_recency(&mut cursor, ceiling, ROWS_PER_BATCH, |_, id, row| {
                        sample(id, row)
                    })
                } else {
                    catalog.visit_dirty_recency(&mut cursor, ceiling, ROWS_PER_BATCH, sample)
                }
            };
            let bytes = batch.finish();
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
        if let Some(fd) = fd {
            if full {
                let mut footer = Vec::new();
                format::frame(&mut footer, &[])?;
                let len = footer.len() as u64;
                self.write_access_chunks(&fd, footer.into(), offset, budget)
                    .await?;
                offset += len;
            }
            self.sync_access(fd, budget).await?;
            if full {
                self.rename_access(tmp, FILE, budget).await?;
                self.publish_access(LOG, header(DELTA_MAGIC, state.generation).into(), budget)
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
            self.journal.access_bytes.store(size, Ordering::Relaxed);
        }
        state.full = false;
        *self.journal.access_disk.lock().unwrap() = state;
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

fn read_header(reader: &mut impl Read, magic: &[u8; 8]) -> io::Result<u64> {
    let mut header = [0; HEADER as usize];
    reader.read_exact(&mut header).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            invalid("truncated compact access header")
        } else {
            error
        }
    })?;
    if &header[..8] != magic {
        return Err(invalid("unsupported compact access version"));
    }
    let generation = u64::from_le_bytes(header[8..].try_into().unwrap());
    if generation == 0 {
        return Err(invalid("invalid compact access generation"));
    }
    Ok(generation)
}

fn reset_state(root: &std::path::Path) -> DiskState {
    // A new checkpoint must also supersede any compatible log left after
    // a reset. A crash before log replacement must not resurrect its ages.
    let generation = open_access(&root.join(LOG))
        .ok()
        .flatten()
        .and_then(|mut file| read_header(&mut file, DELTA_MAGIC).ok())
        .unwrap_or(0);
    DiskState {
        generation,
        ..DiskState::default()
    }
}

#[derive(Default)]
struct AgeBatch {
    bytes: Vec<u8>,
    group: Vec<u8>,
    segment: u64,
}

impl AgeBatch {
    fn push(&mut self, segment: u64, offset: u32, time: u64) {
        if self.segment != segment {
            self.flush();
            self.segment = segment;
            self.group.extend_from_slice(&segment.to_le_bytes());
            self.group.extend_from_slice(&[0; 4]);
        }
        self.group.extend_from_slice(&offset.to_le_bytes());
        self.group.extend_from_slice(&time.to_le_bytes());
    }

    fn flush(&mut self) {
        if self.group.is_empty() {
            return;
        }
        let count = ((self.group.len() - GROUP_HEADER_BYTES) / ENTRY_BYTES) as u32;
        self.group[8..12].copy_from_slice(&count.to_le_bytes());
        format::frame(&mut self.bytes, &self.group).expect("write access frame to memory");
        self.group.clear();
    }

    fn finish(mut self) -> Vec<u8> {
        self.flush();
        self.bytes
    }
}

fn decode_entries(input: &[u8]) -> impl Iterator<Item = (u32, u64)> + '_ {
    input.chunks_exact(ENTRY_BYTES).map(|row| {
        (
            u32::from_le_bytes(row[..4].try_into().unwrap()),
            u64::from_le_bytes(row[4..].try_into().unwrap()),
        )
    })
}

fn replay_access(
    reader: &mut impl Read,
    log: bool,
    mut apply: impl FnMut(u64, &[u8]),
) -> io::Result<bool> {
    let mut header = [0; FRAME_HEADER_BYTES];
    let mut record = Vec::new();
    let mut previous = (0, 0);
    let read_tail = |reader: &mut dyn Read, target: &mut [u8]| -> io::Result<bool> {
        match reader.read_exact(target) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                if log {
                    Ok(false)
                } else {
                    Err(invalid("truncated compact access snapshot"))
                }
            }
            Err(error) => Err(error),
        }
    };
    loop {
        match reader.read_exact(&mut header[..1]) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                return if log {
                    Ok(false)
                } else {
                    Err(invalid("missing compact access footer"))
                };
            }
            Err(error) => return Err(error),
        }
        if !read_tail(reader, &mut header[1..])? {
            return Ok(true);
        }
        let len = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        if len == 0 && !log {
            let expected = u64::from_le_bytes(header[4..].try_into().unwrap());
            if expected != xxhash_rust::xxh3::xxh3_64(&[]) || reader.read(&mut [0])? != 0 {
                return Err(invalid("invalid compact access footer"));
            }
            return Ok(false);
        }
        if !(GROUP_HEADER_BYTES + ENTRY_BYTES..=GROUP_HEADER_BYTES + ROWS_PER_BATCH * ENTRY_BYTES)
            .contains(&len)
            || (len - GROUP_HEADER_BYTES) % ENTRY_BYTES != 0
        {
            return Err(invalid("compact access frame length"));
        }
        record.resize(len, 0);
        if !read_tail(reader, &mut record)? {
            return Ok(true);
        }
        let expected = u64::from_le_bytes(header[4..].try_into().unwrap());
        if xxhash_rust::xxh3::xxh3_64(&record) != expected {
            return Err(invalid("compact access frame checksum"));
        }
        let mut body = record.as_slice();
        let segment = format::u64(&mut body)?;
        let count = format::u32(&mut body)? as usize;
        if segment == 0 || count != body.len() / ENTRY_BYTES {
            return Err(invalid("compact access frame count or segment"));
        }
        if log {
            previous = (0, 0);
        }
        for (offset, time) in decode_entries(body) {
            let address = (segment, offset);
            if offset >= 1 << 30 || time >= 1 << 62 || address <= previous {
                return Err(invalid("compact access row bounds or order"));
            }
            previous = address;
        }
        apply(segment, body);
    }
}

// Called with the relocation gate and the compaction I/O permit held. An
// unpublished destination remains invisible during replay if Replace fails.
pub(in crate::small_store) async fn persist_relocation(
    journal: &journal::Journal,
    old: u64,
    copy: &StagedSegment,
) {
    if journal.model.0.read().unwrap().recency.is_none() {
        return;
    }
    let _writer = journal.access_log_write.lock().await;
    let mut state = *journal.access_disk.lock().unwrap();
    if state.log_bytes < HEADER {
        return;
    }
    let result = async {
        let mut translations: Vec<_> = copy
            .records
            .iter()
            .map(|(&old, record)| (old, record.new_offset))
            .collect();
        translations.sort_unstable();
        let fd = disk_io::open(journal.access_log_path(), disk_io::Open::Update).await?;
        let old_size = disk_io::allocated(fd.clone()).await?;
        for rows in translations.chunks(ROWS_PER_BATCH) {
            let bytes = {
                let catalog = journal.model.0.read().unwrap();
                let Some(recency) = &catalog.recency else {
                    return Ok(());
                };
                let records = &catalog.segment(old).unwrap().records;
                let mut batch = AgeBatch::default();
                let mut row = 0;
                for &(offset, new_offset) in rows {
                    row = records.position_from(row, offset);
                    debug_assert!(records.offset_matches(row, offset));
                    let time = recency.time(recency.value(old, row).load(Ordering::Relaxed));
                    batch.push(copy.id, new_offset as u32, time);
                    row += 1;
                }
                batch.finish()
            };
            let len = bytes.len() as u64;
            disk_io::write(fd.clone(), bytes.into(), state.log_bytes).await?;
            state.log_bytes += len;
        }
        let new_size = disk_io::allocated(fd.clone()).await?;
        disk_io::sync(fd).await?;
        journal
            .access_bytes
            .fetch_add(new_size.saturating_sub(old_size), Ordering::Relaxed);
        Ok::<_, io::Error>(())
    }
    .await;
    if let Err(error) = result {
        state.full = true;
        state.log_bytes = 0;
        tracing::warn!(%error, old, new = copy.id, "small access relocation could not persist ages");
    }
    *journal.access_disk.lock().unwrap() = state;
}

#[cfg(test)]
pub(super) fn read_log(path: &std::path::Path) -> Vec<(u64, u32, u64)> {
    let mut log = open_access(&path.join(LOG)).unwrap().unwrap();
    read_header(&mut log, DELTA_MAGIC).unwrap();
    let mut rows = Vec::new();
    assert!(!replay_access(&mut log, true, |segment, body| {
        rows.extend(decode_entries(body).map(|(offset, time)| (segment, offset, time)));
    })
    .unwrap());
    rows
}
