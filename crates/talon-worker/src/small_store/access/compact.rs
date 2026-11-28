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
    #[cfg(test)]
    pub(crate) fn compact_access_age(&self, key: &talon_core::BlockId) -> u64 {
        let catalog = self.journal.model.0.read().unwrap();
        let slot = catalog
            .find(&key.view(), super::super::lookup::VISIBLE)
            .unwrap();
        let (recency, time) = catalog.recency_at(slot).unwrap();
        recency.time(time.load(Ordering::Relaxed))
    }

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
        let persistence = &self.journal.access;
        let root = &persistence.root;
        let catalog = self.journal.model.0.read().unwrap();
        let recency = catalog.recency.as_ref().unwrap();
        let result = (|| -> io::Result<(DiskState, usize)> {
            let Some(mut snapshot) = open_access(&root.join(FILE))? else {
                return Ok((reset_state(root)?, 0));
            };
            let mut state = DiskState {
                generation: read_header(&mut snapshot, SNAPSHOT_MAGIC)?,
                snapshot_bytes: snapshot.get_ref().metadata()?.len(),
                ..Default::default()
            };

            let mut matched = 0;
            let mut repaired = false;
            let mut apply = |id, entries: &[u8]| {
                let Some(segment) = catalog.model.segments.get(&id) else {
                    return;
                };
                let records = &segment.records;
                let times = recency.segment_times(id);
                let mut row = 0;
                for (offset, time) in decode_entries(entries) {
                    let offset = u64::from(offset);
                    if offset >= segment.durable_end {
                        break;
                    }
                    row = records.position_from(row, offset);
                    if records.offset_matches(row, offset) {
                        matched += usize::from(recency.restore(&times[row], time));
                        repaired |= time == 0 || time > recency.started;
                        row += 1;
                    }
                }
            };
            replay_access(&mut snapshot, false, &mut apply)?;
            if let Some(mut log) = open_access(&root.join(LOG))? {
                if read_header(&mut log, DELTA_MAGIC).ok() == Some(state.generation) {
                    state.log_bytes = HEADER;
                    let result = replay_access(&mut log, true, |id, body| {
                        apply(id, body);
                        state.log_bytes +=
                            (FRAME_HEADER_BYTES + GROUP_HEADER_BYTES + body.len()) as u64;
                    });
                    // Only complete, validated frames belong to this log. Keep
                    // their ages even if the following frame is damaged.
                    state.log_truncate = !matches!(result, Ok(false));
                    state.full = state.log_truncate;
                }
            }
            state.full |= repaired || matched < catalog.live.values().sum();
            Ok((state, matched))
        })();
        match result {
            Ok((state, matched)) => {
                *persistence.disk.lock().unwrap() = state;
                tracing::info!(root = %self.root.display(), matched,
                    access_ms = started.elapsed().as_secs_f64() * 1000.0,
                    snapshot_bytes = state.snapshot_bytes, log_bytes = state.log_bytes,
                    "small compact access restored");
            }
            Err(error) => {
                // Startup publishes no readers until the replay is complete.
                recency.reset();
                if let Ok(state) = reset_state(root) {
                    *persistence.disk.lock().unwrap() = state;
                } else {
                    // Retry starts only after the exhausted log can be removed.
                    persistence.disk.lock().unwrap().generation = u64::MAX;
                }
                tracing::warn!(%error, root = %self.root.display(), "small object access recovery failed; starting fresh idle windows");
            }
        }
        match allocated(root) {
            Ok(size) => persistence.bytes.store(size, Ordering::Relaxed),
            Err(error) => {
                tracing::warn!(%error, root = %root.display(), "refreshing small access allocation failed")
            }
        }
    }

    pub(super) async fn checkpoint_compact_access(
        &self,
        budget: Option<&crate::background::BackgroundBudget>,
        flush: Flush<'_>,
    ) -> io::Result<bool> {
        let persistence = &self.journal.access;
        let mut state = *persistence.disk.lock().unwrap();
        let full = state.full
            || state.log_bytes >= MIN_COMPACT_BYTES.max(state.snapshot_bytes.div_ceil(2));
        // A stopped or failed flush may already have sampled dirty rows.
        // Retry from all live ages, keeping the durable log end for relocation.
        persistence.disk.lock().unwrap().full = true;
        if full && !flush.allows_full() {
            return Ok(false);
        }
        let mut scan = AccessScan {
            cursor: (0, 0),
            ceiling: self
                .journal
                .model
                .0
                .read()
                .unwrap()
                .model
                .segments
                .last_key_value()
                .map_or((0, 0), |(&id, segment)| (id, segment.bytes)),
            full,
        };
        let root = &persistence.root;
        if full {
            if state.snapshot_bytes == 0 && state.generation == u64::MAX {
                flush.check()?;
                let reset_root = root.clone();
                state = tokio::task::spawn_blocking(move || reset_state(&reset_root))
                    .await
                    .map_err(io::Error::other)??;
            }
            state.generation = state
                .generation
                .checked_add(1)
                .ok_or_else(|| invalid("access generation exhausted"))?;
            let tmp = root.join(SNAPSHOT_TEMP);
            let fd = {
                let _io = flush.grant(budget, HEADER as usize).await?;
                let fd = disk_io::open(tmp.clone(), disk_io::Open::Replace).await?;
                disk_io::write(
                    fd.clone(),
                    header(SNAPSHOT_MAGIC, state.generation).into(),
                    0,
                )
                .await?;
                fd
            };
            #[cfg(test)]
            self.pause_access_flush().await;
            let mut offset = HEADER;
            loop {
                let (bytes, done) = scan.next_batch(&self.journal.model, flush).await?;
                if !bytes.is_empty() {
                    let _io = flush.grant(budget, bytes.len()).await?;
                    let len = bytes.len() as u64;
                    disk_io::write(fd.clone(), bytes.into(), offset).await?;
                    offset += len;
                }
                if done {
                    break;
                }
            }
            let mut footer = Vec::new();
            format::frame(&mut footer, &[])?;
            let _io = flush.grant(budget, footer.len()).await?;
            let len = footer.len() as u64;
            disk_io::write(fd.clone(), footer.into(), offset).await?;
            offset += len;
            disk_io::sync(fd).await?;
            drop(_io);
            // Once rename starts, finish both directory updates with this grant.
            let _io = flush.grant(budget, HEADER as usize).await?;
            disk_io::rename(tmp, root.join(FILE)).await?;
            state.snapshot_bytes = offset;
            state.log_bytes = 0;
            state.log_truncate = false;
            // A retry syncs this rename before replacing the preceding log.
            *persistence.disk.lock().unwrap() = DiskState {
                full: true,
                ..state
            };
            open_log_for_append(root, &mut state).await?;
            state.full = false;
            *persistence.disk.lock().unwrap() = state;
            return Ok(true);
        }

        #[cfg(test)]
        self.pause_access_flush().await;
        self.append_access_delta(scan, state, budget, flush).await
    }

    async fn append_access_delta(
        &self,
        mut scan: AccessScan,
        mut state: DiskState,
        budget: Option<&crate::background::BackgroundBudget>,
        flush: Flush<'_>,
    ) -> io::Result<bool> {
        let persistence = &self.journal.access;
        let mut fd = None;
        let mut _io = None;
        let mut written = 0;
        let result = loop {
            let (bytes, done) = match scan.next_batch(&self.journal.model, flush).await {
                Ok(batch) => batch,
                Err(error) => break Err(error),
            };
            if !bytes.is_empty() {
                let len = bytes.len() as u64;
                let header = if state.log_bytes == 0 { HEADER } else { 0 };
                if written + len + header > flush.byte_limit() {
                    break Err(io::ErrorKind::Interrupted.into());
                }
                if fd.is_none() {
                    match flush.grant(budget, bytes.len() + header as usize).await {
                        Ok(permit) => _io = permit,
                        Err(error) => break Err(error),
                    }
                    persistence.disk.lock().unwrap().log_truncate = true;
                    fd = Some(open_log_for_append(&persistence.root, &mut state).await?);
                } else if let Some(budget) = budget {
                    // Keep the permit needed to sync a valid prefix if
                    // shutdown interrupts the next batch's pacing.
                    if let Err(error) = flush.wait(budget.bytes(bytes.len())).await {
                        break Err(error);
                    }
                }
                disk_io::write(
                    fd.as_ref().unwrap().clone(),
                    bytes.into(),
                    state.log_bytes + written,
                )
                .await?;
                written += len;
            }
            if done {
                break Ok(());
            }
        };
        let changed = fd.is_some();
        if let Some(fd) = fd {
            // Only complete frames advance the durable end after sync.
            disk_io::sync(fd).await?;
            state.log_bytes += written;
        }
        let result = result.and_then(|()| flush.check());
        state.full = result.is_err();
        *persistence.disk.lock().unwrap() = state;
        result.map(|()| changed)
    }
}

struct AccessScan {
    cursor: Address,
    ceiling: Address,
    full: bool,
}

impl AccessScan {
    async fn next_batch(
        &mut self,
        model: &super::super::catalog::LiveModel,
        flush: Flush<'_>,
    ) -> io::Result<(Vec<u8>, bool)> {
        let mut batch = AgeBatch::default();
        loop {
            flush.check()?;
            let done = {
                let catalog = model.0.read().unwrap();
                let recency = catalog.recency.as_ref().unwrap();
                let mut sample = |id, row| {
                    let segment = catalog.segment(id).unwrap();
                    let offset = segment.records.offset_at(row);
                    // The journal applies events before acknowledging fsync.
                    if offset < recency.durable_end(id) {
                        if let Some(time) = recency.sample(id, row, self.full) {
                            batch.push(id, offset as u32, time);
                        }
                    }
                    true
                };
                if self.full {
                    catalog.visit_recency(
                        &mut self.cursor,
                        self.ceiling,
                        ROWS_PER_BATCH,
                        |_, id, row| sample(id, row),
                    )
                } else {
                    catalog.visit_dirty_recency(
                        &mut self.cursor,
                        self.ceiling,
                        ROWS_PER_BATCH,
                        sample,
                    )
                }
            };
            // Keep each on-disk frame bounded independently of the write size.
            batch.flush();
            if done || batch.bytes.len() >= WRITE_BYTES {
                return Ok((batch.bytes, done));
            }
            tokio::task::yield_now().await;
        }
    }
}

pub(super) fn relocation_bytes(rows: usize) -> usize {
    if rows == 0 {
        return 0;
    }
    rows * ENTRY_BYTES
        + rows.div_ceil(ROWS_PER_BATCH) * (GROUP_HEADER_BYTES + FRAME_HEADER_BYTES)
        + HEADER as usize
}

fn open_access(path: &std::path::Path) -> io::Result<Option<BufReader<std::fs::File>>> {
    match std::fs::File::open(path) {
        Ok(file) => Ok(Some(BufReader::with_capacity(1 << 20, file))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

async fn open_log_for_append(
    root: &std::path::Path,
    state: &mut DiskState,
) -> io::Result<Arc<OwnedFd>> {
    let path = root.join(LOG);
    if state.log_bytes == 0 {
        // A previous checkpoint may have stopped after renaming its snapshot.
        // Commit the directory entry before overwriting a superseded log.
        disk_io::sync_dir(root.to_owned()).await?;
        let temp = root.join(LOG_TEMP);
        let fd = disk_io::open(temp.clone(), disk_io::Open::Replace).await?;
        disk_io::write(fd.clone(), header(DELTA_MAGIC, state.generation).into(), 0).await?;
        disk_io::sync(fd.clone()).await?;
        disk_io::rename(temp, path).await?;
        disk_io::sync_dir(root.to_owned()).await?;
        state.log_bytes = HEADER;
        return Ok(fd);
    }
    let fd = disk_io::open(path, disk_io::Open::Update).await?;
    if state.log_truncate {
        disk_io::truncate(fd.clone(), state.log_bytes).await?;
        state.log_truncate = false;
    }
    Ok(fd)
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

fn reset_state(root: &std::path::Path) -> io::Result<DiskState> {
    // A new checkpoint must also supersede any compatible log left after
    // a reset. A crash before log replacement must not resurrect its ages.
    let path = root.join(LOG);
    let generation = match open_access(&path)? {
        Some(mut file) => match read_header(&mut file, DELTA_MAGIC) {
            Ok(generation) => generation,
            Err(error) if error.kind() == io::ErrorKind::InvalidData => 0,
            Err(error) => return Err(error),
        },
        None => 0,
    };
    let generation = if generation == u64::MAX {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        std::fs::File::open(root)?.sync_all()?;
        0
    } else {
        generation
    };
    Ok(DiskState {
        generation,
        ..DiskState::default()
    })
}

#[derive(Default)]
struct AgeBatch {
    bytes: Vec<u8>,
    group: Vec<u8>,
    segment: u64,
}

impl AgeBatch {
    fn push(&mut self, segment: u64, offset: u32, time: u64) {
        if self.group.is_empty() || self.segment != segment {
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

// Called after syncing the destination data and directory, with the relocation
// gate and I/O permit held but outside the foreground publication locks. A
// failed Replace leaves the source visible. The destination stays invisible
// and its durable file or WAL record prevents allocator reuse after a crash.
pub(in crate::small_store) async fn persist_relocation(
    journal: &journal::Journal,
    old: u64,
    copy: &StagedSegment,
) -> io::Result<bool> {
    let persistence = &journal.access;
    let _writer = persistence.log_write.lock().await;
    let mut state = {
        let mut disk = persistence.disk.lock().unwrap();
        if disk.snapshot_bytes == 0 {
            return Ok(false);
        }
        let state = *disk;
        // On error a retry must discard only the attempted append.
        disk.log_truncate = true;
        state
    };
    let result = async {
        let fd = open_log_for_append(&persistence.root, &mut state).await?;
        let mut translations: Vec<_> = copy
            .records
            .iter()
            .map(|(&old, record)| (old, record.new_offset))
            .collect();
        translations.sort_unstable();
        let mut batch = AgeBatch::default();
        for rows in translations.chunks(ROWS_PER_BATCH) {
            {
                let catalog = journal.model.0.read().unwrap();
                let recency = catalog.recency.as_ref().unwrap();
                let times = recency.segment_times(old);
                let records = &catalog.segment(old).unwrap().records;
                let mut row = 0;
                for &(offset, new_offset) in rows {
                    row = records.position_from(row, offset);
                    // Staged deletes are reconciled before Replace.
                    if records.offset_matches(row, offset) {
                        let time = recency.time(times[row].load(Ordering::Relaxed));
                        batch.push(copy.id, new_offset as u32, time);
                        row += 1;
                    }
                }
            }
            batch.flush();
            if batch.bytes.len() >= WRITE_BYTES {
                let bytes = std::mem::take(&mut batch.bytes);
                let len = bytes.len() as u64;
                disk_io::write(fd.clone(), bytes.into(), state.log_bytes).await?;
                state.log_bytes += len;
            }
        }
        if !batch.bytes.is_empty() {
            let len = batch.bytes.len() as u64;
            disk_io::write(fd.clone(), batch.bytes.into(), state.log_bytes).await?;
            state.log_bytes += len;
        }
        #[cfg(test)]
        if journal
            .control
            .fail_access_sync
            .swap(false, Ordering::Relaxed)
        {
            return Err(io::Error::other("injected access relocation sync failure"));
        }
        disk_io::sync(fd).await?;
        *persistence.disk.lock().unwrap() = state;
        Ok(true)
    }
    .await;
    persistence.refresh_allocated(result).await?;
    Ok(true)
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
