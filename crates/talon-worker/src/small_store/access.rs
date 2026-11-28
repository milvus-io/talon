//! Advisory recency: coalesced dirty keys, a bounded delta log, and infrequent
//! full checkpoints. Neither file establishes residency or stores payloads.
mod compact;
pub(super) use compact::persist_relocation;
#[cfg(test)]
mod tests;

use super::*;
use crate::eviction::AccessHandle;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{watch, OwnedSemaphorePermit};
use tokio::time::Instant as TokioInstant;

pub(super) const FILE: &str = "access.checkpoint";
const LOG: &str = "access.log";
const SNAPSHOT_TEMP: &str = "access.checkpoint.tmp";
const LOG_TEMP: &str = "access.log.tmp";
const LEGACY: &[u8; 8] = b"TLSA0001";
const MAGIC: &[u8; 8] = b"TLSA0002";
const LOG_MAGIC: &[u8; 8] = b"TLSL0001";
const HEADER: u64 = 16;
const MIN_COMPACT_BYTES: u64 = 1 << 20;
const MAX_DIRTY_KEYS: usize = 262_144;
const MAX_DIRTY_BYTES: usize = 64 << 20;
const WRITE_BYTES: usize = 64 << 10;
const SHUTDOWN_DELTA_BYTES: u64 = 16 << 20;

#[derive(Clone, Copy)]
enum Flush<'a> {
    Periodic(Option<&'a watch::Receiver<bool>>),
    Shutdown { deadline: TokioInstant, bytes: u64 },
}

impl Flush<'_> {
    fn allows_full(self) -> bool {
        matches!(self, Self::Periodic(_))
    }

    fn byte_limit(self) -> u64 {
        match self {
            Self::Periodic(_) => u64::MAX,
            Self::Shutdown { bytes, .. } => bytes,
        }
    }

    fn check(self) -> io::Result<()> {
        let stopped = match self {
            Self::Periodic(stop) => stop.is_some_and(|stop| *stop.borrow()),
            Self::Shutdown { deadline, .. } => TokioInstant::now() >= deadline,
        };
        if stopped {
            return Err(io::ErrorKind::Interrupted.into());
        }
        Ok(())
    }

    // Race only resource waits. Submitted I/O and its durability step
    // finish under their existing grant.
    async fn wait<T>(self, work: impl Future<Output = T>) -> io::Result<T> {
        self.check()?;
        match self {
            Self::Periodic(Some(stop)) => {
                let mut stop = stop.clone();
                tokio::select! {
                    biased;
                    _ = stop.wait_for(|stopped| *stopped) => Err(io::ErrorKind::Interrupted.into()),
                    result = work => Ok(result),
                }
            }
            Self::Periodic(None) => Ok(work.await),
            Self::Shutdown { deadline, .. } => tokio::time::timeout_at(deadline, work)
                .await
                .map_err(|_| io::ErrorKind::Interrupted.into()),
        }
    }

    async fn grant(
        self,
        budget: Option<&crate::background::BackgroundBudget>,
        bytes: usize,
    ) -> io::Result<Option<OwnedSemaphorePermit>> {
        self.wait(async {
            if let Some(budget) = budget {
                if bytes != 0 {
                    budget.bytes(bytes).await;
                }
                Some(budget.io().await)
            } else {
                None
            }
        })
        .await
    }
}

#[derive(Default)]
struct Pending {
    keys: HashMap<usize, Arc<DirtyAccess>>,
    bytes: usize,
}

#[derive(Default)]
pub(super) struct DirtyQueue {
    pending: Mutex<Pending>,
    overflow: AtomicBool,
    enabled: AtomicBool,
}

/// Attached to packed read tokens when access tracking is enabled. The
/// queue owns metadata, never a payload pin; the weak back-reference avoids a cycle.
pub(crate) struct DirtyAccess {
    key: SmallKey,
    clock: Arc<AtomicU64>,
    queue: Weak<DirtyQueue>,
    pending: AtomicBool,
}
impl DirtyAccess {
    pub(crate) fn clock(&self) -> &Arc<AtomicU64> {
        &self.clock
    }
    pub(crate) fn touch(self: &Arc<Self>) {
        // Every hit publishes its timestamp even when already pending. The
        // consumer resets before sampling, so a racing hit belongs to this or
        // the next flush; it cannot be cleared after the sample.
        if self.pending.swap(true, Ordering::AcqRel) {
            return;
        }
        let Some(queue) = self.queue.upgrade() else {
            return;
        };
        let mut pending = queue.pending.lock().unwrap();
        let identity = Arc::as_ptr(self) as usize;
        if !pending.keys.contains_key(&identity) {
            let bytes = format::key_len(&self.key) + std::mem::size_of::<Self>() + 64;
            if pending.keys.len() >= MAX_DIRTY_KEYS || pending.bytes + bytes > MAX_DIRTY_BYTES {
                queue.overflow.store(true, Ordering::Release);
                return;
            }
            pending.bytes += bytes;
        }
        pending.keys.insert(identity, self.clone());
    }

    pub(crate) fn consume(&self) {
        self.pending.swap(false, Ordering::AcqRel);
    }
}

#[derive(Clone, Copy)]
struct DiskState {
    generation: u64,
    snapshot_bytes: u64,
    log_bytes: u64,
    log_truncate: bool,
    full: bool,
}
impl Default for DiskState {
    fn default() -> Self {
        Self {
            generation: 0,
            snapshot_bytes: 0,
            log_bytes: 0,
            log_truncate: false,
            full: true,
        }
    }
}

pub(super) struct Persistence {
    root: PathBuf,
    // A relocation keeps the generation stable through metadata Replace.
    pub(super) relocation: tokio::sync::RwLock<()>,
    log_write: tokio::sync::Mutex<()>,
    disk: Mutex<DiskState>,
    bytes: AtomicU64,
}

impl Persistence {
    pub(super) fn new(root: PathBuf) -> io::Result<Self> {
        discard_temporary(&root);
        Ok(Self {
            bytes: AtomicU64::new(allocated(&root)?),
            root,
            relocation: Default::default(),
            log_write: Default::default(),
            disk: Default::default(),
        })
    }

    pub(super) fn allocated_bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    pub(super) fn relocation_bytes(&self, rows: usize) -> usize {
        compact::relocation_bytes(rows)
    }

    async fn refresh_allocated(&self, result: io::Result<bool>) -> io::Result<()> {
        if matches!(result, Ok(false)) {
            return Ok(());
        }
        let root = self.root.clone();
        let discard = result.is_err();
        let size = tokio::task::spawn_blocking(move || {
            if discard {
                discard_temporary(&root);
            }
            allocated(&root)
        })
        .await
        .map_err(io::Error::other)
        .and_then(|size| size);
        match size {
            Ok(size) => self.bytes.store(size, Ordering::Relaxed),
            Err(error) if discard => tracing::warn!(%error, root = %self.root.display(),
                "refreshing small access allocation failed"),
            Err(error) => return Err(error),
        }
        result.map(|_| ())
    }
}

fn discard_temporary(root: &std::path::Path) {
    for name in [SNAPSHOT_TEMP, LOG_TEMP] {
        let path = root.join(name);
        if let Err(error) = std::fs::remove_file(&path) {
            if error.kind() != io::ErrorKind::NotFound {
                tracing::warn!(%error, path = %path.display(), "discarding temporary small access file failed");
            }
        }
    }
}

fn allocated(root: &std::path::Path) -> io::Result<u64> {
    [FILE, LOG, SNAPSHOT_TEMP, LOG_TEMP].into_iter().try_fold(
        0,
        |sum, name| match std::fs::metadata(root.join(name)) {
            Ok(m) => Ok(sum + m.blocks() * 512),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(sum),
            Err(e) => Err(e),
        },
    )
}

fn header(magic: &[u8; 8], generation: u64) -> Vec<u8> {
    let mut bytes = magic.to_vec();
    bytes.extend_from_slice(&generation.to_le_bytes());
    bytes
}

fn decode(bytes: &[u8], ages: &mut HashMap<CacheUnit, u64>) -> io::Result<()> {
    for mut record in format::frames(bytes)? {
        let last = format::u64(&mut record)?;
        let id = format::id(record)?;
        ages.entry(id)
            .and_modify(|old| *old = (*old).max(last))
            .or_insert(last);
    }
    Ok(())
}

// An interrupted append can leave an incomplete final frame. Keep the validated
// prefix, but force compaction before writing again. Complete corrupt frames
// still invalidate advisory ages rather than guessing at subsequent boundaries.
fn decode_log(mut bytes: &[u8], ages: &mut HashMap<CacheUnit, u64>) -> io::Result<bool> {
    while !bytes.is_empty() {
        if bytes.len() < 12 {
            return Ok(true);
        }
        let len = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        if len > format::MAX_KEY + 32 {
            return Err(invalid("small access log frame length"));
        }
        if bytes.len() < len + 12 {
            return Ok(true);
        }
        decode(&bytes[..len + 12], ages)?;
        bytes = &bytes[len + 12..];
    }
    Ok(false)
}

fn encode(bytes: &mut Vec<u8>, key: &dyn SmallIdentity, last: u64) -> io::Result<()> {
    let mut record = last.to_le_bytes().to_vec();
    format::encode_key(&mut record, key)?;
    format::frame(bytes, &record)
}

impl SmallObjectStore {
    /// Startup only, after policy reconstruction. Validate both files before
    /// applying any ages; corruption gives fresh windows without losing payloads.
    pub(super) fn restore_access(&self) {
        let result = (|| -> io::Result<()> {
            let bytes = match std::fs::read(self.root.join(FILE)) {
                Ok(bytes) => bytes,
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
                Err(e) => return Err(e),
            };
            let mut state = DiskState::default();
            let mut ages = HashMap::new();
            if let Some(body) = bytes.strip_prefix(LEGACY) {
                decode(body, &mut ages)?;
            } else {
                let mut body = bytes
                    .strip_prefix(MAGIC)
                    .ok_or_else(|| invalid("small access snapshot version"))?;
                state.generation = format::u64(&mut body)?;
                state.snapshot_bytes = bytes.len() as u64;
                decode(body, &mut ages)?;
                match std::fs::read(self.root.join(LOG)) {
                    Ok(log) => {
                        let mut body = log
                            .strip_prefix(LOG_MAGIC)
                            .ok_or_else(|| invalid("small access log version"))?;
                        let generation = format::u64(&mut body)?;
                        if generation == state.generation {
                            state.full = decode_log(body, &mut ages)?;
                            state.log_bytes = log.len() as u64;
                        }
                        // A crash after snapshot publication may leave the old
                        // log. The new snapshot already covers it; do not replay it.
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
            }
            let mut matched = 0;
            for (id, last) in ages {
                if let Some(access) = self.lookup_entry(&id)?.and_then(|loc| loc.access) {
                    access.restore(last);
                    matched += 1;
                    // Persist repaired zero/future ages rather than resetting
                    // their idle window again on every subsequent restart.
                    state.full |= last == 0 || last > access.last_access();
                }
            }
            let residents: usize = self
                .streams()
                .iter()
                .map(|stream| stream.entries.read().unwrap().len())
                .sum();
            // A payload may have sealed after the last access flush. Its fresh
            // recovery age must be saved even if it is never read again.
            state.full |= matched < residents;
            *self.journal.access.disk.lock().unwrap() = state;
            Ok(())
        })();
        if let Err(error) = result {
            tracing::warn!(%error, root = %self.root.display(), "small object access recovery failed; starting fresh idle windows");
        }
    }

    /// Startup/configuration only; normal flushes do not walk resident keys.
    pub(crate) fn enable_access_tracking(&self) -> io::Result<()> {
        if self.access_dirty.enabled.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        if self.has_recency() {
            self.restore_compact_access();
            return Ok(());
        }
        let restore = self
            .policy()
            .map_or(true, |policy| policy.enable_small_clock());
        for stream in self.streams() {
            let mut directory = stream.entries.write().unwrap();
            let entries: Vec<_> = directory.iter().collect();
            for (key, loc) in entries {
                if let Some(access) = loc.access {
                    let access = self.attach_access(key.clone(), access.enable_clock(), false);
                    directory.attach_access(&key, access);
                }
            }
        }
        if restore {
            self.restore_access();
        }
        Ok(())
    }

    pub(crate) fn track_access(&self, id: &(impl SmallStoreKey + ?Sized)) -> io::Result<()> {
        format::validate_key(&id.view())?;
        if !self.access_dirty.enabled.load(Ordering::Acquire) {
            return Ok(());
        }
        if self.has_recency() {
            if let Some(loc) = self.lookup_entry(&id.view())? {
                loc.touch();
            }
            return Ok(());
        }
        for stream in self.streams() {
            let mut entries = stream.entries.write().unwrap();
            let key = entries
                .get_key_value(&id.view())
                .map(|(key, _)| key.clone());
            if let Some(key) = key {
                let loc = entries.get(&key).unwrap();
                if let Some(access) = loc.access {
                    let access = self.attach_access(key.clone(), access, true);
                    entries.attach_access(&key, access);
                }
                break;
            }
        }
        Ok(())
    }

    fn attach_access(&self, key: SmallKey, access: AccessHandle, dirty: bool) -> AccessHandle {
        let token = Arc::new(DirtyAccess {
            key,
            clock: access.clock().clone(),
            queue: Arc::downgrade(&self.access_dirty),
            pending: AtomicBool::new(false),
        });
        if dirty {
            token.touch();
        }
        access.track_small(token)
    }

    pub(crate) async fn checkpoint_access(
        self: &Arc<Self>,
        budget: Option<Arc<crate::background::BackgroundBudget>>,
        stop: Option<&watch::Receiver<bool>>,
    ) -> io::Result<()> {
        let stop = stop.cloned();
        let store = self.clone();
        self.tasks
            .run(async move {
                store
                    .flush_access(budget.as_deref(), Flush::Periodic(stop.as_ref()))
                    .await
            })
            .await
    }

    pub(crate) async fn checkpoint_access_on_shutdown(
        self: &Arc<Self>,
        budget: Option<Arc<crate::background::BackgroundBudget>>,
        deadline: TokioInstant,
    ) -> io::Result<()> {
        let store = self.clone();
        self.tasks
            .run(async move {
                store
                    .flush_access(
                        budget.as_deref(),
                        Flush::Shutdown {
                            deadline,
                            bytes: SHUTDOWN_DELTA_BYTES,
                        },
                    )
                    .await
            })
            .await
    }

    async fn flush_access(
        self: &Arc<Self>,
        budget: Option<&crate::background::BackgroundBudget>,
        flush: Flush<'_>,
    ) -> io::Result<()> {
        let _gate = flush.wait(self.access_checkpoint.lock()).await?;
        self.enable_access_tracking()?;
        let persistence = &self.journal.access;
        if self.has_recency() {
            // Cleanup must remain serialized with log repair and Replace.
            let _relocation = flush.wait(persistence.relocation.write()).await?;
            let result = self.checkpoint_compact_access(budget, flush).await;
            persistence.refresh_allocated(result).await
        } else {
            let result = self.checkpoint_keyed_access(budget, flush).await;
            persistence.refresh_allocated(result).await
        }
    }

    async fn checkpoint_keyed_access(
        self: &Arc<Self>,
        budget: Option<&crate::background::BackgroundBudget>,
        flush: Flush<'_>,
    ) -> io::Result<bool> {
        let prepare = self.clone();
        let stop = match flush {
            Flush::Periodic(stop) => stop.cloned(),
            _ => None,
        };
        let shutdown = match flush {
            Flush::Shutdown { deadline, bytes } => Some((deadline, bytes)),
            _ => None,
        };
        let Some((mut state, full, bytes)) =
            tokio::task::spawn_blocking(move || -> io::Result<_> {
                let flush = shutdown.map_or(Flush::Periodic(stop.as_ref()), |(deadline, bytes)| {
                    Flush::Shutdown { deadline, bytes }
                });
                let disk = &prepare.journal.access.disk;
                let mut state = *disk.lock().unwrap();
                let overflow = prepare.access_dirty.overflow.swap(false, Ordering::AcqRel);
                let full = state.full
                    || overflow
                    || state.log_bytes >= MIN_COMPACT_BYTES.max(state.snapshot_bytes.div_ceil(2));
                // An interrupted flush may have consumed dirty state.
                disk.lock().unwrap().full = true;
                if full && !flush.allows_full() {
                    return Ok(None);
                }
                flush.check()?;
                let pending = std::mem::take(&mut *prepare.access_dirty.pending.lock().unwrap());
                let mut bytes = Vec::new();
                if full {
                    state.generation = state
                        .generation
                        .checked_add(1)
                        .ok_or_else(|| invalid("access generation exhausted"))?;
                    bytes = header(MAGIC, state.generation);
                    for stream in prepare.streams() {
                        flush.check()?;
                        let entries: Vec<_> = stream
                            .entries
                            .read()
                            .unwrap()
                            .iter()
                            .filter_map(|(key, loc)| {
                                loc.access.clone().map(|access| (key.clone(), access))
                            })
                            .collect();
                        for (key, access) in entries {
                            flush.check()?;
                            encode(&mut bytes, &key, access.sample_small())?;
                        }
                    }
                } else {
                    for token in pending.keys.values() {
                        flush.check()?;
                        token.consume();
                        if let Some(access) =
                            prepare.lookup_entry(&token.key)?.and_then(|loc| loc.access)
                        {
                            // Deleted/re-admitted keys have a different clock.
                            if Arc::ptr_eq(access.clock(), &token.clock) {
                                encode(&mut bytes, &token.key, access.last_access())?;
                                if bytes.len() as u64 > flush.byte_limit() {
                                    return Err(io::ErrorKind::Interrupted.into());
                                }
                            }
                        }
                    }
                }
                Ok(Some((state, full, Bytes::from(bytes))))
            })
            .await
            .map_err(io::Error::other)??
        else {
            return Ok(false);
        };
        #[cfg(test)]
        self.pause_access_flush().await;
        let persistence = &self.journal.access;
        let root = &persistence.root;
        let written = full || !bytes.is_empty();
        if full {
            state.snapshot_bytes = bytes.len() as u64;
            let tmp = root.join(SNAPSHOT_TEMP);
            let fd = {
                let _io = flush.grant(budget, 0).await?;
                disk_io::open(tmp.clone(), disk_io::Open::Replace).await?
            };
            self.write_access_chunks(&fd, bytes, 0, budget, flush)
                .await?;
            self.sync_access(fd, budget, flush).await?;
            // Complete both directory updates under the publication grant.
            let _io = flush.grant(budget, HEADER as usize).await?;
            disk_io::rename(tmp, root.join(FILE)).await?;
            state.log_bytes = 0;
            *persistence.disk.lock().unwrap() = DiskState {
                full: true,
                ..state
            };
            disk_io::sync_dir(root.clone()).await?;
            let temp = root.join(LOG_TEMP);
            let fd = disk_io::open(temp.clone(), disk_io::Open::Replace).await?;
            disk_io::write(fd.clone(), header(LOG_MAGIC, state.generation).into(), 0).await?;
            disk_io::sync(fd).await?;
            disk_io::rename(temp, root.join(LOG)).await?;
            disk_io::sync_dir(root.clone()).await?;
            state.log_bytes = HEADER;
        } else if !bytes.is_empty() {
            let fd = {
                let _io = flush.grant(budget, 0).await?;
                disk_io::open(root.join(LOG), disk_io::Open::Update).await?
            };
            let len = bytes.len() as u64;
            self.write_access_chunks(&fd, bytes, state.log_bytes, budget, flush)
                .await?;
            state.log_bytes += len;
            self.sync_access(fd, budget, flush).await?;
        }
        state.full = false;
        *persistence.disk.lock().unwrap() = state;
        Ok(written)
    }

    #[cfg(test)]
    async fn pause_access_flush(&self) {
        let pause = self.access_pause.lock().unwrap().take();
        if let Some((entered, release)) = pause {
            entered.notify_one();
            release.notified().await;
        }
    }

    async fn write_access_chunks(
        &self,
        fd: &Arc<OwnedFd>,
        bytes: Bytes,
        offset: u64,
        budget: Option<&crate::background::BackgroundBudget>,
        flush: Flush<'_>,
    ) -> io::Result<()> {
        for start in (0..bytes.len()).step_by(WRITE_BYTES) {
            let end = (start + WRITE_BYTES).min(bytes.len());
            let _io = flush.grant(budget, end - start).await?;
            disk_io::write(fd.clone(), bytes.slice(start..end), offset + start as u64).await?;
        }
        Ok(())
    }

    async fn sync_access(
        &self,
        fd: Arc<OwnedFd>,
        budget: Option<&crate::background::BackgroundBudget>,
        flush: Flush<'_>,
    ) -> io::Result<()> {
        let _io = flush.grant(budget, 0).await?;
        disk_io::sync(fd).await
    }
}
