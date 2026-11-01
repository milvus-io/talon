//! Advisory recency: coalesced dirty keys, a bounded delta log, and infrequent
//! full checkpoints. Neither file establishes residency or stores payloads.
#[cfg(test)]
mod tests;

use super::*;
use crate::eviction::AccessHandle;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub(super) const FILE: &str = "access.checkpoint";
const LOG: &str = "access.log";
const LEGACY: &[u8; 8] = b"TLSA0001";
const MAGIC: &[u8; 8] = b"TLSA0002";
const LOG_MAGIC: &[u8; 8] = b"TLSL0001";
const HEADER: u64 = 16;
const COMPACT_BYTES: u64 = 64 << 20;
const MAX_DIRTY_KEYS: usize = 262_144;
const MAX_DIRTY_BYTES: usize = 64 << 20;

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

/// Attached only to packed whole-object read tokens when TTI is enabled. The
/// queue owns metadata, never a payload pin; the weak back-reference avoids a cycle.
pub(crate) struct DirtyAccess {
    key: Arc<[u8]>,
    clock: Arc<AtomicU64>,
    queue: Weak<DirtyQueue>,
    pending: AtomicBool,
}
impl DirtyAccess {
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
            let bytes = self.key.len() + std::mem::size_of::<Self>() + 64;
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
pub(super) struct DiskState {
    generation: u64,
    snapshot_bytes: u64,
    log_bytes: u64,
    full: bool,
}
impl Default for DiskState {
    fn default() -> Self {
        Self {
            generation: 0,
            snapshot_bytes: 0,
            log_bytes: 0,
            full: true,
        }
    }
}

pub(super) fn allocated(root: &std::path::Path) -> io::Result<u64> {
    [FILE, LOG]
        .into_iter()
        .try_fold(0, |sum, name| match std::fs::metadata(root.join(name)) {
            Ok(m) => Ok(sum + m.blocks() * 512),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(sum),
            Err(e) => Err(e),
        })
}

fn header(magic: &[u8; 8], generation: u64) -> Vec<u8> {
    let mut bytes = magic.to_vec();
    bytes.extend_from_slice(&generation.to_le_bytes());
    bytes
}

fn decode(bytes: &[u8], ages: &mut HashMap<BlockId, u64>) -> io::Result<()> {
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
fn decode_log(mut bytes: &[u8], ages: &mut HashMap<BlockId, u64>) -> io::Result<bool> {
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

fn encode(bytes: &mut Vec<u8>, key: &[u8], last: u64) -> io::Result<()> {
    let mut record = last.to_le_bytes().to_vec();
    record.extend_from_slice(key);
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
            *self.access_disk.lock().unwrap() = state;
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
        for stream in self.streams() {
            for (key, loc) in stream.entries.write().unwrap().iter_mut() {
                if let Some(access) = loc.access.take() {
                    loc.access = Some(self.attach_access(key.clone(), access, false));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn track_access(&self, id: &BlockId) -> io::Result<()> {
        let encoded = format::key(id)?;
        for stream in self.streams() {
            let mut entries = stream.entries.write().unwrap();
            let key = entries.get_key_value(encoded.as_slice()).map(|(key, _)| key.clone());
            if let Some(key) = key {
                let loc = entries.get_mut(key.as_ref()).unwrap();
                if let Some(access) = loc.access.take() {
                    loc.access = Some(self.attach_access(key, access, true));
                }
                break;
            }
        }
        Ok(())
    }

    fn attach_access(&self, key: Arc<[u8]>, access: AccessHandle, dirty: bool) -> AccessHandle {
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
    ) -> io::Result<()> {
        let store = self.clone();
        self.tasks
            .run(async move {
                let _gate = store.access_checkpoint.lock().await;
                let prepare = store.clone();
                let (mut state, full, bytes) =
                    tokio::task::spawn_blocking(move || -> io::Result<_> {
                        prepare.enable_access_tracking()?;
                        let mut state = *prepare.access_disk.lock().unwrap();
                        let overflow = prepare.access_dirty.overflow.swap(false, Ordering::AcqRel);
                        let pending =
                            std::mem::take(&mut *prepare.access_dirty.pending.lock().unwrap());
                        let full = state.full
                            || overflow
                            || state.log_bytes >= COMPACT_BYTES.max(state.snapshot_bytes);
                        // Failure after consuming dirty state must retry from all live
                        // timestamps, including a partial append or ambiguous rename.
                        prepare.access_disk.lock().unwrap().full = true;
                        let mut bytes = Vec::new();
                        if full {
                            state.generation = state
                                .generation
                                .checked_add(1)
                                .ok_or_else(|| invalid("access generation exhausted"))?;
                            bytes = header(MAGIC, state.generation);
                            for stream in prepare.streams() {
                                let entries: Vec<_> = stream.entries.read().unwrap().iter()
                                    .filter_map(|(key, loc)| loc.access.clone().map(|access| (key.clone(), access)))
                                    .collect();
                                for (key, access) in entries {
                                    encode(&mut bytes, &key, access.sample_small())?;
                                }
                            }
                        } else {
                            for token in pending.keys.values() {
                                token.consume();
                                let id = format::id(&token.key)?;
                                if let Some(access) = prepare.lookup(&id)?.and_then(|loc| loc.access) {
                                    // Deleted/re-admitted keys have a different clock.
                                    if Arc::ptr_eq(access.clock(), &token.clock) {
                                        encode(&mut bytes, &token.key, access.last_access())?;
                                    }
                                }
                            }
                        }
                        Ok((state, full, Bytes::from(bytes)))
                    })
                    .await
                    .map_err(io::Error::other)??;
                #[cfg(test)]
                {
                    let pause = store.access_pause.lock().unwrap().take();
                    if let Some((entered, release)) = pause {
                        entered.notify_one();
                        release.notified().await;
                    }
                }
                let written = full || !bytes.is_empty();
                if full {
                    state.snapshot_bytes = bytes.len() as u64;
                    store.publish_access(FILE, bytes, budget.as_deref()).await?;
                    store
                        .publish_access(
                            LOG,
                            header(LOG_MAGIC, state.generation).into(),
                            budget.as_deref(),
                        )
                        .await?;
                    state.log_bytes = HEADER;
                } else if !bytes.is_empty() {
                    let fd = disk_io::open(store.root.join(LOG), disk_io::Open::Update).await?;
                    store
                        .write_access(
                            fd.clone(),
                            bytes.clone(),
                            state.log_bytes,
                            budget.as_deref(),
                        )
                        .await?;
                    state.log_bytes += bytes.len() as u64;
                }
                if written {
                    let root = store.root.clone();
                    let size = tokio::task::spawn_blocking(move || allocated(&root))
                        .await
                        .map_err(io::Error::other)??;
                    store.access_bytes.store(size, Ordering::Relaxed);
                }
                state.full = false;
                *store.access_disk.lock().unwrap() = state;
                Ok(())
            })
            .await
    }

    async fn write_access(
        &self,
        fd: Arc<OwnedFd>,
        bytes: Bytes,
        offset: u64,
        budget: Option<&crate::background::BackgroundBudget>,
    ) -> io::Result<()> {
        for start in (0..bytes.len()).step_by(64 << 10) {
            let end = (start + (64 << 10)).min(bytes.len());
            if let Some(budget) = budget {
                budget.bytes(end - start).await;
            }
            let _io = match budget {
                Some(b) => Some(b.io().await),
                None => None,
            };
            disk_io::write(fd.clone(), bytes.slice(start..end), offset + start as u64).await?;
        }
        let _io = match budget {
            Some(b) => Some(b.io().await),
            None => None,
        };
        disk_io::sync(fd).await
    }

    async fn publish_access(
        &self,
        name: &str,
        bytes: Bytes,
        budget: Option<&crate::background::BackgroundBudget>,
    ) -> io::Result<()> {
        let tmp = self.root.join(format!("{name}.tmp"));
        let fd = disk_io::open(tmp.clone(), disk_io::Open::Replace).await?;
        self.write_access(fd, bytes, 0, budget).await?;
        let _io = match budget {
            Some(b) => Some(b.io().await),
            None => None,
        };
        disk_io::rename(tmp, self.root.join(name)).await?;
        disk_io::sync_dir(self.root.clone()).await
    }
}
