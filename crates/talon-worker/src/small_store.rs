//! Packed small-object cache. Published payload extents are never overwritten.
//!
//! Data streams append independently; one bounded native writer owns the
//! namespace metadata log and snapshots. Durable prefixes survive before sealing.
//! Relocation commits are durable before old payload files are unlinked.
mod access;
pub(crate) use access::DirtyAccess;
mod disk_io;
mod flush;
mod format;
mod journal;
mod rings;
pub use rings::SmallObjectStore;
#[cfg(test)]
pub(crate) mod tests;

use crate::eviction::AccessHandle;
use crate::{CacheUnit, Lru};
use bytes::Bytes;
use format::invalid;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::OpenOptions;
use std::io;
use std::os::fd::OwnedFd;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};
use talon_core::{BlockHandle, BlockId};

#[derive(Clone)]
pub(crate) struct Location {
    file: Arc<SegmentFile>,
    segment: u64,
    offset: u64,
    len: u64,
    checksum: u64,
    // Shared with the eviction policy; relocation preserves this exact token.
    access: Option<AccessHandle>,
}
/// A resolved read owns an fd before the directory can publish an unlink.
pub(crate) struct ReadLocation {
    location: Location,
    fd: Arc<OwnedFd>,
}
impl ReadLocation {
    pub(crate) fn len(&self) -> u64 {
        self.location.len
    }

    pub(crate) fn touch(&self) {
        self.location.touch();
    }

    pub(crate) fn range(&self, offset: u64, len: u64) -> io::Result<BlockHandle> {
        if offset.checked_add(len).map_or(true, |end| end > self.len()) {
            return Err(invalid("small object range out of bounds"));
        }
        self.touch();
        Ok(BlockHandle::from_shared(
            self.fd.clone(),
            self.location.offset + offset,
            len,
        ))
    }

    // Deliberately does not touch: relocation also reads through this snapshot.
    pub(crate) async fn read(&self) -> io::Result<Bytes> {
        let bytes =
            disk_io::read(self.fd.clone(), self.location.offset, self.len() as usize).await?;
        if xxhash_rust::xxh3::xxh3_64(&bytes) != self.location.checksum {
            return Err(invalid("small object payload checksum"));
        }
        Ok(bytes)
    }
}

/// Only writers and borrowed readers retain descriptors. Sealed locators keep
/// a weak reference, so idle segments consume no fds regardless of their count.
struct SegmentFile {
    path: PathBuf,
    fd: tokio::sync::Mutex<Weak<OwnedFd>>,
}
impl SegmentFile {
    async fn open(&self) -> io::Result<Arc<OwnedFd>> {
        let mut cached = self.fd.lock().await;
        if let Some(fd) = cached.upgrade() {
            return Ok(fd);
        }
        let fd = disk_io::open(self.path.clone(), disk_io::Open::Read).await?;
        *cached = Arc::downgrade(&fd);
        Ok(fd)
    }
}

impl Location {
    pub(crate) fn len(&self) -> u64 {
        self.len
    }

    pub(crate) fn touch(&self) {
        if let Some(access) = &self.access {
            access.touch();
        }
    }

    // Caller excludes locator replacement/unlink until this fd is retained.
    async fn retain(self) -> io::Result<ReadLocation> {
        Ok(ReadLocation {
            fd: self.file.open().await?,
            location: self,
        })
    }
}

struct Segment {
    data: Option<Arc<OwnedFd>>,
    file: Arc<SegmentFile>,
    // Reservation ends, including writes whose completions are still pending.
    bytes: u64,
    durable_end: u64,
    metadata: u64,
    in_flight: BTreeSet<u64>,
    live: u64,
    live_payload: u64,
    allocated: u64,
    records: BTreeMap<u64, Arc<[u8]>>,
    sealed: bool,
}
impl Segment {
    async fn data(&self) -> io::Result<Arc<OwnedFd>> {
        match &self.data {
            Some(fd) => Ok(fd.clone()),
            None => self.file.open().await,
        }
    }

    async fn allocated(&self) -> io::Result<u64> {
        disk_io::allocated(self.data().await?).await
    }

    fn close_writers(&mut self) {
        self.data = None;
    }
}
struct State {
    segments: BTreeMap<u64, Segment>,
    active: Option<u64>,
    standby: Option<u64>,
    sealing: Option<u64>,
    pipeline_running: bool,
    allocated: u64,
    live: u64,
    written: u64,
    poisoned: bool,
    last_flush: Instant,
}

/// One independently appended and sealed stream within a namespace/credential scope.
/// The compact encoded identity is stored once here, without separate String
/// allocations. Locators own access tokens shared with the eviction policy;
/// small objects are never registered in the generic BlockIndex.
struct SegmentStore {
    #[cfg(test)]
    seal_pause: Mutex<Option<Arc<tests::SealPause>>>,
    #[cfg(test)]
    append_pause: Mutex<Option<Arc<tests::AppendPause>>>,
    #[cfg(test)]
    flush_pause: Mutex<Option<Arc<tests::AppendPause>>>,
    slot: usize,
    journal: Arc<journal::Journal>,
    threshold: u64,
    segment_size: u64,
    state: tokio::sync::Mutex<State>,
    // Serializes durable publication, deletion and compaction, never append.
    publication: tokio::sync::Mutex<()>,
    pipeline_changed: tokio::sync::Notify,
    appends_drained: tokio::sync::Notify,
    append_slots: tokio::sync::Semaphore,
    entries: RwLock<HashMap<Arc<[u8]>, Location>>,
    // Async opens run outside the directory lock, but must finish before GC
    // replaces locators and unlinks their source paths.
    readers: tokio::sync::RwLock<()>,
    tasks: Arc<crate::page_gc::Mutations>,
    cached_stats: RwLock<SmallStoreStats>,
    retired: Mutex<Vec<(Weak<OwnedFd>, u64)>>,
    // Keep the exclusive cache-root lease through detached blocking work.
    _root_lock: Option<Arc<crate::page_access_store::CacheRootLock>>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SmallStoreStats {
    pub live_bytes: u64,
    pub allocated_bytes: u64,
    pub retired_bytes: u64,
    pub dead_bytes: u64,
    pub segments: usize,
}

/// Only a complete short FIRST block proves this is a small object. A
/// short last block of a large object must remain in the large-object store.
fn accepts_object(threshold: u64, id: &BlockId, len: u64) -> bool {
    threshold > 0 && id.offset == 0 && len > 0 && len < threshold && len < u64::from(id.block_size)
}

impl SegmentStore {
    #[cfg(test)]
    pub fn open(
        root: PathBuf,
        threshold: u64,
        segment_size: u64,
        capacity: u64,
        root_lock: Option<Arc<crate::page_access_store::CacheRootLock>>,
    ) -> io::Result<Self> {
        validate_geometry(threshold, segment_size, capacity)?;
        let (journal, model) = journal::Journal::open(root.clone(), root_lock.clone(), None)?;
        let mut streams = SmallObjectStore::recover_streams(
            &journal,
            &model,
            threshold,
            segment_size,
            root_lock,
        )?;
        if streams.len() != 1 {
            return Err(invalid("test stream has foreign slot"));
        }
        let mut store = streams.remove(&0).unwrap();
        store.refresh_recovered_stats();
        Ok(store)
    }

    /// Construct an empty stream sharing the namespace metadata writer.
    fn empty(
        threshold: u64,
        segment_size: u64,
        root_lock: Option<Arc<crate::page_access_store::CacheRootLock>>,
        slot: usize,
        journal: Arc<journal::Journal>,
    ) -> Self {
        Self {
            #[cfg(test)]
            seal_pause: Mutex::new(None),
            #[cfg(test)]
            append_pause: Mutex::new(None),
            #[cfg(test)]
            flush_pause: Mutex::new(None),
            slot,
            journal,
            threshold,
            segment_size,
            state: tokio::sync::Mutex::new(State {
                segments: BTreeMap::new(),
                active: None,
                standby: None,
                sealing: None,
                pipeline_running: false,
                allocated: 0,
                live: 0,
                written: 0,
                poisoned: false,
                last_flush: Instant::now(),
            }),
            publication: tokio::sync::Mutex::new(()),
            pipeline_changed: tokio::sync::Notify::new(),
            appends_drained: tokio::sync::Notify::new(),
            append_slots: tokio::sync::Semaphore::new(32),
            entries: RwLock::new(HashMap::new()),
            readers: tokio::sync::RwLock::new(()),
            tasks: Arc::new(crate::page_gc::Mutations::new(true)),
            cached_stats: RwLock::new(SmallStoreStats::default()),
            retired: Mutex::new(Vec::new()),
            _root_lock: root_lock,
        }
    }

    pub fn accepts(&self, id: &BlockId, len: u64) -> bool {
        accepts_object(self.threshold, id, len)
    }

    pub fn contains(&self, id: &BlockId) -> bool {
        format::key(id)
            .ok()
            .is_some_and(|k| self.entries.read().unwrap().contains_key(k.as_slice()))
    }

    async fn lookup(&self, id: &BlockId) -> io::Result<Option<ReadLocation>> {
        let _readers = self.readers.read().await;
        let key = format::key(id)?;
        let loc = self.entries.read().unwrap().get(key.as_slice()).cloned();
        match loc {
            Some(loc) => Ok(Some(loc.retain().await?)),
            None => Ok(None),
        }
    }

    #[cfg(test)]
    pub async fn get(&self, id: &BlockId) -> io::Result<Option<BlockHandle>> {
        self.lookup(id)
            .await?
            .map(|loc| loc.range(0, loc.len()))
            .transpose()
    }

    #[cfg(test)]
    pub async fn read(&self, id: &BlockId) -> io::Result<Option<Bytes>> {
        let loc = self.lookup(id).await?;
        match loc {
            Some(loc) => Ok(Some(loc.read().await?)),
            None => Ok(None),
        }
    }

    #[cfg(test)]
    pub async fn put(self: &Arc<Self>, id: &BlockId, value: Bytes) -> io::Result<u64> {
        let store = self.clone();
        let id = id.clone();
        self.tasks
            .run(async move { store.put_inner(&id, value, None).await })
            .await
    }

    /// The public store holds the identity mutation guard through completion.
    /// Different identities reserve briefly, then submit independent I/O.
    async fn put_inner(
        self: &Arc<Self>,
        id: &BlockId,
        value: Bytes,
        policy: Option<Arc<Lru>>,
    ) -> io::Result<u64> {
        if value.is_empty()
            || value.len() > 128 << 20
            || id.offset != 0
            || value.len() as u64 >= u64::from(id.block_size)
        {
            return Err(invalid("invalid small object extent"));
        }
        let key: Arc<[u8]> = format::key(id)?.into();
        let len = value.len() as u64;
        let charge = charge(&key, len);
        if charge > self.segment_size {
            return Err(invalid("small object record exceeds segment"));
        }
        let checksum = xxhash_rust::xxh3::xxh3_64(&value);
        // Bound active submissions, not just queued segment bytes. Reservations
        // also cannot exceed one active plus one sealing segment per stream.
        let _slot = self
            .append_slots
            .acquire()
            .await
            .expect("append slots stay open");
        let (mut loc, data) = loop {
            let changed = self.pipeline_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let mut state = self.state.lock().await;
            self.healthy(&state)?;
            if let Some(old) = self.entries.write().unwrap().get_mut(key.as_ref()) {
                if old.len != len || old.checksum != checksum {
                    return Err(invalid(
                        "different bytes for immutable small object identity",
                    ));
                }
                if let Some(policy) = &policy {
                    old.access = Some(policy.insert(CacheUnit::Whole(id.clone()), charge));
                }
                return Ok(charge);
            }
            let full = state.active.is_some_and(|id| {
                let s = &state.segments[&id];
                s.bytes + s.metadata + charge > self.segment_size
            });
            if full && (state.pipeline_running || state.standby.is_none()) {
                self.start_pipeline(&mut state);
                drop(state);
                changed.await;
                continue;
            }
            if full {
                state.sealing = state.active.take();
                state.active = state.standby.take();
                self.start_pipeline(&mut state);
            }
            let segment = match state.active {
                Some(id) => id,
                None => {
                    let id = match state.standby.take() {
                        Some(id) => id,
                        None => match self.create(&mut state).await {
                            Ok(id) => id,
                            Err(error) => {
                                state.poisoned = true;
                                return Err(error);
                            }
                        },
                    };
                    state.active = Some(id);
                    id
                }
            };
            if state.standby.is_none() {
                self.start_pipeline(&mut state);
            }
            let s = state.segments.get_mut(&segment).unwrap();
            let loc = Location {
                file: s.file.clone(),
                segment,
                offset: s.bytes,
                len,
                checksum,
                access: None,
            };
            let data = s.data().await?;
            s.bytes += len;
            s.metadata += 37 + key.len() as u64;
            s.in_flight.insert(loc.offset);
            state.written += charge;
            break (loc, data);
        };
        // No append state lock is held while any of these operations is in
        // flight. Both writes are submitted; always await both, even on error,
        // so descriptors/buffers and the cache-root lease outlive kernel I/O.
        let result = async {
            #[cfg(test)]
            self.pause_append().await;
            let (payload, record) = tokio::join!(
                async {
                    disk_io::write(data.clone(), value, loc.offset).await?;
                    #[cfg(test)]
                    tests::crash("append_payload");
                    Ok::<_, io::Error>(())
                },
                async {
                    self.journal
                        .append(
                            journal::Event::Insert {
                                slot: self.slot,
                                segment: loc.segment,
                                offset: loc.offset,
                                record: journal::Record {
                                    key: key.clone(),
                                    len,
                                    checksum,
                                },
                            },
                            false,
                        )
                        .await?;
                    #[cfg(test)]
                    tests::crash("append_index");
                    Ok::<_, io::Error>(())
                }
            );
            payload?;
            record?;
            disk_io::allocated(data).await
        }
        .await;
        let mut state = self.state.lock().await;
        let segment = state
            .segments
            .get_mut(&loc.segment)
            .expect("in-flight segment is retained");
        segment.in_flight.remove(&loc.offset);
        self.appends_drained.notify_waiters();
        let allocated = match result {
            Ok(allocated) => allocated,
            Err(error) => {
                // A reserved extent/frame may be missing or torn. Do not publish
                // this segment or accept subsequent writes after the gap.
                state.poisoned = true;
                self.pipeline_changed.notify_waiters();
                self.appends_drained.notify_waiters();
                return Err(error);
            }
        };
        self.healthy(&state)?;
        let segment = state.segments.get_mut(&loc.segment).unwrap();
        // Completion order need not match reservation order. Allocation can
        // grow while another append's stat is in flight; never regress it.
        let added = allocated.saturating_sub(segment.allocated);
        segment.allocated = segment.allocated.max(allocated);
        segment.live += charge;
        segment.live_payload += loc.len;
        segment.records.insert(loc.offset, key.clone());
        state.allocated += added;
        state.live += charge;
        // Publish policy and locator together. The runtime's block gate keeps
        // eviction from retiring this admission before its mutation completes.
        let mut entries = self.entries.write().unwrap();
        if let Some(policy) = policy {
            loc.access = Some(policy.insert(CacheUnit::Whole(id.clone()), charge));
        }
        entries.insert(key, loc);
        drop(entries);
        self.refresh_stats(&state);
        Ok(charge)
    }

    #[cfg(test)]
    async fn pause_append(&self) {
        let pause = self.append_pause.lock().unwrap().take();
        if let Some(pause) = pause {
            pause.entered.notify_one();
            pause.resume.notified().await;
        }
    }

    #[cfg(test)]
    pub async fn delete(self: &Arc<Self>, id: &BlockId) -> io::Result<()> {
        let store = self.clone();
        let id = id.clone();
        self.tasks
            .run(async move { store.delete_inner(&id).await })
            .await
    }

    async fn delete_inner(&self, id: &BlockId) -> io::Result<()> {
        let key = format::key(id)?;
        // A tombstone must be either included in the sealing fsync or synced
        // after publication. Serialize logical publication with seal/relocation.
        let _publication = self.publication.lock().await;
        let mut state = self.state.lock().await;
        self.healthy(&state)?;
        let Some(loc) = self.entries.read().unwrap().get(key.as_slice()).cloned() else {
            return Ok(());
        };
        let result = async {
            let segment = state.segments.get_mut(&loc.segment).unwrap();
            self.journal
                .append(
                    journal::Event::Delete {
                        segment: loc.segment,
                        offset: loc.offset,
                    },
                    loc.offset < segment.durable_end,
                )
                .await?;
            segment.metadata += 21;
            #[cfg(test)]
            tests::crash("delete_sync");
            let removed = charge(&key, loc.len);
            segment.live -= removed;
            segment.live_payload -= loc.len;
            segment.records.remove(&loc.offset);
            let allocated = segment.allocated().await?;
            let added = allocated.saturating_sub(segment.allocated);
            segment.allocated = allocated;
            state.allocated += added;
            state.live -= removed;
            state.written += 21;
            self.refresh_stats(&state);
            self.entries.write().unwrap().remove(key.as_slice());
            Ok(())
        }
        .await;
        if result.is_err() {
            state.poisoned = true;
        }
        result
    }

    pub fn stats(&self) -> io::Result<SmallStoreStats> {
        let mut stats = *self.cached_stats.read().unwrap();
        let mut retired = self.retired.lock().unwrap();
        retired.retain(|(fd, _)| fd.strong_count() > 0);
        stats.retired_bytes = retired.iter().map(|(_, bytes)| bytes).sum();
        stats.allocated_bytes += stats.retired_bytes;
        Ok(stats)
    }

    fn restore_policy_units(
        &self,
        units: &mut Vec<crate::eviction::RecoveredUnit>,
    ) -> io::Result<()> {
        units.reserve(self.entries.read().unwrap().len());
        for (key, loc) in self.entries.write().unwrap().iter_mut() {
            let access = AccessHandle::default();
            units.push(crate::eviction::RecoveredUnit {
                unit: Arc::new(CacheUnit::Whole(format::id(key)?)),
                bytes: charge(key, loc.len),
                access: access.clone(),
            });
            loc.access = Some(access);
        }
        Ok(())
    }

    /// One bounded segment per maintenance call; serialize mutations but do not
    /// hold the read directory lock during copying or fsync. Readers retain old
    /// inodes throughout replacement. No truncate, hole punch, or inode reuse.
    #[cfg(test)]
    pub async fn maintain(self: &Arc<Self>, checkpoint: bool) -> io::Result<bool> {
        let store = self.clone();
        self.tasks
            .run(async move {
                let result = store.maintain_inner(checkpoint, false, None).await?;
                if checkpoint {
                    store.journal.checkpoint().await?;
                }
                Ok(result)
            })
            .await
    }

    async fn maintain_inner(
        self: &Arc<Self>,
        checkpoint: bool,
        pressure: bool,
        budget: Option<Arc<crate::background::BackgroundBudget>>,
    ) -> io::Result<bool> {
        let pressure_seal = if pressure {
            let state = self.state.lock().await;
            state
                .active
                .and_then(|id| state.segments.get(&id))
                .is_some_and(|s| s.in_flight.is_empty() && meets_reclaim_threshold(s))
        } else {
            false
        };
        if checkpoint || pressure_seal {
            self.checkpoint().await?;
        } else if self.flush_due().await {
            self.flush().await?;
        }
        let _publication = self.publication.lock().await;
        // Pay before taking the mutation lock. A segment selected here cannot
        // gain new live bytes once sealed. Recheck identity and eligibility
        // after payment, since another collector may have retired it.
        let paid_candidate = if let Some(budget) = &budget {
            let candidate = {
                let state = self.state.lock().await;
                reclaim_candidate(&state, pressure).map(|id| {
                    let s = &state.segments[&id];
                    (
                        id,
                        s.live_payload.saturating_mul(2)
                            + (s.live - s.live_payload)
                            + 36 * s.records.len() as u64
                            + 49,
                    )
                })
            };
            if let Some((_, cost)) = candidate {
                budget.bytes(cost as usize).await;
            }
            Some(candidate.map(|(id, _)| id))
        } else {
            None
        };
        let mut state = self.state.lock().await;
        self.healthy(&state)?;
        let result = async {
            let candidate = match paid_candidate {
                Some(id) => id.filter(|id| reclaimable(&state, *id)),
                None => reclaim_candidate(&state, pressure),
            };
            let Some(old) = candidate else {
                return Ok(false);
            };
            let entries: Vec<_> = {
                let directory = self.entries.read().unwrap();
                state.segments[&old]
                    .records
                    .values()
                    .map(|key| {
                        (
                            key.clone(),
                            directory
                                .get(key.as_ref())
                                .expect("live segment record")
                                .clone(),
                        )
                    })
                    .collect()
            };
            let replacement = if entries.is_empty() {
                None
            } else {
                Some(self.create(&mut state).await?)
            };
            let mut moved = Vec::with_capacity(entries.len());
            if let Some(new) = replacement {
                // Reuse one source descriptor for the whole copy, then release
                // it before retired-reader accounting at unlink.
                let _source = state.segments[&old].data().await?;
                for (key, loc) in &entries {
                    let bytes = loc.clone().retain().await?.read().await?;
                    let mut replacement = self.append(&mut state, new, key, bytes, false).await?;
                    replacement.access = loc.access.clone();
                    moved.push((key.clone(), replacement));
                }
                let s = state.segments.get_mut(&new).unwrap();
                disk_io::sync(s.data().await?).await?;
                self.sync_data_directory(new).await?;
                let allocated = s.allocated().await?;
                let previous = s.allocated;
                s.allocated = allocated;
                s.sealed = true;
                s.durable_end = s.bytes;
                s.close_writers();
                state.allocated = state
                    .allocated
                    .saturating_sub(previous)
                    .saturating_add(allocated);
                self.refresh_stats(&state);
            }
            #[cfg(test)]
            tests::crash("compact_copy");
            self.journal
                .append(
                    journal::Event::Replace {
                        old,
                        new: replacement,
                    },
                    true,
                )
                .await?;
            #[cfg(test)]
            tests::crash("compact_manifest");
            let _readers = self.readers.write().await;
            {
                let mut directory = self.entries.write().unwrap();
                for (key, loc) in moved {
                    directory.insert(key, loc);
                }
            }
            if state.active == Some(old) {
                state.active = None;
            }
            let segment = state.segments.remove(&old).unwrap();
            let allocated = segment.allocated().await?;
            if let Some(budget) = &budget {
                budget.delete().await;
            }
            disk_io::remove(self.path(old, "data")).await?;
            disk_io::sync_dir(self.path(old, "data").parent().unwrap().to_owned()).await?;
            let readers = segment.file.fd.lock().await.clone();
            self.retired.lock().unwrap().push((readers, allocated));
            state.allocated = state.allocated.saturating_sub(segment.allocated);
            // Relocation keeps the published live charge unchanged. Its extra
            // destination allocation must count as physical overhead while copying.
            if replacement.is_none() {
                state.live -= segment.live;
            }
            state.written -= segment.bytes + segment.metadata;
            self.refresh_stats(&state);
            Ok(true)
        }
        .await;
        if result.is_err() {
            state.poisoned = true;
        }
        result
    }

    fn healthy(&self, state: &State) -> io::Result<()> {
        if state.poisoned {
            Err(io::Error::other(
                "small object store needs restart after failed mutation",
            ))
        } else {
            Ok(())
        }
    }
    fn path(&self, id: u64, extension: &str) -> PathBuf {
        debug_assert_eq!(extension, "data");
        self.journal.data_path(id)
    }
    async fn sync_data_directory(&self, id: u64) -> io::Result<()> {
        let shard = self.path(id, "data").parent().unwrap().to_owned();
        disk_io::sync_dir(shard.clone()).await?;
        // Shards are created lazily. Their names must be durable too before
        // publishing a seal/relocation that references a child payload file.
        disk_io::sync_dir(shard.parent().unwrap().to_owned()).await
    }
    async fn create(&self, state: &mut State) -> io::Result<u64> {
        let id = self.journal.allocate()?;
        let segment = self.create_segment(id).await?;
        state.allocated += segment.allocated;
        state.segments.insert(id, segment);
        state.written += 8;
        Ok(id)
    }

    async fn create_segment(&self, id: u64) -> io::Result<Segment> {
        disk_io::create_dir(self.path(id, "data").parent().unwrap().to_owned()).await?;
        let data = disk_io::open(self.path(id, "data"), disk_io::Open::Create).await?;
        let allocated = disk_io::allocated(data.clone()).await?;
        Ok(Segment {
            file: Arc::new(SegmentFile {
                path: self.path(id, "data"),
                fd: tokio::sync::Mutex::new(Arc::downgrade(&data)),
            }),
            data: Some(data),
            bytes: 0,
            durable_end: 0,
            metadata: 8,
            in_flight: BTreeSet::new(),
            live: 0,
            live_payload: 0,
            allocated,
            records: BTreeMap::new(),
            sealed: false,
        })
    }
    async fn append(
        &self,
        state: &mut State,
        id: u64,
        key: &Arc<[u8]>,
        value: Bytes,
        publish: bool,
    ) -> io::Result<Location> {
        let segment = state.segments.get_mut(&id).unwrap();
        let offset = segment.bytes;
        let checksum = xxhash_rust::xxh3::xxh3_64(&value);
        let len = value.len() as u64;
        disk_io::write(segment.data().await?, value, offset).await?;
        #[cfg(test)]
        tests::crash("append_payload");
        self.journal
            .append(
                journal::Event::Insert {
                    slot: self.slot,
                    segment: id,
                    offset,
                    record: journal::Record {
                        key: key.clone(),
                        len,
                        checksum,
                    },
                },
                false,
            )
            .await?;
        let frame_len = 37 + key.len() as u64;
        #[cfg(test)]
        tests::crash("append_index");
        segment.bytes += len;
        segment.metadata += frame_len;
        segment.live += charge(key, len);
        segment.live_payload += len;
        segment.records.insert(offset, key.clone());
        let allocated = segment.allocated().await?;
        state.allocated += allocated.saturating_sub(segment.allocated);
        segment.allocated = allocated;
        if publish {
            state.live += charge(key, len);
        }
        state.written += charge(key, len);
        let loc = Location {
            file: segment.file.clone(),
            segment: id,
            offset,
            len,
            checksum,
            access: None,
        };
        self.refresh_stats(state);
        Ok(loc)
    }
    fn start_pipeline(self: &Arc<Self>, state: &mut State) {
        if state.pipeline_running {
            return;
        }
        state.pipeline_running = true;
        let store = self.clone();
        drop(self.tasks.spawn(async move {
            let result = store.pipeline().await;
            let mut state = store.state.lock().await;
            if let Err(error) = result {
                state.poisoned = true;
                tracing::error!(%error, "small store segment pipeline failed");
            }
            state.pipeline_running = false;
            store.pipeline_changed.notify_waiters();
        }));
    }

    #[cfg(test)]
    async fn pause_seal(&self) {
        let pause = self.seal_pause.lock().unwrap().clone();
        if let Some(pause) = pause {
            pause.entered.notify_one();
            pause.resume.notified().await;
        }
    }

    async fn pipeline(&self) -> io::Result<()> {
        // Prepare the next spare before flushing the previous segment. No
        // filesystem operation in this phase holds the append state lock.
        let next = {
            let state = self.state.lock().await;
            self.healthy(&state)?;
            if state.standby.is_none() {
                let id = self.journal.allocate()?;
                Some(id)
            } else {
                None
            }
        };
        if let Some(id) = next {
            let segment = self.create_segment(id).await?;
            let mut state = self.state.lock().await;
            state.allocated += segment.allocated;
            state.written += 8;
            state.segments.insert(id, segment);
            state.standby = Some(id);
            self.refresh_stats(&state);
        }
        let _publication = self.publication.lock().await;
        let snapshot = loop {
            let drained = self.appends_drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            let state = self.state.lock().await;
            self.healthy(&state)?;
            let Some(id) = state.sealing else { break None };
            let s = &state.segments[&id];
            if s.in_flight.is_empty() {
                break Some((id, s.data().await?, s.records.is_empty()));
            }
            drop(state);
            drained.await;
        };
        if let Some((id, data, empty)) = snapshot {
            #[cfg(test)]
            self.pause_seal().await;
            disk_io::sync(data.clone()).await?;
            #[cfg(test)]
            tests::crash("seal_data");
            self.sync_data_directory(id).await?;
            #[cfg(test)]
            tests::crash("seal_index");
            // Empty segments need no recovery entry. Their payload files are
            // harmless orphans; GC may still unlink them in this process.
            if !empty {
                self.journal
                    .append(journal::Event::Seal { segment: id }, true)
                    .await?;
            }
            let allocated = disk_io::allocated(data).await?;
            {
                let mut state = self.state.lock().await;
                self.healthy(&state)?;
                let s = state.segments.get_mut(&id).unwrap();
                let previous = s.allocated;
                s.allocated = allocated;
                // Publication lock excludes delete/GC until the seal record is durable.
                s.sealed = true;
                s.durable_end = s.bytes;
                s.close_writers();
                state.allocated = state
                    .allocated
                    .saturating_sub(previous)
                    .saturating_add(allocated);
                self.refresh_stats(&state);
            }
            #[cfg(test)]
            tests::crash("seal_manifest");
            let mut state = self.state.lock().await;
            state.sealing = None;
        }
        Ok(())
    }

    async fn checkpoint(self: &Arc<Self>) -> io::Result<()> {
        let target = loop {
            let changed = self.pipeline_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let mut state = self.state.lock().await;
            self.healthy(&state)?;
            if state.pipeline_running {
                drop(state);
                changed.await;
                continue;
            }
            let Some(id) = state.active.take() else {
                return Ok(());
            };
            state.sealing = Some(id);
            self.start_pipeline(&mut state);
            break id;
        };
        loop {
            let changed = self.pipeline_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let state = self.state.lock().await;
            self.healthy(&state)?;
            if state.sealing != Some(target) {
                return Ok(());
            }
            drop(state);
            changed.await;
        }
    }

    fn recover<'a>(
        &mut self,
        id: u64,
        meta: &'a journal::SegmentMeta,
        keys: &mut Vec<(u64, &'a [u8])>,
    ) -> io::Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.path(id, "data"))?;
        let metadata = file.metadata()?;
        if metadata.len() < meta.durable_end || (meta.sealed && metadata.len() != meta.bytes) {
            return Err(invalid("segment length disagrees with metadata"));
        }
        drop(file);
        let file = Arc::new(SegmentFile {
            path: self.path(id, "data"),
            fd: tokio::sync::Mutex::new(Weak::new()),
        });
        let allocated = metadata.blocks() * 512;
        let records = meta
            .records
            .range(..meta.durable_end)
            .map(|(&offset, record)| (offset, record.key.clone()))
            .collect();
        let mut live = 0;
        let mut live_payload = 0;
        for (&offset, record) in meta.records.range(..meta.durable_end) {
            live += charge(&record.key, record.len);
            live_payload += record.len;
            keys.push((xxhash_rust::xxh3::xxh3_64(&record.key), &record.key));
            if self
                .entries
                .get_mut()
                .unwrap()
                .insert(
                    record.key.clone(),
                    Location {
                        file: file.clone(),
                        segment: id,
                        offset,
                        len: record.len,
                        checksum: record.checksum,
                        access: None,
                    },
                )
                .is_some()
            {
                return Err(invalid("duplicate small object identity"));
            }
        }
        let state = self.state.get_mut();
        state.allocated += allocated;
        state.live += live;
        let recovered_metadata = meta.recovered_metadata();
        state.written += meta.durable_end + recovered_metadata;
        state.segments.insert(
            id,
            Segment {
                data: None,
                file,
                bytes: meta.durable_end,
                durable_end: meta.durable_end,
                metadata: recovered_metadata,
                in_flight: BTreeSet::new(),
                live,
                live_payload,
                allocated,
                records,
                sealed: true,
            },
        );
        Ok(())
    }
    fn refresh_recovered_stats(&mut self) {
        let state = self.state.get_mut();
        *self.cached_stats.get_mut().unwrap() = SmallStoreStats {
            live_bytes: state.live,
            allocated_bytes: state.allocated,
            retired_bytes: 0,
            dead_bytes: state
                .written
                .saturating_sub(state.live + 8 * state.segments.len() as u64),
            segments: state.segments.len(),
        };
    }
    fn refresh_stats(&self, state: &State) {
        *self.cached_stats.write().unwrap() = SmallStoreStats {
            live_bytes: state.live,
            allocated_bytes: state.allocated,
            dead_bytes: state
                .written
                .saturating_sub(state.live + 8 * state.segments.len() as u64),
            segments: state.segments.len(),
            retired_bytes: 0,
        };
    }
}

fn reclaim_candidate(state: &State, pressure: bool) -> Option<u64> {
    state
        .segments
        .iter()
        .filter(|(id, _)| reclaimable(state, **id))
        .max_by_key(|(id, s)| {
            (
                // Empty files need no relocation space; under pressure prefer more
                // garbage before paying to copy a mostly-live segment.
                pressure && s.live == 0,
                if pressure {
                    s.bytes + s.metadata - s.live
                } else {
                    0
                },
                std::cmp::Reverse(**id),
            )
        })
        .map(|(&id, _)| id)
}

// Both routine and pressure reclamation require more than 50% garbage by
// record bytes (payload + index). Never rewrite a mostly-live segment merely
// because the cache is over capacity. Empty segments require no copying.
fn meets_reclaim_threshold(segment: &Segment) -> bool {
    segment.live == 0 || segment.live.saturating_mul(2) < segment.bytes + segment.metadata
}

fn reclaimable(state: &State, id: u64) -> bool {
    // A spare must stay available for rollover; sealing owns its immutable
    // snapshot until publication completes. Empty active segments can be unlinked.
    Some(id) != state.standby
        && Some(id) != state.sealing
        && state.segments.get(&id).is_some_and(|segment| {
            segment.in_flight.is_empty()
                && meets_reclaim_threshold(segment)
                && (segment.live == 0 || segment.sealed)
        })
}

fn charge(key: &[u8], len: u64) -> u64 {
    len + key.len() as u64 + 37
}
fn validate_geometry(threshold: u64, segment_size: u64, capacity: u64) -> io::Result<()> {
    if segment_size < threshold.saturating_add(format::MAX_KEY as u64 + 64)
        || segment_size > 1 << 30
        || threshold > 128 << 20
        || (capacity != 0 && segment_size.saturating_mul(4) > capacity)
    {
        return Err(invalid("small object segment/capacity geometry"));
    }
    Ok(())
}
