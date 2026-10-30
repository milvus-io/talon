//! Packed small-object cache. Published payload extents are never overwritten.
//!
//! Each segment has raw `.data` bytes and a checksummed `.index` stream. Only
//! sealed segments appear in MANIFEST; a crash may lose the bounded active
//! and sealing segments. Deletes of sealed entries are synced before becoming invisible.
//! Compaction publishes a replacement manifest before unlinking old files.
mod access;
mod disk_io;
mod format;
mod rings;
pub use rings::SmallObjectStore;
#[cfg(test)]
mod tests;

use bytes::Bytes;
use format::{invalid, MAGIC};
use std::collections::{BTreeMap, HashMap};
use std::fs::OpenOptions;
use std::io::{self, Read};
use std::os::fd::OwnedFd;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};
use talon_core::{BlockForm, BlockHandle, BlockId, BlockMeta};

#[derive(Clone)]
struct Location {
    fd: Arc<OwnedFd>,
    segment: u64,
    offset: u64,
    len: u64,
    checksum: u64,
}
impl Location {
    async fn read(&self) -> io::Result<Bytes> {
        let bytes = disk_io::read(self.fd.clone(), self.offset, self.len as usize).await?;
        if xxhash_rust::xxh3::xxh3_64(&bytes) != self.checksum {
            return Err(invalid("small object payload checksum"));
        }
        Ok(bytes)
    }
}

struct Segment {
    data: Arc<OwnedFd>,
    index: Arc<OwnedFd>,
    fd: Arc<OwnedFd>,
    // Reservation ends, including writes whose completions are still pending.
    bytes: u64,
    metadata: u64,
    in_flight: usize,
    live: u64,
    live_payload: u64,
    allocated: u64,
    records: BTreeMap<u64, Arc<[u8]>>,
    sealed: bool,
}
impl Segment {
    async fn allocated(&self) -> io::Result<u64> {
        Ok(disk_io::allocated(self.data.clone()).await?
            + disk_io::allocated(self.index.clone()).await?)
    }
}
struct State {
    segments: BTreeMap<u64, Segment>,
    active: Option<u64>,
    standby: Option<u64>,
    sealing: Option<u64>,
    pipeline_running: bool,
    next: u64,
    allocated: u64,
    live: u64,
    written: u64,
    poisoned: bool,
    last_seal: Instant,
}

/// One independently appended and sealed stream within a namespace/credential scope.
/// The compact encoded identity is stored once here, without separate String
/// allocations. Worker BlockIndex/LRU still own policy metadata in v1.
struct SegmentStore {
    #[cfg(test)]
    seal_pause: Mutex<Option<Arc<tests::SealPause>>>,
    #[cfg(test)]
    append_pause: Mutex<Option<Arc<tests::AppendPause>>>,
    root: PathBuf,
    threshold: u64,
    segment_size: u64,
    state: tokio::sync::Mutex<State>,
    // Serializes durable publication, deletion and compaction, never append.
    publication: tokio::sync::Mutex<()>,
    pipeline_changed: tokio::sync::Notify,
    appends_drained: tokio::sync::Notify,
    append_slots: tokio::sync::Semaphore,
    entries: RwLock<HashMap<Arc<[u8]>, Location>>,
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

impl SegmentStore {
    pub fn open(
        root: PathBuf,
        threshold: u64,
        segment_size: u64,
        capacity: u64,
        root_lock: Option<Arc<crate::page_access_store::CacheRootLock>>,
    ) -> io::Result<Self> {
        if segment_size < threshold.saturating_add(format::MAX_KEY as u64 + 64)
            || segment_size > 1 << 30
            || threshold > 128 << 20
            || (capacity != 0 && segment_size.saturating_mul(4) > capacity)
        {
            return Err(invalid("small object segment/capacity geometry"));
        }
        std::fs::create_dir_all(&root)?;
        let manifest = root.join("MANIFEST");
        let ids = match std::fs::read(&manifest) {
            Ok(bytes) => decode_manifest(&bytes)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e),
        };
        let mut store = Self::empty(root, threshold, segment_size, root_lock);
        // Orphaned active/replacement files are never recovered. Determine IDs
        // before removing them so IDs cannot be reused during this startup.
        for item in std::fs::read_dir(&store.root)? {
            let path = item?.path();
            if !matches!(
                path.extension().and_then(|s| s.to_str()),
                Some("data" | "index")
            ) {
                continue;
            }
            let Some(id) = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
            else {
                continue;
            };
            store.state.get_mut().next = store.state.get_mut().next.max(
                id.checked_add(1)
                    .ok_or_else(|| invalid("segment id overflow"))?,
            );
            if !ids.contains(&id) {
                std::fs::remove_file(path)?;
            }
        }
        for id in ids {
            // Fail closed on malformed metadata, including a torn delete. Do
            // not replay a valid prefix that might resurrect a deleted object.
            store.recover(id)?;
        }
        let stats = SmallStoreStats {
            live_bytes: store.state.get_mut().live,
            allocated_bytes: store.state.get_mut().allocated,
            dead_bytes: store.state.get_mut().written.saturating_sub(
                store.state.get_mut().live + 8 * store.state.get_mut().segments.len() as u64,
            ),
            segments: store.state.get_mut().segments.len(),
            retired_bytes: 0,
        };
        *store.cached_stats.write().unwrap() = stats;
        Ok(store)
    }

    /// Construct a new ring stream after its directory has been created and
    /// made durable asynchronously. Existing streams always use open/recovery.
    fn empty(
        root: PathBuf,
        threshold: u64,
        segment_size: u64,
        root_lock: Option<Arc<crate::page_access_store::CacheRootLock>>,
    ) -> Self {
        Self {
            #[cfg(test)]
            seal_pause: Mutex::new(None),
            #[cfg(test)]
            append_pause: Mutex::new(None),
            root,
            threshold,
            segment_size,
            state: tokio::sync::Mutex::new(State {
                segments: BTreeMap::new(),
                active: None,
                standby: None,
                sealing: None,
                pipeline_running: false,
                next: 1,
                allocated: 0,
                live: 0,
                written: 0,
                poisoned: false,
                last_seal: Instant::now(),
            }),
            publication: tokio::sync::Mutex::new(()),
            pipeline_changed: tokio::sync::Notify::new(),
            appends_drained: tokio::sync::Notify::new(),
            append_slots: tokio::sync::Semaphore::new(32),
            entries: RwLock::new(HashMap::new()),
            tasks: Arc::new(crate::page_gc::Mutations::new(true)),
            cached_stats: RwLock::new(SmallStoreStats::default()),
            retired: Mutex::new(Vec::new()),
            _root_lock: root_lock,
        }
    }

    /// Only a complete short FIRST block proves this is a small object. A
    /// short last block of a large object must remain in the large-object store.
    pub fn accepts(&self, id: &BlockId, len: u64) -> bool {
        self.threshold > 0
            && id.offset == 0
            && len > 0
            && len < self.threshold
            && len < u64::from(id.block_size)
    }

    pub fn contains(&self, id: &BlockId) -> bool {
        format::key(id)
            .ok()
            .is_some_and(|k| self.entries.read().unwrap().contains_key(k.as_slice()))
    }

    pub fn get(&self, id: &BlockId) -> io::Result<Option<BlockHandle>> {
        let key = format::key(id)?;
        // Clone the fd under the same lock that removes the locator. An unlink
        // can never occur between discovering a location and retaining its fd.
        Ok(self
            .entries
            .read()
            .unwrap()
            .get(key.as_slice())
            .map(|loc| BlockHandle::from_shared(loc.fd.clone(), loc.offset, loc.len)))
    }

    pub async fn read(&self, id: &BlockId) -> io::Result<Option<Bytes>> {
        let loc = self
            .entries
            .read()
            .unwrap()
            .get(format::key(id)?.as_slice())
            .cloned();
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
            .run(async move { store.put_inner(&id, value).await })
            .await
    }

    /// The public store holds the identity mutation guard through completion.
    /// Different identities reserve briefly, then submit independent I/O.
    async fn put_inner(self: &Arc<Self>, id: &BlockId, value: Bytes) -> io::Result<u64> {
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
        let (loc, index, metadata, frame) = loop {
            let changed = self.pipeline_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let mut state = self.state.lock().await;
            self.healthy(&state)?;
            if let Some(old) = self.entries.read().unwrap().get(key.as_ref()) {
                if old.len != len || old.checksum != checksum {
                    return Err(invalid(
                        "different bytes for immutable small object identity",
                    ));
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
                fd: s.fd.clone(),
                segment,
                offset: s.bytes,
                len,
                checksum,
            };
            let frame = append_frame(loc.offset, len, checksum, &key)?;
            let metadata = s.metadata;
            let index = s.index.clone();
            s.bytes += len;
            s.metadata += frame.len() as u64;
            s.in_flight += 1;
            state.written += charge;
            break (loc, index, metadata, frame);
        };
        // No append state lock is held while any of these operations is in
        // flight. Both writes are submitted; always await both, even on error,
        // so descriptors/buffers and the cache-root lease outlive kernel I/O.
        let result = async {
            #[cfg(test)]
            self.pause_append().await;
            let (payload, record) = tokio::join!(
                async {
                    disk_io::write(loc.fd.clone(), value, loc.offset).await?;
                    #[cfg(test)]
                    tests::crash("append_payload");
                    Ok::<_, io::Error>(())
                },
                async {
                    disk_io::write(index.clone(), frame, metadata).await?;
                    #[cfg(test)]
                    tests::crash("append_index");
                    Ok::<_, io::Error>(())
                }
            );
            payload?;
            record?;
            let (data_allocated, index_allocated) = tokio::join!(
                disk_io::allocated(loc.fd.clone()),
                disk_io::allocated(index)
            );
            Ok::<_, io::Error>(data_allocated? + index_allocated?)
        }
        .await;
        let mut state = self.state.lock().await;
        let segment = state
            .segments
            .get_mut(&loc.segment)
            .expect("in-flight segment is retained");
        segment.in_flight -= 1;
        if segment.in_flight == 0 {
            self.appends_drained.notify_waiters();
        }
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
        self.entries.write().unwrap().insert(key, loc);
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
        // after publication. Never append to an index concurrently with seal.
        let _publication = self.publication.lock().await;
        let mut state = self.state.lock().await;
        self.healthy(&state)?;
        let Some(loc) = self.entries.read().unwrap().get(key.as_slice()).cloned() else {
            return Ok(());
        };
        let result = async {
            let segment = state.segments.get_mut(&loc.segment).unwrap();
            let mut record = vec![1];
            record.extend_from_slice(&loc.offset.to_le_bytes());
            let mut frame = Vec::new();
            format::frame(&mut frame, &record)?;
            disk_io::write(segment.index.clone(), frame.into(), segment.metadata).await?;
            segment.metadata += 12 + record.len() as u64;
            if segment.sealed {
                disk_io::sync(segment.index.clone()).await?;
            }
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

    pub(crate) fn restore_index(&self, index: &crate::BlockIndex) -> io::Result<()> {
        for (key, loc) in self.entries.read().unwrap().iter() {
            index.commit_allocated(
                BlockMeta {
                    id: format::id(key)?,
                    form: BlockForm::Whole,
                    len: loc.len,
                },
                [(None, charge(key, loc.len))],
            );
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
            .run(async move { store.maintain_inner(checkpoint, false, None).await })
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
                .is_some_and(|s| s.in_flight == 0 && meets_reclaim_threshold(s))
        } else {
            false
        };
        if checkpoint
            || pressure_seal
            || self.state.lock().await.last_seal.elapsed() >= Duration::from_secs(30)
        {
            self.checkpoint().await?;
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
                        if s.live == 0 {
                            0
                        } else {
                            s.live_payload.saturating_mul(2) + (s.live - s.live_payload) + 8
                        },
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
                for (key, loc) in &entries {
                    let bytes = loc.read().await?;
                    moved.push((
                        key.clone(),
                        self.append(&mut state, new, key, bytes, false).await?,
                    ));
                }
                let s = state.segments.get_mut(&new).unwrap();
                disk_io::sync(s.data.clone()).await?;
                disk_io::sync(s.index.clone()).await?;
                let allocated = s.allocated().await?;
                let previous = s.allocated;
                s.allocated = allocated;
                s.sealed = true;
                state.allocated = state
                    .allocated
                    .saturating_sub(previous)
                    .saturating_add(allocated);
                self.refresh_stats(&state);
            }
            #[cfg(test)]
            tests::crash("compact_copy");
            self.manifest(&state, Some(old)).await?;
            #[cfg(test)]
            tests::crash("compact_manifest");
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
                budget.delete().await;
            }
            disk_io::remove(self.path(old, "data")).await?;
            disk_io::remove(self.path(old, "index")).await?;
            disk_io::sync_dir(self.root.clone()).await?;
            self.retired
                .lock()
                .unwrap()
                .push((Arc::downgrade(&segment.fd), allocated));
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
        self.root.join(format!("{id}.{extension}"))
    }
    async fn create(&self, state: &mut State) -> io::Result<u64> {
        let id = state.next;
        state.next = state
            .next
            .checked_add(1)
            .ok_or_else(|| invalid("segment id overflow"))?;
        let segment = self.create_segment(id).await?;
        state.allocated += segment.allocated;
        state.segments.insert(id, segment);
        state.written += 8;
        Ok(id)
    }

    async fn create_segment(&self, id: u64) -> io::Result<Segment> {
        let data = disk_io::open(self.path(id, "data"), disk_io::Open::Create).await?;
        let index = disk_io::open(self.path(id, "index"), disk_io::Open::Create).await?;
        disk_io::write(index.clone(), Bytes::from_static(MAGIC), 0).await?;
        let allocated = disk_io::allocated(index.clone()).await?;
        Ok(Segment {
            fd: data.clone(),
            data,
            index,
            bytes: 0,
            metadata: 8,
            in_flight: 0,
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
        disk_io::write(segment.data.clone(), value, offset).await?;
        #[cfg(test)]
        tests::crash("append_payload");
        let frame = append_frame(offset, len, checksum, key)?;
        let frame_len = frame.len() as u64;
        disk_io::write(segment.index.clone(), frame, segment.metadata).await?;
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
            fd: segment.fd.clone(),
            segment: id,
            offset,
            len,
            checksum,
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
            let mut state = self.state.lock().await;
            self.healthy(&state)?;
            if state.standby.is_none() {
                let id = state.next;
                state.next = id
                    .checked_add(1)
                    .ok_or_else(|| invalid("segment id overflow"))?;
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
            if s.in_flight == 0 {
                break Some((id, s.data.clone(), s.index.clone()));
            }
            drop(state);
            drained.await;
        };
        if let Some((id, data, index)) = snapshot {
            #[cfg(test)]
            self.pause_seal().await;
            disk_io::sync(data.clone()).await?;
            #[cfg(test)]
            tests::crash("seal_data");
            disk_io::sync(index.clone()).await?;
            #[cfg(test)]
            tests::crash("seal_index");
            let allocated = disk_io::allocated(data).await? + disk_io::allocated(index).await?;
            let body = {
                let mut state = self.state.lock().await;
                self.healthy(&state)?;
                let s = state.segments.get_mut(&id).unwrap();
                let previous = s.allocated;
                s.allocated = allocated;
                // Publication lock excludes delete/GC until the manifest is durable.
                s.sealed = true;
                state.allocated = state
                    .allocated
                    .saturating_sub(previous)
                    .saturating_add(allocated);
                self.refresh_stats(&state);
                Self::manifest_body(&state, None)
            };
            self.write_manifest(body).await?;
            #[cfg(test)]
            tests::crash("seal_manifest");
            let mut state = self.state.lock().await;
            state.sealing = None;
            state.last_seal = Instant::now();
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
                state.last_seal = Instant::now();
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

    async fn manifest(&self, state: &State, exclude: Option<u64>) -> io::Result<()> {
        self.write_manifest(Self::manifest_body(state, exclude))
            .await
    }

    fn manifest_body(state: &State, exclude: Option<u64>) -> Vec<u8> {
        let mut body = MAGIC.to_vec();
        for (&id, segment) in &state.segments {
            if segment.sealed && Some(id) != exclude {
                body.extend_from_slice(&id.to_le_bytes());
            }
        }
        body.extend_from_slice(&xxhash_rust::xxh3::xxh3_64(&body).to_le_bytes());
        body
    }

    async fn write_manifest(&self, body: Vec<u8>) -> io::Result<()> {
        let tmp = self.root.join("MANIFEST.tmp");
        let file = disk_io::open(tmp.clone(), disk_io::Open::Replace).await?;
        disk_io::write(file.clone(), body.into(), 0).await?;
        disk_io::sync(file).await?;
        disk_io::rename(tmp, self.root.join("MANIFEST")).await?;
        disk_io::sync_dir(self.root.clone()).await
    }
    fn recover(&mut self, id: u64) -> io::Result<()> {
        let data = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.path(id, "data"))?;
        let index = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.path(id, "index"))?;
        let bytes = data.metadata()?.len();
        let metadata = index.metadata()?.len();
        // A record consumes >= 37 index bytes; each delete adds 21. Include a
        // bounded allowance for old geometry when the threshold is changed.
        if metadata > 2 * (1u64 << 30) {
            return Err(invalid("small object index too large"));
        }
        let mut encoded = Vec::new();
        (&index).read_to_end(&mut encoded)?;
        if !encoded.starts_with(MAGIC) {
            return Err(invalid("small object index version"));
        }
        let fd: Arc<OwnedFd> = Arc::new(data.try_clone()?.into());
        let mut records = BTreeMap::new();
        let mut next_offset = 0;
        for mut frame in format::frames(&encoded[8..])? {
            let mut kind = [0];
            frame.read_exact(&mut kind)?;
            let offset = format::u64(&mut frame)?;
            match kind[0] {
                0 => {
                    let len = format::u64(&mut frame)?;
                    let checksum = format::u64(&mut frame)?;
                    let identity = format::id(frame)?;
                    if offset != next_offset
                        || len == 0
                        || len > 128 << 20
                        || offset.checked_add(len).map_or(true, |end| end > bytes)
                        || identity.offset != 0
                        || len >= u64::from(identity.block_size)
                    {
                        return Err(invalid("small object extent"));
                    }
                    next_offset = offset + len;
                    if records
                        .insert(
                            offset,
                            (
                                frame.to_vec(),
                                Location {
                                    fd: fd.clone(),
                                    segment: id,
                                    offset,
                                    len,
                                    checksum,
                                },
                            ),
                        )
                        .is_some()
                    {
                        return Err(invalid("duplicate small object offset"));
                    }
                }
                1 => {
                    if !frame.is_empty() || records.remove(&offset).is_none() {
                        return Err(invalid("small object tombstone"));
                    }
                }
                _ => return Err(invalid("small object record type")),
            }
        }
        if next_offset != bytes {
            return Err(invalid("small object trailing payload"));
        }
        let mut live = 0;
        let mut live_payload = 0;
        let mut segment_records = BTreeMap::new();
        for (offset, (key, loc)) in records {
            live += charge(&key, loc.len);
            live_payload += loc.len;
            let key: Arc<[u8]> = key.into();
            segment_records.insert(offset, key.clone());
            if self.entries.get_mut().unwrap().insert(key, loc).is_some() {
                return Err(invalid("duplicate small object identity"));
            }
        }
        let allocated = (data.metadata()?.blocks() + index.metadata()?.blocks()) * 512;
        let state = self.state.get_mut();
        state.allocated += allocated;
        state.live += live;
        state.written += bytes + metadata;
        state.segments.insert(
            id,
            Segment {
                data: Arc::new(data.into()),
                index: Arc::new(index.into()),
                fd,
                bytes,
                metadata,
                in_flight: 0,
                live,
                live_payload,
                allocated,
                records: segment_records,
                sealed: true,
            },
        );
        Ok(())
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
            segment.in_flight == 0
                && meets_reclaim_threshold(segment)
                && (segment.live == 0 || segment.sealed)
        })
}

fn append_frame(offset: u64, len: u64, checksum: u64, key: &[u8]) -> io::Result<Bytes> {
    let mut record = vec![0];
    record.extend_from_slice(&offset.to_le_bytes());
    record.extend_from_slice(&len.to_le_bytes());
    record.extend_from_slice(&checksum.to_le_bytes());
    record.extend_from_slice(key);
    let mut frame = Vec::new();
    format::frame(&mut frame, &record)?;
    Ok(frame.into())
}

fn charge(key: &[u8], len: u64) -> u64 {
    len + key.len() as u64 + 37
}
fn decode_manifest(bytes: &[u8]) -> io::Result<Vec<u64>> {
    if bytes.len() < 16 || bytes.len() % 8 != 0 || !bytes.starts_with(MAGIC) {
        return Err(invalid("small object manifest version/length"));
    }
    let split = bytes.len() - 8;
    let checksum = u64::from_le_bytes(bytes[split..].try_into().unwrap());
    if checksum != xxhash_rust::xxh3::xxh3_64(&bytes[..split]) {
        return Err(invalid("small object manifest checksum"));
    }
    let ids: Vec<_> = bytes[8..split]
        .chunks_exact(8)
        .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
        .collect();
    if ids.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid("small object manifest order"));
    }
    Ok(ids)
}
