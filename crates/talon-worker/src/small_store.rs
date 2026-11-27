//! Packed small-object cache. Published payload extents are never overwritten.
//!
//! Data streams append independently; one bounded native writer owns the
//! namespace metadata log and snapshots. Durable prefixes survive before sealing.
//! Relocation commits are durable before old payload files are unlinked.
mod access;
mod catalog;
pub(crate) mod key;
pub(crate) mod lookup;
pub(crate) mod policy;
mod recency;
pub(crate) use access::DirtyAccess;
pub(crate) mod directory;
mod disk_io;
mod flush;
mod gc;
pub(crate) use gc::{OwnershipCursor, OwnershipLimit, SmallGcCandidate, SmallStaleCursor};
mod format;
mod journal;
mod key_codec;
mod keys;
mod records;
mod rings;
mod tickets;
pub(crate) use recency::IdleRecencyCursor;
pub use rings::SmallObjectStore;
#[cfg(test)]
pub(crate) mod tests;

use crate::eviction::{AccessHandle, CacheUnit, SharedUnit};
use crate::small_store::key::{SmallIdentity, SmallKey};
use crate::Lru;
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

fn read_for_unlink<T>(
    lock: &RwLock<T>,
    nonblocking: bool,
) -> Option<std::sync::RwLockReadGuard<'_, T>> {
    if nonblocking {
        crate::runtime::uncontended(lock.try_read())
    } else {
        Some(lock.read().unwrap())
    }
}

const CHECKSUM_BYTES: u64 = size_of::<u64>() as u64;

fn checksum_len(legacy_checksum: Option<u64>) -> u64 {
    if legacy_checksum.is_none() {
        CHECKSUM_BYTES
    } else {
        0
    }
}

/// Borrowed identity accepted by packed storage. Ordinary callers keep using BlockId.
pub trait SmallStoreKey: Sync {
    fn block(&self) -> &BlockId;
    fn exact_range(&self) -> bool;
    fn is_page(&self) -> bool {
        false
    }
    fn cache_unit(&self) -> CacheUnit {
        if self.exact_range() {
            CacheUnit::ExactRange(self.block().clone())
        } else {
            CacheUnit::Whole(self.block().clone())
        }
    }
}

impl SmallStoreKey for BlockId {
    fn block(&self) -> &BlockId {
        self
    }
    fn exact_range(&self) -> bool {
        false
    }
}

impl SmallStoreKey for CacheUnit {
    fn cache_unit(&self) -> CacheUnit {
        self.clone()
    }
    fn block(&self) -> &BlockId {
        self.block()
    }
    fn exact_range(&self) -> bool {
        matches!(self, Self::ExactRange(_))
    }
    fn is_page(&self) -> bool {
        matches!(self, Self::Page(..))
    }
}

#[derive(Clone)]
pub(crate) struct Location {
    file: Arc<SegmentFile>,
    segment: u64,
    offset: u64,
    len: u64,
    // Legacy records keep their checksum in metadata.
    checksum: Option<u64>,
    // Shared with the eviction policy; relocation preserves this exact token.
    access: Option<AccessHandle>,
}
/// A resolved read owns an fd before the directory can publish an unlink.
pub(crate) struct ReadLocation {
    location: Location,
    fd: Arc<OwnedFd>,
}

fn validate_range(payload_len: u64, offset: u64, len: u64) -> io::Result<()> {
    if offset
        .checked_add(len)
        .map_or(true, |end| end > payload_len)
    {
        return Err(invalid("small object range out of bounds"));
    }
    Ok(())
}

impl ReadLocation {
    pub(crate) fn len(&self) -> u64 {
        self.location.len
    }

    fn touch(&self) {
        self.location.touch();
    }

    pub(crate) fn range(&self, offset: u64, len: u64) -> io::Result<BlockHandle> {
        validate_range(self.len(), offset, len)?;
        Ok(BlockHandle::from_shared(
            self.fd.clone(),
            self.location.offset + offset,
            len,
        ))
    }

    // Deliberately does not touch: relocation also reads through this snapshot.
    pub(crate) async fn read(&self) -> io::Result<Bytes> {
        let mut bytes = self.read_stored().await?;
        bytes.truncate(self.len() as usize);
        Ok(bytes)
    }

    async fn read_stored(&self) -> io::Result<Bytes> {
        let bytes = disk_io::read(
            self.fd.clone(),
            self.location.offset,
            self.location.stored_len() as usize,
        )
        .await?;
        let len = self.len() as usize;
        let checksum = self
            .location
            .checksum
            .unwrap_or_else(|| u64::from_le_bytes(bytes[len..].try_into().unwrap()));
        if xxhash_rust::xxh3::xxh3_64(&bytes[..len]) != checksum {
            return Err(invalid("small object payload checksum"));
        }
        Ok(bytes)
    }

    async fn read_checksum_trailer(&self) -> io::Result<u64> {
        let bytes = disk_io::read(
            self.fd.clone(),
            self.location.offset + self.location.len,
            CHECKSUM_BYTES as usize,
        )
        .await?;
        Ok(u64::from_le_bytes(bytes[..].try_into().unwrap()))
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
    #[cfg(test)]
    pub(crate) fn segment(&self) -> u64 {
        self.segment
    }

    pub(crate) fn len(&self) -> u64 {
        self.len
    }

    fn stored_len(&self) -> u64 {
        self.len + checksum_len(self.checksum)
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
    live_data_bytes: u64,
    allocated: u64,
    records: usize,
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
    compaction_deletes: Option<CompactionDeletes>,
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

struct CompactionDeletes {
    source: u64,
    offsets: Vec<u64>,
}

struct CopiedRecord {
    new_offset: u64,
    charge: u64,
    stored_len: u64,
}

struct StagedSegment {
    id: u64,
    segment: Segment,
    records: HashMap<u64, CopiedRecord>,
}

#[derive(Default)]
struct PhysicalStats {
    published: SmallStoreStats,
    staging_allocated: u64,
    // Already included in published or staging allocation.
    pending_unlink: u64,
    retired: Vec<(Weak<OwnedFd>, u64)>,
}

impl PhysicalStats {
    fn snapshot(&mut self) -> SmallStoreStats {
        let mut stats = self.published;
        self.retired.retain(|(fd, _)| fd.strong_count() > 0);
        stats.retired_bytes = self.retired.iter().map(|(_, bytes)| bytes).sum();
        stats.allocated_bytes += self.staging_allocated + stats.retired_bytes;
        stats
    }
}

/// One independently appended and sealed stream within a namespace/credential scope.
/// The compact identity is shared with the journal and eviction policy.
/// Locators own access tokens shared with the eviction policy;
/// small objects are never registered in the generic BlockIndex.
struct SegmentStore {
    #[cfg(test)]
    seal_pause: Mutex<Option<Arc<tests::SealPause>>>,
    #[cfg(test)]
    append_pause: Mutex<Option<Arc<tests::AppendPause>>>,
    #[cfg(test)]
    flush_pause: Mutex<Option<Arc<tests::AppendPause>>>,
    #[cfg(test)]
    reconcile_pause: Mutex<Option<Arc<tests::AppendPause>>>,
    #[cfg(test)]
    relocation_pause: Mutex<Option<Arc<tests::AppendPause>>>,
    #[cfg(test)]
    unlink_pause: Mutex<Option<Arc<tests::AppendPause>>>,
    #[cfg(test)]
    fail_delete_after_journal: std::sync::atomic::AtomicBool,
    slot: usize,
    journal: Arc<journal::Journal>,
    threshold: u64,
    segment_size: u64,
    state: tokio::sync::Mutex<State>,
    // Serializes durable publication, deletion and compaction, never append.
    publication: tokio::sync::Mutex<()>,
    // Owns the source and staged file from selection through physical removal.
    compaction: tokio::sync::Mutex<()>,
    pipeline_changed: tokio::sync::Notify,
    appends_drained: tokio::sync::Notify,
    append_slots: tokio::sync::Semaphore,
    entries: Arc<RwLock<directory::Directory>>,
    // Async opens run outside the directory lock, but must finish before GC
    // unlinks their source paths.
    readers: Arc<tokio::sync::RwLock<()>>,
    tasks: Arc<crate::page_gc::Mutations>,
    cached_stats: Mutex<PhysicalStats>,
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
fn accepts_object(threshold: u64, key: &(impl SmallStoreKey + ?Sized), len: u64) -> bool {
    let id = key.block();
    if key.is_page() {
        return false;
    }
    if key.exact_range() {
        len > 0
            && len <= talon_core::MAX_EXACT_READ_LEN
            && len == u64::from(id.block_size)
            && id.offset.checked_add(len).is_some()
    } else {
        threshold > 0
            && id.offset == 0
            && len > 0
            && len < threshold
            && len < u64::from(id.block_size)
    }
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
            false,
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
            #[cfg(test)]
            reconcile_pause: Mutex::new(None),
            #[cfg(test)]
            relocation_pause: Mutex::new(None),
            #[cfg(test)]
            unlink_pause: Mutex::new(None),
            #[cfg(test)]
            fail_delete_after_journal: std::sync::atomic::AtomicBool::new(false),
            slot,
            journal: journal.clone(),
            threshold,
            segment_size,
            state: tokio::sync::Mutex::new(State {
                segments: BTreeMap::new(),
                compaction_deletes: None,
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
            compaction: tokio::sync::Mutex::new(()),
            pipeline_changed: tokio::sync::Notify::new(),
            appends_drained: tokio::sync::Notify::new(),
            append_slots: tokio::sync::Semaphore::new(32),
            entries: Arc::new(RwLock::new(directory::Directory::new(
                journal.model.clone(),
                slot,
            ))),
            readers: journal.readers.clone(),
            tasks: Arc::new(crate::page_gc::Mutations::new(true)),
            cached_stats: Mutex::default(),
            _root_lock: root_lock,
        }
    }

    pub fn accepts(&self, id: &(impl SmallStoreKey + ?Sized), len: u64) -> bool {
        accepts_object(self.threshold, id, len)
    }

    pub fn contains(&self, id: &(impl SmallStoreKey + ?Sized)) -> bool {
        format::validate_key(&id.view()).is_ok()
            && self.entries.read().unwrap().contains_key(&id.view())
    }

    // The outer option distinguishes lock contention from an absent entry.
    fn unlink_location(
        &self,
        id: &dyn SmallIdentity,
        nonblocking: bool,
    ) -> Option<Option<Location>> {
        read_for_unlink(&self.entries, nonblocking)?.get_for_unlink(id, nonblocking)
    }

    #[cfg(test)]
    async fn lookup(&self, id: &(impl SmallStoreKey + ?Sized)) -> io::Result<Option<ReadLocation>> {
        let _readers = self.readers.read().await;
        format::validate_key(&id.view())?;
        let loc = self.entries.read().unwrap().get(&id.view());
        match loc {
            Some(loc) => Ok(Some(loc.retain().await?)),
            None => Ok(None),
        }
    }

    #[cfg(test)]
    pub async fn get(&self, id: &(impl SmallStoreKey + ?Sized)) -> io::Result<Option<BlockHandle>> {
        self.lookup(id)
            .await?
            .map(|loc| {
                loc.touch();
                loc.range(0, loc.len())
            })
            .transpose()
    }

    #[cfg(test)]
    pub async fn read(&self, id: &(impl SmallStoreKey + ?Sized)) -> io::Result<Option<Bytes>> {
        let loc = self.lookup(id).await?;
        match loc {
            Some(loc) => Ok(Some(loc.read().await?)),
            None => Ok(None),
        }
    }

    #[cfg(test)]
    pub async fn put(
        self: &Arc<Self>,
        id: &(impl SmallStoreKey + ?Sized),
        value: Bytes,
    ) -> io::Result<u64> {
        let store = self.clone();
        let id = id.cache_unit();
        self.tasks
            .run(async move { store.put_inner(&id, value, None).await })
            .await
    }

    /// The public store holds the identity mutation guard through completion.
    /// Different identities reserve briefly, then submit independent I/O.
    async fn put_inner(
        self: &Arc<Self>,
        id: &(impl SmallStoreKey + ?Sized),
        value: Bytes,
        policy: Option<Arc<Lru>>,
    ) -> io::Result<u64> {
        if !accepts_object(u64::MAX, id, value.len() as u64) || value.len() > 128 << 20 {
            return Err(invalid("invalid small object extent"));
        }
        format::validate_key(&id.view())?;
        let view = id.view();
        let block = policy.as_ref().map_or_else(
            || crate::block_key::BlockKey::new(&view.block),
            |p| p.shared_identity(&view.block),
        );
        let unit = SharedUnit {
            block,
            kind: view.kind,
        };
        let key = SmallKey(unit.clone());
        let len = value.len() as u64;
        let stored_len = len + CHECKSUM_BYTES;
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
        let (loc, data) = loop {
            let changed = self.pipeline_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let mut state = self.state.lock().await;
            self.healthy(&state)?;
            let existing = self.entries.read().unwrap().get(&key);
            if let Some(old) = existing {
                let conflict = || invalid("different bytes for immutable small object identity");
                if old.len != len {
                    return Err(conflict());
                }
                let weight = old.stored_len();
                let previous = if let Some(previous) = old.checksum {
                    previous
                } else {
                    let read = old.retain().await?;
                    drop(state);
                    read.read_checksum_trailer().await?
                };
                if previous != checksum {
                    return Err(conflict());
                }
                if let Some(policy) = &policy {
                    policy.insert_with_policy(unit.clone(), weight, true);
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
                checksum: None,
                access: None,
            };
            let data = s.data().await?;
            s.bytes += stored_len;
            s.metadata += charge - stored_len;
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
                    disk_io::write_record(data.clone(), value, checksum, loc.offset).await?;
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
                                    checksum: None,
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
        segment.live_data_bytes += stored_len;
        segment.records += 1;
        state.allocated += added;
        state.live += charge;
        // Publish policy and locator together. The runtime's block gate keeps
        // eviction from retiring this admission before its mutation completes.
        if let Some(policy) = policy {
            policy.publish_small(unit, stored_len, &self.entries, loc);
        } else {
            self.entries.write().unwrap().insert(key, loc);
        }
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
    async fn pause_compaction(hook: &Mutex<Option<Arc<tests::AppendPause>>>) {
        let pause = hook.lock().unwrap().take();
        if let Some(pause) = pause {
            pause.entered.notify_one();
            pause.resume.notified().await;
        }
    }

    #[cfg(test)]
    pub async fn delete(self: &Arc<Self>, id: &(impl SmallStoreKey + ?Sized)) -> io::Result<()> {
        let store = self.clone();
        let id = id.cache_unit();
        self.tasks
            .run(async move { store.delete_inner(&id).await })
            .await
    }

    async fn delete_inner(&self, id: &(impl SmallStoreKey + ?Sized)) -> io::Result<()> {
        self.delete_inner_if(id, None, |_| Some(()))
            .await
            .map(|_| ())
    }

    async fn delete_inner_if<G>(
        &self,
        id: &(impl SmallStoreKey + ?Sized),
        deadline: Option<Instant>,
        authorize: impl FnOnce(&Location) -> Option<G>,
    ) -> io::Result<Option<G>> {
        format::validate_key(&id.view())?;
        // A tombstone must be either included in the sealing fsync or synced
        // after publication. Serialize logical publication with seal/relocation.
        use crate::runtime::before_deadline;
        let Some(_publication) = before_deadline(deadline, self.publication.lock()).await else {
            return Ok(None);
        };
        let Some(mut state) = before_deadline(deadline, self.state.lock()).await else {
            return Ok(None);
        };
        self.healthy(&state)?;
        let Some(Some(loc)) = self.unlink_location(&id.view(), deadline.is_some()) else {
            return Ok(None);
        };
        let Some(guard) = authorize(&loc) else {
            return Ok(None);
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
            {
                tests::crash("delete_sync");
                if self
                    .fail_delete_after_journal
                    .swap(false, std::sync::atomic::Ordering::Relaxed)
                {
                    return Err(io::Error::other("injected failure after journal Delete"));
                }
            }
            let removed = charge(&id.view(), loc.len);
            segment.live -= removed;
            segment.live_data_bytes -= loc.stored_len();
            segment.records -= 1;
            let allocated = segment.allocated().await?;
            let added = allocated.saturating_sub(segment.allocated);
            segment.allocated = allocated;
            state.allocated += added;
            state.live -= removed;
            state.written += 21;
            self.refresh_stats(&state);
            self.entries.write().unwrap().remove(&id.view());
            if let Some(deletes) = state
                .compaction_deletes
                .as_mut()
                .filter(|deletes| deletes.source == loc.segment)
            {
                deletes.offsets.push(loc.offset);
            }
            Ok(())
        }
        .await;
        if result.is_err() {
            state.poisoned = true;
        }
        drop(state);
        drop(_publication);
        result.map(|()| Some(guard))
    }

    pub fn stats(&self) -> io::Result<SmallStoreStats> {
        Ok(self.cached_stats.lock().unwrap().snapshot())
    }

    fn restore_policy_units(
        &self,
        units: &mut Vec<crate::eviction::RecoveredUnit>,
        shared_access: &HashMap<BlockId, AccessHandle>,
        track_clock: bool,
    ) -> io::Result<()> {
        units.reserve(self.entries.read().unwrap().len());
        let mut directory = self.entries.write().unwrap();
        let entries: Vec<_> = directory.iter().collect();
        for (key, loc) in entries {
            let access = shared_access
                .get(&key.materialize())
                .map(AccessHandle::sibling)
                .unwrap_or_else(|| AccessHandle::new(track_clock));
            units.push(crate::eviction::RecoveredUnit {
                unit: key.0.clone(),
                bytes: loc.stored_len(),
                independent: true,
                access: access.clone(),
            });
            directory.attach_access(&key, access);
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
        let _compaction = self.compaction.lock().await;
        let pressure_seal = if pressure {
            let state = self.state.lock().await;
            state
                .active
                .and_then(|id| state.segments.get(&id))
                .is_some_and(|s| s.in_flight.is_empty() && meets_reclaim_threshold(s))
        } else {
            false
        };
        let flush = !checkpoint && !pressure_seal && self.flush_due().await;
        if checkpoint || pressure_seal || flush {
            let _io = match &budget {
                Some(budget) => Some(budget.io().await),
                None => None,
            };
            if flush {
                self.flush().await?;
            } else {
                self.checkpoint().await?;
            }
        }
        let result = self.compact(pressure, budget.as_deref()).await;
        if result.is_err() {
            // Keep unresolved allocations charged until recovery. Failed
            // compaction no longer promises to unlink its files.
            let mut state = self.state.lock().await;
            state.compaction_deletes = None;
            state.poisoned = true;
            self.cached_stats.lock().unwrap().pending_unlink = 0;
        }
        result
    }

    async fn compact(
        &self,
        pressure: bool,
        budget: Option<&crate::background::BackgroundBudget>,
    ) -> io::Result<bool> {
        let (old, entries, cost) = {
            let mut state = self.state.lock().await;
            self.healthy(&state)?;
            let Some(old) = reclaim_candidate(&state, pressure) else {
                return Ok(false);
            };
            let source = &state.segments[&old];
            let cost = source.live_data_bytes.saturating_mul(2)
                + (source.live - source.live_data_bytes)
                + 36 * source.records as u64
                + 49;
            let entries = self.entries.read().unwrap().segment_entries(old);
            debug_assert_eq!(entries.len(), source.records);
            // Register under the same lock as the snapshot. A nonempty source
            // is sealed, so its live addresses can only disappear via Delete.
            if !entries.is_empty() {
                state.compaction_deletes = Some(CompactionDeletes {
                    source: old,
                    offsets: Vec::new(),
                });
            }
            (old, entries, cost)
        };
        let tracks_access = self.journal.model.0.read().unwrap().recency.is_some();
        if let Some(budget) = budget {
            let access_bytes = if tracks_access && !entries.is_empty() {
                12 * entries.len() + 24 * entries.len().div_ceil(512)
            } else {
                0
            };
            budget.bytes(cost as usize + access_bytes).await;
        }

        // Full access checkpoints take this gate before I/O. Preserve that
        // ordering so a snapshot and relocation cannot wait on each other.
        let acquire_io = || async {
            let access_relocation = if tracks_access {
                Some(self.journal.access_relocation.read().await)
            } else {
                None
            };
            let io = match budget {
                Some(budget) => Some(budget.io().await),
                None => None,
            };
            (access_relocation, io)
        };
        let mut io = acquire_io().await;
        let mut staged = if entries.is_empty() {
            None
        } else {
            Some(self.copy_segment(entries).await?)
        };
        #[cfg(test)]
        tests::crash("compact_copy");

        let mut source = loop {
            #[cfg(test)]
            Self::pause_compaction(&self.reconcile_pause).await;
            let publication = self.publication.lock().await;
            let mut state = self.state.lock().await;
            self.healthy(&state)?;
            // An empty active source may have accepted appends and sealed
            // during the unlocked phase. This pass has no copy of those rows.
            if staged.is_none() && (state.segments[&old].live != 0 || !reclaimable(&state, old)) {
                return Ok(false);
            }
            let Some(deletes) = state
                .compaction_deletes
                .as_mut()
                .filter(|deletes| !deletes.offsets.is_empty())
            else {
                state.compaction_deletes = None;
                break self
                    .publish_compaction(&mut state, old, &mut staged)
                    .await?;
            };
            let deleted = std::mem::take(&mut deletes.offsets);
            drop(state);
            drop(publication);
            let copy = staged.as_mut().expect("registered compaction copy");
            if let Some(budget) = budget {
                drop(io);
                let frames = if copy.records.len() == deleted.len() {
                    1
                } else {
                    deleted.len()
                };
                budget.bytes(49 * frames).await;
                io = acquire_io().await;
            }
            self.reconcile_copy(copy, deleted).await?;
        };
        drop(io);

        // A reader which resolved the old address must retain its fd before
        // the path goes away. Readers starting after this barrier see new locators.
        drop(self.readers.write().await);
        #[cfg(test)]
        {
            tests::crash("compact_before_unlink");
            Self::pause_compaction(&self.unlink_pause).await;
        }
        source.close_writers();
        let readers = source.file.fd.lock().await.clone();
        self.unlink_compacted_segment(old, budget).await?;
        {
            let mut state = self.state.lock().await;
            state.allocated -= source.allocated;
            let mut cached = self.cached_stats.lock().unwrap();
            cached.pending_unlink -= source.allocated;
            cached.retired.push((readers, source.allocated));
            cached.published = Self::snapshot_stats(&state);
        }
        if let Some(discarded) = staged {
            self.unlink_compacted_segment(discarded.id, budget).await?;
            let mut cached = self.cached_stats.lock().unwrap();
            cached.pending_unlink = 0;
            cached.staging_allocated = 0;
        }
        Ok(true)
    }

    async fn copy_segment(&self, entries: Vec<(SmallKey, Location)>) -> io::Result<StagedSegment> {
        let id = self.journal.allocate()?;
        let mut segment = self.create_segment(id).await?;
        self.cached_stats.lock().unwrap().staging_allocated = segment.allocated;
        let mut records = HashMap::with_capacity(entries.len());
        // Retain one source descriptor through the copy, including across
        // concurrent deletes. The compaction mutex keeps its pathname alive.
        let source = entries[0].1.file.open().await?;
        let data = segment.data().await?;
        for (key, location) in entries {
            let old_offset = location.offset;
            let new_offset = segment.bytes;
            let len = location.len;
            let checksum = location.checksum;
            let stored_len = location.stored_len();
            let charge = charge(&key, len);
            let bytes = ReadLocation {
                fd: source.clone(),
                location,
            }
            .read_stored()
            .await?;
            let write = disk_io::write(data.clone(), bytes, new_offset).await;
            segment.allocated = disk_io::allocated(data.clone()).await?;
            self.cached_stats.lock().unwrap().staging_allocated = segment.allocated;
            write?;
            #[cfg(test)]
            tests::crash("append_payload");
            // A checkpoint may snapshot these Inserts. They remain invisible
            // and recover with durable_end=0 until Replace commits the file.
            self.journal
                .append(
                    journal::Event::Insert {
                        slot: self.slot,
                        segment: id,
                        offset: new_offset,
                        record: journal::Record { key, len, checksum },
                    },
                    false,
                )
                .await?;
            #[cfg(test)]
            tests::crash("append_index");
            segment.bytes += stored_len;
            segment.metadata += charge - stored_len;
            segment.live += charge;
            segment.live_data_bytes += stored_len;
            segment.records += 1;
            records.insert(
                old_offset,
                CopiedRecord {
                    new_offset,
                    charge,
                    stored_len,
                },
            );
        }
        disk_io::sync(data.clone()).await?;
        self.sync_data_directory(id).await?;
        segment.allocated = disk_io::allocated(data).await?;
        self.cached_stats.lock().unwrap().staging_allocated = segment.allocated;
        segment.sealed = true;
        segment.durable_end = segment.bytes;
        segment.close_writers();
        Ok(StagedSegment {
            id,
            segment,
            records,
        })
    }

    async fn reconcile_copy(&self, copy: &mut StagedSegment, deleted: Vec<u64>) -> io::Result<()> {
        if copy.records.len() == deleted.len() {
            copy.records.clear();
            return self
                .journal
                .append(
                    journal::Event::Replace {
                        old: copy.id,
                        new: None,
                    },
                    false,
                )
                .await;
        }
        for offset in deleted {
            let record = copy
                .records
                .remove(&offset)
                .expect("deleted source address");
            self.journal
                .append(
                    journal::Event::Delete {
                        segment: copy.id,
                        offset: record.new_offset,
                    },
                    false,
                )
                .await?;
            copy.segment.metadata += 21;
            copy.segment.live -= record.charge;
            copy.segment.live_data_bytes -= record.stored_len;
            copy.segment.records -= 1;
        }
        Ok(())
    }

    async fn publish_compaction(
        &self,
        state: &mut State,
        old: u64,
        staged: &mut Option<StagedSegment>,
    ) -> io::Result<Segment> {
        let replacement = staged
            .as_ref()
            .filter(|copy| !copy.records.is_empty())
            .map(|copy| copy.id);
        if let Some(copy) = staged.as_ref().filter(|_| replacement.is_some()) {
            access::persist_relocation(&self.journal, old, copy).await;
        }
        #[cfg(test)]
        tests::crash("compact_reconciled");
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
        {
            tests::crash("compact_manifest");
            Self::pause_compaction(&self.relocation_pause).await;
        }
        if state.active == Some(old) {
            state.active = None;
        }
        let source = state.segments.remove(&old).unwrap();
        state.live -= source.live;
        state.written -= source.bytes + source.metadata;
        {
            let mut entries = self.entries.write().unwrap();
            entries.replace_segment(old, staged.as_ref().filter(|_| replacement.is_some()));
        }
        // The source's physical allocation stays charged until unlink.
        let mut cached = self.cached_stats.lock().unwrap();
        if replacement.is_some() {
            let new = staged.take().unwrap();
            debug_assert_eq!(source.live, new.segment.live);
            state.live += new.segment.live;
            state.written += new.segment.bytes + new.segment.metadata;
            state.allocated += new.segment.allocated;
            state.segments.insert(new.id, new.segment);
            cached.staging_allocated = 0;
        }
        cached.pending_unlink = source.allocated + cached.staging_allocated;
        cached.published = Self::snapshot_stats(state);
        Ok(source)
    }

    async fn unlink_compacted_segment(
        &self,
        id: u64,
        budget: Option<&crate::background::BackgroundBudget>,
    ) -> io::Result<()> {
        let _io = match budget {
            Some(budget) => {
                budget.delete().await;
                Some(budget.io().await)
            }
            None => None,
        };
        let path = self.path(id, "data");
        let dir = path.parent().unwrap().to_owned();
        disk_io::remove(path).await?;
        disk_io::sync_dir(dir).await
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
            live_data_bytes: 0,
            allocated,
            records: 0,
            sealed: false,
        })
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
                break Some((id, s.data().await?, s.records == 0));
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

    fn recover(
        &mut self,
        id: u64,
        meta: &journal::SegmentMeta,
        catalog: &mut catalog::Catalog,
        compact: bool,
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
        let (live, live_data_bytes, records) =
            catalog.restore_segment(id, meta, file.clone(), compact)?;
        let state = self.state.get_mut();
        state.allocated += allocated;
        state.live += live;
        let recovered_metadata = if meta.sealed {
            meta.metadata
        } else {
            8 + live - live_data_bytes
        };
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
                live_data_bytes,
                allocated,
                records,
                sealed: true,
            },
        );
        Ok(())
    }
    fn refresh_recovered_stats(&mut self) {
        self.cached_stats.get_mut().unwrap().published = Self::snapshot_stats(self.state.get_mut());
    }

    fn refresh_stats(&self, state: &State) {
        self.cached_stats.lock().unwrap().published = Self::snapshot_stats(state);
    }

    fn snapshot_stats(state: &State) -> SmallStoreStats {
        SmallStoreStats {
            live_bytes: state.live,
            allocated_bytes: state.allocated,
            dead_bytes: state
                .written
                .saturating_sub(state.live + 8 * state.segments.len() as u64),
            segments: state.segments.len(),
            retired_bytes: 0,
        }
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

fn charge(key: &dyn SmallIdentity, len: u64) -> u64 {
    len + format::record_metadata_bytes(key)
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
