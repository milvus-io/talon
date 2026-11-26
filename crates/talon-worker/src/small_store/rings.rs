//! Independent append/rollover pipelines on the worker's existing rings.
//! Slot zero serves non-ring callers. Native ring N owns slot N+1. Every
//! stream shares a metadata writer and globally unique sharded payload paths.
use super::*;
use crate::miss::{InFlightGuard, InFlightLoads, LoadKey};
use crate::small_store::key::SmallIdentity;
use tokio::sync::OnceCell;

type Stream = Arc<OnceCell<Arc<SegmentStore>>>;

pub struct SmallObjectStore {
    pub(super) root: PathBuf,
    fallback: Arc<SegmentStore>,
    pub(super) journal: Arc<journal::Journal>,
    allocation_unit: u64,
    pub(super) access_checkpoint: tokio::sync::Mutex<()>,
    #[cfg(test)]
    pub(super) access_pause: Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
    pub(super) access_dirty: Arc<super::access::DirtyQueue>,
    pub(super) access_disk: Mutex<super::access::DiskState>,
    pub(super) access_bytes: std::sync::atomic::AtomicU64,
    streams: RwLock<BTreeMap<usize, Stream>>,
    policy: std::sync::OnceLock<Arc<Lru>>,
    index: std::sync::OnceLock<Weak<crate::BlockIndex>>,
    // Only in-flight identities are retained; no second resident-object index.
    mutations: Arc<InFlightLoads>,
    pub(super) tasks: Arc<crate::page_gc::Mutations>,
}

impl SmallObjectStore {
    pub fn open(
        root: PathBuf,
        threshold: u64,
        segment_size: u64,
        capacity: u64,
        root_lock: Option<Arc<crate::page_access_store::CacheRootLock>>,
    ) -> io::Result<Self> {
        Self::open_with_service(
            root,
            threshold,
            segment_size,
            capacity,
            root_lock,
            None,
            None,
        )
    }
    pub(crate) fn open_with_service(
        root: PathBuf,
        threshold: u64,
        segment_size: u64,
        capacity: u64,
        root_lock: Option<Arc<crate::page_access_store::CacheRootLock>>,
        service: Option<&Arc<talon_journal::JournalService>>,
        maintenance: Option<Arc<crate::background::BackgroundBudget>>,
    ) -> io::Result<Self> {
        validate_geometry(threshold, segment_size, capacity)?;
        let journal_started = Instant::now();
        let (journal, model) = match service {
            Some(service) => journal::Journal::open_shared(root.clone(), service, maintenance)?,
            None => journal::Journal::open(root.clone(), root_lock.clone(), maintenance)?,
        };
        let journal_ms = journal_started.elapsed().as_secs_f64() * 1000.0;
        let locators_started = Instant::now();
        let recovered =
            Self::recover_streams(&journal, &model, threshold, segment_size, root_lock)?;
        let locators_ms = locators_started.elapsed().as_secs_f64() * 1000.0;
        let setup_started = Instant::now();
        let mut streams = BTreeMap::new();
        for (slot, mut stream) in recovered {
            stream.refresh_recovered_stats();
            streams.insert(slot, Arc::new(OnceCell::new_with(Some(Arc::new(stream)))));
        }
        let fallback = streams[&0].get().unwrap().clone();
        let access_bytes = super::access::allocated(&root)?;
        let allocation_unit = crate::capacity::filesystem_stats(&root)
            .map_err(io::Error::other)?
            .f_frsize
            .max(1);
        tracing::info!(root = %root.display(), journal_ms, locators_ms,
            setup_ms = setup_started.elapsed().as_secs_f64() * 1000.0,
            "small store recovery phases");
        Ok(Self {
            allocation_unit,
            access_checkpoint: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            access_pause: Mutex::default(),
            access_dirty: Arc::default(),
            access_disk: Mutex::default(),
            access_bytes: std::sync::atomic::AtomicU64::new(access_bytes),
            root,
            fallback,
            journal,
            streams: RwLock::new(streams),
            policy: std::sync::OnceLock::new(),
            index: std::sync::OnceLock::new(),
            mutations: Arc::new(InFlightLoads::new()),
            tasks: Arc::new(crate::page_gc::Mutations::new(true)),
        })
    }

    pub(super) fn recover_streams(
        journal: &Arc<journal::Journal>,
        model: &journal::Model,
        threshold: u64,
        segment_size: u64,
        root_lock: Option<Arc<crate::page_access_store::CacheRootLock>>,
    ) -> io::Result<BTreeMap<usize, SegmentStore>> {
        let mut recovered = BTreeMap::new();
        recovered.insert(
            0,
            SegmentStore::empty(
                threshold,
                segment_size,
                root_lock.clone(),
                0,
                journal.clone(),
            ),
        );
        for (&id, meta) in model
            .segments
            .iter()
            .filter(|(_, meta)| meta.durable_end > 0)
        {
            let stream = recovered.entry(meta.slot).or_insert_with(|| {
                SegmentStore::empty(
                    threshold,
                    segment_size,
                    root_lock.clone(),
                    meta.slot,
                    journal.clone(),
                )
            });
            stream.recover(id, meta)?;
        }
        for (id, allocated) in journal.finish_recovery(model)? {
            let state = recovered
                .get_mut(&model.segments[&id].slot)
                .unwrap()
                .state
                .get_mut();
            let segment = state.segments.get_mut(&id).unwrap();
            state.allocated = state.allocated - segment.allocated + allocated;
            segment.allocated = allocated;
        }
        Ok(recovered)
    }

    pub(super) fn streams(&self) -> Vec<Arc<SegmentStore>> {
        self.stream_snapshot(false).unwrap()
    }

    fn stream_snapshot(&self, nonblocking: bool) -> Option<Vec<Arc<SegmentStore>>> {
        let streams = read_for_unlink(&self.streams, nonblocking)?;
        Some(streams.values().filter_map(|s| s.get().cloned()).collect())
    }

    async fn writer(&self, slot: usize) -> io::Result<Arc<SegmentStore>> {
        let existing = self.streams.read().unwrap().get(&slot).cloned();
        let cell = existing.unwrap_or_else(|| {
            self.streams
                .write()
                .unwrap()
                .entry(slot)
                .or_default()
                .clone()
        });
        cell.get_or_try_init(|| async {
            let stream = Arc::new(SegmentStore::empty(
                self.fallback.threshold,
                self.fallback.segment_size,
                self.fallback._root_lock.clone(),
                slot,
                self.journal.clone(),
            ));
            if let Some(index) = self.index.get().and_then(Weak::upgrade) {
                index.register_small_directory(&stream.entries);
            }
            Ok(stream)
        })
        .await
        .cloned()
    }

    fn owner(&self, id: &(impl SmallStoreKey + ?Sized)) -> Option<Arc<SegmentStore>> {
        self.streams().into_iter().find(|s| s.contains(id))
    }

    fn try_owner(&self, id: &impl SmallStoreKey) -> Option<Arc<SegmentStore>> {
        let id = id.view();
        format::validate_key(&id).ok()?;
        for stream in self.stream_snapshot(true)? {
            if stream.unlink_location(&id, true)?.is_some() {
                return Some(stream);
            }
        }
        None
    }

    fn mutation_key(id: &(impl SmallStoreKey + ?Sized)) -> LoadKey {
        if id.exact_range() {
            LoadKey::ExactRange(id.block().clone())
        } else {
            LoadKey::Whole(id.block().clone())
        }
    }

    async fn mutation(&self, id: &(impl SmallStoreKey + ?Sized)) -> InFlightGuard {
        let key = Self::mutation_key(id);
        loop {
            if let Some(guard) = self.mutations.admit_owned(key.clone()) {
                return guard;
            }
            self.mutations.wait(&key).await;
        }
    }

    pub(crate) fn set_background_budget(&self, budget: Arc<crate::background::BackgroundBudget>) {
        self.journal.set_budget(budget);
    }

    pub fn accepts(&self, id: &(impl SmallStoreKey + ?Sized), len: u64) -> bool {
        self.fallback.accepts(id, len)
    }
    pub fn contains(&self, id: &(impl SmallStoreKey + ?Sized)) -> bool {
        self.lookup_entry(&id.view()).ok().flatten().is_some()
    }

    /// Resolve all ring-owned records through the namespace's shared index.
    pub(crate) fn lookup_entry(&self, id: &dyn SmallIdentity) -> io::Result<Option<Location>> {
        let id = id.view();
        if !Self::validate_lookup_key(&id)? {
            return Ok(None);
        }
        Ok(self.journal.model.lookup(&id))
    }

    fn validate_lookup_key(id: &super::key::SmallRef<'_>) -> io::Result<bool> {
        if id.kind != crate::eviction::UnitKind::ExactRange && id.offset != 0 {
            return Ok(false);
        }
        format::validate_key(id)?;
        Ok(true)
    }

    fn lookup_read<T, E: From<io::Error>>(
        &self,
        id: &dyn SmallIdentity,
        validate: impl FnOnce(u64) -> Result<T, E>,
    ) -> Result<Option<(Location, T)>, E> {
        let id = id.view();
        if !Self::validate_lookup_key(&id)? {
            return Ok(None);
        }
        self.journal.model.lookup_read(&id, validate)
    }

    pub(crate) fn touch_entry<T, E: From<io::Error>>(
        &self,
        id: &dyn SmallIdentity,
        validate: impl FnOnce(u64) -> Result<T, E>,
    ) -> Result<Option<T>, E> {
        Ok(self.lookup_read(id, validate)?.map(|(loc, range)| {
            loc.touch();
            range
        }))
    }

    pub(crate) async fn read_location<T, E: From<io::Error>>(
        &self,
        id: &(impl SmallStoreKey + ?Sized),
        validate: impl FnOnce(u64) -> Result<T, E>,
    ) -> Result<Option<(ReadLocation, T)>, E> {
        let readers = self.journal.readers.read().await;
        let Some((location, range)) = self.lookup_read(&id.view(), validate)? else {
            return Ok(None);
        };
        let loc = location.retain().await?;
        drop(readers);
        loc.touch();
        Ok(Some((loc, range)))
    }

    pub(crate) async fn get_range(
        &self,
        id: &(impl SmallStoreKey + ?Sized),
        offset: u64,
        len: u64,
    ) -> io::Result<Option<BlockHandle>> {
        self.read_location(id, |payload_len| validate_range(payload_len, offset, len))
            .await?
            .map(|(loc, ())| loc.range(offset, len))
            .transpose()
    }

    pub async fn get(&self, id: &(impl SmallStoreKey + ?Sized)) -> io::Result<Option<BlockHandle>> {
        self.read_location(id, Ok::<_, io::Error>)
            .await?
            .map(|(loc, len)| loc.range(0, len))
            .transpose()
    }

    pub async fn read(&self, id: &(impl SmallStoreKey + ?Sized)) -> io::Result<Option<Bytes>> {
        if let Some((loc, _)) = self.read_location(id, Ok::<_, io::Error>).await? {
            return Ok(Some(loc.read().await?));
        }
        Ok(None)
    }

    pub async fn put(
        self: &Arc<Self>,
        id: &(impl SmallStoreKey + ?Sized),
        value: Bytes,
    ) -> io::Result<u64> {
        // Capture before an owned task can run on a different Tokio thread.
        let slot = talon_uring::ring_id().map_or(0, |id| id + 1);
        let store = self.clone();
        let id = id.cache_unit();
        self.tasks
            .run(async move {
                let _guard = store.mutation(&id).await;
                // Existing identities stay in their original stream, including
                // immutable-byte validation. New objects append on this ring.
                let stream = match store.owner(&id) {
                    Some(stream) => stream,
                    None => store.writer(slot).await?,
                };
                stream
                    .put_inner(&id, value, store.policy.get().cloned())
                    .await
            })
            .await
    }

    pub async fn delete(self: &Arc<Self>, id: &(impl SmallStoreKey + ?Sized)) -> io::Result<()> {
        let store = self.clone();
        let id = id.cache_unit();
        self.tasks
            .run(async move {
                let _guard = store.mutation(&id).await;
                if let Some(stream) = store.owner(&id) {
                    stream.delete_inner(&id).await?;
                }
                Ok(())
            })
            .await
    }

    #[cfg(test)]
    pub(crate) fn under_mutation_registry<T: Send>(&self, work: impl FnOnce() -> T + Send) -> T {
        self.mutations.under_registry_lock(work)
    }

    #[cfg(test)]
    pub(crate) fn fail_next_delete_after_journal(&self, id: &BlockId) {
        self.owner(id)
            .unwrap()
            .fail_delete_after_journal
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) async fn delete_idle(
        self: &Arc<Self>,
        id: CacheUnit,
        ticket: Arc<super::policy::SmallAccess>,
        expiry: impl FnOnce() -> (u64, u64) + Send + 'static,
    ) -> io::Result<bool> {
        self.delete_if_current(id, ticket, None, move |current| {
            let (now, age) = expiry();
            current.is_idle_at(now, age)
        })
        .await
    }

    pub(crate) async fn delete_gc(
        self: &Arc<Self>,
        candidate: super::SmallGcCandidate,
        revision: u64,
        deadline: Option<Instant>,
        authorized: impl FnOnce() -> bool + Send + 'static,
    ) -> io::Result<bool> {
        self.delete_if_current(candidate.unit, candidate.ticket, deadline, move |current| {
            current.gc_is_current_for_unlink(revision, deadline.is_some()) && authorized()
        })
        .await
    }

    async fn delete_if_current(
        self: &Arc<Self>,
        id: CacheUnit,
        ticket: Arc<super::policy::SmallAccess>,
        deadline: Option<Instant>,
        eligible: impl FnOnce(&super::policy::SmallAccess) -> bool + Send + 'static,
    ) -> io::Result<bool> {
        let store = self.clone();
        self.tasks
            .run(async move {
                let owner = if deadline.is_some() {
                    store.try_owner(&id)
                } else {
                    store.owner(&id)
                };
                let Some(stream) = owner else {
                    return Ok(false);
                };
                let guard = stream
                    .delete_inner_if(&id, deadline, |loc| {
                        let current = loc
                            .access
                            .as_ref()
                            .and_then(AccessHandle::small)
                            .is_some_and(|current| {
                                Arc::ptr_eq(current, &ticket) && eligible(current)
                            });
                        if !current || deadline.is_some_and(|at| Instant::now() >= at) {
                            return None;
                        }
                        // A put or unconditional delete can hold this key while
                        // waiting for stream state. Defer on contention.
                        let key = Self::mutation_key(&id);
                        if deadline.is_some() {
                            store.mutations.try_admit_owned(key)
                        } else {
                            store.mutations.admit_owned(key)
                        }
                    })
                    .await?;
                if let Some(_guard) = guard {
                    store.policy().expect("compact small policy").remove(&id);
                    return Ok(true);
                }
                Ok(false)
            })
            .await
    }

    pub fn stats(&self) -> io::Result<SmallStoreStats> {
        let mut total = SmallStoreStats::default();
        for stream in self.streams() {
            let stats = stream.stats()?;
            total.live_bytes += stats.live_bytes;
            total.allocated_bytes += stats.allocated_bytes;
            total.retired_bytes += stats.retired_bytes;
            total.dead_bytes += stats.dead_bytes;
            total.segments += stats.segments;
        }
        total.allocated_bytes +=
            self.access_bytes.load(std::sync::atomic::Ordering::Relaxed) + self.journal.allocated();
        Ok(total)
    }

    /// Startup only. Attach access tokens to locators and contribute units to
    /// the shared bulk policy construction without registering in BlockIndex.
    pub(crate) fn restore_policy_units(
        &self,
        index: &Arc<crate::BlockIndex>,
        units: &mut Vec<crate::eviction::RecoveredUnit>,
        track_clock: bool,
    ) {
        self.bind_index(index);
        if !track_clock {
            let streams = self.streams.read().unwrap();
            self.journal.model.0.write().unwrap().enable_compact(
                streams
                    .iter()
                    .filter_map(|(&slot, stream)| stream.get().map(|_| slot)),
            );
            return;
        }
        let shared_access = units
            .iter()
            .filter(|unit| {
                u64::from(crate::block_key::BlockIdentity::view(&unit.unit.block).block_size)
                    <= talon_core::MAX_EXACT_READ_LEN
            })
            .map(|unit| (unit.unit.block.materialize(), unit.access.clone()))
            .collect();
        for stream in self.streams() {
            stream
                .restore_policy_units(units, &shared_access, track_clock)
                .expect("validated small object identity");
        }
    }

    /// Bind the policy built from both stores, then restore persisted access ages.
    fn bind_index(&self, index: &Arc<crate::BlockIndex>) {
        if let Some(bound) = self.index.get() {
            assert!(
                bound.ptr_eq(&Arc::downgrade(index)),
                "small store index changed"
            );
            return;
        }
        assert!(self.index.set(Arc::downgrade(index)).is_ok());
        for stream in self.streams() {
            index.register_small_directory(&stream.entries);
        }
    }

    pub(crate) fn bind_policy(&self, policy: Arc<Lru>) {
        self.bind_index(&policy.index());
        let track_clock = policy.tracks_small_clock();
        assert!(
            self.policy.set(policy).is_ok(),
            "small store already bound to a runtime"
        );
        if track_clock && !self.has_recency() {
            self.restore_access();
        }
    }

    pub(super) fn policy(&self) -> Option<&Arc<Lru>> {
        self.policy.get()
    }

    pub(crate) fn len(&self) -> usize {
        self.journal
            .model
            .1
            .live_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn logical_bytes(&self) -> u64 {
        self.streams
            .read()
            .unwrap()
            .values()
            .filter_map(|s| s.get())
            .map(|s| {
                s.entries
                    .read()
                    .unwrap()
                    .values()
                    .map(|loc| loc.len)
                    .sum::<u64>()
            })
            .sum()
    }

    #[cfg(test)]
    pub(crate) fn access(&self, id: &(impl SmallStoreKey + ?Sized)) -> Option<AccessHandle> {
        self.lookup_entry(&id.view()).ok().flatten()?.access
    }

    /// Only threshold-eligible segments provide planning credit. Garbage in a
    /// mostly-live segment cannot cover pressure until more items are evicted.
    /// Planning credit, never physical accounting. Round live record extents
    /// to destination file allocation units. Computed only on the
    /// pressure path, not by walking all segments on each append.
    pub(crate) async fn reclaimable_bytes(&self) -> u64 {
        let mut bytes = 0;
        for stream in self.streams() {
            let state = stream.state.lock().await;
            for (&id, s) in &state.segments {
                if Some(id) == state.standby
                    || !s.in_flight.is_empty()
                    || !meets_reclaim_threshold(s)
                {
                    continue;
                }
                let destination = if s.live == 0 {
                    0
                } else {
                    s.live_data_bytes.div_ceil(self.allocation_unit) * self.allocation_unit
                };
                bytes += s.allocated.saturating_sub(destination);
            }
        }
        bytes
    }

    /// One segment per stream, stopping once enough physical space was released.
    /// Readers retaining retired inodes remain charged until their last fd closes.
    pub(crate) async fn reclaim_with_budget(
        self: &Arc<Self>,
        bytes: u64,
        budget: Option<Arc<crate::background::BackgroundBudget>>,
    ) -> io::Result<bool> {
        let store = self.clone();
        self.tasks
            .run(async move {
                let before = store.stats()?.allocated_bytes;
                let mut reclaimed = false;
                for stream in store.streams() {
                    if before.saturating_sub(store.stats()?.allocated_bytes) >= bytes {
                        break;
                    }
                    reclaimed |= stream.maintain_inner(false, true, budget.clone()).await?;
                }
                if !reclaimed && before.saturating_sub(store.stats()?.allocated_bytes) < bytes {
                    store.journal.checkpoint().await?;
                    reclaimed |= store.stats()?.allocated_bytes < before;
                }
                Ok(reclaimed)
            })
            .await
    }

    pub async fn maintain(self: &Arc<Self>, checkpoint: bool) -> io::Result<bool> {
        self.maintain_with_budget(checkpoint, None).await
    }

    pub(crate) async fn maintain_with_budget(
        self: &Arc<Self>,
        checkpoint: bool,
        budget: Option<Arc<crate::background::BackgroundBudget>>,
    ) -> io::Result<bool> {
        let store = self.clone();
        self.tasks
            .run(async move {
                let mut reclaimed = false;
                for stream in store.streams() {
                    reclaimed |= stream
                        .maintain_inner(checkpoint, false, budget.clone())
                        .await?;
                }
                if checkpoint {
                    store.journal.checkpoint().await?;
                }
                Ok(reclaimed)
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use talon_core::{Backend, ObjectId, Version};

    fn id(name: &str) -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "bucket", name),
            0,
            1 << 20,
            Version::new("v1"),
        )
    }
    fn open(root: &Path) -> Arc<SmallObjectStore> {
        Arc::new(
            SmallObjectStore::open(root.to_owned(), 128 << 10, 256 << 10, 4 << 20, None).unwrap(),
        )
    }
    fn native<T>(slot: usize, work: impl std::future::Future<Output = T>) -> T {
        monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let ring = talon_uring::LocalRing::enter_with_id(slot);
                let result = work.await;
                ring.drain().await;
                result
            })
    }

    #[tokio::test]
    async fn ownership_event_excludes_segments_first_published_after_capture() {
        let root = tempfile::tempdir().unwrap();
        let store = open(root.path());
        let index = Arc::new(crate::BlockIndex::new());
        store.restore_policy_units(&index, &mut Vec::new(), false);
        store.init_recency();
        store.bind_policy(Arc::new(crate::Lru::from_recovered(
            index,
            Vec::new(),
            0,
            true,
        )));
        store.init_ownership();

        let delayed = store.writer(1).await.unwrap();
        let ready = store.writer(2).await.unwrap();
        let pause = Arc::new(super::super::tests::AppendPause::default());
        *delayed.append_pause.lock().unwrap() = Some(pause.clone());
        let first_write = {
            let stream = delayed.clone();
            let policy = store.policy().cloned();
            tokio::spawn(async move {
                stream
                    .put_inner(&id("late-0"), Bytes::from_static(b"L"), policy)
                    .await
                    .unwrap();
            })
        };
        pause.entered.notified().await;
        let before = id("before");
        ready
            .put_inner(&before, Bytes::from_static(b"B"), store.policy().cloned())
            .await
            .unwrap();
        let event = store.ownership_limit(true).unwrap();
        pause.resume.notify_one();
        first_write.await.unwrap();
        let late_segment = store
            .lookup_entry(&id("late-0"))
            .unwrap()
            .unwrap()
            .segment();
        assert!(late_segment < store.lookup_entry(&before).unwrap().unwrap().segment());
        for n in 1..130 {
            let block = id(&format!("late-{n}"));
            delayed
                .put_inner(&block, Bytes::from_static(b"L"), store.policy().cloned())
                .await
                .unwrap();
            assert_eq!(
                store.lookup_entry(&block).unwrap().unwrap().segment(),
                late_segment
            );
        }

        let (done, rows) =
            store.ownership_candidates(&mut Default::default(), 0, event, 64, |_| false);
        assert!(
            done,
            "post-event publication consumed the event scan budget"
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].unit.block(), &before);

        let supplement = store.ownership_limit(false).unwrap();
        let (done, rows) = store.ownership_candidates(
            &mut Default::default(),
            event.epoch,
            supplement,
            256,
            |_| false,
        );
        assert!(done);
        assert_eq!(rows.len(), 130);
        assert!(rows.iter().all(|row| row.unit.block() != &before));
    }

    #[test]
    fn relocation_budget_covers_payload_and_actual_journal_bytes() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            for survivor in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let store = open(dir.path());
                store
                    .put(&id("deleted"), Bytes::from(vec![1; 4096]))
                    .await
                    .unwrap();
                if survivor {
                    store
                        .put(&id("survivor"), Bytes::from(vec![2; 2048]))
                        .await
                        .unwrap();
                }
                store.maintain(true).await.unwrap();
                store.delete(&id("deleted")).await.unwrap();
                let log_bytes = || {
                    super::super::tests::files(dir.path())
                        .into_iter()
                        .filter(|p| {
                            p.file_name()
                                .unwrap()
                                .to_string_lossy()
                                .starts_with("journal.")
                        })
                        .map(|p| std::fs::metadata(p).unwrap().len())
                        .sum::<u64>()
                };
                let before = log_bytes();
                let budget = crate::background::BackgroundBudget::new(&talon_core::WorkerConfig {
                    background_io_max_mb_per_sec: 0,
                    background_delete_max_per_sec: 0,
                    ..Default::default()
                });
                assert!(store
                    .maintain_with_budget(false, Some(budget.clone()))
                    .await
                    .unwrap());
                let payload_io = if survivor {
                    2 * (2048 + CHECKSUM_BYTES)
                } else {
                    0
                };
                assert_eq!(budget.charged_bytes(), payload_io + log_bytes() - before);
            }
        });
    }

    #[tokio::test]
    async fn compact_recovery_classifies_all_streams_in_one_lookup_pass() {
        let root = tempfile::tempdir().unwrap();
        let mut fixtures = Vec::new();
        {
            let store = open(root.path());
            for slot in 0..4 {
                let stream = store.writer(slot).await.unwrap();
                let mut expected = (0, 0);
                for n in 0..=slot {
                    let block = id(&format!("stream-{slot}-row-{n}"));
                    let payload = Bytes::from(vec![slot as u8; 17 + n]);
                    stream
                        .put_inner(&block, payload.clone(), None)
                        .await
                        .unwrap();
                    expected.0 += payload.len() as u64 + CHECKSUM_BYTES;
                    expected.1 += 1;
                    fixtures.push((slot, block, payload, expected));
                }
            }
            store.maintain(true).await.unwrap();
        }
        let reopened = open(root.path());
        let index = Arc::new(crate::BlockIndex::new());
        let mut generic = Vec::new();
        reopened.restore_policy_units(&index, &mut generic, false);
        assert!(generic.is_empty());
        reopened.assert_no_resident_policy_allocations();
        {
            let catalog = reopened.journal.model.0.read().unwrap();
            assert_eq!(catalog.compact_recovery_slots, catalog.lookup.capacity());
            for slot in 0..4 {
                let expected = fixtures
                    .iter()
                    .rfind(|(stream, ..)| *stream == slot)
                    .unwrap()
                    .3;
                assert_eq!(catalog.compact_totals[&slot], expected);
            }
        }
        for (_, block, payload, _) in fixtures {
            assert_eq!(reopened.read(&block).await.unwrap(), Some(payload));
        }
    }

    #[test]
    fn blocked_ring_append_does_not_block_another_ring_and_recovery_keeps_all_slots() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());
        let release = Arc::new(tokio::sync::Notify::new());
        let (entered, held) = std::sync::mpsc::channel();
        let a = store.clone();
        let unblock = release.clone();
        let first = std::thread::spawn(move || {
            native(0, async move {
                a.put(&id("a"), Bytes::from_static(b"ring zero"))
                    .await
                    .unwrap();
                let writer = a.writer(1).await.unwrap();
                writer.tasks.drain().await;
                let _gate = writer.state.lock().await;
                entered.send(()).unwrap();
                unblock.notified().await;
            })
        });
        held.recv_timeout(Duration::from_secs(5)).unwrap();
        let b = store.clone();
        let second = std::thread::spawn(move || {
            native(1, async move {
                disk_io::BLOCKING_CALLS.with(|c| c.set(0));
                let result = talon_uring::timeout(
                    Duration::from_secs(2),
                    b.put(&id("b"), Bytes::from_static(b"ring one")),
                )
                .await;
                assert_eq!(
                    b.read(&id("a")).await.unwrap().unwrap().as_ref(),
                    b"ring zero"
                );
                assert_eq!(disk_io::BLOCKING_CALLS.with(|c| c.get()), 0);
                result
            })
        });
        let result = second.join().unwrap();
        release.notify_one();
        first.join().unwrap();
        result
            .expect("another ring must not wait for ring zero's append lock")
            .unwrap();
        assert!(!dir.path().join("ring-1").exists());
        assert!(!dir.path().join("ring-2").exists());
        let a_segment = store
            .owner(&id("a"))
            .unwrap()
            .entries
            .read()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .segment;
        let b_segment = store
            .owner(&id("b"))
            .unwrap()
            .entries
            .read()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .segment;
        assert_ne!(a_segment, b_segment);
        assert!(store.journal.data_path(a_segment).exists());
        assert!(store.journal.data_path(b_segment).exists());
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(store.maintain(true)).unwrap();
        drop(store);
        // Reopen without any native ring, with admissions disabled. Historical
        // slots remain readable even if the new worker has fewer rings.
        let recovered = Arc::new(
            SmallObjectStore::open(dir.path().to_owned(), 0, 256 << 10, 4 << 20, None).unwrap(),
        );
        rt.block_on(async {
            assert_eq!(
                recovered.read(&id("a")).await.unwrap().unwrap().as_ref(),
                b"ring zero"
            );
            assert_eq!(
                recovered.read(&id("b")).await.unwrap().unwrap().as_ref(),
                b"ring one"
            );
            let handle = recovered.get(&id("a")).await.unwrap().unwrap();
            recovered.delete(&id("a")).await.unwrap();
            recovered.maintain(false).await.unwrap();
            assert!(!recovered.contains(&id("a")));
            assert!(recovered.stats().unwrap().retired_bytes > 0);
            drop(handle);
        });
        drop(recovered);
        let recovered = open(dir.path());
        assert!(!recovered.contains(&id("a")));
        assert!(recovered.contains(&id("b")));
    }

    #[test]
    fn native_ring_submits_another_append_before_first_completes() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());
        native(0, async {
            let stream = store.writer(1).await.unwrap();
            let pause = Arc::new(super::super::tests::AppendPause::default());
            *stream.append_pause.lock().unwrap() = Some(pause.clone());
            disk_io::BLOCKING_CALLS.with(|c| c.set(0));
            let first = store.clone();
            let a = talon_uring::spawn_local(async move {
                first.put(&id("a"), Bytes::from_static(b"first")).await
            });
            pause.entered.notified().await;
            talon_uring::timeout(
                Duration::from_secs(2),
                store.put(&id("b"), Bytes::from_static(b"second")),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(!store.contains(&id("a")) && store.contains(&id("b")));
            pause.resume.notify_one();
            a.await.unwrap().unwrap();
            store.maintain(true).await.unwrap();
            assert_eq!(disk_io::BLOCKING_CALLS.with(|c| c.get()), 0);
        });
        drop(store);
        let recovered = open(dir.path());
        assert!(recovered.contains(&id("a")) && recovered.contains(&id("b")));
    }

    #[test]
    fn concurrent_ring_writes_preserve_one_immutable_identity() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let threads: Vec<_> = (0..2)
            .map(|ring| {
                let store = store.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    native(ring, async move {
                        barrier.wait();
                        store
                            .put(&id("shared"), Bytes::from(vec![ring as u8; 3072]))
                            .await
                    })
                })
            })
            .collect();
        let success: Vec<_> = threads
            .into_iter()
            .map(|t| t.join().unwrap().is_ok())
            .collect();
        assert_eq!(success.iter().filter(|ok| **ok).count(), 1);
        assert_eq!(
            store
                .streams()
                .iter()
                .filter(|s| s.contains(&id("shared")))
                .count(),
            1
        );
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(store.maintain(true)).unwrap();
        drop(store);
        let store = open(dir.path());
        let value = rt.block_on(store.read(&id("shared"))).unwrap().unwrap();
        assert_eq!(value.as_ref(), vec![if success[0] { 0 } else { 1 }; 3072]);
    }

    #[test]
    fn old_format_is_rejected_without_modifying_it() {
        for name in ["MANIFEST", "1.index", "1.data", "ring-1"] {
            let dir = tempfile::tempdir().unwrap();
            let old = dir.path().join(name);
            if name.starts_with("ring-") {
                std::fs::create_dir(&old).unwrap();
            } else {
                std::fs::write(&old, b"old cache").unwrap();
            }
            let error =
                SmallObjectStore::open(dir.path().to_owned(), 128 << 10, 256 << 10, 4 << 20, None)
                    .err()
                    .unwrap();
            assert!(error.to_string().contains("legacy"));
            assert!(old.exists());
            assert!(!dir.path().join("CURRENT").exists());
            assert!(!dir.path().join("metadata").exists());
            if old.is_file() {
                assert_eq!(std::fs::read(old).unwrap(), b"old cache");
            }
        }
    }

    #[tokio::test]
    async fn slow_data_seal_does_not_block_other_stream_commit_or_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());
        let first = store.writer(1).await.unwrap();
        let second = store.writer(2).await.unwrap();
        first
            .put(&id("first"), Bytes::from_static(b"first"))
            .await
            .unwrap();
        first.tasks.drain().await;
        let pause = Arc::new(super::super::tests::SealPause::default());
        *first.seal_pause.lock().unwrap() = Some(pause.clone());
        let a = first.clone();
        let seal = tokio::spawn(async move { a.checkpoint().await });
        pause.entered.notified().await;
        let progress = tokio::time::timeout(Duration::from_secs(2), async {
            second
                .put(&id("second"), Bytes::from_static(b"second"))
                .await?;
            second.checkpoint().await?;
            store.journal.checkpoint().await
        })
        .await;
        pause.resume.notify_one();
        progress
            .expect("other stream and snapshot must progress during data fsync")
            .unwrap();
        seal.await.unwrap().unwrap();
        drop(first);
        drop(second);
        drop(store);
        let recovered = open(dir.path());
        assert!(recovered.contains(&id("first")));
        assert!(recovered.contains(&id("second")));
    }
}

#[cfg(test)]
fn validate_recovered_keys(
    keys: &mut [(u64, crate::small_store::key::SmallRef<'_>)],
) -> io::Result<()> {
    fn parts(
        key: crate::small_store::key::SmallRef<'_>,
    ) -> (&str, &str, &str, u64, u32, &str, bool) {
        (
            key.object.backend.prefix(),
            key.object.bucket,
            key.object.object_path,
            key.offset,
            key.block_size,
            key.version,
            key.kind == crate::eviction::UnitKind::ExactRange,
        )
    }
    keys.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| parts(a.1).cmp(&parts(b.1))));
    if keys.windows(2).any(|pair| pair[0].1 == pair[1].1) {
        return Err(invalid("duplicate recovered object"));
    }
    Ok(())
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use talon_core::{Backend, ObjectId, Version};

    fn id(n: usize) -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "bucket", format!("object/{n}")),
            0,
            1 << 20,
            Version::new("v1"),
        )
    }
    fn open(root: &std::path::Path) -> io::Result<SmallObjectStore> {
        SmallObjectStore::open(root.to_owned(), 128 << 10, 256 << 10, 4 << 20, None)
    }

    #[tokio::test]
    async fn shared_lookup_protects_async_open_and_releases_the_gate_on_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(open(dir.path()).unwrap());
        let stream = store.writer(1).await.unwrap();
        stream
            .put(&id(0), Bytes::from_static(b"data"))
            .await
            .unwrap();
        let other = store.writer(2).await.unwrap();
        let location = store.lookup_entry(&id(0)).unwrap().unwrap();
        let held = location.file.fd.lock().await;
        let reader_store = store.clone();
        let task =
            tokio::spawn(
                async move { reader_store.read_location(&id(0), Ok::<_, io::Error>).await },
            );
        tokio::time::timeout(Duration::from_secs(5), async {
            while stream.readers.try_write().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(other.readers.try_write().is_err());
        assert!(!task.is_finished());
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        assert!(stream.readers.try_write().is_ok());
        drop(held);
        let (retained, _) = store
            .read_location(&id(0), Ok::<_, io::Error>)
            .await
            .unwrap()
            .unwrap();
        assert!(stream.readers.try_write().is_ok());
        store.delete(&id(0)).await.unwrap();
        store.maintain(true).await.unwrap();
        assert!(!store.contains(&id(0)));
        assert_eq!(retained.read().await.unwrap(), Bytes::from_static(b"data"));
    }

    #[tokio::test]
    async fn compaction_publishes_before_waiting_for_old_open_and_survives_cancellation() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let dir = tempfile::tempdir().unwrap();
            let store = Arc::new(open(dir.path()).unwrap());
            let stream = store.writer(1).await.unwrap();
            stream
                .put(&id(0), Bytes::from(vec![0; 8192]))
                .await
                .unwrap();
            stream
                .put(&id(1), Bytes::from_static(b"survivor"))
                .await
                .unwrap();
            stream.checkpoint().await.unwrap();
            store.delete(&id(0)).await.unwrap();
            let old = store.lookup_entry(&id(1)).unwrap().unwrap();
            let before = stream.stats().unwrap();
            let pause = Arc::new(super::super::tests::AppendPause::default());
            *stream.copied_pause.lock().unwrap() = Some(pause.clone());
            let compaction = tokio::spawn({
                let stream = stream.clone();
                async move { stream.maintain(false).await }
            });
            pause.entered.notified().await;
            let staged = stream.stats().unwrap();
            assert!(staged.allocated_bytes > before.allocated_bytes);
            assert_eq!(staged.live_bytes, before.live_bytes);
            assert_eq!(staged.segments, before.segments);
            let held = old.file.fd.lock().await;
            let reader = tokio::spawn({
                let store = store.clone();
                async move { store.read(&id(1)).await }
            });
            while stream.readers.try_write().is_ok() {
                tokio::task::yield_now().await;
            }
            pause.resume.notify_one();
            while store.lookup_entry(&id(1)).unwrap().unwrap().segment == old.segment {
                tokio::task::yield_now().await;
            }
            assert!(old.file.path.is_file());
            assert!(!compaction.is_finished());
            stream
                .put(&id(2), Bytes::from_static(b"foreground append"))
                .await
                .unwrap();
            compaction.abort();
            let _ = compaction.await;
            drop(held);
            assert_eq!(reader.await.unwrap().unwrap().unwrap(), b"survivor"[..]);
            stream.tasks.drain().await;
            assert!(!old.file.path.exists());
            assert_eq!(store.read(&id(1)).await.unwrap().unwrap(), b"survivor"[..]);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn shared_lookup_stays_available_while_another_ring_waits_for_delete_budget() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(open(dir.path()).unwrap());
        let first = store.writer(1).await.unwrap();
        let second = store.writer(2).await.unwrap();
        first.put(&id(0), Bytes::from_static(b"old")).await.unwrap();
        second
            .put(&id(1), Bytes::from_static(b"live"))
            .await
            .unwrap();
        store.delete(&id(0)).await.unwrap();
        let budget = crate::background::BackgroundBudget::new(&talon_core::WorkerConfig {
            background_io_max_mb_per_sec: 0,
            background_delete_max_per_sec: 1,
            ..Default::default()
        });
        let task_budget = budget.clone();
        let task =
            tokio::spawn(
                async move { first.maintain_inner(false, false, Some(task_budget)).await },
            );
        tokio::time::timeout(Duration::from_secs(5), async {
            while budget.charged_deletes() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!task.is_finished());
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(200), store.read(&id(1)))
                .await
                .unwrap()
                .unwrap(),
            Some(Bytes::from_static(b"live"))
        );
        assert!(task.await.unwrap().unwrap());
    }

    #[tokio::test]
    async fn compact_policy_survives_relocation_pins_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = Arc::new(open(dir.path()).unwrap());
            let index = Arc::new(crate::BlockIndex::new());
            let mut units = Vec::new();
            store.restore_policy_units(&index, &mut units, false);
            let policy = Arc::new(crate::Lru::from_recovered(index, units, 0, false));
            store.bind_policy(policy.clone());
            for n in 0..6 {
                store
                    .put(&id(n), Bytes::from(vec![n as u8; 48 << 10]))
                    .await
                    .unwrap();
            }
            let unit = crate::eviction::CacheUnit::Whole(id(3));
            let before = store.lookup_entry(&id(3)).unwrap().unwrap();
            let token = before.access.clone().unwrap();
            assert!(policy.pin(&unit));
            assert!(policy.pin(&unit));
            for n in 0..3 {
                store.delete(&id(n)).await.unwrap();
                policy.remove(&crate::eviction::CacheUnit::Whole(id(n)));
            }
            store.maintain(true).await.unwrap();
            let after = store.lookup_entry(&id(3)).unwrap().unwrap();
            assert_ne!(before.segment, after.segment);
            assert!(Arc::ptr_eq(
                token.small().unwrap(),
                after.access.unwrap().small().unwrap()
            ));
            token.touch();
            assert!(token.small().unwrap().referenced());
            assert!(policy
                .candidates_to_fit(0, &Default::default())
                .iter()
                .all(|c| c.unit != unit));
            policy.unpin(&unit);
            assert!(policy
                .candidates_to_fit(0, &Default::default())
                .iter()
                .all(|c| c.unit != unit));
            policy.unpin(&unit);
            let candidates = policy.candidates_to_fit(0, &Default::default());
            assert!(candidates
                .iter()
                .any(|c| c.unit == unit && policy.candidate_is_current(c)));
        }
        let store = Arc::new(open(dir.path()).unwrap());
        let index = Arc::new(crate::BlockIndex::new());
        let mut units = Vec::new();
        store.restore_policy_units(&index, &mut units, false);
        assert!(units.is_empty());
        let policy = Arc::new(crate::Lru::from_recovered(index, units, 0, false));
        store.bind_policy(policy.clone());
        assert_eq!(policy.len(), 3);
        for n in 0..6 {
            let actual = store.read(&id(n)).await.unwrap();
            assert_eq!(
                actual,
                (n >= 3).then(|| Bytes::from(vec![n as u8; 48 << 10]))
            );
        }
    }

    #[test]
    fn fingerprint_collisions_compare_complete_keys() {
        let a = SmallKey::new(&id(1));
        let b = SmallKey::new(&id(2));
        let same = SmallKey::new(&id(1));
        let mut distinct = [(0, b.view()), (0, a.view())];
        validate_recovered_keys(&mut distinct).unwrap();
        let mut duplicate = [(0, a.view()), (0, b.view()), (0, same.view())];
        assert!(validate_recovered_keys(&mut duplicate).is_err());
    }

    #[tokio::test]
    async fn recovered_compact_policy_accepts_first_fallback_admission() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = open(dir.path()).unwrap();
            let stream = store.writer(1).await.unwrap();
            stream
                .put(&id(0), Bytes::from_static(b"data"))
                .await
                .unwrap();
            stream.maintain(true).await.unwrap();
        }
        let store = Arc::new(open(dir.path()).unwrap());
        let index = Arc::new(crate::BlockIndex::new());
        let mut units = Vec::new();
        store.restore_policy_units(&index, &mut units, false);
        assert!(units.is_empty());
        let policy = Arc::new(crate::Lru::from_recovered(index, units, 0, false));
        store.bind_policy(policy.clone());
        assert_eq!(policy.len(), 1);
        assert_eq!(policy.total_bytes(), 4 + CHECKSUM_BYTES);

        store.put(&id(1), Bytes::from_static(b"new")).await.unwrap();
        assert_eq!(policy.len(), 2);
        assert_eq!(policy.total_bytes(), 7 + 2 * CHECKSUM_BYTES);
        assert_eq!(store.read(&id(1)).await.unwrap().unwrap(), &b"new"[..]);
        store.delete(&id(0)).await.unwrap();
        policy.remove(&crate::eviction::CacheUnit::Whole(id(0)));
        assert_eq!(policy.len(), 1);
        assert_eq!(policy.total_bytes(), 3 + CHECKSUM_BYTES);
        store.delete(&id(1)).await.unwrap();
        policy.remove(&crate::eviction::CacheUnit::Whole(id(1)));
        assert!(policy.is_empty());
        assert_eq!(policy.total_bytes(), 0);
    }

    #[tokio::test]
    async fn cross_ring_duplicates_fail_before_orphan_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path()).unwrap();
        for slot in [1, 2] {
            let stream = store.writer(slot).await.unwrap();
            // Bypass the normal cross-ring admission guard to simulate a
            // corrupt but individually valid pair of sealed journal records.
            stream
                .put(&id(0), Bytes::from_static(b"data"))
                .await
                .unwrap();
            stream.maintain(true).await.unwrap();
        }
        let orphan = store.journal.data_path(10000);
        std::fs::create_dir_all(orphan.parent().unwrap()).unwrap();
        std::fs::write(&orphan, b"retain on validation failure").unwrap();
        drop(store);
        let error = open(dir.path())
            .err()
            .expect("duplicate must fail recovery");
        assert!(error.to_string().contains("duplicate"));
        assert_eq!(
            std::fs::read(orphan).unwrap(),
            b"retain on validation failure"
        );
    }

    #[tokio::test]
    async fn length_mismatch_fails_before_orphan_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path()).unwrap();
        let stream = store.writer(1).await.unwrap();
        stream
            .put(&id(0), Bytes::from_static(b"data"))
            .await
            .unwrap();
        stream.maintain(true).await.unwrap();
        let path = {
            let state = stream.state.lock().await;
            stream.path(*state.segments.keys().next().unwrap(), "data")
        };
        let orphan = store.journal.data_path(10000);
        std::fs::create_dir_all(orphan.parent().unwrap()).unwrap();
        std::fs::write(&orphan, b"retain").unwrap();
        drop(stream);
        drop(store);
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_len(3)
            .unwrap();
        assert!(open(dir.path())
            .err()
            .unwrap()
            .to_string()
            .contains("length"));
        assert_eq!(std::fs::read(orphan).unwrap(), b"retain");
    }

    #[tokio::test]
    async fn recovered_exact_policy_preserves_pins_and_versions_with_optional_block_tracking() {
        for tracking in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut block = id(0);
            block.block_size = 4096;
            let exact = CacheUnit::ExactRange(block.clone());
            let page = CacheUnit::Page(block.clone(), talon_core::PageIndex(0));
            let store = Arc::new(open(dir.path()).unwrap());
            store.put(&exact, Bytes::from(vec![1; 4096])).await.unwrap();
            store.maintain(true).await.unwrap();
            drop(store);

            let store = Arc::new(open(dir.path()).unwrap());
            let index = Arc::new(crate::BlockIndex::new());
            index.init_paged(block.clone(), 4096, 4096);
            index.mark_page_allocated(&block, talon_core::PageIndex(0), 4096);
            let mut units = index.restore_policy_units(tracking);
            store.restore_policy_units(&index, &mut units, true);
            let policy = Arc::new(crate::Lru::from_recovered(
                index.clone(),
                units,
                1,
                tracking,
            ));
            store.bind_policy(policy.clone());
            store.enable_access_tracking().unwrap();
            let access = store.access(&exact).unwrap();
            access.restore(1);
            assert_eq!(access.last_access(), 1);
            assert_eq!(policy.len(), 2);
            assert_eq!(index.page_count(), 1);
            assert_eq!(index.resident_bytes(), 4096);

            let pin = policy.pin_guard(exact.clone());
            let mut newer = block.clone();
            newer.version = Version::new("v2");
            assert_eq!(policy.evict_superseded(&newer), [page]);
            assert!(policy.superseded_candidates(&newer).is_empty());
            assert!(policy.evict_to_fit(0).is_empty());
            assert_eq!(policy.len(), 1);
            drop(pin);
            assert_eq!(policy.evict_to_fit(0), [exact]);
            assert_eq!(policy.total_bytes(), 0);
            assert!(policy.block_candidates(&block).is_empty());
        }
    }

    #[tokio::test]
    async fn recovered_rings_bulk_policy_matches_serial_and_remains_writable() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path()).unwrap();
        for slot in [1, 2] {
            let stream = store.writer(slot).await.unwrap();
            for n in 0..1025 {
                stream
                    .put(&id(slot * 1025 + n), Bytes::from_static(b"data"))
                    .await
                    .unwrap();
            }
            stream.maintain(true).await.unwrap();
        }
        drop(store);
        let store = Arc::new(open(dir.path()).unwrap());
        let mut units = Vec::new();
        let index = Arc::new(crate::BlockIndex::new());
        store.restore_policy_units(&index, &mut units, true);
        let serial = crate::Lru::new();
        for unit in &units {
            serial.insert_with_policy(unit.unit.materialize(), unit.bytes, true);
        }
        let policy = Arc::new(crate::Lru::from_recovered(index, units, store.len(), true));
        store.bind_policy(policy.clone());
        assert_eq!(policy.len(), 2050);
        assert_eq!(policy.total_bytes(), serial.total_bytes());
        for n in 1025..3075 {
            assert!(store.contains(&id(n)));
            assert!(store.access(&id(n)).is_some());
            assert_eq!(policy.block_candidates(&id(n)).len(), 1);
        }
        assert_eq!(
            store.read(&id(1025)).await.unwrap().unwrap(),
            Bytes::from_static(b"data")
        );
        store.delete(&id(1025)).await.unwrap();
        store.put(&id(0), Bytes::from_static(b"new")).await.unwrap();
        store.maintain(true).await.unwrap();
        drop(store);
        let store = open(dir.path()).unwrap();
        assert!(!store.contains(&id(1025)));
        assert_eq!(
            store.read(&id(0)).await.unwrap().unwrap(),
            Bytes::from_static(b"new")
        );
    }

    #[test]
    fn empty_store_validates_geometry_before_creating_streams() {
        let dir = tempfile::tempdir().unwrap();
        for (threshold, segment_size, capacity) in [
            (128 << 10, 128 << 10, 4 << 20),
            (128 << 10, 256 << 10, 512 << 10),
        ] {
            let error = SmallObjectStore::open(
                dir.path().join("small"),
                threshold,
                segment_size,
                capacity,
                None,
            )
            .err()
            .expect("invalid geometry must fail even without existing streams");
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        }
    }
}
