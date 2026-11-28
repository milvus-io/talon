//! Reclaim cold local cache entries only after an authoritative backend NotFound.
use super::WorkerRuntime;
use crate::block_key::ObjectRef;
use crate::eviction::{access_millis, StaleBlockCandidate};
use crate::small_store::{SmallGcCandidate, SmallStaleCursor};
use futures::StreamExt;
use std::{
    borrow::Borrow,
    collections::{HashMap, VecDeque},
    hash::{Hash, Hasher},
    sync::{Arc, RwLock},
    time::Duration,
};
use talon_core::backend::BackgroundRoute;
use talon_core::{BackendStore, Error, ObjectId, WorkerConfig};

const STALE_IDLE_MS: u64 = 25 * 60 * 60 * 1000;
const POSITIVE_HEAD_COOLDOWN_MS: u64 = 24 * 60 * 60 * 1000;
const MAX_HEADS: usize = 32;

enum Candidate {
    Block(StaleBlockCandidate),
    Small(SmallGcCandidate, u64),
}

impl Candidate {
    fn object(&self) -> &ObjectId {
        match self {
            Self::Block(candidate) => &candidate.block.object,
            Self::Small(candidate, _) => &candidate.unit.block().object,
        }
    }

    fn units(&self) -> u64 {
        match self {
            Self::Block(candidate) => candidate.units as u64,
            Self::Small(_, _) => 1,
        }
    }

    fn is_current(&self, worker: &WorkerRuntime) -> bool {
        match self {
            Self::Block(candidate) => worker.lru.stale_candidate_is_current(candidate),
            Self::Small(candidate, revision) => candidate.ticket.gc_is_current(*revision),
        }
    }
}

trait ObjectIdentity {
    fn view(&self) -> ObjectRef<'_>;
}

impl ObjectIdentity for ObjectId {
    fn view(&self) -> ObjectRef<'_> {
        ObjectRef {
            backend: self.backend,
            bucket: &self.bucket,
            object_path: &self.object_path,
        }
    }
}

impl ObjectIdentity for ObjectRef<'_> {
    fn view(&self) -> ObjectRef<'_> {
        *self
    }
}

impl Hash for dyn ObjectIdentity + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.view().hash(state);
    }
}

impl PartialEq for dyn ObjectIdentity + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.view() == other.view()
    }
}

impl Eq for dyn ObjectIdentity + '_ {}

#[derive(Clone, Hash, PartialEq, Eq)]
struct CheckedObject(Arc<ObjectId>);

impl<'a> Borrow<dyn ObjectIdentity + 'a> for CheckedObject {
    fn borrow(&self) -> &(dyn ObjectIdentity + 'a) {
        self.0.as_ref()
    }
}

type HeadTimes = HashMap<CheckedObject, u64>;

#[derive(Default)]
struct PositiveHeads {
    checked: Arc<RwLock<HeadTimes>>,
    expiry: VecDeque<(u64, CheckedObject)>,
}

fn head_due(checked: &HeadTimes, object: ObjectRef<'_>, now: u64) -> bool {
    checked
        .get(&object as &dyn ObjectIdentity)
        .map_or(true, |at| {
            now.saturating_sub(*at) >= POSITIVE_HEAD_COOLDOWN_MS
        })
}

impl PositiveHeads {
    fn record(&mut self, object: ObjectId, now: u64) {
        let object = CheckedObject(Arc::new(object));
        self.checked.write().unwrap().insert(object.clone(), now);
        self.expiry.push_back((now, object));
    }

    /// Returns true when the next slice has more expired records to release.
    fn prune(&mut self, now: u64) -> bool {
        let mut budget = 256;
        let mut checked = self.checked.write().unwrap();
        while let Some((at, _)) = self.expiry.front() {
            if now.saturating_sub(*at) < POSITIVE_HEAD_COOLDOWN_MS {
                return false;
            }
            if budget == 0 {
                return true;
            }
            budget -= 1;
            let (at, object) = self.expiry.pop_front().unwrap();
            if checked.get(&object) == Some(&at) {
                checked.remove(&object);
            }
        }
        drop(checked);
        if self.expiry.capacity() != 0 {
            *self = Self::default();
        }
        false
    }
}

pub(super) struct StaleGc {
    cursor: (usize, usize),
    blocks_done: bool,
    small_cursor: SmallStaleCursor,
    small_done: bool,
    pending: VecDeque<Candidate>,
    small_pending: VecDeque<Candidate>,
    prefer_small: bool,
    round_now: Option<u64>,
    missing: VecDeque<(Candidate, u64)>,
    positive_heads: PositiveHeads,
    heads: Arc<crate::background::Rate>,
    concurrency: usize,
    head_batch: usize,
}
impl Default for StaleGc {
    fn default() -> Self {
        Self::new(&WorkerConfig::default())
    }
}
impl StaleGc {
    pub(super) fn new(config: &WorkerConfig) -> Self {
        Self::with_head_rate(
            config,
            Arc::new(crate::background::Rate::new(
                config.stale_gc_head_max_per_sec,
            )),
        )
    }

    pub(super) fn with_head_rate(
        config: &WorkerConfig,
        heads: Arc<crate::background::Rate>,
    ) -> Self {
        Self {
            cursor: (0, 0),
            blocks_done: false,
            small_cursor: SmallStaleCursor::default(),
            small_done: false,
            positive_heads: PositiveHeads::default(),
            pending: VecDeque::new(),
            small_pending: VecDeque::new(),
            prefer_small: false,
            round_now: None,
            missing: VecDeque::new(),
            heads,
            concurrency: config.stale_gc_head_concurrency,
            head_batch: config.stale_gc_head_concurrency.min(MAX_HEADS).min(
                if config.stale_gc_head_max_per_sec == 0 {
                    MAX_HEADS
                } else {
                    config.stale_gc_head_max_per_sec.min(MAX_HEADS as u64) as usize
                },
            ),
        }
    }

    pub(super) async fn run_slice(&mut self, worker: &WorkerRuntime) -> Option<Duration> {
        self.run_slice_with_clock(worker, access_millis).await
    }

    async fn run_slice_with_clock(
        &mut self,
        worker: &WorkerRuntime,
        clock: impl Fn() -> u64,
    ) -> Option<Duration> {
        if self.positive_heads.prune(clock()) {
            return Some(Duration::ZERO);
        }
        // A recovered credential scope waits for a foreground request to bind it.
        if !worker.backend.get()?.ready_for_background() {
            return None;
        }
        let round_now = *self.round_now.get_or_insert_with(&clock);
        if self.missing.is_empty() {
            self.check_batch(worker, round_now, clock).await;
        }
        self.delete_slice(worker).await;
        let done = self.finish_round_if_done();
        Some(if done {
            Duration::from_secs(3600)
        } else {
            Duration::ZERO
        })
    }

    fn finish_round_if_done(&mut self) -> bool {
        let done = self.blocks_done
            && self.small_done
            && self.pending.is_empty()
            && self.small_pending.is_empty()
            && self.missing.is_empty();
        if done {
            self.round_now = None;
            self.blocks_done = false;
            self.small_done = false;
            self.small_cursor = SmallStaleCursor::default();
            self.prefer_small = false;
        }
        done
    }

    #[cfg(test)]
    async fn run_round_at(&mut self, worker: &WorkerRuntime, now: u64) {
        while !self.tick_at(worker, now).await {
            tokio::task::yield_now().await;
        }
    }

    #[cfg(test)]
    async fn tick_at(&mut self, worker: &WorkerRuntime, now: u64) -> bool {
        if self.positive_heads.prune(now) {
            return false;
        }
        self.check_batch(worker, now, || now).await;
        while !self.missing.is_empty() {
            self.delete_slice(worker).await;
        }
        self.finish_round_if_done()
    }

    async fn delete_slice(&mut self, worker: &WorkerRuntime) {
        let Some((candidate, paid)) = self.missing.front_mut() else {
            return;
        };
        if !candidate.is_current(worker) {
            self.missing.pop_front();
            return;
        }
        if let Some(budget) = &worker.background_budget {
            *paid += budget.delete_slice(candidate.units() - *paid).await;
        } else {
            *paid = candidate.units();
        }
        if *paid == candidate.units() {
            let (candidate, _) = self.missing.pop_front().unwrap();
            match candidate {
                Candidate::Block(candidate) => worker.unlink_stale_block_prepaid(candidate).await,
                Candidate::Small(candidate, revision) => {
                    worker.unlink_small_gc(candidate, revision, None).await
                }
            }
        }
    }

    async fn check_batch(
        &mut self,
        worker: &WorkerRuntime,
        round_now: u64,
        clock: impl Fn() -> u64,
    ) {
        let Some(backend) = worker.backend.get() else {
            return;
        };
        if !backend.ready_for_background() {
            return;
        }
        let compact = worker.small.as_ref().filter(|small| small.has_recency());
        if compact.is_none() {
            self.small_done = true;
        }
        let has_blocks = !self.blocks_done || !self.pending.is_empty();
        let has_small = !self.small_done || !self.small_pending.is_empty();
        if !has_blocks && !has_small {
            return;
        }
        let scan_small = has_small && (self.prefer_small || !has_blocks);
        self.prefer_small = !scan_small;

        let (scan, deletes) = worker.maintenance_batch_limits();
        let scan = scan.min(4096);
        let deletes = deletes.min(256);
        // Keep the round's idle cutoff, but evaluate positive HEAD expiry in
        // real time, both before materializing candidates and before using them.
        let now = clock();
        let checked = self.positive_heads.checked.clone();
        let pending = if scan_small {
            if self.small_pending.is_empty() {
                let small = compact.expect("compact small store").clone();
                let mut cursor = self.small_cursor;
                let (cursor, done, candidates) = tokio::task::spawn_blocking(move || {
                    let checked = checked.read().unwrap();
                    let (done, candidates) = small.stale_candidates(
                        &mut cursor,
                        round_now,
                        STALE_IDLE_MS,
                        scan,
                        deletes,
                        |object| head_due(&checked, object, now),
                    );
                    (cursor, done, candidates)
                })
                .await
                .expect("stale small GC scan panicked");
                self.small_cursor = cursor;
                self.small_done = done;
                self.small_pending.extend(
                    candidates
                        .into_iter()
                        .map(|(candidate, revision)| Candidate::Small(candidate, revision)),
                );
            }
            &mut self.small_pending
        } else {
            if self.pending.is_empty() {
                let lru = worker.lru.clone();
                let mut cursor = self.cursor;
                let (cursor, candidates) = tokio::task::spawn_blocking(move || {
                    let checked = checked.read().unwrap();
                    let candidates = lru.filtered_idle_candidates(
                        &mut cursor,
                        round_now,
                        STALE_IDLE_MS,
                        scan,
                        deletes,
                        |object| head_due(&checked, object, now),
                    );
                    (cursor, candidates)
                })
                .await
                .expect("stale block GC scan panicked");
                self.cursor = cursor;
                self.blocks_done = cursor.0 >= cursor.1;
                self.pending
                    .extend(candidates.into_iter().map(Candidate::Block));
            }
            &mut self.pending
        };
        let now = clock();
        let mut routes = HashMap::new();
        let mut groups: HashMap<ObjectId, (Arc<dyn BackendStore>, Vec<Candidate>)> = HashMap::new();
        {
            let checked = self.positive_heads.checked.read().unwrap();
            while let Some(candidate) = pending.front() {
                if !groups.contains_key(candidate.object()) && groups.len() == self.head_batch {
                    break;
                }
                let candidate = pending.pop_front().unwrap();
                if head_due(&checked, candidate.object().view(), now)
                    && candidate.is_current(worker)
                {
                    let object = candidate.object();
                    // Decide once per bucket for this batch before queuing HEADs.
                    let store = routes
                        .entry((object.backend, object.bucket.clone()))
                        .or_insert_with(|| {
                            match backend.background_route(object.backend, &object.bucket) {
                                BackgroundRoute::Current => Some(backend.clone()),
                                BackgroundRoute::Bound(store) => Some(store),
                                BackgroundRoute::Skip => None,
                            }
                        });
                    if let Some(store) = store {
                        groups
                            .entry(object.clone())
                            .or_insert_with(|| (store.clone(), Vec::new()))
                            .1
                            .push(candidate);
                    }
                }
            }
        }
        let heads = &self.heads;
        let mut work = Vec::with_capacity(groups.len());
        for (object, (store, candidates)) in groups {
            work.push(async move {
                // Origin QPS is independent of local disk pacing and scan slicing.
                heads.charge(1).await;
                let result = tokio::time::timeout(
                    Duration::from_secs(5),
                    worker.background_head(store, &object),
                )
                .await;
                (object, candidates, result)
            });
        }
        let mut work = futures::stream::iter(work).buffer_unordered(self.concurrency);
        while let Some((object, candidates, result)) = work.next().await {
            match result {
                Ok(Ok(_)) => {
                    self.positive_heads.record(object, clock());
                }
                Ok(Err(Error::NotFound(_))) => {
                    self.missing
                        .extend(candidates.into_iter().map(|candidate| (candidate, 0)));
                }
                Ok(Err(error)) => {
                    tracing::debug!(%error, "stale GC HEAD failed; retaining cache");
                }
                Err(_) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Worker;
    use crate::{
        BlockIndex, CacheUnit, InFlightLoads, PagedBlockStore, WholeBlockStore, WorkerMetrics,
    };
    use bytes::Bytes;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };
    use talon_core::{Backend, BackendStore, BlockId, ObjectId, ObjectStat, PageIndex, Version};

    #[derive(Default)]
    struct Origin {
        calls: AtomicUsize,
        objects: std::sync::Mutex<Vec<ObjectId>>,
        mode: AtomicUsize, // 0 missing, 1 exists, 2 forbidden, 3 timeout
        hold: AtomicBool,
        unavailable: AtomicBool,
        unavailable_bucket: Option<&'static str>,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }
    #[async_trait::async_trait]
    impl BackendStore for Origin {
        fn ready_for_background(&self) -> bool {
            !self.unavailable.load(Ordering::Relaxed)
        }

        fn background_route(&self, _: Backend, bucket: &str) -> BackgroundRoute {
            if self.ready_for_background() && self.unavailable_bucket != Some(bucket) {
                BackgroundRoute::Current
            } else {
                BackgroundRoute::Skip
            }
        }

        async fn fetch_range(&self, _: &ObjectId, _: u64, _: u64) -> talon_core::Result<Bytes> {
            panic!("GC must not fetch data")
        }
        async fn head(&self, object: &ObjectId) -> talon_core::Result<ObjectStat> {
            self.objects.lock().unwrap().push(object.clone());
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.entered.notify_one();
            if self.hold.load(Ordering::Relaxed) {
                self.release.notified().await;
            }
            match self.mode.load(Ordering::Relaxed) {
                0 => Err(Error::NotFound(object.to_path())),
                1 => Ok(ObjectStat {
                    len: 32,
                    version: Version::new("new-version"),
                }),
                2 => Err(Error::Backend("403 Forbidden".into())),
                _ => std::future::pending().await,
            }
        }
    }
    fn block(n: u64) -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "bucket", "key"),
            n * 32,
            32,
            Version::new("v1"),
        )
    }
    fn runtime(
        root: &std::path::Path,
        paged: bool,
        origin: Arc<dyn BackendStore>,
    ) -> WorkerRuntime {
        let store = WholeBlockStore::open(root.join("whole")).unwrap();
        let index = Arc::new(BlockIndex::new());
        for entry in store.scan_allocated().unwrap() {
            index.commit_stored(entry);
        }
        WorkerRuntime::new_with_l1(
            store,
            paged.then(|| PagedBlockStore::open(root.join("paged"), 16).unwrap()),
            index,
            Arc::new(InFlightLoads::new()),
            origin,
            32,
            0,
            0,
            0,
            WorkerMetrics::new(0),
        )
    }
    async fn fill(r: &WorkerRuntime, id: &BlockId, paged: bool) {
        if paged {
            for page in 0..2 {
                r.commit_fetched_page(id, PageIndex(page), 32, Bytes::from_static(&[1; 16]))
                    .await
                    .unwrap();
            }
        } else {
            r.commit_cached_block(id, Bytes::from_static(&[1; 32]))
                .await
                .unwrap();
        }
    }
    fn cold_now() -> u64 {
        access_millis() + STALE_IDLE_MS + 1
    }

    fn compact_runtime(root: &std::path::Path, paged: bool, origin: Arc<Origin>) -> Worker {
        compact_runtime_with_config(root, paged, origin, |_| {})
    }

    fn compact_runtime_with_config(
        root: &std::path::Path,
        paged: bool,
        origin: Arc<Origin>,
        configure: impl FnOnce(&mut WorkerConfig),
    ) -> Worker {
        let mut config = WorkerConfig {
            backend: Some("s3".into()),
            block_size: 32,
            l1_page_size_bytes: 16,
            l2_page_size_bytes: if paged { 16 } else { 0 },
            page_tti_hours: 2,
            ..Default::default()
        };
        configure(&mut config);
        assert!(config.stale_gc_enabled && config.ownership_gc_enabled);
        let lock = Arc::new(crate::page_access_store::CacheRootLock::acquire(root).unwrap());
        Worker::open_namespaces(&config, lock, origin, WorkerMetrics::new(0)).unwrap()
    }

    #[tokio::test]
    async fn compact_only_restart_allocates_no_ordinary_policy_or_head_checks() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let count = 128;
        {
            let worker = compact_runtime(root.path(), false, origin.clone());
            for n in 0..count {
                let mut id = block(0);
                id.object.object_path = format!("packed-{n}");
                worker
                    .commit_cached_block(&id, Bytes::from_static(b"packed"))
                    .await
                    .unwrap();
            }
            worker.small.as_ref().unwrap().maintain(true).await.unwrap();
        }

        let worker = compact_runtime(root.path(), false, origin);
        assert_eq!(worker.block_count(), count);
        assert_eq!(worker.lru.len(), count as usize);
        assert!(worker.index.is_empty());
        worker.lru.assert_empty_ordinary_policy();
        worker
            .small
            .as_ref()
            .unwrap()
            .assert_no_resident_policy_allocations();
        let gc = StaleGc::default();
        assert_eq!(gc.positive_heads.checked.read().unwrap().capacity(), 0);
        assert_eq!(gc.positive_heads.expiry.capacity(), 0);
    }

    #[tokio::test]
    async fn compact_stale_gc_preserves_the_pinned_exact_range_of_an_ordinary_block() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            let worker = compact_runtime(root.path(), paged, origin.clone());
            let small = worker.small.as_ref().unwrap();
            let ordinary = block(0);
            let exact = CacheUnit::ExactRange(ordinary.clone());
            let mut whole = ordinary.clone();
            // Exercise the same backend object and offset across versions.
            whole.version = Version::new("v2");
            worker
                .commit_cached_block(&whole, Bytes::from_static(b"packed"))
                .await
                .unwrap();
            fill(&worker, &ordinary, paged).await;
            small
                .put(&exact, Bytes::from_static(&[2; 32]))
                .await
                .unwrap();
            assert_eq!(
                worker.index.policy_block_get(&ordinary).unwrap().units,
                if paged { 2 } else { 1 }
            );
            assert!(worker.index.policy_block_get(&whole).is_none());
            let pin = worker.lru.pin_guard(exact.clone()).unwrap();
            let mut gc = StaleGc::default();

            gc.run_round_at(&worker, access_millis()).await;
            assert_eq!(origin.calls.load(Ordering::Relaxed), 0);
            gc.run_round_at(&worker, cold_now()).await;
            assert!(worker.index.get(&ordinary).is_none());
            assert!(!small.contains(&whole));
            assert!(small.contains(&exact));
            assert_eq!(
                small.read(&exact).await.unwrap(),
                Some(Bytes::from_static(&[2; 32]))
            );
            drop(pin);
            gc.run_round_at(&worker, cold_now()).await;
            assert!(!small.contains(&exact));
            assert_eq!(worker.logical_resident_bytes(), 0);
            assert_eq!(worker.lru.total_bytes(), 0);
        }
    }

    #[tokio::test]
    async fn compact_stale_positive_head_spans_versions_ranges_and_does_not_refresh_tti() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.mode.store(1, Ordering::Relaxed);
        let worker = compact_runtime_with_config(root.path(), false, origin.clone(), |config| {
            config.background_scan_batch_size = 1;
            config.background_delete_batch_size = 1;
        });
        let small = worker.small.as_ref().unwrap();
        let whole = block(0);
        let mut other_version = block(1);
        other_version.version = Version::new("v2");
        let exact = CacheUnit::ExactRange(other_version);
        worker
            .commit_cached_block(&whole, Bytes::from_static(b"packed"))
            .await
            .unwrap();
        small
            .put(&exact, Bytes::from_static(&[2; 32]))
            .await
            .unwrap();
        let mut gc = StaleGc::default();
        let now = access_millis();
        let original = now - STALE_IDLE_MS - 1;
        small.packed_access_time(&whole, Some(original));
        small.packed_access_time(&exact, Some(original));
        assert!(gc.positive_heads.checked.read().unwrap().is_empty());
        gc.run_round_at(&worker, now).await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), 1);
        assert_eq!(gc.positive_heads.checked.read().unwrap().len(), 1);
        assert_eq!(small.packed_access_time(&whole, None), original);
        assert_eq!(small.packed_access_time(&exact, None), original);
        origin.mode.store(0, Ordering::Relaxed);
        gc.run_round_at(&worker, now + POSITIVE_HEAD_COOLDOWN_MS - 1)
            .await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), 1);
        assert!(small.contains(&exact));

        // TTI is shorter and reads its own unaltered Small Store timestamp.
        worker.gc_small_idle_at(now).await;
        assert!(!small.contains(&whole));
        gc.run_round_at(&worker, now + POSITIVE_HEAD_COOLDOWN_MS)
            .await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), 2);
        assert!(!small.contains(&exact));
        assert!(gc.positive_heads.checked.read().unwrap().is_empty());
    }

    #[tokio::test]
    async fn ordinary_positive_head_survives_tti_eviction_for_a_packed_sibling() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.mode.store(1, Ordering::Relaxed);
        let worker = compact_runtime(root.path(), false, origin.clone());
        let ordinary = block(0);
        fill(&worker, &ordinary, false).await;
        let small = worker.small.as_ref().unwrap();
        let exact = CacheUnit::ExactRange(ordinary.clone());
        small
            .put(&exact, Bytes::from_static(&[2; 32]))
            .await
            .unwrap();

        let mut gc = StaleGc::default();
        let now = cold_now();
        assert!(!gc.tick_at(&worker, now).await);
        assert_eq!(origin.calls.load(Ordering::Relaxed), 1);
        assert!(gc.blocks_done && !gc.small_done);

        let candidate = worker
            .lru
            .idle_candidates(&mut (0, 0), now, STALE_IDLE_MS, 8, 8)
            .pop()
            .unwrap();
        worker.unlink_idle_block(candidate, now).await;
        assert!(worker.index.get(&ordinary).is_none());
        assert!(small.contains(&exact));

        origin.mode.store(0, Ordering::Relaxed);
        gc.run_round_at(&worker, now + POSITIVE_HEAD_COOLDOWN_MS - 1)
            .await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), 1);
        assert!(small.contains(&exact));
        gc.run_round_at(&worker, now + POSITIVE_HEAD_COOLDOWN_MS)
            .await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), 2);
        assert!(!small.contains(&exact));
    }

    #[tokio::test]
    async fn empty_cache_drains_expired_heads_in_continuous_bounded_slices() {
        for backend_ready in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            if !backend_ready {
                let scope = format!("v1-{}", "a".repeat(64));
                std::fs::create_dir_all(root.path().join("pending/scopes").join(scope)).unwrap();
            }
            let process = compact_runtime(root.path(), false, origin.clone());
            let worker = if backend_ready {
                process.default_runtime().clone()
            } else {
                process
                    .namespace_runtimes()
                    .into_iter()
                    .find(|runtime| runtime.backend.get().is_none())
                    .unwrap()
            };
            let mut gc = StaleGc::default();
            for row in 0..1025 {
                gc.positive_heads
                    .record(ObjectId::new(Backend::S3, "bucket", row.to_string()), 1);
            }
            for left in [769, 513, 257, 1] {
                assert_eq!(
                    gc.run_slice_with_clock(&worker, || POSITIVE_HEAD_COOLDOWN_MS + 1)
                        .await,
                    Some(Duration::ZERO)
                );
                assert_eq!(gc.positive_heads.checked.read().unwrap().len(), left);
                assert!(gc.round_now.is_none());
            }
            let delay = gc
                .run_slice_with_clock(&worker, || POSITIVE_HEAD_COOLDOWN_MS + 1)
                .await;
            assert_eq!(gc.positive_heads.checked.read().unwrap().capacity(), 0);
            assert_eq!(gc.positive_heads.expiry.capacity(), 0);
            if backend_ready {
                assert_eq!(delay, Some(Duration::ZERO));
                assert_eq!(
                    gc.run_slice_with_clock(&worker, || POSITIVE_HEAD_COOLDOWN_MS + 1)
                        .await,
                    Some(Duration::from_secs(3600))
                );
            } else {
                assert!(delay.is_none());
            }
            assert_eq!(origin.calls.load(Ordering::Relaxed), 0);
        }
    }

    #[tokio::test]
    async fn stale_gc_alternates_mixed_queues_while_ordinary_heads_remain_pending() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.mode.store(2, Ordering::Relaxed);
        let worker = compact_runtime(root.path(), false, origin.clone());
        let mut packed = block(0);
        packed.object.object_path = "earlier-packed".into();
        worker
            .small
            .as_ref()
            .unwrap()
            .put(&packed, Bytes::from_static(b"packed"))
            .await
            .unwrap();
        for n in 0..12 {
            let mut ordinary = block(0);
            ordinary.object.object_path = format!("ordinary-{n}");
            fill(&worker, &ordinary, false).await;
        }
        let start = cold_now() + 1000;
        let mut gc = StaleGc::new(&WorkerConfig {
            stale_gc_head_concurrency: 1,
            ..Default::default()
        });
        assert!(!gc.tick_at(&worker, start).await);
        assert!(!gc.tick_at(&worker, start).await);
        {
            let heads = origin.objects.lock().unwrap();
            assert_eq!(heads.len(), 2);
            assert_eq!(heads[0].object_path, "ordinary-0");
            assert_eq!(heads[1], packed.object);
        }
        gc.run_round_at(&worker, start).await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), 13);
    }

    #[tokio::test]
    async fn cached_positive_heads_do_not_consume_candidate_caps_or_arm_packed_watches() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.mode.store(2, Ordering::Relaxed);
        let worker = compact_runtime_with_config(root.path(), false, origin.clone(), |config| {
            config.background_delete_batch_size = 1;
        });
        let small = worker.small.as_ref().unwrap();
        let cooled = ObjectId::new(Backend::S3, "bucket", "cooled\0☃");
        for n in 0..8 {
            let mut id = block(n);
            id.object = cooled.clone();
            fill(&worker, &id, false).await;
            small
                .put(&CacheUnit::ExactRange(id), Bytes::from_static(&[2; 32]))
                .await
                .unwrap();
        }
        let mut due_block = block(0);
        due_block.object.object_path = "ordinary-due".into();
        fill(&worker, &due_block, false).await;
        let mut due_small = block(0);
        due_small.object.object_path = "packed-due".into();
        small
            .put(&due_small, Bytes::from_static(b"packed"))
            .await
            .unwrap();

        let now = cold_now() + 1000;
        let mut gc = StaleGc::default();
        gc.positive_heads.record(cooled, now - 1);
        let revision = small.catalog_revision();
        assert!(!gc.tick_at(&worker, now).await);
        gc.tick_at(&worker, now).await;
        assert_eq!(
            *origin.objects.lock().unwrap(),
            [due_block.object, due_small.object],
        );
        assert_eq!(
            small.catalog_revision() - revision,
            2,
            "one ticket and one watch"
        );
    }

    #[tokio::test]
    async fn positive_head_expires_during_a_stale_round_with_a_fixed_idle_cutoff() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.mode.store(1, Ordering::Relaxed);
        let worker = compact_runtime_with_config(root.path(), false, origin.clone(), |config| {
            config.background_scan_batch_size = 1;
        });
        let ordinary = block(0);
        fill(&worker, &ordinary, false).await;
        let exact = CacheUnit::ExactRange(ordinary.clone());
        let small = worker.small.as_ref().unwrap();
        small
            .put(&exact, Bytes::from_static(&[2; 32]))
            .await
            .unwrap();

        let mut warm = block(0);
        warm.object.object_path = "warm".into();
        fill(&worker, &warm, false).await;
        let start = cold_now();
        worker.index.set_policy_clock(&warm, start - STALE_IDLE_MS);

        let mut gc = StaleGc::default();
        assert_eq!(
            gc.run_slice_with_clock(&worker, || start).await,
            Some(Duration::ZERO)
        );
        assert_eq!(origin.calls.load(Ordering::Relaxed), 1);
        assert_eq!(gc.positive_heads.checked.read().unwrap().len(), 1);

        origin.mode.store(0, Ordering::Relaxed);
        let later = start + POSITIVE_HEAD_COOLDOWN_MS + 1;
        assert_eq!(
            gc.run_slice_with_clock(&worker, || later).await,
            Some(Duration::ZERO)
        );
        assert_eq!(gc.round_now, Some(start));
        assert!(gc.positive_heads.checked.read().unwrap().is_empty());
        assert!(gc.positive_heads.expiry.is_empty());
        assert_eq!(origin.calls.load(Ordering::Relaxed), 2);
        assert!(!small.contains(&exact));

        for _ in 0..8 {
            if gc.run_slice_with_clock(&worker, || later).await != Some(Duration::ZERO) {
                break;
            }
        }
        assert!(gc.round_now.is_none());
        assert_eq!(origin.calls.load(Ordering::Relaxed), 2);
        assert!(!small.contains(&exact));
        assert!(worker.index.get(&warm).is_some());
    }

    #[tokio::test]
    async fn compact_stale_revalidates_direct_reads_during_head_and_survives_relocation() {
        for read_after_head in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            let worker = compact_runtime(root.path(), false, origin.clone());
            let small = worker.small.as_ref().unwrap();
            let live = block(0);
            worker
                .commit_cached_block(&live, Bytes::from_static(b"packed"))
                .await
                .unwrap();
            for n in 0..8 {
                let mut garbage = block(0);
                garbage.object.object_path = format!("garbage-{n}");
                worker
                    .commit_cached_block(&garbage, Bytes::from_static(b"packed"))
                    .await
                    .unwrap();
                small.delete(&garbage).await.unwrap();
                worker.lru.remove(&CacheUnit::Whole(garbage));
            }
            let before = small.lookup_entry(&live).unwrap().unwrap().segment();
            let mut gc = StaleGc::default();
            origin.hold.store(true, Ordering::Relaxed);
            let collect = gc.run_round_at(&worker, cold_now());
            let relocate = async {
                origin.entered.notified().await;
                assert!(small.maintain(true).await.unwrap());
                assert_ne!(
                    small.lookup_entry(&live).unwrap().unwrap().segment(),
                    before
                );
                if read_after_head {
                    assert_eq!(
                        small.read(&live).await.unwrap(),
                        Some(Bytes::from_static(b"packed"))
                    );
                }
                origin.release.notify_one();
            };
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::join!(collect, relocate);
            })
            .await
            .unwrap();
            assert_eq!(small.contains(&live), read_after_head);
            assert_eq!(origin.calls.load(Ordering::Relaxed), 1);
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires native io_uring; exercises Tokio GC with a real S3 HTTP backend"]
    async fn native_stale_gc_uses_existing_rings_and_cancels_http_on_timeout_and_shutdown() {
        use crate::{ConnectionAdmission, WorkerObservability};
        use talon_backend::{monoio_client::MonoioClient, S3Backend, S3Config, S3Credentials};
        use talon_core::{NodeId, NodeInfo, NodeRole};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let status = Arc::new(AtomicUsize::new(403));
        let response_status = status.clone();
        let (events, mut observed) = tokio::sync::mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let status = response_status.clone();
                let events = events.clone();
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        request.push(socket.read_u8().await.unwrap());
                    }
                    assert!(request.starts_with(b"HEAD /bucket/key "));
                    let status = status.load(Ordering::SeqCst);
                    events.send("head").unwrap();
                    match status {
                        0 => {}, // transport failure, not an authoritative NotFound
                        1 => {
                            assert!(socket.read_u8().await.is_err());
                            events.send("cancelled").unwrap();
                        },
                        _ => socket.write_all(format!(
                            "HTTP/1.1 {status} Response\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        ).as_bytes()).await.unwrap(),
                    }
                });
            }
        });
        let backend = Arc::new(S3Backend::new(
            S3Config {
                endpoint,
                region: "us-east-1".into(),
                path_style: true,
                tls: false,
            },
            S3Credentials {
                access_key_id: "test".into(),
                secret_access_key: "test".into(),
                session_token: None,
            },
            Arc::new(MonoioClient::new()),
        ));
        let root = tempfile::tempdir().unwrap();
        let mut worker = runtime(root.path(), false, backend);
        worker.background_heads = Some(super::super::origin_head::OriginHeads::new(2));
        fill(&worker, &block(0), false).await;
        let worker = Arc::new(worker);
        let obs = Arc::new(
            WorkerObservability::new(
                "test".into(),
                NodeInfo {
                    id: NodeId::new("worker"),
                    address: "127.0.0.1:0".into(),
                    role: NodeRole::Worker,
                },
                "127.0.0.1:0".into(),
                1024,
                worker.index.clone(),
                worker.inflight.clone(),
            )
            .unwrap(),
        );
        let handler =
            crate::uring_conn::RingConnHandler::new(Worker::standalone(worker.clone()), obs);
        let stop = Arc::new(AtomicBool::new(false));
        // Stop even if an assertion unwinds, so a failed test cannot leave rings running.
        struct Stop(Arc<AtomicBool>);
        impl Drop for Stop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let _stop = Stop(stop.clone());
        let stop_rings = stop.clone();
        let tokio = tokio::runtime::Handle::current();
        let rings = tokio::task::spawn_blocking(move || {
            crate::uring_serve::serve_with_shutdown(
                "127.0.0.1:0".into(),
                2,
                ConnectionAdmission::new(8, WorkerMetrics::new(0)),
                handler,
                tokio,
                stop_rings,
            )
        });
        assert!(!talon_uring::on_ring());
        for code in [403, 0] {
            status.store(code, Ordering::SeqCst);
            let mut gc = StaleGc::default();
            tokio::time::timeout(Duration::from_secs(3), gc.run_round_at(&worker, cold_now()))
                .await
                .unwrap();
            assert_eq!(observed.recv().await, Some("head"));
            assert_eq!(worker.logical_resident_bytes(), 32);
        }
        status.store(1, Ordering::SeqCst);
        {
            let mut gc = StaleGc::default();
            let work = gc.run_round_at(&worker, cold_now());
            tokio::pin!(work);
            tokio::select! {
                event = observed.recv() => assert_eq!(event, Some("head")),
                _ = &mut work => panic!("stalled HEAD unexpectedly completed"),
                _ = tokio::time::sleep(Duration::from_secs(3)) => panic!("HEAD never arrived"),
            }
            assert!(tokio::time::timeout(Duration::from_millis(20), work)
                .await
                .is_err());
        }
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), observed.recv())
                .await
                .unwrap(),
            Some("cancelled")
        );
        assert_eq!(worker.logical_resident_bytes(), 32);

        status.store(404, Ordering::SeqCst);
        let mut gc = StaleGc::default();
        tokio::time::timeout(Duration::from_secs(3), gc.run_round_at(&worker, cold_now()))
            .await
            .unwrap();
        assert_eq!(observed.recv().await, Some("head"));
        assert_eq!(worker.logical_resident_bytes(), 0);
        assert!(worker.index.get(&block(0)).is_none());
        assert!(worker.store.scan_allocated().unwrap().is_empty());

        fill(&worker, &block(0), false).await;
        status.store(1, Ordering::SeqCst);
        let mut gc = StaleGc::default();
        let work = gc.run_round_at(&worker, cold_now());
        tokio::pin!(work);
        tokio::select! {
            event = observed.recv() => assert_eq!(event, Some("head")),
            _ = &mut work => panic!("stalled HEAD unexpectedly completed"),
            _ = tokio::time::sleep(Duration::from_secs(3)) => panic!("HEAD never arrived"),
        }
        stop.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(3), work)
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), observed.recv())
                .await
                .unwrap(),
            Some("cancelled")
        );
        assert_eq!(worker.logical_resident_bytes(), 32);
        tokio::time::timeout(Duration::from_secs(3), rings)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn warm_scan_continuations_have_no_head_pacing_delay() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let mut r = runtime(root.path(), false, origin.clone());
        r.page_gc_config.scan_batch_size = 1;
        for n in 0..3 {
            fill(&r, &block(n), false).await;
        }
        let mut gc = StaleGc::default();
        assert_eq!(gc.run_slice(&r).await, Some(Duration::ZERO));
        assert_eq!(gc.run_slice(&r).await, Some(Duration::ZERO));
        while gc.run_slice(&r).await == Some(Duration::ZERO) {}
        assert_eq!(origin.calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn stale_gc_filters_warm_entries_then_removes_both_cache_forms() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            let r = runtime(root.path(), paged, origin.clone());
            fill(&r, &block(0), paged).await;
            fill(&r, &block(1), paged).await;
            let mut gc = StaleGc::default();
            gc.tick_at(&r, access_millis()).await;
            assert_eq!(origin.calls.load(Ordering::Relaxed), 0);
            gc.tick_at(&r, cold_now()).await;
            assert_eq!(
                origin.calls.load(Ordering::Relaxed),
                1,
                "one HEAD per object per batch"
            );
            assert_eq!(r.logical_resident_bytes(), 0);
            assert_eq!(r.lru.total_bytes(), 0);
            assert!(r.index.get(&block(0)).is_none());
            assert!(r.index.get(&block(1)).is_none());
            drop(r);
            let recovered = runtime(root.path(), paged, origin);
            assert_eq!(recovered.logical_resident_bytes(), 0);
        }
    }

    #[tokio::test]
    async fn stale_gc_checks_ordinary_and_packed_entries_only_after_25_idle_hours() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            let worker = compact_runtime(root.path(), paged, origin.clone());
            let ordinary = block(0);
            fill(&worker, &ordinary, paged).await;
            let mut packed = block(0);
            packed.object.object_path = "packed".into();
            let small = worker.small.as_ref().unwrap();
            small
                .put(
                    &CacheUnit::Whole(packed.clone()),
                    Bytes::from_static(b"packed"),
                )
                .await
                .unwrap();
            let last_access = access_millis();
            worker.index.set_policy_clock(&ordinary, last_access);
            small.packed_access_time(&packed, Some(last_access));

            let mut gc = StaleGc::default();
            let cutoff = last_access + 25 * 3_600_000;
            gc.run_round_at(&worker, cutoff).await;
            assert_eq!(origin.calls.load(Ordering::Relaxed), 0);
            assert!(worker.index.get(&ordinary).is_some());
            assert!(small.contains(&packed));

            gc.run_round_at(&worker, cutoff + 1).await;
            assert_eq!(origin.calls.load(Ordering::Relaxed), 2);
            assert!(worker.index.get(&ordinary).is_none());
            assert!(!small.contains(&packed));
        }
    }

    #[tokio::test]
    async fn stale_gc_positive_checks_cover_other_offsets_for_24_hours() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            origin.mode.store(1, Ordering::Relaxed);
            let mut r = runtime(root.path(), paged, origin.clone());
            r.page_gc_config.delete_batch_size = 1;
            fill(&r, &block(0), paged).await;
            fill(&r, &block(1), paged).await;
            let mut gc = StaleGc::default();
            let now = cold_now();
            gc.tick_at(&r, now).await;
            origin.mode.store(0, Ordering::Relaxed);
            for _ in 0..8 {
                gc.tick_at(&r, now + 24 * 3_600_000 - 1).await;
            }
            assert_eq!(origin.calls.load(Ordering::Relaxed), 1);
            assert_eq!(
                r.logical_resident_bytes(),
                64,
                "a version change alone is not deletion"
            );
            for _ in 0..8 {
                gc.tick_at(&r, now + 24 * 3_600_000).await;
            }
            assert_eq!(r.logical_resident_bytes(), 0);
        }
    }

    #[tokio::test]
    async fn stale_gc_skips_scopes_without_a_binding() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin {
            unavailable: AtomicBool::new(true),
            unavailable_bucket: Some("cold"),
            ..Default::default()
        });
        let worker = runtime(root.path(), false, origin.clone());
        let warm = block(0);
        fill(&worker, &warm, false).await;
        for key in ["first", "second"] {
            let mut cold = block(0);
            cold.object = ObjectId::new(Backend::S3, "cold", key);
            fill(&worker, &cold, false).await;
        }
        let mut gc = StaleGc::default();
        let now = cold_now();
        assert!(gc.run_slice_with_clock(&worker, || now).await.is_none());
        assert!(gc.round_now.is_none());
        assert_eq!(origin.calls.load(Ordering::Relaxed), 0);
        assert_eq!(worker.logical_resident_bytes(), 96);

        origin.unavailable.store(false, Ordering::Relaxed);
        gc.run_round_at(&worker, now).await;
        assert_eq!(*origin.objects.lock().unwrap(), [warm.object]);
        assert_eq!(worker.logical_resident_bytes(), 64);
    }

    #[tokio::test]
    async fn stale_gc_errors_retain_cache_and_retry() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            origin.mode.store(2, Ordering::Relaxed);
            let r = runtime(root.path(), paged, origin.clone());
            fill(&r, &block(0), paged).await;
            let mut gc = StaleGc::default();
            let now = cold_now();
            gc.tick_at(&r, now).await;
            assert_eq!(r.logical_resident_bytes(), 32);
            origin.mode.store(0, Ordering::Relaxed);
            for _ in 0..2 {
                gc.tick_at(&r, now).await;
            }
            assert_eq!(r.logical_resident_bytes(), 0);
            assert_eq!(origin.calls.load(Ordering::Relaxed), 2);
        }
    }

    #[tokio::test]
    async fn timed_out_slice_releases_after_one_head_wave() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.mode.store(3, Ordering::Relaxed);
        let r = Arc::new(runtime(root.path(), false, origin.clone()));
        for n in 0..12 {
            let mut id = block(0);
            id.object = ObjectId::new(Backend::S3, "bucket", format!("object-{n}"));
            fill(&r, &id, false).await;
        }
        let mut gc = StaleGc::new(&WorkerConfig {
            stale_gc_head_concurrency: 2,
            stale_gc_head_max_per_sec: 0,
            ..Default::default()
        });
        gc.round_now = Some(cold_now());
        let task = tokio::spawn({
            let r = r.clone();
            async move {
                let delay = gc.run_slice(&r).await;
                (gc, delay)
            }
        });
        origin.entered.notified().await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(6)).await;
        let (gc, delay) = task.await.unwrap();
        tokio::time::resume();
        assert_eq!(origin.calls.load(Ordering::Relaxed), 2);
        assert_eq!(gc.pending.len(), 10);
        assert_eq!(delay, Some(Duration::ZERO));
        assert_eq!(r.logical_resident_bytes(), 12 * 32);
        let slow = StaleGc::new(&WorkerConfig {
            stale_gc_head_max_per_sec: 1,
            ..Default::default()
        });
        assert_eq!(
            slow.head_batch, 1,
            "at most one second of rate waiting per slice"
        );
    }

    #[tokio::test]
    async fn stale_gc_head_timeout_is_bounded_and_non_destructive() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.mode.store(3, Ordering::Relaxed);
        let r = Arc::new(runtime(root.path(), false, origin.clone()));
        fill(&r, &block(0), false).await;
        let task = tokio::spawn({
            let r = r.clone();
            async move {
                StaleGc::default().tick_at(&r, cold_now()).await;
            }
        });
        origin.entered.notified().await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(6)).await;
        task.await.unwrap();
        tokio::time::resume();
        assert_eq!(r.logical_resident_bytes(), 32);
    }

    #[tokio::test]
    async fn stale_gc_read_during_head_invalidates_the_entire_block() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            origin.hold.store(true, Ordering::Relaxed);
            let r = Arc::new(runtime(root.path(), paged, origin.clone()));
            fill(&r, &block(0), paged).await;
            let task = tokio::spawn({
                let r = r.clone();
                async move {
                    StaleGc::default().tick_at(&r, cold_now()).await;
                }
            });
            origin.entered.notified().await;
            tokio::time::sleep(Duration::from_millis(2)).await;
            if paged {
                let guards = r
                    .index
                    .page_reads_and_touch(&block(0), PageIndex(1), PageIndex(2))
                    .unwrap();
                drop(guards);
            } else {
                assert!(r.index.is_whole_and_touch(&block(0)));
            }
            origin.release.notify_one();
            task.await.unwrap();
            assert_eq!(r.logical_resident_bytes(), 32);
        }
    }

    #[tokio::test]
    async fn stale_gc_failed_block_unlink_reopens_all_pages_and_requires_a_fresh_head() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let r = runtime(root.path(), true, origin.clone());
        fill(&r, &block(0), true).await;
        let dir = r.paged.as_ref().unwrap().dir_for(&block(0));
        let saved = dir.with_extension("saved");
        std::fs::rename(&dir, &saved).unwrap();
        // A file where the block directory should be makes remove_dir_all fail
        // deterministically, including when the test runs as root.
        std::fs::write(&dir, b"not a directory").unwrap();
        let mut gc = StaleGc::default();
        gc.tick_at(&r, cold_now()).await;
        assert_eq!(r.logical_resident_bytes(), 32);
        assert_eq!(r.lru.total_bytes(), r.resident_bytes());
        let state = r.page_lifecycle.block(&block(0));
        for page in 0..2 {
            assert!(
                state.acquire(PageIndex(page)).is_some(),
                "failed unlink must reopen every page"
            );
        }
        std::fs::remove_file(&dir).unwrap();
        std::fs::rename(&saved, &dir).unwrap();
        origin.mode.store(1, Ordering::Relaxed);
        r.gc_once().await;
        assert_eq!(
            r.logical_resident_bytes(),
            32,
            "TTI GC cannot retry an old NotFound"
        );
        for _ in 0..2 {
            gc.tick_at(&r, cold_now()).await;
        }
        assert_eq!(origin.calls.load(Ordering::Relaxed), 2);
        assert_eq!(r.logical_resident_bytes(), 32);
    }

    #[tokio::test]
    async fn stale_gc_respects_pins_and_scan_delete_limits() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            let mut r = runtime(root.path(), paged, origin.clone());
            r.page_gc_config.scan_batch_size = 1;
            r.page_gc_config.delete_batch_size = 1;
            fill(&r, &block(0), paged).await;
            fill(&r, &block(1), paged).await;
            let unit = if paged {
                CacheUnit::Page(block(0), PageIndex(0))
            } else {
                CacheUnit::Whole(block(0))
            };
            let pin = r.lru.pin_guard(unit).unwrap();
            let mut gc = StaleGc::default();
            let now = cold_now();
            gc.tick_at(&r, now).await;
            assert_eq!(origin.calls.load(Ordering::Relaxed), 0);
            assert_eq!(r.logical_resident_bytes(), 64);
            gc.tick_at(&r, now).await;
            assert_eq!(
                r.logical_resident_bytes(),
                32,
                "one candidate reclaims a complete block"
            );
            drop(pin);
            for _ in 0..8 {
                gc.tick_at(&r, now).await;
            }
            assert_eq!(r.logical_resident_bytes(), 0);
        }
    }
    #[tokio::test]
    async fn stale_gc_rechecks_sibling_activity_after_waiting_for_mutation_gate() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let r = Arc::new(runtime(root.path(), true, origin));
        fill(&r, &block(0), true).await;
        let state = r.page_lifecycle.block(&block(0));
        let gate = state.lock().await;
        let task = tokio::spawn({
            let r = r.clone();
            async move {
                StaleGc::default().tick_at(&r, cold_now()).await;
            }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while r.page_mutations.active_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        drop(
            r.index
                .page_reads_and_touch(&block(0), PageIndex(1), PageIndex(2))
                .unwrap(),
        );
        drop(gate);
        task.await.unwrap();
        assert_eq!(r.logical_resident_bytes(), 32);
    }

    #[tokio::test]
    async fn stale_gc_caps_heads_per_batch() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.mode.store(2, Ordering::Relaxed);
        let r = runtime(root.path(), false, origin.clone());
        for n in 0..40 {
            let mut id = block(0);
            id.object = ObjectId::new(Backend::S3, "bucket", format!("key-{n}"));
            fill(&r, &id, false).await;
        }
        let mut gc = StaleGc::new(&WorkerConfig {
            stale_gc_head_concurrency: MAX_HEADS,
            ..Default::default()
        });
        gc.tick_at(&r, cold_now()).await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), MAX_HEADS);
        assert_eq!(r.logical_resident_bytes(), 40 * 32);
        gc.tick_at(&r, cold_now()).await;
        assert_eq!(
            origin.calls.load(Ordering::Relaxed),
            40,
            "resume skipped candidates instead of starving them"
        );
    }
    #[tokio::test]
    async fn stale_gc_pending_candidates_honor_positive_checks_from_previous_batch() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.mode.store(1, Ordering::Relaxed);
        let r = runtime(root.path(), false, origin.clone());
        for n in 0..33 {
            let mut id = block(0);
            id.object = ObjectId::new(Backend::S3, "bucket", format!("key-{n}"));
            fill(&r, &id, false).await;
        }
        let mut id = block(1);
        id.object = ObjectId::new(Backend::S3, "bucket", "key-0");
        fill(&r, &id, false).await;
        let mut gc = StaleGc::new(&WorkerConfig {
            stale_gc_head_concurrency: MAX_HEADS,
            ..Default::default()
        });
        let now = cold_now();
        gc.tick_at(&r, now).await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), MAX_HEADS);
        gc.tick_at(&r, now + 1).await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), 33);
        assert_eq!(r.logical_resident_bytes(), 34 * 32);
    }
    #[tokio::test]
    async fn stale_gc_round_drains_batches_without_rescanning_or_chasing_new_entries() {
        for scan_budget in [16, 4096] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            origin.mode.store(2, Ordering::Relaxed);
            origin.hold.store(true, Ordering::Relaxed);
            let mut r = runtime(root.path(), false, origin.clone());
            r.page_gc_config.scan_batch_size = scan_budget;
            for n in 0..40 {
                let mut id = block(0);
                id.object = ObjectId::new(Backend::S3, "bucket", format!("key-{n}"));
                fill(&r, &id, false).await;
            }
            let r = Arc::new(r);
            let task = tokio::spawn({
                let r = r.clone();
                async move {
                    let mut gc = StaleGc::new(&WorkerConfig {
                        stale_gc_head_concurrency: 2,
                        ..Default::default()
                    });
                    gc.run_round_at(&r, cold_now()).await;
                }
            });
            // Hold a full concurrent HEAD wave before inserting the late entry.
            tokio::time::timeout(Duration::from_secs(5), async {
                while origin.calls.load(Ordering::Relaxed) < 2 {
                    origin.entered.notified().await;
                }
            })
            .await
            .expect("both concurrent HEAD requests must enter the backend");
            let mut late = block(0);
            late.object = ObjectId::new(Backend::S3, "bucket", "late");
            fill(&r, &late, false).await;
            origin.hold.store(false, Ordering::Relaxed);
            origin.release.notify_waiters();
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(origin.calls.load(Ordering::Relaxed), 40,
                "drain the scan/HEAD batches exactly once, retaining the round's original upper bound");
            assert_eq!(r.logical_resident_bytes(), 41 * 32);
        }
    }

    #[tokio::test]
    async fn stale_gc_scheduler_waits_an_hour_between_complete_rounds() {
        let root = tempfile::tempdir().unwrap();
        let r = Arc::new(runtime(root.path(), false, Arc::new(Origin::default())));
        let service = super::super::WorkerBackground::start(
            Worker::standalone(r.clone()),
            &talon_core::WorkerConfig {
                ownership_gc_enabled: false,
                ..Default::default()
            },
        );
        let completed = r.metrics.registry.counter(
            "talon_worker_background_task_completed_total",
            "Completed background batches by task.",
            talon_core::metrics::labels(&[("task", "stale_gc")]),
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while completed.get() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // Let the scheduler record the completion time before advancing its clock.
        tokio::time::sleep(Duration::from_millis(5)).await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(3599)).await;
        tokio::time::resume();
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(
            completed.get(),
            1,
            "no repeated scan during the hourly wait"
        );
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::time::resume();
        tokio::time::timeout(Duration::from_secs(2), async {
            while completed.get() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        service.shutdown().await;
        assert_eq!(completed.get(), 2);
    }
    #[tokio::test]
    async fn stale_gc_busy_page_defers_the_entire_block_and_releases_partial_claims() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let r = runtime(root.path(), true, origin.clone());
        fill(&r, &block(0), true).await;
        let state = r.page_lifecycle.block(&block(0));
        let reader = state.acquire(PageIndex(1)).unwrap();
        let mut gc = StaleGc::default();
        let now = cold_now();
        gc.tick_at(&r, now).await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), 1);
        assert_eq!(r.logical_resident_bytes(), 32);
        assert!(
            state.acquire(PageIndex(0)).is_some(),
            "page 0's claim must roll back if page 1 is busy"
        );
        assert!(r
            .paged
            .as_ref()
            .unwrap()
            .dir_for(&block(0))
            .join("0.page")
            .exists());
        drop(reader);
        gc.tick_at(&r, now).await;
        assert_eq!(r.logical_resident_bytes(), 0);
        assert!(state.acquire(PageIndex(0)).is_none());
        assert!(!r.paged.as_ref().unwrap().dir_for(&block(0)).exists());
    }

    #[tokio::test]
    async fn stale_gc_new_sibling_during_head_invalidates_the_block_candidate() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.hold.store(true, Ordering::Relaxed);
        let r = Arc::new(runtime(root.path(), true, origin.clone()));
        r.commit_fetched_page(&block(0), PageIndex(0), 32, Bytes::from_static(&[1; 16]))
            .await
            .unwrap();
        let task = tokio::spawn({
            let r = r.clone();
            async move {
                StaleGc::default().tick_at(&r, cold_now()).await;
            }
        });
        origin.entered.notified().await;
        r.commit_fetched_page(&block(0), PageIndex(1), 32, Bytes::from_static(&[2; 16]))
            .await
            .unwrap();
        origin.release.notify_one();
        task.await.unwrap();
        assert_eq!(r.logical_resident_bytes(), 32);
        assert_eq!(r.lru.total_bytes(), r.resident_bytes());
    }

    #[tokio::test]
    async fn stale_gc_cancelled_caller_still_finishes_whole_block_mutation_with_disk_budget() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let mut r = runtime(root.path(), true, origin);
        let config = talon_core::WorkerConfig {
            background_io_concurrency: 1,
            background_delete_max_per_sec: 0,
            ..Default::default()
        };
        let budget = crate::background::BackgroundBudget::new(&config);
        r.background_budget = Some(budget.clone());
        fill(&r, &block(0), true).await;
        let slot = budget.io().await;
        let r = Arc::new(r);
        let task = tokio::spawn({
            let r = r.clone();
            async move {
                StaleGc::default().tick_at(&r, cold_now()).await;
            }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while r.page_mutations.active_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            r.logical_resident_bytes(),
            32,
            "shared I/O budget holds back the entire block"
        );
        drop(slot);
        r.drain_page_mutations().await;
        assert_eq!(r.logical_resident_bytes(), 0);
        assert_eq!(r.lru.total_bytes(), 0);
        assert!(!r.paged.as_ref().unwrap().dir_for(&block(0)).exists());
    }
}
