//! Event-driven reclamation of cache blocks outside current placement.
use super::{MembershipSnapshot, OwnershipLimits, WorkerRuntime};
use crate::eviction::StaleBlockCandidate;
use crate::small_store::{OwnershipCursor, OwnershipLimit, SmallGcCandidate};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};
use talon_core::{BlockId, CachePlacementTable, NodeId, WorkerConfig};

const SUPPLEMENT_INTERVAL: Duration = Duration::from_secs(3600);

pub(super) struct OwnershipGc {
    worker: NodeId,
    replicas: usize,
    membership: Option<Arc<MembershipSnapshot>>,
    placement: Vec<CachePlacementTable>,
    completed_generation: u64,
    completed_limit: u64,
    completed_small_epoch: u64,
    // Retain fixed supplemental bounds across cancellation and protected retries.
    supplement: Option<std::ops::Range<u64>>,
    next_supplement: tokio::time::Instant,
    pass: Option<OwnershipPass>,
    round_ended: bool,
    #[cfg(test)]
    discovery_visits: usize,
}

struct OwnershipPass {
    generation: u64,
    range: std::ops::Range<u64>,
    cursor: OwnershipScan,
    scanning: bool,
    retry_cursor: u64,
    retries: BTreeMap<u64, StaleBlockCandidate>,
    // Publish before awaiting so cancellation cannot lose a selected block.
    // Observed block and deletion credits already paid.
    attempt: Option<(StaleBlockCandidate, u64)>,
    block_round_done: bool,
    small_next: bool,
    small: Option<SmallPass>,
}

enum OwnershipScan {
    Occupied(std::ops::Range<usize>),
    Changed(std::ops::Range<u64>),
}

struct SmallPass {
    first_epoch: u64,
    limit: OwnershipLimit,
    cursor: OwnershipCursor,
    scan_done: bool,
    pending: VecDeque<SmallGcCandidate>,
    retry_cursor: Option<u64>,
    attempt: Option<(SmallGcCandidate, u64, bool)>,
    round_done: bool,
}

impl SmallPass {
    fn new(first_epoch: u64, limit: OwnershipLimit) -> Self {
        Self {
            first_epoch,
            limit,
            cursor: OwnershipCursor::default(),
            scan_done: false,
            pending: VecDeque::new(),
            retry_cursor: None,
            attempt: None,
            round_done: false,
        }
    }
}

impl OwnershipGc {
    pub(super) fn new(config: &WorkerConfig) -> Self {
        Self {
            worker: NodeId::new(
                config
                    .node_id
                    .clone()
                    .expect("ownership cleanup requires a resolved worker identity"),
            ),
            replicas: config.ownership_gc_replicas,
            membership: None,
            placement: Vec::new(),
            completed_generation: 0,
            completed_limit: 0,
            completed_small_epoch: 0,
            supplement: None,
            next_supplement: tokio::time::Instant::now() + SUPPLEMENT_INTERVAL,
            pass: None,
            round_ended: false,
            #[cfg(test)]
            discovery_visits: 0,
        }
    }

    /// Retry unfinished work promptly; otherwise check for new admissions hourly.
    pub(super) fn next_delay(&self, worker: &WorkerRuntime) -> Option<Duration> {
        worker
            .membership_snapshot()
            .filter(|view| {
                view.members
                    .iter()
                    .any(|member| member.info.id == self.worker)
            })
            .map(|view| {
                if self.pass.is_some() && !self.round_ended {
                    Duration::ZERO
                } else if view.generation != self.completed_generation || self.supplement.is_some()
                {
                    Duration::from_secs(1)
                } else {
                    self.next_supplement
                        .saturating_duration_since(tokio::time::Instant::now())
                }
            })
    }

    #[cfg(test)]
    pub(super) async fn run_round(&mut self, worker: &WorkerRuntime) {
        let generation = worker.membership_snapshot().map(|view| view.generation);
        loop {
            self.run_slice(worker).await;
            if self.next_delay(worker) != Some(Duration::ZERO)
                || worker.membership_snapshot().map(|view| view.generation) != generation
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    }

    pub(super) async fn run_slice(&mut self, worker: &WorkerRuntime) {
        let Some(view) = self.prepare_ownership(worker) else {
            return;
        };
        self.run_slice_for_membership(worker, view).await;
    }

    async fn run_slice_for_membership(
        &mut self,
        worker: &WorkerRuntime,
        view: Arc<MembershipSnapshot>,
    ) {
        if !worker.membership_is_current(&view) {
            return;
        }
        if self
            .pass
            .as_ref()
            .is_some_and(|pass| pass.generation != view.generation)
        {
            self.pass = None;
            self.supplement = None;
        }
        if self.pass.is_none() {
            let (range, small) = if view.generation != self.completed_generation {
                self.supplement = None;
                // Keep any history newer than the event cutoff across generations.
                // A first pickup can start past that cutoff; a supplement with
                // an uncovered interval scans occupied slots.
                worker.index.start_ownership_changes();
                // Each namespace retains its own publication boundaries.
                let limits = view
                    .scan_limits
                    .get(&worker.cache_identity)
                    .copied()
                    .unwrap_or_default();
                (
                    0..limits.block,
                    limits.small.map(|limit| SmallPass::new(0, limit)),
                )
            } else {
                let Some(OwnershipLimits {
                    block: limit,
                    small,
                }) = worker.capture_supplemental_limits(&view)
                else {
                    return;
                };
                if limit <= self.completed_limit && small.is_none() {
                    self.next_supplement = tokio::time::Instant::now() + SUPPLEMENT_INTERVAL;
                    return;
                }
                let range = self.completed_limit..limit.max(self.completed_limit);
                self.supplement = Some(range.clone());
                (
                    range,
                    small.map(|limit| SmallPass::new(self.completed_small_epoch, limit)),
                )
            };
            if let Some(pass) = &small {
                drop(
                    worker
                        .small
                        .as_ref()
                        .unwrap()
                        .take_superseded_retries(pass.limit),
                );
            }
            let cursor = if range.is_empty() {
                OwnershipScan::Occupied(0..0)
            } else if self.supplement.is_some() && worker.index.ownership_changes_cover(&range) {
                OwnershipScan::Changed(range.clone())
            } else {
                OwnershipScan::Occupied(0..worker.lru.block_scan_ceiling())
            };
            self.pass = Some(OwnershipPass {
                generation: view.generation,
                cursor,
                retry_cursor: range.start,
                scanning: !range.is_empty(),
                block_round_done: self.supplement.is_some() && range.is_empty(),
                range,
                retries: BTreeMap::new(),
                attempt: None,
                small_next: false,
                small,
            });
            self.round_ended = false;
        }
        if self.round_ended {
            let pass = self.pass.as_mut().unwrap();
            pass.retry_cursor = pass.range.start;
            pass.block_round_done = self.supplement.is_some() && pass.range.is_empty();
            if let Some(small) = &mut pass.small {
                small.retry_cursor = Some(0);
                small.round_done = false;
            }
        }
        self.round_ended = false;
        let pass = self.pass.as_mut().unwrap();
        let small_has_work = pass.small.as_ref().is_some_and(|small| !small.round_done);
        if pass.block_round_done || (pass.small_next && small_has_work) {
            pass.small_next = false;
            if worker.membership_is_current(&view) {
                self.run_small_slice(worker, &view).await;
            }
            return;
        }
        // An uncovered supplement may have to visit the entire ordinary
        // directory. Give Small discovery a turn after each such slice.
        pass.small_next = small_has_work
            && self.supplement.is_some()
            && pass.scanning
            && matches!(pass.cursor, OwnershipScan::Occupied(_));
        let started = Instant::now();
        for _ in 0..64 {
            if !worker.membership_is_current(&view) {
                return;
            }
            let pass = self.pass.as_mut().unwrap();
            if pass.attempt.is_none() {
                let candidate = if pass.scanning {
                    let step = match &mut pass.cursor {
                        OwnershipScan::Occupied(slots) => {
                            worker.lru.ownership_block_step(slots, &pass.range)
                        }
                        OwnershipScan::Changed(revisions) => {
                            worker.lru.ownership_change_step(revisions)
                        }
                    };
                    let Some(candidate) = step else {
                        pass.scanning = false;
                        pass.block_round_done = true;
                        self.finish_round_if_ready(worker, &view);
                        return;
                    };
                    #[cfg(test)]
                    {
                        self.discovery_visits += 1;
                    }
                    let Some(candidate) = candidate else {
                        continue;
                    };
                    candidate
                } else {
                    let Some((&revision, retry)) = pass.retries.range(pass.retry_cursor..).next()
                    else {
                        pass.block_round_done = true;
                        self.finish_round_if_ready(worker, &view);
                        return;
                    };
                    pass.retry_cursor = revision + 1;
                    // A later admission belongs to a future pass. Re-read the
                    // access age and pin state for this incarnation's retry.
                    let Some(candidate) = worker.lru.refresh_ownership_candidate(retry) else {
                        pass.retries.remove(&revision);
                        continue;
                    };
                    candidate
                };
                worker.page_gc_metrics.ownership_checked.inc();
                pass.attempt = Some((candidate, 0));
            }
            let (candidate, paid) = self
                .pass
                .as_ref()
                .unwrap()
                .attempt
                .as_ref()
                .unwrap()
                .clone();
            let success = if self.owns(&candidate.block)
                || !worker.lru.contains_tracked_block(&candidate.block)
            {
                true
            } else if !worker.lru.stale_candidate_is_current(&candidate) {
                false
            } else {
                if let Some(budget) = &worker.background_budget {
                    let credits = budget.delete_slice(candidate.units as u64 - paid).await;
                    self.pass.as_mut().unwrap().attempt.as_mut().unwrap().1 += credits;
                    if paid + credits < candidate.units as u64 {
                        return;
                    }
                }
                self.reclaim_block(worker, candidate.clone(), &view).await
            };
            let pass = self.pass.as_mut().unwrap();
            pass.attempt = None;
            if success {
                pass.retries.remove(&candidate.revision);
            } else {
                pass.retries.insert(candidate.revision, candidate);
            }
            if started.elapsed() >= Duration::from_millis(10) {
                return;
            }
        }
    }

    async fn run_small_slice(&mut self, worker: &WorkerRuntime, view: &Arc<MembershipSnapshot>) {
        let small = worker.small.as_ref().unwrap();
        let pass = self.pass.as_mut().unwrap().small.as_mut().unwrap();
        let started = Instant::now();
        let limit = pass.limit;
        if pass.attempt.is_none() && pass.pending.is_empty() {
            let mut cursor = pass.cursor;
            let first_epoch = pass.first_epoch;
            if let Some(retry_cursor) = &mut pass.retry_cursor {
                let (done, candidates) = small.ownership_retry_candidates(retry_cursor, limit, 64);
                pass.pending = candidates.into();
                pass.scan_done = done;
            } else if !pass.scan_done {
                let (done, candidates) =
                    small.ownership_candidates(&mut cursor, first_epoch, limit, 64, |block| {
                        worker.page_gc_metrics.ownership_checked.inc();
                        self.owns(block)
                    });
                let pass = self.pass.as_mut().unwrap().small.as_mut().unwrap();
                pass.cursor = cursor;
                pass.scan_done = done;
                pass.pending = candidates.into();
            }
        }
        let pass = self.pass.as_mut().unwrap().small.as_mut().unwrap();
        for _ in 0..64 {
            if !worker.membership_is_current(view) {
                return;
            }
            if pass.attempt.is_none() {
                let Some(mut candidate) = pass.pending.pop_front() else {
                    if pass.scan_done {
                        pass.round_done = true;
                        self.finish_round_if_ready(worker, view);
                    }
                    return;
                };
                let Some(revision) = candidate.observe_ownership(limit.epoch) else {
                    continue;
                };
                pass.attempt = Some((candidate, revision, false));
            }
            let (candidate, revision, paid) = pass.attempt.as_mut().unwrap();
            if !*paid {
                if !candidate.ticket.gc_is_current(*revision) {
                    pass.attempt = None;
                    continue;
                }
                if let Some(budget) = &worker.background_budget {
                    budget.delete().await;
                }
                *paid = true;
            }
            let Some(renewed) = worker
                .membership_snapshot()
                .filter(|snapshot| snapshot.generation == view.generation)
            else {
                return;
            };
            worker.page_gc_metrics.ownership_attempted.inc();
            worker
                .unlink_small_gc(candidate.clone(), *revision, Some(renewed))
                .await;
            pass.attempt = None;
            if started.elapsed() >= Duration::from_millis(10) {
                return;
            }
        }
    }

    fn finish_round_if_ready(&mut self, worker: &WorkerRuntime, view: &MembershipSnapshot) {
        let pass = self.pass.as_ref().unwrap();
        if !pass.block_round_done || pass.small.as_ref().is_some_and(|small| !small.round_done) {
            return;
        }
        self.round_ended = true;
        let small_retries = pass.small.as_ref().is_some_and(|pass| {
            worker
                .small
                .as_ref()
                .unwrap()
                .has_ownership_retries(pass.limit)
        });
        if pass.retries.is_empty() && !small_retries {
            self.completed_generation = view.generation;
            self.completed_limit = pass.range.end;
            worker.index.retire_ownership_changes(self.completed_limit);
            if let Some(small) = &pass.small {
                self.completed_small_epoch = small.limit.epoch;
            } else if self.supplement.is_none() {
                self.completed_small_epoch = 0;
            }
            self.pass = None;
            self.supplement = None;
            self.next_supplement = tokio::time::Instant::now() + SUPPLEMENT_INTERVAL;
        }
    }

    /// Cache placement tables across retries of the same membership event.
    fn prepare_ownership(&mut self, worker: &WorkerRuntime) -> Option<Arc<MembershipSnapshot>> {
        let id = &self.worker;
        let snapshot = worker.membership_snapshot().filter(|view| {
            (view.generation != self.completed_generation
                || self.supplement.is_some()
                || tokio::time::Instant::now() >= self.next_supplement)
                && view.members.iter().any(|member| &member.info.id == id)
        })?;
        if self
            .membership
            .as_ref()
            .map_or(true, |old| old.members != snapshot.members)
        {
            let nodes: Vec<_> = snapshot.members.iter().map(|n| n.info.clone()).collect();
            self.placement = vec![CachePlacementTable::new(&nodes)];
            if let Some(zone) = snapshot
                .members
                .iter()
                .find(|n| &n.info.id == id)
                .and_then(|n| n.zone.as_ref())
            {
                let local: Vec<_> = snapshot
                    .members
                    .iter()
                    .filter(|n| n.zone.as_ref() == Some(zone))
                    .map(|n| n.info.clone())
                    .collect();
                if local.len() != nodes.len() {
                    self.placement.push(CachePlacementTable::new(&local));
                }
            }
        }
        self.membership = Some(snapshot.clone());
        Some(snapshot)
    }

    fn owns(&self, block: &BlockId) -> bool {
        self.placement.iter().any(|table| {
            if self.replicas == 1 {
                table.primary(block).is_some_and(|n| n.id == self.worker)
            } else {
                table
                    .rank(block, self.replicas)
                    .iter()
                    .any(|n| n.id == self.worker)
            }
        })
    }

    /// Reclaim one pre-boundary block; false leaves the pass pending for retry.
    async fn reclaim_block(
        &self,
        worker: &WorkerRuntime,
        candidate: StaleBlockCandidate,
        expected: &Arc<MembershipSnapshot>,
    ) -> bool {
        // Rate waiting may outlive an observation: consume its latest renewal,
        // while requiring the original event generation.
        let Some(snapshot) = worker
            .membership_snapshot()
            .filter(|view| view.generation == expected.generation)
        else {
            return false;
        };
        worker
            .page_gc_metrics
            .ownership_attempted
            .add(candidate.units as u64);
        worker
            .unlink_stale_block_for(candidate, Some(snapshot))
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::Worker;

    use super::*;
    use crate::{
        BlockIndex, CacheUnit, InFlightLoads, PagedBlockStore, WholeBlockStore, WorkerMetrics,
    };
    use bytes::Bytes;
    use std::{sync::Arc, time::Instant};
    use talon_core::{
        Backend, BackendStore, NodeInfo, NodeRole, ObjectId, ObjectStat, PageIndex, Version,
    };
    use talon_transport::ZonedNodeInfo;
    #[derive(Default)]
    struct Origin {}
    #[async_trait::async_trait]
    impl BackendStore for Origin {
        async fn fetch_range(&self, _: &ObjectId, _: u64, _: u64) -> talon_core::Result<Bytes> {
            panic!("ownership GC must not fetch data")
        }
        async fn head(&self, _: &ObjectId) -> talon_core::Result<ObjectStat> {
            panic!("ownership GC must not query backend metadata")
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

    async fn check_sparse_maintenance(historical: u64) {
        use crate::{
            catalog::{Operation, StoreKind},
            index::Directory,
            page_lifecycle::ScanCursor,
        };
        let directory = Arc::new(std::sync::RwLock::new(Directory::default()));
        directory.write().unwrap().attach(16);
        let index = Arc::new(BlockIndex::from_catalog(directory.clone()));
        let root = tempfile::tempdir().unwrap();
        let worker = WorkerRuntime::new(
            WholeBlockStore::open(root.path().join("whole")).unwrap(),
            index,
            Arc::new(InFlightLoads::new()),
            Arc::new(Origin::default()),
            32,
            0,
            WorkerMetrics::new(0),
        );
        worker.page_lifecycle.enable_access_tracking();
        let first = block(63);
        let tail = block(historical - 1);
        let reused = block(historical);
        let late = block(historical + 1);
        let put = |rows: &mut Directory, id: &BlockId, page| {
            rows.apply(
                id,
                StoreKind::Paged,
                &Operation::Put { page, len: 32 },
                None,
                Some(16),
            );
        };
        {
            let mut rows = directory.write().unwrap();
            for n in 0..historical {
                put(&mut rows, &block(n), 0);
            }
            for n in (0..historical - 1).rev().filter(|&n| n != 63) {
                rows.apply(
                    &block(n),
                    StoreKind::Paged,
                    &Operation::DeleteBlock,
                    None,
                    Some(0),
                );
            }
            put(&mut rows, &reused, 0);
            put(&mut rows, &first, 1);
            assert_eq!(rows.slot(&reused), Some(0));
            assert_eq!(rows.slot(&tail), Some(historical as usize - 1));
        }
        let ceiling = worker.lru.block_scan_ceiling();
        assert_eq!(ceiling, historical as usize);

        let mut page_cursor = ScanCursor::default();
        let mut pages = Vec::new();
        for (id, page) in [(&reused, 0), (&first, 0), (&first, 1), (&tail, 0)] {
            let (batch, report) = worker
                .page_lifecycle
                .scan(&mut page_cursor, 1, 100, Some(1), 10);
            assert_eq!(report.checked, 1);
            let candidate = &batch[0];
            assert_eq!(candidate.block.id.materialize(), *id);
            assert_eq!(candidate.page, PageIndex(page));
            pages.extend(batch);
        }
        let (_, report) = worker
            .page_lifecycle
            .scan(&mut page_cursor, 1, 100, Some(1), 10);
        assert!(report.completed);
        assert_eq!(report.checked, 0);
        drop(pages);
        let (pages, report) =
            worker
                .page_lifecycle
                .scan(&mut page_cursor, 4096, 100, Some(1), 4096);
        assert!(report.completed);
        assert_eq!((pages.len(), report.checked), (4, 4));

        let mut stale = (0, 0);
        for id in [&reused, &first, &tail, &reused] {
            let batch = worker.lru.idle_candidates(&mut stale, u64::MAX, 0, 1, 1);
            assert_eq!(batch.len(), 1);
            assert_eq!(batch[0].block, *id);
        }
        let mut stale = (0, 0);
        let batch = worker
            .lru
            .idle_candidates(&mut stale, u64::MAX, 0, 4096, 4096);
        assert_eq!((batch.len(), stale), (3, (ceiling, ceiling)));

        let fixture = MembershipFixture::new();
        *fixture.members.lock().unwrap() = Some(vec!["a"]);
        let mut gc = fixture.gc(&worker);
        let limit = worker.lru.ownership_admission_ceiling();
        put(&mut directory.write().unwrap(), &late, 0);
        let mut slots = 0..ceiling;
        let visits: Vec<_> =
            std::iter::from_fn(|| worker.lru.ownership_block_step(&mut slots, &(0..limit)))
                .map(|candidate| candidate.map(|candidate| candidate.block))
                .collect();
        assert_eq!(visits, [Some(reused), None, Some(first), Some(tail)]);
        gc.run_slice(&worker).await;
        assert!(gc.pass.is_none());
        assert_eq!(gc.completed_limit, limit);
        assert_eq!(worker.page_gc_metrics.ownership_checked.get(), 3);
        gc.next_supplement = tokio::time::Instant::now();
        gc.run_slice(&worker).await;
        assert!(gc.pass.is_none());
        assert_eq!(worker.page_gc_metrics.ownership_checked.get(), 4);
        println!("historical_slots={ceiling} live_slots=3 reused_slot=0 final_live_slot={} tti_pages={} tti_batches=1 stale_visits={} stale_batches=1 ownership_event_visits={} ownership_event_checks=3 ownership_event_slices=1 ownership_supplement_checks=1", ceiling - 1, report.checked, batch.len(), visits.len());
    }

    #[tokio::test]
    async fn sparse_maintenance_skips_retired_slots_and_preserves_bounds() {
        check_sparse_maintenance(16_384).await;
    }

    #[tokio::test]
    #[ignore = "million-slot maintenance work-count probe"]
    async fn sparse_maintenance_million_slot_probe() {
        check_sparse_maintenance(1_000_000).await;
    }

    fn runtime(root: &std::path::Path, paged: bool, origin: Arc<Origin>) -> WorkerRuntime {
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
    struct MembershipFixture {
        members: std::sync::Mutex<Option<Vec<&'static str>>>,
        config: WorkerConfig,
    }
    impl MembershipFixture {
        fn new() -> Self {
            Self {
                members: std::sync::Mutex::new(Some(vec!["a", "b"])),
                config: WorkerConfig {
                    node_id: Some("a".into()),
                    stale_gc_enabled: false,
                    ..Default::default()
                },
            }
        }
        fn view(&self) -> Option<talon_core::worker_membership::WorkerDiscovery> {
            self.members.lock().unwrap().as_ref().map(|ids| {
                discovery(
                    &ids.iter()
                        .map(|id| ZonedNodeInfo {
                            info: NodeInfo {
                                id: NodeId::new(*id),
                                address: String::new(),
                                role: NodeRole::Worker,
                            },
                            zone: None,
                        })
                        .collect::<Vec<_>>(),
                )
            })
        }
        fn publish(&self, worker: &WorkerRuntime) {
            worker
                .update_membership(self.view(), Instant::now())
                .unwrap();
        }
        fn publish_managed(&self, worker: &Worker) {
            worker
                .update_membership(self.view(), Instant::now())
                .unwrap();
        }
        fn gc_managed(&self, worker: &Worker) -> OwnershipGc {
            self.publish_managed(worker);
            OwnershipGc::new(&self.config)
        }
        fn gc(&self, worker: &WorkerRuntime) -> OwnershipGc {
            self.publish(worker);
            OwnershipGc::new(&self.config)
        }
    }
    fn placed(owner: &str, offset: usize) -> BlockId {
        let nodes = ["a", "b"].map(|id| talon_core::NodeInfo {
            id: talon_core::NodeId::new(id),
            address: String::new(),
            role: talon_core::NodeRole::Worker,
        });
        let table = talon_core::CachePlacementTable::new(&nodes);
        (0..10000)
            .map(block)
            .filter(|id| table.primary(id).unwrap().id.0 == owner)
            .nth(offset)
            .unwrap()
    }

    fn packed_placed(owner: &str, nth: usize) -> BlockId {
        let nodes = ["a", "b"].map(|name| NodeInfo {
            id: NodeId::new(name),
            address: String::new(),
            role: NodeRole::Worker,
        });
        let table = CachePlacementTable::new(&nodes);
        (0..10_000)
            .map(|n| {
                BlockId::new(
                    ObjectId::new(Backend::S3, "bucket", format!("packed-{n}")),
                    0,
                    32,
                    Version::new("v1"),
                )
            })
            .filter(|id| table.primary(id).unwrap().id.0 == owner)
            .nth(nth)
            .unwrap()
    }

    fn namespace_runtime(root: &std::path::Path, paged: bool) -> (Arc<Worker>, MembershipFixture) {
        namespace_runtime_with_config(root, paged, |_| {})
    }

    fn namespace_runtime_with_config(
        root: &std::path::Path,
        paged: bool,
        configure: impl FnOnce(&mut WorkerConfig),
    ) -> (Arc<Worker>, MembershipFixture) {
        let mut fixture = MembershipFixture::new();
        fixture.config.block_size = 32;
        fixture.config.small_object_threshold_bytes = 1 << 20;
        fixture.config.l2_page_size_bytes = if paged { 16 } else { 0 };
        fixture.config.l1_page_size_bytes = 16;
        fixture.config.l1_capacity_bytes = 0;
        fixture.config.page_tti_hours = 0;
        fixture.config.backend = Some("s3".into());
        configure(&mut fixture.config);
        let lock = Arc::new(crate::page_access_store::CacheRootLock::acquire(root).unwrap());
        let worker = Arc::new(
            Worker::open_namespaces(
                &fixture.config,
                lock,
                Arc::new(Origin::default()),
                WorkerMetrics::new(0),
            )
            .unwrap(),
        );
        (worker, fixture)
    }

    #[tokio::test]
    async fn membership_publication_and_wakeup_do_not_wait_for_directory_writer() {
        let root = tempfile::tempdir().unwrap();
        let (worker, fixture) = namespace_runtime(root.path(), true);
        let wake = worker.membership_changed.subscribe();
        let publisher = worker.clone();
        let (done, published) = std::sync::mpsc::channel();
        std::thread::scope(|threads| {
            let (start, held) = std::sync::mpsc::channel();
            let attempt = threads.spawn(move || {
                held.recv().unwrap();
                fixture.publish_managed(&publisher);
                done.send(()).unwrap();
            });
            let completed_with_writer_held = worker.index.with_directory_writer(|_| {
                start.send(()).unwrap();
                published.recv_timeout(Duration::from_secs(2))
            });
            attempt.join().unwrap();
            completed_with_writer_held.unwrap();
        });
        assert!(wake.has_changed().unwrap());
        assert_eq!(worker.membership_snapshot().unwrap().generation, 1);
    }

    #[tokio::test]
    async fn dense_ownership_supplement_visits_one_refill_before_packed_work() {
        let root = tempfile::tempdir().unwrap();
        let (worker, fixture) = namespace_runtime(root.path(), true);
        let table = CachePlacementTable::new(&["a", "b"].map(|name| NodeInfo {
            id: NodeId::new(name),
            address: String::new(),
            role: NodeRole::Worker,
        }));
        let rows = 16_384;
        let refill = placed("a", 0);
        worker.index.with_directory_writer(|directory| {
            let put = |directory: &mut crate::index::Directory, id: &BlockId| {
                directory.apply(
                    id,
                    crate::catalog::StoreKind::Paged,
                    &crate::catalog::Operation::Put { page: 0, len: 32 },
                    None,
                    Some(16),
                );
            };
            // The refill sits near the end of the directory to expose linear discovery.
            for id in (0..)
                .map(block)
                .filter(|id| *id != refill && table.primary(id).unwrap().id.0 == "a")
                .take(rows - 1)
            {
                put(directory, &id);
            }
            put(directory, &refill);
        });
        let mut gc = fixture.gc_managed(&worker);
        gc.run_round(&worker).await;
        assert_eq!(gc.discovery_visits, rows);
        gc.discovery_visits = 0;
        worker.index.with_directory_writer(|directory| {
            directory.apply(
                &refill,
                crate::catalog::StoreKind::Paged,
                &crate::catalog::Operation::Put { page: 1, len: 32 },
                None,
                Some(16),
            );
        });
        let packed = packed_placed("b", 0);
        let small = worker.small.as_ref().unwrap();
        small
            .put(&packed, Bytes::from_static(b"packed"))
            .await
            .unwrap();

        gc.next_supplement = tokio::time::Instant::now();
        gc.run_slice(&worker).await;
        println!(
            "dense_rows={rows} supplement_visited={}",
            gc.discovery_visits
        );
        assert_eq!(gc.discovery_visits, 1);
        assert!(small.contains(&packed));
        gc.run_slice(&worker).await;
        assert!(!small.contains(&packed));
        assert_eq!(gc.discovery_visits, 1);
    }

    #[tokio::test]
    async fn uncovered_ownership_supplement_reclaims_packed_before_ordinary_scan_ends() {
        let root = tempfile::tempdir().unwrap();
        let (worker, fixture) = namespace_runtime(root.path(), true);
        let table = CachePlacementTable::new(&["a", "b"].map(|name| NodeInfo {
            id: NodeId::new(name),
            address: String::new(),
            role: NodeRole::Worker,
        }));
        for id in (0..)
            .map(block)
            .filter(|id| table.primary(id).unwrap().id.0 == "a")
            .take(130)
        {
            worker
                .commit_fetched_page(&id, PageIndex(0), 32, Bytes::from_static(&[1; 16]))
                .await
                .unwrap();
        }
        let mut gc = fixture.gc_managed(&worker);
        let late = placed("b", 0);
        fill(&worker, &late, true).await;
        gc.run_round(&worker).await;
        let range = gc.completed_limit..worker.index.ownership_admission_ceiling();
        assert!(!range.is_empty());
        assert!(!worker.index.ownership_changes_cover(&range));

        let packed = packed_placed("b", 0);
        let small = worker.small.as_ref().unwrap();
        small
            .put(&packed, Bytes::from_static(b"packed"))
            .await
            .unwrap();
        gc.next_supplement = tokio::time::Instant::now();
        gc.run_slice(&worker).await;
        let pass = gc.pass.as_ref().unwrap();
        assert_eq!(pass.range, range);
        let OwnershipScan::Occupied(slots) = &pass.cursor else {
            panic!("uncovered supplement must scan occupied slots");
        };
        assert!(slots.start > 0 && !slots.is_empty());
        let cursor = slots.clone();
        assert!(small.contains(&packed));

        gc.run_slice(&worker).await;
        assert!(!small.contains(&packed));
        let pass = gc.pass.as_ref().unwrap();
        assert!(matches!(&pass.cursor, OwnershipScan::Occupied(slots) if slots == &cursor));
        assert!(pass.scanning);
        assert!(worker.index.get(&late).is_some());
        gc.run_round(&worker).await;
        assert!(worker.index.get(&late).is_none());
        assert_eq!(gc.completed_limit, range.end);
        assert!(gc.pass.is_none());
    }

    #[tokio::test]
    async fn small_ownership_gc_uses_store_residency_and_respects_policy_pins() {
        let root = tempfile::tempdir().unwrap();
        let (worker, fixture) = namespace_runtime(root.path(), false);
        let owned = packed_placed("a", 0);
        let unowned = packed_placed("b", 0);
        for id in [&owned, &unowned] {
            worker
                .commit_cached_block(id, Bytes::from_static(b"packed"))
                .await
                .unwrap();
        }
        assert!(worker.index.is_empty());
        assert_eq!(worker.block_count(), 2);
        let pin = worker.lru.pin_guard(CacheUnit::Whole(unowned.clone()));
        fixture.publish_managed(&worker);
        let mut gc = OwnershipGc::new(&fixture.config);
        gc.run_round(&worker).await;
        assert!(worker.contains_block(&unowned));
        drop(pin);
        gc.run_round(&worker).await;
        assert!(!worker.contains_block(&unowned));
        assert!(worker.contains_block(&owned));
        assert_eq!(worker.block_count(), 1);
        assert!(worker.lru.block_candidates(&unowned).is_empty());
        assert_eq!(
            worker
                .small
                .as_ref()
                .unwrap()
                .read(&owned)
                .await
                .unwrap()
                .unwrap(),
            b"packed"[..]
        );
    }

    #[tokio::test]
    async fn packed_ownership_first_attempt_is_revoked_by_immutable_put() {
        let root = tempfile::tempdir().unwrap();
        let (worker, fixture) = namespace_runtime_with_config(root.path(), false, |config| {
            config.background_delete_max_per_sec = 1;
            config.background_io_max_mb_per_sec = 0;
        });
        let budget = worker.background_budget.as_ref().unwrap();
        let payload = Bytes::from_static(b"packed");
        let put = talon_transport::data::CachedBlockPutRequest {
            block: packed_placed("b", 0),
            object_len: payload.len() as u64,
            body_len: payload.len() as u64,
        };
        worker
            .admit_cached_block(&put, payload.clone())
            .await
            .unwrap();
        let small = worker.small.as_ref().unwrap();
        let mut gc = fixture.gc_managed(&worker);
        let generation = worker.membership_snapshot().unwrap().generation;
        tokio::time::pause();
        gc.run_slice(&worker).await;
        {
            let round = gc.run_round(&worker);
            tokio::pin!(round);
            assert!(futures::poll!(&mut round).is_pending());
            assert_eq!(budget.charged_deletes(), 1);
            assert_eq!(worker.page_gc_metrics.ownership_attempted.get(), 0);
            worker
                .admit_cached_block(&put, payload.clone())
                .await
                .unwrap();
            tokio::time::advance(Duration::from_secs(2)).await;
            round.await;
        }

        assert_eq!(worker.page_gc_metrics.ownership_attempted.get(), 1);
        assert_eq!(
            small.ownership_retry_count(),
            0,
            "the PUT revoked this work"
        );
        assert_eq!(gc.completed_generation, generation);
        assert!(gc.pass.is_none());
        assert_eq!(
            small.read(&put.block).await.unwrap().as_ref(),
            Some(&payload)
        );
    }

    #[tokio::test]
    async fn packed_ownership_retry_does_not_reregister_after_immutable_put() {
        let root = tempfile::tempdir().unwrap();
        let (worker, fixture) = namespace_runtime_with_config(root.path(), false, |config| {
            config.background_delete_max_per_sec = 1;
            config.background_io_max_mb_per_sec = 0;
        });
        let budget = worker.background_budget.as_ref().unwrap();
        let payload = Bytes::from_static(b"packed");
        let puts = [0, 1].map(|n| talon_transport::data::CachedBlockPutRequest {
            block: packed_placed("b", n),
            object_len: payload.len() as u64,
            body_len: payload.len() as u64,
        });
        let small = worker.small.as_ref().unwrap();
        for put in &puts {
            worker
                .admit_cached_block(put, payload.clone())
                .await
                .unwrap();
        }
        let pins = puts.each_ref().map(|put| {
            worker
                .lru
                .pin_guard(CacheUnit::Whole(put.block.clone()))
                .unwrap()
        });
        let mut gc = fixture.gc_managed(&worker);
        let generation = worker.membership_snapshot().unwrap().generation;
        gc.run_round(&worker).await;
        assert_eq!(small.ownership_retry_count(), 2);
        gc.run_round(&worker).await;
        assert_eq!(
            small.ownership_retry_count(),
            2,
            "current pinned retries remain indexed"
        );
        drop(pins);

        tokio::time::pause();
        // Finish the ordinary retry slice before polling the packed attempt.
        gc.run_slice(&worker).await;
        {
            let round = gc.run_round(&worker);
            tokio::pin!(round);
            assert!(futures::poll!(&mut round).is_pending());
            assert_eq!(
                budget.charged_deletes(),
                1,
                "retry reached deletion credit after observation"
            );
            assert_eq!(worker.page_gc_metrics.ownership_attempted.get(), 0);
            // Re-admit the observed retry and the queued, unobserved retry.
            for put in &puts {
                worker
                    .admit_cached_block(put, payload.clone())
                    .await
                    .unwrap();
            }
            assert_eq!(small.ownership_retry_count(), 0);
            tokio::time::advance(Duration::from_secs(2)).await;
            round.await;
        }

        assert_eq!(small.ownership_retry_count(), 0);
        for put in &puts {
            assert_eq!(
                small.read(&put.block).await.unwrap().as_ref(),
                Some(&payload)
            );
        }
        assert_eq!(worker.page_gc_metrics.ownership_attempted.get(), 1);
        assert_eq!(gc.completed_generation, generation);
        assert!(gc.pass.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn packed_ownership_unlink_defers_while_catalog_writer_is_held() {
        let root = tempfile::tempdir().unwrap();
        let (worker, fixture) = namespace_runtime(root.path(), false);
        let id = packed_placed("b", 0);
        let small = worker.small.as_ref().unwrap();
        small.put(&id, Bytes::from_static(b"packed")).await.unwrap();
        fixture.publish_managed(&worker);
        let membership = worker.membership_snapshot().unwrap();
        let limit = membership.scan_limits[&worker.cache_identity]
            .small
            .unwrap();
        drop(small.take_superseded_retries(limit));
        let (_, mut candidates) =
            small.ownership_candidates(&mut OwnershipCursor::default(), 0, limit, 64, |_| false);
        let mut candidate = candidates.pop().unwrap();
        let revision = candidate.observe_ownership(limit.epoch).unwrap();
        let state = worker.page_lifecycle.block(&id);
        let budget = worker.background_budget.as_ref().unwrap();
        let io_permits = budget.available_io();
        let handle = tokio::runtime::Handle::current();
        small.under_catalog_writer(|| {
            use futures::FutureExt;
            handle.block_on(async {
                assert!(worker.membership_is_current(&membership));
                worker
                    .unlink_small_gc(candidate.clone(), revision, Some(membership.clone()))
                    .await;
                assert!(worker.membership_is_current(&membership));
                assert!(state.lock().now_or_never().is_some());
                assert_eq!(budget.available_io(), io_permits);
            });
        });
        assert!(small.contains(&id));
        // The same watch and membership can authorize once the writer releases.
        worker
            .unlink_small_gc(candidate, revision, Some(membership))
            .await;
        assert!(!small.contains(&id));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn packed_ownership_unlink_defers_while_identity_registry_is_held() {
        let root = tempfile::tempdir().unwrap();
        let (worker, fixture) = namespace_runtime(root.path(), false);
        let id = packed_placed("b", 0);
        let small = worker.small.as_ref().unwrap();
        small.put(&id, Bytes::from_static(b"packed")).await.unwrap();
        fixture.publish_managed(&worker);
        let membership = worker.membership_snapshot().unwrap();
        let limit = membership.scan_limits[&worker.cache_identity]
            .small
            .unwrap();
        drop(small.take_superseded_retries(limit));
        let (_, mut candidates) =
            small.ownership_candidates(&mut OwnershipCursor::default(), 0, limit, 64, |_| false);
        let mut candidate = candidates.pop().unwrap();
        let revision = candidate.observe_ownership(limit.epoch).unwrap();
        let state = worker.page_lifecycle.block(&id);
        let budget = worker.background_budget.as_ref().unwrap();
        let io_permits = budget.available_io();
        let handle = tokio::runtime::Handle::current();
        small.under_mutation_registry(|| {
            use futures::FutureExt;
            handle.block_on(async {
                assert!(worker.membership_is_current(&membership));
                worker
                    .unlink_small_gc(candidate.clone(), revision, Some(membership.clone()))
                    .await;
                assert!(worker.membership_is_current(&membership));
                assert!(state.lock().now_or_never().is_some());
                assert_eq!(budget.available_io(), io_permits);
            });
        });
        assert!(small.contains(&id));
        worker
            .unlink_small_gc(candidate, revision, Some(membership))
            .await;
        assert!(!small.contains(&id));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rejected_ownership_unlink_returns_io_before_releasing_claim() {
        let root = tempfile::tempdir().unwrap();
        let (worker, fixture) = namespace_runtime_with_config(root.path(), true, |config| {
            config.background_io_concurrency = 1;
        });
        let id = placed("b", 0);
        fill(&worker, &id, true).await;
        let allocated = worker.resident_bytes();
        fixture.publish_managed(&worker);
        let membership = worker.membership_snapshot().unwrap();
        let candidate = worker
            .lru
            .first_ownership_candidate(0..worker.lru.ownership_admission_ceiling())
            .unwrap();
        let page = worker
            .paged
            .as_ref()
            .unwrap()
            .catalog_dir_for(&id)
            .unwrap()
            .join("0.page");
        let (dropping, resume) = worker.index.page_work.pause_next_block_claim_drop();
        let handle = tokio::runtime::Handle::current();
        let unlinked = tokio::task::block_in_place(|| {
            // Force rejection at the last membership check, after acquisition
            // of the claim. Hold the operation stripe during claim cleanup.
            let _membership_writer = worker.membership.lock().unwrap();
            handle.block_on(async {
                let runtime = worker.default_runtime().clone();
                let unlink = tokio::spawn(async move {
                    runtime
                        .unlink_stale_block_for(candidate, Some(membership))
                        .await
                });
                let paused = tokio::time::timeout(Duration::from_secs(2), dropping).await;
                let free_slots = worker.background_io_slots().available_permits();
                let _ = resume.send(());
                paused.unwrap().unwrap();
                assert_eq!(free_slots, 1);
                unlink.await.unwrap()
            })
        });
        assert!(!unlinked);
        assert_eq!(std::fs::read(page).unwrap(), [1; 16]);
        assert_eq!(worker.logical_resident_bytes(), 32);
        assert_eq!(worker.resident_bytes(), allocated);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ownership_callbacks_release_io_while_ordinary_directory_writer_is_held() {
        let mut observations = Vec::new();
        for (packed, hold_gate) in [(true, true), (false, true), (false, false)] {
            let root = tempfile::tempdir().unwrap();
            let (worker, fixture) = namespace_runtime(root.path(), true);
            let small = worker.small.as_ref().unwrap();
            let id = if packed {
                packed_placed("b", 0)
            } else {
                placed("b", 0)
            };
            if packed {
                small.put(&id, Bytes::from_static(b"packed")).await.unwrap();
            } else {
                fill(&worker, &id, true).await;
            }
            fixture.publish_managed(&worker);
            let mut packed_attempt = None;
            let mut ordinary_attempt = None;
            let path = if packed {
                let membership = worker.membership_snapshot().unwrap();
                let limit = membership.scan_limits[&worker.cache_identity]
                    .small
                    .unwrap();
                drop(small.take_superseded_retries(limit));
                let (_, mut selected) = small.ownership_candidates(
                    &mut OwnershipCursor::default(),
                    0,
                    limit,
                    64,
                    |_| false,
                );
                let mut candidate = selected.pop().unwrap();
                let revision = candidate.observe_ownership(limit.epoch).unwrap();
                packed_attempt = Some((candidate, revision));
                None
            } else {
                ordinary_attempt = worker
                    .lru
                    .first_ownership_candidate(0..worker.lru.ownership_admission_ceiling());
                assert!(ordinary_attempt.is_some());
                Some(
                    worker
                        .paged
                        .as_ref()
                        .unwrap()
                        .catalog_dir_for(&id)
                        .unwrap()
                        .join("0.page"),
                )
            };
            let gate = worker.page_lifecycle.block(&id);
            let gate = if hold_gate {
                Some(gate.lock().await)
            } else {
                None
            };
            let budget = worker.background_budget.as_ref().unwrap();
            let full = budget.available_io();
            let (held, ready) = tokio::sync::oneshot::channel();
            let (release, released) = std::sync::mpsc::channel();
            let index = worker.index.clone();
            let holder = std::thread::spawn(move || {
                index.with_directory_writer(|_| {
                    held.send(()).unwrap();
                    released.recv().unwrap();
                });
            });
            ready.await.unwrap();

            let expires = Instant::now() + Duration::from_millis(250);
            let view = {
                let mut membership = worker.membership.lock().unwrap();
                Arc::make_mut(membership.as_mut().unwrap()).valid_until = expires;
                membership.as_ref().unwrap().clone()
            };
            let invoke = |view| {
                let worker = worker.clone();
                let packed_attempt = packed_attempt.clone();
                let ordinary_attempt = ordinary_attempt.clone();
                async move {
                    if let Some((candidate, revision)) = packed_attempt {
                        worker
                            .unlink_small_gc(candidate, revision, Some(view))
                            .await;
                    } else {
                        worker
                            .unlink_stale_block_for(ordinary_attempt.unwrap(), Some(view))
                            .await;
                    }
                }
            };
            let callback = tokio::spawn(invoke(view));
            let borrowed = tokio::time::timeout_at(expires.into(), async {
                if hold_gate {
                    while budget.available_io() == full {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            })
            .await
            .is_ok();
            tokio::time::sleep_until((expires + Duration::from_millis(100)).into()).await;
            let observed = (borrowed, callback.is_finished(), budget.available_io());
            let resident = if let Some(path) = &path {
                path.exists()
            } else {
                small.contains(&id)
            };
            release.send(()).unwrap();
            holder.join().unwrap();
            drop(gate);
            callback.await.unwrap();
            worker.page_mutations.drain().await;

            println!("packed={packed} gate={hold_gate} held_writer={observed:?} expected_full={full:?} resident={resident}");
            observations.push((packed, hold_gate, observed, full, resident));
            fixture.publish_managed(&worker);
            invoke(worker.membership_snapshot().unwrap()).await;
            assert!(!path
                .as_ref()
                .map_or_else(|| small.contains(&id), |path| path.exists()));
        }
        for (packed, hold_gate, observed, full, resident) in observations {
            assert!(resident, "expired membership must leave the payload intact");
            assert_eq!(
                observed,
                (true, true, full),
                "packed={packed} gate={hold_gate}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn packed_ownership_expires_while_shared_mutation_registry_is_held() {
        let root = tempfile::tempdir().unwrap();
        let (worker, fixture) = namespace_runtime(root.path(), true);
        let id = packed_placed("b", 0);
        let small = worker.small.as_ref().unwrap();
        small.put(&id, Bytes::from_static(b"packed")).await.unwrap();
        let mut gc = fixture.gc_managed(&worker);
        let budget = worker.background_budget.as_ref().unwrap();
        let io_permits = budget.available_io();
        let (held, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        let lifecycle = worker.page_lifecycle.clone();
        let holder = tokio::task::spawn_blocking(move || {
            lifecycle.under_mutation_registry(|| {
                held.send(()).unwrap();
                let _ = wait.blocking_recv();
            });
        });
        ready.await.unwrap();

        let expires = Instant::now() + Duration::from_millis(500);
        {
            let mut membership = worker.membership.lock().unwrap();
            Arc::make_mut(membership.as_mut().unwrap()).valid_until = expires;
        }
        let finished = {
            let round = gc.run_round(&worker);
            tokio::pin!(round);
            let borrowed = async {
                while budget.available_io() == io_permits {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            };
            tokio::select! {
                result = tokio::time::timeout_at(expires.into(), borrowed) => result.unwrap(),
                _ = &mut round => panic!("ownership returned before reaching the shared registry"),
            }
            let result =
                tokio::time::timeout_at((expires + Duration::from_secs(1)).into(), round).await;
            (
                result.is_ok(),
                worker.membership_snapshot().is_none(),
                budget.available_io(),
            )
        };
        assert!(small.contains(&id), "unlink must not run under contention");
        release.send(()).unwrap();
        holder.await.unwrap();
        worker.page_mutations.drain().await;
        assert_eq!(
            finished,
            (true, true, io_permits),
            "expired ownership callback must release I/O before the registry unlocks"
        );
        assert_eq!(small.ownership_retry_count(), 1);
        assert_eq!(worker.page_gc_metrics.ownership_attempted.get(), 1);

        fixture.publish_managed(&worker);
        gc.run_round(&worker).await;
        // Renewal lets the interrupted discovery round finish. The next
        // scheduled round uses the indexed retry after the usual delay.
        assert_eq!(gc.next_delay(&worker), Some(Duration::from_secs(1)));
        assert!(small.contains(&id));
        assert_eq!(worker.page_gc_metrics.ownership_attempted.get(), 1);
        gc.run_round(&worker).await;
        assert_eq!(small.ownership_retry_count(), 0);
        assert_eq!(worker.page_gc_metrics.ownership_attempted.get(), 2);
        assert!(!small.contains(&id));
    }

    #[tokio::test]
    async fn packed_ownership_can_reclaim_a_late_exact_range_without_retiring_pages() {
        let root = tempfile::tempdir().unwrap();
        let (worker, fixture) = namespace_runtime(root.path(), true);
        let small = worker.small.as_ref().unwrap();
        let original = packed_placed("b", 0);
        let payload = Bytes::from_static(b"packed");
        small.put(&original, payload.clone()).await.unwrap();
        let mut gc = fixture.gc_managed(&worker);
        // Re-admission leaves the segment eligible for discovery of later rows.
        small.put(&original, payload).await.unwrap();
        let paged = placed("b", 0);
        fill(&worker, &paged, true).await;
        let exact = CacheUnit::ExactRange(paged.clone());
        small
            .put(&exact, Bytes::from_static(&[1; 32]))
            .await
            .unwrap();

        gc.run_round(&worker).await;
        assert!(!small.contains(&original));
        assert!(!small.contains(&exact));
        assert_eq!(worker.index.policy_block_get(&paged).unwrap().units, 2);
    }

    #[tokio::test]
    async fn ownership_discovers_unaccounted_journal_deletes_before_and_after_selection() {
        for selected in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let (worker, fixture) = namespace_runtime(root.path(), false);
            let foreign = packed_placed("b", 0);
            let small = worker.small.as_ref().unwrap();
            let payload = Bytes::from_static(b"packed");
            small.put(&foreign, payload.clone()).await.unwrap();
            small.maintain(true).await.unwrap();
            let mut collector = OwnershipGc::new(&fixture.config);
            if selected {
                let pin = worker
                    .lru
                    .pin_guard(CacheUnit::Whole(foreign.clone()))
                    .unwrap();
                fixture.publish_managed(&worker);
                collector.run_round(&worker).await;
                assert_eq!(small.ownership_retry_count(), 1);
                drop(pin);
            }
            let live = worker.lru.total_bytes();
            let store_charge = small.stats().unwrap().live_bytes;
            small.fail_next_delete_after_journal(&foreign);
            assert_eq!(
                small.delete(&foreign).await.unwrap_err().to_string(),
                "injected failure after journal Delete"
            );
            assert_eq!(small.read(&foreign).await.unwrap(), Some(payload.clone()));
            if !selected {
                fixture.publish_managed(&worker);
            }
            let generation = worker.membership_snapshot().unwrap().generation;
            for _ in 0..2 {
                collector.run_round(&worker).await;
                assert_eq!(small.ownership_retry_count(), 1);
                assert_ne!(collector.completed_generation, generation);
                assert_eq!(collector.next_delay(&worker), Some(Duration::from_secs(1)));
                assert_eq!(worker.lru.total_bytes(), live);
                assert_eq!(small.stats().unwrap().live_bytes, store_charge);
                assert_eq!(worker.block_count(), 1);
                assert!(small.contains(&foreign));
            }
            drop(worker);
            let (reopened, _) = namespace_runtime(root.path(), false);
            assert!(!reopened.contains_block(&foreign));
        }
    }

    #[tokio::test]
    async fn ownership_defers_poisoned_stream_with_bounded_tickets() {
        let root = tempfile::tempdir().unwrap();
        let (worker, fixture) = namespace_runtime(root.path(), false);
        let small = worker.small.as_ref().unwrap();
        let ids: Vec<_> = (0..140).map(|n| packed_placed("b", n)).collect();
        for id in &ids {
            small.put(id, Bytes::from_static(b"packed")).await.unwrap();
        }
        small.fail_next_delete_after_journal(&ids[0]);
        assert!(small.delete(&ids[0]).await.is_err());
        let charge = worker.logical_resident_bytes();
        let mut collector = fixture.gc_managed(&worker);
        for _ in 0..2 {
            collector.run_round(&worker).await;
            assert_eq!(small.ownership_retry_count(), ids.len());
            // The failed tombstone itself is still charged. All remaining
            // retries must release their temporary tickets between slices.
            assert_eq!(small.live_ticket_count(), 1);
            assert_eq!(collector.next_delay(&worker), Some(Duration::from_secs(1)));
            assert_eq!(worker.logical_resident_bytes(), charge);
            assert!(ids.iter().all(|id| small.contains(id)));
        }
    }

    async fn remove_large_packed_row(worker: &WorkerRuntime) {
        let small = worker.small.as_ref().unwrap();
        let garbage = BlockId::new(
            ObjectId::new(Backend::S3, "bucket", "garbage"),
            0,
            128 << 10,
            Version::new("v1"),
        );
        small
            .put(&garbage, Bytes::from(vec![1; 64 << 10]))
            .await
            .unwrap();
        small.delete(&garbage).await.unwrap();
        worker.lru.remove(&CacheUnit::Whole(garbage));
    }

    #[tokio::test]
    async fn ownership_deduplicates_protected_rows_after_mid_scan_relocation() {
        let root = tempfile::tempdir().unwrap();
        let (worker, fixture) = namespace_runtime(root.path(), false);
        let small = worker.small.as_ref().unwrap();
        let protected: Vec<_> = (0..4).map(|n| packed_placed("b", n)).collect();
        let owned: Vec<_> = (0..70).map(|n| packed_placed("a", n)).collect();
        let tail = packed_placed("b", 4);
        remove_large_packed_row(&worker).await;
        for id in protected.iter().chain(&owned).chain([&tail]) {
            small.put(id, Bytes::from_static(b"packed")).await.unwrap();
        }
        let mut pins: Vec<_> = protected
            .iter()
            .map(|id| worker.lru.pin_guard(CacheUnit::Whole(id.clone())).unwrap())
            .collect();
        let mut collector = fixture.gc_managed(&worker);
        collector.run_slice(&worker).await;
        collector.run_slice(&worker).await;
        assert_eq!(small.ownership_retry_count(), protected.len());
        assert!(small.contains(&tail));
        let source = small
            .lookup_entry(&protected[0])
            .unwrap()
            .unwrap()
            .segment();
        assert!(small.maintain(true).await.unwrap());
        assert_ne!(
            small
                .lookup_entry(&protected[0])
                .unwrap()
                .unwrap()
                .segment(),
            source
        );
        collector.run_round(&worker).await;
        assert_eq!(small.ownership_retry_count(), protected.len());
        assert!(!small.contains(&tail));

        // Re-admission at a moved offset belongs to the next epoch.
        small
            .put(&protected[0], Bytes::from_static(b"packed"))
            .await
            .unwrap();
        drop(pins.remove(1));
        small.delete(&protected[1]).await.unwrap();
        worker.lru.remove(&CacheUnit::Whole(protected[1].clone()));
        small
            .put(&protected[1], Bytes::from_static(b"refill"))
            .await
            .unwrap();
        assert_eq!(small.ownership_retry_count(), protected.len() - 2);
        drop(pins);
        collector.run_round(&worker).await;
        assert!(small.contains(&protected[0]));
        assert_eq!(
            small.read(&protected[1]).await.unwrap(),
            Some(Bytes::from_static(b"refill"))
        );
        assert!(protected[2..].iter().all(|id| !small.contains(id)));
        assert!(owned.iter().all(|id| small.contains(id)));
        assert_eq!(small.ownership_retry_count(), 0);
        collector.next_supplement = tokio::time::Instant::now();
        collector.run_round(&worker).await;
        assert!(!small.contains(&protected[0]));
        assert!(!small.contains(&protected[1]));
    }

    #[tokio::test]
    async fn packed_membership_capture_and_count_bypass_catalog_writer() {
        let root = tempfile::tempdir().unwrap();
        let (worker, fixture) = namespace_runtime(root.path(), false);
        let small = worker.small.as_ref().unwrap();
        let id = packed_placed("b", 0);
        small.put(&id, Bytes::from_static(b"packed")).await.unwrap();
        let view = small.under_catalog_writer(|| {
            assert_eq!(worker.block_count(), 1);
            fixture.publish_managed(&worker);
            worker.membership_snapshot().unwrap()
        });

        let pin = worker.lru.pin_guard(CacheUnit::Whole(id.clone())).unwrap();
        let mut gc = OwnershipGc::new(&fixture.config);
        gc.run_round(&worker).await;
        assert_eq!(small.ownership_retry_count(), 1);
        small
            .put(&packed_placed("b", 1), Bytes::from_static(b"late"))
            .await
            .unwrap();
        let old = view.scan_limits[&worker.cache_identity].small.unwrap();
        let (_, candidates) =
            small.ownership_candidates(&mut OwnershipCursor::default(), 0, old, 64, |_| false);
        let mut old_candidate = candidates
            .into_iter()
            .find(|candidate| candidate.unit.block() == &id)
            .unwrap();
        let new = small
            .under_catalog_writer(|| worker.capture_supplemental_limits(&view))
            .unwrap()
            .small
            .unwrap();
        let detached = small.take_superseded_retries(new).unwrap();
        assert!(worker.membership.try_lock().is_ok());
        assert!(small.take_superseded_retries(old).is_none());
        assert!(old_candidate.observe_ownership(old.epoch).is_none());
        assert_eq!(small.ownership_retry_count(), 0);
        drop(detached);
        drop(pin);
    }

    #[tokio::test]
    async fn compact_ownership_cancels_a_selected_unlink_when_membership_changes() {
        let root = tempfile::tempdir().unwrap();
        let (worker, fixture) = namespace_runtime(root.path(), false);
        let foreign = packed_placed("b", 0);
        worker
            .commit_cached_block(&foreign, Bytes::from_static(b"packed"))
            .await
            .unwrap();
        let permit = worker
            .background_io_slots()
            .clone()
            .acquire_many_owned(worker.background_io_slots().available_permits() as u32)
            .await
            .unwrap();
        let mut gc = fixture.gc_managed(&worker);
        let run = gc.run_round(&worker);
        let change = async {
            while worker.page_mutations.active_count() == 0 {
                tokio::task::yield_now().await;
            }
            *fixture.members.lock().unwrap() = Some(vec!["a"]);
            fixture.publish_managed(&worker);
            drop(permit);
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(run, change);
        })
        .await
        .unwrap();
        gc.run_round(&worker).await;
        assert!(worker.small.as_ref().unwrap().contains(&foreign));
        assert!(gc.pass.is_none());
    }

    #[tokio::test]
    async fn small_only_supplement_visits_no_ordinary_slots_even_during_retries() {
        let root = tempfile::tempdir().unwrap();
        let (worker, fixture) = namespace_runtime(root.path(), true);
        let ordinary = [placed("a", 0), placed("a", 1)];
        for id in &ordinary {
            fill(&worker, id, true).await;
        }
        let mut gc = fixture.gc_managed(&worker);
        gc.run_round(&worker).await;
        assert!(gc.pass.is_none());
        assert_eq!(worker.lru.block_scan_ceiling(), ordinary.len());
        assert_eq!(worker.page_gc_metrics.ownership_checked.get(), 2);
        let limit = gc.completed_limit;

        let late = packed_placed("b", 0);
        worker
            .commit_cached_block(&late, Bytes::from_static(b"packed"))
            .await
            .unwrap();
        let small = worker.small.as_ref().unwrap();
        let pin = worker
            .lru
            .pin_guard(CacheUnit::Whole(late.clone()))
            .unwrap();
        assert_eq!(worker.lru.ownership_admission_ceiling(), limit);
        gc.next_supplement = tokio::time::Instant::now();
        for _ in 0..2 {
            gc.run_slice(&worker).await;
            let pass = gc.pass.as_ref().unwrap();
            assert_eq!(pass.range, limit..limit);
            assert!(
                matches!(&pass.cursor, OwnershipScan::Occupied(slots) if slots.is_empty()),
                "no ordinary slot visits"
            );
            assert!(
                gc.round_ended,
                "the Small retry round finished in one slice"
            );
            assert_eq!(small.ownership_retry_count(), 1);
        }
        assert_eq!(worker.page_gc_metrics.ownership_checked.get(), 3);

        drop(pin);
        gc.run_round(&worker).await;
        assert!(!small.contains(&late));
        assert!(gc.pass.is_none());
        assert_eq!(gc.completed_limit, limit);
        assert!(ordinary.iter().all(|id| worker.contains_block(id)));
    }

    #[tokio::test]
    async fn superseded_supplement_leaves_post_event_compact_admission_pending() {
        let root = tempfile::tempdir().unwrap();
        let (worker, fixture) = namespace_runtime(root.path(), false);
        *fixture.members.lock().unwrap() = Some(vec!["a"]);
        let mut gc = fixture.gc_managed(&worker);
        gc.run_round(&worker).await;
        gc.next_supplement = tokio::time::Instant::now();
        let old_view = gc.prepare_ownership(&worker).unwrap();

        *fixture.members.lock().unwrap() = Some(vec!["a", "b"]);
        fixture.publish_managed(&worker);
        let late = packed_placed("b", 0);
        worker
            .commit_cached_block(&late, Bytes::from_static(b"late"))
            .await
            .unwrap();
        gc.run_slice_for_membership(&worker, old_view).await;

        gc.run_round(&worker).await;
        assert!(
            worker.contains_block(&late),
            "the event excludes the late fill"
        );
        gc.next_supplement = tokio::time::Instant::now();
        gc.run_round(&worker).await;
        assert!(!worker.contains_block(&late));
    }

    #[tokio::test]
    async fn compact_ownership_captures_recovered_rows_at_the_first_membership() {
        let root = tempfile::tempdir().unwrap();
        let id = packed_placed("b", 0);
        {
            let (worker, _) = namespace_runtime(root.path(), false);
            worker
                .commit_cached_block(&id, Bytes::from_static(b"packed"))
                .await
                .unwrap();
            worker.small.as_ref().unwrap().maintain(true).await.unwrap();
        }
        let (worker, fixture) = namespace_runtime(root.path(), false);
        let small = worker.small.as_ref().unwrap();
        assert!(small.contains(&id));
        assert!(worker.index.policy_block_get(&id).is_none());
        assert_eq!(
            small.read(&id).await.unwrap(),
            Some(Bytes::from_static(b"packed"))
        );
        let mut gc = fixture.gc_managed(&worker);
        gc.run_round(&worker).await;
        assert!(!small.contains(&id));
    }

    #[tokio::test]
    async fn namespaces_share_membership_but_keep_independent_ownership_boundaries() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let (worker, fixture) = namespace_runtime(root.path(), paged);
            let named = worker
                .for_namespace(&talon_core::NamespaceId::named("analytics"))
                .await
                .unwrap();
            let owned = placed("a", 0);
            let unowned = placed("b", 0);
            fill(&worker, &unowned, paged).await;
            fill(&named, &owned, paged).await;
            fill(&named, &unowned, paged).await;
            fixture.publish_managed(&worker);
            let original = worker.membership_snapshot().unwrap();
            assert!(Arc::ptr_eq(
                &original,
                &named.membership_snapshot().unwrap()
            ));
            assert_ne!(
                original.scan_limits["default"],
                original.scan_limits["analytics"]
            );
            let late_fill = placed("b", 1);
            fill(&named, &late_fill, paged).await;
            let late_range =
                original.scan_limits["analytics"].block..named.index.ownership_admission_ceiling();
            assert!(!late_range.is_empty());
            worker.update_membership(None, Instant::now()).unwrap();
            assert!(named.membership_snapshot().is_none());
            let mut default_gc = OwnershipGc::new(&fixture.config);
            let mut named_gc = OwnershipGc::new(&fixture.config);
            named_gc.run_round(&named).await;
            assert!(named.index.get(&unowned).is_some());
            fixture.publish_managed(&worker);
            assert_eq!(
                named.membership_snapshot().unwrap().scan_limits,
                original.scan_limits
            );
            default_gc.run_round(&worker).await;
            named_gc.run_round(&named).await;
            assert!(worker.index.is_empty());
            assert!(named.index.get(&unowned).is_none());
            assert!(named.index.get(&owned).is_some());
            assert!(named.index.get(&late_fill).is_some());
            // GC started after the late fill. The event keeps its published
            // cutoff and the supplement must discover the uncovered revision.
            assert_eq!(named_gc.completed_limit, late_range.start);
            assert!(!named.index.ownership_changes_cover(&late_range));
            let late = worker
                .for_namespace(&talon_core::NamespaceId::named("late"))
                .await
                .unwrap();
            assert!(late.membership_snapshot().is_some());
            fill(&late, &unowned, paged).await;
            let mut late_gc = OwnershipGc::new(&fixture.config);
            late_gc.run_round(&late).await;
            assert!(late.index.get(&unowned).is_some());
            named_gc.next_supplement = tokio::time::Instant::now();
            late_gc.next_supplement = tokio::time::Instant::now();
            named_gc.run_round(&named).await;
            late_gc.run_round(&late).await;
            assert!(named.index.get(&late_fill).is_none());
            assert!(named.index.get(&owned).is_some());
            assert!(late.index.is_empty());
            let next_fill = placed("b", 2);
            fill(&named, &next_fill, paged).await;
            let fresh_range = late_range.end..named.index.ownership_admission_ceiling();
            assert!(named.index.ownership_changes_cover(&fresh_range));
            named_gc.next_supplement = tokio::time::Instant::now();
            named_gc.run_round(&named).await;
            assert!(named.index.get(&next_fill).is_none());
            assert!(!named.index.ownership_changes_cover(&fresh_range));
        }
    }

    #[tokio::test]
    async fn namespace_restart_respects_ownership_replica_retention() {
        let nodes = members();
        let table = CachePlacementTable::new(
            &nodes
                .iter()
                .map(|node| node.info.clone())
                .collect::<Vec<_>>(),
        );
        let id = (0..10000)
            .map(block)
            .find(|id| table.primary(id).unwrap().id.0 != "a")
            .unwrap();
        for paged in [false, true] {
            for replicas in [1, 3] {
                let root = tempfile::tempdir().unwrap();
                let (worker, mut fixture) = namespace_runtime(root.path(), paged);
                fixture.config.ownership_gc_replicas = replicas;
                let named = worker
                    .for_namespace(&talon_core::NamespaceId::named("analytics"))
                    .await
                    .unwrap();
                fill(&worker, &id, paged).await;
                fill(&named, &id, paged).await;
                drop(named);
                drop(worker);

                let lock = Arc::new(
                    crate::page_access_store::CacheRootLock::acquire(root.path()).unwrap(),
                );
                let restarted = Arc::new(
                    Worker::open_namespaces(
                        &fixture.config,
                        lock,
                        Arc::new(Origin::default()),
                        WorkerMetrics::new(0),
                    )
                    .unwrap(),
                );
                restarted
                    .update_membership(Some(discovery(&nodes)), Instant::now())
                    .unwrap();
                for namespace in restarted.namespace_runtimes() {
                    assert!(namespace.index.get(&id).is_some(), "L2 must recover first");
                    let mut gc = OwnershipGc::new(&fixture.config);
                    gc.run_round(&namespace).await;
                    assert_eq!(
                        namespace.index.get(&id).is_some(),
                        replicas == 3,
                        "startup GC must retain every configured placement candidate"
                    );
                    let on_disk = if let Some(paged) = &namespace.paged {
                        paged.scan().unwrap()
                    } else {
                        namespace.store.scan().unwrap()
                    };
                    assert_eq!(on_disk.len(), usize::from(replicas == 3));
                }
            }
        }
    }

    #[tokio::test]
    async fn background_ownership_gc_includes_existing_and_new_namespaces() {
        let root = tempfile::tempdir().unwrap();
        let (worker, fixture) = namespace_runtime(root.path(), false);
        let named = worker
            .for_namespace(&talon_core::NamespaceId::named("analytics"))
            .await
            .unwrap();
        let unowned = placed("b", 0);
        fill(&named, &unowned, false).await;
        fixture.publish_managed(&worker);
        let service = super::super::WorkerBackground::start(worker.clone(), &fixture.config);
        let completed = worker.metrics.registry.counter(
            "talon_worker_background_task_completed_total",
            "Completed background batches by task.",
            talon_core::metrics::labels(&[("task", "ownership_gc")]),
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            while completed.get() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(named.index.is_empty());
        let before = completed.get();
        let late = worker
            .for_namespace(&talon_core::NamespaceId::named("late"))
            .await
            .unwrap();
        fill(&late, &unowned, false).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while completed.get() == before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            late.index.get(&unowned).is_some(),
            "post-observation fills wait for the supplement"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
        tokio::time::pause();
        tokio::time::advance(SUPPLEMENT_INTERVAL + Duration::from_secs(1)).await;
        tokio::time::resume();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !late.index.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        service.shutdown().await;
    }

    #[tokio::test]
    async fn hourly_supplement_reclaims_late_fills_and_refills_without_rehashing_unchanged_blocks()
    {
        for paged in [false, true] {
            let fixture = MembershipFixture::new();
            let root = tempfile::tempdir().unwrap();
            let r = runtime(root.path(), paged, Arc::new(Origin::default()));
            let owned = placed("a", 0);
            let refilled = placed("b", 0);
            let rewritten = placed("b", 1);
            let new = placed("b", 2);
            for id in [&owned, &refilled, &rewritten] {
                fill(&r, id, paged).await;
            }
            let mut gc = fixture.gc(&r);
            // Old-route traffic after publication excludes the entire old block.
            if paged {
                r.commit_fetched_page(&rewritten, PageIndex(1), 32, Bytes::from_static(&[1; 16]))
                    .await
                    .unwrap();
            } else {
                fill(&r, &rewritten, false).await;
            }
            gc.run_round(&r).await;
            assert!(r.index.get(&refilled).is_none());
            assert!(r.index.get(&rewritten).is_some());
            fill(&r, &refilled, paged).await;
            fill(&r, &new, paged).await;
            let checked = r.page_gc_metrics.ownership_checked.get();
            fixture.publish(&r);
            gc.run_round(&r).await;
            assert_eq!(r.page_gc_metrics.ownership_checked.get(), checked);

            gc.next_supplement = tokio::time::Instant::now();
            gc.run_round(&r).await;
            for id in [&refilled, &rewritten, &new] {
                assert!(r.index.get(id).is_none());
            }
            assert!(r.index.get(&owned).is_some());
            assert_eq!(r.page_gc_metrics.ownership_checked.get(), checked + 3);
            assert!(gc.supplement.is_none());
            assert!(gc.next_delay(&r).unwrap() > Duration::from_secs(1));

            // Hot cache hits and capacity-policy selections do not cause a new sweep.
            let ceiling = r.lru.ownership_admission_ceiling();
            for candidate in r.lru.block_candidates(&owned) {
                r.lru.touch(&candidate.unit);
            }
            assert_eq!(r.lru.ownership_admission_ceiling(), ceiling);
            gc.next_supplement = tokio::time::Instant::now();
            gc.run_round(&r).await;
            assert_eq!(r.page_gc_metrics.ownership_checked.get(), checked + 3);
            assert!(gc.next_delay(&r).unwrap() > Duration::from_secs(1));
        }
    }

    #[tokio::test]
    async fn protected_retry_does_not_rescan_owned_blocks() {
        for paged in [false, true] {
            let fixture = MembershipFixture::new();
            let root = tempfile::tempdir().unwrap();
            let r = runtime(root.path(), paged, Arc::new(Origin::default()));
            for offset in 0..20 {
                fill(&r, &placed("a", offset), paged).await;
            }
            let outside = placed("b", 0);
            fill(&r, &outside, paged).await;
            let unit = if paged {
                CacheUnit::Page(outside.clone(), PageIndex(0))
            } else {
                CacheUnit::Whole(outside.clone())
            };
            let pin = r.lru.pin_guard(unit).unwrap();
            let mut gc = fixture.gc(&r);
            gc.run_round(&r).await;
            let checked = r.page_gc_metrics.ownership_checked.get();
            assert_eq!(checked, 21);
            gc.run_round(&r).await;
            assert_eq!(r.page_gc_metrics.ownership_checked.get(), checked + 1);
            drop(pin);
            gc.run_round(&r).await;
            assert!(r.index.get(&outside).is_none());
            assert_eq!(r.index.len(), 20);
        }
    }

    #[tokio::test]
    async fn supplement_retries_keep_bounds_across_membership_outage() {
        let root = tempfile::tempdir().unwrap();
        let (r, fixture) = namespace_runtime(root.path(), true);
        let mut gc = fixture.gc_managed(&r);
        gc.run_round(&r).await;
        let old = placed("b", 0);
        let later = placed("b", 1);
        fill(&r, &old, true).await;
        let state = r.page_lifecycle.block(&old);
        let reader = state.acquire(PageIndex(0)).unwrap();
        gc.next_supplement = tokio::time::Instant::now();
        gc.run_round(&r).await;
        let bounds = gc.supplement.clone().unwrap();
        assert_eq!(gc.next_delay(&r), Some(Duration::from_secs(1)));
        fill(&r, &later, true).await;
        r.update_membership(None, Instant::now()).unwrap();
        gc.run_round(&r).await;
        assert_eq!(gc.next_delay(&r), None);
        assert_eq!(gc.supplement, Some(bounds.clone()));
        fixture.publish_managed(&r);
        drop(reader);
        gc.run_round(&r).await;
        assert!(r.index.get(&old).is_none());
        assert!(r.index.get(&later).is_some());
        assert_eq!(gc.completed_limit, bounds.end);
        gc.next_supplement = tokio::time::Instant::now();
        gc.run_round(&r).await;
        assert!(r.index.get(&later).is_none());
    }

    #[tokio::test]
    async fn membership_event_supersedes_pending_supplement() {
        let fixture = MembershipFixture::new();
        let root = tempfile::tempdir().unwrap();
        let r = runtime(root.path(), false, Arc::new(Origin::default()));
        let mut gc = fixture.gc(&r);
        gc.run_round(&r).await;
        let old = placed("b", 0);
        fill(&r, &old, false).await;
        let pin = r.lru.pin_guard(CacheUnit::Whole(old.clone())).unwrap();
        gc.next_supplement = tokio::time::Instant::now();
        gc.run_round(&r).await;
        assert!(gc.supplement.is_some());
        drop(pin);
        // This Worker now owns everything; stale supplemental placement cannot delete.
        *fixture.members.lock().unwrap() = Some(vec!["a"]);
        fixture.publish(&r);
        gc.run_round(&r).await;
        assert!(gc.supplement.is_none());
        assert!(r.index.get(&old).is_some());
        assert_eq!(
            gc.completed_generation,
            r.membership_snapshot().unwrap().generation
        );
    }

    #[tokio::test]
    async fn cancelled_supplement_keeps_bounds_and_defers_later_page_fills() {
        let root = tempfile::tempdir().unwrap();
        let (r, fixture) = namespace_runtime(root.path(), true);
        let mut gc = fixture.gc_managed(&r);
        gc.run_round(&r).await;
        let old = placed("b", 0);
        let rewritten = placed("b", 1);
        fill(&r, &old, true).await;
        fill(&r, &rewritten, true).await;
        let state = r.page_lifecycle.block(&old);
        let gate = state.lock().await;
        gc.next_supplement = tokio::time::Instant::now();
        {
            let round = gc.run_round(&r);
            tokio::pin!(round);
            tokio::time::timeout(Duration::from_secs(2), async {
                tokio::select! { biased;
                    _ = async {
                        while r.page_mutations.active_count() == 0 { tokio::task::yield_now().await; }
                    } => {},
                    _ = &mut round => panic!("mutation gate must hold the scan"),
                }
            }).await.unwrap();
        }
        let bounds = gc.supplement.clone().unwrap();
        r.commit_fetched_page(&rewritten, PageIndex(1), 32, Bytes::from_static(&[1; 16]))
            .await
            .unwrap();
        drop(gate);
        r.drain_page_mutations().await;
        gc.run_round(&r).await;
        assert!(r.index.get(&old).is_none());
        assert!(r.index.get(&rewritten).is_some());
        assert_eq!(gc.completed_limit, bounds.end);
        gc.next_supplement = tokio::time::Instant::now();
        gc.run_round(&r).await;
        assert!(r.index.get(&rewritten).is_none());
    }

    #[tokio::test]
    async fn membership_event_freezes_preexisting_blocks_and_converges() {
        for paged in [false, true] {
            let fixture = MembershipFixture::new();
            let root = tempfile::tempdir().unwrap();
            let r = runtime(root.path(), paged, Arc::new(Origin::default()));
            let old = placed("b", 0);
            let rewritten = placed("b", 1);
            let new = placed("b", 2);
            fill(&r, &old, paged).await;
            fill(&r, &rewritten, paged).await;
            let mut gc = fixture.gc(&r);
            let event = r.membership_snapshot().unwrap();
            // The boundary belongs to publication, even if GC starts much later.
            fill(&r, &new, paged).await;
            if paged {
                r.commit_fetched_page(&rewritten, PageIndex(1), 32, Bytes::from_static(&[1; 16]))
                    .await
                    .unwrap();
            } else {
                fill(&r, &rewritten, false).await;
            }
            gc.run_round(&r).await;
            assert!(r.index.get(&old).is_none());
            assert!(r.index.get(&rewritten).is_some());
            assert!(r.index.get(&new).is_some());
            assert_eq!(gc.completed_generation, event.generation);
            assert!(gc.next_delay(&r).unwrap() > Duration::from_secs(1));
            let checked = r.page_gc_metrics.ownership_checked.get();
            fixture.publish(&r); // identical observation cannot widen the event
            fill(&r, &placed("b", 3), paged).await;
            gc.run_round(&r).await;
            assert_eq!(r.page_gc_metrics.ownership_checked.get(), checked);
            // A -> B -> A is a new event even when GC did not observe B.
            *fixture.members.lock().unwrap() = Some(vec!["a"]);
            fixture.publish(&r);
            *fixture.members.lock().unwrap() = Some(vec!["a", "b"]);
            fixture.publish(&r);
            assert!(!r.membership_is_current(&event));
            gc.run_round(&r).await;
            assert!(r.index.is_empty());
            assert!(gc.next_delay(&r).unwrap() > Duration::from_secs(1));
        }
    }

    #[tokio::test]
    async fn cancelled_scan_retries_only_the_original_event_boundary() {
        let fixture = MembershipFixture::new();
        let root = tempfile::tempdir().unwrap();
        let r = runtime(root.path(), false, Arc::new(Origin::default()));
        let old = placed("b", 0);
        fill(&r, &old, false).await;
        fill(&r, &placed("b", 1), false).await;
        let mut gc = fixture.gc(&r);
        let event = r.membership_snapshot().unwrap();
        let state = r.page_lifecycle.block(&old);
        let gate = state.lock().await;
        {
            let round = gc.run_round(&r);
            tokio::pin!(round);
            tokio::time::timeout(Duration::from_secs(2), async {
                tokio::select! { biased;
                    _ = async {
                        while r.page_mutations.active_count() == 0 { tokio::task::yield_now().await; }
                    } => {},
                    _ = &mut round => panic!("mutation gate must hold the scan"),
                }
            }).await.unwrap();
        }
        assert_eq!(gc.completed_generation, 0);
        let new = placed("b", 2);
        fill(&r, &new, false).await;
        drop(gate);
        r.drain_page_mutations().await;
        gc.run_round(&r).await;
        assert_eq!(gc.completed_generation, event.generation);
        assert!(r.index.get(&old).is_none());
        assert!(r.index.get(&placed("b", 1)).is_none());
        assert!(r.index.get(&new).is_some());
        assert!(gc.next_delay(&r).unwrap() > Duration::from_secs(1));
    }

    #[tokio::test]
    async fn concurrent_ownership_and_backend_reclamation_account_once() {
        for paged in [false, true] {
            let fixture = MembershipFixture::new();
            let root = tempfile::tempdir().unwrap();
            let r = runtime(root.path(), paged, Arc::new(Origin::default()));
            let id = placed("b", 0);
            fill(&r, &id, paged).await;
            let mut gc = fixture.gc(&r);
            let candidate = r
                .lru
                .first_ownership_candidate(0..r.lru.ownership_admission_ceiling())
                .unwrap();
            let state = r.page_lifecycle.block(&id);
            let gate = state.lock().await;
            // Both policies already selected the same block; hold commit until
            // both owned mutations are waiting behind the gate.
            tokio::time::timeout(Duration::from_secs(2), async {
                tokio::join!(gc.run_round(&r), r.unlink_stale_block(candidate), async {
                    while r.page_mutations.active_count() < 2 {
                        tokio::task::yield_now().await;
                    }
                    drop(gate);
                },);
            })
            .await
            .unwrap();
            assert!(r.index.is_empty());
            assert_eq!(r.lru.total_bytes(), 0);
            assert_eq!(r.logical_resident_bytes(), 0);
            assert_eq!(r.l1.resident_bytes(), 0);
            assert!(r
                .metrics
                .render()
                .contains("talon_worker_evictions_total 1"));
            if paged {
                assert_eq!(
                    r.page_gc_metrics.deleted[3].get() + r.page_gc_metrics.deleted[4].get(),
                    2
                );
            }
        }
    }

    #[tokio::test]
    async fn ownership_reader_and_pin_defer_entire_block_to_next_round() {
        let fixture = MembershipFixture::new();
        let root = tempfile::tempdir().unwrap();
        let r = runtime(root.path(), true, Arc::new(Origin::default()));
        let id = placed("b", 0);
        fill(&r, &id, true).await;
        let state = r.page_lifecycle.block(&id);
        let reader = state.acquire(PageIndex(0)).unwrap();
        let mut gc = fixture.gc(&r);
        gc.run_round(&r).await;
        assert_eq!(r.index.page_count(), 2);
        drop(reader);
        let pin = r
            .lru
            .pin_guard(CacheUnit::Page(id.clone(), PageIndex(1)))
            .unwrap();
        gc.run_round(&r).await;
        assert_eq!(r.index.page_count(), 2);
        drop(pin);
        gc.run_round(&r).await;
        assert!(r.index.is_empty());
        assert!(state.acquire(PageIndex(0)).is_none());
        assert_eq!(r.logical_resident_bytes(), 0);
    }

    #[tokio::test]
    async fn ownership_deadline_bounds_io_and_gate_waits() {
        for gate_wait in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut r = runtime(root.path(), true, Arc::new(Origin::default()));
            let id = placed("b", 0);
            fill(&r, &id, true).await;
            let budget = crate::background::BackgroundBudget::new(&WorkerConfig {
                background_io_concurrency: 1,
                ..Default::default()
            });
            let held_io = if gate_wait {
                None
            } else {
                Some(budget.io().await)
            };
            r.background_budget = Some(budget);
            let state = r.page_lifecycle.block(&id);
            let held_gate = if gate_wait {
                Some(state.lock().await)
            } else {
                None
            };
            let candidate = r
                .lru
                .first_ownership_candidate(0..r.lru.ownership_admission_ceiling())
                .unwrap();
            tokio::time::timeout(
                Duration::from_secs(1),
                r.unlink_stale_block_for(
                    candidate,
                    Some(Arc::new(MembershipSnapshot {
                        members: vec![],
                        generation: 1,
                        validity_epoch: 1,
                        scan_limits: Arc::default(),
                        valid_until: Instant::now() + Duration::from_millis(20),
                    })),
                ),
            )
            .await
            .unwrap();
            assert_eq!(r.page_mutations.active_count(), 0);
            assert_eq!(r.index.page_count(), 2);
            drop(held_gate);
            drop(held_io);
        }
    }

    #[tokio::test]
    async fn ownership_unlink_failure_requires_fresh_membership_on_retry() {
        let fixture = MembershipFixture::new();
        let root = tempfile::tempdir().unwrap();
        let r = runtime(root.path(), true, Arc::new(Origin::default()));
        let id = placed("b", 0);
        fill(&r, &id, true).await;
        let dir = r.paged.as_ref().unwrap().dir_for(&id);
        let saved = dir.with_extension("saved");
        std::fs::rename(&dir, &saved).unwrap();
        std::fs::write(&dir, b"not a directory").unwrap();
        let mut gc = fixture.gc(&r);
        gc.run_round(&r).await;
        assert_eq!(r.logical_resident_bytes(), 32);
        let state = r.page_lifecycle.block(&id);
        for page in 0..2 {
            assert!(state.acquire(PageIndex(page)).is_some());
        }
        std::fs::remove_file(&dir).unwrap();
        std::fs::rename(&saved, &dir).unwrap();
        r.gc_once().await;
        assert_eq!(r.logical_resident_bytes(), 32);
        *fixture.members.lock().unwrap() = None;
        fixture.publish(&r);
        gc.run_round(&r).await;
        assert_eq!(r.logical_resident_bytes(), 32);
        *fixture.members.lock().unwrap() = Some(vec!["a", "b"]);
        fixture.publish(&r);
        gc.run_round(&r).await;
        assert!(r.index.is_empty());
    }

    #[tokio::test]
    async fn rate_wait_consumes_new_snapshot_before_whole_block_deletion() {
        for changed in [false, true] {
            let fixture = MembershipFixture::new();
            let root = tempfile::tempdir().unwrap();
            let mut r = runtime(root.path(), true, Arc::new(Origin::default()));
            let id = placed("b", 0);
            // More pages than one second of credits: progress requires a new
            // observation after waiting, and a changed view must prevent deletion.
            for p in 0..3 {
                r.commit_fetched_page(&id, PageIndex(p), 64, Bytes::from_static(&[1; 16]))
                    .await
                    .unwrap();
            }
            r.background_budget = Some(crate::background::BackgroundBudget::new(&WorkerConfig {
                background_delete_max_per_sec: 1,
                ..Default::default()
            }));
            let mut gc = fixture.gc(&r);
            // Expire the initial observation during rate waiting. The independent
            // publisher supplies a replacement; GC only consumes it.
            let initial = r.membership_snapshot().unwrap();
            *r.membership.lock().unwrap() = Some(Arc::new(MembershipSnapshot {
                valid_until: Instant::now() + Duration::from_millis(200),
                ..(*initial).clone()
            }));
            let run = gc.run_round(&r);
            let disturbance = async {
                tokio::time::sleep(Duration::from_millis(300)).await;
                if changed {
                    *fixture.members.lock().unwrap() = Some(vec!["a"]);
                }
                fixture.publish(&r);
            };
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(run, disturbance);
            })
            .await
            .unwrap();
            assert_eq!(r.index.is_empty(), !changed);
        }
    }
    fn members() -> Vec<ZonedNodeInfo> {
        ["a", "b", "c"]
            .into_iter()
            .map(|id| ZonedNodeInfo {
                info: NodeInfo {
                    id: NodeId::new(id),
                    address: String::new(),
                    role: NodeRole::Worker,
                },
                zone: None,
            })
            .collect()
    }
    fn discovery(nodes: &[ZonedNodeInfo]) -> talon_core::worker_membership::WorkerDiscovery {
        use talon_core::worker_membership::*;
        WorkerDiscovery::retained(
            &MemberRegistry {
                members: nodes
                    .iter()
                    .map(|n| WorkerMember {
                        worker_id: n.info.id.0.clone(),
                        zone: n.zone.clone(),
                        retired: false,
                    })
                    .collect(),
                ..Default::default()
            },
            &[],
        )
    }
    #[tokio::test]
    async fn missing_expired_or_invalid_snapshots_never_authorize_ownership() {
        let fixture = MembershipFixture::new();
        let root = tempfile::tempdir().unwrap();
        let r = runtime(root.path(), false, Arc::new(Origin::default()));
        let id = placed("b", 0);
        fill(&r, &id, false).await;
        let mut gc = OwnershipGc::new(&fixture.config);
        gc.run_round(&r).await;
        assert_eq!(r.logical_resident_bytes(), 32, "startup has no membership");
        fixture.publish(&r);
        let snapshot = r.membership_snapshot().unwrap();
        *r.membership.lock().unwrap() = Some(Arc::new(MembershipSnapshot {
            valid_until: Instant::now(),
            ..(*snapshot).clone()
        }));
        gc.run_round(&r).await;
        assert_eq!(
            r.logical_resident_bytes(),
            32,
            "stopped publisher cannot extend validity"
        );
        for expired in [false, true] {
            fixture.publish(&r);
            let mut invalid = discovery(&members());
            if expired {
                invalid.valid_for_ms = 1;
            } else {
                invalid.workers.push(invalid.workers[0].clone());
            }
            assert!(r
                .update_membership(Some(invalid), Instant::now() - Duration::from_secs(1))
                .is_err());
            assert!(
                r.membership_snapshot().is_none(),
                "invalid publication revokes old view"
            );
            gc.run_round(&r).await;
            assert_eq!(r.logical_resident_bytes(), 32);
        }
        fixture.publish(&r);
        gc.run_round(&r).await;
        assert_eq!(r.logical_resident_bytes(), 0);
    }

    #[tokio::test]
    async fn same_topology_recovery_keeps_event_boundary_and_wakes_pending_gc() {
        let fixture = MembershipFixture::new();
        let root = tempfile::tempdir().unwrap();
        let r = runtime(root.path(), true, Arc::new(Origin::default()));
        let old = placed("b", 0);
        let new = placed("b", 1);
        fill(&r, &old, true).await;
        let mut events = r.membership_changed.subscribe();
        let mut gc = fixture.gc(&r);
        assert!(events.has_changed().unwrap());
        events.borrow_and_update();
        let original = r.membership_snapshot().unwrap();
        let state = r.page_lifecycle.block(&old);
        let reader = state.acquire(PageIndex(0)).unwrap();
        gc.run_round(&r).await;
        assert_eq!(gc.next_delay(&r), Some(Duration::from_secs(1)));
        fixture.publish(&r);
        assert!(!events.has_changed().unwrap(), "renewal is not a new event");
        *fixture.members.lock().unwrap() = None;
        fixture.publish(&r);
        assert!(
            gc.next_delay(&r).is_none(),
            "no retries without valid membership"
        );
        fill(&r, &new, true).await;
        *fixture.members.lock().unwrap() = Some(vec!["a", "b"]);
        fixture.publish(&r);
        assert!(
            events.has_changed().unwrap(),
            "recovery resumes pending work"
        );
        let recovered = r.membership_snapshot().unwrap();
        assert_eq!(recovered.generation, original.generation);
        assert_eq!(recovered.scan_limits, original.scan_limits);
        drop(reader);
        gc.run_round(&r).await;
        assert!(r.index.get(&old).is_none());
        assert!(r.index.get(&new).is_some());
        assert!(gc.next_delay(&r).unwrap() > Duration::from_secs(1));
    }

    #[tokio::test]
    async fn membership_changes_behind_mutation_gate_retain_whole_block() {
        for transition in 0..3 {
            let fixture = MembershipFixture::new();
            let root = tempfile::tempdir().unwrap();
            let r = runtime(root.path(), true, Arc::new(Origin::default()));
            let id = placed("b", 0);
            fill(&r, &id, true).await;
            let state = r.page_lifecycle.block(&id);
            let gate = state.lock().await;
            let mut gc = fixture.gc(&r);
            let run = gc.run_round(&r);
            let change = async {
                while r.page_mutations.active_count() == 0 {
                    tokio::task::yield_now().await;
                }
                *fixture.members.lock().unwrap() = if transition == 0 {
                    None
                } else {
                    Some(vec!["a"])
                };
                fixture.publish(&r);
                if transition == 2 {
                    // A -> B -> A cannot authorize the old generation's unlink.
                    *fixture.members.lock().unwrap() = Some(vec!["a", "b"]);
                    fixture.publish(&r);
                }
                drop(gate);
            };
            tokio::time::timeout(Duration::from_secs(2), async {
                tokio::join!(run, change);
            })
            .await
            .unwrap();
            assert_eq!(r.logical_resident_bytes(), 32);
            assert_eq!(r.index.page_count(), 2);
            assert!(
                state.acquire(PageIndex(0)).is_some(),
                "claims reopen on changed membership"
            );
        }
    }

    #[test]
    fn membership_uses_logical_ids_and_zones_across_instance_changes() {
        use talon_core::worker_membership::*;
        let root = tempfile::tempdir().unwrap();
        let r = runtime(root.path(), false, Arc::new(Origin::default()));
        let mut view = discovery(&members());
        let mut snapshots = Vec::new();
        for state in [
            InstanceState::Offline,
            InstanceState::Conflict,
            InstanceState::Serving {
                instance_id: "new-process".into(),
                address: "new-address:7001".into(),
            },
        ] {
            view.workers[0].state = state;
            r.update_membership(Some(view.clone()), Instant::now())
                .unwrap();
            snapshots.push(r.membership_snapshot().unwrap().members.clone());
        }
        assert_eq!(snapshots[0].len(), 3, "offline members still own placement");
        assert_eq!(snapshots[0], snapshots[1]);
        assert_eq!(snapshots[1], snapshots[2]);
    }

    #[test]
    fn validates_membership_and_retains_global_and_zone_candidates() {
        let config = WorkerConfig {
            node_id: Some("a".into()),
            ownership_gc_enabled: true,
            ..Default::default()
        };
        let root = tempfile::tempdir().unwrap();
        let r = runtime(root.path(), false, Arc::new(Origin::default()));
        let mut gc = OwnershipGc::new(&config);
        r.update_membership(Some(discovery(&members())), Instant::now())
            .unwrap();
        assert!(gc.prepare_ownership(&r).is_some());
        gc.completed_generation = r.membership_snapshot().unwrap().generation;
        let mut reordered = discovery(&members());
        reordered.workers.reverse();
        r.update_membership(Some(reordered), Instant::now())
            .unwrap();
        assert!(
            gc.prepare_ownership(&r).is_none(),
            "same placement remains converged"
        );
        let mut duplicate = members();
        duplicate.push(duplicate[0].clone());
        for invalid in [Vec::new(), members()[1..].to_vec(), duplicate] {
            let published = r.update_membership(Some(discovery(&invalid)), Instant::now());
            assert!(published.is_err() || gc.prepare_ownership(&r).is_none());
        }
        let table =
            CachePlacementTable::new(&members().into_iter().map(|n| n.info).collect::<Vec<_>>());
        let id = (0..10000)
            .map(|n| {
                BlockId::new(
                    ObjectId::new(Backend::S3, "b", n.to_string()),
                    0,
                    256,
                    Version::new("v1"),
                )
            })
            .find(|id| table.primary(id).unwrap().id.0 == "b")
            .unwrap();
        assert!(!gc.owns(&id));
        gc.replicas = 3;
        assert!(gc.owns(&id));
        gc.replicas = 1;
        let mut zoned = members();
        zoned[0].zone = Some("zone-a".into());
        r.update_membership(Some(discovery(&zoned)), Instant::now())
            .unwrap();
        assert!(gc.prepare_ownership(&r).is_some());
        assert!(gc.owns(&id));
    }
    #[tokio::test]
    async fn independent_tasks_sleep_until_their_own_trigger() {
        for ownership_enabled in [false, true] {
            let fixture = if ownership_enabled {
                Some(MembershipFixture::new())
            } else {
                None
            };
            let root = tempfile::tempdir().unwrap();
            let r = Arc::new(runtime(root.path(), false, Arc::new(Origin::default())));
            let (service, interval) = if let Some(fixture) = &fixture {
                for n in 0..2 {
                    fill(&r, &placed("a", n), false).await;
                }
                fixture.publish(&r);
                let mut config = fixture.config.clone();
                assert!(config.ownership_gc_enabled);
                config.background_scan_batch_size = 1;
                (
                    super::super::WorkerBackground::start(Worker::standalone(r.clone()), &config),
                    3600,
                )
            } else {
                (
                    super::super::WorkerBackground::start(
                        Worker::standalone(r.clone()),
                        &WorkerConfig {
                            ownership_gc_enabled: false,
                            ..Default::default()
                        },
                    ),
                    3600,
                )
            };
            let completed = r.metrics.registry.counter(
                "talon_worker_background_task_completed_total",
                "Completed background batches by task.",
                talon_core::metrics::labels(&[(
                    "task",
                    if ownership_enabled {
                        "ownership_gc"
                    } else {
                        "stale_gc"
                    },
                )]),
            );
            tokio::time::timeout(Duration::from_secs(5), async {
                while completed.get() == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            if fixture.is_some() {
                assert_eq!(
                    r.page_gc_metrics.ownership_checked.get(),
                    2,
                    "all blocks belong to one invocation"
                );
            }
            // Let the scheduler record the completion time before advancing its clock.
            tokio::time::sleep(Duration::from_millis(5)).await;
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(interval - 1)).await;
            tokio::time::resume();
            tokio::time::sleep(Duration::from_millis(5)).await;
            assert_eq!(
                completed.get(),
                1,
                "no repeated scan during the configured round interval"
            );
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(2)).await;
            tokio::time::resume();
            tokio::time::timeout(Duration::from_secs(5), async {
                while completed.get() < 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            if let Some(fixture) = &fixture {
                assert_eq!(
                    r.page_gc_metrics.ownership_checked.get(),
                    2,
                    "hourly wake with no admissions must not recheck residents"
                );
                *fixture.members.lock().unwrap() = Some(vec!["a"]);
                fixture.publish(&r);
                tokio::time::timeout(Duration::from_secs(5), async {
                    while completed.get() < 3 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                assert_eq!(
                    r.page_gc_metrics.ownership_checked.get(),
                    4,
                    "membership events must still scan immediately"
                );
            }
            service.shutdown().await;
            assert_eq!(completed.get(), if ownership_enabled { 3 } else { 2 });
        }
    }
}
