//! Event-driven reclamation of cache blocks outside current placement.
use super::{MembershipSnapshot, WorkerRuntime};
use crate::eviction::{access_millis, StaleBlockCandidate};
use std::{sync::Arc, time::Duration};
use talon_core::{BlockId, CachePlacementTable, NodeId, WorkerConfig};

pub(super) struct OwnershipGc {
    worker: NodeId,
    replicas: usize,
    limits: (usize, usize),
    membership: Option<Arc<MembershipSnapshot>>,
    placement: Vec<CachePlacementTable>,
    completed_generation: u64,
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
            limits: (
                config.ownership_gc_scan_batch_size,
                config.ownership_gc_delete_batch_size,
            ),
            membership: None,
            placement: Vec::new(),
            completed_generation: 0,
        }
    }

    /// Stable, converged membership has no timer. Only unfinished old data retries.
    pub(super) fn next_delay(&self, worker: &WorkerRuntime) -> Option<Duration> {
        worker
            .membership_snapshot()
            .filter(|view| {
                view.generation != self.completed_generation
                    && view
                        .members
                        .iter()
                        .any(|member| member.info.id == self.worker)
            })
            .map(|_| Duration::from_secs(1))
    }

    pub(super) async fn run_round(&mut self, worker: &WorkerRuntime) {
        let Some(view) = self.prepare_ownership(worker) else {
            return;
        };
        let mut cursor = (0, view.scan_limit);
        let mut complete = true;
        let limits = (
            worker
                .page_gc_config
                .scan_batch_size
                .min(4096)
                .min(self.limits.0),
            worker.page_gc_config.delete_batch_size.min(self.limits.1),
        );
        while cursor.0 < cursor.1 {
            if !worker.membership_is_current(&view) {
                return;
            }
            let lru = worker.lru.clone();
            let scanned = tokio::task::spawn_blocking(move || {
                let candidates =
                    lru.scan_stale_blocks(&mut cursor, access_millis(), 0, limits, true);
                (cursor, candidates)
            })
            .await
            .expect("ownership GC scan panicked");
            cursor = scanned.0;
            complete &= self.reclaim_misplaced(worker, scanned.1, &view).await;
            // Local metadata work yields cooperatively; disk deletion is separately paced.
            tokio::task::yield_now().await;
        }
        if complete && worker.membership_is_current(&view) {
            self.completed_generation = view.generation;
        }
    }

    /// Snapshot/table preparation happens once per round. Later batches only
    /// check that the publisher still supplies the same valid placement.
    fn prepare_ownership(&mut self, worker: &WorkerRuntime) -> Option<Arc<MembershipSnapshot>> {
        let id = &self.worker;
        let snapshot = worker.membership_snapshot().filter(|view| {
            view.generation != self.completed_generation
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

    /// Reclaim misplaced pre-event blocks; false leaves the event pending for retry.
    async fn reclaim_misplaced(
        &self,
        worker: &WorkerRuntime,
        candidates: Vec<StaleBlockCandidate>,
        expected: &Arc<MembershipSnapshot>,
    ) -> bool {
        if !worker.membership_is_current(expected) {
            return false;
        }
        let mut misplaced = Vec::new();
        for candidate in candidates {
            if candidate.revision < expected.scan_limit {
                worker.page_gc_metrics.ownership_checked.inc();
                if !self.owns(&candidate.block) {
                    misplaced.push(candidate);
                }
            }
        }
        if misplaced.is_empty() {
            return true;
        }
        // Pay every page's deletion credit before consuming the latest validity.
        if let Some(budget) = &worker.background_budget {
            for candidate in &misplaced {
                budget.delete_units(candidate.units as u64).await;
            }
        }
        let Some(snapshot) = worker
            .membership_snapshot()
            .filter(|view| view.generation == expected.generation)
        else {
            return false;
        };
        let mut complete = true;
        for candidate in misplaced {
            if !worker.membership_is_current(&snapshot) {
                return false;
            }
            worker
                .page_gc_metrics
                .ownership_attempted
                .add(candidate.units as u64);
            let id = candidate.block.clone();
            worker
                .unlink_stale_block_for(candidate, Some(snapshot.clone()))
                .await;
            // Reader/pin protection, activity races and unlink errors all retain
            // the block. A replacement is conservatively retried once, then its
            // post-event revision excludes it from ownership selection.
            complete &= worker.index.get(&id).is_none();
        }
        complete
    }
}

#[cfg(test)]
mod tests {

    use super::super::gc_test_support::*;
    use super::*;
    use crate::CacheUnit;
    use bytes::Bytes;
    use std::{sync::Arc, time::Instant};
    use talon_core::{Backend, NodeInfo, NodeRole, ObjectId, PageIndex, Version};
    use talon_transport::ZonedNodeInfo;
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
                    ownership_gc_enabled: true,
                    stale_gc_enabled: false,
                    ..Default::default()
                },
            }
        }
        fn publish(&self, worker: &WorkerRuntime) {
            let view = self.members.lock().unwrap().as_ref().map(|ids| {
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
            });
            worker.update_membership(view, Instant::now()).unwrap();
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
                r.commit_fetched_page(&rewritten, PageIndex(2), 64, Bytes::from_static(&[1; 16]))
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
            assert_eq!(gc.next_delay(&r), None);
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
            assert_eq!(gc.next_delay(&r), None);
        }
    }

    #[tokio::test]
    async fn membership_change_or_failure_pauses_ownership_until_next_round() {
        for failed in [false, true] {
            let fixture = MembershipFixture::new();
            *fixture.members.lock().unwrap() = Some(vec!["a"]);
            let root = tempfile::tempdir().unwrap();
            let mut r = runtime(root.path(), false, Arc::new(Origin::default()));
            r.page_gc_config.scan_batch_size = 1;
            for n in 0..2 {
                fill(&r, &placed("b", n), false).await;
            }
            let mut gc = fixture.gc(&r);
            let round = gc.run_round(&r);
            let change = async {
                while r.page_gc_metrics.ownership_checked.get() == 0 {
                    tokio::task::yield_now().await;
                }
                *fixture.members.lock().unwrap() = if failed { None } else { Some(vec!["a", "b"]) };
                fixture.publish(&r);
            };
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::join!(round, change);
            })
            .await
            .unwrap();
            assert_eq!(gc.completed_generation, 0);
            assert_eq!(r.index.len(), 2);
            *fixture.members.lock().unwrap() = Some(vec!["a", "b"]);
            fixture.publish(&r);
            gc.run_round(&r).await;
            assert!(r.index.is_empty());
        }
    }

    #[tokio::test]
    async fn cancelled_round_retries_only_the_original_event_boundary() {
        let fixture = MembershipFixture::new();
        let root = tempfile::tempdir().unwrap();
        let mut r = runtime(root.path(), false, Arc::new(Origin::default()));
        r.page_gc_config.scan_batch_size = 1;
        fill(&r, &placed("a", 0), false).await;
        fill(&r, &placed("b", 0), false).await;
        let mut gc = fixture.gc(&r);
        let event = r.membership_snapshot().unwrap();
        {
            let round = gc.run_round(&r);
            tokio::pin!(round);
            tokio::time::timeout(Duration::from_secs(2), async {
                tokio::select! { biased;
                    _ = async {
                        while r.page_gc_metrics.ownership_checked.get() == 0 {
                            tokio::task::yield_now().await;
                        }
                    } => {},
                    _ = &mut round => panic!("round must yield between batches"),
                }
            })
            .await
            .unwrap();
        }
        assert_eq!(gc.completed_generation, 0);
        let new = placed("b", 1);
        fill(&r, &new, false).await;
        gc.run_round(&r).await;
        assert_eq!(gc.completed_generation, event.generation);
        assert!(r.index.get(&placed("b", 0)).is_none());
        assert!(r.index.get(&new).is_some());
        assert_eq!(r.page_gc_metrics.ownership_checked.get(), 3);
        assert_eq!(gc.next_delay(&r), None);
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
                .scan_stale_blocks(
                    &mut (0, r.lru.stale_scan_ceiling()),
                    access_millis(),
                    0,
                    (1, 1),
                    true,
                )
                .pop()
                .unwrap();
            let state = r.page_lifecycle.block(&id);
            let gate = state.gate.lock().await;
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
            assert_eq!(r.resident_bytes(), 0);
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
        assert_eq!(r.resident_bytes(), 0);
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
                Some(state.gate.lock().await)
            } else {
                None
            };
            let candidate = r
                .lru
                .scan_stale_blocks(
                    &mut (0, r.lru.stale_scan_ceiling()),
                    access_millis(),
                    0,
                    (1, 1),
                    true,
                )
                .pop()
                .unwrap();
            tokio::time::timeout(
                Duration::from_secs(1),
                r.unlink_stale_block_for(
                    candidate,
                    Some(Arc::new(MembershipSnapshot {
                        members: vec![],
                        generation: 1,
                        scan_limit: 0,
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
        assert_eq!(r.resident_bytes(), 32);
        let state = r.page_lifecycle.block(&id);
        for page in 0..2 {
            assert!(state.acquire(PageIndex(page)).is_some());
        }
        std::fs::remove_file(&dir).unwrap();
        std::fs::rename(&saved, &dir).unwrap();
        r.gc_once().await;
        assert_eq!(r.resident_bytes(), 32);
        *fixture.members.lock().unwrap() = None;
        fixture.publish(&r);
        gc.run_round(&r).await;
        assert_eq!(r.resident_bytes(), 32);
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
        assert_eq!(r.resident_bytes(), 32, "startup has no membership");
        fixture.publish(&r);
        let snapshot = r.membership_snapshot().unwrap();
        *r.membership.lock().unwrap() = Some(Arc::new(MembershipSnapshot {
            valid_until: Instant::now(),
            ..(*snapshot).clone()
        }));
        gc.run_round(&r).await;
        assert_eq!(
            r.resident_bytes(),
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
            assert_eq!(r.resident_bytes(), 32);
        }
        fixture.publish(&r);
        gc.run_round(&r).await;
        assert_eq!(r.resident_bytes(), 0);
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
        assert_eq!(recovered.scan_limit, original.scan_limit);
        drop(reader);
        gc.run_round(&r).await;
        assert!(r.index.get(&old).is_none());
        assert!(r.index.get(&new).is_some());
        assert_eq!(gc.next_delay(&r), None);
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
            let gate = state.gate.lock().await;
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
            assert_eq!(r.resident_bytes(), 32);
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
                config.ownership_gc_enabled = true;
                config.background_scan_batch_size = 1;
                (
                    super::super::WorkerBackground::start(r.clone(), &config),
                    7200,
                )
            } else {
                (
                    super::super::WorkerBackground::start(r.clone(), &Default::default()),
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
                    "both batches belong to one invocation"
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
            if ownership_enabled {
                tokio::time::sleep(Duration::from_millis(10)).await;
                assert_eq!(completed.get(), 1, "converged ownership has no timer");
                *fixture.as_ref().unwrap().members.lock().unwrap() = Some(vec!["a"]);
                fixture.as_ref().unwrap().publish(&r);
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                while completed.get() < 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            service.shutdown().await;
            assert_eq!(completed.get(), 2);
        }
    }
}
