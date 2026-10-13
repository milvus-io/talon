//! Incremental cache ownership cleanup. No work is added to cache hits.
use super::*;
use talon_core::{CachePlacementTable, NodeId, NodeRole, WorkerConfig};
use talon_transport::ZonedNodeInfo;

#[derive(Clone)]
struct PendingBlock {
    id: BlockId,
    next_page: u64,
    end_page: u64,
}

/// One worker's periodic scanner. Each batch requires fresh Coordinator membership.
/// Membership stability is the Coordinator's responsibility.
pub struct OwnershipGc {
    worker: NodeId,
    members: Vec<ZonedNodeInfo>,
    global: Option<CachePlacementTable>,
    local: Option<CachePlacementTable>,
    cursor: u64,
    round_ceiling: Option<u64>,
    pending: Option<PendingBlock>,
    replicas: usize,
    scan_limit: usize,
    delete_limit: usize,
}

impl OwnershipGc {
    /// Construct before foreground traffic starts. Disabled workers do not
    /// construct this scanner or allocate its optional per-block index.
    pub fn new(worker: NodeId, runtime: &WorkerRuntime, config: &WorkerConfig) -> Self {
        runtime.index.enable_ownership_scan();
        Self {
            worker,
            members: Vec::new(),
            global: None,
            local: None,
            cursor: 0,
            round_ceiling: None,
            pending: None,
            replicas: config.ownership_gc_replicas,
            scan_limit: config
                .ownership_gc_scan_batch_size
                .min(config.background_scan_batch_size),
            delete_limit: config
                .ownership_gc_delete_batch_size
                .min(config.background_delete_batch_size),
        }
    }

    /// End the current round; a later successful query starts a fresh scan.
    pub fn unavailable(&mut self) {
        self.global = None;
        self.local = None;
        self.cursor = 0;
        self.round_ceiling = None;
        self.pending = None;
    }

    /// Whether this round needs another bounded batch (not another hourly wait).
    pub fn has_pending_batch(&self) -> bool {
        self.pending.is_some() || self.round_ceiling.is_some_and(|end| self.cursor < end)
    }

    /// Close a finished/cancelled round without losing the admission watermark.
    pub fn end_round(&mut self) {
        self.round_ceiling = None;
        self.pending = None;
    }

    fn observe(&mut self, mut members: Vec<ZonedNodeInfo>) -> bool {
        members.retain(|n| n.info.role == NodeRole::Worker);
        members.sort_unstable_by(|a, b| a.info.id.0.cmp(&b.info.id.0));
        if members.windows(2).any(|w| w[0].info.id == w[1].info.id)
            || !members.iter().any(|n| n.info.id == self.worker)
        {
            self.unavailable();
            return false;
        }
        if self.global.is_some() && self.members == members {
            return true;
        }
        if self.global.is_some() && self.round_ceiling.is_some() {
            // Do not continue a partially executed round across a changed view.
            // The next scheduled round will use the current authoritative view.
            self.unavailable();
            return false;
        }
        self.members = members;
        self.cursor = 0;
        self.pending = None;
        self.local = None;
        let nodes: Vec<_> = self.members.iter().map(|n| n.info.clone()).collect();
        let zone = self
            .members
            .iter()
            .find(|n| n.info.id == self.worker)
            .and_then(|n| n.zone.as_ref());
        if let Some(zone) = zone {
            let local: Vec<_> = self
                .members
                .iter()
                .filter(|n| n.zone.as_ref() == Some(zone))
                .map(|n| n.info.clone())
                .collect();
            if local.len() != nodes.len() {
                self.local = Some(CachePlacementTable::new(&local));
            }
        }
        self.global = Some(CachePlacementTable::new(&nodes));
        true
    }

    fn owns(&self, block: &BlockId) -> bool {
        // Keep the union of global and zone-affine candidates.
        self.global.iter().chain(self.local.iter()).any(|table| {
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

    /// Process one bounded batch and return the number of block checks.
    /// A round freezes its admission watermark so concurrent fills and failed
    /// deletions cannot make it run forever. They are checked next round.
    pub async fn tick(&mut self, runtime: &WorkerRuntime, members: Vec<ZonedNodeInfo>) -> usize {
        let deadline = Instant::now() + Duration::from_secs(1);
        if !self.observe(members) {
            return 0;
        }
        let ceiling = *self
            .round_ceiling
            .get_or_insert_with(|| runtime.index.ownership_ceiling());
        let mut checked = 0;
        let mut attempted = 0;
        while self.has_pending_batch()
            && checked < self.scan_limit
            && attempted < self.delete_limit
            && Instant::now() < deadline
        {
            let batch: Vec<_> = if let Some(pending) = &self.pending {
                vec![(
                    self.cursor,
                    pending.id.clone(),
                    pending.next_page,
                    Some(pending.end_page),
                )]
            } else {
                runtime
                    .index
                    .ownership_batch(self.cursor, ceiling, (self.scan_limit - checked).min(64))
                    .into_iter()
                    .map(|(seq, id)| (seq, id, 0, None))
                    .collect()
            };
            if batch.is_empty() {
                self.cursor = ceiling;
                break;
            }
            for (seq, id, page_cursor, end_page) in batch {
                if attempted >= self.delete_limit || Instant::now() >= deadline {
                    return checked;
                }
                self.pending = None;
                self.cursor = seq;
                checked += 1;
                runtime.page_gc_metrics.ownership_checked.inc();
                if self.owns(&id) {
                    continue;
                }
                let state = runtime.page_lifecycle.block(&id);
                let limit = (self.delete_limit - attempted).min(64);
                let (mut units, end_page) = {
                    let pages = state.inner.lock().unwrap();
                    let end = end_page.unwrap_or_else(|| {
                        pages
                            .pages
                            .last_key_value()
                            .map_or(0, |(&p, _)| p as u64 + 1)
                    });
                    let units = if pages.pages.is_empty() && end_page.is_none() {
                        vec![CacheUnit::Whole(id.clone())]
                    } else if page_cursor >= end {
                        Vec::new()
                    } else {
                        pages
                            .pages
                            .range(page_cursor as u32..)
                            .take_while(|(&p, _)| (p as u64) < end)
                            .take(limit + 1)
                            .map(|(&p, _)| CacheUnit::Page(id.clone(), PageIndex(p)))
                            .collect()
                    };
                    (units, end)
                };
                if units.len() > limit {
                    units.truncate(limit);
                    if let Some(CacheUnit::Page(_, page)) = units.last() {
                        self.pending = Some(PendingBlock {
                            id: id.clone(),
                            next_page: page.0 as u64 + 1,
                            end_page,
                        });
                    }
                }
                // Requeue before awaiting: failed/protected/cancelled units are
                // retried next scheduled round, never spun on within this one.
                runtime.index.defer_ownership(&id);
                attempted += units.len();
                runtime
                    .page_gc_metrics
                    .ownership_attempted
                    .add(units.len() as u64);
                let candidates = runtime.lru.ownership_candidates(units);
                runtime
                    .unlink_units_before(candidates, 4, runtime.capacity_bytes, Some(deadline))
                    .await;
                drop(state);
                runtime.page_lifecycle.retire_empty(&id);
                // Finish the current block before advancing to another index slot.
                if self.pending.is_some() {
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
        if !self.has_pending_batch() {
            self.round_ceiling = None;
        }
        checked
    }
}

/// One bounded invocation; the shared scheduler owns pacing and shutdown.
pub(super) struct OwnershipService {
    gc: OwnershipGc,
    coordinator: String,
    channel: Option<talon_transport::control_tls::ControlTlsChannel>,
    interval: Duration,
}
impl OwnershipService {
    pub(super) fn new(
        node: NodeId,
        worker: &WorkerRuntime,
        config: &WorkerConfig,
        channel: Option<talon_transport::control_tls::ControlTlsChannel>,
    ) -> Self {
        Self {
            gc: OwnershipGc::new(node, worker, config),
            coordinator: config.coordinator.clone(),
            channel,
            interval: Duration::from_secs(config.ownership_gc_interval_secs),
        }
    }

    pub(super) async fn run_batch(&mut self, worker: &WorkerRuntime) -> Duration {
        let snapshot = tokio::time::timeout(Duration::from_secs(3), async {
            if let Some(channel) = &self.channel {
                ownership_membership(channel.connect(&self.coordinator).await?.stream).await
            } else {
                ownership_membership(tokio::net::TcpStream::connect(&self.coordinator).await?).await
            }
        })
        .await;
        match snapshot {
            Ok(Ok(nodes)) => {
                self.gc.tick(worker, nodes).await;
                if self.gc.has_pending_batch() {
                    return Duration::from_secs(1);
                }
                self.gc.end_round();
            }
            error => {
                self.gc.unavailable();
                tracing::warn!(
                    ?error,
                    "ownership GC membership unavailable; cleanup paused"
                );
            }
        }
        self.interval
    }
}

async fn ownership_membership<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    mut stream: S,
) -> anyhow::Result<Vec<ZonedNodeInfo>> {
    use talon_transport::{codec, ControlMessage};
    use tokio::io::AsyncWriteExt;
    stream
        .write_all(&codec::encode(0, &ControlMessage::MembershipQuery {})?)
        .await?;
    stream.flush().await?;
    match crate::tokio_conn::read_control(&mut stream).await? {
        Some(ControlMessage::MembershipList { view }) => {
            anyhow::ensure!(view.valid_for_ms > 0, "expired ownership membership");
            let registry = talon_core::worker_membership::MemberRegistry {
                members: view.workers.into_iter().map(|w| w.member).collect(),
                ..Default::default()
            };
            registry.validate().map_err(anyhow::Error::msg)?;
            // Placement uses retained logical members, including offline/conflicted
            // instances. Addresses and process liveness must not move ownership.
            Ok(registry
                .members
                .into_iter()
                .filter(|m| !m.retired)
                .map(|m| ZonedNodeInfo {
                    info: talon_core::NodeInfo {
                        id: NodeId::new(m.worker_id),
                        address: String::new(),
                        role: NodeRole::Worker,
                    },
                    zone: m.zone,
                })
                .collect())
        }
        other => anyhow::bail!("ownership GC requires retained membership: {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokio_conn::read_control;
    use bytes::Bytes;
    use std::sync::atomic::AtomicUsize;
    use talon_core::{NodeInfo, ObjectStat};
    use talon_transport::{codec, ControlMessage};
    use tokio::{io::AsyncWriteExt, net::TcpListener};

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
    async fn ownership_service_finishes_multiple_batches_with_an_hourly_interval() {
        for backend_enabled in [false, true] {
            ownership_service_batches(backend_enabled).await;
        }
    }

    async fn ownership_service_batches(backend_enabled: bool) {
        use talon_core::{BlockForm, BlockId, BlockMeta, CachePlacementTable, ObjectStore};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let nodes: Vec<_> = ["a", "b"]
            .into_iter()
            .map(|id| talon_transport::ZonedNodeInfo {
                info: NodeInfo {
                    id: NodeId::new(id),
                    address: format!("{id}:7001"),
                    role: NodeRole::Worker,
                },
                zone: None,
            })
            .collect();
        let table =
            CachePlacementTable::new(&nodes.iter().map(|n| n.info.clone()).collect::<Vec<_>>());
        let ids: Vec<_> = (0..1000)
            .map(|n| {
                BlockId::new(
                    ObjectId::new(Backend::S3, "b", n.to_string()),
                    0,
                    256,
                    Version::new("v1"),
                )
            })
            .filter(|id| table.primary(id).unwrap().id.0 == "b")
            .take(2)
            .collect();
        let root = tempfile::tempdir().unwrap();
        let store = WholeBlockStore::open(root.path()).unwrap();
        let index = Arc::new(BlockIndex::new());
        for id in &ids {
            store.put(id, Bytes::from_static(b"cache")).await.unwrap();
            index.commit(BlockMeta {
                id: id.clone(),
                form: BlockForm::Whole,
                len: 5,
            });
        }
        let worker = Arc::new(WorkerRuntime::new(
            store,
            index.clone(),
            Arc::new(InFlightLoads::new()),
            Arc::new(Origin),
            256,
            0,
            WorkerMetrics::new(0),
        ));
        let cfg = WorkerConfig {
            coordinator: listener.local_addr().unwrap().to_string(),
            ownership_gc_interval_secs: 3600,
            stale_gc_enabled: backend_enabled,
            background_task_concurrency: 1,
            background_scan_batch_size: 1,
            background_delete_batch_size: 1,
            ..Default::default()
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                assert_eq!(
                    read_control(&mut stream).await.unwrap(),
                    Some(ControlMessage::MembershipQuery {})
                );
                seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                stream
                    .write_all(
                        &codec::encode(
                            0,
                            &ControlMessage::MembershipList {
                                view: discovery(&nodes),
                            },
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap();
            }
        });
        let task = WorkerBackground::start_with_ownership(worker, &cfg, NodeId::new("a"), None);
        let completed = tokio::time::timeout(Duration::from_secs(3), async {
            while !index.is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        task.shutdown().await;
        server.abort();
        let _ = server.await;
        completed.unwrap();
        assert!(calls.load(std::sync::atomic::Ordering::Relaxed) >= 2);
    }

    #[tokio::test]
    async fn ownership_membership_rejects_errors_and_unexpected_replies() {
        for reply in [
            ControlMessage::Ack {
                ok: false,
                detail: Some("not ready".into()),
            },
            ControlMessage::MembershipQuery {},
        ] {
            let (client, mut server) = tokio::io::duplex(4096);
            let responder = tokio::spawn(async move {
                assert_eq!(
                    read_control(&mut server).await.unwrap(),
                    Some(ControlMessage::MembershipQuery {})
                );
                server
                    .write_all(&codec::encode(0, &reply).unwrap())
                    .await
                    .unwrap();
            });
            assert!(ownership_membership(client).await.is_err());
            responder.await.unwrap();
        }
    }

    #[tokio::test]
    async fn membership_uses_logical_ids_and_zones_across_instance_changes() {
        use talon_core::worker_membership::*;
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
            let (client, mut server) = tokio::io::duplex(4096);
            let reply = ControlMessage::MembershipList { view: view.clone() };
            let responder = tokio::spawn(async move {
                read_control(&mut server).await.unwrap().unwrap();
                server
                    .write_all(&codec::encode(0, &reply).unwrap())
                    .await
                    .unwrap();
            });
            snapshots.push(ownership_membership(client).await.unwrap());
            responder.await.unwrap();
        }
        assert_eq!(snapshots[0].len(), 3, "offline members still own placement");
        assert_eq!(snapshots[0], snapshots[1]);
        assert_eq!(snapshots[1], snapshots[2]);
    }

    #[tokio::test]
    async fn ownership_waits_for_shared_io_without_outliving_authorization() {
        for paged in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut r = runtime(dir.path(), paged);
            let id = block("b");
            if paged {
                r.commit_fetched_page(&id, PageIndex(0), 256, Bytes::from(vec![7; 16]))
                    .await
                    .unwrap();
            } else {
                r.commit_cached_block(&id, Bytes::from_static(b"old"))
                    .await
                    .unwrap();
            }
            let cfg = WorkerConfig {
                background_io_concurrency: 1,
                background_delete_max_per_sec: 0,
                ..Default::default()
            };
            let budget = crate::background::BackgroundBudget::new(&cfg);
            let held = budget.io().await;
            r.background_budget = Some(budget);
            let unit = if paged {
                CacheUnit::Page(id.clone(), PageIndex(0))
            } else {
                CacheUnit::Whole(id.clone())
            };
            let candidates = r.lru.ownership_candidates(vec![unit.clone()]);
            tokio::time::timeout(
                Duration::from_secs(1),
                r.unlink_units_before(
                    candidates,
                    4,
                    r.capacity_bytes,
                    Some(Instant::now() + Duration::from_millis(20)),
                ),
            )
            .await
            .unwrap();
            assert!(r.index.get(&id).is_some());
            assert_eq!(r.page_mutations.active_count(), 0);
            drop(held);
            r.unlink_units_before(
                r.lru.ownership_candidates(vec![unit]),
                4,
                r.capacity_bytes,
                Some(Instant::now() + Duration::from_secs(1)),
            )
            .await;
            assert!(r.index.get(&id).is_none());
        }
    }

    struct Origin;
    #[async_trait::async_trait]
    impl BackendStore for Origin {
        async fn fetch_range(&self, _: &ObjectId, _: u64, len: u64) -> talon_core::Result<Bytes> {
            Ok(Bytes::from(vec![7; len as usize]))
        }
        async fn head(&self, _: &ObjectId) -> talon_core::Result<ObjectStat> {
            panic!("ownership cleanup of recent entries must not query the backend")
        }
    }

    fn members() -> Vec<ZonedNodeInfo> {
        ["a", "b", "c"]
            .into_iter()
            .map(|id| ZonedNodeInfo {
                info: NodeInfo {
                    id: NodeId::new(id),
                    address: format!("{id}:7001"),
                    role: NodeRole::Worker,
                },
                zone: None,
            })
            .collect()
    }

    fn config() -> WorkerConfig {
        WorkerConfig {
            ownership_gc_interval_secs: 1,
            ownership_gc_delete_batch_size: 2,
            ..Default::default()
        }
    }

    fn runtime(root: &Path, paged: bool) -> WorkerRuntime {
        let r = WorkerRuntime::new_with_l1(
            WholeBlockStore::open(root.join("whole")).unwrap(),
            Arc::new(BlockIndex::new()),
            Arc::new(InFlightLoads::new()),
            Arc::new(Origin),
            256,
            0,
            256,
            16,
            WorkerMetrics::new(0),
        );
        if paged {
            r.with_paged_store(PagedBlockStore::open(root.join("paged"), 16).unwrap())
        } else {
            r
        }
    }

    fn block(owner: &str) -> BlockId {
        let table =
            CachePlacementTable::new(&members().into_iter().map(|n| n.info).collect::<Vec<_>>());
        (0..10000)
            .map(|n| {
                BlockId::new(
                    ObjectId::new(Backend::S3, "bucket", format!("{n}")),
                    0,
                    256,
                    Version::new("v1"),
                )
            })
            .find(|id| table.primary(id).unwrap().id.0 == owner)
            .unwrap()
    }

    #[tokio::test]
    async fn stable_ownership_skips_old_blocks_but_checks_new_admissions() {
        let dir = tempfile::tempdir().unwrap();
        let r = runtime(dir.path(), false);
        let keep = block("a");
        r.commit_cached_block(&keep, Bytes::from_static(b"keep"))
            .await
            .unwrap();
        let mut gc = OwnershipGc::new(NodeId::new("a"), &r, &config());
        assert_eq!(gc.tick(&r, members()).await, 1);
        for _ in 0..10 {
            assert_eq!(gc.tick(&r, members()).await, 0);
        }
        let discard = block("b");
        r.commit_cached_block(&discard, Bytes::from_static(b"old"))
            .await
            .unwrap();
        assert_eq!(gc.tick(&r, members()).await, 1);
        assert!(r.index.get(&discard).is_none());
        assert!(r.index.get(&keep).is_some());
        assert_eq!(gc.tick(&r, members()).await, 0);
    }

    #[tokio::test]
    async fn membership_change_between_rounds_rescans_without_a_second_grace() {
        let dir = tempfile::tempdir().unwrap();
        let r = runtime(dir.path(), false);
        let id = block("b");
        r.commit_cached_block(&id, Bytes::from_static(b"old"))
            .await
            .unwrap();
        let mut gc = OwnershipGc::new(NodeId::new("a"), &r, &config());
        gc.tick(&r, vec![members().remove(0)]).await;
        assert!(r.index.get(&id).is_some());
        assert!(!gc.has_pending_batch());
        gc.tick(&r, members()).await;
        assert!(r.index.get(&id).is_none());
    }

    #[tokio::test]
    async fn changed_membership_ends_an_active_round_before_more_deletions() {
        let dir = tempfile::tempdir().unwrap();
        let r = runtime(dir.path(), false);
        let b = block("b");
        let c = block("c");
        for id in [&b, &c] {
            r.commit_cached_block(id, Bytes::from_static(b"old"))
                .await
                .unwrap();
        }
        let mut cfg = config();
        cfg.ownership_gc_scan_batch_size = 1;
        let mut gc = OwnershipGc::new(NodeId::new("a"), &r, &cfg);
        assert_eq!(gc.tick(&r, vec![members().remove(0)]).await, 1);
        assert!(gc.has_pending_batch());
        assert_eq!(gc.tick(&r, members()).await, 0);
        assert!(!gc.has_pending_batch());
        assert!(r.index.get(&b).is_some() && r.index.get(&c).is_some());
        // Only the next scheduled round starts cleanup against the new view.
        assert_eq!(gc.tick(&r, members()).await, 1);
        assert_eq!(r.index.len(), 1);
    }

    #[test]
    fn invalid_membership_and_outages_discard_cached_authorization() {
        let dir = tempfile::tempdir().unwrap();
        let r = runtime(dir.path(), false);
        let mut gc = OwnershipGc::new(NodeId::new("a"), &r, &config());
        assert!(gc.observe(members()));
        let mut reordered = members();
        reordered.reverse();
        assert!(gc.observe(reordered));
        gc.unavailable();
        assert!(gc.global.is_none());
        let mut duplicate = members();
        duplicate.push(duplicate[0].clone());
        for invalid in [Vec::new(), members()[1..].to_vec(), duplicate] {
            assert!(gc.observe(members()));
            assert!(!gc.observe(invalid));
            assert!(gc.global.is_none());
        }
    }

    #[tokio::test]
    async fn paged_round_continues_in_batches_and_defers_protected_pages() {
        let dir = tempfile::tempdir().unwrap();
        let r = runtime(dir.path(), true);
        let id = block("b");
        for p in 0..5 {
            r.commit_fetched_page(&id, PageIndex(p), 256, Bytes::from(vec![7; 16]))
                .await
                .unwrap();
        }
        let state = r.page_lifecycle.block(&id);
        let reader = state.acquire(PageIndex(0)).unwrap();
        let mut gc = OwnershipGc::new(NodeId::new("a"), &r, &config());
        gc.tick(&r, members()).await;
        assert_eq!(r.index.page_count(), 4);
        assert!(gc.has_pending_batch());
        gc.tick(&r, members()).await;
        assert_eq!(r.index.page_count(), 2);
        assert!(gc.has_pending_batch());
        gc.tick(&r, members()).await;
        assert_eq!(r.index.page_count(), 1);
        assert!(
            !gc.has_pending_batch(),
            "a protected page cannot keep this round alive"
        );
        drop(reader);
        gc.tick(&r, members()).await;
        assert!(r.index.get(&id).is_none());
        r.commit_fetched_page(&id, PageIndex(0), 256, Bytes::from(vec![7; 16]))
            .await
            .unwrap();
        gc.tick(&r, members()).await;
        assert!(r.index.get(&id).is_none());
        assert_eq!(r.l1.resident_bytes(), 0);
    }

    #[tokio::test]
    async fn round_watermark_defers_concurrent_admissions_until_next_round() {
        let dir = tempfile::tempdir().unwrap();
        let r = runtime(dir.path(), false);
        for id in [block("b"), block("c")] {
            r.commit_cached_block(&id, Bytes::from_static(b"keep"))
                .await
                .unwrap();
        }
        let mut cfg = config();
        cfg.ownership_gc_scan_batch_size = 1;
        let mut gc = OwnershipGc::new(NodeId::new("a"), &r, &cfg);
        let one = vec![members().remove(0)];
        assert_eq!(gc.tick(&r, one.clone()).await, 1);
        assert!(gc.has_pending_batch());
        let mut added = block("b");
        added.offset = 256;
        r.commit_cached_block(&added, Bytes::from_static(b"new"))
            .await
            .unwrap();
        assert_eq!(gc.tick(&r, one.clone()).await, 1);
        assert!(!gc.has_pending_batch());
        assert_eq!(gc.tick(&r, one.clone()).await, 1);
        assert_eq!(gc.tick(&r, one).await, 0);
    }

    #[test]
    fn global_top_k_and_zone_affine_candidates_are_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let r = runtime(dir.path(), false);
        let mut cfg = config();
        cfg.ownership_gc_replicas = 3;
        let mut gc = OwnershipGc::new(NodeId::new("a"), &r, &cfg);
        assert!(gc.observe(members()));
        assert!(gc.owns(&block("b")));
        let mut gc = OwnershipGc::new(NodeId::new("a"), &r, &config());
        let mut zoned = members();
        zoned[0].zone = Some("zone-a".into());
        assert!(gc.observe(zoned));
        assert!(gc.owns(&block("b")), "a is the sole local-zone candidate");
    }

    #[tokio::test]
    async fn expired_ownership_decision_cannot_delete_after_waiting_for_gate() {
        let dir = tempfile::tempdir().unwrap();
        let r = runtime(dir.path(), false);
        let id = block("b");
        r.commit_cached_block(&id, Bytes::from_static(b"old"))
            .await
            .unwrap();
        let state = r.page_lifecycle.block(&id);
        let gate = state.gate.lock().await;
        let candidates = r
            .lru
            .ownership_candidates(vec![CacheUnit::Whole(id.clone())]);
        let worker = r.clone();
        let deadline = Instant::now() + Duration::from_millis(10);
        let task = tokio::spawn(async move {
            worker
                .unlink_units_before(candidates, 4, worker.capacity_bytes, Some(deadline))
                .await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            r.page_mutations.active_count(),
            0,
            "expired work must not accumulate behind a held gate"
        );
        drop(gate);
        assert!(r.index.get(&id).is_some());
    }

    #[tokio::test]
    async fn failed_ownership_unlink_cannot_be_retried_by_unfenced_page_gc() {
        let dir = tempfile::tempdir().unwrap();
        let r = runtime(dir.path(), true);
        let id = block("b");
        r.commit_fetched_page(&id, PageIndex(0), 256, Bytes::from(vec![7; 16]))
            .await
            .unwrap();
        let path = r.paged.as_ref().unwrap().dir_for(&id).join("0.page");
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        let mut gc = OwnershipGc::new(NodeId::new("a"), &r, &config());
        gc.tick(&r, members()).await;
        assert_eq!(r.index.page_count(), 1);
        assert!(
            !gc.has_pending_batch(),
            "failed unlink waits until next scheduled round"
        );
        std::fs::remove_dir(&path).unwrap();
        std::fs::write(&path, [7; 16]).unwrap();
        gc.unavailable();
        r.gc_once().await;
        assert!(path.exists());
        gc.tick(&r, vec![members().remove(0)]).await;
        assert!(path.exists());
    }
}
