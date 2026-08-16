//! Shared coordinator observability state: readiness gating, state-store
//! access with deadlines, advertised capabilities, and the leased status
//! record.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::File;
use std::io::Read;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use talon_core::{
    NodeHealth, NodeInfo, NodeMetricsSnapshot, NodeRole, NodeStatus, NODE_STATUS_SCHEMA_VERSION,
};
use talon_metadata::{ClusterCapabilities, MetadataStore};

use super::metrics::CoordinatorMetrics;
use super::now_unix_ms;
use crate::{ClusterSnapshot, ClusterStateStore, StateStoreError, StateStoreResult, WriteResult};

#[cfg(test)]
use crate::MemoryStateStore;

#[cfg(test)]
use talon_core::NodeId;

/// Shared coordinator observability and state-store access.
pub struct CoordinatorObservability {
    cluster_id: String,
    node: NodeInfo,
    admin_address: String,
    incarnation_id: String,
    started_at_unix_ms: u64,
    pub(crate) started: Instant,
    sequence: AtomicU64,
    ready: AtomicBool,
    pub(crate) shutting_down: AtomicBool,
    request_timeout: Duration,
    pub(crate) metrics: CoordinatorMetrics,
    store: Arc<dyn ClusterStateStore>,
    /// What this cluster advertises, and the store that backs it.
    ///
    /// `advertised` and `revision` are fixed at construction: they describe the
    /// deployment's configuration. Reachability is not part of that -- ADR 0003
    /// §6 requires "not configured" and "configured but unreachable" to be
    /// separable *during an incident*, which a value sampled once at startup
    /// cannot do. So it lives in an atomic, refreshed on the readiness path.
    capabilities: ClusterCapabilities,
    /// Current reachability of the metadata store.
    ///
    /// Kept beside `capabilities` rather than inside it so a reachability change
    /// cannot be mistaken for a capability change: the revision must not advance
    /// when a store blips, or every outage would look like a reconfiguration and
    /// invalidate every client's cached capability set.
    metadata_reachable: AtomicBool,
    /// Handle used to sample reachability. `None` when no store is configured.
    metadata_store: Option<Arc<dyn MetadataStore>>,
}

impl CoordinatorObservability {
    /// Create observability state over the selected shared-state backend.
    pub fn new(
        cluster_id: String,
        node: NodeInfo,
        admin_address: String,
        request_timeout: Duration,
        store: Arc<dyn ClusterStateStore>,
    ) -> std::io::Result<Self> {
        Ok(Self {
            cluster_id,
            node,
            admin_address,
            incarnation_id: generate_incarnation_id()?,
            started_at_unix_ms: now_unix_ms(),
            started: Instant::now(),
            sequence: AtomicU64::new(0),
            ready: AtomicBool::new(false),
            shutting_down: AtomicBool::new(false),
            request_timeout,
            metrics: CoordinatorMetrics::new(),
            store,
            // A coordinator with no metadata store advertises nothing. ADR 0003
            // §1 keeps that a complete, supported deployment rather than a
            // degraded one, so this is the correct default and not a
            // placeholder.
            capabilities: ClusterCapabilities::none(),
            metadata_reachable: AtomicBool::new(true),
            metadata_store: None,
        })
    }

    /// Attach the capability set this cluster advertises.
    ///
    /// Builder-style so a coordinator without a metadata store needs no change:
    /// omitting this leaves the empty set from [`ClusterCapabilities::none`].
    #[must_use]
    pub fn with_capabilities(mut self, capabilities: ClusterCapabilities) -> Self {
        self.metadata_reachable = AtomicBool::new(capabilities.store_reachable);
        self.capabilities = capabilities;
        self
    }

    /// Attach the metadata store used to sample reachability.
    ///
    /// Without this the advertised reachability stays at whatever
    /// [`with_capabilities`](Self::with_capabilities) recorded, which is a
    /// startup fact. §6 needs a current one.
    #[must_use]
    pub fn with_metadata_store(mut self, store: Arc<dyn MetadataStore>) -> Self {
        self.metadata_store = Some(store);
        self
    }

    /// Capabilities this cluster advertises.
    ///
    /// ADR 0003 §4 requires these to be discoverable "without attempting an
    /// operation", which is what the management API endpoint is for.
    pub fn capabilities(&self) -> ClusterCapabilities {
        ClusterCapabilities {
            advertised: self.capabilities.advertised,
            // Deliberately unchanged by reachability: a store blip is not a
            // reconfiguration, and advancing this would invalidate every
            // client's cached capability set on every outage.
            revision: self.capabilities.revision,
            store_reachable: self.metadata_reachable.load(Ordering::Acquire),
        }
    }

    /// Sample the metadata store and update advertised reachability.
    ///
    /// Runs on the readiness path, never on the read path. §6 is explicit that
    /// "reads, cache hits and misses, placement lookups, and write-through
    /// writes are unaffected. None consults TMS", so request handling reads the
    /// cached atomic rather than probing the store.
    ///
    /// A cluster with no store keeps reporting reachable: an empty capability
    /// set is always serviceable, and there is nothing that could be down.
    async fn refresh_metadata_reachability(&self) {
        let Some(store) = self.metadata_store.as_ref() else {
            return;
        };
        let reachable = match tokio::time::timeout(self.request_timeout, store.check_ready()).await
        {
            Ok(Ok(health)) => health.ready,
            Ok(Err(_)) | Err(_) => false,
        };
        let previous = self.metadata_reachable.swap(reachable, Ordering::AcqRel);
        if previous != reachable {
            // Logged at the transition rather than every sample, and separately
            // from the "not configured" case, so an incident can tell an outage
            // from a deployment choice (§6).
            if reachable {
                tracing::info!(
                    capabilities = %self.capabilities.advertised,
                    "metadata store reachable again; TMS-backed features restored"
                );
            } else {
                tracing::warn!(
                    capabilities = %self.capabilities.advertised,
                    "metadata store became unreachable; TMS-backed features fail closed"
                );
            }
        }
    }

    /// Coordinator metric handles.
    pub fn metrics(&self) -> &CoordinatorMetrics {
        &self.metrics
    }

    /// Selected state store.
    pub fn store(&self) -> &Arc<dyn ClusterStateStore> {
        &self.store
    }

    /// Logical cluster accepted by status heartbeats.
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// Current process incarnation bound into privileged control updates.
    pub fn incarnation_id(&self) -> &str {
        &self.incarnation_id
    }

    /// Stable coordinator node identity.
    pub fn node_id(&self) -> &str {
        &self.node.id.0
    }

    /// Whether authoritative shared state is currently ready.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire) && !self.shutting_down.load(Ordering::Acquire)
    }

    /// Read-only readiness check with deadline.
    pub async fn check_ready(&self) -> StateStoreResult<()> {
        let started = Instant::now();
        let result =
            match tokio::time::timeout(self.request_timeout, self.store.check_ready()).await {
                Ok(result) => result.map(|_| ()),
                Err(_) => Err(StateStoreError::Timeout {
                    backend: self.store.backend(),
                }),
            };
        self.metrics
            .record_state("readiness", &result, started.elapsed());
        self.ready.store(result.is_ok(), Ordering::Release);
        // Sampled alongside cluster-state readiness so reachability tracks the
        // present rather than startup. A metadata failure must not affect this
        // result: §6 keeps TMS outages away from the read path, and readiness
        // gates the read path.
        self.refresh_metadata_reachability().await;
        result
    }

    /// Upsert a leased node status with metrics and deadline.
    pub async fn upsert_status(
        &self,
        status: NodeStatus,
        lease_ttl: Duration,
    ) -> StateStoreResult<WriteResult> {
        let started = Instant::now();
        let result = match tokio::time::timeout(
            self.request_timeout,
            self.store.upsert_node(status, lease_ttl),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(StateStoreError::Timeout {
                backend: self.store.backend(),
            }),
        };
        self.metrics
            .record_state("upsert", &result, started.elapsed());
        if result.is_err() {
            self.ready.store(false, Ordering::Release);
        }
        result
    }

    /// Refresh live-node and snapshot-age gauges.
    pub async fn refresh_snapshot(&self) -> StateStoreResult<()> {
        let started = Instant::now();
        let result =
            match tokio::time::timeout(self.request_timeout, self.store.snapshot(&self.cluster_id))
                .await
            {
                Ok(result) => result,
                Err(_) => Err(StateStoreError::Timeout {
                    backend: self.store.backend(),
                }),
            };
        self.metrics
            .record_state("snapshot", &result, started.elapsed());
        match result {
            Ok(snapshot) => {
                self.metrics.update_snapshot(&snapshot);
                self.ready.store(true, Ordering::Release);
                Ok(())
            }
            Err(error) => {
                self.ready.store(false, Ordering::Release);
                Err(error)
            }
        }
    }

    /// Reconcile local membership from an authoritative store snapshot.
    ///
    /// This is what makes coordinators active-active: the node set consulted by
    /// placement is derived from shared state, not from whichever heartbeats
    /// happened to land on this process. A worker registered through any
    /// coordinator becomes visible through every coordinator once it reconciles.
    ///
    /// Only non-expired **worker** records populate placement membership;
    /// coordinator records are tracked in the store for the management view but
    /// are not placement targets. On a store error the local membership is left
    /// untouched (last-good), readiness is cleared, and the error is returned so
    /// the caller can apply the #73 fail-closed policy.
    pub async fn reconcile_membership(
        &self,
        membership: &crate::Membership,
    ) -> StateStoreResult<()> {
        let started = Instant::now();
        let result =
            match tokio::time::timeout(self.request_timeout, self.store.snapshot(&self.cluster_id))
                .await
            {
                Ok(result) => result,
                Err(_) => Err(StateStoreError::Timeout {
                    backend: self.store.backend(),
                }),
            };
        self.metrics
            .record_state("snapshot", &result, started.elapsed());
        match result {
            Ok(snapshot) => {
                self.metrics.update_snapshot(&snapshot);
                // Only healthy, ready workers are placement targets. Expired
                // leases are already absent from the snapshot; this additionally
                // excludes present-but-unhealthy/not-ready workers so a degraded
                // node is not handed to clients as an owner (issue #118).
                let workers: Vec<(NodeInfo, Option<String>)> = snapshot
                    .nodes
                    .iter()
                    .filter(|status| {
                        status.node.role == NodeRole::Worker
                            && status.health == NodeHealth::Healthy
                            && status.ready
                    })
                    .map(|status| {
                        (
                            status.node.clone(),
                            status.labels.get(talon_core::NODE_ZONE_LABEL).cloned(),
                        )
                    })
                    .collect();
                membership.reconcile_zoned(workers);
                self.ready.store(true, Ordering::Release);
                Ok(())
            }
            Err(error) => {
                self.ready.store(false, Ordering::Release);
                Err(error)
            }
        }
    }

    /// Fetch a linearizable snapshot for the management API, updating freshness
    /// gauges and readiness. Unlike [`refresh_snapshot`](Self::refresh_snapshot)
    /// this returns the snapshot so a handler can render it.
    pub async fn snapshot_for_api(&self) -> StateStoreResult<ClusterSnapshot> {
        let started = Instant::now();
        let result =
            match tokio::time::timeout(self.request_timeout, self.store.snapshot(&self.cluster_id))
                .await
            {
                Ok(result) => result,
                Err(_) => Err(StateStoreError::Timeout {
                    backend: self.store.backend(),
                }),
            };
        self.metrics
            .record_state("snapshot", &result, started.elapsed());
        match &result {
            Ok(snapshot) => {
                self.metrics.update_snapshot(snapshot);
                self.ready.store(true, Ordering::Release);
            }
            Err(_) => self.ready.store(false, Ordering::Release),
        }
        result
    }

    /// Begin graceful shutdown: mark this coordinator not-live/not-ready so new
    /// authoritative reads fail closed while in-flight ones drain.
    pub fn begin_shutdown(&self) {
        self.shutting_down.store(true, Ordering::Release);
        self.ready.store(false, Ordering::Release);
    }

    /// Remove this coordinator's own lease from shared state on shutdown, so a
    /// crashed-or-draining coordinator disappears from the cluster view without
    /// waiting for lease expiry. Best-effort and deadline-bounded.
    pub async fn remove_self(&self) -> StateStoreResult<WriteResult> {
        let started = Instant::now();
        let result = match tokio::time::timeout(
            self.request_timeout,
            self.store
                .remove_node(&self.cluster_id, &self.node.id, &self.incarnation_id),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(StateStoreError::Timeout {
                backend: self.store.backend(),
            }),
        };
        self.metrics
            .record_state("upsert", &result, started.elapsed());
        result
    }

    /// Build the coordinator's own leased status record.
    pub fn status(&self) -> NodeStatus {
        let (requests_total, errors_total) = self.metrics.totals();
        NodeStatus {
            schema_version: NODE_STATUS_SCHEMA_VERSION,
            cluster_id: self.cluster_id.clone(),
            node: self.node.clone(),
            incarnation_id: self.incarnation_id.clone(),
            admin_address: Some(self.admin_address.clone()),
            build_version: env!("CARGO_PKG_VERSION").into(),
            started_at_unix_ms: self.started_at_unix_ms,
            reported_at_unix_ms: now_unix_ms().max(self.started_at_unix_ms),
            heartbeat_seq: self.sequence.fetch_add(1, Ordering::Relaxed),
            health: if self.is_ready() {
                NodeHealth::Healthy
            } else {
                NodeHealth::Degraded
            },
            ready: self.is_ready(),
            metrics: NodeMetricsSnapshot {
                requests_total,
                errors_total,
                state_snapshot_age_ms: self.metrics.snapshot_age_value.load(Ordering::Relaxed),
                ..Default::default()
            },
            labels: BTreeMap::new(),
        }
    }
}

fn generate_incarnation_id() -> std::io::Result<String> {
    let mut bytes = [0u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    let mut id = String::with_capacity(32);
    for byte in bytes {
        write!(id, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(id)
}

#[cfg(test)]
pub(crate) fn observability() -> (Arc<CoordinatorObservability>, Arc<MemoryStateStore>) {
    let store = Arc::new(MemoryStateStore::new());
    let state_store: Arc<dyn ClusterStateStore> = store.clone();
    let observability = Arc::new(
        CoordinatorObservability::new(
            "cluster-a".into(),
            NodeInfo {
                id: NodeId::new("coordinator-1"),
                address: "127.0.0.1:7000".into(),
                role: NodeRole::Coordinator,
            },
            "127.0.0.1:8000".into(),
            Duration::from_millis(100),
            state_store,
        )
        .unwrap(),
    );
    (observability, store)
}

#[cfg(test)]
pub(crate) fn worker_status() -> NodeStatus {
    let now = now_unix_ms();
    NodeStatus {
        schema_version: NODE_STATUS_SCHEMA_VERSION,
        cluster_id: "cluster-a".into(),
        node: NodeInfo {
            id: NodeId::new("worker-1"),
            address: "127.0.0.1:7001".into(),
            role: NodeRole::Worker,
        },
        incarnation_id: "worker-incarnation".into(),
        admin_address: Some("127.0.0.1:8001".into()),
        build_version: "test".into(),
        started_at_unix_ms: now,
        reported_at_unix_ms: now,
        heartbeat_seq: 0,
        health: NodeHealth::Healthy,
        ready: true,
        metrics: NodeMetricsSnapshot::default(),
        labels: BTreeMap::new(),
    }
}

#[cfg(test)]
mod capability_tests {
    use super::*;
    use crate::MemoryStateStore;
    use async_trait::async_trait;
    use talon_core::{NodeId, NodeRole};
    use talon_metadata::{
        BackendHealth, Capability, CapabilityRevision, CapabilitySet, InodeNumber, InodeRecord,
        MappingRevision, MetadataBackend, MetadataError, MetadataResult, NamespaceId,
        PathIndexEntry, Transaction, TransactionOutcome,
    };

    /// A store whose reachability the test controls.
    struct FlakyStore {
        ready: AtomicBool,
    }

    #[async_trait]
    impl MetadataStore for FlakyStore {
        fn backend(&self) -> MetadataBackend {
            MetadataBackend::Memory
        }

        fn capabilities(&self) -> CapabilitySet {
            CapabilitySet::none().with(Capability::HardLinks)
        }

        async fn check_ready(&self) -> MetadataResult<BackendHealth> {
            if self.ready.load(Ordering::Acquire) {
                Ok(BackendHealth {
                    ready: true,
                    detail: "up".to_owned(),
                })
            } else {
                Err(MetadataError::Unavailable {
                    backend: MetadataBackend::Memory,
                    detail: "simulated outage".to_owned(),
                })
            }
        }

        async fn mapping_revision(
            &self,
            _namespace: &NamespaceId,
        ) -> MetadataResult<MappingRevision> {
            Ok(MappingRevision::INITIAL)
        }

        async fn resolve_path(
            &self,
            _namespace: &NamespaceId,
            _path: &str,
        ) -> MetadataResult<Option<PathIndexEntry>> {
            Ok(None)
        }

        async fn load_inode(
            &self,
            _namespace: &NamespaceId,
            inode: InodeNumber,
        ) -> MetadataResult<InodeRecord> {
            Err(MetadataError::NotFound {
                key: format!("inode/{inode}"),
            })
        }

        async fn commit(&self, _transaction: &Transaction) -> MetadataResult<TransactionOutcome> {
            Err(MetadataError::Unavailable {
                backend: MetadataBackend::Memory,
                detail: "not used in this test".to_owned(),
            })
        }
    }

    fn observability(store: Arc<dyn MetadataStore>) -> CoordinatorObservability {
        CoordinatorObservability::new(
            "c".into(),
            NodeInfo {
                id: NodeId::new("coord"),
                address: "coord:7000".into(),
                role: NodeRole::Coordinator,
            },
            "coord:8000".into(),
            Duration::from_secs(1),
            Arc::new(MemoryStateStore::new()),
        )
        .expect("observability")
        .with_capabilities(ClusterCapabilities {
            advertised: CapabilitySet::none().with(Capability::HardLinks),
            revision: CapabilityRevision::new(3),
            store_reachable: true,
        })
        .with_metadata_store(store)
    }

    #[tokio::test]
    async fn reachability_tracks_the_present_not_startup() {
        // ADR 0003 §6 requires "not configured" and "configured but unreachable"
        // to be separable *during an incident* -- which is exactly when a value
        // sampled once at startup is stale.
        let store = Arc::new(FlakyStore {
            ready: AtomicBool::new(true),
        });
        let obs = observability(store.clone());
        obs.check_ready().await.expect("cluster state is ready");
        assert!(obs.capabilities().store_reachable);

        store.ready.store(false, Ordering::Release);
        obs.check_ready()
            .await
            .expect("cluster state is still ready");
        assert!(
            !obs.capabilities().store_reachable,
            "an outage after startup must be visible"
        );

        store.ready.store(true, Ordering::Release);
        obs.check_ready().await.expect("cluster state is ready");
        assert!(
            obs.capabilities().store_reachable,
            "recovery must be visible too"
        );
    }

    #[tokio::test]
    async fn a_store_outage_does_not_advance_the_capability_revision() {
        // A blip is not a reconfiguration. Advancing the revision here would
        // invalidate every client's cached capability set on every outage, and
        // would make the revision useless as a change signal.
        let store = Arc::new(FlakyStore {
            ready: AtomicBool::new(true),
        });
        let obs = observability(store.clone());
        obs.check_ready().await.expect("ready");
        let before = obs.capabilities().revision;

        store.ready.store(false, Ordering::Release);
        obs.check_ready().await.expect("ready");

        assert_eq!(obs.capabilities().revision, before);
        assert_eq!(
            obs.capabilities().advertised,
            CapabilitySet::none().with(Capability::HardLinks),
            "an unreachable store still advertises what it offers"
        );
    }

    #[tokio::test]
    async fn a_metadata_outage_does_not_make_the_cluster_unready() {
        // §6: "TMS unavailability degrades TMS-backed features; it must not
        // affect the read path." Readiness gates the read path, so a metadata
        // outage must not turn the coordinator unready.
        let store = Arc::new(FlakyStore {
            ready: AtomicBool::new(false),
        });
        let obs = observability(store);
        obs.check_ready()
            .await
            .expect("a metadata outage must not fail cluster readiness");
        assert!(obs.is_ready(), "the read path stays available");
        assert!(!obs.capabilities().store_reachable);
    }

    #[tokio::test]
    async fn a_cluster_without_a_metadata_store_stays_reachable() {
        // An empty capability set is always serviceable: there is nothing that
        // could be down, so reporting it unreachable would invent an outage.
        let obs = CoordinatorObservability::new(
            "c".into(),
            NodeInfo {
                id: NodeId::new("coord"),
                address: "coord:7000".into(),
                role: NodeRole::Coordinator,
            },
            "coord:8000".into(),
            Duration::from_secs(1),
            Arc::new(MemoryStateStore::new()),
        )
        .expect("observability");
        obs.check_ready().await.expect("ready");
        assert!(obs.capabilities().store_reachable);
        assert!(obs.capabilities().advertised.is_empty());
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use talon_core::NodeId;

    use super::*;

    #[tokio::test]
    async fn reconcile_excludes_unhealthy_and_not_ready_workers() {
        use crate::Membership;
        let (observability, _store) = observability();
        observability.check_ready().await.unwrap();

        // A healthy+ready worker, an unhealthy worker, and a not-ready worker.
        let healthy = worker_status();
        let mut unhealthy = worker_status();
        unhealthy.node.id = NodeId::new("worker-unhealthy");
        unhealthy.incarnation_id = "inc-unhealthy".into();
        unhealthy.health = NodeHealth::Unhealthy;
        let mut not_ready = worker_status();
        not_ready.node.id = NodeId::new("worker-not-ready");
        not_ready.incarnation_id = "inc-not-ready".into();
        not_ready.ready = false;

        for status in [healthy, unhealthy, not_ready] {
            observability
                .upsert_status(status, Duration::from_secs(30))
                .await
                .unwrap();
        }

        let membership = Membership::new();
        observability
            .reconcile_membership(&membership)
            .await
            .unwrap();

        // Only the healthy, ready worker is a placement target.
        let ids: Vec<String> = membership.snapshot().into_iter().map(|n| n.id.0).collect();
        assert_eq!(ids, vec!["worker-1".to_string()]);
    }

    #[tokio::test]
    async fn readiness_and_status_use_shared_state() {
        let (observability, store) = observability();
        observability.check_ready().await.unwrap();
        assert!(observability.is_ready());

        let status = observability.status();
        status.validate().unwrap();
        assert!(status.ready);
        observability
            .upsert_status(status, Duration::from_secs(30))
            .await
            .unwrap();
        observability
            .upsert_status(worker_status(), Duration::from_secs(30))
            .await
            .unwrap();
        observability.refresh_snapshot().await.unwrap();

        let rendered = observability.metrics.render();
        assert!(rendered
            .contains("talon_coordinator_live_nodes{health=\"healthy\",role=\"coordinator\"} 1"));
        assert!(
            rendered.contains("talon_coordinator_live_nodes{health=\"healthy\",role=\"worker\"} 1")
        );

        store.set_available(false);
        assert!(observability.check_ready().await.is_err());
        assert!(!observability.is_ready());
        assert!(observability.metrics.render().contains(
            "talon_coordinator_state_store_errors_total{kind=\"unavailable\",operation=\"readiness\"} 1"
        ));
    }
}
