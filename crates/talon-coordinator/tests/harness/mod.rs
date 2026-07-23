//! Reusable in-process harness for end-to-end HA and fault-injection tests
//! (#89).
//!
//! The harness stands up several [`CoordinatorObservability`] instances over a
//! single shared [`ClusterStateStore`], which is exactly the active-active model
//! the coordinator runtime uses: every coordinator derives its placement
//! membership from shared state rather than from whichever heartbeats reached
//! that process. Workers are modeled as leased [`NodeStatus`] records upserted
//! through a coordinator.
//!
//! A [`HaBackend`] abstracts time and the store so the identical scenario set
//! runs against the deterministic in-memory backend (with an injectable clock)
//! and, when `TALON_ETCD_TEST_ENDPOINT` is set, a real etcd cluster.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use talon_coordinator::state_store::testkit::worker_status as contract_worker_status;
use talon_coordinator::{
    ClusterSnapshot, ClusterStateStore, CoordinatorObservability, Epoch, Membership,
    PlacementService, RendezvousPlacement, StateStoreResult, WriteDisposition,
};
use talon_core::{
    Backend, BlockId, NodeHealth, NodeId, NodeInfo, NodeMetricsSnapshot, NodeRole, NodeStatus,
    ObjectId, Version, NODE_STATUS_SCHEMA_VERSION,
};

/// Logical cluster used by every harness scenario.
pub const CLUSTER_ID: &str = "ha";

/// Backend-and-clock abstraction so one scenario body drives both the
/// deterministic in-memory store and a real etcd store.
#[async_trait]
pub trait HaBackend: Send + Sync {
    /// The shared store every coordinator in the scenario reads and writes.
    fn store(&self) -> Arc<dyn ClusterStateStore>;

    /// Lease TTL for node records. Failover deadlines are derived from this.
    fn lease_ttl(&self) -> Duration;

    /// Advance time by `duration`. For the memory backend this steps an
    /// injected clock deterministically; for etcd it sleeps real wall-clock
    /// time so leases actually expire server-side.
    async fn elapse(&self, duration: Duration);

    /// Current logical time in Unix milliseconds, used to stamp status records
    /// consistently with lease expiry.
    fn now_unix_ms(&self) -> u64;

    /// Human-readable backend name for diagnostics.
    fn name(&self) -> &'static str;
}

/// A simulated coordinator process: its own incarnation/observability over the
/// shared store, plus the local placement membership it reconciles from that
/// store. Two coordinators with the same shared snapshot must compute the same
/// placement epoch (the #80 determinism invariant).
pub struct HaCoordinator {
    pub observability: Arc<CoordinatorObservability>,
    pub service: PlacementService<RendezvousPlacement>,
}

impl HaCoordinator {
    /// Reconcile local placement membership from an authoritative snapshot.
    /// Returns the resulting placement epoch.
    pub async fn reconcile(&self) -> StateStoreResult<Epoch> {
        self.observability
            .reconcile_membership(self.service.membership())
            .await?;
        Ok(self.service.membership().epoch())
    }

    /// Ordered owners this coordinator would return for `block` right now.
    pub fn lookup(&self, block: &BlockId, k: usize) -> Vec<String> {
        self.service.lookup(block, k).owners
    }

    /// Whether this coordinator considers shared state ready (fail-closed gate).
    pub fn is_ready(&self) -> bool {
        self.observability.is_ready()
    }
}

/// A running HA scenario: N coordinators over one shared store.
pub struct HaCluster<B: HaBackend> {
    pub backend: B,
    pub coordinators: Vec<HaCoordinator>,
    /// First-seen start time per (worker, incarnation). A process incarnation
    /// has a single fixed start time across all its heartbeats; only
    /// `reported_at` advances. This is what lets a newer incarnation supersede
    /// an older one and a straggler from the old incarnation be rejected.
    incarnation_starts: Mutex<HashMap<(String, String), u64>>,
}

impl<B: HaBackend> HaCluster<B> {
    /// Stand up `coordinator_count` coordinators sharing `backend`'s store.
    pub fn new(backend: B, coordinator_count: usize) -> Self {
        let store = backend.store();
        let mut coordinators = Vec::with_capacity(coordinator_count);
        for i in 0..coordinator_count {
            let node = NodeInfo {
                id: NodeId::new(format!("coord-{i}")),
                address: format!("10.0.0.{i}:7000"),
                role: NodeRole::Coordinator,
            };
            let observability = Arc::new(
                CoordinatorObservability::new(
                    CLUSTER_ID.to_string(),
                    node,
                    format!("10.0.0.{i}:8000"),
                    Duration::from_secs(2),
                    Arc::clone(&store),
                )
                .expect("observability"),
            );
            coordinators.push(HaCoordinator {
                observability,
                service: PlacementService::new(Membership::new(), RendezvousPlacement),
            });
        }
        Self {
            backend,
            coordinators,
            incarnation_starts: Mutex::new(HashMap::new()),
        }
    }

    /// Shared store handle.
    pub fn store(&self) -> Arc<dyn ClusterStateStore> {
        self.backend.store()
    }

    /// Register (or refresh) a worker's leased record through coordinator
    /// `via`, stamped at the current logical time. `incarnation`/`seq` follow
    /// the store's ordering contract.
    pub async fn heartbeat_worker(
        &self,
        via: usize,
        worker_id: &str,
        incarnation: &str,
        seq: u64,
    ) -> WriteDisposition {
        let status = self.worker_status(worker_id, incarnation, seq);
        self.coordinators[via]
            .observability
            .upsert_status(status, self.backend.lease_ttl())
            .await
            .expect("worker heartbeat")
            .disposition
    }

    /// Build a worker status record. `reported_at` is the current clock, but
    /// `started_at` is pinned to when this (worker, incarnation) was first seen,
    /// modeling a real process whose start time is fixed for its lifetime.
    pub fn worker_status(&self, worker_id: &str, incarnation: &str, seq: u64) -> NodeStatus {
        let now = self.backend.now_unix_ms();
        let started_at = {
            let mut starts = self.incarnation_starts.lock().unwrap();
            *starts
                .entry((worker_id.to_string(), incarnation.to_string()))
                .or_insert(now)
        };
        NodeStatus {
            schema_version: NODE_STATUS_SCHEMA_VERSION,
            cluster_id: CLUSTER_ID.to_string(),
            node: NodeInfo {
                id: NodeId::new(worker_id),
                address: format!("{worker_id}:7001"),
                role: NodeRole::Worker,
            },
            incarnation_id: incarnation.to_string(),
            admin_address: Some(format!("{worker_id}:8001")),
            build_version: "test".to_string(),
            started_at_unix_ms: started_at,
            reported_at_unix_ms: now,
            heartbeat_seq: seq,
            health: NodeHealth::Healthy,
            ready: true,
            metrics: NodeMetricsSnapshot {
                block_count: seq,
                ..Default::default()
            },
            labels: BTreeMap::new(),
        }
    }

    /// Publish a coordinator's own leased record through its observability.
    pub async fn heartbeat_coordinator(&self, index: usize) -> WriteDisposition {
        let obs = &self.coordinators[index].observability;
        obs.upsert_status(obs.status(), self.backend.lease_ttl())
            .await
            .expect("coordinator heartbeat")
            .disposition
    }

    /// Reconcile every coordinator and return their placement epochs.
    pub async fn reconcile_all(&self) -> Vec<Epoch> {
        let mut epochs = Vec::with_capacity(self.coordinators.len());
        for coord in &self.coordinators {
            epochs.push(coord.reconcile().await.expect("reconcile"));
        }
        epochs
    }

    /// Linearizable snapshot of the shared cluster state.
    pub async fn snapshot(&self) -> ClusterSnapshot {
        self.store().snapshot(CLUSTER_ID).await.expect("snapshot")
    }

    /// Worker ids currently visible (non-expired) in shared state.
    pub async fn live_worker_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self
            .snapshot()
            .await
            .nodes
            .into_iter()
            .filter(|s| s.node.role == NodeRole::Worker)
            .map(|s| s.node.id.0)
            .collect();
        ids.sort();
        ids
    }

    /// Advance the harness clock.
    pub async fn elapse(&self, duration: Duration) {
        self.backend.elapse(duration).await;
    }

    /// Capture a compact diagnostic bundle for failure messages.
    pub async fn diagnostics(&self, label: &str) -> String {
        let snapshot = self.store().snapshot(CLUSTER_ID).await;
        let mut out = format!("[{} / {}] diagnostics:\n", self.backend.name(), label);
        match snapshot {
            Ok(snapshot) => {
                out.push_str(&format!(
                    "  snapshot revision={} observed_at={} nodes={}\n",
                    snapshot.revision,
                    snapshot.observed_at_unix_ms,
                    snapshot.nodes.len()
                ));
                for node in &snapshot.nodes {
                    out.push_str(&format!(
                        "    {} role={:?} inc={} seq={} health={:?}\n",
                        node.node.id.0,
                        node.node.role,
                        node.incarnation_id,
                        node.heartbeat_seq,
                        node.health
                    ));
                }
            }
            Err(error) => out.push_str(&format!("  snapshot error: {error}\n")),
        }
        for (i, coord) in self.coordinators.iter().enumerate() {
            out.push_str(&format!(
                "  coord-{i} ready={} epoch={}\n",
                coord.is_ready(),
                coord.service.membership().epoch().0
            ));
        }
        out
    }
}

/// Deterministic block id helper for placement assertions.
pub fn block(n: u64) -> BlockId {
    BlockId::new(
        ObjectId::new(Backend::S3, "bucket", format!("obj/{n}")),
        0,
        256 << 20,
        Version::new("v1"),
    )
}

/// Re-export the contract worker builder for scenarios that want the exact
/// record shape used by the store contract tests.
pub fn contract_status(node_id: &str, incarnation: &str, seq: u64) -> NodeStatus {
    contract_worker_status(node_id, incarnation, seq)
}
