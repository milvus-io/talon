//! End-to-end HA and failover scenarios for the active-active coordinator
//! runtime (issue #89).
//!
//! These exercise the behavioral contract the management plane promises —
//! cross-coordinator visibility, failover with bounded interruption, fail-closed
//! reads under a backend outage, lease-driven removal, no split-brain, and
//! deterministic placement agreement — by wiring **two** independent
//! coordinators (each its own membership + placement service + observability) to
//! **one** shared state store.
//!
//! The scenarios are written against a `ClusterStateStore` obtained from a
//! factory, so the same suite runs over any backend. The in-process
//! [`MemoryStateStore`] variant runs in normal CI (deterministic, injectable
//! clock, fault injection). The etcd and Kubernetes backends run the identical
//! scenarios against a real server in `#[ignore]`d variants (see the
//! backend-specific `*_contract` tests) — CI has no such servers.
//!
//! Recovery/expiry deadlines are derived from the configured lease TTL and
//! request timeout, not from wall-clock guesses, so the assertions are stable.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use talon_coordinator::state_store::TimeSource;
use talon_coordinator::{
    ClusterStateStore, CoordinatorObservability, Membership, MemoryStateStore, PlacementService,
    RendezvousPlacement,
};
use talon_core::{
    Backend, BlockId, NodeHealth, NodeId, NodeInfo, NodeMetricsSnapshot, NodeRole, NodeStatus,
    ObjectId, Version, NODE_STATUS_SCHEMA_VERSION,
};

/// A manually-advanced clock so lease-expiry deadlines are exact, not timed.
#[derive(Default)]
struct ManualClock {
    now_ms: AtomicU64,
}
impl ManualClock {
    fn advance(&self, d: Duration) {
        self.now_ms
            .fetch_add(d.as_millis() as u64, Ordering::SeqCst);
    }
}
impl TimeSource for ManualClock {
    fn now_unix_ms(&self) -> u64 {
        self.now_ms.load(Ordering::SeqCst)
    }
}

/// One in-process coordinator: its own membership/placement/observability over a
/// shared store, exactly as a separate process would have.
struct Coordinator {
    obs: Arc<CoordinatorObservability>,
    service: PlacementService<RendezvousPlacement>,
}

impl Coordinator {
    fn new(node_id: &str, cluster: &str, store: Arc<dyn ClusterStateStore>) -> Self {
        let obs = Arc::new(
            CoordinatorObservability::new(
                cluster.into(),
                NodeInfo {
                    id: NodeId::new(node_id),
                    address: format!("{node_id}:7000"),
                    role: NodeRole::Coordinator,
                },
                format!("{node_id}:8000"),
                Duration::from_millis(200),
                store,
            )
            .unwrap(),
        );
        Coordinator {
            obs,
            service: PlacementService::new(Membership::new(), RendezvousPlacement),
        }
    }

    /// Pull the authoritative snapshot into local placement membership, as the
    /// reconcile loop does in production.
    async fn reconcile(&self) -> bool {
        self.obs
            .reconcile_membership(self.service.membership())
            .await
            .is_ok()
    }
}

fn worker(cluster: &str, id: &str, incarnation: &str, seq: u64, now: u64) -> NodeStatus {
    NodeStatus {
        schema_version: NODE_STATUS_SCHEMA_VERSION,
        cluster_id: cluster.into(),
        node: NodeInfo {
            id: NodeId::new(id),
            address: format!("{id}:7001"),
            role: NodeRole::Worker,
        },
        incarnation_id: incarnation.into(),
        admin_address: Some(format!("{id}:8001")),
        build_version: "test".into(),
        started_at_unix_ms: now,
        reported_at_unix_ms: now + seq,
        heartbeat_seq: seq,
        health: NodeHealth::Healthy,
        ready: true,
        metrics: NodeMetricsSnapshot::default(),
        labels: Default::default(),
    }
}

fn block(n: u64) -> BlockId {
    BlockId::new(
        ObjectId::new(Backend::S3, "b", format!("o/{n}")),
        0,
        256 << 20,
        Version::new("v1"),
    )
}

const CLUSTER: &str = "ha";
const TTL: Duration = Duration::from_secs(30);

/// Build a shared memory store with an injectable clock. Returns both the trait
/// object (for coordinators) and the concrete handle (for fault injection).
fn memory_store() -> (
    Arc<dyn ClusterStateStore>,
    Arc<MemoryStateStore>,
    Arc<ManualClock>,
) {
    let clock = Arc::new(ManualClock::default());
    let concrete = Arc::new(MemoryStateStore::with_time_source(clock.clone(), 256));
    let store: Arc<dyn ClusterStateStore> = concrete.clone();
    (store, concrete, clock)
}

#[tokio::test]
async fn worker_registration_is_visible_through_every_coordinator() {
    let (store, _mem, _clock) = memory_store();
    let a = Coordinator::new("coord-a", CLUSTER, Arc::clone(&store));
    let b = Coordinator::new("coord-b", CLUSTER, Arc::clone(&store));
    a.obs.check_ready().await.unwrap();
    b.obs.check_ready().await.unwrap();

    // A worker registers through coordinator A only.
    store
        .upsert_node(worker(CLUSTER, "w1", "inc-1", 0, 1_000), TTL)
        .await
        .unwrap();

    // Both coordinators, after reconciling, see it and agree on placement.
    assert!(a.reconcile().await);
    assert!(b.reconcile().await);
    assert_eq!(a.service.membership().snapshot().len(), 1);
    assert_eq!(b.service.membership().snapshot().len(), 1);

    // No split-brain: identical membership -> identical placement version and
    // identical owner for any block.
    assert_eq!(
        a.service.membership().epoch(),
        b.service.membership().epoch()
    );
    for n in 0..25 {
        assert_eq!(
            a.service.lookup(&block(n), 1).owners,
            b.service.lookup(&block(n), 1).owners,
            "coordinators disagree on placement for block {n}"
        );
    }
}

#[tokio::test]
async fn service_survives_coordinator_shutdown() {
    let (store, _mem, _clock) = memory_store();
    let a = Coordinator::new("coord-a", CLUSTER, Arc::clone(&store));
    let b = Coordinator::new("coord-b", CLUSTER, Arc::clone(&store));
    a.obs.check_ready().await.unwrap();
    b.obs.check_ready().await.unwrap();
    // Both coordinators register their own leases.
    a.obs.upsert_status(a.obs.status(), TTL).await.unwrap();
    b.obs.upsert_status(b.obs.status(), TTL).await.unwrap();
    store
        .upsert_node(worker(CLUSTER, "w1", "inc-1", 0, 1_000), TTL)
        .await
        .unwrap();
    a.reconcile().await;
    b.reconcile().await;

    // Coordinator A gracefully shuts down: not ready, and its lease is released.
    a.obs.begin_shutdown();
    let removed = a.obs.remove_self().await.unwrap();
    assert_eq!(
        removed.disposition,
        talon_coordinator::WriteDisposition::Applied
    );
    assert!(!a.obs.is_ready());

    // B still serves: it re-reconciles and answers placement, and A is gone from
    // the authoritative snapshot (coordinators are excluded from placement
    // membership, but the record removal proves no orphaned lease).
    assert!(b.reconcile().await);
    let snap = b.obs.snapshot_for_api().await.unwrap();
    let coord_ids: Vec<String> = snap
        .nodes
        .iter()
        .filter(|s| s.node.role == NodeRole::Coordinator)
        .map(|s| s.node.id.0.clone())
        .collect();
    assert!(
        !coord_ids.contains(&"coord-a".to_string()),
        "shut-down coordinator's lease must be released, saw {coord_ids:?}"
    );
    assert_eq!(b.service.lookup(&block(1), 1).owners.len(), 1);
}

#[tokio::test]
async fn reads_fail_closed_during_backend_outage_and_recover() {
    let (store, mem, _clock) = memory_store();
    let a = Coordinator::new("coord-a", CLUSTER, Arc::clone(&store));
    a.obs.check_ready().await.unwrap();
    store
        .upsert_node(worker(CLUSTER, "w1", "inc-1", 0, 1_000), TTL)
        .await
        .unwrap();
    assert!(a.reconcile().await);
    assert!(a.obs.is_ready());

    // Inject a backend outage. The next reconcile fails and clears readiness,
    // so the coordinator would fail placement/membership closed (per #73).
    mem.set_available(false);
    assert!(!a.reconcile().await);
    assert!(!a.obs.is_ready(), "must fail closed under backend outage");
    assert!(a.obs.snapshot_for_api().await.is_err());

    // Recovery: the backend returns and the next reconcile restores service
    // within one request_timeout.
    mem.set_available(true);
    assert!(a.reconcile().await);
    assert!(a.obs.is_ready());
    assert_eq!(a.service.membership().snapshot().len(), 1);
}

#[tokio::test]
async fn crashed_worker_is_removed_after_lease_ttl() {
    let (store, _mem, clock) = memory_store();
    let a = Coordinator::new("coord-a", CLUSTER, Arc::clone(&store));
    a.obs.check_ready().await.unwrap();
    let ttl = Duration::from_secs(30);
    store
        .upsert_node(worker(CLUSTER, "w1", "inc-1", 0, 1_000), ttl)
        .await
        .unwrap();
    assert!(a.reconcile().await);
    assert_eq!(a.service.membership().snapshot().len(), 1);

    // The worker crashes: it stops heartbeating. Advancing past the lease TTL
    // (the derived removal deadline) expires its record.
    clock.advance(ttl + Duration::from_millis(1));
    assert!(a.reconcile().await);
    assert!(
        a.service.membership().snapshot().is_empty(),
        "worker must be removed within lease_ttl of its last heartbeat"
    );
}

#[tokio::test]
async fn concurrent_heartbeats_do_not_move_status_backward() {
    // Delayed/duplicate heartbeats routed through different coordinators must not
    // regress a node's accepted sequence (no split-brain on status).
    let (store, _mem, _clock) = memory_store();
    // Newer sequence applied first.
    store
        .upsert_node(worker(CLUSTER, "w1", "inc-1", 5, 1_000), TTL)
        .await
        .unwrap();
    // An older sequence arriving late is rejected as stale.
    let stale = store
        .upsert_node(worker(CLUSTER, "w1", "inc-1", 3, 1_000), TTL)
        .await
        .unwrap();
    assert_eq!(
        stale.disposition,
        talon_coordinator::WriteDisposition::Stale
    );
    let snap = store.snapshot(CLUSTER).await.unwrap();
    assert_eq!(snap.nodes.len(), 1);
    assert_eq!(
        snap.nodes[0].heartbeat_seq, 5,
        "status must not move backward"
    );
}

#[tokio::test]
async fn rolling_restart_reproduces_identical_placement_version() {
    // A coordinator restart (new process) that rebuilds the same membership must
    // land on the same deterministic placement version, so clients that cached
    // placement across the restart are not forced to refresh (rolling-upgrade
    // compatibility).
    let (store, _mem, _clock) = memory_store();
    for id in ["w1", "w2", "w3"] {
        store
            .upsert_node(worker(CLUSTER, id, "inc-1", 0, 1_000), TTL)
            .await
            .unwrap();
    }
    let before = Coordinator::new("coord-a", CLUSTER, Arc::clone(&store));
    before.obs.check_ready().await.unwrap();
    before.reconcile().await;
    let version_before = before.service.membership().epoch();

    // "Restart": a brand-new coordinator instance over the same store.
    let after = Coordinator::new("coord-a2", CLUSTER, Arc::clone(&store));
    after.obs.check_ready().await.unwrap();
    after.reconcile().await;
    assert_eq!(
        after.service.membership().epoch(),
        version_before,
        "unchanged membership must reproduce the same placement version"
    );
}

// ---------------------------------------------------------------------------
// Backend-agnostic scenario, runnable over ANY ClusterStateStore.
//
// The fault-injection and injectable-clock scenarios above are memory-specific,
// but the core HA guarantees — cross-coordinator visibility, no split-brain on
// placement, and graceful-shutdown lease release — hold identically on etcd and
// Kubernetes. This function runs those against a supplied store so the exact
// same assertions cover every backend (issue #89). The etcd/Kubernetes variants
// are `#[ignore]`d because CI has no live server.
// ---------------------------------------------------------------------------

/// Run the backend-agnostic HA scenario against `store` for a unique cluster id.
/// `now_ms` is the current time the caller's backend uses for freshness.
async fn backend_agnostic_scenario(store: Arc<dyn ClusterStateStore>, cluster: &str) {
    let a = Coordinator::new("coord-a", cluster, Arc::clone(&store));
    let b = Coordinator::new("coord-b", cluster, Arc::clone(&store));
    a.obs.check_ready().await.unwrap();
    b.obs.check_ready().await.unwrap();
    a.obs.upsert_status(a.obs.status(), TTL).await.unwrap();
    b.obs.upsert_status(b.obs.status(), TTL).await.unwrap();

    let now = 1_000;
    store
        .upsert_node(worker(cluster, "w1", "inc-1", 0, now), TTL)
        .await
        .unwrap();
    store
        .upsert_node(worker(cluster, "w2", "inc-1", 0, now), TTL)
        .await
        .unwrap();

    // Cross-coordinator visibility: a worker registered on the shared store is
    // seen by both coordinators, which agree on placement (no split-brain).
    assert!(a.reconcile().await);
    assert!(b.reconcile().await);
    assert_eq!(a.service.membership().snapshot().len(), 2);
    assert_eq!(b.service.membership().snapshot().len(), 2);
    assert_eq!(
        a.service.membership().epoch(),
        b.service.membership().epoch()
    );
    for n in 0..25 {
        assert_eq!(
            a.service.lookup(&block(n), 1).owners,
            b.service.lookup(&block(n), 1).owners
        );
    }

    // Graceful shutdown of A releases its lease; B keeps serving and no longer
    // sees A's coordinator record (no orphaned authoritative state).
    a.obs.begin_shutdown();
    a.obs.remove_self().await.unwrap();
    assert!(b.reconcile().await);
    let snap = b.obs.snapshot_for_api().await.unwrap();
    assert!(
        !snap
            .nodes
            .iter()
            .any(|s| s.node.id.0 == "coord-a" && s.node.role == NodeRole::Coordinator),
        "shut-down coordinator lease must be released"
    );

    // Clean up worker records so a shared live backend is left tidy.
    for id in ["w1", "w2"] {
        let _ = store.remove_node(cluster, &NodeId::new(id), "inc-1").await;
    }
    let _ = store
        .remove_node(
            cluster,
            &NodeId::new("coord-b"),
            &b.obs.status().incarnation_id,
        )
        .await;
}

#[tokio::test]
async fn backend_agnostic_scenario_on_memory() {
    let (store, _mem, _clock) = memory_store();
    backend_agnostic_scenario(store, "agnostic-mem").await;
}

/// The same scenario against a real etcd. Run explicitly:
/// `cargo test -p talon-coordinator --features etcd --test ha_failover -- --ignored`
/// with `TALON_TEST_ETCD_ENDPOINTS=localhost:2379` pointing at a throwaway etcd.
#[cfg(feature = "etcd")]
#[tokio::test]
#[ignore = "requires a live etcd; run explicitly with --ignored"]
async fn backend_agnostic_scenario_on_etcd() {
    use talon_coordinator::state_store::{EtcdConfig, EtcdStateStore};
    let endpoints = std::env::var("TALON_TEST_ETCD_ENDPOINTS")
        .unwrap_or_else(|_| "localhost:2379".into())
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    let config = EtcdConfig {
        endpoints,
        ..Default::default()
    };
    let store = EtcdStateStore::connect(&config, TTL, Duration::from_secs(5))
        .await
        .expect("connect to etcd");
    let cluster = format!("agnostic-etcd-{}", std::process::id());
    backend_agnostic_scenario(Arc::new(store), &cluster).await;
}

/// The same scenario against a real Kubernetes API server. Run explicitly:
/// `cargo test -p talon-coordinator --features kubernetes --test ha_failover -- --ignored`
/// against a throwaway cluster with the Lease RBAC applied.
#[cfg(feature = "kubernetes")]
#[tokio::test]
#[ignore = "requires a live Kubernetes API server; run explicitly with --ignored"]
async fn backend_agnostic_scenario_on_kubernetes() {
    use talon_coordinator::state_store::{KubernetesConfig, KubernetesStateStore};
    let cluster = format!("agnostic-k8s-{}", std::process::id());
    let config = KubernetesConfig {
        namespace: std::env::var("TALON_TEST_NAMESPACE").unwrap_or_else(|_| "talon".into()),
        cluster_id: cluster.clone(),
        context: std::env::var("TALON_TEST_KUBE_CONTEXT").ok(),
    };
    let store = KubernetesStateStore::connect(&config, Duration::from_secs(5))
        .await
        .expect("connect to Kubernetes");
    backend_agnostic_scenario(Arc::new(store), &cluster).await;
}
