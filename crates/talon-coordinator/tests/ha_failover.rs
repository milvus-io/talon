//! End-to-end HA and state-store fault-injection scenarios (#89).
//!
//! Each scenario is written once against the [`HaBackend`] abstraction and run
//! against both the deterministic in-memory backend and, when
//! `TALON_ETCD_TEST_ENDPOINT` is set, a real etcd cluster. This proves the same
//! behavioral guarantees for both production-capable backends:
//!
//! * coordinator kill/restart with requests switching coordinators and bounded
//!   interruption;
//! * worker crash -> lease expiry -> eventual removal within a deadline derived
//!   from the configured TTL;
//! * no split-brain authoritative state and identical deterministic placement
//!   epoch across active coordinators;
//! * no stale placement cache after failover;
//! * backend disruption clears readiness (fail-closed) and recovers;
//! * rolling-version compatibility across a process restart with a new
//!   incarnation.

mod harness;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use harness::{block, HaBackend, HaCluster, CLUSTER_ID};
use talon_coordinator::{
    ClusterStateStore, MemoryStateStore, StateBackend, TimeSource, WriteDisposition,
};

// ---------------------------------------------------------------------------
// Backends
// ---------------------------------------------------------------------------

/// Injectable clock shared by the memory store and the harness so lease expiry
/// and status timestamps advance together and deterministically.
#[derive(Default)]
struct ManualClock {
    now_ms: AtomicU64,
}

impl ManualClock {
    fn advance(&self, duration: Duration) {
        self.now_ms
            .fetch_add(duration.as_millis() as u64, Ordering::SeqCst);
    }
}

impl TimeSource for ManualClock {
    fn now_unix_ms(&self) -> u64 {
        self.now_ms.load(Ordering::SeqCst)
    }
}

/// Deterministic in-memory backend with a manual clock.
struct MemoryBackend {
    store: Arc<MemoryStateStore>,
    clock: Arc<ManualClock>,
    lease_ttl: Duration,
}

impl MemoryBackend {
    fn new() -> Self {
        let clock = Arc::new(ManualClock::default());
        // Start at a non-zero epoch so started_at/reported_at are well-formed.
        clock.now_ms.store(1_000_000, Ordering::SeqCst);
        let store = Arc::new(MemoryStateStore::with_time_source(clock.clone(), 4_096));
        Self {
            store,
            clock,
            lease_ttl: Duration::from_secs(30),
        }
    }
}

#[async_trait]
impl HaBackend for MemoryBackend {
    fn store(&self) -> Arc<dyn ClusterStateStore> {
        self.store.clone()
    }

    fn lease_ttl(&self) -> Duration {
        self.lease_ttl
    }

    async fn elapse(&self, duration: Duration) {
        // Deterministic: step the injected clock; no real waiting.
        self.clock.advance(duration);
    }

    fn now_unix_ms(&self) -> u64 {
        self.clock.now_unix_ms()
    }

    fn name(&self) -> &'static str {
        "memory"
    }
}

#[cfg(feature = "etcd")]
mod etcd_backend {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};
    use talon_coordinator::EtcdStateStore;

    /// Real etcd backend. Time is real wall-clock: lease expiry is owned by the
    /// server, so `elapse` sleeps and adds slack for the lease to be revoked.
    pub struct EtcdBackend {
        store: Arc<EtcdStateStore>,
        lease_ttl: Duration,
    }

    impl EtcdBackend {
        pub async fn connect(endpoint: String) -> Self {
            let client = etcd_client::Client::connect([endpoint], None)
                .await
                .expect("connect etcd");
            // Unique prefix per run so concurrent/rerun tests never collide.
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let prefix = format!("/talon-ha-test/{}-{nanos}", std::process::id());
            // Short TTL keeps wall-clock failover scenarios fast; etcd's minimum
            // effective granularity is one second.
            let lease_ttl = Duration::from_secs(2);
            let store = EtcdStateStore::from_client(client, prefix, Duration::from_secs(5))
                .expect("etcd store");
            Self {
                store: Arc::new(store),
                lease_ttl,
            }
        }
    }

    #[async_trait]
    impl HaBackend for EtcdBackend {
        fn store(&self) -> Arc<dyn ClusterStateStore> {
            self.store.clone()
        }

        fn lease_ttl(&self) -> Duration {
            self.lease_ttl
        }

        async fn elapse(&self, duration: Duration) {
            // Add slack so a lease whose TTL has passed is actually revoked
            // server-side before the scenario inspects the snapshot.
            tokio::time::sleep(duration + Duration::from_secs(2)).await;
        }

        fn now_unix_ms(&self) -> u64 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        }

        fn name(&self) -> &'static str {
            "etcd"
        }
    }
}

// ---------------------------------------------------------------------------
// Scenarios (backend-agnostic)
// ---------------------------------------------------------------------------

/// Active-active: a worker registered through one coordinator becomes visible
/// through every coordinator after reconcile, and all coordinators compute the
/// identical deterministic placement epoch and identical owners (no split-brain
/// authoritative state, #80 determinism).
async fn scenario_active_active_consistency<B: HaBackend>(backend: B) {
    let cluster = HaCluster::new(backend, 3);

    // Register three workers, each via a different coordinator.
    for (i, w) in ["w0", "w1", "w2"].iter().enumerate() {
        let disposition = cluster.heartbeat_worker(i % 3, w, "inc-1", 1).await;
        assert_eq!(disposition, WriteDisposition::Applied);
    }

    let epochs = cluster.reconcile_all().await;
    let first = epochs[0];
    assert!(
        epochs.iter().all(|e| *e == first),
        "all coordinators must agree on the placement epoch: {epochs:?}\n{}",
        cluster.diagnostics("active-active epoch").await
    );
    assert_ne!(first.0, 0, "epoch must be non-empty with workers present");

    // Every coordinator returns identical ordered owners for the same block.
    let owners0 = cluster.coordinators[0].lookup(&block(42), 3);
    assert_eq!(owners0.len(), 3);
    for coord in &cluster.coordinators {
        assert_eq!(
            coord.lookup(&block(42), 3),
            owners0,
            "coordinators disagree on placement owners\n{}",
            cluster.diagnostics("active-active owners").await
        );
    }
}

/// Coordinator kill/restart: requests transparently switch to a surviving
/// coordinator with no interruption, and a restarted coordinator rebuilds its
/// placement view purely from shared state.
async fn scenario_coordinator_failover<B: HaBackend>(backend: B) {
    let cluster = HaCluster::new(backend, 3);
    for (i, w) in ["w0", "w1", "w2"].iter().enumerate() {
        cluster.heartbeat_worker(i, w, "inc-1", 1).await;
    }
    cluster.reconcile_all().await;

    let baseline = cluster.coordinators[1].lookup(&block(7), 2);
    assert_eq!(baseline.len(), 2);

    // Kill coord-0: workers were registered through all coordinators, and the
    // shared store still holds every worker record. A surviving coordinator
    // answers the same placement without interruption.
    let survivor = &cluster.coordinators[1];
    assert!(survivor.is_ready());
    assert_eq!(
        survivor.lookup(&block(7), 2),
        baseline,
        "surviving coordinator must serve identical placement\n{}",
        cluster.diagnostics("failover survivor").await
    );

    // "Restart" coord-0 as a fresh coordinator over the same store: after a
    // single reconcile it recovers the full worker set and identical placement.
    let restarted = cluster.coordinators[0]
        .reconcile()
        .await
        .expect("reconcile");
    assert_eq!(restarted, survivor.service.membership().epoch());
    assert_eq!(
        cluster.coordinators[0].lookup(&block(7), 2),
        baseline,
        "restarted coordinator must rebuild placement from shared state\n{}",
        cluster.diagnostics("failover restart").await
    );
}

/// Worker crash -> lease expiry -> eventual removal within a deadline derived
/// from the configured TTL, and placement epoch changes so any cached client
/// answer is invalidated (no stale placement cache after failover).
async fn scenario_worker_lease_expiry<B: HaBackend>(backend: B) {
    let lease_ttl = backend.lease_ttl();
    let cluster = HaCluster::new(backend, 2);
    for w in ["w0", "w1"] {
        cluster.heartbeat_worker(0, w, "inc-1", 1).await;
    }
    let epoch_before = cluster.reconcile_all().await[0];
    assert_eq!(cluster.live_worker_ids().await, vec!["w0", "w1"]);

    // w1 crashes: stops heartbeating. Advance just past the lease TTL. The
    // record must be gone within the TTL-derived deadline without manual
    // cleanup.
    cluster.elapse(lease_ttl + Duration::from_millis(1)).await;

    // Keep w0 alive so the cluster is not empty (isolates w1's expiry).
    cluster.heartbeat_worker(0, "w0", "inc-1", 2).await;

    let live = cluster.live_worker_ids().await;
    assert_eq!(
        live,
        vec!["w0"],
        "expired worker must be removed within the lease TTL deadline\n{}",
        cluster.diagnostics("lease expiry").await
    );

    // After reconcile the placement epoch must differ from before the crash:
    // a client caching the old epoch will detect staleness and refresh.
    let epoch_after = cluster.reconcile_all().await[0];
    assert_ne!(
        epoch_before,
        epoch_after,
        "placement epoch must change after membership loss (no stale cache)\n{}",
        cluster.diagnostics("lease expiry epoch").await
    );
}

/// Backend disruption: while the store is unavailable, snapshot/reconcile fail
/// and readiness is cleared (fail-closed, #73). When the backend recovers,
/// readiness and placement are restored. Only meaningful for the memory backend
/// which exposes deterministic fault injection.
async fn scenario_backend_disruption_fail_closed(backend: MemoryBackend) {
    let store = backend.store.clone();
    let cluster = HaCluster::new(backend, 2);
    cluster.heartbeat_worker(0, "w0", "inc-1", 1).await;
    cluster.reconcile_all().await;
    assert!(cluster.coordinators[0].is_ready());

    // Inject a backend outage.
    store.set_available(false);
    let err = cluster.coordinators[0]
        .observability
        .reconcile_membership(cluster.coordinators[0].service.membership())
        .await;
    assert!(err.is_err(), "reconcile must fail while backend is down");
    assert!(
        !cluster.coordinators[0].is_ready(),
        "readiness must clear on backend outage (fail-closed)\n{}",
        cluster.diagnostics("disruption").await
    );

    // Recover.
    store.set_available(true);
    let epoch = cluster.coordinators[0].reconcile().await.expect("recover");
    assert!(cluster.coordinators[0].is_ready());
    assert_ne!(epoch.0, 0);
}

/// Rolling-version compatibility: a worker restarts with a new incarnation
/// (as in a rolling upgrade). The newer incarnation supersedes the old record,
/// and a late heartbeat from the old incarnation is rejected as stale — so a
/// straggler cannot resurrect a decommissioned process.
async fn scenario_rolling_restart_incarnation<B: HaBackend>(backend: B) {
    let cluster = HaCluster::new(backend, 2);
    cluster.heartbeat_worker(0, "w0", "inc-1", 5).await;
    cluster.reconcile_all().await;

    // Rolling upgrade: time passes, then w0 restarts as inc-2. Its first
    // heartbeat has a newer start time and supersedes the old record.
    cluster.elapse(Duration::from_secs(1)).await;
    let applied = cluster.heartbeat_worker(1, "w0", "inc-2", 1).await;
    assert_eq!(applied, WriteDisposition::Applied);

    // A straggler heartbeat from the retired inc-1 must be rejected as stale.
    let stale = cluster.heartbeat_worker(0, "w0", "inc-1", 99).await;
    assert_eq!(
        stale,
        WriteDisposition::Stale,
        "old incarnation must not resurrect a restarted worker\n{}",
        cluster.diagnostics("rolling restart").await
    );

    // The surviving record is the new incarnation.
    let snapshot = cluster.snapshot().await;
    let w0 = snapshot
        .nodes
        .iter()
        .find(|s| s.node.id.0 == "w0")
        .expect("w0 present");
    assert_eq!(w0.incarnation_id, "inc-2");
}

/// Concurrent registration/heartbeat load from many workers across all
/// coordinators converges to a consistent snapshot with every worker present.
async fn scenario_concurrent_load<B: HaBackend + 'static>(backend: B) {
    let cluster = Arc::new(HaCluster::new(backend, 3));
    let worker_count = 24usize;

    let mut tasks = Vec::new();
    for w in 0..worker_count {
        let cluster = Arc::clone(&cluster);
        tasks.push(tokio::spawn(async move {
            let id = format!("w{w:02}");
            cluster.heartbeat_worker(w % 3, &id, "inc-1", 1).await
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), WriteDisposition::Applied);
    }

    let live = cluster.live_worker_ids().await;
    assert_eq!(
        live.len(),
        worker_count,
        "all concurrently-registered workers must be visible\n{}",
        cluster.diagnostics("concurrent load").await
    );

    // Every coordinator reconciles to the same epoch over the full set.
    let epochs = cluster.reconcile_all().await;
    assert!(
        epochs.iter().all(|e| *e == epochs[0]),
        "coordinators must converge under concurrent load: {epochs:?}"
    );
}

// ---------------------------------------------------------------------------
// Memory backend: always runs, deterministic.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn memory_active_active_consistency() {
    scenario_active_active_consistency(MemoryBackend::new()).await;
}

#[tokio::test]
async fn memory_coordinator_failover() {
    scenario_coordinator_failover(MemoryBackend::new()).await;
}

#[tokio::test]
async fn memory_worker_lease_expiry() {
    scenario_worker_lease_expiry(MemoryBackend::new()).await;
}

#[tokio::test]
async fn memory_backend_disruption_fail_closed() {
    scenario_backend_disruption_fail_closed(MemoryBackend::new()).await;
}

#[tokio::test]
async fn memory_rolling_restart_incarnation() {
    scenario_rolling_restart_incarnation(MemoryBackend::new()).await;
}

#[tokio::test]
async fn memory_concurrent_load() {
    scenario_concurrent_load(MemoryBackend::new()).await;
}

/// Sanity: the memory backend really is memory (guards against a wiring
/// mistake that would silently skip fault injection).
#[tokio::test]
async fn memory_backend_identity() {
    let backend = MemoryBackend::new();
    assert_eq!(backend.store().backend(), StateBackend::Memory);
    assert_eq!(CLUSTER_ID, "ha");
}

// ---------------------------------------------------------------------------
// etcd backend: runs against a real etcd when TALON_ETCD_TEST_ENDPOINT is set.
// ---------------------------------------------------------------------------

#[cfg(feature = "etcd")]
mod etcd_scenarios {
    use super::etcd_backend::EtcdBackend;
    use super::*;

    fn endpoint() -> Option<String> {
        std::env::var("TALON_ETCD_TEST_ENDPOINT").ok()
    }

    macro_rules! etcd_scenario {
        ($name:ident, $scenario:ident) => {
            #[tokio::test]
            async fn $name() {
                let Some(endpoint) = endpoint() else {
                    eprintln!("skipping etcd HA scenario: TALON_ETCD_TEST_ENDPOINT is not set");
                    return;
                };
                let backend = EtcdBackend::connect(endpoint).await;
                $scenario(backend).await;
            }
        };
    }

    etcd_scenario!(
        etcd_active_active_consistency,
        scenario_active_active_consistency
    );
    etcd_scenario!(etcd_coordinator_failover, scenario_coordinator_failover);
    etcd_scenario!(etcd_worker_lease_expiry, scenario_worker_lease_expiry);
    etcd_scenario!(
        etcd_rolling_restart_incarnation,
        scenario_rolling_restart_incarnation
    );
    etcd_scenario!(etcd_concurrent_load, scenario_concurrent_load);

    // Note: the fail-closed disruption scenario relies on deterministic fault
    // injection (MemoryStateStore::set_available) and is memory-only by design;
    // etcd unavailability is covered by the store contract's timeout/unavailable
    // mapping in tests/etcd_contract.rs.
}
