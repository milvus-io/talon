use super::*;
use bytes::Bytes;
use talon_core::{
    worker_membership::{MemberRegistry, WorkerDiscovery, WorkerMember},
    Backend, BackendStore, BlockId, CachePlacementTable, NamespaceId, NodeId, NodeInfo, NodeRole,
    ObjectId, ObjectStat, Version,
};

struct UnusedOrigin;

#[async_trait::async_trait]
impl BackendStore for UnusedOrigin {
    async fn fetch_range(&self, _: &ObjectId, _: u64, _: u64) -> talon_core::Result<Bytes> {
        panic!("ownership GC must not fetch data")
    }

    async fn head(&self, _: &ObjectId) -> talon_core::Result<ObjectStat> {
        panic!("ownership GC must not query backend metadata")
    }
}

fn worker_fixture(root: &std::path::Path) -> (Arc<WorkerRuntime>, WorkerConfig) {
    let config = WorkerConfig {
        node_id: Some("a".into()),
        backend: Some("s3".into()),
        block_size: 32,
        small_object_threshold_bytes: 1 << 20,
        l2_page_size_bytes: 0,
        l1_page_size_bytes: 16,
        l1_capacity_bytes: 0,
        page_tti_hours: 0,
        stale_gc_enabled: false,
        ..Default::default()
    };
    let lock = Arc::new(crate::page_access_store::CacheRootLock::acquire(root).unwrap());
    let worker = WorkerRuntime::open_namespaces(
        &config,
        lock,
        Arc::new(UnusedOrigin),
        crate::WorkerMetrics::new(0),
    )
    .unwrap();
    (Arc::new(worker), config)
}

fn publish(worker: &WorkerRuntime, b_zone: Option<&str>) {
    let discovery = WorkerDiscovery::retained(
        &MemberRegistry {
            members: vec![
                WorkerMember {
                    worker_id: "a".into(),
                    zone: None,
                    retired: false,
                },
                WorkerMember {
                    worker_id: "b".into(),
                    zone: b_zone.map(str::to_owned),
                    retired: false,
                },
            ],
            ..Default::default()
        },
        &[],
    );
    worker
        .update_membership(Some(discovery), Instant::now())
        .unwrap();
}

fn packed_blocks(owner: &str) -> impl Iterator<Item = BlockId> + '_ {
    let nodes = ["a", "b"].map(|id| NodeInfo {
        id: NodeId::new(id),
        address: String::new(),
        role: NodeRole::Worker,
    });
    let placement = CachePlacementTable::new(&nodes);
    (0..10_000)
        .map(|n| {
            BlockId::new(
                ObjectId::new(Backend::S3, "bucket", format!("packed-{n}")),
                0,
                32,
                Version::new("v1"),
            )
        })
        .filter(move |id| placement.primary(id).unwrap().id.0 == owner)
}

async fn fill(worker: &WorkerRuntime, id: &BlockId) {
    worker
        .commit_cached_block(id, Bytes::from_static(b"x"))
        .await
        .unwrap();
    assert!(worker.index.policy_block_get(id).is_none());
}

fn completed_slices(worker: &WorkerRuntime) -> talon_core::Counter {
    worker.metrics.registry.counter(
        "talon_worker_background_task_completed_total",
        "Completed background batches by task.",
        talon_core::metrics::labels(&[("task", "ownership_gc")]),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_protected_namespaces_respect_retry_deadlines() {
    let root = tempfile::tempdir().unwrap();
    let (worker, config) = worker_fixture(root.path());
    let other = worker
        .for_namespace(&NamespaceId::named("analytics"))
        .await
        .unwrap();
    let misplaced = packed_blocks("b").next().unwrap();
    for namespace in [&worker, &other] {
        fill(namespace, &misplaced).await;
    }
    // The extra scan slice puts the two protected rows in opposite retry phases.
    for owned in packed_blocks("a").take(64) {
        fill(&other, &owned).await;
    }
    let first_pin = worker
        .lru
        .pin_guard(crate::CacheUnit::Whole(misplaced.clone()))
        .unwrap();
    let second_pin = other
        .lru
        .pin_guard(crate::CacheUnit::Whole(misplaced.clone()))
        .unwrap();
    publish(&worker, None);
    let completed = completed_slices(&worker);
    let started = Instant::now();
    let service = WorkerBackground::start(worker.clone(), &config);
    tokio::time::sleep(Duration::from_millis(250)).await;
    let elapsed = started.elapsed();
    let count = completed.get();
    eprintln!("ownership slices in {elapsed:?}: {count}");
    assert!(
        count <= 8 + 4 * elapsed.as_secs(),
        "protected namespaces must wait for their own retry deadlines"
    );
    assert!(worker.contains_block(&misplaced));
    assert!(other.contains_block(&misplaced));
    drop((first_pin, second_pin));
    tokio::time::timeout(Duration::from_secs(10), async {
        while worker.contains_block(&misplaced) || other.contains_block(&misplaced) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("both namespaces should retry after their deadlines");
    service.shutdown().await;
}

// Keep the paused clock under test control while polling work that can use
// external I/O threads. Give the scheduler timer wheel a tick for continuations.
async fn drive_scheduler_until(done: impl Fn() -> bool) {
    let started = Instant::now();
    let mut next_tick = started;
    while !done() {
        assert!(started.elapsed() < Duration::from_secs(5));
        if Instant::now() >= next_tick {
            tokio::time::advance(Duration::from_millis(1)).await;
            next_tick = Instant::now() + Duration::from_millis(10);
        }
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn ownership_wakes_for_membership_changes_and_validity_restoration() {
    let root = tempfile::tempdir().unwrap();
    let (worker, config) = worker_fixture(root.path());
    let mut misplaced = packed_blocks("b");
    let protected = misplaced.next().unwrap();
    let late = misplaced.next().unwrap();
    fill(&worker, &protected).await;
    let pin = worker
        .lru
        .pin_guard(crate::CacheUnit::Whole(protected.clone()))
        .unwrap();
    publish(&worker, None);
    let generation = worker.membership_snapshot().unwrap().generation;
    fill(&worker, &late).await;

    tokio::time::pause();
    let started = tokio::time::Instant::now();
    let completed = completed_slices(&worker);
    let service = WorkerBackground::start(worker.clone(), &config);
    drive_scheduler_until(|| completed.get() >= 2).await;
    let previous = completed.get();
    drop(pin);
    publish(&worker, None);
    let namespaced = worker.clone();
    let initializer = tokio::spawn(async move {
        namespaced
            .for_namespace(&NamespaceId::named("analytics"))
            .await
            .unwrap()
    });
    drive_scheduler_until(|| initializer.is_finished() && completed.get() > previous).await;
    initializer.await.unwrap();
    assert!(
        worker.contains_block(&protected),
        "healthy membership and another namespace's wake must preserve the retry deadline"
    );

    // The outage and recovery both occur between scheduler invocations.
    worker.update_membership(None, Instant::now()).unwrap();
    assert!(worker.membership_snapshot().is_none());
    publish(&worker, None);
    assert_eq!(worker.membership_snapshot().unwrap().generation, generation);
    drive_scheduler_until(|| !worker.contains_block(&protected)).await;
    assert!(worker.contains_block(&late), "the event boundary survives");

    publish(&worker, Some("zone-b"));
    assert!(worker.membership_snapshot().unwrap().generation > generation);
    drive_scheduler_until(|| !worker.contains_block(&late)).await;
    assert!(started.elapsed() < Duration::from_secs(1));
    service.shutdown().await;
}
