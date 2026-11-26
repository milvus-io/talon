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

fn fixture_config() -> WorkerConfig {
    WorkerConfig {
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
    }
}

fn open_worker(root: &std::path::Path, config: &WorkerConfig) -> Arc<WorkerRuntime> {
    let lock = Arc::new(crate::page_access_store::CacheRootLock::acquire(root).unwrap());
    let worker = WorkerRuntime::open_namespaces(
        config,
        lock,
        Arc::new(UnusedOrigin),
        crate::WorkerMetrics::new(0),
    )
    .unwrap();
    Arc::new(worker)
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

#[tokio::test]
async fn two_protected_namespaces_respect_retry_deadlines() {
    let root = tempfile::tempdir().unwrap();
    let config = WorkerConfig {
        background_delete_max_per_sec: 0,
        background_io_max_mb_per_sec: 0,
        ..fixture_config()
    };
    let worker = open_worker(root.path(), &config);
    // The scheduler visits this namespace after default.
    let other = worker
        .for_namespace(&NamespaceId::named("reports"))
        .await
        .unwrap();
    let mut foreign = packed_blocks("b");
    let misplaced = foreign.next().unwrap();
    let blocker = foreign.next().unwrap();
    for namespace in [&worker, &other] {
        fill(namespace, &misplaced).await;
    }
    // Delay completion of the second scan to stagger the two retry deadlines.
    fill(&other, &blocker).await;
    let state = other.page_lifecycle.block(&blocker);
    let gate = state.gate.lock().await;
    let first_pin = worker
        .lru
        .pin_guard(crate::CacheUnit::Whole(misplaced.clone()))
        .unwrap();
    let second_pin = other
        .lru
        .pin_guard(crate::CacheUnit::Whole(misplaced.clone()))
        .unwrap();
    publish(&worker, None);
    let membership = worker.membership_snapshot().unwrap();
    let completed = completed_slices(&worker);
    let first = worker.small.as_ref().unwrap();
    let second = other.small.as_ref().unwrap();

    tokio::time::pause();
    let started = tokio::time::Instant::now();
    let service = WorkerBackground::start(worker.clone(), &config);
    // The second namespace has registered both its pinned row and the first
    // attempt blocked on the gate.
    drive_scheduler_until(|| {
        first.ownership_retry_count() == 1
            && second.ownership_retry_count() == 2
            && other.page_mutations.active_count() == 1
    })
    .await;
    let first_scan_finished = tokio::time::Instant::now();
    tokio::time::advance(Duration::from_millis(300)).await;
    let second_scan_released = tokio::time::Instant::now();
    let blocked = completed.get();
    drop(gate);
    drive_scheduler_until(|| completed.get() > blocked && !other.contains_block(&blocker)).await;
    // A slice that hit its wall-clock work limit can finish on a continuation.
    tokio::time::sleep(Duration::from_millis(5)).await;
    let second_scan_finished = tokio::time::Instant::now();
    assert_eq!(first.ownership_retry_count(), 1);
    assert_eq!(second.ownership_retry_count(), 1);
    let before_retries = completed.get();
    drop((first_pin, second_pin));

    let before_first_retry = started + Duration::from_millis(995);
    assert!(second_scan_finished < before_first_retry);
    tokio::time::sleep_until(before_first_retry).await;
    assert_eq!(completed.get(), before_retries, "no slice is due yet");
    assert!(worker.contains_block(&misplaced));
    assert!(other.contains_block(&misplaced));

    tokio::time::sleep_until(first_scan_finished + Duration::from_millis(1010)).await;
    drive_scheduler_until(|| !worker.contains_block(&misplaced)).await;
    assert!(tokio::time::Instant::now() < second_scan_released + Duration::from_secs(1));
    assert_eq!(first.ownership_retry_count(), 0);
    assert_eq!(second.ownership_retry_count(), 1);
    assert!(other.contains_block(&misplaced));

    tokio::time::sleep_until(second_scan_released + Duration::from_millis(995)).await;
    assert!(other.contains_block(&misplaced));
    assert_eq!(second.ownership_retry_count(), 1);
    tokio::time::sleep_until(second_scan_finished + Duration::from_millis(1010)).await;
    drive_scheduler_until(|| !other.contains_block(&misplaced)).await;
    assert_eq!(second.ownership_retry_count(), 0);
    let current = worker.membership_snapshot().unwrap();
    assert!(Arc::ptr_eq(&current, &membership));
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
    // Keep wall-clock rate-limit deadlines out of this paused-clock wakeup test.
    let config = WorkerConfig {
        background_delete_max_per_sec: 0,
        background_io_max_mb_per_sec: 0,
        ..fixture_config()
    };
    let worker = open_worker(root.path(), &config);
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
