use super::*;
use crate::small_store::{tests::files, SegmentStore};
use std::time::Duration;
use talon_core::{Backend, BlockId, ObjectId, Version, WorkerConfig};
fn id(n: &str) -> BlockId {
    BlockId::new(
        ObjectId::new(Backend::S3, "bucket", n),
        0,
        1 << 20,
        Version::new("v1"),
    )
}
fn store(root: &Path) -> Arc<SegmentStore> {
    Arc::new(SegmentStore::open(root.to_owned(), 128 << 10, 256 << 10, 4 << 20, None).unwrap())
}
#[tokio::test]
async fn snapshot_does_not_block_appends_and_replays_mutations_after_its_cut() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(dir.path());
    store
        .put(&id("a"), Bytes::from_static(b"first"))
        .await
        .unwrap();
    store.checkpoint().await.unwrap();
    // Include an unsealed object in the snapshot. Its later seal must apply to
    // the snapshot's insertion rather than requiring the old discarded log.
    store
        .put(&id("b"), Bytes::from_static(b"second"))
        .await
        .unwrap();
    let budget = crate::background::BackgroundBudget::new(&WorkerConfig {
        background_io_concurrency: 1,
        background_io_max_mb_per_sec: 0,
        background_delete_max_per_sec: 0,
        ..Default::default()
    });
    store.journal.set_budget(budget.clone());
    let permit = budget.io().await;
    let j = store.journal.clone();
    let checkpoint = tokio::spawn(async move { j.checkpoint().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !dir
            .path()
            .join("metadata/journal.00000000000000000002")
            .exists()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let writes = tokio::time::timeout(Duration::from_secs(2), async {
        store
            .put(&id("c"), Bytes::from_static(b"third"))
            .await
            .unwrap();
        store.delete(&id("a")).await.unwrap();
        store.checkpoint().await.unwrap();
    })
    .await;
    drop(permit);
    writes.unwrap();
    checkpoint.await.unwrap().unwrap();
    drop(store);
    let recovered = self::store(dir.path());
    assert!(!recovered.contains(&id("a")));
    assert_eq!(
        recovered.read(&id("b")).await.unwrap().unwrap().as_ref(),
        b"second"
    );
    assert_eq!(
        recovered.read(&id("c")).await.unwrap().unwrap().as_ref(),
        b"third"
    );
    assert_eq!(
        files(dir.path())
            .iter()
            .filter(|p| p
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("snapshot."))
            .count(),
        1
    );
    assert!(!files(dir.path())
        .iter()
        .any(|p| p.extension().is_some_and(|e| e == "index")));
}

#[tokio::test]
async fn corrupt_snapshot_is_rejected_without_deleting_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(dir.path());
    store
        .put(&id("a"), Bytes::from_static(b"first"))
        .await
        .unwrap();
    store.maintain(true).await.unwrap();
    drop(store);
    let snapshot = files(dir.path())
        .into_iter()
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("snapshot.")
        })
        .unwrap();
    std::fs::write(snapshot, b"broken snapshot").unwrap();
    let before = files(dir.path());
    assert!(
        SegmentStore::open(dir.path().to_owned(), 128 << 10, 256 << 10, 4 << 20, None).is_err()
    );
    assert_eq!(before, files(dir.path()));
}

#[tokio::test]
async fn reopening_after_cancelled_checkpoint_waits_for_retiring_writer() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(dir.path());
    store
        .put(&id("a"), Bytes::from_static(b"first"))
        .await
        .unwrap();
    store.checkpoint().await.unwrap();
    let j = store.journal.clone();
    let request = tokio::spawn(async move { j.checkpoint().await });
    tokio::task::yield_now().await;
    request.abort();
    let _ = request.await;
    drop(store);
    let recovered = self::store(dir.path());
    assert!(recovered.contains(&id("a")));
}

#[tokio::test]
async fn shared_snapshot_budget_uses_the_journal_runtime_timer() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(dir.path());
    store
        .journal
        .set_budget(crate::background::BackgroundBudget::new(&WorkerConfig {
            background_io_max_mb_per_sec: 1,
            background_delete_max_per_sec: 100,
            ..Default::default()
        }));
    store
        .put(&id("paced"), Bytes::from_static(b"payload"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), store.maintain(true))
        .await
        .unwrap()
        .unwrap();
    drop(store);
    let recovered = self::store(dir.path());
    assert!(recovered.contains(&id("paced")));
}

#[tokio::test]
async fn recovered_small_store_tail_uses_initial_snapshot_budget() {
    let dir = tempfile::tempdir().unwrap();
    let service =
        JournalService::open(dir.path().to_owned(), Arc::new(()), Options::default()).unwrap();
    let root = dir.path().join("small");
    let handle = service
        .open_stream(
            "small/metadata".into(),
            Model {
                next: 1,
                ..Default::default()
            },
            StreamOptions {
                checkpoint_bytes: u64::MAX,
                ..Default::default()
            },
        )
        .unwrap();
    let key: Arc<[u8]> = format::key(&id(&"k".repeat(60 << 10))).unwrap().into();
    let log = root.join("metadata/journal.00000000000000000001");
    let mut offset = 0;
    // Build a valid large tail without checkpointing it, as can remain after
    // process loss while the preceding generation's snapshot is in progress.
    while std::fs::metadata(&log).unwrap().len() < StreamOptions::default().checkpoint_bytes {
        handle
            .append(
                Event::Insert {
                    slot: 0,
                    segment: 1,
                    offset,
                    record: Record {
                        key: key.clone(),
                        len: 1,
                        checksum: 0,
                    },
                },
                Ack::Written,
            )
            .await
            .unwrap();
        handle
            .append(Event::Delete { segment: 1, offset }, Ack::Written)
            .await
            .unwrap();
        offset += 1;
    }
    handle
        .append(Event::SetNext { next: 2 }, Ack::Durable)
        .await
        .unwrap();
    drop(handle);
    drop(service);
    JournalService::wait_for_retiring(dir.path()).unwrap();

    let service =
        JournalService::open(dir.path().to_owned(), Arc::new(()), Options::default()).unwrap();
    let budget = crate::background::BackgroundBudget::new(&WorkerConfig {
        background_io_concurrency: 1,
        background_io_max_mb_per_sec: 0,
        background_delete_max_per_sec: 0,
        ..Default::default()
    });
    let permit = budget.io().await;
    let store = Arc::new(
        crate::small_store::SmallObjectStore::open_with_service(
            root.clone(),
            128 << 10,
            256 << 10,
            4 << 20,
            None,
            Some(&service),
            Some(budget),
        )
        .unwrap(),
    );
    let snapshot = root.join("metadata/snapshot.tmp");
    tokio::time::timeout(Duration::from_secs(2), async {
        while !root.join("metadata/journal.00000000000000000002").exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let snapshot_exists = snapshot.exists();
    let retained = log.exists();
    drop(permit);
    assert!(
        !snapshot_exists,
        "recovery snapshot must wait for the shared I/O permit"
    );
    assert!(
        retained,
        "recovery must retain the old log until snapshot publication"
    );
    tokio::time::timeout(Duration::from_secs(5), store.maintain(true))
        .await
        .unwrap()
        .unwrap();
    assert!(!log.exists());
}
