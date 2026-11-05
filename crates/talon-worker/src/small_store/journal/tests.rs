use super::*;
use crate::small_store::{tests::files, SegmentStore};
use std::time::Duration;
use talon_core::{Backend, BlockId, ObjectId, Version, WorkerConfig};

#[test]
fn snapshot_keeps_rows_when_live_deletion_compacts_and_append_moves_them() {
    use talon_journal::Machine;
    let insert = |offset, key| Event::Insert {
        slot: 0,
        segment: 1,
        offset,
        record: Record {
            key: crate::block_key::BlockKey::new(&id(key)),
            len: 5,
            checksum: offset,
        },
    };
    let mut model = Model::default();
    model.apply(insert(0, "first")).unwrap();
    model.apply(insert(10, "last")).unwrap();
    let mut snapshot = model.snapshot().unwrap();
    let mut restored = Model::default();
    for _ in 0..3 {
        restored
            .restore(Model::decode_snapshot(&snapshot.next().unwrap().unwrap()).unwrap())
            .unwrap();
    }
    model
        .apply(Event::Delete {
            segment: 1,
            offset: 0,
        })
        .unwrap();
    model.apply(insert(5, "middle")).unwrap();
    for bytes in snapshot {
        restored
            .restore(Model::decode_snapshot(&bytes.unwrap()).unwrap())
            .unwrap();
    }
    assert_eq!(
        restored.segments[&1]
            .records
            .iter()
            .map(|(offset, _)| offset)
            .collect::<Vec<_>>(),
        [0, 10]
    );
    restored
        .apply(Event::Delete {
            segment: 1,
            offset: 0,
        })
        .unwrap();
    restored.apply(insert(5, "middle")).unwrap();
    let rows = |model: &Model| {
        model.segments[&1]
            .records
            .iter()
            .map(|(offset, record)| (offset, record.to_owned().key))
            .collect::<Vec<_>>()
    };
    assert_eq!(rows(&model), rows(&restored));
}

#[test]
fn dense_segment_snapshot_restores_one_hundred_thousand_extents() {
    use talon_journal::Machine;
    let mut model = Model::default();
    for offset in 0..100_000 {
        model
            .apply(Event::Insert {
                slot: 0,
                segment: 1,
                offset,
                record: Record {
                    key: crate::block_key::BlockKey::new(&id(&offset.to_string())),
                    len: 1,
                    checksum: offset,
                },
            })
            .unwrap();
    }
    model.apply(Event::Seal { segment: 1 }).unwrap();
    let mut restored = Model::default();
    for bytes in model.snapshot().unwrap() {
        restored
            .restore(Model::decode_snapshot(&bytes.unwrap()).unwrap())
            .unwrap();
    }
    assert_eq!(restored.segments[&1].records.len(), 100_000);
    for (offset, record) in restored.segments[&1].records.iter() {
        assert_eq!(record.checksum, offset);
        assert_eq!(
            record.key,
            crate::block_key::BlockIdentity::view(&id(&offset.to_string()))
        );
    }
}

#[tokio::test]
async fn compact_journal_preserves_live_and_recovered_identity() {
    use crate::block_key::BlockIdentity;
    use crate::{small_store::SmallObjectStore, Lru};
    let dir = tempfile::tempdir().unwrap();
    let block = id("shared/路径");
    for recovered in [false, true] {
        let store = Arc::new(
            SmallObjectStore::open(dir.path().to_owned(), 128 << 10, 256 << 10, 4 << 20, None)
                .unwrap(),
        );
        let mut units = Vec::new();
        let index = Arc::new(crate::BlockIndex::new());
        store.restore_policy_units(&index, &mut units, true);
        let policy = Arc::new(Lru::from_recovered(index, units, store.len(), true, true));
        store.bind_policy(policy.clone());
        if !recovered {
            store
                .put(&block, Bytes::from_static(b"shared"))
                .await
                .unwrap();
        }
        let stream = store
            .streams()
            .into_iter()
            .find(|s| s.contains(&block))
            .unwrap();
        let journal_key = stream
            .journal
            .handle
            .query_blocking(|m| {
                Ok(m.0
                    .read()
                    .unwrap()
                    .model
                    .segments
                    .values()
                    .flat_map(|s| s.records.values())
                    .next()
                    .unwrap()
                    .to_owned()
                    .key)
            })
            .unwrap();
        let locator_key = stream
            .entries
            .read()
            .unwrap()
            .get_key_value(&block as &dyn BlockIdentity)
            .unwrap()
            .0
            .clone();
        assert_eq!(journal_key, locator_key);
        assert_eq!(journal_key, policy.shared_identity(&block));
        assert_eq!(
            store.read(&block).await.unwrap().unwrap(),
            Bytes::from_static(b"shared")
        );
        store.maintain(true).await.unwrap();
    }
}

#[test]
fn legacy_snapshot_headers_preserve_the_original_durability_boundary() {
    use talon_journal::Machine;
    for sealed in [false, true] {
        let mut bytes = vec![4];
        for n in [1u64, 0, 5, 64] {
            bytes.extend_from_slice(&n.to_le_bytes());
        }
        bytes.push(u8::from(sealed));
        let record = Model::decode_snapshot(&bytes).unwrap();
        let mut model = Model {
            next: 2,
            ..Default::default()
        };
        model.restore(record).unwrap();
        assert_eq!(model.segments[&1].durable_end, if sealed { 5 } else { 0 });
    }
}

#[test]
fn flush_boundaries_reject_record_splits_and_regression() {
    let mut model = Model::default();
    for (offset, key) in [(0, "a"), (5, "b")] {
        model
            .apply(Event::Insert {
                slot: 0,
                segment: 1,
                offset,
                record: Record {
                    key: crate::block_key::BlockKey::new(&id(key)),
                    len: 5,
                    checksum: 0,
                },
            })
            .unwrap();
    }
    for durable_end in [0, 3, 11] {
        assert!(model
            .apply(Event::Flush {
                segment: 1,
                durable_end
            })
            .is_err());
    }
    model
        .apply(Event::Flush {
            segment: 1,
            durable_end: 5,
        })
        .unwrap();
    model
        .apply(Event::Flush {
            segment: 1,
            durable_end: 10,
        })
        .unwrap();
    assert!(model
        .apply(Event::Flush {
            segment: 1,
            durable_end: 5
        })
        .is_err());
}

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
    let key = crate::block_key::BlockKey::new(&id(&"k".repeat(60 << 10)));
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
