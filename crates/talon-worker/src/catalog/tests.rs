use super::*;
use crate::PagedBlockStore;
use bytes::Bytes;
use talon_core::{Backend, ObjectId, PageIndex, Version};
use talon_journal::Machine;

#[test]
fn frozen_snapshot_preserves_pages_and_pending_transaction_ids() {
    let mut state = State::default();
    state
        .apply(
            1,
            Record::Store {
                path: "default/shared/paged".into(),
                page_size: 4,
            },
        )
        .unwrap();
    let begin = || Record::Begin {
        intent: model::Intent {
            store: "default/shared/paged".into(),
            block: block(0),
            operation: Operation::Put { page: 0, len: 16 },
        },
    };
    state.apply(2, begin()).unwrap();
    state
        .apply(
            3,
            Record::Complete {
                transaction: 2,
                allocated: 4096,
            },
        )
        .unwrap();
    state.apply(4, begin()).unwrap();
    let snapshot = state.snapshot().unwrap();
    // A later completion must copy the block instead of modifying the frozen
    // snapshot. This state is the one encoded concurrently with new appends.
    state
        .apply(
            5,
            Record::Complete {
                transaction: 4,
                allocated: 8192,
            },
        )
        .unwrap();
    let mut restored = State::default();
    for bytes in snapshot {
        restored
            .restore(State::decode_snapshot(&bytes.unwrap()).unwrap())
            .unwrap();
    }
    let entries = restored.entries("default/shared/paged").unwrap().unwrap();
    assert_eq!(entries[0].allocated, vec![(Some(PageIndex(0)), 4096)]);
    assert!(restored.pending.contains_key(&4));
    restored
        .apply(6, Record::Resolve { transaction: 4 })
        .unwrap();
    assert!(restored.pending.is_empty());
    assert!(restored
        .entries("default/shared/paged")
        .unwrap()
        .unwrap()
        .is_empty());
    assert_eq!(
        state.entries("default/shared/paged").unwrap().unwrap()[0].allocated,
        vec![(Some(PageIndex(0)), 8192)]
    );
    let record = begin();
    assert_eq!(
        State::encoded_len(&record).unwrap(),
        State::encode(&record).unwrap().len()
    );
}

#[tokio::test]
async fn large_block_checkpoint_reopens_without_an_oversized_frame() {
    let config = talon_core::WorkerConfig {
        block_size: 1 << 31,
        l2_page_size_bytes: 1024,
        l1_capacity_bytes: 0,
        capacity_bytes: 16 << 30,
        ..Default::default()
    };
    config.validate().unwrap();
    let mut id = block(0);
    id.block_size = config.block_size;
    let count = id.page_count(config.l2_page_size_bytes as u32);
    let block = model::Block {
        id: id.clone(),
        len: u64::from(id.block_size),
        pages: (0..count).map(|page| (page, 4096)).collect(),
    };
    // This valid state exceeded the engine's single-frame limit before page
    // chunks. Construct metadata directly instead of creating millions of files.
    assert!(serde_json::to_vec(&block).unwrap().len() > 16 << 20);
    let path = "default/shared/paged";
    let mut state = State::default();
    state.stores.insert(
        path.into(),
        model::Store {
            page_size: 1024,
            blocks: [(id.clone(), Arc::new(block))].into(),
        },
    );
    let root = tempfile::tempdir().unwrap();
    let service = service(Arc::new(CacheRootLock::acquire(root.path()).unwrap())).unwrap();
    let handle = service
        .open_stream(PathBuf::from(DIRECTORY), state, StreamOptions::default())
        .unwrap();
    handle.checkpoint().await.unwrap();
    // Tail replay must still mutate a block restored from multiple chunks.
    let transaction = handle
        .append(
            Record::Begin {
                intent: model::Intent {
                    store: path.into(),
                    block: id.clone(),
                    operation: Operation::DeletePage { page: 0 },
                },
            },
            Ack::Durable,
        )
        .await
        .unwrap();
    handle
        .append(
            Record::Complete {
                transaction,
                allocated: 0,
            },
            Ack::Durable,
        )
        .await
        .unwrap();
    drop(handle);
    drop(service);
    let catalog = open(root.path());
    catalog
        .journal
        .query(move |state| {
            let restored = &state.stores[path].blocks[&id];
            assert_eq!(restored.pages.len(), count as usize - 1);
            assert_eq!(restored.len, u64::from(id.block_size));
            assert!(!restored.pages.contains_key(&0));
            assert!(restored
                .pages
                .iter()
                .map(|(&page, &bytes)| (page, bytes))
                .eq((1..count).map(|page| (page, 4096))));
            Ok(())
        })
        .await
        .unwrap();
}

#[test]
fn snapshot_page_chunks_validate_identity_order_and_legacy_records() {
    use model::SnapshotRecord;
    let path = "default/shared/paged";
    let chunk = |page, len, continuation| SnapshotRecord::Block {
        store: path.into(),
        block: model::Block {
            id: block(0),
            len,
            pages: [(page, 4096)].into(),
        },
        continuation,
    };
    let mut state = State::default();
    state
        .restore(SnapshotRecord::Store {
            path: path.into(),
            page_size: 4,
        })
        .unwrap();
    assert!(state.restore(chunk(1, 16, true)).is_err());
    // The old complete-block representation has no continuation flag.
    let mut legacy = serde_json::to_value(chunk(0, 16, false)).unwrap();
    legacy["Block"]
        .as_object_mut()
        .unwrap()
        .remove("continuation");
    state
        .restore(State::decode_snapshot(&serde_json::to_vec(&legacy).unwrap()).unwrap())
        .unwrap();
    assert!(state.restore(chunk(0, 16, false)).is_err());
    assert!(state.restore(chunk(0, 16, true)).is_err());
    assert!(state.restore(chunk(1, 12, true)).is_err());
    assert!(state.restore(chunk(16, 16, true)).is_err());
    state.restore(chunk(2, 16, true)).unwrap();
    assert!(state.restore(chunk(1, 16, true)).is_err());
    assert_eq!(
        state.stores[path].blocks[&block(0)].pages,
        [(0, 4096), (2, 4096)].into()
    );
}

#[tokio::test]
async fn catalog_failure_does_not_poison_another_stream_on_the_root_service() {
    let root = tempfile::tempdir().unwrap();
    let service = service(Arc::new(CacheRootLock::acquire(root.path()).unwrap())).unwrap();
    let catalog = Catalog::with_service(&service, None).unwrap();
    let other = service
        .open_stream(
            PathBuf::from(".independent-test"),
            State::default(),
            StreamOptions::default(),
        )
        .unwrap();
    catalog.register("default/shared/paged", 4).unwrap();
    catalog.poison();
    assert!(catalog
        .begin("default/shared/paged", &block(0), Operation::DeleteBlock)
        .await
        .is_err());
    other
        .append(
            Record::Store {
                path: "other/shared/paged".into(),
                page_size: 4,
            },
            Ack::Durable,
        )
        .await
        .unwrap();
    other.checkpoint().await.unwrap();
    assert!(other
        .query(|state| Ok(state.stores.contains_key("other/shared/paged")))
        .await
        .unwrap());
}

fn block(n: u64) -> BlockId {
    BlockId::new(
        ObjectId::new(Backend::S3, "bucket", format!("key/{n}")),
        0,
        64,
        Version::new("v1"),
    )
}

fn open(root: &Path) -> Arc<Catalog> {
    Catalog::open(Arc::new(CacheRootLock::acquire(root).unwrap())).unwrap()
}

#[test]
fn many_pending_intents_reconcile_inside_tokio_startup() {
    const CHILD: &str = "TALON_CATALOG_COOP_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let root = tempfile::tempdir().unwrap();
        let catalog = open(root.path());
        let path = "default/shared/paged";
        catalog.register(path, 4).unwrap();
        for n in 0..256 {
            futures::executor::block_on(catalog.begin(
                path,
                &block(n),
                Operation::Put { page: 0, len: 4 },
            ))
            .unwrap();
        }
        drop(catalog);
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let catalog = open(root.path());
                assert!(catalog
                    .journal
                    .query_blocking(|state| Ok(state.pending.is_empty()))
                    .unwrap());
                assert!(catalog.recover(path).unwrap().unwrap().is_empty());
            });
        return;
    }
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "catalog::tests::many_pending_intents_reconcile_inside_tokio_startup",
        ])
        .env(CHILD, "1")
        .spawn()
        .unwrap();
    let started = std::time::Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        if started.elapsed() > std::time::Duration::from_secs(10) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("pending-intent reconciliation exhausted the outer Tokio budget");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

fn store(root: &Path, catalog: &Arc<Catalog>, name: &str) -> Arc<PagedBlockStore> {
    let path = format!("{name}/shared/paged");
    if catalog.recover(&path).unwrap().is_none() {
        catalog.register(&path, 4).unwrap();
    }
    Arc::new(
        PagedBlockStore::open(root.join(&path), 4)
            .unwrap()
            .with_catalog(catalog.clone(), path),
    )
}

#[tokio::test]
async fn replay_restores_exact_residency_and_allocation_without_listing_data() {
    let root = tempfile::tempdir().unwrap();
    let catalog = open(root.path());
    let paged = store(root.path(), &catalog, "default");
    paged
        .commit_page(&block(0), PageIndex(0), 16, Bytes::from_static(b"aaaa"))
        .await
        .unwrap();
    paged
        .commit_page(&block(0), PageIndex(2), 16, Bytes::from_static(b"bbbb"))
        .await
        .unwrap();
    paged
        .commit_page(&block(1), PageIndex(1), 12, Bytes::from_static(b"cccc"))
        .await
        .unwrap();
    paged
        .evict_page_async(&block(0), PageIndex(0))
        .await
        .unwrap();
    paged.delete_block_async(&block(1)).await.unwrap();
    assert!(!paged.dir_for(&block(0)).join("block.meta").exists());
    let before = catalog.recover("default/shared/paged").unwrap().unwrap();
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].allocated.len(), 1);
    let expected = before[0].allocated.clone();
    drop(paged);
    drop(catalog);
    // If recovery enumerates or stats data, this replacement makes it fail.
    let data = root.path().join("default/shared/paged");
    std::fs::rename(&data, root.path().join("hidden-data")).unwrap();
    std::fs::write(&data, b"not a directory").unwrap();
    let catalog = open(root.path());
    let recovered = catalog.recover("default/shared/paged").unwrap().unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].allocated, expected);
    assert_eq!(recovered[0].meta.id, block(0));
    assert_eq!(recovered[0].meta.len, 16);
}

#[tokio::test]
async fn pending_put_is_cleaned_by_exact_path_including_replaced_page() {
    for checkpoint in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let catalog = open(root.path());
        let paged = store(root.path(), &catalog, "default");
        paged
            .commit_page(&block(0), PageIndex(0), 16, Bytes::from_static(b"old!"))
            .await
            .unwrap();
        let id = catalog
            .begin(
                "default/shared/paged",
                &block(0),
                Operation::Put { page: 0, len: 16 },
            )
            .await
            .unwrap();
        let dir = paged.dir_for(&block(0));
        let temp = dir.join(format!("0.page.catalog.{}", id.id));
        std::fs::write(&temp, b"partial").unwrap();
        if checkpoint {
            catalog.checkpoint().await.unwrap();
        }
        drop(paged);
        drop(catalog);
        let catalog = open(root.path());
        assert!(!temp.exists());
        assert!(!dir.join("0.page").exists());
        assert!(catalog
            .recover("default/shared/paged")
            .unwrap()
            .unwrap()
            .is_empty());
        let paged = store(root.path(), &catalog, "default");
        paged
            .commit_page(&block(0), PageIndex(0), 16, Bytes::from_static(b"new!"))
            .await
            .unwrap();
        drop(paged);
        drop(catalog);
        let catalog = open(root.path());
        assert_eq!(
            catalog
                .recover("default/shared/paged")
                .unwrap()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(std::fs::read(dir.join("0.page")).unwrap(), b"new!");
    }
}

#[tokio::test]
async fn pending_deletes_survive_checkpoint_and_recreation() {
    for operation in [Operation::DeletePage { page: 0 }, Operation::DeleteBlock] {
        let root = tempfile::tempdir().unwrap();
        let catalog = open(root.path());
        let paged = store(root.path(), &catalog, "default");
        paged
            .commit_page(&block(0), PageIndex(0), 16, Bytes::from_static(b"data"))
            .await
            .unwrap();
        catalog
            .begin("default/shared/paged", &block(0), operation.clone())
            .await
            .unwrap();
        catalog.checkpoint().await.unwrap();
        let path = paged.dir_for(&block(0)).join("0.page");
        drop(paged);
        drop(catalog);
        let catalog = open(root.path());
        assert!(!path.exists());
        assert!(catalog
            .recover("default/shared/paged")
            .unwrap()
            .unwrap()
            .is_empty());
    }
}

#[tokio::test]
async fn checkpoint_replays_only_subsequent_log_and_keeps_namespaces_separate() {
    let root = tempfile::tempdir().unwrap();
    let catalog = open(root.path());
    let a = store(root.path(), &catalog, "a");
    let b = store(root.path(), &catalog, "b");
    let mut tasks = Vec::new();
    for n in 0..32 {
        let paged = if n % 2 == 0 { a.clone() } else { b.clone() };
        tasks.push(tokio::spawn(async move {
            paged
                .commit_page(&block(n), PageIndex(0), 4, Bytes::from_static(b"data"))
                .await
                .unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    catalog.checkpoint().await.unwrap();
    a.delete_block_async(&block(0)).await.unwrap();
    b.commit_page(&block(32), PageIndex(0), 4, Bytes::from_static(b"more"))
        .await
        .unwrap();
    drop(a);
    drop(b);
    drop(catalog);
    let catalog = open(root.path());
    assert_eq!(
        catalog.recover("a/shared/paged").unwrap().unwrap().len(),
        15
    );
    assert_eq!(
        catalog.recover("b/shared/paged").unwrap().unwrap().len(),
        17
    );
}

#[tokio::test]
async fn failed_file_mutation_blocks_further_admissions_until_recovery() {
    let root = tempfile::tempdir().unwrap();
    let catalog = open(root.path());
    let paged = store(root.path(), &catalog, "default");
    let dir = paged.dir_for(&block(0));
    std::fs::create_dir_all(dir.parent().unwrap()).unwrap();
    std::fs::write(&dir, b"obstruction").unwrap();
    assert!(paged
        .commit_page(&block(0), PageIndex(0), 4, Bytes::from_static(b"data"))
        .await
        .is_err());
    assert!(paged
        .commit_page(&block(1), PageIndex(0), 4, Bytes::from_static(b"data"))
        .await
        .is_err());
    drop(paged);
    drop(catalog);
    std::fs::remove_file(dir).unwrap();
    let catalog = open(root.path());
    assert!(catalog
        .recover("default/shared/paged")
        .unwrap()
        .unwrap()
        .is_empty());
}
