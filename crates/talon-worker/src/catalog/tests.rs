use super::*;
use crate::block_key::BlockKey;
use crate::PagedBlockStore;
use bytes::Bytes;
use talon_core::{Backend, ObjectId, PageIndex, Version};
use talon_journal::Machine;

#[test]
fn repeated_whole_put_preserves_canonical_identity() {
    let mut state = State::default();
    state
        .apply(
            1,
            Record::WholeStore {
                path: "whole".into(),
                page_size: 0,
            },
        )
        .unwrap();
    let mut previous = None;
    for transaction in [2, 4] {
        state
            .apply(
                transaction,
                Record::Begin {
                    intent: model::Intent {
                        store: "whole".into(),
                        block: block(0),
                        operation: Operation::PutWhole { len: 4 },
                        shard: Some(0),
                        disk_id: None,
                    },
                },
            )
            .unwrap();
        state
            .apply(
                transaction + 1,
                Record::Complete {
                    transaction,
                    allocated: 4096,
                },
            )
            .unwrap();
        let directory = state.live.directory.read().unwrap();
        let handle = directory.rows.handle(&block(0)).unwrap();
        let row = directory.rows.at(handle as usize - 1).unwrap();
        assert_eq!(row.geometry.disk_id, DiskId::new(2, 0).ok());
        if let Some(old) = previous {
            assert!(BlockKey::ptr_eq(&old, &row.id));
        }
        previous = Some(row.id.clone());
    }
}

#[test]
fn descending_large_page_map_roundtrips_a_frozen_snapshot() {
    const PAGE: u32 = 4096;
    const BLOCK: u32 = 256 << 20;
    const COUNT: u32 = BLOCK / PAGE;
    let mut state = State::default();
    state
        .apply(
            1,
            Record::PagedStore {
                path: "paged".into(),
                page_size: PAGE,
            },
        )
        .unwrap();
    let mut id = block(0);
    id.block_size = BLOCK + 2 * PAGE;
    let disk_id = Some(DiskId::new(2, 0).unwrap());
    let put = |directory: &mut crate::index::Directory, page, bytes| {
        directory.apply(
            &id,
            StoreKind::Paged,
            &Operation::Put {
                page,
                len: u64::from(BLOCK),
            },
            disk_id,
            Some(bytes),
        );
    };
    {
        let mut directory = state.live.directory.write().unwrap();
        for page in (0..COUNT).rev() {
            put(&mut directory, page, u64::from(PAGE));
        }
        put(&mut directory, COUNT + 1, u64::MAX);
    }
    let frozen = state.snapshot().unwrap();
    {
        let mut directory = state.live.directory.write().unwrap();
        directory.apply(
            &id,
            StoreKind::Paged,
            &Operation::DeletePage { page: 255 },
            disk_id,
            Some(0),
        );
        put(&mut directory, 256, 2 * u64::from(PAGE));
        let live = directory.store_geometry(StoreKind::Paged, &id).unwrap();
        assert_eq!(live.allocation(255), None);
        assert_eq!(live.allocation(256), Some(8192));
    }
    let mut restored = State::default();
    for bytes in frozen {
        restored
            .restore(State::decode_snapshot(&bytes.unwrap()).unwrap())
            .unwrap();
    }
    let directory = restored.live.directory.read().unwrap();
    let row = directory.store_geometry(StoreKind::Paged, &id).unwrap();
    assert_eq!(row.page_count(), COUNT as usize + 1);
    assert_eq!(row.last_page(), COUNT + 1);
    assert_eq!(row.disk_id, disk_id);
    assert!(row.pages_after(None).eq((0..COUNT)
        .map(|page| (page, 4096))
        .chain([(COUNT + 1, u64::MAX)])));
    assert!(row
        .pages_after(Some(COUNT - 2))
        .eq([(COUNT - 1, 4096), (COUNT + 1, u64::MAX)]));
}

#[test]
fn range_pins_survive_chunk_promotion_and_exclude_page_deletion() {
    let mut id = block(0);
    id.block_size = 4096 * 512;
    let directory = Arc::new(std::sync::RwLock::new(crate::index::Directory::default()));
    directory.write().unwrap().attach(4096);
    let index = Arc::new(crate::BlockIndex::from_catalog(directory.clone()));
    let lifecycle = crate::page_lifecycle::PageLifecycle::with_index(index.clone(), true);
    let put = |page| {
        directory.write().unwrap().apply(
            &id,
            StoreKind::Paged,
            &Operation::Put {
                page,
                len: u64::from(id.block_size),
            },
            None,
            Some(4096),
        );
    };
    for page in 2..258 {
        put(page);
    }
    let block = lifecycle.block(&id);
    let old = index
        .page_reads_and_touch(&id, PageIndex(254), PageIndex(258))
        .unwrap();
    put(0); // Moves held rows from the flat arrays into chunks.
    for page in 254..258 {
        assert!(block.candidate(PageIndex(page)).is_none());
    }
    let overlap = index
        .page_reads_and_touch(&id, PageIndex(255), PageIndex(257))
        .unwrap();
    old.record_access_and_release(Some(10));
    assert_eq!(block.last_access(254), Some(10));
    assert_eq!(block.last_access(255), Some(10));
    assert_eq!(block.last_access(256), Some(10));
    assert_eq!(block.last_access(257), Some(10));
    assert!(block.candidate(PageIndex(254)).is_some());
    assert!(block.candidate(PageIndex(255)).is_none());
    assert!(block.candidate(PageIndex(256)).is_none());
    assert!(block.candidate(PageIndex(257)).is_some());
    overlap.record_access_and_release(Some(20));
    assert_eq!(block.last_access(255), Some(20));
    assert_eq!(block.last_access(256), Some(20));
    let delete = block.candidate(PageIndex(255)).unwrap();
    assert!(block.claim(&delete, 30, None));
    assert!(index
        .page_reads_and_touch(&id, PageIndex(254), PageIndex(257))
        .is_none());
    assert!(block.candidate(PageIndex(254)).is_some());
    directory.write().unwrap().apply(
        &id,
        StoreKind::Paged,
        &Operation::DeletePage { page: 255 },
        None,
        Some(0),
    );
    put(255);
    assert_eq!(block.last_access(255), None);
    assert_eq!(block.last_access(256), Some(20));
    assert!(block.candidate(PageIndex(256)).is_some());
}

#[tokio::test]
async fn wide_catalog_block_claim_allows_unrelated_reads_and_durable_publication() {
    use crate::page_lifecycle::PageLifecycle;
    use futures::FutureExt;
    use std::time::Duration;

    const PAGES: u32 = 65_536;
    let root = tempfile::tempdir().unwrap();
    let catalog = open(root.path());
    let paged = store(&catalog);
    let index = Arc::new(catalog.index(4));
    let life = PageLifecycle::with_index(index.clone(), false);
    let mut wide = block(0);
    wide.block_size = PAGES * 4;
    let slot = {
        let mut directory = catalog.live.directory.write().unwrap();
        // Seed wide geometry directly so the fixture requires only two durable writes.
        for page in 0..PAGES {
            directory.apply(
                &wide,
                StoreKind::Paged,
                &Operation::Put {
                    page,
                    len: u64::from(wide.block_size),
                },
                None,
                Some(4),
            );
        }
        directory.slot(&wide).unwrap()
    };
    let other = block(1);
    let body = Bytes::from_static(b"data");
    paged
        .commit_page(&other, PageIndex(0), 8, body.clone())
        .await
        .unwrap();

    let state = life.block(&wide);
    let reader = index
        .page_reads_and_touch(&wide, PageIndex(PAGES - 2), PageIndex(PAGES))
        .unwrap();
    let gate = state.lock().await;
    assert!(state.claim_all_pages().is_none());
    drop(reader);

    let claim = state.claim_all_pages().unwrap();
    assert_eq!(claim.len(), PAGES as usize);
    assert_eq!(index.page_work.operation_counts(slot), (1, 0));
    assert!(state.acquire(PageIndex(0)).is_none());
    assert!(index
        .page_reads_and_touch(&wide, PageIndex(0), PageIndex(PAGES))
        .is_none());
    assert!(state.candidate(PageIndex(PAGES - 1)).is_none());
    assert!(
        state.lock().now_or_never().is_none(),
        "single-page eviction uses this same exclusive gate"
    );
    assert!(state.lock_page_fill().now_or_never().is_none());

    tokio::time::timeout(Duration::from_secs(5), async {
        let read = index
            .page_reads_and_touch(&other, PageIndex(0), PageIndex(1))
            .unwrap();
        paged
            .commit_page(&other, PageIndex(1), 8, body.clone())
            .await
            .unwrap();
        assert_eq!(
            paged.get_page_bytes(&other, PageIndex(1), 4).await.unwrap(),
            body
        );
        drop(read);
    })
    .await
    .unwrap();

    // Abort touches only its operation stripe, even while directory access is blocked.
    let directory = catalog.live.directory.write().unwrap();
    std::thread::scope(|scope| {
        let (sent, received) = std::sync::mpsc::channel();
        scope.spawn(move || {
            drop(claim);
            sent.send(()).unwrap();
        });
        let aborted = received.recv_timeout(Duration::from_secs(5));
        drop(directory);
        aborted.unwrap();
    });
    assert_eq!(index.page_work.operation_counts(slot), (0, 0));
    assert!(state.acquire(PageIndex(PAGES - 1)).is_some());
    drop(gate);
    println!(
        "wide_pages={PAGES} active_block_claims=1 active_page_claims=0 unrelated_durable_puts=1"
    );
}

#[tokio::test]
async fn catalog_block_claim_abort_preserves_pending_page_operations_and_reuse() {
    use crate::page_lifecycle::PageLifecycle;

    let root = tempfile::tempdir().unwrap();
    let catalog = open(root.path());
    let paged = store(&catalog);
    let index = Arc::new(catalog.index(4));
    let life = PageLifecycle::with_index(index.clone(), false);
    let id = block(0);
    let page = PageIndex(0);
    let body = Bytes::from_static(b"data");
    paged.commit_page(&id, page, 4, body.clone()).await.unwrap();
    let state = life.block(&id);
    let slot = catalog.live.directory.read().unwrap().slot(&id).unwrap();
    let _gate = state.lock().await;

    let selected = state.candidate(page).unwrap();
    assert!(state.claim(&selected, 0, None));
    assert!(state.claim_all_pages().is_none());
    state.abort(&selected, 1);
    let old = state.claim_all_pages().unwrap();
    assert!(!state.claim(&selected, 0, None));

    // Retire and refill while obsolete handles survive, as a forced replacement.
    paged.delete_block_async(&id).await.unwrap();
    paged.commit_page(&id, page, 4, body).await.unwrap();
    assert_eq!(catalog.live.directory.read().unwrap().slot(&id), Some(slot));
    let pending = state.candidate(page).unwrap();
    let new = state.claim_all_pages().unwrap();
    drop(old);
    assert!(!state.claim(&selected, 0, None));
    assert!(!state.claim(&pending, 0, None));
    assert!(state.acquire(page).is_none());

    // The first unlink fails before removing any resident file.
    let path = paged.catalog_dir_for(&id).unwrap().join("0.page");
    std::fs::rename(&path, path.with_extension("saved")).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(paged.delete_block_async(&id).await.is_err());
    assert!(state.acquire(page).is_none());
    drop(new);
    assert_eq!(index.page_work.operation_counts(slot), (0, 1));
    drop(state.acquire(page).unwrap());
    assert!(state.claim(&pending, 0, None));
    state.abort(&pending, 1);
    assert!(state.acquire(page).is_some());
}

#[test]
fn frozen_snapshot_preserves_pages_and_pending_transaction_ids() {
    let mut state = State::default();
    state
        .apply(
            1,
            Record::PagedStore {
                path: "paged".into(),
                page_size: 4,
            },
        )
        .unwrap();
    let begin = || Record::Begin {
        intent: model::Intent {
            store: "paged".into(),
            block: block(0),
            operation: Operation::Put { page: 0, len: 16 },
            shard: Some(0),
            disk_id: None,
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
    let entries = restored.entries("paged").unwrap().unwrap();
    assert_eq!(entries[0].allocated, vec![(Some(PageIndex(0)), 4096)]);
    assert_eq!(entries[0].meta.disk_id, DiskId::new(2, 0).ok());
    assert_eq!(
        restored.live.pending.read().unwrap()[&4].disk_id,
        DiskId::new(2, 0).ok()
    );
    restored
        .apply(6, Record::Resolve { transaction: 4 })
        .unwrap();
    assert!(restored.live.pending.read().unwrap().is_empty());
    assert!(restored.entries("paged").unwrap().unwrap().is_empty());
    assert_eq!(
        state.entries("paged").unwrap().unwrap()[0].allocated,
        vec![(Some(PageIndex(0)), 8192)]
    );
    let record = begin();
    assert_eq!(
        State::encoded_len(&record).unwrap(),
        State::encode(&record).unwrap().len()
    );
}

#[tokio::test]
async fn sibling_begins_share_the_same_disk_id_before_either_completes() {
    let root = tempfile::tempdir().unwrap();
    let catalog = open(root.path());
    let paged = store(&catalog);
    let key = block(0);
    let first = catalog
        .begin("paged", &key, Operation::Put { page: 0, len: 16 })
        .await
        .unwrap();
    let sibling = catalog
        .begin("paged", &key, Operation::Put { page: 1, len: 16 })
        .await
        .unwrap();
    let other = catalog
        .begin("paged", &block(1), Operation::Put { page: 0, len: 16 })
        .await
        .unwrap();
    let disk_id = first.disk_id;
    assert_eq!(sibling.disk_id, disk_id);
    assert_ne!(other.disk_id, disk_id);
    let dir = crate::paged_store::disk_dir(paged.root(), disk_id);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("0.page"), b"lost").unwrap();
    std::fs::write(dir.join("1.page"), b"kept").unwrap();
    catalog.complete(sibling, 4096).await.unwrap();
    catalog.checkpoint().await.unwrap();
    drop((first, other, paged, catalog));

    let catalog = open(root.path());
    assert!(catalog.disk_id("paged", &block(1)).unwrap().is_none());
    assert_eq!(catalog.disk_id("paged", &key).unwrap(), Some(disk_id));
    assert!(!dir.join("0.page").exists());
    assert_eq!(
        store(&catalog)
            .get_page_bytes_at(&key, Some(disk_id), PageIndex(1), 4)
            .await
            .unwrap()
            .as_ref(),
        b"kept"
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
        id: BlockKey::new(&id),
        disk_id: DiskId::new(1, 0).ok(),
        len: u64::from(id.block_size),
        pages: (0..count).map(|page| (page, 4096)).collect(),
    };
    // This valid state exceeded the engine's single-frame limit before page
    // chunks. Construct metadata directly instead of creating millions of files.
    assert!(serde_json::to_vec(&block).unwrap().len() > 16 << 20);
    let path = "paged";
    let mut state = State::default();
    state
        .restore(model::SnapshotRecord::PagedStore {
            path: path.into(),
            page_size: 1024,
        })
        .unwrap();
    state
        .restore(model::SnapshotRecord::Block {
            store: path.into(),
            block,
            continuation: false,
        })
        .unwrap();
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
                    shard: Some(0),
                    disk_id: None,
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
            let directory = state.live.directory.read().unwrap();
            let restored = directory.store_geometry(StoreKind::Paged, &id).unwrap();
            assert_eq!(restored.page_count(), count as usize - 1);
            assert_eq!(restored.len(), u64::from(id.block_size));
            assert!(restored
                .pages_after(None)
                .eq((1..count).map(|page| (page, 4096))));
            Ok(())
        })
        .await
        .unwrap();
}

#[test]
fn snapshot_page_chunks_validate_identity_order_and_legacy_records() {
    use model::SnapshotRecord;
    let path = "paged";
    let chunk = |page, len, continuation| SnapshotRecord::Block {
        store: path.into(),
        block: model::Block {
            id: BlockKey::new(&block(0)),
            disk_id: DiskId::new(1, 0).ok(),
            len,
            pages: [(page, 4096)].into(),
        },
        continuation,
    };
    let mut state = State::default();
    state
        .restore(SnapshotRecord::PagedStore {
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
        state
            .live
            .directory
            .read()
            .unwrap()
            .store_geometry(StoreKind::Paged, &block(0))
            .unwrap()
            .pages_after(None)
            .collect::<Vec<_>>(),
        [(0, 4096), (2, 4096)]
    );
}

#[tokio::test]
async fn catalog_failure_does_not_poison_another_stream_on_the_root_service() {
    let root = tempfile::tempdir().unwrap();
    let service = service(Arc::new(CacheRootLock::acquire(root.path()).unwrap())).unwrap();
    let catalog = scoped(&service, "a/shared");
    let other = scoped(&service, "a/scopes/test");
    let paged = store(&catalog);
    let other_paged = store(&other);
    let dir = paged.root().to_owned();
    std::fs::remove_dir(&dir).unwrap();
    std::fs::write(&dir, b"obstruction").unwrap();
    assert!(paged
        .commit_page(&block(0), PageIndex(0), 4, Bytes::from_static(b"data"))
        .await
        .is_err());
    assert!(catalog.checkpoint().await.is_err());
    other_paged
        .commit_page(&block(0), PageIndex(0), 4, Bytes::from_static(b"safe"))
        .await
        .unwrap();
    other.checkpoint().await.unwrap();
    assert_eq!(other.recover("paged").unwrap().unwrap().len(), 1);
    drop(paged);
    drop(other_paged);
    drop(catalog);
    drop(other);
    std::fs::remove_file(dir).unwrap();
    let catalog = scoped(&service, "a/shared");
    let other = scoped(&service, "a/scopes/test");
    assert!(catalog.recover("paged").unwrap().unwrap().is_empty());
    assert_eq!(other.recover("paged").unwrap().unwrap().len(), 1);
    assert_eq!(
        store(&other)
            .get_page_bytes(&block(0), PageIndex(0), 4)
            .await
            .unwrap()
            .as_ref(),
        b"safe"
    );
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
        let path = "paged";
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
                    .query_blocking(|state| Ok(state.live.pending.read().unwrap().is_empty()))
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

fn scoped(service: &Arc<JournalService>, scope: &str) -> Arc<Catalog> {
    let root = service.root().join(scope);
    std::fs::create_dir_all(&root).unwrap();
    Catalog::with_service(service, &root, transaction_budget(), None).unwrap()
}

#[tokio::test]
async fn scoped_catalogs_share_transaction_admission() {
    let root = tempfile::tempdir().unwrap();
    let service = service(Arc::new(CacheRootLock::acquire(root.path()).unwrap())).unwrap();
    let admission = Arc::new(tokio::sync::Semaphore::new(1));
    let mut catalogs = Vec::new();
    for scope in ["a/shared", "a/scopes/test"] {
        let path = root.path().join(scope);
        std::fs::create_dir_all(&path).unwrap();
        let catalog = Catalog::with_service(&service, &path, admission.clone(), None).unwrap();
        catalog.register("paged", 4).unwrap();
        catalogs.push(catalog);
    }
    let block = block(0);
    let first = catalogs[0]
        .begin("paged", &block, Operation::Put { page: 0, len: 16 })
        .await
        .unwrap();
    let second = catalogs[1].begin("paged", &block, Operation::Put { page: 0, len: 16 });
    tokio::pin!(second);
    assert!(futures::poll!(&mut second).is_pending());
    catalogs[0].complete(first, 4096).await.unwrap();
    let second = tokio::time::timeout(std::time::Duration::from_secs(2), second)
        .await
        .unwrap()
        .unwrap();
    catalogs[1].complete(second, 4096).await.unwrap();
}

fn store(catalog: &Arc<Catalog>) -> Arc<PagedBlockStore> {
    if catalog.recover("paged").unwrap().is_none() {
        catalog.register("paged", 4).unwrap();
    }
    Arc::new(
        PagedBlockStore::open(catalog.root.join("paged"), 4)
            .unwrap()
            .with_catalog(catalog.clone(), "paged".into()),
    )
}

#[tokio::test]
async fn replay_restores_exact_residency_and_allocation_without_listing_data() {
    let root = tempfile::tempdir().unwrap();
    let catalog = open(root.path());
    let paged = store(&catalog);
    let index = Arc::new(catalog.index(4));
    let policy = index.restore_lru();
    let id = block(0);
    let (first, sibling) = tokio::join!(
        paged.commit_page_payload(&id, PageIndex(0), 16, Bytes::from_static(b"aaaa").into()),
        paged.commit_page_payload(&id, PageIndex(2), 16, Bytes::from_static(b"bbbb").into()),
    );
    let (first, _) = first.unwrap();
    let (sibling, _) = sibling.unwrap();
    assert_eq!(index.len(), 1);
    assert_eq!(index.page_count(), 2);
    assert_eq!(index.resident_bytes(), first + sibling);
    assert_eq!((policy.total_bytes(), policy.len()), (first + sibling, 2));
    assert_eq!(
        index.presence(&id, PageIndex(0), PageIndex(1)),
        crate::Presence::PageHit
    );
    assert_eq!(
        index.presence(&id, PageIndex(0), PageIndex(3)),
        crate::Presence::PageMiss
    );
    let (other, _) = paged
        .commit_page_payload(
            &block(1),
            PageIndex(1),
            12,
            Bytes::from_static(b"cccc").into(),
        )
        .await
        .unwrap();
    paged
        .evict_page_async(&block(0), PageIndex(0))
        .await
        .unwrap();
    assert_eq!(index.len(), 2);
    assert_eq!(index.page_count(), 2);
    assert_eq!(index.resident_bytes(), sibling + other);
    assert_eq!((policy.total_bytes(), policy.len()), (sibling + other, 2));
    paged
        .evict_page_async(&block(1), PageIndex(1))
        .await
        .unwrap();
    assert_eq!(index.len(), 1);
    assert_eq!((policy.total_bytes(), policy.len()), (sibling, 1));
    let dir = paged.catalog_dir_for(&block(0)).unwrap();
    assert!(dir.join("2.page").exists());
    assert!(!dir.join("block.meta").exists());
    let before = catalog.recover("paged").unwrap().unwrap();
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].allocated.len(), 1);
    let expected = before[0].allocated.clone();
    assert_eq!(expected[0].1, sibling);
    assert_eq!(index.resident_bytes(), expected[0].1);
    assert_eq!(
        index.snapshot_units(),
        [(id.clone(), expected[0].0, expected[0].1)]
    );
    assert_eq!(index.page_count(), 1);
    drop(policy);
    drop(index);
    drop(paged);
    drop(catalog);
    // If recovery enumerates or stats data, this replacement makes it fail.
    let data = root.path().join("paged");
    std::fs::rename(&data, root.path().join("hidden-data")).unwrap();
    std::fs::write(&data, b"not a directory").unwrap();
    let catalog = open(root.path());
    let index = catalog.index(4);
    assert_eq!(index.snapshot_units(), [(id, expected[0].0, expected[0].1)]);
    let recovered = catalog.recover("paged").unwrap().unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].allocated, expected);
    assert_eq!(recovered[0].meta.id.materialize(), block(0));
    assert_eq!(recovered[0].meta.len, 16);
}

#[tokio::test]
async fn shared_slots_keep_store_addresses_and_reused_generations_distinct() {
    use crate::WholeBlockStore;
    use talon_core::ObjectStore;

    let root = tempfile::tempdir().unwrap();
    let catalog = open(root.path());
    let whole = WholeBlockStore::open_with_catalog(
        root.path(),
        Arc::new(crate::fd_cache::FdCache::new(8)),
        catalog.clone(),
    )
    .unwrap();
    let paged = store(&catalog);
    let index = Arc::new(catalog.index(4));
    let policy = index.restore_lru();
    let mut id = block(0);
    id.block_size = 16 << 10;
    let whole_data = Bytes::from(vec![b'a'; 8192]);
    whole.put(&id, whole_data.clone()).await.unwrap();
    let whole_bytes = catalog.recover("whole").unwrap().unwrap()[0].allocated[0].1;
    assert_eq!(
        (index.len(), index.page_count(), index.resident_bytes()),
        (1, 0, whole_bytes)
    );
    assert_eq!((policy.total_bytes(), policy.len()), (whole_bytes, 1));
    let whole_disk = catalog.disk_id("whole", &id).unwrap().unwrap();
    let key = index.shared_identity(&id).unwrap();
    assert_eq!(index.whole_and_touch(&id), Some(Some(whole_disk)));

    let (page_bytes, _) = paged
        .commit_page_payload(
            &id,
            PageIndex(1),
            whole_data.len() as u64,
            Bytes::from_static(b"efgh").into(),
        )
        .await
        .unwrap();
    let page_disk = catalog.disk_id("paged", &id).unwrap().unwrap();
    assert_ne!(whole_disk, page_disk);
    assert!(key.ptr_eq(&index.shared_identity(&id).unwrap()));
    assert_eq!(index.len(), 1);
    assert_eq!(index.page_count(), 1);
    assert_eq!(index.resident_bytes(), page_bytes);
    assert_eq!((policy.total_bytes(), policy.len()), (page_bytes, 1));
    assert_eq!(whole.get_bytes(&id).await.unwrap(), whole_data);
    assert_eq!(
        paged.get_page_bytes(&id, PageIndex(1), 4).await.unwrap(),
        Bytes::from_static(b"efgh")
    );

    catalog.checkpoint().await.unwrap();
    drop((paged, whole, index, catalog, policy));

    let catalog = open(root.path());
    let index = Arc::new(catalog.index(4));
    let policy = index.restore_lru();
    assert_eq!(catalog.disk_id("whole", &id).unwrap(), Some(whole_disk));
    assert_eq!(catalog.disk_id("paged", &id).unwrap(), Some(page_disk));
    assert_eq!(index.page_count(), 1);
    let paged = store(&catalog);
    paged.delete_block_async(&id).await.unwrap();
    assert!(index.is_empty());
    assert_eq!(index.resident_bytes(), 0);
    assert_eq!(index.page_count(), 0);
    assert_eq!((policy.total_bytes(), policy.len()), (0, 0));
    drop((paged, index, catalog, policy));

    // The delete is in the tail log, so attachment must expose the Whole
    // shadow from the checkpoint and charge it once.
    let catalog = open(root.path());
    let index = Arc::new(catalog.index(4));
    let policy = index.restore_lru();
    assert_eq!(index.whole_and_touch(&id), Some(Some(whole_disk)));
    assert_eq!(
        (index.len(), index.page_count(), index.resident_bytes()),
        (1, 0, whole_bytes)
    );
    assert_eq!((policy.total_bytes(), policy.len()), (whole_bytes, 1));
    let recovered_handle = catalog
        .live
        .directory
        .read()
        .unwrap()
        .rows
        .handle(&id)
        .unwrap();
    let whole = WholeBlockStore::open_with_catalog(
        root.path(),
        Arc::new(crate::fd_cache::FdCache::new(8)),
        catalog.clone(),
    )
    .unwrap();
    assert_eq!(whole.get_bytes(&id).await.unwrap(), whole_data);
    whole.delete(&id).await.unwrap();
    assert!(index.shared_identity(&id).is_none());
    whole.put(&id, Bytes::from_static(b"ijkl")).await.unwrap();
    assert_eq!(
        catalog.live.directory.read().unwrap().rows.handle(&id),
        Some(recovered_handle)
    );
    let replacement = index.whole_and_touch(&id).flatten().unwrap();
    assert_ne!(replacement, whole_disk);
    assert_eq!(
        whole.get_bytes(&id).await.unwrap(),
        Bytes::from_static(b"ijkl")
    );

    let mut other_version = id.clone();
    other_version.version = Version::new("v2");
    whole
        .put(&other_version, Bytes::from_static(b"mnop"))
        .await
        .unwrap();
    assert_eq!(policy.len(), 2);
    assert_eq!(index.policy_version_blocks(&id).len(), 2);
    assert_eq!(index.get(&other_version).unwrap().id, other_version);
}

#[tokio::test]
async fn pending_put_is_cleaned_by_exact_path_including_replaced_page() {
    for checkpoint in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let catalog = open(root.path());
        let paged = store(&catalog);
        paged
            .commit_page(&block(0), PageIndex(0), 16, Bytes::from_static(b"old!"))
            .await
            .unwrap();
        let id = catalog
            .begin("paged", &block(0), Operation::Put { page: 0, len: 16 })
            .await
            .unwrap();
        let dir = paged.catalog_dir_for(&block(0)).unwrap();
        assert_eq!(dir, crate::paged_store::disk_dir(paged.root(), id.disk_id));
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
        assert!(catalog.recover("paged").unwrap().unwrap().is_empty());
        let paged = store(&catalog);
        paged
            .commit_page(&block(0), PageIndex(0), 16, Bytes::from_static(b"new!"))
            .await
            .unwrap();
        let refill = paged.catalog_dir_for(&block(0)).unwrap();
        assert_ne!(refill, dir);
        drop(paged);
        drop(catalog);
        let catalog = open(root.path());
        assert_eq!(catalog.recover("paged").unwrap().unwrap().len(), 1);
        assert_eq!(std::fs::read(refill.join("0.page")).unwrap(), b"new!");
    }
}

#[tokio::test]
async fn catalog_page_pins_survive_row_moves_and_ignore_reused_slots() {
    use crate::page_lifecycle::{disk_shard, PageLifecycle};

    let root = tempfile::tempdir().unwrap();
    let catalog = open(root.path());
    let paged = store(&catalog);
    let index = Arc::new(catalog.index(4));
    let lifecycle = PageLifecycle::with_index(index.clone(), true);
    let id = block(0);
    let body = Bytes::from_static(b"data");
    paged
        .commit_page(&id, PageIndex(2), 16, body.clone())
        .await
        .unwrap();
    let state = lifecycle.block(&id);
    let shard = disk_shard(&id);
    // An existing access base requires a delta tombstone.
    lifecycle.set_checkpoint_journal(
        shard,
        crate::page_access_shard::JournalState {
            generation: 1,
            ..Default::default()
        },
    );
    let [slot]: [usize; 1] = lifecycle.recovery_slots()[shard]
        .as_slice()
        .try_into()
        .unwrap();
    let (_, old) = index
        .page_reads_and_touch_at(&id, PageIndex(2), PageIndex(3))
        .unwrap();

    paged
        .commit_page(&id, PageIndex(0), 16, body.clone())
        .await
        .unwrap();
    assert!(state.candidate(PageIndex(2)).is_none());
    assert!(index
        .page_reads_and_touch_at(&id, PageIndex(0), PageIndex(3))
        .is_none());
    assert!(
        state.candidate(PageIndex(0)).is_some(),
        "partial-range read leaked a pin"
    );
    paged.evict_page_async(&id, PageIndex(0)).await.unwrap();
    assert!(state.candidate(PageIndex(2)).is_none());

    // Simulate retirement outside normal GC while an old guard still exists.
    paged.delete_block_async(&id).await.unwrap();
    assert_eq!(lifecycle.dirty_summary(), (0, None));
    let (_, membership, changed) = lifecycle.checkpoint_start(shard);
    assert!(changed);
    assert_eq!(
        lifecycle.checkpoint_journal(shard, membership).1,
        vec![id.clone()]
    );
    paged
        .commit_page(&id, PageIndex(2), 16, body)
        .await
        .unwrap();
    assert_eq!(lifecycle.recovery_slots()[shard], vec![slot]);

    let new = state.acquire(PageIndex(2)).unwrap();
    old.record_access_and_release(Some(100));
    assert_eq!(state.last_access(2), None);
    assert!(state.candidate(PageIndex(2)).is_none());
    new.record_access_and_release(Some(200));
    assert_eq!(state.last_access(2), Some(200));
    assert!(state.candidate(PageIndex(2)).is_some());
}

#[tokio::test]
async fn pending_deletes_survive_checkpoint_and_recreation() {
    for operation in [Operation::DeletePage { page: 0 }, Operation::DeleteBlock] {
        let root = tempfile::tempdir().unwrap();
        let catalog = open(root.path());
        let paged = store(&catalog);
        paged
            .commit_page(&block(0), PageIndex(0), 16, Bytes::from_static(b"data"))
            .await
            .unwrap();
        catalog
            .begin("paged", &block(0), operation.clone())
            .await
            .unwrap();
        catalog.checkpoint().await.unwrap();
        let path = paged.catalog_dir_for(&block(0)).unwrap().join("0.page");
        assert!(path.exists());
        drop(paged);
        drop(catalog);
        let catalog = open(root.path());
        assert!(!path.exists());
        assert!(catalog.recover("paged").unwrap().unwrap().is_empty());
    }
}

#[tokio::test]
async fn checkpoint_replays_only_subsequent_log_and_keeps_namespaces_separate() {
    let root = tempfile::tempdir().unwrap();
    let service = service(Arc::new(CacheRootLock::acquire(root.path()).unwrap())).unwrap();
    let catalog_a = scoped(&service, "a/shared");
    let catalog_b = scoped(&service, "b/shared");
    let a = store(&catalog_a);
    let b = store(&catalog_b);
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
    catalog_a.checkpoint().await.unwrap();
    catalog_b.checkpoint().await.unwrap();
    let before_b = std::fs::read(catalog_b.root.join(DIRECTORY).join("CURRENT")).unwrap();
    catalog_a.checkpoint().await.unwrap();
    assert_eq!(
        std::fs::read(catalog_b.root.join(DIRECTORY).join("CURRENT")).unwrap(),
        before_b,
        "another namespace's checkpoint must not rotate this stream"
    );
    a.delete_block_async(&block(0)).await.unwrap();
    b.commit_page(&block(32), PageIndex(0), 4, Bytes::from_static(b"more"))
        .await
        .unwrap();
    drop(a);
    drop(b);
    drop(catalog_a);
    drop(catalog_b);
    let catalog_a = scoped(&service, "a/shared");
    let catalog_b = scoped(&service, "b/shared");
    assert_eq!(catalog_a.recover("paged").unwrap().unwrap().len(), 15);
    assert_eq!(catalog_b.recover("paged").unwrap().unwrap().len(), 17);
}

#[tokio::test]
async fn failed_file_mutation_blocks_further_admissions_until_recovery() {
    let root = tempfile::tempdir().unwrap();
    let catalog = open(root.path());
    let paged = store(&catalog);
    let dir = paged.root().to_owned();
    std::fs::remove_dir(&dir).unwrap();
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
    assert!(catalog.recover("paged").unwrap().unwrap().is_empty());
}

#[test]
fn whole_geometry_rejects_page_operations_and_malformed_snapshots() {
    let mut state = State::default();
    for (path, page_size) in [
        ("paged", 4),
        ("whole", 4),
        ("paged", 0),
        ("../whole", 0),
        ("other/shared/whole", 0),
        ("other/shared/paged", 4),
    ] {
        assert!(state
            .apply(
                1,
                Record::WholeStore {
                    path: path.into(),
                    page_size
                }
            )
            .is_err());
    }
    let legacy = br#"{"Store":{"path":"whole","page_size":0}}"#;
    assert!(State::decode(legacy).is_err());
    assert!(State::decode_snapshot(legacy).is_err());
    state
        .apply(
            1,
            Record::WholeStore {
                path: "whole".into(),
                page_size: 0,
            },
        )
        .unwrap();
    for operation in [
        Operation::Put { page: 0, len: 4 },
        Operation::DeletePage { page: 0 },
        Operation::PutWhole { len: 65 },
    ] {
        assert!(state
            .apply(
                2,
                Record::Begin {
                    intent: model::Intent {
                        store: "whole".into(),
                        block: block(0),
                        operation,
                        shard: Some(0),
                        disk_id: None,
                    }
                }
            )
            .is_err());
    }
    for (pages, continuation) in [
        (vec![(1, 4096)], false),
        (vec![(0, 4096), (1, 4096)], false),
        (vec![(0, 4096)], true),
    ] {
        assert!(state
            .restore(model::SnapshotRecord::Block {
                store: "whole".into(),
                block: model::Block {
                    id: BlockKey::new(&block(0)),
                    disk_id: DiskId::new(1, 0).ok(),
                    len: 4,
                    pages: pages.into_iter().collect()
                },
                continuation,
            })
            .is_err());
    }
    state
        .restore(model::SnapshotRecord::Block {
            store: "whole".into(),
            block: model::Block {
                id: BlockKey::new(&block(0)),
                disk_id: DiskId::new(1, 0).ok(),
                len: 4,
                pages: [(0, 4096)].into(),
            },
            continuation: false,
        })
        .unwrap();
    assert_eq!(
        state.entries("whole").unwrap().unwrap()[0].allocated,
        vec![(None, 4096)]
    );
}
