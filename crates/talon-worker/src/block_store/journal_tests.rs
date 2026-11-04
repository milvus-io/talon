use super::*;
use crate::catalog::Operation;
use talon_core::{Backend, ObjectId, Version};

fn block(n: u64) -> BlockId {
    BlockId::new(
        ObjectId::new(Backend::S3, "bucket", format!("key/{n}")),
        0,
        64,
        Version::new("v1"),
    )
}

#[tokio::test]
async fn pending_whole_mutations_reconcile_snapshot_and_tail() {
    for checkpoint in [false, true] {
        for operation in [Operation::PutWhole { len: 4 }, Operation::DeleteBlock] {
            let root = tempfile::tempdir().unwrap();
            let store = WholeBlockStore::open(root.path()).unwrap();
            store
                .put(&block(0), Bytes::from_static(b"old!"))
                .await
                .unwrap();
            let transaction = store
                .catalog
                .begin(&store.catalog_path, &block(0), operation.clone())
                .await
                .unwrap();
            let path = store.path_for(&block(0));
            let staged = path.with_extension(format!("blk.catalog.{}", transaction.id));
            if matches!(operation, Operation::PutWhole { .. }) {
                std::fs::write(&staged, b"partial").unwrap();
            }
            if checkpoint {
                store.catalog.checkpoint().await.unwrap();
            }
            drop(store);
            let store = WholeBlockStore::open(root.path()).unwrap();
            assert!(store.scan().unwrap().is_empty());
            assert!(!path.exists());
            assert!(!staged.exists());
            store
                .put(&block(0), Bytes::from_static(b"new!"))
                .await
                .unwrap();
            drop(store);
            let store = WholeBlockStore::open(root.path()).unwrap();
            assert_eq!(store.scan().unwrap().len(), 1);
            assert_eq!(store.get_bytes(&block(0)).await.unwrap(), b"new!"[..]);
        }
    }
}

#[tokio::test]
async fn cancelled_request_finishes_owned_whole_transaction() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(WholeBlockStore::open(root.path()).unwrap());
    let id = block(0);
    let gate = store.gate(&id).lock().await;
    let task = tokio::spawn({
        let store = store.clone();
        let id = id.clone();
        async move { store.put(&id, Bytes::from_static(b"data")).await }
    });
    while store.mutations.active_count() == 0 {
        tokio::task::yield_now().await;
    }
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(gate);
    store.drain().await;
    assert_eq!(store.scan().unwrap().len(), 1);
    drop(store);
    let store = WholeBlockStore::open(root.path()).unwrap();
    assert_eq!(store.get_bytes(&id).await.unwrap(), b"data"[..]);
    assert_eq!(store.scan().unwrap().len(), 1);
}

#[tokio::test]
async fn failed_whole_write_stops_admission_until_reconciliation() {
    let root = tempfile::tempdir().unwrap();
    let store = WholeBlockStore::open(root.path()).unwrap();
    let path = store.path_for(&block(0));
    std::fs::write(path.parent().unwrap(), b"obstruction").unwrap();
    assert!(store
        .put(&block(0), Bytes::from_static(b"data"))
        .await
        .is_err());
    assert!(store
        .put(&block(1), Bytes::from_static(b"data"))
        .await
        .is_err());
    drop(store);
    std::fs::remove_file(path.parent().unwrap()).unwrap();
    let store = WholeBlockStore::open(root.path()).unwrap();
    assert!(store.scan().unwrap().is_empty());
    store
        .put(&block(0), Bytes::from_static(b"data"))
        .await
        .unwrap();
}

#[test]
fn legacy_whole_files_are_rejected_without_modification() {
    for name in ["ab", "whole"] {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join(name);
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("legacy.blk"), b"cached").unwrap();
        assert!(WholeBlockStore::open(root.path())
            .err()
            .unwrap()
            .to_string()
            .contains("whole cache has no journal"));
        assert_eq!(std::fs::read(dir.join("legacy.blk")).unwrap(), b"cached");
    }
}

#[tokio::test]
async fn whole_and_paged_share_snapshot_with_independent_residency() {
    let root = tempfile::tempdir().unwrap();
    let whole = WholeBlockStore::open(root.path()).unwrap();
    whole.catalog.register("paged", 4).unwrap();
    let paged = Arc::new(
        crate::PagedBlockStore::open(root.path().join("paged"), 4)
            .unwrap()
            .with_catalog(whole.catalog.clone(), "paged".into()),
    );
    whole
        .put(&block(0), Bytes::from_static(b"whole"))
        .await
        .unwrap();
    paged
        .commit_page(&block(0), PageIndex(1), 8, Bytes::from_static(b"page"))
        .await
        .unwrap();
    whole.catalog.checkpoint().await.unwrap();
    whole
        .put(&block(1), Bytes::from_static(b"tail"))
        .await
        .unwrap();
    whole.delete(&block(0)).await.unwrap();
    let allocated = whole.scan_allocated().unwrap()[0].allocated.clone();
    drop(paged);
    drop(whole);
    let whole = WholeBlockStore::open(root.path()).unwrap();
    let recovered = whole.scan_allocated().unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].meta.id, block(1));
    assert_eq!(recovered[0].allocated, allocated);
    let pages = whole.catalog.recover("paged").unwrap().unwrap();
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].meta.id, block(0));
    assert_eq!(pages[0].allocated[0].0, Some(PageIndex(1)));
}

#[test]
fn kill_at_whole_transaction_boundaries_recovers_only_completed_writes() {
    const CHILD: &str = "TALON_WHOLE_CRASH_ROOT";
    if let Some(root) = std::env::var_os(CHILD) {
        let stage = std::env::var("TALON_CHECKPOINT_CRASH_STAGE").unwrap();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let store = WholeBlockStore::open(PathBuf::from(root)).unwrap();
                if stage.starts_with("whole_delete") {
                    store.delete(&block(0)).await.unwrap();
                } else {
                    store
                        .put(&block(0), Bytes::from_static(b"new!"))
                        .await
                        .unwrap();
                }
            });
        panic!("child did not reach crash boundary");
    }
    for stage in [
        "whole_begin",
        "whole_staged",
        "whole_published",
        "whole_complete",
        "whole_delete_begin",
        "whole_deleted",
    ] {
        let root = tempfile::tempdir().unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let store = WholeBlockStore::open(root.path()).unwrap();
            store
                .put(&block(0), Bytes::from_static(b"old!"))
                .await
                .unwrap();
            store.catalog.checkpoint().await.unwrap();
        });
        let ready = root.path().join("ready");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "block_store::journal_tests::kill_at_whole_transaction_boundaries_recovers_only_completed_writes"])
            .env(CHILD, root.path())
            .env("TALON_CHECKPOINT_CRASH_STAGE", stage)
            .env("TALON_CHECKPOINT_CRASH_READY", &ready)
            .spawn().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !ready.exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("child exited before {stage}: {status}");
            }
            if std::time::Instant::now() > deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("timed out at {stage}");
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        runtime.block_on(async {
            let store = WholeBlockStore::open(root.path()).unwrap();
            let complete = stage == "whole_complete";
            assert_eq!(
                store.scan().unwrap().len(),
                usize::from(complete),
                "{stage}"
            );
            assert_eq!(
                store.contains(&block(0)).await.unwrap(),
                complete,
                "{stage}"
            );
            if complete {
                assert_eq!(store.get_bytes(&block(0)).await.unwrap(), b"new!"[..]);
            }
            let data = store.path_for(&block(0));
            if data.parent().unwrap().exists() {
                assert!(std::fs::read_dir(data.parent().unwrap())
                    .unwrap()
                    .all(|entry| !entry
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .contains("catalog")));
            }
            store
                .put(&block(0), Bytes::from_static(b"retry"))
                .await
                .unwrap();
        });
    }
}

#[test]
#[ignore = "requires native io_uring"]
fn native_whole_write_recovery_and_delete() {
    let root = tempfile::tempdir().unwrap();
    let tokio = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let _entered = tokio.enter();
    monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .enable_timer()
        .build()
        .unwrap()
        .block_on(async {
            let ring = talon_uring::LocalRing::enter();
            let store = WholeBlockStore::open(root.path())
                .unwrap()
                .with_uring(true)
                .unwrap();
            store
                .put(&block(0), Bytes::from_static(b"native"))
                .await
                .unwrap();
            let allocated = store.scan_allocated().unwrap()[0].allocated.clone();
            store.catalog.checkpoint().await.unwrap();
            store.drain().await;
            drop(store);
            let store = WholeBlockStore::open(root.path())
                .unwrap()
                .with_uring(true)
                .unwrap();
            assert_eq!(store.scan_allocated().unwrap()[0].allocated, allocated);
            assert_eq!(store.get_bytes(&block(0)).await.unwrap(), b"native"[..]);
            store.delete(&block(0)).await.unwrap();
            store.drain().await;
            drop(store);
            let store = WholeBlockStore::open(root.path()).unwrap();
            assert!(store.scan().unwrap().is_empty());
            ring.drain().await;
        });
}
