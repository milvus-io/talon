use super::*;
use crate::small_store::tests::AppendPause;
use std::path::Path;
use talon_core::{Backend, ObjectId, Version};

fn id(name: &str) -> BlockId {
    BlockId::new(
        ObjectId::new(Backend::S3, "bucket", name),
        0,
        1 << 20,
        Version::new("v1"),
    )
}

fn open(root: &Path) -> Arc<SegmentStore> {
    Arc::new(SegmentStore::open(root.to_owned(), 128 << 10, 256 << 10, 4 << 20, None).unwrap())
}

async fn put(store: &Arc<SegmentStore>, name: &str, bytes: &'static [u8]) {
    store
        .put(&id(name), Bytes::from_static(bytes))
        .await
        .unwrap();
}

async fn flush(store: &Arc<SegmentStore>) {
    let owned = store.clone();
    store
        .tasks
        .run(async move { owned.flush().await })
        .await
        .unwrap();
}

#[tokio::test]
async fn idle_maintenance_flushes_without_sealing_and_recovery_discards_only_tail() {
    for snapshot in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());
        put(&store, "saved", b"saved").await;
        let segment = store.state.lock().await.active.unwrap();
        store.state.lock().await.last_flush -= FLUSH_INTERVAL;
        store.maintain(false).await.unwrap();
        {
            let state = store.state.lock().await;
            assert_eq!(state.active, Some(segment));
            assert!(!state.segments[&segment].sealed);
            assert_eq!(state.segments[&segment].durable_end, 5);
        }
        put(&store, "tail", b"uncommitted").await;
        assert_eq!(store.state.lock().await.active, Some(segment));
        if snapshot {
            store.journal.checkpoint().await.unwrap();
        }
        store.tasks.drain().await;
        drop(store);
        for _ in 0..2 {
            let store = open(dir.path());
            assert_eq!(
                store.read(&id("saved")).await.unwrap().unwrap(),
                b"saved"[..]
            );
            assert!(!store.contains(&id("tail")));
            assert_eq!(
                std::fs::metadata(store.path(segment, "data"))
                    .unwrap()
                    .len(),
                5
            );
            let state = store.state.lock().await;
            assert_eq!(
                state.allocated,
                state.segments.values().map(|s| s.allocated).sum::<u64>()
            );
            assert_eq!(
                state.segments[&segment].allocated,
                std::fs::metadata(store.path(segment, "data"))
                    .unwrap()
                    .blocks()
                    * 512
            );
        }
        let store = open(dir.path());
        put(&store, "tail", b"refilled").await;
        flush(&store).await;
        store.tasks.drain().await;
        drop(store);
        let store = open(dir.path());
        assert_eq!(
            store.read(&id("tail")).await.unwrap().unwrap(),
            b"refilled"[..]
        );
    }
}

#[tokio::test]
async fn byte_threshold_flushes_before_interval_without_rotating() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SegmentStore::open(dir.path().to_owned(), 128 << 10, 16 << 20, 64 << 20, None).unwrap(),
    );
    for n in 0..84 {
        store
            .put(&id(&n.to_string()), Bytes::from(vec![n as u8; 100 << 10]))
            .await
            .unwrap();
    }
    store.state.lock().await.last_flush = Instant::now();
    let segment = store.state.lock().await.active.unwrap();
    store.maintain(false).await.unwrap();
    let state = store.state.lock().await;
    assert_eq!(state.active, Some(segment));
    let s = &state.segments[&segment];
    assert_eq!(s.durable_end, 84 * (100 << 10));
    assert!(!s.sealed);
}

#[tokio::test]
async fn flush_waits_for_earlier_appends_but_not_later_reservations() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    let first_pause = Arc::new(AppendPause::default());
    *store.append_pause.lock().unwrap() = Some(first_pause.clone());
    let s = store.clone();
    let first = tokio::spawn(async move { put(&s, "first", b"first").await });
    first_pause.entered.notified().await;
    put(&store, "second", b"second").await;
    let cut = Arc::new(AppendPause::default());
    *store.flush_pause.lock().unwrap() = Some(cut.clone());
    let s = store.clone();
    let mut flushing = tokio::spawn(async move { flush(&s).await });
    cut.entered.notified().await;
    let later_pause = Arc::new(AppendPause::default());
    *store.append_pause.lock().unwrap() = Some(later_pause.clone());
    let s = store.clone();
    let later = tokio::spawn(async move { put(&s, "later", b"later").await });
    later_pause.entered.notified().await;
    cut.resume.notify_one();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut flushing)
            .await
            .is_err()
    );
    first_pause.resume.notify_one();
    first.await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), flushing)
        .await
        .unwrap()
        .unwrap();
    {
        let state = store.state.lock().await;
        let s = &state.segments[&state.active.unwrap()];
        assert_eq!(s.durable_end, 11);
        assert_eq!(s.in_flight.len(), 1);
    }
    later_pause.resume.notify_one();
    later.await.unwrap();
    store.tasks.drain().await;
    drop(store);
    let store = open(dir.path());
    assert!(store.contains(&id("first")) && store.contains(&id("second")));
    assert!(!store.contains(&id("later")));
}

#[tokio::test]
async fn deleted_flushed_objects_do_not_reappear_after_snapshot_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    put(&store, "deleted", b"first").await;
    put(&store, "kept", b"second").await;
    flush(&store).await;
    store.delete(&id("deleted")).await.unwrap();
    put(&store, "tail", b"third").await;
    store.journal.checkpoint().await.unwrap();
    store.tasks.drain().await;
    drop(store);
    let store = open(dir.path());
    assert!(!store.contains(&id("deleted")) && !store.contains(&id("tail")));
    assert_eq!(
        store.read(&id("kept")).await.unwrap().unwrap(),
        b"second"[..]
    );
}

#[tokio::test]
async fn failed_flush_keeps_previous_prefix_and_stops_mutations() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    put(&store, "saved", b"saved").await;
    flush(&store).await;
    put(&store, "tail", b"tail").await;
    store
        .journal
        .control
        .fail
        .store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(store.flush().await.is_err());
    assert!(store
        .put(&id("rejected"), Bytes::from_static(b"no"))
        .await
        .is_err());
    store.tasks.drain().await;
    drop(store);
    let store = open(dir.path());
    assert!(store.contains(&id("saved")));
    assert!(!store.contains(&id("tail")));
}

#[tokio::test]
async fn cancelled_flush_finishes_while_rollover_waits_for_publication() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    store
        .put(&id("first"), Bytes::from(vec![1; 100 << 10]))
        .await
        .unwrap();
    store.tasks.drain().await;
    let old = store.state.lock().await.active.unwrap();
    let cut = Arc::new(AppendPause::default());
    *store.flush_pause.lock().unwrap() = Some(cut.clone());
    let s = store.clone();
    let flushing = tokio::spawn(async move { flush(&s).await });
    cut.entered.notified().await;
    flushing.abort();
    assert!(flushing.await.unwrap_err().is_cancelled());
    for name in ["second", "third"] {
        tokio::time::timeout(
            Duration::from_secs(2),
            store.put(&id(name), Bytes::from(vec![2; 100 << 10])),
        )
        .await
        .unwrap()
        .unwrap();
    }
    assert_eq!(store.state.lock().await.sealing, Some(old));
    cut.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(2), store.tasks.drain())
        .await
        .unwrap();
    assert!(store.state.lock().await.segments[&old].sealed);
    drop(store);
    let store = open(dir.path());
    assert!(store.contains(&id("first")) && store.contains(&id("second")));
    assert!(!store.contains(&id("third")));
}

#[tokio::test]
async fn later_flush_commits_pending_entries_retained_in_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    put(&store, "saved", b"saved").await;
    store.journal.checkpoint().await.unwrap();
    flush(&store).await;
    put(&store, "later", b"later").await;
    store.journal.checkpoint().await.unwrap();
    flush(&store).await;
    store.tasks.drain().await;
    drop(store);
    let store = open(dir.path());
    assert_eq!(
        store.read(&id("saved")).await.unwrap().unwrap(),
        b"saved"[..]
    );
    assert_eq!(
        store.read(&id("later")).await.unwrap().unwrap(),
        b"later"[..]
    );
}

#[tokio::test]
async fn truncated_durable_prefix_is_rejected_without_repair() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    put(&store, "saved", b"saved").await;
    flush(&store).await;
    let segment = store.state.lock().await.active.unwrap();
    put(&store, "tail", b"tail").await;
    let path = store.path(segment, "data");
    store.tasks.drain().await;
    drop(store);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(4)
        .unwrap();
    assert!(
        SegmentStore::open(dir.path().to_owned(), 128 << 10, 256 << 10, 4 << 20, None).is_err()
    );
    assert_eq!(std::fs::metadata(path).unwrap().len(), 4);
}

#[tokio::test]
async fn crash_child() {
    let Some(root) = std::env::var_os("TALON_SMALL_FLUSH_CRASH_ROOT") else {
        return;
    };
    let store = open(Path::new(&root));
    put(&store, "saved", b"saved").await;
    flush(&store).await;
    store.delete(&id("saved")).await.unwrap();
}

#[test]
fn process_loss_at_flush_delete_and_recovery_boundaries() {
    for stage in [
        "flush_data",
        "flush_directory",
        "flush_commit",
        "delete_sync",
        "recover_truncate",
        "recover_commit",
    ] {
        let dir = tempfile::tempdir().unwrap();
        if stage.starts_with("recover_") {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async {
                let store = open(dir.path());
                put(&store, "saved", b"saved").await;
                flush(&store).await;
                put(&store, "tail", b"tail").await;
                store.journal.checkpoint().await.unwrap();
                store.tasks.drain().await;
            });
        }
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "small_store::flush::tests::crash_child"])
            .env("TALON_SMALL_FLUSH_CRASH_ROOT", dir.path())
            .env("TALON_SMALL_TEST_CRASH", stage)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("child timed out at {stage}");
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        assert_eq!(status.code(), Some(71), "{stage}");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            for _ in 0..2 {
                let store = open(dir.path());
                assert_eq!(
                    store.contains(&id("saved")),
                    stage == "flush_commit" || stage.starts_with("recover_"),
                    "{stage}"
                );
                assert!(!store.contains(&id("tail")), "{stage}");
                if store.contains(&id("saved")) {
                    assert_eq!(
                        store.read(&id("saved")).await.unwrap().unwrap(),
                        b"saved"[..]
                    );
                }
            }
        });
    }
}
