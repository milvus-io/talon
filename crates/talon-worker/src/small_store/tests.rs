use super::*;
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;
use talon_core::{Backend, ObjectId, Version};

pub(super) fn crash(point: &str) {
    if std::env::var("TALON_SMALL_TEST_CRASH").as_deref() == Ok(point) {
        std::process::exit(71);
    }
}

#[tokio::test]
async fn crash_child() {
    let Ok(path) = std::env::var("TALON_SMALL_TEST_ROOT") else {
        return;
    };
    let store = open(Path::new(&path));
    for n in 0..10 {
        put(&store, &id(&n.to_string()), &vec![n as u8; 4096])
            .await
            .unwrap();
    }
    store.maintain(true).await.unwrap();
    for n in 0..9 {
        store.delete(&id(&n.to_string())).await.unwrap();
    }
    store.maintain(false).await.unwrap();
}

#[tokio::test]
async fn process_loss_at_each_publication_boundary() {
    for point in [
        "append_payload",
        "append_index",
        "seal_data",
        "seal_index",
        "seal_manifest",
        "delete_sync",
        "compact_copy",
        "compact_manifest",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "small_store::tests::crash_child"])
            .env("TALON_SMALL_TEST_ROOT", dir.path())
            .env("TALON_SMALL_TEST_CRASH", point)
            .stdout(std::process::Stdio::null())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(71), "{point}");
        let store = open(dir.path());
        if matches!(
            point,
            "append_payload" | "append_index" | "seal_data" | "seal_index"
        ) {
            assert!(!store.contains(&id("0")), "{point}");
        } else {
            assert_eq!(
                store.read(&id("9")).await.unwrap().unwrap().as_ref(),
                vec![9; 4096]
            );
            if point != "seal_manifest" {
                assert!(!store.contains(&id("0")), "{point}");
            }
            if point.starts_with("compact") {
                for n in 0..9 {
                    assert!(!store.contains(&id(&n.to_string())), "{point}");
                }
            }
        }
    }
}

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
fn read_handle(handle: &BlockHandle) -> Vec<u8> {
    let file = File::from(handle.fd.try_clone().unwrap());
    let mut bytes = vec![0; handle.len as usize];
    file.read_exact_at(&mut bytes, handle.offset).unwrap();
    bytes
}

#[tokio::test]
async fn packed_ranges_recovery_and_threshold_changes() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    for n in 0..100 {
        put(&store, &id(&n.to_string()), &vec![n as u8; 3072])
            .await
            .unwrap();
    }
    store.maintain(true).await.unwrap();
    let stats = store.stats().unwrap();
    assert!(stats.segments <= 3); // two sealed segments plus a prepared spare
    assert!(stats.allocated_bytes < 400_000);
    drop(store);
    let recovered = SegmentStore::open(dir.path().to_owned(), 0, 256 << 10, 4 << 20, None).unwrap();
    assert!(!recovered.accepts(&id("1"), 3072));
    for n in 0..100 {
        assert_eq!(
            read_handle(&recovered.get(&id(&n.to_string())).unwrap().unwrap()),
            vec![n as u8; 3072]
        );
    }
}

#[tokio::test]
async fn unpublished_active_segment_is_discarded_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    put(&store, &id("sealed"), b"saved").await.unwrap();
    store.maintain(true).await.unwrap();
    put(&store, &id("active"), b"discard").await.unwrap();
    store.tasks.drain().await;
    drop(store);
    let store = open(dir.path());
    assert!(store.contains(&id("sealed")));
    assert!(!store.contains(&id("active")));
    assert_eq!(store.stats().unwrap().segments, 1);
}

#[tokio::test]
async fn deletion_is_recovered_and_compaction_preserves_open_handles() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    for n in 0..10 {
        put(&store, &id(&n.to_string()), &vec![n as u8; 4096])
            .await
            .unwrap();
    }
    store.maintain(true).await.unwrap();
    let old = store.get(&id("9")).unwrap().unwrap();
    for n in 0..9 {
        store.delete(&id(&n.to_string())).await.unwrap();
    }
    assert!(store.maintain(false).await.unwrap());
    assert_eq!(read_handle(&old), vec![9; 4096]);
    assert!(store.stats().unwrap().retired_bytes > 0);
    let new = store.get(&id("9")).unwrap().unwrap();
    assert_eq!(read_handle(&new), vec![9; 4096]);
    drop(old);
    assert_eq!(store.stats().unwrap().retired_bytes, 0);
    drop(new);
    drop(store);
    let store = open(dir.path());
    for n in 0..9 {
        assert!(!store.contains(&id(&n.to_string())));
    }
    assert!(store.contains(&id("9")));
}

#[tokio::test]
async fn tombstones_prevent_resurrection_without_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    put(&store, &id("a"), b"first").await.unwrap();
    put(&store, &id("b"), b"second").await.unwrap();
    store.maintain(true).await.unwrap();
    store.delete(&id("a")).await.unwrap();
    drop(store);
    let store = open(dir.path());
    assert!(!store.contains(&id("a")));
    assert!(store.contains(&id("b")));
}

#[tokio::test]
async fn torn_delete_fails_closed_instead_of_replaying_old_entry() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    put(&store, &id("a"), b"first").await.unwrap();
    store.maintain(true).await.unwrap();
    drop(store);
    OpenOptions::new()
        .append(true)
        .open(dir.path().join("1.index"))
        .unwrap()
        .write_all(&[9, 0, 0])
        .unwrap();
    assert!(
        SegmentStore::open(dir.path().to_owned(), 128 << 10, 256 << 10, 4 << 20, None).is_err()
    );
}

#[tokio::test]
async fn manifest_excludes_orphan_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    put(&store, &id("a"), b"first").await.unwrap();
    store.maintain(true).await.unwrap();
    drop(store);
    std::fs::copy(dir.path().join("1.data"), dir.path().join("2.data")).unwrap();
    std::fs::copy(dir.path().join("1.index"), dir.path().join("2.index")).unwrap();
    let store = open(dir.path());
    assert_eq!(store.stats().unwrap().segments, 1);
    assert_eq!(
        store.read(&id("a")).await.unwrap().unwrap().as_ref(),
        b"first"
    );
    assert!(!dir.path().join("2.data").exists());
}

#[tokio::test]
async fn same_identity_is_immutable_and_large_object_tails_are_excluded() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    put(&store, &id("a"), b"first").await.unwrap();
    assert!(put(&store, &id("a"), b"other").await.is_err());
    let mut tail = id("tail");
    tail.offset = 1 << 20;
    assert!(!store.accepts(&tail, 100));
    assert!(!store.accepts(&id("limit"), 128 << 10));
}

#[tokio::test]
async fn concurrent_read_delete_and_compaction_do_not_mutate_old_extents() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    for n in 0..100 {
        put(&store, &id(&n.to_string()), &vec![n as u8; 1000])
            .await
            .unwrap();
    }
    store.maintain(true).await.unwrap();
    let reader = store.clone();
    let thread = std::thread::spawn(move || {
        for _ in 0..100 {
            for n in 0..100 {
                if let Some(handle) = reader.get(&id(&n.to_string())).unwrap() {
                    assert_eq!(read_handle(&handle), vec![n as u8; 1000]);
                }
            }
        }
    });
    for n in 0..90 {
        store.delete(&id(&n.to_string())).await.unwrap();
    }
    store.maintain(false).await.unwrap();
    thread.join().unwrap();
    for n in 0..90 {
        assert!(!store.contains(&id(&n.to_string())));
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn slow_sendfile_survives_retirement() {
    use std::os::{fd::AsRawFd, unix::net::UnixStream};
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    let payload = vec![0x5a; 120 << 10];
    put(&store, &id("wire"), &payload).await.unwrap();
    store.maintain(true).await.unwrap();
    let handle = store.get(&id("wire")).unwrap().unwrap();
    let (tx, mut rx) = UnixStream::pair().unwrap();
    let size: libc::c_int = 4096;
    // SAFETY: a live socket and a pointer to an initialized integer.
    assert_eq!(
        unsafe {
            libc::setsockopt(
                tx.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                &size as *const _ as *const _,
                std::mem::size_of_val(&size) as _,
            )
        },
        0
    );
    rx.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    tx.set_write_timeout(Some(Duration::from_secs(10))).unwrap();
    let sender = std::thread::spawn(move || {
        crate::sendfile::send_file_range(&tx, &*handle.fd, handle.offset, handle.len, 4096).unwrap()
    });
    let mut received = vec![0; payload.len()];
    rx.read_exact(&mut received[..4096]).unwrap();
    store.delete(&id("wire")).await.unwrap();
    store.maintain(false).await.unwrap();
    assert!(!store.contains(&id("wire")));
    for chunk in received[4096..].chunks_mut(4096) {
        rx.read_exact(chunk).unwrap();
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(sender.join().unwrap(), payload.len() as u64);
    assert_eq!(received, payload);
}

/// Local filesystem comparison, deliberately excluded from routine CI. This
/// measures layout and warm-filesystem recovery, not cold-device throughput.
#[tokio::test]
#[ignore]
async fn small_layout_probe() {
    use talon_core::ObjectStore;
    fn usage(root: &Path) -> (u64, u64) {
        let mut todo = vec![root.to_owned()];
        let mut files = 0;
        let mut allocated = 0;
        while let Some(path) = todo.pop() {
            let metadata = std::fs::metadata(&path).unwrap();
            allocated += metadata.blocks() * 512;
            if metadata.is_dir() {
                todo.extend(std::fs::read_dir(path).unwrap().map(|e| e.unwrap().path()));
            } else {
                files += 1;
            }
        }
        (files, allocated)
    }
    let dir = tempfile::tempdir().unwrap();
    let payload = Bytes::from(vec![7; 3072]);
    let whole = crate::WholeBlockStore::open(dir.path().join("whole")).unwrap();
    let start = Instant::now();
    for n in 0..10_000 {
        whole
            .put(&id(&format!("object/{n:08}")), payload.clone())
            .await
            .unwrap();
    }
    let whole_write = start.elapsed();
    let start = Instant::now();
    let index = crate::BlockIndex::new();
    whole.restore_index(&index).unwrap();
    let whole_restore = start.elapsed();
    assert_eq!(index.len(), 10_000);
    drop(index);
    let store = Arc::new(
        SegmentStore::open(
            dir.path().join("packed"),
            128 << 10,
            64 << 20,
            1 << 30,
            None,
        )
        .unwrap(),
    );
    let start = Instant::now();
    for n in 0..10_000 {
        put(&store, &id(&format!("object/{n:08}")), &payload)
            .await
            .unwrap();
    }
    store.maintain(true).await.unwrap();
    let packed_write = start.elapsed();
    drop(store);
    let start = Instant::now();
    let store = Arc::new(
        SegmentStore::open(
            dir.path().join("packed"),
            128 << 10,
            64 << 20,
            1 << 30,
            None,
        )
        .unwrap(),
    );
    let index = crate::BlockIndex::new();
    store.restore_index(&index).unwrap();
    let packed_restore = start.elapsed();
    assert_eq!(index.len(), 10_000);
    let whole_usage = usage(&dir.path().join("whole"));
    let packed_usage = usage(&dir.path().join("packed"));
    println!(
        "{}",
        serde_json::json!({"objects":10000,"payload_bytes":3072,
        "whole":{"write_ms":whole_write.as_millis(),"restore_ms":whole_restore.as_millis(),"files":whole_usage.0,"allocated_bytes":whole_usage.1},
        "packed":{"write_ms":packed_write.as_millis(),"restore_ms":packed_restore.as_millis(),"files":packed_usage.0,"allocated_bytes":packed_usage.1}})
    );
    assert!(packed_usage.1 < whole_usage.1);
    assert!(packed_usage.0 < whole_usage.0 / 100);
}

async fn put(store: &Arc<SegmentStore>, id: &BlockId, bytes: &[u8]) -> io::Result<u64> {
    store.put(id, Bytes::copy_from_slice(bytes)).await
}

#[tokio::test]
async fn accepted_small_mutation_survives_caller_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    let gate = store.state.lock().await;
    let writer = store.clone();
    let task = tokio::spawn(async move {
        writer
            .put(&id("cancelled"), Bytes::from_static(b"complete"))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while store.tasks.active_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    let _ = task.await;
    drop(gate);
    store.tasks.drain().await;
    assert_eq!(
        store
            .read(&id("cancelled"))
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"complete"
    );
    store.maintain(true).await.unwrap();
    drop(store);
    let store = open(dir.path());
    assert_eq!(
        store
            .read(&id("cancelled"))
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"complete"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn native_small_io_uses_current_ring_without_blocking_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    let mut runtime = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let ring = talon_uring::LocalRing::enter();
        disk_io::NATIVE_CALLS.with(|c| c.set(0));
        disk_io::BLOCKING_CALLS.with(|c| c.set(0));
        for n in 0..10 {
            put(&store, &id(&n.to_string()), &vec![n as u8; 3072])
                .await
                .unwrap();
        }
        store.maintain(true).await.unwrap();
        let old = store.get(&id("9")).unwrap().unwrap();
        for n in 0..9 {
            store.delete(&id(&n.to_string())).await.unwrap();
        }
        store.maintain(false).await.unwrap();
        assert_eq!(
            store.read(&id("9")).await.unwrap().unwrap().as_ref(),
            vec![9; 3072]
        );
        // Verify the old inode using the same buffered native read adapter.
        assert_eq!(
            disk_io::read(old.fd, old.offset, old.len as usize)
                .await
                .unwrap()
                .as_ref(),
            vec![9; 3072]
        );
        ring.drain().await;
        assert!(disk_io::NATIVE_CALLS.with(|c| c.get()) > 50);
        assert_eq!(disk_io::BLOCKING_CALLS.with(|c| c.get()), 0);
    });
}

#[tokio::test]
async fn capacity_is_a_runtime_eviction_target_not_an_engine_admission_limit() {
    let dir = tempfile::tempdir().unwrap();
    let capacity = 1 << 20;
    let store = Arc::new(
        SegmentStore::open(dir.path().to_owned(), 128 << 10, 256 << 10, capacity, None).unwrap(),
    );
    // No runtime eviction here: prove that passing the soft target itself
    // cannot reject an otherwise valid write or poison later admissions.
    for n in 0..320 {
        put(&store, &id(&n.to_string()), &vec![n as u8; 4096])
            .await
            .unwrap();
    }
    assert!(store.stats().unwrap().allocated_bytes > capacity);
    for n in [0, 255, 319] {
        assert_eq!(
            store
                .read(&id(&n.to_string()))
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            vec![n as u8; 4096]
        );
    }
}

#[derive(Default)]
pub(super) struct SealPause {
    pub(super) entered: tokio::sync::Notify,
    pub(super) resume: tokio::sync::Notify,
}

#[tokio::test]
async fn rollover_writes_spare_while_seal_waits_and_bounds_backlog() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    // Two 100 KiB records fit; a third rotates the 256 KiB segment.
    let bytes = vec![7; 100 << 10];
    put(&store, &id("a"), &bytes).await.unwrap();
    put(&store, &id("b"), &bytes).await.unwrap();
    store.tasks.drain().await;
    let spare = store.state.lock().await.standby.unwrap();
    let pause = Arc::new(SealPause::default());
    *store.seal_pause.lock().unwrap() = Some(pause.clone());
    put(&store, &id("c"), &bytes).await.unwrap();
    pause.entered.notified().await;
    tokio::time::timeout(Duration::from_secs(2), put(&store, &id("d"), &bytes))
        .await
        .unwrap()
        .unwrap();
    {
        let state = store.state.lock().await;
        assert_eq!(state.active, Some(spare));
        assert!(state.sealing.is_some());
        assert!(state.standby.is_some());
        assert_eq!(state.segments.len(), 3);
    }
    // A second rotation must wait: at most one sealing segment is allowed.
    let writer = store.clone();
    let mut next = tokio::spawn(async move { put(&writer, &id("e"), &vec![7; 100 << 10]).await });
    assert!(tokio::time::timeout(Duration::from_millis(50), &mut next)
        .await
        .is_err());
    // Delete cannot race manifest publication and lose its tombstone.
    let deleting = store.clone();
    let mut delete = tokio::spawn(async move { deleting.delete(&id("a")).await });
    assert!(tokio::time::timeout(Duration::from_millis(50), &mut delete)
        .await
        .is_err());
    *store.seal_pause.lock().unwrap() = None;
    pause.resume.notify_one();
    next.await.unwrap().unwrap();
    delete.await.unwrap().unwrap();
    store.maintain(true).await.unwrap();
    store.tasks.drain().await;
    drop(store);
    let recovered = open(dir.path());
    assert!(!recovered.contains(&id("a")));
    for name in ["b", "c", "d", "e"] {
        assert_eq!(
            recovered.read(&id(name)).await.unwrap().unwrap().as_ref(),
            bytes
        );
    }
}

#[tokio::test]
async fn spare_creation_failure_wakes_waiters_and_fails_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    let bytes = vec![7; 100 << 10];
    put(&store, &id("a"), &bytes).await.unwrap();
    put(&store, &id("b"), &bytes).await.unwrap();
    store.tasks.drain().await;
    let next = store.state.lock().await.next;
    // create_new must fail without touching an existing file.
    std::fs::write(store.path(next, "data"), b"collision").unwrap();
    // The asynchronous creation failure may now precede C's I/O completion.
    let _ = tokio::time::timeout(Duration::from_secs(2), put(&store, &id("c"), &bytes))
        .await
        .unwrap();
    store.tasks.drain().await;
    assert!(store.maintain(true).await.is_err());
    assert!(put(&store, &id("d"), &bytes).await.is_err());
    assert_eq!(
        std::fs::read(store.path(next, "data")).unwrap(),
        b"collision"
    );
}

#[tokio::test]
async fn cancelled_checkpoint_finishes_owned_seal() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    put(&store, &id("a"), b"saved").await.unwrap();
    store.tasks.drain().await;
    let pause = Arc::new(SealPause::default());
    *store.seal_pause.lock().unwrap() = Some(pause.clone());
    let writer = store.clone();
    let checkpoint = tokio::spawn(async move { writer.maintain(true).await });
    pause.entered.notified().await;
    checkpoint.abort();
    let _ = checkpoint.await;
    *store.seal_pause.lock().unwrap() = None;
    pause.resume.notify_one();
    store.tasks.drain().await;
    drop(store);
    assert_eq!(
        open(dir.path())
            .read(&id("a"))
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"saved"
    );
}

#[tokio::test]
async fn failed_manifest_publication_poison_wakes_rotation_waiter() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    let bytes = vec![7; 100 << 10];
    put(&store, &id("a"), &bytes).await.unwrap();
    put(&store, &id("b"), &bytes).await.unwrap();
    store.tasks.drain().await;
    let pause = Arc::new(SealPause::default());
    *store.seal_pause.lock().unwrap() = Some(pause.clone());
    put(&store, &id("c"), &bytes).await.unwrap();
    pause.entered.notified().await;
    put(&store, &id("d"), &bytes).await.unwrap();
    let writer = store.clone();
    let mut next = tokio::spawn(async move { put(&writer, &id("e"), &vec![7; 100 << 10]).await });
    assert!(tokio::time::timeout(Duration::from_millis(50), &mut next)
        .await
        .is_err());
    std::fs::create_dir(dir.path().join("MANIFEST.tmp")).unwrap();
    *store.seal_pause.lock().unwrap() = None;
    pause.resume.notify_one();
    assert!(tokio::time::timeout(Duration::from_secs(2), next)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    store.tasks.drain().await;
    assert!(store.maintain(true).await.is_err());
    drop(store);
    // No manifest was published; neither the sealing nor new active data is recovered.
    let recovered = open(dir.path());
    for name in ["a", "b", "c", "d"] {
        assert!(!recovered.contains(&id(name)));
    }
}

#[derive(Default)]
pub(super) struct AppendPause {
    pub(super) entered: tokio::sync::Notify,
    pub(super) resume: tokio::sync::Notify,
}

#[tokio::test]
async fn out_of_order_append_rollover_and_cancelled_caller_preserve_seal_barrier() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    put(&store, &id("seed"), b"seed").await.unwrap();
    store.tasks.drain().await;
    let old = store.state.lock().await.active.unwrap();
    let pause = Arc::new(AppendPause::default());
    *store.append_pause.lock().unwrap() = Some(pause.clone());
    let first = store.clone();
    let request = tokio::spawn(async move { put(&first, &id("a"), &vec![1; 100 << 10]).await });
    pause.entered.notified().await;
    tokio::time::timeout(
        Duration::from_secs(2),
        put(&store, &id("b"), &vec![2; 100 << 10]),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!store.contains(&id("a")));
    assert!(
        store.contains(&id("b")),
        "later append publishes independently"
    );
    // Reserved A + completed B fill the old segment; C must use the spare.
    tokio::time::timeout(
        Duration::from_secs(2),
        put(&store, &id("c"), &vec![3; 100 << 10]),
    )
    .await
    .unwrap()
    .unwrap();
    {
        let state = store.state.lock().await;
        assert_eq!(state.sealing, Some(old));
        assert_ne!(state.active, Some(old));
        assert_eq!(state.segments[&old].in_flight, 1);
        assert!(!state.segments[&old].sealed);
        assert!(state.segments[&old].bytes + state.segments[&old].metadata <= store.segment_size);
    }
    let checking = store.clone();
    let mut checkpoint = tokio::spawn(async move { checking.maintain(true).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut checkpoint)
            .await
            .is_err()
    );
    assert!(!dir.path().join("MANIFEST").exists());
    request.abort();
    let _ = request.await;
    pause.resume.notify_one();
    checkpoint.await.unwrap().unwrap();
    store.tasks.drain().await;
    drop(store);
    let recovered = open(dir.path());
    assert_eq!(
        recovered.read(&id("seed")).await.unwrap().unwrap().as_ref(),
        b"seed"
    );
    for (name, value) in [("a", 1), ("b", 2), ("c", 3)] {
        assert_eq!(
            recovered.read(&id(name)).await.unwrap().unwrap().as_ref(),
            vec![value; 100 << 10]
        );
    }
}

#[tokio::test]
async fn append_slots_bound_submissions_without_holding_state_lock() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    let held = store.append_slots.acquire_many(31).await.unwrap();
    let pause = Arc::new(AppendPause::default());
    *store.append_pause.lock().unwrap() = Some(pause.clone());
    let first = store.clone();
    let a = tokio::spawn(async move { put(&first, &id("a"), b"first").await });
    pause.entered.notified().await;
    let second = store.clone();
    let mut b = tokio::spawn(async move { put(&second, &id("b"), b"second").await });
    assert!(tokio::time::timeout(Duration::from_millis(50), &mut b)
        .await
        .is_err());
    assert_eq!(
        store
            .state
            .lock()
            .await
            .segments
            .values()
            .map(|s| s.in_flight)
            .sum::<usize>(),
        1
    );
    drop(held);
    tokio::time::timeout(Duration::from_secs(2), b)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    pause.resume.notify_one();
    a.await.unwrap().unwrap();
    store.tasks.drain().await;
}

#[tokio::test]
async fn failed_reserved_index_prevents_peer_publication_and_keeps_durable_segments() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    put(&store, &id("durable"), b"saved").await.unwrap();
    store.maintain(true).await.unwrap();
    let pause = Arc::new(AppendPause::default());
    *store.append_pause.lock().unwrap() = Some(pause.clone());
    let first = store.clone();
    let a = tokio::spawn(async move { put(&first, &id("a"), b"pending").await });
    pause.entered.notified().await;
    {
        let mut state = store.state.lock().await;
        let active = state.active.unwrap();
        // Only later reservations receive this read-only index descriptor.
        state.segments.get_mut(&active).unwrap().index =
            Arc::new(File::open(store.path(active, "index")).unwrap().into());
    }
    assert!(put(&store, &id("b"), b"failed").await.is_err());
    pause.resume.notify_one();
    assert!(a.await.unwrap().is_err());
    store.tasks.drain().await;
    assert!(!store.contains(&id("a")) && !store.contains(&id("b")));
    assert!(store.maintain(true).await.is_err());
    assert_eq!(
        store
            .state
            .lock()
            .await
            .segments
            .values()
            .map(|s| s.in_flight)
            .sum::<usize>(),
        0
    );
    drop(store);
    let recovered = open(dir.path());
    assert_eq!(
        recovered
            .read(&id("durable"))
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"saved"
    );
    assert!(!recovered.contains(&id("a")) && !recovered.contains(&id("b")));
}
