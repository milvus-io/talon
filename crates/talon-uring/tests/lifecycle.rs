use std::{sync::Arc, time::Duration};
use talon_uring::Executor;

#[tokio::test]
#[ignore = "requires a kernel/container allowing native io_uring"]
async fn parallel_directory_creation_and_failed_rename_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let executor = Executor::new("test-atomic-ring", 8).unwrap();
    let mut tasks = Vec::new();
    for index in 0..32 {
        let path = root.path().join(format!("shared/new/{index}"));
        tasks.push(executor.run(move || async move {
            talon_uring::atomic_write(&path, bytes::Bytes::from_static(b"complete")).await
        }));
    }
    for result in futures::future::join_all(tasks).await {
        result.unwrap().unwrap();
    }
    let directory = root.path().join("shared/new");
    assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 32);
    let invalid = directory.clone();
    assert!(executor
        .run(move || async move {
            talon_uring::atomic_write(&invalid, bytes::Bytes::from_static(b"bad")).await
        })
        .await
        .unwrap()
        .is_err());
    assert_eq!(
        std::fs::read_dir(root.path().join("shared"))
            .unwrap()
            .count(),
        1,
        "failed rename must remove its staging file"
    );
}

#[tokio::test]
#[ignore = "requires a kernel/container allowing native io_uring"]
async fn accepted_write_survives_cancellation_and_drains_on_last_handle_drop() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("cache/block");
    let executor = Executor::new("test-disk-ring", 1).unwrap();
    let (started, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = tokio::sync::oneshot::channel();
    let (done, completed) = tokio::sync::oneshot::channel();
    let target = path.clone();
    let task = tokio::spawn(async move {
        executor
            .run(move || async move {
                assert!(!monoio::utils::is_legacy());
                started.send(()).unwrap();
                wait.await.unwrap();
                talon_uring::atomic_write(&target, bytes::Bytes::from_static(b"complete"))
                    .await
                    .unwrap();
                let _ = done.send(());
            })
            .await
            .unwrap();
    });
    ready.await.unwrap();
    task.abort();
    let _ = task.await;
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), completed)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"complete");
    assert_eq!(
        std::fs::read_dir(path.parent().unwrap()).unwrap().count(),
        1
    );
}

#[tokio::test]
#[ignore = "requires a kernel/container allowing native io_uring"]
async fn cancelled_network_releases_admission_and_drops_ring_owned_state() {
    let executor = Executor::new("test-network-ring", 1).unwrap();
    let clone = executor.clone();
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    struct Guard(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let flag = dropped.clone();
    let (started, ready) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        clone
            .run_cancellable(move || async move {
                let _guard = Guard(flag);
                started.send(()).unwrap();
                futures::future::pending::<()>().await;
            })
            .await
            .unwrap();
    });
    ready.await.unwrap();
    task.abort();
    let _ = task.await;
    tokio::time::timeout(Duration::from_secs(5), executor.run(|| async { 42 }))
        .await
        .unwrap()
        .unwrap();
    assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
}

#[test]
#[ignore = "requires a kernel/container allowing native io_uring"]
fn local_ring_keeps_jobs_on_thread_and_drains_cancelled_callers() {
    let executor = Executor::new("unused-fallback", 4).unwrap();
    let mut runtime = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .enable_timer()
        .build()
        .unwrap();
    runtime.block_on(async {
        let scope = talon_uring::LocalRing::enter();
        let caller = std::thread::current().id();
        assert_eq!(
            executor
                .run(|| async { std::thread::current().id() })
                .await
                .unwrap(),
            caller
        );
        let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = finished.clone();
        // Drop the wait handle just like a disconnected request. Nested I/O
        // remains owned by this ring and must complete before its drain returns.
        drop(talon_uring::spawn_local(async move {
            talon_uring::sleep(Duration::from_millis(10)).await;
            executor
                .run(move || async move {
                    assert_eq!(std::thread::current().id(), caller);
                    flag.store(true, std::sync::atomic::Ordering::SeqCst);
                })
                .await
                .unwrap();
        }));
        // A completed timeout must cancel its long-lived timer, not keep the
        // ring drain waiting for five minutes.
        talon_uring::timeout(Duration::from_secs(300), async {
            talon_uring::sleep(Duration::from_millis(1)).await;
        })
        .await
        .unwrap();
        monoio::time::timeout(Duration::from_secs(2), scope.drain())
            .await
            .unwrap();
        assert!(finished.load(std::sync::atomic::Ordering::SeqCst));
    });
    assert!(!talon_uring::on_ring());
}
