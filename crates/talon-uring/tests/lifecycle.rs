use std::{sync::Arc, time::Duration};

fn on_ring(work: impl std::future::Future<Output = ()>) {
    monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .enable_timer()
        .build()
        .unwrap()
        .block_on(async {
            let ring = talon_uring::LocalRing::enter();
            work.await;
            ring.drain().await;
        });
}

#[test]
#[ignore = "requires a kernel/container allowing native io_uring"]
fn parallel_directory_creation_and_failed_rename_cleanup() {
    on_ring(async {
        let root = tempfile::tempdir().unwrap();
        let mut tasks = Vec::new();
        for index in 0..32 {
            let path = root.path().join(format!("shared/new/{index}"));
            tasks.push(talon_uring::spawn_local(async move {
                talon_uring::atomic_write(&path, bytes::Bytes::from_static(b"complete")).await
            }));
        }
        for result in futures::future::join_all(tasks).await {
            result.unwrap().unwrap();
        }
        let directory = root.path().join("shared/new");
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 32);
        assert!(
            talon_uring::atomic_write(&directory, bytes::Bytes::from_static(b"bad"))
                .await
                .is_err()
        );
        assert_eq!(
            std::fs::read_dir(root.path().join("shared"))
                .unwrap()
                .count(),
            1,
            "failed rename must remove its staging file"
        );
    });
}

#[test]
#[ignore = "requires a kernel/container allowing native io_uring"]
fn accepted_write_survives_caller_cancellation_and_drains_on_its_ring() {
    on_ring(async {
        let scope_thread = std::thread::current().id();
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("cache/block");
        let target = path.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        let (done, completed) = tokio::sync::oneshot::channel();
        let caller = talon_uring::spawn_local(async move {
            started.send(()).unwrap();
            wait.await.unwrap();
            assert_eq!(std::thread::current().id(), scope_thread);
            talon_uring::atomic_write(&target, bytes::Bytes::from_static(b"complete"))
                .await
                .unwrap();
            done.send(()).unwrap();
        });
        ready.await.unwrap();
        drop(caller);
        release.send(()).unwrap();
        monoio::time::timeout(Duration::from_secs(2), completed)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"complete");
        assert_eq!(
            std::fs::read_dir(path.parent().unwrap()).unwrap().count(),
            1
        );
    });
}

#[test]
#[ignore = "requires a kernel/container allowing native io_uring"]
fn cancelled_network_drops_ring_owned_state() {
    on_ring(async {
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        struct Guard(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let flag = dropped.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        let task = talon_uring::spawn_cancellable(async move {
            let _guard = Guard(flag);
            started.send(()).unwrap();
            futures::future::pending::<()>().await;
        });
        ready.await.unwrap();
        drop(task);
        monoio::time::timeout(Duration::from_secs(2), async {
            while !dropped.load(std::sync::atomic::Ordering::SeqCst) {
                monoio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
    });
}

#[test]
#[ignore = "requires a kernel/container allowing native io_uring"]
fn local_ring_keeps_jobs_on_thread_and_drains_cancelled_callers() {
    let mut runtime = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .enable_timer()
        .build()
        .unwrap();
    runtime.block_on(async {
        let scope = talon_uring::LocalRing::enter();
        let caller = std::thread::current().id();
        let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = finished.clone();
        drop(talon_uring::spawn_local(async move {
            talon_uring::sleep(Duration::from_millis(10)).await;
            assert_eq!(std::thread::current().id(), caller);
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        }));
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

#[test]
fn native_io_requires_existing_ring() {
    assert!(talon_uring::require_ring().is_err());
}

#[test]
#[ignore = "requires a kernel/container allowing native io_uring"]
fn drain_cancels_background_work_even_before_first_poll() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Guard(Arc<AtomicBool>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let mut runtime = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .enable_timer()
        .build()
        .unwrap();
    runtime.block_on(async {
        let ring = talon_uring::LocalRing::enter();
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = Guard(dropped.clone());
        let (started, ready) = tokio::sync::oneshot::channel();
        talon_uring::spawn_background(async move {
            let _guard = guard;
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        ready.await.unwrap();

        let unpolled_dropped = Arc::new(AtomicBool::new(false));
        let guard = Guard(unpolled_dropped.clone());
        talon_uring::spawn_background(async move {
            let _guard = guard;
            panic!("background work queued before drain must not start");
        });
        monoio::time::timeout(Duration::from_secs(2), ring.drain())
            .await
            .unwrap();
        assert!(dropped.load(Ordering::SeqCst));
        assert!(unpolled_dropped.load(Ordering::SeqCst));

        // Accepted mutations can finish during drain and attempt to return an
        // origin connection to the pool. Late background work must also drop.
        let late_dropped = Arc::new(AtomicBool::new(false));
        let guard = Guard(late_dropped.clone());
        talon_uring::spawn_background(async move {
            let _guard = guard;
            panic!("background work submitted after drain must not start");
        });
        monoio::time::timeout(Duration::from_secs(2), ring.drain())
            .await
            .unwrap();
        assert!(late_dropped.load(Ordering::SeqCst));
    });
}
