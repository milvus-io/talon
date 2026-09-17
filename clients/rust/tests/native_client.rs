#![cfg(target_os = "linux")]
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Barrier},
    time::Duration,
};
use talon_core::{NodeInfo, NodeRole};
use talon_rust_client::{ClientBuilder, ObjectId, Version};
use talon_transport::{ControlMessage, FrameHeader, HEADER_LEN};

fn frame(peer: &mut TcpStream) -> Option<Vec<u8>> {
    let mut header = [0; HEADER_LEN];
    match peer.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return None,
        Err(e) => panic!("reading request: {e}"),
    }
    let h = FrameHeader::decode(&header).unwrap();
    let mut bytes = header.to_vec();
    bytes.resize(HEADER_LEN + h.length as usize, 0);
    peer.read_exact(&mut bytes[HEADER_LEN..]).unwrap();
    Some(bytes)
}
fn accept(listener: &TcpListener) -> TcpStream {
    let (peer, _) = listener.accept().unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    peer.set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    peer
}

fn cluster() -> (
    String,
    std::thread::JoinHandle<()>,
    std::thread::JoinHandle<()>,
) {
    let worker = TcpListener::bind("127.0.0.1:0").unwrap();
    let worker_addr = format!("localhost:{}", worker.local_addr().unwrap().port());
    let coordinator = TcpListener::bind("127.0.0.1:0").unwrap();
    let coordinator_addr = format!("localhost:{}", coordinator.local_addr().unwrap().port());
    let worker_thread = std::thread::spawn(move || {
        let barrier = Arc::new(Barrier::new(3));
        let mut tasks = Vec::new();
        for _ in 0..3 {
            let mut peer = accept(&worker);
            let barrier = barrier.clone();
            tasks.push(std::thread::spawn(move || {
                let mut first = true;
                while let Some(bytes) = frame(&mut peer) {
                    let (h, request) = talon_transport::decode_versioned_request(&bytes).unwrap();
                    assert_eq!(request.version, Version::new("v1"));
                    if first {
                        barrier.wait();
                        first = false;
                    }
                    let r = request.request;
                    let body: Vec<u8> = (r.offset..r.offset + r.len).map(|n| n as u8).collect();
                    peer.write_all(&talon_transport::response_header_ok(
                        h.request_id,
                        body.len() as u32,
                    ))
                    .unwrap();
                    peer.write_all(&body).unwrap();
                }
            }));
        }
        for task in tasks {
            task.join().unwrap();
        }
    });
    let coordinator_thread = std::thread::spawn(move || {
        let mut peer = accept(&coordinator);
        while let Some(bytes) = frame(&mut peer) {
            let (h, request) = talon_transport::decode(&bytes).unwrap();
            let info = NodeInfo {
                id: talon_core::NodeId::new("worker"),
                address: worker_addr.clone(),
                role: NodeRole::Worker,
            };
            let response = match request {
                ControlMessage::StatObject { .. } => ControlMessage::ObjectStat {
                    size: 20,
                    version: "v1".into(),
                },
                ControlMessage::MembershipQuery {} => {
                    ControlMessage::MembershipList { nodes: vec![info] }
                }
                ControlMessage::MembershipQueryV2 {} => ControlMessage::MembershipListV2 {
                    nodes: vec![talon_transport::ZonedNodeInfo { info, zone: None }],
                },
                ControlMessage::ListObjects { prefix } => {
                    assert_eq!(prefix, "s3/bucket");
                    ControlMessage::ObjectList {
                        entries: vec![talon_rust_client::ObjectEntry {
                            path: "s3/bucket/key".into(),
                            size: 20,
                        }],
                    }
                }
                other => panic!("unexpected request {other:?}"),
            };
            peer.write_all(&talon_transport::encode(h.request_id, &response).unwrap())
                .unwrap();
        }
    });
    (coordinator_addr, worker_thread, coordinator_thread)
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn full_native_sdk_stat_discovery_multiblock_read_and_dns_without_tokio() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let (coordinator_addr, worker_thread, coordinator_thread) = cluster();
    monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .enable_timer()
        .build()
        .unwrap()
        .block_on(async {
            let client = ClientBuilder::default()
                .with_coordinator(coordinator_addr)
                .with_block_size(8)
                .build_native()
                .unwrap();
            let object = ObjectId::new(talon_core::Backend::S3, "bucket", "key");
            let stat = client.stat(&object).await.unwrap();
            assert_eq!(stat.size, 20);
            assert_eq!(
                client.read(&object, 0, None, Some(&stat)).await.unwrap(),
                (0..20).collect::<Vec<u8>>()
            );
            let (result, dst) = client
                .clone()
                .read_into(&object, 5, Box::new([0; 10]), Some(&stat))
                .await;
            assert_eq!(result.unwrap(), 10);
            assert_eq!(*dst, [5, 6, 7, 8, 9, 10, 11, 12, 13, 14]);
        });
    worker_thread.join().unwrap();
    coordinator_thread.join().unwrap();
}

fn exercise_hosted(backend: talon_rust_client::ClientIoBackend) {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let (coordinator_addr, worker_thread, coordinator_thread) = cluster();
    let client = ClientBuilder::default()
        .with_coordinator(coordinator_addr)
        .with_block_size(8)
        .with_io_backend(backend)
        .with_io_threads(1)
        .build_hosted()
        .unwrap();
    assert_eq!(
        client.io_backend(),
        if backend == talon_rust_client::ClientIoBackend::Auto {
            talon_rust_client::ClientIoBackend::Tokio
        } else {
            backend
        }
    );
    let object = ObjectId::new(talon_core::Backend::S3, "bucket", "key");
    futures::executor::block_on(async {
        // Includes stat, discovery and three concurrent block reads in one operation.
        assert_eq!(
            client.read(&object, 0, None, None).await.unwrap(),
            (0..20).collect::<Vec<u8>>()
        );
        let stat = client
            .stat_with_options(&object, &Default::default())
            .await
            .unwrap();
        assert_eq!(stat.size, 20);
        assert_eq!(stat.version, "v1");
        assert_eq!(client.list("s3/bucket").await.unwrap()[0].size, 20);
        assert_eq!(
            client
                .read(&object, 17, Some(10), Some(&stat))
                .await
                .unwrap(),
            [17, 18, 19]
        );
        assert!(client
            .read(&object, 20, Some(10), Some(&stat))
            .await
            .unwrap()
            .is_empty());
        // The eager request owns everything it needs after all public handles drop.
        let read = client.clone().read(&object, 5, Some(10), Some(&stat));
        drop(client);
        assert_eq!(read.await.unwrap(), (5..15).collect::<Vec<u8>>());
    });
    // Teardown must close both the control and all pooled data sockets.
    worker_thread.join().unwrap();
    coordinator_thread.join().unwrap();
}

#[test]
fn hosted_tokio_runs_whole_operations_without_a_caller_runtime() {
    exercise_hosted(talon_rust_client::ClientIoBackend::Tokio);
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn hosted_native_runs_whole_operations_without_a_caller_runtime() {
    exercise_hosted(talon_rust_client::ClientIoBackend::IoUring);
}

#[test]
#[ignore = "run through scripts/test_client_no_uring.py with process-local seccomp"]
fn hosted_auto_falls_back_when_ring_setup_is_denied() {
    use talon_rust_client::{ClientIoBackend, CoordinatorError, Error};
    assert!(monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .build()
        .is_err());
    match ClientBuilder::default()
        .with_coordinator("unused:1")
        .with_io_backend(ClientIoBackend::IoUring)
        .with_io_threads(1)
        .build_hosted()
    {
        Err(Error::Coordinator(CoordinatorError::Io(error))) => {
            assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied)
        }
        _ => panic!("strict hosted runtime must preserve EPERM"),
    }
    exercise_hosted(ClientIoBackend::Auto);
    parallel_callbacks(ClientIoBackend::Auto);
}

fn cancel_hosted(backend: talon_rust_client::ClientIoBackend) {
    let coordinator = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = coordinator.local_addr().unwrap().to_string();
    let (accepted, waiting) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let mut stalled = accept(&coordinator);
        frame(&mut stalled).unwrap();
        accepted.send(()).unwrap();
        // A second operation must make progress while the first response is stalled.
        let mut healthy = accept(&coordinator);
        let bytes = frame(&mut healthy).unwrap();
        let (header, _) = talon_transport::decode(&bytes).unwrap();
        healthy
            .write_all(
                &talon_transport::encode(
                    header.request_id,
                    &ControlMessage::ObjectStat {
                        size: 1,
                        version: "v1".into(),
                    },
                )
                .unwrap(),
            )
            .unwrap();
        assert!(frame(&mut stalled).is_none(), "cancelled socket must close");
        assert!(
            frame(&mut healthy).is_none(),
            "dropping handles must close idle sockets"
        );
    });
    let client = ClientBuilder::default()
        .with_coordinator(addr)
        .with_io_backend(backend)
        .with_io_threads(1)
        .build_hosted()
        .unwrap();
    let object = ObjectId::new(talon_core::Backend::S3, "bucket", "key");
    // Submission is eager: no caller executor has polled this future.
    let cancelled = client.stat_with_options(&object, &Default::default());
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    let result =
        futures::executor::block_on(client.stat_with_options(&object, &Default::default()))
            .unwrap();
    assert_eq!(result.size, 1);
    drop(cancelled);
    drop(client);
    server.join().unwrap();
}

#[test]
fn hosted_tokio_cancellation_and_concurrent_progress() {
    cancel_hosted(talon_rust_client::ClientIoBackend::Tokio);
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn hosted_native_cancellation_and_concurrent_progress() {
    cancel_hosted(talon_rust_client::ClientIoBackend::IoUring);
}

fn callback_lifecycle(backend: talon_rust_client::ClientIoBackend) {
    let coordinator = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = coordinator.local_addr().unwrap().to_string();
    let (waiting, received) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let mut peer = accept(&coordinator);
        let bytes = frame(&mut peer).unwrap();
        let (header, _) = talon_transport::decode(&bytes).unwrap();
        peer.write_all(
            &talon_transport::encode(
                header.request_id,
                &ControlMessage::ObjectStat {
                    size: 1,
                    version: "v1".into(),
                },
            )
            .unwrap(),
        )
        .unwrap();
        frame(&mut peer).unwrap();
        waiting.send(()).unwrap();
        assert!(
            frame(&mut peer).is_none(),
            "dropping the callback owner must close the socket"
        );
    });
    let client = ClientBuilder::default()
        .with_coordinator(addr)
        .with_io_backend(backend)
        .with_io_threads(1)
        .build_hosted()
        .unwrap();
    let object = ObjectId::new(talon_core::Backend::S3, "bucket", "key");
    let (done, callback) = std::sync::mpsc::channel();
    client
        .stat_with_callback(&object, &Default::default(), move |result| {
            assert_eq!(result.unwrap().size, 1);
            done.send((
                std::thread::current().name().unwrap().to_owned(),
                tokio::runtime::Handle::try_current().is_ok(),
            ))
            .unwrap();
        })
        .unwrap();
    let (thread, in_tokio) = callback.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(
        thread,
        if backend == talon_rust_client::ClientIoBackend::Tokio {
            "talon-sdk-tokio"
        } else {
            "talon-sdk-ring-0"
        }
    );
    assert_eq!(
        in_tokio,
        backend == talon_rust_client::ClientIoBackend::Tokio
    );
    let (done, callback) = std::sync::mpsc::channel();
    client
        .stat_with_callback(&object, &Default::default(), move |_| {
            done.send(()).unwrap();
        })
        .unwrap();
    received.recv_timeout(Duration::from_secs(5)).unwrap();
    drop(client);
    server.join().unwrap();
    assert_eq!(
        callback.recv_timeout(Duration::from_secs(5)),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
    );
}

#[test]
fn hosted_tokio_inline_callback_and_owner_cancellation() {
    callback_lifecycle(talon_rust_client::ClientIoBackend::Tokio);
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn hosted_native_inline_callback_and_owner_cancellation() {
    callback_lifecycle(talon_rust_client::ClientIoBackend::IoUring);
}

fn parallel_callbacks(backend: talon_rust_client::ClientIoBackend) {
    use std::{
        collections::HashSet,
        sync::{Condvar, Mutex},
    };
    let client = ClientBuilder::default()
        .with_coordinator("unused:1")
        .with_io_backend(backend)
        .with_io_threads(3)
        .build_hosted()
        .unwrap();
    assert_eq!(client.io_threads(), 3);
    assert_eq!(client.clone().io_threads(), 3);
    let gate = Arc::new((Mutex::new(0), Condvar::new()));
    let (done, received) = std::sync::mpsc::channel();
    let object = ObjectId::new(talon_core::Backend::S3, "bucket", "key");
    for _ in 0..3 {
        let (gate, done) = (gate.clone(), done.clone());
        client
            .clone()
            .read_into_with_callback(
                &object,
                0,
                Vec::<u8>::new(),
                None,
                &Default::default(),
                move |result| {
                    assert_eq!(result.unwrap(), 0);
                    // Deliberately rendezvous synchronous callbacks to prove CPU execution
                    // on three distinct workers, not just concurrent socket waits.
                    let (lock, ready) = &*gate;
                    let mut arrived = lock.lock().unwrap();
                    *arrived += 1;
                    ready.notify_all();
                    let (arrived, timeout) = ready
                        .wait_timeout_while(arrived, Duration::from_secs(5), |n| *n < 3)
                        .unwrap();
                    done.send((
                        std::thread::current().id(),
                        *arrived == 3 && !timeout.timed_out(),
                        tokio::runtime::Handle::try_current().is_ok(),
                    ))
                    .unwrap();
                },
            )
            .unwrap();
    }
    let mut threads = HashSet::new();
    for _ in 0..3 {
        let (id, parallel, tokio) = received.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(parallel, "callbacks serialized on one execution thread");
        assert_eq!(
            tokio,
            backend != talon_rust_client::ClientIoBackend::IoUring
        );
        threads.insert(id);
    }
    assert_eq!(threads.len(), 3);
}

#[test]
fn hosted_tokio_restores_parallel_execution_for_one_client() {
    parallel_callbacks(talon_rust_client::ClientIoBackend::Tokio);
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn hosted_native_runs_one_client_on_three_rings() {
    parallel_callbacks(talon_rust_client::ClientIoBackend::IoUring);
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn hosted_rings_share_membership_refresh_and_placement() {
    let (coordinator_addr, worker, coordinator) = cluster();
    // The fixture accepts a single coordinator connection. Three rings must
    // share the initial membership refresh rather than issue separate queries.
    let client = ClientBuilder::default()
        .with_coordinator(coordinator_addr)
        .with_block_size(8)
        .with_io_threads(3)
        .with_io_backend(talon_rust_client::ClientIoBackend::IoUring)
        .build_hosted()
        .unwrap();
    let object = ObjectId::new(talon_core::Backend::S3, "bucket", "key");
    let stat = talon_rust_client::ObjectStat {
        size: 20,
        version: "v1".into(),
    };
    let reads: Vec<_> = (0..3)
        .map(|_| client.read(&object, 0, Some(8), Some(&stat)))
        .collect();
    let results = futures::executor::block_on(futures::future::try_join_all(reads)).unwrap();
    assert_eq!(results, vec![(0..8).collect::<Vec<u8>>(); 3]);
    drop(client);
    worker.join().unwrap();
    coordinator.join().unwrap();
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn hosted_native_callback_chain_keeps_ring_affinity() {
    use std::{collections::HashSet, sync::mpsc, thread::ThreadId};
    use talon_rust_client::HostedClient;

    fn next(client: HostedClient, mut threads: Vec<ThreadId>, done: mpsc::Sender<Vec<ThreadId>>) {
        let keep_alive = client.clone();
        let object = ObjectId::new(talon_core::Backend::S3, "bucket", "key");
        client
            .read_into_with_callback(
                &object,
                0,
                Vec::<u8>::new(),
                None,
                &Default::default(),
                move |result| {
                    assert_eq!(result.unwrap(), 0);
                    threads.push(std::thread::current().id());
                    if threads.len() == 1024 {
                        done.send(threads).unwrap();
                    } else {
                        next(keep_alive, threads, done);
                    }
                },
            )
            .unwrap();
    }

    let client = ClientBuilder::default()
        .with_coordinator("unused:1")
        .with_io_backend(talon_rust_client::ClientIoBackend::IoUring)
        .with_io_threads(3)
        .build_hosted()
        .unwrap();
    let (done, received) = mpsc::channel();
    next(client.clone(), Vec::new(), done);
    let threads = received.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_eq!(
        threads.into_iter().collect::<HashSet<_>>().len(),
        1,
        "a callback continuation should reuse its ring and connection pool"
    );
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn hosted_native_callback_affinity_does_not_cross_clients() {
    let first = ClientBuilder::default()
        .with_coordinator("unused:1")
        .with_io_backend(talon_rust_client::ClientIoBackend::IoUring)
        .with_io_threads(3)
        .build_hosted()
        .unwrap();
    let second = ClientBuilder::default()
        .with_coordinator("unused:1")
        .with_io_backend(talon_rust_client::ClientIoBackend::IoUring)
        .with_io_threads(1)
        .build_hosted()
        .unwrap();
    let barrier = Arc::new((std::sync::Mutex::new(0), std::sync::Condvar::new()));
    let (sent, received) = std::sync::mpsc::channel();
    for _ in 0..3 {
        let (barrier, second, sent) = (barrier.clone(), second.clone(), sent.clone());
        let object = ObjectId::new(talon_core::Backend::S3, "bucket", "key");
        first
            .read_into_with_callback(
                &object,
                0,
                Vec::<u8>::new(),
                None,
                &Default::default(),
                move |result| {
                    assert_eq!(result.unwrap(), 0);
                    let origin = std::thread::current().id();
                    let (lock, ready) = &*barrier;
                    let mut arrived = lock.lock().unwrap();
                    *arrived += 1;
                    ready.notify_all();
                    let (arrived, timeout) = ready
                        .wait_timeout_while(arrived, Duration::from_secs(5), |n| *n < 3)
                        .unwrap();
                    assert!(*arrived == 3 && !timeout.timed_out());
                    drop(arrived);
                    second
                        .read_into_with_callback(
                            &ObjectId::new(talon_core::Backend::S3, "bucket", "other"),
                            0,
                            Vec::<u8>::new(),
                            None,
                            &Default::default(),
                            move |result| {
                                assert_eq!(result.unwrap(), 0);
                                sent.send((origin, std::thread::current().id())).unwrap();
                            },
                        )
                        .unwrap();
                },
            )
            .unwrap();
    }
    let pairs: Vec<_> = (0..3)
        .map(|_| received.recv_timeout(Duration::from_secs(10)).unwrap())
        .collect();
    for (origin, destination) in &pairs {
        assert_ne!(origin, destination);
        assert_eq!(*destination, pairs[0].1);
    }
}

fn hosted_dispatch_isolation(backend: talon_rust_client::ClientIoBackend) {
    struct Marker(u64);
    impl tracing::Subscriber for Marker {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, _: &tracing::Event<'_>) {}
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }
    let client = ClientBuilder::default()
        .with_coordinator("unused:1")
        .with_io_backend(backend)
        .with_io_threads(1)
        .build_hosted()
        .unwrap();
    let (sent, received) = std::sync::mpsc::channel();
    if std::env::var_os("TALON_TEST_GLOBAL_DISPATCH").is_some() {
        tracing::dispatcher::set_global_default(tracing::Dispatch::new(Marker(99))).unwrap();
    }
    for expected in [Some(11), None, Some(22), None] {
        let sent = sent.clone();
        let submit = || {
            client
                .read_into_with_callback(
                    &ObjectId::new(talon_core::Backend::S3, "bucket", "key"),
                    0,
                    Vec::<u8>::new(),
                    None,
                    &Default::default(),
                    move |result| {
                        assert_eq!(result.unwrap(), 0);
                        let actual = tracing::dispatcher::get_default(|d| {
                            d.downcast_ref::<Marker>().map(|m| m.0)
                        });
                        sent.send((actual, expected)).unwrap();
                    },
                )
                .unwrap();
        };
        match expected {
            Some(id) => {
                tracing::dispatcher::with_default(&tracing::Dispatch::new(Marker(id)), submit)
            }
            None => tracing::dispatcher::with_default(&tracing::Dispatch::none(), submit),
        }
    }
    for _ in 0..4 {
        let (actual, expected) = received.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(
            actual, expected,
            "subscriber lost or leaked between operations"
        );
    }
}

#[test]
fn hosted_tokio_preserves_subscribers_without_leaking_to_disabled_tasks() {
    hosted_dispatch_isolation(talon_rust_client::ClientIoBackend::Tokio);
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn hosted_native_preserves_subscribers_without_leaking_to_disabled_tasks() {
    hosted_dispatch_isolation(talon_rust_client::ClientIoBackend::IoUring);
}

fn check_global_dispatch_child(test: &str) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--include-ignored", "--nocapture"])
        .env("TALON_TEST_GLOBAL_DISPATCH", "1")
        .status()
        .unwrap();
    assert!(status.success());
}

#[test]
fn hosted_tokio_explicit_no_subscriber_overrides_global_default() {
    check_global_dispatch_child(
        "hosted_tokio_preserves_subscribers_without_leaking_to_disabled_tasks",
    );
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn hosted_native_explicit_no_subscriber_overrides_global_default() {
    check_global_dispatch_child(
        "hosted_native_preserves_subscribers_without_leaking_to_disabled_tasks",
    );
}
