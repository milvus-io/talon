#![cfg(target_os = "linux")]
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    rc::Rc,
    sync::Arc,
    time::Duration,
};
use talon_cache_client::{
    monoio_client::MonoioClient,
    rpc::{Reply, Request},
    ClientIoBackend, ConnectionPool, WorkerClient, WorkerError,
};
use talon_core::{Backend, ObjectId, Version};
use talon_transport::{FrameHeader, HEADER_LEN};
const T: Duration = Duration::from_secs(5);
fn object() -> ObjectId {
    ObjectId::new(Backend::S3, "bucket", "object")
}
fn request(stream: &mut TcpStream) -> (FrameHeader, Vec<u8>) {
    let mut header = [0; HEADER_LEN];
    stream.read_exact(&mut header).unwrap();
    let header = FrameHeader::decode(&header).unwrap();
    let mut body = vec![0; header.length as usize];
    stream.read_exact(&mut body).unwrap();
    (header, body)
}
fn reply(stream: &mut TcpStream, id: u32, bytes: &[u8]) {
    stream
        .write_all(&talon_transport::response_header_ok(id, bytes.len() as u32))
        .unwrap();
    stream.write_all(bytes).unwrap();
}
fn server(
    f: impl FnOnce(TcpListener) + Send + 'static,
) -> (SocketAddr, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    (addr, std::thread::spawn(move || f(listener)))
}
fn accept(listener: &TcpListener) -> TcpStream {
    let (s, _) = listener.accept().unwrap();
    s.set_read_timeout(Some(T)).unwrap();
    s.set_write_timeout(Some(T)).unwrap();
    s
}
fn native<F: std::future::Future>(f: F) -> F::Output {
    assert!(tokio::runtime::Handle::try_current().is_err());
    monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .enable_timer()
        .build()
        .unwrap()
        .block_on(f)
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn direct_native_ranges_control_and_writes_need_no_tokio_runtime() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let contents: Vec<u8> = (0..(256 * 1024 + 17)).map(|i| i as u8).collect();
    std::fs::write(file.path(), &contents).unwrap();
    let expected = contents.clone();
    let (addr, task) = server(move |listener| {
        let mut peer = accept(&listener);
        for _ in 0..3 {
            let (h, _) = request(&mut peer);
            reply(&mut peer, h.request_id, b"payload");
        }
        let (h, _) = request(&mut peer);
        peer.write_all(
            &talon_transport::encode(
                h.request_id,
                &talon_transport::ControlMessage::MembershipList { nodes: vec![] },
            )
            .unwrap(),
        )
        .unwrap();
        for bytes in [b"body".to_vec(), expected] {
            let (h, p) = request(&mut peer);
            let mut encoded = h.encode().to_vec();
            encoded.extend_from_slice(&p);
            let (_, put) = talon_transport::decode_put_header(&encoded).unwrap();
            assert_eq!(put.body_len, bytes.len() as u64);
            let mut got = vec![0; bytes.len()];
            peer.read_exact(&mut got).unwrap();
            assert_eq!(got, bytes);
            reply(&mut peer, h.request_id, b"committed");
        }
        let (h, _) = request(&mut peer);
        reply(&mut peer, h.request_id, b"");
    });
    native(async {
        let client = MonoioClient::new();
        assert_eq!(
            client.fetch_range(addr, &object(), 0, 7).await.unwrap(),
            b"payload"
        );
        let version = Version::new("v1");
        let frame = talon_transport::encode_versioned_request(
            2,
            &talon_transport::VersionedRangeRequest {
                request: talon_transport::RangeRequest {
                    object: object(),
                    offset: 0,
                    len: 7,
                },
                version: version.clone(),
            },
        )
        .unwrap();
        assert!(
            matches!(client.request(addr,Request::range(frame,7)).await.unwrap(),Reply::Range(b) if b==b"payload")
        );
        let frame = talon_transport::encode_cached_request(
            3,
            &talon_transport::CachedRangeRequest {
                object: object(),
                version,
                offset: 0,
                len: 7,
            },
        )
        .unwrap();
        client
            .request(addr, Request::range(frame, 7))
            .await
            .unwrap();
        let frame =
            talon_transport::encode(4, &talon_transport::ControlMessage::MembershipQuery {})
                .unwrap();
        assert!(
            matches!(client.request(addr,Request::control(frame)).await.unwrap(),Reply::Control(talon_transport::ControlMessage::MembershipList { nodes }) if nodes.is_empty())
        );
        let frame = talon_transport::encode_put_header(
            5,
            &talon_transport::PutRequest {
                object: object(),
                body_len: 4,
            },
        )
        .unwrap();
        assert!(
            matches!(client.request(addr,Request::put(frame,bytes::Bytes::from_static(b"body"))).await.unwrap(),Reply::Version(v) if v==Version::new("committed"))
        );
        let frame = talon_transport::encode_put_header(
            6,
            &talon_transport::PutRequest {
                object: object(),
                body_len: contents.len() as u64,
            },
        )
        .unwrap();
        client
            .request(
                addr,
                Request::put_file(frame, file.path().to_owned(), contents.len() as u64),
            )
            .await
            .unwrap();
        let frame =
            talon_transport::encode_delete(7, &talon_transport::DeleteRequest { object: object() })
                .unwrap();
        client.request(addr, Request::delete(frame)).await.unwrap();
        assert_eq!(client.idle_count(addr), 1);
    });
    task.join().unwrap();
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn native_stalled_request_does_not_block_other_calls_or_leak_on_cancel() {
    let (addr, task) = server(|listener| {
        let mut stalled = accept(&listener);
        request(&mut stalled);
        let mut healthy = accept(&listener);
        let (h, _) = request(&mut healthy);
        reply(&mut healthy, h.request_id, b"yes");
        let mut byte = [0];
        assert_eq!(stalled.read(&mut byte).unwrap(), 0);
    });
    native(async {
        let client = Rc::new(MonoioClient::new());
        // Own the object for the lifetime of the borrowed future.
        let obj = object();
        let stalled = client.fetch_range(addr, &obj, 0, 3);
        let timeout = monoio::time::timeout(Duration::from_millis(200), stalled);
        let healthy = async {
            monoio::time::sleep(Duration::from_millis(30)).await;
            assert_eq!(
                client.fetch_range(addr, &object(), 0, 3).await.unwrap(),
                b"yes"
            );
        };
        let (result, ()) = futures::join!(timeout, healthy);
        assert!(result.is_err());
        assert_eq!(client.idle_count(addr), 1);
    });
    task.join().unwrap();
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn native_timeout_is_enforced_without_tokio_and_rejects_untrusted_lengths() {
    let (addr, task) = server(|listener| {
        let mut first = accept(&listener);
        let (h, _) = request(&mut first);
        first
            .write_all(&talon_transport::response_header_ok(h.request_id, 4096))
            .unwrap();
        first.write_all(b"prefix").unwrap();
        let mut byte = [0];
        assert_eq!(first.read(&mut byte).unwrap(), 0);
        let mut second = accept(&listener);
        let (h, _) = request(&mut second);
        second
            .write_all(&talon_transport::response_header_ok(
                h.request_id,
                1024 * 1024,
            ))
            .unwrap();
        assert_eq!(second.read(&mut byte).unwrap(), 0);
    });
    native(async {
        let client = MonoioClient::new().with_timeouts(T, Duration::from_millis(200));
        assert!(
            matches!(client.fetch_range(addr,&object(),0,4096).await,Err(WorkerError::Io(e)) if e.kind()==std::io::ErrorKind::TimedOut)
        );
        assert!(matches!(
            client.fetch_range(addr, &object(), 0, 1).await,
            Err(WorkerError::RangeLengthMismatch { .. })
        ));
        assert_eq!(client.idle_count(addr), 0);
    });
    task.join().unwrap();
}

#[tokio::test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
async fn bridge_cancellation_retains_owned_destination() {
    let (addr, task) = server(|listener| {
        let mut peer = accept(&listener);
        let (h, _) = request(&mut peer);
        peer.write_all(&talon_transport::response_header_ok(h.request_id, 4096))
            .unwrap();
        peer.write_all(b"prefix").unwrap();
        let mut b = [0];
        assert_eq!(peer.read(&mut b).unwrap(), 0);
    });
    let pool = Arc::new(ConnectionPool::new().with_io_backend(ClientIoBackend::IoUring));
    let client = WorkerClient::with_pool(addr.to_string(), pool.clone());
    struct Destination {
        bytes: Vec<u8>,
        retired: Option<tokio::sync::oneshot::Sender<()>>,
    }
    impl AsMut<[u8]> for Destination {
        fn as_mut(&mut self) -> &mut [u8] {
            &mut self.bytes
        }
    }
    impl Drop for Destination {
        fn drop(&mut self) {
            let _ = self.retired.take().unwrap().send(());
        }
    }
    let (retired, finished) = tokio::sync::oneshot::channel();
    let dst = Destination {
        bytes: vec![0xCD; 4096],
        retired: Some(retired),
    };
    assert!(tokio::time::timeout(
        Duration::from_millis(200),
        client.fetch_range_into(&object(), 0, Box::new(dst))
    )
    .await
    .is_err());
    tokio::time::timeout(Duration::from_secs(5), finished)
        .await
        .unwrap()
        .unwrap();
    tokio::task::spawn_blocking(move || task.join().unwrap())
        .await
        .unwrap();
    assert_eq!(pool.idle_count(&addr.to_string()), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
async fn bridge_concurrent_requests_and_pool_drop_close_all_idle_sockets() {
    let (addr, task) = server(|listener| {
        let mut connections = Vec::new();
        let barrier = Arc::new(std::sync::Barrier::new(32));
        for _ in 0..32 {
            let barrier = barrier.clone();
            let mut peer = accept(&listener);
            connections.push(std::thread::spawn(move || {
                let (h, _) = request(&mut peer);
                barrier.wait();
                reply(&mut peer, h.request_id, &vec![0xAB; 65536 + 17]);
                let mut b = [0];
                assert_eq!(peer.read(&mut b).unwrap(), 0);
            }));
        }
        for c in connections {
            c.join().unwrap();
        }
    });
    let pool = Arc::new(
        ConnectionPool::with_limits(32, Duration::from_secs(30))
            .with_io_backend(ClientIoBackend::IoUring),
    );
    let client = WorkerClient::with_pool(addr.to_string(), pool.clone());
    // Hold all replies until every call has its own in-flight connection.
    let obj = object();
    let calls = (0..32).map(|_| client.fetch_range(&obj, 0, 65536 + 17));
    let replies = futures::future::try_join_all(calls).await.unwrap();
    assert!(replies
        .iter()
        .all(|b| b.len() == 65536 + 17 && b.iter().all(|v| *v == 0xAB)));
    assert_eq!(pool.idle_count(&addr.to_string()), 32);
    drop(client);
    drop(pool);
    tokio::task::spawn_blocking(move || task.join().unwrap())
        .await
        .unwrap();
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn native_reuses_across_calls_and_expired_pool_redials() {
    let (addr, task) = server(|listener| {
        for _ in 0..2 {
            let mut peer = accept(&listener);
            let (h, _) = request(&mut peer);
            reply(&mut peer, h.request_id, b"x");
        }
    });
    native(async {
        let client = MonoioClient::new().with_limits(1, Duration::ZERO);
        for _ in 0..2 {
            assert_eq!(
                client.fetch_range(addr, &object(), 0, 1).await.unwrap(),
                b"x"
            );
        }
    });
    task.join().unwrap();
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn facade_pool_survives_caller_runtime_replacement_and_retries_stale_socket() {
    let (addr, task) = server(|listener| {
        let mut peer = accept(&listener);
        for _ in 0..2 {
            let (h, _) = request(&mut peer);
            reply(&mut peer, h.request_id, b"x");
        }
        // The SDK pooled a now-stale socket; a range can retry it once.
        drop(peer);
        let mut peer = accept(&listener);
        let (h, _) = request(&mut peer);
        let header = talon_transport::response_header_ok(h.request_id, 1);
        for part in header.chunks(3) {
            peer.write_all(part).unwrap();
        }
        peer.write_all(b"y").unwrap();
    });
    let pool = Arc::new(ConnectionPool::new().with_io_backend(ClientIoBackend::IoUring));
    let client = WorkerClient::with_pool(addr.to_string(), pool);
    for expected in [b"x", b"x", b"y"] {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert_eq!(
            runtime
                .block_on(client.fetch_range(&object(), 0, 1))
                .unwrap(),
            expected
        );
    }
    drop(client);
    task.join().unwrap();
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn rings_share_one_idle_budget_and_release_it_on_drop() {
    use std::sync::Barrier;
    use talon_cache_client::monoio_client::SharedIdleBudget;
    let budget = Arc::new(SharedIdleBudget::new(2));
    let (addr, server) = server(|listener| {
        let barrier = Arc::new(Barrier::new(3));
        let mut peers = Vec::new();
        for _ in 0..3 {
            let mut peer = accept(&listener);
            let barrier = barrier.clone();
            peers.push(std::thread::spawn(move || {
                let (h, _) = request(&mut peer);
                barrier.wait();
                reply(&mut peer, h.request_id, b"x");
                assert_eq!(peer.read(&mut [0]).unwrap(), 0);
            }));
        }
        for peer in peers {
            peer.join().unwrap();
        }
    });
    let retained = Arc::new(Barrier::new(4));
    let release = Arc::new(Barrier::new(4));
    let mut rings = Vec::new();
    for _ in 0..3 {
        let (budget, retained, release) = (budget.clone(), retained.clone(), release.clone());
        rings.push(std::thread::spawn(move || {
            native(async {
                let client = MonoioClient::new().with_idle_budget(&budget);
                assert_eq!(
                    client.fetch_range(addr, &object(), 0, 1).await.unwrap(),
                    b"x"
                );
                retained.wait();
                release.wait();
            })
        }));
    }
    retained.wait();
    assert_eq!(budget.idle_count(&addr.to_string()), 2);
    release.wait();
    for ring in rings {
        ring.join().unwrap();
    }
    assert_eq!(budget.idle_count(&addr.to_string()), 0);
    server.join().unwrap();
}

#[test]
#[ignore = "requires Linux io_uring; run explicitly with --ignored"]
fn borrowed_native_range_timeout_retires_kernel_access_before_reuse() {
    use talon_cache_client::{read_buffer::ReadBuffer, rpc::RequestExecutor};
    let (addr, task) = server(|listener| {
        let mut stalled = accept(&listener);
        let (header, _) = request(&mut stalled);
        stalled
            .write_all(&talon_transport::response_header_ok(header.request_id, 32))
            .unwrap();
        stalled.write_all(b"old").unwrap();
        assert_eq!(
            stalled.read(&mut [0]).unwrap(),
            0,
            "timed-out direct socket must close"
        );
        let mut healthy = accept(&listener);
        for byte in [7, 9] {
            let (header, _) = request(&mut healthy);
            reply(&mut healthy, header.request_id, &[byte; 32]);
        }
    });
    native(async {
        let client = MonoioClient::new().with_timeouts(T, Duration::from_millis(200));
        let mut buffer = ReadBuffer::new(vec![0xCD; 32]);
        let mut target = buffer.take_target();
        let frame = talon_transport::encode_request(
            1,
            &talon_transport::RangeRequest {
                object: object(),
                offset: 0,
                len: 32,
            },
        )
        .unwrap();
        let result = client
            .execute_range_into(&addr.to_string(), frame.clone(), &mut target)
            .await;
        assert!(
            matches!(result, Err(talon_cache_client::rpc::Error::Io(ref e)) if e.kind() == std::io::ErrorKind::TimedOut)
        );
        for _ in 0..2 {
            assert!(matches!(
                client
                    .execute_range_into(&addr.to_string(), frame.clone(), &mut target)
                    .await
                    .unwrap(),
                Reply::Written(32)
            ));
            // Successful completion must have initialized every byte; reusing the
            // same target must not race the previously cancelled partial receive.
        }
        drop(target);
        assert_eq!(buffer.finish().await, [9; 32]);
        assert_eq!(client.idle_count(addr), 1);
    });
    task.join().unwrap();
}
