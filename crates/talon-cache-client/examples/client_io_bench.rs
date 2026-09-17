//! Local framed-TCP comparison; excludes storage, TLS and deployment effects.
//! cargo run --release -p talon-cache-client --example client_io_bench -- 1000
use std::sync::Arc;
use std::time::Instant;
use talon_cache_client::{ClientIoBackend, ConnectionPool, WorkerClient};
use talon_core::{Backend, ObjectId};
use talon_transport::{FrameHeader, HEADER_LEN};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main(worker_threads = 4)]
async fn main() {
    let samples: usize = std::env::args()
        .nth(1)
        .unwrap_or("1000".into())
        .parse()
        .unwrap();
    assert!(samples > 0);
    println!("backend,bytes,concurrency,requests,qps,p50_us,p99_us");
    for size in [4096, 65536] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut socket, _) = accepted.unwrap();
                        tasks.spawn(async move {
                            let mut header = [0; HEADER_LEN];
                            while socket.read_exact(&mut header).await.is_ok() {
                                let header = FrameHeader::decode(&header).unwrap();
                                let mut payload = vec![0; header.length as usize];
                                if socket.read_exact(&mut payload).await.is_err() { break; }
                                let mut reply = talon_transport::response_header_ok(header.request_id, size as u32).to_vec();
                                reply.resize(HEADER_LEN + size, 0xAB);
                                if socket.write_all(&reply).await.is_err() { break; }
                            }
                        });
                    },
                    _ = tasks.join_next(), if !tasks.is_empty() => {},
                }
            }
        });
        for concurrency in [1, 64] {
            // Alternating order across repeats reduces one-sided warmup bias.
            for repeat in 0..3 {
                let mut backends = ["Tokio", "MonoioFacade", "MonoioNative"];
                if repeat % 2 == 1 {
                    backends.reverse();
                }
                for name in backends {
                    if name == "MonoioNative" {
                        let (seconds, mut latencies) =
                            bench_native(addr.parse().unwrap(), size, concurrency, samples).await;
                        latencies.sort_unstable();
                        println!(
                            "{name},{size},{concurrency},{},{:.0},{},{}",
                            latencies.len(),
                            latencies.len() as f64 / seconds,
                            latencies[latencies.len() / 2],
                            latencies[latencies.len() * 99 / 100]
                        );
                        continue;
                    }
                    let backend = if name == "Tokio" {
                        ClientIoBackend::Tokio
                    } else {
                        ClientIoBackend::IoUring
                    };
                    let pool = Arc::new(ConnectionPool::new().with_io_backend(backend));
                    let client = WorkerClient::with_pool(addr.clone(), pool);
                    let object = ObjectId::new(Backend::S3, "bench", "object");
                    let barrier = Arc::new(tokio::sync::Barrier::new(concurrency + 1));
                    let mut tasks = tokio::task::JoinSet::new();
                    for _ in 0..concurrency {
                        let client = client.clone();
                        let object = object.clone();
                        let barrier = barrier.clone();
                        tasks.spawn(async move {
                            for _ in 0..20 {
                                client.fetch_range(&object, 0, size as u64).await.unwrap();
                            }
                            tokio::time::timeout(
                                std::time::Duration::from_secs(10),
                                barrier.wait(),
                            )
                            .await
                            .unwrap();
                            let started = Instant::now();
                            let mut latencies = Vec::with_capacity(samples);
                            for _ in 0..samples {
                                let start = Instant::now();
                                let bytes =
                                    client.fetch_range(&object, 0, size as u64).await.unwrap();
                                latencies.push(start.elapsed().as_micros());
                                assert_eq!(bytes.len(), size);
                                assert!(bytes.iter().all(|b| *b == 0xAB));
                            }
                            (started, latencies)
                        });
                    }
                    tokio::time::timeout(std::time::Duration::from_secs(10), barrier.wait())
                        .await
                        .unwrap();
                    // A caller may run before this task resumes from the barrier.
                    // Start at the earliest caller timestamp, never after work began.
                    let mut start: Option<Instant> = None;
                    let mut latencies = Vec::new();
                    while let Some(result) = tasks.join_next().await {
                        let (started, part) = result.unwrap();
                        start = Some(start.map_or(started, |old| old.min(started)));
                        latencies.extend(part);
                    }
                    let qps = latencies.len() as f64 / start.unwrap().elapsed().as_secs_f64();
                    latencies.sort_unstable();
                    println!(
                        "{name},{size},{concurrency},{},{qps:.0},{},{}",
                        latencies.len(),
                        latencies[latencies.len() / 2],
                        latencies[latencies.len() * 99 / 100]
                    );
                }
            }
        }
        server.abort();
        let _ = server.await;
    }
}

#[cfg(target_os = "linux")]
async fn bench_native(
    addr: std::net::SocketAddr,
    size: usize,
    concurrency: usize,
    samples: usize,
) -> (f64, Vec<u128>) {
    let (done, result) = tokio::sync::oneshot::channel();
    let thread = std::thread::spawn(move || {
        let result = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .enable_timer()
            .build()
            .unwrap()
            .block_on(async {
                use futures::stream::{FuturesUnordered, StreamExt};
                // Match the public WorkerClient Arc pool API; this client stays on this ring.
                #[allow(clippy::arc_with_non_send_sync)]
                let client = WorkerClient::with_pool(
                    addr.to_string(),
                    Arc::new(talon_cache_client::monoio_client::MonoioClient::new()),
                );
                let barrier = std::rc::Rc::new(tokio::sync::Barrier::new(concurrency + 1));
                let mut calls = FuturesUnordered::new();
                for _ in 0..concurrency {
                    let client = client.clone();
                    let barrier = barrier.clone();
                    calls.push(monoio::spawn(async move {
                        let object = ObjectId::new(Backend::S3, "bench", "object");
                        for _ in 0..20 {
                            client.fetch_range(&object, 0, size as u64).await.unwrap();
                        }
                        monoio::time::timeout(std::time::Duration::from_secs(10), barrier.wait())
                            .await
                            .unwrap();
                        let started = Instant::now();
                        let mut latencies = Vec::with_capacity(samples);
                        for _ in 0..samples {
                            let start = Instant::now();
                            let bytes = client.fetch_range(&object, 0, size as u64).await.unwrap();
                            latencies.push(start.elapsed().as_micros());
                            assert_eq!(bytes.len(), size);
                            assert!(bytes.iter().all(|b| *b == 0xAB));
                        }
                        (started, latencies)
                    }));
                }
                monoio::time::timeout(std::time::Duration::from_secs(10), barrier.wait())
                    .await
                    .unwrap();
                let mut start: Option<Instant> = None;
                let mut latencies = Vec::new();
                while let Some((started, part)) = calls.next().await {
                    start = Some(start.map_or(started, |old| old.min(started)));
                    latencies.extend(part);
                }
                (start.unwrap().elapsed().as_secs_f64(), latencies)
            });
        let _ = done.send(result);
    });
    let result = result.await.unwrap();
    thread.join().unwrap();
    result
}
#[cfg(not(target_os = "linux"))]
async fn bench_native(_: std::net::SocketAddr, _: usize, _: usize, _: usize) -> (f64, Vec<u128>) {
    panic!("requires Linux io_uring")
}
