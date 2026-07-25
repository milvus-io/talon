//! Tokio vs io_uring data-plane comparison over real loopback TCP (#285).
//!
//! Both sides run the **production connection handlers** — `talon-worker`'s
//! Tokio `handle_conn` equivalent and [`talon_worker::uring_conn::handle_conn`]
//! — against the same warmed [`WorkerRuntime`], serving the same resident block
//! through the same `sendfile` path. The only variable is the runtime beneath.
//!
//! This is the gate for flipping the default (#285 step 5). The measurements in
//! #273 were taken in a standalone harness outside the repository; a claim that
//! cannot be re-run by a reviewer is not a basis for changing production
//! behaviour, so the comparison lives here instead.
//!
//! # Reading the results
//!
//! Divan reports wall-clock per iteration, where an iteration is one client
//! round-trip: connect, send a `RangeRequest`, read the response header, read
//! exactly `len` payload bytes. Concurrency is deliberately low — this measures
//! per-request latency on an idle server, not saturation throughput. The
//! throughput advantage of thread-per-core appears under many concurrent
//! connections, which a microbenchmark harness cannot model honestly; that
//! belongs in a load test.
//!
//! So: **treat these numbers as a latency floor comparison**, not as evidence
//! about scaling.
//!
//! # What this benchmark actually showed
//!
//! At concurrency 1, the two data planes are **statistically
//! indistinguishable**. Across seven runs on a 16-thread EPYC 7763, medians
//! landed between 218-295 us for both, with the sign of the difference flipping
//! run to run — sometimes uring ahead by 18%, sometimes behind by 8%. The
//! spread within a single implementation exceeded the gap between them.
//!
//! That is the expected result, not a disappointment. io_uring's advantage is
//! amortizing syscalls across many in-flight operations; with one connection
//! and one request in flight there is nothing to amortize, and the bulk bytes
//! bypass the ring entirely via `sendfile` either way. The standalone harness in
//! #273 measured its 35% win at **1024 concurrent connections**, where per-core
//! efficiency and tail latency diverge sharply.
//!
//! A Divan microbenchmark cannot model that honestly: it drives one client
//! serially, and a synthetic fan-out inside `bench()` would measure the
//! harness's own scheduling more than the server's. **Flipping the default
//! therefore needs a concurrent load test, not this file.** What this file does
//! give is a regression floor — if a change makes single-request latency
//! materially worse on either plane, it shows up here.

use std::path::PathBuf;
use std::sync::Arc;

use talon_core::{BackendStore, ObjectId, ObjectStat, Result, Version};
use talon_transport::data::{encode_request, RangeRequest};
use talon_transport::frame::{FrameHeader, HEADER_LEN};
use talon_worker::{BlockIndex, InFlightLoads, WholeBlockStore, WorkerMetrics, WorkerRuntime};

fn main() {
    divan::main();
}

/// 8 MiB block: large enough that the block file is real work to open and seek,
/// small enough to warm quickly in a bench harness.
const BLOCK_BYTES: u32 = 8 << 20;
/// The sub-range a client actually asks for.
const RANGE_LEN: u64 = 64 << 10;

struct RampBackend;

#[async_trait::async_trait]
impl BackendStore for RampBackend {
    async fn fetch_range(&self, _o: &ObjectId, offset: u64, len: u64) -> Result<bytes::Bytes> {
        Ok(bytes::Bytes::from(
            (0..len)
                .map(|i| ((offset + i) % 251) as u8)
                .collect::<Vec<u8>>(),
        ))
    }
    async fn head(&self, _o: &ObjectId) -> Result<ObjectStat> {
        Ok(ObjectStat {
            len: u64::MAX,
            version: Version::new("v1"),
        })
    }
}

fn tmp_root(tag: &str) -> PathBuf {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("talon-dp-bench-{tag}-{}-{n}", std::process::id()))
}

fn obj() -> ObjectId {
    ObjectId::new(talon_core::Backend::Azure, "c", "bench-obj")
}

fn request() -> RangeRequest {
    RangeRequest {
        object: obj(),
        offset: 0,
        len: RANGE_LEN,
    }
}

/// Build a runtime and commit one block so every measured serve is a resident
/// hit down the `sendfile` path.
async fn warm_runtime(root: &PathBuf) -> Arc<WorkerRuntime> {
    let runtime = Arc::new(WorkerRuntime::new(
        WholeBlockStore::open(root).unwrap(),
        Arc::new(BlockIndex::new()),
        Arc::new(InFlightLoads::new()),
        Arc::new(RampBackend) as Arc<dyn BackendStore>,
        BLOCK_BYTES,
        0,
        WorkerMetrics::new(1 << 30),
    ));
    let _ = runtime.serve(&request()).await.unwrap();
    runtime
}

/// One blocking client round-trip, so the same client drives both servers and
/// no client-side runtime difference contaminates the comparison.
fn client_fetch(addr: &str, encoded: &[u8]) -> usize {
    use std::io::{Read, Write};
    let mut sock = std::net::TcpStream::connect(addr).unwrap();
    sock.set_nodelay(true).ok();
    sock.write_all(encoded).unwrap();
    let mut hdr = [0u8; HEADER_LEN];
    sock.read_exact(&mut hdr).unwrap();
    let header = FrameHeader::decode(&hdr).unwrap();
    let mut body = vec![0u8; header.length as usize];
    sock.read_exact(&mut body).unwrap();
    body.len()
}

/// Serve on the Tokio data plane: sendfile requires taking the socket out of
/// the runtime and putting it back on every transfer.
fn spawn_tokio_server(runtime: Arc<WorkerRuntime>, rt: &tokio::runtime::Runtime) -> String {
    use talon_transport::data::response_header_ok;
    use talon_worker::runtime::ServeOutcome;
    use talon_worker::{send_file_range, DEFAULT_CHUNK};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (addr, listener) = rt.block_on(async {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        (l.local_addr().unwrap().to_string(), l)
    });
    rt.spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => return,
            };
            let runtime = Arc::clone(&runtime);
            tokio::spawn(async move {
                let mut hdr = [0u8; HEADER_LEN];
                if sock.read_exact(&mut hdr).await.is_err() {
                    return;
                }
                let header = FrameHeader::decode(&hdr).unwrap();
                let mut body = vec![0u8; header.length as usize];
                sock.read_exact(&mut body).await.unwrap();
                let mut full = hdr.to_vec();
                full.extend_from_slice(&body);
                let (_, req) = talon_transport::data::decode_request(&full).unwrap();

                match runtime.serve(&req).await.unwrap() {
                    ServeOutcome::Sendfile(handle) => {
                        let h = response_header_ok(0, handle.len as u32);
                        sock.write_all(&h).await.unwrap();
                        sock.flush().await.unwrap();
                        let std_sock = sock.into_std().unwrap();
                        std_sock.set_nonblocking(false).unwrap();
                        let _ = tokio::task::spawn_blocking(move || {
                            send_file_range(
                                &std_sock,
                                &handle.fd,
                                handle.offset,
                                handle.len,
                                DEFAULT_CHUNK,
                            )
                            .unwrap();
                            std_sock
                        })
                        .await
                        .unwrap();
                    }
                    ServeOutcome::Bytes(b) => {
                        let h = response_header_ok(0, b.len() as u32);
                        sock.write_all(&h).await.unwrap();
                        sock.write_all(&b).await.unwrap();
                        sock.flush().await.unwrap();
                    }
                }
            });
        }
    });
    addr
}

/// Serve on the io_uring data plane, via the production ring runtime and
/// connection handler.
fn spawn_uring_server(
    runtime: Arc<WorkerRuntime>,
    observability: Arc<talon_worker::observability::WorkerObservability>,
    handle: tokio::runtime::Handle,
) -> String {
    // Reserve a port then release it, so the rings can SO_REUSEPORT bind it.
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = probe.local_addr().unwrap().to_string();
    drop(probe);

    let h = talon_worker::uring_conn::RingConnHandler::new(runtime, observability, 128);
    let serve_addr = addr.clone();
    std::thread::spawn(move || {
        let _ = talon_worker::uring_serve::serve(serve_addr, 1, 4, h, handle);
    });

    // Wait for the ring to bind before the harness starts timing.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::net::TcpStream::connect(&addr).is_err() {
        assert!(std::time::Instant::now() < deadline, "ring never bound");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    addr
}

fn observability(
    index: Arc<BlockIndex>,
    inflight: Arc<InFlightLoads>,
) -> Arc<talon_worker::observability::WorkerObservability> {
    use talon_core::{NodeId, NodeInfo, NodeRole};
    let node = NodeInfo {
        id: NodeId::new("bench"),
        address: "127.0.0.1:7001".into(),
        role: NodeRole::Worker,
    };
    let obs = Arc::new(
        talon_worker::observability::WorkerObservability::new(
            "c".into(),
            node,
            "127.0.0.1:8001".into(),
            1024,
            index,
            inflight,
        )
        .unwrap(),
    );
    obs.readiness().set_backend_ready(true);
    obs.readiness().set_store_ready(true);
    obs.readiness().set_control_registered(true);
    obs
}

/// Deliver a 64 KiB sub-range of a resident block over the **Tokio** data plane.
#[divan::bench(sample_count = 50)]
fn dataplane_tokio(bencher: divan::Bencher) {
    let root = tmp_root("tokio");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let runtime = rt.block_on(warm_runtime(&root));
    let addr = spawn_tokio_server(runtime, &rt);
    let encoded = encode_request(0, &request()).unwrap();

    bencher.bench(|| {
        let n = client_fetch(&addr, &encoded);
        divan::black_box(n);
    });

    std::fs::remove_dir_all(&root).ok();
}

/// Deliver the same sub-range over the **io_uring** data plane.
#[divan::bench(sample_count = 50)]
fn dataplane_uring(bencher: divan::Bencher) {
    let root = tmp_root("uring");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let index = Arc::new(BlockIndex::new());
    let inflight = Arc::new(InFlightLoads::new());
    let runtime = rt.block_on(async {
        let r = Arc::new(WorkerRuntime::new(
            WholeBlockStore::open(&root).unwrap(),
            Arc::clone(&index),
            Arc::clone(&inflight),
            Arc::new(RampBackend) as Arc<dyn BackendStore>,
            BLOCK_BYTES,
            0,
            WorkerMetrics::new(1 << 30),
        ));
        let _ = r.serve(&request()).await.unwrap();
        r
    });
    let obs = observability(index, inflight);
    let addr = spawn_uring_server(runtime, obs, rt.handle().clone());
    let encoded = encode_request(0, &request()).unwrap();

    bencher.bench(|| {
        let n = client_fetch(&addr, &encoded);
        divan::black_box(n);
    });

    std::fs::remove_dir_all(&root).ok();
}
