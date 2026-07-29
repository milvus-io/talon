//! Real-socket serve-path benchmarks quantifying the read-path refinements.
//!
//! Unlike the FUSE `read_path_benches` (pure-CPU read planning), these drive
//! **real loopback TCP** through the worker's serve logic, so they measure the
//! actual wins from the epic:
//!
//! - `serve_userspace_range_read`, `serve_l1_page`, and `serve_sendfile`:
//!   delivering a small sub-range of a large resident block through L2
//!   userspace I/O, the fine-grained L1 page cache, or zero-copy L2 sendfile.
//! - `fetch_unpooled` vs `fetch_pooled`: N sequential small round-trips to a
//!   worker, dialing a fresh TCP connection each time vs reusing one pooled
//!   connection — the handshake cost the client pool removes (#181).
//!
//! Both run without `/dev/fuse` (no kernel mount needed); the delta between the
//! paired benches is the refinement's payoff.

use std::path::PathBuf;
use std::sync::Arc;

use talon_core::{Backend, BackendStore, ObjectId, ObjectStat, Result, Version};
use talon_transport::data::{encode_request, response_header_ok, RangeRequest};
use talon_transport::frame::{FrameHeader, HEADER_LEN};
use talon_worker::{
    send_file_range, BlockIndex, InFlightLoads, ServeOutcome, WholeBlockStore, WorkerMetrics,
    WorkerRuntime, DEFAULT_CHUNK,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn main() {
    divan::main();
}

/// Block size for the serve benches: 32 MiB. Large enough that "read the whole
/// block" (old path) is dramatically more work than "sendfile a 64 KiB window"
/// (new path), while keeping each iteration fast.
const BLOCK_BYTES: u32 = 32 << 20;
/// The small sub-range a client actually asks for.
const RANGE_LEN: u64 = 64 << 10; // 64 KiB

/// A backend that serves deterministic bytes so a block can be committed once.
struct RampBackend;

#[async_trait::async_trait]
impl BackendStore for RampBackend {
    async fn fetch_range(&self, _obj: &ObjectId, offset: u64, len: u64) -> Result<bytes::Bytes> {
        Ok(bytes::Bytes::from(
            (0..len)
                .map(|i| ((offset + i) % 251) as u8)
                .collect::<Vec<u8>>(),
        ))
    }
    async fn head(&self, _obj: &ObjectId) -> Result<ObjectStat> {
        Ok(ObjectStat {
            len: u64::MAX,
            version: Version::new("v1"),
        })
    }
}

fn tmp_root(tag: &str) -> PathBuf {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    std::time::SystemTime::now().hash(&mut h);
    tag.hash(&mut h);
    std::env::temp_dir().join(format!(
        "talon-serve-bench-{}-{}",
        std::process::id(),
        h.finish()
    ))
}

fn obj() -> ObjectId {
    ObjectId::new(Backend::Azure, "c", "bench-obj")
}

/// Build a runtime with one committed block resident on disk, ready to serve.
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
    // One serve to fetch + commit the block so subsequent serves are resident hits.
    let warm = RangeRequest {
        object: obj(),
        offset: 0,
        len: RANGE_LEN,
    };
    let _ = runtime.serve(&warm).await.unwrap();
    runtime
}

async fn warm_l1_runtime(root: &PathBuf) -> Arc<WorkerRuntime> {
    let runtime = Arc::new(WorkerRuntime::new_with_l1(
        WholeBlockStore::open(root).unwrap(),
        Arc::new(BlockIndex::new()),
        Arc::new(InFlightLoads::new()),
        Arc::new(RampBackend) as Arc<dyn BackendStore>,
        BLOCK_BYTES,
        0,
        8 << 20,
        256 << 10,
        WorkerMetrics::new(1 << 30),
    ));
    let warm = RangeRequest {
        object: obj(),
        offset: 0,
        len: RANGE_LEN,
    };
    let _ = runtime.serve(&warm).await.unwrap();
    runtime
}

/// Spawn a server that serves a request through the userspace byte path.
async fn spawn_bytes_server(runtime: Arc<WorkerRuntime>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => return,
            };
            let runtime = Arc::clone(&runtime);
            tokio::spawn(async move {
                let req = match read_request(&mut sock).await {
                    Some(r) => r,
                    None => return,
                };
                let bytes = runtime.serve_range(&req).await.unwrap();
                let hdr = response_header_ok(0, bytes.len() as u32);
                sock.write_all(&hdr).await.unwrap();
                sock.write_all(&bytes).await.unwrap();
                sock.flush().await.unwrap();
            });
        }
    });
    addr
}

/// Spawn a server that serves with the NEW path: `serve` yields a Sendfile
/// handle for a resident hit; we write the header async then stream the fd with
/// `send_file_range` on the blocking pool.
async fn spawn_new_server(runtime: Arc<WorkerRuntime>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => return,
            };
            let runtime = Arc::clone(&runtime);
            tokio::spawn(async move {
                let req = match read_request(&mut sock).await {
                    Some(r) => r,
                    None => return,
                };
                match runtime.serve(&req).await.unwrap() {
                    ServeOutcome::Sendfile(handle) => {
                        let hdr = response_header_ok(0, handle.len as u32);
                        sock.write_all(&hdr).await.unwrap();
                        sock.flush().await.unwrap();
                        let std_sock = sock.into_std().unwrap();
                        std_sock.set_nonblocking(false).unwrap();
                        let std_sock = tokio::task::spawn_blocking(move || {
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
                        std_sock.set_nonblocking(true).unwrap();
                        // drop restores/closes the socket after the client read.
                    }
                    ServeOutcome::Bytes(bytes) => {
                        let hdr = response_header_ok(0, bytes.len() as u32);
                        sock.write_all(&hdr).await.unwrap();
                        sock.write_all(&bytes).await.unwrap();
                        sock.flush().await.unwrap();
                    }
                }
            });
        }
    });
    addr
}

/// Read one framed RangeRequest from a socket.
async fn read_request(sock: &mut TcpStream) -> Option<RangeRequest> {
    let mut hdr = [0u8; HEADER_LEN];
    sock.read_exact(&mut hdr).await.ok()?;
    let header = FrameHeader::decode(&hdr).ok()?;
    let mut body = vec![0u8; header.length as usize];
    sock.read_exact(&mut body).await.ok()?;
    let mut full = hdr.to_vec();
    full.extend_from_slice(&body);
    talon_transport::data::decode_request(&full)
        .ok()
        .map(|(_, r)| r)
}

/// One client round-trip: send the range request, read header + exactly `len`
/// bytes back. Returns the payload length.
async fn client_fetch(addr: &str, req: &RangeRequest) -> usize {
    let mut sock = TcpStream::connect(addr).await.unwrap();
    let out = encode_request(0, req).unwrap();
    sock.write_all(&out).await.unwrap();
    sock.flush().await.unwrap();
    let mut hdr = [0u8; HEADER_LEN];
    sock.read_exact(&mut hdr).await.unwrap();
    let header = FrameHeader::decode(&hdr).unwrap();
    let mut body = vec![0u8; header.length as usize];
    sock.read_exact(&mut body).await.unwrap();
    body.len()
}

/// Deliver a 64 KiB sub-range with a block-relative userspace L2 read.
#[divan::bench]
fn serve_userspace_range_read(bencher: divan::Bencher) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let root = tmp_root("old");
    let (addr, req) = rt.block_on(async {
        let runtime = warm_runtime(&root).await;
        let addr = spawn_bytes_server(runtime).await;
        let req = RangeRequest {
            object: obj(),
            offset: 4096,
            len: RANGE_LEN,
        };
        (addr, req)
    });
    bencher.bench(|| {
        let n = rt.block_on(client_fetch(&addr, &req));
        assert_eq!(n, RANGE_LEN as usize);
    });
    std::fs::remove_dir_all(&root).ok();
}

/// Deliver the same range from one resident 256 KiB L1 page.
#[divan::bench]
fn serve_l1_page(bencher: divan::Bencher) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let root = tmp_root("l1-page");
    let (addr, req) = rt.block_on(async {
        let runtime = warm_l1_runtime(&root).await;
        let addr = spawn_bytes_server(runtime).await;
        let req = RangeRequest {
            object: obj(),
            offset: 4096,
            len: RANGE_LEN,
        };
        (addr, req)
    });
    bencher.bench(|| {
        let n = rt.block_on(client_fetch(&addr, &req));
        assert_eq!(n, RANGE_LEN as usize);
    });
    std::fs::remove_dir_all(&root).ok();
}

/// Deliver the same 64 KiB sub-range via the NEW path (sendfile the exact window
/// from the block file's fd), over real loopback TCP.
#[divan::bench]
fn serve_sendfile(bencher: divan::Bencher) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let root = tmp_root("new");
    let (addr, req) = rt.block_on(async {
        let runtime = warm_runtime(&root).await;
        let addr = spawn_new_server(runtime).await;
        let req = RangeRequest {
            object: obj(),
            offset: 4096,
            len: RANGE_LEN,
        };
        (addr, req)
    });
    bencher.bench(|| {
        let n = rt.block_on(client_fetch(&addr, &req));
        assert_eq!(n, RANGE_LEN as usize);
    });
    std::fs::remove_dir_all(&root).ok();
}
