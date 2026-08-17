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
    send_file_range, BlockIndex, InFlightLoads, PagedBlockStore, ServeOutcome, WholeBlockStore,
    WorkerMetrics, WorkerRuntime, DEFAULT_CHUNK,
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
                    ServeOutcome::SendfileMany(handles) => {
                        let total: u64 = handles.iter().map(|x| x.len).sum();
                        let hdr = response_header_ok(0, total as u32);
                        sock.write_all(&hdr).await.unwrap();
                        sock.flush().await.unwrap();
                        let std_sock = sock.into_std().unwrap();
                        std_sock.set_nonblocking(false).unwrap();
                        let std_sock = tokio::task::spawn_blocking(move || {
                            for x in &handles {
                                send_file_range(&std_sock, &x.fd, x.offset, x.len, DEFAULT_CHUNK)
                                    .unwrap();
                            }
                            std_sock
                        })
                        .await
                        .unwrap();
                        std_sock.set_nonblocking(true).unwrap();
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

// ---------------------------------------------------------------------------
// Cross-page paged-L2 reads.
//
// A paged worker stores each page in its own file, so a read spanning N pages
// has no single contiguous source. The byte path answered these by reading each
// page into a buffer and concatenating; the sendfile path answers them with one
// `sendfile` per page, keeping the payload out of userspace entirely.
//
// The pairs below hold the data, the request, and the page size identical and
// vary only the serve mechanism, so the delta is the copy elimination.
// ---------------------------------------------------------------------------

/// Page size for the paged benches: 1 MiB, the size the community reported
/// running in production.
const PAGE_BYTES: u32 = 1 << 20;
/// Block size for the paged benches. Kept modest so warming is quick while
/// still holding many pages.
const PAGED_BLOCK_BYTES: u32 = 16 << 20;

/// Build a paged runtime with `span_pages` worth of pages already resident.
async fn warm_paged_runtime(root: &std::path::Path, span: u64, l1: bool) -> Arc<WorkerRuntime> {
    let base = WorkerRuntime::new_with_l1(
        WholeBlockStore::open(root.join("whole")).unwrap(),
        Arc::new(BlockIndex::new()),
        Arc::new(InFlightLoads::new()),
        Arc::new(RampBackend) as Arc<dyn BackendStore>,
        PAGED_BLOCK_BYTES,
        0,
        if l1 { 256 << 20 } else { 0 },
        if l1 { u64::from(PAGE_BYTES) } else { 0 },
        WorkerMetrics::new(1 << 30),
    )
    .with_paged_store(PagedBlockStore::open(root.join("paged"), PAGE_BYTES).unwrap());
    let runtime = Arc::new(base);
    // Warm every page the benchmark range will touch.
    let warm = RangeRequest {
        object: obj(),
        offset: 0,
        len: span,
    };
    let _ = runtime.serve(&warm).await.unwrap();
    runtime
}

/// Drive one paged bench over an explicit `[offset, offset + len)` window,
/// byte path vs sendfile path.
///
/// Kept separate from page counts because the interesting regime is not only
/// "how many pages" but *how much of them* is asked for: a 4 KiB read sitting
/// on a page boundary touches two pages while copying almost nothing, so the
/// per-page `openat` + `sendfile` syscalls are no longer amortised the way
/// they are for a multi-megabyte span.
fn run_paged_window(
    bencher: divan::Bencher,
    tag: &str,
    offset: u64,
    len: u64,
    zero_copy: bool,
    l1: bool,
) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let root = tmp_root(tag);
    // Warm whole pages covering the window so the serve path is a pure cache
    // hit; otherwise the first iteration would measure a backend fetch.
    let page = u64::from(PAGE_BYTES);
    let warm_span = (offset + len).div_ceil(page) * page;
    let (addr, req) = rt.block_on(async {
        let runtime = warm_paged_runtime(&root, warm_span, l1).await;
        let addr = if zero_copy {
            spawn_new_server(runtime).await
        } else {
            spawn_bytes_server(runtime).await
        };
        let req = RangeRequest {
            object: obj(),
            offset,
            len,
        };
        (addr, req)
    });
    bencher.bench(|| {
        let n = rt.block_on(client_fetch(&addr, &req));
        assert_eq!(n, len as usize);
    });
    std::fs::remove_dir_all(&root).ok();
}

/// Drive one cross-page bench: `pages` pages, byte path vs sendfile path.
fn run_cross_page(bencher: divan::Bencher, tag: &str, pages: u64, zero_copy: bool, l1: bool) {
    let span = pages * u64::from(PAGE_BYTES);
    run_paged_window(bencher, tag, 0, span, zero_copy, l1);
}

#[divan::bench]
fn cross_page_2_bytes(b: divan::Bencher) {
    run_cross_page(b, "xp2-bytes", 2, false, false);
}
#[divan::bench]
fn cross_page_2_sendfile(b: divan::Bencher) {
    run_cross_page(b, "xp2-sendfile", 2, true, false);
}
#[divan::bench]
fn cross_page_4_bytes(b: divan::Bencher) {
    run_cross_page(b, "xp4-bytes", 4, false, false);
}
#[divan::bench]
fn cross_page_4_sendfile(b: divan::Bencher) {
    run_cross_page(b, "xp4-sendfile", 4, true, false);
}
#[divan::bench]
fn cross_page_8_bytes(b: divan::Bencher) {
    run_cross_page(b, "xp8-bytes", 8, false, false);
}
#[divan::bench]
fn cross_page_8_sendfile(b: divan::Bencher) {
    run_cross_page(b, "xp8-sendfile", 8, true, false);
}
/// With L1 enabled the fast path used to be disabled outright, so this pair
/// measures the case that previously had no zero-copy option at all.
#[divan::bench]
fn cross_page_4_l1_bytes(b: divan::Bencher) {
    run_cross_page(b, "xp4-l1-bytes", 4, false, true);
}
#[divan::bench]
fn cross_page_4_l1_sendfile(b: divan::Bencher) {
    run_cross_page(b, "xp4-l1-sendfile", 4, true, true);
}

// --- Small reads straddling a page boundary -------------------------------
//
// The community's actual workload is not a multi-megabyte scan: Parquet row
// groups are ~1 MiB but their boundaries do not line up with cache pages, so
// "quite a lot of IO ends up crossing a page" — each such IO being small. This
// is the regime where multi-sendfile is *least* obviously a win, since the copy
// it removes shrinks with the read while the extra `openat` + `sendfile` per
// page stays fixed. Every bench below centres the window on the boundary
// between page 0 and page 1, so exactly two pages are touched.

/// A window of `len` bytes centred on the page-0/page-1 boundary.
fn straddle_offset(len: u64) -> u64 {
    u64::from(PAGE_BYTES) - len / 2
}

#[divan::bench]
fn straddle_4k_bytes(b: divan::Bencher) {
    run_paged_window(
        b,
        "sd4k-bytes",
        straddle_offset(4 << 10),
        4 << 10,
        false,
        false,
    );
}
#[divan::bench]
fn straddle_4k_sendfile(b: divan::Bencher) {
    run_paged_window(
        b,
        "sd4k-sendfile",
        straddle_offset(4 << 10),
        4 << 10,
        true,
        false,
    );
}
#[divan::bench]
fn straddle_16k_bytes(b: divan::Bencher) {
    run_paged_window(
        b,
        "sd16k-bytes",
        straddle_offset(16 << 10),
        16 << 10,
        false,
        false,
    );
}
#[divan::bench]
fn straddle_16k_sendfile(b: divan::Bencher) {
    run_paged_window(
        b,
        "sd16k-sendfile",
        straddle_offset(16 << 10),
        16 << 10,
        true,
        false,
    );
}
#[divan::bench]
fn straddle_64k_bytes(b: divan::Bencher) {
    run_paged_window(
        b,
        "sd64k-bytes",
        straddle_offset(64 << 10),
        64 << 10,
        false,
        false,
    );
}
#[divan::bench]
fn straddle_64k_sendfile(b: divan::Bencher) {
    run_paged_window(
        b,
        "sd64k-sendfile",
        straddle_offset(64 << 10),
        64 << 10,
        true,
        false,
    );
}
#[divan::bench]
fn straddle_256k_bytes(b: divan::Bencher) {
    run_paged_window(
        b,
        "sd256k-bytes",
        straddle_offset(256 << 10),
        256 << 10,
        false,
        false,
    );
}
#[divan::bench]
fn straddle_256k_sendfile(b: divan::Bencher) {
    run_paged_window(
        b,
        "sd256k-sendfile",
        straddle_offset(256 << 10),
        256 << 10,
        true,
        false,
    );
}

/// Control: the same 64 KiB read placed *inside* one page. The delta against
/// `straddle_64k_*` isolates what crossing the boundary actually costs on each
/// path.
#[divan::bench]
fn in_page_64k_bytes(b: divan::Bencher) {
    run_paged_window(b, "ip64k-bytes", 4 << 10, 64 << 10, false, false);
}
#[divan::bench]
fn in_page_64k_sendfile(b: divan::Bencher) {
    run_paged_window(b, "ip64k-sendfile", 4 << 10, 64 << 10, true, false);
}

/// A small straddling read with L1 enabled — the configuration that previously
/// had no zero-copy path at all.
#[divan::bench]
fn straddle_64k_l1_bytes(b: divan::Bencher) {
    run_paged_window(
        b,
        "sd64k-l1-bytes",
        straddle_offset(64 << 10),
        64 << 10,
        false,
        true,
    );
}
#[divan::bench]
fn straddle_64k_l1_sendfile(b: divan::Bencher) {
    run_paged_window(
        b,
        "sd64k-l1-sendfile",
        straddle_offset(64 << 10),
        64 << 10,
        true,
        true,
    );
}

/// L1-resident straddling reads across sizes, to locate the size at which
/// zero-copy overtakes serving the bytes straight out of DRAM.
#[divan::bench]
fn straddle_256k_l1_bytes(b: divan::Bencher) {
    run_paged_window(
        b,
        "sd256k-l1-bytes",
        straddle_offset(256 << 10),
        256 << 10,
        false,
        true,
    );
}
#[divan::bench]
fn straddle_256k_l1_sendfile(b: divan::Bencher) {
    run_paged_window(
        b,
        "sd256k-l1-sendfile",
        straddle_offset(256 << 10),
        256 << 10,
        true,
        true,
    );
}
#[divan::bench]
fn straddle_1m_l1_bytes(b: divan::Bencher) {
    run_paged_window(
        b,
        "sd1m-l1-bytes",
        straddle_offset(1 << 20),
        1 << 20,
        false,
        true,
    );
}
#[divan::bench]
fn straddle_1m_l1_sendfile(b: divan::Bencher) {
    run_paged_window(
        b,
        "sd1m-l1-sendfile",
        straddle_offset(1 << 20),
        1 << 20,
        true,
        true,
    );
}

/// Very small L1-resident straddling reads: the regime where the per-page
/// syscall cost is least amortised, and so the last place a zero-copy
/// regression could hide.
#[divan::bench]
fn straddle_4k_l1_bytes(b: divan::Bencher) {
    run_paged_window(
        b,
        "sd4k-l1-bytes",
        straddle_offset(4 << 10),
        4 << 10,
        false,
        true,
    );
}
#[divan::bench]
fn straddle_4k_l1_sendfile(b: divan::Bencher) {
    run_paged_window(
        b,
        "sd4k-l1-sendfile",
        straddle_offset(4 << 10),
        4 << 10,
        true,
        true,
    );
}
#[divan::bench]
fn straddle_16k_l1_bytes(b: divan::Bencher) {
    run_paged_window(
        b,
        "sd16k-l1-bytes",
        straddle_offset(16 << 10),
        16 << 10,
        false,
        true,
    );
}
#[divan::bench]
fn straddle_16k_l1_sendfile(b: divan::Bencher) {
    run_paged_window(
        b,
        "sd16k-l1-sendfile",
        straddle_offset(16 << 10),
        16 << 10,
        true,
        true,
    );
}

// --- Partially L1-resident spans ------------------------------------------
//
// L1 is far smaller than L2, so in steady state a span is routinely part in
// DRAM and part only on disk. Zero-copy must decline there (serving the whole
// span with sendfile would leave the evicted pages out of inclusive L1
// forever), which means these reads pay the byte path plus a re-admission.
// This measures what that fallback actually costs relative to a fully resident
// span and a fully L1-cold one.

/// Warm a paged runtime over `span`, then drop every `nth` covered page from
/// L1 so the span is partially resident while L2 still holds all of it.
fn run_partial_l1(bencher: divan::Bencher, tag: &str, span: u64, drop_every: u32) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let root = tmp_root(tag);
    let (runtime, addr, req) = rt.block_on(async {
        let runtime = warm_paged_runtime(&root, span, true).await;
        let addr = spawn_new_server(Arc::clone(&runtime)).await;
        let req = RangeRequest {
            object: obj(),
            offset: 0,
            len: span,
        };
        (runtime, addr, req)
    });
    let block = runtime
        .block_for_bench(&obj(), 0)
        .expect("warmed runtime must have a cached version");
    let pages = (span / u64::from(PAGE_BYTES)) as u32;
    bencher.bench(|| {
        if drop_every > 0 {
            // Re-create the partial state each iteration: the previous read
            // re-admitted the pages it was missing.
            for p in (0..pages).step_by(drop_every as usize) {
                runtime.l1_drop_page_for_test(&block, talon_core::PageIndex(p));
            }
        }
        let n = rt.block_on(client_fetch(&addr, &req));
        assert_eq!(n, span as usize);
    });
    std::fs::remove_dir_all(&root).ok();
}

/// Baseline: every page L1-resident, so the read goes zero-copy.
#[divan::bench]
fn partial_l1_4p_all_resident(b: divan::Bencher) {
    run_partial_l1(b, "pl1-all", 4 * u64::from(PAGE_BYTES), 0);
}
/// One page in four missing from L1 — enough to disqualify the whole span.
#[divan::bench]
fn partial_l1_4p_one_missing(b: divan::Bencher) {
    run_partial_l1(b, "pl1-one", 4 * u64::from(PAGE_BYTES), 4);
}
/// Every other page missing.
#[divan::bench]
fn partial_l1_4p_half_missing(b: divan::Bencher) {
    run_partial_l1(b, "pl1-half", 4 * u64::from(PAGE_BYTES), 2);
}
/// Every page missing from L1 (still all present in L2).
#[divan::bench]
fn partial_l1_4p_none_resident(b: divan::Bencher) {
    run_partial_l1(b, "pl1-none", 4 * u64::from(PAGE_BYTES), 1);
}
