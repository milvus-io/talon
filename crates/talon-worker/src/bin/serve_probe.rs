//! Serve-path probe: syscall counts, memory, and concurrency for paged reads.
//!
//! `serve_path_benches` measures wall-clock latency, which answers "is it
//! faster" but not *why*, and not what it costs. This binary answers the three
//! questions a reviewer should ask about replacing a userspace copy with N
//! `sendfile` calls:
//!
//! - **Syscalls**: zero-copy trades one `pread` + memcpy per page for one
//!   `sendfile` per page. Under `strace -c -f` the two paths' syscall profiles
//!   are directly comparable, and the fd cache's effect on `openat` is visible
//!   rather than inferred.
//! - **Memory**: the byte path allocates the full response before writing it,
//!   so peak RSS scales with `span x concurrency`. Zero-copy allocates nothing
//!   per byte. This reports RSS delta across a fixed load.
//! - **Concurrency**: per-request latency on an idle server says nothing about
//!   a saturated one. The copy path burns memory bandwidth that does not scale
//!   with cores; `sendfile` does not.
//!
//! This is a manual tool for a known machine, not a CI gate — absolute times on
//! a shared runner are noise. It is checked in so the numbers in the PR can be
//! re-run by a reviewer rather than taken on faith.
//!
//! # Usage
//!
//! ```sh
//! # Concurrency sweep + RSS, both paths:
//! talon-serve-probe --mode concurrency --conns 1,16,64,256
//!
//! # Syscall profile of exactly N reads on one path (run under strace):
//! strace -c -f -- talon-serve-probe --mode syscalls --path sendfile --reads 200
//! strace -c -f -- talon-serve-probe --mode syscalls --path bytes    --reads 200
//! ```
//!
//! In `syscalls` mode the process warms the cache, then prints a `READY`
//! marker, runs exactly `--reads` requests, and exits. Counting the whole
//! process includes warm-up, so compare two runs at different `--reads` values
//! and take the difference — that isolates the per-read cost from the fixed
//! setup, which is what the tables in the PR report.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use talon_core::{Backend, BackendStore, BlockHandle, ObjectId, ObjectStat, Result, Version};
use talon_transport::data::{encode_request, response_header_ok, RangeRequest};
use talon_transport::frame::{FrameHeader, HEADER_LEN};
use talon_worker::{
    send_file_range, BlockIndex, InFlightLoads, PagedBlockStore, ServeOutcome, WholeBlockStore,
    WorkerMetrics, WorkerRuntime, DEFAULT_CHUNK,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// 1 MiB pages, the size the community reported running in production.
const PAGE_BYTES: u32 = 1 << 20;
const BLOCK_BYTES: u32 = 16 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathKind {
    Bytes,
    Sendfile,
}

impl std::str::FromStr for PathKind {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "bytes" => Ok(Self::Bytes),
            "sendfile" => Ok(Self::Sendfile),
            other => Err(format!("unknown path {other}, expected bytes|sendfile")),
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "talon-serve-probe")]
struct Args {
    /// `concurrency` sweeps connection counts; `syscalls` runs a fixed number
    /// of reads for tracing under `strace -c`.
    #[arg(long, default_value = "concurrency")]
    mode: String,
    /// Which serve path to drive. In concurrency mode, omit to run both.
    #[arg(long)]
    path: Option<PathKind>,
    /// Connection counts to sweep (concurrency mode).
    #[arg(long, default_value = "1,16,64,256", value_delimiter = ',')]
    conns: Vec<usize>,
    /// Seconds of closed-loop load per connection count.
    #[arg(long, default_value_t = 3)]
    secs: u64,
    /// Exact number of sequential reads (syscalls mode).
    #[arg(long, default_value_t = 200)]
    reads: usize,
    /// Bytes per request. The default straddles a page boundary.
    #[arg(long, default_value_t = 64 << 10)]
    len: u64,
    /// Request offset. Defaults to centring the window on a page boundary.
    #[arg(long)]
    offset: Option<u64>,
    /// Enable L1 (DRAM page cache) with this capacity in MiB. 0 disables it.
    #[arg(long, default_value_t = 0)]
    l1_mib: u64,
}

/// Deterministic bytes so a block can be committed without a real backend.
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

fn obj() -> ObjectId {
    ObjectId::new(Backend::Azure, "c", "probe-obj")
}

/// Resident-set size in KiB, read from `/proc/self/status`.
///
/// `VmRSS` rather than an allocator statistic: the byte path's cost is real
/// pages touched, and a Rust allocator counter would miss what the kernel
/// actually had to back.
fn rss_kib() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            if let Some(kb) = rest.split_whitespace().next() {
                return kb.parse().unwrap_or(0);
            }
        }
    }
    0
}

async fn warm_runtime(root: &std::path::Path, span_end: u64, l1_mib: u64) -> Arc<WorkerRuntime> {
    let l1_bytes = l1_mib * (1 << 20);
    let base = WorkerRuntime::new_with_l1(
        WholeBlockStore::open(root.join("whole")).unwrap(),
        Arc::new(BlockIndex::new()),
        Arc::new(InFlightLoads::new()),
        Arc::new(RampBackend) as Arc<dyn BackendStore>,
        BLOCK_BYTES,
        0,
        l1_bytes,
        if l1_bytes > 0 {
            u64::from(PAGE_BYTES)
        } else {
            0
        },
        WorkerMetrics::new(1 << 30),
    )
    .with_paged_store(PagedBlockStore::open(root.join("paged"), PAGE_BYTES).unwrap());
    let runtime = Arc::new(base);
    // Warm every page the load will touch, so measurements see cache hits only.
    let page = u64::from(PAGE_BYTES);
    let warm_span = span_end.div_ceil(page) * page;
    let warm = RangeRequest {
        object: obj(),
        offset: 0,
        len: warm_span,
    };
    let _ = runtime.serve(&warm).await.unwrap();
    runtime
}

async fn read_request(sock: &mut TcpStream) -> Option<RangeRequest> {
    let mut hdr = [0_u8; HEADER_LEN];
    sock.read_exact(&mut hdr).await.ok()?;
    let header = FrameHeader::decode(&hdr).ok()?;
    let mut body = vec![0_u8; header.length as usize];
    sock.read_exact(&mut body).await.ok()?;
    let mut full = hdr.to_vec();
    full.extend_from_slice(&body);
    talon_transport::data::decode_request(&full)
        .ok()
        .map(|(_, r)| r)
}

/// Write one segment list with `sendfile`, mirroring the production Tokio path.
async fn write_sendfile(sock: TcpStream, handles: Vec<BlockHandle>) {
    let total: u64 = handles.iter().map(|h| h.len).sum();
    let mut sock = sock;
    let hdr = response_header_ok(0, total as u32);
    sock.write_all(&hdr).await.unwrap();
    sock.flush().await.unwrap();
    let std_sock = sock.into_std().unwrap();
    std_sock.set_nonblocking(false).unwrap();
    let std_sock = tokio::task::spawn_blocking(move || {
        for h in &handles {
            send_file_range(&std_sock, &h.fd, h.offset, h.len, DEFAULT_CHUNK).unwrap();
        }
        std_sock
    })
    .await
    .unwrap();
    std_sock.set_nonblocking(true).unwrap();
    drop(TcpStream::from_std(std_sock).unwrap());
}

/// Serve connections until the listener closes, on the requested path.
///
/// Both arms run the real `WorkerRuntime::serve`; the `bytes` arm forces the
/// userspace path via `serve_range` so the comparison isolates the transport,
/// not the cache lookup.
fn spawn_server(runtime: Arc<WorkerRuntime>, path: PathKind, served: Arc<AtomicU64>) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    listener.set_nonblocking(true).unwrap();
    let listener = TcpListener::from_std(listener).unwrap();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => return,
            };
            let runtime = Arc::clone(&runtime);
            let served = Arc::clone(&served);
            tokio::spawn(async move {
                // Keep serving on this connection until the client hangs up, so
                // the concurrency sweep measures steady-state serving rather
                // than repeated accept cost.
                loop {
                    let Some(req) = read_request(&mut sock).await else {
                        return;
                    };
                    match path {
                        PathKind::Bytes => {
                            let bytes = runtime.serve_range(&req).await.unwrap();
                            let hdr = response_header_ok(0, bytes.len() as u32);
                            if sock.write_all(&hdr).await.is_err()
                                || sock.write_all(&bytes).await.is_err()
                            {
                                return;
                            }
                            sock.flush().await.ok();
                            served.fetch_add(1, Ordering::Relaxed);
                        }
                        PathKind::Sendfile => {
                            let handles = match runtime.serve(&req).await.unwrap() {
                                ServeOutcome::Sendfile(h) => vec![h],
                                ServeOutcome::SendfileMany(hs) => hs,
                                ServeOutcome::Bytes(_) => panic!(
                                    "sendfile mode served bytes: the request did not qualify \
                                     for the zero-copy path"
                                ),
                            };
                            // sendfile consumes the socket, so this connection
                            // serves one request. Accounted for by the client
                            // reconnecting.
                            served.fetch_add(1, Ordering::Relaxed);
                            write_sendfile(sock, handles).await;
                            return;
                        }
                    }
                }
            });
        }
    });
    addr
}

/// One request/response round-trip on an existing connection.
async fn fetch_on(sock: &mut TcpStream, req: &RangeRequest, expect: u64) {
    let frame = encode_request(0, req).unwrap();
    sock.write_all(&frame).await.unwrap();
    sock.flush().await.unwrap();
    let mut hdr = [0_u8; HEADER_LEN];
    sock.read_exact(&mut hdr).await.unwrap();
    let header = FrameHeader::decode(&hdr).unwrap();
    let mut body = vec![0_u8; header.length as usize];
    sock.read_exact(&mut body).await.unwrap();
    // The response frame carries a small status prefix ahead of the payload.
    assert!(
        body.len() as u64 >= expect,
        "short response: {} < {expect}",
        body.len()
    );
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let offset = args
        .offset
        .unwrap_or_else(|| u64::from(PAGE_BYTES) - args.len / 2);
    let root = std::env::temp_dir().join(format!("talon-probe-{}", std::process::id()));
    let runtime = warm_runtime(&root, offset + args.len, args.l1_mib).await;
    let req = RangeRequest {
        object: obj(),
        offset,
        len: args.len,
    };

    let pages_touched = {
        let page = u64::from(PAGE_BYTES);
        (offset + args.len - 1) / page - offset / page + 1
    };
    eprintln!(
        "window: offset={offset} len={} -> {pages_touched} pages, l1={} MiB",
        args.len, args.l1_mib
    );

    match args.mode.as_str() {
        "syscalls" => {
            let path = args.path.expect("--path is required in syscalls mode");
            let served = Arc::new(AtomicU64::new(0));
            let addr = spawn_server(Arc::clone(&runtime), path, Arc::clone(&served));
            // Everything above is warm-up; the trace should be read as the
            // difference between two --reads values.
            eprintln!("READY");
            for _ in 0..args.reads {
                let mut sock = TcpStream::connect(&addr).await.unwrap();
                fetch_on(&mut sock, &req, args.len).await;
            }
            eprintln!("done: {} reads on {:?}", args.reads, path);
        }
        "concurrency" => {
            let paths = match args.path {
                Some(p) => vec![p],
                None => vec![PathKind::Bytes, PathKind::Sendfile],
            };
            println!(
                "{:<10} {:>6} {:>12} {:>12} {:>10} {:>10}",
                "path", "conns", "reqs/s", "MiB/s", "p50 us", "rss MiB"
            );
            for path in paths {
                for &conns in &args.conns {
                    let served = Arc::new(AtomicU64::new(0));
                    let addr = spawn_server(Arc::clone(&runtime), path, Arc::clone(&served));
                    // Settle before sampling RSS so the baseline excludes
                    // connection setup.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    let rss_before = rss_kib();

                    let deadline = Instant::now() + Duration::from_secs(args.secs);
                    let latencies = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
                    let mut tasks = Vec::new();
                    for _ in 0..conns {
                        let addr = addr.clone();
                        let req = req.clone();
                        let latencies = Arc::clone(&latencies);
                        tasks.push(tokio::spawn(async move {
                            let mut local = Vec::new();
                            while Instant::now() < deadline {
                                let start = Instant::now();
                                let mut sock = match TcpStream::connect(&addr).await {
                                    Ok(s) => s,
                                    Err(_) => break,
                                };
                                fetch_on(&mut sock, &req, req.len).await;
                                local.push(start.elapsed().as_micros() as u64);
                            }
                            latencies.lock().unwrap().extend(local);
                        }));
                    }
                    let rss_peak = {
                        // Sample RSS while the load is running; the byte path's
                        // allocation is transient and invisible afterwards.
                        let mut peak = rss_before;
                        while Instant::now() < deadline {
                            peak = peak.max(rss_kib());
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                        peak
                    };
                    for t in tasks {
                        let _ = t.await;
                    }

                    let mut lat = latencies.lock().unwrap().clone();
                    lat.sort_unstable();
                    let n = lat.len() as u64;
                    let p50 = lat.get(lat.len() / 2).copied().unwrap_or(0);
                    let secs = args.secs as f64;
                    let rps = n as f64 / secs;
                    let mib = (n as f64 * args.len as f64) / secs / (1024.0 * 1024.0);
                    println!(
                        "{:<10} {:>6} {:>12.0} {:>12.1} {:>10} {:>10.1}",
                        format!("{path:?}").to_lowercase(),
                        conns,
                        rps,
                        mib,
                        p50,
                        (rss_peak - rss_before.min(rss_peak)) as f64 / 1024.0
                    );
                }
            }
        }
        other => panic!("unknown mode {other}, expected concurrency|syscalls"),
    }

    std::fs::remove_dir_all(&root).ok();
}
