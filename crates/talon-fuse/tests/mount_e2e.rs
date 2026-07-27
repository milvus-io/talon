//! Real-kernel FUSE mount smoke test (opt-in).
//!
//! Unlike `read_path_e2e.rs`, which drives the read path in-process with mock
//! TCP servers and never touches the kernel, this test **mounts a real
//! `TalonFuse` on `/dev/fuse`** and reads a file back through the kernel with
//! `std::fs::read`, asserting byte-exactness end to end.
//!
//! It is doubly gated so the default `cargo test` matrix (and CI without
//! `/dev/fuse` or the `mount` feature) is unaffected:
//!
//! - compiled only with `--features mount` (the whole file is behind
//!   `#![cfg(feature = "mount")]`), and
//! - marked `#[ignore]`, so even with the feature it runs only when explicitly
//!   requested: `cargo test -p talon-fuse --features mount --test mount_e2e -- --ignored`.
//!
//! Running it requires a working `/dev/fuse` (present on GitHub's Linux runners
//! and most Linux dev boxes, absent on macOS and in restricted sandboxes), which
//! is why it is not part of the default suite. By default a missing `/dev/fuse`
//! makes the test skip (pass as a no-op). Set `TALON_REQUIRE_FUSE=1` — as the CI
//! `fuse-mount` job does — to turn a mount failure into a hard error instead, so
//! a runner that unexpectedly lacks FUSE fails the job rather than passing
//! without exercising the kernel path.
#![cfg(feature = "mount")]

use std::collections::HashMap;
use std::io::{Read, Write};
use std::os::unix::fs::FileExt;
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use talon_core::{NodeId, NodeInfo, NodeRole};
use talon_fuse::mount::TalonFuse;
use talon_fuse::{BlockReader, CoordinatorClient, PlacementCache, ReadOnlyFs};
use talon_transport::data;
use talon_transport::frame::{FrameHeader, MsgType, HEADER_LEN};
use talon_transport::{decode_request, response_header_ok, ControlMessage, RangeRequest};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Deterministic content byte for an absolute object offset.
fn content_byte(abs_offset: u64) -> u8 {
    (abs_offset % 251) as u8
}

/// Spawn a mock worker serving deterministic bytes for any range, counting the
/// largest single `len` it is asked for (so the test can observe that the
/// kernel issues large reads once `max_write` is raised, #180).
async fn spawn_worker(max_len: Arc<AtomicU32>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => return,
            };
            let max_len = Arc::clone(&max_len);
            tokio::spawn(async move {
                let mut hdr = [0u8; HEADER_LEN];
                if sock.read_exact(&mut hdr).await.is_err() {
                    return;
                }
                let h = FrameHeader::decode(&hdr).unwrap();
                let mut body = vec![0u8; h.length as usize];
                sock.read_exact(&mut body).await.unwrap();
                let mut full = hdr.to_vec();
                full.extend_from_slice(&body);
                let (_h, req): (_, RangeRequest) = decode_request(&full).unwrap();
                max_len.fetch_max(req.len as u32, Ordering::SeqCst);
                let payload: Vec<u8> = (0..req.len).map(|i| content_byte(req.offset + i)).collect();
                let mut out = response_header_ok(0, payload.len() as u32).to_vec();
                out.extend_from_slice(&payload);
                sock.write_all(&out).await.unwrap();
                sock.flush().await.unwrap();
            });
        }
    });
    addr
}

/// Spawn a mock coordinator placing every block on the single worker.
async fn spawn_coordinator(worker_addr: String) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => return,
            };
            let worker_addr = worker_addr.clone();
            tokio::spawn(async move {
                let mut hdr = [0u8; HEADER_LEN];
                if sock.read_exact(&mut hdr).await.is_err() {
                    return;
                }
                let h = FrameHeader::decode(&hdr).unwrap();
                let mut body = vec![0u8; h.length as usize];
                sock.read_exact(&mut body).await.unwrap();
                let mut full = hdr.to_vec();
                full.extend_from_slice(&body);
                let (_h, msg) = talon_transport::decode(&full).unwrap();
                let reply = match msg {
                    ControlMessage::PlacementLookup { .. } => ControlMessage::PlacementResponse {
                        owners: vec![NodeId::new("w1")],
                        epoch: 1,
                    },
                    ControlMessage::MembershipQuery {} => ControlMessage::MembershipList {
                        nodes: vec![NodeInfo {
                            id: NodeId::new("w1"),
                            address: worker_addr.clone(),
                            role: NodeRole::Worker,
                        }],
                    },
                    _ => ControlMessage::Ack {
                        ok: false,
                        detail: None,
                    },
                };
                let out = talon_transport::encode(0, &reply).unwrap();
                sock.write_all(&out).await.unwrap();
                sock.flush().await.unwrap();
            });
        }
    });
    addr
}

/// Mount a Talon filesystem over mocks, read a file through the kernel, and
/// assert byte-exactness. Ignored by default (needs `/dev/fuse`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires /dev/fuse; run with --features mount -- --ignored"]
async fn mount_read_is_byte_exact_through_the_kernel() {
    use fuser::MountOption;

    let file_size: u64 = 3 * 1024 * 1024; // 3 MiB, spans several 1 MiB reads.
    let block_size: u32 = 4 * 1024 * 1024; // one block covers the whole file.

    let max_len = Arc::new(AtomicU32::new(0));
    let worker = spawn_worker(Arc::clone(&max_len)).await;
    let coord = spawn_coordinator(worker).await;

    // Namespace with a single object; the mount exposes it at /s3/bucket/obj.bin.
    let fs = Arc::new(ReadOnlyFs::new());
    fs.insert_object("s3/bucket/obj.bin", file_size);

    let cache = Arc::new(PlacementCache::new(10_000));
    let reader = BlockReader::new(CoordinatorClient::new(coord), cache, 1);
    let adapter = TalonFuse::new(
        Arc::clone(&fs),
        reader,
        tokio::runtime::Handle::current(),
        block_size,
        talon_core::Version::new(talon_fuse::mount::CANONICAL_MOUNT_VERSION),
    );

    // Mount at a temp dir. If /dev/fuse is unavailable this normally skips, so
    // the test is a no-op on machines without FUSE. In CI, set
    // TALON_REQUIRE_FUSE=1 so a mount failure is a hard error instead — that way
    // a runner that silently lacks /dev/fuse turns the job red rather than
    // passing without exercising the kernel path (a false green).
    let require_fuse = std::env::var_os("TALON_REQUIRE_FUSE").is_some();
    let mountpoint = std::env::temp_dir().join(format!("talon-mount-e2e-{}", std::process::id()));
    std::fs::create_dir_all(&mountpoint).unwrap();
    let options = vec![MountOption::RO, MountOption::FSName("talon".into())];
    let session = match fuser::spawn_mount2(adapter, &mountpoint, &options) {
        Ok(s) => s,
        Err(e) => {
            std::fs::remove_dir_all(&mountpoint).ok();
            if require_fuse {
                panic!(
                    "TALON_REQUIRE_FUSE is set but the FUSE mount failed: {e}. \
                     The runner must provide an accessible /dev/fuse."
                );
            }
            eprintln!("skipping: /dev/fuse unavailable: {e}");
            return;
        }
    };

    // Give the mount a moment to become visible, then read through the kernel.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let path = mountpoint.join("s3").join("bucket").join("obj.bin");
    let read_result = tokio::task::spawn_blocking(move || std::fs::read(&path))
        .await
        .unwrap();

    // Unmount before asserting so a failure can't leak the mount.
    drop(session);
    std::fs::remove_dir_all(&mountpoint).ok();

    let bytes = read_result.expect("read through mount");
    assert_eq!(bytes.len(), file_size as usize, "full file read back");
    for (i, b) in bytes.iter().enumerate() {
        assert_eq!(*b, content_byte(i as u64), "byte {i} mismatch");
    }
    // Sanity: the kernel issued at least one multi-KiB read (exact size depends
    // on kernel/readahead; #180 raises the ceiling to 1 MiB).
    assert!(
        max_len.load(Ordering::SeqCst) >= 4096,
        "kernel should issue reads larger than a single page"
    );
}

/// Shared object store for the read-write mock worker: object path → bytes.
type Store = Arc<Mutex<HashMap<String, Vec<u8>>>>;

/// Spawn a mock worker that honours the full data plane: `Put` writes an object
/// into the shared store (replying with a committed version), `Delete` removes
/// it, and `GetRange` serves whatever bytes the store currently holds (zero-fill
/// past end). This is enough to exercise the real write-through path through the
/// kernel without standing up a real backend.
async fn spawn_rw_worker(store: Store) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => return,
            };
            let store = Arc::clone(&store);
            tokio::spawn(async move {
                loop {
                    let mut hdr = [0u8; HEADER_LEN];
                    if sock.read_exact(&mut hdr).await.is_err() {
                        return;
                    }
                    let h = FrameHeader::decode(&hdr).unwrap();
                    let mut body = vec![0u8; h.length as usize];
                    if sock.read_exact(&mut body).await.is_err() {
                        return;
                    }
                    let mut full = hdr.to_vec();
                    full.extend_from_slice(&body);
                    match h.msg_type {
                        MsgType::Put => {
                            let (ph, req) = data::decode_put_header(&full).unwrap();
                            let mut obj = vec![0u8; req.body_len as usize];
                            sock.read_exact(&mut obj).await.unwrap();
                            store.lock().unwrap().insert(req.object.to_path(), obj);
                            let version = b"v-written";
                            let out = data::response_header_ok(ph.request_id, version.len() as u32);
                            sock.write_all(&out).await.unwrap();
                            sock.write_all(version).await.unwrap();
                            sock.flush().await.unwrap();
                        }
                        MsgType::Delete => {
                            let (dh, req) = data::decode_delete(&full).unwrap();
                            store.lock().unwrap().remove(&req.object.to_path());
                            let out = data::response_header_ok(dh.request_id, 0);
                            sock.write_all(&out).await.unwrap();
                            sock.flush().await.unwrap();
                        }
                        _ => {
                            let (_h, req): (_, RangeRequest) = decode_request(&full).unwrap();
                            let stored = store.lock().unwrap().get(&req.object.to_path()).cloned();
                            let payload: Vec<u8> = (0..req.len)
                                .map(|i| {
                                    let abs = (req.offset + i) as usize;
                                    stored
                                        .as_ref()
                                        .and_then(|b| b.get(abs).copied())
                                        .unwrap_or(0)
                                })
                                .collect();
                            let mut out = response_header_ok(0, payload.len() as u32).to_vec();
                            out.extend_from_slice(&payload);
                            sock.write_all(&out).await.unwrap();
                            sock.flush().await.unwrap();
                        }
                    }
                }
            });
        }
    });
    addr
}

/// Mount read-write, then create a file, overwrite it, and delete it through the
/// kernel — asserting the mock worker's object store reflects each operation.
/// Ignored by default (needs `/dev/fuse`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires /dev/fuse; run with --features mount -- --ignored"]
async fn mount_write_through_is_visible_in_backend() {
    use fuser::MountOption;

    let block_size: u32 = 4 * 1024 * 1024;
    let store: Store = Arc::new(Mutex::new(HashMap::new()));
    let worker = spawn_rw_worker(Arc::clone(&store)).await;
    let coord = spawn_coordinator(worker).await;

    let fs = Arc::new(ReadOnlyFs::new());
    // Pre-declare the parent directory so create() has a home; the object itself
    // is created through the kernel.
    fs.insert_object("s3/bucket/placeholder", 0);

    let cache = Arc::new(PlacementCache::new(10_000));
    let reader = BlockReader::new(CoordinatorClient::new(coord), cache, 1);
    let adapter = TalonFuse::new(
        Arc::clone(&fs),
        reader,
        tokio::runtime::Handle::current(),
        block_size,
        talon_core::Version::new(talon_fuse::mount::CANONICAL_MOUNT_VERSION),
    )
    .with_read_write(true);

    let require_fuse = std::env::var_os("TALON_REQUIRE_FUSE").is_some();
    let mountpoint =
        std::env::temp_dir().join(format!("talon-mount-e2e-write-{}", std::process::id()));
    std::fs::create_dir_all(&mountpoint).unwrap();
    let options = vec![MountOption::FSName("talon".into())];
    let session = match fuser::spawn_mount2(adapter, &mountpoint, &options) {
        Ok(s) => s,
        Err(e) => {
            std::fs::remove_dir_all(&mountpoint).ok();
            if require_fuse {
                panic!(
                    "TALON_REQUIRE_FUSE is set but the FUSE mount failed: {e}. \
                     The runner must provide an accessible /dev/fuse."
                );
            }
            eprintln!("skipping: /dev/fuse unavailable: {e}");
            return;
        }
    };

    tokio::time::sleep(Duration::from_millis(100)).await;
    let path = mountpoint.join("s3").join("bucket").join("hello.bin");

    // 1) Create + write a fresh file. release() flushes the dirty buffer through
    //    the write-through path into the mock backend store.
    let payload = vec![7u8; 1024];
    let p = path.clone();
    let pl = payload.clone();
    tokio::task::spawn_blocking(move || std::fs::write(&p, &pl))
        .await
        .unwrap()
        .expect("write new file");

    // 2) Overwrite the same path with different contents.
    let overwrite = vec![9u8; 2048];
    let p = path.clone();
    let ov = overwrite.clone();
    tokio::task::spawn_blocking(move || std::fs::write(&p, &ov))
        .await
        .unwrap()
        .expect("overwrite file");

    // 2b) Regression: opening the existing object read-write WITHOUT O_TRUNC
    //     and immediately closing must succeed without changing backend bytes.
    let p = path.clone();
    let bytes = tokio::task::spawn_blocking(move || {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&p)?;
        drop(file);
        // Re-open read-only and report what the object now holds.
        std::fs::read(&p)
    })
    .await
    .unwrap()
    .expect("open existing object read-write without truncating");
    assert_eq!(
        bytes, overwrite,
        "O_RDWR open+close must not truncate the object"
    );
    assert_eq!(
        store.lock().unwrap().get("/s3/bucket/hello.bin"),
        Some(&overwrite),
        "backend bytes must survive a bare O_RDWR open+close"
    );

    // 3) Delete it.
    let p = path.clone();
    let removed = tokio::task::spawn_blocking(move || std::fs::remove_file(&p)).await;

    drop(session);
    std::fs::remove_dir_all(&mountpoint).ok();

    removed.unwrap().expect("remove file");

    // The backend should have seen the overwrite bytes, then the delete.
    let final_store = store.lock().unwrap();
    assert!(
        !final_store.contains_key("/s3/bucket/hello.bin"),
        "object should be gone from backend after unlink"
    );
}

/// Mount the read-write fixture and run the pinned pjdfstest suite against it.
///
/// This is separately gated because the normal real-kernel smoke job runs all
/// ignored tests. Set `TALON_RUN_PJDFSTEST=1` to opt in. A comma-separated
/// `TALON_PJDFSTEST_TESTS` value selects groups or files; omitting it runs the
/// complete suite.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires /dev/fuse, root, and pjdfstest build dependencies"]
async fn mount_pjdfstest_compatibility_suite() {
    use fuser::MountOption;

    if std::env::var_os("TALON_RUN_PJDFSTEST").is_none() {
        eprintln!("skipping: set TALON_RUN_PJDFSTEST=1 to run pjdfstest");
        return;
    }

    let block_size: u32 = 4 * 1024 * 1024;
    let store: Store = Arc::new(Mutex::new(HashMap::new()));
    let worker = spawn_rw_worker(store).await;
    let coord = spawn_coordinator(worker).await;

    let fs = Arc::new(ReadOnlyFs::new());
    fs.insert_object("s3/bucket/placeholder", 0);

    let cache = Arc::new(PlacementCache::new(10_000));
    let reader = BlockReader::new(CoordinatorClient::new(coord), cache, 1);
    let adapter = TalonFuse::new(
        Arc::clone(&fs),
        reader,
        tokio::runtime::Handle::current(),
        block_size,
        talon_core::Version::new(talon_fuse::mount::CANONICAL_MOUNT_VERSION),
    )
    .with_read_write(true);

    let mountpoint = std::env::temp_dir().join(format!("talon-pjdfstest-{}", std::process::id()));
    std::fs::create_dir_all(&mountpoint).unwrap();
    let options = vec![MountOption::FSName("talon".into())];
    let session = fuser::spawn_mount2(adapter, &mountpoint, &options)
        .expect("TALON_RUN_PJDFSTEST requires a working /dev/fuse");

    tokio::time::sleep(Duration::from_millis(100)).await;
    let test_dir = mountpoint.join("s3").join("bucket");
    let runner =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/posix/pjdfstest/run.sh");
    let selectors = std::env::var("TALON_PJDFSTEST_TESTS").unwrap_or_default();

    let status = tokio::task::spawn_blocking(move || {
        let mut command = Command::new(runner);
        command.arg("--mountpoint").arg(test_dir);
        for selector in selectors
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            command.arg(selector);
        }
        command.status()
    })
    .await
    .unwrap()
    .expect("start pjdfstest runner");

    drop(session);
    std::fs::remove_dir_all(&mountpoint).ok();

    assert!(
        status.success(),
        "pjdfstest reported compatibility failures"
    );
}

/// Measure I/O through a real kernel FUSE mount backed by local protocol mocks.
///
/// Set `TALON_RUN_FUSE_BENCH=1` to opt in. The benchmark reports FUSE/Talon
/// userspace path performance, not object-store or cross-region performance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires /dev/fuse; set TALON_RUN_FUSE_BENCH=1"]
async fn mount_kernel_io_benchmark() {
    use fuser::MountOption;

    if std::env::var_os("TALON_RUN_FUSE_BENCH").is_none() {
        eprintln!("skipping: set TALON_RUN_FUSE_BENCH=1 to run the benchmark");
        return;
    }

    let read_mib = env_u64("TALON_FUSE_BENCH_READ_MIB", 128);
    let write_mib = env_u64("TALON_FUSE_BENCH_WRITE_MIB", 64);
    let random_ops = env_u64("TALON_FUSE_BENCH_RANDOM_OPS", 4096);
    let read_size = read_mib * 1024 * 1024;
    let write_size = write_mib * 1024 * 1024;

    let block_size: u32 = 4 * 1024 * 1024;
    let store: Store = Arc::new(Mutex::new(HashMap::new()));
    let worker = spawn_rw_worker(Arc::clone(&store)).await;
    let coord = spawn_coordinator(worker).await;

    let fs = Arc::new(ReadOnlyFs::new());
    fs.insert_object("s3/bucket/bench-read.bin", read_size);

    let cache = Arc::new(PlacementCache::new(10_000));
    let reader = BlockReader::new(CoordinatorClient::new(coord), cache, 1);
    let adapter = TalonFuse::new(
        Arc::clone(&fs),
        reader,
        tokio::runtime::Handle::current(),
        block_size,
        talon_core::Version::new(talon_fuse::mount::CANONICAL_MOUNT_VERSION),
    )
    .with_read_write(true);

    let mountpoint = std::env::temp_dir().join(format!("talon-fuse-bench-{}", std::process::id()));
    std::fs::create_dir_all(&mountpoint).unwrap();
    let options = vec![MountOption::FSName("talon".into())];
    let session = fuser::spawn_mount2(adapter, &mountpoint, &options)
        .expect("TALON_RUN_FUSE_BENCH requires a working /dev/fuse");

    tokio::time::sleep(Duration::from_millis(100)).await;
    let read_path = mountpoint.join("s3").join("bucket").join("bench-read.bin");
    let write_path = mountpoint.join("s3").join("bucket").join("bench-write.bin");

    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&read_path).expect("open benchmark read file");
        let mut page = [0u8; 4096];
        let max_page = (read_size / page.len() as u64).max(1);
        let mut state = 0x4d595df4d0f33173u64;
        let random_started = Instant::now();
        for _ in 0..random_ops {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let offset = (state % max_page) * page.len() as u64;
            file.read_exact_at(&mut page, offset)
                .expect("random benchmark read");
            std::hint::black_box(&page);
        }
        let random_elapsed = random_started.elapsed();
        report_rate(
            "random_read_4k_iops",
            random_ops as f64 / random_elapsed.as_secs_f64(),
            "ops/s",
        );

        let cold_started = Instant::now();
        let cold_bytes = stream_file(&read_path);
        let cold_elapsed = cold_started.elapsed();
        report_throughput("sequential_read_cold", cold_bytes, cold_elapsed);

        let warm_started = Instant::now();
        let warm_bytes = stream_file(&read_path);
        let warm_elapsed = warm_started.elapsed();
        report_throughput("sequential_read_warm", warm_bytes, warm_elapsed);

        let payload = vec![0x5au8; write_size as usize];
        let write_started = Instant::now();
        {
            let mut output =
                std::fs::File::create(&write_path).expect("create benchmark write file");
            output.write_all(&payload).expect("write benchmark payload");
        }
        let write_elapsed = write_started.elapsed();
        report_throughput("write_through", write_size, write_elapsed);
    })
    .await
    .unwrap();

    drop(session);
    std::fs::remove_dir_all(&mountpoint).ok();

    assert_eq!(
        store
            .lock()
            .unwrap()
            .get("/s3/bucket/bench-write.bin")
            .map(Vec::len),
        Some(write_size as usize),
        "benchmark write must reach the mock backend"
    );
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn stream_file(path: &std::path::Path) -> u64 {
    let mut file = std::fs::File::open(path).expect("open sequential benchmark file");
    let mut buffer = vec![0u8; 1024 * 1024];
    let mut total = 0u64;
    loop {
        let read = file.read(&mut buffer).expect("sequential benchmark read");
        if read == 0 {
            return total;
        }
        total += read as u64;
        std::hint::black_box(&buffer[..read]);
    }
}

fn report_throughput(metric: &str, bytes: u64, elapsed: Duration) {
    let mib_per_second = bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
    report_rate(metric, mib_per_second, "MiB/s");
}

fn report_rate(metric: &str, value: f64, unit: &str) {
    println!("TALON_FUSE_BENCH metric={metric} value={value:.2} unit={unit}");
}
