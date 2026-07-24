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
//! Running it requires a working `/dev/fuse` (present in most Linux dev boxes
//! and CI runners with `--privileged`, absent in restricted sandboxes), which
//! is why it is not part of the default suite.
#![cfg(feature = "mount")]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use talon_core::{NodeId, NodeInfo, NodeRole};
use talon_fuse::mount::TalonFuse;
use talon_fuse::{BlockReader, CoordinatorClient, PlacementCache, ReadOnlyFs};
use talon_transport::frame::{FrameHeader, HEADER_LEN};
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
        talon_core::Version::new("v1"),
    );

    // Mount at a temp dir. Skip (don't fail) if /dev/fuse is unavailable.
    let mountpoint = std::env::temp_dir().join(format!("talon-mount-e2e-{}", std::process::id()));
    std::fs::create_dir_all(&mountpoint).unwrap();
    let options = vec![MountOption::RO, MountOption::FSName("talon".into())];
    let session = match fuser::spawn_mount2(adapter, &mountpoint, &options) {
        Ok(s) => s,
        Err(e) => {
            std::fs::remove_dir_all(&mountpoint).ok();
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
