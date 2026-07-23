//! In-process end-to-end read-path integration test.
//!
//! This is the ground-truth for the FUSE read path: it wires a **mock
//! coordinator** and a **mock worker** — both real TCP servers speaking the
//! actual Talon wire protocol — to a real [`BlockReader`], with no kernel mount
//! and no cloud backend. It asserts the behaviors the unit tests cover in
//! isolation actually compose end to end:
//!
//! 1. A cold read is a cache **miss** → coordinator lookup → worker fetch →
//!    bytes; the second read of the same block is a cache **hit** (no second
//!    coordinator round-trip) and returns identical bytes.
//! 2. A multi-block read splits across block boundaries and **stitches** the
//!    per-block results into one contiguous buffer.
//! 3. When the primary replica reports the block missing, the reader **falls
//!    back** to the healthy secondary within the cached list.
//!
//! The mock worker synthesizes deterministic content — byte `i` of an object is
//! `(absolute_offset + i) % 256` — so the test can verify exact bytes without a
//! real backend, and counts requests so cache-hit / fallback behavior is
//! observable.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use talon_core::{Backend, NodeId, NodeInfo, NodeRole, ObjectId, Version};
use talon_fuse::{BlockReader, CoordinatorClient, FileView, PlacementCache};
use talon_transport::frame::{FrameHeader, HEADER_LEN};
use talon_transport::{
    decode_request, encode_error, response_header_ok, ControlMessage, RangeRequest,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Deterministic content byte for an absolute object offset.
fn content_byte(abs_offset: u64) -> u8 {
    (abs_offset % 256) as u8
}

/// Counters a mock worker exposes so tests can observe request behavior.
#[derive(Default)]
struct WorkerCounters {
    fetches: AtomicU32,
}

/// Spawn a mock worker that serves deterministic bytes for any range request,
/// counting fetches. If `fail_all` is set it always returns an ERROR frame.
async fn spawn_worker(counters: Arc<WorkerCounters>, fail_all: bool) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => return,
            };
            let counters = Arc::clone(&counters);
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
                counters.fetches.fetch_add(1, Ordering::SeqCst);

                if fail_all {
                    sock.write_all(&encode_error(0, "block not present"))
                        .await
                        .unwrap();
                    sock.flush().await.unwrap();
                    return;
                }
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

/// Spawn a mock coordinator that places every block on `owners` (in order),
/// resolving each id to the paired worker address. Counts placement lookups so
/// a test can prove the second read was a cache hit (no new lookup).
async fn spawn_coordinator(owners: Vec<(String, String)>, lookups: Arc<AtomicU32>) -> String {
    // owners: (node_id, worker_addr) in priority order.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => return,
            };
            let owners = owners.clone();
            let lookups = Arc::clone(&lookups);
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
                    ControlMessage::PlacementLookup { .. } => {
                        lookups.fetch_add(1, Ordering::SeqCst);
                        ControlMessage::PlacementResponse {
                            owners: owners.iter().map(|(id, _)| NodeId::new(id)).collect(),
                            epoch: 1,
                        }
                    }
                    ControlMessage::MembershipQuery {} => ControlMessage::MembershipList {
                        nodes: owners
                            .iter()
                            .map(|(id, a)| NodeInfo {
                                id: NodeId::new(id),
                                address: a.clone(),
                                role: NodeRole::Worker,
                            })
                            .collect(),
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

fn object() -> ObjectId {
    ObjectId::new(Backend::S3, "bucket", "data/checkpoint.bin")
}

#[tokio::test]
async fn cold_read_miss_then_warm_hit_same_bytes() {
    let wc = Arc::new(WorkerCounters::default());
    let worker = spawn_worker(Arc::clone(&wc), false).await;
    let lookups = Arc::new(AtomicU32::new(0));
    let coord = spawn_coordinator(vec![("w1".into(), worker)], Arc::clone(&lookups)).await;

    let cache = Arc::new(PlacementCache::new(10_000));
    let reader = BlockReader::new(CoordinatorClient::new(coord), cache, 1);
    let obj = object();
    let ver = Version::new("v1");
    let block_size = 1024u32;
    let view = FileView {
        object: &obj,
        block_size,
        version: &ver,
        size: 1_000_000,
    };

    // Cold read: miss → coordinator lookup → worker fetch. Kept within a single
    // block (block 3 spans 3072..4096) so exactly one placement lookup happens.
    let first = reader.read(&view, 3100, 256, 0).await.unwrap();
    assert_eq!(first.len(), 256);
    for (i, b) in first.iter().enumerate() {
        assert_eq!(*b, content_byte(3100 + i as u64));
    }
    assert_eq!(
        lookups.load(Ordering::SeqCst),
        1,
        "one placement lookup on miss"
    );

    // Warm read of the same block: cache hit, no new coordinator lookup.
    let second = reader.read(&view, 3100, 256, 1).await.unwrap();
    assert_eq!(second, first, "warm read returns identical bytes");
    assert_eq!(
        lookups.load(Ordering::SeqCst),
        1,
        "warm read must not re-query the coordinator"
    );

    let snap = reader.stats().snapshot();
    assert_eq!(snap.cache_misses, 1);
    assert_eq!(snap.cache_hits, 1);
}

#[tokio::test]
async fn multi_block_read_stitches_contiguous_bytes() {
    let wc = Arc::new(WorkerCounters::default());
    let worker = spawn_worker(Arc::clone(&wc), false).await;
    let lookups = Arc::new(AtomicU32::new(0));
    let coord = spawn_coordinator(vec![("w1".into(), worker)], lookups).await;

    let cache = Arc::new(PlacementCache::new(10_000));
    let reader = BlockReader::new(CoordinatorClient::new(coord), cache, 1);
    let obj = object();
    let ver = Version::new("v1");
    let block_size = 1024u32;
    let view = FileView {
        object: &obj,
        block_size,
        version: &ver,
        size: 1_000_000,
    };

    // 900..900+2300 spans 4 blocks (tail, full, full, head).
    let offset = 900u64;
    let len = 2300u64;
    let bytes = reader.read(&view, offset, len, 0).await.unwrap();
    assert_eq!(bytes.len() as u64, len);
    for (i, b) in bytes.iter().enumerate() {
        assert_eq!(
            *b,
            content_byte(offset + i as u64),
            "byte {i} not contiguous across block boundary"
        );
    }
    // One worker fetch per block.
    assert_eq!(wc.fetches.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn falls_back_to_healthy_replica() {
    // Primary w1 always fails; secondary w2 serves the bytes.
    let bad = Arc::new(WorkerCounters::default());
    let good = Arc::new(WorkerCounters::default());
    let w1 = spawn_worker(Arc::clone(&bad), true).await;
    let w2 = spawn_worker(Arc::clone(&good), false).await;
    let lookups = Arc::new(AtomicU32::new(0));
    let coord = spawn_coordinator(
        vec![("w1".into(), w1), ("w2".into(), w2)],
        Arc::clone(&lookups),
    )
    .await;

    let cache = Arc::new(PlacementCache::new(10_000));
    // Request k=2 so both replicas are cached and available for fallback.
    let reader = BlockReader::new(CoordinatorClient::new(coord), cache, 2);
    let obj = object();
    let ver = Version::new("v1");
    let view = FileView {
        object: &obj,
        block_size: 1024,
        version: &ver,
        size: 1_000_000,
    };

    let bytes = reader.read(&view, 0, 128, 0).await.unwrap();
    assert_eq!(bytes.len(), 128);
    for (i, b) in bytes.iter().enumerate() {
        assert_eq!(*b, content_byte(i as u64));
    }
    // Primary was tried once and failed; secondary served it — within the same
    // cached placement (a single coordinator lookup, no refresh).
    assert_eq!(bad.fetches.load(Ordering::SeqCst), 1);
    assert_eq!(good.fetches.load(Ordering::SeqCst), 1);
    assert_eq!(lookups.load(Ordering::SeqCst), 1);

    let snap = reader.stats().snapshot();
    assert_eq!(snap.worker_failures, 1);
    assert_eq!(snap.coordinator_refreshes, 0);
}
