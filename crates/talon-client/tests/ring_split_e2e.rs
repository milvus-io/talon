// SPDX-License-Identifier: Apache-2.0
//! Integration test: the two rendezvous rings, over real TCP against the real
//! coordinator binary.
//!
//! ADR 0005 §6 splits placement into two disjoint rings selected by node role.
//! Three properties make that worth having, and none of them are observable
//! from a unit test of `PlacementService` alone — they depend on the coordinator
//! binary wiring both rings to the same membership and answering both message
//! variants:
//!
//! 1. **A lookup never crosses pools.** An async lookup that returned a block
//!    worker would fail at read time, one round trip later, instead of here.
//! 2. **The async ring pins a whole object to one worker**, at every offset, so
//!    one reader's footer fetch warms the next reader's column-chunk read. The
//!    block ring deliberately does the opposite.
//! 3. **An async lookup against a cluster with no async workers answers
//!    "nobody"** rather than quietly substituting a block worker.
//!
//! Mock nodes register through `NodeStatusHeartbeat`, the store-authoritative
//! path a real worker uses, so this exercises the same membership plumbing the
//! binaries do without needing a running data plane or object-store
//! credentials.

use std::collections::BTreeMap;
use std::process::{Child, Command};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use talon_core::{
    Backend, BlockId, NodeHealth, NodeId, NodeInfo, NodeMetricsSnapshot, NodeRole, NodeStatus,
    ObjectId, Version, NODE_STATUS_SCHEMA_VERSION,
};
use talon_transport::frame::HEADER_LEN;
use talon_transport::{codec, ControlMessage, FrameHeader};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Kill the coordinator child on drop so a failing assert cannot leak it.
struct Killer(Child);
impl Drop for Killer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Locate the sibling `talon-coordinator` binary next to this test's target
/// dir (`.../target/<profile>/deps/<test>` → `.../target/<profile>/`).
fn coordinator_bin() -> std::path::PathBuf {
    let mut dir = std::env::current_exe().unwrap();
    dir.pop();
    if dir.ends_with("deps") {
        dir.pop();
    }
    let exe = if cfg!(windows) {
        "talon-coordinator.exe"
    } else {
        "talon-coordinator"
    };
    dir.join(exe)
}

async fn round_trip(addr: &str, msg: &ControlMessage) -> ControlMessage {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let buf = codec::encode(0, msg).unwrap();
    stream.write_all(&buf).await.unwrap();
    stream.flush().await.unwrap();

    let mut header_buf = [0u8; HEADER_LEN];
    stream.read_exact(&mut header_buf).await.unwrap();
    let header = FrameHeader::decode(&header_buf).unwrap();
    let mut payload = vec![0u8; header.length as usize];
    stream.read_exact(&mut payload).await.unwrap();
    let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
    full.extend_from_slice(&header_buf);
    full.extend_from_slice(&payload);
    codec::decode(&full).unwrap().1
}

/// Start a coordinator on `addr` and wait for it to accept connections.
async fn coordinator(addr: &str, admin: &str) -> Killer {
    let child = Command::new(coordinator_bin())
        .args(["--listen", addr, "--admin-listen", admin])
        .spawn()
        .unwrap();
    let killer = Killer(child);
    for _ in 0..100 {
        if TcpStream::connect(addr).await.is_ok() {
            return killer;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("coordinator did not start listening on {addr}");
}

/// Register a node of `role` through the store-authoritative heartbeat.
async fn register(addr: &str, id: &str, role: NodeRole) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let status = NodeStatus {
        schema_version: NODE_STATUS_SCHEMA_VERSION,
        // The coordinator binary defaults to cluster_id "default".
        cluster_id: "default".into(),
        node: NodeInfo {
            id: NodeId::new(id),
            address: format!("{id}:7001"),
            role,
        },
        incarnation_id: format!("{id}-incarnation"),
        admin_address: Some(format!("{id}:8001")),
        build_version: "test".into(),
        started_at_unix_ms: now,
        reported_at_unix_ms: now,
        heartbeat_seq: 0,
        health: NodeHealth::Healthy,
        ready: true,
        metrics: NodeMetricsSnapshot::default(),
        labels: BTreeMap::new(),
    };
    let ack = round_trip(
        addr,
        &ControlMessage::NodeStatusHeartbeat {
            status: Box::new(status),
        },
    )
    .await;
    assert!(
        matches!(ack, ControlMessage::Ack { ok: true, .. }),
        "registering {id} as {role}: {ack:?}"
    );
}

fn block(object: &str, offset: u64) -> BlockId {
    BlockId::new(
        ObjectId::new(Backend::S3, "warehouse", object),
        offset,
        256 << 20,
        Version::new("v1"),
    )
}

async fn owners(addr: &str, msg: ControlMessage) -> Vec<String> {
    match round_trip(addr, &msg).await {
        ControlMessage::PlacementResponse { owners, .. } => {
            owners.into_iter().map(|n| n.0).collect()
        }
        other => panic!("expected PlacementResponse, got {other:?}"),
    }
}

async fn block_owners(addr: &str, object: &str, offset: u64) -> Vec<String> {
    owners(
        addr,
        ControlMessage::PlacementLookup {
            block: block(object, offset),
            k: 1,
        },
    )
    .await
}

async fn async_owners(addr: &str, object: &str, offset: u64) -> Vec<String> {
    owners(
        addr,
        ControlMessage::AsyncPlacementLookup {
            block: block(object, offset),
            k: 1,
        },
    )
    .await
}

/// The whole point of the split: neither lookup can return the other pool's
/// nodes. A block worker holds no extents and an async worker holds no blocks,
/// so a crossed answer is a read failure a round trip later.
#[tokio::test]
async fn a_lookup_never_returns_the_other_rings_workers() {
    let addr = "127.0.0.1:7421";
    let _coordinator = coordinator(addr, "127.0.0.1:8421").await;
    for id in ["blk-a", "blk-b", "blk-c"] {
        register(addr, id, NodeRole::Worker).await;
    }
    for id in ["ext-a", "ext-b"] {
        register(addr, id, NodeRole::AsyncWorker).await;
    }

    for i in 0..40u64 {
        let object = format!("part-{i:05}.parquet");
        let blocks = block_owners(addr, &object, 0).await;
        assert_eq!(blocks.len(), 1, "block ring must answer: {blocks:?}");
        assert!(
            blocks[0].starts_with("blk-"),
            "block lookup returned an async worker: {blocks:?}"
        );

        let extents = async_owners(addr, &object, 0).await;
        assert_eq!(extents.len(), 1, "async ring must answer: {extents:?}");
        assert!(
            extents[0].starts_with("ext-"),
            "async lookup returned a block worker: {extents:?}"
        );
    }
}

/// Whole-object affinity, which is the reason the async ring exists.
///
/// A columnar reader fetches a footer and then cherry-picks column chunks at
/// unrelated offsets. On the block ring those hash apart, so every chunk pays a
/// cold miss on a different worker. On the async ring they all land on one.
#[tokio::test]
async fn the_async_ring_pins_every_offset_of_an_object_to_one_worker() {
    let addr = "127.0.0.1:7422";
    let _coordinator = coordinator(addr, "127.0.0.1:8422").await;
    for id in ["blk-a", "blk-b", "blk-c", "blk-d"] {
        register(addr, id, NodeRole::Worker).await;
    }
    for id in ["ext-a", "ext-b", "ext-c", "ext-d"] {
        register(addr, id, NodeRole::AsyncWorker).await;
    }

    // Offsets a real Parquet reader would touch: footer, metadata, then column
    // chunks scattered through the file.
    let offsets = [
        0,
        4 << 20,
        200 << 20,
        (256 << 20) + 1,
        (512 << 20) + 4096,
        3 << 30,
    ];
    let object = "sales/part-00000.parquet";

    let pinned = async_owners(addr, object, offsets[0]).await;
    for offset in offsets {
        assert_eq!(
            async_owners(addr, object, offset).await,
            pinned,
            "the async ring moved the object at offset {offset}"
        );
    }

    // The block ring must genuinely differ, or the assertion above proves
    // nothing about the split — it would just mean four workers happened to
    // hash the same way.
    let mut block_placements = std::collections::HashSet::new();
    for offset in offsets {
        block_placements.insert(block_owners(addr, object, offset).await);
    }
    assert!(
        block_placements.len() > 1,
        "the block ring must spread offsets across the fleet; got {block_placements:?}"
    );
}

/// Distinct objects must still spread. Pinning per object is affinity; pinning
/// every object to one node would be a hotspot.
#[tokio::test]
async fn the_async_ring_still_spreads_distinct_objects_across_the_pool() {
    let addr = "127.0.0.1:7423";
    let _coordinator = coordinator(addr, "127.0.0.1:8423").await;
    for id in ["ext-a", "ext-b", "ext-c", "ext-d"] {
        register(addr, id, NodeRole::AsyncWorker).await;
    }

    let mut seen = std::collections::HashSet::new();
    for i in 0..60u64 {
        let placed = async_owners(addr, &format!("part-{i:05}.parquet"), 0).await;
        assert_eq!(placed.len(), 1);
        seen.insert(placed[0].clone());
    }
    assert_eq!(
        seen.len(),
        4,
        "60 objects must reach all four workers, reached {seen:?}"
    );
}

/// No async workers means no owners — never a block worker as a consolation.
/// A substituted block worker would accept the connection and then fail the
/// read, which is strictly worse than an empty answer the client can act on.
#[tokio::test]
async fn an_async_lookup_against_a_block_only_cluster_has_no_owners() {
    let addr = "127.0.0.1:7424";
    let _coordinator = coordinator(addr, "127.0.0.1:8424").await;
    for id in ["blk-a", "blk-b"] {
        register(addr, id, NodeRole::Worker).await;
    }

    let extents = async_owners(addr, "part-00000.parquet", 0).await;
    assert!(
        extents.is_empty(),
        "async lookup fell back to a block worker: {extents:?}"
    );

    // The block ring is unaffected: this is not a broken coordinator.
    let blocks = block_owners(addr, "part-00000.parquet", 0).await;
    assert_eq!(blocks.len(), 1);
}

/// Both rings report the same epoch, so a client caches one number and compares
/// it for equality regardless of which ring it asked.
#[tokio::test]
async fn both_rings_report_the_same_epoch() {
    let addr = "127.0.0.1:7425";
    let _coordinator = coordinator(addr, "127.0.0.1:8425").await;
    register(addr, "blk-a", NodeRole::Worker).await;
    register(addr, "ext-a", NodeRole::AsyncWorker).await;

    let epoch_of = |msg: ControlMessage| async move {
        match round_trip(addr, &msg).await {
            ControlMessage::PlacementResponse { epoch, .. } => epoch,
            other => panic!("expected PlacementResponse, got {other:?}"),
        }
    };
    let b = epoch_of(ControlMessage::PlacementLookup {
        block: block("part-00000.parquet", 0),
        k: 1,
    })
    .await;
    let a = epoch_of(ControlMessage::AsyncPlacementLookup {
        block: block("part-00000.parquet", 0),
        k: 1,
    })
    .await;
    assert_eq!(a, b, "the epoch covers the whole membership, not one ring");
}

/// An async worker must reach the membership feed, or a client that resolves
/// an owner id to an address finds nothing and the lookup is useless.
#[tokio::test]
async fn an_async_worker_is_resolvable_through_the_membership_query() {
    let addr = "127.0.0.1:7426";
    let _coordinator = coordinator(addr, "127.0.0.1:8426").await;
    register(addr, "ext-a", NodeRole::AsyncWorker).await;

    let owner = async_owners(addr, "part-00000.parquet", 0)
        .await
        .into_iter()
        .next()
        .expect("an async worker owns it");

    let nodes = match round_trip(addr, &ControlMessage::MembershipQuery {}).await {
        ControlMessage::MembershipList { nodes } => nodes,
        other => panic!("expected MembershipList, got {other:?}"),
    };
    let resolved = nodes
        .iter()
        .find(|n| n.id.0 == owner)
        .expect("the owner must be resolvable to an address");
    assert_eq!(resolved.role, NodeRole::AsyncWorker);
    assert_eq!(resolved.address, "ext-a:7001");
}
