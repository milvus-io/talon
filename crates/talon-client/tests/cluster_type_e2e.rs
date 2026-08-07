// SPDX-License-Identifier: Apache-2.0
//! Integration test: the two cluster types, over real TCP against the real
//! coordinator binary.
//!
//! ADR 0006 moves the choice of ring from the request to the cluster. A
//! coordinator is started as `block` or `async` and runs exactly one ring for
//! its whole life. Four properties follow, and none are observable from a unit
//! test of `PlacementService` — they depend on the binary reading
//! `--cluster-type` and wiring the membership, the ring, and the heartbeat
//! check consistently from it:
//!
//! 1. **A cluster refuses the other kind's workers**, at registration, with a
//!    reason. Before ADR 0006 such a worker joined and was merely never
//!    chosen, so the misconfiguration showed up as a permanently cold cache.
//! 2. **An async cluster pins a whole object to one worker**, at every offset,
//!    so one reader's footer fetch warms the next reader's column-chunk read.
//!    A block cluster deliberately does the opposite.
//! 3. **The two clusters' epochs are independent.** Churn in one is invisible
//!    to the other's clients — the property the single shared registry could
//!    not offer.
//! 4. **`ListObjects` against an async cluster errors** rather than returning
//!    an empty listing, which a client would cache as an empty bucket.
//!
//! Mock nodes register through `NodeStatusHeartbeat`, the store-authoritative
//! path a real worker uses, so this exercises the same membership plumbing the
//! binaries do without needing a running data plane or object-store
//! credentials.

use std::collections::BTreeMap;
use std::process::{Child, Command};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use talon_core::{
    Backend, BlockId, ClusterType, NodeHealth, NodeId, NodeInfo, NodeMetricsSnapshot, NodeRole,
    NodeStatus, ObjectId, Version, NODE_STATUS_SCHEMA_VERSION,
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

/// Start a coordinator of `cluster_type` on `addr` and wait for it to listen.
async fn coordinator(cluster_type: ClusterType, addr: &str, admin: &str) -> Killer {
    let child = Command::new(coordinator_bin())
        .args([
            "--listen",
            addr,
            "--admin-listen",
            admin,
            "--cluster-type",
            cluster_type.as_str(),
        ])
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

fn status(id: &str, role: NodeRole) -> NodeStatus {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    NodeStatus {
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
    }
}

/// Heartbeat a node of `role` and return the coordinator's `Ack`.
async fn heartbeat(addr: &str, id: &str, role: NodeRole) -> (bool, Option<String>) {
    match round_trip(
        addr,
        &ControlMessage::NodeStatusHeartbeat {
            status: Box::new(status(id, role)),
        },
    )
    .await
    {
        ControlMessage::Ack { ok, detail } => (ok, detail),
        other => panic!("expected Ack, got {other:?}"),
    }
}

/// Register a node that this cluster must accept.
async fn register(addr: &str, id: &str, role: NodeRole) {
    let (ok, detail) = heartbeat(addr, id, role).await;
    assert!(ok, "registering {id} as {role}: {detail:?}");
}

fn block(object: &str, offset: u64) -> BlockId {
    BlockId::new(
        ObjectId::new(Backend::S3, "warehouse", object),
        offset,
        256 << 20,
        Version::new("v1"),
    )
}

/// Ask a coordinator who owns `object` at `offset`.
///
/// One message, whichever kind of cluster answers it: the ring is the
/// coordinator's property, not the request's.
async fn owners(addr: &str, object: &str, offset: u64) -> Vec<String> {
    let msg = ControlMessage::PlacementLookup {
        block: block(object, offset),
        k: 1,
    };
    match round_trip(addr, &msg).await {
        ControlMessage::PlacementResponse { owners, .. } => {
            owners.into_iter().map(|n| n.0).collect()
        }
        other => panic!("expected PlacementResponse, got {other:?}"),
    }
}

async fn epoch_of(addr: &str, object: &str) -> u64 {
    let msg = ControlMessage::PlacementLookup {
        block: block(object, 0),
        k: 1,
    };
    match round_trip(addr, &msg).await {
        ControlMessage::PlacementResponse { epoch, .. } => epoch,
        other => panic!("expected PlacementResponse, got {other:?}"),
    }
}

/// The boundary that replaces the old per-request ring: a worker of the wrong
/// kind is turned away at the door, told why, and never becomes a placement
/// candidate. Previously it joined silently and was filtered at every lookup,
/// so the only symptom was a cache that never warmed.
#[tokio::test]
async fn a_cluster_refuses_the_other_kinds_worker_and_says_why() {
    let block_addr = "127.0.0.1:7421";
    let async_addr = "127.0.0.1:7431";
    let _block = coordinator(ClusterType::Block, block_addr, "127.0.0.1:8421").await;
    let _async = coordinator(ClusterType::Async, async_addr, "127.0.0.1:8431").await;

    register(block_addr, "blk-a", NodeRole::Worker).await;
    register(async_addr, "ext-a", NodeRole::AsyncWorker).await;

    let (ok, detail) = heartbeat(block_addr, "ext-b", NodeRole::AsyncWorker).await;
    assert!(!ok, "a block cluster admitted an async worker");
    let detail = detail.expect("the refusal must say why");
    assert!(
        detail.contains("async") && detail.contains("block"),
        "the refusal must name both the node and the cluster: {detail}"
    );

    let (ok, detail) = heartbeat(async_addr, "blk-b", NodeRole::Worker).await;
    assert!(!ok, "an async cluster admitted a block worker");
    assert!(detail.is_some(), "the refusal must say why");

    // Refused, not merely unplaceable: neither cluster can return the other's
    // node, because it never entered the registry.
    for i in 0..20u64 {
        let object = format!("part-{i:05}.parquet");
        assert_eq!(owners(block_addr, &object, 0).await, vec!["blk-a"]);
        assert_eq!(owners(async_addr, &object, 0).await, vec!["ext-a"]);
    }
}

/// Whole-object affinity, which is the reason the async cluster exists.
///
/// A columnar reader fetches a footer and then cherry-picks column chunks at
/// unrelated offsets. In a block cluster those hash apart, so every chunk pays
/// a cold miss on a different worker. In an async cluster they all land on one.
#[tokio::test]
async fn an_async_cluster_pins_every_offset_of_an_object_to_one_worker() {
    let block_addr = "127.0.0.1:7422";
    let async_addr = "127.0.0.1:7432";
    let _block = coordinator(ClusterType::Block, block_addr, "127.0.0.1:8422").await;
    let _async = coordinator(ClusterType::Async, async_addr, "127.0.0.1:8432").await;
    for id in ["blk-a", "blk-b", "blk-c", "blk-d"] {
        register(block_addr, id, NodeRole::Worker).await;
    }
    for id in ["ext-a", "ext-b", "ext-c", "ext-d"] {
        register(async_addr, id, NodeRole::AsyncWorker).await;
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

    let pinned = owners(async_addr, object, offsets[0]).await;
    for offset in offsets {
        assert_eq!(
            owners(async_addr, object, offset).await,
            pinned,
            "the async cluster moved the object at offset {offset}"
        );
    }

    // The block cluster must genuinely differ, or the assertion above proves
    // nothing about the split — it would just mean four workers happened to
    // hash the same way.
    let mut block_placements = std::collections::HashSet::new();
    for offset in offsets {
        block_placements.insert(owners(block_addr, object, offset).await);
    }
    assert!(
        block_placements.len() > 1,
        "a block cluster must spread offsets across the fleet; got {block_placements:?}"
    );
}

/// Distinct objects must still spread. Pinning per object is affinity; pinning
/// every object to one node would be a hotspot.
#[tokio::test]
async fn an_async_cluster_still_spreads_distinct_objects_across_the_pool() {
    let addr = "127.0.0.1:7423";
    let _coordinator = coordinator(ClusterType::Async, addr, "127.0.0.1:8423").await;
    for id in ["ext-a", "ext-b", "ext-c", "ext-d"] {
        register(addr, id, NodeRole::AsyncWorker).await;
    }

    let mut seen = std::collections::HashSet::new();
    for i in 0..60u64 {
        let placed = owners(addr, &format!("part-{i:05}.parquet"), 0).await;
        assert_eq!(placed.len(), 1);
        seen.insert(placed[0].clone());
    }
    assert_eq!(
        seen.len(),
        4,
        "60 objects must reach all four workers, reached {seen:?}"
    );
}

/// A cluster with no workers answers "nobody" — never a node borrowed from
/// somewhere else. A substituted worker would accept the connection and then
/// fail the read, strictly worse than an empty answer the client can act on.
#[tokio::test]
async fn a_cluster_with_no_workers_yet_has_no_owners() {
    let addr = "127.0.0.1:7424";
    let _coordinator = coordinator(ClusterType::Async, addr, "127.0.0.1:8424").await;

    assert!(
        owners(addr, "part-00000.parquet", 0).await.is_empty(),
        "an empty async cluster produced an owner"
    );

    register(addr, "ext-a", NodeRole::AsyncWorker).await;
    assert_eq!(owners(addr, "part-00000.parquet", 0).await, vec!["ext-a"]);
}

/// Independent epochs are the concrete payoff of separate registries. When
/// both rings shared one membership, scaling the async pool changed the epoch
/// every block client was comparing against, forcing a fleet-wide placement
/// refresh for a change that could not have moved a single block.
#[tokio::test]
async fn churn_in_one_cluster_does_not_move_the_others_epoch() {
    let block_addr = "127.0.0.1:7425";
    let async_addr = "127.0.0.1:7435";
    let _block = coordinator(ClusterType::Block, block_addr, "127.0.0.1:8425").await;
    let _async = coordinator(ClusterType::Async, async_addr, "127.0.0.1:8435").await;
    register(block_addr, "blk-a", NodeRole::Worker).await;
    register(async_addr, "ext-a", NodeRole::AsyncWorker).await;

    let before = epoch_of(block_addr, "part-00000.parquet").await;

    // Scale the async pool. Nothing about block placement changed.
    for id in ["ext-b", "ext-c"] {
        register(async_addr, id, NodeRole::AsyncWorker).await;
    }
    assert_ne!(
        epoch_of(async_addr, "part-00000.parquet").await,
        before,
        "the async cluster's own epoch must track its membership"
    );
    assert_eq!(
        epoch_of(block_addr, "part-00000.parquet").await,
        before,
        "async churn moved the block cluster's epoch"
    );
}

/// An async worker serves reads and stats, not listings (ADR 0005 §8), and an
/// async cluster has no block worker to fall back to. Saying so is the point:
/// an empty listing is indistinguishable from an empty bucket, and a client
/// would cache that as fact.
#[tokio::test]
async fn listing_an_async_cluster_errors_rather_than_looking_empty() {
    let addr = "127.0.0.1:7427";
    let _coordinator = coordinator(ClusterType::Async, addr, "127.0.0.1:8427").await;
    register(addr, "ext-a", NodeRole::AsyncWorker).await;

    let reply = round_trip(
        addr,
        &ControlMessage::ListObjects {
            prefix: "s3/warehouse/sales".into(),
        },
    )
    .await;
    match reply {
        ControlMessage::Ack { ok: false, detail } => {
            let detail = detail.expect("the refusal must say why");
            assert!(
                detail.contains("listing"),
                "the refusal must name the limitation: {detail}"
            );
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// An async worker must reach the membership feed, or a client that resolves
/// an owner id to an address finds nothing and the lookup is useless.
#[tokio::test]
async fn an_async_worker_is_resolvable_through_the_membership_query() {
    let addr = "127.0.0.1:7426";
    let _coordinator = coordinator(ClusterType::Async, addr, "127.0.0.1:8426").await;
    register(addr, "ext-a", NodeRole::AsyncWorker).await;

    let owner = owners(addr, "part-00000.parquet", 0)
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
