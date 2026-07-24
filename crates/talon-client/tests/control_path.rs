//! Integration test: the coordinator serve loop over real TCP.
//!
//! Launches the built `talon-coordinator` binary, registers a mock worker via
//! the real control protocol (`NodeStatusHeartbeat`, the store-authoritative
//! path), then exercises the two client-side lookups (`PlacementLookup` +
//! `MembershipQuery`) and asserts the owner id resolves back to the worker's
//! address. This covers the whole control path end-to-end without needing Azure
//! credentials or a running worker/data plane.

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

/// Kill the coordinator child on drop so a failing assert can't leak it.
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
    dir.pop(); // drop test exe name
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

#[tokio::test]
async fn control_path_register_lookup_resolve() {
    let addr = "127.0.0.1:7411";
    let bin = coordinator_bin();
    let child = Command::new(&bin).args(["--listen", addr]).spawn().unwrap();
    let _killer = Killer(child);

    // Wait for the listener to come up.
    let mut connected = false;
    for _ in 0..50 {
        if TcpStream::connect(addr).await.is_ok() {
            connected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(connected, "coordinator did not start listening");

    // A mock worker registers itself via the store-authoritative heartbeat
    // (the legacy Register path is now a membership no-op, #167).
    let node = NodeInfo {
        id: NodeId::new("127.0.0.1:9999"),
        address: "127.0.0.1:9999".into(),
        role: NodeRole::Worker,
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let status = NodeStatus {
        schema_version: NODE_STATUS_SCHEMA_VERSION,
        // The coordinator binary defaults to cluster_id "default".
        cluster_id: "default".into(),
        node: node.clone(),
        incarnation_id: "e2e-incarnation".into(),
        admin_address: Some("127.0.0.1:8999".into()),
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
    assert!(matches!(ack, ControlMessage::Ack { ok: true, .. }));

    // Placement lookup should now name our worker as the owner.
    let block = BlockId::new(
        ObjectId::new(Backend::Azure, "container", "path/blob.bin"),
        0,
        256 << 20,
        Version::new("e2e-v1"),
    );
    let owners = match round_trip(addr, &ControlMessage::PlacementLookup { block, k: 1 }).await {
        ControlMessage::PlacementResponse { owners, .. } => owners,
        other => panic!("expected PlacementResponse, got {other:?}"),
    };
    assert_eq!(owners, vec![NodeId::new("127.0.0.1:9999")]);

    // Membership query resolves that id back to the worker's address.
    let nodes = match round_trip(addr, &ControlMessage::MembershipQuery {}).await {
        ControlMessage::MembershipList { nodes } => nodes,
        other => panic!("expected MembershipList, got {other:?}"),
    };
    let resolved = nodes.iter().find(|n| n.id == node.id).unwrap();
    assert_eq!(resolved.address, "127.0.0.1:9999");
}
