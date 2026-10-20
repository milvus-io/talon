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
async fn cli_namespace_and_properties_reach_worker_without_tracing() {
    let broker = r#"{"url":"http://broker.example/credentials?token=a=b","request":{"csp":"aws","region":"us-west-2","bucket":"bucket","roleArn":"fixture"},"origin":{"endpoint":"storage.example","region":"us-west-2"}}"#;
    for (dynamic, namespace) in [
        (true, "analytics"),
        (false, "analytics"),
        (true, "default"),
        (false, "default"),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (header, payload) =
                talon_transport::read_frame(&mut stream, talon_transport::DEFAULT_READ_TIMEOUT)
                    .await
                    .unwrap();
            let (metadata, _) = talon_transport::envelope::decode(&header, &payload).unwrap();
            assert_eq!(
                metadata.namespace.unwrap_or_default().effective_name(),
                namespace
            );
            assert_eq!(
                header.version,
                if dynamic {
                    4
                } else if namespace == "default" {
                    1
                } else {
                    3
                }
            );
            if dynamic {
                let config = metadata.credential_broker.unwrap();
                assert_eq!(
                    config.config().url,
                    "http://broker.example/credentials?token=a=b"
                );
                assert_eq!(config.config().request["roleArn"], "fixture");
            } else {
                assert!(metadata.credential_broker.is_none());
            }
            let mut reply =
                FrameHeader::new(talon_transport::MsgType::GetRange, header.request_id, 4);
            reply.version = header.version;
            stream.write_all(&reply.encode()).await.unwrap();
            stream.write_all(b"data").await.unwrap();
        });
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_talon-client"));
        command
            .args([
                "--namespace",
                namespace,
                "--worker",
                &address.to_string(),
                "--path",
                "/s3/bucket/key",
                "--len",
                "4",
            ])
            .env_clear()
            .env("RUST_LOG", "off")
            .kill_on_drop(true);
        if dynamic {
            command
                .arg("--request-option")
                .arg(format!("credential_broker={broker}"));
        }
        let output = tokio::time::timeout(Duration::from_secs(10), command.output())
            .await
            .unwrap()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        server.await.unwrap();
    }
}

#[tokio::test]
async fn cli_rejects_missing_origin_settings_before_dispatch() {
    for suffix in [
        "",
        r#", "origin":null"#,
        r#", "origin":{}"#,
        r#", "origin":{"request_timeout_ms":17}"#,
        r#", "origin":{"endpoint":null}"#,
        r#", "origin":{"endpoint":""}"#,
        r#", "origin":{"endpoint":" \t\n"}"#,
        r#", "origin":{"endpoint":"storage.example"}"#,
        r#", "origin":{"endpoint":"storage.example","region":null}"#,
        r#", "origin":{"endpoint":"storage.example","region":""}"#,
        r#", "origin":{"endpoint":"storage.example","region":" \t\n"}"#,
    ] {
        let broker = format!(
            r#"credential_broker={{"url":"http://broker/PRIVATE_FIXTURE_MARKER","request":{{"csp":"aws","region":"broker-region"}}{suffix}}}"#
        );
        let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_talon-client"))
            .args([
                "--worker",
                "127.0.0.1:1",
                "--path",
                "/s3/bucket/key",
                "--len",
                "0",
                "--request-option",
                &broker,
            ])
            .env_clear()
            .env("RUST_LOG", "off")
            .kill_on_drop(true)
            .output()
            .await
            .unwrap();
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains("credential_broker origin "), "{error}");
        assert!(!error.contains("PRIVATE_FIXTURE_MARKER"));
    }
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
    assert!(matches!(
        ack,
        ControlMessage::NodeStatusAck {
            accepted: true,
            serving: true,
            ..
        }
    ));

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
        ControlMessage::MembershipList { view } => view
            .workers
            .into_iter()
            .map(|worker| talon_core::NodeInfo {
                id: talon_core::NodeId::new(worker.member.worker_id),
                address: match worker.state {
                    talon_core::worker_membership::InstanceState::Serving { address, .. } => {
                        address
                    }
                    _ => String::new(),
                },
                role: talon_core::NodeRole::Worker,
            })
            .collect::<Vec<_>>(),
        other => panic!("expected MembershipList, got {other:?}"),
    };
    let resolved = nodes.iter().find(|n| n.id == node.id).unwrap();
    assert_eq!(resolved.address, "127.0.0.1:9999");
}

/// Run the real CLI against the unified membership discovery protocol.
async fn retained_cli(
    view: talon_core::worker_membership::WorkerDiscovery,
    args: &[&str],
) -> std::process::Output {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header = [0; HEADER_LEN];
            socket.read_exact(&mut header).await.unwrap();
            let frame = FrameHeader::decode(&header).unwrap();
            let mut payload = vec![0; frame.length as usize];
            socket.read_exact(&mut payload).await.unwrap();
            let mut full = header.to_vec();
            full.extend(payload);
            let (_, request) = codec::decode(&full).unwrap();
            assert_eq!(request, ControlMessage::MembershipQuery {});
            let reply = ControlMessage::MembershipList { view };
            socket
                .write_all(&codec::encode(frame.request_id, &reply).unwrap())
                .await
                .unwrap();
        }
    });
    let args: Vec<_> = args.iter().map(|s| s.to_string()).collect();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(coordinator_bin().with_file_name(if cfg!(windows) {
            "talon-client.exe"
        } else {
            "talon-client"
        }))
        .args(["--coordinator", &address])
        .args(args)
        .output()
        .unwrap()
    })
    .await
    .unwrap();
    server.await.unwrap();
    output
}

#[tokio::test]
async fn retained_cli_uses_published_availability_without_age_checks() {
    use talon_core::worker_membership::*;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let mut view = WorkerDiscovery {
        topology_token: 1,
        state_token: 1,
        valid_for_ms: 500,
        workers: ["w1", "w2"]
            .into_iter()
            .map(|id| DiscoveredWorker {
                member: WorkerMember {
                    worker_id: id.into(),
                    zone: None,
                    retired: false,
                },
                state: InstanceState::Serving {
                    instance_id: format!("{id}-process"),
                    address: address.clone(),
                },
            })
            .collect(),
    };
    let block = BlockId::new(
        ObjectId::new(Backend::Azure, "container", "object"),
        0,
        256 << 20,
        Version::new("e2e-v1"),
    );
    let nodes: Vec<_> = view
        .workers
        .iter()
        .map(|w| NodeInfo {
            id: NodeId::new(&w.member.worker_id),
            address: address.clone(),
            role: NodeRole::Worker,
        })
        .collect();
    let table = talon_core::CachePlacementTable::new(&nodes);
    let owner = table.primary(&block).unwrap().id.0.clone();
    view.workers
        .iter_mut()
        .find(|w| w.member.worker_id == owner)
        .unwrap()
        .state = InstanceState::Offline;

    let members = retained_cli(view.clone(), &["--membership-only"]).await;
    assert!(members.status.success(), "{:?}", members);
    let stdout = String::from_utf8_lossy(&members.stdout);
    assert!(
        stdout.contains(&format!("member {owner} unavailable")),
        "{stdout}"
    );
    assert!(stdout.contains("member w1 ") && stdout.contains("member w2 "));

    let read_args = ["--path", "/az/container/object", "--len", "4"];
    let offline = retained_cli(view.clone(), &read_args).await;
    assert!(!offline.status.success());
    let error = String::from_utf8_lossy(&offline.stderr);
    assert!(
        error.contains("Unavailable") && error.contains(&owner),
        "{error}"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err(),
        "offline owner was replaced by an online member"
    );

    view.workers
        .iter_mut()
        .find(|w| w.member.worker_id == owner)
        .unwrap()
        .state = InstanceState::Serving {
        instance_id: "restarted".into(),
        address: address.clone(),
    };
    view.valid_for_ms = 0;
    let worker = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut header = [0; HEADER_LEN];
        socket.read_exact(&mut header).await.unwrap();
        let frame = FrameHeader::decode(&header).unwrap();
        let mut payload = vec![0; frame.length as usize];
        socket.read_exact(&mut payload).await.unwrap();
        let mut full = header.to_vec();
        full.extend(payload);
        let (_, request) = talon_transport::decode_request(&full).unwrap();
        assert_eq!(request.object, block.object);
        let mut reply = talon_transport::response_header_ok(frame.request_id, 4).to_vec();
        reply.extend([1, 2, 3, 4]);
        socket.write_all(&reply).await.unwrap();
    });
    let recovered = retained_cli(view, &read_args).await;
    assert!(recovered.status.success(), "{:?}", recovered);
    assert!(String::from_utf8_lossy(&recovered.stdout).contains("first 4 bytes (hex): 01020304"));
    worker.await.unwrap();
}
