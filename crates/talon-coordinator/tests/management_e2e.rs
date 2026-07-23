//! End-to-end test of the coordinator management plane over real HTTP.
//!
//! Launches the built `talon-coordinator` binary (memory backend), registers a
//! worker through the real control protocol, then drives the `/api/v1` REST
//! surface, the embedded UI, and the operational endpoints over plain TCP HTTP —
//! asserting the whole admin server composes end to end: the worker registered
//! on the control plane shows up in the API snapshot, the UI shell and its
//! assets are served with security headers, and health/metrics respond.
//!
//! A second coordinator launched with `TALON_COORDINATOR_AUTH_TOKEN` proves the
//! management security layer fails closed on `/api/v1` while leaving the
//! operational endpoints public.

use std::io::{Read, Write};
use std::net::TcpStream as StdTcpStream;
use std::process::{Child, Command};
use std::time::Duration;

use talon_core::{
    NodeHealth, NodeId, NodeInfo, NodeMetricsSnapshot, NodeRole, NodeStatus,
    NODE_STATUS_SCHEMA_VERSION,
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

/// A parsed HTTP response: status code, lowercased header lines, and body.
struct HttpResponse {
    status: u16,
    headers: String,
    body: String,
}

/// Minimal blocking HTTP/1.1 GET over TCP with an optional Authorization header.
fn http_get(addr: &str, path: &str, auth: Option<&str>) -> HttpResponse {
    let mut stream = StdTcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let auth_line = match auth {
        Some(token) => format!("Authorization: Bearer {token}\r\n"),
        None => String::new(),
    };
    let req =
        format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n{auth_line}Connection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).unwrap();
    let text = String::from_utf8_lossy(&raw).to_string();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let (head, body) = match text.split_once("\r\n\r\n") {
        Some((h, b)) => (h.to_lowercase(), b.to_string()),
        None => (text.to_lowercase(), String::new()),
    };
    HttpResponse {
        status,
        headers: head,
        body,
    }
}

async fn wait_for_listener(addr: &str) -> bool {
    for _ in 0..100 {
        if TcpStream::connect(addr).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn control_round_trip(addr: &str, msg: &ControlMessage) -> ControlMessage {
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

fn worker_status(cluster: &str, id: &str) -> NodeStatus {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    NodeStatus {
        schema_version: NODE_STATUS_SCHEMA_VERSION,
        cluster_id: cluster.into(),
        node: NodeInfo {
            id: NodeId::new(id),
            address: format!("{id}:7001"),
            role: NodeRole::Worker,
        },
        incarnation_id: format!("inc-{id}"),
        admin_address: Some(format!("{id}:8001")),
        build_version: "e2e".into(),
        started_at_unix_ms: now,
        reported_at_unix_ms: now,
        heartbeat_seq: 0,
        health: NodeHealth::Healthy,
        ready: true,
        metrics: NodeMetricsSnapshot {
            capacity_bytes: 1024,
            resident_bytes: 256,
            block_count: 4,
            ..Default::default()
        },
        labels: Default::default(),
    }
}

#[tokio::test]
async fn management_api_and_ui_serve_registered_cluster() {
    let control = "127.0.0.1:7451";
    let admin = "127.0.0.1:8451";
    let cluster = "e2e-mgmt";
    let child = Command::new(coordinator_bin())
        .args([
            "--listen",
            control,
            "--admin-listen",
            admin,
            "--cluster-id",
            cluster,
        ])
        .spawn()
        .unwrap();
    let _killer = Killer(child);
    assert!(wait_for_listener(control).await, "control plane not up");
    assert!(wait_for_listener(admin).await, "admin plane not up");

    // Register a worker via the real control protocol (status heartbeat writes
    // the shared store the API reads from).
    let ack = control_round_trip(
        control,
        &ControlMessage::NodeStatusHeartbeat {
            status: Box::new(worker_status(cluster, "worker-1")),
        },
    )
    .await;
    assert!(
        matches!(ack, ControlMessage::Ack { ok: true, .. }),
        "{ack:?}"
    );

    // Operational endpoints are public and healthy.
    assert_eq!(http_get(admin, "/healthz", None).status, 200);
    assert_eq!(http_get(admin, "/readyz", None).status, 200);
    let metrics = http_get(admin, "/metrics", None);
    assert_eq!(metrics.status, 200);
    assert!(metrics.body.contains("talon_coordinator_build_info"));

    // The management API reflects the registered worker.
    let cluster_resp = http_get(admin, "/api/v1/cluster", None);
    assert_eq!(cluster_resp.status, 200);
    assert!(
        cluster_resp.body.contains("\"worker_count\":1"),
        "{}",
        cluster_resp.body
    );
    assert!(cluster_resp.body.contains("\"cluster_id\":\"e2e-mgmt\""));

    let nodes = http_get(admin, "/api/v1/nodes", None);
    assert_eq!(nodes.status, 200);
    assert!(nodes.body.contains("\"node_id\":\"worker-1\""));

    let detail = http_get(admin, "/api/v1/nodes/worker-1", None);
    assert_eq!(detail.status, 200);
    assert!(detail.body.contains("\"role\":\"worker\""));
    assert!(detail.body.contains("\"capacity_bytes\":1024"));

    // A missing node is a clean 404.
    assert_eq!(http_get(admin, "/api/v1/nodes/absent", None).status, 404);

    // The OpenAPI contract is served.
    let openapi = http_get(admin, "/api/v1/openapi.json", None);
    assert_eq!(openapi.status, 200);
    assert!(openapi.body.contains("\"openapi\""));

    // The UI shell and an asset are served with security headers.
    let ui = http_get(admin, "/ui", None);
    assert_eq!(ui.status, 200);
    assert!(ui.headers.contains("content-security-policy"));
    assert!(ui.body.contains("id=\"app\""));
    let asset = http_get(admin, "/ui/assets/app.js", None);
    assert_eq!(asset.status, 200);
    assert!(asset.headers.contains("text/javascript"));
}

#[tokio::test]
async fn management_api_requires_auth_when_token_configured() {
    let control = "127.0.0.1:7452";
    let admin = "127.0.0.1:8452";
    let token = "an-adequately-long-e2e-token";
    let child = Command::new(coordinator_bin())
        .args(["--listen", control, "--admin-listen", admin])
        .env("TALON_COORDINATOR_AUTH_TOKEN", token)
        .spawn()
        .unwrap();
    let _killer = Killer(child);
    assert!(wait_for_listener(admin).await, "admin plane not up");

    // Operational endpoints stay public even with auth enabled.
    assert_eq!(http_get(admin, "/healthz", None).status, 200);
    assert_eq!(http_get(admin, "/metrics", None).status, 200);

    // Protected API fails closed without a token.
    let unauth = http_get(admin, "/api/v1/cluster", None);
    assert_eq!(unauth.status, 401);
    assert!(unauth.headers.contains("www-authenticate"));

    // A wrong token is rejected.
    assert_eq!(http_get(admin, "/api/v1/cluster", Some("nope")).status, 401);

    // The correct token is accepted and carries hardening headers.
    let ok = http_get(admin, "/api/v1/cluster", Some(token));
    assert_eq!(ok.status, 200);
    assert!(ok.headers.contains("x-frame-options: deny"));
    assert!(ok.headers.contains("x-content-type-options: nosniff"));
}
