//! Talon worker entry point.
//!
//! Registers with the coordinator, then serves data-plane range requests. On a
//! miss it fetches the block-aligned range from the configured Azure backend
//! over real HTTPS, commits it durably to the local block store, and serves the
//! requested sub-range. A subsequent request for the same block is a local hit.
//!
//! # Wiring
//!
//! - Control plane (register/heartbeat) reuses [`talon_transport::codec`].
//! - Data plane uses [`talon_transport::data`]: a
//!   [`talon_transport::data::RangeRequest`] in, raw bytes (or an
//!   `ERROR`-flagged frame) out.
//! - Backend fetch is the real [`AzureBackend`] over [`ReqwestClient`]; the SAS
//!   token is read from the environment and **never logged**.
//!
//! The response returns bytes inline for simplicity; the production hot path
//! serves them zero-copy via `sendfile` from the committed block file
//! ([`send_file_range`](talon_worker::send_file_range)).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use talon_backend::{AzureBackend, AzureConfig, ReqwestClient};
use talon_core::{
    azure_sas_from_env, BackendStore, NodeId, NodeInfo, NodeRole, WorkerConfig, WorkerConfigPatch,
};
use talon_transport::data;
use talon_transport::frame::{MsgType, HEADER_LEN};
use talon_transport::{codec, ControlMessage, FrameHeader};
use talon_worker::{
    serve_admin, BlockIndex, InFlightLoads, WholeBlockStore, WorkerObservability, WorkerRuntime,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const CONTROL_OPERATION_TIMEOUT: Duration = Duration::from_secs(3);

/// Upper bound on concurrent data-plane connections. Beyond this, new peers wait
/// for an in-flight connection to finish rather than each spawning an unbounded
/// task that could pin a payload buffer (issue #111).
const MAX_DATA_PLANE_CONNECTIONS: usize = 1024;

/// Command-line arguments for a Talon worker.
#[derive(Debug, Parser)]
#[command(name = "talon-worker", version, about)]
struct Args {
    /// Path to a TOML config file.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Address to bind the worker RPC service to.
    #[arg(long)]
    listen: Option<String>,
    /// Routable address advertised to clients (defaults to `listen`).
    #[arg(long)]
    advertise_addr: Option<String>,
    /// Address to bind the worker HTTP administration service to.
    #[arg(long)]
    admin_listen: Option<String>,
    /// Address of the coordinator to register with.
    #[arg(long)]
    coordinator: Option<String>,
    /// Logical cluster advertised by worker status.
    #[arg(long)]
    cluster_id: Option<String>,
    /// Stable node identity; defaults to the RPC listen address.
    #[arg(long)]
    node_id: Option<String>,
    /// Control-plane heartbeat interval in milliseconds.
    #[arg(long)]
    heartbeat_interval_ms: Option<u64>,
    /// Logical block size in bytes.
    #[arg(long)]
    block_size: Option<u32>,
}

impl Args {
    fn into_patch(self) -> WorkerConfigPatch {
        WorkerConfigPatch {
            listen: self.listen,
            advertise_addr: self.advertise_addr,
            admin_listen: self.admin_listen,
            coordinator: self.coordinator,
            cluster_id: self.cluster_id,
            node_id: self.node_id,
            heartbeat_interval_ms: self.heartbeat_interval_ms,
            block_size: self.block_size,
            cache_dirs: None,
            capacity_bytes: None,
            azure_account: None,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let file = match &args.config {
        Some(path) => WorkerConfigPatch::from_file(path)?,
        None => WorkerConfigPatch::default(),
    };
    let env = WorkerConfigPatch::from_env()?;
    let cli = args.into_patch();
    let cfg = WorkerConfig::resolve(file, env, cli)?;

    tracing::info!(
        listen = %cfg.listen,
        admin_listen = %cfg.admin_listen,
        coordinator = %cfg.coordinator,
        cluster_id = %cfg.cluster_id,
        node_id = ?cfg.node_id,
        heartbeat_interval_ms = cfg.heartbeat_interval_ms,
        block_size = cfg.block_size,
        cache_dirs = ?cfg.cache_dirs,
        capacity_bytes = cfg.capacity_bytes,
        azure_account = ?cfg.azure_account,
        "starting talon-worker"
    );

    // Build the Azure backend from account (config/env) + SAS (env only).
    let account = cfg.azure_account.clone().ok_or_else(|| {
        anyhow::anyhow!("azure_account is required (set TALON_WORKER_AZURE_ACCOUNT)")
    })?;
    let sas = azure_sas_from_env()
        .ok_or_else(|| anyhow::anyhow!("TALON_WORKER_AZURE_SAS must be set (SAS token)"))?;
    let http = Arc::new(ReqwestClient::new());
    let backend: Arc<dyn BackendStore> = Arc::new(AzureBackend::new(
        AzureConfig::new(account),
        Some(sas),
        http,
    ));

    // Local store stack rooted at the first cache dir.
    let root = cfg
        .cache_dirs
        .first()
        .cloned()
        .unwrap_or_else(|| PathBuf::from("/tmp/talon-cache"));
    std::fs::create_dir_all(&root)?;
    let store = WholeBlockStore::open(&root)?;

    // Rebuild the in-memory index from blocks already on local disk so a restart
    // does not re-download the resident working set (issue #114).
    let index = Arc::new(BlockIndex::new());
    match store.scan() {
        Ok(metas) => {
            let count = metas.len();
            for meta in metas {
                index.commit(meta);
            }
            if count > 0 {
                tracing::info!(
                    blocks = count,
                    resident_bytes = index.resident_bytes(),
                    "rebuilt block index from on-disk cache"
                );
            }
        }
        Err(error) => {
            tracing::warn!(%error, "failed to scan on-disk cache; starting with an empty index");
        }
    }
    let inflight = Arc::new(InFlightLoads::new());
    let node = NodeInfo {
        id: NodeId::new(
            cfg.node_id
                .clone()
                .unwrap_or_else(|| cfg.advertise_addr.clone()),
        ),
        // Advertise the routable address, not the (possibly wildcard) bind
        // address, so clients receive a connectable owner (issue #118).
        address: cfg.advertise_addr.clone(),
        role: NodeRole::Worker,
    };
    let observability = Arc::new(WorkerObservability::new(
        cfg.cluster_id.clone(),
        node.clone(),
        cfg.admin_listen.clone(),
        cfg.capacity_bytes,
        Arc::clone(&index),
        Arc::clone(&inflight),
    )?);
    observability.readiness().set_backend_ready(true);
    observability.readiness().set_store_ready(true);

    let worker = Arc::new(WorkerRuntime::new(
        store,
        index,
        inflight,
        backend,
        cfg.block_size,
        cfg.capacity_bytes,
        observability.metrics().clone(),
    ));

    let admin_listener = TcpListener::bind(&cfg.admin_listen).await?;
    tracing::info!(listen = %cfg.admin_listen, "worker serving administration API");
    let admin_observability = Arc::clone(&observability);
    tokio::spawn(async move {
        if let Err(error) = serve_admin(admin_listener, admin_observability).await {
            tracing::error!(%error, "worker administration server stopped");
        }
    });

    let _control_plane = spawn_control_plane(
        cfg.coordinator.clone(),
        node,
        Arc::clone(&worker),
        Arc::clone(&observability),
        Duration::from_millis(cfg.heartbeat_interval_ms),
    );

    // Serve the data plane.
    let listener = TcpListener::bind(&cfg.listen).await?;
    tracing::info!(listen = %cfg.listen, "worker serving data plane");
    // Bound concurrent connections so a flood of idle peers cannot exhaust
    // memory/FDs (issue #111).
    let conn_limit = talon_transport::ConnectionLimit::new(MAX_DATA_PLANE_CONNECTIONS);
    loop {
        let permit = conn_limit.acquire().await;
        let (stream, peer) = listener.accept().await?;
        let worker = Arc::clone(&worker);
        let observability = Arc::clone(&observability);
        tokio::spawn(async move {
            // Hold the permit for the connection's lifetime.
            let _permit = permit;
            if let Err(e) = handle_conn(stream, worker, observability).await {
                tracing::debug!(%peer, error = %e, "worker: connection ended");
            }
        });
    }
}

/// Open a control connection to the coordinator and send `Register`.
async fn register_with_coordinator(coordinator: &str, node: &NodeInfo) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(coordinator).await?;
    let buf = codec::encode(0, &ControlMessage::Register { node: node.clone() })?;
    stream.write_all(&buf).await?;
    stream.flush().await?;
    match read_control(&mut stream).await? {
        Some(ControlMessage::Ack {
            ok: true,
            detail: _,
        }) => {}
        Some(ControlMessage::Ack { ok: false, detail }) => {
            anyhow::bail!("coordinator rejected registration: {detail:?}")
        }
        Some(other) => anyhow::bail!("unexpected coordinator registration reply: {other:?}"),
        None => anyhow::bail!("coordinator closed registration connection without an Ack"),
    }
    tracing::info!(%coordinator, "registered with coordinator");
    Ok(())
}

/// Maintain registration and send legacy plus versioned status heartbeats.
fn spawn_control_plane(
    coordinator: String,
    node: NodeInfo,
    worker: Arc<WorkerRuntime>,
    observability: Arc<WorkerObservability>,
    heartbeat_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(heartbeat_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut registered = false;
        loop {
            ticker.tick().await;

            if !registered {
                match tokio::time::timeout(
                    CONTROL_OPERATION_TIMEOUT,
                    register_with_coordinator(&coordinator, &node),
                )
                .await
                {
                    Ok(Ok(())) => {
                        registered = true;
                        observability.readiness().set_control_registered(true);
                    }
                    Ok(Err(error)) => {
                        observability.metrics().record_heartbeat_failure();
                        observability.readiness().set_control_registered(false);
                        tracing::warn!(%error, "worker registration failed; retrying");
                        continue;
                    }
                    Err(_) => {
                        observability.metrics().record_heartbeat_failure();
                        observability.readiness().set_control_registered(false);
                        tracing::warn!("worker registration timed out; retrying");
                        continue;
                    }
                }
            }

            let legacy = ControlMessage::Heartbeat {
                node: node.id.clone(),
                block_count: worker.block_count(),
            };
            let status = ControlMessage::NodeStatusHeartbeat {
                status: Box::new(observability.status()),
            };
            let heartbeat = tokio::time::timeout(CONTROL_OPERATION_TIMEOUT, async {
                send_oneshot(&coordinator, &legacy).await?;
                send_oneshot(&coordinator, &status).await
            })
            .await;
            match heartbeat {
                Ok(Ok(())) => observability.metrics().record_heartbeat_success(),
                Ok(Err(error)) => {
                    registered = false;
                    observability.metrics().record_heartbeat_failure();
                    observability.readiness().set_control_registered(false);
                    tracing::warn!(%error, "control heartbeat failed; registration will retry");
                }
                Err(_) => {
                    registered = false;
                    observability.metrics().record_heartbeat_failure();
                    observability.readiness().set_control_registered(false);
                    tracing::warn!("control heartbeat timed out; registration will retry");
                }
            }
        }
    })
}

/// Connect, send one control message, and drop (fire-and-forget over TCP).
async fn send_oneshot(addr: &str, msg: &ControlMessage) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(addr).await?;
    let buf = codec::encode(0, msg)?;
    stream.write_all(&buf).await?;
    stream.flush().await?;
    Ok(())
}

/// Serve data-plane range requests on one connection until EOF.
async fn handle_conn(
    mut stream: TcpStream,
    worker: Arc<WorkerRuntime>,
    observability: Arc<WorkerObservability>,
) -> anyhow::Result<()> {
    let _active_connection = observability.metrics().track_connection();
    loop {
        let request_started = Instant::now();
        // Read one frame with a per-type size cap enforced BEFORE allocation and
        // a read timeout, so a peer cannot pin a 320 MiB buffer by advertising a
        // huge length and stalling (issue #111).
        let (header, payload) =
            match talon_transport::read_frame(&mut stream, talon_transport::DEFAULT_READ_TIMEOUT)
                .await
            {
                Ok(frame) => frame,
                Err(talon_transport::ReadFrameError::Eof) => return Ok(()),
                Err(talon_transport::ReadFrameError::Timeout) => {
                    tracing::debug!("worker: connection read timed out");
                    return Ok(());
                }
                Err(e) => return Err(anyhow::anyhow!(e)),
            };

        // Type check BEFORE any per-request work; a data listener only serves
        // GetRange, and non-data frames are already capped tightly by read_frame.
        if header.msg_type != MsgType::GetRange {
            let err = data::encode_error(header.request_id, "worker only serves GetRange");
            stream.write_all(&err).await?;
            stream.flush().await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
            continue;
        }

        let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
        full.extend_from_slice(&header.encode());
        full.extend_from_slice(&payload);
        let (h, req) = match data::decode_request(&full) {
            Ok(v) => v,
            Err(e) => {
                let err = data::encode_error(header.request_id, &format!("bad request: {e}"));
                stream.write_all(&err).await?;
                stream.flush().await?;
                observability
                    .metrics()
                    .record_request_error(request_started.elapsed());
                continue;
            }
        };

        if !observability.is_ready() {
            let err = data::encode_error(h.request_id, "worker is not ready");
            stream.write_all(&err).await?;
            stream.flush().await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
            continue;
        }

        match worker.serve_range(&req).await {
            Ok(bytes) => {
                let hdr = data::response_header_ok(h.request_id, bytes.len() as u32);
                stream.write_all(&hdr).await?;
                stream.write_all(&bytes).await?;
                stream.flush().await?;
                observability
                    .metrics()
                    .record_request_success(bytes.len() as u64, request_started.elapsed());
            }
            Err(e) => {
                let err = data::encode_error(h.request_id, &e.to_string());
                stream.write_all(&err).await?;
                stream.flush().await?;
                observability
                    .metrics()
                    .record_request_error(request_started.elapsed());
            }
        }
    }
}

/// Read one framed control message (header + payload). `Ok(None)` on clean EOF.
async fn read_control(stream: &mut TcpStream) -> anyhow::Result<Option<ControlMessage>> {
    let mut header_buf = [0u8; HEADER_LEN];
    match stream.read_exact(&mut header_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let header = FrameHeader::decode(&header_buf)?;
    let mut payload = vec![0u8; header.length as usize];
    stream.read_exact(&mut payload).await?;
    let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
    full.extend_from_slice(&header_buf);
    full.extend_from_slice(&payload);
    let (_h, msg) = codec::decode(&full)?;
    Ok(Some(msg))
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::path::PathBuf;
    use std::sync::atomic::AtomicUsize;
    use std::time::SystemTime;

    use async_trait::async_trait;
    use bytes::Bytes;
    use talon_core::{Error, ObjectId, ObjectStat, Result, Version};
    use tokio::sync::oneshot;

    use super::*;

    struct MockBackend {
        _calls: AtomicUsize,
    }

    #[async_trait]
    impl BackendStore for MockBackend {
        async fn fetch_range(&self, _object: &ObjectId, _offset: u64, _len: u64) -> Result<Bytes> {
            Err(Error::Backend("not used".into()))
        }

        async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
            Ok(ObjectStat {
                len: 0,
                version: Version::new("v1"),
            })
        }
    }

    #[tokio::test]
    async fn control_plane_sends_legacy_and_versioned_heartbeats() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let coordinator = listener.local_addr().unwrap();
        let (messages_tx, messages_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut messages = Vec::new();
            for _ in 0..3 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let message = read_control(&mut stream).await.unwrap().unwrap();
                if matches!(message, ControlMessage::Register { .. }) {
                    let ack = codec::encode(
                        0,
                        &ControlMessage::Ack {
                            ok: true,
                            detail: None,
                        },
                    )
                    .unwrap();
                    stream.write_all(&ack).await.unwrap();
                    stream.flush().await.unwrap();
                }
                messages.push(message);
            }
            messages_tx.send(messages).unwrap();
        });

        let (worker, observability, node, root) = test_worker();
        observability.readiness().set_backend_ready(true);
        observability.readiness().set_store_ready(true);
        let control = spawn_control_plane(
            coordinator.to_string(),
            node.clone(),
            worker,
            Arc::clone(&observability),
            Duration::from_secs(60),
        );

        let messages = tokio::time::timeout(Duration::from_secs(2), messages_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            &messages[0],
            ControlMessage::Register { node: registered } if registered == &node
        ));
        assert!(matches!(
            &messages[1],
            ControlMessage::Heartbeat {
                node: heartbeat_node,
                block_count: 0
            } if heartbeat_node == &node.id
        ));
        match &messages[2] {
            ControlMessage::NodeStatusHeartbeat { status } => {
                status.validate().unwrap();
                assert_eq!(status.node, node);
                assert!(status.ready);
            }
            other => panic!("unexpected status heartbeat: {other:?}"),
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(observability.is_ready());
        assert!(observability
            .metrics()
            .render()
            .contains("talon_worker_control_heartbeat_total{result=\"success\"} 1"));

        control.abort();
        server.await.unwrap();
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn registration_failure_keeps_worker_unready_and_is_counted() {
        let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let coordinator = unused.local_addr().unwrap();
        drop(unused);

        let (worker, observability, node, root) = test_worker();
        observability.readiness().set_backend_ready(true);
        observability.readiness().set_store_ready(true);
        let control = spawn_control_plane(
            coordinator.to_string(),
            node,
            worker,
            Arc::clone(&observability),
            Duration::from_secs(60),
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if observability
                    .metrics()
                    .render()
                    .contains("talon_worker_control_heartbeat_total{result=\"failure\"} 1")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(!observability.is_ready());

        control.abort();
        std::fs::remove_dir_all(root).ok();
    }

    fn test_worker() -> (
        Arc<WorkerRuntime>,
        Arc<WorkerObservability>,
        NodeInfo,
        PathBuf,
    ) {
        let root = tmp_root();
        let index = Arc::new(BlockIndex::new());
        let inflight = Arc::new(InFlightLoads::new());
        let node = NodeInfo {
            id: NodeId::new("worker-test"),
            address: "127.0.0.1:7001".into(),
            role: NodeRole::Worker,
        };
        let observability = Arc::new(
            WorkerObservability::new(
                "test-cluster".into(),
                node.clone(),
                "127.0.0.1:8001".into(),
                1024,
                Arc::clone(&index),
                Arc::clone(&inflight),
            )
            .unwrap(),
        );
        let backend: Arc<dyn BackendStore> = Arc::new(MockBackend {
            _calls: AtomicUsize::new(0),
        });
        let worker = Arc::new(WorkerRuntime::new(
            WholeBlockStore::open(&root).unwrap(),
            index,
            inflight,
            backend,
            8,
            0,
            observability.metrics().clone(),
        ));
        (worker, observability, node, root)
    }

    fn tmp_root() -> PathBuf {
        let mut hasher = DefaultHasher::new();
        SystemTime::now().hash(&mut hasher);
        std::thread::current().id().hash(&mut hasher);
        std::env::temp_dir().join(format!(
            "talon-control-{}-{}",
            std::process::id(),
            hasher.finish()
        ))
    }
}
