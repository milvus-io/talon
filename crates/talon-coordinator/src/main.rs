//! Talon coordinator control and administration servers.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use talon_coordinator::{
    serve_coordinator_admin, ClusterStateStore, CoordinatorConfig, CoordinatorConfigPatch,
    CoordinatorObservability, Membership, MemoryStateStore, PlacementService, RendezvousPlacement,
    StateBackend, WriteDisposition,
};
use talon_core::{NodeInfo, NodeRole};
use talon_transport::frame::HEADER_LEN;
use talon_transport::{codec, ControlMessage, FrameHeader};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, Parser)]
#[command(name = "talon-coordinator", version, about)]
struct Args {
    /// Path to a TOML configuration file.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Control-plane bind address.
    #[arg(long)]
    listen: Option<String>,
    /// Administration HTTP bind address.
    #[arg(long)]
    admin_listen: Option<String>,
    /// Administration address advertised in coordinator status.
    #[arg(long)]
    admin_advertise: Option<String>,
    /// Logical cluster identity.
    #[arg(long)]
    cluster_id: Option<String>,
    /// Stable coordinator node identity.
    #[arg(long)]
    node_id: Option<String>,
    /// Shared-state backend.
    #[arg(long, value_enum)]
    state_backend: Option<StateBackend>,
    /// Enable active-active coordinator mode.
    #[arg(long)]
    ha_enabled: Option<bool>,
    /// Expected coordinator replica count.
    #[arg(long)]
    coordinator_replicas: Option<u16>,
    /// Node heartbeat interval in milliseconds.
    #[arg(long)]
    heartbeat_interval_ms: Option<u64>,
    /// Node unhealthy threshold in milliseconds.
    #[arg(long)]
    unhealthy_after_ms: Option<u64>,
    /// Node lease TTL in milliseconds.
    #[arg(long)]
    lease_ttl_ms: Option<u64>,
    /// Shared-state request timeout in milliseconds.
    #[arg(long)]
    request_timeout_ms: Option<u64>,
}

impl Args {
    fn into_patch(self) -> CoordinatorConfigPatch {
        CoordinatorConfigPatch {
            listen: self.listen,
            admin_listen: self.admin_listen,
            admin_advertise: self.admin_advertise,
            cluster_id: self.cluster_id,
            node_id: self.node_id,
            state_backend: self.state_backend,
            ha_enabled: self.ha_enabled,
            coordinator_replicas: self.coordinator_replicas,
            heartbeat_interval_ms: self.heartbeat_interval_ms,
            unhealthy_after_ms: self.unhealthy_after_ms,
            lease_ttl_ms: self.lease_ttl_ms,
            request_timeout_ms: self.request_timeout_ms,
        }
    }
}

struct Coordinator {
    service: PlacementService<RendezvousPlacement>,
    observability: Arc<CoordinatorObservability>,
    lease_ttl: Duration,
}

impl Coordinator {
    fn new(observability: Arc<CoordinatorObservability>, lease_ttl: Duration) -> Arc<Self> {
        Arc::new(Self {
            service: PlacementService::new(Membership::new(), RendezvousPlacement),
            observability,
            lease_ttl,
        })
    }

    async fn dispatch(&self, message: ControlMessage) -> ControlMessage {
        match message {
            ControlMessage::Register { node } => {
                tracing::info!(id = %node.id, address = %node.address, "worker registered");
                self.service.membership().register(node);
                self.observability.metrics().record_registration(true);
                ControlMessage::Ack {
                    ok: true,
                    detail: None,
                }
            }
            ControlMessage::Heartbeat { node, block_count } => {
                tracing::debug!(%node, block_count, "legacy heartbeat");
                self.observability.metrics().record_heartbeat(false, true);
                ControlMessage::Ack {
                    ok: true,
                    detail: None,
                }
            }
            ControlMessage::NodeStatusHeartbeat { status } => {
                if status.cluster_id != self.observability.cluster_id() {
                    self.observability.metrics().record_heartbeat(true, false);
                    return ControlMessage::Ack {
                        ok: false,
                        detail: Some("node status belongs to another cluster".into()),
                    };
                }
                let node = status.node.clone();
                match self
                    .observability
                    .upsert_status(*status, self.lease_ttl)
                    .await
                {
                    Ok(result) => {
                        if result.disposition == WriteDisposition::Applied
                            && node.role == NodeRole::Worker
                        {
                            self.service.membership().register(node);
                        }
                        self.observability.metrics().record_heartbeat(true, true);
                        ControlMessage::Ack {
                            ok: true,
                            detail: None,
                        }
                    }
                    Err(error) => {
                        self.observability.metrics().record_heartbeat(true, false);
                        ControlMessage::Ack {
                            ok: false,
                            detail: Some(error.to_string()),
                        }
                    }
                }
            }
            lookup @ ControlMessage::PlacementLookup { .. } => self.service.handle(lookup),
            ControlMessage::MembershipQuery {} => ControlMessage::MembershipList {
                nodes: self.service.membership().snapshot(),
            },
            other => ControlMessage::Ack {
                ok: false,
                detail: Some(format!("unexpected control message: {other:?}")),
            },
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
        Some(path) => CoordinatorConfigPatch::from_file(path)?,
        None => CoordinatorConfigPatch::default(),
    };
    let config =
        CoordinatorConfig::resolve(file, CoordinatorConfigPatch::from_env()?, args.into_patch())?;
    if config.state.backend != StateBackend::Memory {
        anyhow::bail!(
            "{} state backend is configured but its implementation is not installed",
            config.state.backend
        );
    }

    tracing::info!(
        listen = %config.listen,
        admin_listen = %config.admin_listen,
        admin_advertise = %config.admin_advertise,
        cluster_id = %config.cluster_id,
        node_id = %config.node_id,
        state_backend = %config.state.backend,
        "starting talon-coordinator"
    );

    let store: Arc<dyn ClusterStateStore> = Arc::new(MemoryStateStore::new());
    let node = NodeInfo {
        id: talon_core::NodeId::new(config.node_id.clone()),
        address: config.listen.clone(),
        role: NodeRole::Coordinator,
    };
    let observability = Arc::new(CoordinatorObservability::new(
        config.cluster_id.clone(),
        node,
        config.admin_advertise.clone(),
        Duration::from_millis(config.state.request_timeout_ms),
        store,
    )?);
    observability.check_ready().await?;
    let state = Coordinator::new(
        Arc::clone(&observability),
        Duration::from_millis(config.state.lease_ttl_ms),
    );

    let admin_listener = TcpListener::bind(&config.admin_listen).await?;
    let admin_state = Arc::clone(&observability);
    tokio::spawn(async move {
        if let Err(error) = serve_coordinator_admin(admin_listener, admin_state).await {
            tracing::error!(%error, "coordinator administration server stopped");
        }
    });
    spawn_self_heartbeat(
        Arc::clone(&observability),
        Duration::from_millis(config.state.heartbeat_interval_ms),
        Duration::from_millis(config.state.lease_ttl_ms),
    );

    let listener = TcpListener::bind(&config.listen).await?;
    tracing::info!(listen = %config.listen, "coordinator serving control plane");
    loop {
        let (stream, peer) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(error) = handle_conn(stream, state).await {
                tracing::debug!(%peer, %error, "coordinator connection ended");
            }
        });
    }
}

fn spawn_self_heartbeat(
    observability: Arc<CoordinatorObservability>,
    interval: Duration,
    lease_ttl: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Err(error) = observability
                .upsert_status(observability.status(), lease_ttl)
                .await
            {
                tracing::warn!(%error, "coordinator status heartbeat failed");
            }
        }
    })
}

async fn handle_conn(mut stream: TcpStream, state: Arc<Coordinator>) -> anyhow::Result<()> {
    let _connection = state.observability.metrics().track_connection();
    loop {
        let message = match read_control(&mut stream).await {
            Ok(Some((_header, message))) => message,
            Ok(None) => return Ok(()),
            Err(error) => {
                state.observability.metrics().record_protocol_error();
                return Err(error);
            }
        };
        let operation = talon_coordinator::ControlOperation::from_message(&message);
        let started = Instant::now();
        let reply = state.dispatch(message).await;
        let error = matches!(&reply, ControlMessage::Ack { ok: false, .. });
        state
            .observability
            .metrics()
            .record_control(operation, error, started.elapsed());
        if matches!(operation, talon_coordinator::ControlOperation::Placement) {
            state
                .observability
                .metrics()
                .record_placement(error, started.elapsed());
        }
        let buffer = codec::encode(0, &reply)?;
        stream.write_all(&buffer).await?;
        stream.flush().await?;
    }
}

async fn read_control(
    stream: &mut TcpStream,
) -> anyhow::Result<Option<(FrameHeader, ControlMessage)>> {
    let mut header_buffer = [0u8; HEADER_LEN];
    match stream.read_exact(&mut header_buffer).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let header = FrameHeader::decode(&header_buffer)?;
    let mut payload = vec![0u8; header.length as usize];
    stream.read_exact(&mut payload).await?;
    let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
    full.extend_from_slice(&header_buffer);
    full.extend_from_slice(&payload);
    let (header, message) = codec::decode(&full)?;
    Ok(Some((header, message)))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use talon_coordinator::MemoryStateStore;
    use talon_core::{
        NodeHealth, NodeId, NodeMetricsSnapshot, NodeStatus, NODE_STATUS_SCHEMA_VERSION,
    };

    use super::*;

    #[tokio::test]
    async fn status_heartbeat_updates_store_and_worker_membership() {
        let store: Arc<dyn ClusterStateStore> = Arc::new(MemoryStateStore::new());
        let observability = Arc::new(
            CoordinatorObservability::new(
                "cluster-a".into(),
                NodeInfo {
                    id: NodeId::new("coordinator-1"),
                    address: "127.0.0.1:7000".into(),
                    role: NodeRole::Coordinator,
                },
                "127.0.0.1:8000".into(),
                Duration::from_secs(1),
                store,
            )
            .unwrap(),
        );
        observability.check_ready().await.unwrap();
        let coordinator = Coordinator::new(Arc::clone(&observability), Duration::from_secs(30));
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let status = NodeStatus {
            schema_version: NODE_STATUS_SCHEMA_VERSION,
            cluster_id: "cluster-a".into(),
            node: NodeInfo {
                id: NodeId::new("worker-1"),
                address: "127.0.0.1:7001".into(),
                role: NodeRole::Worker,
            },
            incarnation_id: "worker-incarnation".into(),
            admin_address: Some("127.0.0.1:8001".into()),
            build_version: "test".into(),
            started_at_unix_ms: now,
            reported_at_unix_ms: now,
            heartbeat_seq: 0,
            health: NodeHealth::Healthy,
            ready: true,
            metrics: NodeMetricsSnapshot::default(),
            labels: BTreeMap::new(),
        };

        let reply = coordinator
            .dispatch(ControlMessage::NodeStatusHeartbeat {
                status: Box::new(status),
            })
            .await;
        assert!(matches!(reply, ControlMessage::Ack { ok: true, .. }));
        assert_eq!(coordinator.service.membership().snapshot().len(), 1);
        assert_eq!(
            observability
                .store()
                .snapshot("cluster-a")
                .await
                .unwrap()
                .nodes
                .len(),
            1
        );
    }
}
