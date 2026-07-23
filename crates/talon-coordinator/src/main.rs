//! Talon coordinator control and administration servers.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use talon_coordinator::{
    ClusterStateStore, CoordinatorConfig, CoordinatorConfigPatch, CoordinatorObservability,
    Membership, MemoryStateStore, PlacementService, RendezvousPlacement, StateBackend,
    WriteDisposition,
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
            // Backend blocks come from the config file / environment, not CLI
            // flags. Feature-gated fields default to None here.
            ..Default::default()
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
            lookup @ ControlMessage::PlacementLookup { .. } => {
                // Fail closed: without a fresh authoritative snapshot we must not
                // answer placement from possibly-stale local membership (#73).
                if !self.observability.is_ready() {
                    return ControlMessage::Ack {
                        ok: false,
                        detail: Some("coordinator not ready: shared state unavailable".into()),
                    };
                }
                self.service.handle(lookup)
            }
            ControlMessage::MembershipQuery {} => {
                if !self.observability.is_ready() {
                    return ControlMessage::Ack {
                        ok: false,
                        detail: Some("coordinator not ready: shared state unavailable".into()),
                    };
                }
                ControlMessage::MembershipList {
                    nodes: self.service.membership().snapshot(),
                }
            }
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

    tracing::info!(
        listen = %config.listen,
        admin_listen = %config.admin_listen,
        admin_advertise = %config.admin_advertise,
        cluster_id = %config.cluster_id,
        node_id = %config.node_id,
        state_backend = %config.state.backend,
        "starting talon-coordinator"
    );

    let store: Arc<dyn ClusterStateStore> = build_store(&config).await?;
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

    // Management security (#85): auth mode from the environment. A bearer token
    // in TALON_COORDINATOR_AUTH_TOKEN enables authentication on /api/v1 and the
    // UI; health/metrics stay public. TLS is reverse-proxy terminated.
    let security = Arc::new(build_security_config()?);
    if security.auth_enabled() {
        tracing::info!("management authentication: bearer token enabled");
    } else {
        tracing::warn!(
            "management authentication is DISABLED; protect /api/v1 and the UI \
             behind a trusted proxy or set TALON_COORDINATOR_AUTH_TOKEN"
        );
    }

    let admin_listener = TcpListener::bind(&config.admin_listen).await?;
    let admin_state = Arc::clone(&observability);
    let admin_security = Arc::clone(&security);
    tokio::spawn(async move {
        if let Err(error) = talon_coordinator::observability::serve_admin_secured(
            admin_listener,
            admin_state,
            admin_security,
        )
        .await
        {
            tracing::error!(%error, "coordinator administration server stopped");
        }
    });
    spawn_self_heartbeat(
        Arc::clone(&observability),
        Duration::from_millis(config.state.heartbeat_interval_ms),
        Duration::from_millis(config.state.lease_ttl_ms),
    );
    // Keep local placement membership reconciled from shared state so this
    // coordinator serves the same node set as its peers (active-active).
    spawn_membership_reconcile(
        Arc::clone(&observability),
        Arc::clone(&state),
        Duration::from_millis(config.state.heartbeat_interval_ms),
    );

    let listener = TcpListener::bind(&config.listen).await?;
    tracing::info!(listen = %config.listen, "coordinator serving control plane");
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    if let Err(error) = handle_conn(stream, state).await {
                        tracing::debug!(%peer, %error, "coordinator connection ended");
                    }
                });
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("SIGINT received; draining and releasing coordinator lease");
                observability.begin_shutdown();
                // Best-effort: remove our own lease so peers see us leave promptly
                // instead of waiting out the TTL.
                if let Err(error) = observability.remove_self().await {
                    tracing::warn!(%error, "failed to release coordinator lease on shutdown");
                }
                return Ok(());
            }
        }
    }
}

/// Construct the shared cluster-state store selected by configuration.
///
/// The memory backend is always available for development. The etcd and
/// Kubernetes backends are compiled in only when their features are enabled;
/// selecting one in a binary built without the matching feature is rejected at
/// configuration validation time, so the `not(feature)` arms here are
/// unreachable in practice and exist only to keep the match total.
async fn build_store(config: &CoordinatorConfig) -> anyhow::Result<Arc<dyn ClusterStateStore>> {
    // Only the production backends consume the request timeout; suppress the
    // unused-binding warning in builds without either feature.
    #[cfg_attr(
        not(any(feature = "etcd", feature = "kubernetes")),
        allow(unused_variables)
    )]
    let request_timeout = Duration::from_millis(config.state.request_timeout_ms);
    match config.state.backend {
        StateBackend::Memory => Ok(Arc::new(MemoryStateStore::new())),
        StateBackend::Etcd => {
            #[cfg(feature = "etcd")]
            {
                let etcd = config.etcd.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("etcd backend selected without [etcd] config")
                })?;
                let lease_ttl = Duration::from_millis(config.state.lease_ttl_ms);
                let store =
                    talon_coordinator::EtcdStateStore::connect(etcd, lease_ttl, request_timeout)
                        .await?;
                Ok(Arc::new(store))
            }
            #[cfg(not(feature = "etcd"))]
            anyhow::bail!(
                "etcd backend selected but this binary was built without the etcd feature"
            )
        }
        StateBackend::Kubernetes => {
            #[cfg(feature = "kubernetes")]
            {
                let kubernetes = config.kubernetes.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("kubernetes backend selected without [kubernetes] config")
                })?;
                let store =
                    talon_coordinator::KubernetesStateStore::connect(kubernetes, request_timeout)
                        .await?;
                Ok(Arc::new(store))
            }
            #[cfg(not(feature = "kubernetes"))]
            anyhow::bail!(
                "kubernetes backend selected but this binary was built without the kubernetes \
                 feature"
            )
        }
    }
}

/// Build the management security configuration from the environment (#85).
///
/// `TALON_COORDINATOR_AUTH_TOKEN` (>= 16 chars) enables bearer-token auth;
/// unset means authentication is disabled (proxy-terminated deployments).
/// `TALON_COORDINATOR_TRUST_FORWARDED=1` honors `X-Forwarded-For` for audit
/// attribution behind a trusted proxy. TLS is reverse-proxy terminated.
fn build_security_config() -> anyhow::Result<talon_coordinator::security::SecurityConfig> {
    use talon_coordinator::security::{AuthMode, SecurityConfig};
    let auth = match std::env::var("TALON_COORDINATOR_AUTH_TOKEN") {
        Ok(token) if !token.is_empty() => AuthMode::BearerToken { token },
        _ => AuthMode::Disabled,
    };
    let trust_forwarded_headers = std::env::var("TALON_COORDINATOR_TRUST_FORWARDED")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let config = SecurityConfig {
        auth,
        trust_forwarded_headers,
    };
    config
        .validate()
        .map_err(|error| anyhow::anyhow!("invalid management security configuration: {error}"))?;
    Ok(config)
}

fn spawn_membership_reconcile(
    observability: Arc<CoordinatorObservability>,
    state: Arc<Coordinator>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Err(error) = observability
                .reconcile_membership(state.service.membership())
                .await
            {
                // Non-fatal: local membership is left last-good and readiness is
                // cleared, so placement fails closed until the store recovers.
                tracing::warn!(%error, "membership reconcile from shared state failed");
            }
        }
    })
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

    fn worker_status(cluster: &str, id: &str, incarnation: &str, addr: &str) -> NodeStatus {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        NodeStatus {
            schema_version: NODE_STATUS_SCHEMA_VERSION,
            cluster_id: cluster.into(),
            node: NodeInfo {
                id: NodeId::new(id),
                address: addr.into(),
                role: NodeRole::Worker,
            },
            incarnation_id: incarnation.into(),
            admin_address: Some("127.0.0.1:9001".into()),
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

    fn observability_over(
        store: Arc<dyn ClusterStateStore>,
        node_id: &str,
    ) -> Arc<CoordinatorObservability> {
        Arc::new(
            CoordinatorObservability::new(
                "cluster-a".into(),
                NodeInfo {
                    id: NodeId::new(node_id),
                    address: format!("127.0.0.1:70{}", node_id.len()),
                    role: NodeRole::Coordinator,
                },
                "127.0.0.1:8000".into(),
                Duration::from_secs(1),
                store,
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn worker_registered_on_one_coordinator_is_visible_through_another() {
        // Two coordinators share one backend. A worker heartbeat lands on A; B
        // must observe it after reconciling from shared state, and both derive
        // the same deterministic placement version (#80/#81).
        let store: Arc<dyn ClusterStateStore> = Arc::new(MemoryStateStore::new());
        let obs_a = observability_over(Arc::clone(&store), "coord-a");
        let obs_b = observability_over(Arc::clone(&store), "coord-b");
        obs_a.check_ready().await.unwrap();
        obs_b.check_ready().await.unwrap();
        let coord_a = Coordinator::new(Arc::clone(&obs_a), Duration::from_secs(30));
        let coord_b = Coordinator::new(Arc::clone(&obs_b), Duration::from_secs(30));

        let reply = coord_a
            .dispatch(ControlMessage::NodeStatusHeartbeat {
                status: Box::new(worker_status(
                    "cluster-a",
                    "worker-1",
                    "inc-1",
                    "127.0.0.1:7001",
                )),
            })
            .await;
        assert!(matches!(reply, ControlMessage::Ack { ok: true, .. }));

        // B has not seen the worker locally yet.
        assert_eq!(coord_b.service.membership().snapshot().len(), 0);
        // After B reconciles from the shared store, it sees the worker.
        obs_b
            .reconcile_membership(coord_b.service.membership())
            .await
            .unwrap();
        assert_eq!(coord_b.service.membership().snapshot().len(), 1);

        // Both coordinators now compute the identical placement version.
        obs_a
            .reconcile_membership(coord_a.service.membership())
            .await
            .unwrap();
        assert_eq!(
            coord_a.service.membership().epoch(),
            coord_b.service.membership().epoch()
        );
    }

    #[tokio::test]
    async fn reads_fail_closed_when_state_store_unavailable() {
        // With shared state unavailable the coordinator must not answer placement
        // or membership from stale local state (#73).
        let store = Arc::new(MemoryStateStore::new());
        let obs = observability_over(Arc::clone(&store) as Arc<dyn ClusterStateStore>, "coord-a");
        obs.check_ready().await.unwrap();
        let coord = Coordinator::new(Arc::clone(&obs), Duration::from_secs(30));
        // Seed a worker so a "leaky" implementation would have something to serve.
        coord
            .dispatch(ControlMessage::NodeStatusHeartbeat {
                status: Box::new(worker_status(
                    "cluster-a",
                    "worker-1",
                    "inc-1",
                    "127.0.0.1:7001",
                )),
            })
            .await;

        // Inject a store outage; the next reconcile clears readiness.
        store.set_available(false);
        let _ = obs.reconcile_membership(coord.service.membership()).await;
        assert!(!obs.is_ready());

        let placement = coord
            .dispatch(ControlMessage::PlacementLookup {
                block: sample_block(),
                k: 1,
            })
            .await;
        assert!(matches!(placement, ControlMessage::Ack { ok: false, .. }));
        let membership = coord.dispatch(ControlMessage::MembershipQuery {}).await;
        assert!(matches!(membership, ControlMessage::Ack { ok: false, .. }));

        // Recovery restores service.
        store.set_available(true);
        obs.reconcile_membership(coord.service.membership())
            .await
            .unwrap();
        assert!(obs.is_ready());
        let placement = coord
            .dispatch(ControlMessage::PlacementLookup {
                block: sample_block(),
                k: 1,
            })
            .await;
        assert!(matches!(
            placement,
            ControlMessage::PlacementResponse { .. }
        ));
    }

    #[tokio::test]
    async fn graceful_shutdown_releases_lease_and_stops_serving() {
        let store: Arc<dyn ClusterStateStore> = Arc::new(MemoryStateStore::new());
        let obs = observability_over(Arc::clone(&store), "coord-a");
        obs.check_ready().await.unwrap();
        // The coordinator has registered its own lease via a heartbeat.
        obs.upsert_status(obs.status(), Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(store.snapshot("cluster-a").await.unwrap().nodes.len(), 1);

        obs.begin_shutdown();
        assert!(!obs.is_ready(), "shutting-down coordinator is not ready");
        let removed = obs.remove_self().await.unwrap();
        assert_eq!(removed.disposition, WriteDisposition::Applied);
        assert_eq!(store.snapshot("cluster-a").await.unwrap().nodes.len(), 0);
    }

    fn sample_block() -> talon_core::BlockId {
        talon_core::BlockId::new(
            talon_core::ObjectId::new(talon_core::Backend::S3, "b", "o/1"),
            0,
            256 << 20,
            talon_core::Version::new("v1"),
        )
    }
}
