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
use talon_metadata::ClusterCapabilities;
#[cfg(feature = "etcd")]
use talon_metadata::MetadataStore as _;
use talon_transport::frame::HEADER_LEN;
use talon_transport::{codec, ControlMessage, FrameHeader};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

/// Upper bound on concurrent control-plane connections (issue #111). Beyond
/// this, new peers wait for an in-flight connection to finish.
const MAX_CONTROL_CONNECTIONS: usize = 1024;

/// Bound on establishing a proxied worker connection (#318). Keep this short:
/// trying the next worker beats waiting on an unreachable one.
const PROXY_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Default total budget for trying all workers for one proxied request.
const PROXY_REQUEST_BUDGET: Duration = Duration::from_secs(5);

/// Total budget for trying all workers for a listing. Listing drains multiple
/// backend pages, so it needs more time than single-operation RPCs. This is one
/// shared deadline across retries—not 25 seconds per worker—and leaves headroom
/// inside the FUSE client's default 30-second coordinator exchange deadline.
const LIST_OBJECTS_PROXY_BUDGET: Duration = Duration::from_secs(25);

/// Minimum time preserved for each worker that has not yet been attempted.
/// When the request budget cannot cover this reserve, attempts share the
/// remaining time evenly instead.
const MIN_PROXY_RETRY_RESERVE: Duration = Duration::from_secs(1);

/// Keep aggregated worker diagnostics comfortably below the 1 MiB control
/// payload cap even when a worker returns an unusually large rejection detail.
const MAX_PROXY_ATTEMPT_ERRORS_BYTES: usize = 8 * 1024;

/// Prevent one oversized worker rejection from consuming the whole aggregate
/// and hiding diagnostics from workers attempted afterward.
const MAX_PROXY_ATTEMPT_ERROR_BYTES: usize = 1024;

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
    // With neither `etcd` nor `kubernetes` enabled, every remaining field is
    // listed explicitly below and the struct update becomes a no-op that clippy
    // rejects under -D warnings. It is still required when either feature is
    // on, so it cannot simply be deleted -- hence the allow rather than a fix.
    #[allow(clippy::needless_update)]
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

/// Connect the optional metadata store and derive what this cluster advertises.
///
/// A failure to reach a *configured* store is not fatal. ADR 0003 §6:
///
/// > TMS unavailability degrades TMS-backed features; it must not affect the
/// > read path. Reads, cache hits and misses, placement lookups, and
/// > write-through writes are unaffected. None consults TMS.
///
/// So an unreachable store still advertises its capabilities, with
/// `store_reachable` false. Refusing to start would turn a metadata outage into
/// a cache outage, which is the opposite of what §6 requires.
async fn build_capabilities(config: &CoordinatorConfig) -> ClusterCapabilities {
    #[cfg(feature = "etcd")]
    {
        let Some(metadata) = config.metadata.as_ref() else {
            return ClusterCapabilities::none();
        };
        let store_config = talon_metadata::EtcdMetadataConfig {
            endpoints: metadata.endpoints.clone(),
            prefix: metadata.prefix.clone(),
        };
        match talon_metadata::EtcdMetadataStore::connect(&store_config).await {
            Ok(store) => {
                let advertised = store.capabilities();
                let health = store.check_ready().await;
                let store_reachable = health.as_ref().map(|h| h.ready).unwrap_or(false);
                if store_reachable {
                    tracing::info!(
                        capabilities = %advertised,
                        prefix = %metadata.prefix,
                        "metadata store connected"
                    );
                } else {
                    // Distinct from the "not configured" path below, as §6
                    // requires: an operator must be able to tell an outage from
                    // a deployment choice.
                    tracing::warn!(
                        capabilities = %advertised,
                        prefix = %metadata.prefix,
                        "metadata store configured but not ready; \
                         TMS-backed features will fail closed"
                    );
                }
                ClusterCapabilities {
                    advertised,
                    revision: talon_metadata::CapabilityRevision::new(1),
                    store_reachable,
                }
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    prefix = %metadata.prefix,
                    "metadata store configured but unreachable; \
                     TMS-backed features will fail closed"
                );
                // The capabilities an etcd-backed store would offer, reported as
                // unreachable rather than absent. Dropping them here would make
                // this indistinguishable from an unconfigured cluster and send
                // clients the wrong errno (§4).
                ClusterCapabilities {
                    advertised: talon_metadata::CapabilitySet::none()
                        .with(talon_metadata::Capability::HardLinks),
                    revision: talon_metadata::CapabilityRevision::new(1),
                    store_reachable: false,
                }
            }
        }
    }
    #[cfg(not(feature = "etcd"))]
    {
        let _ = config;
        ClusterCapabilities::none()
    }
}

struct Coordinator {
    service: PlacementService<RendezvousPlacement>,
    observability: Arc<CoordinatorObservability>,
    lease_ttl: Duration,
}

fn proxy_request_budget(message: &ControlMessage) -> Duration {
    match message {
        ControlMessage::ListObjects { .. } => LIST_OBJECTS_PROXY_BUDGET,
        _ => PROXY_REQUEST_BUDGET,
    }
}

/// Give one serial attempt most of the time still available while preserving a
/// minimum retry window for every worker that follows.
///
/// This lets a listing use nearly all of its 25-second budget to drain backend
/// pages: with two workers the first gets up to 24 seconds, rather than an even
/// 12.5-second split. A silent worker still cannot consume the one-second retry
/// reserve. Fast failures donate all unused time to later workers. When the
/// remaining budget is tight, clamp each reservation to the current fair share;
/// this degrades smoothly to an even split without starving the current worker.
fn proxy_attempt_budget(remaining: Duration, workers_remaining: usize) -> Duration {
    debug_assert!(workers_remaining > 0);
    let divisor = u32::try_from(workers_remaining).unwrap_or(u32::MAX);
    let later_workers = divisor.saturating_sub(1);
    let fair_share = remaining / divisor;
    let reserve_per_later = MIN_PROXY_RETRY_RESERVE.min(fair_share);
    let retry_reserve = reserve_per_later.saturating_mul(later_workers);
    remaining.saturating_sub(retry_reserve)
}

fn append_proxy_attempt_error(errors: &mut String, error: &str) {
    let separator = if errors.is_empty() { "" } else { "; " };
    let remaining = MAX_PROXY_ATTEMPT_ERRORS_BYTES.saturating_sub(errors.len());
    if remaining <= separator.len() {
        return;
    }
    errors.push_str(separator);

    let remaining =
        (MAX_PROXY_ATTEMPT_ERRORS_BYTES - errors.len()).min(MAX_PROXY_ATTEMPT_ERROR_BYTES);
    if error.len() <= remaining {
        errors.push_str(error);
        return;
    }

    const TRUNCATED: &str = "...";
    let mut end = remaining.saturating_sub(TRUNCATED.len()).min(error.len());
    while !error.is_char_boundary(end) {
        end -= 1;
    }
    errors.push_str(&error[..end]);
    if remaining >= TRUNCATED.len() {
        errors.push_str(TRUNCATED);
    }
}

impl Coordinator {
    fn new(observability: Arc<CoordinatorObservability>, lease_ttl: Duration) -> Arc<Self> {
        Arc::new(Self {
            service: PlacementService::new(Membership::new(), RendezvousPlacement),
            observability,
            lease_ttl,
        })
    }

    /// Forward a message to any healthy worker and return its reply (#318).
    ///
    /// `StatObject` needs backend credentials, which only workers hold. Giving
    /// the coordinator its own credentials would duplicate secret distribution
    /// for one read-only call, so it proxies instead.
    ///
    /// Any worker will do: a stat is independent of where the object's blocks
    /// live. That is what breaks the circularity a client would otherwise hit —
    /// it needs a version to compute placement, but placement to pick a worker.
    ///
    /// Workers are tried in membership order until one answers, so a single
    /// unreachable worker does not fail the request.
    async fn proxy_to_worker(&self, message: ControlMessage) -> ControlMessage {
        let workers: Vec<_> = self
            .service
            .membership()
            .snapshot()
            .into_iter()
            .filter(|node| node.role == NodeRole::Worker)
            .collect();
        if workers.is_empty() {
            return ControlMessage::Ack {
                ok: false,
                detail: Some("no worker available to serve the request".into()),
            };
        }

        Self::proxy_to_workers(&workers, message).await
    }

    /// Try workers in order, treating both transport errors and explicit worker
    /// rejections as retryable. A worker can be ready enough to advertise but
    /// still reject a request that a later healthy worker can serve.
    async fn proxy_to_workers(workers: &[NodeInfo], message: ControlMessage) -> ControlMessage {
        let budget = proxy_request_budget(&message);
        Self::proxy_to_workers_with_budget(workers, message, budget).await
    }

    /// Proxy under one deadline shared by every serial worker attempt.
    ///
    /// Each attempt receives the remaining total budget minus a small reserve
    /// for every later worker. This prevents a silent first worker from
    /// consuming the whole request while leaving long-running listings most of
    /// their 25-second budget.
    async fn proxy_to_workers_with_budget(
        workers: &[NodeInfo],
        message: ControlMessage,
        budget: Duration,
    ) -> ControlMessage {
        let deadline = tokio::time::Instant::now() + budget;
        let mut attempt_errors = String::new();
        let mut tried = 0;
        for (index, worker) in workers.iter().enumerate() {
            let now = tokio::time::Instant::now();
            let remaining = deadline.saturating_duration_since(now);
            if remaining.is_zero() {
                break;
            }
            let workers_remaining = workers.len() - index;
            let attempt_budget = proxy_attempt_budget(remaining, workers_remaining);
            let attempt_deadline = now + attempt_budget;
            tried += 1;
            let attempt = tokio::time::timeout_at(
                attempt_deadline,
                Self::round_trip_worker(&worker.address, &message, attempt_deadline),
            )
            .await;
            match attempt {
                Err(_) => append_proxy_attempt_error(
                    &mut attempt_errors,
                    &format!(
                        "{}: worker attempt timed out after {attempt_budget:?}",
                        worker.address
                    ),
                ),
                Ok(Ok(ControlMessage::Ack { ok: false, detail })) => {
                    append_proxy_attempt_error(
                        &mut attempt_errors,
                        &format!(
                            "{}: worker rejected request: {}",
                            worker.address,
                            detail.unwrap_or_else(|| "no detail provided".into())
                        ),
                    );
                }
                Ok(Ok(reply)) => return reply,
                Ok(Err(e)) => append_proxy_attempt_error(
                    &mut attempt_errors,
                    &format!("{}: {e}", worker.address),
                ),
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
        }
        ControlMessage::Ack {
            ok: false,
            detail: Some(format!(
                "no worker served the request ({tried}/{} tried within {budget:?}); attempt errors: {}",
                workers.len(),
                if attempt_errors.is_empty() {
                    "unknown"
                } else {
                    &attempt_errors
                }
            )),
        }
    }

    /// One request/response against a worker's data-plane port.
    async fn round_trip_worker(
        address: &str,
        message: &ControlMessage,
        attempt_deadline: tokio::time::Instant,
    ) -> anyhow::Result<ControlMessage> {
        let remaining = attempt_deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            anyhow::bail!("worker attempt budget exhausted before connecting");
        }
        let connect_timeout = PROXY_CONNECT_TIMEOUT.min(remaining);
        let mut stream = tokio::time::timeout(connect_timeout, TcpStream::connect(address))
            .await
            .map_err(|_| anyhow::anyhow!("connect timed out after {connect_timeout:?}"))??;
        let buf = codec::encode(0, message)?;
        stream.write_all(&buf).await?;
        stream.flush().await?;

        let remaining = attempt_deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            anyhow::bail!("worker attempt budget exhausted before reading response");
        }
        let (header, payload) = tokio::time::timeout(
            remaining,
            talon_transport::read_frame(&mut stream, remaining),
        )
        .await
        .map_err(|_| anyhow::anyhow!("worker response did not arrive within its attempt budget"))?
        .map_err(|e| anyhow::anyhow!("{e}"))?;
        let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
        full.extend_from_slice(&header.encode());
        full.extend_from_slice(&payload);
        let (_, reply) = codec::decode(&full)?;
        Ok(reply)
    }

    async fn dispatch(&self, message: ControlMessage) -> ControlMessage {
        match message {
            ControlMessage::Register { node } => {
                // Legacy control-plane path. It used to call
                // `membership().register(node)` directly, which writes only this
                // coordinator's in-memory set — no ClusterStateStore write, no
                // is_ready()/health gate. In active-active that makes the node
                // visible on just the receiving coordinator, and the very next
                // reconcile_membership tick (which replaces the whole set from the
                // store snapshot) deletes it. That local-only, self-erasing insert
                // is misleading, so make Register a no-op for membership: workers
                // use NodeStatusHeartbeat, which persists through the store (#167).
                tracing::info!(
                    id = %node.id,
                    address = %node.address,
                    "legacy Register received; membership is store-authoritative, \
                     use NodeStatusHeartbeat"
                );
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
                let healthy_ready =
                    status.health == talon_core::NodeHealth::Healthy && status.ready;
                match self
                    .observability
                    .upsert_status(*status, self.lease_ttl)
                    .await
                {
                    Ok(result) => {
                        // Fast-path local visibility before the next reconcile
                        // tick, but only for a healthy, ready worker — an
                        // unhealthy/not-ready node must not be injected into
                        // placement (issue #118); the store reconcile remains the
                        // authoritative source and will drop it otherwise.
                        if result.disposition == WriteDisposition::Applied
                            && node.role == NodeRole::Worker
                            && healthy_ready
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
            listing @ ControlMessage::ListObjects { .. } => {
                if !self.observability.is_ready() {
                    return ControlMessage::Ack {
                        ok: false,
                        detail: Some("coordinator not ready: shared state unavailable".into()),
                    };
                }
                self.proxy_to_worker(listing).await
            }
            stat @ ControlMessage::StatObject { .. } => {
                if !self.observability.is_ready() {
                    return ControlMessage::Ack {
                        ok: false,
                        detail: Some("coordinator not ready: shared state unavailable".into()),
                    };
                }
                self.proxy_to_worker(stat).await
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
    let capabilities = build_capabilities(&config).await;
    let observability = Arc::new(
        CoordinatorObservability::new(
            config.cluster_id.clone(),
            node,
            config.admin_advertise.clone(),
            Duration::from_millis(config.state.request_timeout_ms),
            store,
        )?
        .with_capabilities(capabilities),
    );
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
    // Bound concurrent control connections so a flood of idle peers cannot
    // exhaust memory/FDs (issue #111).
    let conn_limit = talon_transport::ConnectionLimit::new(MAX_CONTROL_CONNECTIONS);
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let conn_limit = conn_limit.clone();
                let state = Arc::clone(&state);
                // Acquire the connection permit *inside* the spawned task, not in
                // the select! arm: at MAX_CONTROL_CONNECTIONS a blocking
                // acquire().await here would stall the accept loop and starve the
                // ctrl_c shutdown branch until a permit frees (#167). The task
                // waits for a slot instead, keeping the select! responsive.
                tokio::spawn(async move {
                    let _permit = conn_limit.acquire().await;
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
    use talon_coordinator::config::env_names;
    use talon_coordinator::security::{AuthMode, SecurityConfig};
    let auth = match std::env::var(env_names::AUTH_TOKEN) {
        Ok(token) if !token.is_empty() => AuthMode::BearerToken { token },
        _ => AuthMode::Disabled,
    };
    let trust_forwarded_headers = std::env::var(env_names::TRUST_FORWARDED)
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
    // Read one frame with the per-type size cap (control frames are capped at
    // 1 MiB, far below the 320 MiB data-plane max) enforced before allocation,
    // plus a read timeout, so a peer cannot pin a large buffer by advertising a
    // huge length and stalling (issue #111).
    let (header, payload) =
        match talon_transport::read_frame(stream, talon_transport::DEFAULT_READ_TIMEOUT).await {
            Ok(frame) => frame,
            Err(talon_transport::ReadFrameError::Eof) => return Ok(None),
            Err(error) => return Err(anyhow::anyhow!(error)),
        };
    let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
    full.extend_from_slice(&header.encode());
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

    #[test]
    fn listings_use_one_dedicated_total_proxy_budget() {
        let listing = ControlMessage::ListObjects {
            prefix: "s3/bucket".into(),
        };
        let stat = ControlMessage::StatObject {
            object: talon_core::ObjectId::new(talon_core::Backend::S3, "bucket", "object"),
        };

        assert_eq!(proxy_request_budget(&listing), Duration::from_secs(25));
        assert_eq!(proxy_request_budget(&stat), Duration::from_secs(5));
        assert!(proxy_request_budget(&listing) > proxy_request_budget(&stat));
        assert!(
            proxy_request_budget(&listing) < Duration::from_secs(30),
            "listing proxy must leave headroom inside the FUSE client deadline"
        );
        assert_eq!(
            proxy_attempt_budget(proxy_request_budget(&listing), 2),
            Duration::from_secs(24),
            "listing keeps most of its long-running budget while reserving a retry"
        );
        assert_eq!(
            proxy_attempt_budget(proxy_request_budget(&listing), 1),
            Duration::from_secs(25),
            "a sole listing worker keeps the full long-running budget"
        );
        assert_eq!(
            proxy_attempt_budget(Duration::from_millis(2_100), 3),
            Duration::from_millis(700),
            "a tight budget degrades to an even split"
        );
        assert_eq!(
            proxy_attempt_budget(Duration::from_millis(3_001), 3),
            Duration::from_millis(1_001),
            "crossing the one-second reserve threshold must be continuous"
        );
    }

    #[test]
    fn proxy_attempt_errors_are_bounded_without_hiding_the_next_worker() {
        let mut errors = String::new();
        let oversized = format!(
            "worker-a: actionable listing limit; narrow the namespace prefix: {}",
            "x".repeat(MAX_PROXY_ATTEMPT_ERRORS_BYTES)
        );

        append_proxy_attempt_error(&mut errors, &oversized);
        append_proxy_attempt_error(&mut errors, "worker-b: backend mismatch");

        assert!(errors.len() <= MAX_PROXY_ATTEMPT_ERRORS_BYTES);
        assert!(errors.contains("worker-a: actionable listing limit"));
        assert!(errors.contains("worker-b: backend mismatch"));
    }

    #[tokio::test]
    async fn proxy_retries_after_a_worker_rejects_the_request() {
        let rejecting_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rejecting_address = rejecting_listener.local_addr().unwrap().to_string();
        let rejecting_server = tokio::spawn(async move {
            let (mut stream, _) = rejecting_listener.accept().await.unwrap();
            let (_, request) = read_control(&mut stream).await.unwrap().unwrap();
            assert!(matches!(request, ControlMessage::ListObjects { .. }));
            let reply = codec::encode(
                0,
                &ControlMessage::Ack {
                    ok: false,
                    detail: Some("worker backend mismatch".into()),
                },
            )
            .unwrap();
            stream.write_all(&reply).await.unwrap();
            stream.flush().await.unwrap();
        });

        let serving_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let serving_address = serving_listener.local_addr().unwrap().to_string();
        let serving_server = tokio::spawn(async move {
            let (mut stream, _) = serving_listener.accept().await.unwrap();
            let (_, request) = read_control(&mut stream).await.unwrap().unwrap();
            assert!(matches!(request, ControlMessage::ListObjects { .. }));
            let reply = codec::encode(
                0,
                &ControlMessage::ObjectList {
                    entries: vec![talon_transport::ObjectEntry {
                        path: "s3/bucket/object".into(),
                        size: 42,
                    }],
                },
            )
            .unwrap();
            stream.write_all(&reply).await.unwrap();
            stream.flush().await.unwrap();
        });

        let workers = vec![
            NodeInfo {
                id: NodeId::new("worker-rejects"),
                address: rejecting_address,
                role: NodeRole::Worker,
            },
            NodeInfo {
                id: NodeId::new("worker-serves"),
                address: serving_address,
                role: NodeRole::Worker,
            },
        ];
        let reply = Coordinator::proxy_to_workers(
            &workers,
            ControlMessage::ListObjects {
                prefix: "s3/bucket".into(),
            },
        )
        .await;

        assert!(matches!(
            reply,
            ControlMessage::ObjectList { entries }
                if entries == vec![talon_transport::ObjectEntry {
                    path: "s3/bucket/object".into(),
                    size: 42,
                }]
        ));
        rejecting_server.await.unwrap();
        serving_server.await.unwrap();
    }

    #[tokio::test]
    async fn proxy_retries_a_healthy_worker_after_a_silent_worker_times_out() {
        let stalled_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stalled_address = stalled_listener.local_addr().unwrap().to_string();
        let (stalled_request_tx, stalled_request_rx) = tokio::sync::oneshot::channel();
        let stalled_server = tokio::spawn(async move {
            let (mut stream, _) = stalled_listener.accept().await.unwrap();
            let (_, request) = read_control(&mut stream).await.unwrap().unwrap();
            assert!(matches!(request, ControlMessage::ListObjects { .. }));
            stalled_request_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });

        let serving_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let serving_address = serving_listener.local_addr().unwrap().to_string();
        let serving_server = tokio::spawn(async move {
            let (mut stream, _) = serving_listener.accept().await.unwrap();
            let (_, request) = read_control(&mut stream).await.unwrap().unwrap();
            assert!(matches!(request, ControlMessage::ListObjects { .. }));
            let reply = codec::encode(
                0,
                &ControlMessage::ObjectList {
                    entries: vec![talon_transport::ObjectEntry {
                        path: "s3/bucket/object".into(),
                        size: 42,
                    }],
                },
            )
            .unwrap();
            stream.write_all(&reply).await.unwrap();
            stream.flush().await.unwrap();
        });

        let workers = vec![
            NodeInfo {
                id: NodeId::new("worker-stalls"),
                address: stalled_address,
                role: NodeRole::Worker,
            },
            NodeInfo {
                id: NodeId::new("worker-serves"),
                address: serving_address,
                role: NodeRole::Worker,
            },
        ];
        let budget = Duration::from_secs(4);
        let proxy = tokio::spawn(async move {
            Coordinator::proxy_to_workers_with_budget(
                &workers,
                ControlMessage::ListObjects {
                    prefix: "s3/bucket".into(),
                },
                budget,
            )
            .await
        });

        tokio::time::timeout(Duration::from_secs(5), stalled_request_rx)
            .await
            .expect("coordinator did not reach the stalled worker")
            .expect("stalled worker exited before receiving the request");
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(3)).await;
        tokio::time::resume();

        let reply = tokio::time::timeout(Duration::from_secs(3), proxy)
            .await
            .expect("coordinator did not retry the healthy worker")
            .unwrap();
        assert!(matches!(
            reply,
            ControlMessage::ObjectList { entries }
                if entries == vec![talon_transport::ObjectEntry {
                    path: "s3/bucket/object".into(),
                    size: 42,
                }]
        ));

        stalled_server.abort();
        serving_server.await.unwrap();
    }

    #[tokio::test]
    async fn proxy_attempt_timeouts_do_not_reset_the_shared_total_budget() {
        let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_address = first_listener.local_addr().unwrap().to_string();
        let (first_request_tx, first_request_rx) = tokio::sync::oneshot::channel();
        let first_server = tokio::spawn(async move {
            let (mut stream, _) = first_listener.accept().await.unwrap();
            let (_, request) = read_control(&mut stream).await.unwrap().unwrap();
            assert!(matches!(request, ControlMessage::ListObjects { .. }));
            first_request_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });

        let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_address = second_listener.local_addr().unwrap().to_string();
        let (second_request_tx, second_request_rx) = tokio::sync::oneshot::channel();
        let second_server = tokio::spawn(async move {
            let (mut stream, _) = second_listener.accept().await.unwrap();
            let (_, request) = read_control(&mut stream).await.unwrap().unwrap();
            assert!(matches!(request, ControlMessage::ListObjects { .. }));
            second_request_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });

        let workers = vec![
            NodeInfo {
                id: NodeId::new("worker-stalls-first"),
                address: first_address.clone(),
                role: NodeRole::Worker,
            },
            NodeInfo {
                id: NodeId::new("worker-stalls-second"),
                address: second_address.clone(),
                role: NodeRole::Worker,
            },
        ];
        let budget = Duration::from_secs(6);
        let started = tokio::time::Instant::now();
        let proxy = tokio::spawn(async move {
            Coordinator::proxy_to_workers_with_budget(
                &workers,
                ControlMessage::ListObjects {
                    prefix: "s3/bucket".into(),
                },
                budget,
            )
            .await
        });

        tokio::time::timeout(Duration::from_secs(5), first_request_rx)
            .await
            .expect("coordinator did not reach the first worker")
            .expect("first worker exited before receiving the request");
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::time::resume();
        tokio::time::timeout(Duration::from_secs(1), second_request_rx)
            .await
            .expect("coordinator did not spend a separate slice on the second worker")
            .expect("second worker exited before receiving the request");
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::time::resume();

        let reply = tokio::time::timeout(Duration::from_secs(1), proxy)
            .await
            .expect("second worker received a fresh total budget")
            .unwrap();
        assert!(
            started.elapsed() < budget + Duration::from_secs(2),
            "shared {budget:?} budget took {:?}",
            started.elapsed()
        );
        let detail = match reply {
            ControlMessage::Ack {
                ok: false,
                detail: Some(detail),
            } => detail,
            other => panic!("expected aggregate proxy failure, got {other:?}"),
        };
        assert!(detail.contains("2/2 tried within 6s"), "{detail}");
        assert!(detail.contains(&first_address), "{detail}");
        assert!(detail.contains(&second_address), "{detail}");

        first_server.abort();
        second_server.abort();
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
