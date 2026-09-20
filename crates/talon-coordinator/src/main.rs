//! Talon coordinator control and administration servers.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::Parser;
use talon_coordinator::{
    ClusterStateStore, CoordinatorConfig, CoordinatorConfigPatch, CoordinatorObservability,
    Membership, MemoryStateStore, PlacementService, RendezvousPlacement, StateBackend,
    WriteDisposition,
};
use talon_core::{
    NamespacePolicy, NodeHealth, NodeInfo, NodeRole, ObjectNamespace, WorkloadIdentity,
    WorkloadRole,
};
use talon_metadata::{ClusterCapabilities, MappingRevision, MetadataStore, NamespaceId};
use talon_transport::control_tls::ControlTlsChannel;
use talon_transport::frame::HEADER_LEN;
use talon_transport::{codec, ControlMessage, FrameHeader};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

/// Upper bound on concurrent control-plane connections (issue #111). Beyond
/// this, new peers wait for an in-flight connection to finish.
const MAX_CONTROL_CONNECTIONS: usize = 1024;

const CONTROL_TLS_RELOAD_INTERVAL: Duration = Duration::from_secs(5);
const CONTROL_OPERATION_TIMEOUT: Duration = Duration::from_secs(3);

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

const MAX_CONCURRENT_WORKER_PROXY_REQUESTS: usize = 64;
const MAX_IDLE_WORKER_PROXY_CONNECTIONS: usize = 64;
// Retire idle sockets before the worker's 30-second frame-read deadline.
const WORKER_PROXY_IDLE_TTL: Duration = Duration::from_secs(20);

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
    /// Dedicated worker-service mTLS bind address.
    #[arg(long)]
    control_listen: Option<String>,
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
    /// Node unhealthy threshold and last-good membership grace in milliseconds.
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
            control_listen: self.control_listen,
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
async fn build_capabilities(
    config: &CoordinatorConfig,
) -> (
    ClusterCapabilities,
    Option<Arc<dyn talon_metadata::MetadataStore>>,
) {
    #[cfg(feature = "etcd")]
    {
        let Some(metadata) = config.metadata.as_ref() else {
            return (ClusterCapabilities::none(), None);
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
                (
                    ClusterCapabilities {
                        advertised,
                        revision: talon_metadata::CapabilityRevision::new(1),
                        store_reachable,
                    },
                    Some(Arc::new(store) as Arc<dyn talon_metadata::MetadataStore>),
                )
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
                // No handle: the connection never succeeded, so there is
                // nothing to sample. Reachability stays false until a restart,
                // which is honest -- a store that was never reachable cannot be
                // observed to recover through a handle that does not exist.
                (
                    ClusterCapabilities {
                        advertised: talon_metadata::CapabilitySet::none()
                            .with(talon_metadata::Capability::HardLinks),
                        revision: talon_metadata::CapabilityRevision::new(1),
                        store_reachable: false,
                    },
                    None,
                )
            }
        }
    }
    #[cfg(not(feature = "etcd"))]
    {
        let _ = config;
        (ClusterCapabilities::none(), None)
    }
}

struct IdleWorkerConnection {
    stream: TcpStream,
    returned_at: Instant,
}

#[derive(Default)]
struct WorkerProxyIdleState {
    by_address: HashMap<String, Vec<IdleWorkerConnection>>,
    total: usize,
    /// `None` for direct/test calls that supply an explicit worker list. The
    /// normal membership-driven path installs the current address set so a
    /// request that finishes after removal cannot put its old socket back.
    allowed_addresses: Option<HashSet<String>>,
}

/// Exclusive-checkout pool for coordinator -> worker request/response traffic.
///
/// A connection is returned only after a complete, successfully decoded
/// exchange. No multiplexing is needed: the checkout owner is the sole user of
/// the stream until it releases it, preserving the protocol's ordered framing.
struct WorkerProxyPool {
    idle: Mutex<WorkerProxyIdleState>,
    max_idle: usize,
    idle_ttl: Duration,
}

impl WorkerProxyPool {
    fn new(max_idle: usize, idle_ttl: Duration) -> Self {
        Self {
            idle: Mutex::new(WorkerProxyIdleState::default()),
            max_idle,
            idle_ttl,
        }
    }

    fn lock_idle(&self) -> std::sync::MutexGuard<'_, WorkerProxyIdleState> {
        self.idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn prune_expired(&self, state: &mut WorkerProxyIdleState) {
        let idle_ttl = self.idle_ttl;
        state.by_address.retain(|_, bucket| {
            bucket.retain(|idle| idle.returned_at.elapsed() < idle_ttl);
            !bucket.is_empty()
        });
        state.total = state.by_address.values().map(Vec::len).sum();
    }

    fn checkout(&self, address: &str) -> Option<TcpStream> {
        let mut state = self.lock_idle();
        let mut stream = None;
        let mut removed = 0;
        if let Some(bucket) = state.by_address.get_mut(address) {
            while let Some(idle) = bucket.pop() {
                removed += 1;
                if idle.returned_at.elapsed() < self.idle_ttl {
                    stream = Some(idle.stream);
                    break;
                }
            }
        }
        state.total -= removed;
        if state.by_address.get(address).is_some_and(Vec::is_empty) {
            state.by_address.remove(address);
        }
        stream
    }

    fn release(&self, address: &str, stream: TcpStream) {
        let mut state = self.lock_idle();
        if state.total >= self.max_idle {
            self.prune_expired(&mut state);
        }
        if state.total >= self.max_idle
            || state
                .allowed_addresses
                .as_ref()
                .is_some_and(|allowed| !allowed.contains(address))
        {
            return;
        }
        state
            .by_address
            .entry(address.to_owned())
            .or_default()
            .push(IdleWorkerConnection {
                stream,
                returned_at: Instant::now(),
            });
        state.total += 1;
    }

    /// Drop idle sockets for workers no longer present in the authoritative
    /// membership snapshot. Checked-out sockets finish normally and are
    /// rejected on release once the pool observes the new membership.
    fn retain_workers(&self, addresses: &HashSet<String>) {
        let mut state = self.lock_idle();
        self.prune_expired(&mut state);
        state
            .by_address
            .retain(|address, _| addresses.contains(address));
        state.total = state.by_address.values().map(Vec::len).sum();
        state.allowed_addresses = Some(addresses.clone());
    }

    #[cfg(test)]
    fn idle_count(&self, address: &str) -> usize {
        self.lock_idle().by_address.get(address).map_or(0, Vec::len)
    }
}

struct Coordinator {
    service: PlacementService<RendezvousPlacement>,
    observability: Arc<CoordinatorObservability>,
    lease_ttl: Duration,
    worker_proxy_pool: WorkerProxyPool,
    worker_proxy_slots: Semaphore,
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
        Self::with_proxy_limits(
            observability,
            lease_ttl,
            MAX_CONCURRENT_WORKER_PROXY_REQUESTS,
            MAX_IDLE_WORKER_PROXY_CONNECTIONS,
            WORKER_PROXY_IDLE_TTL,
        )
    }

    fn with_proxy_limits(
        observability: Arc<CoordinatorObservability>,
        lease_ttl: Duration,
        max_concurrent: usize,
        max_idle: usize,
        idle_ttl: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            service: PlacementService::new(Membership::new(), RendezvousPlacement),
            observability,
            lease_ttl,
            worker_proxy_pool: WorkerProxyPool::new(max_idle, idle_ttl),
            worker_proxy_slots: Semaphore::new(max_concurrent.max(1)),
        })
    }

    fn refresh_worker_proxy_membership(&self) {
        let addresses: HashSet<_> = self
            .service
            .membership()
            .snapshot()
            .into_iter()
            .filter(|node| node.role == NodeRole::Worker)
            .map(|node| node.address)
            .collect();
        self.worker_proxy_pool.retain_workers(&addresses);
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

        self.proxy_to_workers(&workers, message).await
    }

    /// Try workers in order, treating both transport errors and explicit worker
    /// rejections as retryable. A worker can be ready enough to advertise but
    /// still reject a request that a later healthy worker can serve.
    async fn proxy_to_workers(
        &self,
        workers: &[NodeInfo],
        message: ControlMessage,
    ) -> ControlMessage {
        let budget = proxy_request_budget(&message);
        self.proxy_to_workers_with_budget(workers, message, budget)
            .await
    }

    /// Proxy under one deadline shared by every serial worker attempt.
    ///
    /// Each attempt receives the remaining total budget minus a small reserve
    /// for every later worker. This prevents a silent first worker from
    /// consuming the whole request while leaving long-running listings most of
    /// their 25-second budget.
    async fn proxy_to_workers_with_budget(
        &self,
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
                self.round_trip_worker(&worker.address, &message, attempt_deadline),
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
        &self,
        address: &str,
        message: &ControlMessage,
        attempt_deadline: tokio::time::Instant,
    ) -> anyhow::Result<ControlMessage> {
        talon_telemetry::observe("talon.rpc", "client", async {
            let _permit = tokio::time::timeout_at(attempt_deadline, self.worker_proxy_slots.acquire())
                .await
                .map_err(|_| anyhow::anyhow!("worker proxy concurrency wait exhausted attempt budget"))?
                .map_err(|_| anyhow::anyhow!("worker proxy concurrency limiter closed"))?;

            if let Some(stream) = self.worker_proxy_pool.checkout(address) {
                match Self::exchange_with_worker(stream, address, message, attempt_deadline).await {
                    Ok((stream, reply)) => {
                        self.worker_proxy_pool.release(address, stream);
                        return Ok(reply);
                    }
                    Err(reused_error) => {
                        // The peer may have closed an otherwise healthy socket
                        // while it sat idle. Retry only this transport exchange on
                        // a fresh connection and keep the same attempt deadline.
                        let stream = Self::connect_worker(address, attempt_deadline)
                            .await
                            .map_err(|fresh_error| {
                                anyhow::anyhow!(
                                    "reused connection failed ({reused_error}); fresh retry failed: {fresh_error}"
                                )
                            })?;
                        return match Self::exchange_with_worker(stream, address, message, attempt_deadline).await
                        {
                            Ok((stream, reply)) => {
                                self.worker_proxy_pool.release(address, stream);
                                Ok(reply)
                            }
                            Err(fresh_error) => Err(anyhow::anyhow!(
                                "reused connection failed ({reused_error}); fresh retry failed: {fresh_error}"
                            )),
                        };
                    }
                }
            }

            let stream = Self::connect_worker(address, attempt_deadline).await?;
            match Self::exchange_with_worker(stream, address, message, attempt_deadline).await {
                Ok((stream, reply)) => {
                    self.worker_proxy_pool.release(address, stream);
                    Ok(reply)
                }
                Err(error) => Err(error),
            }
        })
        .await
    }

    async fn connect_worker(
        address: &str,
        attempt_deadline: tokio::time::Instant,
    ) -> anyhow::Result<TcpStream> {
        let remaining = attempt_deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            anyhow::bail!("worker attempt budget exhausted before connecting");
        }
        let connect_timeout = PROXY_CONNECT_TIMEOUT.min(remaining);
        tokio::time::timeout(connect_timeout, TcpStream::connect(address))
            .await
            .map_err(|_| anyhow::anyhow!("connect timed out after {connect_timeout:?}"))?
            .map_err(Into::into)
    }

    async fn exchange_with_worker(
        mut stream: TcpStream,
        address: &str,
        message: &ControlMessage,
        attempt_deadline: tokio::time::Instant,
    ) -> anyhow::Result<(TcpStream, ControlMessage)> {
        let mut buf = codec::encode(0, message)?;
        talon_transport::envelope::outbound(&mut buf, address)?;
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
        if matches!(&reply, ControlMessage::Ack { ok: false, .. }) {
            talon_telemetry::outcome("error");
        }
        Ok((stream, reply))
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
                let zone = status.labels.get(talon_core::NODE_ZONE_LABEL).cloned();
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
                            self.service.membership().register_zoned(node, zone);
                            self.refresh_worker_proxy_membership();
                        }
                        let accepted = matches!(
                            result.disposition,
                            WriteDisposition::Applied | WriteDisposition::Duplicate
                        );
                        self.observability
                            .metrics()
                            .record_heartbeat(true, accepted);
                        match result.disposition {
                            WriteDisposition::Stale => ControlMessage::Ack {
                                ok: false,
                                detail: Some("stale node status was not accepted".into()),
                            },
                            WriteDisposition::Applied | WriteDisposition::Duplicate => {
                                ControlMessage::Ack {
                                    ok: true,
                                    detail: None,
                                }
                            }
                            WriteDisposition::NotFound => ControlMessage::Ack {
                                ok: false,
                                detail: Some("node status upsert did not find its record".into()),
                            },
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
            ControlMessage::MembershipQueryV2 {} => {
                if !self.observability.is_ready() {
                    return ControlMessage::Ack {
                        ok: false,
                        detail: Some("coordinator not ready: shared state unavailable".into()),
                    };
                }
                ControlMessage::MembershipListV2 {
                    nodes: self
                        .service
                        .membership()
                        .snapshot_zoned()
                        .into_iter()
                        .map(|(info, zone)| talon_transport::ZonedNodeInfo { info, zone })
                        .collect(),
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
    #[cfg(feature = "telemetry")]
    let _telemetry = talon_telemetry::export::init("talon-coordinator")
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    #[cfg(not(feature = "telemetry"))]
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    #[cfg(not(feature = "telemetry"))]
    talon_telemetry::Config::from_env()
        .and_then(talon_telemetry::configure)
        .map_err(std::io::Error::other)?;
    let result = run().await;
    #[cfg(feature = "telemetry")]
    let _ = tokio::task::spawn_blocking(move || _telemetry.shutdown()).await;
    result
}

async fn run() -> anyhow::Result<()> {
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
    let (capabilities, metadata_store) = build_capabilities(&config).await;
    let mut observability = CoordinatorObservability::new(
        config.cluster_id.clone(),
        node,
        config.admin_advertise.clone(),
        Duration::from_millis(config.state.request_timeout_ms),
        store,
    )?
    .with_state_failure_grace(Duration::from_millis(config.state.unhealthy_after_ms))
    .with_capabilities(capabilities);
    if let Some(metadata_store) = metadata_store.clone() {
        observability = observability.with_metadata_store(metadata_store);
    }
    let observability = Arc::new(observability);
    observability.check_ready().await?;
    let state = Coordinator::new(
        Arc::clone(&observability),
        Duration::from_millis(config.state.lease_ttl_ms),
    );

    // Install authoritative membership before opening listeners. A health
    // probe alone cannot establish the placement view used during a failure.
    observability
        .reconcile_membership(state.service.membership())
        .await?;
    state.refresh_worker_proxy_membership();

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

    let secure_control_enabled = config.control_tls.is_some();
    if let (Some(control_listen), Some(control_tls)) = (&config.control_listen, &config.control_tls)
    {
        let identity = WorkloadIdentity::new(
            control_tls.trust_domain.clone(),
            config.cluster_id.clone(),
            WorkloadRole::Coordinator,
            config.node_id.clone(),
        )?;
        let channel = ControlTlsChannel::load(
            control_tls.clone(),
            identity,
            WorkloadRole::Worker,
            CONTROL_TLS_RELOAD_INTERVAL,
        )?;
        let listener = TcpListener::bind(control_listen).await?;
        tracing::info!(listen = %control_listen, "coordinator serving worker mTLS control plane");
        if let (Some(metadata), Some(policy)) =
            (metadata_store.clone(), config.namespace_policy.clone())
        {
            spawn_revision_propagation(
                Arc::clone(&observability),
                metadata,
                channel.clone(),
                policy,
                Duration::from_millis(config.state.heartbeat_interval_ms),
            );
        }
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(error) = serve_worker_control(listener, channel, state).await {
                tracing::error!(%error, "coordinator worker mTLS control plane stopped");
            }
        });
    }

    let listener = TcpListener::bind(&config.listen).await?;
    tracing::info!(listen = %config.listen, "coordinator serving public control plane");
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
                    let access = if secure_control_enabled {
                        ControlAccess::PublicOnly
                    } else {
                        ControlAccess::Legacy
                    };
                    if let Err(error) = handle_conn(stream, state, access).await {
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
/// `TALON_COORDINATOR_TRUST_FORWARDED=1` is parsed into the config but has no
/// effect yet: request audit logging is not implemented, so nothing reads
/// `X-Forwarded-For`. TLS is reverse-proxy terminated.
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
                // retained only within the configured last-good grace.
                tracing::warn!(%error, "membership reconcile from shared state failed");
            } else {
                state.refresh_worker_proxy_membership();
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

fn spawn_revision_propagation(
    observability: Arc<CoordinatorObservability>,
    metadata: Arc<dyn MetadataStore>,
    channel: ControlTlsChannel,
    policy: NamespacePolicy,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Err(error) =
                propagate_mapping_revisions(&observability, metadata.as_ref(), &channel, &policy)
                    .await
            {
                tracing::warn!(%error, "mapping revision propagation pass failed closed");
            }
        }
    })
}

async fn propagate_mapping_revisions(
    observability: &CoordinatorObservability,
    metadata: &dyn MetadataStore,
    channel: &ControlTlsChannel,
    policy: &NamespacePolicy,
) -> anyhow::Result<()> {
    let snapshot = observability.snapshot_for_api().await?;
    let mut revisions = HashMap::<ObjectNamespace, MappingRevision>::new();
    let mut unavailable = HashSet::<ObjectNamespace>::new();

    for target in revision_targets(&snapshot, policy) {
        let status = &target.worker;
        let worker_id = status.node.id.0.as_str();
        let address = target.address.as_str();
        let namespace = &target.namespace;
        if unavailable.contains(namespace) {
            continue;
        }
        let revision = if let Some(revision) = revisions.get(namespace) {
            *revision
        } else {
            let namespace_id = NamespaceId::new(namespace.to_string())?;
            let revision = match metadata.mapping_revision(&namespace_id).await {
                Ok(revision) => revision,
                Err(error) => {
                    tracing::warn!(
                        namespace = %namespace,
                        %error,
                        "mapping revision lookup failed closed"
                    );
                    unavailable.insert(namespace.clone());
                    continue;
                }
            };
            revisions.insert(namespace.clone(), revision);
            revision
        };
        let update = RevisionUpdate {
            cluster_id: observability.cluster_id(),
            coordinator_id: observability.node_id(),
            coordinator_incarnation: observability.incarnation_id(),
            namespace,
            revision,
        };
        let result = tokio::time::timeout(
            CONTROL_OPERATION_TIMEOUT,
            push_mapping_revision(channel, address, status, update),
        )
        .await;
        match result {
            Ok(Ok(ack_revision)) => tracing::debug!(
                worker_id,
                namespace = %namespace,
                revision = ack_revision.get(),
                "worker acknowledged mapping revision"
            ),
            Ok(Err(error)) => tracing::warn!(
                worker_id,
                namespace = %namespace,
                %error,
                "worker mapping revision update rejected"
            ),
            Err(_) => tracing::warn!(
                worker_id,
                namespace = %namespace,
                "worker mapping revision update timed out"
            ),
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct RevisionTarget {
    worker: talon_core::NodeStatus,
    address: String,
    namespace: ObjectNamespace,
}

fn revision_targets(
    snapshot: &talon_coordinator::ClusterSnapshot,
    policy: &NamespacePolicy,
) -> Vec<RevisionTarget> {
    snapshot
        .nodes
        .iter()
        .filter(|status| {
            status.node.role == NodeRole::Worker
                && status.health == NodeHealth::Healthy
                && status.ready
        })
        .flat_map(|status| {
            let worker_id = status.node.id.0.as_str();
            let address = policy.control_address(worker_id).map(str::to_owned);
            policy
                .grants(worker_id)
                .iter()
                .filter_map(move |namespace| {
                    address.as_ref().map(|address| RevisionTarget {
                        worker: status.clone(),
                        address: address.clone(),
                        namespace: namespace.clone(),
                    })
                })
        })
        .collect()
}

struct RevisionUpdate<'a> {
    cluster_id: &'a str,
    coordinator_id: &'a str,
    coordinator_incarnation: &'a str,
    namespace: &'a ObjectNamespace,
    revision: MappingRevision,
}

async fn push_mapping_revision(
    channel: &ControlTlsChannel,
    address: &str,
    worker: &talon_core::NodeStatus,
    update: RevisionUpdate<'_>,
) -> anyhow::Result<MappingRevision> {
    let mut authenticated = channel.connect(address).await?;
    if authenticated.identity.cluster_id() != update.cluster_id
        || authenticated.identity.node_id() != worker.node.id.0
    {
        anyhow::bail!("connected worker identity does not match authoritative membership");
    }
    let message = ControlMessage::MappingRevisionUpdate {
        cluster_id: update.cluster_id.to_owned(),
        namespace: update.namespace.to_string(),
        revision: update.revision.get(),
        coordinator_id: update.coordinator_id.to_owned(),
        coordinator_incarnation: update.coordinator_incarnation.to_owned(),
    };
    authenticated
        .stream
        .write_all(&codec::encode(0, &message)?)
        .await?;
    authenticated.stream.flush().await?;
    let reply = read_control_reply(&mut authenticated.stream).await?;
    validate_revision_ack(
        reply,
        &authenticated.identity,
        worker,
        update.cluster_id,
        update.namespace,
        update.revision,
    )
}

async fn read_control_reply<S>(stream: &mut S) -> anyhow::Result<ControlMessage>
where
    S: AsyncRead + Unpin,
{
    let (header, payload) = talon_transport::read_frame(stream, CONTROL_OPERATION_TIMEOUT)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
    full.extend_from_slice(&header.encode());
    full.extend_from_slice(&payload);
    Ok(codec::decode(&full)?.1)
}

fn validate_revision_ack(
    reply: ControlMessage,
    peer: &WorkloadIdentity,
    worker: &talon_core::NodeStatus,
    cluster_id: &str,
    namespace: &ObjectNamespace,
    requested: MappingRevision,
) -> anyhow::Result<MappingRevision> {
    let ControlMessage::MappingRevisionAck {
        cluster_id: ack_cluster,
        namespace: ack_namespace,
        revision,
        worker_id,
        worker_incarnation,
    } = reply
    else {
        anyhow::bail!("worker returned a non-revision acknowledgement");
    };
    if ack_cluster != cluster_id || peer.cluster_id() != cluster_id {
        anyhow::bail!("revision acknowledgement cluster mismatch");
    }
    if ack_namespace != namespace.to_string() {
        anyhow::bail!("revision acknowledgement namespace mismatch");
    }
    if worker_id != worker.node.id.0 || worker_id != peer.node_id() {
        anyhow::bail!("revision acknowledgement worker identity mismatch");
    }
    if worker_incarnation != worker.incarnation_id {
        anyhow::bail!("revision acknowledgement came from a stale worker incarnation");
    }
    let revision = MappingRevision::new(revision);
    if revision < requested {
        anyhow::bail!("worker acknowledged a revision below the requested fence");
    }
    Ok(revision)
}

#[derive(Clone)]
enum ControlAccess {
    Legacy,
    PublicOnly,
    WorkerService(WorkloadIdentity),
}

impl ControlAccess {
    fn allows(&self, worker_service: bool) -> bool {
        match self {
            Self::Legacy => true,
            Self::PublicOnly => !worker_service,
            Self::WorkerService(_) => worker_service,
        }
    }

    fn authorize(&self, message: &ControlMessage) -> Result<(), String> {
        let Self::WorkerService(identity) = self else {
            return Ok(());
        };
        match message {
            ControlMessage::Register { node } => {
                validate_worker_node(identity, &node.id, node.role, "registration")
            }
            ControlMessage::Heartbeat { node, .. } => {
                validate_worker_node_id(identity, node, "legacy heartbeat")
            }
            ControlMessage::NodeStatusHeartbeat { status } => {
                if status.cluster_id != identity.cluster_id() {
                    return Err("status cluster does not match authenticated worker".into());
                }
                validate_worker_node(
                    identity,
                    &status.node.id,
                    status.node.role,
                    "status heartbeat",
                )
            }
            _ => Ok(()),
        }
    }

    fn identity(&self) -> Option<&WorkloadIdentity> {
        match self {
            Self::WorkerService(identity) => Some(identity),
            _ => None,
        }
    }
}

fn validate_worker_node(
    identity: &WorkloadIdentity,
    node_id: &talon_core::NodeId,
    role: NodeRole,
    message: &str,
) -> Result<(), String> {
    if role != NodeRole::Worker {
        return Err(format!(
            "{message} role does not match authenticated worker"
        ));
    }
    validate_worker_node_id(identity, node_id, message)
}

fn validate_worker_node_id(
    identity: &WorkloadIdentity,
    node_id: &talon_core::NodeId,
    message: &str,
) -> Result<(), String> {
    if node_id.0 != identity.node_id() {
        return Err(format!(
            "{message} node ID does not match authenticated worker"
        ));
    }
    Ok(())
}

async fn serve_worker_control(
    listener: TcpListener,
    channel: ControlTlsChannel,
    state: Arc<Coordinator>,
) -> anyhow::Result<()> {
    let conn_limit = talon_transport::ConnectionLimit::new(MAX_CONTROL_CONNECTIONS);
    loop {
        let (stream, peer) = listener.accept().await?;
        let conn_limit = conn_limit.clone();
        let channel = channel.clone();
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let _permit = conn_limit.acquire().await;
            let result = async {
                let authenticated = channel.accept(stream).await?;
                tracing::debug!(identity = %authenticated.identity, %peer, "accepted worker mTLS connection");
                handle_conn(
                    authenticated.stream,
                    state,
                    ControlAccess::WorkerService(authenticated.identity),
                )
                .await
            }
            .await;
            if let Err(error) = result {
                tracing::debug!(%peer, %error, "coordinator worker mTLS connection ended");
            }
        });
    }
}

async fn handle_conn<S>(
    mut stream: S,
    state: Arc<Coordinator>,
    access: ControlAccess,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let _connection = state.observability.metrics().track_connection();
    loop {
        let (header, message, metadata) = match read_control(&mut stream).await {
            Ok(Some(frame)) => frame,
            Ok(None) => return Ok(()),
            Err(error) => {
                state.observability.metrics().record_protocol_error();
                return Err(error);
            }
        };
        let operation =
            talon_telemetry::Operation::server(metadata.context.as_ref(), metadata.read_id);
        let result: anyhow::Result<()> = operation
            .scope(async {
                let worker_service = matches!(
                    message,
                    ControlMessage::Register { .. }
                        | ControlMessage::Heartbeat { .. }
                        | ControlMessage::NodeStatusHeartbeat { .. }
                );
                let allowed = access.allows(worker_service);
                let operation = talon_coordinator::ControlOperation::from_message(&message);
                let started = Instant::now();
                let reply = if !allowed {
                    ControlMessage::Ack {
                        ok: false,
                        detail: Some("message is not allowed on this control listener".into()),
                    }
                } else if let Err(error) = access.authorize(&message) {
                    tracing::warn!(
                        identity = ?access.identity(),
                        %error,
                        "rejected worker control identity mismatch"
                    );
                    ControlMessage::Ack {
                        ok: false,
                        detail: Some(error),
                    }
                } else {
                    state.dispatch(message).await
                };
                let error = matches!(&reply, ControlMessage::Ack { ok: false, .. });
                if error {
                    talon_telemetry::outcome("error");
                }
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
                let buffer = talon_transport::envelope::response_version(
                    codec::encode(header.request_id, &reply)?,
                    header.version,
                );
                stream.write_all(&buffer).await?;
                stream.flush().await?;
                Ok(())
            })
            .await;
        operation.outcome(if result.is_ok() { "success" } else { "error" });
        result?;
    }
}

async fn read_control<S>(
    stream: &mut S,
) -> anyhow::Result<
    Option<(
        FrameHeader,
        ControlMessage,
        talon_transport::envelope::Metadata,
    )>,
>
where
    S: AsyncRead + Unpin,
{
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
    let (metadata, _) = talon_transport::envelope::decode(&header, &payload)?;
    let (header, message) = codec::decode_request(&full)?;
    Ok(Some((header, message, metadata)))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use talon_coordinator::MemoryStateStore;
    use talon_core::{
        NodeHealth, NodeId, NodeMetricsSnapshot, NodeStatus, NODE_STATUS_SCHEMA_VERSION,
    };

    use super::*;

    fn worker_identity(id: &str) -> WorkloadIdentity {
        WorkloadIdentity::new("cluster.example", "cluster-a", WorkloadRole::Worker, id).unwrap()
    }

    fn revision_policy() -> NamespacePolicy {
        NamespacePolicy::from_toml(
            "version = 1\n\
             [[workers]]\n\
             node_id = \"worker-1\"\n\
             control_address = \"worker-1:7002\"\n\
             grants = [\"s3/data/models\"]\n\
             [[workers]]\n\
             node_id = \"worker-no-address\"\n\
             grants = [\"s3/data/models\"]\n",
        )
        .unwrap()
    }

    fn revision_ack(
        cluster_id: &str,
        namespace: &str,
        revision: u64,
        worker_id: &str,
        incarnation: &str,
    ) -> ControlMessage {
        ControlMessage::MappingRevisionAck {
            cluster_id: cluster_id.into(),
            namespace: namespace.into(),
            revision,
            worker_id: worker_id.into(),
            worker_incarnation: incarnation.into(),
        }
    }

    #[test]
    fn revision_ack_must_match_membership_and_requested_fence() {
        let worker = worker_status(
            "cluster-a",
            "worker-1",
            "worker-incarnation-1",
            "127.0.0.1:7001",
        );
        let namespace = "s3/data/models".parse().unwrap();
        let requested = MappingRevision::new(7);
        let valid = || {
            revision_ack(
                "cluster-a",
                "s3/data/models",
                7,
                "worker-1",
                "worker-incarnation-1",
            )
        };

        assert_eq!(
            validate_revision_ack(
                valid(),
                &worker_identity("worker-1"),
                &worker,
                "cluster-a",
                &namespace,
                requested,
            )
            .unwrap(),
            requested
        );
        assert!(validate_revision_ack(
            revision_ack(
                "cluster-a",
                "s3/data/models",
                7,
                "worker-1",
                "stale-incarnation",
            ),
            &worker_identity("worker-1"),
            &worker,
            "cluster-a",
            &namespace,
            requested,
        )
        .is_err());
        assert!(validate_revision_ack(
            revision_ack(
                "cluster-a",
                "s3/other",
                7,
                "worker-1",
                "worker-incarnation-1",
            ),
            &worker_identity("worker-1"),
            &worker,
            "cluster-a",
            &namespace,
            requested,
        )
        .is_err());
        assert!(validate_revision_ack(
            valid(),
            &worker_identity("worker-2"),
            &worker,
            "cluster-a",
            &namespace,
            requested,
        )
        .is_err());
        assert!(validate_revision_ack(
            revision_ack(
                "cluster-a",
                "s3/data/models",
                6,
                "worker-1",
                "worker-incarnation-1",
            ),
            &worker_identity("worker-1"),
            &worker,
            "cluster-a",
            &namespace,
            requested,
        )
        .is_err());
    }

    #[test]
    fn revision_targets_require_healthy_ready_workers_and_control_addresses() {
        let mut unhealthy = worker_status(
            "cluster-a",
            "worker-1",
            "unhealthy-incarnation",
            "127.0.0.1:7001",
        );
        unhealthy.health = NodeHealth::Unhealthy;
        let mut unready = worker_status(
            "cluster-a",
            "worker-1",
            "unready-incarnation",
            "127.0.0.1:7001",
        );
        unready.ready = false;
        let no_address = worker_status(
            "cluster-a",
            "worker-no-address",
            "no-address-incarnation",
            "127.0.0.1:7001",
        );
        let unknown = worker_status(
            "cluster-a",
            "worker-unknown",
            "unknown-incarnation",
            "127.0.0.1:7001",
        );
        let healthy = worker_status(
            "cluster-a",
            "worker-1",
            "healthy-incarnation",
            "127.0.0.1:7001",
        );
        let snapshot = talon_coordinator::ClusterSnapshot {
            nodes: vec![unhealthy, unready, no_address, unknown, healthy],
            revision: talon_coordinator::StoreRevision::new("1").unwrap(),
            observed_at_unix_ms: 1,
        };

        let targets = revision_targets(&snapshot, &revision_policy());
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].worker.incarnation_id, "healthy-incarnation");
        assert_eq!(targets[0].address, "worker-1:7002");
        assert_eq!(targets[0].namespace.to_string(), "s3/data/models");
    }

    #[test]
    fn dedicated_listener_routes_worker_service_messages_only() {
        let worker = ControlAccess::WorkerService(worker_identity("worker-1"));
        assert!(ControlAccess::Legacy.allows(true));
        assert!(ControlAccess::Legacy.allows(false));
        assert!(worker.allows(true));
        assert!(!worker.allows(false));
        assert!(!ControlAccess::PublicOnly.allows(true));
        assert!(ControlAccess::PublicOnly.allows(false));
    }

    #[test]
    fn authenticated_worker_fields_must_match_the_certificate_identity() {
        let access = ControlAccess::WorkerService(worker_identity("worker-1"));
        let matching = NodeInfo {
            id: NodeId::new("worker-1"),
            address: "127.0.0.1:7001".into(),
            role: NodeRole::Worker,
        };
        assert!(access
            .authorize(&ControlMessage::Register {
                node: matching.clone(),
            })
            .is_ok());
        assert!(access
            .authorize(&ControlMessage::Heartbeat {
                node: matching.id.clone(),
                block_count: 1,
            })
            .is_ok());

        let mut wrong_node = matching.clone();
        wrong_node.id = NodeId::new("worker-2");
        assert!(access
            .authorize(&ControlMessage::Register { node: wrong_node })
            .is_err());
        let mut wrong_role = matching;
        wrong_role.role = NodeRole::Coordinator;
        assert!(access
            .authorize(&ControlMessage::Register { node: wrong_role })
            .is_err());
        assert!(access
            .authorize(&ControlMessage::Heartbeat {
                node: NodeId::new("worker-2"),
                block_count: 1,
            })
            .is_err());

        let mut status = worker_status("cluster-a", "worker-1", "inc-1", "127.0.0.1:7001");
        assert!(access
            .authorize(&ControlMessage::NodeStatusHeartbeat {
                status: Box::new(status.clone()),
            })
            .is_ok());
        status.cluster_id = "cluster-b".into();
        assert!(access
            .authorize(&ControlMessage::NodeStatusHeartbeat {
                status: Box::new(status.clone()),
            })
            .is_err());
        status.cluster_id = "cluster-a".into();
        status.node.role = NodeRole::Coordinator;
        assert!(access
            .authorize(&ControlMessage::NodeStatusHeartbeat {
                status: Box::new(status),
            })
            .is_err());
    }

    #[tokio::test]
    async fn secure_listener_binds_status_incarnation_before_dispatch() {
        let store = Arc::new(MemoryStateStore::new());
        let observability = observability_over(
            Arc::clone(&store) as Arc<dyn ClusterStateStore>,
            "coordinator-1",
        );
        observability.check_ready().await.unwrap();
        let coordinator = Coordinator::new(Arc::clone(&observability), Duration::from_secs(30));
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let connection = tokio::spawn(handle_conn(
            server,
            coordinator,
            ControlAccess::WorkerService(worker_identity("worker-1")),
        ));

        let spoofed = ControlMessage::NodeStatusHeartbeat {
            status: Box::new(worker_status(
                "cluster-a",
                "worker-2",
                "spoofed-incarnation",
                "127.0.0.1:7002",
            )),
        };
        client
            .write_all(&codec::encode(0, &spoofed).unwrap())
            .await
            .unwrap();
        let (_, reply, _) = read_control(&mut client).await.unwrap().unwrap();
        assert!(matches!(reply, ControlMessage::Ack { ok: false, .. }));
        assert!(store.snapshot("cluster-a").await.unwrap().nodes.is_empty());

        let bound = ControlMessage::NodeStatusHeartbeat {
            status: Box::new(worker_status(
                "cluster-a",
                "worker-1",
                "bound-incarnation",
                "127.0.0.1:7001",
            )),
        };
        client
            .write_all(&codec::encode(0, &bound).unwrap())
            .await
            .unwrap();
        let (_, reply, _) = read_control(&mut client).await.unwrap().unwrap();
        assert!(matches!(reply, ControlMessage::Ack { ok: true, .. }));
        let snapshot = store.snapshot("cluster-a").await.unwrap();
        assert_eq!(snapshot.nodes.len(), 1);
        assert_eq!(snapshot.nodes[0].node.id, NodeId::new("worker-1"));
        assert_eq!(snapshot.nodes[0].incarnation_id, "bound-incarnation");

        drop(client);
        connection.await.unwrap().unwrap();
    }

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

    #[tokio::test]
    async fn stale_status_heartbeat_is_rejected_in_the_ack() {
        let store: Arc<dyn ClusterStateStore> = Arc::new(MemoryStateStore::new());
        let observability = observability_over(Arc::clone(&store), "coordinator-1");
        observability.check_ready().await.unwrap();
        let coordinator = Coordinator::new(observability, Duration::from_secs(30));

        let mut newer = worker_status(
            "cluster-a",
            "worker-1",
            "worker-incarnation",
            "127.0.0.1:7001",
        );
        newer.heartbeat_seq = 2;
        let accepted = coordinator
            .dispatch(ControlMessage::NodeStatusHeartbeat {
                status: Box::new(newer),
            })
            .await;
        assert!(matches!(accepted, ControlMessage::Ack { ok: true, .. }));

        let mut stale = worker_status(
            "cluster-a",
            "worker-1",
            "worker-incarnation",
            "127.0.0.1:7001",
        );
        stale.heartbeat_seq = 1;
        let rejected = coordinator
            .dispatch(ControlMessage::NodeStatusHeartbeat {
                status: Box::new(stale),
            })
            .await;
        assert!(matches!(
            rejected,
            ControlMessage::Ack {
                ok: false,
                detail: Some(_)
            }
        ));
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

    fn proxy_test_coordinator() -> Arc<Coordinator> {
        let store: Arc<dyn ClusterStateStore> = Arc::new(MemoryStateStore::new());
        Coordinator::new(
            observability_over(store, "proxy-test"),
            Duration::from_secs(30),
        )
    }

    fn proxy_worker(address: String) -> NodeInfo {
        NodeInfo {
            id: NodeId::new("proxy-worker"),
            address,
            role: NodeRole::Worker,
        }
    }

    fn stat_request() -> ControlMessage {
        ControlMessage::StatObject {
            object: talon_core::ObjectId::new(talon_core::Backend::S3, "bucket", "object"),
        }
    }

    fn stat_reply() -> ControlMessage {
        ControlMessage::ObjectStat {
            size: 42,
            version: "version-1".into(),
        }
    }

    #[tokio::test]
    async fn proxy_idle_pool_bounds_all_addresses_and_rejects_departed_workers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = WorkerProxyPool::new(2, WORKER_PROXY_IDLE_TTL);
        let mut peers = Vec::new();
        for address in ["worker-a", "worker-b", "worker-c"] {
            let stream = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            peers.push(listener.accept().await.unwrap().0);
            pool.release(address, stream);
        }
        assert_eq!(pool.lock_idle().total, 2);
        assert_eq!(pool.idle_count("worker-c"), 0);
        pool.lock_idle().by_address.get_mut("worker-a").unwrap()[0].returned_at =
            Instant::now() - Duration::from_secs(21);
        assert!(pool.checkout("worker-a").is_none());
        let active = pool.checkout("worker-b").unwrap();
        pool.retain_workers(&HashSet::new());
        pool.release("worker-b", active);
        assert_eq!(pool.lock_idle().total, 0);
        assert!(pool.lock_idle().by_address.is_empty());
    }

    #[tokio::test]
    async fn proxy_capacity_wait_respects_the_request_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let coordinator = Coordinator::with_proxy_limits(
            observability_over(Arc::new(MemoryStateStore::new()), "waiting-proxy"),
            Duration::from_secs(30),
            1,
            1,
            WORKER_PROXY_IDLE_TTL,
        );
        let held = coordinator.worker_proxy_slots.acquire().await.unwrap();
        let workers = [proxy_worker(listener.local_addr().unwrap().to_string())];
        let result = coordinator
            .proxy_to_workers_with_budget(&workers, stat_request(), Duration::from_millis(20))
            .await;
        assert!(matches!(result, ControlMessage::Ack { ok: false, .. }));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
        drop(held);
        assert_eq!(coordinator.worker_proxy_slots.available_permits(), 1);
    }

    #[tokio::test]
    async fn proxy_reuses_one_worker_connection_for_sequential_stats() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let accepts = Arc::new(AtomicUsize::new(0));
        let server_accepts = Arc::clone(&accepts);
        let server = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                server_accepts.fetch_add(1, Ordering::SeqCst);
                let _connection = tokio::spawn(async move {
                    while let Some((_, request, _)) = read_control(&mut stream).await.unwrap() {
                        assert!(matches!(request, ControlMessage::StatObject { .. }));
                        stream
                            .write_all(&codec::encode(0, &stat_reply()).unwrap())
                            .await
                            .unwrap();
                        stream.flush().await.unwrap();
                    }
                });
            }
        });
        let workers = vec![proxy_worker(address.clone())];
        let coordinator = proxy_test_coordinator();

        for _ in 0..100 {
            assert_eq!(
                coordinator.proxy_to_workers(&workers, stat_request()).await,
                stat_reply()
            );
        }

        assert_eq!(accepts.load(Ordering::SeqCst), 1);
        assert_eq!(coordinator.worker_proxy_pool.idle_count(&address), 1);
        server.abort();
    }

    #[tokio::test]
    async fn proxy_retries_a_peer_closed_idle_connection_once() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let accepts = Arc::new(AtomicUsize::new(0));
        let server_accepts = Arc::clone(&accepts);
        let (first_closed_tx, first_closed_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut first_closed_tx = Some(first_closed_tx);
            for accepted in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                server_accepts.fetch_add(1, Ordering::SeqCst);
                let (_, request, _) = read_control(&mut stream).await.unwrap().unwrap();
                assert!(matches!(request, ControlMessage::StatObject { .. }));
                stream
                    .write_all(&codec::encode(0, &stat_reply()).unwrap())
                    .await
                    .unwrap();
                stream.flush().await.unwrap();
                drop(stream);
                if accepted == 0 {
                    first_closed_tx.take().unwrap().send(()).unwrap();
                }
            }
        });
        let workers = vec![proxy_worker(address.clone())];
        let coordinator = proxy_test_coordinator();

        assert_eq!(
            coordinator.proxy_to_workers(&workers, stat_request()).await,
            stat_reply()
        );
        first_closed_rx.await.unwrap();
        assert_eq!(
            coordinator.proxy_to_workers(&workers, stat_request()).await,
            stat_reply()
        );

        server.await.unwrap();
        assert_eq!(accepts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn proxy_concurrency_limit_applies_backpressure_before_connecting() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let accepts = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let server_accepts = Arc::clone(&accepts);
        let server_active = Arc::clone(&active);
        let server_max_active = Arc::clone(&max_active);
        let server_release = Arc::clone(&release);
        let server = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                server_accepts.fetch_add(1, Ordering::SeqCst);
                let active = Arc::clone(&server_active);
                let max_active = Arc::clone(&server_max_active);
                let release = Arc::clone(&server_release);
                let started_tx = started_tx.clone();
                let _connection = tokio::spawn(async move {
                    while let Some((_, request, _)) = read_control(&mut stream).await.unwrap() {
                        assert!(matches!(request, ControlMessage::StatObject { .. }));
                        let now_active = active.fetch_add(1, Ordering::SeqCst) + 1;
                        max_active.fetch_max(now_active, Ordering::SeqCst);
                        started_tx.send(()).unwrap();
                        let permit = Arc::clone(&release).acquire_owned().await.unwrap();
                        permit.forget();
                        stream
                            .write_all(&codec::encode(0, &stat_reply()).unwrap())
                            .await
                            .unwrap();
                        stream.flush().await.unwrap();
                        active.fetch_sub(1, Ordering::SeqCst);
                    }
                });
            }
        });

        let coordinator = Coordinator::with_proxy_limits(
            observability_over(Arc::new(MemoryStateStore::new()), "bounded-proxy"),
            Duration::from_secs(30),
            2,
            2,
            WORKER_PROXY_IDLE_TTL,
        );
        let workers = vec![proxy_worker(address)];
        let mut requests = Vec::new();
        for _ in 0..4 {
            let coordinator = Arc::clone(&coordinator);
            let workers = workers.clone();
            requests.push(tokio::spawn(async move {
                coordinator.proxy_to_workers(&workers, stat_request()).await
            }));
        }

        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
                .await
                .expect("two requests did not reach the worker")
                .expect("worker start channel closed");
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), started_rx.recv())
                .await
                .is_err(),
            "a third request reached the worker before a proxy slot was released"
        );

        release.add_permits(4);
        for request in requests {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), request)
                    .await
                    .expect("bounded proxy request stalled")
                    .unwrap(),
                stat_reply()
            );
        }
        assert_eq!(max_active.load(Ordering::SeqCst), 2);
        assert_eq!(accepts.load(Ordering::SeqCst), 2);
        server.abort();
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
            let (_, request, _) = read_control(&mut stream).await.unwrap().unwrap();
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
            let (_, request, _) = read_control(&mut stream).await.unwrap().unwrap();
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
        let reply = proxy_test_coordinator()
            .proxy_to_workers(
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
            let (_, request, _) = read_control(&mut stream).await.unwrap().unwrap();
            assert!(matches!(request, ControlMessage::ListObjects { .. }));
            stalled_request_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });

        let serving_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let serving_address = serving_listener.local_addr().unwrap().to_string();
        let serving_server = tokio::spawn(async move {
            let (mut stream, _) = serving_listener.accept().await.unwrap();
            let (_, request, _) = read_control(&mut stream).await.unwrap().unwrap();
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
            proxy_test_coordinator()
                .proxy_to_workers_with_budget(
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
            let (_, request, _) = read_control(&mut stream).await.unwrap().unwrap();
            assert!(matches!(request, ControlMessage::ListObjects { .. }));
            first_request_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });

        let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_address = second_listener.local_addr().unwrap().to_string();
        let (second_request_tx, second_request_rx) = tokio::sync::oneshot::channel();
        let second_server = tokio::spawn(async move {
            let (mut stream, _) = second_listener.accept().await.unwrap();
            let (_, request, _) = read_control(&mut stream).await.unwrap().unwrap();
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
            proxy_test_coordinator()
                .proxy_to_workers_with_budget(
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
