//! Production etcd v3 [`ClusterStateStore`] backend.
//!
//! Node records live under a cluster-scoped prefix and are attached to an etcd
//! lease, so a crashed coordinator's record disappears automatically once its
//! heartbeat stops refreshing the lease. Snapshots are linearizable and return
//! the etcd response revision; watches resume from that revision and recover
//! from compaction by relisting.
//!
//! The incarnation/heartbeat-sequence ordering rule from the backend contract is
//! enforced with a read then a `mod_revision`-guarded transaction, retried a
//! bounded number of times under write contention.

use std::fmt;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use etcd_client::{
    Certificate, Client, Compare, CompareOp, ConnectOptions, EventType, GetOptions, Identity,
    PutOptions, TlsOptions, Txn, TxnOp, WatchOptions, WatchStream,
};
use serde::Deserialize;
use talon_core::{NodeId, NodeRole, NodeStatus};
use tokio::time::timeout;

use super::{
    BackendHealth, ClusterSnapshot, ClusterStateStore, ClusterStateWatch, NodeEvent, NodeEventKind,
    StateBackend, StateStoreError, StateStoreResult, StoreRevision, WriteDisposition, WriteResult,
};

/// Default keyspace prefix for all Talon cluster state.
pub const DEFAULT_ETCD_PREFIX: &str = "/talon";

/// Maximum number of optimistic retries for a single mutation before the write
/// is reported as unavailable.
const MAX_WRITE_RETRIES: usize = 32;

const REVISION_BACKEND: StateBackend = StateBackend::Etcd;

/// TLS material for connecting to an etcd cluster.
#[derive(Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EtcdTlsConfig {
    /// PEM-encoded certificate authority used to verify the etcd servers.
    pub ca_cert_path: Option<PathBuf>,
    /// PEM-encoded client certificate for mutual TLS.
    pub client_cert_path: Option<PathBuf>,
    /// PEM-encoded client private key for mutual TLS.
    pub client_key_path: Option<PathBuf>,
    /// Optional domain name override used for certificate verification.
    pub domain_name: Option<String>,
}

impl fmt::Debug for EtcdTlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EtcdTlsConfig")
            .field("ca_cert_path", &self.ca_cert_path)
            .field("client_cert_path", &self.client_cert_path)
            .field(
                "client_key_path",
                &self.client_key_path.as_ref().map(|_| "<redacted>"),
            )
            .field("domain_name", &self.domain_name)
            .finish()
    }
}

/// etcd backend connection and keyspace configuration.
///
/// `request_timeout` and `lease_ttl` are supplied separately from the shared
/// [`ClusterStateConfig`](super::ClusterStateConfig) so lease timing stays
/// backend-neutral.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EtcdConfig {
    /// One or more `host:port` etcd endpoints.
    pub endpoints: Vec<String>,
    /// Optional username for password authentication.
    pub username: Option<String>,
    /// Optional password for password authentication. Never logged.
    pub password: Option<String>,
    /// Optional transport security.
    pub tls: Option<EtcdTlsConfig>,
    /// Keyspace prefix shared by every Talon record.
    pub prefix: String,
}

impl Default for EtcdConfig {
    fn default() -> Self {
        Self {
            endpoints: Vec::new(),
            username: None,
            password: None,
            tls: None,
            prefix: DEFAULT_ETCD_PREFIX.to_string(),
        }
    }
}

impl fmt::Debug for EtcdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EtcdConfig")
            .field("endpoints", &self.endpoints)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("tls", &self.tls)
            .field("prefix", &self.prefix)
            .finish()
    }
}

/// Invalid etcd backend configuration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EtcdConfigError {
    /// No endpoints were supplied.
    #[error("etcd backend requires at least one endpoint")]
    NoEndpoints,
    /// An endpoint entry was blank.
    #[error("etcd endpoint must not be empty")]
    EmptyEndpoint,
    /// The keyspace prefix was blank.
    #[error("etcd prefix must not be empty")]
    EmptyPrefix,
    /// A username was supplied without a password or vice versa.
    #[error("etcd authentication requires both a username and a password")]
    IncompleteCredentials,
    /// A client certificate was supplied without a matching key or vice versa.
    #[error("etcd mutual TLS requires both a client certificate and a client key")]
    IncompleteClientIdentity,
}

impl EtcdConfig {
    /// Validate endpoints, prefix, credentials, and TLS material pairing.
    pub fn validate(&self) -> Result<(), EtcdConfigError> {
        if self.endpoints.is_empty() {
            return Err(EtcdConfigError::NoEndpoints);
        }
        if self
            .endpoints
            .iter()
            .any(|endpoint| endpoint.trim().is_empty())
        {
            return Err(EtcdConfigError::EmptyEndpoint);
        }
        if self.prefix.trim().is_empty() {
            return Err(EtcdConfigError::EmptyPrefix);
        }
        if self.username.is_some() != self.password.is_some() {
            return Err(EtcdConfigError::IncompleteCredentials);
        }
        if let Some(tls) = &self.tls {
            if tls.client_cert_path.is_some() != tls.client_key_path.is_some() {
                return Err(EtcdConfigError::IncompleteClientIdentity);
            }
        }
        Ok(())
    }

    fn normalized_prefix(&self) -> String {
        self.prefix.trim_end_matches('/').to_string()
    }
}

/// Strongly consistent etcd-backed [`ClusterStateStore`].
pub struct EtcdStateStore {
    client: Client,
    prefix: String,
    request_timeout: Duration,
}

impl EtcdStateStore {
    /// Connect to etcd and construct a store.
    ///
    /// `lease_ttl` and `request_timeout` come from the shared cluster-state
    /// configuration. Lease TTLs are rounded up to whole seconds because etcd
    /// lease granularity is one second.
    pub async fn connect(
        config: &EtcdConfig,
        lease_ttl: Duration,
        request_timeout: Duration,
    ) -> StateStoreResult<Self> {
        config
            .validate()
            .map_err(|error| StateStoreError::Unavailable {
                backend: REVISION_BACKEND,
                detail: error.to_string(),
            })?;
        // Fail fast if the configured lease TTL cannot be represented in etcd's
        // one-second granularity before opening a connection.
        lease_ttl_to_seconds(lease_ttl)?;

        let options = build_connect_options(config)?;
        let client = timeout(
            request_timeout,
            Client::connect(config.endpoints.clone(), Some(options)),
        )
        .await
        .map_err(|_| StateStoreError::Timeout {
            backend: REVISION_BACKEND,
        })?
        .map_err(map_connect_error)?;

        Ok(Self {
            client,
            prefix: config.normalized_prefix(),
            request_timeout,
        })
    }

    /// Build a store from an already-connected client (used by tests).
    pub fn from_client(
        client: Client,
        prefix: impl Into<String>,
        request_timeout: Duration,
    ) -> StateStoreResult<Self> {
        Ok(Self {
            client,
            prefix: prefix.into().trim_end_matches('/').to_string(),
            request_timeout,
        })
    }

    fn cluster_prefix(&self, cluster_id: &str) -> String {
        format!("{}/clusters/{cluster_id}/nodes/", self.prefix)
    }

    fn node_key(&self, status: &NodeStatus) -> String {
        self.record_key(&status.cluster_id, status.node.role, &status.node.id)
    }

    fn record_key(&self, cluster_id: &str, role: NodeRole, node_id: &NodeId) -> String {
        format!(
            "{}{}/{}",
            self.cluster_prefix(cluster_id),
            role_segment(role),
            node_id.0
        )
    }

    async fn with_timeout<T, F>(&self, future: F) -> StateStoreResult<T>
    where
        F: std::future::Future<Output = Result<T, etcd_client::Error>>,
    {
        match timeout(self.request_timeout, future).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(map_etcd_error(error)),
            Err(_) => Err(StateStoreError::Timeout {
                backend: REVISION_BACKEND,
            }),
        }
    }
}

#[async_trait]
impl ClusterStateStore for EtcdStateStore {
    fn backend(&self) -> StateBackend {
        StateBackend::Etcd
    }

    async fn upsert_node(
        &self,
        status: NodeStatus,
        lease_ttl: Duration,
    ) -> StateStoreResult<WriteResult> {
        status.validate()?;
        let ttl_seconds = lease_ttl_to_seconds(lease_ttl)?;
        let key = self.node_key(&status);
        let value = serde_json::to_vec(&status).map_err(|error| StateStoreError::Unavailable {
            backend: REVISION_BACKEND,
            detail: format!("failed to encode node status: {error}"),
        })?;

        let mut client = self.client.clone();
        for _ in 0..MAX_WRITE_RETRIES {
            // Linearizable read of the current record and its lease.
            let current = self.with_timeout(client.get(key.clone(), None)).await?;
            let header_revision = current
                .header()
                .map(|header| header.revision())
                .unwrap_or_default();
            let existing = current.kvs().first();

            if let Some(kv) = existing {
                let current_status: NodeStatus = decode_status(kv.value())?;
                if let Some(disposition) = disposition_for(&current_status, &status) {
                    return Ok(WriteResult {
                        disposition,
                        revision: revision(header_revision)?,
                    });
                }
            }
            let observed_mod_revision = existing.map(|kv| kv.mod_revision()).unwrap_or(0);
            let previous_lease = existing.map(|kv| kv.lease()).unwrap_or(0);

            // Fresh lease for this heartbeat; overwriting the key detaches the
            // previous lease, which we revoke best-effort after committing.
            let lease = self
                .with_timeout(client.lease_grant(ttl_seconds, None))
                .await?;

            // Guard the write on the record being unchanged since the read so a
            // concurrent writer cannot clobber a newer sequence.
            let compare = if observed_mod_revision == 0 {
                Compare::create_revision(key.clone(), CompareOp::Equal, 0)
            } else {
                Compare::mod_revision(key.clone(), CompareOp::Equal, observed_mod_revision)
            };
            let txn = Txn::new().when(vec![compare]).and_then(vec![TxnOp::put(
                key.clone(),
                value.clone(),
                Some(PutOptions::new().with_lease(lease.id())),
            )]);
            let response = self.with_timeout(client.txn(txn)).await?;

            if response.succeeded() {
                let committed = response
                    .header()
                    .map(|header| header.revision())
                    .unwrap_or(header_revision);
                if previous_lease != 0 {
                    // Best-effort: an orphaned lease would expire on its own.
                    let _ = client.lease_revoke(previous_lease).await;
                }
                return Ok(WriteResult {
                    disposition: WriteDisposition::Applied,
                    revision: revision(committed)?,
                });
            }

            // Lost the race; drop the unused lease and retry with a fresh read.
            let _ = client.lease_revoke(lease.id()).await;
        }

        Err(StateStoreError::Unavailable {
            backend: REVISION_BACKEND,
            detail: "exceeded optimistic retry budget under write contention".into(),
        })
    }

    async fn remove_node(
        &self,
        cluster_id: &str,
        node_id: &NodeId,
        incarnation_id: &str,
    ) -> StateStoreResult<WriteResult> {
        let mut client = self.client.clone();
        for _ in 0..MAX_WRITE_RETRIES {
            // The role is part of the key, so scan the cluster prefix to find
            // the record regardless of role.
            let record = self.find_record(&mut client, cluster_id, node_id).await?;
            let Some((key, kv_value, mod_revision, lease, header_revision)) = record else {
                let latest = self
                    .with_timeout(client.get(
                        self.cluster_prefix(cluster_id),
                        Some(GetOptions::new().with_prefix().with_count_only()),
                    ))
                    .await?;
                let header_revision = latest
                    .header()
                    .map(|header| header.revision())
                    .unwrap_or_default();
                return Ok(WriteResult {
                    disposition: WriteDisposition::NotFound,
                    revision: revision(header_revision)?,
                });
            };

            let current_status: NodeStatus = decode_status(&kv_value)?;
            if current_status.incarnation_id != incarnation_id {
                return Ok(WriteResult {
                    disposition: WriteDisposition::Stale,
                    revision: revision(header_revision)?,
                });
            }

            let txn = Txn::new()
                .when(vec![Compare::mod_revision(
                    key.clone(),
                    CompareOp::Equal,
                    mod_revision,
                )])
                .and_then(vec![TxnOp::delete(key.clone(), None)]);
            let response = self.with_timeout(client.txn(txn)).await?;
            if response.succeeded() {
                let committed = response
                    .header()
                    .map(|header| header.revision())
                    .unwrap_or(header_revision);
                if lease != 0 {
                    let _ = client.lease_revoke(lease).await;
                }
                return Ok(WriteResult {
                    disposition: WriteDisposition::Applied,
                    revision: revision(committed)?,
                });
            }
            // Concurrent change; retry.
        }
        Err(StateStoreError::Unavailable {
            backend: REVISION_BACKEND,
            detail: "exceeded optimistic retry budget while removing node".into(),
        })
    }

    async fn snapshot(&self, cluster_id: &str) -> StateStoreResult<ClusterSnapshot> {
        let mut client = self.client.clone();
        let response = self
            .with_timeout(client.get(
                self.cluster_prefix(cluster_id),
                Some(GetOptions::new().with_prefix()),
            ))
            .await?;
        let header_revision = response
            .header()
            .map(|header| header.revision())
            .unwrap_or_default();
        let mut nodes = Vec::with_capacity(response.kvs().len());
        for kv in response.kvs() {
            nodes.push(decode_status(kv.value())?);
        }
        nodes.sort_by(|a, b| a.node.id.0.cmp(&b.node.id.0));
        Ok(ClusterSnapshot {
            nodes,
            revision: revision(header_revision)?,
            observed_at_unix_ms: now_unix_ms(),
        })
    }

    async fn watch(
        &self,
        cluster_id: &str,
        after_revision: Option<&StoreRevision>,
    ) -> StateStoreResult<Box<dyn ClusterStateWatch>> {
        let mut client = self.client.clone();
        let start_revision = match after_revision {
            Some(revision) => parse_revision(revision)?.saturating_add(1),
            None => {
                let response = self
                    .with_timeout(client.get(
                        self.cluster_prefix(cluster_id),
                        Some(GetOptions::new().with_prefix().with_count_only()),
                    ))
                    .await?;
                response
                    .header()
                    .map(|header| header.revision())
                    .unwrap_or_default()
                    .saturating_add(1)
            }
        };

        let options = WatchOptions::new()
            .with_prefix()
            .with_start_revision(start_revision);
        let stream = self
            .with_timeout(client.watch(self.cluster_prefix(cluster_id), Some(options)))
            .await?;
        Ok(Box::new(EtcdWatch {
            cluster_id: cluster_id.to_string(),
            stream,
            buffered: Vec::new(),
        }))
    }

    async fn check_ready(&self) -> StateStoreResult<BackendHealth> {
        let mut client = self.client.clone();
        // A linearizable range read confirms the backend can serve
        // authoritative requests and yields the current revision cheaply.
        let response = self
            .with_timeout(client.get(
                self.prefix.clone(),
                Some(GetOptions::new().with_prefix().with_count_only()),
            ))
            .await?;
        let header_revision = response
            .header()
            .map(|header| header.revision())
            .unwrap_or_default();
        Ok(BackendHealth {
            backend: StateBackend::Etcd,
            checked_at_unix_ms: now_unix_ms(),
            revision: Some(revision(header_revision)?),
        })
    }
}

impl EtcdStateStore {
    #[allow(clippy::type_complexity)]
    async fn find_record(
        &self,
        client: &mut Client,
        cluster_id: &str,
        node_id: &NodeId,
    ) -> StateStoreResult<Option<(String, Vec<u8>, i64, i64, i64)>> {
        let response = self
            .with_timeout(client.get(
                self.cluster_prefix(cluster_id),
                Some(GetOptions::new().with_prefix()),
            ))
            .await?;
        let header_revision = response
            .header()
            .map(|header| header.revision())
            .unwrap_or_default();
        for kv in response.kvs() {
            let key = String::from_utf8_lossy(kv.key()).to_string();
            if key.rsplit('/').next() == Some(node_id.0.as_str()) {
                return Ok(Some((
                    key,
                    kv.value().to_vec(),
                    kv.mod_revision(),
                    kv.lease(),
                    header_revision,
                )));
            }
        }
        Ok(None)
    }
}

/// Live etcd watch adapted to the ordered [`ClusterStateWatch`] contract.
struct EtcdWatch {
    cluster_id: String,
    stream: WatchStream,
    buffered: Vec<NodeEvent>,
}

#[async_trait]
impl ClusterStateWatch for EtcdWatch {
    async fn next(&mut self) -> StateStoreResult<NodeEvent> {
        loop {
            if !self.buffered.is_empty() {
                return Ok(self.buffered.remove(0));
            }
            let message = self.stream.message().await.map_err(map_etcd_error)?;
            let Some(response) = message else {
                return Err(StateStoreError::Unavailable {
                    backend: REVISION_BACKEND,
                    detail: "watch stream closed".into(),
                });
            };
            if response.canceled() {
                let compact_revision = response.compact_revision();
                if compact_revision > 0 {
                    return Err(StateStoreError::Compacted {
                        requested: revision(compact_revision.saturating_sub(1).max(1))?,
                        oldest: revision(compact_revision)?,
                    });
                }
                return Err(StateStoreError::Unavailable {
                    backend: REVISION_BACKEND,
                    detail: "watch canceled by backend".into(),
                });
            }
            for event in response.events() {
                let Some(kv) = event.kv() else { continue };
                let observed_at_unix_ms = now_unix_ms();
                let event_revision = revision(kv.mod_revision())?;
                match event.event_type() {
                    EventType::Put => {
                        let status: NodeStatus = decode_status(kv.value())?;
                        self.buffered.push(NodeEvent {
                            cluster_id: status.cluster_id.clone(),
                            node_id: status.node.id.clone(),
                            kind: NodeEventKind::Upserted,
                            status: Some(status),
                            revision: event_revision,
                            observed_at_unix_ms,
                        });
                    }
                    EventType::Delete => {
                        let key = String::from_utf8_lossy(kv.key());
                        let node_id = key.rsplit('/').next().unwrap_or_default().to_string();
                        self.buffered.push(NodeEvent {
                            cluster_id: self.cluster_id.clone(),
                            node_id: NodeId::new(node_id),
                            kind: NodeEventKind::Removed,
                            status: None,
                            revision: event_revision,
                            observed_at_unix_ms,
                        });
                    }
                }
            }
        }
    }
}

fn role_segment(role: NodeRole) -> &'static str {
    match role {
        NodeRole::Coordinator => "coordinator",
        NodeRole::Worker => "worker",
    }
}

/// Reproduce the backend contract's incarnation/sequence ordering rule.
///
/// Returns `Some(disposition)` when the write must be rejected as a no-op and
/// `None` when it should be applied.
fn disposition_for(current: &NodeStatus, incoming: &NodeStatus) -> Option<WriteDisposition> {
    if current.incarnation_id == incoming.incarnation_id {
        match incoming.heartbeat_seq.cmp(&current.heartbeat_seq) {
            std::cmp::Ordering::Less => Some(WriteDisposition::Stale),
            std::cmp::Ordering::Equal => Some(WriteDisposition::Duplicate),
            std::cmp::Ordering::Greater => None,
        }
    } else if incoming.started_at_unix_ms < current.started_at_unix_ms
        || (incoming.started_at_unix_ms == current.started_at_unix_ms
            && incoming.reported_at_unix_ms <= current.reported_at_unix_ms)
    {
        Some(WriteDisposition::Stale)
    } else {
        None
    }
}

fn decode_status(bytes: &[u8]) -> StateStoreResult<NodeStatus> {
    serde_json::from_slice(bytes).map_err(|error| StateStoreError::Unavailable {
        backend: REVISION_BACKEND,
        detail: format!("failed to decode node status: {error}"),
    })
}

fn revision(value: i64) -> StateStoreResult<StoreRevision> {
    StoreRevision::new(format!("etcd:{value}"))
}

fn parse_revision(revision: &StoreRevision) -> StateStoreResult<i64> {
    revision
        .as_str()
        .strip_prefix("etcd:")
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| StateStoreError::InvalidRevision {
            backend: Some(StateBackend::Etcd),
            revision: revision.to_string(),
            detail: "expected etcd:<i64>",
        })
}

fn lease_ttl_to_seconds(ttl: Duration) -> StateStoreResult<i64> {
    if ttl.is_zero() {
        return Err(StateStoreError::InvalidLeaseTtl(ttl));
    }
    // Round up to whole seconds: etcd lease granularity is one second, and a
    // shorter effective TTL than requested would expire records too early.
    let millis = ttl.as_millis();
    let seconds = millis.div_ceil(1_000);
    i64::try_from(seconds)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(StateStoreError::InvalidLeaseTtl(ttl))
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn build_connect_options(config: &EtcdConfig) -> StateStoreResult<ConnectOptions> {
    let mut options = ConnectOptions::new();
    if let (Some(user), Some(password)) = (&config.username, &config.password) {
        options = options.with_user(user.clone(), password.clone());
    }
    if let Some(tls) = &config.tls {
        let mut tls_options = TlsOptions::new();
        if let Some(ca_path) = &tls.ca_cert_path {
            let pem = read_pem(ca_path)?;
            tls_options = tls_options.ca_certificate(Certificate::from_pem(pem));
        }
        if let (Some(cert_path), Some(key_path)) = (&tls.client_cert_path, &tls.client_key_path) {
            let cert = read_pem(cert_path)?;
            let key = read_pem(key_path)?;
            tls_options = tls_options.identity(Identity::from_pem(cert, key));
        }
        if let Some(domain) = &tls.domain_name {
            tls_options = tls_options.domain_name(domain.clone());
        }
        options = options.with_tls(tls_options);
    }
    Ok(options)
}

fn read_pem(path: &std::path::Path) -> StateStoreResult<Vec<u8>> {
    std::fs::read(path).map_err(|error| StateStoreError::Unavailable {
        backend: REVISION_BACKEND,
        // Path only; never surfaces certificate/key material.
        detail: format!("failed to read TLS material at {}: {error}", path.display()),
    })
}

fn map_connect_error(error: etcd_client::Error) -> StateStoreError {
    match map_etcd_error(error) {
        // A failed connection is always an availability problem regardless of
        // the underlying transport detail.
        StateStoreError::Unavailable { detail, .. } => StateStoreError::Unavailable {
            backend: REVISION_BACKEND,
            detail,
        },
        other => other,
    }
}

fn map_etcd_error(error: etcd_client::Error) -> StateStoreError {
    use etcd_client::Error as E;
    match error {
        E::GRpcStatus(status) => match status.code() {
            tonic::Code::Unauthenticated => StateStoreError::Authentication {
                backend: REVISION_BACKEND,
            },
            tonic::Code::PermissionDenied => StateStoreError::PermissionDenied {
                backend: REVISION_BACKEND,
            },
            tonic::Code::DeadlineExceeded => StateStoreError::Timeout {
                backend: REVISION_BACKEND,
            },
            code => StateStoreError::Unavailable {
                backend: REVISION_BACKEND,
                detail: format!("etcd rpc failed: {code}"),
            },
        },
        other => StateStoreError::Unavailable {
            backend: REVISION_BACKEND,
            detail: sanitize_transport_error(&other),
        },
    }
}

/// Produce a stable, credential-free description of a transport-level failure.
fn sanitize_transport_error(error: &etcd_client::Error) -> String {
    use etcd_client::Error as E;
    match error {
        E::InvalidArgs(_) => "invalid etcd request arguments".into(),
        E::InvalidUri(_) => "invalid etcd endpoint uri".into(),
        E::TransportError(_) => "etcd transport error".into(),
        E::WatchError(_) => "etcd watch error".into(),
        E::LeaseKeepAliveError(_) => "etcd lease keep-alive error".into(),
        E::ElectError(_) => "etcd election error".into(),
        _ => "etcd backend error".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_ttl_rounds_up_to_whole_seconds() {
        assert_eq!(lease_ttl_to_seconds(Duration::from_millis(100)).unwrap(), 1);
        assert_eq!(
            lease_ttl_to_seconds(Duration::from_millis(1_000)).unwrap(),
            1
        );
        assert_eq!(
            lease_ttl_to_seconds(Duration::from_millis(1_001)).unwrap(),
            2
        );
        assert_eq!(lease_ttl_to_seconds(Duration::from_secs(30)).unwrap(), 30);
        assert!(matches!(
            lease_ttl_to_seconds(Duration::ZERO),
            Err(StateStoreError::InvalidLeaseTtl(_))
        ));
    }

    #[test]
    fn revision_round_trips_through_opaque_token() {
        let token = revision(42).unwrap();
        assert_eq!(token.as_str(), "etcd:42");
        assert_eq!(parse_revision(&token).unwrap(), 42);
        assert!(matches!(
            parse_revision(&StoreRevision::new("memory:7").unwrap()),
            Err(StateStoreError::InvalidRevision { .. })
        ));
    }

    #[test]
    fn disposition_enforces_incarnation_and_sequence_rule() {
        let base = crate::state_store::testkit::worker_status("w1", "inc-1", 5);
        let duplicate = crate::state_store::testkit::worker_status("w1", "inc-1", 5);
        assert_eq!(
            disposition_for(&base, &duplicate),
            Some(WriteDisposition::Duplicate)
        );
        let older = crate::state_store::testkit::worker_status("w1", "inc-1", 4);
        assert_eq!(
            disposition_for(&base, &older),
            Some(WriteDisposition::Stale)
        );
        let newer = crate::state_store::testkit::worker_status("w1", "inc-1", 6);
        assert_eq!(disposition_for(&base, &newer), None);

        let restart = crate::state_store::testkit::worker_status("w1", "inc-2", 0);
        assert_eq!(disposition_for(&base, &restart), None);
        let stale_restart = crate::state_store::testkit::worker_status("w1", "inc-1", 99);
        assert_eq!(
            disposition_for(&restart, &stale_restart),
            Some(WriteDisposition::Stale)
        );
    }

    #[test]
    fn config_validation_catches_missing_and_mismatched_fields() {
        assert_eq!(
            EtcdConfig::default().validate(),
            Err(EtcdConfigError::NoEndpoints)
        );
        let mut config = EtcdConfig {
            endpoints: vec!["localhost:2379".into()],
            ..Default::default()
        };
        config.validate().unwrap();

        config.username = Some("root".into());
        assert_eq!(
            config.validate(),
            Err(EtcdConfigError::IncompleteCredentials)
        );
        config.password = Some("secret".into());
        config.validate().unwrap();

        config.tls = Some(EtcdTlsConfig {
            client_cert_path: Some("cert.pem".into()),
            ..Default::default()
        });
        assert_eq!(
            config.validate(),
            Err(EtcdConfigError::IncompleteClientIdentity)
        );
    }

    #[test]
    fn debug_redacts_password_and_key_paths() {
        let config = EtcdConfig {
            endpoints: vec!["localhost:2379".into()],
            username: Some("root".into()),
            password: Some("super-secret".into()),
            tls: Some(EtcdTlsConfig {
                client_key_path: Some("/etc/talon/client.key".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("super-secret"));
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("client.key"));
    }
}
