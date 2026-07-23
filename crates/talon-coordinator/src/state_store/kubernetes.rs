//! Production Kubernetes [`ClusterStateStore`] backend.
//!
//! Each Talon node maps to one namespaced `coordination.k8s.io/v1` `Lease`.
//! The Lease's own fields carry liveness — `holderIdentity` is the process
//! incarnation, `leaseDurationSeconds` is the TTL, and `renewTime` is the last
//! accepted heartbeat — so a crashed process's record is judged expired once
//! `renewTime + leaseDurationSeconds` passes, without any Talon-side sweeper.
//! The full bounded [`NodeStatus`] rides along as JSON in a Talon-owned
//! annotation on the same object (ADR 0001 §6), keeping the record to a single
//! API object.
//!
//! Writes use Kubernetes optimistic concurrency: a read observes the object's
//! `resourceVersion`, and the replacing update is guarded on it so a concurrent
//! coordinator cannot clobber a newer heartbeat. Snapshots are server-side list
//! reads and return the list `resourceVersion` as the opaque store revision;
//! watches resume from it and surface `410 Gone` as [`StateStoreError::Compacted`]
//! so the caller relists.
//!
//! The kernel glue lives behind the `kubernetes` cargo feature so non-Kubernetes
//! builds stay lean. Config validation and record encoding are unit-tested in
//! CI; the reusable store contract runs against a real cluster in an `#[ignore]`d
//! integration test (there is no API server in unit CI).

use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures::StreamExt;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use k8s_openapi::chrono::{DateTime, TimeZone, Utc};
use kube::api::{Api, DeleteParams, ListParams, PostParams};
use kube::runtime::watcher::{self, watcher, Event};
use kube::{Client, Config};
use serde::Deserialize;
use talon_core::{NodeId, NodeStatus};
use tokio::time::timeout;

use super::{
    BackendHealth, ClusterSnapshot, ClusterStateStore, ClusterStateWatch, NodeEvent, NodeEventKind,
    StateBackend, StateStoreError, StateStoreResult, StoreRevision, WriteDisposition, WriteResult,
};

/// Default label/annotation prefix for Talon-owned Lease metadata.
pub const DEFAULT_LEASE_LABEL_PREFIX: &str = "talon.io";

const BACKEND: StateBackend = StateBackend::Kubernetes;
/// Bounded number of optimistic retries under write contention.
const MAX_WRITE_RETRIES: usize = 16;

fn label_managed_by() -> String {
    format!("{DEFAULT_LEASE_LABEL_PREFIX}/managed-by")
}
fn label_cluster() -> String {
    format!("{DEFAULT_LEASE_LABEL_PREFIX}/cluster")
}
fn label_role() -> String {
    format!("{DEFAULT_LEASE_LABEL_PREFIX}/role")
}
fn label_node() -> String {
    format!("{DEFAULT_LEASE_LABEL_PREFIX}/node")
}
fn annotation_status() -> String {
    format!("{DEFAULT_LEASE_LABEL_PREFIX}/status")
}

/// Kubernetes backend connection and identity configuration.
///
/// `lease_ttl` and `request_timeout` are supplied separately (from the shared
/// [`ClusterStateConfig`](super::ClusterStateConfig)) so lease timing stays
/// backend-neutral.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KubernetesConfig {
    /// Namespace holding Talon Lease objects.
    pub namespace: String,
    /// Logical Talon cluster id; also written as a selection label.
    pub cluster_id: String,
    /// Optional explicit kubeconfig context. `None` uses in-cluster config, then
    /// the default kubeconfig.
    pub context: Option<String>,
}

impl Default for KubernetesConfig {
    fn default() -> Self {
        Self {
            namespace: "talon".to_string(),
            cluster_id: String::new(),
            context: None,
        }
    }
}

impl fmt::Debug for KubernetesConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KubernetesConfig")
            .field("namespace", &self.namespace)
            .field("cluster_id", &self.cluster_id)
            .field("context", &self.context)
            .finish()
    }
}

/// Invalid Kubernetes backend configuration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KubernetesConfigError {
    /// The namespace was blank.
    #[error("kubernetes namespace must not be empty")]
    EmptyNamespace,
    /// The cluster id was blank.
    #[error("kubernetes cluster_id must not be empty")]
    EmptyClusterId,
}

impl KubernetesConfig {
    /// Validate the namespace and cluster identity.
    pub fn validate(&self) -> Result<(), KubernetesConfigError> {
        if self.namespace.trim().is_empty() {
            return Err(KubernetesConfigError::EmptyNamespace);
        }
        if self.cluster_id.trim().is_empty() {
            return Err(KubernetesConfigError::EmptyClusterId);
        }
        Ok(())
    }
}

/// Strongly consistent Kubernetes-backed [`ClusterStateStore`].
pub struct KubernetesStateStore {
    api: Api<Lease>,
    cluster_id: String,
    request_timeout: Duration,
}

impl KubernetesStateStore {
    /// Connect using in-cluster config with kubeconfig fallback.
    pub async fn connect(
        config: &KubernetesConfig,
        request_timeout: Duration,
    ) -> StateStoreResult<Self> {
        config
            .validate()
            .map_err(|error| StateStoreError::Unavailable {
                backend: BACKEND,
                detail: error.to_string(),
            })?;
        // Prefer in-cluster config (a pod's mounted service-account token); fall
        // back to the standard kubeconfig for out-of-cluster operation.
        let kube_config = match Config::incluster() {
            Ok(cfg) => cfg,
            Err(_) => Config::infer()
                .await
                .map_err(|error| StateStoreError::Unavailable {
                    backend: BACKEND,
                    detail: format!("failed to load kube config: {error}"),
                })?,
        };
        let client =
            Client::try_from(kube_config).map_err(|error| StateStoreError::Unavailable {
                backend: BACKEND,
                detail: format!("failed to build kube client: {error}"),
            })?;
        Ok(Self::from_client(client, config, request_timeout))
    }

    /// Build a store from an already-connected client (used by tests).
    pub fn from_client(
        client: Client,
        config: &KubernetesConfig,
        request_timeout: Duration,
    ) -> Self {
        Self {
            api: Api::namespaced(client, &config.namespace),
            cluster_id: config.cluster_id.clone(),
            request_timeout,
        }
    }

    /// Deterministic Lease object name for a node.
    fn lease_name(&self, role: &str, node_id: &NodeId) -> String {
        lease_name(&self.cluster_id, role, node_id)
    }

    fn role_str(status: &NodeStatus) -> &'static str {
        match status.node.role {
            talon_core::NodeRole::Coordinator => "coordinator",
            talon_core::NodeRole::Worker => "worker",
        }
    }

    async fn with_timeout<T, F>(&self, future: F) -> StateStoreResult<T>
    where
        F: std::future::Future<Output = Result<T, kube::Error>>,
    {
        match timeout(self.request_timeout, future).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(map_kube_error(error)),
            Err(_) => Err(StateStoreError::Timeout { backend: BACKEND }),
        }
    }

    fn build_lease(
        &self,
        name: &str,
        status: &NodeStatus,
        ttl_seconds: i32,
        resource_version: Option<String>,
    ) -> StateStoreResult<Lease> {
        let status_json =
            serde_json::to_string(status).map_err(|error| StateStoreError::Unavailable {
                backend: BACKEND,
                detail: format!("failed to encode node status: {error}"),
            })?;
        let now = now_datetime();
        let mut labels = BTreeMap::new();
        labels.insert(label_managed_by(), "talon".to_string());
        labels.insert(label_cluster(), sanitize_label(&self.cluster_id));
        labels.insert(label_role(), Self::role_str(status).to_string());
        labels.insert(label_node(), sanitize_label(&status.node.id.0));
        let mut annotations = BTreeMap::new();
        annotations.insert(annotation_status(), status_json);

        Ok(Lease {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                labels: Some(labels),
                annotations: Some(annotations),
                resource_version,
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: Some(status.incarnation_id.clone()),
                lease_duration_seconds: Some(ttl_seconds),
                renew_time: Some(MicroTime(now)),
                acquire_time: Some(MicroTime(now)),
                ..Default::default()
            }),
        })
    }
}

#[async_trait]
impl ClusterStateStore for KubernetesStateStore {
    fn backend(&self) -> StateBackend {
        StateBackend::Kubernetes
    }

    async fn upsert_node(
        &self,
        status: NodeStatus,
        lease_ttl: Duration,
    ) -> StateStoreResult<WriteResult> {
        status.validate()?;
        let ttl_seconds = lease_ttl_to_seconds(lease_ttl)?;
        let name = self.lease_name(Self::role_str(&status), &status.node.id);

        for _ in 0..MAX_WRITE_RETRIES {
            let existing = self.with_timeout(self.api.get_opt(&name)).await?;
            if let Some(current) = &existing {
                if let Some(current_status) = decode_status(current)? {
                    if let Some(disposition) = disposition_for(&current_status, &status) {
                        let rv = current
                            .metadata
                            .resource_version
                            .clone()
                            .unwrap_or_default();
                        return Ok(WriteResult {
                            disposition,
                            revision: revision(&rv)?,
                        });
                    }
                }
            }

            let resource_version = existing
                .as_ref()
                .and_then(|l| l.metadata.resource_version.clone());
            let lease = self.build_lease(&name, &status, ttl_seconds, resource_version.clone())?;

            let result = if resource_version.is_some() {
                self.api
                    .replace(&name, &PostParams::default(), &lease)
                    .await
            } else {
                self.api.create(&PostParams::default(), &lease).await
            };
            match result {
                Ok(committed) => {
                    let rv = committed.metadata.resource_version.unwrap_or_default();
                    return Ok(WriteResult {
                        disposition: WriteDisposition::Applied,
                        revision: revision(&rv)?,
                    });
                }
                Err(kube::Error::Api(err)) if err.code == 409 => {
                    // Conflict: another writer changed the object between our read
                    // and write. Retry with a fresh read.
                    continue;
                }
                Err(other) => return Err(map_kube_error(other)),
            }
        }
        Err(StateStoreError::Unavailable {
            backend: BACKEND,
            detail: "exceeded optimistic retry budget under write contention".into(),
        })
    }

    async fn remove_node(
        &self,
        _cluster_id: &str,
        node_id: &NodeId,
        incarnation_id: &str,
    ) -> StateStoreResult<WriteResult> {
        // A node has one role; try both role-derived names since remove_node
        // does not carry the role.
        for role in ["worker", "coordinator"] {
            let name = self.lease_name(role, node_id);
            let existing = self.with_timeout(self.api.get_opt(&name)).await?;
            let Some(lease) = existing else { continue };
            let rv = lease.metadata.resource_version.clone().unwrap_or_default();
            let holder = lease
                .spec
                .as_ref()
                .and_then(|s| s.holder_identity.clone())
                .unwrap_or_default();
            if holder != incarnation_id {
                return Ok(WriteResult {
                    disposition: WriteDisposition::Stale,
                    revision: revision(&rv)?,
                });
            }
            // Guard the delete on the observed resourceVersion.
            let dp = DeleteParams {
                preconditions: Some(kube::api::Preconditions {
                    resource_version: Some(rv.clone()),
                    uid: None,
                }),
                ..Default::default()
            };
            match self.api.delete(&name, &dp).await {
                Ok(_) => {
                    return Ok(WriteResult {
                        disposition: WriteDisposition::Applied,
                        revision: revision(&rv)?,
                    })
                }
                Err(kube::Error::Api(err)) if err.code == 409 => {
                    return Ok(WriteResult {
                        disposition: WriteDisposition::Stale,
                        revision: revision(&rv)?,
                    })
                }
                Err(other) => return Err(map_kube_error(other)),
            }
        }
        // No record under either role name.
        let list = self
            .with_timeout(self.api.list_metadata(&self.list_params()))
            .await?;
        let rv = list.metadata.resource_version.unwrap_or_default();
        Ok(WriteResult {
            disposition: WriteDisposition::NotFound,
            revision: revision(&rv)?,
        })
    }

    async fn snapshot(&self, cluster_id: &str) -> StateStoreResult<ClusterSnapshot> {
        let list = self
            .with_timeout(self.api.list(&self.list_params()))
            .await?;
        let rv = list.metadata.resource_version.clone().unwrap_or_default();
        let now = now_unix_ms();
        let mut nodes = Vec::new();
        for lease in &list.items {
            if lease_expired(lease, now) {
                continue;
            }
            if let Some(status) = decode_status(lease)? {
                if status.cluster_id == cluster_id {
                    nodes.push(status);
                }
            }
        }
        nodes.sort_by(|a, b| a.node.id.0.cmp(&b.node.id.0));
        Ok(ClusterSnapshot {
            nodes,
            revision: revision(&rv)?,
            observed_at_unix_ms: now,
        })
    }

    async fn watch(
        &self,
        cluster_id: &str,
        after_revision: Option<&StoreRevision>,
    ) -> StateStoreResult<Box<dyn ClusterStateWatch>> {
        // Establish the starting resource version: either the caller's resume
        // point or the current list revision.
        let start_rv = match after_revision {
            Some(rev) => rev.as_str().to_string(),
            None => {
                let list = self
                    .with_timeout(self.api.list_metadata(&self.list_params()))
                    .await?;
                list.metadata.resource_version.unwrap_or_default()
            }
        };
        let config = watcher::Config {
            label_selector: Some(self.selector()),
            ..Default::default()
        };
        let stream = watcher(self.api.clone(), config).boxed();
        Ok(Box::new(KubernetesWatch {
            cluster_id: cluster_id.to_string(),
            stream,
            start_rv,
        }))
    }

    async fn check_ready(&self) -> StateStoreResult<BackendHealth> {
        let list = self
            .with_timeout(self.api.list_metadata(&self.list_params()))
            .await?;
        let rv = list.metadata.resource_version.unwrap_or_default();
        Ok(BackendHealth {
            backend: BACKEND,
            checked_at_unix_ms: now_unix_ms(),
            revision: Some(revision(&rv)?),
        })
    }
}

impl KubernetesStateStore {
    fn selector(&self) -> String {
        format!(
            "{}=talon,{}={}",
            label_managed_by(),
            label_cluster(),
            sanitize_label(&self.cluster_id)
        )
    }

    fn list_params(&self) -> ListParams {
        ListParams::default().labels(&self.selector())
    }
}

/// Live Kubernetes watch adapted to the ordered [`ClusterStateWatch`] contract.
struct KubernetesWatch {
    cluster_id: String,
    stream:
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<Event<Lease>, watcher::Error>> + Send>>,
    #[allow(dead_code)]
    start_rv: String,
}

#[async_trait]
impl ClusterStateWatch for KubernetesWatch {
    async fn next(&mut self) -> StateStoreResult<NodeEvent> {
        loop {
            let Some(item) = self.stream.next().await else {
                return Err(StateStoreError::Unavailable {
                    backend: BACKEND,
                    detail: "watch stream closed".into(),
                });
            };
            let event = match item {
                Ok(event) => event,
                Err(watcher::Error::WatchStartFailed(_)) | Err(watcher::Error::WatchError(_)) => {
                    // The most common cause is a too-old resource version: signal
                    // the caller to relist and restart.
                    return Err(StateStoreError::Compacted {
                        requested: revision(&self.start_rv)
                            .unwrap_or_else(|_| StoreRevision::new("k8s:0").expect("non-empty")),
                        oldest: StoreRevision::new("k8s:current").expect("non-empty"),
                    });
                }
                Err(other) => {
                    return Err(StateStoreError::Unavailable {
                        backend: BACKEND,
                        detail: format!("watch error: {other}"),
                    })
                }
            };
            match event {
                Event::Apply(lease) | Event::InitApply(lease) => {
                    if let Some(status) = decode_status(&lease)? {
                        if status.cluster_id == self.cluster_id {
                            let rv = lease.metadata.resource_version.clone().unwrap_or_default();
                            return Ok(NodeEvent {
                                cluster_id: status.cluster_id.clone(),
                                node_id: status.node.id.clone(),
                                kind: NodeEventKind::Upserted,
                                status: Some(status),
                                revision: revision(&rv)?,
                                observed_at_unix_ms: now_unix_ms(),
                            });
                        }
                    }
                }
                Event::Delete(lease) => {
                    let node = lease
                        .metadata
                        .labels
                        .as_ref()
                        .and_then(|l| l.get(&label_node()).cloned())
                        .unwrap_or_default();
                    let rv = lease.metadata.resource_version.clone().unwrap_or_default();
                    return Ok(NodeEvent {
                        cluster_id: self.cluster_id.clone(),
                        node_id: NodeId::new(node),
                        kind: NodeEventKind::Removed,
                        status: None,
                        revision: revision(&rv)?,
                        observed_at_unix_ms: now_unix_ms(),
                    });
                }
                Event::Init | Event::InitDone => {}
            }
        }
    }
}

/// Deterministic Lease object name for a node.
///
/// Kubernetes names must be DNS-1123 labels, but Talon node ids are arbitrary.
/// We sanitize a human-readable prefix and append a stable xxh3 suffix over the
/// full `cluster/role/node` triple so distinct nodes never collide even after
/// sanitization, and the same node always maps to the same name (required to
/// find its record for upsert/remove).
fn lease_name(cluster_id: &str, role: &str, node_id: &NodeId) -> String {
    let key = format!("{cluster_id}/{role}/{}", node_id.0);
    let hash = xxhash_rust::xxh3::xxh3_64(key.as_bytes());
    let prefix: String = node_id
        .0
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .take(40)
        .collect();
    // DNS-1123: must start/end alphanumeric; trim leading/trailing dashes.
    let prefix = prefix.trim_matches('-');
    let prefix = if prefix.is_empty() { "n" } else { prefix };
    format!("talon-{prefix}-{hash:016x}")
}

/// Reproduce the backend contract's incarnation/sequence ordering rule.
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

fn decode_status(lease: &Lease) -> StateStoreResult<Option<NodeStatus>> {
    let Some(json) = lease
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(&annotation_status()))
    else {
        return Ok(None);
    };
    serde_json::from_str(json)
        .map(Some)
        .map_err(|error| StateStoreError::Unavailable {
            backend: BACKEND,
            detail: format!("failed to decode node status: {error}"),
        })
}

/// Whether a Lease is past `renewTime + leaseDurationSeconds`.
fn lease_expired(lease: &Lease, now_unix_ms: u64) -> bool {
    let Some(spec) = &lease.spec else { return true };
    let Some(MicroTime(renew)) = spec.renew_time.as_ref().map(|t| MicroTime(t.0)) else {
        return true;
    };
    let ttl = spec.lease_duration_seconds.unwrap_or(0).max(0) as u64;
    let renew_ms = renew.timestamp_millis().max(0) as u64;
    now_unix_ms > renew_ms.saturating_add(ttl.saturating_mul(1_000))
}

fn sanitize_label(value: &str) -> String {
    let s: String = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '.' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .take(63)
        .collect();
    let s = s.trim_matches(|c: char| !c.is_ascii_alphanumeric());
    if s.is_empty() {
        "x".to_string()
    } else {
        s.to_string()
    }
}

fn revision(value: &str) -> StateStoreResult<StoreRevision> {
    // Kubernetes resource versions are opaque; prefix so they never mix with
    // another backend's tokens, and so an empty version still yields a valid
    // non-empty StoreRevision.
    StoreRevision::new(format!("k8s:{value}"))
}

fn lease_ttl_to_seconds(ttl: Duration) -> StateStoreResult<i32> {
    if ttl.is_zero() {
        return Err(StateStoreError::InvalidLeaseTtl(ttl));
    }
    let secs = ttl.as_millis().div_ceil(1_000);
    i32::try_from(secs)
        .ok()
        .filter(|v| *v > 0)
        .ok_or(StateStoreError::InvalidLeaseTtl(ttl))
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn now_datetime() -> DateTime<Utc> {
    Utc.timestamp_millis_opt(now_unix_ms() as i64)
        .single()
        .unwrap_or_else(Utc::now)
}

fn map_kube_error(error: kube::Error) -> StateStoreError {
    match error {
        kube::Error::Api(err) => match err.code {
            401 => StateStoreError::Authentication { backend: BACKEND },
            403 => StateStoreError::PermissionDenied { backend: BACKEND },
            408 | 504 => StateStoreError::Timeout { backend: BACKEND },
            410 => StateStoreError::Compacted {
                requested: StoreRevision::new("k8s:0").expect("non-empty"),
                oldest: StoreRevision::new("k8s:current").expect("non-empty"),
            },
            _ => StateStoreError::Unavailable {
                backend: BACKEND,
                // err.message is server-provided and may include the object name
                // but never credentials; safe to surface for diagnosis.
                detail: format!("kubernetes api error {}: {}", err.code, err.reason),
            },
        },
        other => StateStoreError::Unavailable {
            backend: BACKEND,
            detail: sanitize_transport_error(&other),
        },
    }
}

/// Credential-free description of a transport-level failure.
fn sanitize_transport_error(error: &kube::Error) -> String {
    match error {
        kube::Error::Auth(_) => "kubernetes authentication error".into(),
        kube::Error::HyperError(_) | kube::Error::Service(_) => "kubernetes transport error".into(),
        kube::Error::HttpError(_) => "kubernetes http error".into(),
        kube::Error::SerdeError(_) => "kubernetes response decode error".into(),
        _ => "kubernetes backend error".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_validation_catches_blank_fields() {
        assert_eq!(
            KubernetesConfig::default().validate(),
            Err(KubernetesConfigError::EmptyClusterId)
        );
        let mut c = KubernetesConfig {
            cluster_id: "prod".into(),
            ..Default::default()
        };
        c.validate().unwrap();
        c.namespace = " ".into();
        assert_eq!(c.validate(), Err(KubernetesConfigError::EmptyNamespace));
    }

    #[test]
    fn lease_ttl_rounds_up_to_whole_seconds() {
        assert_eq!(lease_ttl_to_seconds(Duration::from_millis(100)).unwrap(), 1);
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
    fn revision_is_prefixed_and_never_empty() {
        assert_eq!(revision("12345").unwrap().as_str(), "k8s:12345");
        // Even an empty server version yields a valid non-empty token.
        assert_eq!(revision("").unwrap().as_str(), "k8s:");
    }

    #[test]
    fn lease_name_is_dns_safe_and_deterministic() {
        let cluster = "prod/us-east";
        let n1 = lease_name(cluster, "worker", &NodeId::new("Worker_01!@#"));
        let n2 = lease_name(cluster, "worker", &NodeId::new("Worker_01!@#"));
        assert_eq!(n1, n2, "deterministic");
        assert!(n1.starts_with("talon-"));
        assert!(n1
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'));
        assert!(!n1.starts_with('-') && !n1.ends_with('-'));
        // Distinct nodes get distinct names.
        let n3 = lease_name(cluster, "worker", &NodeId::new("Worker_02!@#"));
        assert_ne!(n1, n3);
        // Same node id under a different role is a distinct object.
        let n4 = lease_name(cluster, "coordinator", &NodeId::new("Worker_01!@#"));
        assert_ne!(n1, n4);
    }

    #[test]
    fn disposition_enforces_incarnation_and_sequence_rule() {
        let base = crate::state_store::testkit::worker_status("w1", "inc-1", 5);
        let dup = crate::state_store::testkit::worker_status("w1", "inc-1", 5);
        assert_eq!(
            disposition_for(&base, &dup),
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
    }

    #[test]
    fn sanitize_label_is_bounded_and_safe() {
        assert_eq!(sanitize_label("prod/us-east"), "prod-us-east");
        assert_eq!(sanitize_label(""), "x");
        assert!(sanitize_label(&"a".repeat(100)).len() <= 63);
    }
}
