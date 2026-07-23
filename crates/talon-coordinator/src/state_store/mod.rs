//! Backend-neutral shared cluster-state contract.
//!
//! Coordinators use this API for leased node status, consistent snapshots, and
//! resumable watches. Backend revisions are deliberately opaque: etcd happens
//! to use integers while Kubernetes resource versions must not be parsed or
//! ordered by clients.

mod config;
#[cfg(feature = "kubernetes")]
mod kubernetes;
mod memory;

#[cfg(any(test, feature = "state-store-testkit"))]
#[doc(hidden)]
pub mod testkit;

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use talon_core::{NodeId, NodeStatus, NodeStatusError};

pub use config::{ClusterStateConfig, ConfigError, StateBackend};
#[cfg(feature = "kubernetes")]
pub use kubernetes::{
    KubernetesConfig, KubernetesConfigError, KubernetesStateStore, DEFAULT_LEASE_LABEL_PREFIX,
};
pub use memory::{MemoryStateStore, TimeSource};

/// Result alias for cluster-state operations.
pub type StateStoreResult<T> = Result<T, StateStoreError>;

/// Opaque backend revision used for watch resume and diagnostics.
///
/// The type intentionally does not implement `Ord` or `PartialOrd`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StoreRevision(String);

impl StoreRevision {
    /// Construct a revision from a non-empty backend token.
    pub fn new(value: impl Into<String>) -> StateStoreResult<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(StateStoreError::InvalidRevision {
                backend: None,
                revision: value,
                detail: "revision must not be empty",
            });
        }
        Ok(Self(value))
    }

    /// Borrow the backend token without interpreting it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StoreRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Outcome of an idempotent state mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteDisposition {
    /// The record changed and a new revision was committed.
    Applied,
    /// The same heartbeat sequence was already accepted.
    Duplicate,
    /// The write belongs to an older sequence or process incarnation.
    Stale,
    /// The requested record did not exist.
    NotFound,
}

/// Result of an upsert or explicit removal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteResult {
    /// Whether the mutation changed shared state.
    pub disposition: WriteDisposition,
    /// Backend revision after evaluating the mutation.
    pub revision: StoreRevision,
}

/// One consistent cluster snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterSnapshot {
    /// Non-expired node records in stable node-id order.
    pub nodes: Vec<NodeStatus>,
    /// Opaque revision at which the snapshot was observed.
    pub revision: StoreRevision,
    /// Backend/client observation time as Unix milliseconds.
    pub observed_at_unix_ms: u64,
}

/// Kind of event emitted by a cluster watch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeEventKind {
    /// A node was inserted or refreshed with newer status.
    Upserted,
    /// A node was explicitly removed or its lease expired.
    Removed,
}

/// Ordered node event emitted after a snapshot revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeEvent {
    /// Logical cluster containing the node.
    pub cluster_id: String,
    /// Stable node identity.
    pub node_id: NodeId,
    /// Event type.
    pub kind: NodeEventKind,
    /// Status for an upsert; absent for removal.
    pub status: Option<NodeStatus>,
    /// Opaque revision committed with the event.
    pub revision: StoreRevision,
    /// Observation time as Unix milliseconds.
    pub observed_at_unix_ms: u64,
}

/// Successful backend readiness result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendHealth {
    /// Selected backend implementation.
    pub backend: StateBackend,
    /// Time the readiness check completed, as Unix milliseconds.
    pub checked_at_unix_ms: u64,
    /// Latest revision if the backend returned one cheaply.
    pub revision: Option<StoreRevision>,
}

/// Resumable stream of ordered cluster events.
#[async_trait]
pub trait ClusterStateWatch: Send {
    /// Wait for the next event.
    async fn next(&mut self) -> StateStoreResult<NodeEvent>;
}

/// Strongly consistent, lease-oriented storage used by active-active
/// coordinators.
#[async_trait]
pub trait ClusterStateStore: Send + Sync {
    /// Backend implementation represented by this store.
    fn backend(&self) -> StateBackend;

    /// Insert or refresh a node status for `lease_ttl`.
    ///
    /// Implementations reject duplicate/out-of-order heartbeat sequences
    /// without refreshing the lease.
    async fn upsert_node(
        &self,
        status: NodeStatus,
        lease_ttl: Duration,
    ) -> StateStoreResult<WriteResult>;

    /// Remove a node only if `incarnation_id` still owns the current record.
    async fn remove_node(
        &self,
        cluster_id: &str,
        node_id: &NodeId,
        incarnation_id: &str,
    ) -> StateStoreResult<WriteResult>;

    /// Return a linearizable snapshot for one cluster.
    async fn snapshot(&self, cluster_id: &str) -> StateStoreResult<ClusterSnapshot>;

    /// Watch events strictly after `after_revision`.
    ///
    /// `None` starts after the backend's current revision. A compacted revision
    /// returns [`StateStoreError::Compacted`], prompting a fresh snapshot.
    async fn watch(
        &self,
        cluster_id: &str,
        after_revision: Option<&StoreRevision>,
    ) -> StateStoreResult<Box<dyn ClusterStateWatch>>;

    /// Verify the backend can serve authoritative requests.
    async fn check_ready(&self) -> StateStoreResult<BackendHealth>;
}

/// Backend-neutral state-store failures.
#[derive(Debug, thiserror::Error)]
pub enum StateStoreError {
    /// A status record violated the shared schema contract.
    #[error("invalid node status: {0}")]
    InvalidRecord(#[from] NodeStatusError),
    /// A lease duration was zero or could not be represented.
    #[error("invalid lease TTL: {0:?}")]
    InvalidLeaseTtl(Duration),
    /// A revision token was empty, malformed, or belonged to another backend.
    #[error("invalid revision {revision:?}: {detail}")]
    InvalidRevision {
        /// Backend that rejected the token, when known.
        backend: Option<StateBackend>,
        /// Rejected opaque token.
        revision: String,
        /// Stable diagnostic detail without credentials.
        detail: &'static str,
    },
    /// A watch can no longer resume from the requested revision.
    #[error("revision {requested} was compacted; oldest available is {oldest}")]
    Compacted {
        /// Requested resume revision.
        requested: StoreRevision,
        /// Oldest revision retained by the backend.
        oldest: StoreRevision,
    },
    /// A live watch receiver fell behind its bounded event buffer.
    #[error("state watch lagged by {skipped} events after revision {after}")]
    WatchLagged {
        /// Last revision delivered to the caller.
        after: StoreRevision,
        /// Number of skipped events reported by the backend/client.
        skipped: u64,
    },
    /// Backend authentication failed.
    #[error("{backend} state backend authentication failed")]
    Authentication {
        /// Selected backend.
        backend: StateBackend,
    },
    /// Backend authorization denied the requested operation.
    #[error("{backend} state backend permission denied")]
    PermissionDenied {
        /// Selected backend.
        backend: StateBackend,
    },
    /// A backend operation exceeded its configured deadline.
    #[error("{backend} state backend operation timed out")]
    Timeout {
        /// Selected backend.
        backend: StateBackend,
    },
    /// The backend or watch transport is unavailable.
    #[error("{backend} state backend unavailable: {detail}")]
    Unavailable {
        /// Selected backend.
        backend: StateBackend,
        /// Sanitized diagnostic detail.
        detail: String,
    },
}

impl StateStoreError {
    /// Whether retrying after backoff or relisting can recover the operation.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Compacted { .. }
                | Self::WatchLagged { .. }
                | Self::Timeout { .. }
                | Self::Unavailable { .. }
        )
    }
}
