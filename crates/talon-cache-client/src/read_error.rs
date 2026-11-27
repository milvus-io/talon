use std::sync::Arc;

use talon_transport::DataErrorCode;

use crate::coordinator_client::CoordinatorError;
use crate::worker_client::WorkerError;

/// Errors from a block read.
#[derive(Debug, thiserror::Error)]
pub enum BlockReadError {
    /// Authoritative membership refresh failed.
    #[error(transparent)]
    Coordinator(#[from] CoordinatorError),
    /// The most recent background discovery attempt failed.
    #[error(transparent)]
    Discovery(Arc<CoordinatorError>),
    /// The worker fetch failed.
    #[error(transparent)]
    Worker(#[from] WorkerError),
    #[error("worker {worker_id}, instance {instance_id} at {address}: {source}")]
    Target {
        worker_id: String,
        instance_id: String,
        address: String,
        #[source]
        source: WorkerError,
    },
    /// The cluster returned no owners for the block (empty cluster).
    #[error("no owners for block")]
    NoOwners,
    /// All configured logical candidates failed, without refreshing discovery.
    #[error("all replicas failed; last worker {worker_id}, instance {instance_id:?} at {worker}: {source}")]
    AllReplicasFailed {
        /// Last candidate's address when known, otherwise its logical worker ID.
        worker: String,
        /// Last candidate logical identity, including unavailable candidates.
        worker_id: String,
        /// Last observed instance identity, absent for offline or conflicting members.
        instance_id: Option<String>,
        /// Original failure, preserved for diagnostics and classification.
        #[source]
        source: WorkerError,
    },
}

/// A typed cache-read failure suitable for an HTTP routing decision.
#[derive(Debug, thiserror::Error)]
pub enum CacheReadError {
    /// The requested range or stream configuration is invalid.
    #[error("invalid cache request: {0}")]
    InvalidRequest(String),
    /// The authoritative object does not exist.
    #[error("object not found: {0}")]
    NotFound(String),
    /// The block is absent and the selected route disabled origin fill.
    #[error("cache miss: {0}")]
    CacheMiss(String),
    /// Cache infrastructure is unavailable.
    #[error("cache unavailable: {0}")]
    Unavailable(String),
    /// Cache infrastructure exceeded its deadline.
    #[error("cache timeout: {0}")]
    Timeout(String),
    /// The source version changed during the read.
    #[error("source version mismatch: {0}")]
    VersionMismatch(String),
    /// The authoritative origin failed while a worker was filling the cache.
    #[error("origin failure: {0}")]
    Origin(String),
    /// A wire/framing invariant was violated.
    #[error("cache protocol failure: {0}")]
    Protocol(String),
    /// An explicitly typed worker-internal failure.
    #[error("cache internal failure: {0}")]
    Internal(String),
    /// A legacy worker returned only an unclassified string.
    #[error("unclassified legacy worker failure: {0}")]
    Unknown(String),
    /// The namespace exceeded its rate limit; the caller should back off and
    /// retry. Never origin-fallback-eligible — falling back would evade the
    /// limit.
    #[error("namespace rate limit exceeded: {0}")]
    RateLimited(String),
}

impl CacheReadError {
    /// Whether direct-origin fallback may safely hide this cache-path failure.
    ///
    /// Policy must still opt into fallback. Correctness, origin, protocol, and
    /// legacy-unknown failures remain visible rather than being guessed from
    /// diagnostic text.
    pub fn fallback_eligible(&self) -> bool {
        matches!(self, Self::Unavailable(_) | Self::Timeout(_))
    }
}

impl From<WorkerError> for CacheReadError {
    fn from(error: WorkerError) -> Self {
        match error {
            WorkerError::Io(error) if error.kind() == std::io::ErrorKind::TimedOut => {
                Self::Timeout(error.to_string())
            }
            WorkerError::Io(error) => Self::Unavailable(error.to_string()),
            WorkerError::Remote(error) => match error.code {
                DataErrorCode::Unknown => Self::Unknown(error.message),
                DataErrorCode::InvalidRequest => Self::InvalidRequest(error.message),
                DataErrorCode::NotFound => Self::NotFound(error.message),
                DataErrorCode::CacheMiss => Self::CacheMiss(error.message),
                DataErrorCode::Unavailable => Self::Unavailable(error.message),
                DataErrorCode::Timeout => Self::Timeout(error.message),
                DataErrorCode::VersionMismatch => Self::VersionMismatch(error.message),
                DataErrorCode::Origin => Self::Origin(error.message),
                DataErrorCode::Internal => Self::Internal(error.message),
                DataErrorCode::RateLimited => Self::RateLimited(error.message),
            },
            other => Self::Protocol(other.to_string()),
        }
    }
}

impl From<CoordinatorError> for CacheReadError {
    fn from(error: CoordinatorError) -> Self {
        Self::from(&error)
    }
}

impl From<&CoordinatorError> for CacheReadError {
    fn from(error: &CoordinatorError) -> Self {
        match error {
            CoordinatorError::Remote(error) => WorkerError::Remote(error.clone()).into(),
            CoordinatorError::Io(error) if error.kind() == std::io::ErrorKind::TimedOut => {
                Self::Timeout(error.to_string())
            }
            CoordinatorError::Io(error) => Self::Unavailable(error.to_string()),
            other => Self::Protocol(other.to_string()),
        }
    }
}

impl From<BlockReadError> for CacheReadError {
    fn from(error: BlockReadError) -> Self {
        match error {
            BlockReadError::Coordinator(error) => error.into(),
            BlockReadError::Discovery(error) => error.as_ref().into(),
            BlockReadError::Worker(error)
            | BlockReadError::Target { source: error, .. }
            | BlockReadError::AllReplicasFailed { source: error, .. } => error.into(),
            BlockReadError::NoOwners => Self::Unavailable(error.to_string()),
        }
    }
}
