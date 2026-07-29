//! Backend-neutral failures for [`MetadataStore`](crate::MetadataStore).
//!
//! ADR 0003 §7 requires the error model to mirror `ClusterStateStore`'s shape,
//! with explicit `Compacted` / `WatchLagged` / `Timeout` / `Unavailable`
//! variants. Two additions carry weight beyond diagnostics.
//!
//! [`MetadataError::CapabilityUnsupported`] is how a backend refuses rather
//! than approximates. §4 calls silent degradation "precisely the defect in
//! #363", so a store that cannot do multi-record transactions must fail here
//! instead of emulating one with a sequence of single-key writes.
//!
//! [`MetadataError::Unavailable`] must stay distinguishable from
//! `CapabilityUnsupported` all the way up to the errno the caller returns. §4
//! maps them differently on purpose — `EOPNOTSUPP` for "this cluster does not
//! implement locking" versus `ENOLCK` for "it does, but the lock service is
//! unreachable" — and §6 requires "not configured" and "configured but
//! unreachable" to be separable during an incident.

use core::fmt;
use core::time::Duration;

use crate::capability::Capability;
use crate::revision::StoreRevision;

/// Result alias for metadata-store operations.
pub type MetadataResult<T> = Result<T, MetadataError>;

/// Identifies which backend produced an error, for logs and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MetadataBackend {
    /// In-process store used by tests and single-node development.
    Memory,
    /// etcd-backed store.
    Etcd,
}

impl MetadataBackend {
    /// Stable lowercase identifier for logs, metrics, and configuration.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Etcd => "etcd",
        }
    }
}

impl fmt::Display for MetadataBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Backend-neutral metadata-store failures.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MetadataError {
    /// The backend does not advertise the capability this operation needs.
    ///
    /// This is a permanent property of the deployed backend, not a transient
    /// condition: retrying cannot help, and the caller must surface a distinct
    /// errno rather than falling back to an approximation (§4).
    #[error("{backend} metadata backend does not support {capability}")]
    CapabilityUnsupported {
        /// Backend that refused.
        backend: MetadataBackend,
        /// Capability the operation required.
        capability: Capability,
    },

    /// A compare-and-swap observed a different revision than the caller expected.
    ///
    /// The caller re-reads and decides whether its intent still applies. This is
    /// the ordinary contention path, not a fault.
    #[error("compare-and-swap failed: expected revision {expected}, found {observed}")]
    CompareAndSwapFailed {
        /// Revision the caller predicated the write on.
        expected: StoreRevision,
        /// Revision actually found.
        observed: StoreRevision,
    },

    /// A record required by the operation does not exist.
    #[error("metadata record not found: {key}")]
    NotFound {
        /// Sanitized record key. Must not carry credentials.
        key: String,
    },

    /// A record already exists where the operation required none.
    #[error("metadata record already exists: {key}")]
    AlreadyExists {
        /// Sanitized record key. Must not carry credentials.
        key: String,
    },

    /// A record violated the shared schema contract.
    #[error("invalid metadata record: {detail}")]
    InvalidRecord {
        /// Stable diagnostic detail without credentials.
        detail: String,
    },

    /// A revision token was empty, malformed, or belonged to another backend.
    #[error("invalid revision {revision:?}: {detail}")]
    InvalidRevision {
        /// Backend that rejected the token, when known.
        backend: Option<MetadataBackend>,
        /// Rejected opaque token.
        revision: String,
        /// Stable diagnostic detail without credentials.
        detail: &'static str,
    },

    /// A lease duration was zero or could not be represented.
    #[error("invalid lease TTL: {0:?}")]
    InvalidLeaseTtl(Duration),

    /// The session backing this operation has expired or was never valid.
    ///
    /// §7 is explicit about what a client must conclude: "If renewal fails, the
    /// client must assume the session and all of its locks are lost." A caller
    /// must not retry the operation as though the session still held.
    #[error("metadata session {session} is no longer valid")]
    SessionExpired {
        /// Sanitized session identifier.
        session: String,
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
    #[error("metadata watch lagged by {skipped} events after revision {after}")]
    WatchLagged {
        /// Last revision delivered to the caller.
        after: StoreRevision,
        /// Number of skipped events reported by the backend.
        skipped: u64,
    },

    /// Backend authentication failed.
    #[error("{backend} metadata backend authentication failed")]
    Authentication {
        /// Selected backend.
        backend: MetadataBackend,
    },

    /// Backend authorization denied the requested operation.
    #[error("{backend} metadata backend permission denied")]
    PermissionDenied {
        /// Selected backend.
        backend: MetadataBackend,
    },

    /// A backend operation exceeded its configured deadline.
    #[error("{backend} metadata backend operation timed out")]
    Timeout {
        /// Selected backend.
        backend: MetadataBackend,
    },

    /// The backend or watch transport is unavailable.
    ///
    /// Distinct from [`MetadataError::CapabilityUnsupported`]: the cluster does
    /// offer the feature, it just cannot reach the store right now. §6 requires
    /// callers to fail closed here — never to substitute a local approximation —
    /// while keeping the two cases separable in logs and metrics.
    #[error("{backend} metadata backend unavailable: {detail}")]
    Unavailable {
        /// Selected backend.
        backend: MetadataBackend,
        /// Sanitized diagnostic detail.
        detail: String,
    },
}

impl MetadataError {
    /// Whether retrying after backoff, or relisting, can recover the operation.
    ///
    /// [`MetadataError::CapabilityUnsupported`] is deliberately **not**
    /// retryable: the backend will never grow the capability at runtime, so a
    /// retry loop would spin until a deadline instead of reporting the honest
    /// answer to the application.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Compacted { .. }
                | Self::WatchLagged { .. }
                | Self::Timeout { .. }
                | Self::Unavailable { .. }
        )
    }

    /// Whether the failure reflects a permanent property of the deployment
    /// rather than a transient condition.
    ///
    /// Callers use this to choose between §4's paired errnos: an unsupported
    /// capability is reported as "this cluster does not offer the feature",
    /// while an unavailable store is reported as "the feature exists but its
    /// store is unreachable".
    pub fn is_capability_gap(&self) -> bool {
        matches!(self, Self::CapabilityUnsupported { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unsupported_capability_is_never_retryable() {
        // ADR 0003 §4: a missing capability is a permanent property of the
        // deployed backend. Retrying it would spin instead of reporting the
        // honest answer, which is the silent-degradation failure mode #363 is
        // about.
        let error = MetadataError::CapabilityUnsupported {
            backend: MetadataBackend::Memory,
            capability: Capability::HardLinks,
        };
        assert!(!error.is_retryable());
        assert!(error.is_capability_gap());
    }

    #[test]
    fn an_unavailable_backend_is_retryable_and_not_a_capability_gap() {
        // The pairing that §4 maps to EOPNOTSUPP vs ENOLCK and that §6 requires
        // to stay separable during an incident.
        let error = MetadataError::Unavailable {
            backend: MetadataBackend::Etcd,
            detail: "connection refused".to_owned(),
        };
        assert!(error.is_retryable());
        assert!(!error.is_capability_gap());
    }

    #[test]
    fn compare_and_swap_contention_is_not_retryable_without_re_reading() {
        // A failed CAS is not a transport fault. Blind retry would re-apply an
        // intent formed against a revision that no longer exists, so the caller
        // must re-read first.
        let error = MetadataError::CompareAndSwapFailed {
            expected: StoreRevision::new("7").expect("non-empty token"),
            observed: StoreRevision::new("9").expect("non-empty token"),
        };
        assert!(!error.is_retryable());
    }

    #[test]
    fn watch_recovery_paths_are_retryable() {
        let compacted = MetadataError::Compacted {
            requested: StoreRevision::new("3").expect("non-empty token"),
            oldest: StoreRevision::new("11").expect("non-empty token"),
        };
        let lagged = MetadataError::WatchLagged {
            after: StoreRevision::new("11").expect("non-empty token"),
            skipped: 42,
        };
        assert!(compacted.is_retryable());
        assert!(lagged.is_retryable());
    }

    #[test]
    fn session_expiry_is_not_retryable() {
        // §7: "If renewal fails, the client must assume the session and all of
        // its locks are lost." Retrying under the dead session would act on
        // ownership the store no longer grants.
        let error = MetadataError::SessionExpired {
            session: "session-1".to_owned(),
        };
        assert!(!error.is_retryable());
    }
}
