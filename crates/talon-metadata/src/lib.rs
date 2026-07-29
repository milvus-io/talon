//! Optional Metadata Store (TMS) for Talon.
//!
//! TMS holds ownership facts that **cannot be derived by listing the object
//! store**. Everything else stays out, by rule (ADR 0003 §2):
//!
//! > If a fact can be rebuilt by listing the object store, it does not go in TMS.
//!
//! # Why this store exists at all
//!
//! Three features converged on the same missing primitive: hard links (#363),
//! write-back (ADR 0002 §2), and POSIX locking. None of them needs "somewhere
//! to put data" — they need **durable, strongly consistent ownership that
//! outlives a single process and is visible to every client**.
//!
//! # Why it is optional
//!
//! Talon's namespace is derived, not stored: the object store's key list *is*
//! the truth. ADR 0003 is blunt about what that buys and what would lose it:
//!
//! > That property — **path == data** — is why a plain S3 client can read what
//! > Talon wrote, why a coordinator is disposable, and why there is no metadata
//! > to recover after a restart. Any metadata store that dilutes it converts
//! > Talon from a cache in front of an object store into an object-store-backed
//! > filesystem (the JuiceFS model). That is a legitimate architecture and it is
//! > not this one.
//!
//! So a cluster without TMS is a complete, supported deployment offering today's
//! feature set. TMS is "the price of admission for a specific group of features,
//! and clusters that do not need those features must not pay it" (§1).
//!
//! # Why sparsity is load-bearing
//!
//! §3 makes a quantity claim, not a style preference: a singly-linked file, an
//! unlocked file, and a cluster without write-back each occupy **zero** records.
//! That is what makes an etcd-class backend viable, since etcd holds its
//! keyspace in memory with a practical ceiling in the single-digit GB. Per-object
//! records for a billion-object bucket would not fit.
//!
//! [`LinkCount`] enforces the sharpest edge of this in the type system: it
//! cannot represent 1, so a record for an ordinary single-path file is
//! unrepresentable rather than merely discouraged.
//!
//! # Scope of this crate
//!
//! This crate defines the contract only. Per ADR 0003 §9.11, none of this
//! enables write-back:
//!
//! > ADR 0002 §2 remains in force. [...] Write-back still requires bounded
//! > production implementation, failure-injection evidence, observability,
//! > operator procedures, and its own ADR superseding ADR 0002 before any
//! > configuration can make this path reachable.
//!
//! Likewise, [`Capability::Locks`] says a backend *could* support distributed
//! locking; shipping POSIX locks additionally requires a locking ADR defining
//! byte-range representation, blocking and fairness rules, waiter recovery, and
//! cross-file deadlock detection.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod assignment;
mod capability;
mod cluster;
pub mod contract;
mod error;
#[cfg(feature = "etcd")]
mod etcd;
pub mod fence;
pub mod link;
mod memory;
mod record;
pub mod recovery;
mod revision;
pub mod scrub;
pub mod shard;
mod transaction;

pub use capability::{Capability, CapabilitySet};
pub use cluster::{CapabilityRevision, CapabilityState, ClusterCapabilities};
pub use error::{MetadataBackend, MetadataError, MetadataResult};
pub use memory::{MemoryMetadataStore, NamespaceSnapshot};
pub use record::{
    InodeNumber, InodeRecord, LinkCount, LinkTransition, LinkTransitionState, NamespaceId,
    PathIndexEntry,
};
pub use revision::{MappingRevision, StoreRevision};
pub use transaction::{Operation, Precondition, Transaction, TransactionOutcome};

#[cfg(feature = "etcd")]
pub use etcd::{EtcdMetadataConfig, EtcdMetadataStore, DEFAULT_METADATA_PREFIX};

use async_trait::async_trait;

/// Health of a metadata backend, as reported by [`MetadataStore::check_ready`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendHealth {
    /// Whether the backend can currently serve authoritative requests.
    pub ready: bool,
    /// Sanitized diagnostic detail. Must never carry credentials.
    pub detail: String,
}

/// A durable, strongly consistent store for non-derivable ownership facts.
///
/// # Relationship to `ClusterStateStore`
///
/// This is deliberately a separate trait rather than new record types on
/// `ClusterStateStore`, because the two have opposite durability invariants
/// (§7):
///
/// > Cluster membership is ephemeral, bounded, and rebuildable from live
/// > processes (ADR 0001 §2); TMS records are durable and *not* rebuildable.
/// > Mixing them in one abstraction would make ADR 0001 §2's "bounded,
/// > rebuildable" invariant untrue by construction.
///
/// Both may target the same physical etcd cluster under different prefixes.
/// They remain separate abstractions with separate invariants.
///
/// # Capability checking
///
/// Implementations must reject an operation whose capability they do not
/// advertise, returning [`MetadataError::CapabilityUnsupported`]. They must not
/// emulate a missing guarantee with a weaker one — §4 identifies that as the
/// defect behind #363, where an operation "appears to succeed, produces copies
/// that can diverge, cannot self-repair, and does not report which copies went
/// stale".
///
/// # Credential boundary
///
/// §7 places TMS access in the management tier: FUSE clients never receive TMS
/// credentials or connect directly, and hard-link and lock operations are
/// proxied through any active coordinator. A lock belongs to a renewable client
/// session, not to the coordinator process that happened to proxy its
/// acquisition, so a coordinator failure neither transfers nor drops it.
#[async_trait]
pub trait MetadataStore: Send + Sync {
    /// Backend implementation represented by this store.
    fn backend(&self) -> MetadataBackend;

    /// Capabilities this backend advertises.
    ///
    /// Declared by the implementation, never inferred from the presence of a
    /// generic compare-and-swap call (§7). A backend that cannot satisfy a
    /// capability's contract must omit it here and reject the corresponding
    /// operations.
    fn capabilities(&self) -> CapabilitySet;

    /// Whether this backend advertises `capability`.
    fn supports(&self, capability: Capability) -> bool {
        self.capabilities().supports(capability)
    }

    /// Verify the backend can serve authoritative requests.
    ///
    /// A failure here means "configured but unreachable", which §4 and §6
    /// require callers to keep distinct from "not configured" — the two map to
    /// different errnos and must be separable in an incident.
    async fn check_ready(&self) -> MetadataResult<BackendHealth>;

    /// Return the current revision of the namespace's hard-link mapping.
    ///
    /// A namespace that has never had a mapping transition reports
    /// [`MappingRevision::INITIAL`], which requires no stored record: §3's
    /// sparsity claim covers the mapping guard too, so an untouched namespace
    /// occupies nothing.
    ///
    /// # Errors
    ///
    /// [`MetadataError::CapabilityUnsupported`] when the backend does not
    /// advertise [`Capability::HardLinks`].
    async fn mapping_revision(&self, namespace: &NamespaceId) -> MetadataResult<MappingRevision>;

    /// Resolve a visible path to its inode, if the path is multiply linked.
    ///
    /// Returns `Ok(None)` for an ordinary single-path file. That is the common
    /// case and it is not an error: such a file has no TMS record because its
    /// path *is* its address in the object store.
    ///
    /// # Errors
    ///
    /// [`MetadataError::CapabilityUnsupported`] when the backend does not
    /// advertise [`Capability::HardLinks`].
    async fn resolve_path(
        &self,
        namespace: &NamespaceId,
        path: &str,
    ) -> MetadataResult<Option<PathIndexEntry>>;

    /// Load an inode record.
    ///
    /// # Errors
    ///
    /// [`MetadataError::NotFound`] when no such inode exists, or
    /// [`MetadataError::CapabilityUnsupported`] when the backend does not
    /// advertise [`Capability::HardLinks`].
    async fn load_inode(
        &self,
        namespace: &NamespaceId,
        inode: InodeNumber,
    ) -> MetadataResult<InodeRecord>;

    /// Apply a transaction atomically.
    ///
    /// Every precondition is evaluated against one linearizable view before any
    /// operation applies. A failure leaves the store untouched.
    ///
    /// This is on the trait rather than each backend because §5's promotion
    /// commit depends on the property, and the shared contract suite has to be
    /// able to assert it for *every* backend. A backend that cannot provide
    /// cross-record atomicity must not advertise [`Capability::HardLinks`] and
    /// must reject this call, per §7.
    ///
    /// # Errors
    ///
    /// [`MetadataError::CompareAndSwapFailed`] when a precondition does not
    /// hold, or [`MetadataError::CapabilityUnsupported`] when the backend does
    /// not advertise the capability the transaction requires.
    async fn commit(&self, transaction: &Transaction) -> MetadataResult<TransactionOutcome>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A store advertising nothing, used to pin the refusal contract.
    struct UncapableStore;

    #[async_trait]
    impl MetadataStore for UncapableStore {
        fn backend(&self) -> MetadataBackend {
            MetadataBackend::Memory
        }

        fn capabilities(&self) -> CapabilitySet {
            CapabilitySet::none()
        }

        async fn check_ready(&self) -> MetadataResult<BackendHealth> {
            Ok(BackendHealth {
                ready: true,
                detail: "in-process".to_owned(),
            })
        }

        async fn mapping_revision(
            &self,
            _namespace: &NamespaceId,
        ) -> MetadataResult<MappingRevision> {
            Err(MetadataError::CapabilityUnsupported {
                backend: self.backend(),
                capability: Capability::HardLinks,
            })
        }

        async fn resolve_path(
            &self,
            _namespace: &NamespaceId,
            _path: &str,
        ) -> MetadataResult<Option<PathIndexEntry>> {
            Err(MetadataError::CapabilityUnsupported {
                backend: self.backend(),
                capability: Capability::HardLinks,
            })
        }

        async fn load_inode(
            &self,
            _namespace: &NamespaceId,
            _inode: InodeNumber,
        ) -> MetadataResult<InodeRecord> {
            Err(MetadataError::CapabilityUnsupported {
                backend: self.backend(),
                capability: Capability::HardLinks,
            })
        }

        async fn commit(&self, _transaction: &Transaction) -> MetadataResult<TransactionOutcome> {
            Err(MetadataError::CapabilityUnsupported {
                backend: self.backend(),
                capability: Capability::HardLinks,
            })
        }
    }

    #[test]
    fn a_backend_advertises_nothing_until_it_says_otherwise() {
        // §7: capabilities are declared per backend, never inferred. A store
        // that has not claimed a capability must report that honestly.
        let store = UncapableStore;
        assert!(store.capabilities().is_empty());
        for capability in Capability::ALL {
            assert!(!store.supports(capability));
        }
    }

    #[test]
    fn refusal_is_reported_as_a_capability_gap_not_a_transient_fault() {
        // The distinction §4 maps to EOPNOTSUPP vs ENOLCK: a caller must be able
        // to tell "this cluster does not offer the feature" from "it does, but
        // the store is unreachable", and must not retry the former.
        let error = MetadataError::CapabilityUnsupported {
            backend: MetadataBackend::Memory,
            capability: Capability::HardLinks,
        };
        assert!(error.is_capability_gap());
        assert!(!error.is_retryable());
    }
}
