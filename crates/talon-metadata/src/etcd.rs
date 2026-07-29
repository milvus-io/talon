//! etcd-backed [`MetadataStore`].
//!
//! ADR 0003 §7 names etcd as the first backend able to carry the richer
//! capabilities:
//!
//! > The first hard-link and write-back TMS implementation therefore targets
//! > etcd; a lease-only backend may still provide locking if it meets the
//! > locking contract.
//!
//! The reason is transactions. etcd's `Txn` evaluates every comparison against
//! one revision and applies every operation atomically, which is what §5's
//! promotion commit needs — the four changes it makes have no valid partial
//! interpretation.
//!
//! # Why etcd is viable despite holding its keyspace in memory
//!
//! Only because of §3's sparsity. etcd has "a practical ceiling in the
//! single-digit GB and a 1.5 MB per-value limit", so per-object records for a
//! billion-object bucket would not fit. Per-*linked*-file and per-*locked*-file
//! records are orders of magnitude smaller, and zero for workloads using
//! neither feature.
//!
//! This backend is therefore safe **only while §2's admission rule holds**. If a
//! per-object record for ordinary files ever lands here, etcd stops being a
//! viable backend. `record_population_does_not_scale_with_object_count` guards
//! that.
//!
//! # Prefix separation
//!
//! §7 permits sharing a physical etcd cluster with `ClusterStateStore` but
//! requires the two to stay separate abstractions. They use disjoint prefixes,
//! and `metadata_and_cluster_state_prefixes_do_not_collide` asserts it: mixing
//! them would make ADR 0001 §2's "bounded, rebuildable" invariant untrue by
//! construction, because TMS records are neither.

use std::time::Duration;

use async_trait::async_trait;
use etcd_client::{Client, Compare, CompareOp, GetOptions, Txn, TxnOp};
use tokio::time::timeout;

use crate::capability::{Capability, CapabilitySet};
use crate::error::{MetadataBackend, MetadataError, MetadataResult};
use crate::record::{InodeNumber, InodeRecord, LinkCount, NamespaceId, PathIndexEntry};
use crate::revision::{MappingRevision, StoreRevision};
use crate::transaction::{Operation, Precondition, Transaction, TransactionOutcome};
use crate::{BackendHealth, MetadataStore};

/// Default keyspace prefix for TMS records.
///
/// Deliberately distinct from `ClusterStateStore`'s `/talon`: §7 keeps the two
/// stores separate even when they share an etcd cluster.
pub const DEFAULT_METADATA_PREFIX: &str = "/talon-metadata";

const OPERATION_TIMEOUT: Duration = Duration::from_secs(10);

/// Configuration for the etcd metadata backend.
#[derive(Debug, Clone)]
pub struct EtcdMetadataConfig {
    /// etcd endpoints.
    pub endpoints: Vec<String>,
    /// Keyspace prefix for TMS records.
    pub prefix: String,
}

impl EtcdMetadataConfig {
    /// Configuration for a single endpoint using the default prefix.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoints: vec![endpoint.into()],
            prefix: DEFAULT_METADATA_PREFIX.to_owned(),
        }
    }

    fn normalized_prefix(&self) -> String {
        self.prefix.trim_end_matches('/').to_owned()
    }
}

/// An etcd-backed metadata store.
#[derive(Clone)]
pub struct EtcdMetadataStore {
    client: Client,
    prefix: String,
}

impl std::fmt::Debug for EtcdMetadataStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdMetadataStore")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl EtcdMetadataStore {
    /// Connect to etcd.
    ///
    /// # Errors
    ///
    /// [`MetadataError::Unavailable`] when the cluster cannot be reached.
    pub async fn connect(config: &EtcdMetadataConfig) -> MetadataResult<Self> {
        let client = timeout(
            OPERATION_TIMEOUT,
            Client::connect(config.endpoints.clone(), None),
        )
        .await
        .map_err(|_| MetadataError::Timeout {
            backend: MetadataBackend::Etcd,
        })?
        .map_err(map_etcd_error)?;

        Ok(Self {
            client,
            prefix: config.normalized_prefix(),
        })
    }

    fn mapping_revision_key(&self, namespace: &NamespaceId) -> String {
        format!("{}/ns/{}/mapping_revision", self.prefix, namespace)
    }

    fn path_key(&self, namespace: &NamespaceId, path: &str) -> String {
        format!("{}/ns/{}/paths/{path}", self.prefix, namespace)
    }

    fn inode_key(&self, namespace: &NamespaceId, inode: InodeNumber) -> String {
        format!("{}/ns/{}/inodes/{inode}", self.prefix, namespace)
    }

    fn transition_key(&self, namespace: &NamespaceId, operation_id: &str) -> String {
        format!(
            "{}/ns/{}/transitions/{operation_id}",
            self.prefix, namespace
        )
    }

    async fn get_raw(&self, key: &str) -> MetadataResult<Option<Vec<u8>>> {
        let mut client = self.client.clone();
        let response = timeout(OPERATION_TIMEOUT, client.get(key, None))
            .await
            .map_err(|_| MetadataError::Timeout {
                backend: MetadataBackend::Etcd,
            })?
            .map_err(map_etcd_error)?;
        Ok(response.kvs().first().map(|kv| kv.value().to_vec()))
    }

    async fn commit_txn(&self, transaction: &Transaction) -> MetadataResult<TransactionOutcome> {
        let mut compares = Vec::new();
        for precondition in transaction.preconditions() {
            compares.push(self.compare_for(precondition).await?);
        }

        let mut ops = Vec::new();
        let mut mapping_revision = None;
        for operation in transaction.operations() {
            let (op, revision) = self.op_for(operation).await?;
            // A read-modify-write operation fences itself against the value it
            // observed, so a concurrent advance between the read and the commit
            // aborts this transaction rather than silently losing an increment.
            if let Some(next) = revision {
                if let Some(fence) = self.fence_for(operation, previous_of(next)) {
                    compares.push(fence);
                }
            }
            ops.push(op);
            mapping_revision = revision.or(mapping_revision);
        }

        let txn = Txn::new().when(compares).and_then(ops);
        let mut client = self.client.clone();
        let response = timeout(OPERATION_TIMEOUT, client.txn(txn))
            .await
            .map_err(|_| MetadataError::Timeout {
                backend: MetadataBackend::Etcd,
            })?
            .map_err(map_etcd_error)?;

        if !response.succeeded() {
            return Err(MetadataError::CompareAndSwapFailed {
                expected: StoreRevision::new("precondition")?,
                observed: StoreRevision::new("mismatch")?,
            });
        }

        let revision = response
            .header()
            .map(|header| header.revision())
            .unwrap_or_default();
        Ok(TransactionOutcome {
            revision: StoreRevision::new(revision.to_string())?,
            mapping_revision,
        })
    }

    async fn compare_for(&self, precondition: &Precondition) -> MetadataResult<Compare> {
        Ok(match precondition {
            Precondition::MappingRevisionIs {
                namespace,
                expected,
            } => {
                let key = self.mapping_revision_key(namespace);
                if *expected == MappingRevision::INITIAL {
                    // A namespace that has never had a transition stores no
                    // revision key at all -- that is §3's sparsity claim applied
                    // to the mapping guard. Comparing its value would evaluate
                    // against a missing key and always fail, so the absence of
                    // the key *is* the initial revision.
                    Compare::create_revision(key, CompareOp::Equal, 0)
                } else {
                    Compare::value(key, CompareOp::Equal, expected.get().to_string())
                }
            }
            Precondition::PathIsUnmapped { namespace, path } => {
                Compare::create_revision(self.path_key(namespace, path), CompareOp::Equal, 0)
            }
            Precondition::PathResolvesTo {
                namespace,
                path,
                inode,
            } => Compare::value(
                self.path_key(namespace, path),
                CompareOp::Equal,
                inode.get().to_string(),
            ),
            Precondition::TransitionExists {
                namespace,
                operation_id,
            } => Compare::create_revision(
                self.transition_key(namespace, operation_id),
                CompareOp::Greater,
                0,
            ),
            Precondition::TransitionAbsent {
                namespace,
                operation_id,
            } => Compare::create_revision(
                self.transition_key(namespace, operation_id),
                CompareOp::Equal,
                0,
            ),
        })
    }

    async fn op_for(
        &self,
        operation: &Operation,
    ) -> MetadataResult<(TxnOp, Option<MappingRevision>)> {
        Ok(match operation {
            Operation::PutPathIndex(entry) => (
                TxnOp::put(
                    self.path_key(&entry.namespace, &entry.path),
                    entry.inode.get().to_string(),
                    None,
                ),
                None,
            ),
            Operation::RemovePathIndex { namespace, path } => {
                (TxnOp::delete(self.path_key(namespace, path), None), None)
            }
            Operation::PutInode(record) => (
                TxnOp::put(
                    self.inode_key(&record.namespace, record.inode),
                    encode_inode(record),
                    None,
                ),
                None,
            ),
            Operation::RemoveInode { namespace, inode } => {
                (TxnOp::delete(self.inode_key(namespace, *inode), None), None)
            }
            Operation::PutTransition(transition) => (
                TxnOp::put(
                    self.transition_key(&transition.namespace, &transition.operation_id),
                    transition.source_path.clone(),
                    None,
                ),
                None,
            ),
            Operation::RemoveTransition {
                namespace,
                operation_id,
            } => (
                TxnOp::delete(self.transition_key(namespace, operation_id), None),
                None,
            ),
            Operation::AdvanceMappingRevision { namespace } => {
                // Read-modify-write, so the value read here must be fenced or a
                // concurrent transaction could advance the revision between the
                // read and the commit and one increment would be lost. The
                // caller usually supplies MappingRevisionIs, but correctness
                // must not depend on the caller remembering: `fence_for` adds
                // the matching comparison unconditionally, and a duplicate is
                // harmless because etcd evaluates all comparisons against one
                // revision.
                let current = self.read_mapping_revision(namespace).await?;
                let next = current.next();
                (
                    TxnOp::put(
                        self.mapping_revision_key(namespace),
                        next.get().to_string(),
                        None,
                    ),
                    Some(next),
                )
            }
        })
    }

    /// The comparison that fences a read-modify-write operation.
    ///
    /// Returns `None` for operations that write an absolute value and therefore
    /// need no fence.
    fn fence_for(&self, operation: &Operation, observed: MappingRevision) -> Option<Compare> {
        match operation {
            Operation::AdvanceMappingRevision { namespace } => {
                let key = self.mapping_revision_key(namespace);
                Some(if observed == MappingRevision::INITIAL {
                    Compare::create_revision(key, CompareOp::Equal, 0)
                } else {
                    Compare::value(key, CompareOp::Equal, observed.get().to_string())
                })
            }
            _ => None,
        }
    }

    async fn read_mapping_revision(
        &self,
        namespace: &NamespaceId,
    ) -> MetadataResult<MappingRevision> {
        let key = self.mapping_revision_key(namespace);
        let Some(raw) = self.get_raw(&key).await? else {
            return Ok(MappingRevision::INITIAL);
        };
        decode_u64(&raw).map(MappingRevision::new)
    }

    /// Count stored keys under this store's prefix.
    ///
    /// Exists for the regression test that guards §2's admission rule: if the
    /// record population ever starts tracking object count, etcd stops being a
    /// viable backend (§3).
    ///
    /// # Errors
    ///
    /// [`MetadataError::Unavailable`] when etcd cannot be reached.
    pub async fn record_count(&self) -> MetadataResult<i64> {
        let mut client = self.client.clone();
        let response = timeout(
            OPERATION_TIMEOUT,
            client.get(
                self.prefix.clone(),
                Some(GetOptions::new().with_prefix().with_count_only()),
            ),
        )
        .await
        .map_err(|_| MetadataError::Timeout {
            backend: MetadataBackend::Etcd,
        })?
        .map_err(map_etcd_error)?;
        Ok(response.count())
    }
}

#[async_trait]
impl MetadataStore for EtcdMetadataStore {
    fn backend(&self) -> MetadataBackend {
        MetadataBackend::Etcd
    }

    fn capabilities(&self) -> CapabilitySet {
        // Hard links only. etcd can satisfy the write-back contract's
        // transactional requirements, but §9.11 keeps write-back unreachable
        // until an ADR superseding ADR 0002 is accepted, and locks await their
        // own ADR defining byte-range representation, fairness, waiter
        // recovery, and deadlock detection. Advertising either before the
        // supporting mechanism exists would be the over-promise §7 forbids.
        CapabilitySet::none().with(Capability::HardLinks)
    }

    async fn check_ready(&self) -> MetadataResult<BackendHealth> {
        let mut client = self.client.clone();
        match timeout(OPERATION_TIMEOUT, client.status()).await {
            Ok(Ok(_)) => Ok(BackendHealth {
                ready: true,
                detail: "etcd reachable".to_owned(),
            }),
            Ok(Err(error)) => Ok(BackendHealth {
                ready: false,
                detail: sanitize(&error.to_string()),
            }),
            Err(_) => Ok(BackendHealth {
                ready: false,
                detail: "status request timed out".to_owned(),
            }),
        }
    }

    async fn mapping_revision(&self, namespace: &NamespaceId) -> MetadataResult<MappingRevision> {
        self.read_mapping_revision(namespace).await
    }

    async fn resolve_path(
        &self,
        namespace: &NamespaceId,
        path: &str,
    ) -> MetadataResult<Option<PathIndexEntry>> {
        let key = self.path_key(namespace, path);
        let Some(raw) = self.get_raw(&key).await? else {
            return Ok(None);
        };
        let inode = InodeNumber::new(decode_u64(&raw)?)?;
        Ok(Some(PathIndexEntry {
            namespace: namespace.clone(),
            path: path.to_owned(),
            inode,
        }))
    }

    async fn commit(&self, transaction: &Transaction) -> MetadataResult<TransactionOutcome> {
        self.commit_txn(transaction).await
    }

    async fn load_inode(
        &self,
        namespace: &NamespaceId,
        inode: InodeNumber,
    ) -> MetadataResult<InodeRecord> {
        let key = self.inode_key(namespace, inode);
        let raw = self
            .get_raw(&key)
            .await?
            .ok_or_else(|| MetadataError::NotFound {
                key: format!("inode/{inode}"),
            })?;
        decode_inode(namespace, inode, &raw)
    }
}

fn encode_inode(record: &InodeRecord) -> String {
    format!("{}:{}", record.link_count.get(), u8::from(record.corrupt))
}

fn decode_inode(
    namespace: &NamespaceId,
    inode: InodeNumber,
    raw: &[u8],
) -> MetadataResult<InodeRecord> {
    let text = std::str::from_utf8(raw).map_err(|_| MetadataError::InvalidRecord {
        detail: "inode record is not valid UTF-8".to_owned(),
    })?;
    let (count, corrupt) = text
        .split_once(':')
        .ok_or_else(|| MetadataError::InvalidRecord {
            detail: "inode record must be <link_count>:<corrupt>".to_owned(),
        })?;
    let link_count =
        LinkCount::new(
            count
                .parse::<u64>()
                .map_err(|_| MetadataError::InvalidRecord {
                    detail: "inode link count is not a number".to_owned(),
                })?,
        )?;
    Ok(InodeRecord {
        namespace: namespace.clone(),
        inode,
        link_count,
        corrupt: corrupt != "0",
    })
}

fn decode_u64(raw: &[u8]) -> MetadataResult<u64> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|text| text.parse::<u64>().ok())
        .ok_or_else(|| MetadataError::InvalidRecord {
            detail: "value is not a base-10 integer".to_owned(),
        })
}

fn sanitize(detail: &str) -> String {
    detail.replace(['\n', '\r'], " ")
}

fn map_etcd_error(error: etcd_client::Error) -> MetadataError {
    use etcd_client::Error;
    match error {
        Error::GRpcStatus(status) => match status.code() {
            tonic::Code::Unauthenticated => MetadataError::Authentication {
                backend: MetadataBackend::Etcd,
            },
            tonic::Code::PermissionDenied => MetadataError::PermissionDenied {
                backend: MetadataBackend::Etcd,
            },
            tonic::Code::DeadlineExceeded => MetadataError::Timeout {
                backend: MetadataBackend::Etcd,
            },
            _ => MetadataError::Unavailable {
                backend: MetadataBackend::Etcd,
                detail: sanitize(status.message()),
            },
        },
        other => MetadataError::Unavailable {
            backend: MetadataBackend::Etcd,
            detail: sanitize(&other.to_string()),
        },
    }
}

/// The revision immediately before `next`.
///
/// `op_for` returns the value it will write; the fence has to compare against
/// the value it read. Saturating at zero is correct because
/// [`MappingRevision::INITIAL`] is the floor.
fn previous_of(next: MappingRevision) -> MappingRevision {
    MappingRevision::new(next.get().saturating_sub(1))
}
