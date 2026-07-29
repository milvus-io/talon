//! In-process [`MetadataStore`] for tests and single-node development.
//!
//! Advertises [`Capability::HardLinks`] because it does provide real atomic
//! multi-record transactions — everything happens under one mutex, so a
//! transaction either applies fully or not at all.
//!
//! It does **not** advertise [`Capability::Locks`] or
//! [`Capability::WriteBack`]. Both require server-observed expiring sessions,
//! and §9's write-back additionally requires fencing terms that "must survive
//! owner lease expiry". A single-process store cannot observe the expiry of a
//! session belonging to a client in another process, and its records do not
//! survive the process at all. Claiming either would be exactly the silent
//! over-promise §7 forbids:
//!
//! > A backend that cannot satisfy one of these contracts must not advertise
//! > that capability.
//!
//! This store is for tests. Nothing in production should depend on it.

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::capability::{Capability, CapabilitySet};
use crate::error::{MetadataBackend, MetadataError, MetadataResult};
use crate::record::{InodeNumber, InodeRecord, LinkTransition, NamespaceId, PathIndexEntry};
use crate::revision::{MappingRevision, StoreRevision};
use crate::transaction::{Operation, Precondition, Transaction, TransactionOutcome};
use crate::{BackendHealth, MetadataStore};

#[derive(Debug, Default)]
struct NamespaceState {
    mapping_revision: MappingRevision,
    paths: BTreeMap<String, InodeNumber>,
    inodes: BTreeMap<u64, InodeRecord>,
    transitions: BTreeMap<String, LinkTransition>,
}

/// A point-in-time view of one namespace, for contract assertions.
#[derive(Debug, Default)]
pub struct NamespaceSnapshot {
    /// Current mapping revision.
    pub mapping_revision: MappingRevision,
    /// Path index entries, keyed by visible path.
    pub paths: BTreeMap<String, InodeNumber>,
    /// Inode records, keyed by inode number.
    pub inodes: BTreeMap<u64, InodeRecord>,
    /// In-flight link transitions, keyed by operation id.
    pub transitions: BTreeMap<String, LinkTransition>,
}

#[derive(Debug, Default)]
struct State {
    namespaces: BTreeMap<String, NamespaceState>,
    revision: u64,
}

/// An in-process metadata store.
#[derive(Debug)]
pub struct MemoryMetadataStore {
    state: Mutex<State>,
    capabilities: CapabilitySet,
}

impl MemoryMetadataStore {
    /// A store advertising hard links.
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
            capabilities: CapabilitySet::none().with(Capability::HardLinks),
        }
    }

    /// A store advertising nothing.
    ///
    /// Used to exercise the refusal paths: callers must return the errno for
    /// "this cluster does not offer the feature" without falling back to a
    /// local approximation (§4).
    pub fn without_capabilities() -> Self {
        Self {
            state: Mutex::new(State::default()),
            capabilities: CapabilitySet::none(),
        }
    }

    fn require(&self, capability: Capability) -> MetadataResult<()> {
        if self.capabilities.supports(capability) {
            return Ok(());
        }
        Err(MetadataError::CapabilityUnsupported {
            backend: MetadataBackend::Memory,
            capability,
        })
    }

    /// Apply a transaction atomically.
    ///
    /// Every precondition is evaluated against one view before any operation is
    /// applied, so a failure leaves the store untouched. This is the property
    /// §5's promotion commit depends on.
    ///
    /// # Errors
    ///
    /// [`MetadataError::CompareAndSwapFailed`] when a precondition does not
    /// hold, or [`MetadataError::CapabilityUnsupported`] when hard links are not
    /// advertised.
    pub fn commit(&self, transaction: &Transaction) -> MetadataResult<TransactionOutcome> {
        self.require(Capability::HardLinks)?;
        let mut state = self.state.lock().expect("metadata state mutex poisoned");

        for precondition in transaction.preconditions() {
            Self::check(&state, precondition)?;
        }

        let mut mapping_revision = None;
        for operation in transaction.operations() {
            mapping_revision = Self::apply(&mut state, operation).or(mapping_revision);
        }

        state.revision += 1;
        let revision = StoreRevision::new(state.revision.to_string())?;
        Ok(TransactionOutcome {
            revision,
            mapping_revision,
        })
    }

    fn check(state: &State, precondition: &Precondition) -> MetadataResult<()> {
        let fail =
            |detail: String| Err(MetadataError::InvalidRecord { detail }) as MetadataResult<()>;
        match precondition {
            Precondition::MappingRevisionIs {
                namespace,
                expected,
            } => {
                let actual = state
                    .namespaces
                    .get(namespace.as_str())
                    .map_or(MappingRevision::INITIAL, |ns| ns.mapping_revision);
                if actual != *expected {
                    return Err(MetadataError::CompareAndSwapFailed {
                        expected: StoreRevision::new(expected.to_string())?,
                        observed: StoreRevision::new(actual.to_string())?,
                    });
                }
                Ok(())
            }
            Precondition::PathIsUnmapped { namespace, path } => {
                let mapped = state
                    .namespaces
                    .get(namespace.as_str())
                    .is_some_and(|ns| ns.paths.contains_key(path));
                if mapped {
                    return fail(format!("path {path} is already mapped to an inode"));
                }
                Ok(())
            }
            Precondition::PathResolvesTo {
                namespace,
                path,
                inode,
            } => {
                let actual = state
                    .namespaces
                    .get(namespace.as_str())
                    .and_then(|ns| ns.paths.get(path));
                if actual != Some(inode) {
                    return fail(format!("path {path} does not resolve to inode {inode}"));
                }
                Ok(())
            }
            Precondition::TransitionExists {
                namespace,
                operation_id,
            } => {
                let present = state
                    .namespaces
                    .get(namespace.as_str())
                    .is_some_and(|ns| ns.transitions.contains_key(operation_id));
                if !present {
                    return Err(MetadataError::NotFound {
                        key: format!("transition/{operation_id}"),
                    });
                }
                Ok(())
            }
            Precondition::TransitionAbsent {
                namespace,
                operation_id,
            } => {
                let present = state
                    .namespaces
                    .get(namespace.as_str())
                    .is_some_and(|ns| ns.transitions.contains_key(operation_id));
                if present {
                    return Err(MetadataError::AlreadyExists {
                        key: format!("transition/{operation_id}"),
                    });
                }
                Ok(())
            }
        }
    }

    fn apply(state: &mut State, operation: &Operation) -> Option<MappingRevision> {
        match operation {
            Operation::PutPathIndex(entry) => {
                state
                    .namespaces
                    .entry(entry.namespace.as_str().to_owned())
                    .or_default()
                    .paths
                    .insert(entry.path.clone(), entry.inode);
                None
            }
            Operation::RemovePathIndex { namespace, path } => {
                if let Some(ns) = state.namespaces.get_mut(namespace.as_str()) {
                    ns.paths.remove(path);
                }
                None
            }
            Operation::PutInode(record) => {
                state
                    .namespaces
                    .entry(record.namespace.as_str().to_owned())
                    .or_default()
                    .inodes
                    .insert(record.inode.get(), record.clone());
                None
            }
            Operation::RemoveInode { namespace, inode } => {
                if let Some(ns) = state.namespaces.get_mut(namespace.as_str()) {
                    ns.inodes.remove(&inode.get());
                }
                None
            }
            Operation::PutTransition(transition) => {
                state
                    .namespaces
                    .entry(transition.namespace.as_str().to_owned())
                    .or_default()
                    .transitions
                    .insert(transition.operation_id.clone(), transition.clone());
                None
            }
            Operation::RemoveTransition {
                namespace,
                operation_id,
            } => {
                if let Some(ns) = state.namespaces.get_mut(namespace.as_str()) {
                    ns.transitions.remove(operation_id);
                }
                None
            }
            Operation::AdvanceMappingRevision { namespace } => {
                let ns = state
                    .namespaces
                    .entry(namespace.as_str().to_owned())
                    .or_default();
                ns.mapping_revision = ns.mapping_revision.next();
                Some(ns.mapping_revision)
            }
        }
    }

    /// A snapshot of one namespace, for contract assertions.
    ///
    /// Exposes internal state so the shared suite can prove that a failed
    /// transaction left *nothing* behind — a claim that cannot be made from the
    /// public read API alone, since it must also cover records the caller never
    /// asked for.
    pub fn state_snapshot(&self, namespace: &NamespaceId) -> NamespaceSnapshot {
        let state = self.state.lock().expect("metadata state mutex poisoned");
        state
            .namespaces
            .get(namespace.as_str())
            .map_or_else(NamespaceSnapshot::default, |ns| NamespaceSnapshot {
                mapping_revision: ns.mapping_revision,
                paths: ns.paths.clone(),
                inodes: ns.inodes.clone(),
                transitions: ns.transitions.clone(),
            })
    }

    /// Load a link transition, if present.
    ///
    /// # Errors
    ///
    /// [`MetadataError::CapabilityUnsupported`] when hard links are not
    /// advertised.
    pub fn load_transition(
        &self,
        namespace: &NamespaceId,
        operation_id: &str,
    ) -> MetadataResult<Option<LinkTransition>> {
        self.require(Capability::HardLinks)?;
        let state = self.state.lock().expect("metadata state mutex poisoned");
        Ok(state
            .namespaces
            .get(namespace.as_str())
            .and_then(|ns| ns.transitions.get(operation_id))
            .cloned())
    }
}

impl Default for MemoryMetadataStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MetadataStore for MemoryMetadataStore {
    fn backend(&self) -> MetadataBackend {
        MetadataBackend::Memory
    }

    fn capabilities(&self) -> CapabilitySet {
        self.capabilities
    }

    async fn check_ready(&self) -> MetadataResult<BackendHealth> {
        Ok(BackendHealth {
            ready: true,
            detail: "in-process metadata store".to_owned(),
        })
    }

    async fn mapping_revision(&self, namespace: &NamespaceId) -> MetadataResult<MappingRevision> {
        self.require(Capability::HardLinks)?;
        let state = self.state.lock().expect("metadata state mutex poisoned");
        Ok(state
            .namespaces
            .get(namespace.as_str())
            .map_or(MappingRevision::INITIAL, |ns| ns.mapping_revision))
    }

    async fn resolve_path(
        &self,
        namespace: &NamespaceId,
        path: &str,
    ) -> MetadataResult<Option<PathIndexEntry>> {
        self.require(Capability::HardLinks)?;
        let state = self.state.lock().expect("metadata state mutex poisoned");
        Ok(state
            .namespaces
            .get(namespace.as_str())
            .and_then(|ns| ns.paths.get(path))
            .map(|inode| PathIndexEntry {
                namespace: namespace.clone(),
                path: path.to_owned(),
                inode: *inode,
            }))
    }

    async fn load_inode(
        &self,
        namespace: &NamespaceId,
        inode: InodeNumber,
    ) -> MetadataResult<InodeRecord> {
        self.require(Capability::HardLinks)?;
        let state = self.state.lock().expect("metadata state mutex poisoned");
        state
            .namespaces
            .get(namespace.as_str())
            .and_then(|ns| ns.inodes.get(&inode.get()))
            .cloned()
            .ok_or_else(|| MetadataError::NotFound {
                key: format!("inode/{inode}"),
            })
    }
}
