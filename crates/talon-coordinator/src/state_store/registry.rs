//! Atomic persistent registry operations shared by every backend.
use super::{ClusterStateStore, StateBackend, StateStoreError, StateStoreResult, StoreRevision};
use talon_core::worker_membership::{MemberRegistry, MembershipMode, WorkerMember};

pub const MAX_REGISTRY_BYTES: usize = 512 * 1024;
#[derive(Debug, Clone)]
pub struct RegistrySnapshot {
    pub value: MemberRegistry,
    pub revision: Option<StoreRevision>,
}

pub(crate) fn invalid(backend: StateBackend, detail: impl Into<String>) -> StateStoreError {
    StateStoreError::InvalidRegistry {
        backend,
        detail: detail.into(),
    }
}
pub(crate) fn encode(value: &MemberRegistry, backend: StateBackend) -> StateStoreResult<Vec<u8>> {
    value.validate().map_err(|e| invalid(backend, e))?;
    let bytes = serde_json::to_vec(value).map_err(|e| invalid(backend, e.to_string()))?;
    if bytes.len() > MAX_REGISTRY_BYTES {
        return Err(invalid(backend, "member registry exceeds 512 KiB limit"));
    }
    Ok(bytes)
}
#[cfg(any(feature = "etcd", feature = "kubernetes"))]
pub(crate) fn decode(bytes: &[u8], backend: StateBackend) -> StateStoreResult<MemberRegistry> {
    if bytes.len() > MAX_REGISTRY_BYTES {
        return Err(invalid(backend, "member registry exceeds size limit"));
    }
    let value: MemberRegistry =
        serde_json::from_slice(bytes).map_err(|e| invalid(backend, e.to_string()))?;
    value.validate().map_err(|e| invalid(backend, e))?;
    Ok(value)
}

/// Administrative mutations cannot be synthesized by heartbeats.
#[derive(Debug, Clone)]
pub enum MemberChange {
    Register {
        worker_id: String,
        zone: Option<String>,
    },
    SetMember(WorkerMember),
    SetMode(MembershipMode),
}

pub async fn change(
    store: &dyn ClusterStateStore,
    cluster: &str,
    change: MemberChange,
) -> StateStoreResult<MemberRegistry> {
    for _ in 0..32 {
        let snapshot = store.member_registry(cluster).await?;
        let mut value = snapshot.value;
        match &change {
            MemberChange::Register { worker_id, zone } => {
                match value.members.iter().find(|m| &m.worker_id == worker_id) {
                    Some(member) if member.retired => {
                        return Err(invalid(
                            store.backend(),
                            "member retired; explicit administrative re-enable required",
                        ))
                    }
                    Some(member) if &member.zone != zone => {
                        return Err(invalid(
                            store.backend(),
                            "member zone differs; explicit administrative change required",
                        ))
                    }
                    Some(_) => return Ok(value),
                    None => value.members.push(WorkerMember {
                        worker_id: worker_id.clone(),
                        zone: zone.clone(),
                        retired: false,
                    }),
                }
            }
            MemberChange::SetMember(member) => {
                if let Some(existing) = value
                    .members
                    .iter_mut()
                    .find(|m| m.worker_id == member.worker_id)
                {
                    *existing = member.clone();
                } else {
                    value.members.push(member.clone());
                }
            }
            MemberChange::SetMode(mode) => value.mode = *mode,
        }
        value.members.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));
        encode(&value, store.backend())?;
        if store
            .compare_member_registry(cluster, snapshot.revision.as_ref(), &value)
            .await?
        {
            tracing::info!(
                cluster,
                ?change,
                topology_token = value.topology_token(),
                "persistent membership changed"
            );
            return Ok(value);
        }
    }
    Err(StateStoreError::Unavailable {
        backend: store.backend(),
        detail: "member registry write contention".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStateStore;
    #[test]
    #[cfg(any(feature = "etcd", feature = "kubernetes"))]
    fn malformed_registry_is_not_a_retryable_backend_outage() {
        let unsupported = serde_json::to_vec(&MemberRegistry {
            format_version: 2,
            ..Default::default()
        })
        .unwrap();
        for bytes in [
            b"{invalid".to_vec(),
            unsupported,
            vec![b' '; MAX_REGISTRY_BYTES + 1],
        ] {
            let error = decode(&bytes, StateBackend::Memory).unwrap_err();
            assert!(matches!(error, StateStoreError::InvalidRegistry { .. }));
            assert!(!error.is_retryable());
        }
        let oversized = MemberRegistry {
            members: vec![WorkerMember {
                worker_id: "x".repeat(257),
                zone: None,
                retired: false,
            }],
            ..Default::default()
        };
        assert!(!encode(&oversized, StateBackend::Memory)
            .unwrap_err()
            .is_retryable());
    }

    #[test]
    fn oversized_future_discovery_is_a_nonretryable_registry_error() {
        let registry = MemberRegistry {
            members: (0..2_000)
                .map(|n| WorkerMember {
                    worker_id: format!("w{n}"),
                    zone: None,
                    retired: false,
                })
                .collect(),
            ..Default::default()
        };
        assert!(serde_json::to_vec(&registry).unwrap().len() < MAX_REGISTRY_BYTES);
        let error = encode(&registry, StateBackend::Memory).unwrap_err();
        assert!(matches!(error, StateStoreError::InvalidRegistry { .. }));
        assert!(!error.is_retryable());
    }

    #[tokio::test]
    async fn registry_cas_retirement_and_mode_are_persistent() {
        let store = MemoryStateStore::new();
        let initial = store.member_registry("c").await.unwrap();
        assert!(initial.revision.is_none());
        assert_eq!(initial.value.mode, MembershipMode::Legacy);
        let first = change(
            &store,
            "c",
            MemberChange::Register {
                worker_id: "w".into(),
                zone: Some("z".into()),
            },
        )
        .await
        .unwrap();
        assert!(!store
            .compare_member_registry("c", None, &MemberRegistry::default())
            .await
            .unwrap());
        let token = first.topology_token();
        assert_eq!(
            change(&store, "c", MemberChange::SetMode(MembershipMode::Retained))
                .await
                .unwrap()
                .topology_token(),
            token
        );
        change(
            &store,
            "c",
            MemberChange::SetMember(WorkerMember {
                worker_id: "w".into(),
                zone: Some("z".into()),
                retired: true,
            }),
        )
        .await
        .unwrap();
        assert!(change(
            &store,
            "c",
            MemberChange::Register {
                worker_id: "w".into(),
                zone: Some("z".into())
            }
        )
        .await
        .is_err());
        assert!(store.member_registry("c").await.unwrap().value.members[0].retired);
        assert!(store
            .member_registry("other")
            .await
            .unwrap()
            .value
            .members
            .is_empty());
    }
    #[tokio::test]
    async fn concurrent_registration_preserves_all_members() {
        let store = std::sync::Arc::new(MemoryStateStore::new());
        let mut tasks = tokio::task::JoinSet::new();
        for n in 0..32 {
            let store = store.clone();
            tasks.spawn(async move {
                change(
                    store.as_ref(),
                    "c",
                    MemberChange::Register {
                        worker_id: format!("w{n}"),
                        zone: None,
                    },
                )
                .await
                .unwrap();
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        assert_eq!(
            store
                .member_registry("c")
                .await
                .unwrap()
                .value
                .members
                .len(),
            32
        );
    }
}
