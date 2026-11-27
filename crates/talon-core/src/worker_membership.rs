//! Persistent logical membership, independent of process leases.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Maximum observation lifetime advertised as a client background-refresh hint.
pub const DISCOVERY_MAX_AGE_MS: u64 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerMember {
    pub worker_id: String,
    pub zone: Option<String>,
    pub retired: bool,
}

/// One atomically updated cluster resource. Instance leases are stored separately.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberRegistry {
    pub format_version: u32,
    pub members: Vec<WorkerMember>,
}

impl Default for MemberRegistry {
    fn default() -> Self {
        Self {
            format_version: 1,
            members: Vec::new(),
        }
    }
}

impl MemberRegistry {
    /// Register a worker idempotently. Changing an existing zone or re-enabling
    /// a retired worker requires an explicit administrative update.
    pub fn register(&mut self, worker_id: &str, zone: Option<&str>) -> Result<bool, &'static str> {
        match self.members.iter().find(|m| m.worker_id == worker_id) {
            Some(member) if member.retired => {
                Err("member retired; explicit administrative re-enable required")
            }
            Some(member) if member.zone.as_deref() != zone => {
                Err("member zone differs; explicit administrative change required")
            }
            Some(_) => Ok(false),
            None => {
                self.members.push(WorkerMember {
                    worker_id: worker_id.to_owned(),
                    zone: zone.map(str::to_owned),
                    retired: false,
                });
                Ok(true)
            }
        }
    }

    /// Replace or insert an explicitly configured member.
    pub fn set_member(&mut self, member: WorkerMember) {
        if let Some(existing) = self
            .members
            .iter_mut()
            .find(|m| m.worker_id == member.worker_id)
        {
            *existing = member;
        } else {
            self.members.push(member);
        }
    }

    /// Put member records in stable worker ID order for persistence.
    pub fn sort_members(&mut self) {
        self.members.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));
    }

    /// Require a replacement to retain every existing member ID.
    pub fn validate_retained_members(&self, current: &Self) -> Result<(), &'static str> {
        if current.members.iter().any(|old| {
            !self
                .members
                .iter()
                .any(|new| old.worker_id == new.worker_id)
        }) {
            return Err("retain all member records; retire instead of deleting");
        }
        Ok(())
    }

    /// Require ready instances to withdraw before their members can be retired.
    pub fn validate_retirements(
        &self,
        instances: &[crate::NodeStatus],
    ) -> Result<(), &'static str> {
        if self.members.iter().any(|member| {
            member.retired
                && instances
                    .iter()
                    .any(|instance| instance.node.id.0 == member.worker_id && instance.ready)
        }) {
            return Err(
                "stop worker and wait for instance withdrawal/expiration before retirement",
            );
        }
        Ok(())
    }

    /// Non-retired members in registry order.
    pub fn active_members(&self) -> impl Iterator<Item = &WorkerMember> {
        self.members.iter().filter(|member| !member.retired)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.format_version != 1 {
            return Err("unsupported member registry format".into());
        }
        let mut ids = std::collections::HashSet::new();
        // Schema/enums/tokens/vector length fit within 64 bytes. Each serving
        // member needs at most 38 bytes of bincode lengths/tags plus ID, zone,
        // and the largest legal future incarnation/address. Reserve serving
        // size even while offline so a heartbeat cannot make discovery exceed
        // the transport limit. Retired records are absent from discovery.
        let mut discovery_bytes = 64usize;
        for member in &self.members {
            if member.worker_id.trim().is_empty()
                || member.worker_id.len() > 256
                || member.zone.as_ref().is_some_and(|z| z.len() > 256)
                || !ids.insert(&member.worker_id)
            {
                return Err("invalid or duplicate member identity".into());
            }
            if !member.retired {
                discovery_bytes += 38
                    + member.worker_id.len()
                    + member.zone.as_ref().map_or(0, String::len)
                    + 2 * crate::MAX_STATUS_FIELD_BYTES;
                if discovery_bytes > crate::MAX_CONTROL_PAYLOAD_BYTES {
                    return Err("active membership exceeds worker discovery frame budget".into());
                }
            }
        }
        Ok(())
    }

    /// Deterministic equality token. Neither addresses nor liveness participate.
    pub fn topology_token(&self) -> u64 {
        let mut members: Vec<_> = self.active_members().collect();
        members.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));
        u64::from_le_bytes(
            Sha256::digest(serde_json::to_vec(&members).expect("member JSON"))[..8]
                .try_into()
                .unwrap(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstanceState {
    Offline,
    Conflict,
    Serving {
        instance_id: String,
        address: String,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveredWorker {
    pub member: WorkerMember,
    pub state: InstanceState,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerDiscovery {
    pub topology_token: u64,
    pub state_token: u64,
    /// Maximum age of this complete, authoritative observation at the client.
    pub valid_for_ms: u64,
    pub workers: Vec<DiscoveredWorker>,
}
impl WorkerDiscovery {
    pub fn retained(registry: &MemberRegistry, instances: &[crate::NodeStatus]) -> Self {
        // Scan leases once. None records a conflict permanently for this
        // observation, so a third candidate cannot accidentally restore service.
        let mut candidates = std::collections::HashMap::<&str, Option<&crate::NodeStatus>>::new();
        for instance in instances.iter().filter(|instance| {
            instance.node.role == crate::NodeRole::Worker
                && instance.ready
                && instance.health == crate::NodeHealth::Healthy
        }) {
            candidates
                .entry(instance.node.id.0.as_str())
                .and_modify(|candidate| *candidate = None)
                .or_insert(Some(instance));
        }
        let mut workers: Vec<_> = registry
            .active_members()
            .map(|member| {
                let state = match candidates.get(member.worker_id.as_str()) {
                    None => InstanceState::Offline,
                    Some(Some(instance)) => InstanceState::Serving {
                        instance_id: instance.incarnation_id.clone(),
                        address: instance.node.address.clone(),
                    },
                    Some(None) => InstanceState::Conflict,
                };
                DiscoveredWorker {
                    member: member.clone(),
                    state,
                }
            })
            .collect();
        workers.sort_by(|a, b| a.member.worker_id.cmp(&b.member.worker_id));
        let state_token = u64::from_le_bytes(
            Sha256::digest(serde_json::to_vec(&workers).unwrap())[..8]
                .try_into()
                .unwrap(),
        );
        Self {
            topology_token: registry.topology_token(),
            state_token,
            valid_for_ms: DISCOVERY_MAX_AGE_MS,
            workers,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_reserves_future_serving_bytes_and_retirement_releases_budget() {
        let mut registry = MemberRegistry::default();
        loop {
            registry.members.push(WorkerMember {
                worker_id: format!("w{}", registry.members.len()),
                zone: None,
                retired: false,
            });
            if registry.validate().is_err() {
                break;
            }
        }
        // These short records fit the persistent JSON limit but cannot all
        // serve with maximum legal incarnation/address values in one frame.
        assert!(serde_json::to_vec(&registry).unwrap().len() < 512 * 1024);
        registry.members.last_mut().unwrap().retired = true;
        registry.validate().unwrap();
        // Placement attributes also consume actual bytes, not merely a slot.
        registry.members[0].zone = Some("z".repeat(crate::MAX_STATUS_LABEL_VALUE_BYTES));
        registry.members[1].zone = Some("z".repeat(crate::MAX_STATUS_LABEL_VALUE_BYTES));
        registry.members[2].zone = Some("z".repeat(crate::MAX_STATUS_LABEL_VALUE_BYTES));
        assert!(registry.validate().is_err());
    }
    #[test]
    fn discovery_indexes_only_serving_candidates_and_preserves_conflict() {
        fn status(id: &str, incarnation: &str) -> crate::NodeStatus {
            crate::NodeStatus {
                schema_version: crate::NODE_STATUS_SCHEMA_VERSION,
                cluster_id: "c".into(),
                node: crate::NodeInfo {
                    id: crate::NodeId::new(id),
                    address: "127.0.0.1:7".into(),
                    role: crate::NodeRole::Worker,
                },
                incarnation_id: incarnation.into(),
                admin_address: None,
                build_version: "test".into(),
                started_at_unix_ms: 1,
                reported_at_unix_ms: 1,
                heartbeat_seq: 0,
                health: crate::NodeHealth::Healthy,
                ready: true,
                metrics: Default::default(),
                labels: Default::default(),
            }
        }
        let registry = MemberRegistry {
            members: ["conflict", "offline", "retired", "serving"]
                .into_iter()
                .map(|id| WorkerMember {
                    worker_id: id.into(),
                    zone: None,
                    retired: id == "retired",
                })
                .collect(),
            ..Default::default()
        };
        let mut not_ready = status("serving", "recovering");
        not_ready.ready = false;
        let mut unhealthy = status("serving", "unhealthy");
        unhealthy.health = crate::NodeHealth::Unhealthy;
        let mut coordinator = status("serving", "coordinator");
        coordinator.node.role = crate::NodeRole::Coordinator;
        let mut instances = vec![
            status("conflict", "one"),
            status("conflict", "two"),
            status("conflict", "three"),
            status("serving", "sole"),
            status("retired", "stale"),
            status("unknown", "unknown"),
            not_ready,
            unhealthy,
            coordinator,
        ];
        let view = WorkerDiscovery::retained(&registry, &instances);
        assert_eq!(view.valid_for_ms, 30_000);
        assert_eq!(view.workers.len(), 3);
        assert_eq!(view.workers[0].state, InstanceState::Conflict);
        assert_eq!(view.workers[1].state, InstanceState::Offline);
        assert!(
            matches!(&view.workers[2].state, InstanceState::Serving { instance_id, .. } if instance_id == "sole")
        );
        instances.reverse();
        assert_eq!(WorkerDiscovery::retained(&registry, &instances), view);
        // Even identical repeated candidates preserve the old conflict behavior.
        assert_eq!(
            WorkerDiscovery::retained(
                &registry,
                &[status("serving", "sole"), status("serving", "sole")]
            )
            .workers[2]
                .state,
            InstanceState::Conflict
        );
    }
}
