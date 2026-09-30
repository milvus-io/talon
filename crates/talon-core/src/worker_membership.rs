//! Persistent logical membership, independent of process leases.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipMode {
    #[default]
    Legacy,
    Retained,
}

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
    pub mode: MembershipMode,
    pub members: Vec<WorkerMember>,
}

impl Default for MemberRegistry {
    fn default() -> Self {
        Self {
            format_version: 1,
            mode: MembershipMode::Legacy,
            members: Vec::new(),
        }
    }
}

impl MemberRegistry {
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
        let mut members: Vec<_> = self.members.iter().filter(|m| !m.retired).collect();
        members.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));
        u64::from_le_bytes(
            Sha256::digest(serde_json::to_vec(&members).expect("member JSON"))[..8]
                .try_into()
                .unwrap(),
        )
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
}
