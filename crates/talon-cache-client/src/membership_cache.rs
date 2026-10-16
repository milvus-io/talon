//! Last-good worker membership used for client-side block placement.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use talon_core::{CachePlacementTable, NodeInfo};

use crate::lock::RwLockExt;

/// One immutable membership view and its equality-only content token.
#[derive(Debug, Clone)]
pub struct MembershipSnapshot {
    /// Prebuilt O(1) placement index for the persistent logical workers.
    ///
    /// With zone affinity active this is built from the same-zone subset, so
    /// every ranking downstream stays inside the reader's zone (ADR 0006).
    pub placement: Arc<CachePlacementTable>,
    /// Optional read-only fallback over members outside the reader's zone.
    /// LOAD always uses `placement` and never remaps an unavailable primary.
    pub fallback_placement: Option<Arc<CachePlacementTable>>,
    /// Equality-only topology token, independent of instance availability.
    pub epoch: u64,
    /// Worker zones by dialable address, for read classification.
    pub zones_by_address: Arc<HashMap<String, String>>,
    /// Zone affinity was requested but no same-zone worker exists, so
    /// `placement` covers the full membership instead.
    pub affinity_fallback: bool,
    /// Serving instances keyed by logical worker ID. Missing IDs are offline or conflicted.
    pub instances: Arc<HashMap<String, (String, crate::WorkerClient)>>,
    /// Refresh scheduling hint only; requests use published availability.
    pub valid_until: std::time::Instant,
}

/// Short-TTL membership cache that retains stale data across refresh errors.
pub struct MembershipCache {
    ttl_ms: u64,
    /// The reader's own zone, when known.
    zone: Option<String>,
    /// Whether same-zone placement filtering is enabled (default off).
    zone_affinity: bool,
    zone_fallback: bool,
    entry: RwLock<Option<MembershipSnapshot>>,
}

impl MembershipCache {
    /// Create an empty cache with zone affinity disabled.
    pub fn new(ttl_ms: u64) -> Self {
        Self {
            ttl_ms,
            zone: None,
            zone_affinity: false,
            zone_fallback: false,
            entry: RwLock::new(None),
        }
    }

    /// Configure zone-affine placement (ADR 0006). With `enabled` and a known
    /// `zone`, placement tables are built from the same-zone worker subset;
    /// an empty subset falls back to the full membership.
    pub fn with_zone_affinity(mut self, zone: Option<String>, enabled: bool) -> Self {
        self.zone = zone;
        self.zone_affinity = enabled;
        self
    }

    /// Allow reads to try nonlocal candidates after local candidates fail.
    /// Has no effect unless affinity selects a nonempty local member set.
    pub fn with_zone_fallback(mut self, enabled: bool) -> Self {
        self.zone_fallback = enabled;
        self
    }

    pub(crate) fn empty_with_same_policy(&self) -> Self {
        Self::new(self.ttl_ms)
            .with_zone_affinity(self.zone.clone(), self.zone_affinity)
            .with_zone_fallback(self.zone_fallback)
    }

    /// Return the snapshot only while its refresh TTL is current.
    pub fn fresh(&self) -> Option<MembershipSnapshot> {
        self.entry.read_recover().as_ref().and_then(|snapshot| {
            (std::time::Instant::now() < snapshot.valid_until).then(|| snapshot.clone())
        })
    }

    /// Return the last successful snapshot regardless of age.
    pub fn last_good(&self) -> Option<MembershipSnapshot> {
        self.entry.read_recover().as_ref().cloned()
    }

    /// Install bounded instance discovery while reusing unchanged logical placement.
    pub fn replace(
        &self,
        view: talon_core::worker_membership::WorkerDiscovery,
        observed: std::time::Instant,
        pool: &crate::ConnectionPool,
    ) -> MembershipSnapshot {
        use talon_core::worker_membership::InstanceState;
        // Build the next view without blocking foreground snapshot readers.
        // Discovery has a single background publisher.
        let previous = self.last_good();
        let old = previous.as_ref();
        let local: Vec<_> = view
            .workers
            .iter()
            .filter(|w| {
                self.zone_affinity
                    && self
                        .zone
                        .as_ref()
                        .is_some_and(|z| Some(z) == w.member.zone.as_ref())
            })
            .collect();
        let affinity_fallback = self.zone_affinity && local.is_empty() && !view.workers.is_empty();
        let fallback_placement = if self.zone_fallback && !local.is_empty() {
            match old {
                Some(old) if old.epoch == view.topology_token => old.fallback_placement.clone(),
                _ => {
                    let remote: Vec<_> = view
                        .workers
                        .iter()
                        .filter(|w| w.member.zone != self.zone)
                        .map(|w| NodeInfo {
                            id: talon_core::NodeId::new(w.member.worker_id.clone()),
                            address: String::new(),
                            role: talon_core::NodeRole::Worker,
                        })
                        .collect();
                    (!remote.is_empty()).then(|| Arc::new(CachePlacementTable::new(&remote)))
                }
            }
        } else {
            None
        };
        let selected: Vec<_> = if local.is_empty() {
            view.workers.iter().collect()
        } else {
            local
        };
        let nodes: Vec<_> = selected
            .iter()
            .map(|w| NodeInfo {
                id: talon_core::NodeId::new(w.member.worker_id.clone()),
                address: String::new(),
                role: talon_core::NodeRole::Worker,
            })
            .collect();
        let placement = match old {
            Some(old) if old.epoch == view.topology_token => old.placement.clone(),
            _ => Arc::new(CachePlacementTable::new(&nodes)),
        };
        let mut instances = HashMap::new();
        let mut zones = HashMap::new();
        for worker in &view.workers {
            if let InstanceState::Serving {
                instance_id,
                address,
            } = &worker.state
            {
                let existing = old.and_then(|s| s.instances.get(&worker.member.worker_id));
                let client = match existing {
                    Some((id, client)) if id == instance_id && client.addr() == address => {
                        client.clone()
                    }
                    _ => crate::WorkerClient::with_pool(address.clone(), Arc::new(pool.isolated())),
                };
                instances.insert(
                    worker.member.worker_id.clone(),
                    (instance_id.clone(), client),
                );
                if let Some(zone) = &worker.member.zone {
                    zones.insert(address.clone(), zone.clone());
                }
            }
        }
        let snapshot = MembershipSnapshot {
            placement,
            fallback_placement,
            epoch: view.topology_token,
            zones_by_address: Arc::new(zones),
            affinity_fallback,
            instances: Arc::new(instances),
            valid_until: observed
                + std::time::Duration::from_millis(
                    view.valid_for_ms
                        .min(self.ttl_ms.max(1))
                        .min(talon_core::worker_membership::DISCOVERY_MAX_AGE_MS),
                ),
        };
        *self.entry.write_recover() = Some(snapshot.clone());
        snapshot
    }
}

impl MembershipSnapshot {
    /// Rank logical candidates before resolving their published availability.
    pub(crate) fn candidates<'a>(
        &'a self,
        block: &'a talon_core::BlockId,
        replicas_k: u8,
    ) -> impl Iterator<
        Item = (
            &'a str,
            Result<crate::WorkerClient, crate::WorkerError>,
            bool,
        ),
    > + 'a {
        self.placement
            .rank(block, usize::from(replicas_k.max(1)))
            .into_iter()
            .map(|owner| (owner.id.0.as_str(), self.instance(&owner.id.0), false))
            .chain(self.fallback_placement.iter().flat_map(move |table| {
                table
                    .rank(block, usize::from(replicas_k.max(1)))
                    .into_iter()
                    .map(|owner| (owner.id.0.as_str(), self.instance(&owner.id.0), true))
            }))
    }

    pub(crate) fn target_error(
        &self,
        worker_id: &str,
        source: crate::WorkerError,
    ) -> crate::BlockReadError {
        let (instance_id, client) = self.instances.get(worker_id).expect("selected instance");
        crate::BlockReadError::Target {
            worker_id: worker_id.into(),
            instance_id: instance_id.clone(),
            address: client.addr().into(),
            source,
        }
    }

    pub(crate) fn replica_error(
        &self,
        worker_id: &str,
        source: crate::WorkerError,
    ) -> crate::BlockReadError {
        let instance = self.instances.get(worker_id);
        crate::BlockReadError::AllReplicasFailed {
            worker: instance
                .map_or(worker_id, |(_, client)| client.addr())
                .into(),
            worker_id: worker_id.into(),
            instance_id: instance.map(|(id, _)| id.clone()),
            source,
        }
    }

    pub fn owner(
        &self,
        block: &talon_core::BlockId,
    ) -> Result<crate::WorkerClient, crate::BlockReadError> {
        let owner = self
            .placement
            .primary(block)
            .ok_or(crate::BlockReadError::NoOwners)?;
        self.instance(&owner.id.0).map_err(Into::into)
    }

    fn instance(&self, worker_id: &str) -> Result<crate::WorkerClient, crate::WorkerError> {
        self.instances
            .get(worker_id)
            .map(|(_, client)| client.clone())
            .ok_or_else(|| {
                crate::WorkerError::Remote(talon_transport::DataPlaneError {
                    code: talon_transport::DataErrorCode::Unavailable,
                    message: format!("worker {worker_id} is offline or has conflicting instances"),
                })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};
    use talon_core::worker_membership::*;

    fn view(zones: &[Option<&str>]) -> WorkerDiscovery {
        let members = zones
            .iter()
            .enumerate()
            .map(|(i, zone)| WorkerMember {
                worker_id: format!("w{i}"),
                zone: zone.map(str::to_owned),
                retired: false,
            })
            .collect::<Vec<_>>();
        let registry = MemberRegistry {
            format_version: 1,
            members,
        };
        WorkerDiscovery {
            topology_token: registry.topology_token(),
            state_token: 1,
            valid_for_ms: 500,
            workers: registry
                .members
                .into_iter()
                .map(|member| DiscoveredWorker {
                    state: InstanceState::Serving {
                        instance_id: format!("{}-instance", member.worker_id),
                        address: format!("{}:7001", member.worker_id),
                    },
                    member,
                })
                .collect(),
        }
    }

    #[test]
    fn discovery_lifetime_respects_server_client_and_thirty_second_bounds() {
        let pool = crate::ConnectionPool::new();
        let observed = Instant::now();
        for (advertised, configured, expected) in [
            (30_000, 30_000, 30_000),
            (60_000, 60_000, 30_000),
            (50, 30_000, 50),
            (30_000, 5_000, 5_000),
            (30_000, 0, 1),
            (0, 30_000, 0),
        ] {
            let cache = MembershipCache::new(configured);
            let mut discovery = view(&[None]);
            discovery.valid_for_ms = advertised;
            let snapshot = cache.replace(discovery, observed, &pool);
            assert_eq!(
                snapshot.valid_until,
                observed + Duration::from_millis(expected)
            );
        }
        let cache = MembershipCache::new(30_000);
        let mut discovery = view(&[None]);
        discovery.valid_for_ms = 30_000;
        cache.replace(discovery.clone(), observed - Duration::from_secs(1), &pool);
        assert!(
            cache.fresh().is_some(),
            "discovery survives the old 500ms limit"
        );
        cache.replace(discovery, observed - Duration::from_secs(31), &pool);
        assert!(cache.fresh().is_none());
    }

    #[test]
    fn expiry_marks_refresh_due_but_retains_published_snapshot() {
        let cache = MembershipCache::new(100);
        let pool = crate::ConnectionPool::new();
        let observed = Instant::now();
        let snapshot = cache.replace(view(&[None]), observed, &pool);
        assert_eq!(snapshot.valid_until, observed + Duration::from_millis(100));
        assert!(cache.fresh().is_some());
        cache.replace(view(&[None]), observed - Duration::from_secs(1), &pool);
        assert!(cache.fresh().is_none());
        assert!(cache.last_good().is_some());
    }

    #[test]
    fn affinity_tracks_logical_zones_and_falls_back_only_without_local_members() {
        let pool = crate::ConnectionPool::new();
        let cache = MembershipCache::new(500).with_zone_affinity(Some("a".into()), true);
        let first = cache.replace(view(&[None, None]), Instant::now(), &pool);
        assert_eq!(first.placement.workers().len(), 2);
        assert!(first.affinity_fallback);
        let second = cache.replace(view(&[Some("a"), Some("b")]), Instant::now(), &pool);
        assert_ne!(first.epoch, second.epoch);
        assert_eq!(second.placement.workers().len(), 1);
        assert_eq!(second.placement.workers()[0].id.0, "w0");
        assert!(!second.affinity_fallback);
        assert_eq!(second.zones_by_address.get("w1:7001").unwrap(), "b");
        for (zone, enabled) in [(Some("a".into()), false), (None, true)] {
            let cache = MembershipCache::new(500).with_zone_affinity(zone, enabled);
            let snapshot = cache.replace(view(&[Some("a"), Some("b")]), Instant::now(), &pool);
            assert_eq!(snapshot.placement.workers().len(), 2);
            assert_eq!(snapshot.affinity_fallback, enabled);
        }
    }

    #[test]
    fn cross_zone_candidates_preserve_load_ownership_and_recover_locally() {
        let pool = crate::ConnectionPool::new();
        let cache = MembershipCache::new(500)
            .with_zone_affinity(Some("a".into()), true)
            .with_zone_fallback(true);
        let mut discovery = view(&[Some("a"), Some("b")]);
        let block = talon_core::BlockId::new(
            talon_core::ObjectId::new(talon_core::Backend::S3, "bucket", "key"),
            0,
            64,
            talon_core::Version::new("v"),
        );
        let first = cache.replace(discovery.clone(), Instant::now(), &pool);
        for state in [InstanceState::Offline, InstanceState::Conflict] {
            discovery.workers[0].state = state;
            let snapshot = cache.replace(discovery.clone(), Instant::now(), &pool);
            assert!(Arc::ptr_eq(&first.placement, &snapshot.placement));
            assert!(
                snapshot.owner(&block).is_err(),
                "LOAD cannot remap the primary"
            );
            let candidates: Vec<_> = snapshot.candidates(&block, 1).collect();
            assert_eq!(candidates.len(), 2);
            assert_eq!(candidates[0].0, "w0");
            assert!(candidates[0].1.is_err());
            assert!(!candidates[0].2);
            assert_eq!(candidates[1].0, "w1");
            assert!(candidates[1].1.is_ok());
            assert!(candidates[1].2);
        }
        let recovered = cache.replace(view(&[Some("a"), Some("b")]), Instant::now(), &pool);
        assert!(Arc::ptr_eq(&first.placement, &recovered.placement));
        assert_eq!(recovered.owner(&block).unwrap().addr(), "w0:7001");
        let no_affinity = MembershipCache::new(500).with_zone_fallback(true).replace(
            discovery,
            Instant::now(),
            &pool,
        );
        assert!(no_affinity.fallback_placement.is_none());
    }

    #[test]
    fn logical_topology_survives_offline_address_and_instance_changes() {
        use talon_core::worker_membership::*;
        let cache = MembershipCache::new(30_000).with_zone_affinity(Some("a".into()), true);
        let pool = crate::ConnectionPool::new();
        let mut view = WorkerDiscovery {
            topology_token: 7,
            state_token: 1,
            valid_for_ms: 500,
            workers: vec![
                DiscoveredWorker {
                    member: WorkerMember {
                        worker_id: "local".into(),
                        zone: Some("a".into()),
                        retired: false,
                    },
                    state: InstanceState::Serving {
                        instance_id: "old".into(),
                        address: "127.0.0.1:1".into(),
                    },
                },
                DiscoveredWorker {
                    member: WorkerMember {
                        worker_id: "remote".into(),
                        zone: Some("b".into()),
                        retired: false,
                    },
                    state: InstanceState::Serving {
                        instance_id: "remote".into(),
                        address: "127.0.0.1:2".into(),
                    },
                },
            ],
        };
        let first = cache.replace(view.clone(), std::time::Instant::now(), &pool);
        let block = talon_core::BlockId::new(
            talon_core::ObjectId::new(talon_core::Backend::S3, "b", "key"),
            0,
            64,
            talon_core::Version::new("v"),
        );
        assert_eq!(first.owner(&block).unwrap().addr(), "127.0.0.1:1");
        view.workers[0].state = InstanceState::Offline;
        let offline = cache.replace(view.clone(), std::time::Instant::now(), &pool);
        assert!(Arc::ptr_eq(&first.placement, &offline.placement));
        assert!(offline.owner(&block).is_err());
        assert!(!offline.affinity_fallback);
        view.workers[0].state = InstanceState::Conflict;
        let conflict = cache.replace(view.clone(), std::time::Instant::now(), &pool);
        assert!(Arc::ptr_eq(&first.placement, &conflict.placement));
        assert!(conflict.owner(&block).is_err());
        view.workers[0].state = InstanceState::Serving {
            instance_id: "new".into(),
            address: "127.0.0.1:3".into(),
        };
        let changed = cache.replace(view.clone(), std::time::Instant::now(), &pool);
        assert!(Arc::ptr_eq(&first.placement, &changed.placement));
        assert_eq!(changed.owner(&block).unwrap().addr(), "127.0.0.1:3");
        let expired = cache.replace(
            view,
            std::time::Instant::now() - std::time::Duration::from_secs(1),
            &pool,
        );
        assert_eq!(expired.owner(&block).unwrap().addr(), "127.0.0.1:3");
        assert!(cache.fresh().is_none());
        assert!(cache.last_good().is_some());
    }
}
