//! Last-good worker membership used for client-side block placement.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};

use talon_core::{cache_membership_epoch, CachePlacementTable, NodeInfo};
use talon_transport::ZonedNodeInfo;

use crate::lock::RwLockExt;

/// One immutable membership view and its equality-only content token.
#[derive(Debug, Clone)]
pub struct MembershipSnapshot {
    /// Prebuilt O(1) placement index for the advertised healthy workers.
    ///
    /// With zone affinity active this is built from the same-zone subset, so
    /// every ranking downstream stays inside the reader's zone (ADR 0006).
    pub placement: Arc<CachePlacementTable>,
    /// Content-derived token used to invalidate per-block placements.
    pub epoch: u64,
    /// Worker zones by dialable address, for read classification.
    pub zones_by_address: Arc<HashMap<String, String>>,
    /// Zone affinity was requested but no same-zone worker exists, so
    /// `placement` covers the full membership instead.
    pub affinity_fallback: bool,
    /// Present only in retained mode. Missing IDs are offline or conflicted.
    pub instances: Option<Arc<HashMap<String, (String, crate::WorkerClient)>>>,
    pub valid_until: Option<std::time::Instant>,
}

struct Entry {
    snapshot: MembershipSnapshot,
    refreshed_ms: u64,
    /// Equality token over the reported zones, so a zone arriving later for an
    /// unchanged node set still rebuilds the (possibly filtered) table.
    zones_token: u64,
}

/// Short-TTL membership cache that retains stale data across refresh errors.
pub struct MembershipCache {
    ttl_ms: u64,
    /// The reader's own zone, when known.
    zone: Option<String>,
    /// Whether same-zone placement filtering is enabled (default off).
    zone_affinity: bool,
    entry: RwLock<Option<Entry>>,
}

impl MembershipCache {
    /// Create an empty cache with zone affinity disabled.
    pub fn new(ttl_ms: u64) -> Self {
        Self {
            ttl_ms,
            zone: None,
            zone_affinity: false,
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

    /// Return the snapshot only while its refresh TTL is current.
    pub fn fresh(&self, now_ms: u64) -> Option<MembershipSnapshot> {
        self.entry.read_recover().as_ref().and_then(|entry| {
            (entry.snapshot.valid_until.map_or_else(
                || now_ms.saturating_sub(entry.refreshed_ms) <= self.ttl_ms,
                |until| std::time::Instant::now() < until,
            ))
            .then(|| entry.snapshot.clone())
        })
    }

    /// Return the last successful snapshot regardless of age.
    pub fn last_good(&self) -> Option<MembershipSnapshot> {
        self.entry
            .read_recover()
            .as_ref()
            .map(|entry| entry.snapshot.clone())
    }

    /// Store a successful refresh and return whether membership changed.
    pub fn replace(&self, members: Vec<ZonedNodeInfo>, now_ms: u64) -> (MembershipSnapshot, bool) {
        let nodes: Vec<NodeInfo> = members.iter().map(|member| member.info.clone()).collect();
        let epoch = cache_membership_epoch(&nodes);
        let zones_token = zones_token(&members);
        let mut entry = self.entry.write_recover();
        if let Some(current) = entry.as_mut() {
            if current.snapshot.instances.is_none()
                && current.snapshot.epoch == epoch
                && current.zones_token == zones_token
            {
                current.refreshed_ms = now_ms;
                return (current.snapshot.clone(), false);
            }
        }
        let (table_nodes, affinity_fallback) = match (&self.zone, self.zone_affinity) {
            (Some(zone), true) => {
                let local: Vec<NodeInfo> = members
                    .iter()
                    .filter(|member| member.zone.as_deref() == Some(zone.as_str()))
                    .map(|member| member.info.clone())
                    .collect();
                if local.is_empty() {
                    (nodes.clone(), !nodes.is_empty())
                } else {
                    (local, false)
                }
            }
            _ => (nodes.clone(), false),
        };
        let zones_by_address: HashMap<String, String> = members
            .iter()
            .filter_map(|member| {
                member
                    .zone
                    .clone()
                    .map(|zone| (member.info.address.clone(), zone))
            })
            .collect();
        let snapshot = MembershipSnapshot {
            instances: None,
            valid_until: None,
            epoch,
            placement: Arc::new(CachePlacementTable::new(&table_nodes)),
            zones_by_address: Arc::new(zones_by_address),
            affinity_fallback,
        };
        let changed = entry.is_some();
        *entry = Some(Entry {
            snapshot: snapshot.clone(),
            refreshed_ms: now_ms,
            zones_token,
        });
        (snapshot, changed)
    }
    pub fn replace_retained(
        &self,
        view: talon_core::worker_membership::WorkerDiscovery,
        observed: std::time::Instant,
        pool: &crate::ConnectionPool,
    ) -> MembershipSnapshot {
        use talon_core::worker_membership::InstanceState;
        let mut entry = self.entry.write_recover();
        let old = entry.as_ref().map(|e| &e.snapshot);
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
        let affinity_fallback = self.zone_affinity
            && self.zone.is_some()
            && local.is_empty()
            && !view.workers.is_empty();
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
            Some(old) if old.instances.is_some() && old.epoch == view.topology_token => {
                old.placement.clone()
            }
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
                let existing = old
                    .and_then(|s| s.instances.as_ref())
                    .and_then(|i| i.get(&worker.member.worker_id));
                let client = match existing {
                    Some((id, client)) if id == instance_id && client.addr() == address => {
                        client.clone()
                    }
                    _ => crate::WorkerClient::with_pool(address.clone(), Arc::new(pool.isolated()))
                        .without_read_retry(),
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
            epoch: view.topology_token,
            zones_by_address: Arc::new(zones),
            affinity_fallback,
            instances: Some(Arc::new(instances)),
            valid_until: Some(
                observed
                    + std::time::Duration::from_millis(
                        view.valid_for_ms.min(self.ttl_ms.max(1)).min(500),
                    ),
            ),
        };
        *entry = Some(Entry {
            snapshot: snapshot.clone(),
            refreshed_ms: 0,
            zones_token: 0,
        });
        snapshot
    }
}

impl MembershipSnapshot {
    pub fn retained_owner(
        &self,
        block: &talon_core::BlockId,
    ) -> Result<Option<crate::WorkerClient>, crate::BlockReadError> {
        let Some(instances) = &self.instances else {
            return Ok(None);
        };
        let owner = self
            .placement
            .primary(block)
            .ok_or(crate::BlockReadError::NoOwners)?;
        if self
            .valid_until
            .map_or(true, |until| std::time::Instant::now() >= until)
        {
            return Err(crate::BlockReadError::Worker(crate::WorkerError::Remote(
                talon_transport::DataPlaneError {
                    code: talon_transport::DataErrorCode::Unavailable,
                    message: format!("worker {} has expired instance discovery", owner.id),
                },
            )));
        }
        instances
            .get(&owner.id.0)
            .map(|(_, client)| Some(client.clone()))
            .ok_or_else(|| {
                crate::BlockReadError::Worker(crate::WorkerError::Remote(
                    talon_transport::DataPlaneError {
                        code: talon_transport::DataErrorCode::Unavailable,
                        message: format!(
                            "worker {} is offline or has conflicting instances",
                            owner.id
                        ),
                    },
                ))
            })
    }
}

/// Order-independent equality token over `(address, zone)` pairs.
///
/// Process-local only (never on the wire), so the std hasher is fine.
fn zones_token(members: &[ZonedNodeInfo]) -> u64 {
    let mut pairs: Vec<(&str, Option<&str>)> = members
        .iter()
        .map(|member| (member.info.address.as_str(), member.zone.as_deref()))
        .collect();
    pairs.sort_unstable();
    let mut hasher = DefaultHasher::new();
    pairs.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{NodeId, NodeRole};

    fn worker(address: &str) -> ZonedNodeInfo {
        zoned("worker-a", address, None)
    }

    fn zoned(id: &str, address: &str, zone: Option<&str>) -> ZonedNodeInfo {
        ZonedNodeInfo {
            info: NodeInfo {
                id: NodeId::new(id),
                address: address.into(),
                role: NodeRole::Worker,
            },
            zone: zone.map(str::to_string),
        }
    }

    #[test]
    fn expiry_retains_a_last_good_snapshot() {
        let cache = MembershipCache::new(100);
        cache.replace(vec![worker("old:7001")], 10);
        assert!(cache.fresh(110).is_some());
        assert!(cache.fresh(111).is_none());
        assert_eq!(
            cache.last_good().unwrap().placement.workers()[0].address,
            "old:7001"
        );
    }

    #[test]
    fn address_change_advances_the_equality_token() {
        let cache = MembershipCache::new(100);
        let first = cache.replace(vec![worker("old:7001")], 0).0;
        let unchanged = cache.replace(vec![worker("old:7001")], 1).0;
        assert!(Arc::ptr_eq(&first.placement, &unchanged.placement));
        assert!(cache.replace(vec![worker("new:7001")], 2).1);
    }

    #[test]
    fn affinity_builds_the_table_from_the_same_zone_subset() {
        let cache = MembershipCache::new(100).with_zone_affinity(Some("az-a".into()), true);
        let (snapshot, _) = cache.replace(
            vec![
                zoned("w1", "10.0.0.1:7001", Some("az-a")),
                zoned("w2", "10.0.0.2:7001", Some("az-b")),
                zoned("w3", "10.0.0.3:7001", None),
            ],
            0,
        );
        let addresses: Vec<&str> = snapshot
            .placement
            .workers()
            .iter()
            .map(|worker| worker.address.as_str())
            .collect();
        assert_eq!(addresses, vec!["10.0.0.1:7001"]);
        assert!(!snapshot.affinity_fallback);
        assert_eq!(
            snapshot.zones_by_address.get("10.0.0.2:7001").unwrap(),
            "az-b"
        );
    }

    #[test]
    fn empty_local_subset_falls_back_to_the_full_membership() {
        let cache = MembershipCache::new(100).with_zone_affinity(Some("az-c".into()), true);
        let (snapshot, _) = cache.replace(
            vec![
                zoned("w1", "10.0.0.1:7001", Some("az-a")),
                zoned("w2", "10.0.0.2:7001", Some("az-b")),
            ],
            0,
        );
        assert_eq!(snapshot.placement.workers().len(), 2);
        assert!(snapshot.affinity_fallback);
    }

    #[test]
    fn disabled_affinity_and_unknown_self_zone_use_the_full_set() {
        let mixed = vec![
            zoned("w1", "10.0.0.1:7001", Some("az-a")),
            zoned("w2", "10.0.0.2:7001", Some("az-b")),
        ];
        let off = MembershipCache::new(100).with_zone_affinity(Some("az-a".into()), false);
        assert_eq!(off.replace(mixed.clone(), 0).0.placement.workers().len(), 2);
        let unknown = MembershipCache::new(100).with_zone_affinity(None, true);
        let (snapshot, _) = unknown.replace(mixed, 0);
        assert_eq!(snapshot.placement.workers().len(), 2);
        assert!(!snapshot.affinity_fallback);
    }

    #[test]
    fn a_zone_arriving_for_an_unchanged_node_set_rebuilds_the_table() {
        let cache = MembershipCache::new(100).with_zone_affinity(Some("az-a".into()), true);
        let (first, _) = cache.replace(
            vec![
                zoned("w1", "10.0.0.1:7001", None),
                zoned("w2", "10.0.0.2:7001", None),
            ],
            0,
        );
        // No zones known yet: full table, no fallback flag.
        assert_eq!(first.placement.workers().len(), 2);

        // Same node set, zones now reported: the filtered table must appear
        // and the caller must see a change so stale placements are dropped.
        let (second, changed) = cache.replace(
            vec![
                zoned("w1", "10.0.0.1:7001", Some("az-a")),
                zoned("w2", "10.0.0.2:7001", Some("az-b")),
            ],
            1,
        );
        assert!(changed);
        assert_eq!(second.placement.workers().len(), 1);
        assert_eq!(second.epoch, first.epoch);
    }
    #[test]
    fn retained_topology_survives_offline_address_and_instance_changes() {
        use talon_core::worker_membership::*;
        let cache = MembershipCache::new(30_000).with_zone_affinity(Some("a".into()), true);
        let pool = crate::ConnectionPool::new();
        let mut view = WorkerDiscovery {
            mode: MembershipMode::Retained,
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
        let first = cache.replace_retained(view.clone(), std::time::Instant::now(), &pool);
        let block = talon_core::BlockId::new(
            talon_core::ObjectId::new(talon_core::Backend::S3, "b", "key"),
            0,
            64,
            talon_core::Version::new("v"),
        );
        assert_eq!(
            first.retained_owner(&block).unwrap().unwrap().addr(),
            "127.0.0.1:1"
        );
        view.workers[0].state = InstanceState::Offline;
        let offline = cache.replace_retained(view.clone(), std::time::Instant::now(), &pool);
        assert!(Arc::ptr_eq(&first.placement, &offline.placement));
        assert!(offline.retained_owner(&block).is_err());
        assert!(!offline.affinity_fallback);
        view.workers[0].state = InstanceState::Serving {
            instance_id: "new".into(),
            address: "127.0.0.1:3".into(),
        };
        let changed = cache.replace_retained(view.clone(), std::time::Instant::now(), &pool);
        assert!(Arc::ptr_eq(&first.placement, &changed.placement));
        assert_eq!(
            changed.retained_owner(&block).unwrap().unwrap().addr(),
            "127.0.0.1:3"
        );
        let expired = cache.replace_retained(
            view,
            std::time::Instant::now() - std::time::Duration::from_secs(1),
            &pool,
        );
        assert!(expired.retained_owner(&block).is_err());
        assert!(cache.fresh(0).is_none());
        assert!(cache.last_good().is_some());
    }
}
