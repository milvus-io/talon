//! Coalesce locally received heartbeats between shared-state refreshes.
//!
//! Only an authoritatively admitted, unchanged instance can renew in memory.
//! Neither receiving a heartbeat nor rendering discovery refreshes admission.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use talon_core::worker_membership::{InstanceState, MemberRegistry, WorkerDiscovery};
use talon_core::NodeStatus;

#[derive(Clone)]
pub(super) struct Heartbeat {
    pub status: NodeStatus,
    pub ttl: Duration,
    pub received: Instant,
    persisted: Instant,
    persisted_seq: u64,
    pub serving: bool,
    admitted: bool,
}

#[derive(Default)]
pub(super) struct Heartbeats {
    entries: HashMap<(String, String), Heartbeat>,
}

impl Heartbeats {
    pub fn is_stale(&self, status: &NodeStatus) -> bool {
        self.entries
            .get(&(status.node.id.0.clone(), status.incarnation_id.clone()))
            .is_some_and(|entry| status.heartbeat_seq < entry.status.heartbeat_seq)
    }

    /// None needs authoritative admission; Err rejects a stale sequence.
    pub fn receive(&mut self, status: &NodeStatus, ttl: Duration) -> Option<Result<bool, ()>> {
        let key = (status.node.id.0.clone(), status.incarnation_id.clone());
        let entry = self.entries.get_mut(&key)?;
        if status.heartbeat_seq < entry.status.heartbeat_seq {
            return Some(Err(()));
        }
        if status.heartbeat_seq == entry.status.heartbeat_seq
            && !same_admission(&entry.status, status)
        {
            return Some(Err(()));
        }
        if !same_admission(&entry.status, status)
            || (!entry.ttl.is_zero() && entry.ttl != ttl)
            || entry.persisted.elapsed() >= ttl / 2
            || !entry.admitted
        {
            // A state change must not race with further cached approvals of
            // the old state while its authoritative publication is in flight.
            entry.admitted = false;
            return None;
        }
        entry.ttl = ttl;
        if status.heartbeat_seq > entry.status.heartbeat_seq {
            entry.status = status.clone();
            entry.received = Instant::now();
        }
        Some(Ok(entry.serving))
    }

    pub fn admit(&mut self, status: NodeStatus, ttl: Duration, received: Instant, serving: bool) {
        let key = (status.node.id.0.clone(), status.incarnation_id.clone());
        let persisted_seq = status.heartbeat_seq;
        self.entries.insert(
            key,
            Heartbeat {
                status,
                ttl,
                received,
                persisted: received,
                persisted_seq,
                serving,
                admitted: true,
            },
        );
    }

    pub fn pending(&mut self, registry: &MemberRegistry) -> Vec<Heartbeat> {
        let members: HashMap<_, _> = registry
            .members
            .iter()
            .filter(|member| !member.retired)
            .map(|member| (member.worker_id.as_str(), member.zone.as_ref()))
            .collect();
        self.entries.retain(|(id, _), entry| {
            (entry.ttl.is_zero() || entry.received.elapsed() < entry.ttl)
                && members.get(id.as_str()).is_some_and(|zone| {
                    *zone == entry.status.labels.get(talon_core::NODE_ZONE_LABEL)
                })
        });
        self.entries
            .values()
            .filter(|entry| {
                entry.admitted
                    && !entry.ttl.is_zero()
                    && entry.status.heartbeat_seq > entry.persisted_seq
                    && entry.persisted.elapsed() >= entry.ttl / 3
            })
            .cloned()
            .collect()
    }

    pub fn persisted(&mut self, flushed: &Heartbeat) {
        let key = (
            flushed.status.node.id.0.clone(),
            flushed.status.incarnation_id.clone(),
        );
        if let Some(entry) = self.entries.get_mut(&key) {
            if entry.persisted_seq > flushed.status.heartbeat_seq
                || !same_admission(&entry.status, &flushed.status)
            {
                return;
            }
            entry.persisted_seq = flushed.status.heartbeat_seq;
            // Use receipt time, not flush time: buffering must not extend the
            // life of a process which stopped sending heartbeats.
            entry.persisted = flushed.received;
        }
    }

    pub fn forget(&mut self, status: &NodeStatus) {
        let key = (status.node.id.0.clone(), status.incarnation_id.clone());
        if self
            .entries
            .get(&key)
            .is_some_and(|entry| entry.status.heartbeat_seq <= status.heartbeat_seq)
        {
            self.entries.remove(&key);
        }
    }

    pub fn install(&mut self, view: &WorkerDiscovery, instances: &[NodeStatus], observed: Instant) {
        let members: HashMap<_, _> = view
            .workers
            .iter()
            .map(|worker| {
                (
                    worker.member.worker_id.as_str(),
                    worker.member.zone.as_ref(),
                )
            })
            .collect();
        // Shared snapshots warm every Coordinator, including replicas which
        // have never received this Worker's heartbeat. Merely observing a
        // record again never renews its publication timestamp.
        for status in instances {
            if status.node.role != talon_core::NodeRole::Worker
                || !members
                    .get(status.node.id.0.as_str())
                    .is_some_and(|zone| *zone == status.labels.get(talon_core::NODE_ZONE_LABEL))
            {
                continue;
            }
            let key = (status.node.id.0.clone(), status.incarnation_id.clone());
            match self.entries.get_mut(&key) {
                Some(entry) => {
                    if status.heartbeat_seq > entry.persisted_seq {
                        entry.persisted_seq = status.heartbeat_seq;
                        entry.persisted = observed;
                    }
                    if status.heartbeat_seq > entry.status.heartbeat_seq {
                        entry.status = status.clone();
                        entry.received = observed;
                    }
                }
                None => self.admit(status.clone(), Duration::ZERO, observed, false),
            }
        }
        let present: std::collections::HashSet<_> = instances
            .iter()
            .map(|status| (status.node.id.0.as_str(), status.incarnation_id.as_str()))
            .collect();
        self.entries.retain(|(id, incarnation), entry| {
            members
                .get(id.as_str())
                .is_some_and(|zone| *zone == entry.status.labels.get(talon_core::NODE_ZONE_LABEL))
                && (present.contains(&(id.as_str(), incarnation.as_str()))
                    || (!entry.ttl.is_zero() && entry.received.elapsed() < entry.ttl))
        });
        let workers: HashMap<_, _> = view
            .workers
            .iter()
            .map(|worker| (worker.member.worker_id.as_str(), &worker.state))
            .collect();
        for ((id, incarnation), entry) in &mut self.entries {
            entry.serving = matches!(workers.get(id.as_str()),
                Some(InstanceState::Serving { instance_id, address })
                    if instance_id == incarnation && address == &entry.status.node.address);
        }
    }
}

fn same_admission(previous: &NodeStatus, next: &NodeStatus) -> bool {
    previous.schema_version == next.schema_version
        && previous.cluster_id == next.cluster_id
        && previous.node == next.node
        && previous.incarnation_id == next.incarnation_id
        && previous.admin_address == next.admin_address
        && previous.build_version == next.build_version
        && previous.started_at_unix_ms == next.started_at_unix_ms
        && previous.health == next.health
        && previous.ready == next.ready
        && previous.labels == next.labels
}

#[cfg(test)]
mod tests {
    use super::super::state::worker_status;
    use super::*;
    use talon_core::worker_membership::WorkerMember;

    fn registry(status: &NodeStatus) -> MemberRegistry {
        MemberRegistry {
            members: vec![WorkerMember {
                worker_id: status.node.id.0.clone(),
                zone: None,
                retired: false,
            }],
            ..Default::default()
        }
    }

    #[test]
    fn flush_completion_preserves_a_newer_buffered_report() {
        let ttl = Duration::from_secs(30);
        let mut status = worker_status();
        let registry = registry(&status);
        let mut cache = Heartbeats::default();
        cache.admit(
            status.clone(),
            ttl,
            Instant::now() - Duration::from_secs(11),
            true,
        );
        status.heartbeat_seq = 1;
        assert_eq!(cache.receive(&status, ttl), Some(Ok(true)));
        let flushed = cache.pending(&registry).pop().unwrap();
        status.heartbeat_seq = 2;
        status.metrics.block_count = 42;
        assert_eq!(cache.receive(&status, ttl), Some(Ok(true)));
        cache.persisted(&flushed);
        let entry = cache.entries.values_mut().next().unwrap();
        assert_eq!(entry.persisted_seq, 1);
        assert_eq!(entry.status.heartbeat_seq, 2);
        entry.persisted -= Duration::from_secs(11);
        let pending = cache.pending(&registry);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].status.metrics.block_count, 42);
    }

    #[test]
    fn duplicate_reports_never_extend_receipt_or_publication_time() {
        let ttl = Duration::from_secs(30);
        let status = worker_status();
        let received = Instant::now() - Duration::from_secs(11);
        let mut cache = Heartbeats::default();
        cache.admit(status.clone(), ttl, received, true);
        assert_eq!(cache.receive(&status, ttl), Some(Ok(true)));
        let entry = cache.entries.values().next().unwrap();
        assert_eq!(entry.received, received);
        assert_eq!(entry.persisted, received);
        assert!(cache.pending(&registry(&status)).is_empty());
        let mut changed = status;
        changed.ready = false;
        assert_eq!(cache.receive(&changed, ttl), Some(Err(())));
    }

    #[test]
    fn expired_retired_and_rezoned_reports_are_not_flushed() {
        let ttl = Duration::from_secs(30);
        let status = worker_status();
        for case in 0..3 {
            let mut cache = Heartbeats::default();
            cache.admit(status.clone(), ttl, Instant::now(), true);
            let entry = cache.entries.values_mut().next().unwrap();
            entry.status.heartbeat_seq += 1;
            entry.persisted -= Duration::from_secs(11);
            let mut registry = registry(&status);
            match case {
                0 => entry.received -= ttl,
                1 => registry.members[0].retired = true,
                _ => registry.members[0].zone = Some("changed".into()),
            }
            assert!(cache.pending(&registry).is_empty());
            assert!(cache.entries.is_empty());
        }
    }

    #[test]
    fn local_heartbeats_cannot_extend_authoritative_admission() {
        let ttl = Duration::from_secs(30);
        let mut status = worker_status();
        let mut cache = Heartbeats::default();
        cache.admit(status.clone(), ttl, Instant::now() - ttl / 2, true);
        status.heartbeat_seq += 1;
        assert_eq!(cache.receive(&status, ttl), None);
        assert!(!cache.entries.values().next().unwrap().admitted);
    }

    #[test]
    fn pending_admission_preserves_the_highest_accepted_sequence() {
        let ttl = Duration::from_secs(30);
        let mut status = worker_status();
        let mut cache = Heartbeats::default();
        cache.admit(status.clone(), ttl, Instant::now(), true);
        status.heartbeat_seq = 100;
        assert_eq!(cache.receive(&status, ttl), Some(Ok(true)));
        let mut changed = status.clone();
        changed.heartbeat_seq += 1;
        changed.ready = false;
        assert_eq!(cache.receive(&changed, ttl), None);
        status.heartbeat_seq = 99;
        assert!(cache.is_stale(&status));
        assert!(cache.pending(&registry(&status)).is_empty());
    }

    #[test]
    fn old_flush_completion_cannot_replace_a_new_admission() {
        let ttl = Duration::from_secs(30);
        let mut status = worker_status();
        let mut cache = Heartbeats::default();
        cache.admit(
            status.clone(),
            ttl,
            Instant::now() - Duration::from_secs(11),
            true,
        );
        status.heartbeat_seq = 1;
        cache.receive(&status, ttl).unwrap().unwrap();
        let old = cache.pending(&registry(&status)).pop().unwrap();
        status.heartbeat_seq = 2;
        status.ready = false;
        cache.admit(status.clone(), ttl, Instant::now(), false);
        cache.persisted(&old);
        cache.forget(&old.status);
        let entry = cache.entries.values().next().unwrap();
        assert_eq!(entry.persisted_seq, 2);
        assert_eq!(entry.status, status);
        assert!(!entry.serving);
    }

    #[test]
    fn observing_the_same_shared_report_does_not_extend_admission() {
        let ttl = Duration::from_secs(30);
        let mut status = worker_status();
        let registry = registry(&status);
        let instances = vec![status.clone()];
        let view = WorkerDiscovery::retained(&registry, &instances);
        let mut cache = Heartbeats::default();
        cache.install(&view, &instances, Instant::now() - Duration::from_secs(16));
        cache.install(&view, &instances, Instant::now());
        status.heartbeat_seq += 1;
        assert_eq!(cache.receive(&status, ttl), None);
    }
}
