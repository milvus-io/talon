//! Cluster membership tracking.
//!
//! [`Membership`] is the authoritative in-memory node registry consulted by
//! placement. Its contents are driven by a [`MembershipSource`] — in production
//! a Kubernetes watch/poll ([`KubernetesMembership`]), in tests a mock. On each
//! poll the source produces the desired node set and [`Membership::reconcile`]
//! applies the diff.
//!
//! # One worker role
//!
//! A registry is built for one [`ClusterType`] and admits only that type's
//! worker role; a node reporting the other one is **refused**, not filtered.
//! That is the difference this makes: an async worker pointed at a block
//! cluster used to register happily and simply never be chosen, so the
//! misconfiguration was invisible until someone noticed a cold cache. See
//! ADR 0006.
//!
//! Coordinators are admitted regardless — the type constrains the worker pool.
//!
//! The placement version ([`Epoch`]) is **not** a stored counter: it is derived
//! on demand from the current node set via [`Epoch::for_nodes`], so it is
//! identical on every coordinator observing the same membership and changes iff
//! the placement-relevant node set changes. This is what lets coordinators run
//! active-active without a client seeing the version flip as it is load-balanced
//! between processes (issue #80).
//!
//! Liveness and block inventory come separately from worker heartbeats
//! (see the heartbeat issue); the K8s source only answers "which pods exist".

use std::collections::HashMap;
use std::sync::RwLock;
use talon_core::{ClusterType, NodeId, NodeInfo};

use crate::Epoch;

/// An in-memory registry of known cluster nodes.
///
/// The placement version is a pure function of the node set, so the registry
/// stores only the nodes; [`Membership::epoch`] computes the version on demand.
pub struct Membership {
    cluster_type: ClusterType,
    inner: RwLock<HashMap<NodeId, NodeInfo>>,
}

impl Default for Membership {
    fn default() -> Self {
        Self::for_cluster(ClusterType::default())
    }
}

impl Membership {
    /// Create an empty registry for a cluster of `cluster_type`.
    pub fn for_cluster(cluster_type: ClusterType) -> Self {
        Self {
            cluster_type,
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// The cluster type this registry admits workers for.
    pub fn cluster_type(&self) -> ClusterType {
        self.cluster_type
    }

    /// Register or update a node, returning whether it was admitted.
    ///
    /// A worker whose role is not this cluster's is refused and logged. The
    /// caller should surface that to the node rather than swallow it: a worker
    /// that believes it joined but was dropped will sit idle indefinitely.
    pub fn register(&self, info: NodeInfo) -> bool {
        if !self.cluster_type.admits(info.role) {
            tracing::warn!(
                node = %info.id,
                role = %info.role,
                cluster_type = %self.cluster_type,
                "refusing a node whose role does not belong to this cluster"
            );
            return false;
        }
        self.inner.write().unwrap().insert(info.id.clone(), info);
        true
    }

    /// Remove a node.
    pub fn remove(&self, id: &NodeId) {
        self.inner.write().unwrap().remove(id);
    }

    /// Return a snapshot of all currently known nodes.
    pub fn snapshot(&self) -> Vec<NodeInfo> {
        self.inner.read().unwrap().values().cloned().collect()
    }

    /// Return the nodes placement draws from: this cluster's workers.
    ///
    /// Coordinators are in the registry (the epoch covers them) but are never
    /// placement candidates.
    pub fn worker_snapshot(&self) -> Vec<NodeInfo> {
        let role = self.cluster_type.worker_role();
        self.inner
            .read()
            .unwrap()
            .values()
            .filter(|node| node.role == role)
            .cloned()
            .collect()
    }

    /// The current placement version, derived from the node set.
    ///
    /// Deterministic across coordinators: any process holding the same
    /// membership computes the same value (see [`Epoch::for_nodes`]).
    pub fn epoch(&self) -> Epoch {
        let nodes: Vec<NodeInfo> = self.inner.read().unwrap().values().cloned().collect();
        Epoch::for_nodes(&nodes)
    }

    /// Replace the node set with `desired`, dropping nodes this cluster does
    /// not admit.
    ///
    /// This is the reconcile step a [`MembershipSource`] poll feeds into:
    /// additions, removals, and address/role changes are all applied
    /// atomically. Returns `true` if the set changed.
    pub fn reconcile(&self, desired: Vec<NodeInfo>) -> bool {
        let desired: HashMap<NodeId, NodeInfo> = desired
            .into_iter()
            .filter(|node| self.cluster_type.admits(node.role))
            .map(|n| (n.id.clone(), n))
            .collect();
        let mut g = self.inner.write().unwrap();
        if *g == desired {
            return false;
        }
        *g = desired;
        true
    }
}

/// A source that yields the desired cluster node set on demand.
///
/// Implementations poll or watch an external system (Kubernetes) and return the
/// current membership; errors are the source's own type so a transient API blip
/// can be surfaced without conflating with cache errors.
pub trait MembershipSource {
    /// Error returned when the source cannot produce a snapshot.
    type Error;

    /// Fetch the current desired node set.
    fn poll(&self) -> Result<Vec<NodeInfo>, Self::Error>;
}

/// Selector for which pods/endpoints form the worker set.
#[derive(Debug, Clone)]
pub struct K8sSelector {
    /// Kubernetes namespace to look in.
    pub namespace: String,
    /// Label selector identifying worker pods (e.g. `app=talon-worker`).
    pub label_selector: String,
}

/// A Kubernetes-backed membership source.
///
/// The actual API call (list endpoints/pods matching [`K8sSelector`]) is
/// injected as a closure so the reconcile logic is testable without a live
/// cluster: production wires a real client; tests pass a mock returning a
/// scripted set. Transient API failures propagate as `E` and leave the last
/// good [`Membership`] snapshot untouched (the caller simply skips reconcile).
pub struct KubernetesMembership<F, E>
where
    F: Fn(&K8sSelector) -> Result<Vec<NodeInfo>, E>,
{
    selector: K8sSelector,
    lister: F,
}

impl<F, E> KubernetesMembership<F, E>
where
    F: Fn(&K8sSelector) -> Result<Vec<NodeInfo>, E>,
{
    /// Create a source over the given selector and endpoint lister.
    pub fn new(selector: K8sSelector, lister: F) -> Self {
        Self { selector, lister }
    }

    /// The selector this source watches.
    pub fn selector(&self) -> &K8sSelector {
        &self.selector
    }
}

impl<F, E> MembershipSource for KubernetesMembership<F, E>
where
    F: Fn(&K8sSelector) -> Result<Vec<NodeInfo>, E>,
{
    type Error = E;

    fn poll(&self) -> Result<Vec<NodeInfo>, E> {
        (self.lister)(&self.selector)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use talon_core::NodeRole;

    fn worker(id: &str, addr: &str) -> NodeInfo {
        NodeInfo {
            id: NodeId::new(id),
            address: addr.into(),
            role: NodeRole::Worker,
        }
    }

    fn async_worker(id: &str, addr: &str) -> NodeInfo {
        NodeInfo {
            id: NodeId::new(id),
            address: addr.into(),
            role: NodeRole::AsyncWorker,
        }
    }

    fn coordinator(id: &str, addr: &str) -> NodeInfo {
        NodeInfo {
            id: NodeId::new(id),
            address: addr.into(),
            role: NodeRole::Coordinator,
        }
    }

    #[test]
    fn a_block_cluster_admits_block_workers_and_refuses_async_ones() {
        let m = Membership::for_cluster(ClusterType::Block);
        m.reconcile(vec![
            worker("blk-a", "1"),
            worker("blk-b", "2"),
            async_worker("ext-a", "3"),
            coordinator("coord", "4"),
        ]);

        assert_eq!(ids(m.worker_snapshot()), vec!["blk-a", "blk-b"]);
        // The async worker is gone entirely, not merely unplaceable: it is
        // absent from the unfiltered snapshot and so from the epoch too.
        assert_eq!(ids(m.snapshot()), vec!["blk-a", "blk-b", "coord"]);
    }

    #[test]
    fn an_async_cluster_admits_async_workers_and_refuses_block_ones() {
        let m = Membership::for_cluster(ClusterType::Async);
        m.reconcile(vec![
            worker("blk-a", "1"),
            async_worker("ext-a", "3"),
            coordinator("coord", "4"),
        ]);

        assert_eq!(ids(m.worker_snapshot()), vec!["ext-a"]);
        assert_eq!(ids(m.snapshot()), vec!["coord", "ext-a"]);
    }

    #[test]
    fn registering_the_wrong_worker_role_is_refused_not_ignored() {
        // The caller has to be able to tell the node it was rejected. An
        // async worker that believes it joined a block cluster would sit idle
        // forever with nothing to explain why.
        let m = Membership::for_cluster(ClusterType::Block);
        assert!(m.register(worker("blk-a", "1")));
        assert!(!m.register(async_worker("ext-a", "2")));
        assert_eq!(ids(m.snapshot()), vec!["blk-a"]);
    }

    #[test]
    fn a_coordinator_is_admitted_by_either_cluster_type() {
        // The type constrains the worker pool, not the control plane.
        for cluster in [ClusterType::Block, ClusterType::Async] {
            let m = Membership::for_cluster(cluster);
            assert!(m.register(coordinator("coord", "1")), "{cluster}");
            // ...but a coordinator is never a placement candidate.
            assert!(m.worker_snapshot().is_empty(), "{cluster}");
        }
    }

    #[test]
    fn a_cluster_with_no_workers_yet_has_an_empty_worker_snapshot() {
        let m = Membership::for_cluster(ClusterType::Async);
        m.reconcile(vec![coordinator("coord", "1")]);
        assert!(m.worker_snapshot().is_empty());
    }

    fn ids(mut nodes: Vec<NodeInfo>) -> Vec<String> {
        nodes.sort_by(|a, b| a.id.0.cmp(&b.id.0));
        nodes.into_iter().map(|n| n.id.0).collect()
    }

    #[test]
    fn reconcile_changes_version_only_on_change() {
        let m = Membership::default();
        let empty = m.epoch();
        assert_eq!(empty, Epoch::EMPTY);

        assert!(m.reconcile(vec![worker("a", "1"), worker("b", "2")]));
        let two = m.epoch();
        assert_ne!(two, empty);
        assert_eq!(m.snapshot().len(), 2);

        // Same set (order-independent) -> no change, identical version.
        assert!(!m.reconcile(vec![worker("b", "2"), worker("a", "1")]));
        assert_eq!(m.epoch(), two);

        // Address change -> version changes.
        assert!(m.reconcile(vec![worker("a", "9"), worker("b", "2")]));
        let moved = m.epoch();
        assert_ne!(moved, two);

        // Removal -> version changes.
        assert!(m.reconcile(vec![worker("a", "9")]));
        assert_ne!(m.epoch(), moved);
        assert_eq!(m.snapshot().len(), 1);
    }

    #[test]
    fn register_and_remove_track_version() {
        let m = Membership::default();
        assert_eq!(m.epoch(), Epoch::EMPTY);
        m.register(worker("a", "1"));
        let one = m.epoch();
        assert_ne!(one, Epoch::EMPTY);
        // Re-register identical -> version unchanged.
        m.register(worker("a", "1"));
        assert_eq!(m.epoch(), one);
        m.remove(&NodeId::new("a"));
        assert_eq!(m.epoch(), Epoch::EMPTY);
        m.remove(&NodeId::new("a")); // absent -> still empty
        assert_eq!(m.epoch(), Epoch::EMPTY);
    }

    #[test]
    fn identical_membership_yields_identical_version_across_instances() {
        // Two independent coordinator processes (simulated by two registries)
        // that observe the same healthy worker set must advertise the *same*
        // placement version, so a load-balanced client never thrashes its
        // cache (issue #80). Order of registration must not matter.
        let a = Membership::default();
        a.register(worker("w1", "10.0.0.1"));
        a.register(worker("w2", "10.0.0.2"));
        a.register(worker("w3", "10.0.0.3"));

        let b = Membership::default();
        b.register(worker("w3", "10.0.0.3"));
        b.register(worker("w1", "10.0.0.1"));
        b.register(worker("w2", "10.0.0.2"));

        assert_eq!(a.epoch(), b.epoch());
    }

    #[test]
    fn restarted_coordinator_reproduces_prior_version() {
        // A coordinator restart that rebuilds the same membership must land on
        // the *same* version it had before, not a larger one: the placement is
        // unchanged, so a client's cache is still valid and need not refresh.
        let before = Membership::default();
        before.register(worker("w1", "a"));
        before.register(worker("w2", "b"));
        let v = before.epoch();

        let after_restart = Membership::default();
        after_restart.register(worker("w2", "b"));
        after_restart.register(worker("w1", "a"));
        assert_eq!(after_restart.epoch(), v);
    }

    #[test]
    fn k8s_source_reflects_cluster_changes() {
        // A mock lister scripted to add then remove a node across polls; each
        // real change must move the placement version.
        let step = Cell::new(0u32);
        let selector = K8sSelector {
            namespace: "talon".into(),
            label_selector: "app=talon-worker".into(),
        };
        let source = KubernetesMembership::new(selector, |sel| -> Result<_, ()> {
            assert_eq!(sel.namespace, "talon");
            Ok(match step.get() {
                0 => vec![worker("w1", "10.0.0.1")],
                1 => vec![worker("w1", "10.0.0.1"), worker("w2", "10.0.0.2")],
                _ => vec![worker("w2", "10.0.0.2")],
            })
        });

        let m = Membership::default();

        assert!(m.reconcile(source.poll().unwrap()));
        assert_eq!(m.snapshot().len(), 1);
        let v0 = m.epoch();

        step.set(1);
        assert!(m.reconcile(source.poll().unwrap()));
        assert_eq!(m.snapshot().len(), 2);
        let v1 = m.epoch();
        assert_ne!(v1, v0);

        step.set(2);
        assert!(m.reconcile(source.poll().unwrap()));
        let ids: Vec<String> = m.snapshot().into_iter().map(|n| n.id.0).collect();
        assert_eq!(ids, vec!["w2".to_string()]);
        assert_ne!(m.epoch(), v1);
    }

    #[test]
    fn transient_api_error_is_surfaced_not_swallowed() {
        let selector = K8sSelector {
            namespace: "n".into(),
            label_selector: "l".into(),
        };
        let source = KubernetesMembership::new(selector, |_| -> Result<Vec<NodeInfo>, &str> {
            Err("api blip")
        });
        assert_eq!(source.poll(), Err("api blip"));
    }
}
