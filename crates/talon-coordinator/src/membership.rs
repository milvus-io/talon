//! Cluster membership tracking.
//!
//! [`Membership`] is the authoritative in-memory node registry consulted by
//! placement. Its contents are driven by a [`MembershipSource`] — in production
//! a Kubernetes watch/poll ([`KubernetesMembership`]), in tests a mock. On each
//! poll the source produces the desired node set and [`Membership::reconcile`]
//! applies the diff.
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
use talon_core::{NodeId, NodeInfo};

use crate::Epoch;

/// A registered node together with the deployment zone it last reported
/// (ADR 0006).
///
/// Node and zone live in one record behind one lock so every reader sees them
/// move together; the placement version stays a pure function of the
/// [`NodeInfo`] projection alone.
struct Member {
    info: NodeInfo,
    zone: Option<String>,
}

/// An in-memory registry of known cluster nodes.
///
/// The placement version is a pure function of the node set, so the registry
/// stores only the nodes; [`Membership::epoch`] computes the version on demand.
pub struct Membership {
    inner: RwLock<HashMap<NodeId, Member>>,
}

impl Default for Membership {
    fn default() -> Self {
        Self::new()
    }
}

impl Membership {
    /// Create an empty membership registry.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Register or update a node, keeping any zone it reported earlier.
    pub fn register(&self, info: NodeInfo) {
        let mut g = self.inner.write().unwrap();
        let zone = g.get(&info.id).and_then(|m| m.zone.clone());
        g.insert(info.id.clone(), Member { info, zone });
    }

    /// Register or update a node together with its reported zone (ADR 0006).
    ///
    /// One write, so no reader can observe the node without its zone; `None`
    /// clears a previously reported zone.
    pub fn register_zoned(&self, info: NodeInfo, zone: Option<String>) {
        self.inner
            .write()
            .unwrap()
            .insert(info.id.clone(), Member { info, zone });
    }

    /// Remove a node.
    pub fn remove(&self, id: &NodeId) {
        self.inner.write().unwrap().remove(id);
    }

    /// Return a snapshot of all currently known nodes.
    pub fn snapshot(&self) -> Vec<NodeInfo> {
        self.inner
            .read()
            .unwrap()
            .values()
            .map(|m| m.info.clone())
            .collect()
    }

    /// Return all known nodes paired with their reported zones (ADR 0006).
    pub fn snapshot_zoned(&self) -> Vec<(NodeInfo, Option<String>)> {
        self.inner
            .read()
            .unwrap()
            .values()
            .map(|m| (m.info.clone(), m.zone.clone()))
            .collect()
    }

    /// The current placement version, derived from the node set.
    ///
    /// Deterministic across coordinators: any process holding the same
    /// membership computes the same value (see [`Epoch::for_nodes`]).
    pub fn epoch(&self) -> Epoch {
        let nodes: Vec<NodeInfo> = self
            .inner
            .read()
            .unwrap()
            .values()
            .map(|m| m.info.clone())
            .collect();
        Epoch::for_nodes(&nodes)
    }

    /// Replace the node set with `desired`, keeping reported zones for nodes
    /// that survive.
    ///
    /// This is the reconcile step a [`MembershipSource`] poll feeds into:
    /// additions, removals, and address/role changes are all applied
    /// atomically. Returns `true` if the node set changed.
    pub fn reconcile(&self, desired: Vec<NodeInfo>) -> bool {
        let mut g = self.inner.write().unwrap();
        let desired = desired
            .into_iter()
            .map(|info| {
                let zone = g.get(&info.id).and_then(|m| m.zone.clone());
                (info, zone)
            })
            .collect();
        Self::apply(&mut g, desired)
    }

    /// [`reconcile`](Self::reconcile) with the zones reported per node; zones
    /// are replaced wholesale even when the node set is unchanged.
    pub fn reconcile_zoned(&self, desired: Vec<(NodeInfo, Option<String>)>) -> bool {
        Self::apply(&mut self.inner.write().unwrap(), desired)
    }

    /// Swap in the desired members; the returned change flag tracks the
    /// [`NodeInfo`] projection only, mirroring what [`Membership::epoch`]
    /// derives the placement version from.
    fn apply(g: &mut HashMap<NodeId, Member>, desired: Vec<(NodeInfo, Option<String>)>) -> bool {
        let changed = g.len() != desired.len()
            || desired
                .iter()
                .any(|(info, _)| g.get(&info.id).map(|m| &m.info) != Some(info));
        *g = desired
            .into_iter()
            .map(|(info, zone)| (info.id.clone(), Member { info, zone }))
            .collect();
        changed
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

    #[test]
    fn reconcile_changes_version_only_on_change() {
        let m = Membership::new();
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
        let m = Membership::new();
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
        let a = Membership::new();
        a.register(worker("w1", "10.0.0.1"));
        a.register(worker("w2", "10.0.0.2"));
        a.register(worker("w3", "10.0.0.3"));

        let b = Membership::new();
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
        let before = Membership::new();
        before.register(worker("w1", "a"));
        before.register(worker("w2", "b"));
        let v = before.epoch();

        let after_restart = Membership::new();
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

        let m = Membership::new();

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
    fn zones_ride_the_member_record() {
        let m = Membership::new();
        m.register_zoned(worker("a", "1"), Some("us-east-1a".into()));
        m.register_zoned(worker("b", "2"), None);
        let mut zoned = m.snapshot_zoned();
        zoned.sort_by(|(x, _), (y, _)| x.id.0.cmp(&y.id.0));
        assert_eq!(zoned[0].1.as_deref(), Some("us-east-1a"));
        assert_eq!(zoned[1].1, None);

        // A zone-less re-register (legacy path) keeps the reported zone; an
        // explicit `None` from the zoned path clears it.
        m.register(worker("a", "1"));
        assert!(m
            .snapshot_zoned()
            .iter()
            .any(|(n, z)| n.id.0 == "a" && z.as_deref() == Some("us-east-1a")));
        m.register_zoned(worker("a", "1"), None);
        assert!(m
            .snapshot_zoned()
            .iter()
            .any(|(n, z)| n.id.0 == "a" && z.is_none()));
    }

    #[test]
    fn reconcile_keeps_zones_for_survivors_and_prunes_the_rest() {
        let m = Membership::new();
        m.register_zoned(worker("a", "1"), Some("z1".into()));
        m.register_zoned(worker("b", "2"), Some("z2".into()));

        // Zone-less reconcile (K8s source path): survivor keeps its zone,
        // the removed node's zone leaves with it.
        assert!(m.reconcile(vec![worker("a", "1"), worker("c", "3")]));
        let mut zoned = m.snapshot_zoned();
        zoned.sort_by(|(x, _), (y, _)| x.id.0.cmp(&y.id.0));
        assert_eq!(zoned.len(), 2);
        assert_eq!(zoned[0].1.as_deref(), Some("z1"));
        assert_eq!(zoned[1].1, None);

        // Zone changing with an identical node set: epoch and the change flag
        // stay put (placement is a function of nodes alone), but the zones
        // are replaced wholesale.
        let before = m.epoch();
        assert!(!m.reconcile_zoned(vec![
            (worker("a", "1"), Some("z9".into())),
            (worker("c", "3"), Some("z3".into())),
        ]));
        assert_eq!(m.epoch(), before);
        let mut zoned = m.snapshot_zoned();
        zoned.sort_by(|(x, _), (y, _)| x.id.0.cmp(&y.id.0));
        assert_eq!(zoned[0].1.as_deref(), Some("z9"));
        assert_eq!(zoned[1].1.as_deref(), Some("z3"));
    }

    #[test]
    fn concurrent_reconcile_and_zoned_snapshot_never_wedge() {
        // Regression for an ABBA deadlock: zones once lived behind their own
        // lock, so `snapshot_zoned` (zones -> inner) racing `reconcile_zoned`
        // (inner -> zones) could wedge every membership operation. The
        // single-lock `Member` layout makes the inversion impossible; this
        // hammers both paths from two threads and fails by timeout instead
        // of hanging CI if the maps are ever split again.
        use std::sync::mpsc;
        use std::sync::Arc;
        use std::time::Duration;

        let m = Arc::new(Membership::new());
        let writer = {
            let m = Arc::clone(&m);
            std::thread::spawn(move || {
                for i in 0..10_000u32 {
                    let zone = if i % 2 == 0 { "z1" } else { "z2" };
                    m.reconcile_zoned(vec![
                        (worker("a", "1"), Some(zone.to_string())),
                        (worker("b", "2"), None),
                    ]);
                }
            })
        };
        let reader = {
            let m = Arc::clone(&m);
            std::thread::spawn(move || {
                for _ in 0..10_000u32 {
                    let _ = m.snapshot_zoned();
                    let _ = m.epoch();
                }
            })
        };

        // Join through a channel so a wedge fails the test cleanly after the
        // timeout (the leaked threads die with the test process) instead of
        // hanging the run forever.
        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = writer.join();
            let _ = reader.join();
            let _ = done_tx.send(());
        });
        done_rx.recv_timeout(Duration::from_secs(30)).expect(
            "membership wedged: concurrent reconcile_zoned and snapshot_zoned never finished",
        );
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
