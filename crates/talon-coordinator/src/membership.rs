//! Cluster membership tracking.
//!
//! [`Membership`] is the authoritative in-memory node registry consulted by
//! placement. Its contents are driven by a [`MembershipSource`] — in production
//! a Kubernetes watch/poll ([`KubernetesMembership`]), in tests a mock. On each
//! poll the source produces the desired node set and [`Membership::reconcile`]
//! applies the diff, bumping the [`Epoch`] whenever the set
//! actually changes so clients/workers can detect stale placement.
//!
//! Liveness and block inventory come separately from worker heartbeats
//! (see the heartbeat issue); the K8s source only answers "which pods exist".

use std::collections::HashMap;
use std::sync::RwLock;
use talon_core::{NodeId, NodeInfo};

use crate::Epoch;

/// An in-memory registry of known cluster nodes, versioned by an epoch.
pub struct Membership {
    inner: RwLock<Inner>,
}

impl Default for Membership {
    fn default() -> Self {
        Self::new()
    }
}

struct Inner {
    nodes: HashMap<NodeId, NodeInfo>,
    epoch: Epoch,
}

impl Membership {
    /// Create an empty membership registry seeded from the process start time.
    ///
    /// The epoch base comes from [`Epoch::seeded_now`] rather than `0` so that
    /// this coordinator's epochs outrank any earlier process's, keeping client
    /// placement caches correct across a coordinator restart (issue #69).
    pub fn new() -> Self {
        Self::with_epoch_base(Epoch::seeded_now())
    }

    /// Create an empty registry starting at a specific epoch base.
    ///
    /// Production uses [`Membership::new`] (wall-clock seed); this constructor
    /// lets tests pin a deterministic base (e.g. `Epoch(0)`) or simulate a
    /// restart by seeding a second instance at a strictly larger base.
    pub fn with_epoch_base(base: Epoch) -> Self {
        Self {
            inner: RwLock::new(Inner {
                nodes: HashMap::new(),
                epoch: base,
            }),
        }
    }

    /// Register or update a node. Bumps the epoch if this changed the set.
    pub fn register(&self, info: NodeInfo) {
        let mut g = self.inner.write().unwrap();
        let changed = g.nodes.insert(info.id.clone(), info.clone()) != Some(info);
        if changed {
            g.epoch = g.epoch.next();
        }
    }

    /// Remove a node. Bumps the epoch if a node was actually removed.
    pub fn remove(&self, id: &NodeId) {
        let mut g = self.inner.write().unwrap();
        if g.nodes.remove(id).is_some() {
            g.epoch = g.epoch.next();
        }
    }

    /// Return a snapshot of all currently known nodes.
    pub fn snapshot(&self) -> Vec<NodeInfo> {
        self.inner.read().unwrap().nodes.values().cloned().collect()
    }

    /// The current placement epoch.
    pub fn epoch(&self) -> Epoch {
        self.inner.read().unwrap().epoch
    }

    /// Replace the node set with `desired`, bumping the epoch iff it changed.
    ///
    /// This is the reconcile step a [`MembershipSource`] poll feeds into:
    /// additions, removals, and address/role changes are all applied
    /// atomically. Returns `true` if the set changed (epoch bumped).
    pub fn reconcile(&self, desired: Vec<NodeInfo>) -> bool {
        let desired: HashMap<NodeId, NodeInfo> =
            desired.into_iter().map(|n| (n.id.clone(), n)).collect();
        let mut g = self.inner.write().unwrap();
        if g.nodes == desired {
            return false;
        }
        g.nodes = desired;
        g.epoch = g.epoch.next();
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

    #[test]
    fn reconcile_bumps_epoch_only_on_change() {
        // Pin a deterministic base so we can assert exact epoch values; the
        // +1-on-change semantics are what matter here, not the base.
        let m = Membership::with_epoch_base(Epoch(0));
        assert_eq!(m.epoch(), Epoch(0));

        assert!(m.reconcile(vec![worker("a", "1"), worker("b", "2")]));
        assert_eq!(m.epoch(), Epoch(1));
        assert_eq!(m.snapshot().len(), 2);

        // Same set (order-independent) -> no bump.
        assert!(!m.reconcile(vec![worker("b", "2"), worker("a", "1")]));
        assert_eq!(m.epoch(), Epoch(1));

        // Address change -> bump.
        assert!(m.reconcile(vec![worker("a", "9"), worker("b", "2")]));
        assert_eq!(m.epoch(), Epoch(2));

        // Removal -> bump.
        assert!(m.reconcile(vec![worker("a", "9")]));
        assert_eq!(m.epoch(), Epoch(3));
        assert_eq!(m.snapshot().len(), 1);
    }

    #[test]
    fn register_and_remove_track_epoch() {
        let m = Membership::with_epoch_base(Epoch(0));
        m.register(worker("a", "1"));
        assert_eq!(m.epoch(), Epoch(1));
        // Re-register identical -> no change.
        m.register(worker("a", "1"));
        assert_eq!(m.epoch(), Epoch(1));
        m.remove(&NodeId::new("a"));
        assert_eq!(m.epoch(), Epoch(2));
        m.remove(&NodeId::new("a")); // absent -> no bump
        assert_eq!(m.epoch(), Epoch(2));
    }

    #[test]
    fn new_seeds_nonzero_epoch_base() {
        // A production Membership seeds from the wall clock, so its base is far
        // above 0 and — critically — above the low-counter range a prior
        // process would have reached (issue #69).
        let m = Membership::new();
        assert!(m.epoch().0 > 0, "epoch base must be seeded, not 0");
        // The seed lives in the high 32 bits; the low counter starts at 0.
        assert_eq!(m.epoch().0 & 0xFFFF_FFFF, 0);
    }

    #[test]
    fn restarted_coordinator_outranks_prior_process() {
        // Simulate: an earlier process seeded at second T1 that then churned
        // through many membership changes, vs. a later process seeded at a
        // strictly greater second T2. The later process's *initial* epoch must
        // already exceed the earlier one's *final* epoch, so clients refresh.
        let t1 = 1_000u64;
        let t2 = 1_001u64; // one second later
        let old = Membership::with_epoch_base(Epoch(t1 << 32));
        for i in 0..10_000 {
            old.register(worker(&format!("w{i}"), "a"));
        }
        let new = Membership::with_epoch_base(Epoch(t2 << 32));
        assert!(
            new.epoch() > old.epoch(),
            "restarted coordinator epoch {:?} must outrank prior {:?}",
            new.epoch(),
            old.epoch()
        );
    }

    #[test]
    fn k8s_source_reflects_cluster_changes() {
        // A mock lister scripted to add then remove a node across polls.
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

        let m = Membership::with_epoch_base(Epoch(0));

        assert!(m.reconcile(source.poll().unwrap()));
        assert_eq!(m.snapshot().len(), 1);
        assert_eq!(m.epoch(), Epoch(1));

        step.set(1);
        assert!(m.reconcile(source.poll().unwrap()));
        assert_eq!(m.snapshot().len(), 2);
        assert_eq!(m.epoch(), Epoch(2));

        step.set(2);
        assert!(m.reconcile(source.poll().unwrap()));
        let ids: Vec<String> = m.snapshot().into_iter().map(|n| n.id.0).collect();
        assert_eq!(ids, vec!["w2".to_string()]);
        assert_eq!(m.epoch(), Epoch(3));
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
