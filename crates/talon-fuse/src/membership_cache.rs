//! Last-good worker membership used for client-side block placement.

use std::sync::RwLock;

use talon_core::{cache_membership_epoch, NodeInfo};

use crate::lock::RwLockExt;

/// One immutable membership view and its equality-only content token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipSnapshot {
    /// Healthy, ready workers advertised by the management plane.
    pub nodes: Vec<NodeInfo>,
    /// Content-derived token used to invalidate per-block placements.
    pub epoch: u64,
}

struct Entry {
    snapshot: MembershipSnapshot,
    refreshed_ms: u64,
}

/// Short-TTL membership cache that retains stale data across refresh errors.
pub struct MembershipCache {
    ttl_ms: u64,
    entry: RwLock<Option<Entry>>,
}

impl MembershipCache {
    /// Create an empty cache.
    pub fn new(ttl_ms: u64) -> Self {
        Self {
            ttl_ms,
            entry: RwLock::new(None),
        }
    }

    /// Return the snapshot only while its refresh TTL is current.
    pub fn fresh(&self, now_ms: u64) -> Option<MembershipSnapshot> {
        self.entry.read_recover().as_ref().and_then(|entry| {
            (now_ms.saturating_sub(entry.refreshed_ms) <= self.ttl_ms)
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
    pub fn replace(&self, nodes: Vec<NodeInfo>, now_ms: u64) -> (MembershipSnapshot, bool) {
        let snapshot = MembershipSnapshot {
            epoch: cache_membership_epoch(&nodes),
            nodes,
        };
        let mut entry = self.entry.write_recover();
        let changed = entry
            .as_ref()
            .is_some_and(|old| old.snapshot.epoch != snapshot.epoch);
        *entry = Some(Entry {
            snapshot: snapshot.clone(),
            refreshed_ms: now_ms,
        });
        (snapshot, changed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{NodeId, NodeRole};

    fn worker(address: &str) -> NodeInfo {
        NodeInfo {
            id: NodeId::new("worker-a"),
            address: address.into(),
            role: NodeRole::Worker,
        }
    }

    #[test]
    fn expiry_retains_a_last_good_snapshot() {
        let cache = MembershipCache::new(100);
        cache.replace(vec![worker("old:7001")], 10);
        assert!(cache.fresh(110).is_some());
        assert!(cache.fresh(111).is_none());
        assert_eq!(cache.last_good().unwrap().nodes[0].address, "old:7001");
    }

    #[test]
    fn address_change_advances_the_equality_token() {
        let cache = MembershipCache::new(100);
        assert!(!cache.replace(vec![worker("old:7001")], 0).1);
        assert!(!cache.replace(vec![worker("old:7001")], 1).1);
        assert!(cache.replace(vec![worker("new:7001")], 2).1);
    }
}
