//! Worker heartbeat liveness and inventory tracking.
//!
//! Each worker periodically sends a heartbeat carrying its liveness and a
//! compact inventory summary (block count / resident bytes). The coordinator
//! records the arrival time per node and marks a node **unhealthy** once no
//! heartbeat has arrived within a configurable timeout window — typically 3–6
//! heartbeat intervals (e.g. 10s interval → unhealthy at 30–60s).
//!
//! Time is injected as a monotonic millisecond value so the policy is
//! deterministically testable without sleeping; production passes a real clock
//! reading (e.g. from [`Instant`](std::time::Instant)).

use std::collections::HashMap;
use std::sync::RwLock;

use talon_core::NodeId;

/// Configuration for heartbeat liveness.
#[derive(Debug, Clone, Copy)]
pub struct HeartbeatConfig {
    /// Expected interval between heartbeats, in milliseconds.
    pub interval_ms: u64,
    /// A node is unhealthy if silent for longer than this, in milliseconds.
    pub timeout_ms: u64,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        // 10s interval; unhealthy after 30s (3 missed windows).
        Self {
            interval_ms: 10_000,
            timeout_ms: 30_000,
        }
    }
}

/// A compact inventory summary reported alongside a heartbeat.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Inventory {
    /// Number of blocks resident on the worker.
    pub block_count: u64,
    /// Total resident bytes on the worker.
    pub resident_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct Record {
    last_seen_ms: u64,
    inventory: Inventory,
}

/// Tracks per-worker heartbeat liveness and last-reported inventory.
pub struct HeartbeatTracker {
    config: HeartbeatConfig,
    records: RwLock<HashMap<NodeId, Record>>,
}

impl HeartbeatTracker {
    /// Create a tracker with the given config.
    pub fn new(config: HeartbeatConfig) -> Self {
        Self {
            config,
            records: RwLock::new(HashMap::new()),
        }
    }

    /// The active configuration.
    pub fn config(&self) -> HeartbeatConfig {
        self.config
    }

    /// Record a heartbeat from `node` at monotonic time `now_ms` with `inventory`.
    pub fn record(&self, node: NodeId, now_ms: u64, inventory: Inventory) {
        self.records.write().unwrap().insert(
            node,
            Record {
                last_seen_ms: now_ms,
                inventory,
            },
        );
    }

    /// Whether `node` is healthy as of `now_ms` (seen within the timeout).
    pub fn is_healthy(&self, node: &NodeId, now_ms: u64) -> bool {
        self.records
            .read()
            .unwrap()
            .get(node)
            .is_some_and(|r| now_ms.saturating_sub(r.last_seen_ms) <= self.config.timeout_ms)
    }

    /// Last-reported inventory for `node`, if any heartbeat was seen.
    pub fn inventory(&self, node: &NodeId) -> Option<Inventory> {
        self.records.read().unwrap().get(node).map(|r| r.inventory)
    }

    /// All nodes currently considered healthy as of `now_ms`.
    pub fn healthy_nodes(&self, now_ms: u64) -> Vec<NodeId> {
        self.records
            .read()
            .unwrap()
            .iter()
            .filter(|(_, r)| now_ms.saturating_sub(r.last_seen_ms) <= self.config.timeout_ms)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Nodes that have gone silent past the timeout as of `now_ms`.
    pub fn unhealthy_nodes(&self, now_ms: u64) -> Vec<NodeId> {
        self.records
            .read()
            .unwrap()
            .iter()
            .filter(|(_, r)| now_ms.saturating_sub(r.last_seen_ms) > self.config.timeout_ms)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Drop records for nodes silent past `retain_ms` (e.g. removed from the
    /// cluster). Returns the number of records pruned.
    pub fn prune(&self, now_ms: u64, retain_ms: u64) -> usize {
        let mut g = self.records.write().unwrap();
        let before = g.len();
        g.retain(|_, r| now_ms.saturating_sub(r.last_seen_ms) <= retain_ms);
        before - g.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> HeartbeatConfig {
        HeartbeatConfig {
            interval_ms: 10_000,
            timeout_ms: 30_000,
        }
    }

    fn node(id: &str) -> NodeId {
        NodeId::new(id)
    }

    #[test]
    fn healthy_within_window_unhealthy_after() {
        let t = HeartbeatTracker::new(cfg());
        t.record(
            node("w1"),
            1_000,
            Inventory {
                block_count: 5,
                resident_bytes: 500,
            },
        );

        // Just inside the window.
        assert!(t.is_healthy(&node("w1"), 1_000 + 30_000));
        // Just past it.
        assert!(!t.is_healthy(&node("w1"), 1_000 + 30_001));
        // Unknown node is never healthy.
        assert!(!t.is_healthy(&node("ghost"), 1_000));
    }

    #[test]
    fn stopped_worker_marked_unhealthy_within_window() {
        let t = HeartbeatTracker::new(cfg());
        t.record(node("a"), 0, Inventory::default());
        t.record(node("b"), 0, Inventory::default());
        // b keeps beating; a stops.
        t.record(node("b"), 25_000, Inventory::default());

        let now = 40_000; // a last seen 0 -> 40s silent > 30s
        assert_eq!(t.unhealthy_nodes(now), vec![node("a")]);
        assert_eq!(t.healthy_nodes(now), vec![node("b")]);
    }

    #[test]
    fn inventory_is_recorded_and_updated() {
        let t = HeartbeatTracker::new(cfg());
        t.record(
            node("w"),
            0,
            Inventory {
                block_count: 1,
                resident_bytes: 10,
            },
        );
        assert_eq!(t.inventory(&node("w")).unwrap().block_count, 1);
        t.record(
            node("w"),
            100,
            Inventory {
                block_count: 9,
                resident_bytes: 90,
            },
        );
        assert_eq!(
            t.inventory(&node("w")).unwrap(),
            Inventory {
                block_count: 9,
                resident_bytes: 90
            }
        );
        assert!(t.inventory(&node("absent")).is_none());
    }

    #[test]
    fn config_is_respected() {
        let t = HeartbeatTracker::new(HeartbeatConfig {
            interval_ms: 1000,
            timeout_ms: 5000,
        });
        t.record(node("x"), 0, Inventory::default());
        assert!(t.is_healthy(&node("x"), 5000));
        assert!(!t.is_healthy(&node("x"), 5001));
    }

    #[test]
    fn prune_drops_stale_records() {
        let t = HeartbeatTracker::new(cfg());
        t.record(node("old"), 0, Inventory::default());
        t.record(node("new"), 100_000, Inventory::default());
        let pruned = t.prune(100_000, 60_000);
        assert_eq!(pruned, 1);
        assert!(t.inventory(&node("old")).is_none());
        assert!(t.inventory(&node("new")).is_some());
    }
}
