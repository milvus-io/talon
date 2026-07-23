//! Reusable behavioral contract for production state-store implementations.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use talon_core::{
    NodeHealth, NodeId, NodeInfo, NodeMetricsSnapshot, NodeRole, NodeStatus,
    NODE_STATUS_SCHEMA_VERSION,
};

use super::{ClusterStateStore, NodeEventKind, StoreRevision, WriteDisposition};

#[async_trait]
pub trait StoreContractHarness {
    fn store(&self) -> Arc<dyn ClusterStateStore>;
    fn lease_ttl(&self) -> Duration;
    async fn elapse(&self, duration: Duration);
}

pub fn worker_status(node_id: &str, incarnation: &str, seq: u64) -> NodeStatus {
    NodeStatus {
        schema_version: NODE_STATUS_SCHEMA_VERSION,
        cluster_id: "contract".into(),
        node: NodeInfo {
            id: NodeId::new(node_id),
            address: format!("{node_id}:7001"),
            role: NodeRole::Worker,
        },
        incarnation_id: incarnation.into(),
        admin_address: Some(format!("{node_id}:8001")),
        build_version: "test".into(),
        started_at_unix_ms: if incarnation == "inc-1" { 1_000 } else { 2_000 },
        reported_at_unix_ms: if incarnation == "inc-1" {
            1_000 + seq
        } else {
            2_000 + seq
        },
        heartbeat_seq: seq,
        health: NodeHealth::Healthy,
        ready: true,
        metrics: NodeMetricsSnapshot {
            requests_total: seq,
            block_count: seq,
            ..Default::default()
        },
        labels: BTreeMap::new(),
    }
}

pub async fn assert_store_contract<H: StoreContractHarness>(harness: &H) {
    let store = harness.store();
    let ttl = harness.lease_ttl();

    let first = store
        .upsert_node(worker_status("w1", "inc-1", 0), ttl)
        .await
        .unwrap();
    assert_eq!(first.disposition, WriteDisposition::Applied);
    let duplicate = store
        .upsert_node(worker_status("w1", "inc-1", 0), ttl)
        .await
        .unwrap();
    assert_eq!(duplicate.disposition, WriteDisposition::Duplicate);
    assert_eq!(duplicate.revision, first.revision);

    let snapshot = store.snapshot("contract").await.unwrap();
    assert_eq!(snapshot.nodes.len(), 1);
    assert_eq!(snapshot.nodes[0].heartbeat_seq, 0);
    let mut watch = store
        .watch("contract", Some(&snapshot.revision))
        .await
        .unwrap();

    let newer = store
        .upsert_node(worker_status("w1", "inc-1", 2), ttl)
        .await
        .unwrap();
    assert_eq!(newer.disposition, WriteDisposition::Applied);
    let stale = store
        .upsert_node(worker_status("w1", "inc-1", 1), ttl)
        .await
        .unwrap();
    assert_eq!(stale.disposition, WriteDisposition::Stale);
    assert_eq!(stale.revision, newer.revision);

    let event = watch.next().await.unwrap();
    assert_eq!(event.kind, NodeEventKind::Upserted);
    assert_eq!(event.status.unwrap().heartbeat_seq, 2);

    let mut tasks = Vec::new();
    for seq in 3..=16 {
        let store = Arc::clone(&store);
        tasks.push(tokio::spawn(async move {
            store
                .upsert_node(worker_status("w1", "inc-1", seq), ttl)
                .await
                .unwrap()
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(
        store.snapshot("contract").await.unwrap().nodes[0].heartbeat_seq,
        16
    );

    let restarted = store
        .upsert_node(worker_status("w1", "inc-2", 0), ttl)
        .await
        .unwrap();
    assert_eq!(restarted.disposition, WriteDisposition::Applied);
    let old_incarnation = store
        .upsert_node(worker_status("w1", "inc-1", 99), ttl)
        .await
        .unwrap();
    assert_eq!(old_incarnation.disposition, WriteDisposition::Stale);

    let wrong_owner = store
        .remove_node("contract", &NodeId::new("w1"), "inc-1")
        .await
        .unwrap();
    assert_eq!(wrong_owner.disposition, WriteDisposition::Stale);
    let removed = store
        .remove_node("contract", &NodeId::new("w1"), "inc-2")
        .await
        .unwrap();
    assert_eq!(removed.disposition, WriteDisposition::Applied);
    assert!(store.snapshot("contract").await.unwrap().nodes.is_empty());

    store
        .upsert_node(worker_status("expiring", "inc-1", 0), ttl)
        .await
        .unwrap();
    let before_expiry = store.snapshot("contract").await.unwrap();
    let mut expiry_watch = store
        .watch("contract", Some(&before_expiry.revision))
        .await
        .unwrap();
    harness.elapse(ttl + Duration::from_millis(1)).await;
    assert!(store.snapshot("contract").await.unwrap().nodes.is_empty());
    let expired = expiry_watch.next().await.unwrap();
    assert_eq!(expired.kind, NodeEventKind::Removed);
    assert_eq!(expired.node_id, NodeId::new("expiring"));

    let health = store.check_ready().await.unwrap();
    assert_eq!(health.backend, store.backend());
    assert!(health.revision.is_some());

    let empty = StoreRevision::new("");
    assert!(empty.is_err());
}
