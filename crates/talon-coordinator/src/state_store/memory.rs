//! Deterministic in-memory state backend for development and contract tests.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use talon_core::{NodeId, NodeStatus};
use tokio::sync::broadcast;

use super::{
    BackendHealth, ClusterSnapshot, ClusterStateStore, ClusterStateWatch, NodeEvent, NodeEventKind,
    StateBackend, StateStoreError, StateStoreResult, StoreRevision, WriteDisposition, WriteResult,
};

const DEFAULT_EVENT_HISTORY: usize = 1_024;

/// Time source used for lease expiry.
pub trait TimeSource: Send + Sync {
    /// Current Unix time in milliseconds.
    fn now_unix_ms(&self) -> u64;
}

#[derive(Debug)]
struct SystemTimeSource;

impl TimeSource for SystemTimeSource {
    fn now_unix_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RecordKey {
    cluster_id: String,
    node_id: NodeId,
}

#[derive(Debug, Clone)]
struct StoredRecord {
    status: NodeStatus,
    expires_at_unix_ms: u64,
}

#[derive(Debug, Default)]
struct Inner {
    revision: u64,
    records: HashMap<RecordKey, StoredRecord>,
    history: VecDeque<NodeEvent>,
}

/// Single-process backend with injected time, bounded watch history, and fault
/// injection.
pub struct MemoryStateStore {
    clock: Arc<dyn TimeSource>,
    history_limit: usize,
    available: AtomicBool,
    events: broadcast::Sender<NodeEvent>,
    inner: Mutex<Inner>,
}

impl Default for MemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStateStore {
    /// Create a store using wall-clock time and the default event history.
    pub fn new() -> Self {
        Self::with_time_source(Arc::new(SystemTimeSource), DEFAULT_EVENT_HISTORY)
    }

    /// Create a store with injectable time and bounded watch history.
    pub fn with_time_source(clock: Arc<dyn TimeSource>, history_limit: usize) -> Self {
        let history_limit = history_limit.max(1);
        let (events, _) = broadcast::channel(history_limit);
        Self {
            clock,
            history_limit,
            available: AtomicBool::new(true),
            events,
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Toggle deterministic backend availability for tests and local fault
    /// injection.
    pub fn set_available(&self, available: bool) {
        self.available.store(available, Ordering::Release);
    }

    fn ensure_available(&self) -> StateStoreResult<()> {
        if self.available.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(StateStoreError::Unavailable {
                backend: StateBackend::Memory,
                detail: "fault injection is active".into(),
            })
        }
    }

    fn revision(value: u64) -> StoreRevision {
        StoreRevision::new(format!("memory:{value}")).expect("memory revision is never empty")
    }

    fn parse_revision(revision: &StoreRevision) -> StateStoreResult<u64> {
        revision
            .as_str()
            .strip_prefix("memory:")
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| StateStoreError::InvalidRevision {
                backend: Some(StateBackend::Memory),
                revision: revision.to_string(),
                detail: "expected memory:<u64>",
            })
    }

    fn publish_locked(&self, inner: &mut Inner, mut event: NodeEvent) {
        inner.revision = inner.revision.saturating_add(1);
        event.revision = Self::revision(inner.revision);
        inner.history.push_back(event.clone());
        while inner.history.len() > self.history_limit {
            inner.history.pop_front();
        }
        let _ = self.events.send(event);
    }

    fn prune_expired_locked(&self, inner: &mut Inner, now_unix_ms: u64) {
        let expired: Vec<_> = inner
            .records
            .iter()
            .filter(|(_, record)| record.expires_at_unix_ms <= now_unix_ms)
            .map(|(key, _)| key.clone())
            .collect();
        for key in expired {
            inner.records.remove(&key);
            self.publish_locked(
                inner,
                NodeEvent {
                    cluster_id: key.cluster_id,
                    node_id: key.node_id,
                    kind: NodeEventKind::Removed,
                    status: None,
                    revision: Self::revision(0),
                    observed_at_unix_ms: now_unix_ms,
                },
            );
        }
    }

    fn ttl_ms(ttl: Duration) -> StateStoreResult<u64> {
        if ttl.is_zero() {
            return Err(StateStoreError::InvalidLeaseTtl(ttl));
        }
        let ttl_ms =
            u64::try_from(ttl.as_millis()).map_err(|_| StateStoreError::InvalidLeaseTtl(ttl))?;
        if ttl_ms == 0 {
            return Err(StateStoreError::InvalidLeaseTtl(ttl));
        }
        Ok(ttl_ms)
    }
}

#[async_trait]
impl ClusterStateStore for MemoryStateStore {
    fn backend(&self) -> StateBackend {
        StateBackend::Memory
    }

    async fn upsert_node(
        &self,
        status: NodeStatus,
        lease_ttl: Duration,
    ) -> StateStoreResult<WriteResult> {
        self.ensure_available()?;
        status.validate()?;
        let ttl_ms = Self::ttl_ms(lease_ttl)?;
        let now = self.clock.now_unix_ms();
        let key = RecordKey {
            cluster_id: status.cluster_id.clone(),
            node_id: status.node.id.clone(),
        };
        let mut inner = self.inner.lock().unwrap();
        self.prune_expired_locked(&mut inner, now);

        if let Some(current) = inner.records.get(&key) {
            let disposition = if current.status.incarnation_id == status.incarnation_id {
                match status.heartbeat_seq.cmp(&current.status.heartbeat_seq) {
                    std::cmp::Ordering::Less => Some(WriteDisposition::Stale),
                    std::cmp::Ordering::Equal => Some(WriteDisposition::Duplicate),
                    std::cmp::Ordering::Greater => None,
                }
            } else if status.started_at_unix_ms < current.status.started_at_unix_ms
                || (status.started_at_unix_ms == current.status.started_at_unix_ms
                    && status.reported_at_unix_ms <= current.status.reported_at_unix_ms)
            {
                Some(WriteDisposition::Stale)
            } else {
                None
            };
            if let Some(disposition) = disposition {
                return Ok(WriteResult {
                    disposition,
                    revision: Self::revision(inner.revision),
                });
            }
        }

        inner.records.insert(
            key,
            StoredRecord {
                status: status.clone(),
                expires_at_unix_ms: now.saturating_add(ttl_ms),
            },
        );
        self.publish_locked(
            &mut inner,
            NodeEvent {
                cluster_id: status.cluster_id.clone(),
                node_id: status.node.id.clone(),
                kind: NodeEventKind::Upserted,
                status: Some(status),
                revision: Self::revision(0),
                observed_at_unix_ms: now,
            },
        );
        Ok(WriteResult {
            disposition: WriteDisposition::Applied,
            revision: Self::revision(inner.revision),
        })
    }

    async fn remove_node(
        &self,
        cluster_id: &str,
        node_id: &NodeId,
        incarnation_id: &str,
    ) -> StateStoreResult<WriteResult> {
        self.ensure_available()?;
        let now = self.clock.now_unix_ms();
        let key = RecordKey {
            cluster_id: cluster_id.to_string(),
            node_id: node_id.clone(),
        };
        let mut inner = self.inner.lock().unwrap();
        self.prune_expired_locked(&mut inner, now);
        let Some(record) = inner.records.get(&key) else {
            return Ok(WriteResult {
                disposition: WriteDisposition::NotFound,
                revision: Self::revision(inner.revision),
            });
        };
        if record.status.incarnation_id != incarnation_id {
            return Ok(WriteResult {
                disposition: WriteDisposition::Stale,
                revision: Self::revision(inner.revision),
            });
        }

        inner.records.remove(&key);
        self.publish_locked(
            &mut inner,
            NodeEvent {
                cluster_id: cluster_id.to_string(),
                node_id: node_id.clone(),
                kind: NodeEventKind::Removed,
                status: None,
                revision: Self::revision(0),
                observed_at_unix_ms: now,
            },
        );
        Ok(WriteResult {
            disposition: WriteDisposition::Applied,
            revision: Self::revision(inner.revision),
        })
    }

    async fn snapshot(&self, cluster_id: &str) -> StateStoreResult<ClusterSnapshot> {
        self.ensure_available()?;
        let now = self.clock.now_unix_ms();
        let mut inner = self.inner.lock().unwrap();
        self.prune_expired_locked(&mut inner, now);
        let mut nodes: Vec<_> = inner
            .records
            .iter()
            .filter(|(key, _)| key.cluster_id == cluster_id)
            .map(|(_, record)| record.status.clone())
            .collect();
        nodes.sort_by(|a, b| a.node.id.0.cmp(&b.node.id.0));
        Ok(ClusterSnapshot {
            nodes,
            revision: Self::revision(inner.revision),
            observed_at_unix_ms: now,
        })
    }

    async fn watch(
        &self,
        cluster_id: &str,
        after_revision: Option<&StoreRevision>,
    ) -> StateStoreResult<Box<dyn ClusterStateWatch>> {
        self.ensure_available()?;
        let now = self.clock.now_unix_ms();
        let mut inner = self.inner.lock().unwrap();
        self.prune_expired_locked(&mut inner, now);
        let after = match after_revision {
            Some(revision) => Self::parse_revision(revision)?,
            None => inner.revision,
        };
        if after > inner.revision {
            return Err(StateStoreError::InvalidRevision {
                backend: Some(StateBackend::Memory),
                revision: Self::revision(after).to_string(),
                detail: "revision is newer than current state",
            });
        }
        if let Some(oldest) = inner.history.front().map(|event| {
            Self::parse_revision(&event.revision).expect("memory history has memory revisions")
        }) {
            if after.saturating_add(1) < oldest {
                return Err(StateStoreError::Compacted {
                    requested: Self::revision(after),
                    oldest: Self::revision(oldest),
                });
            }
        }

        let backlog = inner
            .history
            .iter()
            .filter(|event| {
                event.cluster_id == cluster_id
                    && Self::parse_revision(&event.revision).is_ok_and(|revision| revision > after)
            })
            .cloned()
            .collect();
        let receiver = self.events.subscribe();
        Ok(Box::new(MemoryWatch {
            cluster_id: cluster_id.to_string(),
            backlog,
            receiver,
            last_revision: Self::revision(after),
        }))
    }

    async fn check_ready(&self) -> StateStoreResult<BackendHealth> {
        self.ensure_available()?;
        let now = self.clock.now_unix_ms();
        let mut inner = self.inner.lock().unwrap();
        self.prune_expired_locked(&mut inner, now);
        Ok(BackendHealth {
            backend: StateBackend::Memory,
            checked_at_unix_ms: now,
            revision: Some(Self::revision(inner.revision)),
        })
    }
}

struct MemoryWatch {
    cluster_id: String,
    backlog: VecDeque<NodeEvent>,
    receiver: broadcast::Receiver<NodeEvent>,
    last_revision: StoreRevision,
}

#[async_trait]
impl ClusterStateWatch for MemoryWatch {
    async fn next(&mut self) -> StateStoreResult<NodeEvent> {
        if let Some(event) = self.backlog.pop_front() {
            self.last_revision = event.revision.clone();
            return Ok(event);
        }
        loop {
            match self.receiver.recv().await {
                Ok(event) => {
                    self.last_revision = event.revision.clone();
                    if event.cluster_id == self.cluster_id {
                        return Ok(event);
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    return Err(StateStoreError::WatchLagged {
                        after: self.last_revision.clone(),
                        skipped,
                    });
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(StateStoreError::Unavailable {
                        backend: StateBackend::Memory,
                        detail: "watch channel closed".into(),
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::state_store::testkit::{assert_store_contract, StoreContractHarness};

    #[derive(Debug, Default)]
    struct ManualTime {
        now_ms: AtomicU64,
    }

    impl ManualTime {
        fn advance(&self, duration: Duration) {
            self.now_ms
                .fetch_add(duration.as_millis() as u64, Ordering::SeqCst);
        }
    }

    impl TimeSource for ManualTime {
        fn now_unix_ms(&self) -> u64 {
            self.now_ms.load(Ordering::SeqCst)
        }
    }

    struct MemoryHarness {
        store: Arc<MemoryStateStore>,
        clock: Arc<ManualTime>,
    }

    #[async_trait]
    impl StoreContractHarness for MemoryHarness {
        fn store(&self) -> Arc<dyn ClusterStateStore> {
            self.store.clone()
        }

        fn lease_ttl(&self) -> Duration {
            Duration::from_millis(100)
        }

        async fn elapse(&self, duration: Duration) {
            self.clock.advance(duration);
        }
    }

    #[tokio::test]
    async fn reusable_store_contract_passes() {
        let clock = Arc::new(ManualTime::default());
        let harness = MemoryHarness {
            store: Arc::new(MemoryStateStore::with_time_source(clock.clone(), 64)),
            clock,
        };
        assert_store_contract(&harness).await;
    }

    #[tokio::test]
    async fn compacted_and_foreign_revisions_are_rejected() {
        let clock = Arc::new(ManualTime::default());
        let store = MemoryStateStore::with_time_source(clock, 2);
        for seq in 0..3 {
            let mut status = crate::state_store::testkit::worker_status("w1", "inc-1", seq);
            status.reported_at_unix_ms += seq;
            store
                .upsert_node(status, Duration::from_secs(30))
                .await
                .unwrap();
        }

        assert!(matches!(
            store
                .watch("contract", Some(&StoreRevision::new("memory:0").unwrap()))
                .await,
            Err(StateStoreError::Compacted { .. })
        ));
        assert!(matches!(
            store
                .watch("contract", Some(&StoreRevision::new("etcd:7").unwrap()))
                .await,
            Err(StateStoreError::InvalidRevision { .. })
        ));
        assert!(matches!(
            store
                .watch("contract", Some(&StoreRevision::new("memory:99").unwrap()))
                .await,
            Err(StateStoreError::InvalidRevision { .. })
        ));
    }

    #[tokio::test]
    async fn unavailable_backend_fails_without_mutating_state() {
        let store = MemoryStateStore::new();
        store.set_available(false);
        assert!(matches!(
            store.snapshot("contract").await,
            Err(StateStoreError::Unavailable { .. })
        ));
        assert!(matches!(
            store.check_ready().await,
            Err(StateStoreError::Unavailable { .. })
        ));
        assert!(StateStoreError::Unavailable {
            backend: StateBackend::Memory,
            detail: "test".into()
        }
        .is_retryable());

        store.set_available(true);
        assert!(store.snapshot("contract").await.unwrap().nodes.is_empty());
    }

    #[tokio::test]
    async fn sub_millisecond_lease_is_rejected() {
        let store = MemoryStateStore::new();
        assert!(matches!(
            store
                .upsert_node(
                    crate::state_store::testkit::worker_status("w1", "inc-1", 0),
                    Duration::from_nanos(1)
                )
                .await,
            Err(StateStoreError::InvalidLeaseTtl(_))
        ));
    }
}
