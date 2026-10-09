//! Shared coordinator observability state: readiness gating, state-store
//! access with deadlines, advertised capabilities, and the leased status
//! record.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::File;
use std::io::Read;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use talon_core::{
    NodeHealth, NodeInfo, NodeMetricsSnapshot, NodeRole, NodeStatus, NODE_STATUS_SCHEMA_VERSION,
};
use talon_metadata::{ClusterCapabilities, MetadataStore};

use super::metrics::CoordinatorMetrics;
use super::now_unix_ms;
use crate::{ClusterSnapshot, ClusterStateStore, StateStoreError, StateStoreResult, WriteResult};

#[cfg(test)]
use crate::MemoryStateStore;

#[cfg(test)]
use talon_core::NodeId;

/// Shared coordinator observability and state-store access.
pub struct CoordinatorObservability {
    cluster_id: String,
    node: NodeInfo,
    admin_address: String,
    incarnation_id: String,
    started_at_unix_ms: u64,
    pub(crate) started: Instant,
    sequence: AtomicU64,
    ready: AtomicBool,
    /// Whether a state-store operation has failed since the last successful
    /// membership reconciliation.
    state_store_degraded: AtomicBool,
    /// Monotonic timestamp of the last snapshot that was actually installed
    /// into placement membership. Zero means no successful reconciliation.
    last_membership_refresh_elapsed_ms: AtomicU64,
    /// How long the installed last-good membership remains authoritative after
    /// a transient state-store failure.
    state_failure_grace: Duration,
    pub(crate) shutting_down: AtomicBool,
    shutdown: Arc<talon_transport::shutdown::Shutdown>,
    listeners_ready: AtomicBool,
    request_timeout: Duration,
    pub(crate) metrics: CoordinatorMetrics,
    store: Arc<dyn ClusterStateStore>,
    last_discovery: RwLock<Option<(talon_core::worker_membership::WorkerDiscovery, Instant)>>,
    heartbeats: Mutex<super::heartbeats::Heartbeats>,
    // Serialize snapshot installation and slow-path admission. The heartbeat
    // fast path never waits for this lock or performs backend I/O.
    membership_refresh: tokio::sync::Mutex<()>,
    /// What this cluster advertises, and the store that backs it.
    ///
    /// `advertised` and `revision` are fixed at construction: they describe the
    /// deployment's configuration. Reachability is not part of that -- ADR 0003
    /// §6 requires "not configured" and "configured but unreachable" to be
    /// separable *during an incident*, which a value sampled once at startup
    /// cannot do. So it lives in an atomic, refreshed on the readiness path.
    capabilities: ClusterCapabilities,
    /// Current reachability of the metadata store.
    ///
    /// Kept beside `capabilities` rather than inside it so a reachability change
    /// cannot be mistaken for a capability change: the revision must not advance
    /// when a store blips, or every outage would look like a reconfiguration and
    /// invalidate every client's cached capability set.
    metadata_reachable: AtomicBool,
    /// Handle used to sample reachability. `None` when no store is configured.
    metadata_store: Option<Arc<dyn MetadataStore>>,
}

impl CoordinatorObservability {
    /// Create observability state over the selected shared-state backend.
    pub fn new(
        cluster_id: String,
        node: NodeInfo,
        admin_address: String,
        request_timeout: Duration,
        store: Arc<dyn ClusterStateStore>,
    ) -> std::io::Result<Self> {
        Ok(Self {
            cluster_id,
            node,
            admin_address,
            incarnation_id: generate_incarnation_id()?,
            started_at_unix_ms: now_unix_ms(),
            started: Instant::now(),
            last_discovery: RwLock::new(None),
            heartbeats: Mutex::new(Default::default()),
            membership_refresh: tokio::sync::Mutex::new(()),
            sequence: AtomicU64::new(0),
            ready: AtomicBool::new(false),
            state_store_degraded: AtomicBool::new(false),
            last_membership_refresh_elapsed_ms: AtomicU64::new(0),
            state_failure_grace: Duration::ZERO,
            shutting_down: AtomicBool::new(false),
            shutdown: Arc::default(),
            listeners_ready: AtomicBool::new(true),
            request_timeout,
            metrics: CoordinatorMetrics::new(),
            store,
            // A coordinator with no metadata store advertises nothing. ADR 0003
            // §1 keeps that a complete, supported deployment rather than a
            // degraded one, so this is the correct default and not a
            // placeholder.
            capabilities: ClusterCapabilities::none(),
            metadata_reachable: AtomicBool::new(true),
            metadata_store: None,
        })
    }

    /// Attach the capability set this cluster advertises.
    ///
    /// Builder-style so a coordinator without a metadata store needs no change:
    /// omitting this leaves the empty set from [`ClusterCapabilities::none`].
    #[must_use]
    pub fn with_capabilities(mut self, capabilities: ClusterCapabilities) -> Self {
        self.metadata_reachable = AtomicBool::new(capabilities.store_reachable);
        self.capabilities = capabilities;
        self
    }

    /// Attach the metadata store used to sample reachability.
    ///
    /// Without this the advertised reachability stays at whatever
    /// [`with_capabilities`](Self::with_capabilities) recorded, which is a
    /// startup fact. §6 needs a current one.
    #[must_use]
    pub fn with_metadata_store(mut self, store: Arc<dyn MetadataStore>) -> Self {
        self.metadata_store = Some(store);
        self
    }

    /// Keep serving the last successfully reconciled membership through a
    /// short shared-state disturbance.
    ///
    /// The grace is intentionally bounded: once it expires, or before any
    /// snapshot has been installed, authoritative reads fail closed. A later
    /// successful readiness probe alone cannot restore service; only a
    /// successful membership reconciliation can prove that local placement
    /// state is current again.
    #[must_use]
    pub fn with_state_failure_grace(mut self, grace: Duration) -> Self {
        self.state_failure_grace = grace;
        self
    }

    /// Capabilities this cluster advertises.
    ///
    /// ADR 0003 §4 requires these to be discoverable "without attempting an
    /// operation", which is what the management API endpoint is for.
    pub fn capabilities(&self) -> ClusterCapabilities {
        ClusterCapabilities {
            advertised: self.capabilities.advertised,
            // Deliberately unchanged by reachability: a store blip is not a
            // reconfiguration, and advancing this would invalidate every
            // client's cached capability set on every outage.
            revision: self.capabilities.revision,
            store_reachable: self.metadata_reachable.load(Ordering::Acquire),
        }
    }

    /// Sample the metadata store and update advertised reachability.
    ///
    /// Runs on the readiness path, never on the read path. §6 is explicit that
    /// "reads, cache hits and misses, placement lookups, and write-through
    /// writes are unaffected. None consults TMS", so request handling reads the
    /// cached atomic rather than probing the store.
    ///
    /// A cluster with no store keeps reporting reachable: an empty capability
    /// set is always serviceable, and there is nothing that could be down.
    async fn refresh_metadata_reachability(&self) {
        let Some(store) = self.metadata_store.as_ref() else {
            return;
        };
        let reachable = match tokio::time::timeout(self.request_timeout, store.check_ready()).await
        {
            Ok(Ok(health)) => health.ready,
            Ok(Err(_)) | Err(_) => false,
        };
        let previous = self.metadata_reachable.swap(reachable, Ordering::AcqRel);
        if previous != reachable {
            // Logged at the transition rather than every sample, and separately
            // from the "not configured" case, so an incident can tell an outage
            // from a deployment choice (§6).
            if reachable {
                tracing::info!(
                    capabilities = %self.capabilities.advertised,
                    "metadata store reachable again; TMS-backed features restored"
                );
            } else {
                tracing::warn!(
                    capabilities = %self.capabilities.advertised,
                    "metadata store became unreachable; TMS-backed features fail closed"
                );
            }
        }
    }

    /// Coordinator metric handles.
    pub fn metrics(&self) -> &CoordinatorMetrics {
        &self.metrics
    }

    /// Selected state store.
    pub fn store(&self) -> &Arc<dyn ClusterStateStore> {
        &self.store
    }

    /// Logical cluster accepted by status heartbeats.
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// Current process incarnation bound into privileged control updates.
    pub fn incarnation_id(&self) -> &str {
        &self.incarnation_id
    }

    /// Stable coordinator node identity.
    pub fn node_id(&self) -> &str {
        &self.node.id.0
    }

    fn elapsed_marker_ms(&self) -> u64 {
        let elapsed = self.started.elapsed().as_millis();
        u64::try_from(elapsed.min(u128::from(u64::MAX - 1))).unwrap_or(u64::MAX - 1) + 1
    }

    fn membership_within_failure_grace(&self) -> bool {
        if self.state_failure_grace.is_zero() {
            return false;
        }
        let refreshed = self
            .last_membership_refresh_elapsed_ms
            .load(Ordering::Acquire);
        if refreshed == 0 {
            return false;
        }
        let age_ms = self.elapsed_marker_ms().saturating_sub(refreshed);
        Duration::from_millis(age_ms) <= self.state_failure_grace
    }

    fn record_state_store_failure(&self) {
        self.state_store_degraded.store(true, Ordering::Release);
        self.ready
            .store(self.membership_within_failure_grace(), Ordering::Release);
    }

    fn record_membership_refresh(&self) {
        self.last_membership_refresh_elapsed_ms
            .store(self.elapsed_marker_ms(), Ordering::Release);
        self.state_store_degraded.store(false, Ordering::Release);
        self.ready.store(true, Ordering::Release);
    }

    /// Whether authoritative shared state is currently ready.
    pub fn set_listeners_ready(&self, ready: bool) {
        self.listeners_ready.store(ready, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
            && self
                .last_discovery
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
                .map_or(true, |(_, refreshed)| {
                    refreshed.elapsed() < self.discovery_max_age()
                })
            && self.listeners_ready.load(Ordering::Acquire)
            && (!self.state_store_degraded.load(Ordering::Acquire)
                || self.membership_within_failure_grace())
            && !self.shutting_down.load(Ordering::Acquire)
    }

    /// Read-only readiness check with deadline.
    pub async fn check_ready(&self) -> StateStoreResult<()> {
        let started = Instant::now();
        let result =
            match tokio::time::timeout(self.request_timeout, self.store.check_ready()).await {
                Ok(result) => result.map(|_| ()),
                Err(_) => Err(StateStoreError::Timeout {
                    backend: self.store.backend(),
                }),
            };
        self.metrics
            .record_state("readiness", &result, started.elapsed());
        if result.is_err() {
            self.record_state_store_failure();
        } else if self.state_store_degraded.load(Ordering::Acquire) {
            // Store reachability alone does not make the installed membership
            // current. Keep the bounded last-good state until reconcile has
            // successfully installed a fresh snapshot.
            self.ready
                .store(self.membership_within_failure_grace(), Ordering::Release);
        } else {
            self.ready.store(true, Ordering::Release);
        }
        // Sampled alongside cluster-state readiness so reachability tracks the
        // present rather than startup. A metadata failure must not affect this
        // result: §6 keeps TMS outages away from the read path, and readiness
        // gates the read path.
        self.refresh_metadata_reachability().await;
        result
    }

    /// Upsert a leased node status with metrics and deadline.
    pub async fn upsert_status(
        &self,
        status: NodeStatus,
        lease_ttl: Duration,
    ) -> StateStoreResult<WriteResult> {
        if status.node.role == NodeRole::Worker {
            return self
                .publish_instance(status, lease_ttl)
                .await
                .map(|(result, _)| result);
        }
        let started = Instant::now();
        let result = match tokio::time::timeout(
            self.request_timeout,
            self.store.upsert_node(status, lease_ttl),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(StateStoreError::Timeout {
                backend: self.store.backend(),
            }),
        };
        self.metrics
            .record_state("upsert", &result, started.elapsed());
        if result.is_err() {
            self.record_state_store_failure();
        }
        result
    }

    /// Refresh live-node and snapshot-age gauges.
    pub async fn refresh_snapshot(&self) -> StateStoreResult<()> {
        let started = Instant::now();
        let result =
            match tokio::time::timeout(self.request_timeout, self.store.snapshot(&self.cluster_id))
                .await
            {
                Ok(result) => result,
                Err(_) => Err(StateStoreError::Timeout {
                    backend: self.store.backend(),
                }),
            };
        self.metrics
            .record_state("snapshot", &result, started.elapsed());
        match result {
            Ok(snapshot) => {
                self.metrics.update_snapshot(&snapshot);
                Ok(())
            }
            Err(error) => {
                self.record_state_store_failure();
                Err(error)
            }
        }
    }

    pub async fn worker_discovery(
        &self,
    ) -> StateStoreResult<talon_core::worker_membership::WorkerDiscovery> {
        use talon_core::worker_membership::*;
        let view = tokio::time::timeout(self.request_timeout, async {
            let registry = self.store.member_registry(&self.cluster_id).await?.value;
            let instances = self.store.instance_snapshot(&self.cluster_id).await?;
            Ok::<_, StateStoreError>(WorkerDiscovery::retained(&registry, &instances.nodes))
        })
        .await
        .map_err(|_| StateStoreError::Timeout {
            backend: self.store.backend(),
        })??;
        self.metrics.update_worker_discovery(&view);
        Ok(view)
    }

    fn discovery_max_age(&self) -> Duration {
        if self.state_failure_grace.is_zero() {
            self.request_timeout
        } else {
            self.state_failure_grace
        }
    }

    /// Serve the last installed observation without a backend round trip.
    /// Periodic reconciliation alone renews its bounded validity.
    pub async fn membership_for_query(
        &self,
    ) -> StateStoreResult<talon_core::worker_membership::WorkerDiscovery> {
        if !self.is_ready() {
            return Err(StateStoreError::Unavailable {
                backend: self.store.backend(),
                detail: "coordinator not ready".into(),
            });
        }
        let cached = self
            .last_discovery
            .read()
            .unwrap_or_else(|e| e.into_inner());
        if let Some((view, refreshed)) = cached.as_ref() {
            let remaining = self.discovery_max_age().saturating_sub(refreshed.elapsed());
            if remaining.as_millis() > 0 {
                let mut view = view.clone();
                view.valid_for_ms = view.valid_for_ms.min(remaining.as_millis() as u64);
                return Ok(view);
            }
        }
        Err(StateStoreError::Unavailable {
            backend: self.store.backend(),
            detail: "membership cache requires refresh".into(),
        })
    }

    fn buffer_instance(
        &self,
        status: &NodeStatus,
        ttl: Duration,
    ) -> Option<StateStoreResult<bool>> {
        if !self.is_ready() || !self.membership_cache_is_fresh() {
            return None;
        }
        self.heartbeats
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .receive(status, ttl)
            .map(|result| {
                result.map_err(|()| {
                    crate::state_store::registry::invalid(
                        self.store.backend(),
                        "stale instance heartbeat",
                    )
                })
            })
    }

    fn membership_cache_is_fresh(&self) -> bool {
        self.last_discovery
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|(_, refreshed)| refreshed.elapsed() < self.discovery_max_age())
    }

    /// Accept an unchanged admitted instance in memory, or synchronously admit
    /// a new/changed instance and install its routing observation before Ack.
    /// The second result says whether proxy membership needs refreshing.
    pub async fn receive_instance(
        &self,
        status: NodeStatus,
        ttl: Duration,
        membership: &crate::Membership,
    ) -> StateStoreResult<(bool, bool)> {
        let received = Instant::now();
        status.validate()?;
        if status.cluster_id != self.cluster_id || status.node.role != NodeRole::Worker {
            return Err(crate::state_store::registry::invalid(
                self.store.backend(),
                "invalid worker instance cluster or role",
            ));
        }
        self.check_cached_sequence(&status)?;
        if let Some(result) = self.buffer_instance(&status, ttl) {
            return result.map(|serving| (serving, false));
        }
        // Queueing is part of the request budget. A disconnected caller's
        // report must not wait indefinitely and then acquire a fresh lease.
        let result = tokio::time::timeout(self.request_timeout, async {
            let _refresh = self.membership_refresh.lock().await;
            self.check_cached_sequence(&status)?;
            if let Some(result) = self.buffer_instance(&status, ttl) {
                return result.map(|serving| (serving, false));
            }
            let remaining = ttl.saturating_sub(received.elapsed());
            if remaining.is_zero() {
                return Err(StateStoreError::InvalidLeaseTtl(remaining));
            }
            let (written, _) = self.publish_instance(status.clone(), remaining).await?;
            self.reconcile_membership_inner(membership).await?;
            let cached = self.last_discovery.read().unwrap_or_else(|e| e.into_inner());
            let serving = cached.as_ref().is_some_and(|(view, _)| view.workers.iter().any(|worker|
                worker.member.worker_id == status.node.id.0
                    && matches!(&worker.state, talon_core::worker_membership::InstanceState::Serving { instance_id, .. }
                        if instance_id == &status.incarnation_id)));
            if written.disposition == crate::WriteDisposition::Applied {
                self.heartbeats.lock().unwrap_or_else(|e| e.into_inner())
                    .admit(status, ttl, received, serving);
            }
            Ok((serving, true))
        }).await.unwrap_or_else(|_| Err(StateStoreError::Timeout { backend: self.store.backend() }));
        if result.is_err() {
            self.record_state_store_failure();
        }
        result
    }

    fn check_cached_sequence(&self, status: &NodeStatus) -> StateStoreResult<()> {
        if self
            .heartbeats
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_stale(status)
        {
            return Err(crate::state_store::registry::invalid(
                self.store.backend(),
                "stale instance heartbeat",
            ));
        }
        Ok(())
    }

    /// Publish coalesced reports independently of discovery reconciliation.
    /// Backend latency here must not block snapshot reads or new admissions.
    pub async fn publish_buffered_instances(&self) -> StateStoreResult<()> {
        let result = async {
            let registry = self.worker_registry().await?.value;
            self.flush_heartbeats(&registry).await
        }
        .await;
        if result.is_err() {
            self.record_state_store_failure();
        }
        result
    }

    async fn flush_heartbeats(
        &self,
        registry: &talon_core::worker_membership::MemberRegistry,
    ) -> StateStoreResult<()> {
        // One latest report per locally admitted instance, not a task per
        // heartbeat. Keep backend concurrency bounded independently of N.
        let pending = self
            .heartbeats
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pending(registry);
        let mut pending = pending.into_iter();
        let mut writes = tokio::task::JoinSet::new();
        loop {
            while writes.len() < 16 {
                let Some(heartbeat) = pending.next() else {
                    break;
                };
                let store = self.store.clone();
                let request_timeout = self.request_timeout;
                writes.spawn(async move {
                    let result = tokio::time::timeout(request_timeout, async {
                        let remaining = heartbeat.ttl.saturating_sub(heartbeat.received.elapsed());
                        if remaining.is_zero() {
                            return Ok(None);
                        }
                        let written = store
                            .upsert_instance(heartbeat.status.clone(), remaining)
                            .await?;
                        if written.disposition == crate::WriteDisposition::Stale {
                            return Ok(None);
                        }
                        let remaining = heartbeat.ttl.saturating_sub(heartbeat.received.elapsed());
                        if !remaining.is_zero() {
                            let mut status = heartbeat.status.clone();
                            status.ready &= heartbeat.serving;
                            store.upsert_node(status, remaining).await?;
                        }
                        Ok(Some(()))
                    })
                    .await
                    .unwrap_or_else(|_| {
                        Err(StateStoreError::Timeout {
                            backend: store.backend(),
                        })
                    });
                    (heartbeat, result)
                });
            }
            let Some(completed) = writes.join_next().await else {
                break;
            };
            let (heartbeat, result) = completed.map_err(|error| StateStoreError::Unavailable {
                backend: self.store.backend(),
                detail: format!("heartbeat publication task failed: {error}"),
            })?;
            let mut cache = self.heartbeats.lock().unwrap_or_else(|e| e.into_inner());
            match result? {
                Some(()) => cache.persisted(&heartbeat),
                None => cache.forget(&heartbeat.status),
            }
        }
        Ok(())
    }

    /// Authoritative targets for control updates, without manufacturing a watch
    /// cursor across the independently read member and instance resources.
    pub async fn serving_workers(&self) -> StateStoreResult<Vec<NodeStatus>> {
        use talon_core::worker_membership::{InstanceState, WorkerDiscovery};
        tokio::time::timeout(self.request_timeout, async {
            let registry = self.store.member_registry(&self.cluster_id).await?.value;
            let instances = self.store.instance_snapshot(&self.cluster_id).await?;
            let view = WorkerDiscovery::retained(&registry, &instances.nodes);
            let serving: std::collections::HashSet<_> = view
                .workers
                .iter()
                .filter_map(|worker| match &worker.state {
                    InstanceState::Serving { instance_id, .. } => {
                        Some((worker.member.worker_id.as_str(), instance_id.as_str()))
                    }
                    _ => None,
                })
                .collect();
            Ok(instances
                .nodes
                .into_iter()
                .filter(|status| {
                    serving.contains(&(status.node.id.0.as_str(), status.incarnation_id.as_str()))
                })
                .collect())
        })
        .await
        .map_err(|_| StateStoreError::Timeout {
            backend: self.store.backend(),
        })?
    }

    pub async fn report_instance(
        &self,
        status: NodeStatus,
        ttl: Duration,
    ) -> StateStoreResult<bool> {
        self.publish_instance(status, ttl)
            .await
            .map(|(_, serving)| serving)
    }

    async fn publish_instance(
        &self,
        mut status: NodeStatus,
        ttl: Duration,
    ) -> StateStoreResult<(WriteResult, bool)> {
        use crate::state_store::registry::{change, MemberChange};
        use talon_core::worker_membership::InstanceState;
        if status.cluster_id != self.cluster_id || status.node.role != NodeRole::Worker {
            return Err(crate::state_store::registry::invalid(
                self.store.backend(),
                "invalid worker instance cluster or role",
            ));
        }
        status.validate()?;
        let worker_id = status.node.id.0.clone();
        let incarnation = status.incarnation_id.clone();
        tokio::time::timeout(self.request_timeout, async {
            change(self.store.as_ref(), &self.cluster_id, MemberChange::Register {
                worker_id: worker_id.clone(), zone: status.labels.get(talon_core::NODE_ZONE_LABEL).cloned(),
            }).await?;
            let result = self.store.upsert_instance(status.clone(), ttl).await?;
            if !matches!(result.disposition, crate::WriteDisposition::Applied | crate::WriteDisposition::Duplicate) {
                return Err(crate::state_store::registry::invalid(self.store.backend(), "stale instance heartbeat"));
            }
            let registry = self.store.member_registry(&self.cluster_id).await?.value;
            let instances = self.store.instance_snapshot(&self.cluster_id).await?;
            let view = talon_core::worker_membership::WorkerDiscovery::retained(&registry, &instances.nodes);
            let serving = view.workers.iter().any(|w| w.member.worker_id == worker_id && matches!(&w.state, InstanceState::Serving { instance_id, .. } if instance_id == &incarnation));
            // Keep the single-node management projection current too. Routing
            // uses the persistent registry and instance leases; management APIs
            // retain their consistent node snapshot and backend revision.
            status.ready &= serving;
            self.store.upsert_node(status, ttl).await?;
            Ok((result, serving))
        }).await.map_err(|_| StateStoreError::Timeout { backend: self.store.backend() })?
    }

    pub async fn worker_registry(
        &self,
    ) -> StateStoreResult<crate::state_store::registry::RegistrySnapshot> {
        tokio::time::timeout(
            self.request_timeout,
            self.store.member_registry(&self.cluster_id),
        )
        .await
        .map_err(|_| StateStoreError::Timeout {
            backend: self.store.backend(),
        })?
    }

    pub async fn update_worker_registry(
        &self,
        expected_revision: Option<&str>,
        mut value: talon_core::worker_membership::MemberRegistry,
    ) -> StateStoreResult<bool> {
        tokio::time::timeout(self.request_timeout, async {
            let current = self.store.member_registry(&self.cluster_id).await?;
            if current.revision.as_ref().map(|r| r.as_str()) != expected_revision { return Ok(false); }
            if current.value.members.iter().any(|old| !value.members.iter().any(|new| old.worker_id == new.worker_id)) {
                return Err(crate::state_store::registry::invalid(self.store.backend(), "retain all member records; retire instead of deleting"));
            }
            let instances = self.store.instance_snapshot(&self.cluster_id).await?;
            if value.members.iter().any(|m| m.retired && instances.nodes.iter().any(|n| n.node.id.0 == m.worker_id && n.ready)) {
                return Err(crate::state_store::registry::invalid(self.store.backend(), "stop worker and wait for instance withdrawal/expiration before retirement"));
            }
            value.members.sort_by(|a,b| a.worker_id.cmp(&b.worker_id));
            let applied = self.store.compare_member_registry(&self.cluster_id, current.revision.as_ref(), &value).await?;
            if applied { tracing::info!(cluster = %self.cluster_id, ?value, "administrative membership update"); }
            Ok(applied)
        }).await.map_err(|_| StateStoreError::Timeout { backend: self.store.backend() })?
    }

    /// Reconcile local membership from an authoritative store snapshot.
    ///
    /// This is what makes coordinators active-active: the node set consulted by
    /// placement is derived from shared state, not from whichever heartbeats
    /// happened to land on this process. A worker registered through any
    /// coordinator becomes visible through every coordinator once it reconciles.
    ///
    /// Every non-retired worker remains a logical placement member. Only a sole
    /// healthy, ready instance contributes a serving address; coordinator records
    /// remain management-only. On a store error the local membership is left
    /// untouched (last-good). A transient store error keeps that snapshot usable
    /// only for the configured grace; after it expires readiness fails closed.
    pub async fn reconcile_membership(
        &self,
        membership: &crate::Membership,
    ) -> StateStoreResult<()> {
        let _refresh = self.membership_refresh.lock().await;
        self.reconcile_membership_inner(membership).await
    }

    async fn reconcile_membership_inner(
        &self,
        membership: &crate::Membership,
    ) -> StateStoreResult<()> {
        let result = async {
            // Refresh discovery independently of pending publication work.
            let registry = self.worker_registry().await?.value;
            let observed = Instant::now();
            let instances = tokio::time::timeout(
                self.request_timeout,
                self.store.instance_snapshot(&self.cluster_id),
            )
            .await
            .map_err(|_| StateStoreError::Timeout {
                backend: self.store.backend(),
            })??;
            let view = talon_core::worker_membership::WorkerDiscovery::retained(
                &registry,
                &instances.nodes,
            );
            self.refresh_snapshot().await?;
            self.metrics.update_worker_discovery(&view);
            self.heartbeats
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .install(&view, &instances.nodes, observed);
            let cached = view.clone();
            let workers = view
                .workers
                .into_iter()
                .map(|w| {
                    let address = match w.state {
                        talon_core::worker_membership::InstanceState::Serving {
                            address, ..
                        } => address,
                        _ => String::new(),
                    };
                    (
                        NodeInfo {
                            id: talon_core::NodeId::new(w.member.worker_id),
                            address,
                            role: NodeRole::Worker,
                        },
                        w.member.zone,
                    )
                })
                .collect();
            membership.reconcile_zoned(workers);
            *self
                .last_discovery
                .write()
                .unwrap_or_else(|e| e.into_inner()) = Some((cached, observed));
            self.record_membership_refresh();
            Ok(())
        }
        .await;
        if result.is_err() {
            self.record_state_store_failure();
        }
        result
    }

    /// Fetch a linearizable snapshot for the management API, updating freshness
    /// gauges and readiness. Unlike [`refresh_snapshot`](Self::refresh_snapshot)
    /// this returns the snapshot so a handler can render it.
    pub async fn snapshot_for_api(&self) -> StateStoreResult<ClusterSnapshot> {
        let started = Instant::now();
        let result =
            match tokio::time::timeout(self.request_timeout, self.store.snapshot(&self.cluster_id))
                .await
            {
                Ok(result) => result,
                Err(_) => Err(StateStoreError::Timeout {
                    backend: self.store.backend(),
                }),
            };
        self.metrics
            .record_state("snapshot", &result, started.elapsed());
        match &result {
            Ok(snapshot) => {
                self.metrics.update_snapshot(snapshot);
            }
            Err(_) => self.record_state_store_failure(),
        }
        result
    }

    /// Shared stop signal; listener owners join their accepted work.
    pub fn shutdown(&self) -> &Arc<talon_transport::shutdown::Shutdown> {
        &self.shutdown
    }

    /// Begin graceful shutdown: mark this coordinator not-live/not-ready so new
    /// authoritative reads fail closed while in-flight ones drain.
    pub fn begin_shutdown(&self) {
        self.shutdown.begin();
        self.shutting_down.store(true, Ordering::Release);
        self.ready.store(false, Ordering::Release);
    }

    /// Remove this coordinator's own lease from shared state on shutdown, so a
    /// crashed-or-draining coordinator disappears from the cluster view without
    /// waiting for lease expiry. Best-effort and deadline-bounded.
    pub async fn remove_self(&self) -> StateStoreResult<WriteResult> {
        let started = Instant::now();
        let result = match tokio::time::timeout(
            self.request_timeout,
            self.store
                .remove_node(&self.cluster_id, &self.node.id, &self.incarnation_id),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(StateStoreError::Timeout {
                backend: self.store.backend(),
            }),
        };
        self.metrics
            .record_state("upsert", &result, started.elapsed());
        result
    }

    /// Build the coordinator's own leased status record.
    pub fn status(&self) -> NodeStatus {
        let (requests_total, errors_total) = self.metrics.totals();
        NodeStatus {
            schema_version: NODE_STATUS_SCHEMA_VERSION,
            cluster_id: self.cluster_id.clone(),
            node: self.node.clone(),
            incarnation_id: self.incarnation_id.clone(),
            admin_address: Some(self.admin_address.clone()),
            build_version: env!("CARGO_PKG_VERSION").into(),
            started_at_unix_ms: self.started_at_unix_ms,
            reported_at_unix_ms: now_unix_ms().max(self.started_at_unix_ms),
            heartbeat_seq: self.sequence.fetch_add(1, Ordering::Relaxed),
            health: if self.is_ready() {
                NodeHealth::Healthy
            } else {
                NodeHealth::Degraded
            },
            ready: self.is_ready(),
            metrics: NodeMetricsSnapshot {
                requests_total,
                errors_total,
                state_snapshot_age_ms: self.metrics.snapshot_age_value.load(Ordering::Relaxed),
                ..Default::default()
            },
            labels: BTreeMap::new(),
        }
    }
}

fn generate_incarnation_id() -> std::io::Result<String> {
    let mut bytes = [0u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    let mut id = String::with_capacity(32);
    for byte in bytes {
        write!(id, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(id)
}

#[cfg(test)]
pub(crate) fn observability() -> (Arc<CoordinatorObservability>, Arc<MemoryStateStore>) {
    observability_with_state_failure_grace(Duration::ZERO)
}

#[cfg(test)]
pub(crate) fn observability_with_state_failure_grace(
    grace: Duration,
) -> (Arc<CoordinatorObservability>, Arc<MemoryStateStore>) {
    let store = Arc::new(MemoryStateStore::new());
    let state_store: Arc<dyn ClusterStateStore> = store.clone();
    let observability = Arc::new(
        CoordinatorObservability::new(
            "cluster-a".into(),
            NodeInfo {
                id: NodeId::new("coordinator-1"),
                address: "127.0.0.1:7000".into(),
                role: NodeRole::Coordinator,
            },
            "127.0.0.1:8000".into(),
            Duration::from_millis(100),
            state_store,
        )
        .unwrap()
        .with_state_failure_grace(grace),
    );
    (observability, store)
}

#[cfg(test)]
pub(crate) fn worker_status() -> NodeStatus {
    let now = now_unix_ms();
    NodeStatus {
        schema_version: NODE_STATUS_SCHEMA_VERSION,
        cluster_id: "cluster-a".into(),
        node: NodeInfo {
            id: NodeId::new("worker-1"),
            address: "127.0.0.1:7001".into(),
            role: NodeRole::Worker,
        },
        incarnation_id: "worker-incarnation".into(),
        admin_address: Some("127.0.0.1:8001".into()),
        build_version: "test".into(),
        started_at_unix_ms: now,
        reported_at_unix_ms: now,
        heartbeat_seq: 0,
        health: NodeHealth::Healthy,
        ready: true,
        metrics: NodeMetricsSnapshot::default(),
        labels: BTreeMap::new(),
    }
}

#[cfg(test)]
mod capability_tests {
    use super::*;
    use crate::MemoryStateStore;
    use async_trait::async_trait;
    use talon_core::{NodeId, NodeRole};
    use talon_metadata::{
        BackendHealth, Capability, CapabilityRevision, CapabilitySet, InodeNumber, InodeRecord,
        MappingRevision, MetadataBackend, MetadataError, MetadataResult, NamespaceId,
        PathIndexEntry, Transaction, TransactionOutcome,
    };

    /// A store whose reachability the test controls.
    struct FlakyStore {
        ready: AtomicBool,
    }

    #[async_trait]
    impl MetadataStore for FlakyStore {
        fn backend(&self) -> MetadataBackend {
            MetadataBackend::Memory
        }

        fn capabilities(&self) -> CapabilitySet {
            CapabilitySet::none().with(Capability::HardLinks)
        }

        async fn check_ready(&self) -> MetadataResult<BackendHealth> {
            if self.ready.load(Ordering::Acquire) {
                Ok(BackendHealth {
                    ready: true,
                    detail: "up".to_owned(),
                })
            } else {
                Err(MetadataError::Unavailable {
                    backend: MetadataBackend::Memory,
                    detail: "simulated outage".to_owned(),
                })
            }
        }

        async fn mapping_revision(
            &self,
            _namespace: &NamespaceId,
        ) -> MetadataResult<MappingRevision> {
            Ok(MappingRevision::INITIAL)
        }

        async fn resolve_path(
            &self,
            _namespace: &NamespaceId,
            _path: &str,
        ) -> MetadataResult<Option<PathIndexEntry>> {
            Ok(None)
        }

        async fn load_inode(
            &self,
            _namespace: &NamespaceId,
            inode: InodeNumber,
        ) -> MetadataResult<InodeRecord> {
            Err(MetadataError::NotFound {
                key: format!("inode/{inode}"),
            })
        }

        async fn commit(&self, _transaction: &Transaction) -> MetadataResult<TransactionOutcome> {
            Err(MetadataError::Unavailable {
                backend: MetadataBackend::Memory,
                detail: "not used in this test".to_owned(),
            })
        }
    }

    fn observability(store: Arc<dyn MetadataStore>) -> CoordinatorObservability {
        CoordinatorObservability::new(
            "c".into(),
            NodeInfo {
                id: NodeId::new("coord"),
                address: "coord:7000".into(),
                role: NodeRole::Coordinator,
            },
            "coord:8000".into(),
            Duration::from_secs(1),
            Arc::new(MemoryStateStore::new()),
        )
        .expect("observability")
        .with_capabilities(ClusterCapabilities {
            advertised: CapabilitySet::none().with(Capability::HardLinks),
            revision: CapabilityRevision::new(3),
            store_reachable: true,
        })
        .with_metadata_store(store)
    }

    #[tokio::test]
    async fn reachability_tracks_the_present_not_startup() {
        // ADR 0003 §6 requires "not configured" and "configured but unreachable"
        // to be separable *during an incident* -- which is exactly when a value
        // sampled once at startup is stale.
        let store = Arc::new(FlakyStore {
            ready: AtomicBool::new(true),
        });
        let obs = observability(store.clone());
        obs.check_ready().await.expect("cluster state is ready");
        assert!(obs.capabilities().store_reachable);

        store.ready.store(false, Ordering::Release);
        obs.check_ready()
            .await
            .expect("cluster state is still ready");
        assert!(
            !obs.capabilities().store_reachable,
            "an outage after startup must be visible"
        );

        store.ready.store(true, Ordering::Release);
        obs.check_ready().await.expect("cluster state is ready");
        assert!(
            obs.capabilities().store_reachable,
            "recovery must be visible too"
        );
    }

    #[tokio::test]
    async fn a_store_outage_does_not_advance_the_capability_revision() {
        // A blip is not a reconfiguration. Advancing the revision here would
        // invalidate every client's cached capability set on every outage, and
        // would make the revision useless as a change signal.
        let store = Arc::new(FlakyStore {
            ready: AtomicBool::new(true),
        });
        let obs = observability(store.clone());
        obs.check_ready().await.expect("ready");
        let before = obs.capabilities().revision;

        store.ready.store(false, Ordering::Release);
        obs.check_ready().await.expect("ready");

        assert_eq!(obs.capabilities().revision, before);
        assert_eq!(
            obs.capabilities().advertised,
            CapabilitySet::none().with(Capability::HardLinks),
            "an unreachable store still advertises what it offers"
        );
    }

    #[tokio::test]
    async fn a_metadata_outage_does_not_make_the_cluster_unready() {
        // §6: "TMS unavailability degrades TMS-backed features; it must not
        // affect the read path." Readiness gates the read path, so a metadata
        // outage must not turn the coordinator unready.
        let store = Arc::new(FlakyStore {
            ready: AtomicBool::new(false),
        });
        let obs = observability(store);
        obs.check_ready()
            .await
            .expect("a metadata outage must not fail cluster readiness");
        assert!(obs.is_ready(), "the read path stays available");
        assert!(!obs.capabilities().store_reachable);
    }

    #[tokio::test]
    async fn a_cluster_without_a_metadata_store_stays_reachable() {
        // An empty capability set is always serviceable: there is nothing that
        // could be down, so reporting it unreachable would invent an outage.
        let obs = CoordinatorObservability::new(
            "c".into(),
            NodeInfo {
                id: NodeId::new("coord"),
                address: "coord:7000".into(),
                role: NodeRole::Coordinator,
            },
            "coord:8000".into(),
            Duration::from_secs(1),
            Arc::new(MemoryStateStore::new()),
        )
        .expect("observability");
        obs.check_ready().await.expect("ready");
        assert!(obs.capabilities().store_reachable);
        assert!(obs.capabilities().advertised.is_empty());
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use talon_core::NodeId;

    use super::*;

    #[tokio::test]
    async fn warm_heartbeats_and_discovery_do_not_access_the_backend() {
        let (obs, store) = observability_with_state_failure_grace(Duration::from_secs(15));
        let membership = crate::Membership::new();
        let ttl = Duration::from_secs(30);
        let mut status = worker_status();
        assert_eq!(
            obs.receive_instance(status.clone(), ttl, &membership)
                .await
                .unwrap(),
            (true, true)
        );
        let before = store.instance_snapshot("cluster-a").await.unwrap();
        // Any attempted backend access now fails. Warm requests must succeed
        // entirely in memory, without swallowing an I/O error or marking the
        // coordinator degraded.
        store.set_available(false);
        for seq in 1..=100 {
            status.heartbeat_seq = seq;
            status.metrics.block_count = seq;
            assert_eq!(
                obs.receive_instance(status.clone(), ttl, &membership)
                    .await
                    .unwrap(),
                (true, false)
            );
            assert_eq!(obs.membership_for_query().await.unwrap().workers.len(), 1);
        }
        assert!(!obs.state_store_degraded.load(Ordering::Acquire));
        status.heartbeat_seq -= 1;
        assert!(obs
            .receive_instance(status.clone(), ttl, &membership)
            .await
            .is_err());
        store.set_available(true);
        assert_eq!(
            store.instance_snapshot("cluster-a").await.unwrap().nodes,
            before.nodes
        );
    }

    #[tokio::test]
    async fn periodic_publication_coalesces_reports_and_reaches_other_coordinators() {
        let (obs, store) = observability_with_state_failure_grace(Duration::from_secs(15));
        let membership = crate::Membership::new();
        let ttl = Duration::from_secs(30);
        let mut status = worker_status();
        obs.receive_instance(status.clone(), ttl, &membership)
            .await
            .unwrap();
        // Make the last publication due without sleeping or changing the
        // backend clock; receipt of new heartbeats remains current.
        obs.heartbeats.lock().unwrap().admit(
            status.clone(),
            ttl,
            Instant::now() - Duration::from_secs(11),
            true,
        );
        for seq in 1..=100 {
            status.heartbeat_seq = seq;
            status.metrics.block_count = seq;
            assert_eq!(
                obs.receive_instance(status.clone(), ttl, &membership)
                    .await
                    .unwrap(),
                (true, false)
            );
        }
        assert_eq!(
            store.instance_snapshot("cluster-a").await.unwrap().nodes[0].heartbeat_seq,
            0
        );
        obs.publish_buffered_instances().await.unwrap();
        obs.reconcile_membership(&membership).await.unwrap();
        let published = store.instance_snapshot("cluster-a").await.unwrap();
        assert_eq!(published.nodes[0].heartbeat_seq, 100);
        assert_eq!(
            store.snapshot("cluster-a").await.unwrap().nodes[0]
                .metrics
                .block_count,
            100
        );
        obs.publish_buffered_instances().await.unwrap();
        obs.reconcile_membership(&membership).await.unwrap();
        assert_eq!(
            store.instance_snapshot("cluster-a").await.unwrap().revision,
            published.revision,
            "a reconcile with no new report must not renew a lease"
        );

        let peer = CoordinatorObservability::new(
            "cluster-a".into(),
            obs.node.clone(),
            obs.admin_address.clone(),
            Duration::from_secs(1),
            store.clone(),
        )
        .unwrap();
        peer.reconcile_membership(&crate::Membership::new())
            .await
            .unwrap();
        assert_eq!(
            peer.membership_for_query().await.unwrap().workers,
            obs.membership_for_query().await.unwrap().workers
        );
        // A Service can route the next heartbeat to a different replica. Its
        // shared observation must already warm the instance fast path.
        store.set_available(false);
        status.heartbeat_seq += 1;
        assert_eq!(
            peer.receive_instance(status, ttl, &crate::Membership::new())
                .await
                .unwrap(),
            (true, false)
        );
    }

    #[tokio::test]
    async fn admission_queue_wait_is_part_of_the_request_deadline() {
        let (obs, store) = observability();
        let _blocked = obs.membership_refresh.lock().await;
        let error = obs
            .receive_instance(
                worker_status(),
                Duration::from_secs(30),
                &crate::Membership::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, StateStoreError::Timeout { .. }));
        assert!(store
            .instance_snapshot("cluster-a")
            .await
            .unwrap()
            .nodes
            .is_empty());
    }

    #[tokio::test]
    async fn cached_queries_do_not_refresh_observation_age() {
        let (obs, store) = observability_with_state_failure_grace(Duration::from_secs(15));
        let membership = crate::Membership::new();
        obs.receive_instance(worker_status(), Duration::from_secs(30), &membership)
            .await
            .unwrap();
        obs.last_discovery.write().unwrap().as_mut().unwrap().1 -= Duration::from_secs(16);
        assert!(
            !obs.is_ready(),
            "a stalled refresh task must expire even without a reported backend error"
        );
        assert!(obs.membership_for_query().await.is_err());
        obs.check_ready().await.unwrap();
        assert!(
            obs.membership_for_query().await.is_err(),
            "health probes cannot refresh discovery"
        );
        store.set_available(false);
        let mut status = worker_status();
        status.heartbeat_seq += 1;
        assert!(obs
            .receive_instance(status, Duration::from_secs(30), &membership)
            .await
            .is_err());
        store.set_available(true);
        obs.reconcile_membership(&membership).await.unwrap();
        assert!(obs.membership_for_query().await.is_ok());
    }

    #[tokio::test]
    async fn state_changes_require_publication_and_invalidate_cached_grants() {
        let (obs, store) = observability_with_state_failure_grace(Duration::from_secs(15));
        let membership = crate::Membership::new();
        let ttl = Duration::from_secs(30);
        let mut first = worker_status();
        obs.receive_instance(first.clone(), ttl, &membership)
            .await
            .unwrap();
        let mut second = first.clone();
        second.incarnation_id = "second".into();
        assert_eq!(
            obs.receive_instance(second.clone(), ttl, &membership)
                .await
                .unwrap(),
            (false, true)
        );
        first.heartbeat_seq += 1;
        assert_eq!(
            obs.receive_instance(first.clone(), ttl, &membership)
                .await
                .unwrap(),
            (false, false)
        );
        second.heartbeat_seq += 1;
        second.ready = false;
        assert_eq!(
            obs.receive_instance(second, ttl, &membership)
                .await
                .unwrap(),
            (false, true)
        );
        first.heartbeat_seq += 1;
        assert_eq!(
            obs.receive_instance(first.clone(), ttl, &membership)
                .await
                .unwrap(),
            (true, false)
        );
        first.heartbeat_seq += 1;
        first.node.address = "127.0.0.1:9001".into();
        store.set_available(false);
        assert!(
            obs.receive_instance(first, ttl, &membership).await.is_err(),
            "a cached grant must never approve an unconfirmed address"
        );
    }

    #[tokio::test]
    async fn reconcile_retains_unavailable_owners_without_serving_addresses() {
        use crate::Membership;
        let (observability, _store) = observability();
        observability.check_ready().await.unwrap();

        // A healthy+ready worker, an unhealthy worker, and a not-ready worker.
        let healthy = worker_status();
        let mut unhealthy = worker_status();
        unhealthy.node.id = NodeId::new("worker-unhealthy");
        unhealthy.incarnation_id = "inc-unhealthy".into();
        unhealthy.health = NodeHealth::Unhealthy;
        let mut not_ready = worker_status();
        not_ready.node.id = NodeId::new("worker-not-ready");
        not_ready.incarnation_id = "inc-not-ready".into();
        not_ready.ready = false;

        for status in [healthy, unhealthy, not_ready] {
            observability
                .upsert_status(status, Duration::from_secs(30))
                .await
                .unwrap();
        }

        let membership = Membership::new();
        observability
            .reconcile_membership(&membership)
            .await
            .unwrap();

        // All logical owners remain; only the healthy, ready instance is dialable.
        let mut nodes = membership.snapshot();
        nodes.sort_by(|a, b| a.id.0.cmp(&b.id.0));
        assert_eq!(nodes.len(), 3);
        assert_eq!(nodes[0].id.0, "worker-1");
        assert!(!nodes[0].address.is_empty());
        assert!(nodes[1..].iter().all(|node| node.address.is_empty()));
    }

    #[tokio::test]
    async fn readiness_and_status_use_shared_state() {
        let (observability, store) = observability();
        observability.check_ready().await.unwrap();
        assert!(observability.is_ready());

        let status = observability.status();
        status.validate().unwrap();
        assert!(status.ready);
        observability
            .upsert_status(status, Duration::from_secs(30))
            .await
            .unwrap();
        observability
            .upsert_status(worker_status(), Duration::from_secs(30))
            .await
            .unwrap();
        observability.refresh_snapshot().await.unwrap();

        let rendered = observability.metrics.render();
        assert!(rendered
            .contains("talon_coordinator_live_nodes{health=\"healthy\",role=\"coordinator\"} 1"));
        assert!(
            rendered.contains("talon_coordinator_live_nodes{health=\"healthy\",role=\"worker\"} 1")
        );

        store.set_available(false);
        assert!(observability.check_ready().await.is_err());
        assert!(!observability.is_ready());
        assert!(observability.metrics.render().contains(
            "talon_coordinator_state_store_errors_total{kind=\"unavailable\",operation=\"readiness\"} 1"
        ));
    }

    #[tokio::test]
    async fn transient_store_failure_uses_bounded_last_good_membership() {
        use crate::Membership;

        let grace = Duration::from_millis(100);
        let (observability, store) = observability_with_state_failure_grace(grace);
        observability.check_ready().await.unwrap();
        observability
            .upsert_status(worker_status(), Duration::from_secs(30))
            .await
            .unwrap();
        let membership = Membership::new();
        observability
            .reconcile_membership(&membership)
            .await
            .unwrap();
        assert!(observability.is_ready());

        store.set_available(false);
        assert!(observability
            .reconcile_membership(&membership)
            .await
            .is_err());
        assert!(
            observability.is_ready(),
            "one failed refresh must retain a recent last-good membership"
        );

        tokio::time::sleep(grace + Duration::from_millis(50)).await;
        assert!(
            !observability.is_ready(),
            "last-good membership must fail closed after its grace expires"
        );

        store.set_available(true);
        observability.check_ready().await.unwrap();
        assert!(
            !observability.is_ready(),
            "a health probe cannot make stale local membership authoritative"
        );
        observability.snapshot_for_api().await.unwrap();
        observability
            .upsert_status(worker_status(), Duration::from_secs(30))
            .await
            .unwrap();
        assert!(
            !observability.is_ready(),
            "other successful operations must not renew membership"
        );
        observability
            .reconcile_membership(&membership)
            .await
            .unwrap();
        assert!(observability.is_ready());
        observability.begin_shutdown();
        assert!(
            !observability.is_ready(),
            "grace must never override shutdown"
        );
    }

    #[tokio::test]
    async fn no_installed_membership_means_no_failure_grace() {
        let (observability, store) =
            observability_with_state_failure_grace(Duration::from_secs(15));
        observability.check_ready().await.unwrap();
        store.set_available(false);
        assert!(observability.check_ready().await.is_err());
        assert!(!observability.is_ready());
    }
}
