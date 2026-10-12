//! Tenant namespaces own their cache state; placement continues to hash BlockId.
use super::*;
use crate::page_access_store::CacheRootLock;
use std::{collections::BTreeMap, path::Path, sync::RwLock};
use talon_core::{TenantId, WorkerConfig, DEFAULT_TENANT};

/// Bounds disk namespaces and runtime metadata created by declared identities.
const MAX_TENANTS: usize = 4096;

pub(super) struct TenantRuntimes {
    workers: RwLock<BTreeMap<String, Arc<WorkerRuntime>>>,
    initialize: tokio::sync::Mutex<()>,
    initializers: Arc<Mutations>,
    config: WorkerConfig,
    root_lock: Arc<CacheRootLock>,
    backend: Arc<dyn BackendStore>,
    metrics: WorkerMetrics,
    limiter: Arc<TenantRateLimiter>,
    load_slots: Arc<tokio::sync::Semaphore>,
    load_bytes: Arc<tokio::sync::Semaphore>,
    l1_budget: Arc<std::sync::atomic::AtomicU64>,
    background_budget: Arc<crate::background::BackgroundBudget>,
}

impl TenantRuntimes {
    pub(super) fn snapshot(&self) -> Vec<Arc<WorkerRuntime>> {
        self.workers.read().unwrap().values().cloned().collect()
    }

    fn open(self: &Arc<Self>, tenant: &TenantId) -> anyhow::Result<WorkerRuntime> {
        let config = self.config.for_tenant(tenant)?;
        let tenant_root = self.root_lock.root.join(tenant.effective_name());
        checked_dir(&tenant_root)?;
        let root = tenant_root.join("shared");
        checked_dir(&root)?;
        // Persist geometry independently for each tenant. A changed page size
        // must never reinterpret existing page indices on restart.
        let identity_path = root.join("cache_geometry.json");
        let geometry = (config.block_size, config.l2_page_size_bytes);
        match std::fs::read(&identity_path) {
            Ok(bytes) => anyhow::ensure!(
                serde_json::from_slice::<(u32, u64)>(&bytes)? == geometry,
                "cache geometry changed for tenant {}; use a new empty tenant cache directory",
                tenant.effective_name()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                use std::io::Write;
                let temp = root.join(".cache_geometry.tmp");
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&temp)?;
                file.write_all(&serde_json::to_vec(&geometry)?)?;
                file.sync_all()?;
                std::fs::rename(temp, &identity_path)?;
                std::fs::File::open(&root)?.sync_all()?;
            }
            Err(error) => return Err(error.into()),
        }
        let store = WholeBlockStore::open(&root)?.with_root_lock(self.root_lock.clone());
        let index = Arc::new(BlockIndex::new());
        let paged = if config.l2_page_size_bytes > 0 {
            checked_dir(&root.join("paged"))?;
            Some(
                PagedBlockStore::open(root.join("paged"), config.l2_page_size_bytes as u32)?
                    .with_root_lock(self.root_lock.clone()),
            )
        } else {
            None
        };
        for meta in store.scan()? {
            index.commit(meta);
        }
        if let Some(paged) = &paged {
            for meta in paged.scan()? {
                index.commit(meta);
            }
        }
        let mut worker = WorkerRuntime::new_with_l1(
            store,
            index,
            Arc::new(InFlightLoads::new()),
            self.backend.clone(),
            config.block_size,
            config.capacity_bytes,
            config.l1_capacity_bytes,
            config.l1_page_size_bytes,
            self.metrics.clone(),
        )
        .with_backend_kind(config.backend.as_deref().unwrap_or("azure").parse()?)
        .with_rate_limiter(self.limiter.clone())
        .with_paged_miss_run_concurrency(config.paged_miss_run_concurrency);
        worker.l1 = Arc::new(MemoryStore::with_shared_budget(
            config.l1_capacity_bytes,
            config.l1_page_size_bytes,
            self.l1_budget.clone(),
        ));
        if let Some(paged) = paged {
            worker = worker.with_paged_store(paged);
        }
        let mut gc = PageGcConfig::from(&config);
        gc.scan_batch_size = gc.scan_batch_size.min(config.background_scan_batch_size);
        gc.delete_batch_size = gc
            .delete_batch_size
            .min(config.background_delete_batch_size);
        worker = worker.with_page_gc(gc)?;
        worker.tenant = tenant.normalized();
        worker.tenant_parent = Arc::downgrade(self);
        worker.load_slots = self.load_slots.clone();
        worker.load_bytes = self.load_bytes.clone();
        worker.load_inflight_bytes = config.load_inflight_bytes;
        worker.background_budget = Some(self.background_budget.clone());
        Ok(worker)
    }

    async fn resolve(self: &Arc<Self>, tenant: &TenantId) -> anyhow::Result<Arc<WorkerRuntime>> {
        tenant.validate()?;
        if let Some(worker) = self.workers.read().unwrap().get(tenant.effective_name()) {
            return Ok(worker.clone());
        }
        // The initializer owns publication even when the triggering request is
        // cancelled. Holding this mutex in a detached task prevents duplicate
        // runtimes and competing writes to the same namespace.
        let registry = self.clone();
        let tenant = tenant.normalized();
        self.initializers
            .run(async move {
                let _guard = registry.initialize.lock().await;
                if let Some(worker) = registry
                    .workers
                    .read()
                    .unwrap()
                    .get(tenant.effective_name())
                {
                    return Ok(worker.clone());
                }
                anyhow::ensure!(
                    registry.workers.read().unwrap().len() < MAX_TENANTS,
                    "tenant namespace limit reached"
                );
                let owner = registry.clone();
                let name = tenant.effective_name().to_owned();
                let worker =
                    Arc::new(tokio::task::spawn_blocking(move || owner.open(&tenant)).await??);
                worker.recover_page_file_cleanup().await;
                registry
                    .workers
                    .write()
                    .unwrap()
                    .insert(name, worker.clone());
                Ok::<_, anyhow::Error>(worker)
            })
            .await
    }
}

fn checked_dir(path: &Path) -> anyhow::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) => anyhow::ensure!(
            meta.is_dir() && !meta.file_type().is_symlink(),
            "tenant cache path must be a real directory: {}",
            path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => std::fs::create_dir(path)?,
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

impl WorkerRuntime {
    pub(crate) fn inflight_count(&self) -> u64 {
        self.inflight.len() as u64
    }
    pub(crate) fn page_count(&self) -> u64 {
        self.index.page_count()
    }

    /// Keep streamed PUT staging in the same tenant directory as cached data.
    pub(crate) fn staging_file(&self) -> std::io::Result<tempfile::NamedTempFile> {
        tempfile::NamedTempFile::new_in(self.store.root())
    }

    /// Effective resource owner of this cache runtime.
    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    pub(super) async fn drain_tenant_initializers(&self) {
        if let Some(registry) = self.registry() {
            registry.initializers.drain().await;
        }
    }

    /// Open all persisted and configured tenant namespaces under one locked root.
    pub fn open_tenants(
        config: &WorkerConfig,
        root_lock: Arc<CacheRootLock>,
        backend: Arc<dyn BackendStore>,
        metrics: WorkerMetrics,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let registry = Arc::new(TenantRuntimes {
            workers: RwLock::new(BTreeMap::new()),
            initialize: tokio::sync::Mutex::new(()),
            initializers: Arc::new(Mutations::default()),
            config: config.clone(),
            root_lock,
            backend,
            metrics,
            limiter: Arc::new(TenantRateLimiter::new(config.rate_limits.clone())),
            load_slots: Arc::new(tokio::sync::Semaphore::new(8)),
            load_bytes: Arc::new(tokio::sync::Semaphore::new(
                config.load_inflight_bytes as usize,
            )),
            l1_budget: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            background_budget: crate::background::BackgroundBudget::new(config),
        });
        let mut names = std::collections::BTreeSet::from([DEFAULT_TENANT.to_owned()]);
        names.extend(config.tenants.keys().cloned());
        for entry in std::fs::read_dir(&registry.root_lock.root)? {
            let entry = entry?;
            if entry.file_name() == "lost+found" {
                continue;
            }
            if entry.file_type()?.is_dir() || entry.file_type()?.is_symlink() {
                let name = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| anyhow::anyhow!("invalid tenant directory"))?;
                // Never silently strand an old flat cache or adopt it as a tenant.
                anyhow::ensure!(entry.path().join("shared").is_dir() || (entry.file_type()?.is_dir() && std::fs::read_dir(entry.path())?.next().is_none()), "legacy/unrecognized cache directory {}; configure a new cache root for tenant storage", entry.path().display());
                names.insert(name);
            }
        }
        anyhow::ensure!(names.len() <= MAX_TENANTS, "too many tenant namespaces");
        for name in names {
            let tenant = TenantId::named(&name);
            let worker = Arc::new(registry.open(&tenant)?);
            registry.workers.write().unwrap().insert(name, worker);
        }
        let mut worker = (**registry
            .workers
            .read()
            .unwrap()
            .get(DEFAULT_TENANT)
            .unwrap())
        .clone();
        worker.tenants = Some(registry);
        Ok(worker)
    }

    /// Resolve an explicit identity, inheriting the global cache policy when absent.
    pub async fn for_tenant(self: &Arc<Self>, tenant: &TenantId) -> anyhow::Result<Arc<Self>> {
        tenant.validate()?;
        if let Some(registry) = self
            .tenants
            .as_ref()
            .cloned()
            .or_else(|| self.tenant_parent.upgrade())
        {
            registry.resolve(tenant).await
        } else {
            anyhow::ensure!(
                tenant.effective_name() == self.tenant.effective_name(),
                "runtime has no tenant namespace registry"
            );
            Ok(self.clone())
        }
    }

    /// Snapshot used by maintenance and aggregate process metrics.
    pub fn tenant_runtimes(self: &Arc<Self>) -> Vec<Arc<Self>> {
        match self
            .tenants
            .as_ref()
            .cloned()
            .or_else(|| self.tenant_parent.upgrade())
        {
            Some(registry) => registry.snapshot(),
            None => vec![self.clone()],
        }
    }

    pub(super) fn registry(&self) -> Option<Arc<TenantRuntimes>> {
        self.tenants
            .as_ref()
            .cloned()
            .or_else(|| self.tenant_parent.upgrade())
    }

    /// Resolve legacy read carriers and v3 metadata consistently for every operation.
    pub async fn for_request(
        self: &Arc<Self>,
        header: &talon_transport::FrameHeader,
        payload: &[u8],
    ) -> Result<Arc<Self>, talon_transport::DataPlaneError> {
        use talon_transport::{data, DataErrorCode, DataPlaneError, MsgType, HEADER_LEN};
        let tenant = (|| -> anyhow::Result<TenantId> {
            let (metadata, _) = talon_transport::envelope::decode(header, payload)?;
            let mut frame = Vec::new();
            let legacy = if matches!(
                header.msg_type,
                MsgType::GetRangeTenant
                    | MsgType::GetCachedRangeTenant
                    | MsgType::GetVersionedRangeTenant
            ) {
                frame.reserve(HEADER_LEN + payload.len());
                frame.extend_from_slice(&header.encode());
                frame.extend_from_slice(payload);
                Some(match header.msg_type {
                    MsgType::GetRangeTenant => data::decode_tenant_request(&frame)?.1.tenant,
                    MsgType::GetCachedRangeTenant => {
                        data::decode_cached_tenant_request(&frame)?.1.tenant
                    }
                    _ => data::decode_versioned_tenant_request(&frame)?.1.tenant,
                })
            } else {
                None
            };
            if let (Some(a), Some(b)) = (&metadata.tenant, &legacy) {
                anyhow::ensure!(
                    a.effective_name() == b.effective_name(),
                    "conflicting tenant identities"
                );
            }
            let tenant = metadata.tenant.or(legacy).unwrap_or_default();
            tenant.validate()?;
            Ok(tenant)
        })()
        .map_err(|error| DataPlaneError {
            code: DataErrorCode::InvalidRequest,
            message: error.to_string(),
        })?;
        self.for_tenant(&tenant)
            .await
            .map_err(|error| DataPlaneError {
                code: DataErrorCode::Internal,
                message: error.to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use talon_core::{ObjectStat, TenantCacheConfig};

    #[derive(Default)]
    struct Origin {
        reads: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl BackendStore for Origin {
        async fn put(&self, _: &ObjectId, _: Bytes) -> talon_core::Result<Version> {
            Ok(Version::new("v1"))
        }
        async fn delete(&self, _: &ObjectId) -> talon_core::Result<()> {
            Ok(())
        }
        async fn head(&self, _: &ObjectId) -> talon_core::Result<ObjectStat> {
            Ok(ObjectStat {
                len: 64,
                version: Version::new("v1"),
            })
        }
        async fn fetch_range(
            &self,
            _: &ObjectId,
            offset: u64,
            len: u64,
        ) -> talon_core::Result<Bytes> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            Ok(Bytes::from(
                (offset..offset + len).map(|n| n as u8).collect::<Vec<_>>(),
            ))
        }
        async fn fetch_range_if_match(
            &self,
            object: &ObjectId,
            offset: u64,
            len: u64,
            _: Option<&Version>,
        ) -> talon_core::Result<Bytes> {
            self.fetch_range(object, offset, len).await
        }
    }
    fn config() -> WorkerConfig {
        WorkerConfig {
            block_size: 16,
            capacity_bytes: 1024,
            l1_capacity_bytes: 0,
            l2_page_size_bytes: 4,
            backend: Some("s3".into()),
            page_tti_hours: 0,
            page_access_checkpoint_interval_minutes: 1,
            ..Default::default()
        }
    }
    fn open(root: &Path, config: &WorkerConfig, backend: Arc<Origin>) -> Arc<WorkerRuntime> {
        let lock = Arc::new(CacheRootLock::acquire(root).unwrap());
        Arc::new(
            WorkerRuntime::open_tenants(
                config,
                lock,
                backend,
                WorkerMetrics::new(config.capacity_bytes),
            )
            .unwrap(),
        )
    }
    fn request() -> RangeRequest {
        RangeRequest {
            object: ObjectId::new(Backend::S3, "bucket", "object"),
            offset: 0,
            len: 4,
        }
    }
    fn block(size: u32) -> BlockId {
        BlockId::new(request().object, 0, size, Version::new("v1"))
    }
    async fn read(worker: &WorkerRuntime) {
        worker
            .serve_versioned(&request(), &Version::new("v1"), 64)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn tenants_share_block_hash_but_never_files_indices_or_inflight_fills() {
        for paged in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            let mut config = config();
            if !paged {
                config.l2_page_size_bytes = 0;
            }
            let root = open(dir.path(), &config, origin.clone());
            let (ta, tb) = (TenantId::named("a"), TenantId::named("b"));
            let (a, b) = tokio::join!(root.for_tenant(&ta), root.for_tenant(&tb));
            let (a, b) = (a.unwrap(), b.unwrap());
            tokio::join!(read(&a), read(&b));
            assert_eq!(origin.reads.load(Ordering::SeqCst), 2);
            assert!(!Arc::ptr_eq(&a.index, &b.index));
            assert!(!Arc::ptr_eq(&a.inflight, &b.inflight));
            assert_eq!(a.block_count(), 1);
            assert_eq!(b.block_count(), 1);
            if paged {
                let ap = a.paged.as_ref().unwrap().dir_for(&block(16));
                let bp = b.paged.as_ref().unwrap().dir_for(&block(16));
                assert_eq!(ap.file_name(), bp.file_name());
                assert_ne!(ap, bp);
                assert!(ap.starts_with(dir.path().join("a/shared/paged")));
                assert!(bp.starts_with(dir.path().join("b/shared/paged")));
            } else {
                for tenant in [&a, &b] {
                    assert_eq!(tenant.store.scan().unwrap()[0].id, block(16));
                }
                assert!(a.store.root().ends_with("a/shared"));
                assert!(b.store.root().ends_with("b/shared"));
            }
            drop(a);
            drop(b);
            drop(root);
            let restarted = open(dir.path(), &config, origin.clone());
            let a = restarted.for_tenant(&TenantId::named("a")).await.unwrap();
            let b = restarted.for_tenant(&TenantId::named("b")).await.unwrap();
            tokio::join!(read(&a), read(&b));
            assert_eq!(
                origin.reads.load(Ordering::SeqCst),
                2,
                "restart must recover both tenant caches"
            );
        }
    }

    #[tokio::test]
    async fn interrupted_namespace_creation_recovers_and_symlinks_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("partial")).unwrap();
        let root = open(dir.path(), &config(), Arc::default());
        let partial = root.for_tenant(&TenantId::named("partial")).await.unwrap();
        read(&partial).await;
        assert!(dir
            .path()
            .join("partial/shared/cache_geometry.json")
            .exists());
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("alias")).unwrap();
        assert!(root.for_tenant(&TenantId::named("alias")).await.is_err());
        assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);
        drop(partial);
        drop(root);
        let lock = Arc::new(CacheRootLock::acquire(dir.path()).unwrap());
        assert!(WorkerRuntime::open_tenants(
            &config(),
            lock,
            Arc::new(Origin::default()),
            WorkerMetrics::new(1024)
        )
        .is_err());
    }

    #[tokio::test]
    async fn cancelled_initializer_is_published_once_and_drained() {
        let dir = tempfile::tempdir().unwrap();
        let root = open(dir.path(), &config(), Arc::default());
        let registry = root.registry().unwrap();
        let guard = registry.initialize.lock().await;
        let request = root.clone();
        let task = tokio::spawn(async move { request.for_tenant(&TenantId::named("new")).await });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while registry.initializers.active_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        let _ = task.await;
        drop(guard);
        root.drain_tenant_initializers().await;
        assert_eq!(registry.snapshot().len(), 2);
        let a = root.for_tenant(&TenantId::named("new")).await.unwrap();
        let b = root.for_tenant(&TenantId::named("new")).await.unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        assert!(a
            .staging_file()
            .unwrap()
            .path()
            .starts_with(dir.path().join("new/shared")));
    }

    #[tokio::test]
    async fn foreground_capacity_is_shared_across_tenants() {
        for paged in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = config();
            cfg.capacity_bytes = 16;
            if !paged {
                cfg.l2_page_size_bytes = 0;
            }
            let root = open(dir.path(), &cfg, Arc::default());
            for name in ["a", "b", "c", "d", "e"] {
                read(&root.for_tenant(&TenantId::named(name)).await.unwrap()).await;
                assert!(
                    root.tenant_runtimes()
                        .iter()
                        .map(|w| w.resident_bytes())
                        .sum::<u64>()
                        <= cfg.capacity_bytes
                );
            }
        }
    }

    #[tokio::test]
    async fn unspecified_and_explicit_default_share_one_namespace() {
        let dir = tempfile::tempdir().unwrap();
        let root = open(dir.path(), &config(), Arc::default());
        let legacy = root.for_tenant(&TenantId::Unattributed).await.unwrap();
        let named = root.for_tenant(&TenantId::named("default")).await.unwrap();
        assert!(Arc::ptr_eq(&legacy, &named));
        read(&legacy).await;
        assert_eq!(named.block_count(), 1);
        assert!(named.store.root().ends_with("default/shared"));
        assert!(!dir.path().join("unattributed").exists());
        for name in ["../escape", ".", "..", "a/b", "a\\b", ""] {
            assert!(root.for_tenant(&TenantId::named(name)).await.is_err());
        }
    }

    #[tokio::test]
    async fn policies_control_geometry_and_idle_expiration_independently() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config();
        config.tenants.insert(
            "short".into(),
            TenantCacheConfig {
                page_tti_hours: Some(1),
                ..Default::default()
            },
        );
        config.tenants.insert(
            "long".into(),
            TenantCacheConfig {
                block_size: Some(32),
                l2_page_size_bytes: Some(8),
                page_tti_hours: Some(2),
                ..Default::default()
            },
        );
        let root = open(dir.path(), &config, Arc::default());
        let short = root.for_tenant(&TenantId::named("short")).await.unwrap();
        let long = root.for_tenant(&TenantId::named("long")).await.unwrap();
        let inherited = root
            .for_tenant(&TenantId::named("unconfigured"))
            .await
            .unwrap();
        assert_eq!(inherited.block_size, 16);
        assert_eq!(inherited.paged_page_size(), Some(4));
        assert_eq!(inherited.page_gc_config.tti_ms, 0);
        assert_eq!(long.block_size, 32);
        assert_eq!(long.paged_page_size(), Some(8));
        for worker in [&short, &long] {
            worker.page_clock.set(10_000);
            read(worker).await;
        }
        assert_eq!(short.resident_bytes(), 4);
        assert_eq!(long.resident_bytes(), 8);
        for worker in [&short, &long] {
            worker.page_clock.set(10_000 + 3_600_001);
        }
        assert_eq!(short.gc_once().await.bytes, 4);
        assert_eq!(long.gc_once().await.bytes, 0);
        assert_eq!(short.block_count(), 0);
        assert_eq!(long.resident_bytes(), 8);
        assert!(long.load_block(&block(16), 16).await.is_err());
        long.load_block(&block(32), 32).await.unwrap();
    }

    #[tokio::test]
    async fn one_l1_budget_and_load_budget_cover_all_tenants() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config();
        config.l1_capacity_bytes = 4;
        config.l1_page_size_bytes = 4;
        let root = open(dir.path(), &config, Arc::default());
        let a = root.for_tenant(&TenantId::named("a")).await.unwrap();
        let b = root.for_tenant(&TenantId::named("b")).await.unwrap();
        tokio::join!(read(&a), read(&b));
        assert!(a.l1_resident_bytes() + b.l1_resident_bytes() <= 4);
        assert!(Arc::ptr_eq(&a.load_slots, &b.load_slots));
        assert!(Arc::ptr_eq(&a.load_bytes, &b.load_bytes));
        assert!(Arc::ptr_eq(
            a.background_budget.as_ref().unwrap(),
            b.background_budget.as_ref().unwrap()
        ));
    }

    #[tokio::test]
    async fn conflicting_carriers_and_geometry_changes_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config();
        let root = open(dir.path(), &config, Arc::default());
        let mut frame = talon_transport::data::encode_tenant_request(
            1,
            &talon_transport::data::TenantScopedRange {
                tenant: TenantId::named("a"),
                request: request(),
            },
        )
        .unwrap();
        talon_transport::envelope::encode_tenant(&mut frame, &TenantId::named("b")).unwrap();
        let header = talon_transport::FrameHeader::decode(&frame).unwrap();
        assert!(root
            .for_request(&header, &frame[talon_transport::HEADER_LEN..])
            .await
            .is_err());
        let a = root.for_tenant(&TenantId::named("a")).await.unwrap();
        read(&a).await;
        drop(a);
        drop(root);
        config.tenants.insert(
            "a".into(),
            TenantCacheConfig {
                l2_page_size_bytes: Some(8),
                ..Default::default()
            },
        );
        let lock = Arc::new(CacheRootLock::acquire(dir.path()).unwrap());
        assert!(WorkerRuntime::open_tenants(
            &config,
            lock,
            Arc::new(Origin::default()),
            WorkerMetrics::new(1024)
        )
        .is_err());
    }
    #[tokio::test]
    async fn mixed_tenant_wire_requests_isolate_reads_admission_load_and_batch_load() {
        use talon_transport::{codec, data, ControlMessage, Flags, FrameHeader, HEADER_LEN};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        async fn exchange(
            stream: &mut tokio::net::TcpStream,
            mut frame: Vec<u8>,
            tenant: Option<&str>,
            body: &[u8],
        ) -> (FrameHeader, Vec<u8>) {
            if let Some(tenant) = tenant {
                talon_transport::envelope::encode_tenant(&mut frame, &TenantId::named(tenant))
                    .unwrap();
            }
            stream.write_all(&frame).await.unwrap();
            stream.write_all(body).await.unwrap();
            stream.flush().await.unwrap();
            let mut header = [0; HEADER_LEN];
            stream.read_exact(&mut header).await.unwrap();
            let header = FrameHeader::decode(&header).unwrap();
            let mut body = vec![0; header.length as usize];
            stream.read_exact(&mut body).await.unwrap();
            (header, body)
        }
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config();
        cfg.l2_page_size_bytes = 0;
        let worker = open(dir.path(), &cfg, Arc::default());
        let obs = Arc::new(
            crate::WorkerObservability::new(
                "test".into(),
                talon_core::NodeInfo {
                    id: talon_core::NodeId::new("worker"),
                    address: "127.0.0.1:0".into(),
                    role: talon_core::NodeRole::Worker,
                },
                "127.0.0.1:0".into(),
                cfg.capacity_bytes,
                worker.index.clone(),
                worker.inflight.clone(),
            )
            .unwrap(),
        );
        obs.attach_tenant_runtime(&worker);
        obs.readiness().set_backend_ready(true);
        obs.readiness().set_store_ready(true);
        obs.readiness().set_control_registered(true);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_worker = worker.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            crate::tokio_conn::handle_conn(stream, server_worker, obs)
                .await
                .unwrap();
        });
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        // Same physical connection switches tenant on every request.
        let admission = data::encode_cached_block_put_header(
            1,
            &CachedBlockPutRequest {
                block: block(16),
                object_len: 64,
                body_len: 16,
            },
        )
        .unwrap();
        assert!(!exchange(&mut stream, admission, Some("a"), &[42; 16])
            .await
            .0
            .flags
            .contains(Flags::ERROR));
        let cached = || {
            data::encode_cached_request(
                2,
                &CachedRangeRequest {
                    object: request().object,
                    version: Version::new("v1"),
                    offset: 0,
                    len: 4,
                },
            )
            .unwrap()
        };
        for tenant in [None, Some("b")] {
            let (header, body) = exchange(&mut stream, cached(), tenant, &[]).await;
            assert!(header.flags.contains(Flags::ERROR));
            assert_eq!(
                data::decode_error_payload(&body).code,
                talon_transport::DataErrorCode::CacheMiss
            );
        }
        assert_eq!(
            exchange(&mut stream, cached(), Some("a"), &[]).await.1,
            [42; 4]
        );
        // The SDK carries the borrowed tenant only in v3 metadata; the read body
        // uses the ordinary versioned request rather than owning another tenant string.
        let versioned = data::encode_versioned_request(
            8,
            &data::VersionedRangeRequest {
                request: data::RangeRequest {
                    object: request().object,
                    offset: 0,
                    len: 4,
                },
                version: Version::new("v1"),
                object_len: 64,
            },
        )
        .unwrap();
        let (header, body) = exchange(&mut stream, versioned, Some("a"), &[]).await;
        assert!(!header.flags.contains(Flags::ERROR));
        assert_eq!(body, [42; 4]);
        let load = codec::encode(
            3,
            &ControlMessage::LoadBlock {
                block: block(16),
                len: 16,
            },
        )
        .unwrap();
        let (header, body) = exchange(&mut stream, load, Some("b"), &[]).await;
        let mut full = header.encode().to_vec();
        full.extend(body);
        assert!(matches!(
            codec::decode(&full).unwrap().1,
            ControlMessage::Ack { ok: true, .. }
        ));
        assert_eq!(
            exchange(&mut stream, cached(), Some("b"), &[]).await.1,
            [0, 1, 2, 3]
        );
        assert_eq!(
            exchange(&mut stream, cached(), Some("a"), &[]).await.1,
            [42; 4]
        );
        let load = codec::encode(
            4,
            &ControlMessage::BatchLoad {
                blocks: vec![talon_transport::LoadBlockRequest {
                    block: block(16),
                    len: 16,
                }],
            },
        )
        .unwrap();
        let (header, body) = exchange(&mut stream, load, None, &[]).await;
        let mut full = header.encode().to_vec();
        full.extend(body);
        assert!(
            matches!(codec::decode(&full).unwrap().1, ControlMessage::BatchLoadResult { failures } if failures.is_empty())
        );
        assert_eq!(
            exchange(&mut stream, cached(), None, &[]).await.1,
            [0, 1, 2, 3]
        );
        for name in ["default", "a", "b"] {
            assert_eq!(
                worker
                    .for_tenant(&TenantId::named(name))
                    .await
                    .unwrap()
                    .block_count(),
                1
            );
        }
        let put = data::encode_put_header(
            5,
            &data::PutRequest {
                object: request().object,
                body_len: 16,
            },
        )
        .unwrap();
        assert!(!exchange(&mut stream, put, Some("a"), &[99; 16])
            .await
            .0
            .flags
            .contains(Flags::ERROR));
        assert_eq!(
            exchange(&mut stream, cached(), Some("a"), &[]).await.1,
            [99; 4]
        );
        assert_eq!(
            exchange(&mut stream, cached(), Some("b"), &[]).await.1,
            [0, 1, 2, 3]
        );
        let delete = data::encode_delete(
            6,
            &data::DeleteRequest {
                object: request().object,
            },
        )
        .unwrap();
        assert!(!exchange(&mut stream, delete, Some("a"), &[])
            .await
            .0
            .flags
            .contains(Flags::ERROR));
        assert!(exchange(&mut stream, cached(), Some("a"), &[])
            .await
            .0
            .flags
            .contains(Flags::ERROR));
        assert_eq!(
            exchange(&mut stream, cached(), None, &[]).await.1,
            [0, 1, 2, 3]
        );
        drop(stream);
        server.await.unwrap();
    }
}
