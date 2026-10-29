//! Cache namespaces own their cache state; placement continues to hash BlockId.
use super::*;
use crate::fd_cache::FdCache;
use crate::page_access_store::CacheRootLock;
use std::{collections::BTreeMap, path::Path, sync::RwLock};
use talon_core::{NamespaceId, WorkerConfig, DEFAULT_NAMESPACE};

/// Bounds all namespace and credential-scope cache runtimes and disk directories.
const MAX_CACHE_RUNTIMES: usize = 4096;

/// Cache identity is separate from the original namespace used by policy and QoS.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct CacheIdentity(String);
impl CacheIdentity {
    pub(super) fn new(
        namespace: &NamespaceId,
        scope: Option<talon_core::backend::AccessScope>,
    ) -> Self {
        // Namespace names cannot contain '/', making the compound identity
        // unambiguous while static lookups can still borrow the original name.
        Self(match scope {
            Some(scope) => format!("{}/{}", namespace.effective_name(), scope.as_str()),
            None => namespace.effective_name().to_owned(),
        })
    }
}
impl std::borrow::Borrow<str> for CacheIdentity {
    fn borrow(&self) -> &str {
        &self.0
    }
}

pub(super) struct NamespaceRuntimes {
    uring: bool,
    membership: Arc<Mutex<Option<Arc<MembershipSnapshot>>>>,
    membership_changed: tokio::sync::watch::Sender<()>,
    workers: RwLock<BTreeMap<CacheIdentity, Arc<WorkerRuntime>>>,
    initialize: tokio::sync::Mutex<()>,
    initializers: Arc<Mutations>,
    config: WorkerConfig,
    root_lock: Arc<CacheRootLock>,
    backend: Arc<dyn BackendStore>,
    background_heads: Option<Arc<origin_head::OriginHeads>>,
    metrics: WorkerMetrics,
    limiter: Arc<NamespaceRateLimiter>,
    load_slots: Arc<tokio::sync::Semaphore>,
    load_bytes: Arc<tokio::sync::Semaphore>,
    pub(super) l1_budget: Arc<crate::memory_store::L1Budget>,
    whole_fds: Arc<FdCache>,
    page_fds: Arc<FdCache>,
    background_budget: Arc<crate::background::BackgroundBudget>,
}

impl NamespaceRuntimes {
    pub(super) fn snapshot(&self) -> Vec<Arc<WorkerRuntime>> {
        self.workers.read().unwrap().values().cloned().collect()
    }

    fn open(
        self: &Arc<Self>,
        namespace: &NamespaceId,
        scope: Option<talon_core::backend::AccessScope>,
        backend: Option<Arc<dyn BackendStore>>,
    ) -> anyhow::Result<WorkerRuntime> {
        let config = self.config.for_namespace(namespace)?;
        let namespace_root = self.root_lock.root.join(namespace.effective_name());
        checked_dir(&namespace_root)?;
        let root = match &scope {
            None => namespace_root.join("shared"),
            Some(scope) => {
                let scopes = namespace_root.join("scopes");
                checked_dir(&scopes)?;
                scopes.join(scope.as_str())
            }
        };
        checked_dir(&root)?;
        // Persist geometry independently for each namespace. A changed page size
        // must never reinterpret existing page indices on restart.
        let identity_path = root.join("cache_geometry.json");
        let geometry = (config.block_size, config.l2_page_size_bytes);
        match std::fs::read(&identity_path) {
            Ok(bytes) => anyhow::ensure!(
                serde_json::from_slice::<(u32, u64)>(&bytes)? == geometry,
                "cache geometry changed for namespace {}; use a new empty namespace cache directory",
                namespace.effective_name()
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
        let store = WholeBlockStore::open_with_fd_cache(&root, self.whole_fds.clone())?
            .with_root_lock(self.root_lock.clone())
            .with_uring(self.uring)?;
        let index = Arc::new(BlockIndex::new());
        let paged = if config.l2_page_size_bytes > 0 {
            checked_dir(&root.join("paged"))?;
            Some(
                PagedBlockStore::open_with_fd_cache(
                    root.join("paged"),
                    config.l2_page_size_bytes as u32,
                    self.page_fds.clone(),
                )?
                .with_root_lock(self.root_lock.clone())
                .with_uring(self.uring)?,
            )
        } else {
            None
        };
        let recovery_started = Instant::now();
        store.restore_index(&index)?;
        if let Some(paged) = &paged {
            paged.restore_index(&index)?;
        }
        tracing::info!(root = %root.display(), blocks = index.len(),
            elapsed_ms = recovery_started.elapsed().as_millis() as u64,
            "cache index restored");
        let runtime_started = Instant::now();
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
        worker.page_mutations = Arc::new(Mutations::new(self.uring));
        worker.background_heads = self.background_heads.clone();
        // The backend is deliberately absent for recovered credential scopes.
        worker.backend = Arc::new(backend.map(OnceLock::from).unwrap_or_default());
        worker.page_gc_metrics = PageGcMetrics::with_scope(
            &self.metrics.registry,
            namespace.effective_name(),
            scope.as_ref().map(|scope| scope.as_str()),
        );
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
        tracing::info!(root = %root.display(),
            elapsed_ms = runtime_started.elapsed().as_millis() as u64,
            "cache runtime restored");
        worker.membership = self.membership.clone();
        worker.membership_changed = self.membership_changed.clone();
        worker.namespace = namespace.normalized();
        worker.cache_identity = CacheIdentity::new(namespace, scope);
        worker.namespace_parent = Arc::downgrade(self);
        worker.load_slots = self.load_slots.clone();
        worker.load_bytes = self.load_bytes.clone();
        worker.load_inflight_bytes = config.load_inflight_bytes;
        worker.background_budget = Some(self.background_budget.clone());
        Ok(worker)
    }

    async fn resolve(
        self: &Arc<Self>,
        namespace: &NamespaceId,
        scope: Option<talon_core::backend::AccessScope>,
        backend: Arc<dyn BackendStore>,
    ) -> anyhow::Result<Arc<WorkerRuntime>> {
        namespace.validate()?;
        let identity = CacheIdentity::new(namespace, scope.clone());
        if let Some(worker) = self.workers.read().unwrap().get(&identity) {
            if worker.backend.get().is_none() {
                let _ = worker.backend.set(backend);
            }
            return Ok(worker.clone());
        }
        // The initializer owns publication even when the triggering request is
        // cancelled. Holding this mutex in a detached task prevents duplicate
        // runtimes and competing writes to the same namespace.
        let registry = self.clone();
        let namespace = namespace.normalized();
        self.initializers
            .run(async move {
                let _guard = registry.initialize.lock().await;
                if let Some(worker) = registry.workers.read().unwrap().get(&identity) {
                    let _ = worker.backend.set(backend);
                    return Ok(worker.clone());
                }
                anyhow::ensure!(
                    registry.workers.read().unwrap().len() < MAX_CACHE_RUNTIMES,
                    "cache runtime limit reached"
                );
                let owner = registry.clone();
                let worker = Arc::new(
                    tokio::task::spawn_blocking(move || {
                        owner.open(&namespace, scope, Some(backend))
                    })
                    .await??,
                );
                // Publish after index recovery; background cleanup also covers
                // newly discovered namespaces without delaying their first read.
                registry
                    .workers
                    .write()
                    .unwrap()
                    .insert(identity, worker.clone());
                registry.membership_changed.send_replace(());
                Ok::<_, anyhow::Error>(worker)
            })
            .await
    }
}

fn checked_dir(path: &Path) -> anyhow::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) => anyhow::ensure!(
            meta.is_dir() && !meta.file_type().is_symlink(),
            "namespace cache path must be a real directory: {}",
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

    /// Keep streamed PUT staging in the same namespace directory as cached data.
    pub(crate) fn staging_file(&self) -> std::io::Result<tempfile::NamedTempFile> {
        tempfile::NamedTempFile::new_in(self.store.root())
    }

    /// Effective namespace of this cache runtime.
    pub fn namespace(&self) -> &NamespaceId {
        &self.namespace
    }

    pub(super) async fn drain_namespace_initializers(&self) {
        if let Some(registry) = self.registry() {
            registry.initializers.drain().await;
        }
    }

    /// Open configured namespaces and every persisted credential scope under one locked root.
    pub fn open_namespaces(
        config: &WorkerConfig,
        root_lock: Arc<CacheRootLock>,
        backend: Arc<dyn BackendStore>,
        metrics: WorkerMetrics,
    ) -> anyhow::Result<Self> {
        Self::open_namespaces_with_uring(config, root_lock, backend, metrics, false)
    }

    pub fn open_namespaces_with_uring(
        config: &WorkerConfig,
        root_lock: Arc<CacheRootLock>,
        backend: Arc<dyn BackendStore>,
        metrics: WorkerMetrics,
        uring: bool,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let registry = Arc::new(NamespaceRuntimes {
            uring,
            membership: Arc::new(Mutex::new(None)),
            membership_changed: tokio::sync::watch::channel(()).0,
            workers: RwLock::new(BTreeMap::new()),
            initialize: tokio::sync::Mutex::new(()),
            initializers: Arc::new(Mutations::default()),
            config: config.clone(),
            root_lock,
            backend,
            background_heads: uring
                .then(|| origin_head::OriginHeads::new(config.stale_gc_head_concurrency)),
            metrics,
            limiter: Arc::new(NamespaceRateLimiter::new(config.rate_limits.clone())),
            load_slots: Arc::new(tokio::sync::Semaphore::new(8)),
            load_bytes: Arc::new(tokio::sync::Semaphore::new(
                config.load_inflight_bytes as usize,
            )),
            l1_budget: Arc::default(),
            whole_fds: Arc::new(FdCache::new(config.whole_fd_cache_capacity)),
            page_fds: Arc::new(FdCache::for_pages(config.page_fd_cache_capacity)),
            background_budget: crate::background::BackgroundBudget::new(config),
        });
        let mut names = std::collections::BTreeSet::from([DEFAULT_NAMESPACE.to_owned()]);
        names.extend(config.namespaces.keys().cloned());
        for entry in std::fs::read_dir(&registry.root_lock.root)? {
            let entry = entry?;
            if entry.file_name() == "lost+found" {
                continue;
            }
            if entry.file_type()?.is_dir() || entry.file_type()?.is_symlink() {
                let name = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| anyhow::anyhow!("invalid namespace directory"))?;
                // Never silently strand an old flat cache or adopt it as a namespace.
                anyhow::ensure!(entry.path().join("shared").is_dir() || entry.path().join("scopes").is_dir() || (entry.file_type()?.is_dir() && std::fs::read_dir(entry.path())?.next().is_none()), "legacy/unrecognized cache directory {}; configure a new cache root for namespace storage", entry.path().display());
                names.insert(name);
            }
        }
        anyhow::ensure!(names.len() <= MAX_CACHE_RUNTIMES, "too many namespaces");
        for name in names {
            let namespace = NamespaceId::named(&name);
            namespace.validate()?;
            let namespace_root = registry.root_lock.root.join(&name);
            checked_dir(&namespace_root)?;
            let has_shared = match std::fs::symlink_metadata(namespace_root.join("shared")) {
                Ok(_) => true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                Err(error) => return Err(error.into()),
            };
            let scopes = namespace_root.join("scopes");
            let has_scopes = match std::fs::symlink_metadata(&scopes) {
                Ok(_) => true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                Err(error) => return Err(error.into()),
            };
            if namespace.is_default()
                || config.namespaces.contains_key(&name)
                || has_shared
                || !has_scopes
            {
                anyhow::ensure!(
                    registry.workers.read().unwrap().len() < MAX_CACHE_RUNTIMES,
                    "too many cache runtimes"
                );
                let worker =
                    Arc::new(registry.open(&namespace, None, Some(registry.backend.clone()))?);
                registry
                    .workers
                    .write()
                    .unwrap()
                    .insert(worker.cache_identity.clone(), worker);
            }
            if has_scopes {
                checked_dir(&scopes)?;
                for entry in std::fs::read_dir(scopes)? {
                    let entry = entry?;
                    let scope = entry
                        .file_name()
                        .into_string()
                        .map_err(|_| anyhow::anyhow!("invalid credential scope directory"))?
                        .parse()?;
                    anyhow::ensure!(
                        registry.workers.read().unwrap().len() < MAX_CACHE_RUNTIMES,
                        "too many cache runtimes"
                    );
                    let worker = Arc::new(registry.open(&namespace, Some(scope), None)?);
                    registry
                        .workers
                        .write()
                        .unwrap()
                        .insert(worker.cache_identity.clone(), worker);
                }
            }
        }
        let mut worker = (**registry
            .workers
            .read()
            .unwrap()
            .get(DEFAULT_NAMESPACE)
            .unwrap())
        .clone();
        worker.namespaces = Some(registry);
        Ok(worker)
    }

    /// Resolve an explicit identity, inheriting the global cache policy when absent.
    pub async fn for_namespace(
        self: &Arc<Self>,
        namespace: &NamespaceId,
    ) -> anyhow::Result<Arc<Self>> {
        namespace.validate()?;
        if let Some(registry) = self
            .namespaces
            .as_ref()
            .cloned()
            .or_else(|| self.namespace_parent.upgrade())
        {
            if let Some(worker) = registry
                .workers
                .read()
                .unwrap()
                .get(namespace.effective_name())
            {
                return Ok(worker.clone());
            }
            registry
                .resolve(namespace, None, registry.backend.clone())
                .await
        } else {
            anyhow::ensure!(
                namespace.effective_name() == self.namespace.effective_name(),
                "runtime has no namespace registry"
            );
            Ok(self.clone())
        }
    }

    /// Snapshot every namespace and credential scope for maintenance and process metrics.
    pub fn namespace_runtimes(self: &Arc<Self>) -> Vec<Arc<Self>> {
        match self
            .namespaces
            .as_ref()
            .cloned()
            .or_else(|| self.namespace_parent.upgrade())
        {
            Some(registry) => registry.snapshot(),
            None => vec![self.clone()],
        }
    }

    pub(super) fn registry(&self) -> Option<Arc<NamespaceRuntimes>> {
        self.namespaces
            .as_ref()
            .cloned()
            .or_else(|| self.namespace_parent.upgrade())
    }

    /// Resolve namespace and credential metadata consistently for every operation.
    pub async fn for_request(
        self: &Arc<Self>,
        header: &talon_transport::FrameHeader,
        payload: &[u8],
    ) -> Result<Arc<Self>, talon_transport::DataPlaneError> {
        use talon_transport::{data, DataErrorCode, DataPlaneError, MsgType, HEADER_LEN};
        let (namespace, broker) = (|| -> anyhow::Result<_> {
            let (metadata, _) = talon_transport::envelope::decode(header, payload)?;
            let mut frame = Vec::new();
            let legacy = if matches!(
                header.msg_type,
                MsgType::GetRangeNamespace
                    | MsgType::GetCachedRangeNamespace
                    | MsgType::GetVersionedRangeNamespace
            ) {
                frame.reserve(HEADER_LEN + payload.len());
                frame.extend_from_slice(&header.encode());
                frame.extend_from_slice(payload);
                Some(match header.msg_type {
                    MsgType::GetRangeNamespace => {
                        data::decode_namespace_request(&frame)?.1.namespace
                    }
                    MsgType::GetCachedRangeNamespace => {
                        data::decode_cached_namespace_request(&frame)?.1.namespace
                    }
                    _ => {
                        data::decode_versioned_namespace_request(&frame)?
                            .1
                            .namespace
                    }
                })
            } else {
                None
            };
            if let (Some(a), Some(b)) = (&metadata.namespace, &legacy) {
                anyhow::ensure!(
                    a.effective_name() == b.effective_name(),
                    "conflicting namespace identities"
                );
            }
            let namespace = metadata.namespace.or(legacy).unwrap_or_default();
            namespace.validate()?;
            Ok((namespace, metadata.credential_broker))
        })()
        .map_err(|error| DataPlaneError {
            code: DataErrorCode::InvalidRequest,
            message: error.to_string(),
        })?;
        if let Some(broker) = broker {
            let (backend, bucket, objects) =
                request_target(header, payload).map_err(|error| DataPlaneError {
                    code: DataErrorCode::InvalidRequest,
                    message: error.to_string(),
                })?;
            let result = async {
                self.ensure_configured_backend(backend)?;
                let registry = self
                    .registry()
                    .ok_or_else(|| anyhow::anyhow!("runtime has no credential scope registry"))?;
                let resolved = registry
                    .backend
                    .bind_request(backend, &bucket, broker.config())
                    .await?;
                anyhow::ensure!(
                    resolved.scope.is_some(),
                    "explicit credentials require a cache scope"
                );
                for object in &objects {
                    resolved.store.validate_object(object)?;
                }
                registry
                    .resolve(&namespace, resolved.scope, resolved.store)
                    .await
            }
            .await;
            return result.map_err(|error| DataPlaneError {
                code: crate::data_error::classify(&error),
                message: error.to_string(),
            });
        }
        self.for_namespace(&namespace)
            .await
            .map_err(|error| DataPlaneError {
                code: DataErrorCode::Internal,
                message: error.to_string(),
            })
    }
}

/// Resolve one request's target before selecting its credential-scoped cache.
/// A batch shares one Broker request, so none of its blocks may cross that target.
fn request_target(
    header: &talon_transport::FrameHeader,
    payload: &[u8],
) -> anyhow::Result<(Backend, String, Vec<ObjectId>)> {
    use talon_transport::{data, MsgType, HEADER_LEN};
    let mut frame = Vec::with_capacity(HEADER_LEN + payload.len());
    frame.extend_from_slice(&header.encode());
    frame.extend_from_slice(payload);
    let object = match header.msg_type {
        MsgType::GetRange => data::decode_request(&frame)?.1.object,
        MsgType::GetRangeNamespace => data::decode_namespace_request(&frame)?.1.request.object,
        MsgType::GetVersionedRange => data::decode_versioned_request(&frame)?.1.request.object,
        MsgType::GetVersionedRangeNamespace => {
            data::decode_versioned_namespace_request(&frame)?
                .1
                .request
                .request
                .object
        }
        MsgType::GetCachedRange => data::decode_cached_request(&frame)?.1.object,
        MsgType::GetCachedRangeNamespace => {
            data::decode_cached_namespace_request(&frame)?
                .1
                .request
                .object
        }
        MsgType::Put => data::decode_put_header(&frame)?.1.object,
        MsgType::AdmitCachedBlock => data::decode_cached_block_put_header(&frame)?.1.block.object,
        MsgType::Delete => data::decode_delete(&frame)?.1.object,
        MsgType::Control => match codec::decode_request(&frame)?.1 {
            ControlMessage::StatObject { object } => object,
            ControlMessage::LoadBlock { block, .. } => block.object,
            ControlMessage::BatchLoad { blocks } => {
                let first = blocks
                    .first()
                    .ok_or_else(|| anyhow::anyhow!("empty credential-scoped batch"))?;
                let object = &first.block.object;
                anyhow::ensure!(
                    blocks
                        .iter()
                        .all(|request| request.block.object.backend == object.backend
                            && request.block.object.bucket == object.bucket),
                    "credential-scoped batch must use one backend and bucket"
                );
                return Ok((
                    object.backend,
                    object.bucket.clone(),
                    blocks
                        .into_iter()
                        .map(|request| request.block.object)
                        .collect(),
                ));
            }
            ControlMessage::ListObjects { prefix } => {
                let mut parts = prefix.trim_start_matches('/').splitn(3, '/');
                let backend = parts.next().unwrap_or("").parse()?;
                let bucket = parts.next().unwrap_or("");
                anyhow::ensure!(!bucket.is_empty(), "listing prefix must name a bucket");
                return Ok((backend, bucket.to_owned(), Vec::new()));
            }
            _ => anyhow::bail!("unsupported credential-scoped control request"),
        },
        _ => anyhow::bail!("unsupported credential-scoped request"),
    };
    Ok((object.backend, object.bucket.clone(), vec![object]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use talon_core::{NamespaceCacheConfig, ObjectStat};

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
            capacity_bytes: 1024 * disk_unit(),
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
            WorkerRuntime::open_namespaces(
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

    /// Synthetic filesystem benchmark, not a UAT readiness measurement. Run in
    /// release mode with TMPDIR on the benchmark disk; never evicts host caches.
    #[tokio::test]
    #[ignore = "manual filesystem recovery benchmark"]
    async fn benchmark_cache_startup() {
        let count: usize = std::env::var("TALON_RECOVERY_BENCH_BLOCKS")
            .unwrap_or_else(|_| "100000".into())
            .parse()
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cfg = WorkerConfig {
            block_size: 256 << 20,
            l2_page_size_bytes: 1 << 20,
            capacity_bytes: 1 << 40,
            ..config()
        };
        drop(open(dir.path(), &cfg, Arc::default()));
        let paged =
            PagedBlockStore::open(dir.path().join("default/shared/paged"), 1 << 20).unwrap();
        for n in 0..count {
            let id = BlockId::new(
                ObjectId::new(Backend::S3, "bucket", format!("object/{n}")),
                0,
                cfg.block_size,
                Version::new("v1"),
            );
            let block_dir = paged.dir_for(&id);
            std::fs::create_dir_all(&block_dir).unwrap();
            let meta = talon_core::BlockMeta {
                id,
                form: talon_core::BlockForm::Paged {
                    page_size: 1 << 20,
                    present: talon_core::PresentBitmap::new(256),
                },
                len: 256 << 20,
            };
            std::fs::write(
                block_dir.join("block.meta"),
                serde_json::to_vec(&meta).unwrap(),
            )
            .unwrap();
            // Sparse payload keeps the metadata workload realistic without
            // allocating count MiB of test data. Index allocation remains exact.
            let page = std::fs::File::create(block_dir.join("0.page")).unwrap();
            page.set_len(1 << 20).unwrap();
        }
        eprintln!("fixture blocks={count} entries={} ready", count * 3);
        let startup_only = std::env::var("TALON_RECOVERY_BENCH_PHASE").as_deref() == Ok("startup");
        let cases: &[usize] = if startup_only {
            &[]
        } else {
            &[0, 1, 2, 4, 8, 16, 16, 8, 4, 2, 1, 0]
        };
        for &workers in cases {
            let index = BlockIndex::new();
            let started = Instant::now();
            if workers > 0 {
                paged.restore_index_with_workers(&index, workers).unwrap();
            } else {
                for entry in paged.scan_allocated().unwrap() {
                    index.commit_allocated(entry.meta, entry.allocated);
                }
            }
            let elapsed = started.elapsed();
            assert_eq!(index.len(), count);
            eprintln!("index workers={workers} blocks={count} elapsed={elapsed:?}");
        }
        let started = Instant::now();
        let worker = open(dir.path(), &cfg, Arc::default());
        let recovery = started.elapsed();
        assert_eq!(worker.index.len(), count);
        eprintln!("startup blocks={count} recovery={recovery:?}");
        if startup_only {
            return;
        }
        let started = Instant::now();
        for namespace in worker.namespace_runtimes() {
            namespace.recover_page_file_cleanup().await;
        }
        eprintln!(
            "startup blocks={count} deferred_cleanup={:?}",
            started.elapsed()
        );
    }
    async fn read(worker: &WorkerRuntime) {
        worker
            .serve_versioned(&request(), &Version::new("v1"), 64)
            .await
            .unwrap();
    }

    fn scope(digit: char) -> talon_core::backend::AccessScope {
        format!("v1-{}", digit.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    #[derive(Default)]
    struct CredentialHttp {
        exchanges: AtomicUsize,
        gets: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl talon_backend::HttpClient for CredentialHttp {
        async fn execute(
            &self,
            request: talon_backend::HttpRequest,
        ) -> Result<talon_backend::HttpResponse, String> {
            use talon_backend::http::Method;
            if request.method == Method::Post {
                self.exchanges.fetch_add(1, Ordering::SeqCst);
                let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
                let external_id = body["externalId"].as_str().unwrap();
                return Ok(talon_backend::HttpResponse {
                    status: if external_id == "rejected" { 403 } else { 200 },
                    headers: vec![],
                    body: Bytes::from(
                        serde_json::json!({
                            "success": true,
                            "credentials": {
                                "tempAk": external_id, "tempSk": "test-secret",
                                "sessionToken": "test-token", "expiredAt": "2099-01-01T00:00:00Z"
                            }
                        })
                        .to_string(),
                    ),
                });
            }
            let mut body = Bytes::new();
            if request.method == Method::Get {
                self.gets.fetch_add(1, Ordering::SeqCst);
                let (start, end) = request
                    .header("range")
                    .unwrap()
                    .strip_prefix("bytes=")
                    .unwrap()
                    .split_once('-')
                    .unwrap();
                let len = end.parse::<usize>().unwrap() - start.parse::<usize>().unwrap() + 1;
                let value = if request
                    .header("authorization")
                    .unwrap()
                    .contains("Credential=a/")
                {
                    42
                } else {
                    84
                };
                body = Bytes::from(vec![value; len]);
            }
            Ok(talon_backend::HttpResponse {
                status: if request.method == Method::Get {
                    206
                } else {
                    200
                },
                headers: vec![
                    ("etag".into(), "\"v1\"".into()),
                    ("content-length".into(), "64".into()),
                ],
                body,
            })
        }
    }

    fn open_broker(
        root: &Path,
        config: &WorkerConfig,
        http: Arc<CredentialHttp>,
    ) -> Arc<WorkerRuntime> {
        let resolver = talon_backend::DefaultBackendResolver::new(
            Backend::S3,
            http.clone(),
            http,
            Arc::new(talon_backend::NoopObserver),
        )
        .with_legacy_backend(Arc::new(Origin::default()));
        Arc::new(
            WorkerRuntime::open_namespaces(
                config,
                Arc::new(CacheRootLock::acquire(root).unwrap()),
                Arc::new(talon_backend::BackendRouter::new(
                    Backend::S3,
                    Arc::new(resolver),
                )),
                WorkerMetrics::new(config.capacity_bytes),
            )
            .unwrap(),
        )
    }

    fn broker_config(external_id: &str) -> talon_core::BrokerConfig {
        talon_core::BrokerConfig {
            origin: Some(talon_core::OriginOptions {
                endpoint: Some("s3.us-east-1.amazonaws.com".into()),
                region: Some("us-east-1".into()),
                ..Default::default()
            }),
            url: "https://broker.example/credentials".into(),
            request: serde_json::json!({
                "csp": "aws", "bucket": "bucket", "region": "us-east-1",
                "roleArn": "arn:aws:iam::123456789012:role/shared",
                "externalId": external_id
            }),
        }
    }

    async fn select_broker(
        worker: &Arc<WorkerRuntime>,
        external_id: &str,
        mut frame: Vec<u8>,
    ) -> Result<Arc<WorkerRuntime>, talon_transport::DataPlaneError> {
        let broker = broker_config(external_id);
        talon_transport::envelope::encode_credential_broker(&mut frame, &broker).unwrap();
        let header = talon_transport::FrameHeader::decode(&frame).unwrap();
        worker
            .for_request(&header, &frame[talon_transport::HEADER_LEN..])
            .await
    }

    #[tokio::test]
    async fn request_external_ids_isolate_same_bucket_cache_and_survive_restart() {
        use talon_transport::{codec, data, ControlMessage};
        for paged in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = config();
            cfg.l1_capacity_bytes = 8;
            cfg.l1_page_size_bytes = 4;
            if !paged {
                cfg.l2_page_size_bytes = 0;
            }
            let http = Arc::new(CredentialHttp::default());
            let root = open_broker(dir.path(), &cfg, http.clone());
            let a = select_broker(&root, "a", data::encode_request(1, &request()).unwrap())
                .await
                .unwrap();
            assert_eq!(
                match a.serve(&request()).await.unwrap() {
                    ServeOutcome::Bytes(bytes) => bytes.to_vec(),
                    _ => panic!("first request must fetch bytes"),
                },
                [42; 4]
            );
            let cached = CachedRangeRequest {
                object: request().object,
                version: Version::new("v1"),
                offset: 0,
                len: 4,
            };
            let b = select_broker(&root, "b", data::encode_cached_request(2, &cached).unwrap())
                .await
                .unwrap();
            assert!(b.serve_cached(&cached).await.is_err());
            assert!(
                root.serve_cached(&cached).await.is_err(),
                "static cache is separate"
            );
            assert_eq!(
                http.gets.load(Ordering::SeqCst),
                1,
                "cache-only B never fetches A's object"
            );
            assert!(
                b.cached_version(&request().object).is_none(),
                "B cannot reuse A's version lookup"
            );
            let load = select_broker(
                &root,
                "b",
                codec::encode(
                    3,
                    &ControlMessage::LoadBlock {
                        block: block(16),
                        len: 16,
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();
            load.load_block(&block(16), 16).await.unwrap();
            assert_eq!(b.serve_cached(&cached).await.unwrap().to_vec(), [84; 4]);
            assert_eq!(a.serve_cached(&cached).await.unwrap().to_vec(), [42; 4]);
            assert_ne!(a.cache_identity, b.cache_identity);
            assert_eq!(a.namespace(), &NamespaceId::default());
            assert_eq!(http.exchanges.load(Ordering::SeqCst), 2);
            assert!(select_broker(
                &root,
                "rejected",
                data::encode_cached_request(4, &cached).unwrap()
            )
            .await
            .is_err());
            assert_eq!(
                root.namespace_runtimes().len(),
                3,
                "failed Broker must not create a cache scope"
            );
            let mut other = block(16);
            other.object.bucket = "other".into();
            let batch = codec::encode(
                5,
                &ControlMessage::BatchLoad {
                    blocks: vec![
                        talon_transport::LoadBlockRequest {
                            block: block(16),
                            len: 16,
                        },
                        talon_transport::LoadBlockRequest {
                            block: other,
                            len: 16,
                        },
                    ],
                },
            )
            .unwrap();
            assert!(select_broker(&root, "new", batch).await.is_err());
            // Two successful acquisitions plus four rejected acquisition attempts.
            assert_eq!(
                http.exchanges.load(Ordering::SeqCst),
                6,
                "cross-bucket batch rejects before binding"
            );
            let gets = http.gets.load(Ordering::SeqCst);
            drop(a);
            drop(b);
            drop(load);
            drop(root);

            let restarted = open_broker(dir.path(), &cfg, http.clone());
            assert_eq!(
                http.exchanges.load(Ordering::SeqCst),
                6,
                "restart does not resolve credentials"
            );
            assert_eq!(restarted.namespace_runtimes().len(), 3);
            let b = select_broker(
                &restarted,
                "b",
                data::encode_cached_request(6, &cached).unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(b.serve_cached(&cached).await.unwrap().to_vec(), [84; 4]);
            assert_eq!(http.gets.load(Ordering::SeqCst), gets);
        }
    }

    async fn check_broker_rejection_replies(use_uring: bool) {
        use talon_transport::{codec, data, ControlMessage, DataErrorCode, Flags, HEADER_LEN};
        use tokio::io::AsyncWriteExt;

        let requests = [
            Some(ControlMessage::StatObject {
                object: request().object,
            }),
            Some(ControlMessage::ListObjects {
                prefix: "s3/bucket/".into(),
            }),
            Some(ControlMessage::LoadBlock {
                block: block(16),
                len: 16,
            }),
            Some(ControlMessage::BatchLoad {
                blocks: vec![talon_transport::LoadBlockRequest {
                    block: block(16),
                    len: 16,
                }],
            }),
            None,
        ];
        let request_count = requests.len();
        let dir = tempfile::tempdir().unwrap();
        let cfg = config();
        let worker = open_broker(dir.path(), &cfg, Arc::new(CredentialHttp::default()));
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
        obs.readiness().set_backend_ready(true);
        obs.readiness().set_store_ready(true);
        obs.readiness().set_control_registered(true);
        let (ready, address) = tokio::sync::oneshot::channel();
        let server = if use_uring {
            #[cfg(target_os = "linux")]
            {
                let handle = tokio::runtime::Handle::current();
                tokio::task::spawn_blocking(move || {
                    let _entered = handle.enter();
                    monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                        .with_iowq_max_workers(1, 1)
                        .enable_timer()
                        .build()
                        .unwrap()
                        .block_on(async {
                            let listener = monoio::net::TcpListener::bind("127.0.0.1:0").unwrap();
                            ready.send(listener.local_addr().unwrap()).unwrap();
                            for _ in 0..request_count {
                                let (stream, _) = listener.accept().await.unwrap();
                                crate::uring_conn::handle_conn(stream, worker.clone(), obs.clone())
                                    .await
                                    .unwrap();
                            }
                        });
                })
            }
            #[cfg(not(target_os = "linux"))]
            unreachable!("io_uring requires Linux")
        } else {
            tokio::spawn(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                ready.send(listener.local_addr().unwrap()).unwrap();
                for _ in 0..request_count {
                    let (stream, _) = listener.accept().await.unwrap();
                    crate::tokio_conn::handle_conn(stream, worker.clone(), obs.clone())
                        .await
                        .unwrap();
                }
            })
        };
        let address = address.await.unwrap();
        let mut replies = Vec::new();
        for (index, message) in requests.iter().enumerate() {
            let request_id = 17 + index as u32;
            let mut frame = match message {
                Some(message) => codec::encode(request_id, message).unwrap(),
                None => data::encode_request(request_id, &request()).unwrap(),
            };
            talon_transport::envelope::encode_credential_broker(
                &mut frame,
                &broker_config("rejected"),
            )
            .unwrap();
            let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
            stream.write_all(&frame).await.unwrap();
            replies.push(
                talon_transport::read_frame(&mut stream, std::time::Duration::from_secs(5))
                    .await
                    .unwrap(),
            );
        }
        server.await.unwrap();

        for (index, (header, body)) in replies.into_iter().enumerate() {
            assert_eq!(header.request_id, 17 + index as u32);
            assert_eq!(header.version, 4);
            let diagnostic = if requests[index].is_some() {
                assert_eq!(header.msg_type, talon_transport::MsgType::Control);
                assert!(!header.flags.contains(Flags::ERROR));
                let mut frame = Vec::with_capacity(HEADER_LEN + body.len());
                frame.extend_from_slice(&header.encode());
                frame.extend_from_slice(&body);
                match codec::decode(&frame).unwrap().1 {
                    ControlMessage::ControlFailure { code, message } if index < 2 => {
                        assert_eq!(code, DataErrorCode::Origin);
                        message
                    }
                    ControlMessage::Ack { ok: false, detail } if index >= 2 => detail.unwrap(),
                    reply => panic!("unexpected rejection for {:?}: {reply:?}", requests[index]),
                }
            } else {
                assert_eq!(header.msg_type, talon_transport::MsgType::GetRange);
                assert!(header.flags.contains(Flags::ERROR));
                let error = data::decode_error_payload(&body);
                assert_eq!(error.code, DataErrorCode::Origin);
                error.message
            };
            assert!(
                diagnostic.contains("broker returned HTTP status 403"),
                "lost broker diagnostic: {diagnostic}"
            );
        }
    }

    #[tokio::test]
    async fn tokio_broker_rejections_preserve_reply_type_and_diagnostic() {
        check_broker_rejection_replies(false).await;
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn uring_broker_rejections_preserve_reply_type_and_diagnostic() {
        check_broker_rejection_replies(true).await;
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_uring_binds_cold_v4_credentials_before_cache_access() {
        use talon_transport::{data, Flags, FrameHeader, HEADER_LEN};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config();
        cfg.l2_page_size_bytes = 0;
        let http = Arc::new(CredentialHttp::default());
        let worker = open_broker(dir.path(), &cfg, http.clone());
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
        obs.readiness().set_backend_ready(true);
        obs.readiness().set_store_ready(true);
        obs.readiness().set_control_registered(true);
        let handle = tokio::runtime::Handle::current();
        let (ready, address) = tokio::sync::oneshot::channel();
        let server = std::thread::spawn(move || {
            // Production ring threads enter the worker's Tokio handle so a
            // cold Broker exchange can spawn credential initialization there.
            let _entered = handle.enter();
            monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                .with_iowq_max_workers(1, 1)
                .enable_timer()
                .build()
                .unwrap()
                .block_on(async {
                    let listener = monoio::net::TcpListener::bind("127.0.0.1:0").unwrap();
                    ready.send(listener.local_addr().unwrap()).unwrap();
                    let (stream, _) = listener.accept().await.unwrap();
                    crate::uring_conn::handle_conn(stream, worker, obs)
                        .await
                        .unwrap();
                });
        });
        let mut stream = tokio::net::TcpStream::connect(address.await.unwrap())
            .await
            .unwrap();
        let cached = CachedRangeRequest {
            object: request().object,
            version: Version::new("v1"),
            offset: 0,
            len: 4,
        };
        for (id, cached_only, expected) in [
            ("a", true, None),
            ("a", false, Some(vec![42; 4])),
            ("b", true, None),
            ("b", false, Some(vec![84; 4])),
            ("a", true, Some(vec![42; 4])),
        ] {
            let mut frame = if cached_only {
                data::encode_cached_request(1, &cached).unwrap()
            } else {
                data::encode_request(1, &request()).unwrap()
            };
            talon_transport::envelope::encode_credential_broker(&mut frame, &broker_config(id))
                .unwrap();
            stream.write_all(&frame).await.unwrap();
            let mut header = [0; HEADER_LEN];
            stream.read_exact(&mut header).await.unwrap();
            let header = FrameHeader::decode(&header).unwrap();
            let mut body = vec![0; header.length as usize];
            stream.read_exact(&mut body).await.unwrap();
            assert_eq!(header.version, 4);
            if let Some(expected) = expected {
                assert!(!header.flags.contains(Flags::ERROR));
                assert_eq!(body, expected);
            } else {
                assert!(header.flags.contains(Flags::ERROR));
                assert_eq!(
                    data::decode_error_payload(&body).code,
                    talon_transport::DataErrorCode::CacheMiss
                );
            }
        }
        assert_eq!(http.exchanges.load(Ordering::SeqCst), 2);
        assert_eq!(http.gets.load(Ordering::SeqCst), 2);
        drop(stream);
        tokio::task::spawn_blocking(move || server.join().unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn credential_scopes_isolate_admissions_load_versions_and_l1() {
        for paged in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = config();
            cfg.l1_capacity_bytes = 8;
            cfg.l1_page_size_bytes = 4;
            if !paged {
                cfg.l2_page_size_bytes = 0;
            }
            let root = open(dir.path(), &cfg, Arc::default());
            let registry = root.registry().unwrap();
            let namespace = NamespaceId::named("analytics");
            let a = registry
                .resolve(&namespace, Some(scope('a')), Arc::new(Origin::default()))
                .await
                .unwrap();
            let b = registry
                .resolve(&namespace, Some(scope('b')), Arc::new(Origin::default()))
                .await
                .unwrap();
            let cached = CachedRangeRequest {
                object: request().object,
                version: Version::new("v1"),
                offset: 0,
                len: 4,
            };
            a.admit_cached_block(
                &CachedBlockPutRequest {
                    block: block(16),
                    object_len: 64,
                    body_len: 16,
                },
                Bytes::from_static(&[42; 16]),
            )
            .await
            .unwrap();
            assert_eq!(a.serve_cached(&cached).await.unwrap().to_vec(), [42; 4]);
            assert!(b.serve_cached(&cached).await.is_err());
            assert!(root.serve_cached(&cached).await.is_err());
            a.stat_object(&request().object).await.unwrap();
            assert!(a.cached_version(&request().object).is_some());
            assert!(b.cached_version(&request().object).is_none());
            b.load_block(&block(16), 16).await.unwrap();
            assert_eq!(
                b.serve_cached(&cached).await.unwrap().to_vec(),
                [0, 1, 2, 3]
            );
            assert_eq!(a.serve_cached(&cached).await.unwrap().to_vec(), [42; 4]);
            assert_eq!(a.namespace(), &namespace);
            assert_eq!(b.namespace(), &namespace);
            assert!(Arc::ptr_eq(&a.rate_limiter, &b.rate_limiter));
            assert!(Arc::ptr_eq(&a.load_slots, &b.load_slots));
            assert!(Arc::ptr_eq(&a.load_bytes, &b.load_bytes));
            assert!(registry.l1_budget.residency().1 <= 8);
            assert_eq!(
                a.page_gc_metrics.tti_seconds.get(),
                b.page_gc_metrics.tti_seconds.get()
            );
            b.page_gc_metrics.dirty_blocks.set(3.0);
            a.page_gc_metrics.dirty_blocks.set(7.0);
            assert_eq!(b.page_gc_metrics.dirty_blocks.get(), 3.0);
        }
    }

    #[tokio::test]
    async fn recovered_scope_waits_for_binding_and_local_eviction_remains_available() {
        for paged in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = config();
            if !paged {
                cfg.l2_page_size_bytes = 0;
            }
            let root = open(dir.path(), &cfg, Arc::default());
            let namespace = NamespaceId::named("dynamic-only");
            let scoped = root
                .registry()
                .unwrap()
                .resolve(&namespace, Some(scope('a')), Arc::new(Origin::default()))
                .await
                .unwrap();
            read(&scoped).await;
            drop(scoped);
            drop(root);

            let restarted = open(dir.path(), &cfg, Arc::default());
            let registry = restarted.registry().unwrap();
            let recovered = registry
                .snapshot()
                .into_iter()
                .find(|worker| worker.namespace() == &namespace)
                .unwrap();
            assert!(recovered.backend.get().is_none());
            assert!(super::super::stale_gc::StaleGc::new(&cfg)
                .run_slice(&recovered)
                .await
                .is_none());
            assert!(recovered
                .serve_versioned(&request(), &Version::new("v1"), 64)
                .await
                .is_err());
            let origin = Arc::new(Origin::default());
            let rebound = registry
                .resolve(&namespace, Some(scope('a')), origin.clone())
                .await
                .unwrap();
            assert!(Arc::ptr_eq(&recovered, &rebound));
            read(&rebound).await;
            assert_eq!(
                origin.reads.load(Ordering::SeqCst),
                0,
                "rebinding reuses only this scope's persisted bytes"
            );
            rebound.evict_to_target(0).await;
            assert_eq!(rebound.resident_bytes(), 0);
        }
    }

    #[tokio::test]
    async fn canceled_scope_initializer_is_published_once() {
        let dir = tempfile::tempdir().unwrap();
        let root = open(dir.path(), &config(), Arc::default());
        let registry = root.registry().unwrap();
        let guard = registry.initialize.lock().await;
        let pending = registry.clone();
        let task = tokio::spawn(async move {
            pending
                .resolve(
                    &NamespaceId::default(),
                    Some(scope('a')),
                    Arc::new(Origin::default()),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while registry.initializers.active_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        let _ = task.await;
        drop(guard);
        root.drain_namespace_initializers().await;
        let a = registry
            .resolve(
                &NamespaceId::default(),
                Some(scope('a')),
                Arc::new(Origin::default()),
            )
            .await
            .unwrap();
        let b = registry
            .resolve(
                &NamespaceId::default(),
                Some(scope('a')),
                Arc::new(Origin::default()),
            )
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(root.namespace_runtimes().len(), 2);
    }

    #[tokio::test]
    async fn concurrent_scopes_do_not_coalesce_origin_fills() {
        for paged in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = config();
            if !paged {
                cfg.l2_page_size_bytes = 0;
            }
            let root = open(dir.path(), &cfg, Arc::default());
            let registry = root.registry().unwrap();
            let origin = Arc::new(Origin::default());
            let a = registry
                .resolve(&NamespaceId::default(), Some(scope('a')), origin.clone())
                .await
                .unwrap();
            let b = registry
                .resolve(&NamespaceId::default(), Some(scope('b')), origin.clone())
                .await
                .unwrap();
            tokio::join!(read(&a), read(&b));
            assert_eq!(origin.reads.load(Ordering::SeqCst), 2);
            assert_eq!(a.resident_bytes(), b.resident_bytes());
        }
    }

    #[tokio::test]
    async fn credential_scopes_consume_the_existing_total_runtime_limit() {
        let dir = tempfile::tempdir().unwrap();
        let root = open(dir.path(), &config(), Arc::default());
        let registry = root.registry().unwrap();
        // Populate only registry bookkeeping to exercise the boundary without
        // allocating thousands of independent disk stores in a unit test.
        let existing = registry.snapshot().pop().unwrap();
        {
            let mut workers = registry.workers.write().unwrap();
            for n in 0..MAX_CACHE_RUNTIMES - 1 {
                workers.insert(
                    CacheIdentity::new(
                        &NamespaceId::default(),
                        Some(format!("v1-{n:064x}").parse().unwrap()),
                    ),
                    existing.clone(),
                );
            }
        }
        assert!(registry
            .resolve(
                &NamespaceId::default(),
                Some(scope('f')),
                Arc::new(Origin::default())
            )
            .await
            .is_err());
        assert!(root
            .for_namespace(&NamespaceId::named("overflow"))
            .await
            .is_err());
        assert!(!dir.path().join("default/scopes").exists());
        assert!(!dir.path().join("overflow").exists());
    }

    #[tokio::test]
    async fn restart_accounts_for_persisted_credential_scopes_without_requests() {
        for paged in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = config();
            if !paged {
                cfg.l2_page_size_bytes = 0;
            }
            drop(open(dir.path(), &cfg, Arc::default()));
            let scope = dir
                .path()
                .join("default/scopes")
                .join(format!("v1-{}", "a".repeat(64)));
            std::fs::create_dir_all(&scope).unwrap();
            if paged {
                let store = PagedBlockStore::open(scope.join("paged"), 4).unwrap();
                store.write_sidecar(&block(16), 16).unwrap();
                store
                    .put_page(&block(16), PageIndex(0), Bytes::from_static(b"data"))
                    .unwrap();
            } else {
                let store = WholeBlockStore::open(&scope).unwrap();
                store
                    .put(&block(16), Bytes::from_static(b"data"))
                    .await
                    .unwrap();
            }
            let restarted = open(dir.path(), &cfg, Arc::default());
            let runtimes = restarted.namespace_runtimes();
            assert_eq!(runtimes.len(), 2, "idle scopes must be recovered");
            assert_eq!(
                runtimes
                    .iter()
                    .map(|worker| worker.resident_bytes())
                    .sum::<u64>(),
                disk_unit()
            );
        }
    }

    #[tokio::test]
    async fn allocated_residency_survives_replacement_recovery_and_namespace_aggregation() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let cfg = config();
        let body = Bytes::from_static(b"short-tail-13");
        let id = block(16);
        let root = open(dir.path(), &cfg, Arc::default());
        let whole = root
            .for_namespace(&NamespaceId::named("whole"))
            .await
            .unwrap();
        let paged = root
            .for_namespace(&NamespaceId::named("paged"))
            .await
            .unwrap();
        let mut expected_whole = 0;
        let mut expected_paged = 0;
        for _ in 0..2 {
            whole.commit_cached_block(&id, body.clone()).await.unwrap();
            let handle = whole.store.get_block(&id).await.unwrap();
            expected_whole = std::fs::File::from(handle.fd.try_clone().unwrap())
                .metadata()
                .unwrap()
                .blocks()
                * 512;
            expected_paged = 0;
            for (page, chunk) in body.chunks(4).enumerate() {
                paged
                    .commit_fetched_page(
                        &id,
                        PageIndex(page as u32),
                        body.len() as u64,
                        Bytes::copy_from_slice(chunk),
                    )
                    .await
                    .unwrap();
                let path = paged
                    .paged
                    .as_ref()
                    .unwrap()
                    .dir_for(&id)
                    .join(format!("{page}.page"));
                expected_paged += std::fs::metadata(path).unwrap().blocks() * 512;
            }
            assert_eq!(whole.resident_bytes(), expected_whole);
            assert_eq!(paged.resident_bytes(), expected_paged);
            assert_eq!(whole.lru.total_bytes(), expected_whole);
            assert_eq!(paged.lru.total_bytes(), expected_paged);
            assert_eq!(whole.index.get(&id).unwrap().len, body.len() as u64);
            assert_eq!(paged.index.get(&id).unwrap().len, body.len() as u64);
        }
        let obs = crate::WorkerObservability::new(
            "test".into(),
            talon_core::NodeInfo {
                id: talon_core::NodeId::new("worker"),
                address: "127.0.0.1:0".into(),
                role: talon_core::NodeRole::Worker,
            },
            "127.0.0.1:0".into(),
            cfg.capacity_bytes,
            root.index.clone(),
            root.inflight.clone(),
        )
        .unwrap();
        obs.attach_namespace_runtime(&root);
        let total = expected_whole + expected_paged;
        assert_eq!(obs.status().metrics.resident_bytes, total);
        assert!(obs
            .metrics()
            .render()
            .contains(&format!("talon_worker_resident_bytes {total}\n")));
        drop((obs, whole, paged, root));
        let root = open(dir.path(), &cfg, Arc::default());
        let whole = root
            .for_namespace(&NamespaceId::named("whole"))
            .await
            .unwrap();
        let paged = root
            .for_namespace(&NamespaceId::named("paged"))
            .await
            .unwrap();
        assert_eq!(whole.resident_bytes(), expected_whole);
        assert_eq!(paged.resident_bytes(), expected_paged);
        assert_eq!(whole.lru.total_bytes(), expected_whole);
        assert_eq!(paged.lru.total_bytes(), expected_paged);
        let tail = paged.paged.as_ref().unwrap().dir_for(&id).join("3.page");
        let tail_bytes = std::fs::metadata(tail).unwrap().blocks() * 512;
        whole.unlink_units(whole.lru.block_candidates(&id), 2).await;
        paged.unlink_units(paged.lru.block_candidates(&id), 2).await;
        assert!(
            tail_bytes > 1,
            "short tail still allocates a filesystem block"
        );
        assert_eq!(whole.resident_bytes(), 0);
        assert_eq!(paged.resident_bytes(), 0);
        assert_eq!(whole.lru.total_bytes(), 0);
        assert_eq!(paged.lru.total_bytes(), 0);
    }

    #[tokio::test]
    async fn maintenance_gauges_preserve_each_namespaces_policy_and_checkpoint_failure() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config();
        cfg.page_tti_hours = 1;
        cfg.namespaces.insert(
            "disabled".into(),
            NamespaceCacheConfig {
                page_tti_hours: Some(0),
                page_access_checkpoint_interval_minutes: Some(2),
                ..Default::default()
            },
        );
        let root = open(dir.path(), &cfg, Arc::default());
        let a = root.for_namespace(&NamespaceId::named("a")).await.unwrap();
        let z = root.for_namespace(&NamespaceId::named("z")).await.unwrap();
        let disabled = root
            .for_namespace(&NamespaceId::named("disabled"))
            .await
            .unwrap();
        assert_eq!(a.page_gc_metrics.tti_seconds.get(), 3600.0);
        assert_eq!(disabled.page_gc_metrics.tti_seconds.get(), 0.0);
        assert_eq!(a.page_gc_metrics.checkpoint_interval.get(), 60.0);
        assert_eq!(disabled.page_gc_metrics.checkpoint_interval.get(), 120.0);

        a.page_clock.set(10_000);
        read(&a).await;
        let pages = a.paged.as_ref().unwrap().dir_for(&block(16));
        // A directory at the publication path makes the checkpoint rename fail.
        let checkpoint = pages.parent().unwrap().join("access.shard");
        std::fs::create_dir(&checkpoint).unwrap();
        a.page_clock.set(210_000);
        assert_eq!(a.checkpoint_access_times().await.failures, 1);
        assert_eq!(z.checkpoint_access_times().await.failures, 0);
        assert_eq!(a.page_gc_metrics.dirty_blocks.get(), 1.0);
        assert_eq!(a.page_gc_metrics.dirty_age.get(), 200.0);
        assert_eq!(z.page_gc_metrics.dirty_blocks.get(), 0.0);
        let rendered = root.metrics.render();
        assert!(
            rendered.contains("talon_worker_page_access_dirty_blocks{namespace=\"a\"} 1\n"),
            "{rendered}"
        );
        assert!(
            rendered.contains("talon_worker_page_access_dirty_blocks{namespace=\"z\"} 0\n"),
            "{rendered}"
        );
        std::fs::remove_dir(checkpoint).unwrap();
        assert_eq!(a.checkpoint_access_times().await.failures, 0);
        assert_eq!(a.page_gc_metrics.dirty_blocks.get(), 0.0);
    }

    #[tokio::test]
    async fn l1_metrics_refresh_does_not_wait_for_namespace_registry() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config();
        cfg.l1_capacity_bytes = 8;
        cfg.l1_page_size_bytes = 4;
        let root = open(dir.path(), &cfg, Arc::default());
        let a = root.for_namespace(&NamespaceId::named("a")).await.unwrap();
        read(&a).await;
        let registry = root.registry().unwrap();
        let guard = registry.workers.write().unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            a.refresh_l1_metrics();
            send.send(()).unwrap();
        });
        let result = receive.recv_timeout(Duration::from_secs(2));
        drop(guard);
        thread.join().unwrap();
        result.expect("L1 accounting must not scan the namespace registry");
        assert!(root
            .metrics
            .render()
            .contains("talon_worker_l1_resident_bytes 4\n"));
    }

    #[tokio::test]
    async fn namespace_runtime_reads_populate_shared_descriptor_caches() {
        for capacity in [None, Some(0), Some(1)] {
            for paged in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let mut config = config();
                if !paged {
                    config.l2_page_size_bytes = 0;
                }
                if let Some(capacity) = capacity {
                    config.whole_fd_cache_capacity = capacity;
                    config.page_fd_cache_capacity = capacity;
                }
                let root = open(dir.path(), &config, Arc::default());
                for name in ["a", "b", "c"] {
                    let namespace = root.for_namespace(&NamespaceId::named(name)).await.unwrap();
                    read(&namespace).await;
                    // Repeat a warm read, including when descriptor caching is disabled.
                    read(&namespace).await;
                }
                let registry = root.registry().unwrap();
                let expected = capacity.unwrap_or(3);
                assert_eq!(registry.whole_fds.len(), if paged { 0 } else { expected });
                assert_eq!(registry.page_fds.len(), if paged { expected } else { 0 });
            }
        }
    }

    #[tokio::test]
    async fn new_namespace_stat_preserves_existing_l1_metrics() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config();
        config.l1_capacity_bytes = 8;
        config.l1_page_size_bytes = 4;
        let root = open(dir.path(), &config, Arc::default());
        let a = root.for_namespace(&NamespaceId::named("a")).await.unwrap();
        read(&a).await;
        let check = || {
            assert_eq!(a.l1_resident_bytes(), 4);
            let metrics = root.metrics.render();
            assert!(metrics.contains("talon_worker_l1_pages 1\n"), "{metrics}");
            assert!(
                metrics.contains("talon_worker_l1_resident_bytes 4\n"),
                "{metrics}"
            );
        };
        check();
        let b = root.for_namespace(&NamespaceId::named("b")).await.unwrap();
        b.stat_object(&request().object).await.unwrap();
        check();
        read(&a).await;
        check();
    }

    #[tokio::test]
    async fn reserved_namespaces_fail_request_validation_before_opening_files() {
        let dir = tempfile::tempdir().unwrap();
        let root = open(dir.path(), &config(), Arc::default());
        for name in ["worker_identity", ".worker.lock"] {
            let frame = talon_transport::data::encode_namespace_request(
                1,
                &talon_transport::data::NamespaceScopedRange {
                    namespace: NamespaceId::named(name),
                    request: request(),
                },
            )
            .unwrap();
            let header = talon_transport::FrameHeader::decode(&frame).unwrap();
            let result = root
                .for_request(&header, &frame[talon_transport::HEADER_LEN..])
                .await;
            assert!(matches!(
                result,
                Err(talon_transport::DataPlaneError {
                    code: talon_transport::DataErrorCode::InvalidRequest,
                    ..
                })
            ));
        }
        assert_eq!(root.namespace_runtimes().len(), 1);
    }

    #[tokio::test]
    async fn namespaces_share_block_hash_but_never_files_indices_or_inflight_fills() {
        for paged in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            let mut config = config();
            if !paged {
                config.l2_page_size_bytes = 0;
            }
            let root = open(dir.path(), &config, origin.clone());
            let (ta, tb) = (NamespaceId::named("a"), NamespaceId::named("b"));
            let (a, b) = tokio::join!(root.for_namespace(&ta), root.for_namespace(&tb));
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
                for namespace in [&a, &b] {
                    assert_eq!(namespace.store.scan().unwrap()[0].id, block(16));
                }
                assert!(a.store.root().ends_with("a/shared"));
                assert!(b.store.root().ends_with("b/shared"));
            }
            drop(a);
            drop(b);
            drop(root);
            let restarted = open(dir.path(), &config, origin.clone());
            let a = restarted
                .for_namespace(&NamespaceId::named("a"))
                .await
                .unwrap();
            let b = restarted
                .for_namespace(&NamespaceId::named("b"))
                .await
                .unwrap();
            tokio::join!(read(&a), read(&b));
            assert_eq!(
                origin.reads.load(Ordering::SeqCst),
                2,
                "restart must recover both namespace caches"
            );
        }
    }

    #[tokio::test]
    async fn interrupted_namespace_creation_recovers_and_symlinks_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("partial")).unwrap();
        let root = open(dir.path(), &config(), Arc::default());
        let partial = root
            .for_namespace(&NamespaceId::named("partial"))
            .await
            .unwrap();
        read(&partial).await;
        assert!(dir
            .path()
            .join("partial/shared/cache_geometry.json")
            .exists());
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("alias")).unwrap();
        assert!(root
            .for_namespace(&NamespaceId::named("alias"))
            .await
            .is_err());
        assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);
        drop(partial);
        drop(root);
        let lock = Arc::new(CacheRootLock::acquire(dir.path()).unwrap());
        assert!(WorkerRuntime::open_namespaces(
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
        let task =
            tokio::spawn(async move { request.for_namespace(&NamespaceId::named("new")).await });
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
        root.drain_namespace_initializers().await;
        assert_eq!(registry.snapshot().len(), 2);
        let a = root
            .for_namespace(&NamespaceId::named("new"))
            .await
            .unwrap();
        let b = root
            .for_namespace(&NamespaceId::named("new"))
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        assert!(a
            .staging_file()
            .unwrap()
            .path()
            .starts_with(dir.path().join("new/shared")));
    }

    #[tokio::test]
    async fn foreground_capacity_is_shared_across_namespaces() {
        for paged in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = config();
            cfg.capacity_bytes = 4 * disk_unit();
            if !paged {
                cfg.l2_page_size_bytes = 0;
            }
            let root = open(dir.path(), &cfg, Arc::default());
            for name in ["a", "b", "c", "d", "e"] {
                read(&root.for_namespace(&NamespaceId::named(name)).await.unwrap()).await;
                assert!(
                    root.namespace_runtimes()
                        .iter()
                        .map(|w| w.resident_bytes())
                        .sum::<u64>()
                        <= cfg.capacity_bytes
                );
            }
        }
    }

    #[tokio::test]
    async fn unscoped_v1_and_v2_operations_select_default_namespace() {
        use talon_transport::{codec, data, envelope, ControlMessage, FrameHeader, HEADER_LEN};
        let dir = tempfile::tempdir().unwrap();
        let root = open(dir.path(), &config(), Arc::default());
        let default = root.for_namespace(&NamespaceId::default()).await.unwrap();
        // An earlier named request must not make omission inherit that namespace.
        root.for_namespace(&NamespaceId::named("previous"))
            .await
            .unwrap();
        let mut frames = vec![
            data::encode_request(1, &request()).unwrap(),
            data::encode_cached_request(
                2,
                &CachedRangeRequest {
                    object: request().object,
                    version: Version::new("v1"),
                    offset: 0,
                    len: 4,
                },
            )
            .unwrap(),
            data::encode_versioned_request(
                3,
                &data::VersionedRangeRequest {
                    request: request(),
                    version: Version::new("v1"),
                    object_len: 64,
                },
            )
            .unwrap(),
            data::encode_put_header(
                4,
                &data::PutRequest {
                    object: request().object,
                    body_len: 16,
                },
            )
            .unwrap(),
            data::encode_delete(
                5,
                &data::DeleteRequest {
                    object: request().object,
                },
            )
            .unwrap(),
            data::encode_cached_block_put_header(
                6,
                &CachedBlockPutRequest {
                    block: block(16),
                    object_len: 64,
                    body_len: 16,
                },
            )
            .unwrap(),
        ];
        for message in [
            ControlMessage::StatObject {
                object: request().object,
            },
            ControlMessage::ListObjects {
                prefix: "s3/bucket".into(),
            },
            ControlMessage::LoadBlock {
                block: block(16),
                len: 16,
            },
            ControlMessage::BatchLoad {
                blocks: vec![talon_transport::LoadBlockRequest {
                    block: block(16),
                    len: 16,
                }],
            },
        ] {
            frames.push(codec::encode(7, &message).unwrap());
        }
        let trace = talon_telemetry::TraceContext::from_w3c(
            "00-11111111111111111111111111111111-2222222222222222-01",
            None,
        )
        .unwrap();
        for frame in frames {
            let kind = FrameHeader::decode(&frame).unwrap().msg_type;
            for version in [1, 2] {
                if version == 2 && !envelope::supports_v2(kind) {
                    continue;
                }
                let mut wire = frame.clone();
                if version == 2 {
                    envelope::encode(&mut wire, Some(&trace), Some([1; 16])).unwrap();
                }
                let header = FrameHeader::decode(&wire).unwrap();
                let resolved = root
                    .for_request(&header, &wire[HEADER_LEN..])
                    .await
                    .unwrap();
                assert!(Arc::ptr_eq(&resolved, &default), "{kind:?} v{version}");
                assert!(resolved.store.root().ends_with("default/shared"));
            }
        }
        assert_eq!(root.namespace_runtimes().len(), 2);
        assert!(!dir.path().join("unattributed").exists());
    }

    #[tokio::test]
    async fn unspecified_and_explicit_default_share_one_namespace() {
        let dir = tempfile::tempdir().unwrap();
        let root = open(dir.path(), &config(), Arc::default());
        let legacy = root
            .for_namespace(&NamespaceId::Unattributed)
            .await
            .unwrap();
        let named = root
            .for_namespace(&NamespaceId::named("default"))
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&legacy, &named));
        read(&legacy).await;
        assert_eq!(named.block_count(), 1);
        assert!(named.store.root().ends_with("default/shared"));
        assert!(!dir.path().join("unattributed").exists());
        for name in ["../escape", ".", "..", "a/b", "a\\b", ""] {
            assert!(root.for_namespace(&NamespaceId::named(name)).await.is_err());
        }
    }

    #[tokio::test]
    async fn policies_control_geometry_and_idle_expiration_independently() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config();
        config.namespaces.insert(
            "short".into(),
            NamespaceCacheConfig {
                page_tti_hours: Some(1),
                ..Default::default()
            },
        );
        config.namespaces.insert(
            "long".into(),
            NamespaceCacheConfig {
                l2_page_size_bytes: Some(8),
                page_tti_hours: Some(2),
                ..Default::default()
            },
        );
        let root = open(dir.path(), &config, Arc::default());
        let short = root
            .for_namespace(&NamespaceId::named("short"))
            .await
            .unwrap();
        let long = root
            .for_namespace(&NamespaceId::named("long"))
            .await
            .unwrap();
        let inherited = root
            .for_namespace(&NamespaceId::named("unconfigured"))
            .await
            .unwrap();
        assert_eq!(inherited.block_size, 16);
        assert_eq!(inherited.paged_page_size(), Some(4));
        assert_eq!(inherited.page_gc_config.tti_ms, 0);
        assert_eq!(long.block_size, 16);
        assert_eq!(long.paged_page_size(), Some(8));
        for worker in [&short, &long] {
            worker.page_clock.set(10_000);
            read(worker).await;
        }
        assert_eq!(short.logical_resident_bytes(), 4);
        assert_eq!(long.logical_resident_bytes(), 8);
        for worker in [&short, &long] {
            worker.page_clock.set(10_000 + 3_600_001);
        }
        assert_eq!(short.gc_once().await.bytes, 4);
        assert_eq!(long.gc_once().await.bytes, 0);
        assert_eq!(short.block_count(), 0);
        assert_eq!(long.logical_resident_bytes(), 8);
        assert!(long.load_block(&block(32), 32).await.is_err());
        long.load_block(&block(16), 16).await.unwrap();
    }

    #[tokio::test]
    async fn one_l1_budget_and_load_budget_cover_all_namespaces() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config();
        config.l1_capacity_bytes = 4;
        config.l1_page_size_bytes = 4;
        config.load_inflight_bytes = 32;
        let root = open(dir.path(), &config, Arc::default());
        let a = root.for_namespace(&NamespaceId::named("a")).await.unwrap();
        let b = root.for_namespace(&NamespaceId::named("b")).await.unwrap();
        tokio::join!(read(&a), read(&b));
        assert!(a.l1_resident_bytes() + b.l1_resident_bytes() <= 4);
        assert!(Arc::ptr_eq(&a.load_slots, &b.load_slots));
        assert!(Arc::ptr_eq(&a.load_bytes, &b.load_bytes));
        assert_eq!(a.load_inflight_bytes, 32);
        assert_eq!(b.load_inflight_bytes, 32);
        let reservation = a.load_bytes.clone().try_acquire_many_owned(32).unwrap();
        assert!(b.load_bytes.try_acquire().is_err());
        assert!(root.load_bytes.try_acquire().is_err());
        drop(reservation);
        assert_eq!(b.load_bytes.available_permits(), 32);
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
        let mut frame = talon_transport::data::encode_namespace_request(
            1,
            &talon_transport::data::NamespaceScopedRange {
                namespace: NamespaceId::named("a"),
                request: request(),
            },
        )
        .unwrap();
        talon_transport::envelope::encode_namespace(&mut frame, &NamespaceId::named("b")).unwrap();
        let header = talon_transport::FrameHeader::decode(&frame).unwrap();
        assert!(root
            .for_request(&header, &frame[talon_transport::HEADER_LEN..])
            .await
            .is_err());
        let a = root.for_namespace(&NamespaceId::named("a")).await.unwrap();
        read(&a).await;
        drop(a);
        drop(root);
        config.namespaces.insert(
            "a".into(),
            NamespaceCacheConfig {
                l2_page_size_bytes: Some(8),
                ..Default::default()
            },
        );
        let lock = Arc::new(CacheRootLock::acquire(dir.path()).unwrap());
        assert!(WorkerRuntime::open_namespaces(
            &config,
            lock,
            Arc::new(Origin::default()),
            WorkerMetrics::new(1024)
        )
        .is_err());
    }
    #[tokio::test]
    async fn mixed_namespace_wire_requests_isolate_reads_admission_load_and_batch_load() {
        use talon_transport::{codec, data, ControlMessage, Flags, FrameHeader, HEADER_LEN};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        async fn exchange(
            stream: &mut tokio::net::TcpStream,
            mut frame: Vec<u8>,
            namespace: Option<&str>,
            body: &[u8],
        ) -> (FrameHeader, Vec<u8>) {
            if let Some(namespace) = namespace {
                talon_transport::envelope::encode_namespace(
                    &mut frame,
                    &NamespaceId::named(namespace),
                )
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
        let worker = open_broker(dir.path(), &cfg, Arc::new(CredentialHttp::default()));
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
        obs.attach_namespace_runtime(&worker);
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
        // Same physical connection switches namespace on every request.
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
        for namespace in [None, Some("b")] {
            let (header, body) = exchange(&mut stream, cached(), namespace, &[]).await;
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
        // V3 metadata selects the namespace without changing the versioned read body.
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
                    .for_namespace(&NamespaceId::named(name))
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
        // v4 admissions retain the raw response body and echo the request
        // version. This connection can switch credentials just like namespaces.
        let mut admission = data::encode_cached_block_put_header(
            9,
            &CachedBlockPutRequest {
                block: block(16),
                object_len: 64,
                body_len: 16,
            },
        )
        .unwrap();
        talon_transport::envelope::encode_credential_broker(&mut admission, &broker_config("a"))
            .unwrap();
        let (header, body) = exchange(&mut stream, admission, None, &[51; 16]).await;
        assert_eq!(header.version, 4);
        assert!(!header.flags.contains(Flags::ERROR));
        assert!(body.is_empty());
        for (id, expected) in [("a", Some(vec![51; 4])), ("b", None)] {
            let mut frame = cached();
            talon_transport::envelope::encode_credential_broker(&mut frame, &broker_config(id))
                .unwrap();
            let (header, body) = exchange(&mut stream, frame, None, &[]).await;
            assert_eq!(header.version, 4);
            if let Some(expected) = expected {
                assert!(!header.flags.contains(Flags::ERROR));
                assert_eq!(body, expected);
            } else {
                assert_eq!(
                    data::decode_error_payload(&body).code,
                    talon_transport::DataErrorCode::CacheMiss
                );
            }
        }
        let scoped = select_broker(&worker, "a", cached()).await.unwrap();
        let scope_root = scoped.store.root().to_path_buf();
        std::fs::rename(&scope_root, scope_root.with_extension("saved")).unwrap();
        std::fs::write(&scope_root, b"block disk writes").unwrap();
        let mut other = block(16);
        other.object.object_path = "failed-admission".into();
        let mut admission = data::encode_cached_block_put_header(
            10,
            &CachedBlockPutRequest {
                block: other,
                object_len: 64,
                body_len: 16,
            },
        )
        .unwrap();
        talon_transport::envelope::encode_credential_broker(&mut admission, &broker_config("a"))
            .unwrap();
        let (header, body) = exchange(&mut stream, admission, None, &[51; 16]).await;
        assert_eq!(header.version, 4);
        assert!(header.flags.contains(Flags::ERROR));
        assert_eq!(
            data::decode_error_payload(&body).code,
            talon_transport::DataErrorCode::Unavailable
        );
        drop(stream);
        server.await.unwrap();
    }
}
