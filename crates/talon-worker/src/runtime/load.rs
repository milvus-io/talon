use super::*;
use futures::stream;

// Bounds scheduler bookkeeping, independently of the shared payload byte budget.
const MAX_PENDING_LOAD_RANGES: usize = 1024;
const LOAD_RANGE_BYTES: u64 = 4 << 20;

impl WorkerRuntime {
    /// Set the payload budget shared by LOAD/BatchLoad and all runtime clones.
    /// Reservations cover origin retries and cache commits, not L1 or HTTP overhead.
    pub fn with_load_inflight_bytes(mut self, bytes: u64) -> anyhow::Result<Self> {
        anyhow::ensure!(
            bytes > 0 && bytes <= tokio::sync::Semaphore::MAX_PERMITS as u64,
            "load_inflight_bytes exceeds semaphore capacity or is zero"
        );
        self.load_inflight_bytes = bytes;
        self.load_bytes = Arc::new(tokio::sync::Semaphore::new(bytes as usize));
        Ok(self)
    }

    /// Warm one block. Paged ranges within this block may fill concurrently.
    pub async fn load_block(&self, block: &BlockId, len: u64) -> anyhow::Result<()> {
        self.validate_load_block(block, len)?;
        let _permit = self
            .load_slots
            .try_acquire()
            .map_err(|_| anyhow::anyhow!("worker load capacity exhausted; retry later"))?;
        let requests = [talon_transport::LoadBlockRequest {
            block: block.clone(),
            len,
        }];
        let failures = self.load_assignments(&requests).await;
        if let Some((_, error)) = failures.into_iter().next() {
            return Err(error);
        }
        Ok(())
    }

    /// Attempt every assignment; failures do not cancel unrelated blocks.
    pub async fn batch_load(
        &self,
        blocks: &[talon_transport::LoadBlockRequest],
    ) -> anyhow::Result<Vec<talon_transport::LoadBlockFailure>> {
        anyhow::ensure!(
            !blocks.is_empty() && blocks.len() <= talon_transport::codec::MAX_BATCH_LOAD_BLOCKS,
            "invalid batch load count"
        );
        let _permit = self
            .load_slots
            .try_acquire()
            .map_err(|_| anyhow::anyhow!("worker load capacity exhausted; retry later"))?;
        Ok(self
            .load_assignments(blocks)
            .await
            .into_iter()
            .map(|(index, error)| {
                talon_transport::LoadBlockFailure::new(index as u32, error.to_string())
            })
            .collect())
    }

    async fn load_assignments(
        &self,
        blocks: &[talon_transport::LoadBlockRequest],
    ) -> std::collections::BTreeMap<usize, anyhow::Error> {
        let mut failures = std::collections::BTreeMap::new();
        let mut valid = Vec::new();
        for (index, request) in blocks.iter().enumerate() {
            match self.validate_load_block(&request.block, request.len) {
                Ok(()) => valid.push((index, request.clone())),
                Err(error) => {
                    failures.insert(index, error);
                }
            }
        }
        // Round down to whole pages so adjacent jobs never claim the same page.
        let window = match self.paged_page_size() {
            Some(size) => {
                let size = u64::from(size);
                self.load_inflight_bytes.min(LOAD_RANGE_BYTES.max(size)) / size * size
            }
            None => u64::from(self.block_size),
        };
        let jobs = valid.into_iter().flat_map(|(index, request)| {
            (0..request.len)
                .step_by(window as usize)
                .map(move |offset| {
                    (
                        index,
                        request.block.clone(),
                        offset,
                        window.min(request.len - offset),
                        request.len,
                    )
                })
        });
        let mut pending = stream::iter(jobs)
            .map(|(index, block, offset, len, block_len)| async move {
                if self.load_range_cached(&block, offset, len) {
                    return (index, Ok(()));
                }
                // Only misses wait for payload capacity. The probe uses metadata,
                // without opening files or admitting cache payloads into L1.
                // Cancellation while queued is harmless.
                let reservation = if self.paged.is_some() {
                    len
                } else {
                    u64::from(self.block_size)
                };
                let permit = self
                    .load_bytes
                    .clone()
                    .acquire_many_owned(reservation as u32)
                    .await
                    .expect("LOAD byte semaphore is never closed");
                let runtime = self.clone();
                let operation = talon_telemetry::Operation::new(
                    "talon.load.range",
                    "internal",
                    talon_telemetry::TraceParent::Inherit,
                );
                // Accepted work owns the reservation and single-flight guards through
                // disk+metadata completion, including after RPC cancellation.
                let result = self
                    .page_mutations
                    .run(async move {
                        let result = operation
                            .scope(async move {
                                let _permit = permit;
                                let request = RangeRequest {
                                    object: block.object.clone(),
                                    offset: block.offset + offset,
                                    len,
                                };
                                if runtime.paged.is_some() {
                                    runtime
                                        .paged_block_range_inner(
                                            &request,
                                            &block,
                                            offset,
                                            len,
                                            Some(block_len),
                                            true,
                                        )
                                        .await?;
                                } else {
                                    runtime
                                        .warm_whole_block(&request, &block, block_len)
                                        .await?;
                                }
                                Ok::<_, anyhow::Error>(())
                            })
                            .await;
                        operation.outcome(if result.is_ok() { "success" } else { "error" });
                        result
                    })
                    .await;
                (index, result)
            })
            .buffer_unordered(MAX_PENDING_LOAD_RANGES);
        while let Some((index, result)) = pending.next().await {
            if let Err(error) = result {
                failures.entry(index).or_insert(error);
            }
        }
        failures
    }

    /// Trust published residency metadata; LOAD does not verify cache files.
    fn load_range_cached(&self, block: &BlockId, offset: u64, len: u64) -> bool {
        let Some(meta) = self.index.get_and_touch(block) else {
            return false;
        };
        if offset.checked_add(len).map_or(true, |end| end > meta.len) {
            return false;
        }
        if matches!(meta.form, BlockForm::Whole) {
            self.record_l2_hit();
            return true;
        }
        let Some(paged) = &self.paged else {
            return false;
        };
        let page_size = u64::from(paged.page_size());
        let first = PageIndex((offset / page_size) as u32);
        let end = PageIndex((offset + len).div_ceil(page_size) as u32);
        let Some((first_guard, guards)) = self.index.page_reads_and_touch(block, first, end) else {
            return false;
        };
        // Preserve read/delete arbitration and access tracking. No guard crosses
        // a wait for the payload budget, including for partially resident ranges.
        let now = self.page_access_time();
        first_guard.record_access_and_release(now);
        for guard in guards {
            guard.record_access_and_release(now);
        }
        self.record_l2_hit();
        true
    }

    async fn warm_whole_block(
        &self,
        request: &RangeRequest,
        block: &BlockId,
        len: u64,
    ) -> anyhow::Result<()> {
        let key = LoadKey::Whole(block.clone());
        loop {
            if self.load_range_cached(block, 0, len) {
                return Ok(());
            }
            let (admission, flight) = self.inflight.admit_traced(key.clone());
            if let Some(_guard) = admission {
                self.metrics.record_cache_miss();
                let bytes = self.fetch_and_commit(request, block, true).await?;
                anyhow::ensure!(bytes.len() as u64 == len, "short load for {block}");
                // Prewarm L2 directly; no response assembly or L1 slices retain
                // the origin allocation after the payload reservation is returned.
                return Ok(());
            }
            talon_telemetry::wait_for(&flight, self.inflight.wait(&key)).await;
        }
    }

    fn validate_load_block(&self, block: &BlockId, len: u64) -> anyhow::Result<()> {
        if block.block_size != self.block_size
            || block.block_size == 0
            || block.offset % u64::from(block.block_size) != 0
            || len == 0
            || len > u64::from(block.block_size)
            || block
                .offset
                .checked_add(u64::from(block.block_size))
                .is_none()
        {
            anyhow::bail!("invalid load block or block size differs from worker configuration");
        }
        let minimum = self
            .paged_page_size()
            .map(u64::from)
            .unwrap_or(u64::from(self.block_size));
        anyhow::ensure!(
            self.load_inflight_bytes >= minimum,
            "load_inflight_bytes must fit one page or whole block"
        );
        self.ensure_configured_backend(block.object.backend)?;
        if block.version.as_str().trim().is_empty() {
            anyhow::bail!("load requires a non-empty source version");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use talon_core::{ObjectStat, Result};

    struct Backend {
        changed: AtomicBool,
        fetches: AtomicUsize,
        heads: AtomicUsize,
    }

    #[async_trait]
    impl BackendStore for Backend {
        async fn head(&self, _: &ObjectId) -> Result<ObjectStat> {
            self.heads.fetch_add(1, Ordering::SeqCst);
            Err(Error::Backend("HEAD is forbidden in LOAD tests".into()))
        }

        async fn fetch_range(&self, _: &ObjectId, offset: u64, len: u64) -> Result<Bytes> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            Ok(Bytes::from(
                (offset..(offset + len).min(21))
                    .map(|n| n as u8)
                    .collect::<Vec<_>>(),
            ))
        }

        async fn fetch_range_if_match(
            &self,
            object: &ObjectId,
            offset: u64,
            len: u64,
            version: Option<&Version>,
        ) -> Result<Bytes> {
            let current = if self.changed.load(Ordering::SeqCst) {
                "v2"
            } else {
                "v1"
            };
            if version.map(Version::as_str) != Some(current) {
                return Err(Error::VersionMismatch {
                    expected: version.unwrap().0.clone(),
                    found: current.into(),
                });
            }
            self.fetch_range(object, offset, len).await
        }
    }

    fn setup(paged: bool) -> (tempfile::TempDir, Arc<Backend>, WorkerRuntime) {
        let backend = Arc::new(Backend {
            changed: AtomicBool::new(false),
            fetches: AtomicUsize::new(0),
            heads: AtomicUsize::new(0),
        });
        let (root, runtime) = setup_backend(paged, backend.clone());
        (root, backend, runtime)
    }

    fn setup_backend(
        paged: bool,
        backend: Arc<dyn BackendStore>,
    ) -> (tempfile::TempDir, WorkerRuntime) {
        let root = tempfile::tempdir().unwrap();
        let mut runtime = WorkerRuntime::new(
            WholeBlockStore::open(root.path()).unwrap(),
            Arc::new(BlockIndex::new()),
            Arc::new(InFlightLoads::new()),
            backend,
            8,
            1024,
            WorkerMetrics::new(1024),
        );
        if paged {
            runtime = runtime
                .with_paged_store(PagedBlockStore::open(root.path().join("paged"), 4).unwrap());
        }
        (root, runtime)
    }

    fn block(offset: u64) -> BlockId {
        BlockId::new(
            ObjectId::new(talon_core::Backend::S3, "bucket", "file"),
            offset,
            8,
            Version::new("v1"),
        )
    }

    /// Exercises the production S3 + HTTP retry stack without network access.
    struct Origin {
        status: u16,
        failures: usize,
        attempts: std::sync::Mutex<std::collections::BTreeMap<u64, usize>>,
        gate: tokio::sync::Semaphore,
        entered: tokio::sync::Notify,
    }

    impl Origin {
        fn new(status: u16, failures: usize, permits: usize) -> Arc<Self> {
            Arc::new(Self {
                status,
                failures,
                attempts: Default::default(),
                gate: tokio::sync::Semaphore::new(permits),
                entered: Default::default(),
            })
        }

        fn backend(self: &Arc<Self>) -> Arc<dyn BackendStore> {
            Arc::new(talon_backend::S3Backend::new(
                talon_backend::S3Config::aws("us-east-1"),
                talon_backend::S3Credentials {
                    access_key_id: "test".into(),
                    secret_access_key: "test".into(),
                    session_token: None,
                },
                Arc::new(talon_backend::RetryingHttpClient::new(
                    self.clone(),
                    talon_backend::RetryConfig::default(),
                    42,
                )),
            ))
        }

        async fn wait_for_calls(&self, count: usize) {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let notified = self.entered.notified();
                    if self.attempts.lock().unwrap().values().sum::<usize>() >= count {
                        break;
                    }
                    notified.await;
                }
            })
            .await
            .expect("origin calls did not start concurrently");
        }
    }

    #[async_trait]
    impl talon_backend::HttpClient for Origin {
        async fn execute(
            &self,
            request: talon_backend::HttpRequest,
        ) -> std::result::Result<talon_backend::HttpResponse, String> {
            assert_eq!(
                request.method,
                talon_backend::Method::Get,
                "LOAD must not HEAD"
            );
            assert_eq!(request.header("if-match"), Some("\"v1\""));
            let (start, end) = request
                .header("range")
                .unwrap()
                .strip_prefix("bytes=")
                .unwrap()
                .split_once('-')
                .unwrap();
            let start = start.parse::<u64>().unwrap();
            let end = end.parse::<u64>().unwrap();
            let attempt = {
                let mut attempts = self.attempts.lock().unwrap();
                let count = attempts.entry(start).or_default();
                *count += 1;
                *count
            };
            self.entered.notify_one();
            self.gate.acquire().await.unwrap().forget();
            let failed = start == 0 && attempt <= self.failures;
            Ok(talon_backend::HttpResponse {
                status: if failed { self.status } else { 206 },
                // A deterministic backoff also lets tests prove permits remain
                // held throughout Retry-After, with no real-time sleeping.
                headers: if failed {
                    vec![("Retry-After".into(), "1".into())]
                } else {
                    vec![]
                },
                body: if failed {
                    Bytes::from_static(b"<Error><Code>SlowDown</Code></Error>")
                } else {
                    Bytes::from((start..=end).map(|n| n as u8).collect::<Vec<_>>())
                },
            })
        }
    }

    fn assignments(first: u64, count: u64) -> Vec<talon_transport::LoadBlockRequest> {
        (first..first + count)
            .map(|index| talon_transport::LoadBlockRequest {
                block: block(index * 8),
                len: 8,
            })
            .collect()
    }

    #[tokio::test]
    async fn default_budget_allows_more_than_eight_block_fills() {
        for paged in [false, true] {
            let origin = Origin::new(206, 0, 0);
            let (_root, runtime) = setup_backend(paged, origin.backend());
            let clone = runtime.clone();
            let task = tokio::spawn(async move { clone.batch_load(&assignments(0, 16)).await });
            origin.wait_for_calls(16).await;
            assert_eq!(runtime.load_inflight_bytes, 1 << 30);
            assert_eq!(runtime.load_bytes.available_permits(), (1 << 30) - 128);
            origin.gate.add_permits(16);
            assert!(task.await.unwrap().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn one_block_pipelines_ranges_within_budget_and_handles_short_tail() {
        let origin = Origin::new(206, 0, 0);
        let (root, mut runtime) = setup_backend(false, origin.backend());
        runtime.block_size = 16 << 20;
        runtime.capacity_bytes = 64 << 20;
        runtime.l1 = Arc::new(MemoryStore::with_limits(32 << 20, 1 << 20));
        let runtime = runtime
            .with_paged_store(PagedBlockStore::open(root.path().join("pages"), 1 << 20).unwrap())
            .with_load_inflight_bytes(8 << 20)
            .unwrap();
        let mut id = block(0);
        id.block_size = runtime.block_size;
        let len = (10 << 20) + 123;
        let clone = runtime.clone();
        let target = id.clone();
        let task = tokio::spawn(async move { clone.load_block(&target, len).await });
        origin.wait_for_calls(2).await;
        assert_eq!(runtime.load_bytes.available_permits(), 0);
        assert_eq!(
            *origin.attempts.lock().unwrap(),
            [(0, 1), (4 << 20, 1)].into()
        );
        origin.gate.add_permits(2);
        origin.wait_for_calls(3).await;
        origin.gate.add_permits(1);
        task.await.unwrap().unwrap();
        assert_eq!(runtime.load_bytes.available_permits(), 8 << 20);
        assert_eq!(
            runtime.l1.len(),
            0,
            "cold LOAD must not retain range slices in L1"
        );
        assert_eq!(
            *origin.attempts.lock().unwrap(),
            [(0, 1), (4 << 20, 1), (8 << 20, 1)].into()
        );
        let bytes = runtime
            .serve_versioned(
                &RangeRequest {
                    object: id.object.clone(),
                    offset: 0,
                    len,
                },
                &id.version,
                len,
            )
            .await
            .unwrap();
        // Check every page and the final short page through the normal read path.
        let bytes = match bytes {
            ServeOutcome::Bytes(bytes) => bytes,
            _ => panic!("L1 miss should return bytes"),
        };
        assert_eq!(bytes.len() as u64, len);
        assert!(bytes.iter().enumerate().all(|(i, b)| *b == i as u8));
        assert_eq!(origin.attempts.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn sub_window_budget_aligns_pages_and_handles_partial_page() {
        let origin = Origin::new(206, 0, 100);
        let (_root, runtime) = setup_backend(true, origin.backend());
        // Five bytes fit one four-byte page, not the usual 4 MiB range.
        let runtime = runtime.with_load_inflight_bytes(5).unwrap();
        runtime.load_block(&block(0), 7).await.unwrap();
        assert_eq!(*origin.attempts.lock().unwrap(), [(0, 1), (4, 1)].into());
        assert_eq!(runtime.load_bytes.available_permits(), 5);
        assert_eq!(runtime.inflight_loads(), 0);
    }

    #[tokio::test]
    async fn parallel_page_write_errors_drain_and_return_the_budget() {
        let origin = Origin::new(206, 0, 100);
        let (_root, runtime) = setup_backend(true, origin.backend());
        let runtime = runtime.with_load_inflight_bytes(16).unwrap();
        let dir = runtime.paged.as_ref().unwrap().dir_for(&block(0));
        std::fs::create_dir_all(dir.parent().unwrap()).unwrap();
        // An ordinary file where the block directory belongs makes both page
        // writes fail after the HTTP response has already been received.
        std::fs::write(&dir, b"not a directory").unwrap();
        let failures = runtime.batch_load(&assignments(0, 2)).await.unwrap();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].index, 0);
        assert_eq!(runtime.load_bytes.available_permits(), 16);
        assert_eq!(runtime.page_mutations.active_count(), 0);
        assert_eq!(runtime.inflight_loads(), 0);
        assert!(runtime
            .cached_page(&block(8), PageIndex(0), 8)
            .await
            .unwrap()
            .is_some());
        std::fs::remove_file(dir).unwrap();
        runtime.load_block(&block(0), 8).await.unwrap();
        assert_eq!(runtime.load_bytes.available_permits(), 16);
    }

    #[tokio::test]
    async fn cancelled_load_keeps_budget_until_blocked_disk_commit_finishes() {
        for paged in [false, true] {
            let origin = Origin::new(206, 0, 100);
            let (_root, runtime) = setup_backend(paged, origin.backend());
            let runtime = runtime.with_load_inflight_bytes(8).unwrap();
            let state = runtime.page_lifecycle.block(&block(0));
            let gate = state.gate.lock().await;
            let clone = runtime.clone();
            let task = tokio::spawn(async move { clone.load_block(&block(0), 8).await });
            origin.wait_for_calls(1).await;
            tokio::time::timeout(Duration::from_secs(5), async {
                while runtime.page_mutations.active_count() < 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert_eq!(runtime.load_bytes.available_permits(), 0);
            assert!(runtime.inflight_loads() > 0);
            drop(gate);
            tokio::time::timeout(Duration::from_secs(5), runtime.page_mutations.drain())
                .await
                .unwrap();
            assert_eq!(runtime.load_bytes.available_permits(), 8);
            assert_eq!(runtime.inflight_loads(), 0);
            runtime.load_block(&block(0), 8).await.unwrap();
            assert_eq!(origin.attempts.lock().unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn batches_and_single_loads_share_byte_budget() {
        for paged in [false, true] {
            let origin = Origin::new(206, 0, 0);
            let (_root, runtime) = setup_backend(paged, origin.backend());
            let runtime = runtime.with_load_inflight_bytes(64).unwrap();
            let clone = runtime.clone();
            let task = tokio::spawn(async move {
                clone.batch_load(&assignments(0, 16)).await.unwrap();
            });
            origin.wait_for_calls(8).await;
            assert_eq!(runtime.load_bytes.available_permits(), 0);
            assert_eq!(origin.attempts.lock().unwrap().len(), 8);

            let clone = runtime.clone();
            let other_task = tokio::spawn(async move {
                let second = assignments(16, 16);
                let single = block(32 * 8);
                let (b, c) = tokio::join!(clone.batch_load(&second), clone.load_block(&single, 8));
                b.unwrap();
                c.unwrap();
            });
            tokio::time::timeout(Duration::from_secs(5), async {
                while runtime.load_slots.available_permits() != 5 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(runtime.load_slots.available_permits(), 5);
            {
                let attempts = origin.attempts.lock().unwrap();
                assert_eq!(attempts.len(), 8);
                // The first batch alone fills the concurrent window.
                assert!(attempts.keys().all(|offset| *offset < 16 * 8));
            }
            // Let the next eight fills start, then block again. This catches
            // refills bypassing the shared budget after the first window.
            origin.gate.add_permits(8);
            origin.wait_for_calls(16).await;
            assert_eq!(origin.attempts.lock().unwrap().len(), 16);
            assert_eq!(runtime.load_bytes.available_permits(), 0);
            origin.gate.add_permits(33);
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), other_task)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(origin.attempts.lock().unwrap().len(), 33);
            assert_eq!(
                runtime.load_bytes.available_permits() as u64,
                runtime.load_inflight_bytes
            );
            assert_eq!(runtime.load_slots.available_permits(), 8);
            assert_eq!(runtime.inflight_loads(), 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn batch_retries_s3_throttling_without_replaying_successful_blocks() {
        for paged in [false, true] {
            for status in [429, 503] {
                let origin = Origin::new(status, 2, 100);
                let (_root, runtime) = setup_backend(paged, origin.backend());
                let requests = assignments(0, 3);
                let batch = runtime.batch_load(&requests);
                tokio::pin!(batch);
                assert!(futures::poll!(&mut batch).is_pending());
                origin.wait_for_calls(3).await;
                assert_eq!(origin.attempts.lock().unwrap().get(&0), Some(&1));
                assert!(
                    (runtime.load_bytes.available_permits() as u64) < runtime.load_inflight_bytes,
                    "retry backoff must retain its payload reservation"
                );
                tokio::time::advance(Duration::from_millis(999)).await;
                assert!(futures::poll!(&mut batch).is_pending());
                assert_eq!(
                    origin.attempts.lock().unwrap().get(&0),
                    Some(&1),
                    "Retry-After must be honored"
                );
                batch.await.unwrap();
                assert_eq!(
                    *origin.attempts.lock().unwrap(),
                    [(0, 3), (8, 1), (16, 1)].into()
                );
                // Repeating the batch reuses the warmed cache, including the
                // block that originally needed two retries.
                runtime.batch_load(&requests).await.unwrap();
                assert_eq!(
                    *origin.attempts.lock().unwrap(),
                    [(0, 3), (8, 1), (16, 1)].into()
                );
                assert_eq!(
                    runtime.load_bytes.available_permits() as u64,
                    runtime.load_inflight_bytes
                );
                assert_eq!(runtime.inflight_loads(), 0);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn batch_bounds_s3_retries_and_does_not_retry_permanent_errors() {
        for paged in [false, true] {
            for (status, calls) in [(429, 4), (503, 4), (403, 1), (404, 1), (412, 1)] {
                let origin = Origin::new(status, usize::MAX, 100);
                let (_root, runtime) = setup_backend(paged, origin.backend());
                let failures = runtime.batch_load(&assignments(0, 3)).await.unwrap();
                assert_eq!(failures.len(), 1);
                assert_eq!(failures[0].index, 0);
                assert_eq!(
                    *origin.attempts.lock().unwrap(),
                    [(0, calls), (8, 1), (16, 1)].into()
                );
                assert_eq!(runtime.load_slots.available_permits(), 8);
                assert_eq!(
                    runtime.load_bytes.available_permits() as u64,
                    runtime.load_inflight_bytes
                );
                assert_eq!(runtime.inflight_loads(), 0);
            }
        }
    }

    #[tokio::test]
    async fn invalid_assignments_are_reported_without_skipping_other_files() {
        for paged in [false, true] {
            let (_root, backend, runtime) = setup(paged);
            let mut requests = assignments(0, 3);
            requests[1].len = 0;
            requests[2].len = 5;
            let failures = runtime.batch_load(&requests).await.unwrap();
            assert_eq!(failures.len(), 1);
            assert_eq!(failures[0].index, 1);
            assert_eq!(backend.fetches.load(Ordering::SeqCst), 2);
        }
    }

    #[tokio::test]
    async fn failed_batch_continues_scheduling_beyond_its_window() {
        for paged in [false, true] {
            let origin = Origin::new(404, usize::MAX, 100);
            let (_root, runtime) = setup_backend(paged, origin.backend());
            let failures = runtime.batch_load(&assignments(0, 16)).await.unwrap();
            assert_eq!(failures.len(), 1);
            assert_eq!(failures[0].index, 0);
            assert_eq!(
                *origin.attempts.lock().unwrap(),
                (0..16).map(|index| (index * 8, 1)).collect()
            );
            assert_eq!(
                runtime.load_bytes.available_permits() as u64,
                runtime.load_inflight_bytes
            );
            assert_eq!(runtime.inflight_loads(), 0);
        }
    }

    #[tokio::test]
    async fn load_limit_is_shared_by_worker_runtime_clones() {
        let (_root, backend, runtime) = setup(true);
        let clone = runtime.clone();
        let permits = runtime.load_slots.acquire_many(8).await.unwrap();
        assert!(clone
            .load_block(&block(0), 8)
            .await
            .unwrap_err()
            .to_string()
            .contains("capacity exhausted"));
        assert_eq!(backend.fetches.load(Ordering::SeqCst), 0);
        drop(permits);
        clone.load_block(&block(0), 8).await.unwrap();
        assert_eq!(runtime.load_slots.available_permits(), 8);
    }

    #[tokio::test]
    async fn load_control_frame_warms_cache_and_honors_readiness() {
        use crate::WorkerObservability;
        use talon_core::{NodeId, NodeInfo, NodeRole};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for (paged, batch) in [(false, false), (true, false), (false, true), (true, true)] {
            let (_root, backend, runtime) = setup(paged);
            let obs = Arc::new(
                WorkerObservability::new(
                    "test".into(),
                    NodeInfo {
                        id: NodeId::new("worker"),
                        address: "127.0.0.1:1".into(),
                        role: NodeRole::Worker,
                    },
                    "127.0.0.1:2".into(),
                    1024,
                    runtime.index.clone(),
                    runtime.inflight.clone(),
                )
                .unwrap(),
            );
            obs.readiness().set_backend_ready(true);
            obs.readiness().set_store_ready(true);
            let worker = Arc::new(runtime);
            let runtime = worker.clone();
            let server_obs = obs.clone();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                crate::tokio_conn::handle_conn(stream, worker, server_obs)
                    .await
                    .unwrap();
            });
            let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
            for (ready, reject_one) in [(false, false), (true, false), (true, true)] {
                obs.readiness().set_control_registered(ready);
                let request = if batch {
                    ControlMessage::BatchLoad {
                        blocks: vec![
                            talon_transport::LoadBlockRequest {
                                block: block(0),
                                len: 8,
                            },
                            talon_transport::LoadBlockRequest {
                                block: {
                                    let mut block = block(8);
                                    if reject_one {
                                        block.version = Version::new("wrong-version");
                                    }
                                    block
                                },
                                len: 8,
                            },
                            talon_transport::LoadBlockRequest {
                                block: block(16),
                                len: 5,
                            },
                        ],
                    }
                } else {
                    ControlMessage::LoadBlock {
                        block: block(0),
                        len: 8,
                    }
                };
                let message = codec::encode(7, &request).unwrap();
                socket.write_all(&message).await.unwrap();
                let mut header = [0; talon_transport::HEADER_LEN];
                socket.read_exact(&mut header).await.unwrap();
                let parsed = talon_transport::FrameHeader::decode(&header).unwrap();
                assert_eq!(parsed.request_id, 7);
                let mut message = header.to_vec();
                message.resize(header.len() + parsed.length as usize, 0);
                socket
                    .read_exact(&mut message[header.len()..])
                    .await
                    .unwrap();
                match codec::decode(&message).unwrap().1 {
                    ControlMessage::BatchLoadResult { failures } if batch && ready => {
                        assert_eq!(failures.len(), usize::from(reject_one));
                        if reject_one {
                            assert_eq!(failures[0].index, 1);
                        }
                    }
                    ControlMessage::Ack { ok, .. } => assert_eq!(ok, ready),
                    other => panic!("unexpected reply {other:?}"),
                }
                assert_eq!(
                    backend.fetches.load(Ordering::SeqCst),
                    usize::from(ready) * if batch { 3 } else { 1 }
                );
                assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
            }
            assert_eq!(
                runtime
                    .serve_cached(&CachedRangeRequest {
                        object: block(0).object,
                        version: Version::new("v1"),
                        offset: 0,
                        len: 8
                    })
                    .await
                    .unwrap()
                    .len(),
                8
            );
            for namespace in [false, true] {
                let request = talon_transport::VersionedRangeRequest {
                    request: RangeRequest {
                        object: ObjectId::new(
                            talon_core::Backend::S3,
                            "bucket",
                            format!("cold-{namespace}"),
                        ),
                        offset: 1,
                        len: 20,
                    },
                    version: Version::new("v1"),
                    object_len: 21,
                };
                let frame = if namespace {
                    talon_transport::data::encode_versioned_namespace_request(
                        8,
                        &talon_transport::NamespaceScopedVersionedRange {
                            namespace: talon_core::NamespaceId::default(),
                            request,
                        },
                    )
                    .unwrap()
                } else {
                    talon_transport::data::encode_versioned_request(8, &request).unwrap()
                };
                socket.write_all(&frame).await.unwrap();
                let mut header = [0; talon_transport::HEADER_LEN];
                socket.read_exact(&mut header).await.unwrap();
                let header = talon_transport::FrameHeader::decode(&header).unwrap();
                assert!(!header.flags.contains(talon_transport::Flags::ERROR));
                let mut bytes = vec![0; header.length as usize];
                socket.read_exact(&mut bytes).await.unwrap();
                assert_eq!(bytes, (1..21).collect::<Vec<u8>>());
                assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
            }
            drop(socket);
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn sized_reads_fill_short_tail_without_head_or_current_version_update() {
        for paged in [false, true] {
            let (_root, backend, runtime) = setup(paged);
            let object = block(0).object;
            // A pinned historical read must not replace known current metadata.
            runtime.store_version(&object, &Version::new("v2"), 33);
            let request = RangeRequest {
                object: object.clone(),
                offset: 1,
                len: 20,
            };
            let outcome = runtime
                .serve_versioned(&request, &Version::new("v1"), 21)
                .await
                .unwrap();
            let ServeOutcome::Bytes(bytes) = outcome else {
                panic!("cross-block byte response")
            };
            assert_eq!(bytes.as_ref(), (1..21).collect::<Vec<u8>>());
            assert_eq!(
                runtime.cached_object_len(&object, &Version::new("v2")),
                Some(33)
            );
            assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
            let calls = backend.fetches.load(Ordering::SeqCst);
            backend.changed.store(true, Ordering::SeqCst);
            // The final one-byte page remains readable from the pinned cache.
            runtime
                .serve_versioned(
                    &RangeRequest {
                        object: object.clone(),
                        offset: 20,
                        len: 1,
                    },
                    &Version::new("v1"),
                    21,
                )
                .await
                .unwrap();
            assert_eq!(backend.fetches.load(Ordering::SeqCst), calls);
            // A different cold object still uses If-Match and rejects v2.
            let cold = RangeRequest {
                object: ObjectId::new(talon_core::Backend::S3, "bucket", "cold"),
                offset: 0,
                len: 1,
            };
            let error = runtime
                .serve_versioned(&cold, &Version::new("v1"), 21)
                .await
                .err()
                .unwrap();
            assert!(matches!(
                error.downcast_ref::<Error>(),
                Some(Error::VersionMismatch { .. })
            ));
            assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn sized_reads_reject_bad_extents_and_short_origin_without_head() {
        let (_root, backend, runtime) = setup(true);
        for (offset, len, size) in [(0, 1, 0), (20, 2, 21), (u64::MAX, 1, u64::MAX)] {
            let request = RangeRequest {
                object: block(0).object,
                offset,
                len,
            };
            assert!(runtime
                .serve_versioned(&request, &Version::new("v1"), size)
                .await
                .is_err());
        }
        assert_eq!(backend.fetches.load(Ordering::SeqCst), 0);
        let request = RangeRequest {
            object: block(0).object,
            offset: 20,
            len: 4,
        };
        assert!(runtime
            .serve_versioned(&request, &Version::new("v1"), 24)
            .await
            .is_err());
        assert!(
            runtime
                .serve_cached(&CachedRangeRequest {
                    object: request.object,
                    version: Version::new("v1"),
                    offset: 20,
                    len: 1,
                })
                .await
                .is_err(),
            "a short origin response must not publish the page"
        );
        assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cached_loads_bypass_exhausted_payload_budget_without_populating_l1() {
        // Cover whole files, page files, and whole files retained by a paged worker.
        for mode in 0..3 {
            let (root, backend, runtime) = setup(mode == 1);
            let mut runtime = runtime.with_load_inflight_bytes(8).unwrap();
            let requests: Vec<_> = [(0, 8), (8, 8), (16, 5)]
                .into_iter()
                .map(|(offset, len)| talon_transport::LoadBlockRequest {
                    block: block(offset),
                    len,
                })
                .collect();
            assert!(runtime.batch_load(&requests).await.unwrap().is_empty());
            if mode == 2 {
                runtime = runtime
                    .with_paged_store(PagedBlockStore::open(root.path().join("paged"), 4).unwrap());
            }
            // LOAD trusts published metadata, even if files disappear outside
            // the cache lifecycle. This also prevents fd-based probes regressing.
            for request in &requests {
                if mode == 1 {
                    runtime
                        .paged
                        .as_ref()
                        .unwrap()
                        .delete_block(&request.block)
                        .unwrap();
                } else {
                    runtime.store.delete(&request.block).await.unwrap();
                }
            }
            runtime.l1 = Arc::new(MemoryStore::with_limits(64, 4));
            let calls = backend.fetches.load(Ordering::SeqCst);
            backend.changed.store(true, Ordering::SeqCst);
            let _budget = runtime.load_bytes.acquire_many(8).await.unwrap();
            tokio::time::timeout(Duration::from_secs(1), async {
                for request in &requests {
                    runtime
                        .load_block(&request.block, request.len)
                        .await
                        .unwrap();
                }
                assert!(runtime.batch_load(&requests).await.unwrap().is_empty());
            })
            .await
            .expect("cache hits must complete while all payload permits are held");
            assert_eq!(runtime.load_bytes.available_permits(), 0);
            assert_eq!(backend.fetches.load(Ordering::SeqCst), calls);
            assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
            assert_eq!(runtime.l1.resident_bytes(), 0);
            assert_eq!(runtime.inflight_loads(), 0);
        }
    }

    #[tokio::test]
    async fn whole_load_waiters_recheck_residency_without_populating_l1() {
        for waiting_on_fill in [false, true] {
            let (_root, backend, runtime) = setup(false);
            let mut runtime = runtime.with_load_inflight_bytes(8).unwrap();
            runtime.l1 = Arc::new(MemoryStore::with_limits(64, 4));
            let target = block(0);
            let request = RangeRequest {
                object: target.object.clone(),
                offset: 0,
                len: 8,
            };
            let leader = runtime
                .inflight
                .admit_owned(LoadKey::Whole(target.clone()))
                .unwrap();
            let budget = runtime.load_bytes.acquire_many(8).await.unwrap();
            let load = async {
                if waiting_on_fill {
                    // Poll the inner operation directly to establish that it
                    // has reached the single-flight wait before publishing.
                    runtime.warm_whole_block(&request, &target, 8).await
                } else {
                    // The initial cache probe misses, then the RPC queues for
                    // payload capacity while another fill completes.
                    runtime.load_block(&target, 8).await
                }
            };
            tokio::pin!(load);
            assert!(futures::poll!(&mut load).is_pending());
            runtime
                .commit_cached_block(&target, Bytes::from_static(b"abcdefgh"))
                .await
                .unwrap();
            drop(leader);
            drop(budget);
            tokio::time::timeout(Duration::from_secs(1), load)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(backend.fetches.load(Ordering::SeqCst), 0);
            assert_eq!(runtime.l1.resident_bytes(), 0);
            assert_eq!(runtime.load_bytes.available_permits(), 8);
            assert_eq!(runtime.inflight_loads(), 0);
            assert!(runtime.load_range_cached(&target, 0, 8));
        }
    }

    #[tokio::test]
    async fn cached_load_probe_checks_extent_and_rejects_deleting_pages() {
        for paged in [false, true] {
            let (_root, _, runtime) = setup(paged);
            let target = block(16);
            runtime.load_block(&target, 5).await.unwrap();
            assert!(runtime.load_range_cached(&target, 0, 5));
            assert!(!runtime.load_range_cached(&target, 0, 8));
            if paged {
                let state = runtime.page_lifecycle.block(&target);
                let _gate = state.gate.lock().await;
                let candidate = state.candidate(PageIndex(1)).unwrap();
                assert!(state.claim(&candidate, runtime.page_clock.now(), None));
                // GC has claimed deletion, but has not unlinked or cleared the bit.
                assert!(!runtime.load_range_cached(&target, 0, 5));
                state.abort(&candidate, 0);
                assert!(runtime.load_range_cached(&target, 0, 5));
            }
        }
    }

    #[tokio::test]
    async fn cached_load_probe_respects_missing_residency() {
        for paged in [false, true] {
            let (_root, backend, runtime) = setup(paged);
            let runtime = runtime.with_load_inflight_bytes(8).unwrap();
            let target = block(0);
            runtime.load_block(&target, 8).await.unwrap();
            // Eviction removes both the file and its published residency.
            assert!(runtime.load_range_cached(&target, 0, 8));
            if let Some(store) = &runtime.paged {
                store.evict_page(&target, PageIndex(1)).unwrap();
                runtime.index.clear_page(&target, PageIndex(1));
            } else {
                runtime.store.delete(&target).await.unwrap();
                runtime.index.remove(&target);
            }
            let calls = backend.fetches.load(Ordering::SeqCst);
            let budget = runtime.load_bytes.acquire_many(8).await.unwrap();
            let load = runtime.load_block(&target, 8);
            tokio::pin!(load);
            assert!(tokio::time::timeout(Duration::from_millis(50), &mut load)
                .await
                .is_err());
            assert_eq!(backend.fetches.load(Ordering::SeqCst), calls);
            drop(budget);
            tokio::time::timeout(Duration::from_secs(1), load)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(backend.fetches.load(Ordering::SeqCst), calls + 1);
            assert!(runtime.load_range_cached(&target, 0, 8));
            assert_eq!(runtime.load_bytes.available_permits(), 8);
        }
    }

    #[tokio::test]
    async fn load_fills_whole_and_paged_cache_including_short_tail() {
        for paged in [false, true] {
            let (_root, backend, runtime) = setup(paged);
            for (offset, len) in [(0, 8), (8, 8), (16, 5)] {
                runtime.load_block(&block(offset), len).await.unwrap();
            }
            let calls = backend.fetches.load(Ordering::SeqCst);
            let heads = backend.heads.load(Ordering::SeqCst);
            assert_eq!(heads, 0, "cold LOAD must use the caller size without HEAD");
            backend.changed.store(true, Ordering::SeqCst);
            for (offset, len) in [(0, 8), (8, 8), (16, 5)] {
                runtime.load_block(&block(offset), len).await.unwrap();
                let request = CachedRangeRequest {
                    object: block(offset).object,
                    version: Version::new("v1"),
                    offset,
                    len,
                };
                assert_eq!(
                    runtime.serve_cached(&request).await.unwrap().as_ref(),
                    (offset..offset + len).map(|n| n as u8).collect::<Vec<_>>()
                );
            }
            assert_eq!(backend.fetches.load(Ordering::SeqCst), calls);
            assert_eq!(
                backend.heads.load(Ordering::SeqCst),
                heads,
                "resident load must not probe origin version"
            );
        }
    }

    #[tokio::test]
    async fn concurrent_loads_share_fills_and_do_not_switch_version() {
        for paged in [false, true] {
            let (_root, backend, runtime) = setup(paged);
            let block0 = block(0);
            let (a, b) = tokio::join!(
                runtime.load_block(&block0, 8),
                runtime.load_block(&block0, 8)
            );
            a.unwrap();
            b.unwrap();
            assert_eq!(backend.fetches.load(Ordering::SeqCst), 1);
            backend.changed.store(true, Ordering::SeqCst);
            assert!(runtime.load_block(&block(8), 8).await.is_err());
            assert!(runtime
                .index
                .get(&block(8))
                .is_none_or(|meta| match meta.form {
                    BlockForm::Whole => false,
                    BlockForm::Paged { present, .. } => present.count() == 0,
                }));
            assert_eq!(runtime.inflight_loads(), 0);
        }
    }

    #[tokio::test]
    async fn batch_reports_failures_retains_completed_blocks_and_shares_admission() {
        for paged in [false, true] {
            let (_root, backend, runtime) = setup(paged);
            let mut wrong = block(8);
            wrong.version = Version::new("v2");
            let requests = vec![
                talon_transport::LoadBlockRequest {
                    block: block(0),
                    len: 8,
                },
                talon_transport::LoadBlockRequest {
                    block: wrong,
                    len: 8,
                },
                talon_transport::LoadBlockRequest {
                    block: block(16),
                    len: 5,
                },
            ];
            let permits = runtime.load_slots.acquire_many(8).await.unwrap();
            assert!(runtime
                .batch_load(&requests)
                .await
                .unwrap_err()
                .to_string()
                .contains("capacity exhausted"));
            assert_eq!(backend.fetches.load(Ordering::SeqCst), 0);
            drop(permits);
            let failures = runtime.batch_load(&requests).await.unwrap();
            assert_eq!(failures.len(), 1);
            assert_eq!(failures[0].index, 1);
            assert_eq!(backend.fetches.load(Ordering::SeqCst), 2);
            assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
            assert_eq!(runtime.load_slots.available_permits(), 8);
            assert_eq!(
                runtime.load_bytes.available_permits() as u64,
                runtime.load_inflight_bytes
            );
            assert_eq!(runtime.inflight_loads(), 0);
            runtime.load_block(&block(0), 8).await.unwrap();
            runtime.load_block(&block(16), 5).await.unwrap();
            assert_eq!(
                backend.fetches.load(Ordering::SeqCst),
                2,
                "completed blocks on either side of the failure stay cached"
            );
        }
    }

    #[tokio::test]
    async fn load_rejects_bad_assignments_and_short_source() {
        let (_root, backend, runtime) = setup(false);
        for (b, len) in [
            (block(1), 8),
            (block(0), 0),
            (block(0), 9),
            (block(u64::MAX - 7), 8),
        ] {
            assert!(runtime.load_block(&b, len).await.is_err());
        }
        let mut wrong_size = block(0);
        wrong_size.block_size = 4;
        assert!(runtime.load_block(&wrong_size, 4).await.is_err());
        assert_eq!(backend.fetches.load(Ordering::SeqCst), 0);
        assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
        assert!(runtime
            .load_block(&block(16), 8)
            .await
            .unwrap_err()
            .to_string()
            .contains("short load"));
    }
}
