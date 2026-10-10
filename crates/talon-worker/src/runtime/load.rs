use super::*;

pub(super) const MAX_CONCURRENT_LOAD_BLOCKS: usize = 8;

impl WorkerRuntime {
    /// Warm one assigned block using the same version-pinned path as reads.
    /// No payload is returned over the network. Paged stores are warmed in
    /// bounded windows; whole stores retain their existing whole-block fill.
    /// `len` comes from the caller's file size, so neither path needs HEAD.
    pub async fn load_block(&self, block: &BlockId, len: u64) -> anyhow::Result<()> {
        self.validate_load_block(block, len)?;
        let _permit = self
            .load_slots
            .try_acquire()
            .map_err(|_| anyhow::anyhow!("worker load capacity exhausted; retry later"))?;
        self.load_block_with_budget(block, len).await
    }

    /// Attempt every assignment with a bounded, shared concurrency budget.
    /// Failures do not cancel unrelated assignments. Reply only after all fills finish.
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
        let mut requests = blocks.iter().enumerate();
        let mut pending = FuturesUnordered::new();
        let mut failures = Vec::new();
        loop {
            while pending.len() < MAX_CONCURRENT_LOAD_BLOCKS {
                let Some((index, request)) = requests.next() else {
                    break;
                };
                pending.push(async move {
                    let result = async {
                        self.validate_load_block(&request.block, request.len)?;
                        self.load_block_with_budget(&request.block, request.len)
                            .await
                    }
                    .await;
                    result.err().map(|error| {
                        talon_transport::LoadBlockFailure::new(index as u32, error.to_string())
                    })
                });
            }
            match pending.next().await {
                Some(Some(failure)) => failures.push(failure),
                Some(None) => {}
                None => break,
            }
        }
        failures.sort_unstable_by_key(|failure| failure.index);
        Ok(failures)
    }

    async fn load_block_with_budget(&self, block: &BlockId, len: u64) -> anyhow::Result<()> {
        // Origin HTTP retries (including S3 throttling backoff) happen inside
        // this permit. Reuse the backend policy without replaying a whole fill.
        let _permit = self
            .load_block_slots
            .acquire()
            .await
            .expect("LOAD block semaphore is never closed");
        self.load_block_inner(block, len).await
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
        self.ensure_configured_backend(block.object.backend)?;
        if block.version.as_str().trim().is_empty() {
            anyhow::bail!("load requires a non-empty source version");
        }
        Ok(())
    }

    async fn load_block_inner(&self, block: &BlockId, len: u64) -> anyhow::Result<()> {
        let window = self
            .paged_page_size()
            .map(|page_size| u64::from(page_size).max(4 << 20))
            .unwrap_or(len);
        let mut loaded = 0;
        while loaded < len {
            let take = window.min(len - loaded);
            let request = RangeRequest {
                object: block.object.clone(),
                offset: block.offset + loaded,
                len: take,
            };
            let outcome = if self.paged.is_some() {
                ServeOutcome::Bytes(
                    self.paged_block_range(&request, block, loaded, take, Some(len))
                        .await?,
                )
            } else {
                self.serve_at(&request, &block.version, None).await?
            };
            let actual = match outcome {
                ServeOutcome::Bytes(bytes) => bytes.len() as u64,
                ServeOutcome::Sendfile(handle) => handle.len,
                ServeOutcome::SendfileMany(handles) => handles.iter().map(|h| h.len).sum(),
            };
            if actual != take {
                anyhow::bail!("short load for {block}: expected {take} bytes, got {actual}");
            }
            loaded += take;
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
    async fn batches_and_single_loads_share_eight_concurrent_block_fills() {
        for paged in [false, true] {
            let origin = Origin::new(206, 0, 0);
            let (_root, runtime) = setup_backend(paged, origin.backend());
            let clone = runtime.clone();
            let task = tokio::spawn(async move {
                clone.batch_load(&assignments(0, 16)).await.unwrap();
            });
            origin.wait_for_calls(8).await;
            assert_eq!(runtime.load_block_slots.available_permits(), 0);
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
            assert_eq!(runtime.load_block_slots.available_permits(), 0);
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
            assert_eq!(runtime.load_block_slots.available_permits(), 8);
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
                assert_eq!(origin.attempts.lock().unwrap().get(&0), Some(&1));
                assert!(
                    runtime.load_block_slots.available_permits() < 8,
                    "retry backoff must retain its block permit"
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
                assert_eq!(runtime.load_block_slots.available_permits(), 8);
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
                assert_eq!(runtime.load_block_slots.available_permits(), 8);
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
            assert_eq!(runtime.load_block_slots.available_permits(), 8);
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
            for tenant in [false, true] {
                let request = talon_transport::VersionedRangeRequest {
                    request: RangeRequest {
                        object: ObjectId::new(
                            talon_core::Backend::S3,
                            "bucket",
                            format!("cold-{tenant}"),
                        ),
                        offset: 1,
                        len: 20,
                    },
                    version: Version::new("v1"),
                    object_len: 21,
                };
                let frame = if tenant {
                    talon_transport::data::encode_versioned_tenant_request(
                        8,
                        &talon_transport::TenantScopedVersionedRange {
                            tenant: talon_core::TenantId::named("tenant"),
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
            assert_eq!(runtime.load_block_slots.available_permits(), 8);
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
