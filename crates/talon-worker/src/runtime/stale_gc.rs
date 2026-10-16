//! Reclaim cold local cache entries only after an authoritative backend NotFound.
use super::WorkerRuntime;
use crate::eviction::access_millis;
use futures::StreamExt;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};
use talon_core::{Error, ObjectId, WorkerConfig};

const DAY_MS: u64 = 24 * 60 * 60 * 1000;
const MAX_HEADS: usize = 32;

pub(super) struct StaleGc {
    cursor: (u64, u64),
    pending: VecDeque<crate::eviction::StaleBlockCandidate>,
    round_now: Option<u64>,
    missing: VecDeque<(crate::eviction::StaleBlockCandidate, u64)>,
    heads: Arc<crate::background::Rate>,
    concurrency: usize,
    head_batch: usize,
}
impl Default for StaleGc {
    fn default() -> Self {
        Self::new(&WorkerConfig::default())
    }
}
impl StaleGc {
    pub(super) fn new(config: &WorkerConfig) -> Self {
        Self::with_head_rate(
            config,
            Arc::new(crate::background::Rate::new(
                config.stale_gc_head_max_per_sec,
            )),
        )
    }

    pub(super) fn with_head_rate(
        config: &WorkerConfig,
        heads: Arc<crate::background::Rate>,
    ) -> Self {
        Self {
            cursor: (0, 0),
            pending: VecDeque::new(),
            round_now: None,
            missing: VecDeque::new(),
            heads,
            concurrency: config.stale_gc_head_concurrency,
            head_batch: config.stale_gc_head_concurrency.min(MAX_HEADS).min(
                if config.stale_gc_head_max_per_sec == 0 {
                    MAX_HEADS
                } else {
                    config.stale_gc_head_max_per_sec.min(MAX_HEADS as u64) as usize
                },
            ),
        }
    }

    pub(super) async fn run_slice(&mut self, worker: &WorkerRuntime) -> Option<Duration> {
        let now = *self.round_now.get_or_insert_with(access_millis);
        if self.missing.is_empty() {
            self.check_batch(worker, now).await;
        }
        self.delete_slice(worker).await;
        let done =
            self.cursor.0 >= self.cursor.1 && self.pending.is_empty() && self.missing.is_empty();
        if done {
            self.round_now = None;
        }
        Some(if done {
            Duration::from_secs(3600)
        } else {
            Duration::ZERO
        })
    }

    #[cfg(test)]
    async fn run_round_at(&mut self, worker: &WorkerRuntime, now: u64) {
        loop {
            self.tick_at(worker, now).await;
            if self.cursor.0 >= self.cursor.1 && self.pending.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    }

    #[cfg(test)]
    async fn tick_at(&mut self, worker: &WorkerRuntime, now: u64) {
        self.check_batch(worker, now).await;
        while !self.missing.is_empty() {
            self.delete_slice(worker).await;
        }
    }

    async fn delete_slice(&mut self, worker: &WorkerRuntime) {
        let Some((candidate, paid)) = self.missing.front_mut() else {
            return;
        };
        if !worker.lru.stale_candidate_is_current(candidate) {
            self.missing.pop_front();
            return;
        }
        if let Some(budget) = &worker.background_budget {
            *paid += budget.delete_slice(candidate.units as u64 - *paid).await;
        } else {
            *paid = candidate.units as u64;
        }
        if *paid == candidate.units as u64 {
            let (candidate, _) = self.missing.pop_front().unwrap();
            worker.unlink_stale_block_prepaid(candidate).await;
        }
    }

    async fn check_batch(&mut self, worker: &WorkerRuntime, now: u64) {
        if self.pending.is_empty() {
            let lru = worker.lru.clone();
            let mut cursor = self.cursor;
            let scan = worker.page_gc_config.scan_batch_size.min(4096);
            let deletes = worker.page_gc_config.delete_batch_size.min(256);
            let (cursor, candidates) = tokio::task::spawn_blocking(move || {
                let candidates = lru.stale_gc_candidates(&mut cursor, now, DAY_MS, scan, deletes);
                (cursor, candidates)
            })
            .await
            .expect("stale GC scan panicked");
            self.cursor = cursor;
            self.pending = candidates.into();
        }
        let mut groups: HashMap<ObjectId, Vec<crate::eviction::StaleBlockCandidate>> =
            HashMap::new();
        while let Some(candidate) = self.pending.front() {
            if !groups.contains_key(&candidate.block.object) && groups.len() == self.head_batch {
                break;
            }
            let candidate = self.pending.pop_front().unwrap();
            if worker.lru.stale_candidate_is_current(&candidate)
                && worker
                    .lru
                    .stale_check_due(&candidate.block.object, now, DAY_MS)
            {
                groups
                    .entry(candidate.block.object.clone())
                    .or_default()
                    .push(candidate);
            }
        }
        let heads = &self.heads;
        let work =
            futures::stream::iter(groups.into_iter().map(|(object, candidates)| async move {
                // Origin QPS is independent of local disk pacing and scan slicing.
                heads.charge(1).await;
                let missing = match tokio::time::timeout(
                    Duration::from_secs(5),
                    worker.background_head(&object),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        worker.lru.stale_verified(&object, now.max(access_millis()));
                        false
                    }
                    Ok(Err(Error::NotFound(_))) => true,
                    Ok(Err(error)) => {
                        tracing::debug!(%error, "stale GC HEAD failed; retaining cache");
                        false
                    }
                    Err(_) => false,
                };
                if missing {
                    candidates
                } else {
                    Vec::new()
                }
            }));
        let mut work = work.buffer_unordered(self.concurrency);
        while let Some(candidates) = work.next().await {
            self.missing
                .extend(candidates.into_iter().map(|candidate| (candidate, 0)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BlockIndex, CacheUnit, InFlightLoads, PagedBlockStore, WholeBlockStore, WorkerMetrics,
    };
    use bytes::Bytes;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };
    use talon_core::{Backend, BackendStore, BlockId, ObjectId, ObjectStat, PageIndex, Version};

    #[derive(Default)]
    struct Origin {
        calls: AtomicUsize,
        mode: AtomicUsize, // 0 missing, 1 exists, 2 forbidden, 3 timeout
        hold: AtomicBool,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }
    #[async_trait::async_trait]
    impl BackendStore for Origin {
        async fn fetch_range(&self, _: &ObjectId, _: u64, _: u64) -> talon_core::Result<Bytes> {
            panic!("GC must not fetch data")
        }
        async fn head(&self, object: &ObjectId) -> talon_core::Result<ObjectStat> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.entered.notify_one();
            if self.hold.load(Ordering::Relaxed) {
                self.release.notified().await;
            }
            match self.mode.load(Ordering::Relaxed) {
                0 => Err(Error::NotFound(object.to_path())),
                1 => Ok(ObjectStat {
                    len: 32,
                    version: Version::new("new-version"),
                }),
                2 => Err(Error::Backend("403 Forbidden".into())),
                _ => std::future::pending().await,
            }
        }
    }
    fn block(n: u64) -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "bucket", "key"),
            n * 32,
            32,
            Version::new("v1"),
        )
    }
    fn runtime(
        root: &std::path::Path,
        paged: bool,
        origin: Arc<dyn BackendStore>,
    ) -> WorkerRuntime {
        let store = WholeBlockStore::open(root.join("whole")).unwrap();
        let index = Arc::new(BlockIndex::new());
        for entry in store.scan_allocated().unwrap() {
            index.commit_allocated(entry.meta, entry.allocated);
        }
        let r = WorkerRuntime::new(
            store,
            index,
            Arc::new(InFlightLoads::new()),
            origin,
            32,
            0,
            WorkerMetrics::new(0),
        );
        if paged {
            r.with_paged_store(PagedBlockStore::open(root.join("paged"), 16).unwrap())
        } else {
            r
        }
    }
    async fn fill(r: &WorkerRuntime, id: &BlockId, paged: bool) {
        if paged {
            for page in 0..2 {
                r.commit_fetched_page(id, PageIndex(page), 32, Bytes::from_static(&[1; 16]))
                    .await
                    .unwrap();
            }
        } else {
            r.commit_cached_block(id, Bytes::from_static(&[1; 32]))
                .await
                .unwrap();
        }
    }
    fn cold_now() -> u64 {
        access_millis() + DAY_MS + 1
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires native io_uring; exercises Tokio GC with a real S3 HTTP backend"]
    async fn native_stale_gc_uses_existing_rings_and_cancels_http_on_timeout_and_shutdown() {
        use crate::{ConnectionAdmission, WorkerObservability};
        use talon_backend::{monoio_client::MonoioClient, S3Backend, S3Config, S3Credentials};
        use talon_core::{NodeId, NodeInfo, NodeRole};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let status = Arc::new(AtomicUsize::new(403));
        let response_status = status.clone();
        let (events, mut observed) = tokio::sync::mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let status = response_status.clone();
                let events = events.clone();
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        request.push(socket.read_u8().await.unwrap());
                    }
                    assert!(request.starts_with(b"HEAD /bucket/key "));
                    let status = status.load(Ordering::SeqCst);
                    events.send("head").unwrap();
                    match status {
                        0 => {}, // transport failure, not an authoritative NotFound
                        1 => {
                            assert!(socket.read_u8().await.is_err());
                            events.send("cancelled").unwrap();
                        },
                        _ => socket.write_all(format!(
                            "HTTP/1.1 {status} Response\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        ).as_bytes()).await.unwrap(),
                    }
                });
            }
        });
        let backend = Arc::new(S3Backend::new(
            S3Config {
                endpoint,
                region: "us-east-1".into(),
                path_style: true,
                tls: false,
            },
            S3Credentials {
                access_key_id: "test".into(),
                secret_access_key: "test".into(),
                session_token: None,
            },
            Arc::new(MonoioClient::new()),
        ));
        let root = tempfile::tempdir().unwrap();
        let mut worker = runtime(root.path(), false, backend);
        worker.background_heads = Some(super::super::origin_head::OriginHeads::new(2));
        fill(&worker, &block(0), false).await;
        let worker = Arc::new(worker);
        let obs = Arc::new(
            WorkerObservability::new(
                "test".into(),
                NodeInfo {
                    id: NodeId::new("worker"),
                    address: "127.0.0.1:0".into(),
                    role: NodeRole::Worker,
                },
                "127.0.0.1:0".into(),
                1024,
                worker.index.clone(),
                worker.inflight.clone(),
            )
            .unwrap(),
        );
        let handler = crate::uring_conn::RingConnHandler::new(worker.clone(), obs);
        let stop = Arc::new(AtomicBool::new(false));
        // Stop even if an assertion unwinds, so a failed test cannot leave rings running.
        struct Stop(Arc<AtomicBool>);
        impl Drop for Stop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let _stop = Stop(stop.clone());
        let stop_rings = stop.clone();
        let tokio = tokio::runtime::Handle::current();
        let rings = tokio::task::spawn_blocking(move || {
            crate::uring_serve::serve_with_shutdown(
                "127.0.0.1:0".into(),
                2,
                ConnectionAdmission::new(8, WorkerMetrics::new(0)),
                handler,
                tokio,
                stop_rings,
            )
        });
        assert!(!talon_uring::on_ring());
        for code in [403, 0] {
            status.store(code, Ordering::SeqCst);
            let mut gc = StaleGc::default();
            tokio::time::timeout(Duration::from_secs(3), gc.run_round_at(&worker, cold_now()))
                .await
                .unwrap();
            assert_eq!(observed.recv().await, Some("head"));
            assert_eq!(worker.logical_resident_bytes(), 32);
        }
        status.store(1, Ordering::SeqCst);
        {
            let mut gc = StaleGc::default();
            let work = gc.run_round_at(&worker, cold_now());
            tokio::pin!(work);
            tokio::select! {
                event = observed.recv() => assert_eq!(event, Some("head")),
                _ = &mut work => panic!("stalled HEAD unexpectedly completed"),
                _ = tokio::time::sleep(Duration::from_secs(3)) => panic!("HEAD never arrived"),
            }
            assert!(tokio::time::timeout(Duration::from_millis(20), work)
                .await
                .is_err());
        }
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), observed.recv())
                .await
                .unwrap(),
            Some("cancelled")
        );
        assert_eq!(worker.logical_resident_bytes(), 32);

        status.store(404, Ordering::SeqCst);
        let mut gc = StaleGc::default();
        tokio::time::timeout(Duration::from_secs(3), gc.run_round_at(&worker, cold_now()))
            .await
            .unwrap();
        assert_eq!(observed.recv().await, Some("head"));
        assert_eq!(worker.logical_resident_bytes(), 0);
        assert!(worker.index.get(&block(0)).is_none());
        assert!(worker.store.scan_allocated().unwrap().is_empty());

        fill(&worker, &block(0), false).await;
        status.store(1, Ordering::SeqCst);
        let mut gc = StaleGc::default();
        let work = gc.run_round_at(&worker, cold_now());
        tokio::pin!(work);
        tokio::select! {
            event = observed.recv() => assert_eq!(event, Some("head")),
            _ = &mut work => panic!("stalled HEAD unexpectedly completed"),
            _ = tokio::time::sleep(Duration::from_secs(3)) => panic!("HEAD never arrived"),
        }
        stop.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(3), work)
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), observed.recv())
                .await
                .unwrap(),
            Some("cancelled")
        );
        assert_eq!(worker.logical_resident_bytes(), 32);
        tokio::time::timeout(Duration::from_secs(3), rings)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn warm_scan_continuations_have_no_head_pacing_delay() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let mut r = runtime(root.path(), false, origin.clone());
        r.page_gc_config.scan_batch_size = 1;
        for n in 0..3 {
            fill(&r, &block(n), false).await;
        }
        let mut gc = StaleGc::default();
        assert_eq!(gc.run_slice(&r).await, Some(Duration::ZERO));
        assert_eq!(gc.run_slice(&r).await, Some(Duration::ZERO));
        while gc.run_slice(&r).await == Some(Duration::ZERO) {}
        assert_eq!(origin.calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn stale_gc_filters_warm_entries_then_removes_both_cache_forms() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            let r = runtime(root.path(), paged, origin.clone());
            fill(&r, &block(0), paged).await;
            fill(&r, &block(1), paged).await;
            let mut gc = StaleGc::default();
            gc.tick_at(&r, access_millis()).await;
            assert_eq!(origin.calls.load(Ordering::Relaxed), 0);
            gc.tick_at(&r, cold_now()).await;
            assert_eq!(
                origin.calls.load(Ordering::Relaxed),
                1,
                "one HEAD per object per batch"
            );
            assert_eq!(r.logical_resident_bytes(), 0);
            assert_eq!(r.lru.total_bytes(), 0);
            assert!(r.index.get(&block(0)).is_none());
            assert!(r.index.get(&block(1)).is_none());
            let recovered = runtime(root.path(), paged, origin);
            assert_eq!(recovered.logical_resident_bytes(), 0);
        }
    }

    #[tokio::test]
    async fn stale_gc_positive_checks_cover_other_offsets_for_24_hours() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            origin.mode.store(1, Ordering::Relaxed);
            let mut r = runtime(root.path(), paged, origin.clone());
            r.page_gc_config.delete_batch_size = 1;
            fill(&r, &block(0), paged).await;
            fill(&r, &block(1), paged).await;
            let mut gc = StaleGc::default();
            let now = cold_now();
            gc.tick_at(&r, now).await;
            origin.mode.store(0, Ordering::Relaxed);
            for _ in 0..8 {
                gc.tick_at(&r, now + DAY_MS - 1).await;
            }
            assert_eq!(origin.calls.load(Ordering::Relaxed), 1);
            assert_eq!(
                r.logical_resident_bytes(),
                64,
                "a version change alone is not deletion"
            );
            for _ in 0..8 {
                gc.tick_at(&r, now + DAY_MS).await;
            }
            assert_eq!(r.logical_resident_bytes(), 0);
        }
    }

    #[tokio::test]
    async fn stale_gc_errors_retain_cache_and_retry() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            origin.mode.store(2, Ordering::Relaxed);
            let r = runtime(root.path(), paged, origin.clone());
            fill(&r, &block(0), paged).await;
            let mut gc = StaleGc::default();
            let now = cold_now();
            gc.tick_at(&r, now).await;
            assert_eq!(r.logical_resident_bytes(), 32);
            origin.mode.store(0, Ordering::Relaxed);
            for _ in 0..2 {
                gc.tick_at(&r, now).await;
            }
            assert_eq!(r.logical_resident_bytes(), 0);
            assert_eq!(origin.calls.load(Ordering::Relaxed), 2);
        }
    }

    #[tokio::test]
    async fn timed_out_slice_releases_after_one_head_wave() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.mode.store(3, Ordering::Relaxed);
        let r = Arc::new(runtime(root.path(), false, origin.clone()));
        for n in 0..12 {
            let mut id = block(0);
            id.object = ObjectId::new(Backend::S3, "bucket", format!("object-{n}"));
            fill(&r, &id, false).await;
        }
        let mut gc = StaleGc::new(&WorkerConfig {
            stale_gc_head_concurrency: 2,
            stale_gc_head_max_per_sec: 0,
            ..Default::default()
        });
        gc.round_now = Some(cold_now());
        let task = tokio::spawn({
            let r = r.clone();
            async move {
                let delay = gc.run_slice(&r).await;
                (gc, delay)
            }
        });
        origin.entered.notified().await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(6)).await;
        let (gc, delay) = task.await.unwrap();
        tokio::time::resume();
        assert_eq!(origin.calls.load(Ordering::Relaxed), 2);
        assert_eq!(gc.pending.len(), 10);
        assert_eq!(delay, Some(Duration::ZERO));
        assert_eq!(r.logical_resident_bytes(), 12 * 32);
        let slow = StaleGc::new(&WorkerConfig {
            stale_gc_head_max_per_sec: 1,
            ..Default::default()
        });
        assert_eq!(
            slow.head_batch, 1,
            "at most one second of rate waiting per slice"
        );
    }

    #[tokio::test]
    async fn stale_gc_head_timeout_is_bounded_and_non_destructive() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.mode.store(3, Ordering::Relaxed);
        let r = Arc::new(runtime(root.path(), false, origin.clone()));
        fill(&r, &block(0), false).await;
        let task = tokio::spawn({
            let r = r.clone();
            async move {
                StaleGc::default().tick_at(&r, cold_now()).await;
            }
        });
        origin.entered.notified().await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(6)).await;
        task.await.unwrap();
        tokio::time::resume();
        assert_eq!(r.logical_resident_bytes(), 32);
    }

    #[tokio::test]
    async fn stale_gc_read_during_head_invalidates_the_entire_block() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            origin.hold.store(true, Ordering::Relaxed);
            let r = Arc::new(runtime(root.path(), paged, origin.clone()));
            fill(&r, &block(0), paged).await;
            let task = tokio::spawn({
                let r = r.clone();
                async move {
                    StaleGc::default().tick_at(&r, cold_now()).await;
                }
            });
            origin.entered.notified().await;
            tokio::time::sleep(Duration::from_millis(2)).await;
            if paged {
                let guards = r
                    .index
                    .page_reads_and_touch(&block(0), PageIndex(1), PageIndex(2))
                    .unwrap();
                drop(guards);
            } else {
                assert!(r.index.is_whole_and_touch(&block(0)));
            }
            origin.release.notify_one();
            task.await.unwrap();
            assert_eq!(r.logical_resident_bytes(), 32);
        }
    }

    #[tokio::test]
    async fn stale_gc_failed_block_unlink_reopens_all_pages_and_requires_a_fresh_head() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let r = runtime(root.path(), true, origin.clone());
        fill(&r, &block(0), true).await;
        let dir = r.paged.as_ref().unwrap().dir_for(&block(0));
        let saved = dir.with_extension("saved");
        std::fs::rename(&dir, &saved).unwrap();
        // A file where the block directory should be makes remove_dir_all fail
        // deterministically, including when the test runs as root.
        std::fs::write(&dir, b"not a directory").unwrap();
        let mut gc = StaleGc::default();
        gc.tick_at(&r, cold_now()).await;
        assert_eq!(r.logical_resident_bytes(), 32);
        assert_eq!(r.lru.total_bytes(), r.resident_bytes());
        let state = r.page_lifecycle.block(&block(0));
        for page in 0..2 {
            assert!(
                state.acquire(PageIndex(page)).is_some(),
                "failed unlink must reopen every page"
            );
        }
        std::fs::remove_file(&dir).unwrap();
        std::fs::rename(&saved, &dir).unwrap();
        origin.mode.store(1, Ordering::Relaxed);
        r.gc_once().await;
        assert_eq!(
            r.logical_resident_bytes(),
            32,
            "TTI GC cannot retry an old NotFound"
        );
        for _ in 0..2 {
            gc.tick_at(&r, cold_now()).await;
        }
        assert_eq!(origin.calls.load(Ordering::Relaxed), 2);
        assert_eq!(r.logical_resident_bytes(), 32);
    }

    #[tokio::test]
    async fn stale_gc_respects_pins_and_scan_delete_limits() {
        for paged in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            let mut r = runtime(root.path(), paged, origin.clone());
            r.page_gc_config.scan_batch_size = 1;
            r.page_gc_config.delete_batch_size = 1;
            fill(&r, &block(0), paged).await;
            fill(&r, &block(1), paged).await;
            let unit = if paged {
                CacheUnit::Page(block(0), PageIndex(0))
            } else {
                CacheUnit::Whole(block(0))
            };
            let pin = r.lru.pin_guard(unit).unwrap();
            let mut gc = StaleGc::default();
            let now = cold_now();
            gc.tick_at(&r, now).await;
            assert_eq!(origin.calls.load(Ordering::Relaxed), 0);
            assert_eq!(r.logical_resident_bytes(), 64);
            gc.tick_at(&r, now).await;
            assert_eq!(
                r.logical_resident_bytes(),
                32,
                "one candidate reclaims a complete block"
            );
            drop(pin);
            for _ in 0..8 {
                gc.tick_at(&r, now).await;
            }
            assert_eq!(r.logical_resident_bytes(), 0);
        }
    }
    #[tokio::test]
    async fn stale_gc_rechecks_sibling_activity_after_waiting_for_mutation_gate() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let r = Arc::new(runtime(root.path(), true, origin));
        fill(&r, &block(0), true).await;
        let state = r.page_lifecycle.block(&block(0));
        let gate = state.gate.lock().await;
        let task = tokio::spawn({
            let r = r.clone();
            async move {
                StaleGc::default().tick_at(&r, cold_now()).await;
            }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while r.page_mutations.active_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        drop(
            r.index
                .page_reads_and_touch(&block(0), PageIndex(1), PageIndex(2))
                .unwrap(),
        );
        drop(gate);
        task.await.unwrap();
        assert_eq!(r.logical_resident_bytes(), 32);
    }

    #[tokio::test]
    async fn stale_gc_caps_heads_per_batch() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.mode.store(2, Ordering::Relaxed);
        let r = runtime(root.path(), false, origin.clone());
        for n in 0..40 {
            let mut id = block(0);
            id.object = ObjectId::new(Backend::S3, "bucket", format!("key-{n}"));
            fill(&r, &id, false).await;
        }
        let mut gc = StaleGc::new(&WorkerConfig {
            stale_gc_head_concurrency: MAX_HEADS,
            ..Default::default()
        });
        gc.tick_at(&r, cold_now()).await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), MAX_HEADS);
        assert_eq!(r.logical_resident_bytes(), 40 * 32);
        gc.tick_at(&r, cold_now()).await;
        assert_eq!(
            origin.calls.load(Ordering::Relaxed),
            40,
            "resume skipped candidates instead of starving them"
        );
    }
    #[tokio::test]
    async fn stale_gc_pending_candidates_honor_positive_checks_from_previous_batch() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.mode.store(1, Ordering::Relaxed);
        let r = runtime(root.path(), false, origin.clone());
        for n in 0..33 {
            let mut id = block(0);
            id.object = ObjectId::new(Backend::S3, "bucket", format!("key-{n}"));
            fill(&r, &id, false).await;
        }
        let mut id = block(1);
        id.object = ObjectId::new(Backend::S3, "bucket", "key-0");
        fill(&r, &id, false).await;
        let mut gc = StaleGc::new(&WorkerConfig {
            stale_gc_head_concurrency: MAX_HEADS,
            ..Default::default()
        });
        let now = cold_now();
        gc.tick_at(&r, now).await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), MAX_HEADS);
        gc.tick_at(&r, now + 1).await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), 33);
        assert_eq!(r.logical_resident_bytes(), 34 * 32);
    }
    #[tokio::test]
    async fn stale_gc_round_drains_batches_without_rescanning_or_chasing_new_entries() {
        for scan_budget in [16, 4096] {
            let root = tempfile::tempdir().unwrap();
            let origin = Arc::new(Origin::default());
            origin.mode.store(2, Ordering::Relaxed);
            origin.hold.store(true, Ordering::Relaxed);
            let mut r = runtime(root.path(), false, origin.clone());
            r.page_gc_config.scan_batch_size = scan_budget;
            for n in 0..40 {
                let mut id = block(0);
                id.object = ObjectId::new(Backend::S3, "bucket", format!("key-{n}"));
                fill(&r, &id, false).await;
            }
            let r = Arc::new(r);
            let task = tokio::spawn({
                let r = r.clone();
                async move {
                    StaleGc::default().run_round_at(&r, cold_now()).await;
                }
            });
            origin.entered.notified().await;
            let mut late = block(0);
            late.object = ObjectId::new(Backend::S3, "bucket", "late");
            fill(&r, &late, false).await;
            origin.hold.store(false, Ordering::Relaxed);
            origin.release.notify_one();
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(origin.calls.load(Ordering::Relaxed), 40,
                "drain the scan/HEAD batches exactly once, retaining the round's original upper bound");
            assert_eq!(r.logical_resident_bytes(), 41 * 32);
        }
    }

    #[tokio::test]
    async fn stale_gc_scheduler_waits_an_hour_between_complete_rounds() {
        let root = tempfile::tempdir().unwrap();
        let r = Arc::new(runtime(root.path(), false, Arc::new(Origin::default())));
        let service = super::super::WorkerBackground::start(
            r.clone(),
            &talon_core::WorkerConfig {
                ownership_gc_enabled: false,
                ..Default::default()
            },
        );
        let completed = r.metrics.registry.counter(
            "talon_worker_background_task_completed_total",
            "Completed background batches by task.",
            talon_core::metrics::labels(&[("task", "stale_gc")]),
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while completed.get() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // Let the scheduler record the completion time before advancing its clock.
        tokio::time::sleep(Duration::from_millis(5)).await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(3599)).await;
        tokio::time::resume();
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(
            completed.get(),
            1,
            "no repeated scan during the hourly wait"
        );
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::time::resume();
        tokio::time::timeout(Duration::from_secs(2), async {
            while completed.get() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        service.shutdown().await;
        assert_eq!(completed.get(), 2);
    }
    #[tokio::test]
    async fn stale_gc_busy_page_defers_the_entire_block_and_releases_partial_claims() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let r = runtime(root.path(), true, origin.clone());
        fill(&r, &block(0), true).await;
        let state = r.page_lifecycle.block(&block(0));
        let reader = state.acquire(PageIndex(1)).unwrap();
        let mut gc = StaleGc::default();
        let now = cold_now();
        gc.tick_at(&r, now).await;
        assert_eq!(origin.calls.load(Ordering::Relaxed), 1);
        assert_eq!(r.logical_resident_bytes(), 32);
        assert!(
            state.acquire(PageIndex(0)).is_some(),
            "page 0's claim must roll back if page 1 is busy"
        );
        assert!(r
            .paged
            .as_ref()
            .unwrap()
            .dir_for(&block(0))
            .join("0.page")
            .exists());
        drop(reader);
        gc.tick_at(&r, now).await;
        assert_eq!(r.logical_resident_bytes(), 0);
        assert!(state.acquire(PageIndex(0)).is_none());
        assert!(!r.paged.as_ref().unwrap().dir_for(&block(0)).exists());
    }

    #[tokio::test]
    async fn stale_gc_new_sibling_during_head_invalidates_the_block_candidate() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        origin.hold.store(true, Ordering::Relaxed);
        let r = Arc::new(runtime(root.path(), true, origin.clone()));
        r.commit_fetched_page(&block(0), PageIndex(0), 32, Bytes::from_static(&[1; 16]))
            .await
            .unwrap();
        let task = tokio::spawn({
            let r = r.clone();
            async move {
                StaleGc::default().tick_at(&r, cold_now()).await;
            }
        });
        origin.entered.notified().await;
        r.commit_fetched_page(&block(0), PageIndex(1), 32, Bytes::from_static(&[2; 16]))
            .await
            .unwrap();
        origin.release.notify_one();
        task.await.unwrap();
        assert_eq!(r.logical_resident_bytes(), 32);
        assert_eq!(r.lru.total_bytes(), r.resident_bytes());
    }

    #[tokio::test]
    async fn stale_gc_cancelled_caller_still_finishes_whole_block_mutation_with_disk_budget() {
        let root = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let mut r = runtime(root.path(), true, origin);
        let config = talon_core::WorkerConfig {
            background_io_concurrency: 1,
            background_delete_max_per_sec: 0,
            ..Default::default()
        };
        let budget = crate::background::BackgroundBudget::new(&config);
        r.background_budget = Some(budget.clone());
        fill(&r, &block(0), true).await;
        let slot = budget.io().await;
        let r = Arc::new(r);
        let task = tokio::spawn({
            let r = r.clone();
            async move {
                StaleGc::default().tick_at(&r, cold_now()).await;
            }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while r.page_mutations.active_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            r.logical_resident_bytes(),
            32,
            "shared I/O budget holds back the entire block"
        );
        drop(slot);
        r.drain_page_mutations().await;
        assert_eq!(r.logical_resident_bytes(), 0);
        assert_eq!(r.lru.total_bytes(), 0);
        assert!(!r.paged.as_ref().unwrap().dir_for(&block(0)).exists());
    }
}
