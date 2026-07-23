//! Instrumented worker cache request runtime.

use std::sync::Arc;
use std::time::Instant;

use talon_core::{
    BackendStore, BlockForm, BlockId, BlockMeta, ObjectId, ObjectStore, PageIndex, Version,
};
use talon_transport::data::RangeRequest;

use crate::{
    Admission, BlockIndex, InFlightLoads, LoadKey, Presence, WholeBlockStore, WorkerMetrics,
};

/// Placeholder source version used until coordinator metadata includes etags.
const PLACEHOLDER_VERSION: &str = "e2e-v1";

/// Shared state required to serve instrumented data-plane range requests.
pub struct WorkerRuntime {
    store: WholeBlockStore,
    index: Arc<BlockIndex>,
    inflight: Arc<InFlightLoads>,
    backend: Arc<dyn BackendStore>,
    block_size: u32,
    metrics: WorkerMetrics,
}

impl WorkerRuntime {
    /// Create a request runtime over an initialized cache and backend.
    pub fn new(
        store: WholeBlockStore,
        index: Arc<BlockIndex>,
        inflight: Arc<InFlightLoads>,
        backend: Arc<dyn BackendStore>,
        block_size: u32,
        metrics: WorkerMetrics,
    ) -> Self {
        Self {
            store,
            index,
            inflight,
            backend,
            block_size,
            metrics,
        }
    }

    /// The block-aligned [`BlockId`] containing `offset` of `object`.
    fn block_for(&self, object: &ObjectId, offset: u64) -> BlockId {
        let block_size = self.block_size as u64;
        let block_start = (offset / block_size) * block_size;
        BlockId::new(
            object.clone(),
            block_start,
            self.block_size,
            Version::new(PLACEHOLDER_VERSION),
        )
    }

    /// Serve `[offset, offset + len)`, spanning block boundaries as needed.
    ///
    /// A request whose range crosses one or more block boundaries is split into
    /// per-block reads (each a cache hit or a backend miss) and the pieces are
    /// stitched into one contiguous buffer. Previously only the block containing
    /// the *start* offset was read and the result clamped to that block's end,
    /// silently truncating cross-block reads (issue #112).
    pub async fn serve_range(&self, request: &RangeRequest) -> anyhow::Result<bytes::Bytes> {
        if request.len == 0 {
            return Ok(bytes::Bytes::new());
        }
        let block_size = self.block_size as u64;
        let end = request
            .offset
            .checked_add(request.len)
            .ok_or_else(|| anyhow::anyhow!("range offset+len overflows u64"))?;

        // Fast path: the whole range lies within a single block. Keeps the
        // common case allocation-free (returns the per-block slice directly).
        let start_block = (request.offset / block_size) * block_size;
        if end <= start_block + block_size {
            let block = self.block_for(&request.object, request.offset);
            let offset_in_block = request.offset - block.offset;
            let bytes = self.block_bytes(request, &block).await?;
            return slice(&bytes, offset_in_block, request.len);
        }

        // Slow path: stitch across blocks.
        let mut out = bytes::BytesMut::with_capacity(request.len as usize);
        let mut cursor = request.offset;
        while cursor < end {
            let block = self.block_for(&request.object, cursor);
            let offset_in_block = cursor - block.offset;
            let block_end = block.offset + block_size;
            let take = block_end.min(end) - cursor;
            let bytes = self.block_bytes(request, &block).await?;
            let piece = slice(&bytes, offset_in_block, take)?;
            // A block that returned fewer bytes than its share means the object
            // ends inside it; stop rather than silently returning a short read.
            let short = piece.len() < take as usize;
            out.extend_from_slice(&piece);
            if short {
                break;
            }
            cursor += take;
        }
        Ok(out.freeze())
    }

    /// Return the full committed/fetched bytes of a single block, using the
    /// cache-hit path when resident and the backend-miss path otherwise.
    ///
    /// Concurrent misses for the same block are deduplicated: the first caller
    /// (`Admission::Started`) performs the backend fetch; the rest wait for it
    /// and then serve from the now-warm cache, so N concurrent misses trigger a
    /// single backend fetch instead of N (issue #113).
    async fn block_bytes(
        &self,
        request: &RangeRequest,
        block: &BlockId,
    ) -> anyhow::Result<bytes::Bytes> {
        if let Some(bytes) = self.cached_block(block).await? {
            return Ok(bytes);
        }

        self.metrics.record_cache_miss();
        let key = LoadKey::Whole(block.clone());
        match self.inflight.admit(key.clone()) {
            Admission::AlreadyLoading => {
                // A peer is already fetching this block; wait for it and serve
                // from cache rather than issuing a duplicate backend fetch.
                self.inflight.wait(&key).await;
                if let Some(bytes) = self.cached_block(block).await? {
                    return Ok(bytes);
                }
                // The leader's load failed (key cleared, block still absent).
                // Fall through and fetch ourselves, admitting a fresh load.
                if self.inflight.admit(key.clone()) == Admission::AlreadyLoading {
                    // Another peer already restarted; wait once more, then, if
                    // still absent, fetch without holding admission to avoid an
                    // unbounded wait loop.
                    self.inflight.wait(&key).await;
                    if let Some(bytes) = self.cached_block(block).await? {
                        return Ok(bytes);
                    }
                    return self.fetch_and_commit(request, block).await;
                }
                let result = self.fetch_and_commit(request, block).await;
                self.inflight.complete(&key);
                result
            }
            Admission::Started => {
                let result = self.fetch_and_commit(request, block).await;
                self.inflight.complete(&key);
                result
            }
        }
    }

    /// Return a block's bytes from the local cache if resident, else `None`.
    async fn cached_block(&self, block: &BlockId) -> anyhow::Result<Option<bytes::Bytes>> {
        if matches!(
            self.index.presence(block, PageIndex(0), PageIndex(1)),
            Presence::Whole
        ) {
            self.metrics.record_cache_hit();
            tracing::info!(block = %block, "HIT");
            let bytes = self
                .store
                .get_bytes(block)
                .await
                .map_err(|error| anyhow::anyhow!("read committed block: {error}"))?;
            Ok(Some(bytes))
        } else {
            Ok(None)
        }
    }

    /// Fetch a block from the backend and commit it to the local cache.
    async fn fetch_and_commit(
        &self,
        request: &RangeRequest,
        block: &BlockId,
    ) -> anyhow::Result<bytes::Bytes> {
        tracing::info!(block = %block, "MISS -> backend fetch");
        let started = Instant::now();
        let fetched = self
            .backend
            .fetch_range(&request.object, block.offset, self.block_size as u64)
            .await;
        let bytes = match fetched {
            Ok(bytes) => {
                self.metrics
                    .record_backend_fetch_success(bytes.len() as u64, started.elapsed());
                bytes
            }
            Err(error) => {
                self.metrics.record_backend_fetch_error(started.elapsed());
                return Err(error.into());
            }
        };

        self.store
            .put(block, bytes.clone())
            .await
            .map_err(|error| anyhow::anyhow!("commit block failed: {error}"))?;
        self.index.commit(BlockMeta {
            id: block.clone(),
            form: BlockForm::Whole,
            len: bytes.len() as u64,
        });
        tracing::info!(block = %block, bytes = bytes.len(), "committed block");
        Ok(bytes)
    }

    /// Number of blocks currently indexed.
    pub fn block_count(&self) -> u64 {
        self.index.len() as u64
    }

    /// Number of backend loads currently in flight.
    pub fn inflight_loads(&self) -> u64 {
        self.inflight.len() as u64
    }
}

fn slice(buffer: &[u8], offset: u64, len: u64) -> anyhow::Result<bytes::Bytes> {
    let start = usize::try_from(offset).map_err(|_| anyhow::anyhow!("offset is too large"))?;
    if start > buffer.len() {
        anyhow::bail!("offset {offset} beyond block length {} bytes", buffer.len());
    }
    let requested = usize::try_from(len).unwrap_or(usize::MAX);
    let end = start.saturating_add(requested).min(buffer.len());
    Ok(bytes::Bytes::copy_from_slice(&buffer[start..end]))
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::SystemTime;

    use async_trait::async_trait;
    use bytes::Bytes;
    use talon_core::{Backend, Error, ObjectStat, Result};

    use super::*;

    struct MockBackend {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl BackendStore for MockBackend {
        async fn fetch_range(&self, object: &ObjectId, _offset: u64, _len: u64) -> Result<Bytes> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if object.object_path == "failure" {
                Err(Error::Backend("simulated failure".into()))
            } else {
                Ok(Bytes::from_static(b"abcdefgh"))
            }
        }

        async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
            Ok(ObjectStat {
                len: 8,
                version: Version::new("v1"),
            })
        }
    }

    fn request(path: &str) -> RangeRequest {
        RangeRequest {
            object: ObjectId::new(Backend::Azure, "container", path),
            offset: 0,
            len: 4,
        }
    }

    fn runtime(backend: Arc<MockBackend>, metrics: WorkerMetrics, root: &PathBuf) -> WorkerRuntime {
        WorkerRuntime::new(
            WholeBlockStore::open(root).unwrap(),
            Arc::new(BlockIndex::new()),
            Arc::new(InFlightLoads::new()),
            backend,
            8,
            metrics,
        )
    }

    #[tokio::test]
    async fn miss_then_hit_records_cache_and_backend_metrics() {
        let root = tmp_root();
        let backend = Arc::new(MockBackend {
            calls: AtomicUsize::new(0),
        });
        let metrics = WorkerMetrics::new(1024);
        let runtime = runtime(Arc::clone(&backend), metrics.clone(), &root);

        assert_eq!(
            runtime.serve_range(&request("ok")).await.unwrap(),
            Bytes::from_static(b"abcd")
        );
        assert_eq!(
            runtime.serve_range(&request("ok")).await.unwrap(),
            Bytes::from_static(b"abcd")
        );
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.block_count(), 1);

        let rendered = metrics.render();
        assert!(rendered.contains("talon_worker_cache_misses_total{form=\"whole\"} 1"));
        assert!(rendered.contains("talon_worker_cache_hits_total{form=\"whole\"} 1"));
        assert!(rendered.contains("talon_worker_backend_fetch_bytes_total{backend=\"azure\"} 8"));
        std::fs::remove_dir_all(root).ok();
    }

    /// A backend whose block content is deterministic per absolute offset, so a
    /// stitched multi-block read can be verified byte-for-byte.
    struct RampBackend {
        block_size: u64,
    }

    #[async_trait]
    impl BackendStore for RampBackend {
        async fn fetch_range(&self, _object: &ObjectId, offset: u64, len: u64) -> Result<Bytes> {
            // Return one block worth of bytes starting at `offset`; byte i has
            // value (offset + i) % 251 (prime, so no accidental alignment).
            let n = len.min(self.block_size) as usize;
            let buf: Vec<u8> = (0..n).map(|i| ((offset + i as u64) % 251) as u8).collect();
            Ok(Bytes::from(buf))
        }

        async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
            Ok(ObjectStat {
                len: u64::MAX,
                version: Version::new("v1"),
            })
        }
    }

    fn expected(offset: u64, len: u64) -> Bytes {
        Bytes::from(
            (0..len)
                .map(|i| ((offset + i) % 251) as u8)
                .collect::<Vec<u8>>(),
        )
    }

    fn runtime_with<B: BackendStore + 'static>(
        backend: Arc<B>,
        metrics: WorkerMetrics,
        root: &PathBuf,
        block_size: u32,
    ) -> WorkerRuntime {
        WorkerRuntime::new(
            WholeBlockStore::open(root).unwrap(),
            Arc::new(BlockIndex::new()),
            Arc::new(InFlightLoads::new()),
            backend,
            block_size,
            metrics,
        )
    }

    #[tokio::test]
    async fn cross_block_read_stitches_multiple_blocks() {
        // block_size 8; read [6, 14) spans blocks [0,8) and [8,16), so it must
        // stitch two per-block fetches into one 8-byte contiguous result.
        let root = tmp_root();
        let runtime = runtime_with(
            Arc::new(RampBackend { block_size: 8 }),
            WorkerMetrics::new(1024),
            &root,
            8,
        );
        let req = RangeRequest {
            object: ObjectId::new(Backend::Azure, "container", "ramp"),
            offset: 6,
            len: 8,
        };
        let got = runtime.serve_range(&req).await.unwrap();
        assert_eq!(got.len(), 8);
        assert_eq!(got, expected(6, 8));
        assert_eq!(runtime.block_count(), 2);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn read_spanning_three_blocks_is_contiguous() {
        let root = tmp_root();
        let runtime = runtime_with(
            Arc::new(RampBackend { block_size: 8 }),
            WorkerMetrics::new(1024),
            &root,
            8,
        );
        // [4, 22): tail of block0, all of block1, head of block2.
        let req = RangeRequest {
            object: ObjectId::new(Backend::Azure, "container", "ramp"),
            offset: 4,
            len: 18,
        };
        let got = runtime.serve_range(&req).await.unwrap();
        assert_eq!(got, expected(4, 18));
        assert_eq!(runtime.block_count(), 3);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn single_block_read_still_works() {
        let root = tmp_root();
        let runtime = runtime_with(
            Arc::new(RampBackend { block_size: 8 }),
            WorkerMetrics::new(1024),
            &root,
            8,
        );
        let req = RangeRequest {
            object: ObjectId::new(Backend::Azure, "container", "ramp"),
            offset: 2,
            len: 4,
        };
        let got = runtime.serve_range(&req).await.unwrap();
        assert_eq!(got, expected(2, 4));
        assert_eq!(runtime.block_count(), 1);
        std::fs::remove_dir_all(root).ok();
    }

    /// A backend that counts calls and is slow enough that concurrent misses
    /// overlap, so the dedup path is actually exercised.
    struct SlowCountingBackend {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl BackendStore for SlowCountingBackend {
        async fn fetch_range(&self, _object: &ObjectId, _offset: u64, _len: u64) -> Result<Bytes> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok(Bytes::from_static(b"abcdefgh"))
        }

        async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
            Ok(ObjectStat {
                len: 8,
                version: Version::new("v1"),
            })
        }
    }

    #[tokio::test]
    async fn concurrent_misses_trigger_a_single_backend_fetch() {
        // Many simultaneous misses for the same block must dedup to one backend
        // fetch; the followers wait for the leader and serve from cache (#113).
        let root = tmp_root();
        let calls = Arc::new(AtomicUsize::new(0));
        let backend = Arc::new(SlowCountingBackend {
            calls: Arc::clone(&calls),
        });
        let runtime = Arc::new(runtime_with(backend, WorkerMetrics::new(1024), &root, 8));

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let runtime = Arc::clone(&runtime);
            tasks.push(tokio::spawn(async move {
                runtime.serve_range(&request("ok")).await.unwrap()
            }));
        }
        for t in tasks {
            assert_eq!(t.await.unwrap(), Bytes::from_static(b"abcd"));
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "concurrent misses must dedup to one backend fetch"
        );
        assert_eq!(runtime.block_count(), 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn backend_error_is_counted_and_clears_inflight_state() {
        let root = tmp_root();
        let backend = Arc::new(MockBackend {
            calls: AtomicUsize::new(0),
        });
        let metrics = WorkerMetrics::new(1024);
        let runtime = runtime(backend, metrics.clone(), &root);

        assert!(runtime.serve_range(&request("failure")).await.is_err());
        assert_eq!(runtime.inflight_loads(), 0);
        assert!(metrics
            .render()
            .contains("talon_worker_backend_fetch_errors_total{backend=\"azure\"} 1"));
        std::fs::remove_dir_all(root).ok();
    }

    fn tmp_root() -> PathBuf {
        let mut hasher = DefaultHasher::new();
        SystemTime::now().hash(&mut hasher);
        std::thread::current().id().hash(&mut hasher);
        std::env::temp_dir().join(format!(
            "talon-runtime-{}-{}",
            std::process::id(),
            hasher.finish()
        ))
    }
}
