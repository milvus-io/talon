//! Instrumented worker cache request runtime.

use std::sync::Arc;
use std::time::Instant;

use talon_core::{
    BackendStore, BlockForm, BlockId, BlockMeta, ObjectId, ObjectStore, PageIndex, Version,
};
use talon_transport::data::RangeRequest;

use crate::{BlockIndex, InFlightLoads, LoadKey, Presence, WholeBlockStore, WorkerMetrics};

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

    /// Serve `[offset, offset + len)` using cache hit or backend miss paths.
    pub async fn serve_range(&self, request: &RangeRequest) -> anyhow::Result<bytes::Bytes> {
        let block = self.block_for(&request.object, request.offset);
        let offset_in_block = request.offset - block.offset;

        if matches!(
            self.index.presence(&block, PageIndex(0), PageIndex(1)),
            Presence::Whole
        ) {
            self.metrics.record_cache_hit();
            tracing::info!(block = %block, "HIT");
            let bytes = self
                .store
                .get_bytes(&block)
                .await
                .map_err(|error| anyhow::anyhow!("read committed block: {error}"))?;
            return slice(&bytes, offset_in_block, request.len);
        }

        self.metrics.record_cache_miss();
        tracing::info!(block = %block, "MISS -> backend fetch");
        let key = LoadKey::Whole(block.clone());
        let _ = self.inflight.admit(key.clone());

        let started = Instant::now();
        let fetched = self
            .backend
            .fetch_range(&request.object, block.offset, self.block_size as u64)
            .await;
        self.inflight.complete(&key);
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
            .put(&block, bytes.clone())
            .await
            .map_err(|error| anyhow::anyhow!("commit block failed: {error}"))?;
        self.index.commit(BlockMeta {
            id: block.clone(),
            form: BlockForm::Whole,
            len: bytes.len() as u64,
        });
        tracing::info!(block = %block, bytes = bytes.len(), "committed block");

        slice(&bytes, offset_in_block, request.len)
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
