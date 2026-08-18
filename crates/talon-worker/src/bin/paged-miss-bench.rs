//! Local latency probe for the paged miss-run concurrency limit.
//!
//! It warms the middle page of a three-page read, leaving two one-page misses
//! separated by the resident page. The samples directly compare a worker limit
//! of one run with a limit of two runs; setup and warming are outside the
//! recorded interval.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use talon_core::{Backend, BackendStore, ObjectId, ObjectStat, Result, Version};
use talon_transport::data::RangeRequest;
use talon_worker::{
    BlockIndex, InFlightLoads, PagedBlockStore, WholeBlockStore, WorkerMetrics, WorkerRuntime,
};

const PAGE_SIZE: u32 = 64 << 10;
const OBJECT_LEN: u64 = 3 * PAGE_SIZE as u64;
const BACKEND_DELAY: Duration = Duration::from_millis(20);
const SAMPLES: usize = 21;
static ROOT_SEQ: AtomicUsize = AtomicUsize::new(0);

struct LatencyBackend;

#[async_trait]
impl BackendStore for LatencyBackend {
    async fn fetch_range(&self, _object: &ObjectId, offset: u64, len: u64) -> Result<Bytes> {
        tokio::time::sleep(BACKEND_DELAY).await;
        Ok(Bytes::from(
            (0..len)
                .map(|i| ((offset + i) % 251) as u8)
                .collect::<Vec<_>>(),
        ))
    }

    async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
        Ok(ObjectStat {
            len: OBJECT_LEN,
            version: Version::new("v1"),
        })
    }
}

fn object() -> ObjectId {
    ObjectId::new(Backend::Azure, "bench", "paged-miss-runs")
}

fn tmp_root(limit: usize) -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after Unix epoch")
        .as_nanos();
    let seq = ROOT_SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "talon-paged-miss-bench-limit-{limit}-{}-{now}-{seq}",
        std::process::id()
    ))
}

async fn sample(limit: usize) -> Duration {
    let root = tmp_root(limit);
    let runtime = WorkerRuntime::new(
        WholeBlockStore::open(root.join("whole")).expect("open whole store"),
        Arc::new(BlockIndex::new()),
        Arc::new(InFlightLoads::new()),
        Arc::new(LatencyBackend),
        4 * PAGE_SIZE,
        0,
        WorkerMetrics::new(1 << 30),
    )
    .with_paged_store(
        PagedBlockStore::open(root.join("paged"), PAGE_SIZE).expect("open paged store"),
    )
    .with_paged_miss_run_concurrency(limit);

    // Keep page 1 resident: pages 0 and 2 then form two leader runs for the
    // measured three-page request.
    runtime
        .serve_range(&RangeRequest {
            object: object(),
            offset: u64::from(PAGE_SIZE),
            len: 1,
        })
        .await
        .expect("warm middle page");

    let started = Instant::now();
    let bytes = runtime
        .serve_range(&RangeRequest {
            object: object(),
            offset: 0,
            len: OBJECT_LEN,
        })
        .await
        .expect("read split miss runs");
    let elapsed = started.elapsed();
    assert_eq!(bytes.len(), OBJECT_LEN as usize);
    drop(runtime);
    std::fs::remove_dir_all(root).ok();
    elapsed
}

fn percentile(samples: &mut [Duration], numerator: usize, denominator: usize) -> Duration {
    samples.sort_unstable();
    samples[(samples.len() - 1) * numerator / denominator]
}

fn describe(label: &str, samples: &[Duration]) -> (Duration, Duration) {
    let mut median = samples.to_vec();
    let mut p95 = samples.to_vec();
    let median = percentile(&mut median, 50, 100);
    let p95 = percentile(&mut p95, 95, 100);
    println!("{label:8} p50={median:?} p95={p95:?}");
    (median, p95)
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build Tokio runtime");
    let limit_one: Vec<_> = (0..SAMPLES).map(|_| runtime.block_on(sample(1))).collect();
    let limit_two: Vec<_> = (0..SAMPLES).map(|_| runtime.block_on(sample(2))).collect();
    let (limit_one_p50, _) = describe("limit=1", &limit_one);
    let (limit_two_p50, _) = describe("limit=2", &limit_two);
    let improvement = (1.0 - limit_two_p50.as_secs_f64() / limit_one_p50.as_secs_f64()) * 100.0;
    println!(
        "limit=2 p50 latency reduction: {improvement:.1}% (backend delay {:?}, {SAMPLES} samples)",
        BACKEND_DELAY
    );
}
