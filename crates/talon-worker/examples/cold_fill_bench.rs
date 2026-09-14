//! Local cold-read/fill probe. Every read touches a new page and commits it to
//! disk through WorkerRuntime, including write, fsync, rename, and eviction.
//! The in-memory origin has no network latency; this is not a remote S3 or SDK
//! throughput benchmark. Run with --help for the workload parameters.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::ensure;
use bytes::Bytes;
use clap::Parser;
use futures::{stream, StreamExt};
use talon_core::{Backend, BackendStore, ObjectId, ObjectStat, Result, Version};
use talon_transport::data::RangeRequest;
use talon_worker::{
    BlockIndex, InFlightLoads, PagedBlockStore, WholeBlockStore, WorkerMetrics, WorkerRuntime,
};

const BLOCK_BYTES: u32 = 256 << 20;

#[derive(Parser)]
struct Args {
    /// Parent directory on the filesystem being measured. Only a fresh temporary
    /// subdirectory is used, and it is removed before successful completion.
    #[arg(long)]
    cache_root: PathBuf,
    #[arg(long, default_value_t = 100000)]
    pages: u32,
    #[arg(long, default_value_t = 10000)]
    interval_pages: u32,
    #[arg(long, default_value_t = 65536)]
    page_bytes: u32,
    #[arg(long, default_value_t = 4096)]
    read_bytes: u32,
    #[arg(long, default_value_t = 1)]
    concurrency: usize,
    #[arg(long, default_value_t = 8)]
    threads: usize,
}

struct Origin {
    page_bytes: u32,
    len: u64,
    fetches: AtomicU64,
}

#[async_trait::async_trait]
impl BackendStore for Origin {
    async fn fetch_range(&self, _: &ObjectId, offset: u64, len: u64) -> Result<Bytes> {
        assert_eq!(len, u64::from(self.page_bytes));
        assert_eq!(offset % u64::from(self.page_bytes), 0);
        assert!(offset + len <= self.len);
        self.fetches.fetch_add(1, Ordering::Relaxed);
        let value = ((offset / u64::from(self.page_bytes)) % 251) as u8;
        Ok(Bytes::from(vec![value; len as usize]))
    }

    async fn head(&self, _: &ObjectId) -> Result<ObjectStat> {
        Ok(ObjectStat {
            len: self.len,
            version: Version::new("v1"),
        })
    }
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    ensure!(args.pages > 0 && args.interval_pages > 0);
    ensure!(args.page_bytes > 0 && BLOCK_BYTES % args.page_bytes == 0);
    ensure!(args.read_bytes > 0 && args.read_bytes <= args.page_bytes);
    ensure!(args.concurrency > 0 && args.threads > 0);
    std::fs::create_dir_all(&args.cache_root)?;
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads)
        .enable_all()
        .build()?
        .block_on(run(&args))
}

async fn run(args: &Args) -> anyhow::Result<()> {
    let root = tempfile::Builder::new()
        .prefix("cold-fill-")
        .tempdir_in(&args.cache_root)?;
    let paged_root = root.path().join("paged");
    let object_bytes = u64::from(args.pages) * u64::from(args.page_bytes);
    let origin = Arc::new(Origin {
        page_bytes: args.page_bytes,
        len: object_bytes,
        fetches: AtomicU64::new(0),
    });
    let index = Arc::new(BlockIndex::new());
    let worker = Arc::new(
        WorkerRuntime::new(
            WholeBlockStore::open(root.path().join("whole"))?,
            Arc::clone(&index),
            Arc::new(InFlightLoads::new()),
            Arc::clone(&origin) as Arc<dyn BackendStore>,
            BLOCK_BYTES,
            object_bytes * 2,
            WorkerMetrics::new(object_bytes * 2),
        )
        .with_paged_store(PagedBlockStore::open(&paged_root, args.page_bytes)?),
    );
    let object = ObjectId::new(Backend::S3, "cold-fill-bench", "object");
    let mut elapsed_seconds = 0.0;
    for first in (0..args.pages).step_by(args.interval_pages as usize) {
        let end = first.saturating_add(args.interval_pages).min(args.pages);
        let start = Instant::now();
        // Separate Tokio tasks exercise the shared LRU from multiple runtime
        // threads. Bound outstanding requests and drain them before validation.
        let results: Vec<_> = stream::iter(first..end)
            .map(|page| {
                let worker = Arc::clone(&worker);
                let request = RangeRequest {
                    object: object.clone(),
                    offset: u64::from(page) * u64::from(args.page_bytes),
                    len: u64::from(args.read_bytes),
                };
                async move {
                    tokio::spawn(async move {
                        let start = Instant::now();
                        let bytes = worker.serve_range(&request).await?;
                        let latency = start.elapsed().as_secs_f64();
                        ensure!(bytes.len() as u64 == request.len, "short response");
                        ensure!(bytes.iter().all(|b| *b == (page % 251) as u8), "wrong data");
                        Ok::<_, anyhow::Error>(latency)
                    })
                    .await
                }
            })
            .buffer_unordered(args.concurrency)
            .collect()
            .await;
        let seconds = start.elapsed().as_secs_f64();
        elapsed_seconds += seconds;
        let mut latencies = Vec::with_capacity(results.len());
        for result in results {
            latencies.push(result??);
        }
        latencies.sort_by(f64::total_cmp);
        let count = end - first;
        ensure!(
            origin.fetches.load(Ordering::Relaxed) == u64::from(end),
            "unexpected hit or duplicate fetch"
        );
        ensure!(index.page_count() == u64::from(end), "missing cached page");
        ensure!(worker.resident_bytes() == u64::from(end) * u64::from(args.page_bytes));
        println!(
            "{}",
            serde_json::json!({
                "kind": "interval", "from_pages": first, "to_pages": end,
                "page_bytes": args.page_bytes, "read_bytes": args.read_bytes,
                "concurrency": args.concurrency, "threads": args.threads,
                "seconds": seconds, "elapsed_seconds": elapsed_seconds,
                "reads_per_second": f64::from(count) / seconds,
                "fill_mib_per_second": f64::from(count) * f64::from(args.page_bytes) / (1_048_576.0 * seconds),
                "p50_ms": latencies[latencies.len() / 2] * 1000.0,
                "p99_ms": latencies[((latencies.len() * 99) / 100).min(latencies.len() - 1)] * 1000.0,
                "backend_fetches": origin.fetches.load(Ordering::Relaxed),
                "resident_pages": index.page_count(), "errors": 0,
            })
        );
    }
    // Validate disk state independently of the live index, outside timing.
    let recovered = BlockIndex::new();
    for meta in PagedBlockStore::open(&paged_root, args.page_bytes)?.scan()? {
        recovered.commit(meta);
    }
    ensure!(recovered.page_count() == u64::from(args.pages));
    ensure!(recovered.resident_bytes() == object_bytes);
    drop(worker);
    root.close()?;
    println!(
        "{}",
        serde_json::json!({
            "kind": "complete", "pages": args.pages, "errors": 0,
            "disk_pages_verified": recovered.page_count(), "cleanup_ok": true,
            "elapsed_seconds": elapsed_seconds,
        })
    );
    Ok(())
}
