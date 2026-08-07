// SPDX-License-Identifier: Apache-2.0
//! Extent-cache benchmarks, and the over-fetch measurement behind ADR 0005.
//!
//! Two different things live here, because the claim needs both.
//!
//! **Latency benches** (Divan, recorded by `just bench`) guard the serve path
//! against regression: a cold miss, an L1 hit, an L2 `sendfile`, and the
//! interning that every read pays. These are a floor, not the argument.
//!
//! **The over-fetch measurement** (printed by `main` before Divan runs) is the
//! argument. It replays one Parquet-shaped read trace at two granularities over
//! the same recording origin and prints what each cost in origin bytes. Latency
//! alone would understate the point: this worker's win is bandwidth, and the
//! bench harness only records nanoseconds.
//!
//! # What the block-granularity side is, and is not
//!
//! It is the *definition* of block granularity — any byte touched costs the
//! whole aligned block — implemented over the identical cache, so the only
//! variable between the two runs is the fetch unit. Everything else is held
//! constant: same origin, same tier configuration, same trace.
//!
//! It is **not** a model of `talon-worker`. That worker's disk layout, index,
//! and WAL are not reproduced here, and its latency is not what this measures.
//! A head-to-head on wall-clock latency would need both binaries and a real
//! object store; this measures the one quantity the two designs actually
//! disagree about.
//!
//! # Scale
//!
//! [`BLOCK_BYTES`] is 4 MiB here, not Talon's 256 MiB, so an iteration moves
//! megabytes instead of gigabytes. The printed ratio is therefore a **lower
//! bound**. Doubling the block size doubles the block side's cost and leaves
//! the extent side untouched, up to the point where one block covers the whole
//! object — so the gap on a real 256 MiB block against a real multi-hundred-MB
//! Parquet file is far wider than what this prints, but by an amount that
//! depends on the file size rather than by a fixed multiplier.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use talon_async_worker::cache::tiered::{ExtentCacheConfig, TieredExtentCache};
use talon_async_worker::{AsyncWorkerRuntime, ServeOutcome};
use talon_core::{Backend, BackendStore, ObjectId, ObjectStat, Result, Version};
use talon_transport::data::RangeRequest;

/// The granularity a block cache would use, scaled down so a bench iteration
/// stays in the millisecond range. See the module docs on scale.
const BLOCK_BYTES: u64 = 4 << 20;

/// Object size: four blocks, so a footer read and a head-of-file read land in
/// different blocks and the block side pays for both.
const OBJECT_LEN: u64 = 4 * BLOCK_BYTES;

/// A Parquet footer read.
const FOOTER_LEN: u64 = 4 << 10;

/// A column chunk read.
const CHUNK_LEN: u64 = 64 << 10;

/// Objects in the replayed trace.
const OBJECTS: u64 = 16;

fn main() {
    // Printed before Divan takes over, so it appears even when a filter selects
    // no benchmarks.
    report_over_fetch();
    divan::main();
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Counts every byte the origin is asked for. This is the measured quantity.
struct CountingOrigin {
    bytes: AtomicU64,
    fetches: AtomicU64,
}

impl CountingOrigin {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            bytes: AtomicU64::new(0),
            fetches: AtomicU64::new(0),
        })
    }
}

#[async_trait]
impl BackendStore for CountingOrigin {
    async fn fetch_range(&self, _object: &ObjectId, offset: u64, len: u64) -> Result<bytes::Bytes> {
        self.bytes.fetch_add(len, Ordering::Relaxed);
        self.fetches.fetch_add(1, Ordering::Relaxed);
        Ok(bytes::Bytes::from(
            (0..len)
                .map(|i| ((offset + i) % 251) as u8)
                .collect::<Vec<u8>>(),
        ))
    }

    async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
        Ok(ObjectStat {
            len: OBJECT_LEN,
            version: Version::new("v1"),
        })
    }
}

fn object(n: u64) -> ObjectId {
    ObjectId::new(
        Backend::S3,
        "warehouse",
        format!("sales/part-{n:05}.parquet"),
    )
}

fn read(object: ObjectId, offset: u64, len: u64) -> RangeRequest {
    RangeRequest {
        object,
        offset,
        len,
    }
}

/// The read trace a columnar engine produces for one file: the footer, then the
/// footer metadata, then column chunks scattered through the body.
fn trace(object: ObjectId) -> Vec<RangeRequest> {
    let mut reads = vec![
        read(object.clone(), OBJECT_LEN - 8, 8),
        read(object.clone(), OBJECT_LEN - FOOTER_LEN, FOOTER_LEN),
    ];
    for i in 0..6u64 {
        reads.push(read(object.clone(), i * (OBJECT_LEN / 8), CHUNK_LEN));
    }
    reads
}

async fn runtime(memory_bytes: u64) -> (Arc<AsyncWorkerRuntime>, Arc<CountingOrigin>) {
    let origin = CountingOrigin::new();
    let cache = TieredExtentCache::new(&ExtentCacheConfig {
        memory_bytes,
        memory_shards: 8,
        ..Default::default()
    })
    .await
    .unwrap();
    let backend: Arc<dyn BackendStore> = origin.clone();
    (
        Arc::new(AsyncWorkerRuntime::new(cache, backend).with_configured_backend(Backend::S3)),
        origin,
    )
}

/// Serve `request` as a block cache would: round out to the enclosing aligned
/// block, fetch and cache *that*, then hand back the requested slice.
///
/// This is the whole of what "block granularity" means as a fetch policy. It
/// runs through the same `serve` entry point, so caching, coalescing, and
/// version resolution are identical to the extent side.
async fn serve_block_granularity(
    worker: &AsyncWorkerRuntime,
    request: &RangeRequest,
) -> ServeOutcome {
    let aligned = (request.offset / BLOCK_BYTES) * BLOCK_BYTES;
    let len = BLOCK_BYTES.min(OBJECT_LEN - aligned);
    worker
        .serve(&read(request.object.clone(), aligned, len))
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------
// The over-fetch measurement
// ---------------------------------------------------------------------------

fn report_over_fetch() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (extent_bytes, extent_fetches, served) = rt.block_on(async {
        let (worker, origin) = runtime(64 << 20).await;
        let mut served = 0u64;
        for n in 0..OBJECTS {
            for request in trace(object(n)) {
                served += worker.serve(&request).await.unwrap().len();
            }
        }
        (
            origin.bytes.load(Ordering::Relaxed),
            origin.fetches.load(Ordering::Relaxed),
            served,
        )
    });

    let (block_bytes, block_fetches) = rt.block_on(async {
        let (worker, origin) = runtime(64 << 20).await;
        for n in 0..OBJECTS {
            for request in trace(object(n)) {
                serve_block_granularity(&worker, &request).await;
            }
        }
        (
            origin.bytes.load(Ordering::Relaxed),
            origin.fetches.load(Ordering::Relaxed),
        )
    });

    let mib = |b: u64| b as f64 / (1024.0 * 1024.0);
    println!("\nOver-fetch, {OBJECTS} Parquet-shaped files, 8 reads each");
    println!("  block size for the comparison: {} MiB", BLOCK_BYTES >> 20);
    println!(
        "  bytes actually needed by the reads: {:.2} MiB",
        mib(served)
    );
    println!(
        "  extent granularity: {:.2} MiB in {extent_fetches} fetches",
        mib(extent_bytes)
    );
    println!(
        "  block  granularity: {:.2} MiB in {block_fetches} fetches",
        mib(block_bytes)
    );
    println!(
        "  origin bytes saved: {:.1}x fewer ({:.2} MiB avoided)",
        block_bytes as f64 / extent_bytes.max(1) as f64,
        mib(block_bytes.saturating_sub(extent_bytes)),
    );
    // Deliberately not extrapolated to a number. The block side's cost grows
    // with the block size only until one block covers the object, so a fixed
    // multiplier would overstate it for small files and understate it for
    // large ones.
    println!(
        "  scaling: the block side grows with the block size (Talon's is {} MiB, \
         {}x this) until one block covers the object; the extent side does not\n",
        256,
        (256 << 20) / BLOCK_BYTES,
    );
}

// ---------------------------------------------------------------------------
// Latency benches
// ---------------------------------------------------------------------------

/// One selective read against a cold cache: HEAD, ranged GET, admit, return.
///
/// The origin here is in-process, so this is the worker's own cost of a miss
/// with network time removed — a regression floor for the serve path, not an
/// estimate of what a miss costs in production.
#[divan::bench]
fn cold_miss(bencher: divan::Bencher) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let counter = AtomicU64::new(0);
    bencher.bench_local(|| {
        rt.block_on(async {
            // A distinct object each iteration, so every read is a true miss
            // rather than the first one plus N hits.
            let n = counter.fetch_add(1, Ordering::Relaxed);
            let (worker, _) = runtime(16 << 20).await;
            worker
                .serve(&read(object(n), 0, FOOTER_LEN))
                .await
                .unwrap()
                .len()
        })
    });
}

/// A warm DRAM hit — the common case once a file is being read.
#[divan::bench]
fn l1_hit(bencher: divan::Bencher) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let worker = rt.block_on(async {
        let (worker, _) = runtime(64 << 20).await;
        worker.serve(&read(object(0), 0, FOOTER_LEN)).await.unwrap();
        worker
    });
    bencher.bench_local(|| {
        rt.block_on(async {
            worker
                .serve(&read(object(0), 0, FOOTER_LEN))
                .await
                .unwrap()
                .len()
        })
    });
}

/// A warm hit on a larger extent, so the win is not measuring 4 KiB of memcpy.
#[divan::bench]
fn l1_hit_column_chunk(bencher: divan::Bencher) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let worker = rt.block_on(async {
        let (worker, _) = runtime(64 << 20).await;
        worker.serve(&read(object(0), 0, CHUNK_LEN)).await.unwrap();
        worker
    });
    bencher.bench_local(|| {
        rt.block_on(async {
            worker
                .serve(&read(object(0), 0, CHUNK_LEN))
                .await
                .unwrap()
                .len()
        })
    });
}

/// Interning an [`ObjectId`] to a `stream_id`. Every read pays it, and it is on
/// the path before any cache lookup, so a regression here is a regression
/// everywhere.
#[divan::bench]
fn intern_stream_id(bencher: divan::Bencher) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (cache, object) = rt.block_on(async {
        let cache = TieredExtentCache::new(&ExtentCacheConfig {
            memory_bytes: 1 << 20,
            memory_shards: 8,
            ..Default::default()
        })
        .await
        .unwrap();
        let object = object(0);
        cache.intern(&object);
        (cache, object)
    });
    bencher.bench_local(|| cache.intern(divan::black_box(&object)));
}

/// The full per-file trace on a cold cache, at each granularity.
///
/// The pair is the point: same trace, same cache, same origin, one variable.
/// The block side is slower here because it moves more bytes — which is the
/// bandwidth cost showing up as latency.
#[divan::bench(args = ["extent", "block"])]
fn file_trace_cold(bencher: divan::Bencher, granularity: &str) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let counter = AtomicU64::new(0);
    let block = granularity == "block";
    bencher.bench_local(|| {
        rt.block_on(async {
            let n = counter.fetch_add(1, Ordering::Relaxed);
            let (worker, _) = runtime(64 << 20).await;
            for request in trace(object(n)) {
                if block {
                    serve_block_granularity(&worker, &request).await;
                } else {
                    worker.serve(&request).await.unwrap();
                }
            }
        })
    });
}
