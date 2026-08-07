// SPDX-License-Identifier: Apache-2.0
//! The claim this worker exists to make, tested against the real backend stack.
//!
//! > A selective read fetches **exactly** the bytes it asked for.
//!
//! ADR 0005 opens with the cost of not having this: a query engine reads a
//! Parquet footer and then cherry-picks column chunks, and on a 256MB block
//! cache each of those few-kilobyte reads costs a 256MB transfer. The whole
//! design — variable-length extents, no page size, no rounding — is in service
//! of one measurable number, origin bytes fetched.
//!
//! So that is what these assert, and they assert it where it is observable:
//! **on the `Range` header the origin actually received**. A test on
//! `ServeStats::origin_bytes_fetched` alone would pass if the worker fetched a
//! superset and counted only the slice it returned. The recording HTTP client
//! below sits at the real network boundary — `AsyncWorkerRuntime →
//! TieredExtentCache → S3Backend → HttpClient` — so a future prefetch,
//! read-ahead, or block alignment shows up here as a wider range and fails.
//!
//! What is deliberately *not* here: a head-to-head against `talon-worker`.
//! Comparing the two on the same workload is the benchmark's job, where the
//! result is a number rather than an assertion. Simulating the block worker's
//! fetch shape inline would only test that the simulation was written
//! correctly.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use talon_async_worker::cache::tiered::{ExtentCacheConfig, TieredExtentCache};
use talon_async_worker::AsyncWorkerRuntime;
use talon_backend::http::{HttpClient, HttpRequest, HttpResponse, Method};
use talon_backend::{S3Backend, S3Config, S3Credentials};
use talon_core::{Backend, BackendStore, ObjectId};
use talon_transport::data::RangeRequest;

/// A synthetic Parquet file: big enough that a 256MB block would cover it
/// whole, so "fetched only what was asked" is not true by accident.
const OBJECT_LEN: u64 = 512 << 20;

/// A realistic footer read.
const FOOTER_LEN: u64 = 4096;

/// One recorded ranged GET.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Fetch {
    start: u64,
    end: u64,
}

impl Fetch {
    fn len(&self) -> u64 {
        self.end - self.start + 1
    }
}

/// Records every ranged GET the backend issues and answers it faithfully.
struct RecordingOrigin {
    gets: Mutex<Vec<Fetch>>,
    heads: AtomicUsize,
}

impl RecordingOrigin {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            gets: Mutex::new(Vec::new()),
            heads: AtomicUsize::new(0),
        })
    }

    fn fetches(&self) -> Vec<Fetch> {
        self.gets.lock().unwrap().clone()
    }

    /// Total bytes the origin was actually asked for.
    fn bytes_fetched(&self) -> u64 {
        self.fetches().iter().map(Fetch::len).sum()
    }
}

#[async_trait]
impl HttpClient for RecordingOrigin {
    async fn execute(&self, req: HttpRequest) -> Result<HttpResponse, String> {
        match req.method {
            Method::Head => {
                self.heads.fetch_add(1, Ordering::SeqCst);
                Ok(HttpResponse {
                    status: 200,
                    headers: vec![
                        ("Content-Length".into(), OBJECT_LEN.to_string()),
                        ("ETag".into(), "\"v1\"".into()),
                    ],
                    body: bytes::Bytes::new(),
                })
            }
            Method::Get => {
                let raw = req
                    .header("range")
                    .or_else(|| req.header("x-ms-range"))
                    .ok_or_else(|| {
                        // An unranged GET is the worst over-fetch there is: the
                        // whole object. Fail loudly rather than serving it.
                        "the async worker must never issue an unranged GET".to_string()
                    })?;
                let spec = raw
                    .strip_prefix("bytes=")
                    .ok_or("expected a bytes= range")?;
                let (s, e) = spec.split_once('-').ok_or("expected start-end")?;
                let start: u64 = s.parse().map_err(|_| "bad range start")?;
                let end: u64 = e.parse().map_err(|_| "bad range end")?;
                self.gets.lock().unwrap().push(Fetch { start, end });

                let len = (end - start + 1) as usize;
                let body: Vec<u8> = (0..len)
                    .map(|i| ((start as usize + i) % 251) as u8)
                    .collect();
                Ok(HttpResponse {
                    status: 206,
                    headers: vec![
                        (
                            "Content-Range".into(),
                            format!("bytes {start}-{end}/{OBJECT_LEN}"),
                        ),
                        ("ETag".into(), "\"v1\"".into()),
                    ],
                    body: bytes::Bytes::from(body),
                })
            }
            other => Err(format!("unexpected {other:?} on a read-only worker")),
        }
    }
}

/// The production stack, wired the way `main.rs` wires it: recording HTTP
/// client → `S3Backend` → `AsyncWorkerRuntime` over a DRAM-only cache.
///
/// DRAM-only keeps the test deterministic — the NVMe tier admits in the
/// background — and it does not weaken the claim, because admission happens
/// strictly *after* the origin fetch that these assertions measure.
async fn worker() -> (Arc<AsyncWorkerRuntime>, Arc<RecordingOrigin>) {
    let origin = RecordingOrigin::new();
    let http: Arc<dyn HttpClient> = origin.clone();
    let mut config = S3Config::aws("us-east-1");
    config.endpoint = "127.0.0.1:9000".into();
    config.tls = false;
    config.path_style = true;
    let backend: Arc<dyn BackendStore> = Arc::new(S3Backend::new(
        config,
        S3Credentials {
            access_key_id: "test".into(),
            secret_access_key: "test".into(),
            session_token: None,
        },
        http,
    ));

    let cache = TieredExtentCache::new(&ExtentCacheConfig {
        memory_bytes: 64 << 20,
        memory_shards: 4,
        ..Default::default()
    })
    .await
    .unwrap();

    let runtime =
        Arc::new(AsyncWorkerRuntime::new(cache, backend).with_configured_backend(Backend::S3));
    (runtime, origin)
}

fn parquet() -> ObjectId {
    ObjectId::new(Backend::S3, "warehouse", "sales/part-00000.parquet")
}

fn read(offset: u64, len: u64) -> RangeRequest {
    RangeRequest {
        object: parquet(),
        offset,
        len,
    }
}

/// The headline claim: a 4KB footer read costs 4KB at the origin.
///
/// The offset is deliberately in the middle of a 256MB block, so a
/// block-aligned fetch would be visibly different — 268435456 bytes starting
/// at 0 — rather than coincidentally equal.
#[tokio::test]
async fn a_selective_read_fetches_exactly_the_bytes_it_asked_for() {
    let (worker, origin) = worker().await;
    let offset = (200 << 20) + 12_345;

    let served = worker.serve(&read(offset, FOOTER_LEN)).await.unwrap();
    assert_eq!(served.len(), FOOTER_LEN);

    let fetches = origin.fetches();
    assert_eq!(
        fetches.len(),
        1,
        "one read must cost one fetch: {fetches:?}"
    );
    assert_eq!(
        fetches[0],
        Fetch {
            start: offset,
            end: offset + FOOTER_LEN - 1,
        },
        "the origin saw a different range than the client asked for"
    );
    assert_eq!(origin.bytes_fetched(), FOOTER_LEN);

    // Stated the other way, because this is the number in the ADR: a block
    // cache would have moved 256MB for the same 4KB.
    assert!(
        origin.bytes_fetched() < (256u64 << 20),
        "fetched {} bytes for a {FOOTER_LEN}-byte read",
        origin.bytes_fetched()
    );
}

/// The footer-then-column-chunk pattern, which is the workload this worker was
/// built for. Scattered small reads must stay scattered small fetches — no
/// coalescing into the span between them, no rounding up.
#[tokio::test]
async fn scattered_column_chunk_reads_never_fetch_the_gaps_between_them() {
    let (worker, origin) = worker().await;
    let reads = [
        (OBJECT_LEN - 8, 8u64),        // the Parquet magic + footer length
        (OBJECT_LEN - 65_536, 65_536), // the footer metadata
        (1 << 20, 4096),               // column chunk A
        (300 << 20, 128 << 10),        // column chunk B, far away
    ];

    let mut expected_total = 0;
    for (offset, len) in reads {
        assert_eq!(worker.serve(&read(offset, len)).await.unwrap().len(), len);
        expected_total += len;
    }

    let fetches = origin.fetches();
    assert_eq!(
        fetches.len(),
        reads.len(),
        "one fetch per read: {fetches:?}"
    );
    assert_eq!(
        origin.bytes_fetched(),
        expected_total,
        "fetched more than the sum of the reads: {fetches:?}"
    );

    // The span from the first chunk to the last is ~511MB. Fetching anywhere
    // near it would mean the reads were coalesced.
    for (fetch, (offset, len)) in fetches.iter().zip(reads) {
        assert_eq!((fetch.start, fetch.len()), (offset, len));
    }
}

/// A repeat read must not touch the origin at all. Without this the cache is
/// only a latency trick, not a bandwidth one.
#[tokio::test]
async fn a_repeated_read_costs_the_origin_nothing() {
    let (worker, origin) = worker().await;
    let offset = 4 << 20;

    let first = worker.serve(&read(offset, FOOTER_LEN)).await.unwrap();
    let after_first = origin.bytes_fetched();
    assert_eq!(after_first, FOOTER_LEN);

    for _ in 0..10 {
        let again = worker.serve(&read(offset, FOOTER_LEN)).await.unwrap();
        assert_eq!(again.len(), first.len());
    }
    assert_eq!(
        origin.bytes_fetched(),
        after_first,
        "a cache hit reached the origin: {:?}",
        origin.fetches()
    );
}

/// A read *inside* an already-cached extent is a hit, not a second fetch.
#[tokio::test]
async fn a_shorter_read_inside_a_cached_extent_is_a_hit() {
    let (worker, origin) = worker().await;
    let offset = 8 << 20;

    worker.serve(&read(offset, 65_536)).await.unwrap();
    assert_eq!(origin.bytes_fetched(), 65_536);

    // Same start, less length: covered by what is already held.
    let short = worker.serve(&read(offset, 1024)).await.unwrap();
    assert_eq!(short.len(), 1024);
    assert_eq!(
        origin.bytes_fetched(),
        65_536,
        "a covered read refetched: {:?}",
        origin.fetches()
    );
}

/// A read *longer* than the cached extent refetches at the larger size — and
/// at that size only. ADR 0005 §2: for a given `(object, offset)` the cache
/// converges on the largest range any reader has asked for.
#[tokio::test]
async fn a_longer_read_refetches_at_the_new_size_and_no_further() {
    let (worker, origin) = worker().await;
    let offset = 16 << 20;

    worker.serve(&read(offset, 4096)).await.unwrap();
    worker.serve(&read(offset, 32_768)).await.unwrap();

    let fetches = origin.fetches();
    assert_eq!(
        fetches.len(),
        2,
        "the longer read must refetch: {fetches:?}"
    );
    assert_eq!(fetches[1].len(), 32_768, "refetched at the requested size");
    assert_eq!(origin.bytes_fetched(), 4096 + 32_768);

    // Converged: a third read at the larger size is now a hit.
    worker.serve(&read(offset, 32_768)).await.unwrap();
    assert_eq!(origin.bytes_fetched(), 4096 + 32_768);
}

/// A read past the end of the object is clamped before it is fetched, so the
/// origin never sees a range it cannot satisfy.
#[tokio::test]
async fn a_read_past_the_end_is_clamped_rather_than_over_requested() {
    let (worker, origin) = worker().await;
    let offset = OBJECT_LEN - 100;

    let served = worker.serve(&read(offset, 4096)).await.unwrap();
    assert_eq!(served.len(), 100, "clamped to what exists");

    let fetches = origin.fetches();
    assert_eq!(fetches.len(), 1);
    assert_eq!(fetches[0].end, OBJECT_LEN - 1, "never past the last byte");
    assert_eq!(fetches[0].len(), 100);
}

/// The runtime's own counter must agree with what the origin recorded.
///
/// These are measured at different layers — one is an atomic in the serve path,
/// the other is the `Range` header at the socket — so agreement means the
/// counter is not quietly under-reporting a wider fetch.
#[tokio::test]
async fn the_reported_origin_bytes_match_what_the_origin_actually_served() {
    let (worker, origin) = worker().await;
    for (offset, len) in [(0u64, 4096u64), (1 << 20, 8192), (64 << 20, 1 << 20)] {
        worker.serve(&read(offset, len)).await.unwrap();
    }

    let stats = worker.stats();
    assert_eq!(stats.origin_bytes_fetched, origin.bytes_fetched());
    assert_eq!(stats.bytes_served, 4096 + 8192 + (1 << 20));
    assert_eq!(stats.served, 3);
}

/// Origin bytes must never exceed bytes served on a cold, no-repeat workload —
/// and must fall below it the moment anything repeats. This is the ratio the
/// metrics doc calls "the whole thesis"; a regression that over-fetches pushes
/// it above 1.
#[tokio::test]
async fn origin_bytes_never_exceed_bytes_served() {
    let (worker, origin) = worker().await;

    // Cold and distinct: parity.
    for i in 0..8u64 {
        worker.serve(&read(i * (1 << 20), 4096)).await.unwrap();
    }
    let cold = worker.stats();
    assert_eq!(cold.origin_bytes_fetched, cold.bytes_served);

    // Warm: served climbs, origin does not.
    for _ in 0..4 {
        for i in 0..8u64 {
            worker.serve(&read(i * (1 << 20), 4096)).await.unwrap();
        }
    }
    let warm = worker.stats();
    assert_eq!(warm.origin_bytes_fetched, cold.origin_bytes_fetched);
    assert_eq!(warm.bytes_served, cold.bytes_served * 5);
    assert_eq!(origin.bytes_fetched(), warm.origin_bytes_fetched);
}

/// Concurrent readers of the same extent collapse into one origin fetch.
///
/// Without coalescing, a query engine opening a file from N threads pays N
/// footer fetches — which on a cold cache is the exact stampede the cache is
/// supposed to absorb.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_readers_of_one_extent_produce_one_fetch() {
    let (worker, origin) = worker().await;
    let offset = 32 << 20;

    let mut tasks = Vec::new();
    for _ in 0..16 {
        let worker = Arc::clone(&worker);
        tasks.push(tokio::spawn(async move {
            worker.serve(&read(offset, FOOTER_LEN)).await.unwrap().len()
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), FOOTER_LEN);
    }

    assert_eq!(
        origin.bytes_fetched(),
        FOOTER_LEN,
        "16 concurrent readers produced {:?}",
        origin.fetches()
    );
}

/// A write must be refused rather than quietly forwarded to the origin
/// (ADR 0005 §8). The recording origin rejects PUT outright, so a regression
/// here shows up as an unexpected method rather than a silent write.
#[tokio::test]
async fn the_origin_never_sees_a_write() {
    let (worker, origin) = worker().await;
    worker.serve(&read(0, 4096)).await.unwrap();

    let refusal = AsyncWorkerRuntime::write_unsupported(&parquet()).to_string();
    assert!(refusal.contains("read-only"), "{refusal}");
    assert!(
        refusal.contains("talon-worker"),
        "the refusal must say where writes go: {refusal}"
    );

    // Only the read's HEAD and GET reached the origin.
    assert_eq!(origin.fetches().len(), 1);
}
