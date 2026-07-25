//! End-to-end latency-lab invariant test.
//!
//! The latency lab (issue #233) ships two reusable mechanisms — the
//! [`AzureBackend`] endpoint override and the [`DelayingHttpClient`] decorator —
//! wired into the worker exactly as `talon-worker/src/main.rs` wires them when
//! the `TALON_WORKER_AZURE_ENDPOINT` / `TALON_WORKER_BACKEND_*` env vars are set.
//! Its whole point is a single guarantee:
//!
//!   a cache **miss** pays the backend latency; a **hit** does not.
//!
//! This test locks that guarantee into the integration harness by driving a real
//! [`WorkerRuntime`] over the production backend stack
//! (`WorkerRuntime → AzureBackend → DelayingHttpClient → HttpClient`) on tokio's
//! **virtual clock**. The delay is deterministic (fixed base, no jitter, paused
//! clock), so the modeled latency is asserted without any real sleeping — no
//! wall-clock flake in CI.
//!
//! The HTTP layer is a counting mock (the real backend I/O boundary): it returns
//! a fixed ranged-GET body and counts every request, so "did we touch the
//! backend?" is directly observable. If a future refactor breaks
//! miss-fetches-but-hit-does-not, or the endpoint override stops producing a
//! path-style emulator URL, this test goes red.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use talon_backend::http::{HttpClient, HttpRequest, HttpResponse, Method};
use talon_backend::{AzureBackend, AzureConfig, DelayConfig, DelayingHttpClient};
use talon_core::{Backend, BackendStore, ObjectId};
use talon_transport::data::RangeRequest;
use talon_worker::{BlockIndex, InFlightLoads, WholeBlockStore, WorkerMetrics, WorkerRuntime};

/// Modeled first-byte backend latency. Must stay under the runtime's 3s
/// resolved-version TTL so the warm read is a pure cache hit (no re-HEAD).
const BASE_DELAY: Duration = Duration::from_millis(200);

/// Block size for the test (small; the whole object fits one block).
const BLOCK_BYTES: u32 = 1 << 20; // 1 MiB
const OBJECT_LEN: u64 = 4096;

/// A counting [`HttpClient`]: records each request URL and returns a fixed body
/// for GET (a ranged read) and metadata headers for HEAD. This is the real
/// networked boundary the delay decorator wraps.
struct CountingHttpClient {
    calls: AtomicUsize,
    last_get_url: Mutex<Option<String>>,
}

impl CountingHttpClient {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            last_get_url: Mutex::new(None),
        })
    }
}

#[async_trait]
impl HttpClient for CountingHttpClient {
    async fn execute(&self, req: HttpRequest) -> Result<HttpResponse, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match req.method {
            Method::Head => Ok(HttpResponse {
                status: 200,
                headers: vec![
                    ("Content-Length".into(), OBJECT_LEN.to_string()),
                    ("ETag".into(), "\"v1\"".into()),
                ],
                body: bytes::Bytes::new(),
            }),
            Method::Get => {
                *self.last_get_url.lock().unwrap() = Some(req.url.clone());
                // Honor the requested window: parse the Azure x-ms-range header
                // (`bytes=<start>-<end>`) and answer a matching 206 body.
                let (start, end) = parse_range(&req);
                let len = (end - start + 1) as usize;
                let body: Vec<u8> = (0..len)
                    .map(|i| ((start as usize + i) % 251) as u8)
                    .collect();
                Ok(HttpResponse {
                    status: 206,
                    headers: vec![(
                        "Content-Range".into(),
                        format!("bytes {start}-{end}/{OBJECT_LEN}"),
                    )],
                    body: bytes::Bytes::from(body),
                })
            }
            // The read path only issues HEAD + ranged GET.
            Method::Put | Method::Delete => {
                Err(format!("unexpected {:?} in the read-path test", req.method))
            }
        }
    }
}

/// Parse `bytes=<start>-<end>` from the request's range header (Azure uses
/// `x-ms-range`).
fn parse_range(req: &HttpRequest) -> (u64, u64) {
    let raw = req
        .header("x-ms-range")
        .or_else(|| req.header("range"))
        .expect("a range header");
    let spec = raw.strip_prefix("bytes=").expect("bytes= range");
    let (s, e) = spec.split_once('-').expect("start-end");
    (s.parse().unwrap(), e.parse().unwrap())
}

/// Build the production backend stack the worker uses in the latency lab:
/// counting HTTP client → delay decorator → AzureBackend targeting an emulator.
fn lab_backend(http: Arc<CountingHttpClient>) -> Arc<dyn BackendStore> {
    let delay = DelayConfig {
        base: BASE_DELAY,
        jitter: Duration::ZERO,
        throughput_bytes_per_sec: None,
    };
    let delaying: Arc<dyn HttpClient> = Arc::new(DelayingHttpClient::new(http, delay, 1));
    // http:// → plaintext + path-style, exactly as main.rs derives it from
    // TALON_WORKER_AZURE_ENDPOINT.
    let cfg = AzureConfig::emulator("devstoreaccount1", "127.0.0.1:10000", false);
    Arc::new(AzureBackend::new(
        cfg,
        Some("sv=lab&sig=lab".into()),
        delaying,
    ))
}

fn tmp_root(tag: &str) -> PathBuf {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    std::time::SystemTime::now().hash(&mut h);
    tag.hash(&mut h);
    std::env::temp_dir().join(format!(
        "talon-latency-lab-{}-{}",
        std::process::id(),
        h.finish()
    ))
}

fn runtime(backend: Arc<dyn BackendStore>, root: &PathBuf) -> WorkerRuntime {
    WorkerRuntime::new(
        WholeBlockStore::open(root).unwrap(),
        Arc::new(BlockIndex::new()),
        Arc::new(InFlightLoads::new()),
        backend,
        BLOCK_BYTES,
        0,
        WorkerMetrics::new(1 << 20),
    )
}

fn request() -> RangeRequest {
    RangeRequest {
        object: ObjectId::new(Backend::Azure, "cache-demo", "obj-1.bin"),
        offset: 0,
        len: 256,
    }
}

#[tokio::test(start_paused = true)]
async fn miss_pays_backend_latency_hit_is_free() {
    let root = tmp_root("invariant");
    let http = CountingHttpClient::new();
    let rt = runtime(lab_backend(Arc::clone(&http)), &root);

    // --- cold read: a cache MISS must reach the backend and pay the latency ---
    let t0 = tokio::time::Instant::now();
    let first = rt.serve_range(&request()).await.unwrap();
    let miss_elapsed = t0.elapsed();
    assert_eq!(first.len(), 256, "first read returns the requested window");
    // The miss touched the backend (HEAD to resolve the version + GET the block).
    let calls_after_miss = http.calls.load(Ordering::SeqCst);
    assert!(
        calls_after_miss >= 1,
        "a miss must hit the backend, got {calls_after_miss} calls"
    );
    // ...and it paid at least the modeled first-byte latency (virtual time).
    assert!(
        miss_elapsed >= BASE_DELAY,
        "miss should pay >= {BASE_DELAY:?} of backend latency, paid {miss_elapsed:?}"
    );

    // --- warm read: a cache HIT must NOT reach the backend and pays ~nothing ---
    let t1 = tokio::time::Instant::now();
    let second = rt.serve_range(&request()).await.unwrap();
    let hit_elapsed = t1.elapsed();
    assert_eq!(second, first, "hit returns identical bytes");
    assert_eq!(
        http.calls.load(Ordering::SeqCst),
        calls_after_miss,
        "a hit must not touch the backend"
    );
    assert!(
        hit_elapsed < BASE_DELAY,
        "hit should not pay backend latency, paid {hit_elapsed:?}"
    );

    std::fs::remove_dir_all(root).ok();
}

#[tokio::test(start_paused = true)]
async fn endpoint_override_produces_path_style_emulator_url() {
    // The lab's other half: the endpoint override must address the emulator
    // path-style (scheme://host/account/container/blob), not the public-cloud
    // virtual-host form. Prove the URL the transport actually received.
    let root = tmp_root("endpoint");
    let http = CountingHttpClient::new();
    let rt = runtime(lab_backend(Arc::clone(&http)), &root);

    rt.serve_range(&request()).await.unwrap();

    let url = http
        .last_get_url
        .lock()
        .unwrap()
        .clone()
        .expect("a GET reached the backend");
    assert!(
        url.starts_with("http://127.0.0.1:10000/devstoreaccount1/cache-demo/obj-1.bin"),
        "expected a path-style emulator URL, got {url}"
    );

    std::fs::remove_dir_all(root).ok();
}
