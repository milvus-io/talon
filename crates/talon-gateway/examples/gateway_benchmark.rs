//! Reproducible loopback benchmark for shared gateway overhead and backpressure.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::body::Body;
use axum::extract::Request;
use bytes::Bytes;
use futures::StreamExt;
use serde_json::json;
use talon_gateway::{
    serve, GatewayAdapter, GatewayConfig, GatewayOperation, GatewayRequestContext, GatewayResponse,
    GatewayRuntime, GatewaySecurity, ProviderProtocol,
};
use tokio::net::TcpListener;

const DEFAULT_REQUESTS: usize = 2_000;
const DEFAULT_LARGE_OBJECT_BYTES: usize = 64 * 1024 * 1024;
const STREAM_CHUNK_BYTES: usize = 64 * 1024;

struct BenchmarkAdapter;

#[async_trait]
impl GatewayAdapter for BenchmarkAdapter {
    fn protocol(&self) -> ProviderProtocol {
        ProviderProtocol::S3
    }

    async fn handle(&self, request: Request, _context: GatewayRequestContext) -> GatewayResponse {
        let delay = match request.uri().path() {
            "/cache" => Duration::from_micros(100),
            "/worker" => Duration::from_millis(1),
            "/origin" => Duration::from_millis(5),
            _ => Duration::ZERO,
        };
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        let body = if request.uri().path() == "/large" {
            let bytes = std::env::var("TALON_GATEWAY_BENCH_OBJECT_BYTES")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(DEFAULT_LARGE_OBJECT_BYTES);
            let chunks = bytes.div_ceil(STREAM_CHUNK_BYTES);
            let chunk = Bytes::from(vec![0x5a; STREAM_CHUNK_BYTES]);
            let stream = futures::stream::unfold(0usize, move |index| {
                let chunk = chunk.clone();
                async move {
                    (index < chunks).then(|| {
                        let remaining = bytes.saturating_sub(index * STREAM_CHUNK_BYTES);
                        let frame = if remaining < chunk.len() {
                            chunk.slice(..remaining)
                        } else {
                            chunk
                        };
                        (Ok::<_, std::io::Error>(frame), index + 1)
                    })
                }
            });
            Body::from_stream(stream)
        } else {
            Body::empty()
        };
        GatewayResponse::new(axum::response::Response::new(body), GatewayOperation::Read)
    }
}

fn percentile(samples: &mut [u64], percent: usize) -> u64 {
    samples.sort_unstable();
    samples[(samples.len() - 1) * percent / 100]
}

async fn phase(client: &reqwest::Client, base: &str, name: &str, injected_us: u64, count: usize) {
    for _ in 0..100 {
        client
            .get(format!("{base}/{name}"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }
    let mut samples = Vec::with_capacity(count);
    for _ in 0..count {
        let started = Instant::now();
        client
            .get(format!("{base}/{name}"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        samples.push(started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
    }
    let p50 = percentile(&mut samples, 50);
    let p99 = percentile(&mut samples, 99);
    println!(
        "{}",
        json!({
            "kind": "phase",
            "phase": name,
            "requests": count,
            "injected_us": injected_us,
            "p50_ns": p50,
            "p99_ns": p99,
            "p50_over_injected_ns": p50.saturating_sub(injected_us * 1_000),
        })
    );
}

#[cfg(target_os = "linux")]
fn rss_bytes() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()
        .map(|kib| kib * 1024)
}

#[cfg(not(target_os = "linux"))]
fn rss_bytes() -> Option<u64> {
    None
}

async fn slow_stream(client: &reqwest::Client, base: &str) {
    let baseline = rss_bytes();
    let mut response = client
        .get(format!("{base}/large"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes_stream();
    let started = Instant::now();
    let mut bytes = 0u64;
    let mut peak = baseline;
    while let Some(chunk) = response.next().await {
        bytes += chunk.unwrap().len() as u64;
        peak = peak.max(rss_bytes());
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let elapsed = started.elapsed();
    println!(
        "{}",
        json!({
            "kind": "slow_stream",
            "object_bytes": bytes,
            "chunk_bytes": STREAM_CHUNK_BYTES,
            "client_delay_ms": 1,
            "elapsed_ms": elapsed.as_millis(),
            "throughput_mib_s": bytes as f64 / elapsed.as_secs_f64() / 1024.0 / 1024.0,
            "baseline_rss_bytes": baseline,
            "peak_rss_bytes": peak,
            "peak_rss_delta_bytes": peak.zip(baseline).map(|(peak, base)| peak.saturating_sub(base)),
        })
    );
}

#[tokio::main]
async fn main() {
    let count = std::env::var("TALON_GATEWAY_BENCH_REQUESTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_REQUESTS);
    assert!(count > 0, "TALON_GATEWAY_BENCH_REQUESTS must be positive");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let runtime = Arc::new(
        GatewayRuntime::new(
            GatewayConfig {
                bind: address,
                ..GatewayConfig::default()
            },
            Arc::new(BenchmarkAdapter),
            GatewaySecurity::default(),
        )
        .unwrap(),
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(serve(listener, runtime, async move {
        let _ = shutdown_rx.await;
    }));
    let base = format!("http://{address}");
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(1)
        .build()
        .unwrap();

    phase(&client, &base, "empty", 0, count).await;
    phase(&client, &base, "cache", 100, count).await;
    phase(&client, &base, "worker", 1_000, count).await;
    phase(&client, &base, "origin", 5_000, count).await;
    slow_stream(&client, &base).await;

    shutdown_tx.send(()).unwrap();
    server.await.unwrap().unwrap();
}
