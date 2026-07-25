//! A latency-injecting [`HttpClient`] decorator for the test/latency lab.
//!
//! Wraps any inner [`HttpClient`] and sleeps before (fixed first-byte latency +
//! optional jitter) and after (optional bandwidth throttle proportional to the
//! response size) delegating. This models the latency *pattern* of a real object
//! store — first-byte latency, tail jitter, throughput ceiling — on top of a
//! fast local origin (Azurite), independent of any network proxy.
//!
//! It is **opt-in and inert by default**: [`DelayConfig::default`] injects no
//! delay, and the worker only wraps its client when the delay knobs are set. The
//! jitter is a deterministic LCG seeded per client, so behavior is reproducible
//! in tests without pulling in an RNG dependency and without wall-clock flake.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::http::{HttpClient, HttpRequest, HttpResponse};

/// Tunable latency model applied by [`DelayingHttpClient`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DelayConfig {
    /// Fixed first-byte latency added to every request.
    pub base: Duration,
    /// Upper bound of uniformly-distributed extra latency in `[0, jitter]` added
    /// to each request. `Duration::ZERO` disables jitter (fully deterministic).
    pub jitter: Duration,
    /// Optional bandwidth ceiling in bytes/second. When set, a response of `n`
    /// bytes adds `n / throughput` seconds of transfer time on top of `base`.
    /// `None` (or zero) disables the throttle.
    pub throughput_bytes_per_sec: Option<u64>,
}

impl DelayConfig {
    /// A no-op config (no latency injected). Same as [`Default`].
    pub const NONE: DelayConfig = DelayConfig {
        base: Duration::ZERO,
        jitter: Duration::ZERO,
        throughput_bytes_per_sec: None,
    };

    /// Whether this config would inject any delay at all.
    pub fn is_active(&self) -> bool {
        !self.base.is_zero()
            || !self.jitter.is_zero()
            || matches!(self.throughput_bytes_per_sec, Some(bps) if bps > 0)
    }
}

impl Default for DelayConfig {
    fn default() -> Self {
        Self::NONE
    }
}

/// An [`HttpClient`] that injects latency around an inner client.
pub struct DelayingHttpClient {
    inner: Arc<dyn HttpClient>,
    config: DelayConfig,
    /// Deterministic LCG state advanced once per request for jitter selection.
    rng_state: AtomicU64,
}

impl DelayingHttpClient {
    /// Wrap `inner`, injecting latency per `config`. Seed fixes the jitter
    /// sequence so runs are reproducible.
    pub fn new(inner: Arc<dyn HttpClient>, config: DelayConfig, seed: u64) -> Self {
        Self {
            inner,
            config,
            // Avoid a zero state (the LCG below still progresses, but a nonzero
            // seed gives a more varied low-index sequence).
            rng_state: AtomicU64::new(seed ^ 0x9E37_79B9_7F4A_7C15),
        }
    }

    /// Draw the next jitter value in `[0, jitter]` deterministically. Returns
    /// `Duration::ZERO` when jitter is disabled.
    fn next_jitter(&self) -> Duration {
        let span_ms = self.config.jitter.as_millis() as u64;
        if span_ms == 0 {
            return Duration::ZERO;
        }
        // SplitMix64-style advance: cheap, dependency-free, good enough spread
        // for latency jitter. fetch_update keeps it correct under concurrency.
        let prev = self
            .rng_state
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |s| {
                Some(s.wrapping_add(0x9E37_79B9_7F4A_7C15))
            })
            .unwrap();
        let mut z = prev.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        // Inclusive of the upper bound.
        Duration::from_millis(z % (span_ms + 1))
    }

    /// Total delay for a response of `response_len` bytes: base + jitter +
    /// transfer time under the optional bandwidth ceiling. Exposed for testing.
    pub fn delay_for(&self, response_len: usize) -> Duration {
        let mut total = self.config.base + self.next_jitter();
        if let Some(bps) = self.config.throughput_bytes_per_sec {
            if bps > 0 {
                // seconds = bytes / (bytes/sec); compute in nanos to avoid float.
                let nanos = (response_len as u128 * 1_000_000_000u128) / bps as u128;
                total += Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX));
            }
        }
        total
    }
}

#[async_trait]
impl HttpClient for DelayingHttpClient {
    async fn execute(&self, req: HttpRequest) -> Result<HttpResponse, String> {
        // Delegate first so the throttle can scale with the actual response size,
        // then sleep for the modeled latency before returning the bytes. A
        // transport error skips the delay (nothing was transferred).
        let resp = self.inner.execute(req).await?;
        let delay = self.delay_for(resp.body.len());
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct MockHttp {
        body_len: usize,
        calls: Mutex<u32>,
    }

    impl MockHttp {
        fn new(body_len: usize) -> Arc<Self> {
            Arc::new(Self {
                body_len,
                calls: Mutex::new(0),
            })
        }
    }

    #[async_trait]
    impl HttpClient for MockHttp {
        async fn execute(&self, _req: HttpRequest) -> Result<HttpResponse, String> {
            *self.calls.lock().unwrap() += 1;
            Ok(HttpResponse {
                status: 200,
                headers: vec![],
                body: bytes::Bytes::from(vec![0u8; self.body_len]),
            })
        }
    }

    fn req() -> HttpRequest {
        HttpRequest {
            method: crate::http::Method::Get,
            url: "http://origin/x".into(),
            headers: vec![],
            body: bytes::Bytes::new(),
        }
    }

    #[test]
    fn default_config_is_inert() {
        assert!(!DelayConfig::default().is_active());
        let c = DelayingHttpClient::new(MockHttp::new(1024), DelayConfig::default(), 1);
        assert_eq!(c.delay_for(1024), Duration::ZERO);
    }

    #[test]
    fn base_latency_is_fixed_without_jitter() {
        let cfg = DelayConfig {
            base: Duration::from_millis(200),
            jitter: Duration::ZERO,
            throughput_bytes_per_sec: None,
        };
        let c = DelayingHttpClient::new(MockHttp::new(0), cfg, 7);
        // Deterministic across draws when jitter is off.
        assert_eq!(c.delay_for(0), Duration::from_millis(200));
        assert_eq!(c.delay_for(9999), Duration::from_millis(200));
    }

    #[test]
    fn throughput_adds_transfer_time() {
        // 1 MiB at 1 MB/s ≈ 1.048s transfer, plus 100ms base.
        let cfg = DelayConfig {
            base: Duration::from_millis(100),
            jitter: Duration::ZERO,
            throughput_bytes_per_sec: Some(1_000_000),
        };
        let c = DelayingHttpClient::new(MockHttp::new(0), cfg, 1);
        let d = c.delay_for(1_048_576);
        assert_eq!(
            d,
            Duration::from_millis(100) + Duration::from_nanos(1_048_576_000)
        );
    }

    #[test]
    fn jitter_stays_within_bounds_and_varies() {
        let cfg = DelayConfig {
            base: Duration::from_millis(50),
            jitter: Duration::from_millis(100),
            throughput_bytes_per_sec: None,
        };
        let c = DelayingHttpClient::new(MockHttp::new(0), cfg, 42);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..200 {
            let d = c.delay_for(0);
            assert!(
                d >= Duration::from_millis(50) && d <= Duration::from_millis(150),
                "delay {d:?} out of [50,150]ms"
            );
            seen.insert(d.as_millis());
        }
        // A real jitter distribution produces more than one distinct value.
        assert!(seen.len() > 1, "jitter did not vary");
    }

    #[test]
    fn seed_makes_jitter_reproducible() {
        let cfg = DelayConfig {
            base: Duration::ZERO,
            jitter: Duration::from_millis(1000),
            throughput_bytes_per_sec: None,
        };
        let a = DelayingHttpClient::new(MockHttp::new(0), cfg, 123);
        let b = DelayingHttpClient::new(MockHttp::new(0), cfg, 123);
        let seq_a: Vec<_> = (0..10).map(|_| a.delay_for(0)).collect();
        let seq_b: Vec<_> = (0..10).map(|_| b.delay_for(0)).collect();
        assert_eq!(seq_a, seq_b);
    }

    #[tokio::test(start_paused = true)]
    async fn execute_sleeps_for_the_modeled_delay() {
        // With the virtual clock paused, the whole delay must elapse in-test
        // without real waiting, and the inner client is called exactly once.
        let inner = MockHttp::new(0);
        let cfg = DelayConfig {
            base: Duration::from_secs(2),
            jitter: Duration::ZERO,
            throughput_bytes_per_sec: None,
        };
        let c = DelayingHttpClient::new(inner.clone(), cfg, 1);
        let start = tokio::time::Instant::now();
        let resp = c.execute(req()).await.unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(*inner.calls.lock().unwrap(), 1);
        assert!(start.elapsed() >= Duration::from_secs(2));
    }

    #[tokio::test(start_paused = true)]
    async fn transport_error_skips_delay() {
        struct FailingHttp;
        #[async_trait]
        impl HttpClient for FailingHttp {
            async fn execute(&self, _req: HttpRequest) -> Result<HttpResponse, String> {
                Err("connect refused".into())
            }
        }
        let cfg = DelayConfig {
            base: Duration::from_secs(30),
            jitter: Duration::ZERO,
            throughput_bytes_per_sec: None,
        };
        let c = DelayingHttpClient::new(Arc::new(FailingHttp), cfg, 1);
        let start = tokio::time::Instant::now();
        assert!(c.execute(req()).await.is_err());
        assert_eq!(start.elapsed(), Duration::ZERO);
    }
}
