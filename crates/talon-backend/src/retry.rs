//! A retrying [`HttpClient`] decorator with backoff, jitter, and per-attempt
//! timeouts.
//!
//! Object stores throttle (`429`) and shed load (`503`) as a matter of routine,
//! and TCP connections to them die for uninteresting reasons. Without a retry
//! layer every one of those becomes a failed block fetch, which surfaces to the
//! application as a failed read — a cache that is *less* reliable than reading
//! the origin directly. This decorator wraps any inner [`HttpClient`] so all
//! three backends (S3/GCS/Azure) get the same policy from one place.
//!
//! **What is retried.** Transport errors (connect/DNS/reset) and the transient
//! status set `408, 429, 500, 502, 503, 504`. Everything else is returned to the
//! caller untouched — in particular `404` (mapped to `ENOENT` upstream) and
//! `412`, which is not a failure at all but the version-mismatch signal the
//! `If-Match` read path relies on (issue #163); retrying it would defeat the
//! TOCTOU guard it exists to provide.
//!
//! **What is never retried.** A request that both carries a body *and* a
//! precondition header — i.e. a conditional PUT. If such a request succeeds but
//! the response is lost, the retry sees the precondition it already consumed and
//! returns `412`, turning a success into a spurious failure. Unconditional PUT
//! and DELETE are safe (whole-object replace and idempotent delete respectively)
//! and are retried; conditional *reads* are also safe, since a GET has no side
//! effect and the precondition simply re-evaluates.
//!
//! **Backoff** is full jitter — `random(0, min(cap, base * 2^attempt))` — which
//! spreads a fleet's retries instead of aligning them into a second thundering
//! herd against an already-struggling origin. The randomness is a deterministic
//! SplitMix64 seeded per client, matching [`crate::delay`]: reproducible in
//! tests, no RNG dependency. A `Retry-After` header on `429`/`503` overrides the
//! computed delay (clamped to `max_delay`), because obeying the origin's own
//! backoff hint is what actually clears a throttle.
//!
//! **Timeouts scale with transfer size.** Requests here span a ~200-byte HEAD
//! and a 256 MiB block fetch — six orders of magnitude. A single flat deadline
//! cannot serve both: sized for the HEAD, every large fetch times out and
//! retries, each retry re-transferring 256 MiB and worsening the congestion;
//! sized for the fetch, a hung HEAD wedges the read path for minutes. So the
//! per-attempt deadline is `timeout_floor + bytes / min_throughput`, where
//! `bytes` comes from the request body (PUT) or the requested range (GET), both
//! known before the request goes out.
//!
//! Timeouts are applied whether or not retries are enabled; `max_retries = 0`
//! disables retrying but still bounds each attempt.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::http::{HttpClient, HttpRequest, HttpRequestBody, HttpResponse};

/// Statuses worth another attempt: request timeout, throttling, and the
/// transient server-side 5xx set. Deliberately excludes `501 Not Implemented`
/// and `505` (permanent), and every 4xx other than `408`/`429`.
fn is_retryable_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
}

/// Precondition headers across the three backends. S3 and Azure use the RFC
/// `If-Match`/`If-None-Match`; GCS additionally accepts a generation number via
/// `x-goog-if-generation-match`.
const PRECONDITION_HEADERS: [&str; 3] = ["if-match", "if-none-match", "x-goog-if-generation-match"];

/// Whether `req` may be re-issued after an inconclusive attempt.
///
/// The unsafe case is a *conditional mutation*: the precondition is consumed by
/// the first (apparently failed) attempt, so the retry gets `412` and reports
/// failure for a write that actually landed. A body is the proxy for "mutates" —
/// GET/HEAD/DELETE carry none.
fn is_retryable_request(req: &HttpRequest) -> bool {
    if req.body.is_empty() {
        return true;
    }
    !PRECONDITION_HEADERS.iter().any(|h| req.header(h).is_some())
}

/// Parse a `Retry-After` value expressed as delta-seconds.
///
/// RFC 7231 also permits an HTTP-date, but S3 and GCS send delta-seconds in
/// practice, and supporting dates would mean taking on a date-parsing
/// dependency for a format we never see. A non-numeric value is ignored and the
/// computed backoff is used instead.
fn parse_retry_after(value: &str) -> Option<Duration> {
    value.trim().parse::<u64>().ok().map(Duration::from_secs)
}

/// Bytes expected to cross the wire for `req`, used to size the deadline.
///
/// A PUT carries them in the body. A ranged GET declares them in `Range`
/// (S3/GCS) or `x-ms-range` (Azure), both formatted `bytes=<start>-<end>`
/// inclusive. Anything else (HEAD, DELETE, unranged GET) is treated as
/// metadata-sized and gets the floor.
fn expected_transfer_bytes(req: &HttpRequest) -> u64 {
    if !req.body.is_empty() {
        return req.body.len() as u64;
    }
    let range = req
        .header("range")
        .or_else(|| req.header("x-ms-range"))
        .unwrap_or_default();
    let Some(spec) = range.trim().strip_prefix("bytes=") else {
        return 0;
    };
    let Some((start, end)) = spec.split_once('-') else {
        return 0;
    };
    match (start.trim().parse::<u64>(), end.trim().parse::<u64>()) {
        // Inclusive range, so the length is end - start + 1.
        (Ok(s), Ok(e)) if e >= s => e - s + 1,
        _ => 0,
    }
}

/// Notified when the decorator retries or times out, so the worker can surface
/// both as metrics.
///
/// A retrying cache hides backend degradation by design: reads keep succeeding
/// while the origin gets slower and more expensive. Without these counters that
/// decline is invisible until it becomes an outage.
pub trait RetryObserver: Send + Sync {
    /// A request is about to be re-issued. `attempt` is 0-indexed (0 = the first
    /// retry). `status` is the response code that triggered it, or `None` for a
    /// transport error.
    fn on_retry(&self, attempt: u32, status: Option<u16>);
    /// An attempt exceeded its deadline.
    fn on_timeout(&self);
}

/// Retry and timeout policy for [`RetryingHttpClient`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryConfig {
    /// Retries after the initial attempt. `0` disables retrying; timeouts still
    /// apply.
    pub max_retries: u32,
    /// Base of the exponential backoff.
    pub base_delay: Duration,
    /// Ceiling for any single backoff wait, including a `Retry-After` hint.
    pub max_delay: Duration,
    /// Fixed part of the per-attempt deadline, covering connect and
    /// time-to-first-byte.
    pub timeout_floor: Duration,
    /// Throughput floor used to extend the deadline by transfer size. A transfer
    /// slower than this is treated as hung. `0` disables the size-scaled part,
    /// leaving a flat `timeout_floor`.
    pub min_throughput_bytes_per_sec: u64,
}

impl RetryConfig {
    /// Retry disabled, with a flat one-minute deadline. For tests that want the
    /// decorator inert rather than absent.
    pub const NONE: RetryConfig = RetryConfig {
        max_retries: 0,
        base_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
        timeout_floor: Duration::from_secs(60),
        min_throughput_bytes_per_sec: 0,
    };

    /// Whether this config would retry at all.
    pub fn is_active(&self) -> bool {
        self.max_retries > 0
    }

    /// Deadline for a single attempt transferring `bytes`.
    ///
    /// Saturating throughout: a pathological `bytes` or a tiny throughput floor
    /// yields a very long deadline rather than an overflow panic.
    pub fn attempt_timeout(&self, bytes: u64) -> Duration {
        if self.min_throughput_bytes_per_sec == 0 {
            return self.timeout_floor;
        }
        let transfer_secs = bytes / self.min_throughput_bytes_per_sec;
        self.timeout_floor
            .saturating_add(Duration::from_secs(transfer_secs))
    }
}

impl Default for RetryConfig {
    /// Retry is **on by default**, unlike the latency knobs in [`crate::delay`].
    /// A cache with no retry is the bug this module exists to fix, so the safe
    /// default is the protective one.
    ///
    /// The defaults give a worst case of roughly `0.1 + 0.2 + 0.4 = 0.7s` of
    /// backoff across three retries, and a deadline of 5s for a HEAD or ~30s for
    /// a 256 MiB block at 10 MiB/s.
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            timeout_floor: Duration::from_secs(5),
            min_throughput_bytes_per_sec: 10 * 1024 * 1024,
        }
    }
}

/// An [`HttpClient`] that retries transient failures of an inner client.
pub struct RetryingHttpClient {
    inner: Arc<dyn HttpClient>,
    config: RetryConfig,
    observer: Option<Arc<dyn RetryObserver>>,
    /// Deterministic SplitMix64 state, advanced once per backoff draw.
    rng_state: AtomicU64,
}

impl RetryingHttpClient {
    /// Wrap `inner` with `config`. `seed` fixes the jitter sequence so runs are
    /// reproducible; pass a per-node value so peers do not retry in lockstep.
    pub fn new(inner: Arc<dyn HttpClient>, config: RetryConfig, seed: u64) -> Self {
        Self {
            inner,
            config,
            observer: None,
            rng_state: AtomicU64::new(seed ^ 0x9E37_79B9_7F4A_7C15),
        }
    }

    /// Attach a [`RetryObserver`] to receive retry and timeout notifications.
    pub fn with_observer(mut self, observer: Arc<dyn RetryObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Draw the next pseudo-random value (SplitMix64), as in [`crate::delay`].
    fn next_random(&self) -> u64 {
        let prev = self
            .rng_state
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |s| {
                Some(s.wrapping_add(0x9E37_79B9_7F4A_7C15))
            })
            .unwrap_or(0);
        let mut z = prev.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Backoff before the retry following `attempt` (0-indexed).
    ///
    /// Full jitter over the exponential window. `retry_after`, when the origin
    /// supplied one, replaces the draw — it is a real instruction rather than a
    /// guess — but is still clamped to `max_delay` so a hostile or mistaken
    /// header cannot stall a read indefinitely.
    pub fn backoff_delay(&self, attempt: u32, retry_after: Option<Duration>) -> Duration {
        if let Some(hint) = retry_after {
            return hint.min(self.config.max_delay);
        }
        // Saturating shift: attempt is capped so `1 << attempt` cannot overflow.
        let window = self
            .config
            .base_delay
            .saturating_mul(1u32 << attempt.min(16))
            .min(self.config.max_delay);
        let window_ms = window.as_millis() as u64;
        if window_ms == 0 {
            return Duration::ZERO;
        }
        Duration::from_millis(self.next_random() % (window_ms + 1))
    }
}

#[async_trait]
impl HttpClient for RetryingHttpClient {
    async fn execute(&self, req: HttpRequest) -> Result<HttpResponse, String> {
        let retryable_request = is_retryable_request(&req);
        let timeout = self.config.attempt_timeout(expected_transfer_bytes(&req));
        let mut attempt: u32 = 0;

        loop {
            // Clone per attempt: `HttpRequest` is cheap to clone (the body is
            // `Bytes`), and the inner client consumes it.
            let outcome = tokio::time::timeout(timeout, self.inner.execute(req.clone())).await;

            // `Err` here is the deadline, not a transport failure.
            let result = match outcome {
                Ok(result) => result,
                Err(_) => {
                    if let Some(observer) = &self.observer {
                        observer.on_timeout();
                    }
                    Err(format!("request timed out after {timeout:?}"))
                }
            };

            // Decide whether another attempt is warranted, and capture the
            // origin's backoff hint while the response is still in hand.
            let (should_retry, status, retry_after) = match &result {
                Ok(resp) if is_retryable_status(resp.status) => {
                    let hint = match resp.status {
                        429 | 503 => resp.header("retry-after").and_then(parse_retry_after),
                        _ => None,
                    };
                    (true, Some(resp.status), hint)
                }
                Ok(_) => (false, None, None),
                Err(_) => (true, None, None),
            };

            let exhausted = attempt >= self.config.max_retries;
            if !should_retry || !retryable_request || exhausted {
                // Return the last outcome as-is. A retryable status that ran out
                // of attempts is handed back unchanged so the backend maps it to
                // its usual error rather than a synthesized one; a timeout
                // surfaces as a transport error.
                return result;
            }

            if let Some(observer) = &self.observer {
                observer.on_retry(attempt, status);
            }
            let delay = self.backoff_delay(attempt, retry_after);
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            attempt += 1;
        }
    }

    async fn execute_body(
        &self,
        req: HttpRequest,
        body: HttpRequestBody,
        len: u64,
    ) -> Result<HttpResponse, String> {
        let timeout = self.config.attempt_timeout(len);
        match tokio::time::timeout(timeout, self.inner.execute_body(req, body, len)).await {
            Ok(result) => result,
            Err(_) => {
                if let Some(observer) = &self.observer {
                    observer.on_timeout();
                }
                Err(format!("request timed out after {timeout:?}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::Method;
    use std::sync::Mutex;

    /// A client that returns a scripted sequence of outcomes, then repeats the
    /// last one, counting calls.
    struct ScriptedHttp {
        script: Mutex<Vec<Result<u16, String>>>,
        calls: Mutex<u32>,
        retry_after: Option<String>,
    }

    impl ScriptedHttp {
        fn new(script: Vec<Result<u16, String>>) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(script),
                calls: Mutex::new(0),
                retry_after: None,
            })
        }

        fn with_retry_after(script: Vec<Result<u16, String>>, value: &str) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(script),
                calls: Mutex::new(0),
                retry_after: Some(value.to_string()),
            })
        }

        fn calls(&self) -> u32 {
            *self.calls.lock().unwrap()
        }
    }

    #[async_trait]
    impl HttpClient for ScriptedHttp {
        async fn execute(&self, _req: HttpRequest) -> Result<HttpResponse, String> {
            let mut calls = self.calls.lock().unwrap();
            let idx = *calls as usize;
            *calls += 1;
            drop(calls);
            let script = self.script.lock().unwrap();
            let entry = script.get(idx).unwrap_or_else(|| script.last().unwrap());
            match entry {
                Ok(status) => {
                    let mut headers = vec![];
                    if let Some(v) = &self.retry_after {
                        headers.push(("Retry-After".to_string(), v.clone()));
                    }
                    Ok(HttpResponse {
                        status: *status,
                        headers,
                        body: bytes::Bytes::new(),
                    })
                }
                Err(e) => Err(e.clone()),
            }
        }

        async fn execute_body(
            &self,
            req: HttpRequest,
            _body: HttpRequestBody,
            _len: u64,
        ) -> Result<HttpResponse, String> {
            self.execute(req).await
        }
    }

    /// A client that never responds, to exercise the deadline.
    struct HangingHttp;

    #[async_trait]
    impl HttpClient for HangingHttp {
        async fn execute(&self, _req: HttpRequest) -> Result<HttpResponse, String> {
            // Far longer than any deadline under test.
            tokio::time::sleep(Duration::from_secs(86_400)).await;
            unreachable!("deadline should fire first")
        }
    }

    #[derive(Default)]
    struct CountingObserver {
        retries: AtomicU64,
        timeouts: AtomicU64,
    }

    impl RetryObserver for CountingObserver {
        fn on_retry(&self, _attempt: u32, _status: Option<u16>) {
            self.retries.fetch_add(1, Ordering::Relaxed);
        }
        fn on_timeout(&self) {
            self.timeouts.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn get() -> HttpRequest {
        HttpRequest::new(Method::Get, "https://origin/o".into(), vec![])
    }

    fn ranged_get(offset: u64, end: u64) -> HttpRequest {
        HttpRequest::new(
            Method::Get,
            "https://origin/o".into(),
            vec![("Range".into(), format!("bytes={offset}-{end}"))],
        )
    }

    fn conditional_put() -> HttpRequest {
        HttpRequest::with_body(
            Method::Put,
            "https://origin/o".into(),
            vec![("If-Match".into(), "\"v1\"".into())],
            bytes::Bytes::from_static(b"payload"),
        )
    }

    fn unconditional_put() -> HttpRequest {
        HttpRequest::with_body(
            Method::Put,
            "https://origin/o".into(),
            vec![],
            bytes::Bytes::from_static(b"payload"),
        )
    }

    fn client(inner: Arc<dyn HttpClient>) -> RetryingHttpClient {
        RetryingHttpClient::new(inner, RetryConfig::default(), 7)
    }

    #[tokio::test]
    async fn single_use_stream_is_never_retried() {
        let inner = ScriptedHttp::new(vec![Ok(503), Ok(200)]);
        let body = futures::stream::once(async { Ok(bytes::Bytes::from_static(b"payload")) });
        let response = client(Arc::clone(&inner) as Arc<dyn HttpClient>)
            .execute_body(
                HttpRequest::new(Method::Put, "https://origin/o".into(), Vec::new()),
                Box::pin(body),
                7,
            )
            .await
            .unwrap();
        assert_eq!(response.status, 503);
        assert_eq!(inner.calls(), 1);
    }

    // ── classification ───────────────────────────────────────────────

    #[test]
    fn transient_statuses_are_retryable_and_others_are_not() {
        for s in [408, 429, 500, 502, 503, 504] {
            assert!(is_retryable_status(s), "{s} should be retryable");
        }
        // 404 is ENOENT upstream; 412 is the #163 version-mismatch signal.
        for s in [200, 206, 301, 400, 403, 404, 412, 501] {
            assert!(!is_retryable_status(s), "{s} should not be retryable");
        }
    }

    #[test]
    fn conditional_mutations_are_not_retryable_but_other_shapes_are() {
        assert!(!is_retryable_request(&conditional_put()));
        assert!(is_retryable_request(&unconditional_put()));
        assert!(is_retryable_request(&get()));
        // A conditional GET is safe: no side effect, precondition re-evaluates.
        let cond_get = HttpRequest::new(
            Method::Get,
            "https://origin/o".into(),
            vec![("If-Match".into(), "\"v1\"".into())],
        );
        assert!(is_retryable_request(&cond_get));
    }

    #[test]
    fn gcs_generation_precondition_blocks_retry_of_a_write() {
        let req = HttpRequest::with_body(
            Method::Put,
            "https://origin/o".into(),
            vec![("x-goog-if-generation-match".into(), "42".into())],
            bytes::Bytes::from_static(b"payload"),
        );
        assert!(!is_retryable_request(&req));
    }

    // ── deadline sizing ──────────────────────────────────────────────

    #[test]
    fn transfer_size_comes_from_body_or_range_header() {
        assert_eq!(expected_transfer_bytes(&get()), 0);
        assert_eq!(expected_transfer_bytes(&unconditional_put()), 7);
        // Inclusive range: bytes=0-1023 is 1024 bytes.
        assert_eq!(expected_transfer_bytes(&ranged_get(0, 1023)), 1024);
        assert_eq!(expected_transfer_bytes(&ranged_get(4096, 8191)), 4096);
        // Azure's header name is honored too.
        let azure = HttpRequest::new(
            Method::Get,
            "https://origin/o".into(),
            vec![("x-ms-range".into(), "bytes=8-15".into())],
        );
        assert_eq!(expected_transfer_bytes(&azure), 8);
    }

    #[test]
    fn malformed_range_falls_back_to_the_floor() {
        for value in ["garbage", "bytes=", "bytes=abc-def", "bytes=100-1"] {
            let req = HttpRequest::new(
                Method::Get,
                "https://origin/o".into(),
                vec![("Range".into(), value.into())],
            );
            assert_eq!(expected_transfer_bytes(&req), 0, "value {value:?}");
        }
    }

    #[test]
    fn deadline_scales_with_size() {
        let cfg = RetryConfig::default();
        // A HEAD-sized request gets the bare floor.
        assert_eq!(cfg.attempt_timeout(0), Duration::from_secs(5));
        // 256 MiB at 10 MiB/s ≈ 25.6s of transfer on top of the floor.
        assert_eq!(
            cfg.attempt_timeout(256 * 1024 * 1024),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn deadline_is_flat_when_throughput_floor_is_disabled() {
        let cfg = RetryConfig {
            min_throughput_bytes_per_sec: 0,
            ..RetryConfig::default()
        };
        assert_eq!(cfg.attempt_timeout(u64::MAX), cfg.timeout_floor);
    }

    // ── backoff ──────────────────────────────────────────────────────

    #[test]
    fn full_jitter_stays_within_the_exponential_window() {
        let c = client(ScriptedHttp::new(vec![Ok(200)]));
        for attempt in 0..4u32 {
            let window = c
                .config
                .base_delay
                .saturating_mul(1 << attempt)
                .min(c.config.max_delay);
            for _ in 0..64 {
                let d = c.backoff_delay(attempt, None);
                assert!(d <= window, "attempt {attempt}: {d:?} exceeds {window:?}");
            }
        }
    }

    #[test]
    fn jitter_varies_and_is_reproducible_for_a_seed() {
        let a =
            RetryingHttpClient::new(ScriptedHttp::new(vec![Ok(200)]), RetryConfig::default(), 99);
        let b =
            RetryingHttpClient::new(ScriptedHttp::new(vec![Ok(200)]), RetryConfig::default(), 99);
        let seq_a: Vec<_> = (0..16).map(|_| a.backoff_delay(3, None)).collect();
        let seq_b: Vec<_> = (0..16).map(|_| b.backoff_delay(3, None)).collect();
        assert_eq!(seq_a, seq_b, "same seed must replay the same sequence");
        assert!(
            seq_a.iter().collect::<std::collections::HashSet<_>>().len() > 1,
            "jitter did not vary"
        );
    }

    #[test]
    fn retry_after_overrides_backoff_but_is_clamped() {
        let c = client(ScriptedHttp::new(vec![Ok(200)]));
        assert_eq!(
            c.backoff_delay(0, Some(Duration::from_secs(2))),
            Duration::from_secs(2)
        );
        // A 1-hour hint must not stall the read path past max_delay.
        assert_eq!(
            c.backoff_delay(0, Some(Duration::from_secs(3600))),
            c.config.max_delay
        );
    }

    #[test]
    fn retry_after_parses_delta_seconds_only() {
        assert_eq!(parse_retry_after("3"), Some(Duration::from_secs(3)));
        assert_eq!(parse_retry_after("  7 "), Some(Duration::from_secs(7)));
        // HTTP-date form is unsupported by design; fall back to computed backoff.
        assert_eq!(parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT"), None);
        assert_eq!(parse_retry_after(""), None);
    }

    // ── retry loop ───────────────────────────────────────────────────

    #[tokio::test(start_paused = true)]
    async fn transient_failures_are_retried_until_success() {
        let inner = ScriptedHttp::new(vec![Err("connection reset".into()), Ok(503), Ok(200)]);
        let observer = Arc::new(CountingObserver::default());
        let c = client(inner.clone()).with_observer(observer.clone());
        let resp = c.execute(get()).await.unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(inner.calls(), 3);
        assert_eq!(observer.retries.load(Ordering::Relaxed), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn success_is_not_retried() {
        let inner = ScriptedHttp::new(vec![Ok(206)]);
        let c = client(inner.clone());
        assert_eq!(c.execute(get()).await.unwrap().status, 206);
        assert_eq!(inner.calls(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn exhausted_retries_return_the_last_response_unchanged() {
        // The backend, not this decorator, decides what a terminal 503 means.
        let inner = ScriptedHttp::new(vec![Ok(503)]);
        let c = client(inner.clone());
        let resp = c.execute(get()).await.unwrap();
        assert_eq!(resp.status, 503);
        assert_eq!(inner.calls(), 4, "1 initial + 3 retries");
    }

    #[tokio::test(start_paused = true)]
    async fn permanent_statuses_are_not_retried() {
        for status in [404, 412, 403] {
            let inner = ScriptedHttp::new(vec![Ok(status)]);
            let c = client(inner.clone());
            assert_eq!(c.execute(get()).await.unwrap().status, status);
            assert_eq!(inner.calls(), 1, "status {status} must not be retried");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn conditional_put_is_attempted_exactly_once() {
        let inner = ScriptedHttp::new(vec![Ok(503)]);
        let c = client(inner.clone());
        assert_eq!(c.execute(conditional_put()).await.unwrap().status, 503);
        assert_eq!(inner.calls(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn unconditional_put_is_retried() {
        let inner = ScriptedHttp::new(vec![Ok(500), Ok(200)]);
        let c = client(inner.clone());
        assert_eq!(c.execute(unconditional_put()).await.unwrap().status, 200);
        assert_eq!(inner.calls(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn zero_max_retries_disables_retrying() {
        let inner = ScriptedHttp::new(vec![Ok(503)]);
        let cfg = RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        };
        assert!(!cfg.is_active());
        let c = RetryingHttpClient::new(inner.clone(), cfg, 1);
        assert_eq!(c.execute(get()).await.unwrap().status, 503);
        assert_eq!(inner.calls(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_after_hint_is_honored_on_503() {
        let inner = ScriptedHttp::with_retry_after(vec![Ok(503), Ok(200)], "2");
        let c = client(inner.clone());
        let start = tokio::time::Instant::now();
        assert_eq!(c.execute(get()).await.unwrap().status, 200);
        // Computed backoff at attempt 0 is at most base_delay (100ms), so a wait
        // of ~2s can only have come from the header.
        assert!(start.elapsed() >= Duration::from_secs(2));
    }

    // ── deadlines ────────────────────────────────────────────────────

    #[tokio::test(start_paused = true)]
    async fn a_hung_attempt_times_out_and_is_retried() {
        let observer = Arc::new(CountingObserver::default());
        let cfg = RetryConfig {
            max_retries: 1,
            ..RetryConfig::default()
        };
        let c =
            RetryingHttpClient::new(Arc::new(HangingHttp), cfg, 1).with_observer(observer.clone());
        let err = c.execute(get()).await.unwrap_err();
        assert!(err.contains("timed out"), "unexpected error: {err}");
        // Initial attempt plus one retry, each hitting the deadline.
        assert_eq!(observer.timeouts.load(Ordering::Relaxed), 2);
        assert_eq!(observer.retries.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_is_bounded_by_the_configured_floor() {
        let cfg = RetryConfig {
            max_retries: 0,
            timeout_floor: Duration::from_secs(5),
            ..RetryConfig::default()
        };
        let c = RetryingHttpClient::new(Arc::new(HangingHttp), cfg, 1);
        let start = tokio::time::Instant::now();
        assert!(c.execute(get()).await.is_err());
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_secs(5) && elapsed < Duration::from_secs(6),
            "elapsed {elapsed:?} should be just over the 5s floor"
        );
    }
}
