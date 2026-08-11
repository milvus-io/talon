//! Snapshot-based origin credential providers with background refresh.
//!
//! Backends sign every request with a point-in-time snapshot taken from a
//! provider, so a refresh atomically replaces credentials without tearing an
//! in-flight request. Static providers wrap fixed env/config material and
//! never spawn anything; refreshing providers poll a [`CredentialsFetch`]
//! implementation (STS, IMDS-style metadata, or an OAuth token endpoint) and
//! retain the last good value when a refresh fails, matching the gateway's
//! TLS and auth-file reload semantics.

use std::sync::{Arc, RwLock, Weak};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;

use crate::s3::S3Credentials;

/// Source of the S3-family credential triple used for SigV4 signing.
pub trait ProvideS3Credentials: Send + Sync {
    /// Cheap point-in-time snapshot used to build and sign exactly one request.
    fn current(&self) -> Arc<S3Credentials>;
}

/// Source of an OAuth2-style bearer token (GCS, Azure AD).
pub trait ProvideBearerToken: Send + Sync {
    /// Current token, or `None` when the endpoint needs no authentication
    /// (emulators).
    fn current(&self) -> Option<Arc<String>>;
}

/// Fixed credentials from env/config; never refreshed.
pub struct StaticS3Credentials(Arc<S3Credentials>);

impl StaticS3Credentials {
    /// Wrap fixed credentials.
    pub fn new(creds: S3Credentials) -> Self {
        Self(Arc::new(creds))
    }
}

impl ProvideS3Credentials for StaticS3Credentials {
    fn current(&self) -> Arc<S3Credentials> {
        Arc::clone(&self.0)
    }
}

/// Fixed bearer token from env/config; never refreshed.
pub struct StaticBearerToken(Option<Arc<String>>);

impl StaticBearerToken {
    /// Wrap a fixed optional token.
    pub fn new(token: Option<String>) -> Self {
        Self(token.map(Arc::new))
    }
}

impl ProvideBearerToken for StaticBearerToken {
    fn current(&self) -> Option<Arc<String>> {
        self.0.clone()
    }
}

/// Observer for refresh outcomes so each binary counts them in its own
/// metrics registry. Implementations must not block.
pub trait CredentialsObserver: Send + Sync {
    /// A refresh produced and installed new credentials.
    fn refresh_succeeded(&self, expires_at: Option<SystemTime>) {
        let _ = expires_at;
    }

    /// A refresh failed; the previous credentials remain in use.
    fn refresh_failed(&self) {}
}

/// Observer that drops every event.
pub struct NoopObserver;

impl CredentialsObserver for NoopObserver {}

/// One credential acquisition protocol (STS exchange, metadata endpoint, or
/// OAuth token endpoint).
#[async_trait]
pub trait CredentialsFetch: Send + Sync + 'static {
    /// The credential material produced by this fetcher.
    type Value: Send + Sync + 'static;

    /// Acquire fresh credentials and their hard expiry. Errors must not
    /// contain token or key material.
    async fn fetch(&self) -> Result<(Self::Value, Option<SystemTime>), String>;

    /// Bounded label naming the acquisition mechanism, for logs.
    fn source(&self) -> &'static str;
}

/// Refresh pacing. The margin scales with the credential lifetime and is
/// clamped so short-lived tokens still refresh early and long-lived ones do
/// not refresh needlessly often.
#[derive(Debug, Clone)]
pub struct RefreshPolicy {
    /// Fraction of the remaining lifetime reserved as the refresh margin.
    pub margin_fraction: f64,
    /// Lower clamp for the margin.
    pub min_margin: Duration,
    /// Upper clamp for the margin.
    pub max_margin: Duration,
    /// Delay before retrying after a failed refresh.
    pub retry_interval: Duration,
    /// Refresh period when the mechanism reports no expiry.
    pub unknown_expiry_interval: Duration,
}

impl Default for RefreshPolicy {
    fn default() -> Self {
        Self {
            margin_fraction: 0.2,
            min_margin: Duration::from_secs(60),
            max_margin: Duration::from_secs(15 * 60),
            retry_interval: Duration::from_secs(30),
            unknown_expiry_interval: Duration::from_secs(10 * 60),
        }
    }
}

impl RefreshPolicy {
    /// Time to sleep before the next refresh attempt for credentials fetched
    /// `now` with `expires_at`.
    fn next_delay(&self, now: SystemTime, expires_at: Option<SystemTime>) -> Duration {
        let Some(expires_at) = expires_at else {
            return self.unknown_expiry_interval;
        };
        let lifetime = expires_at.duration_since(now).unwrap_or(Duration::ZERO);
        let margin = lifetime.mul_f64(self.margin_fraction.clamp(0.0, 1.0));
        let margin = margin.clamp(self.min_margin, self.max_margin);
        let delay = lifetime.saturating_sub(margin);
        // An already-expired or nearly-expired credential still backs off by
        // the retry interval so a broken issuer is not hammered.
        delay.max(self.retry_interval)
    }
}

/// Shared cell holding the latest credentials from a background refresh loop.
pub struct RefreshingCredentials<F: CredentialsFetch> {
    current: RwLock<Arc<F::Value>>,
    expires_at: RwLock<Option<SystemTime>>,
}

impl<F: CredentialsFetch> RefreshingCredentials<F> {
    /// Fetch initial credentials, then keep them fresh for as long as the
    /// returned handle is alive. A failed initial fetch is a startup error so
    /// a misconfigured process refuses to serve, exactly like missing static
    /// credentials.
    pub async fn bootstrap(
        fetcher: F,
        policy: RefreshPolicy,
        observer: Arc<dyn CredentialsObserver>,
    ) -> Result<Arc<Self>, String> {
        let source = fetcher.source();
        let (value, expires_at) = fetcher
            .fetch()
            .await
            .map_err(|error| format!("initial {source} credential fetch failed: {error}"))?;
        observer.refresh_succeeded(expires_at);
        let cell = Arc::new(Self {
            current: RwLock::new(Arc::new(value)),
            expires_at: RwLock::new(expires_at),
        });
        spawn_refresh(Arc::downgrade(&cell), fetcher, policy, observer);
        Ok(cell)
    }

    /// Expiry of the credentials currently installed, if the mechanism
    /// reports one.
    pub fn expires_at(&self) -> Option<SystemTime> {
        *self.expires_at.read().unwrap()
    }

    fn snapshot(&self) -> Arc<F::Value> {
        Arc::clone(&self.current.read().unwrap())
    }

    fn install(&self, value: F::Value, expires_at: Option<SystemTime>) {
        *self.current.write().unwrap() = Arc::new(value);
        *self.expires_at.write().unwrap() = expires_at;
    }
}

impl<F> ProvideS3Credentials for RefreshingCredentials<F>
where
    F: CredentialsFetch<Value = S3Credentials>,
{
    fn current(&self) -> Arc<S3Credentials> {
        self.snapshot()
    }
}

impl<F> ProvideBearerToken for RefreshingCredentials<F>
where
    F: CredentialsFetch<Value = String>,
{
    fn current(&self) -> Option<Arc<String>> {
        Some(self.snapshot())
    }
}

fn spawn_refresh<F: CredentialsFetch>(
    cell: Weak<RefreshingCredentials<F>>,
    fetcher: F,
    policy: RefreshPolicy,
    observer: Arc<dyn CredentialsObserver>,
) {
    tokio::spawn(async move {
        let source = fetcher.source();
        let mut delay = {
            let Some(cell) = cell.upgrade() else { return };
            policy.next_delay(SystemTime::now(), cell.expires_at())
        };
        loop {
            tokio::time::sleep(delay).await;
            let Some(cell) = cell.upgrade() else { return };
            match fetcher.fetch().await {
                Ok((value, expires_at)) => {
                    cell.install(value, expires_at);
                    observer.refresh_succeeded(expires_at);
                    tracing::info!(source, "origin credentials refreshed");
                    delay = policy.next_delay(SystemTime::now(), expires_at);
                }
                Err(error) => {
                    observer.refresh_failed();
                    // Fetch errors never contain credential material, so the
                    // reason is safe to log.
                    tracing::warn!(
                        source,
                        %error,
                        "origin credential refresh failed; retaining last credentials"
                    );
                    delay = policy.retry_interval;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct ScriptedFetch {
        calls: AtomicU32,
        fail_from: u32,
        lifetime: Duration,
    }

    #[async_trait]
    impl CredentialsFetch for ScriptedFetch {
        type Value = S3Credentials;

        async fn fetch(&self) -> Result<(S3Credentials, Option<SystemTime>), String> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call >= self.fail_from {
                return Err("issuer unavailable".into());
            }
            Ok((
                S3Credentials {
                    access_key_id: format!("AKID{call}"),
                    secret_access_key: "secret".into(),
                    session_token: Some(format!("token{call}")),
                },
                Some(SystemTime::now() + self.lifetime),
            ))
        }

        fn source(&self) -> &'static str {
            "scripted"
        }
    }

    #[derive(Default)]
    struct CountingObserver {
        succeeded: AtomicU32,
        failed: AtomicU32,
    }

    impl CredentialsObserver for CountingObserver {
        fn refresh_succeeded(&self, _expires_at: Option<SystemTime>) {
            self.succeeded.fetch_add(1, Ordering::SeqCst);
        }

        fn refresh_failed(&self) {
            self.failed.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn fast_policy() -> RefreshPolicy {
        RefreshPolicy {
            margin_fraction: 0.5,
            min_margin: Duration::from_millis(40),
            max_margin: Duration::from_millis(40),
            retry_interval: Duration::from_millis(10),
            unknown_expiry_interval: Duration::from_millis(10),
        }
    }

    #[tokio::test]
    async fn refresh_replaces_snapshots_and_reports_expiry() {
        let observer = Arc::new(CountingObserver::default());
        let cell = RefreshingCredentials::bootstrap(
            ScriptedFetch {
                calls: AtomicU32::new(0),
                fail_from: u32::MAX,
                lifetime: Duration::from_millis(50),
            },
            fast_policy(),
            Arc::clone(&observer) as Arc<dyn CredentialsObserver>,
        )
        .await
        .unwrap();
        assert_eq!(cell.current().access_key_id, "AKID0");
        assert!(cell.expires_at().is_some());

        tokio::time::timeout(Duration::from_secs(2), async {
            while cell.current().access_key_id == "AKID0" {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("credentials must rotate");
        assert!(observer.succeeded.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn failed_refresh_retains_last_credentials_and_counts() {
        let observer = Arc::new(CountingObserver::default());
        let cell = RefreshingCredentials::bootstrap(
            ScriptedFetch {
                calls: AtomicU32::new(0),
                fail_from: 1,
                lifetime: Duration::from_millis(30),
            },
            fast_policy(),
            Arc::clone(&observer) as Arc<dyn CredentialsObserver>,
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while observer.failed.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("failures must be observed");
        // The last good snapshot survives every failed refresh.
        assert_eq!(cell.current().access_key_id, "AKID0");
        assert_eq!(cell.current().session_token.as_deref(), Some("token0"));
    }

    #[tokio::test]
    async fn failed_initial_fetch_is_a_startup_error() {
        let result = RefreshingCredentials::bootstrap(
            ScriptedFetch {
                calls: AtomicU32::new(0),
                fail_from: 0,
                lifetime: Duration::from_secs(1),
            },
            RefreshPolicy::default(),
            Arc::new(NoopObserver),
        )
        .await;
        let error = result.err().unwrap();
        assert!(error.contains("scripted"));
        assert!(!error.contains("AKID"));
    }

    #[tokio::test]
    async fn refresh_loop_exits_when_the_cell_is_dropped() {
        let observer = Arc::new(CountingObserver::default());
        let cell = RefreshingCredentials::bootstrap(
            ScriptedFetch {
                calls: AtomicU32::new(0),
                fail_from: u32::MAX,
                lifetime: Duration::from_millis(20),
            },
            fast_policy(),
            Arc::clone(&observer) as Arc<dyn CredentialsObserver>,
        )
        .await
        .unwrap();
        drop(cell);
        tokio::time::sleep(Duration::from_millis(80)).await;
        let after_drop = observer.succeeded.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(80)).await;
        // At most one in-flight refresh lands after the drop; the loop must
        // then stop fetching.
        assert!(observer.succeeded.load(Ordering::SeqCst) <= after_drop + 1);
    }

    #[test]
    fn margins_are_clamped_and_never_zero() {
        let policy = RefreshPolicy::default();
        let now = SystemTime::now();
        // 1h lifetime: 20% margin = 12min, clamped nothing → refresh ~48min.
        let delay = policy.next_delay(now, Some(now + Duration::from_secs(3600)));
        assert!(delay > Duration::from_secs(40 * 60) && delay < Duration::from_secs(50 * 60));
        // Expired credential still backs off by the retry interval.
        assert_eq!(
            policy.next_delay(now, Some(now - Duration::from_secs(5))),
            policy.retry_interval
        );
        // No expiry → periodic refresh.
        assert_eq!(policy.next_delay(now, None), policy.unknown_expiry_interval);
    }

    #[test]
    fn static_providers_hand_out_the_same_material() {
        let creds = StaticS3Credentials::new(S3Credentials {
            access_key_id: "AKID".into(),
            secret_access_key: "secret".into(),
            session_token: None,
        });
        assert_eq!(creds.current().access_key_id, "AKID");
        assert!(StaticBearerToken::new(None).current().is_none());
        assert_eq!(
            StaticBearerToken::new(Some("tok".into()))
                .current()
                .unwrap()
                .as_str(),
            "tok"
        );
    }
}
