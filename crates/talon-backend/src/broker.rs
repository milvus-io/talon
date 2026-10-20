//! Explicit single-origin Cloud Credential Broker acquisition.
//!
//! The endpoint and request come from worker configuration or request metadata.
//! Neither broker replies nor transport errors are included in errors: both can
//! carry credentials.
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use serde_json::Value;

use crate::credentials::{
    CredentialsFetch, CredentialsObserver, ProvideBearerToken, ProvideS3Credentials,
    ProvideSasToken, RefreshPolicy,
};
use crate::gcs::GcpBearerToken;
use crate::http::{HttpClient, HttpRequest, Method};
use crate::workload_identity::{env_var, EnvFn};
use crate::{AzureSas, S3Credentials};

pub use talon_core::BrokerConfig;

// Common request metadata. CSP may also be echoed in the response.
pub const FIELD_CSP: &str = "csp";
const FIELD_CREDENTIAL_TYPE: &str = "credentialType";
pub(crate) const FIELD_REGION: &str = "region";
pub(crate) const FIELD_BUCKET: &str = "bucket";
const FIELD_DURATION_SECONDS: &str = "durationSeconds";

// AWS target-role parameters; ignored when the Broker uses its own IAM identity.
const FIELD_ROLE_ARN: &str = "roleArn";
const FIELD_EXTERNAL_ID: &str = "externalId";

// GCP impersonation target; ignored in Broker IAM mode.
const FIELD_GCP_SERVICE_ACCOUNT_EMAIL: &str = "gcpServiceAccountEmail";

// Azure target identity and storage account. IAM ignores client/tenant IDs,
// while the account remains required to select the storage resource.
const FIELD_AZURE_CLIENT_ID: &str = "azureClientId";
const FIELD_AZURE_TENANT_ID: &str = "azureTenantId";
pub(crate) const FIELD_AZURE_ACCOUNT_NAME: &str = "azureAccountName";

// Response envelope: success status and the nested credential object.
const FIELD_SUCCESS: &str = "success";
const FIELD_CREDENTIALS: &str = "credentials";

// Credential payload: temporary keys/token and their authoritative expiry.
const FIELD_TEMP_AK: &str = "tempAk";
const FIELD_TEMP_SK: &str = "tempSk";
const FIELD_SESSION_TOKEN: &str = "sessionToken";
const FIELD_EXPIRED_AT: &str = "expiredAt";

// Cloud identifiers used by the Broker to choose its credential provider.
pub(crate) const CSP_AWS: &str = "aws";
pub(crate) const CSP_GCP: &str = "gcp";
pub(crate) const CSP_AZURE: &str = "azure";
const CSP_ALI: &str = "ali";
const CSP_TC: &str = "tc";
const CSP_HWC: &str = "hwc";

/// Broker CSPs with S3 credentials that require an explicit storage endpoint.
pub const S3_COMPATIBLE_CSPS: &[&str] = &[CSP_ALI, CSP_TC, CSP_HWC];

// Acquisition modes: the Broker's own IAM identity or an explicit target identity.
const CREDENTIAL_TYPE_IAM: &str = "iam";
const CREDENTIAL_TYPE_STS_ASSUME_ROLE: &str = "stsAssumeRole";

// Fields removed in IAM mode from both the outgoing request and cache identity.
const IAM_IGNORED_IDENTITY_FIELDS: [&str; 5] = [
    FIELD_ROLE_ARN,
    FIELD_EXTERNAL_ID,
    FIELD_GCP_SERVICE_ACCOUNT_EMAIL,
    FIELD_AZURE_CLIENT_ID,
    FIELD_AZURE_TENANT_ID,
];

// Broker exchange deadline and credential lifetime defaults/bounds, in seconds.

const DEFAULT_DURATION_SECONDS: i64 = 3600;
const MAX_DURATION_SECONDS: i64 = 3600;
const MIN_IAM_DURATION_SECONDS: i64 = 1;
const MIN_STS_DURATION_SECONDS: i64 = 900;

// Environment override for the Broker HTTP deadline.
const ENV_REQUEST_TIMEOUT_MS: &str = "TALON_CREDENTIAL_BROKER_TIMEOUT_MS";

// Provider-specific identifier lengths checked before credential acquisition.
const MAX_IAM_REGION_LEN: usize = 63;
const AZURE_ACCOUNT_NAME_LENGTH: std::ops::RangeInclusive<usize> = 3..=24;
const AZURE_CONTAINER_NAME_LENGTH: std::ops::RangeInclusive<usize> = 3..=63;

/// Validated exchange state; origin options are consumed by origin preparation.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct NormalizedBrokerConfig {
    url: String,
    request: Value,
    request_timeout: Duration,
}

impl NormalizedBrokerConfig {
    pub(crate) fn url(&self) -> &str {
        &self.url
    }
    pub(crate) fn request(&self) -> &Value {
        &self.request
    }
}

impl std::fmt::Debug for NormalizedBrokerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NormalizedBrokerConfig([REDACTED])")
    }
}

pub(crate) fn config_from_env(env: EnvFn<'_>) -> Result<BrokerConfig, String> {
    let url =
        env("TALON_CREDENTIAL_BROKER_URL").ok_or("broker requires TALON_CREDENTIAL_BROKER_URL")?;
    let raw = env("TALON_CREDENTIAL_BROKER_REQUEST")
        .ok_or("broker requires TALON_CREDENTIAL_BROKER_REQUEST")?;
    let request = serde_json::from_str(&raw).map_err(|_| "broker request must be a JSON object")?;
    Ok(BrokerConfig {
        url,
        request,
        origin: env(ENV_REQUEST_TIMEOUT_MS)
            .filter(|value| !value.is_empty())
            .map(|value| {
                value
                    .parse::<u64>()
                    .map(|timeout| talon_core::OriginOptions {
                        request_timeout_ms: Some(timeout),
                        ..Default::default()
                    })
                    .map_err(|_| format!("{ENV_REQUEST_TIMEOUT_MS} must be a positive integer"))
            })
            .transpose()?,
    })
}

/// Broker fetcher for S3 credentials, a GCP bearer token, or an Azure SAS.
/// Construction validates configuration before any network request.
pub(crate) struct BrokerFetch<T> {
    url: String,
    request: Value,
    http: Arc<dyn HttpClient>,
    request_timeout: Duration,
    value: PhantomData<T>,
}

impl<T> BrokerFetch<T> {
    pub(crate) fn from_env(
        env: EnvFn<'_>,
        csp: &str,
        account: Option<&str>,
        http: Arc<dyn HttpClient>,
    ) -> Result<Self, String> {
        let config = normalize_config(&config_from_env(env)?, csp, account)?;
        Ok(Self::new(config, http))
    }

    pub(crate) fn new(config: NormalizedBrokerConfig, http: Arc<dyn HttpClient>) -> Self {
        Self {
            url: config.url,
            request: config.request,
            request_timeout: config.request_timeout,
            http,
            value: PhantomData,
        }
    }

    async fn exchange(&self) -> Result<(Value, Option<SystemTime>), String> {
        let request = HttpRequest::with_body(
            Method::Post,
            self.url.clone(),
            vec![("content-type".into(), "application/json".into())],
            bytes::Bytes::from(self.request.to_string()),
        );
        let response = tokio::time::timeout(self.request_timeout, self.http.execute(request))
            .await
            .map_err(|_| "broker request timed out")?
            .map_err(|_| "broker transport failed")?;
        if !response.is_success() {
            return Err(format!("broker returned HTTP status {}", response.status));
        }
        let body: Value = serde_json::from_slice(&response.body)
            .map_err(|_| "broker response is not valid JSON")?;
        if body.get(FIELD_SUCCESS).and_then(Value::as_bool) != Some(true) {
            return Err("broker reported failure".into());
        }
        let credentials = body
            .get(FIELD_CREDENTIALS)
            .filter(|v| v.is_object())
            .ok_or("broker response is missing credentials")?
            .clone();
        // A CSP field is optional in the documented reply. If an issuer supplies
        // it, do not silently accept credentials for a different cloud.
        for value in [body.get(FIELD_CSP), credentials.get(FIELD_CSP)]
            .into_iter()
            .flatten()
        {
            if Some(value) != self.request.get(FIELD_CSP) {
                return Err("broker response csp does not match selected backend".into());
            }
        }
        let expiry = required_string(&credentials, FIELD_EXPIRED_AT)?;
        let parsed = chrono::DateTime::parse_from_rfc3339(expiry)
            .map_err(|_| "broker expiredAt is not a valid RFC3339 date")?;
        let expiry: SystemTime = parsed.into();
        if expiry <= SystemTime::now() {
            return Err("broker credentials are expired".into());
        }
        Ok((credentials, Some(expiry)))
    }
}

/// Validate once without I/O and return the exact payload retained for refresh.
/// Routing uses this same normalization for backend reuse and cache isolation.
pub(crate) fn normalize_config(
    config: &BrokerConfig,
    expected_csp: &str,
    expected_account: Option<&str>,
) -> Result<NormalizedBrokerConfig, String> {
    if let Some(origin) = &config.origin {
        origin.validate()?;
    }
    let endpoint =
        url::Url::parse(&config.url).map_err(|_| "broker endpoint must be a full HTTP(S) URL")?;
    if !matches!(endpoint.scheme(), "http" | "https")
        || endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(
            "broker endpoint must be a full HTTP(S) URL without userinfo or fragment".into(),
        );
    }
    // Each provider retains its own normalized request and URL for refresh.
    let mut request = config.request.clone();
    if !request.is_object() {
        return Err("broker request must be a JSON object".into());
    }
    // Pod IAM is a mode of the remote Broker, not the worker's local IAM
    // source. Missing target identity fields must never select it implicitly.
    let iam = match request.get(FIELD_CREDENTIAL_TYPE) {
        None => false,
        Some(Value::String(value))
            if value.is_empty() || value == CREDENTIAL_TYPE_STS_ASSUME_ROLE =>
        {
            false
        }
        Some(Value::String(value)) if value == CREDENTIAL_TYPE_IAM => true,
        _ => return Err("broker credentialType must be iam or stsAssumeRole".into()),
    };
    let requested_csp = required_string(&request, FIELD_CSP)?.to_owned();
    let raw_duration = match request.get(FIELD_DURATION_SECONDS) {
        None => DEFAULT_DURATION_SECONDS,
        Some(value) => value
            .as_i64()
            .ok_or("broker durationSeconds must be an integer")?,
    };
    let duration = if iam {
        // S3-compatible clouds share the credential shape, but retain their own
        // CSP in both the POST body and the cache key.
        if requested_csp != expected_csp
            && !(expected_csp == CSP_AWS && S3_COMPATIBLE_CSPS.contains(&requested_csp.as_str()))
        {
            return Err("broker request csp does not match selected backend".into());
        }
        // IAM uses the Broker's own identity. Ignore target identity fields in
        // both the outbound payload and the backend/cache configuration key.
        for field in IAM_IGNORED_IDENTITY_FIELDS {
            request.as_object_mut().unwrap().remove(field);
        }
        let min_duration = match requested_csp.as_str() {
            CSP_AWS | CSP_ALI | CSP_TC | CSP_HWC => {
                let region = required_string(&request, FIELD_REGION)?;
                if region.len() > MAX_IAM_REGION_LEN
                    || !region.as_bytes()[0].is_ascii_lowercase()
                    || !region.contains('-')
                    || !region.split('-').all(|part| {
                        !part.is_empty()
                            && part
                                .bytes()
                                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
                    })
                {
                    return Err("broker IAM requires a valid region".into());
                }
                if matches!(requested_csp.as_str(), CSP_AWS | CSP_ALI) {
                    MIN_STS_DURATION_SECONDS
                } else {
                    MIN_IAM_DURATION_SECONDS
                }
            }
            CSP_GCP => MIN_IAM_DURATION_SECONDS,
            CSP_AZURE => {
                let bucket = required_string(&request, FIELD_BUCKET)?;
                let account = required_string(&request, FIELD_AZURE_ACCOUNT_NAME)?;
                if Some(account) != expected_account {
                    return Err(
                        "broker request Azure account does not match configured backend".into(),
                    );
                }
                let alnum = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
                if !AZURE_ACCOUNT_NAME_LENGTH.contains(&account.len())
                    || !account.bytes().all(alnum)
                    || !AZURE_CONTAINER_NAME_LENGTH.contains(&bucket.len())
                    || bucket.contains("--")
                    || !alnum(bucket.as_bytes()[0])
                    || !alnum(bucket.as_bytes()[bucket.len() - 1])
                    || !bucket.bytes().all(|byte| alnum(byte) || byte == b'-')
                {
                    return Err("broker IAM requires a valid Azure account and container".into());
                }
                MIN_IAM_DURATION_SECONDS
            }
            _ => return Err("unsupported broker csp".into()),
        };
        if raw_duration < 0 {
            return Err("broker IAM durationSeconds must not be negative".into());
        }
        let duration = if raw_duration == 0 {
            DEFAULT_DURATION_SECONDS
        } else {
            raw_duration
        };
        if !(min_duration..=MAX_DURATION_SECONDS).contains(&duration) {
            return Err(format!(
                "broker IAM durationSeconds must be in {min_duration}..{MAX_DURATION_SECONDS}"
            ));
        }
        duration
    } else {
        if requested_csp != expected_csp {
            return Err("broker request csp does not match selected backend".into());
        }
        // All spellings of the target-identity mode retain the legacy cache key.
        request
            .as_object_mut()
            .unwrap()
            .remove(FIELD_CREDENTIAL_TYPE);
        let duration = if raw_duration <= 0 {
            DEFAULT_DURATION_SECONDS
        } else {
            raw_duration
        };
        match requested_csp.as_str() {
            CSP_AWS => {
                required_string(&request, FIELD_REGION)?;
                required_string(&request, FIELD_ROLE_ARN)?;
                if !(MIN_STS_DURATION_SECONDS..=MAX_DURATION_SECONDS).contains(&duration) {
                    return Err(format!("broker AWS durationSeconds must be in {MIN_STS_DURATION_SECONDS}..{MAX_DURATION_SECONDS}"));
                }
            }
            CSP_GCP => {
                required_string(&request, FIELD_GCP_SERVICE_ACCOUNT_EMAIL)?;
            }
            CSP_AZURE => {
                for field in [
                    FIELD_REGION,
                    FIELD_BUCKET,
                    FIELD_AZURE_CLIENT_ID,
                    FIELD_AZURE_TENANT_ID,
                    FIELD_AZURE_ACCOUNT_NAME,
                ] {
                    required_string(&request, field)?;
                }
                if request[FIELD_AZURE_ACCOUNT_NAME].as_str() != expected_account {
                    return Err(
                        "broker request Azure account does not match configured backend".into(),
                    );
                }
            }
            _ => return Err("unsupported broker csp".into()),
        }
        duration
    };
    request[FIELD_DURATION_SECONDS] = duration.into();
    request.sort_all_objects();
    Ok(NormalizedBrokerConfig {
        url: endpoint.to_string(),
        request,
        request_timeout: Duration::from_millis(
            config
                .origin
                .as_ref()
                .and_then(|origin| origin.request_timeout_ms)
                .unwrap_or(talon_core::BROKER_REQUEST_TIMEOUT_MS),
        ),
    })
}

fn required_string<'a>(value: &'a Value, field: &str) -> Result<&'a str, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|v| !v.trim().is_empty() && !v.bytes().any(|b| b.is_ascii_control()))
        .ok_or_else(|| format!("broker missing or invalid {field}"))
}

#[async_trait]
impl CredentialsFetch for BrokerFetch<S3Credentials> {
    type Value = S3Credentials;
    async fn fetch(&self) -> Result<(Self::Value, Option<SystemTime>), String> {
        let (value, expiry) = self.exchange().await?;
        Ok((
            S3Credentials {
                access_key_id: required_string(&value, FIELD_TEMP_AK)?.into(),
                secret_access_key: required_string(&value, FIELD_TEMP_SK)?.into(),
                session_token: Some(required_string(&value, FIELD_SESSION_TOKEN)?.into()),
            },
            expiry,
        ))
    }
    fn source(&self) -> &'static str {
        "broker"
    }
}

#[async_trait]
impl CredentialsFetch for BrokerFetch<GcpBearerToken> {
    type Value = GcpBearerToken;
    async fn fetch(&self) -> Result<(Self::Value, Option<SystemTime>), String> {
        let (value, expiry) = self.exchange().await?;
        Ok((
            GcpBearerToken {
                token: Arc::new(required_string(&value, FIELD_SESSION_TOKEN)?.into()),
            },
            expiry,
        ))
    }
    fn source(&self) -> &'static str {
        "broker"
    }
}

#[async_trait]
impl CredentialsFetch for BrokerFetch<AzureSas> {
    type Value = AzureSas;
    async fn fetch(&self) -> Result<(Self::Value, Option<SystemTime>), String> {
        let (value, expiry) = self.exchange().await?;
        if Some(required_string(&value, FIELD_TEMP_AK)?)
            != self
                .request
                .get(FIELD_AZURE_ACCOUNT_NAME)
                .and_then(Value::as_str)
        {
            return Err("broker response Azure account does not match configured backend".into());
        }
        let sas = AzureSas::new(required_string(&value, FIELD_SESSION_TOKEN)?)?;
        // The shared constructor preserves existing request-credential semantics
        // and checks key presence only. Broker snapshots must also be usable:
        // reject empty decoded required values before replacing a working SAS.
        if url::form_urlencoded::parse(sas.expose_secret().as_bytes())
            .any(|(key, value)| matches!(key.as_ref(), "sig" | "sv") && value.trim().is_empty())
        {
            return Err("broker Azure SAS has empty required query values".into());
        }
        Ok((sas, expiry))
    }
    fn source(&self) -> &'static str {
        "broker"
    }
}

/// Broker snapshots refresh only when an outgoing request needs them.
/// The async mutex is held through the exchange and released on cancellation.
pub(crate) struct BrokerCredentials<F: CredentialsFetch> {
    fetcher: F,
    snapshot: RwLock<(Arc<F::Value>, tokio::time::Instant)>,
    refresh: tokio::sync::Mutex<()>,
    policy: RefreshPolicy,
    observer: Arc<dyn CredentialsObserver>,
}

impl<F: CredentialsFetch> BrokerCredentials<F> {
    pub(crate) async fn bootstrap(
        fetcher: F,
        policy: RefreshPolicy,
        observer: Arc<dyn CredentialsObserver>,
    ) -> Result<Arc<Self>, String> {
        let mut attempt = 0;
        let (value, expiry) = loop {
            match fetcher.fetch().await {
                Ok(result) => break result,
                Err(error) => {
                    attempt += 1;
                    if attempt == talon_core::BROKER_INITIAL_ATTEMPTS {
                        return Err(format!("initial broker credential fetch failed: {error}"));
                    }
                    tokio::time::sleep(Duration::from_millis(
                        talon_core::BROKER_INITIAL_RETRY_DELAY_MS << (attempt - 1),
                    ))
                    .await;
                }
            }
        };
        observer.refresh_succeeded(expiry);
        let due = tokio::time::Instant::now() + policy.request_delay(SystemTime::now(), expiry);
        Ok(Arc::new(Self {
            fetcher,
            snapshot: RwLock::new((Arc::new(value), due)),
            refresh: tokio::sync::Mutex::new(()),
            policy,
            observer,
        }))
    }

    fn current(&self) -> Arc<F::Value> {
        Arc::clone(&self.snapshot.read().unwrap().0)
    }

    async fn prepare(&self) -> Result<(), String> {
        if tokio::time::Instant::now() < self.snapshot.read().unwrap().1 {
            return Ok(());
        }
        // Wait for the in-flight refresh before signing, matching milvus-storage.
        // Recheck the deadline under the lock so waiters reuse a successful refresh.
        let _guard = self.refresh.lock().await;
        if tokio::time::Instant::now() < self.snapshot.read().unwrap().1 {
            return Ok(());
        }
        match self.fetcher.fetch().await {
            Ok((value, expiry)) => {
                let due = tokio::time::Instant::now()
                    + self.policy.request_delay(SystemTime::now(), expiry);
                *self.snapshot.write().unwrap() = (Arc::new(value), due);
                self.observer.refresh_succeeded(expiry);
            }
            Err(error) => {
                self.observer.refresh_failed();
                tracing::warn!(source = self.fetcher.source(), %error,
                    "origin credential refresh failed; retaining last credentials");
                // Keep the snapshot and its due time, even after expiry. The next
                // request retries immediately; no background task or retry timer.
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<F: CredentialsFetch<Value = S3Credentials>> ProvideS3Credentials for BrokerCredentials<F> {
    fn current(&self) -> Arc<S3Credentials> {
        self.current()
    }
    async fn prepare(&self) -> Result<(), String> {
        self.prepare().await
    }
}
#[async_trait]
impl<F: CredentialsFetch<Value = GcpBearerToken>> ProvideBearerToken for BrokerCredentials<F> {
    fn current(&self) -> Option<Arc<String>> {
        Some(Arc::clone(&self.current().token))
    }
    async fn prepare(&self) -> Result<(), String> {
        self.prepare().await
    }
}
#[async_trait]
impl<F: CredentialsFetch<Value = AzureSas>> ProvideSasToken for BrokerCredentials<F> {
    fn current(&self) -> Arc<AzureSas> {
        self.current()
    }
    async fn prepare(&self) -> Result<(), String> {
        self.prepare().await
    }
}

/// Resolve a refreshing Azure SAS for the configured storage account. The
/// broker request's bucket names the single container covered by the SAS.
pub async fn resolve_azure_broker_sas(
    account: &str,
    http: Arc<dyn HttpClient>,
    observer: Arc<dyn CredentialsObserver>,
) -> Result<Arc<dyn ProvideSasToken>, String> {
    resolve_azure_broker_sas_with_env(&env_var, account, http, observer).await
}

/// Injectable environment variant of [`resolve_azure_broker_sas`].
pub async fn resolve_azure_broker_sas_with_env(
    env: EnvFn<'_>,
    account: &str,
    http: Arc<dyn HttpClient>,
    observer: Arc<dyn CredentialsObserver>,
) -> Result<Arc<dyn ProvideSasToken>, String> {
    let fetcher = BrokerFetch::<AzureSas>::from_env(env, CSP_AZURE, Some(account), http)?;
    Ok(BrokerCredentials::bootstrap(fetcher, RefreshPolicy::default(), observer).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::NoopObserver;
    use crate::workload_identity::resolve_s3_credentials_with_env;
    use crate::{
        resolve_azure_credentials, resolve_gcs_credentials, resolve_s3_credentials, AzureBackend,
        AzureConfig, CredentialOptions, GcsBackend, GcsConfig, HttpResponse, S3Backend, S3Config,
    };
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use talon_core::{Backend, BackendStore, ObjectId};

    // Only the external HTTP seam is scripted. Resolvers, request-driven refresh and
    // storage request authentication run through production implementations.
    struct ScriptedHttp {
        replies: Mutex<VecDeque<Result<HttpResponse, String>>>,
        requests: Mutex<Vec<HttpRequest>>,
    }
    #[async_trait]
    impl HttpClient for ScriptedHttp {
        async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, String> {
            let broker = request.method == Method::Post;
            self.requests.lock().unwrap().push(request);
            if broker {
                self.replies
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or_else(|| Err("secret transport detail".into()))
            } else {
                Ok(HttpResponse {
                    status: 404,
                    headers: vec![],
                    body: bytes::Bytes::new(),
                })
            }
        }
    }
    fn http(replies: Vec<Result<HttpResponse, String>>) -> Arc<ScriptedHttp> {
        Arc::new(ScriptedHttp {
            replies: Mutex::new(replies.into()),
            requests: Mutex::new(vec![]),
        })
    }
    fn response(status: u16, body: Value) -> Result<HttpResponse, String> {
        Ok(HttpResponse {
            status,
            headers: vec![],
            body: bytes::Bytes::from(body.to_string()),
        })
    }
    fn credentials(ak: &str, token: &str, expiry: &str) -> Value {
        serde_json::json!({"success":true,"credentials":{"tempAk":ak,"tempSk":"secret-value","sessionToken":token,"expiredAt":expiry}})
    }
    fn env(request: Value) -> impl Fn(&str) -> Option<String> {
        move |name| match name {
            "TALON_CREDENTIAL_BROKER_URL" => Some("https://broker.example/v1/credentials".into()),
            "TALON_CREDENTIAL_BROKER_REQUEST" => Some(request.to_string()),
            _ => None,
        }
    }
    fn aws_request() -> Value {
        serde_json::json!({"csp":"aws","region":"us-east-1","roleArn":"arn:aws:iam::123456789012:role/example","externalId":"external-example","bucket":"bucket","cluster":"cluster-example"})
    }
    fn azure_request() -> Value {
        serde_json::json!({"csp":"azure","region":"eastus","bucket":"container","azureAccountName":"account","azureClientId":"client","azureTenantId":"tenant"})
    }
    fn options(request: Value) -> CredentialOptions {
        CredentialOptions {
            source: Some("broker".into()),
            broker: Some(BrokerConfig {
                origin: None,
                url: "https://explicit-broker.example/v1/credentials?key=url-secret".into(),
                request,
            }),
            ..CredentialOptions::default()
        }
    }
    fn ok<T>(result: Result<T, String>) -> T {
        result.unwrap_or_else(|error| panic!("unexpected error: {error}"))
    }
    fn error<T>(result: Result<T, String>) -> String {
        result.err().expect("must reject invalid input")
    }
    fn expiry_after(seconds: i64) -> String {
        let now: chrono::DateTime<chrono::Utc> = SystemTime::now().into();
        (now + chrono::Duration::seconds(seconds)).to_rfc3339()
    }

    #[tokio::test(start_paused = true)]
    async fn broker_concurrent_requests_wait_for_refresh_and_recover_from_cancellation() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct ControlledFetch {
            calls: Arc<AtomicUsize>,
            resume: Arc<tokio::sync::Notify>,
            initial_expiry: SystemTime,
        }
        #[async_trait]
        impl CredentialsFetch for ControlledFetch {
            type Value = String;
            async fn fetch(&self) -> Result<(String, Option<SystemTime>), String> {
                let call = self.calls.fetch_add(1, Ordering::SeqCst);
                if call == 1 {
                    self.resume.notified().await;
                }
                Ok((
                    format!("token-{call}"),
                    Some(if call == 0 {
                        self.initial_expiry
                    } else {
                        SystemTime::now() + Duration::from_secs(3600)
                    }),
                ))
            }
            fn source(&self) -> &'static str {
                "broker"
            }
        }
        // Both expired credentials and credentials in the early-refresh window
        // must wait for the in-flight exchange, matching milvus-storage.
        for initial_expiry in [
            SystemTime::UNIX_EPOCH,
            SystemTime::now() + Duration::from_secs(30),
        ] {
            for cancel_refresh in [false, true] {
                let calls = Arc::new(AtomicUsize::new(0));
                let resume = Arc::new(tokio::sync::Notify::new());
                let provider = BrokerCredentials::bootstrap(
                    ControlledFetch {
                        calls: calls.clone(),
                        resume: resume.clone(),
                        initial_expiry,
                    },
                    RefreshPolicy::default(),
                    Arc::new(NoopObserver),
                )
                .await
                .unwrap();
                let mut refresh = Box::pin(provider.prepare());
                assert!(futures::poll!(&mut refresh).is_pending());
                assert_eq!(calls.load(Ordering::SeqCst), 2);
                let mut requests = Box::pin(futures::future::join_all((0..16).map(|_| async {
                    provider.prepare().await.unwrap();
                    provider.current()
                })));
                assert!(
                    futures::poll!(&mut requests).is_pending(),
                    "concurrent requests must wait for the in-flight refresh"
                );
                assert_eq!(calls.load(Ordering::SeqCst), 2);
                if cancel_refresh {
                    drop(refresh);
                } else {
                    resume.notify_one();
                    refresh.await.unwrap();
                }
                let expected = if cancel_refresh { "token-2" } else { "token-1" };
                for snapshot in tokio::time::timeout(Duration::from_secs(1), requests)
                    .await
                    .expect("waiting requests must resume after refresh or cancellation")
                {
                    assert_eq!(&*snapshot, expected);
                }
                assert_eq!(
                    calls.load(Ordering::SeqCst),
                    if cancel_refresh { 3 } else { 2 }
                );
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn broker_gcp_refreshes_before_storage_authentication() {
        let http = http(vec![
            response(200, credentials("unused", "first", &expiry_after(100))),
            response(200, credentials("unused", "second", &expiry_after(3600))),
        ]);
        let resolved = ok(resolve_gcs_credentials(
            None,
            options(serde_json::json!({"csp":"gcp","gcpServiceAccountEmail":"reader@example.com"})),
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await);
        let backend =
            GcsBackend::with_credentials(GcsConfig::default(), resolved.provider, http.clone());
        let obj = ObjectId::new(Backend::Gcs, "bucket", "key");
        let _ = backend.head(&obj).await;
        tokio::time::advance(Duration::from_secs(40)).await;
        let _ = backend.head(&obj).await;
        let requests = http.requests.lock().unwrap();
        let heads: Vec<_> = requests
            .iter()
            .filter(|r| r.method == Method::Head)
            .collect();
        assert_eq!(heads[0].header("authorization"), Some("Bearer first"));
        assert_eq!(heads[1].header("authorization"), Some("Bearer second"));
    }

    #[tokio::test]
    async fn broker_failed_refresh_keeps_even_expired_snapshot_and_retries_immediately() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct ExpiredFetch(Arc<AtomicUsize>);
        #[async_trait]
        impl CredentialsFetch for ExpiredFetch {
            type Value = String;
            async fn fetch(&self) -> Result<(String, Option<SystemTime>), String> {
                let call = self.0.fetch_add(1, Ordering::SeqCst);
                if call == 1 {
                    return Err("broker returned HTTP status 503".into());
                }
                let expiry = if call == 0 {
                    SystemTime::UNIX_EPOCH
                } else {
                    SystemTime::now() + Duration::from_secs(3600)
                };
                Ok((format!("token-{call}"), Some(expiry)))
            }
            fn source(&self) -> &'static str {
                "broker"
            }
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = BrokerCredentials::bootstrap(
            ExpiredFetch(calls.clone()),
            RefreshPolicy::default(),
            Arc::new(NoopObserver),
        )
        .await
        .unwrap();
        provider.prepare().await.unwrap();
        assert_eq!(&*provider.current(), "token-0");
        provider.prepare().await.unwrap();
        assert_eq!(&*provider.current(), "token-2");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn broker_refresh_window_has_no_timer_floor_and_preserves_existing_margins() {
        let policy = RefreshPolicy::default();
        let now = SystemTime::UNIX_EPOCH;
        for (lifetime, delay) in [(30, 0), (80, 20), (100, 40), (3600, 2880), (10000, 9100)] {
            assert_eq!(
                policy.request_delay(now, Some(now + Duration::from_secs(lifetime))),
                Duration::from_secs(delay)
            );
        }
        assert_eq!(policy.request_delay(now, None), Duration::from_secs(600));
    }

    #[tokio::test(start_paused = true)]
    async fn typed_origins_carry_broker_exchange_deadline() {
        use crate::origin::*;
        struct HangingHttp;
        #[async_trait]
        impl HttpClient for HangingHttp {
            async fn execute(&self, _: HttpRequest) -> Result<HttpResponse, String> {
                std::future::pending().await
            }
        }
        for cloud in [Backend::S3, Backend::Gcs, Backend::Azure] {
            let request = match cloud {
                Backend::S3 => aws_request(),
                Backend::Gcs => {
                    serde_json::json!({"csp":"gcp","gcpServiceAccountEmail":"reader@example.com"})
                }
                _ => azure_request(),
            };
            let mut credentials = options(request);
            credentials.broker.as_mut().unwrap().origin = Some(talon_core::OriginOptions {
                request_timeout_ms: Some(17),
                ..Default::default()
            });
            let start = tokio::time::Instant::now();
            let http: Arc<dyn HttpClient> = Arc::new(HangingHttp);
            let failure = match cloud {
                Backend::S3 => error(
                    S3Origin::prepare(S3OriginInput {
                        region: "us-east-1".into(),
                        endpoint: None,
                        path_style: true,
                        use_ssl: None,
                        validation: OriginValidation::Routed,
                        keys: None,
                        credentials,
                    })
                    .unwrap()
                    .build(http.clone(), http, Arc::new(NoopObserver))
                    .await,
                ),
                Backend::Gcs => error(
                    GcsOrigin::prepare(GcsOriginInput {
                        endpoint: None,
                        use_ssl: None,
                        validation: OriginValidation::Routed,
                        credentials: GcsCredentialInput::Resolve {
                            keys: None,
                            options: Box::new(credentials),
                        },
                    })
                    .unwrap()
                    .build(http.clone(), http, Arc::new(NoopObserver))
                    .await,
                ),
                _ => error(
                    AzureOrigin::prepare(AzureOriginInput {
                        account: "account".into(),
                        endpoint: None,
                        endpoint_suffix: None,
                        path_style: None,
                        use_ssl: None,
                        validation: OriginValidation::Routed,
                        credentials: AzureCredentialInput::Resolve {
                            shared_key: None,
                            options: Box::new(credentials),
                        },
                    })
                    .unwrap()
                    .build(http.clone(), http, Arc::new(NoopObserver))
                    .await,
                ),
            };
            assert!(failure.contains("broker request timed out"));
            assert_eq!(start.elapsed(), Duration::from_millis(768));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn broker_exchange_uses_explicit_or_default_deadline() {
        struct HangingHttp;
        #[async_trait]
        impl HttpClient for HangingHttp {
            async fn execute(&self, _: HttpRequest) -> Result<HttpResponse, String> {
                std::future::pending().await
            }
        }
        for timeout_ms in [None, Some(17)] {
            let mut config = options(aws_request()).broker.unwrap();
            config.origin = Some(talon_core::OriginOptions {
                request_timeout_ms: timeout_ms,
                ..Default::default()
            });
            let normalized = ok(normalize_config(&config, "aws", None));
            let fetcher = BrokerFetch::<S3Credentials>::new(normalized, Arc::new(HangingHttp));
            let start = tokio::time::Instant::now();
            assert_eq!(error(fetcher.fetch().await), "broker request timed out");
            assert_eq!(
                start.elapsed(),
                Duration::from_millis(timeout_ms.unwrap_or(3_000))
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn broker_initial_acquisition_retries_three_times_and_keeps_final_context() {
        for succeeds in [false, true] {
            let mut replies = vec![
                Err("private transport".into()),
                response(503, Value::Null),
                response(200, serde_json::json!({"success":false})),
            ];
            replies.push(if succeeds {
                response(200, credentials("FOURTH", "token", &expiry_after(3600)))
            } else {
                response(429, Value::Null)
            });
            let http = http(replies);
            let start = tokio::time::Instant::now();
            let result = resolve_s3_credentials(
                None,
                options(aws_request()),
                http.clone(),
                Arc::new(NoopObserver),
            )
            .await;
            assert_eq!(http.requests.lock().unwrap().len(), 4);
            assert_eq!(start.elapsed(), Duration::from_millis(700));
            if succeeds {
                assert_eq!(ok(result).provider.current().access_key_id, "FOURTH");
            } else {
                let failure = error(result);
                assert!(failure.contains("initial broker credential fetch failed"));
                assert!(failure.contains("429"));
                assert!(!failure.contains("private"));
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn broker_refresh_is_request_driven_and_failures_retry_next_request() {
        let expiry = expiry_after(100);
        let http = http(vec![
            response(200, credentials("FIRST", "token", &expiry)),
            response(500, Value::Null),
            response(200, credentials("SECOND", "token", &expiry_after(3600))),
        ]);
        let resolved = ok(resolve_s3_credentials(
            None,
            options(aws_request()),
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await);
        let provider = resolved.provider;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(200)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            http.requests.lock().unwrap().len(),
            1,
            "idle providers must not refresh"
        );
        let backend = S3Backend::with_credentials_provider(
            S3Config::aws("us-east-1"),
            provider,
            http.clone(),
        );
        let obj = ObjectId::new(Backend::S3, "bucket", "key");
        let _ = backend.head(&obj).await;
        let _ = backend.head(&obj).await;
        let requests = http.requests.lock().unwrap();
        assert_eq!(
            requests.iter().filter(|r| r.method == Method::Post).count(),
            3
        );
        let heads: Vec<_> = requests
            .iter()
            .filter(|r| r.method == Method::Head)
            .collect();
        assert!(heads[0]
            .header("authorization")
            .unwrap()
            .contains("Credential=FIRST/"));
        assert!(heads[1]
            .header("authorization")
            .unwrap()
            .contains("Credential=SECOND/"));
    }

    #[tokio::test]
    async fn broker_pod_iam_s3_sources_sign_with_forwarded_session_credentials() {
        for (csp, region, endpoint) in [
            ("aws", "us-west-2", "s3.us-west-2.amazonaws.com"),
            ("aws", "cn-north-1", "s3.cn-north-1.amazonaws.com.cn"),
            ("ali", "cn-hangzhou", "s3.oss-cn-hangzhou.aliyuncs.com"),
            ("tc", "ap-beijing", "cos.ap-beijing.myqcloud.com"),
            ("hwc", "cn-east-3", "obs.cn-east-3.myhuaweicloud.com"),
        ] {
            let mut reply = credentials("PODAK", "pod-token", "2099-01-01T00:00:00Z");
            reply["csp"] = csp.into();
            let http = http(vec![response(200, reply)]);
            let resolved = ok(resolve_s3_credentials(
                Some(S3Credentials {
                    access_key_id: "unused-static".into(),
                    secret_access_key: "unused-secret".into(),
                    session_token: None,
                }),
                options(serde_json::json!({"credentialType":"iam","csp":csp,"region":region})),
                http.clone(),
                Arc::new(NoopObserver),
            )
            .await);
            assert_eq!(resolved.source, "broker");
            let mut origin = S3Config::aws(region);
            if csp != "aws" {
                origin.endpoint = endpoint.into();
            }
            let backend =
                S3Backend::with_credentials_provider(origin, resolved.provider, http.clone());
            let _ = backend
                .head(&ObjectId::new(Backend::S3, "bucket", "key"))
                .await;
            let requests = http.requests.lock().unwrap();
            let sent: Value = serde_json::from_slice(&requests[0].body).unwrap();
            assert_eq!(
                sent,
                serde_json::json!({"credentialType":"iam","csp":csp,"region":region,"durationSeconds":3600})
            );
            assert_eq!(requests[1].url, format!("https://bucket.{endpoint}/key"));
            assert!(requests[1]
                .header("authorization")
                .unwrap()
                .contains("Credential=PODAK/"));
            assert_eq!(
                requests[1].header("x-amz-security-token"),
                Some("pod-token")
            );
        }
    }

    #[tokio::test]
    async fn broker_pod_iam_gcp_needs_no_target_service_account() {
        let mut reply = credentials("", "pod-access-token", "2099-01-01T00:00:00Z");
        reply["credentials"]["tempSk"] = "".into();
        let http = http(vec![response(200, reply)]);
        let resolved = ok(resolve_gcs_credentials(
            None,
            options(serde_json::json!({"credentialType":"iam","csp":"gcp"})),
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await);
        assert_eq!(resolved.source, "broker");
        let backend =
            GcsBackend::with_credentials(GcsConfig::default(), resolved.provider, http.clone());
        let _ = backend
            .head(&ObjectId::new(Backend::Gcs, "bucket", "key"))
            .await;
        let requests = http.requests.lock().unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&requests[0].body).unwrap(),
            serde_json::json!({"credentialType":"iam","csp":"gcp","durationSeconds":3600})
        );
        assert_eq!(
            requests[1].header("authorization"),
            Some("Bearer pod-access-token")
        );
    }

    #[tokio::test]
    async fn broker_pod_iam_azure_uses_sas_without_target_identity_or_region() {
        let mut reply = credentials("account", "sv=2026&sig=pod-sas", "2099-01-01T00:00:00Z");
        reply["credentials"]["tempSk"] = "".into();
        let http = http(vec![response(200, reply)]);
        let resolved = ok(resolve_azure_credentials(
            "account", None,
            options(serde_json::json!({"credentialType":"iam","csp":"azure","azureAccountName":"account","bucket":"container"})),
            http.clone(), Arc::new(NoopObserver),
        ).await);
        assert_eq!(resolved.source, "broker");
        let backend = AzureBackend::with_credentials(
            AzureConfig::new("account"),
            resolved.provider,
            http.clone(),
        );
        let _ = backend
            .head(&ObjectId::new(Backend::Azure, "container", "key"))
            .await;
        let requests = http.requests.lock().unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&requests[0].body).unwrap(),
            serde_json::json!({"credentialType":"iam","csp":"azure","azureAccountName":"account","bucket":"container","durationSeconds":3600})
        );
        assert!(requests[1].url.ends_with("?sv=2026&sig=pod-sas"));
        assert!(requests[1].header("authorization").is_none());
    }

    #[test]
    fn broker_pod_iam_validation_matches_duration_and_target_identity_rules() {
        for csp in ["aws", "ali", "tc", "hwc", "gcp", "azure"] {
            let backend = if matches!(csp, "ali" | "tc" | "hwc") {
                "aws"
            } else {
                csp
            };
            let request = serde_json::json!({"credentialType":"iam","csp":csp,"region":"us-west-2","azureAccountName":"account","bucket":"container"});
            for duration in [-1, 0, 1, 899, 900, 3600, 3601] {
                let mut config = options(request.clone()).broker.unwrap();
                config.request["durationSeconds"] = duration.into();
                let normalized = normalize_config(&config, backend, Some("account"));
                let valid = duration == 0
                    || ((1..=3600).contains(&duration)
                        && (!matches!(csp, "aws" | "ali") || duration >= 900));
                assert_eq!(normalized.is_ok(), valid, "csp={csp}, duration={duration}");
                if duration == 0 {
                    assert_eq!(ok(normalized).request["durationSeconds"], 3600);
                }
            }
            for field in [
                "roleArn",
                "externalId",
                "gcpServiceAccountEmail",
                "azureClientId",
                "azureTenantId",
            ] {
                let baseline = options(request.clone()).broker.unwrap();
                let expected = ok(normalize_config(&baseline, backend, Some("account")));
                for value in [
                    serde_json::json!("private-target-value"),
                    serde_json::json!(""),
                    Value::Null,
                    serde_json::json!(false),
                    serde_json::json!(42),
                    serde_json::json!({"nested": "ignored"}),
                    serde_json::json!(["ignored"]),
                ] {
                    let mut config = baseline.clone();
                    config.request[field] = value;
                    assert_eq!(
                        ok(normalize_config(&config, backend, Some("account"))),
                        expected,
                        "IAM must ignore {field} for {csp}"
                    );
                }
            }
        }
        for region in ["", "us_west_2", "Us-west-2", "us--west", "us-west/secret"] {
            let config =
                options(serde_json::json!({"credentialType":"iam","csp":"aws","region":region}))
                    .broker
                    .unwrap();
            assert!(normalize_config(&config, "aws", None).is_err());
        }
    }

    #[test]
    fn broker_credential_type_is_explicit_and_legacy_aliases_share_configuration() {
        let baseline = options(aws_request()).broker.unwrap();
        let expected = ok(normalize_config(&baseline, "aws", None));
        for mode in ["", "stsAssumeRole"] {
            let mut config = baseline.clone();
            config.request["credentialType"] = mode.into();
            assert_eq!(
                ok(normalize_config(&config, "aws", None)).request,
                expected.request
            );
        }
        for mode in [
            serde_json::json!("IAM"),
            serde_json::json!("private-invalid-mode"),
            serde_json::json!(false),
            Value::Null,
        ] {
            let mut config = baseline.clone();
            config.request["credentialType"] = mode;
            let failure = error(normalize_config(&config, "aws", None));
            assert!(!failure.contains("private-invalid-mode"));
        }
        let mut config = baseline.clone();
        config.request.as_object_mut().unwrap().remove("roleArn");
        assert!(
            normalize_config(&config, "aws", None).is_err(),
            "missing target identity must never select Pod IAM implicitly"
        );
        for csp in ["ali", "tc", "hwc"] {
            let mut config = baseline.clone();
            config.request["csp"] = csp.into();
            assert!(
                normalize_config(&config, "aws", None).is_err(),
                "legacy Broker mode only supports AWS S3 credentials"
            );
        }
    }

    #[tokio::test]
    async fn broker_aws_posts_documented_fields_and_signs_storage_requests() {
        let http = http(vec![response(
            200,
            credentials("BROKERAK", "broker-token", "2099-01-01T08:00:00.123+08:00"),
        )]);
        let resolved = ok(resolve_s3_credentials(
            None,
            options(aws_request()),
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await);
        let backend = S3Backend::with_credentials_provider(
            S3Config::aws("us-east-1"),
            resolved.provider,
            http.clone(),
        );
        let _ = backend
            .head(&ObjectId::new(Backend::S3, "bucket", "key"))
            .await;
        let requests = http.requests.lock().unwrap();
        assert_eq!(
            requests[0].url,
            "https://explicit-broker.example/v1/credentials?key=url-secret"
        );
        assert_eq!(requests[0].method, Method::Post);
        assert_eq!(requests[0].header("content-type"), Some("application/json"));
        assert_eq!(
            serde_json::from_slice::<Value>(&requests[0].body).unwrap(),
            serde_json::json!({"csp":"aws","region":"us-east-1","roleArn":"arn:aws:iam::123456789012:role/example","externalId":"external-example","durationSeconds":3600,"bucket":"bucket","cluster":"cluster-example"})
        );
        assert!(requests[1]
            .header("authorization")
            .unwrap()
            .contains("Credential=BROKERAK/"));
        assert_eq!(
            requests[1].header("x-amz-security-token"),
            Some("broker-token")
        );
    }

    #[tokio::test]
    async fn broker_preserves_absolute_rfc3339_offset_and_fractional_expiry() {
        let fetcher = ok(BrokerFetch::<S3Credentials>::from_env(
            &env(aws_request()),
            "aws",
            None,
            http(vec![response(
                200,
                credentials("AK", "token", "2099-01-01T08:00:00.123+08:00"),
            )]),
        ));
        let (_, expiry) = ok(fetcher.fetch().await);
        assert_eq!(
            expiry,
            Some(SystemTime::UNIX_EPOCH + Duration::from_millis(4_070_908_800_123))
        );
    }

    #[tokio::test]
    async fn broker_environment_does_not_trigger_implicit_acquisition() {
        let http = http(vec![]);
        let resolved = ok(resolve_s3_credentials_with_env(
            &env(aws_request()),
            Some(S3Credentials {
                access_key_id: "STATIC".into(),
                secret_access_key: "static-secret".into(),
                session_token: None,
            }),
            None,
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await);
        for _ in 0..20 {
            assert_eq!(resolved.provider.current().access_key_id, "STATIC");
        }
        assert!(http.requests.lock().unwrap().is_empty());
        let backend = S3Backend::with_credentials_provider(
            S3Config::aws("us-east-1"),
            resolved.provider,
            http.clone(),
        );
        let _ = backend
            .head(&ObjectId::new(Backend::S3, "bucket", "key"))
            .await;
        let requests = http.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, Method::Head);
        assert!(requests[0]
            .header("authorization")
            .unwrap()
            .contains("Credential=STATIC/"));
    }

    #[tokio::test]
    async fn broker_rejects_invalid_endpoint_and_request_configuration_without_echoing_it() {
        for endpoint in [
            "relative/path",
            "ftp://broker.example/secret",
            "https://user:secret@broker.example/",
            "https://broker.example/#secret",
        ] {
            let request_env = env(aws_request());
            let env = |name: &str| {
                if name == "TALON_CREDENTIAL_BROKER_URL" {
                    Some(endpoint.into())
                } else {
                    request_env(name)
                }
            };
            let error = error(BrokerFetch::<S3Credentials>::from_env(
                &env,
                "aws",
                None,
                http(vec![]),
            ));
            assert!(!error.contains("secret"));
        }
        for request in ["secret invalid JSON", "[]", "null"] {
            let request_env = env(aws_request());
            let env = |name: &str| {
                if name == "TALON_CREDENTIAL_BROKER_REQUEST" {
                    Some(request.into())
                } else {
                    request_env(name)
                }
            };
            let error = error(BrokerFetch::<S3Credentials>::from_env(
                &env,
                "aws",
                None,
                http(vec![]),
            ));
            assert!(!error.contains("secret"));
        }
        for request in [
            serde_json::json!({"csp":"gcp"}),
            serde_json::json!({"csp":"azure","azureAccountName":"account"}),
        ] {
            let csp = request["csp"].as_str().unwrap();
            assert!(BrokerFetch::<GcpBearerToken>::from_env(
                &env(request.clone()),
                csp,
                Some("account"),
                http(vec![])
            )
            .is_err());
        }
    }

    #[tokio::test]
    async fn broker_gcp_uses_bearer_instead_of_s3_credentials() {
        let http = http(vec![response(
            200,
            credentials("", "oauth-token", "2099-01-01T00:00:00Z"),
        )]);
        let request = serde_json::json!({"csp":"gcp","gcpServiceAccountEmail":"reader@example.iam.gserviceaccount.com"});
        let resolved = ok(resolve_gcs_credentials(
            Some(crate::GcsHmacCredentials {
                access_key_id: "unused-key".into(),
                secret_access_key: "unused-secret".into(),
            }),
            options(request),
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await);
        let backend =
            GcsBackend::with_credentials(GcsConfig::default(), resolved.provider, http.clone());
        let _ = backend
            .head(&ObjectId::new(Backend::Gcs, "bucket", "key"))
            .await;
        let requests = http.requests.lock().unwrap();
        assert_eq!(
            requests[0].url,
            "https://explicit-broker.example/v1/credentials?key=url-secret"
        );
        assert_eq!(requests[0].method, Method::Post);
        assert_eq!(
            requests[1].header("authorization"),
            Some("Bearer oauth-token")
        );
        assert!(requests[1].header("x-amz-security-token").is_none());
        assert_eq!(
            serde_json::from_slice::<Value>(&requests[0].body).unwrap(),
            serde_json::json!({"csp":"gcp","gcpServiceAccountEmail":"reader@example.iam.gserviceaccount.com","durationSeconds":3600})
        );
    }

    #[tokio::test(start_paused = true)]
    async fn broker_pod_iam_refresh_keeps_mode_and_last_good_snapshot() {
        let expiry = expiry_after(100);
        let http = http(vec![
            response(200, credentials("FIRST", "first-token", &expiry)),
            response(200, credentials("SECOND", "second-token", &expiry)),
            response(
                200,
                serde_json::json!({"success":false,"msg":"private-failure"}),
            ),
        ]);
        let resolved = ok(resolve_s3_credentials(
            None,
            options(serde_json::json!({"credentialType":"iam","csp":"aws","region":"us-west-2"})),
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await);
        assert_eq!(resolved.provider.current().access_key_id, "FIRST");
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(40)).await;
        tokio::task::yield_now().await;
        resolved.provider.prepare().await.unwrap();
        assert_eq!(resolved.provider.current().access_key_id, "SECOND");
        tokio::time::advance(Duration::from_secs(40)).await;
        tokio::task::yield_now().await;
        resolved.provider.prepare().await.unwrap();
        assert_eq!(resolved.provider.current().access_key_id, "SECOND");
        let requests = http.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        for request in requests.iter() {
            assert_eq!(
                serde_json::from_slice::<Value>(&request.body).unwrap(),
                serde_json::json!({"credentialType":"iam","csp":"aws","region":"us-west-2","durationSeconds":3600})
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn broker_rotates_cached_credentials_and_retains_last_good_signature() {
        let expiry = expiry_after(100);
        let http = http(vec![
            response(200, credentials("FIRST", "first-token", &expiry)),
            response(200, credentials("SECOND", "second-token", &expiry)),
            response(
                200,
                serde_json::json!({"success":false,"msg":"secret-value"}),
            ),
        ]);
        let mut options = options(aws_request());
        let resolved =
            ok(
                resolve_s3_credentials(None, options.clone(), http.clone(), Arc::new(NoopObserver))
                    .await,
            );
        // A provider owns its original configuration throughout refresh.
        let config = options.broker.as_mut().unwrap();
        config.url = "https://replacement-broker.example/".into();
        config.request["roleArn"] = "replacement-role".into();
        let backend = S3Backend::with_credentials_provider(
            S3Config::aws("us-east-1"),
            resolved.provider,
            http.clone(),
        );
        let obj = ObjectId::new(Backend::S3, "bucket", "key");
        let _ = backend.head(&obj).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(40)).await;
        tokio::task::yield_now().await;
        let _ = backend.head(&obj).await;
        tokio::time::advance(Duration::from_secs(40)).await;
        tokio::task::yield_now().await;
        let _ = backend.head(&obj).await;
        let requests = http.requests.lock().unwrap();
        let heads: Vec<_> = requests
            .iter()
            .filter(|r| r.method == Method::Head)
            .collect();
        assert!(heads[0]
            .header("authorization")
            .unwrap()
            .contains("Credential=FIRST/"));
        assert!(heads[1]
            .header("authorization")
            .unwrap()
            .contains("Credential=SECOND/"));
        assert!(heads[2]
            .header("authorization")
            .unwrap()
            .contains("Credential=SECOND/"));
        // Tokio controls when the next request sees refresh due; SystemTime
        // stays on the real clock for parsing the Broker's absolute expiry.
        assert_eq!(
            requests.iter().filter(|r| r.method == Method::Post).count(),
            3
        );
        for request in requests.iter().filter(|r| r.method == Method::Post) {
            assert_eq!(
                request.url,
                "https://explicit-broker.example/v1/credentials?key=url-secret"
            );
            assert_eq!(
                serde_json::from_slice::<Value>(&request.body).unwrap(),
                serde_json::json!({"csp":"aws","region":"us-east-1","roleArn":"arn:aws:iam::123456789012:role/example","externalId":"external-example","durationSeconds":3600,"bucket":"bucket","cluster":"cluster-example"})
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn broker_azure_rotation_updates_read_write_copy_and_listing_urls() {
        let expiry = expiry_after(100);
        let http = http(vec![
            response(200, credentials("account", "sv=2026&sig=first", &expiry)),
            response(200, credentials("account", "sv=2026&sig=second", &expiry)),
        ]);
        let resolved = ok(resolve_azure_credentials(
            "account",
            None,
            options(azure_request()),
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await);
        let backend = AzureBackend::with_credentials(
            AzureConfig::new("account"),
            resolved.provider,
            http.clone(),
        );
        let obj = ObjectId::new(Backend::Azure, "container", "key");
        let _ = backend.execute_head_raw(&obj, &[]).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(40)).await;
        tokio::task::yield_now().await;
        let _ = backend.execute_head_raw(&obj, &[]).await;
        let _ = backend
            .execute_get_stream_raw(&obj, Some((0, 2)), &[])
            .await;
        let _ = backend.execute_put_raw(&obj, "comp=metadata", &[]).await;
        let _ = backend.execute_copy_raw(&obj, &obj, &[]).await;
        let _ = backend
            .execute_list_raw("container", "", None, None, 10, &[])
            .await;
        let requests = http.requests.lock().unwrap();
        let storage: Vec<_> = requests
            .iter()
            .filter(|r| r.method != Method::Post)
            .collect();
        assert!(storage[0].url.ends_with("sig=first"));
        for request in &storage[1..] {
            assert!(request
                .url
                .starts_with("https://account.blob.core.windows.net/container"));
            assert!(request.url.contains("sig=second"));
            assert_eq!(request.url.matches('?').count(), 1);
            assert!(request.header("authorization").is_none());
        }
        assert!(storage[4]
            .header("x-ms-copy-source")
            .unwrap()
            .ends_with("sig=second"));
        assert_eq!(
            requests[0].url,
            "https://explicit-broker.example/v1/credentials?key=url-secret"
        );
        assert_eq!(requests[0].method, Method::Post);
        assert_eq!(
            serde_json::from_slice::<Value>(&requests[0].body).unwrap(),
            serde_json::json!({"csp":"azure","region":"eastus","bucket":"container","azureAccountName":"account","azureClientId":"client","azureTenantId":"tenant","durationSeconds":3600})
        );
    }

    #[tokio::test]
    async fn explicit_broker_validates_configuration_and_suppresses_secrets() {
        for (url, request) in [
            ("https://user:url-secret@broker.example/", aws_request()),
            ("https://broker.example/?key=url-secret", Value::Null),
            (
                "https://broker.example/?key=url-secret",
                serde_json::json!({"csp":"request-secret"}),
            ),
        ] {
            let http = http(vec![]);
            let error = error(
                resolve_s3_credentials(
                    None,
                    CredentialOptions {
                        source: Some("broker".into()),
                        broker: Some(BrokerConfig {
                            origin: None,
                            url: url.into(),
                            request,
                        }),
                        ..CredentialOptions::default()
                    },
                    http.clone(),
                    Arc::new(NoopObserver),
                )
                .await,
            );
            assert!(!error.contains("url-secret"));
            assert!(!error.contains("request-secret"));
            assert!(http.requests.lock().unwrap().is_empty());
        }
        let account_http = http(vec![]);
        let account_error = error(
            resolve_azure_credentials(
                "different",
                None,
                options(azure_request()),
                account_http.clone(),
                Arc::new(NoopObserver),
            )
            .await,
        );
        assert!(account_error.contains("account"));
        assert!(account_http.requests.lock().unwrap().is_empty());
        let error = error(
            resolve_s3_credentials(
                None,
                options(aws_request()),
                http(vec![Err(
                    "https://broker.example/?key=url-secret request-secret returned-secret".into(),
                )]),
                Arc::new(NoopObserver),
            )
            .await,
        );
        for secret in ["url-secret", "request-secret", "returned-secret"] {
            assert!(!error.contains(secret));
        }
    }

    #[tokio::test]
    async fn broker_azure_rejects_empty_required_sas_values_at_acquisition() {
        for token in [
            "sv=2026&sig=",
            "sv=&sig=secret-token",
            "sv=2026&sig=%20",
            "sv=2026&sig=secret-token&sig=",
        ] {
            let error = error(
                resolve_azure_broker_sas_with_env(
                    &env(azure_request()),
                    "account",
                    http(vec![response(
                        200,
                        credentials("account", token, "2099-01-01T00:00:00Z"),
                    )]),
                    Arc::new(NoopObserver),
                )
                .await,
            );
            assert!(!error.contains(token));
            assert!(!error.contains("secret-token"));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn broker_azure_invalid_sas_refresh_retains_prior_sas_on_executed_requests() {
        let expiry = expiry_after(100);
        let http = http(vec![
            response(200, credentials("account", "sv=2026&sig=first", &expiry)),
            response(200, credentials("account", "sv=2026&sig=", &expiry)),
            response(
                200,
                credentials("account", "sv=&sig=invalid-secret", &expiry),
            ),
        ]);
        let provider = ok(resolve_azure_broker_sas_with_env(
            &env(azure_request()),
            "account",
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await);
        let backend =
            AzureBackend::with_sas_provider(AzureConfig::new("account"), provider, http.clone());
        let obj = ObjectId::new(Backend::Azure, "container", "key");
        ok(backend.execute_head_raw(&obj, &[]).await);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(40)).await;
        tokio::task::yield_now().await;
        ok(backend.execute_head_raw(&obj, &[]).await);
        ok(backend.execute_head_raw(&obj, &[]).await);
        let requests = http.requests.lock().unwrap();
        let storage: Vec<_> = requests
            .iter()
            .filter(|r| r.method == Method::Head)
            .collect();
        assert_eq!(storage.len(), 3);
        for request in storage {
            assert_eq!(
                request.url,
                "https://account.blob.core.windows.net/container/key?sv=2026&sig=first"
            );
        }
        assert_eq!(
            requests.iter().filter(|r| r.method == Method::Post).count(),
            3
        );
    }

    #[tokio::test]
    async fn broker_rejects_invalid_responses_without_secret_data_in_errors() {
        let valid = credentials("AK", "secret-token", "2099-01-01T00:00:00Z");
        let mut replies = vec![
            response(403, valid.clone()),
            response(
                200,
                serde_json::json!({"success":false,"msg":"secret-token"}),
            ),
            response(
                200,
                serde_json::json!({"credentials": valid["credentials"]}),
            ),
            Err("secret-token transport detail".into()),
            Ok(HttpResponse {
                status: 200,
                headers: vec![],
                body: bytes::Bytes::from_static(b"secret-token invalid-json"),
            }),
        ];
        for field in ["tempAk", "tempSk", "sessionToken", "expiredAt"] {
            for invalid in [Value::Null, Value::String(String::new())] {
                let mut body = valid.clone();
                body["credentials"][field] = invalid;
                replies.push(response(200, body));
            }
        }
        for expiry in [
            "not-a-date-secret-token",
            "2026-02-30T00:00:00Z",
            "2000-01-01T00:00:00Z",
            "2099-01-01T25:00:00Z",
            "2099-01-01T00:00:00+25:00",
        ] {
            replies.push(response(200, credentials("AK", "secret-token", expiry)));
        }
        let mut wrong_csp = valid.clone();
        wrong_csp["csp"] = "azure".into();
        replies.push(response(200, wrong_csp));
        for reply in replies {
            let fetcher = ok(BrokerFetch::<S3Credentials>::from_env(
                &env(aws_request()),
                "aws",
                None,
                http(vec![reply]),
            ));
            let error = error(fetcher.fetch().await);
            assert!(!error.contains("secret-token"));
            assert!(!error.contains("secret-value"));
            assert!(!error.contains("AK"));
        }
    }

    #[tokio::test]
    async fn broker_validates_requests_accounts_and_duration_before_http() {
        for csp in ["gcp", "azure", "AWS", ""] {
            let mut request = aws_request();
            request["csp"] = csp.into();
            assert!(BrokerFetch::<S3Credentials>::from_env(
                &env(request),
                "aws",
                None,
                http(vec![])
            )
            .is_err());
        }
        for field in ["region", "roleArn"] {
            let mut request = aws_request();
            request[field] = "".into();
            assert!(BrokerFetch::<S3Credentials>::from_env(
                &env(request),
                "aws",
                None,
                http(vec![])
            )
            .is_err());
        }
        for duration in [1, 899, 3601] {
            let mut request = aws_request();
            request["durationSeconds"] = duration.into();
            assert!(BrokerFetch::<S3Credentials>::from_env(
                &env(request),
                "aws",
                None,
                http(vec![])
            )
            .is_err());
        }
        for (duration, want) in [(0, 3600), (-1, 3600), (900, 900), (3600, 3600)] {
            let mut request = aws_request();
            request["durationSeconds"] = duration.into();
            let http = http(vec![response(
                200,
                credentials("AK", "token", "2099-01-01T00:00:00Z"),
            )]);
            let fetcher = ok(BrokerFetch::<S3Credentials>::from_env(
                &env(request),
                "aws",
                None,
                http.clone(),
            ));
            ok(fetcher.fetch().await);
            let sent: Value =
                serde_json::from_slice(&http.requests.lock().unwrap()[0].body).unwrap();
            assert_eq!(sent["durationSeconds"], want);
        }
        assert!(resolve_azure_broker_sas_with_env(
            &env(azure_request()),
            "different",
            http(vec![]),
            Arc::new(NoopObserver)
        )
        .await
        .is_err());
        let http = http(
            (0..4)
                .map(|_| {
                    response(
                        200,
                        credentials(
                            "different",
                            "sv=2026&sig=secret-token",
                            "2099-01-01T00:00:00Z",
                        ),
                    )
                })
                .collect(),
        );
        let error = error(
            resolve_azure_broker_sas_with_env(
                &env(azure_request()),
                "account",
                http,
                Arc::new(NoopObserver),
            )
            .await,
        );
        assert!(!error.contains("secret-token"));
        assert!(error.contains("account"));
    }

    #[tokio::test(start_paused = true)]
    async fn broker_times_out_a_stalled_exchange_after_3_seconds() {
        struct Stalled;
        #[async_trait]
        impl HttpClient for Stalled {
            async fn execute(&self, _: HttpRequest) -> Result<HttpResponse, String> {
                std::future::pending().await
            }
        }
        let fetcher = ok(BrokerFetch::<S3Credentials>::from_env(
            &env(aws_request()),
            "aws",
            None,
            Arc::new(Stalled),
        ));
        let started = tokio::time::Instant::now();
        let error = error(fetcher.fetch().await);
        assert!(error.contains("timed out"));
        assert_eq!(started.elapsed(), Duration::from_secs(3));
    }
}

#[cfg(test)]
#[path = "broker_live_tests.rs"]
mod live_tests;
