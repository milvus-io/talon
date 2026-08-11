//! Cloud workload-identity credential acquisition.
//!
//! Each cloud's Kubernetes platform hands a pod an identity instead of a
//! static key: EKS IRSA and its analogues inject a token file plus role
//! coordinates through env variables, and GKE/CCE expose a metadata endpoint.
//! The fetchers here exchange that platform identity for short-lived object
//! store credentials using plain HTTP against documented endpoints — no cloud
//! SDKs — so every exchange is offline-testable through [`HttpClient`].
//!
//! Verification status: the AWS contract is confirmed against a live EKS
//! deployment; Aliyun, Tencent, and Huawei mirror the contracts used by the
//! Milvus object-storage providers and remain to be validated against live
//! clusters. Fetch errors carry the HTTP status and a bounded provider error
//! code, never token or key material.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::credentials::{
    CredentialsFetch, CredentialsObserver, ProvideBearerToken, ProvideS3Credentials, RefreshPolicy,
    RefreshingCredentials, StaticBearerToken, StaticS3Credentials,
};
use crate::http::{HttpClient, HttpRequest, Method};
use crate::s3::S3Credentials;

/// Environment lookup used by source resolution, injectable for tests.
pub type EnvFn<'a> = &'a dyn Fn(&str) -> Option<String>;

fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn read_token(path: &PathBuf) -> Result<String, String> {
    std::fs::read_to_string(path)
        .map(|token| token.trim().to_string())
        .map_err(|error| format!("identity token file is unreadable: {error}"))
}

fn form_body(pairs: &[(&str, &str)]) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (name, value) in pairs {
        serializer.append_pair(name, value);
    }
    serializer.finish()
}

fn json_body(body: &[u8]) -> Result<serde_json::Value, String> {
    serde_json::from_slice(body).map_err(|_| "response is not valid JSON".to_string())
}

/// Bounded provider error identifier from a JSON error body, if present.
fn json_error_code(body: &[u8], pointers: &[&str]) -> Option<String> {
    let value = serde_json::from_slice::<serde_json::Value>(body).ok()?;
    pointers
        .iter()
        .find_map(|pointer| value.pointer(pointer))
        .and_then(|code| code.as_str())
        .map(|code| code.chars().take(64).collect())
}

fn exchange_error(mechanism: &str, status: u16, code: Option<String>) -> String {
    match code {
        Some(code) => format!("{mechanism} exchange returned status {status} ({code})"),
        None => format!("{mechanism} exchange returned status {status}"),
    }
}

// --- UTC calendar helpers -----------------------------------------------

/// Civil (year, month, day) → days since 1970-01-01. Howard Hinnant's
/// algorithm, the inverse of `sigv4::civil_from_days`.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let mp = ((m as u64) + 9) % 12;
    let doy = (153 * mp + 2) / 5 + (d as u64) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i64 - 719_468
}

/// Parse `YYYY-MM-DDTHH:MM:SS[.fraction](Z|+00:00)` into a [`SystemTime`].
///
/// This is the only timestamp shape the exchanges emit; fractional seconds
/// are truncated. Anything else yields `None` and the caller treats the
/// expiry as unknown rather than failing the exchange.
pub(crate) fn parse_iso8601_utc(text: &str) -> Option<SystemTime> {
    let text = text.trim();
    let digits = |range: std::ops::Range<usize>| -> Option<i64> {
        let slice = text.get(range)?;
        if slice.bytes().all(|byte| byte.is_ascii_digit()) {
            slice.parse().ok()
        } else {
            None
        }
    };
    if text.len() < 20 || text.get(4..5)? != "-" || text.get(7..8)? != "-" {
        return None;
    }
    let time_separator = text.get(10..11)?;
    if time_separator != "T" || text.get(13..14)? != ":" || text.get(16..17)? != ":" {
        return None;
    }
    let tail = text.get(19..)?;
    if !(tail == "Z" || tail == "+00:00" || (tail.starts_with('.') && tail.ends_with('Z'))) {
        return None;
    }
    let (year, month, day) = (digits(0..4)?, digits(5..7)?, digits(8..10)?);
    let (hour, minute, second) = (digits(11..13)?, digits(14..16)?, digits(17..19)?);
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let days = days_from_civil(year, month as u32, day as u32);
    let seconds = days
        .checked_mul(86_400)?
        .checked_add(hour * 3600 + minute * 60 + second)?;
    u64::try_from(seconds)
        .ok()
        .map(|seconds| UNIX_EPOCH + Duration::from_secs(seconds))
}

/// Format a [`SystemTime`] as `YYYY-MM-DDTHH:MM:SSZ` (Aliyun's `Timestamp`
/// common parameter).
fn format_iso8601_utc(time: SystemTime) -> String {
    let seconds = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let (year, month, day) = crate::sigv4::civil_from_days((seconds / 86_400) as i64);
    let second_of_day = seconds % 86_400;
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        second_of_day / 3600,
        (second_of_day % 3600) / 60,
        second_of_day % 60
    )
}

fn expires_in_seconds(now: SystemTime, seconds: u64) -> SystemTime {
    now + Duration::from_secs(seconds)
}

static NONCE: AtomicU64 = AtomicU64::new(0);

fn signature_nonce() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("talon-{nanos}-{}", NONCE.fetch_add(1, Ordering::Relaxed))
}

// --- AWS ------------------------------------------------------------------

/// EKS IRSA: exchange the projected service-account token for temporary
/// credentials through `sts:AssumeRoleWithWebIdentity` (plain form POST,
/// unsigned, XML response).
pub struct AwsWebIdentity {
    role_arn: String,
    token_file: PathBuf,
    session_name: String,
    endpoint: String,
    http: Arc<dyn HttpClient>,
}

impl AwsWebIdentity {
    /// Build from the env contract the EKS pod-identity webhook injects.
    /// Returns `None` when the contract is absent.
    pub fn from_env(env: EnvFn<'_>, http: Arc<dyn HttpClient>) -> Option<Self> {
        let role_arn = env("AWS_ROLE_ARN")?;
        let token_file = PathBuf::from(env("AWS_WEB_IDENTITY_TOKEN_FILE")?);
        let region = env("AWS_REGION").or_else(|| env("AWS_DEFAULT_REGION"));
        // `regional` is what EKS injects; only an explicit `legacy` (or a
        // missing region) falls back to the global endpoint.
        let endpoint = match (region, env("AWS_STS_REGIONAL_ENDPOINTS").as_deref()) {
            (Some(region), mode) if mode != Some("legacy") => {
                format!("https://sts.{region}.amazonaws.com")
            }
            _ => "https://sts.amazonaws.com".to_string(),
        };
        Some(Self {
            role_arn,
            token_file,
            session_name: env("AWS_ROLE_SESSION_NAME").unwrap_or_else(|| "talon".to_string()),
            endpoint,
            http,
        })
    }
}

#[async_trait]
impl CredentialsFetch for AwsWebIdentity {
    type Value = S3Credentials;

    async fn fetch(&self) -> Result<(S3Credentials, Option<SystemTime>), String> {
        let token = read_token(&self.token_file)?;
        let body = form_body(&[
            ("Action", "AssumeRoleWithWebIdentity"),
            ("Version", "2011-06-15"),
            ("RoleArn", &self.role_arn),
            ("RoleSessionName", &self.session_name),
            ("WebIdentityToken", &token),
            ("DurationSeconds", "3600"),
        ]);
        let request = HttpRequest::with_body(
            Method::Post,
            self.endpoint.clone(),
            vec![(
                "content-type".to_string(),
                "application/x-www-form-urlencoded".to_string(),
            )],
            bytes::Bytes::from(body),
        );
        let response = self.http.execute(request).await?;
        let text = String::from_utf8_lossy(&response.body);
        if !response.is_success() {
            let code = crate::xml::element(&text, "Code").map(|code| code.to_string());
            return Err(exchange_error(self.source(), response.status, code));
        }
        let field = |tag: &str| {
            crate::xml::element(&text, tag)
                .map(crate::xml::unescape)
                .ok_or_else(|| format!("STS response is missing <{tag}>"))
        };
        let credentials = S3Credentials {
            access_key_id: field("AccessKeyId")?,
            secret_access_key: field("SecretAccessKey")?,
            session_token: Some(field("SessionToken")?),
        };
        let expires_at = field("Expiration").ok().and_then(|t| parse_iso8601_utc(&t));
        Ok((credentials, expires_at))
    }

    fn source(&self) -> &'static str {
        "aws-web-identity"
    }
}

// --- Aliyun -----------------------------------------------------------------

/// ACK RRSA: `sts:AssumeRoleWithOIDC` (anonymous RPC-style form POST, JSON
/// response).
pub struct AliyunOidc {
    role_arn: String,
    provider_arn: String,
    token_file: PathBuf,
    session_name: String,
    endpoint: String,
    http: Arc<dyn HttpClient>,
}

impl AliyunOidc {
    /// Build from the env contract the ACK RRSA webhook injects.
    pub fn from_env(env: EnvFn<'_>, http: Arc<dyn HttpClient>) -> Option<Self> {
        Some(Self {
            role_arn: env("ALIBABA_CLOUD_ROLE_ARN")?,
            provider_arn: env("ALIBABA_CLOUD_OIDC_PROVIDER_ARN")?,
            token_file: PathBuf::from(env("ALIBABA_CLOUD_OIDC_TOKEN_FILE")?),
            session_name: env("ALIBABA_CLOUD_ROLE_SESSION_NAME")
                .unwrap_or_else(|| "talon".to_string()),
            endpoint: env("ALIBABA_CLOUD_STS_ENDPOINT")
                .map(|host| format!("https://{}", host.trim_start_matches("https://")))
                .unwrap_or_else(|| "https://sts.aliyuncs.com".to_string()),
            http,
        })
    }
}

#[async_trait]
impl CredentialsFetch for AliyunOidc {
    type Value = S3Credentials;

    async fn fetch(&self) -> Result<(S3Credentials, Option<SystemTime>), String> {
        let token = read_token(&self.token_file)?;
        let timestamp = format_iso8601_utc(SystemTime::now());
        let nonce = signature_nonce();
        let body = form_body(&[
            ("Action", "AssumeRoleWithOIDC"),
            ("Version", "2015-04-01"),
            ("Format", "JSON"),
            ("Timestamp", &timestamp),
            ("SignatureNonce", &nonce),
            ("RoleArn", &self.role_arn),
            ("OIDCProviderArn", &self.provider_arn),
            ("OIDCToken", &token),
            ("RoleSessionName", &self.session_name),
            ("DurationSeconds", "3600"),
        ]);
        let request = HttpRequest::with_body(
            Method::Post,
            self.endpoint.clone(),
            vec![(
                "content-type".to_string(),
                "application/x-www-form-urlencoded".to_string(),
            )],
            bytes::Bytes::from(body),
        );
        let response = self.http.execute(request).await?;
        if !response.is_success() {
            let code = json_error_code(&response.body, &["/Code"]);
            return Err(exchange_error(self.source(), response.status, code));
        }
        let value = json_body(&response.body)?;
        let credential = value
            .pointer("/Credentials")
            .ok_or("STS response is missing Credentials")?;
        let field = |name: &str| {
            credential
                .pointer(&format!("/{name}"))
                .and_then(|field| field.as_str())
                .map(str::to_string)
                .ok_or_else(|| format!("STS response is missing Credentials.{name}"))
        };
        let credentials = S3Credentials {
            access_key_id: field("AccessKeyId")?,
            secret_access_key: field("AccessKeySecret")?,
            session_token: Some(field("SecurityToken")?),
        };
        let expires_at = field("Expiration").ok().and_then(|t| parse_iso8601_utc(&t));
        Ok((credentials, expires_at))
    }

    fn source(&self) -> &'static str {
        "aliyun-oidc"
    }
}

// --- Tencent ----------------------------------------------------------------

/// TKE OIDC: `sts:AssumeRoleWithWebIdentity` (signature-exempt JSON POST with
/// `Authorization: SKIP`, mirroring the Tencent SDK's TKE provider).
pub struct TencentOidc {
    role_arn: String,
    provider_id: String,
    token_file: PathBuf,
    session_name: String,
    region: String,
    endpoint: String,
    http: Arc<dyn HttpClient>,
}

impl TencentOidc {
    /// Build from the env contract the TKE OIDC webhook injects.
    pub fn from_env(env: EnvFn<'_>, http: Arc<dyn HttpClient>) -> Option<Self> {
        Some(Self {
            role_arn: env("TKE_ROLE_ARN")?,
            provider_id: env("TKE_PROVIDER_ID")?,
            token_file: PathBuf::from(env("TKE_WEB_IDENTITY_TOKEN_FILE")?),
            session_name: env("TKE_ROLE_SESSION_NAME").unwrap_or_else(|| "talon".to_string()),
            region: env("TKE_REGION")
                .or_else(|| env("TENCENTCLOUD_REGION"))
                .unwrap_or_else(|| "ap-guangzhou".to_string()),
            endpoint: env("TENCENTCLOUD_STS_ENDPOINT")
                .map(|host| format!("https://{}", host.trim_start_matches("https://")))
                .unwrap_or_else(|| "https://sts.tencentcloudapi.com".to_string()),
            http,
        })
    }
}

#[async_trait]
impl CredentialsFetch for TencentOidc {
    type Value = S3Credentials;

    async fn fetch(&self) -> Result<(S3Credentials, Option<SystemTime>), String> {
        let token = read_token(&self.token_file)?;
        let unix_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let body = serde_json::json!({
            "ProviderId": self.provider_id,
            "WebIdentityToken": token,
            "RoleArn": self.role_arn,
            "RoleSessionName": self.session_name,
            "DurationSeconds": 3600,
        });
        let request = HttpRequest::with_body(
            Method::Post,
            self.endpoint.clone(),
            vec![
                ("content-type".to_string(), "application/json".to_string()),
                ("authorization".to_string(), "SKIP".to_string()),
                (
                    "x-tc-action".to_string(),
                    "AssumeRoleWithWebIdentity".to_string(),
                ),
                ("x-tc-version".to_string(), "2018-08-13".to_string()),
                ("x-tc-timestamp".to_string(), unix_now.to_string()),
                ("x-tc-region".to_string(), self.region.clone()),
            ],
            bytes::Bytes::from(body.to_string()),
        );
        let response = self.http.execute(request).await?;
        if !response.is_success() {
            let code = json_error_code(&response.body, &["/Response/Error/Code"]);
            return Err(exchange_error(self.source(), response.status, code));
        }
        let value = json_body(&response.body)?;
        if let Some(code) = value
            .pointer("/Response/Error/Code")
            .and_then(|code| code.as_str())
        {
            return Err(exchange_error(
                self.source(),
                response.status,
                Some(code.chars().take(64).collect()),
            ));
        }
        let field = |name: &str| {
            value
                .pointer(&format!("/Response/Credentials/{name}"))
                .and_then(|field| field.as_str())
                .map(str::to_string)
                .ok_or_else(|| format!("STS response is missing Credentials.{name}"))
        };
        let credentials = S3Credentials {
            access_key_id: field("TmpSecretId")?,
            secret_access_key: field("TmpSecretKey")?,
            session_token: Some(field("Token")?),
        };
        let expires_at = value
            .pointer("/Response/ExpiredTime")
            .and_then(|time| time.as_u64())
            .map(|seconds| UNIX_EPOCH + Duration::from_secs(seconds));
        Ok((credentials, expires_at))
    }

    fn source(&self) -> &'static str {
        "tencent-oidc"
    }
}

// --- Huawei -----------------------------------------------------------------

/// CCE agency: the node metadata service hands out temporary credentials for
/// the bound agency directly (`/openstack/latest/securitykey`).
pub struct HuaweiAgency {
    endpoint: String,
    http: Arc<dyn HttpClient>,
}

impl HuaweiAgency {
    /// Build against the standard metadata endpoint, honoring an override
    /// for tests and emulators.
    pub fn from_env(env: EnvFn<'_>, http: Arc<dyn HttpClient>) -> Self {
        Self {
            endpoint: env("HUAWEICLOUD_METADATA_ENDPOINT")
                .unwrap_or_else(|| "http://169.254.169.254".to_string()),
            http,
        }
    }
}

#[async_trait]
impl CredentialsFetch for HuaweiAgency {
    type Value = S3Credentials;

    async fn fetch(&self) -> Result<(S3Credentials, Option<SystemTime>), String> {
        let request = HttpRequest::new(
            Method::Get,
            format!("{}/openstack/latest/securitykey", self.endpoint),
            Vec::new(),
        );
        let response = self.http.execute(request).await?;
        if !response.is_success() {
            return Err(exchange_error(self.source(), response.status, None));
        }
        let value = json_body(&response.body)?;
        let field = |name: &str| {
            value
                .pointer(&format!("/credential/{name}"))
                .and_then(|field| field.as_str())
                .map(str::to_string)
                .ok_or_else(|| format!("metadata response is missing credential.{name}"))
        };
        let credentials = S3Credentials {
            access_key_id: field("access")?,
            secret_access_key: field("secret")?,
            session_token: Some(field("securitytoken")?),
        };
        let expires_at = field("expires_at").ok().and_then(|t| parse_iso8601_utc(&t));
        Ok((credentials, expires_at))
    }

    fn source(&self) -> &'static str {
        "huawei-agency"
    }
}

// --- GCP ----------------------------------------------------------------------

/// GKE Workload Identity: the metadata server mints OAuth2 access tokens for
/// the pod's bound service account; GCS accepts them as `Bearer`.
pub struct GcpMetadataToken {
    endpoint: String,
    http: Arc<dyn HttpClient>,
}

impl GcpMetadataToken {
    /// Build against the GKE metadata server, honoring the standard
    /// `GCE_METADATA_HOST` override.
    pub fn from_env(env: EnvFn<'_>, http: Arc<dyn HttpClient>) -> Self {
        let host =
            env("GCE_METADATA_HOST").unwrap_or_else(|| "metadata.google.internal".to_string());
        Self {
            endpoint: format!(
                "http://{host}/computeMetadata/v1/instance/service-accounts/default/token"
            ),
            http,
        }
    }
}

#[async_trait]
impl CredentialsFetch for GcpMetadataToken {
    type Value = String;

    async fn fetch(&self) -> Result<(String, Option<SystemTime>), String> {
        let request = HttpRequest::new(
            Method::Get,
            self.endpoint.clone(),
            vec![("metadata-flavor".to_string(), "Google".to_string())],
        );
        let response = self.http.execute(request).await?;
        if !response.is_success() {
            return Err(exchange_error(self.source(), response.status, None));
        }
        let value = json_body(&response.body)?;
        let token = value
            .pointer("/access_token")
            .and_then(|token| token.as_str())
            .ok_or("metadata response is missing access_token")?
            .to_string();
        let expires_at = value
            .pointer("/expires_in")
            .and_then(|seconds| seconds.as_u64())
            .map(|seconds| expires_in_seconds(SystemTime::now(), seconds));
        Ok((token, expires_at))
    }

    fn source(&self) -> &'static str {
        "gcp-metadata"
    }
}

// --- Azure ----------------------------------------------------------------------

/// AKS Workload Identity: exchange the projected federated token for an
/// Azure AD access token scoped to Azure Storage.
pub struct AzureWorkloadIdentity {
    client_id: String,
    token_url: String,
    token_file: PathBuf,
    http: Arc<dyn HttpClient>,
}

impl AzureWorkloadIdentity {
    /// Build from the env contract the AKS workload-identity webhook injects.
    pub fn from_env(env: EnvFn<'_>, http: Arc<dyn HttpClient>) -> Option<Self> {
        let tenant = env("AZURE_TENANT_ID")?;
        let authority = env("AZURE_AUTHORITY_HOST")
            .unwrap_or_else(|| "https://login.microsoftonline.com/".to_string());
        Some(Self {
            client_id: env("AZURE_CLIENT_ID")?,
            token_file: PathBuf::from(env("AZURE_FEDERATED_TOKEN_FILE")?),
            token_url: format!(
                "{}/{tenant}/oauth2/v2.0/token",
                authority.trim_end_matches('/')
            ),
            http,
        })
    }
}

#[async_trait]
impl CredentialsFetch for AzureWorkloadIdentity {
    type Value = String;

    async fn fetch(&self) -> Result<(String, Option<SystemTime>), String> {
        let assertion = read_token(&self.token_file)?;
        let body = form_body(&[
            ("client_id", &self.client_id),
            ("grant_type", "client_credentials"),
            ("scope", "https://storage.azure.com/.default"),
            (
                "client_assertion_type",
                "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            ),
            ("client_assertion", &assertion),
        ]);
        let request = HttpRequest::with_body(
            Method::Post,
            self.token_url.clone(),
            vec![(
                "content-type".to_string(),
                "application/x-www-form-urlencoded".to_string(),
            )],
            bytes::Bytes::from(body),
        );
        let response = self.http.execute(request).await?;
        if !response.is_success() {
            let code = json_error_code(&response.body, &["/error"]);
            return Err(exchange_error(self.source(), response.status, code));
        }
        let value = json_body(&response.body)?;
        let token = value
            .pointer("/access_token")
            .and_then(|token| token.as_str())
            .ok_or("token response is missing access_token")?
            .to_string();
        let expires_at = value
            .pointer("/expires_in")
            .and_then(|seconds| seconds.as_u64())
            .map(|seconds| expires_in_seconds(SystemTime::now(), seconds));
        Ok((token, expires_at))
    }

    fn source(&self) -> &'static str {
        "azure-workload-identity"
    }
}

// --- Source resolution ----------------------------------------------------------

/// A resolved provider plus the bounded source label for startup logs.
pub struct ResolvedS3Credentials {
    /// Snapshot source installed into the backend.
    pub provider: Arc<dyn ProvideS3Credentials>,
    /// Which mechanism produced it (`static`, `aws-web-identity`, ...).
    pub source: &'static str,
}

/// A resolved bearer-token provider plus its source label.
pub struct ResolvedBearerToken {
    /// Snapshot source installed into the backend.
    pub provider: Arc<dyn ProvideBearerToken>,
    /// Which mechanism produced it.
    pub source: &'static str,
}

/// Resolve the S3-family credential source from explicit static material, an
/// optional explicit selector, and the process environment.
///
/// Static credentials always win so existing deployments keep their exact
/// behavior. With `selector` unset (`auto`), the env contracts injected by
/// EKS IRSA, ACK RRSA, and TKE OIDC are detected in that order; the Huawei
/// metadata agency has no env marker and must be selected explicitly.
pub async fn resolve_s3_credentials(
    static_credentials: Option<S3Credentials>,
    selector: Option<&str>,
    http: Arc<dyn HttpClient>,
    observer: Arc<dyn CredentialsObserver>,
) -> Result<ResolvedS3Credentials, String> {
    resolve_s3_credentials_with_env(&env_var, static_credentials, selector, http, observer).await
}

/// [`resolve_s3_credentials`] with an injectable environment, for tests.
pub async fn resolve_s3_credentials_with_env(
    env: EnvFn<'_>,
    static_credentials: Option<S3Credentials>,
    selector: Option<&str>,
    http: Arc<dyn HttpClient>,
    observer: Arc<dyn CredentialsObserver>,
) -> Result<ResolvedS3Credentials, String> {
    let selector = selector.unwrap_or("auto");
    let policy = RefreshPolicy::default();
    let missing =
        |what: &str| format!("credentials source {selector} requires {what} in the environment");
    match selector {
        "static" => {
            let credentials =
                static_credentials.ok_or("static origin credentials are not configured")?;
            Ok(ResolvedS3Credentials {
                provider: Arc::new(StaticS3Credentials::new(credentials)),
                source: "static",
            })
        }
        "aws-web-identity" => {
            let fetcher = AwsWebIdentity::from_env(env, http)
                .ok_or_else(|| missing("AWS_ROLE_ARN and AWS_WEB_IDENTITY_TOKEN_FILE"))?;
            let source = fetcher.source();
            let cell = RefreshingCredentials::bootstrap(fetcher, policy, observer).await?;
            Ok(ResolvedS3Credentials {
                provider: cell,
                source,
            })
        }
        "aliyun-oidc" => {
            let fetcher = AliyunOidc::from_env(env, http).ok_or_else(|| {
                missing("ALIBABA_CLOUD_ROLE_ARN, ALIBABA_CLOUD_OIDC_PROVIDER_ARN, and ALIBABA_CLOUD_OIDC_TOKEN_FILE")
            })?;
            let source = fetcher.source();
            let cell = RefreshingCredentials::bootstrap(fetcher, policy, observer).await?;
            Ok(ResolvedS3Credentials {
                provider: cell,
                source,
            })
        }
        "tencent-oidc" => {
            let fetcher = TencentOidc::from_env(env, http).ok_or_else(|| {
                missing("TKE_ROLE_ARN, TKE_PROVIDER_ID, and TKE_WEB_IDENTITY_TOKEN_FILE")
            })?;
            let source = fetcher.source();
            let cell = RefreshingCredentials::bootstrap(fetcher, policy, observer).await?;
            Ok(ResolvedS3Credentials {
                provider: cell,
                source,
            })
        }
        "huawei-agency" => {
            let fetcher = HuaweiAgency::from_env(env, http);
            let source = fetcher.source();
            let cell = RefreshingCredentials::bootstrap(fetcher, policy, observer).await?;
            Ok(ResolvedS3Credentials {
                provider: cell,
                source,
            })
        }
        "auto" => {
            if let Some(credentials) = static_credentials {
                return Ok(ResolvedS3Credentials {
                    provider: Arc::new(StaticS3Credentials::new(credentials)),
                    source: "static",
                });
            }
            if let Some(fetcher) = AwsWebIdentity::from_env(env, Arc::clone(&http)) {
                let source = fetcher.source();
                let cell = RefreshingCredentials::bootstrap(fetcher, policy, observer).await?;
                return Ok(ResolvedS3Credentials {
                    provider: cell,
                    source,
                });
            }
            if let Some(fetcher) = AliyunOidc::from_env(env, Arc::clone(&http)) {
                let source = fetcher.source();
                let cell = RefreshingCredentials::bootstrap(fetcher, policy, observer).await?;
                return Ok(ResolvedS3Credentials {
                    provider: cell,
                    source,
                });
            }
            if let Some(fetcher) = TencentOidc::from_env(env, Arc::clone(&http)) {
                let source = fetcher.source();
                let cell = RefreshingCredentials::bootstrap(fetcher, policy, observer).await?;
                return Ok(ResolvedS3Credentials {
                    provider: cell,
                    source,
                });
            }
            Err(
                "no origin S3 credentials: configure static keys, or a workload \
                 identity (AWS IRSA, ACK RRSA, TKE OIDC via env; huawei-agency or \
                 gcp-metadata via the explicit credentials-source setting)"
                    .to_string(),
            )
        }
        other => Err(format!(
            "unknown origin credentials source {other:?}; expected auto, static, \
             aws-web-identity, aliyun-oidc, tencent-oidc, or huawei-agency"
        )),
    }
}

/// Resolve a bearer-token source for GCS: explicit static token, the GKE
/// metadata service when selected, or unauthenticated (emulators).
pub async fn resolve_gcs_bearer(
    static_token: Option<String>,
    selector: Option<&str>,
    http: Arc<dyn HttpClient>,
    observer: Arc<dyn CredentialsObserver>,
) -> Result<ResolvedBearerToken, String> {
    resolve_gcs_bearer_with_env(&env_var, static_token, selector, http, observer).await
}

/// [`resolve_gcs_bearer`] with an injectable environment, for tests.
pub async fn resolve_gcs_bearer_with_env(
    env: EnvFn<'_>,
    static_token: Option<String>,
    selector: Option<&str>,
    http: Arc<dyn HttpClient>,
    observer: Arc<dyn CredentialsObserver>,
) -> Result<ResolvedBearerToken, String> {
    match selector.unwrap_or("auto") {
        "gcp-metadata" => {
            let fetcher = GcpMetadataToken::from_env(env, http);
            let source = fetcher.source();
            let cell =
                RefreshingCredentials::bootstrap(fetcher, RefreshPolicy::default(), observer)
                    .await?;
            Ok(ResolvedBearerToken {
                provider: cell,
                source,
            })
        }
        "static" | "auto" => Ok(ResolvedBearerToken {
            source: if static_token.is_some() {
                "static"
            } else {
                "unauthenticated"
            },
            provider: Arc::new(StaticBearerToken::new(static_token)),
        }),
        other => Err(format!(
            "unknown GCS credentials source {other:?}; expected auto, static, or gcp-metadata"
        )),
    }
}

/// Resolve the Azure AD workload-identity bearer source from the environment.
///
/// Callers only reach this when neither a shared key nor a SAS is configured;
/// the env contract must therefore be present.
pub async fn resolve_azure_bearer(
    http: Arc<dyn HttpClient>,
    observer: Arc<dyn CredentialsObserver>,
) -> Result<ResolvedBearerToken, String> {
    resolve_azure_bearer_with_env(&env_var, http, observer).await
}

/// [`resolve_azure_bearer`] with an injectable environment, for tests.
pub async fn resolve_azure_bearer_with_env(
    env: EnvFn<'_>,
    http: Arc<dyn HttpClient>,
    observer: Arc<dyn CredentialsObserver>,
) -> Result<ResolvedBearerToken, String> {
    let fetcher = AzureWorkloadIdentity::from_env(env, http).ok_or(
        "Azure workload identity requires AZURE_CLIENT_ID, AZURE_TENANT_ID, \
         and AZURE_FEDERATED_TOKEN_FILE in the environment",
    )?;
    let source = fetcher.source();
    let cell =
        RefreshingCredentials::bootstrap(fetcher, RefreshPolicy::default(), observer).await?;
    Ok(ResolvedBearerToken {
        provider: cell,
        source,
    })
}

/// Whether the AKS workload-identity env contract is present.
pub fn azure_workload_identity_available() -> bool {
    azure_workload_identity_available_with_env(&env_var)
}

/// [`azure_workload_identity_available`] with an injectable environment.
pub fn azure_workload_identity_available_with_env(env: EnvFn<'_>) -> bool {
    env("AZURE_CLIENT_ID").is_some()
        && env("AZURE_TENANT_ID").is_some()
        && env("AZURE_FEDERATED_TOKEN_FILE").is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::HttpResponse;
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct MockHttp {
        response: HttpResponse,
        requests: Mutex<Vec<HttpRequest>>,
    }

    impl MockHttp {
        fn new(status: u16, body: &str) -> Arc<Self> {
            Arc::new(Self {
                response: HttpResponse {
                    status,
                    headers: Vec::new(),
                    body: bytes::Bytes::from(body.to_string()),
                },
                requests: Mutex::new(Vec::new()),
            })
        }

        fn request(&self) -> HttpRequest {
            self.requests.lock().unwrap()[0].clone()
        }
    }

    #[async_trait]
    impl HttpClient for MockHttp {
        async fn execute(&self, req: HttpRequest) -> Result<HttpResponse, String> {
            self.requests.lock().unwrap().push(req);
            Ok(self.response.clone())
        }
    }

    fn env_map(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        move |name: &str| map.get(name).cloned()
    }

    // The credential types deliberately lack Debug, so unwrap through
    // helpers that surface the error text without requiring it.
    fn ok<T>(result: Result<T, String>) -> T {
        match result {
            Ok(value) => value,
            Err(error) => panic!("unexpected error: {error}"),
        }
    }

    fn err<T>(result: Result<T, String>) -> String {
        match result {
            Ok(_) => panic!("unexpected success"),
            Err(error) => error,
        }
    }

    fn token_file(dir: &tempfile::TempDir, token: &str) -> String {
        let path = dir.path().join("token");
        std::fs::write(&path, token).unwrap();
        path.to_str().unwrap().to_string()
    }

    #[test]
    fn iso8601_parses_the_shapes_the_exchanges_emit() {
        let parsed = parse_iso8601_utc("2026-08-10T01:02:03Z").unwrap();
        assert_eq!(format_iso8601_utc(parsed), "2026-08-10T01:02:03Z");
        // Huawei stamps fractional seconds.
        assert_eq!(
            parse_iso8601_utc("2026-08-10T01:02:03.000000Z").unwrap(),
            parsed
        );
        assert_eq!(
            parse_iso8601_utc("2026-08-10T01:02:03+00:00").unwrap(),
            parsed
        );
        assert!(parse_iso8601_utc("garbage").is_none());
        assert!(parse_iso8601_utc("2026-13-10T01:02:03Z").is_none());
        assert!(parse_iso8601_utc("2026-08-10 01:02:03Z").is_none());
    }

    const AWS_STS_OK: &str = r#"<AssumeRoleWithWebIdentityResponse>
      <AssumeRoleWithWebIdentityResult><Credentials>
        <AccessKeyId>ASIAEXAMPLE</AccessKeyId>
        <SecretAccessKey>temp-secret</SecretAccessKey>
        <SessionToken>temp-token</SessionToken>
        <Expiration>2026-08-10T13:00:00Z</Expiration>
      </Credentials></AssumeRoleWithWebIdentityResult>
    </AssumeRoleWithWebIdentityResponse>"#;

    #[tokio::test]
    async fn aws_web_identity_exchanges_and_parses() {
        let temp = tempfile::TempDir::new().unwrap();
        let http = MockHttp::new(200, AWS_STS_OK);
        let env = env_map(&[
            ("AWS_ROLE_ARN", "arn:aws:iam::1:role/talon"),
            ("AWS_WEB_IDENTITY_TOKEN_FILE", &token_file(&temp, "jwt-abc")),
            ("AWS_REGION", "us-west-2"),
            ("AWS_STS_REGIONAL_ENDPOINTS", "regional"),
        ]);
        let fetcher = AwsWebIdentity::from_env(&env, http.clone() as Arc<dyn HttpClient>).unwrap();
        let (creds, expires) = ok(fetcher.fetch().await);
        assert_eq!(creds.access_key_id, "ASIAEXAMPLE");
        assert_eq!(creds.secret_access_key, "temp-secret");
        assert_eq!(creds.session_token.as_deref(), Some("temp-token"));
        assert_eq!(expires, parse_iso8601_utc("2026-08-10T13:00:00Z"));
        let request = http.request();
        assert_eq!(request.url, "https://sts.us-west-2.amazonaws.com");
        let body = String::from_utf8(request.body.to_vec()).unwrap();
        assert!(body.contains("Action=AssumeRoleWithWebIdentity"));
        assert!(body.contains("WebIdentityToken=jwt-abc"));
        assert!(body.contains("RoleArn=arn%3Aaws%3Aiam%3A%3A1%3Arole%2Ftalon"));
    }

    #[tokio::test]
    async fn aws_errors_expose_the_code_but_never_the_token() {
        let temp = tempfile::TempDir::new().unwrap();
        let http = MockHttp::new(
            403,
            "<ErrorResponse><Error><Code>ExpiredTokenException</Code>\
             <Message>Token expired: jwt-abc</Message></Error></ErrorResponse>",
        );
        let env = env_map(&[
            ("AWS_ROLE_ARN", "arn:aws:iam::1:role/talon"),
            ("AWS_WEB_IDENTITY_TOKEN_FILE", &token_file(&temp, "jwt-abc")),
        ]);
        let fetcher = AwsWebIdentity::from_env(&env, http as Arc<dyn HttpClient>).unwrap();
        let error = err(fetcher.fetch().await);
        assert!(error.contains("ExpiredTokenException"));
        assert!(error.contains("403"));
        assert!(!error.contains("jwt-abc"));
    }

    #[tokio::test]
    async fn aliyun_oidc_exchanges_and_parses() {
        let temp = tempfile::TempDir::new().unwrap();
        let http = MockHttp::new(
            200,
            r#"{"RequestId":"r","Credentials":{"AccessKeyId":"STS.ali","AccessKeySecret":"ali-secret","SecurityToken":"ali-token","Expiration":"2026-08-10T13:00:00Z"}}"#,
        );
        let env = env_map(&[
            ("ALIBABA_CLOUD_ROLE_ARN", "acs:ram::1:role/talon"),
            (
                "ALIBABA_CLOUD_OIDC_PROVIDER_ARN",
                "acs:ram::1:oidc-provider/ack",
            ),
            (
                "ALIBABA_CLOUD_OIDC_TOKEN_FILE",
                &token_file(&temp, "oidc-jwt"),
            ),
        ]);
        let fetcher = AliyunOidc::from_env(&env, http.clone() as Arc<dyn HttpClient>).unwrap();
        let (creds, expires) = ok(fetcher.fetch().await);
        assert_eq!(creds.access_key_id, "STS.ali");
        assert_eq!(creds.session_token.as_deref(), Some("ali-token"));
        assert!(expires.is_some());
        let request = http.request();
        assert_eq!(request.url, "https://sts.aliyuncs.com");
        let body = String::from_utf8(request.body.to_vec()).unwrap();
        assert!(body.contains("Action=AssumeRoleWithOIDC"));
        assert!(body.contains("OIDCToken=oidc-jwt"));
        assert!(body.contains("Timestamp="));
    }

    #[tokio::test]
    async fn tencent_oidc_exchanges_and_surfaces_in_band_errors() {
        let temp = tempfile::TempDir::new().unwrap();
        let success = MockHttp::new(
            200,
            r#"{"Response":{"Credentials":{"TmpSecretId":"tc-id","TmpSecretKey":"tc-key","Token":"tc-token"},"ExpiredTime":1786500000,"RequestId":"r"}}"#,
        );
        let env_pairs = [
            ("TKE_ROLE_ARN", "qcs::cam::uin/1:roleName/talon".to_string()),
            ("TKE_PROVIDER_ID", "oidc-provider".to_string()),
            ("TKE_WEB_IDENTITY_TOKEN_FILE", token_file(&temp, "tke-jwt")),
            ("TKE_REGION", "ap-shanghai".to_string()),
        ];
        let pairs: Vec<(&str, &str)> = env_pairs
            .iter()
            .map(|(name, value)| (*name, value.as_str()))
            .collect();
        let env = env_map(&pairs);
        let fetcher = TencentOidc::from_env(&env, success.clone() as Arc<dyn HttpClient>).unwrap();
        let (creds, expires) = ok(fetcher.fetch().await);
        assert_eq!(creds.access_key_id, "tc-id");
        assert_eq!(creds.session_token.as_deref(), Some("tc-token"));
        assert_eq!(
            expires,
            Some(UNIX_EPOCH + Duration::from_secs(1_786_500_000))
        );
        let request = success.request();
        assert_eq!(
            request.header("x-tc-action"),
            Some("AssumeRoleWithWebIdentity")
        );
        assert_eq!(request.header("authorization"), Some("SKIP"));
        assert_eq!(request.header("x-tc-region"), Some("ap-shanghai"));

        // Tencent reports failures inside a 200 body.
        let failed = MockHttp::new(
            200,
            r#"{"Response":{"Error":{"Code":"InvalidParameter.Token","Message":"bad tke-jwt"},"RequestId":"r"}}"#,
        );
        let fetcher = TencentOidc::from_env(&env, failed as Arc<dyn HttpClient>).unwrap();
        let error = err(fetcher.fetch().await);
        assert!(error.contains("InvalidParameter.Token"));
        assert!(!error.contains("tke-jwt"));
    }

    #[tokio::test]
    async fn huawei_agency_reads_the_metadata_credential() {
        let http = MockHttp::new(
            200,
            r#"{"credential":{"access":"hw-ak","secret":"hw-sk","securitytoken":"hw-token","expires_at":"2026-08-10T13:00:00.531000Z"}}"#,
        );
        let env = env_map(&[]);
        let fetcher = HuaweiAgency::from_env(&env, http.clone() as Arc<dyn HttpClient>);
        let (creds, expires) = ok(fetcher.fetch().await);
        assert_eq!(creds.access_key_id, "hw-ak");
        assert_eq!(creds.session_token.as_deref(), Some("hw-token"));
        assert!(expires.is_some());
        assert_eq!(
            http.request().url,
            "http://169.254.169.254/openstack/latest/securitykey"
        );
    }

    #[tokio::test]
    async fn gcp_metadata_token_sets_the_flavor_header() {
        let http = MockHttp::new(
            200,
            r#"{"access_token":"ya29.token","expires_in":3599,"token_type":"Bearer"}"#,
        );
        let env = env_map(&[]);
        let fetcher = GcpMetadataToken::from_env(&env, http.clone() as Arc<dyn HttpClient>);
        let (token, expires) = ok(fetcher.fetch().await);
        assert_eq!(token, "ya29.token");
        assert!(expires.unwrap() > SystemTime::now() + Duration::from_secs(3000));
        let request = http.request();
        assert_eq!(request.header("metadata-flavor"), Some("Google"));
        assert!(request.url.contains("metadata.google.internal"));
    }

    #[tokio::test]
    async fn azure_workload_identity_exchanges_the_federated_token() {
        let temp = tempfile::TempDir::new().unwrap();
        let http = MockHttp::new(
            200,
            r#"{"token_type":"Bearer","expires_in":3600,"access_token":"aad-token"}"#,
        );
        let env = env_map(&[
            ("AZURE_CLIENT_ID", "client-1"),
            ("AZURE_TENANT_ID", "tenant-1"),
            ("AZURE_FEDERATED_TOKEN_FILE", &token_file(&temp, "fed-jwt")),
        ]);
        let fetcher =
            AzureWorkloadIdentity::from_env(&env, http.clone() as Arc<dyn HttpClient>).unwrap();
        let (token, expires) = ok(fetcher.fetch().await);
        assert_eq!(token, "aad-token");
        assert!(expires.is_some());
        let request = http.request();
        assert_eq!(
            request.url,
            "https://login.microsoftonline.com/tenant-1/oauth2/v2.0/token"
        );
        let body = String::from_utf8(request.body.to_vec()).unwrap();
        assert!(body.contains("client_assertion=fed-jwt"));
        assert!(body.contains("scope=https%3A%2F%2Fstorage.azure.com%2F.default"));

        let failed = MockHttp::new(
            401,
            r#"{"error":"invalid_client","error_description":"AADSTS700016 fed-jwt"}"#,
        );
        let fetcher = AzureWorkloadIdentity::from_env(&env, failed as Arc<dyn HttpClient>).unwrap();
        let error = err(fetcher.fetch().await);
        assert!(error.contains("invalid_client"));
        assert!(!error.contains("fed-jwt"));
    }

    #[tokio::test]
    async fn resolution_prefers_static_then_detects_workload_identity() {
        let temp = tempfile::TempDir::new().unwrap();
        let http = MockHttp::new(200, AWS_STS_OK);
        let aws_env_token = token_file(&temp, "jwt");
        let env = env_map(&[
            ("AWS_ROLE_ARN", "arn:aws:iam::1:role/talon"),
            ("AWS_WEB_IDENTITY_TOKEN_FILE", &aws_env_token),
        ]);

        // Static credentials win even with IRSA env present.
        let resolved = ok(resolve_s3_credentials_with_env(
            &env,
            Some(S3Credentials {
                access_key_id: "AKIDSTATIC".into(),
                secret_access_key: "s".into(),
                session_token: None,
            }),
            None,
            http.clone() as Arc<dyn HttpClient>,
            Arc::new(crate::credentials::NoopObserver),
        )
        .await);
        assert_eq!(resolved.source, "static");
        assert_eq!(resolved.provider.current().access_key_id, "AKIDSTATIC");
        assert!(http.requests.lock().unwrap().is_empty());

        // Without static material the IRSA contract is detected and used.
        let resolved = ok(resolve_s3_credentials_with_env(
            &env,
            None,
            None,
            http.clone() as Arc<dyn HttpClient>,
            Arc::new(crate::credentials::NoopObserver),
        )
        .await);
        assert_eq!(resolved.source, "aws-web-identity");
        assert_eq!(resolved.provider.current().access_key_id, "ASIAEXAMPLE");

        // Nothing configured is a startup error naming the options.
        let empty = env_map(&[]);
        let error = err(resolve_s3_credentials_with_env(
            &empty,
            None,
            None,
            http.clone() as Arc<dyn HttpClient>,
            Arc::new(crate::credentials::NoopObserver),
        )
        .await);
        assert!(error.contains("workload identity"));

        // Unknown selectors are rejected loudly.
        let error = err(resolve_s3_credentials_with_env(
            &empty,
            None,
            Some("magic"),
            http as Arc<dyn HttpClient>,
            Arc::new(crate::credentials::NoopObserver),
        )
        .await);
        assert!(error.contains("magic"));
    }

    #[tokio::test]
    async fn gcs_resolution_covers_static_metadata_and_unauthenticated() {
        let http = MockHttp::new(200, r#"{"access_token":"ya29","expires_in":600}"#);
        let env = env_map(&[]);
        let resolved = ok(resolve_gcs_bearer_with_env(
            &env,
            Some("fixed".into()),
            None,
            http.clone() as Arc<dyn HttpClient>,
            Arc::new(crate::credentials::NoopObserver),
        )
        .await);
        assert_eq!(resolved.source, "static");
        assert_eq!(resolved.provider.current().unwrap().as_str(), "fixed");

        let resolved = ok(resolve_gcs_bearer_with_env(
            &env,
            None,
            Some("gcp-metadata"),
            http.clone() as Arc<dyn HttpClient>,
            Arc::new(crate::credentials::NoopObserver),
        )
        .await);
        assert_eq!(resolved.source, "gcp-metadata");
        assert_eq!(resolved.provider.current().unwrap().as_str(), "ya29");

        let resolved = ok(resolve_gcs_bearer_with_env(
            &env,
            None,
            None,
            http as Arc<dyn HttpClient>,
            Arc::new(crate::credentials::NoopObserver),
        )
        .await);
        assert_eq!(resolved.source, "unauthenticated");
        assert!(resolved.provider.current().is_none());
    }
}
