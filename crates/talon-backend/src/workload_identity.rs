//! Cloud workload-identity and EC2 instance-profile credential acquisition.
//!
//! Each cloud's Kubernetes platform hands a pod an identity instead of a
//! static key: EKS IRSA and its analogues inject a token file plus role
//! coordinates through env variables, and GKE/CCE expose a metadata endpoint.
//! Explicit providers use documented HTTP exchanges. The `iam` mode delegates
//! AWS/GCP discovery to their SDK default chains and selects Azure workload
//! identity or managed identity using the same token-file rule as milvus-storage.
//!
//! Verification status: the AWS contract is confirmed against a live EKS
//! deployment; Aliyun, Tencent, and Huawei mirror the contracts used by the
//! Milvus object-storage providers and remain to be validated against live
//! clusters. Fetch errors carry the HTTP status and a bounded provider error
//! code, never token or key material.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::credentials::{
    CredentialsFetch, CredentialsObserver, ProvideBearerToken, ProvideS3Credentials, RefreshPolicy,
    RefreshingCredentials, StaticS3Credentials,
};
use crate::http::{HttpClient, HttpRequest, Method};
use crate::s3::S3Credentials;

/// Environment lookup used by source resolution, injectable for tests.
pub type EnvFn<'a> = &'a dyn Fn(&str) -> Option<String>;

pub(crate) fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

struct AwsDefaultCredentials(aws_config::default_provider::credentials::DefaultCredentialsChain);

#[async_trait]
impl CredentialsFetch for AwsDefaultCredentials {
    type Value = S3Credentials;

    async fn fetch(&self) -> Result<(S3Credentials, Option<SystemTime>), String> {
        use aws_credential_types::provider::ProvideCredentials;
        let credentials = self
            .0
            .provide_credentials()
            .await
            .map_err(|error| format!("AWS default credential chain: {error}"))?;
        let expires = credentials.expiry();
        Ok((
            S3Credentials {
                access_key_id: credentials.access_key_id().into(),
                secret_access_key: credentials.secret_access_key().into(),
                session_token: credentials.session_token().map(str::to_owned),
            },
            expires,
        ))
    }

    fn source(&self) -> &'static str {
        "aws-default"
    }
}

struct GcpDefaultCredentials {
    credentials: google_cloud_auth::credentials::AccessTokenCredentials,
    current: RwLock<Arc<String>>,
    observer: Arc<dyn CredentialsObserver>,
}

#[async_trait]
impl ProvideBearerToken for GcpDefaultCredentials {
    fn current(&self) -> Option<Arc<String>> {
        Some(Arc::clone(&self.current.read().unwrap()))
    }

    async fn prepare(&self) -> Result<(), String> {
        // The SDK caches and refreshes ADC tokens, but does not expose their
        // expiration. Await its cache instead of adding a second timed cache.
        let result =
            tokio::time::timeout(Duration::from_secs(30), self.credentials.access_token()).await;
        let token = match result {
            Ok(Ok(token))
                if !token.token.is_empty()
                    && !token
                        .token
                        .bytes()
                        .any(|b| b.is_ascii_whitespace() || b.is_ascii_control()) =>
            {
                token.token
            }
            _ => {
                self.observer.refresh_failed();
                return Err(
                    "GCP ADC token acquisition failed (credential details suppressed)".into(),
                );
            }
        };
        let changed = {
            let mut current = self.current.write().unwrap();
            if current.as_str() == token {
                false
            } else {
                *current = Arc::new(token);
                true
            }
        };
        if changed {
            self.observer.refresh_succeeded(None);
        }
        Ok(())
    }
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

// --- Credential timestamps -----------------------------------------------

/// Parse a trimmed RFC3339 expiry, preserving offsets and fractional seconds.
/// Keep the workload exchanges' uppercase `T` separator contract. Invalid
/// timestamps yield `None`; existing callers treat that as unknown expiry.
pub(crate) fn parse_iso8601_utc(text: &str) -> Option<SystemTime> {
    let text = text.trim();
    if text.as_bytes().get(10) != Some(&b'T') {
        return None;
    }
    chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .map(SystemTime::from)
}

/// Format Aliyun's `Timestamp` in UTC seconds, retaining the existing epoch
/// clamp for pre-epoch values and omitting fractional seconds.
fn format_iso8601_utc(time: SystemTime) -> String {
    let time: chrono::DateTime<chrono::Utc> = time.max(UNIX_EPOCH).into();
    time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn expires_in_seconds(now: SystemTime, seconds: u64) -> SystemTime {
    now + Duration::from_secs(seconds)
}

fn metadata_origin(endpoint: Option<String>) -> Result<url::Url, String> {
    let endpoint = endpoint.unwrap_or_else(|| "http://169.254.169.254".into());
    let url = url::Url::parse(&endpoint).map_err(|_| "invalid metadata endpoint")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err(
            "metadata endpoint must be an HTTP(S) origin without userinfo, path, query or fragment"
                .into(),
        );
    }
    Ok(url)
}

/// VM metadata must not inherit origin proxies/retries or follow redirects.
/// One client is shared across all refreshes of the resolved provider.
fn metadata_http(timeout: Duration) -> Result<Arc<dyn HttpClient>, String> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(2))
        .timeout(timeout)
        .build()
        .map_err(|_| "cannot initialize metadata HTTP client")?;
    Ok(Arc::new(crate::ReqwestClient::with_client(client)))
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

/// EC2 instance profile: retrieve the attached role's temporary credentials
/// through IMDSv2. Each refresh obtains a new metadata token and rediscovers
/// the role, so replacing an instance profile does not pin the previous role.
pub struct AwsInstanceProfile {
    endpoint: String,
    http: Arc<dyn HttpClient>,
}

impl AwsInstanceProfile {
    /// Use the standard metadata endpoint or its explicit SDK-compatible
    /// override. Injected transports must not proxy or redirect metadata tokens.
    pub fn from_env(env: EnvFn<'_>, http: Arc<dyn HttpClient>) -> Result<Self, String> {
        if env("AWS_EC2_METADATA_DISABLED").is_some_and(|value| value.eq_ignore_ascii_case("true"))
        {
            return Err("EC2 metadata is disabled by AWS_EC2_METADATA_DISABLED".into());
        }
        let url = metadata_origin(env("AWS_EC2_METADATA_SERVICE_ENDPOINT"))?;
        Ok(Self {
            endpoint: url.as_str().trim_end_matches('/').into(),
            http,
        })
    }

    async fn request(
        &self,
        method: Method,
        path: &str,
        headers: Vec<(String, String)>,
    ) -> Result<bytes::Bytes, String> {
        let response = self
            .http
            .execute(HttpRequest::new(
                method,
                format!("{}{path}", self.endpoint),
                headers,
            ))
            .await
            .map_err(|_| "EC2 metadata transport failed")?;
        if response.status != 200 {
            return Err(format!(
                "EC2 metadata returned HTTP status {}",
                response.status
            ));
        }
        Ok(response.body)
    }
}

#[async_trait]
impl CredentialsFetch for AwsInstanceProfile {
    type Value = S3Credentials;

    async fn fetch(&self) -> Result<(S3Credentials, Option<SystemTime>), String> {
        // IMDS is local to the instance. Bound the whole handshake, including
        // response bodies, rather than inheriting an origin transfer deadline.
        tokio::time::timeout(Duration::from_secs(5), async {
            let token = self
                .request(
                    Method::Put,
                    "/latest/api/token",
                    vec![(
                        "x-aws-ec2-metadata-token-ttl-seconds".into(),
                        "21600".into(),
                    )],
                )
                .await?;
            let token = std::str::from_utf8(&token).map_err(|_| "invalid EC2 metadata token")?;
            if token.trim().is_empty() || token.bytes().any(|b| b.is_ascii_control()) {
                return Err("invalid EC2 metadata token".into());
            }
            let headers = vec![("x-aws-ec2-metadata-token".into(), token.into())];
            let role = self
                .request(
                    Method::Get,
                    "/latest/meta-data/iam/security-credentials/",
                    headers.clone(),
                )
                .await?;
            let role = std::str::from_utf8(&role)
                .map_err(|_| "invalid EC2 instance role name")?
                .trim();
            // Dot-only segments would be normalized into another metadata path.
            if role.is_empty()
                || matches!(role, "." | "..")
                || !role.bytes().all(|b| {
                    b.is_ascii_alphanumeric()
                        || matches!(b, b'+' | b'=' | b',' | b'.' | b'@' | b'_' | b'-')
                })
            {
                return Err("missing or invalid EC2 instance role name".into());
            }
            let body = self
                .request(
                    Method::Get,
                    &format!("/latest/meta-data/iam/security-credentials/{role}"),
                    headers,
                )
                .await?;
            let value = json_body(&body)?;
            if value.get("Code").and_then(serde_json::Value::as_str) != Some("Success") {
                return Err("EC2 metadata reported credential acquisition failure".into());
            }
            let field = |name: &str| -> Result<&str, String> {
                value
                    .get(name)
                    .and_then(serde_json::Value::as_str)
                    .filter(|value| {
                        !value.trim().is_empty() && !value.bytes().any(|b| b.is_ascii_control())
                    })
                    .ok_or_else(|| format!("EC2 metadata missing or invalid {name}"))
            };
            let expiry = parse_iso8601_utc(field("Expiration")?)
                .ok_or("invalid EC2 credential expiration")?;
            if expiry <= SystemTime::now() {
                return Err("EC2 metadata credentials are expired".into());
            }
            Ok((
                S3Credentials {
                    access_key_id: field("AccessKeyId")?.into(),
                    secret_access_key: field("SecretAccessKey")?.into(),
                    session_token: Some(field("Token")?.into()),
                },
                Some(expiry),
            ))
        })
        .await
        .map_err(|_| "EC2 metadata request timed out")?
    }

    fn source(&self) -> &'static str {
        "aws-instance-profile"
    }
}

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

const AZURE_STORAGE_RESOURCE: &str = "https://storage.azure.com/";
const AZURE_IMDS_TIMEOUT: Duration = Duration::from_secs(30);

/// Azure VM managed identity: acquire an Azure Storage token from IMDS.
/// AZURE_CLIENT_ID selects a user-assigned identity; otherwise IMDS chooses
/// the VM's default identity (normally its system-assigned identity).
pub struct AzureManagedIdentity {
    endpoint: String,
    http: Arc<dyn HttpClient>,
}

impl AzureManagedIdentity {
    /// Build the VM metadata request, optionally using a controlled local
    /// endpoint override. Injected HTTP transports must bypass proxies/redirects.
    pub fn from_env(env: EnvFn<'_>, http: Arc<dyn HttpClient>) -> Result<Self, String> {
        let mut url = metadata_origin(env("TALON_AZURE_IMDS_ENDPOINT"))?;
        url.set_path("/metadata/identity/oauth2/token");
        url.query_pairs_mut()
            .append_pair("api-version", "2018-02-01")
            .append_pair("resource", AZURE_STORAGE_RESOURCE);
        if let Some(client_id) = env("AZURE_CLIENT_ID").filter(|value| !value.is_empty()) {
            if client_id
                .bytes()
                .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
            {
                return Err("invalid Azure managed identity client ID".into());
            }
            url.query_pairs_mut().append_pair("client_id", &client_id);
        }
        Ok(Self {
            endpoint: url.into(),
            http,
        })
    }
}

#[async_trait]
impl CredentialsFetch for AzureManagedIdentity {
    type Value = String;

    async fn fetch(&self) -> Result<(String, Option<SystemTime>), String> {
        let request = HttpRequest::new(
            Method::Get,
            self.endpoint.clone(),
            vec![("metadata".into(), "true".into())],
        );
        let response = tokio::time::timeout(AZURE_IMDS_TIMEOUT, self.http.execute(request))
            .await
            .map_err(|_| "Azure IMDS request timed out")?
            .map_err(|_| "Azure IMDS transport failed")?;
        if response.status != 200 {
            return Err(format!(
                "Azure IMDS returned HTTP status {}",
                response.status
            ));
        }
        let value = json_body(&response.body)?;
        let token = value
            .get("access_token")
            .and_then(serde_json::Value::as_str)
            .filter(|value| {
                !value.is_empty()
                    && !value
                        .bytes()
                        .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
            })
            .ok_or("Azure IMDS missing or invalid access_token")?;
        if !value
            .get("token_type")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|kind| kind.eq_ignore_ascii_case("Bearer"))
        {
            return Err("Azure IMDS token_type must be Bearer".into());
        }
        if value.get("resource").and_then(serde_json::Value::as_str) != Some(AZURE_STORAGE_RESOURCE)
        {
            return Err("Azure IMDS resource does not match Azure Storage".into());
        }
        // IMDS may return a cached token. Use its absolute expiry, not an
        // expires_in value measured from an earlier issuance time.
        let seconds = match value.get("expires_on") {
            Some(serde_json::Value::String(value)) => value.parse::<u64>().ok(),
            Some(value) => value.as_u64(),
            None => None,
        }
        .ok_or("Azure IMDS missing or invalid expires_on")?;
        let expiry = UNIX_EPOCH
            .checked_add(Duration::from_secs(seconds))
            .ok_or("Azure IMDS expires_on is out of range")?;
        if expiry <= SystemTime::now() {
            return Err("Azure IMDS token is expired".into());
        }
        Ok((token.into(), Some(expiry)))
    }

    fn source(&self) -> &'static str {
        "azure-managed-identity"
    }
}

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

/// Credential acquisition modes. Token formats (Bearer/SAS) are provider
/// outputs, not additional modes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialMode {
    AkSk,
    Iam,
    Broker,
}

impl CredentialMode {
    /// Infer an omitted mode from key configuration. Historical source names
    /// remain aliases; all selection passes through the same three modes.
    pub fn select(selector: Option<&str>, has_aksk: bool) -> Result<Self, String> {
        match selector {
            None | Some("auto") => Ok(if has_aksk { Self::AkSk } else { Self::Iam }),
            Some("aksk" | "static") => Ok(Self::AkSk),
            Some(
                "iam"
                | "aws-instance-profile"
                | "aws-web-identity"
                | "aliyun-oidc"
                | "tencent-oidc"
                | "huawei-agency"
                | "gcp-metadata"
                | "azure-managed-identity"
                | "azure-workload-identity",
            ) => Ok(Self::Iam),
            Some("broker") => Ok(Self::Broker),
            Some(other) => Err(format!(
                "unknown credentials source {other:?}; expected aksk, iam, or broker"
            )),
        }
    }
}

/// Resolved S3 signing credentials and their concrete provider name.
pub struct ResolvedS3Credentials {
    pub provider: Arc<dyn ProvideS3Credentials>,
    pub source: &'static str,
}

/// Resolved GCS HMAC keys or an IAM/Broker bearer-token provider.
pub struct ResolvedGcsCredentials {
    pub provider: crate::gcs::GcsCredentials,
    pub source: &'static str,
}

/// Resolved Azure account key, IAM token provider, or Broker SAS provider.
pub struct ResolvedAzureCredentials {
    pub provider: crate::azure::AzureCredentials,
    pub source: &'static str,
}

struct ResolutionContext<'a> {
    env: EnvFn<'a>,
    selector: Option<&'a str>,
    http: Arc<dyn HttpClient>,
    observer: Arc<dyn CredentialsObserver>,
    // Native SDK discovery reads process configuration, so its tests use
    // isolated subprocesses rather than the protocol-level injectable seam.
    process_environment: bool,
}

trait CredentialResolver {
    type Output;
    fn has_aksk(&self) -> bool;
    fn aksk(self) -> Result<Self::Output, String>;
    async fn iam(self, context: ResolutionContext<'_>) -> Result<Self::Output, String>;
    async fn broker(self, context: ResolutionContext<'_>) -> Result<Self::Output, String>;
}

/// The only acquisition-mode dispatch. Backend implementations supply typed
/// factories, preserving S3/GCS/Azure result types at their public entry points.
async fn resolve_credentials<R: CredentialResolver>(
    resolver: R,
    context: ResolutionContext<'_>,
) -> Result<R::Output, String> {
    match CredentialMode::select(context.selector, resolver.has_aksk())? {
        CredentialMode::AkSk => resolver.aksk(),
        CredentialMode::Iam => resolver.iam(context).await,
        CredentialMode::Broker => resolver.broker(context).await,
    }
}

struct S3Resolver(Option<S3Credentials>);

async fn refreshing_s3<F: CredentialsFetch<Value = S3Credentials>>(
    fetcher: F,
    observer: Arc<dyn CredentialsObserver>,
) -> Result<ResolvedS3Credentials, String> {
    let source = fetcher.source();
    let provider =
        RefreshingCredentials::bootstrap(fetcher, RefreshPolicy::default(), observer).await?;
    Ok(ResolvedS3Credentials { provider, source })
}

impl CredentialResolver for S3Resolver {
    type Output = ResolvedS3Credentials;
    fn has_aksk(&self) -> bool {
        self.0.is_some()
    }
    fn aksk(self) -> Result<Self::Output, String> {
        let credentials = self
            .0
            .ok_or("S3 aksk mode requires access key and secret key")?;
        if credentials.access_key_id.is_empty() || credentials.secret_access_key.is_empty() {
            return Err("S3 aksk mode requires nonempty access key and secret key".into());
        }
        Ok(ResolvedS3Credentials {
            provider: Arc::new(StaticS3Credentials::new(credentials)),
            source: "aksk",
        })
    }
    async fn broker(self, context: ResolutionContext<'_>) -> Result<Self::Output, String> {
        let fetcher = crate::broker::BrokerFetch::<S3Credentials>::from_env(
            context.env,
            "aws",
            None,
            context.http,
        )?;
        refreshing_s3(fetcher, context.observer).await
    }
    async fn iam(self, context: ResolutionContext<'_>) -> Result<Self::Output, String> {
        let env = context.env;
        let selector = context.selector.unwrap_or("auto");
        let missing = |vars: &str| format!("credentials source {selector} requires {vars}");
        let http = context.http;
        let observer = context.observer;
        match selector {
            "aws-instance-profile" => {
                let http = if context.process_environment { metadata_http(Duration::from_secs(5))? } else { http };
                refreshing_s3(AwsInstanceProfile::from_env(env, http)?, observer).await
            }
            "aws-web-identity" => refreshing_s3(AwsWebIdentity::from_env(env, http)
                .ok_or_else(|| missing("AWS_ROLE_ARN and AWS_WEB_IDENTITY_TOKEN_FILE"))?, observer).await,
            "aliyun-oidc" => refreshing_s3(AliyunOidc::from_env(env, http)
                .ok_or_else(|| missing("ALIBABA_CLOUD_ROLE_ARN, ALIBABA_CLOUD_OIDC_PROVIDER_ARN and ALIBABA_CLOUD_OIDC_TOKEN_FILE"))?, observer).await,
            "tencent-oidc" => refreshing_s3(TencentOidc::from_env(env, http)
                .ok_or_else(|| missing("TKE_ROLE_ARN, TKE_PROVIDER_ID and TKE_WEB_IDENTITY_TOKEN_FILE"))?, observer).await,
            "huawei-agency" => refreshing_s3(HuaweiAgency::from_env(env, http), observer).await,
            "auto" | "iam" => {
                // Preserve existing injected Kubernetes contracts in auto mode.
                if selector == "auto" {
                    if let Some(fetcher) = AwsWebIdentity::from_env(env, http.clone()) {
                        return refreshing_s3(fetcher, observer).await;
                    }
                    if let Some(fetcher) = AliyunOidc::from_env(env, http.clone()) {
                        return refreshing_s3(fetcher, observer).await;
                    }
                    if let Some(fetcher) = TencentOidc::from_env(env, http) {
                        return refreshing_s3(fetcher, observer).await;
                    }
                }
                if !context.process_environment {
                    return Err("no injected workload identity; AWS SDK IAM discovery requires the process-environment resolver".into());
                }
                use aws_smithy_http_client::tls::{rustls_provider::CryptoMode, Provider};
                let http = aws_smithy_http_client::Builder::new()
                    .tls_provider(Provider::Rustls(CryptoMode::Ring)).build_https();
                let config = aws_config::provider_config::ProviderConfig::default().with_http_client(http);
                let chain = aws_config::default_provider::credentials::DefaultCredentialsChain::builder()
                    .configure(config).build().await;
                refreshing_s3(AwsDefaultCredentials(chain), observer).await
            }
            _ => Err(format!("IAM provider {selector:?} does not apply to S3")),
        }
    }
}

struct GcsResolver(Option<crate::gcs::GcsHmacCredentials>);

impl CredentialResolver for GcsResolver {
    type Output = ResolvedGcsCredentials;
    fn has_aksk(&self) -> bool {
        self.0.is_some()
    }
    fn aksk(self) -> Result<Self::Output, String> {
        let credentials = self
            .0
            .ok_or("GCS aksk mode requires an HMAC access ID and secret")?;
        if credentials.access_key_id.is_empty() || credentials.secret_access_key.is_empty() {
            return Err("GCS aksk mode requires nonempty HMAC access ID and secret".into());
        }
        Ok(ResolvedGcsCredentials {
            provider: crate::gcs::GcsCredentials::Hmac(credentials),
            source: "aksk",
        })
    }
    async fn broker(self, context: ResolutionContext<'_>) -> Result<Self::Output, String> {
        let fetcher =
            crate::broker::BrokerFetch::<String>::from_env(context.env, "gcp", None, context.http)?;
        let provider =
            RefreshingCredentials::bootstrap(fetcher, RefreshPolicy::default(), context.observer)
                .await?;
        Ok(ResolvedGcsCredentials {
            provider: crate::gcs::GcsCredentials::Bearer(provider),
            source: "broker",
        })
    }
    async fn iam(self, context: ResolutionContext<'_>) -> Result<Self::Output, String> {
        let (provider, source): (Arc<dyn ProvideBearerToken>, _) = match context.selector {
            Some("gcp-metadata") => {
                let fetcher = GcpMetadataToken::from_env(context.env, context.http);
                (
                    RefreshingCredentials::bootstrap(
                        fetcher,
                        RefreshPolicy::default(),
                        context.observer,
                    )
                    .await?,
                    "gcp-metadata",
                )
            }
            None | Some("auto" | "iam") => {
                if !context.process_environment {
                    return Err(
                        "GCP ADC discovery requires the process-environment resolver".into(),
                    );
                }
                let _ = rustls::crypto::ring::default_provider().install_default();
                let credentials = google_cloud_auth::credentials::Builder::default()
                    .with_scopes(["https://www.googleapis.com/auth/cloud-platform"])
                    .build_access_token_credentials()
                    .map_err(|_| {
                        "GCP ADC configuration is invalid (credential details suppressed)"
                    })?;
                let provider = Arc::new(GcpDefaultCredentials {
                    credentials,
                    current: RwLock::new(Arc::new(String::new())),
                    observer: context.observer,
                });
                provider.prepare().await?;
                (provider, "gcp-adc")
            }
            Some(other) => return Err(format!("IAM provider {other:?} does not apply to GCS")),
        };
        Ok(ResolvedGcsCredentials {
            provider: crate::gcs::GcsCredentials::Bearer(provider),
            source,
        })
    }
}

struct AzureResolver<'a> {
    account: &'a str,
    shared_key: Option<String>,
}

impl CredentialResolver for AzureResolver<'_> {
    type Output = ResolvedAzureCredentials;
    fn has_aksk(&self) -> bool {
        self.shared_key.is_some()
    }
    fn aksk(self) -> Result<Self::Output, String> {
        let key = self
            .shared_key
            .filter(|key| !key.is_empty())
            .ok_or("Azure aksk mode requires a storage account key")?;
        if self.account.is_empty() {
            return Err("Azure aksk mode requires a storage account name".into());
        }
        Ok(ResolvedAzureCredentials {
            provider: crate::azure::AzureCredentials::SharedKey(key),
            source: "aksk",
        })
    }
    async fn broker(self, context: ResolutionContext<'_>) -> Result<Self::Output, String> {
        let provider = crate::broker::resolve_azure_broker_sas_with_env(
            context.env,
            self.account,
            context.http,
            context.observer,
        )
        .await?;
        Ok(ResolvedAzureCredentials {
            provider: crate::azure::AzureCredentials::BrokerSas(provider),
            source: "broker",
        })
    }
    async fn iam(self, context: ResolutionContext<'_>) -> Result<Self::Output, String> {
        let managed = match context.selector {
            Some("azure-managed-identity") => true,
            Some("azure-workload-identity") => false,
            None | Some("auto" | "iam") => {
                if (context.env)("AZURE_CLIENT_ID")
                    .filter(|v| !v.is_empty())
                    .is_none()
                {
                    return Err("Azure IAM requires AZURE_CLIENT_ID".into());
                }
                (context.env)("AZURE_FEDERATED_TOKEN_FILE")
                    .filter(|v| !v.is_empty())
                    .is_none()
            }
            Some(other) => return Err(format!("IAM provider {other:?} does not apply to Azure")),
        };
        let (provider, source): (Arc<dyn ProvideBearerToken>, _) = if managed {
            let http = if context.process_environment {
                metadata_http(AZURE_IMDS_TIMEOUT)?
            } else {
                context.http
            };
            let fetcher = AzureManagedIdentity::from_env(context.env, http)?;
            (
                RefreshingCredentials::bootstrap(
                    fetcher,
                    RefreshPolicy::default(),
                    context.observer,
                )
                .await?,
                "azure-managed-identity",
            )
        } else {
            let fetcher = AzureWorkloadIdentity::from_env(context.env, context.http).ok_or(
                "Azure workload identity requires AZURE_CLIENT_ID, AZURE_TENANT_ID, and AZURE_FEDERATED_TOKEN_FILE in the environment")?;
            (
                RefreshingCredentials::bootstrap(
                    fetcher,
                    RefreshPolicy::default(),
                    context.observer,
                )
                .await?,
                "azure-workload-identity",
            )
        };
        Ok(ResolvedAzureCredentials {
            provider: crate::azure::AzureCredentials::Bearer(provider),
            source,
        })
    }
}

/// Resolve S3 AK/SK, IAM, or Broker credentials from the process environment.
pub async fn resolve_s3_credentials(
    keys: Option<S3Credentials>,
    selector: Option<&str>,
    http: Arc<dyn HttpClient>,
    observer: Arc<dyn CredentialsObserver>,
) -> Result<ResolvedS3Credentials, String> {
    resolve_credentials(
        S3Resolver(keys),
        ResolutionContext {
            env: &env_var,
            selector,
            http,
            observer,
            process_environment: true,
        },
    )
    .await
}

/// Protocol-test variant. Native SDK IAM discovery is tested in isolated processes.
#[cfg(test)]
pub(crate) async fn resolve_s3_credentials_with_env(
    env: EnvFn<'_>,
    keys: Option<S3Credentials>,
    selector: Option<&str>,
    http: Arc<dyn HttpClient>,
    observer: Arc<dyn CredentialsObserver>,
) -> Result<ResolvedS3Credentials, String> {
    resolve_credentials(
        S3Resolver(keys),
        ResolutionContext {
            env,
            selector,
            http,
            observer,
            process_environment: false,
        },
    )
    .await
}

/// Resolve GCS HMAC keys, IAM tokens, or Broker tokens.
pub async fn resolve_gcs_credentials(
    keys: Option<crate::gcs::GcsHmacCredentials>,
    selector: Option<&str>,
    http: Arc<dyn HttpClient>,
    observer: Arc<dyn CredentialsObserver>,
) -> Result<ResolvedGcsCredentials, String> {
    resolve_credentials(
        GcsResolver(keys),
        ResolutionContext {
            env: &env_var,
            selector,
            http,
            observer,
            process_environment: true,
        },
    )
    .await
}

/// Protocol-test variant. Native ADC discovery is tested in isolated processes.
#[cfg(test)]
pub(crate) async fn resolve_gcs_credentials_with_env(
    env: EnvFn<'_>,
    keys: Option<crate::gcs::GcsHmacCredentials>,
    selector: Option<&str>,
    http: Arc<dyn HttpClient>,
    observer: Arc<dyn CredentialsObserver>,
) -> Result<ResolvedGcsCredentials, String> {
    resolve_credentials(
        GcsResolver(keys),
        ResolutionContext {
            env,
            selector,
            http,
            observer,
            process_environment: false,
        },
    )
    .await
}

/// Resolve Azure account keys, IAM tokens, or Broker SAS. The configured account
/// is required for AK/SK and Broker; IAM selection follows the token-file rule.
pub async fn resolve_azure_credentials(
    account: &str,
    shared_key: Option<String>,
    selector: Option<&str>,
    http: Arc<dyn HttpClient>,
    observer: Arc<dyn CredentialsObserver>,
) -> Result<ResolvedAzureCredentials, String> {
    let from_env = env_var("TALON_ORIGIN_CREDENTIALS_SOURCE");
    resolve_credentials(
        AzureResolver {
            account,
            shared_key,
        },
        ResolutionContext {
            env: &env_var,
            selector: selector.or(from_env.as_deref()),
            http,
            observer,
            process_environment: true,
        },
    )
    .await
}

/// Injectable environment and HTTP variant of [`resolve_azure_credentials`].
#[cfg(test)]
pub(crate) async fn resolve_azure_credentials_with_env(
    env: EnvFn<'_>,
    account: &str,
    shared_key: Option<String>,
    selector: Option<&str>,
    http: Arc<dyn HttpClient>,
    observer: Arc<dyn CredentialsObserver>,
) -> Result<ResolvedAzureCredentials, String> {
    let from_env = env("TALON_ORIGIN_CREDENTIALS_SOURCE");
    resolve_credentials(
        AzureResolver {
            account,
            shared_key,
        },
        ResolutionContext {
            env,
            selector: selector.or(from_env.as_deref()),
            http,
            observer,
            process_environment: false,
        },
    )
    .await
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

    fn azure_bearer(resolved: &ResolvedAzureCredentials) -> Arc<dyn ProvideBearerToken> {
        match &resolved.provider {
            crate::AzureCredentials::Bearer(provider) => provider.clone(),
            _ => panic!("expected IAM bearer provider"),
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

    struct ImdsHttp {
        replies: std::sync::Mutex<std::collections::VecDeque<Result<HttpResponse, String>>>,
        requests: std::sync::Mutex<Vec<HttpRequest>>,
    }

    impl ImdsHttp {
        fn new(replies: Vec<Result<HttpResponse, String>>) -> Arc<Self> {
            Arc::new(Self {
                replies: std::sync::Mutex::new(replies.into()),
                requests: std::sync::Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait]
    impl HttpClient for ImdsHttp {
        async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, String> {
            self.requests.lock().unwrap().push(request);
            self.replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected HTTP request")
        }
    }

    fn imds_reply(status: u16, body: &str) -> Result<HttpResponse, String> {
        Ok(HttpResponse {
            status,
            headers: vec![],
            body: body.to_owned().into(),
        })
    }

    const IMDS_CREDS: &str = r#"{"Code":"Success","AccessKeyId":"INSTANCEAK","SecretAccessKey":"instance-secret","Token":"instance-session","Expiration":"2099-01-01T00:00:00Z"}"#;

    #[tokio::test]
    async fn aws_instance_profile_resolves_and_signs_without_static_keys() {
        use talon_core::{Backend, BackendStore, ObjectId};
        let http = ImdsHttp::new(vec![
            imds_reply(200, "metadata-token"),
            imds_reply(200, "instance-role\n"),
            imds_reply(200, IMDS_CREDS),
            Ok(HttpResponse {
                status: 200,
                headers: vec![
                    ("content-length".into(), "3".into()),
                    ("etag".into(), "\"v1\"".into()),
                ],
                body: bytes::Bytes::new(),
            }),
        ]);
        let env = env_map(&[(
            "AWS_EC2_METADATA_SERVICE_ENDPOINT",
            "http://metadata.test:8080/",
        )]);
        let resolved = ok(resolve_s3_credentials_with_env(
            &env,
            None,
            Some("aws-instance-profile"),
            http.clone(),
            Arc::new(crate::NoopObserver),
        )
        .await);
        assert_eq!(resolved.source, "aws-instance-profile");
        let backend = crate::S3Backend::with_credentials_provider(
            crate::S3Config::aws("us-west-2"),
            resolved.provider,
            http.clone(),
        );
        let stat = backend
            .head(&ObjectId::new(Backend::S3, "example-bucket", "key"))
            .await
            .unwrap();
        assert_eq!(stat.len, 3);
        let requests = http.requests.lock().unwrap();
        assert_eq!(requests.len(), 4);
        assert_eq!(requests[0].method, Method::Put);
        assert_eq!(
            requests[0].url,
            "http://metadata.test:8080/latest/api/token"
        );
        assert!(requests[0]
            .header("x-aws-ec2-metadata-token-ttl-seconds")
            .unwrap()
            .parse::<u32>()
            .is_ok_and(|ttl| (1..=21600).contains(&ttl)));
        for request in &requests[1..3] {
            assert_eq!(request.method, Method::Get);
            assert_eq!(
                request.header("x-aws-ec2-metadata-token"),
                Some("metadata-token")
            );
        }
        assert_eq!(
            requests[1].url,
            "http://metadata.test:8080/latest/meta-data/iam/security-credentials/"
        );
        assert_eq!(
            requests[2].url,
            "http://metadata.test:8080/latest/meta-data/iam/security-credentials/instance-role"
        );
        assert!(requests[3]
            .header("authorization")
            .unwrap()
            .contains("Credential=INSTANCEAK/"));
        assert!(requests[3]
            .header("authorization")
            .unwrap()
            .contains("/us-west-2/s3/aws4_request"));
        assert_eq!(
            requests[3].header("x-amz-security-token"),
            Some("instance-session")
        );
        assert!(requests[3].header("x-aws-ec2-metadata-token").is_none());
    }

    #[tokio::test]
    async fn aws_instance_profile_failure_does_not_fall_back_or_expose_response() {
        let http = ImdsHttp::new(vec![imds_reply(403, "private-metadata-value")]);
        let error = err(resolve_s3_credentials_with_env(
            &|_| None,
            Some(S3Credentials {
                access_key_id: "STATIC".into(),
                secret_access_key: "static-secret".into(),
                session_token: None,
            }),
            Some("aws-instance-profile"),
            http.clone(),
            Arc::new(crate::NoopObserver),
        )
        .await);
        assert!(error.contains("403"), "unexpected failure stage: {error}");
        assert!(!error.contains("private-metadata-value"));
        assert_eq!(http.requests.lock().unwrap().len(), 1);
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

    #[test]
    fn iso8601_preserves_timezone_offsets_and_fractional_seconds() {
        let expected = UNIX_EPOCH + Duration::from_millis(1_786_323_723_125);
        for text in [
            "2026-08-10T09:02:03.125+08:00",
            "2026-08-09T18:02:03.125-07:00",
            "2026-08-10T01:02:03.125Z",
            "  2026-08-10T01:02:03.125+00:00\n",
        ] {
            assert_eq!(parse_iso8601_utc(text), Some(expected));
        }
    }

    #[test]
    fn iso8601_rejects_invalid_calendar_dates() {
        for text in [
            "2026-02-29T01:02:03Z",
            "2026-02-30T01:02:03Z",
            "2026-04-31T01:02:03Z",
        ] {
            assert!(parse_iso8601_utc(text).is_none());
        }
        assert!(parse_iso8601_utc("2024-02-29T01:02:03Z").is_some());
    }

    #[test]
    fn iso8601_rejects_malformed_fraction_and_non_t_separator_without_panicking() {
        for text in [
            "2026-08-10T01:02:03.invalidZ",
            "2026-08-10T01:02:03.125garbageZ",
            "2026-08-10 01:02:03Z",
            "2026-08-10t01:02:03Z",
            "2026-08-10T01:02:03+25:00",
            "2026-08-1éT01:02:03Z",
            "",
            "🕒",
        ] {
            assert!(parse_iso8601_utc(text).is_none());
        }
    }

    #[test]
    fn iso8601_aliyun_timestamp_stays_utc_seconds_only() {
        assert_eq!(format_iso8601_utc(UNIX_EPOCH), "1970-01-01T00:00:00Z");
        assert_eq!(
            format_iso8601_utc(UNIX_EPOCH - Duration::from_secs(1)),
            "1970-01-01T00:00:00Z"
        );
        assert_eq!(
            format_iso8601_utc(UNIX_EPOCH + Duration::from_millis(1_786_323_723_125)),
            "2026-08-10T01:02:03Z"
        );
    }

    #[tokio::test]
    async fn aws_instance_profile_rejects_invalid_metadata_before_use() {
        for (token, role, credentials, expected, calls) in [
            ("", "role", IMDS_CREDS.to_owned(), "metadata token", 1),
            (
                "token\r\ninjected: value",
                "role",
                IMDS_CREDS.to_owned(),
                "metadata token",
                1,
            ),
            ("token", "", IMDS_CREDS.to_owned(), "role name", 2),
            ("token", ".", IMDS_CREDS.to_owned(), "role name", 2),
            ("token", "..", IMDS_CREDS.to_owned(), "role name", 2),
            (
                "token",
                "role-a\nrole-b",
                IMDS_CREDS.to_owned(),
                "role name",
                2,
            ),
            (
                "token",
                "../other?secret=value",
                IMDS_CREDS.to_owned(),
                "role name",
                2,
            ),
            (
                "token",
                "role",
                IMDS_CREDS.replace("Success", "Failure"),
                "acquisition failure",
                3,
            ),
            (
                "token",
                "role",
                IMDS_CREDS.replace("instance-session", ""),
                "Token",
                3,
            ),
            (
                "token",
                "role",
                IMDS_CREDS.replace("instance-secret", ""),
                "SecretAccessKey",
                3,
            ),
            (
                "token",
                "role",
                IMDS_CREDS.replace("INSTANCEAK", ""),
                "AccessKeyId",
                3,
            ),
            (
                "token",
                "role",
                IMDS_CREDS.replace("2099-01-01T00:00:00Z", "invalid"),
                "expiration",
                3,
            ),
            (
                "token",
                "role",
                IMDS_CREDS.replace("2099-01-01T00:00:00Z", "2000-01-01T00:00:00Z"),
                "expired",
                3,
            ),
            ("token", "role", "not-json-private-value".into(), "JSON", 3),
        ] {
            let http = ImdsHttp::new(vec![
                imds_reply(200, token),
                imds_reply(200, role),
                imds_reply(200, &credentials),
            ]);
            let error = err(resolve_s3_credentials_with_env(
                &|_| None,
                None,
                Some("aws-instance-profile"),
                http.clone(),
                Arc::new(crate::NoopObserver),
            )
            .await);
            assert!(
                error.contains(expected),
                "unexpected failure stage: {error}"
            );
            assert_eq!(http.requests.lock().unwrap().len(), calls);
            assert!(!error.contains("private-value"));
            assert!(!error.contains("instance-secret"));
            assert!(!error.contains("instance-session"));
        }
    }

    #[tokio::test]
    async fn aws_instance_profile_configuration_fails_before_network() {
        for (name, value, expected) in [
            ("AWS_EC2_METADATA_DISABLED", "TRUE", "disabled"),
            (
                "AWS_EC2_METADATA_SERVICE_ENDPOINT",
                "file:///private",
                "endpoint",
            ),
            (
                "AWS_EC2_METADATA_SERVICE_ENDPOINT",
                "http://user:private@metadata.test",
                "endpoint",
            ),
            (
                "AWS_EC2_METADATA_SERVICE_ENDPOINT",
                "http://metadata.test/?secret=private",
                "endpoint",
            ),
        ] {
            let http = ImdsHttp::new(vec![]);
            let env = env_map(&[(name, value)]);
            let error = err(resolve_s3_credentials_with_env(
                &env,
                None,
                Some("aws-instance-profile"),
                http.clone(),
                Arc::new(crate::NoopObserver),
            )
            .await);
            assert!(error.contains(expected));
            assert!(!error.contains("private"));
            assert!(http.requests.lock().unwrap().is_empty());
        }
        let http = ImdsHttp::new(vec![Err("transport-private-value".into())]);
        let error = err(resolve_s3_credentials_with_env(
            &|_| None,
            None,
            Some("aws-instance-profile"),
            http,
            Arc::new(crate::NoopObserver),
        )
        .await);
        assert!(error.contains("transport failed"));
        assert!(!error.contains("private-value"));
    }

    struct HangingHttp;
    #[async_trait]
    impl HttpClient for HangingHttp {
        async fn execute(&self, _: HttpRequest) -> Result<HttpResponse, String> {
            std::future::pending().await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn aws_instance_profile_bounds_an_unresponsive_metadata_service() {
        let error = err(resolve_s3_credentials_with_env(
            &|_| None,
            None,
            Some("aws-instance-profile"),
            Arc::new(HangingHttp),
            Arc::new(crate::NoopObserver),
        )
        .await);
        assert!(error.contains("timed out"));
    }

    #[tokio::test(start_paused = true)]
    async fn azure_managed_identity_bounds_an_unresponsive_metadata_service() {
        let env = env_map(&[("TALON_ORIGIN_CREDENTIALS_SOURCE", "azure-managed-identity")]);
        let error = err(resolve_azure_credentials_with_env(
            &env,
            "",
            None,
            None,
            Arc::new(HangingHttp),
            Arc::new(crate::NoopObserver),
        )
        .await);
        assert!(error.contains("timed out"));
    }

    #[tokio::test(start_paused = true)]
    async fn aws_instance_profile_refresh_recovers_without_per_request_fetches() {
        let expires: chrono::DateTime<chrono::Utc> =
            (SystemTime::now() + Duration::from_secs(60)).into();
        let initial = IMDS_CREDS.replace("2099-01-01T00:00:00Z", &expires.to_rfc3339());
        let http = ImdsHttp::new(vec![
            imds_reply(200, "metadata-token-1"),
            imds_reply(200, "role-1"),
            imds_reply(200, &initial),
            imds_reply(401, "private-error"),
            imds_reply(200, "metadata-token-2"),
            imds_reply(200, "role-2"),
            imds_reply(200, &IMDS_CREDS.replace("INSTANCEAK", "ROTATEDAK")),
        ]);
        let resolved = ok(resolve_s3_credentials_with_env(
            &|_| None,
            None,
            Some("aws-instance-profile"),
            http.clone(),
            Arc::new(crate::NoopObserver),
        )
        .await);
        let initial = resolved.provider.current();
        for _ in 0..10 {
            assert!(Arc::ptr_eq(&initial, &resolved.provider.current()));
        }
        assert_eq!(http.requests.lock().unwrap().len(), 3);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(31)).await;
        tokio::task::yield_now().await;
        assert_eq!(http.requests.lock().unwrap().len(), 4);
        assert!(Arc::ptr_eq(&initial, &resolved.provider.current()));
        tokio::time::advance(Duration::from_secs(31)).await;
        tokio::task::yield_now().await;
        assert_eq!(resolved.provider.current().access_key_id, "ROTATEDAK");
        let requests = http.requests.lock().unwrap();
        assert_eq!(requests.len(), 7);
        assert_eq!(requests[4].method, Method::Put);
        assert_eq!(
            requests[6].header("x-aws-ec2-metadata-token"),
            Some("metadata-token-2")
        );
        assert!(requests[6].url.ends_with("/role-2"));
    }

    #[tokio::test]
    #[ignore = "requires EC2 IMDSv2 access and a readable S3 bucket; read-only"]
    async fn live_aws_iam_reads_one_bucket() {
        use talon_core::Backend;
        let bucket = std::env::var("TALON_LIVE_IAM_BUCKET").expect("set TALON_LIVE_IAM_BUCKET");
        let region = std::env::var("TALON_LIVE_IAM_REGION").expect("set TALON_LIVE_IAM_REGION");
        tokio::time::timeout(Duration::from_secs(120), async {
            let http: Arc<dyn HttpClient> = Arc::new(crate::ReqwestClient::new());
            let resolved = resolve_s3_credentials(None, Some("iam"), http.clone(), Arc::new(crate::NoopObserver))
                .await.unwrap_or_else(|_| panic!("real EC2 IAM acquisition failed (details suppressed)"));
            assert_eq!(resolved.source, "aws-default");
            let backend = crate::S3Backend::with_credentials_provider(crate::S3Config::aws(region), resolved.provider, http);
            live_identity_read(&backend, Backend::S3, &bucket, "TALON_LIVE_IAM_KEY").await;
            eprintln!("EC2 instance profile: acquisition, S3 LIST, HEAD and conditional range GET succeeded");
        }).await.expect("real IAM read deadline exceeded");
    }

    #[tokio::test]
    #[ignore = "requires GCE/GKE metadata access and a readable GCS bucket; read-only"]
    async fn live_gcp_iam_reads_one_bucket() {
        let bucket = std::env::var("TALON_LIVE_GCP_BUCKET").expect("set TALON_LIVE_GCP_BUCKET");
        tokio::time::timeout(Duration::from_secs(120), async {
            let http: Arc<dyn HttpClient> = Arc::new(crate::ReqwestClient::new());
            let resolved = resolve_gcs_credentials(None, Some("iam"), http.clone(), Arc::new(crate::NoopObserver))
                .await.unwrap_or_else(|_| panic!("real GCP metadata acquisition failed (details suppressed)"));
            assert_eq!(resolved.source, "gcp-adc");
            let backend = crate::GcsBackend::with_credentials(crate::GcsConfig::default(), resolved.provider, http);
            live_identity_read(&backend, talon_core::Backend::Gcs, &bucket, "TALON_LIVE_GCP_KEY").await;
            eprintln!("GCP metadata identity: acquisition, GCS LIST, HEAD and conditional range GET succeeded");
        }).await.expect("real GCP metadata read deadline exceeded");
    }

    struct LiveOriginHttp(crate::ReqwestClient);

    #[async_trait::async_trait]
    impl HttpClient for LiveOriginHttp {
        async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, String> {
            let method = request.method;
            let response = self.0.execute(request).await?;
            // Print only the status and service error code, never credentials,
            // signed URLs, object names, or the response body.
            eprintln!("origin {method:?} HTTP {}", response.status);
            if !response.is_success() {
                let body = String::from_utf8_lossy(&response.body);
                if let Some(code) = body
                    .split_once("<Code>")
                    .and_then(|(_, s)| s.split_once("</Code>").map(|(code, _)| code))
                {
                    if code.len() < 80 && code.bytes().all(|b| b.is_ascii_alphanumeric()) {
                        eprintln!("origin service error: {code}");
                    }
                }
            }
            Ok(response)
        }
    }

    #[tokio::test]
    #[ignore = "requires real S3-compatible AK/SK and a readable bucket; read-only"]
    async fn live_s3_aksk_reads_one_bucket() {
        let bucket = std::env::var("TALON_LIVE_AKSK_BUCKET").expect("set TALON_LIVE_AKSK_BUCKET");
        let config = crate::S3Config {
            region: std::env::var("TALON_LIVE_AKSK_REGION").expect("set TALON_LIVE_AKSK_REGION"),
            endpoint: std::env::var("TALON_LIVE_AKSK_ENDPOINT")
                .expect("set TALON_LIVE_AKSK_ENDPOINT"),
            path_style: false,
            tls: true,
        };
        let keys = S3Credentials {
            access_key_id: std::env::var("TALON_LIVE_ACCESS_KEY")
                .expect("set TALON_LIVE_ACCESS_KEY"),
            secret_access_key: std::env::var("TALON_LIVE_SECRET_KEY")
                .expect("set TALON_LIVE_SECRET_KEY"),
            session_token: None,
        };
        tokio::time::timeout(Duration::from_secs(120), async {
            let http: Arc<dyn HttpClient> = Arc::new(LiveOriginHttp(crate::ReqwestClient::new()));
            let resolved = resolve_s3_credentials(
                Some(keys),
                Some("aksk"),
                http.clone(),
                Arc::new(crate::NoopObserver),
            )
            .await
            .unwrap_or_else(|_| panic!("AK/SK resolution failed (details suppressed)"));
            assert_eq!(resolved.source, "aksk");
            let backend =
                crate::S3Backend::with_credentials_provider(config, resolved.provider, http);
            live_identity_read(
                &backend,
                talon_core::Backend::S3,
                &bucket,
                "TALON_LIVE_AKSK_KEY",
            )
            .await;
        })
        .await
        .expect("real AK/SK read deadline exceeded");
    }

    #[tokio::test]
    #[ignore = "requires real GCS HMAC keys and a readable bucket; read-only"]
    async fn live_gcp_aksk_reads_one_bucket() {
        let bucket = std::env::var("TALON_LIVE_AKSK_BUCKET").expect("set TALON_LIVE_AKSK_BUCKET");
        let keys = crate::GcsHmacCredentials {
            access_key_id: std::env::var("TALON_LIVE_ACCESS_KEY")
                .expect("set TALON_LIVE_ACCESS_KEY"),
            secret_access_key: std::env::var("TALON_LIVE_SECRET_KEY")
                .expect("set TALON_LIVE_SECRET_KEY"),
        };
        tokio::time::timeout(Duration::from_secs(120), async {
            let http: Arc<dyn HttpClient> = Arc::new(LiveOriginHttp(crate::ReqwestClient::new()));
            let resolved = resolve_gcs_credentials(
                Some(keys),
                Some("aksk"),
                http.clone(),
                Arc::new(crate::NoopObserver),
            )
            .await
            .unwrap_or_else(|_| panic!("GCS HMAC resolution failed (details suppressed)"));
            assert_eq!(resolved.source, "aksk");
            let backend = crate::GcsBackend::with_credentials(
                crate::GcsConfig::default(),
                resolved.provider,
                http,
            );
            live_identity_read(
                &backend,
                talon_core::Backend::Gcs,
                &bucket,
                "TALON_LIVE_AKSK_KEY",
            )
            .await;
        })
        .await
        .expect("real GCS HMAC read deadline exceeded");
    }

    #[tokio::test]
    #[ignore = "requires a real Azure account key and a readable container; read-only"]
    async fn live_azure_aksk_reads_one_container() {
        let account = std::env::var("TALON_LIVE_ACCESS_KEY").expect("set TALON_LIVE_ACCESS_KEY");
        let key = std::env::var("TALON_LIVE_SECRET_KEY").expect("set TALON_LIVE_SECRET_KEY");
        let container =
            std::env::var("TALON_LIVE_AKSK_BUCKET").expect("set TALON_LIVE_AKSK_BUCKET");
        tokio::time::timeout(Duration::from_secs(120), async {
            let http: Arc<dyn HttpClient> = Arc::new(LiveOriginHttp(crate::ReqwestClient::new()));
            let resolved = resolve_azure_credentials(
                &account,
                Some(key),
                Some("aksk"),
                http.clone(),
                Arc::new(crate::NoopObserver),
            )
            .await
            .unwrap_or_else(|_| panic!("Azure Shared Key resolution failed (details suppressed)"));
            assert_eq!(resolved.source, "aksk");
            let backend = crate::AzureBackend::with_credentials(
                crate::AzureConfig::new(account),
                resolved.provider,
                http,
            );
            live_identity_read(
                &backend,
                talon_core::Backend::Azure,
                &container,
                "TALON_LIVE_AKSK_KEY",
            )
            .await;
        })
        .await
        .expect("real Azure Shared Key read deadline exceeded");
    }

    async fn live_identity_read(
        backend: &dyn talon_core::BackendStore,
        kind: talon_core::Backend,
        bucket: &str,
        key_env: &str,
    ) {
        let mut key = std::env::var(key_env).ok().filter(|s| !s.is_empty());
        let mut cursor = None;
        for _ in 0..3 {
            let page = backend
                .list_objects(bucket, "", cursor.as_deref(), 100)
                .await
                .unwrap_or_else(|_| {
                    panic!("real identity origin LIST failed (details suppressed)")
                });
            if key.is_none() {
                key = page.objects.into_iter().find(|o| o.size > 0).map(|o| o.key);
            }
            if key.is_some() || page.next.is_none() {
                break;
            }
            cursor = page.next;
        }
        let object = talon_core::ObjectId::new(
            kind,
            bucket,
            key.unwrap_or_else(|| panic!("no nonempty object found; set {key_env}")),
        );
        let stat = backend
            .head(&object)
            .await
            .unwrap_or_else(|_| panic!("real identity origin HEAD failed (details suppressed)"));
        assert!(stat.len > 0 && !stat.version.as_str().is_empty());
        let len = stat.len.min(1024);
        let body = backend
            .fetch_range_if_match(&object, 0, len, Some(&stat.version))
            .await
            .unwrap_or_else(|_| {
                panic!("real identity conditional GET failed (details suppressed)")
            });
        assert_eq!(body.len() as u64, len);
    }

    #[tokio::test]
    #[ignore = "requires Azure workload/managed identity and a readable Blob container; read-only"]
    async fn live_azure_iam_reads_one_container() {
        assert_eq!(
            env_var("TALON_ORIGIN_CREDENTIALS_SOURCE").as_deref(),
            Some("iam"),
            "explicitly select iam for this live test"
        );
        let account =
            std::env::var("TALON_LIVE_AZURE_ACCOUNT").expect("set TALON_LIVE_AZURE_ACCOUNT");
        let container =
            std::env::var("TALON_LIVE_AZURE_CONTAINER").expect("set TALON_LIVE_AZURE_CONTAINER");
        tokio::time::timeout(Duration::from_secs(120), async {
            let http: Arc<dyn HttpClient> = Arc::new(crate::ReqwestClient::new());
            let resolved = resolve_azure_credentials(
                &account,
                None,
                Some("iam"),
                http.clone(),
                Arc::new(crate::NoopObserver),
            )
            .await
            .unwrap_or_else(|_| {
                panic!("real Azure managed identity acquisition failed (details suppressed)")
            });
            assert_eq!(
                resolved.source,
                if env_var("AZURE_FEDERATED_TOKEN_FILE").is_some() {
                    "azure-workload-identity"
                } else {
                    "azure-managed-identity"
                }
            );
            let backend = crate::AzureBackend::with_credentials(
                crate::AzureConfig::new(account),
                resolved.provider,
                http,
            );
            live_identity_read(
                &backend,
                talon_core::Backend::Azure,
                &container,
                "TALON_LIVE_AZURE_KEY",
            )
            .await;
            eprintln!(
                "Azure IAM ({}): acquisition, Blob LIST, HEAD and conditional range GET succeeded",
                resolved.source
            );
        })
        .await
        .expect("real Azure managed identity read deadline exceeded");
    }

    const AZURE_IMDS_TOKEN: &str = r#"{"access_token":"managed-token","expires_in":"3600","expires_on":"4102444800","token_type":"Bearer","resource":"https://storage.azure.com/"}"#;

    #[tokio::test]
    async fn azure_iam_selects_identity_like_milvus_storage() {
        let temp = tempfile::TempDir::new().unwrap();
        let token_path = token_file(&temp, "pod-assertion");
        for token_path in [None, Some(""), Some(token_path.as_str())] {
            let workload = token_path.is_some_and(|path| !path.is_empty());
            let http = MockHttp::new(
                200,
                if workload {
                    r#"{"access_token":"pod-token","expires_in":3600}"#
                } else {
                    AZURE_IMDS_TOKEN
                },
            );
            let env = |name: &str| match name {
                "TALON_ORIGIN_CREDENTIALS_SOURCE" => Some("iam".into()),
                "AZURE_CLIENT_ID" => Some("client-1".into()),
                "AZURE_TENANT_ID" => Some("tenant-1".into()),
                "AZURE_FEDERATED_TOKEN_FILE" => token_path.map(str::to_owned),
                _ => None,
            };
            let resolved = ok(resolve_azure_credentials_with_env(
                &env,
                "",
                None,
                None,
                http.clone(),
                Arc::new(crate::NoopObserver),
            )
            .await);
            assert_eq!(
                resolved.source,
                if workload {
                    "azure-workload-identity"
                } else {
                    "azure-managed-identity"
                }
            );
            let request = http.request();
            assert_eq!(
                request.method,
                if workload { Method::Post } else { Method::Get }
            );
            if workload {
                assert!(request.url.contains("login.microsoftonline.com/tenant-1/"));
            } else {
                assert!(request.url.contains("/metadata/identity/oauth2/token"));
                assert!(request.url.contains("client_id=client-1"));
            }
        }
    }

    #[tokio::test]
    async fn azure_iam_requires_client_id_before_metadata() {
        let http = MockHttp::new(200, AZURE_IMDS_TOKEN);
        let env = env_map(&[("TALON_ORIGIN_CREDENTIALS_SOURCE", "iam")]);
        let error = err(resolve_azure_credentials_with_env(
            &env,
            "",
            None,
            None,
            http.clone(),
            Arc::new(crate::NoopObserver),
        )
        .await);
        assert!(error.contains("AZURE_CLIENT_ID"));
        assert!(http.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn azure_managed_identity_selects_storage_identity_and_authenticates_origin() {
        use talon_core::{Backend, BackendStore, ObjectId};
        for client_id in [None, Some("11111111-1111-1111-1111-111111111111")] {
            let http = ImdsHttp::new(vec![
                imds_reply(200, AZURE_IMDS_TOKEN),
                Ok(HttpResponse {
                    status: 200,
                    headers: vec![
                        ("content-length".into(), "3".into()),
                        ("etag".into(), "\"v1\"".into()),
                    ],
                    body: bytes::Bytes::new(),
                }),
            ]);
            let env = |name: &str| match name {
                "TALON_ORIGIN_CREDENTIALS_SOURCE" => Some("azure-managed-identity".into()),
                "TALON_AZURE_IMDS_ENDPOINT" => Some("http://metadata.test:8080".into()),
                "AZURE_CLIENT_ID" => client_id.map(str::to_owned),
                _ => None,
            };
            let resolved = ok(resolve_azure_credentials_with_env(
                &env,
                "",
                None,
                None,
                http.clone(),
                Arc::new(crate::NoopObserver),
            )
            .await);
            assert_eq!(resolved.source, "azure-managed-identity");
            let backend = crate::AzureBackend::with_credentials(
                crate::AzureConfig::new("fixtureaccount"),
                resolved.provider,
                http.clone(),
            );
            let stat = backend
                .head(&ObjectId::new(Backend::Azure, "example-container", "key"))
                .await
                .unwrap();
            assert_eq!(stat.len, 3);
            let requests = http.requests.lock().unwrap();
            let url = url::Url::parse(&requests[0].url).unwrap();
            assert_eq!(url.host_str(), Some("metadata.test"));
            assert_eq!(url.path(), "/metadata/identity/oauth2/token");
            let query: HashMap<_, _> = url.query_pairs().collect();
            assert_eq!(
                query.get("api-version").map(|v| v.as_ref()),
                Some("2018-02-01")
            );
            assert_eq!(
                query.get("resource").map(|v| v.as_ref()),
                Some("https://storage.azure.com/")
            );
            assert_eq!(query.get("client_id").map(|v| v.as_ref()), client_id);
            assert_eq!(requests[0].method, Method::Get);
            assert_eq!(requests[0].header("metadata"), Some("true"));
            assert!(requests[0].header("authorization").is_none());
            assert_eq!(
                requests[1].header("authorization"),
                Some("Bearer managed-token")
            );
            assert!(requests[1].header("metadata").is_none());
        }
    }

    #[tokio::test]
    async fn azure_managed_identity_uses_absolute_expiry_and_rejects_invalid_tokens() {
        for expiry in [
            serde_json::json!("4102444800"),
            serde_json::json!(4102444800_u64),
        ] {
            let mut reply: serde_json::Value = serde_json::from_str(AZURE_IMDS_TOKEN).unwrap();
            reply["expires_on"] = expiry;
            reply["expires_in"] = serde_json::json!("1");
            let http = ImdsHttp::new(vec![imds_reply(200, &reply.to_string())]);
            let fetcher = ok(AzureManagedIdentity::from_env(&|_| None, http));
            let (_, expires) = ok(fetcher.fetch().await);
            assert_eq!(expires, Some(UNIX_EPOCH + Duration::from_secs(4102444800)));
        }
        for (field, bad_value) in [
            ("access_token", serde_json::json!("")),
            (
                "access_token",
                serde_json::json!("private-token\r\ninjected"),
            ),
            ("access_token", serde_json::Value::Null),
            ("token_type", serde_json::json!("Basic")),
            (
                "resource",
                serde_json::json!("https://management.azure.com/"),
            ),
            ("expires_on", serde_json::json!("invalid")),
            ("expires_on", serde_json::json!("1")),
            ("expires_on", serde_json::json!(-1)),
            ("expires_on", serde_json::json!(u64::MAX)),
            ("expires_on", serde_json::Value::Null),
        ] {
            let mut reply: serde_json::Value = serde_json::from_str(AZURE_IMDS_TOKEN).unwrap();
            reply[field] = bad_value;
            let http = ImdsHttp::new(vec![imds_reply(200, &reply.to_string())]);
            let fetcher = ok(AzureManagedIdentity::from_env(&|_| None, http));
            let error = err(fetcher.fetch().await);
            assert!(!error.contains("private-token"));
            assert!(!error.contains("managed-token"));
        }
    }

    #[tokio::test]
    async fn azure_managed_identity_errors_do_not_fall_back_or_echo_contents() {
        let env = env_map(&[
            ("TALON_ORIGIN_CREDENTIALS_SOURCE", "azure-managed-identity"),
            ("AZURE_CLIENT_ID", "11111111-1111-1111-1111-111111111111"),
            ("AZURE_TENANT_ID", "unused-tenant"),
            ("AZURE_FEDERATED_TOKEN_FILE", "/must-not-read-this-file"),
        ]);
        for reply in [
            imds_reply(400, "private-response"),
            imds_reply(429, "private-response"),
            imds_reply(302, "private-response"),
            imds_reply(200, "private-response"),
            Err("private-transport".into()),
        ] {
            let http = ImdsHttp::new(vec![reply]);
            let error = err(resolve_azure_credentials_with_env(
                &env,
                "",
                None,
                None,
                http.clone(),
                Arc::new(crate::NoopObserver),
            )
            .await);
            assert!(error.contains("initial azure-managed-identity"));
            assert!(!error.contains("private-"));
            assert!(!error.contains("must-not-read"));
            assert_eq!(http.requests.lock().unwrap().len(), 1);
        }
        for (name, value) in [
            (
                "TALON_AZURE_IMDS_ENDPOINT",
                "http://user:private@metadata.test",
            ),
            ("TALON_AZURE_IMDS_ENDPOINT", "http://metadata.test/path"),
            ("AZURE_CLIENT_ID", "private\nidentity"),
        ] {
            let http = ImdsHttp::new(vec![]);
            let env = env_map(&[(name, value)]);
            let error = err(AzureManagedIdentity::from_env(&env, http.clone()));
            assert!(!error.contains("private"));
            assert!(http.requests.lock().unwrap().is_empty());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn azure_managed_identity_refresh_recovers_without_on_demand_exchange() {
        let expires = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 60;
        let http = ImdsHttp::new(vec![
            imds_reply(
                200,
                &AZURE_IMDS_TOKEN.replace("4102444800", &expires.to_string()),
            ),
            imds_reply(503, "private-error"),
            imds_reply(
                200,
                &AZURE_IMDS_TOKEN.replace("managed-token", "rotated-token"),
            ),
        ]);
        let env = env_map(&[("TALON_ORIGIN_CREDENTIALS_SOURCE", "azure-managed-identity")]);
        let resolved = ok(resolve_azure_credentials_with_env(
            &env,
            "",
            None,
            None,
            http.clone(),
            Arc::new(crate::NoopObserver),
        )
        .await);
        let initial = azure_bearer(&resolved).current().unwrap();
        for _ in 0..10 {
            assert!(Arc::ptr_eq(
                &initial,
                &azure_bearer(&resolved).current().unwrap()
            ));
        }
        assert_eq!(http.requests.lock().unwrap().len(), 1);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(31)).await;
        tokio::task::yield_now().await;
        assert_eq!(http.requests.lock().unwrap().len(), 2);
        assert!(Arc::ptr_eq(
            &initial,
            &azure_bearer(&resolved).current().unwrap()
        ));
        tokio::time::advance(Duration::from_secs(31)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            azure_bearer(&resolved).current().unwrap().as_str(),
            "rotated-token"
        );
        assert_eq!(http.requests.lock().unwrap().len(), 3);
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
    async fn aws_invalid_calendar_expiry_keeps_credentials_with_unknown_expiry() {
        let temp = tempfile::TempDir::new().unwrap();
        let http = MockHttp::new(
            200,
            &AWS_STS_OK.replace("2026-08-10T13:00:00Z", "2026-02-30T13:00:00Z"),
        );
        let env = env_map(&[
            ("AWS_ROLE_ARN", "arn:aws:iam::1:role/talon"),
            (
                "AWS_WEB_IDENTITY_TOKEN_FILE",
                &token_file(&temp, "fixture-token"),
            ),
        ]);
        let fetcher = AwsWebIdentity::from_env(&env, http).unwrap();
        let (credentials, expiry) = ok(fetcher.fetch().await);
        assert_eq!(credentials.access_key_id, "ASIAEXAMPLE");
        assert!(expiry.is_none());
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
    async fn broker_selector_fetches_instead_of_using_static_credentials() {
        let http = MockHttp::new(
            200,
            r#"{"success":true,"credentials":{"tempAk":"BROKERAK","tempSk":"secret","sessionToken":"token","expiredAt":"2099-01-01T00:00:00Z"}}"#,
        );
        let env = env_map(&[
            (
                "TALON_CREDENTIAL_BROKER_URL",
                "https://broker.example/credentials",
            ),
            (
                "TALON_CREDENTIAL_BROKER_REQUEST",
                r#"{"csp":"aws","region":"us-east-1","roleArn":"arn:aws:iam::123456789012:role/example"}"#,
            ),
        ]);
        let resolved = ok(resolve_s3_credentials_with_env(
            &env,
            Some(S3Credentials {
                access_key_id: "STATIC".into(),
                secret_access_key: "static-secret".into(),
                session_token: None,
            }),
            Some("broker"),
            http.clone(),
            Arc::new(crate::credentials::NoopObserver),
        )
        .await);
        assert_eq!(resolved.provider.current().access_key_id, "BROKERAK");
        assert_eq!(http.request().method, Method::Post);
    }

    #[tokio::test]
    async fn aksk_mode_uses_keys_without_an_identity_exchange() {
        let http = MockHttp::new(500, "must not be contacted");
        let resolved = ok(resolve_s3_credentials_with_env(
            &|_| None,
            Some(S3Credentials {
                access_key_id: "AKSK".into(),
                secret_access_key: "secret".into(),
                session_token: None,
            }),
            Some("aksk"),
            http.clone(),
            Arc::new(crate::NoopObserver),
        )
        .await);
        assert_eq!(resolved.source, "aksk");
        assert_eq!(resolved.provider.current().access_key_id, "AKSK");
        assert!(http.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn azure_modes_keep_account_keys_and_broker_sas_distinct() {
        let http = MockHttp::new(
            200,
            r#"{"success":true,"credentials":{"tempAk":"account","tempSk":"unused","sessionToken":"sv=2026&sig=broker","expiredAt":"2099-01-01T00:00:00Z"}}"#,
        );
        let env = env_map(&[
            (
                "TALON_CREDENTIAL_BROKER_URL",
                "https://broker.example/v1/credentials/assume-role",
            ),
            (
                "TALON_CREDENTIAL_BROKER_REQUEST",
                r#"{"csp":"azure","region":"westus3","bucket":"container","azureClientId":"client","azureTenantId":"tenant","azureAccountName":"account"}"#,
            ),
            ("AZURE_FEDERATED_TOKEN_FILE", "/must-not-read"),
        ]);
        let resolved = ok(resolve_azure_credentials_with_env(
            &env,
            "account",
            Some("c2VjcmV0".into()),
            Some("aksk"),
            http.clone(),
            Arc::new(crate::NoopObserver),
        )
        .await);
        assert_eq!(resolved.source, "aksk");
        assert!(
            matches!(resolved.provider, crate::AzureCredentials::SharedKey(key) if key == "c2VjcmV0")
        );
        assert!(http.requests.lock().unwrap().is_empty());

        let resolved = ok(resolve_azure_credentials_with_env(
            &env,
            "account",
            Some("ignored-key".into()),
            Some("broker"),
            http.clone(),
            Arc::new(crate::NoopObserver),
        )
        .await);
        assert_eq!(resolved.source, "broker");
        let crate::AzureCredentials::BrokerSas(provider) = resolved.provider else {
            panic!("expected Broker SAS")
        };
        assert!(provider.current().expose_secret().contains("sig=broker"));
        let requests = http.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, Method::Post);
    }

    #[test]
    fn credential_modes_share_default_and_explicit_precedence() {
        for has_keys in [false, true] {
            assert_eq!(
                ok(CredentialMode::select(Some("aksk"), has_keys)),
                CredentialMode::AkSk
            );
            assert_eq!(
                ok(CredentialMode::select(Some("iam"), has_keys)),
                CredentialMode::Iam
            );
            assert_eq!(
                ok(CredentialMode::select(Some("broker"), has_keys)),
                CredentialMode::Broker
            );
            assert_eq!(
                ok(CredentialMode::select(None, has_keys)),
                if has_keys {
                    CredentialMode::AkSk
                } else {
                    CredentialMode::Iam
                }
            );
        }
        assert!(CredentialMode::select(Some("bearer"), false).is_err());
        assert!(CredentialMode::select(Some("sas"), false).is_err());
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
        assert_eq!(resolved.source, "aksk");
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
    async fn gcs_resolution_distinguishes_keys_from_iam_tokens() {
        let http = MockHttp::new(200, r#"{"access_token":"ya29","expires_in":600}"#);
        let env = env_map(&[]);
        let resolved = ok(resolve_gcs_credentials_with_env(
            &env,
            Some(crate::GcsHmacCredentials {
                access_key_id: "hmac-id".into(),
                secret_access_key: "hmac-secret".into(),
            }),
            Some("aksk"),
            http.clone(),
            Arc::new(crate::NoopObserver),
        )
        .await);
        assert_eq!(resolved.source, "aksk");
        let crate::GcsCredentials::Hmac(keys) = resolved.provider else {
            panic!("expected HMAC keys")
        };
        assert_eq!(keys.access_key_id, "hmac-id");
        assert!(http.requests.lock().unwrap().is_empty());
        let resolved = ok(resolve_gcs_credentials_with_env(
            &env,
            None,
            Some("gcp-metadata"),
            http,
            Arc::new(crate::NoopObserver),
        )
        .await);
        let crate::GcsCredentials::Bearer(provider) = resolved.provider else {
            panic!("expected IAM bearer")
        };
        assert_eq!(provider.current().unwrap().as_str(), "ya29");
    }
}
