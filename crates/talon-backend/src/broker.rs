//! Explicit single-origin Cloud Credential Broker acquisition.
//!
//! The endpoint and request are operator configuration. Neither broker replies
//! nor transport errors are included in errors: both can carry credentials.
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use serde_json::Value;

use crate::credentials::{
    CredentialsFetch, CredentialsObserver, ProvideSasToken, RefreshPolicy, RefreshingCredentials,
};
use crate::http::{HttpClient, HttpRequest, Method};
use crate::workload_identity::{env_var, EnvFn};
use crate::{AzureSas, S3Credentials};

/// Broker fetcher for S3 credentials, a GCP bearer token, or an Azure SAS.
/// Construction validates configuration before any network request.
pub(crate) struct BrokerFetch<T> {
    url: String,
    request: Value,
    http: Arc<dyn HttpClient>,
    value: PhantomData<T>,
}

impl<T> BrokerFetch<T> {
    pub(crate) fn from_env(
        env: EnvFn<'_>,
        csp: &str,
        account: Option<&str>,
        http: Arc<dyn HttpClient>,
    ) -> Result<Self, String> {
        let url = env("TALON_CREDENTIAL_BROKER_URL")
            .ok_or("broker requires TALON_CREDENTIAL_BROKER_URL")?;
        let endpoint =
            url::Url::parse(&url).map_err(|_| "broker endpoint must be a full HTTP(S) URL")?;
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
        let raw = env("TALON_CREDENTIAL_BROKER_REQUEST")
            .ok_or("broker requires TALON_CREDENTIAL_BROKER_REQUEST")?;
        let mut request: Value =
            serde_json::from_str(&raw).map_err(|_| "broker request must be a JSON object")?;
        if !request.is_object() {
            return Err("broker request must be a JSON object".into());
        }
        if request.get("csp").and_then(Value::as_str) != Some(csp) {
            return Err("broker request csp does not match selected backend".into());
        }
        let required: &[&str] = match csp {
            "aws" => &["region", "roleArn"],
            "gcp" => &["gcpServiceAccountEmail"],
            "azure" => &[
                "region",
                "bucket",
                "azureClientId",
                "azureTenantId",
                "azureAccountName",
            ],
            _ => return Err("unsupported broker csp".into()),
        };
        for field in required {
            required_string(&request, field)?;
        }
        if csp == "azure" && request.get("azureAccountName").and_then(Value::as_str) != account {
            return Err("broker request Azure account does not match configured backend".into());
        }
        let duration = match request.get("durationSeconds") {
            None => 3600,
            Some(value) => match value.as_i64() {
                Some(seconds) if seconds <= 0 => 3600,
                Some(seconds) => seconds,
                None => return Err("broker durationSeconds must be an integer".into()),
            },
        };
        if csp == "aws" && !(900..=3600).contains(&duration) {
            return Err("broker AWS durationSeconds must be in 900..3600".into());
        }
        request["durationSeconds"] = duration.into();
        Ok(Self {
            url,
            request,
            http,
            value: PhantomData,
        })
    }

    async fn exchange(&self) -> Result<(Value, Option<SystemTime>), String> {
        let request = HttpRequest::with_body(
            Method::Post,
            self.url.clone(),
            vec![("content-type".into(), "application/json".into())],
            bytes::Bytes::from(self.request.to_string()),
        );
        let response = tokio::time::timeout(Duration::from_secs(75), self.http.execute(request))
            .await
            .map_err(|_| "broker request timed out")?
            .map_err(|_| "broker transport failed")?;
        if !response.is_success() {
            return Err(format!("broker returned HTTP status {}", response.status));
        }
        let body: Value = serde_json::from_slice(&response.body)
            .map_err(|_| "broker response is not valid JSON")?;
        if body.get("success").and_then(Value::as_bool) != Some(true) {
            return Err("broker reported failure".into());
        }
        let credentials = body
            .get("credentials")
            .filter(|v| v.is_object())
            .ok_or("broker response is missing credentials")?
            .clone();
        // A CSP field is optional in the documented reply. If an issuer supplies
        // it, do not silently accept credentials for a different cloud.
        for value in [body.get("csp"), credentials.get("csp")]
            .into_iter()
            .flatten()
        {
            if Some(value) != self.request.get("csp") {
                return Err("broker response csp does not match selected backend".into());
            }
        }
        let expiry = required_string(&credentials, "expiredAt")?;
        let parsed = chrono::DateTime::parse_from_rfc3339(expiry)
            .map_err(|_| "broker expiredAt is not a valid RFC3339 date")?;
        let expiry: SystemTime = parsed.into();
        if expiry <= SystemTime::now() {
            return Err("broker credentials are expired".into());
        }
        Ok((credentials, Some(expiry)))
    }
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
                access_key_id: required_string(&value, "tempAk")?.into(),
                secret_access_key: required_string(&value, "tempSk")?.into(),
                session_token: Some(required_string(&value, "sessionToken")?.into()),
            },
            expiry,
        ))
    }
    fn source(&self) -> &'static str {
        "broker"
    }
}

#[async_trait]
impl CredentialsFetch for BrokerFetch<String> {
    type Value = String;
    async fn fetch(&self) -> Result<(Self::Value, Option<SystemTime>), String> {
        let (value, expiry) = self.exchange().await?;
        Ok((required_string(&value, "sessionToken")?.into(), expiry))
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
        if Some(required_string(&value, "tempAk")?)
            != self.request.get("azureAccountName").and_then(Value::as_str)
        {
            return Err("broker response Azure account does not match configured backend".into());
        }
        let sas = AzureSas::new(required_string(&value, "sessionToken")?)?;
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
    let fetcher = BrokerFetch::<AzureSas>::from_env(env, "azure", Some(account), http)?;
    Ok(RefreshingCredentials::bootstrap(fetcher, RefreshPolicy::default(), observer).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::NoopObserver;
    use crate::workload_identity::{resolve_gcs_bearer_with_env, resolve_s3_credentials_with_env};
    use crate::{
        AzureBackend, AzureConfig, GcsBackend, GcsConfig, HttpResponse, S3Backend, S3Config,
    };
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use talon_core::{Backend, BackendStore, ObjectId};

    // Only the external HTTP seam is scripted. Resolvers, refresh loops and
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

    #[tokio::test]
    async fn broker_aws_posts_documented_fields_and_signs_storage_requests() {
        let http = http(vec![response(
            200,
            credentials("BROKERAK", "broker-token", "2099-01-01T08:00:00.123+08:00"),
        )]);
        let resolved = ok(resolve_s3_credentials_with_env(
            &env(aws_request()),
            None,
            Some("broker"),
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
        assert_eq!(requests[0].url, "https://broker.example/v1/credentials");
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
            assert!(BrokerFetch::<String>::from_env(
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
        let resolved = ok(resolve_gcs_bearer_with_env(
            &env(request),
            Some("static-token".into()),
            Some("broker"),
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await);
        let backend =
            GcsBackend::with_bearer_provider(GcsConfig::default(), resolved.provider, http.clone());
        let _ = backend
            .head(&ObjectId::new(Backend::Gcs, "bucket", "key"))
            .await;
        let requests = http.requests.lock().unwrap();
        assert_eq!(
            requests[1].header("authorization"),
            Some("Bearer oauth-token")
        );
        assert!(requests[1].header("x-amz-security-token").is_none());
        let request: Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(request["csp"], "gcp");
        assert_eq!(
            request["gcpServiceAccountEmail"],
            "reader@example.iam.gserviceaccount.com"
        );
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
        let resolved = ok(resolve_s3_credentials_with_env(
            &env(aws_request()),
            None,
            Some("broker"),
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await);
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
        // Tokio controls the refresh timer; SystemTime stays on the real
        // clock. Explicit wall-time pacing is covered in credentials tests.
        assert_eq!(
            requests.iter().filter(|r| r.method == Method::Post).count(),
            3
        );
    }

    #[tokio::test(start_paused = true)]
    async fn broker_azure_rotation_updates_read_write_copy_and_listing_urls() {
        let expiry = expiry_after(100);
        let http = http(vec![
            response(200, credentials("account", "sv=2026&sig=first", &expiry)),
            response(200, credentials("account", "sv=2026&sig=second", &expiry)),
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
        let request: Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(request["csp"], "azure");
        assert_eq!(request["azureAccountName"], "account");
        assert_eq!(request["bucket"], "container");
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
        tokio::time::advance(Duration::from_secs(30)).await;
        tokio::task::yield_now().await;
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
        let http = http(vec![response(
            200,
            credentials(
                "different",
                "sv=2026&sig=secret-token",
                "2099-01-01T00:00:00Z",
            ),
        )]);
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
    async fn broker_times_out_a_stalled_exchange_after_75_seconds() {
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
        assert_eq!(started.elapsed(), Duration::from_secs(75));
    }
}

#[cfg(test)]
#[path = "broker_live_tests.rs"]
mod live_tests;
