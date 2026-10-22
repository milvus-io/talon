//! Opt-in real broker/cloud tests. Every HTTP call uses ReqwestClient; no mocks.
//! Each test owns its Tokio runtime, which drops remaining refresh tasks on exit.

use super::BrokerFetch;
use crate::credentials::{
    CredentialsObserver, NoopObserver, ProvideBearerToken, ProvideS3Credentials, ProvideSasToken,
    RefreshPolicy, RefreshingCredentials,
};
use crate::workload_identity::{
    resolve_azure_credentials_with_env, resolve_gcs_credentials_with_env,
    resolve_s3_credentials_with_env,
};
use crate::{
    AzureBackend, AzureConfig, AzureSas, CredentialsFetch, GcsBackend, GcsConfig, HttpClient,
    HttpRequest, HttpResponse, ReqwestClient, S3Backend, S3Config, S3Credentials,
};
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use talon_core::{Backend, BackendStore, ObjectId};

struct LiveConfig {
    url: String,
    request: String,
    value: Value,
}

impl LiveConfig {
    fn load(cloud: &str) -> Self {
        let url = required_env(&format!("TALON_LIVE_BROKER_{cloud}_URL"));
        let request = required_env(&format!("TALON_LIVE_BROKER_{cloud}_REQUEST"));
        let value = live_result(serde_json::from_str(&request), "request JSON parsing");
        Self {
            url,
            request,
            value,
        }
    }

    fn env(&self, name: &str) -> Option<String> {
        match name {
            "TALON_CREDENTIAL_BROKER_URL" => Some(self.url.clone()),
            "TALON_CREDENTIAL_BROKER_REQUEST" => Some(self.request.clone()),
            _ => None,
        }
    }

    fn field(&self, name: &str) -> &str {
        self.value
            .get(name)
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| panic!("live request requires nonempty field {name}"))
    }
}

fn required_env(name: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| panic!("missing required live configuration {name}"))
}

// Never include errors: transport/storage errors can contain identity, object
// names or query credentials. Stage names are fixed strings owned by these tests.
fn live_result<T, E>(result: Result<T, E>, stage: &str) -> T {
    result.unwrap_or_else(|_| panic!("real {stage} failed (details suppressed)"))
}

struct LiveHttp {
    inner: ReqwestClient,
    cloud: &'static str,
    broker_url: String,
}

#[async_trait::async_trait]
impl HttpClient for LiveHttp {
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, String> {
        let target = if request.url == self.broker_url {
            "broker"
        } else {
            "origin"
        };
        let method = request.method;
        match self.inner.execute(request).await {
            Ok(response) => {
                eprintln!(
                    "{} {target} {method:?} HTTP {}",
                    self.cloud, response.status
                );
                Ok(response)
            }
            Err(_) => {
                eprintln!("{} {target} {method:?} transport failed", self.cloud);
                Err("real HTTP transport failed (details suppressed)".into())
            }
        }
    }
}

fn real_http(cloud: &'static str, broker_url: &str) -> Arc<dyn HttpClient> {
    Arc::new(LiveHttp {
        inner: ReqwestClient::new(),
        cloud,
        broker_url: broker_url.into(),
    })
}

#[derive(Default)]
struct SuccessfulFetches(AtomicUsize);
impl CredentialsObserver for SuccessfulFetches {
    fn refresh_succeeded(&self, _: Option<SystemTime>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

async fn refresh_twice<F: CredentialsFetch>(
    fetcher: F,
    valid: impl Fn(&RefreshingCredentials<F>) -> bool,
) {
    let observer = Arc::new(SuccessfulFetches::default());
    // Test-only pacing invokes the real background fetch in two seconds. The
    // broker may return the same cached token/expiry; this is not expiry rotation.
    let policy = RefreshPolicy {
        margin_fraction: 1.0,
        min_margin: Duration::ZERO,
        max_margin: Duration::MAX,
        retry_interval: Duration::from_secs(2),
        ..RefreshPolicy::default()
    };
    let cell = live_result(
        RefreshingCredentials::bootstrap(fetcher, policy, observer.clone()).await,
        "refresh bootstrap",
    );
    assert!(valid(&cell), "initial real snapshot is invalid");
    live_result(
        tokio::time::timeout(Duration::from_secs(90), async {
            while observer.0.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await,
        "background refresh (two successful real fetches required)",
    );
    assert!(valid(&cell), "refreshed real snapshot is invalid");
    assert!(
        cell.expires_at().is_some_and(|at| at > SystemTime::now()),
        "real refreshed expiry is not in the future"
    );
    drop(cell);
}

fn valid_s3(credentials: &S3Credentials) -> bool {
    !credentials.access_key_id.is_empty()
        && !credentials.secret_access_key.is_empty()
        && credentials
            .session_token
            .as_ref()
            .is_some_and(|s| !s.is_empty())
}

async fn read_storage(backend: &dyn BackendStore, kind: Backend, bucket: &str, cloud: &str) {
    live_result(
        tokio::time::timeout(Duration::from_secs(120), async {
            let configured_key = std::env::var(format!("TALON_LIVE_BROKER_{cloud}_KEY"))
                .ok()
                .filter(|key| !key.is_empty());
            let mut selected = configured_key;
            let mut cursor = None;
            // Always require a real successful LIST, including with an explicit
            // key. Search at most three pages of 100 objects for nonempty data.
            for _ in 0..3 {
                let page = live_result(
                    backend
                        .list_objects(bucket, "", cursor.as_deref(), 100)
                        .await,
                    "origin LIST",
                );
                if selected.is_none() {
                    selected = page.objects.into_iter().find(|o| o.size > 0).map(|o| o.key);
                }
                if selected.is_some() || page.next.is_none() {
                    break;
                }
                cursor = page.next;
            }
            let key = selected.expect(
                "real LIST found no nonempty object within 300 entries; configure the cloud KEY",
            );
            let object = ObjectId::new(kind, bucket, key);
            let stat = live_result(backend.head(&object).await, "origin HEAD");
            assert!(stat.len > 0, "real selected object must be nonempty");
            assert!(
                !stat.version.as_str().is_empty(),
                "real HEAD must report a version"
            );
            let len = stat.len.min(1024);
            let bytes = live_result(
                backend
                    .fetch_range_if_match(&object, 0, len, Some(&stat.version))
                    .await,
                "origin conditional range GET",
            );
            assert_eq!(
                bytes.len() as u64,
                len,
                "real range GET returned the wrong length"
            );
        })
        .await,
        "origin read sequence deadline",
    );
}

#[tokio::test]
#[ignore = "requires live broker AWS configuration and real network access"]
async fn live_broker_aws_acquisition_and_background_refresh() {
    let config = LiveConfig::load("AWS");
    let http = real_http("AWS", &config.url);
    let resolved = live_result(
        resolve_s3_credentials_with_env(
            &|name| config.env(name),
            None,
            Some("broker"),
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await,
        "AWS production resolver",
    );
    assert!(
        valid_s3(&resolved.provider.current()),
        "real AWS resolver snapshot is invalid"
    );
    drop(resolved);
    let fetcher = live_result(
        BrokerFetch::<S3Credentials>::from_env(&|name| config.env(name), "aws", None, http),
        "AWS fetcher configuration",
    );
    refresh_twice(fetcher, |cell| {
        valid_s3(&ProvideS3Credentials::current(cell))
    })
    .await;
}

#[tokio::test]
#[ignore = "requires live broker GCP configuration and real network access"]
async fn live_broker_gcp_acquisition_and_background_refresh() {
    let config = LiveConfig::load("GCP");
    let http = real_http("GCP", &config.url);
    let resolved = live_result(
        resolve_gcs_credentials_with_env(
            &|name| config.env(name),
            None,
            Some("broker"),
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await,
        "GCP production resolver",
    );
    assert!(
        matches!(&resolved.provider, crate::GcsCredentials::Bearer(provider) if provider.current().is_some_and(|s| !s.is_empty())),
        "real GCP resolver snapshot is invalid"
    );
    drop(resolved);
    let fetcher = live_result(
        BrokerFetch::<String>::from_env(&|name| config.env(name), "gcp", None, http),
        "GCP fetcher configuration",
    );
    refresh_twice(fetcher, |cell| {
        ProvideBearerToken::current(cell).is_some_and(|s| !s.is_empty())
    })
    .await;
}

#[tokio::test]
#[ignore = "requires live broker Azure configuration and real network access"]
async fn live_broker_azure_acquisition_and_background_refresh() {
    let config = LiveConfig::load("AZURE");
    let http = real_http("AZURE", &config.url);
    let account = config.field("azureAccountName");
    let resolved = live_result(
        resolve_azure_credentials_with_env(
            &|name| config.env(name),
            account,
            None,
            Some("broker"),
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await,
        "Azure production resolver",
    );
    assert_eq!(resolved.source, "broker");
    let crate::AzureCredentials::BrokerSas(provider) = &resolved.provider else {
        panic!("Azure broker must return SAS credentials");
    };
    assert!(
        !provider.current().expose_secret().is_empty(),
        "real Azure resolver snapshot is invalid"
    );
    drop(resolved);
    let fetcher = live_result(
        BrokerFetch::<AzureSas>::from_env(&|name| config.env(name), "azure", Some(account), http),
        "Azure fetcher configuration",
    );
    refresh_twice(fetcher, |cell| {
        !ProvideSasToken::current(cell).expose_secret().is_empty()
    })
    .await;
}

#[tokio::test]
#[ignore = "requires live broker AWS configuration and an existing readable cloud object"]
async fn live_broker_aws_storage_list_head_conditional_range_get() {
    let config = LiveConfig::load("AWS");
    let bucket = required_env("TALON_LIVE_BROKER_AWS_BUCKET");
    let http = real_http("AWS", &config.url);
    let resolved = live_result(
        resolve_s3_credentials_with_env(
            &|name| config.env(name),
            None,
            Some("broker"),
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await,
        "AWS production resolver",
    );
    let backend = S3Backend::with_credentials_provider(
        S3Config::aws(config.field("region")),
        resolved.provider,
        http,
    );
    read_storage(&backend, Backend::S3, &bucket, "AWS").await;
    drop(backend);
}

#[tokio::test]
#[ignore = "requires live broker GCP configuration and an existing readable cloud object"]
async fn live_broker_gcp_storage_list_head_conditional_range_get() {
    let config = LiveConfig::load("GCP");
    let bucket = required_env("TALON_LIVE_BROKER_GCP_BUCKET");
    let http = real_http("GCP", &config.url);
    let resolved = live_result(
        resolve_gcs_credentials_with_env(
            &|name| config.env(name),
            None,
            Some("broker"),
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await,
        "GCP production resolver",
    );
    let backend = GcsBackend::with_credentials(GcsConfig::default(), resolved.provider, http);
    read_storage(&backend, Backend::Gcs, &bucket, "GCP").await;
    drop(backend);
}

#[tokio::test]
#[ignore = "requires live broker Azure configuration and an existing readable cloud object"]
async fn live_broker_azure_storage_list_head_conditional_range_get() {
    let config = LiveConfig::load("AZURE");
    let http = real_http("AZURE", &config.url);
    let resolved = live_result(
        resolve_azure_credentials_with_env(
            &|name| config.env(name),
            config.field("azureAccountName"),
            None,
            Some("broker"),
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await,
        "Azure production resolver",
    );
    let backend = AzureBackend::with_credentials(
        AzureConfig::new(config.field("azureAccountName")),
        resolved.provider,
        http,
    );
    read_storage(&backend, Backend::Azure, config.field("bucket"), "AZURE").await;
    drop(backend);
}
