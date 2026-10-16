//! Live workload-identity verification probe.
//!
//! Resolves origin credentials through the production chain and optionally
//! HEADs one key. Reports the observed result; a 404 alone cannot establish
//! IAM authorization, because an origin may allow public access.
//!
//! ```sh
//! credentials_probe s3  --region us-west-2 --bucket b --key prefix/absent
//! credentials_probe gcs --bucket b --key prefix/absent
//! ```
//!
//! Select an env-markerless mechanism with
//! `TALON_ORIGIN_CREDENTIALS_SOURCE` (`broker`, `gcp-metadata`, `huawei-agency`).
//! Output never contains key or token material.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use talon_backend::{
    resolve_azure_credentials, resolve_gcs_credentials, resolve_s3_credentials, AzureBackend,
    AzureConfig, CredentialsObserver, GcsBackend, GcsConfig, HttpClient, ReqwestClient, S3Backend,
    S3Config,
};
use talon_core::{Backend, BackendStore, Error, ObjectId};

// Print only method/status for the origin HEAD, without URLs or error bodies.
struct ProbeHttp(ReqwestClient);

#[async_trait::async_trait]
impl HttpClient for ProbeHttp {
    async fn execute(
        &self,
        request: talon_backend::HttpRequest,
    ) -> Result<talon_backend::HttpResponse, String> {
        let is_head = request.method == talon_backend::Method::Head;
        let result = self.0.execute(request).await;
        if is_head {
            match &result {
                Ok(response) => println!("origin HEAD HTTP status: {}", response.status),
                Err(_) => println!("origin HEAD transport failed"),
            }
        }
        result
    }
}

struct PrintObserver;

impl CredentialsObserver for PrintObserver {
    fn refresh_succeeded(&self, expires_at: Option<SystemTime>) {
        match expires_at {
            Some(at) => println!(
                "credential expiry: unix {}",
                at.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
            ),
            None => println!("credential expiry: unreported"),
        }
    }

    fn refresh_failed(&self) {
        println!("credential refresh failed");
    }
}

fn arg(name: &str) -> Option<String> {
    let mut args = std::env::args();
    while let Some(current) = args.next() {
        if current == name {
            return args.next();
        }
    }
    None
}

fn redacted(value: &str) -> String {
    format!("present ({} chars)", value.chars().count())
}

async fn probe(backend: &dyn BackendStore, kind: Backend, bucket: &str, key: &str) -> i32 {
    match backend.head(&ObjectId::new(kind, bucket, key)).await {
        Err(Error::NotFound(_)) => {
            println!("probe HEAD: HTTP 404 (NotFound)");
            0
        }
        Ok(stat) => {
            println!("probe HEAD: object exists ({} bytes)", stat.len);
            0
        }
        Err(_) => {
            println!("probe HEAD failed (details suppressed to protect query credentials)");
            2
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    let selector = std::env::var("TALON_ORIGIN_CREDENTIALS_SOURCE")
        .ok()
        .filter(|value| !value.is_empty());
    let http: Arc<dyn HttpClient> = Arc::new(ProbeHttp(ReqwestClient::new()));
    let observer = Arc::new(PrintObserver);
    let key = arg("--key").unwrap_or_else(|| "talon-credentials-probe/absent".to_string());
    let code = match mode.as_str() {
        "s3" => {
            let access_key = std::env::var("AWS_ACCESS_KEY_ID").ok();
            let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok();
            let keys = (access_key.is_some() || secret_key.is_some()).then(|| {
                talon_backend::S3Credentials {
                    access_key_id: access_key.unwrap_or_default(),
                    secret_access_key: secret_key.unwrap_or_default(),
                    session_token: std::env::var("AWS_SESSION_TOKEN").ok(),
                }
            });
            let resolved = match resolve_s3_credentials(
                keys,
                selector.as_deref(),
                Arc::clone(&http),
                observer,
            )
            .await
            {
                Ok(resolved) => resolved,
                Err(error) => {
                    println!("credential resolution failed: {error}");
                    std::process::exit(1);
                }
            };
            println!(
                "credential acquisition succeeded; source: {}",
                resolved.source
            );
            let snapshot = resolved.provider.current();
            println!("access key id: {}", redacted(&snapshot.access_key_id));
            println!(
                "session token: {}",
                snapshot
                    .session_token
                    .as_deref()
                    .map_or_else(|| "absent".to_string(), redacted)
            );
            match arg("--bucket") {
                Some(bucket) => {
                    let region = arg("--region")
                        .or_else(|| std::env::var("AWS_REGION").ok())
                        .unwrap_or_else(|| "us-east-1".to_string());
                    // `--endpoint` targets an S3-compatible store (OSS, COS,
                    // OBS, MinIO); `--path-style` selects path addressing.
                    let config = match arg("--endpoint") {
                        Some(endpoint) => S3Config {
                            region,
                            endpoint,
                            path_style: std::env::args().any(|flag| flag == "--path-style"),
                            tls: true,
                        },
                        None => S3Config::aws(&region),
                    };
                    let backend =
                        S3Backend::with_credentials_provider(config, resolved.provider, http);
                    probe(&backend, Backend::S3, &bucket, &key).await
                }
                None => 0,
            }
        }
        "gcs" => {
            let access_key = std::env::var("TALON_WORKER_GCS_ACCESS_KEY_ID").ok();
            let secret_key = std::env::var("TALON_WORKER_GCS_SECRET_ACCESS_KEY").ok();
            let keys = (access_key.is_some() || secret_key.is_some()).then(|| {
                talon_backend::GcsHmacCredentials {
                    access_key_id: access_key.unwrap_or_default(),
                    secret_access_key: secret_key.unwrap_or_default(),
                }
            });
            let resolved = match resolve_gcs_credentials(
                keys,
                selector.as_deref(),
                Arc::clone(&http),
                observer,
            )
            .await
            {
                Ok(resolved) => resolved,
                Err(error) => {
                    println!("credential resolution failed: {error}");
                    std::process::exit(1);
                }
            };
            println!(
                "credential acquisition succeeded; source: {}",
                resolved.source
            );
            match &resolved.provider {
                talon_backend::GcsCredentials::Bearer(provider) => {
                    if let Some(token) = provider.current() {
                        println!("bearer token: {}", redacted(&token));
                    }
                }
                talon_backend::GcsCredentials::Hmac(_) => println!("HMAC credentials: present"),
            }
            match arg("--bucket") {
                Some(bucket) => {
                    let backend =
                        GcsBackend::with_credentials(GcsConfig::default(), resolved.provider, http);
                    probe(&backend, Backend::Gcs, &bucket, &key).await
                }
                None => 0,
            }
        }
        "azure" => {
            let account = arg("--account").unwrap_or_default();
            let key_credential = std::env::var("TALON_WORKER_AZURE_SHARED_KEY").ok();
            let resolved = match resolve_azure_credentials(
                &account,
                key_credential,
                selector.as_deref(),
                Arc::clone(&http),
                observer,
            )
            .await
            {
                Ok(resolved) => resolved,
                Err(error) => {
                    println!("credential resolution failed: {error}");
                    std::process::exit(1);
                }
            };
            println!(
                "credential acquisition succeeded; source: {}",
                resolved.source
            );
            match &resolved.provider {
                talon_backend::AzureCredentials::SharedKey(_) => println!("account key: present"),
                talon_backend::AzureCredentials::Sas(_) => println!("fixed SAS: present"),
                talon_backend::AzureCredentials::BrokerSas(provider) => println!(
                    "SAS token: {}",
                    redacted(provider.current().expose_secret())
                ),
                talon_backend::AzureCredentials::Bearer(provider) => {
                    if let Some(token) = provider.current() {
                        println!("bearer token: {}", redacted(&token));
                    }
                }
            }
            match arg("--container") {
                Some(container) if !account.is_empty() => {
                    let backend = AzureBackend::with_credentials(
                        AzureConfig::new(&account),
                        resolved.provider,
                        http,
                    );
                    probe(&backend, Backend::Azure, &container, &key).await
                }
                _ => 0,
            }
        }
        other => {
            println!(
                "usage: credentials_probe <s3|gcs|azure> [--region r] [--endpoint e] \
                 [--path-style] [--bucket b] [--account a] [--container c] [--key k] \
                 (got {other:?})"
            );
            1
        }
    };
    std::process::exit(code);
}
