//! Live workload-identity verification probe.
//!
//! Resolves origin credentials through the production chain (static env,
//! then the detected or explicitly selected cloud mechanism), then proves
//! the material signs a real request by HEADing one key. A `NotFound`
//! answer is the success signal: the exchange, the signature, and the
//! bucket authorization all worked without reading any data.
//!
//! ```sh
//! credentials_probe s3  --region us-west-2 --bucket b --key prefix/absent
//! credentials_probe gcs --bucket b --key prefix/absent
//! ```
//!
//! Select an env-markerless mechanism with
//! `TALON_ORIGIN_CREDENTIALS_SOURCE` (`gcp-metadata`, `huawei-agency`).
//! Output never contains key or token material.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use talon_backend::{
    resolve_azure_bearer, resolve_gcs_bearer, resolve_s3_credentials, AzureBackend, AzureConfig,
    CredentialsObserver, GcsBackend, GcsConfig, HttpClient, ReqwestClient, S3Backend, S3Config,
};
use talon_core::{Backend, BackendStore, Error, ObjectId};

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
    let head: String = value.chars().take(4).collect();
    format!("{head}... ({} chars)", value.chars().count())
}

async fn probe(backend: &dyn BackendStore, kind: Backend, bucket: &str, key: &str) -> i32 {
    match backend.head(&ObjectId::new(kind, bucket, key)).await {
        Err(Error::NotFound(_)) => {
            println!("probe HEAD: NotFound - exchange, signing, and authorization verified");
            0
        }
        Ok(stat) => {
            println!(
                "probe HEAD: object exists ({} bytes) - exchange, signing, and authorization verified",
                stat.len
            );
            0
        }
        Err(error) => {
            println!("probe HEAD failed: {error}");
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
    let http: Arc<dyn HttpClient> = Arc::new(ReqwestClient::new());
    let observer = Arc::new(PrintObserver);
    let key = arg("--key").unwrap_or_else(|| "talon-credentials-probe/absent".to_string());
    let code = match mode.as_str() {
        "s3" => {
            let resolved = match resolve_s3_credentials(
                None,
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
            println!("credential source: {}", resolved.source);
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
            let resolved =
                match resolve_gcs_bearer(None, selector.as_deref(), Arc::clone(&http), observer)
                    .await
                {
                    Ok(resolved) => resolved,
                    Err(error) => {
                        println!("credential resolution failed: {error}");
                        std::process::exit(1);
                    }
                };
            println!("credential source: {}", resolved.source);
            match resolved.provider.current() {
                Some(token) => println!("bearer token: {}", redacted(&token)),
                None => println!("bearer token: absent (unauthenticated)"),
            }
            match arg("--bucket") {
                Some(bucket) => {
                    let backend = GcsBackend::with_bearer_provider(
                        GcsConfig::default(),
                        resolved.provider,
                        http,
                    );
                    probe(&backend, Backend::Gcs, &bucket, &key).await
                }
                None => 0,
            }
        }
        "azure" => {
            let resolved = match resolve_azure_bearer(Arc::clone(&http), observer).await {
                Ok(resolved) => resolved,
                Err(error) => {
                    println!("credential resolution failed: {error}");
                    std::process::exit(1);
                }
            };
            println!("credential source: {}", resolved.source);
            match resolved.provider.current() {
                Some(token) => println!("bearer token: {}", redacted(&token)),
                None => println!("bearer token: absent"),
            }
            match (arg("--account"), arg("--container")) {
                (Some(account), Some(container)) => {
                    let backend = AzureBackend::with_bearer_provider(
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
