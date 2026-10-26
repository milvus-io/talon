//! Opt-in read-only multi-bucket validation against real cloud services.
use std::sync::Arc;
use std::time::Duration;

use talon_backend::{
    BackendResolver, BackendRouter, DefaultBackendResolver, HttpClient, HttpRequest, HttpResponse,
    NoopObserver, ReqwestClient, RouteRequest,
};
use talon_core::{Backend, BackendStore, BrokerConfig, ObjectId, WorkerConfig, WorkerConfigPatch};

struct LiveHttp(ReqwestClient);
#[async_trait::async_trait]
impl HttpClient for LiveHttp {
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, String> {
        let method = request.method;
        let response = self
            .0
            .execute(request)
            .await
            .map_err(|_| "live transport failed")?;
        eprintln!("live bucket router {method:?} HTTP {}", response.status);
        Ok(response)
    }
}

#[tokio::test]
#[ignore = "requires TALON_LIVE_CREDENTIAL_BROKER JSON; acquires credentials without storage I/O"]
async fn live_dynamic_broker_acquisition_reuses_binding() {
    let json = std::env::var("TALON_LIVE_CREDENTIAL_BROKER")
        .unwrap_or_else(|_| panic!("set TALON_LIVE_CREDENTIAL_BROKER"));
    let broker = BrokerConfig::from_json(&json)
        .unwrap_or_else(|_| panic!("invalid live broker descriptor (details suppressed)"));
    let backend = match broker.request.get("csp").and_then(|value| value.as_str()) {
        Some("aws") => Backend::S3,
        Some("gcp") => Backend::Gcs,
        Some("azure") => Backend::Azure,
        _ => panic!("live descriptor requires a supported csp"),
    };
    let bucket = std::env::var("TALON_LIVE_BUCKET")
        .ok()
        .or_else(|| {
            broker
                .request
                .get("bucket")
                .and_then(|value| value.as_str())
                .map(str::to_owned)
        })
        .filter(|value| !value.is_empty())
        .expect("descriptor request.bucket or TALON_LIVE_BUCKET is required");
    tokio::time::timeout(Duration::from_secs(120), async {
        let http: Arc<dyn HttpClient> = Arc::new(LiveHttp(ReqwestClient::new()));
        let resolver =
            DefaultBackendResolver::new(backend, http.clone(), http, Arc::new(NoopObserver));
        let request = RouteRequest {
            backend,
            bucket: &bucket,
            broker: Some(&broker),
        };
        let first = resolver.resolve(&request).await.unwrap_or_else(|_| {
            panic!("live dynamic credential acquisition failed (details suppressed)")
        });
        let second = resolver
            .resolve(&request)
            .await
            .unwrap_or_else(|_| panic!("live dynamic binding reuse failed (details suppressed)"));
        assert!(
            Arc::ptr_eq(&first.store, &second.store),
            "dynamic binding was not reused"
        );
        assert!(
            first
                .scope
                .as_ref()
                .is_some_and(|scope| !scope.as_str().is_empty()),
            "dynamic binding requires an access scope"
        );
        assert!(
            first.scope == second.scope,
            "reused binding changed its access scope"
        );
        eprintln!("live dynamic broker acquisition and binding reuse passed; storage not accessed");
    })
    .await
    .expect("live dynamic broker acquisition deadline exceeded");
}

#[tokio::test]
#[ignore = "requires TALON_LIVE_BUCKET_CONFIG TOML, real AWS IAM and a reachable Broker; read-only"]
async fn live_aws_mixed_iam_broker_buckets() {
    let text = std::env::var("TALON_LIVE_BUCKET_CONFIG").expect("set TALON_LIVE_BUCKET_CONFIG");
    let patch = WorkerConfigPatch::from_toml(&text)
        .unwrap_or_else(|_| panic!("live configuration parsing failed"));
    let config = WorkerConfig::resolve(patch, Default::default(), Default::default())
        .unwrap_or_else(|_| panic!("live configuration validation failed"));
    assert_eq!(config.backend.as_deref(), Some("s3"));
    let buckets = config
        .resolved_buckets()
        .unwrap()
        .expect("configure buckets");
    assert_eq!(buckets.len(), 2, "configure exactly two distinct buckets");
    assert!(buckets.iter().any(|b| b.credentials_source == "iam"));
    assert!(buckets.iter().any(|b| b.credentials_source == "broker"));
    let targets: Vec<_> = buckets
        .iter()
        .map(|b| (b.name.clone(), b.credentials_source.clone()))
        .collect();
    tokio::time::timeout(Duration::from_secs(180), async {
        let http: Arc<dyn HttpClient> = Arc::new(LiveHttp(ReqwestClient::new()));
        let router = BackendRouter::from_configs(
            Backend::S3,
            buckets,
            http.clone(),
            http,
            Arc::new(NoopObserver),
        )
        .await
        .unwrap_or_else(|_| {
            panic!("real router credential acquisition failed (details suppressed)")
        });
        for (bucket, source) in targets {
            let mut cursor = None;
            let mut key = None;
            for _ in 0..3 {
                let page = router
                    .list_objects(&bucket, "", cursor.as_deref(), 100)
                    .await
                    .unwrap_or_else(|_| panic!("real bucket LIST failed (details suppressed)"));
                key = page
                    .objects
                    .into_iter()
                    .find(|item| item.size > 0)
                    .map(|item| item.key);
                if key.is_some() || page.next.is_none() {
                    break;
                }
                cursor = page.next;
            }
            let object = ObjectId::new(
                Backend::S3,
                bucket,
                key.expect("no nonempty object in first 300 entries"),
            );
            let stat = router
                .head(&object)
                .await
                .unwrap_or_else(|_| panic!("real bucket HEAD failed (details suppressed)"));
            assert!(stat.len > 0 && !stat.version.as_str().is_empty());
            let len = stat.len.min(1024);
            let data = router
                .fetch_range_if_match(&object, 0, len, Some(&stat.version))
                .await
                .unwrap_or_else(|_| {
                    panic!("real conditional range GET failed (details suppressed)")
                });
            assert_eq!(data.len() as u64, len);
            eprintln!("real {source} bucket: LIST, HEAD and conditional range GET passed");
        }
        assert!(router
            .validate_bucket(Backend::S3, "unconfigured-live-bucket")
            .is_err());
    })
    .await
    .expect("live router read deadline exceeded");
}
