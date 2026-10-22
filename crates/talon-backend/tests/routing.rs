use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::{json, Value};
use talon_backend::{
    BackendResolver, BackendRouter, CredentialsObserver, DefaultBackendResolver, HttpClient,
    HttpRequest, HttpResponse, Method, NoopObserver, ReqwestClient, RouteRequest,
};
use talon_core::config::BucketConfig;
use talon_core::{Backend, BackendStore, Error, ObjectId, Version};

#[derive(Default)]
struct OriginHttp {
    requests: Mutex<Vec<HttpRequest>>,
}

#[async_trait]
impl HttpClient for OriginHttp {
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, String> {
        let body = if request.url.contains("list-type=2") {
            Bytes::from_static(
                b"<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>shared/key</Key><Size>4</Size></Contents></ListBucketResult>",
            )
        } else if request.url.contains("comp=list") {
            Bytes::from_static(
                b"<EnumerationResults><Blobs><Blob><Name>shared/key</Name><Properties><Content-Length>4</Content-Length></Properties></Blob></Blobs></EnumerationResults>",
            )
        } else if request.url.contains("/storage/v1/b/") {
            Bytes::from_static(br#"{"items":[{"name":"shared/key","size":"4"}]}"#)
        } else {
            Bytes::from_static(b"data")
        };
        self.requests.lock().unwrap().push(request);
        Ok(HttpResponse {
            status: 200,
            headers: vec![
                ("content-length".into(), "4".into()),
                ("etag".into(), "\"v1\"".into()),
                ("x-goog-generation".into(), "1".into()),
            ],
            body,
        })
    }

    async fn execute_file(
        &self,
        mut request: HttpRequest,
        path: &Path,
        len: u64,
    ) -> Result<HttpResponse, String> {
        let body = std::fs::read(path).unwrap();
        assert_eq!(body.len() as u64, len);
        request.body = body.into();
        self.execute(request).await
    }
}

fn static_config(backend: Backend, name: &str) -> BucketConfig {
    let mut config = BucketConfig {
        name: name.into(),
        credentials_source: "aksk".into(),
        ..Default::default()
    };
    match backend {
        Backend::S3 => {
            config.s3_region = Some(format!("{name}-region"));
            config.s3_endpoint = Some(format!("https://{name}.s3.example/"));
            config.s3_path_style = Some(true);
            config.s3_access_key_id = Some(format!("{name}-access"));
            config.s3_secret_access_key = Some(format!("{name}-secret").into());
            config.s3_session_token = Some(format!("{name}-session").into());
        }
        Backend::Gcs => {
            config.gcs_endpoint = Some(format!("https://{name}.gcs.example/"));
            config.gcs_access_key_id = Some(format!("{name}-access"));
            config.gcs_secret_access_key = Some(format!("{name}-secret").into());
        }
        Backend::Azure => {
            config.azure_account = Some(format!("{name}account"));
            config.azure_endpoint = Some(format!("https://{name}.azure.example/"));
            config.azure_shared_key = Some("c2VjcmV0".to_string().into());
        }
    }
    config
}

async fn router(
    backend: Backend,
    configs: Vec<BucketConfig>,
    http: Arc<OriginHttp>,
) -> Result<BackendRouter, String> {
    let resolver = DefaultBackendResolver::from_configs(
        backend,
        configs,
        http.clone(),
        http,
        Arc::new(NoopObserver),
    )
    .await?;
    Ok(BackendRouter::new(backend, Arc::new(resolver)))
}

#[tokio::test]
async fn static_resolver_reuses_instances_and_rejects_unknown_targets() {
    let http = Arc::new(OriginHttp::default());
    let resolver: Arc<dyn BackendResolver> = Arc::new(
        DefaultBackendResolver::from_configs(
            Backend::S3,
            vec![
                static_config(Backend::S3, "alpha"),
                static_config(Backend::S3, "bravo"),
            ],
            http.clone(),
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await
        .unwrap(),
    );
    let alpha = RouteRequest {
        backend: Backend::S3,
        bucket: "alpha",
    };
    resolver.validate(&alpha).unwrap();
    let first = resolver.resolve(&alpha).await.unwrap();
    let second = resolver.resolve(&alpha).await.unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    let bravo = resolver
        .resolve(&RouteRequest {
            backend: Backend::S3,
            bucket: "bravo",
        })
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&first, &bravo));
    for (backend, bucket) in [(Backend::S3, "unknown"), (Backend::Azure, "alpha")] {
        let request = RouteRequest { backend, bucket };
        assert!(resolver.validate(&request).is_err());
        assert!(resolver.resolve(&request).await.is_err());
    }
    assert!(http.requests.lock().unwrap().is_empty());
}

struct GatedResolver {
    inner: DefaultBackendResolver,
    unavailable: AtomicBool,
}

#[async_trait]
impl BackendResolver for GatedResolver {
    fn validate(&self, request: &RouteRequest<'_>) -> talon_core::Result<()> {
        self.inner.validate(request)
    }

    async fn resolve(
        &self,
        request: &RouteRequest<'_>,
    ) -> talon_core::Result<Arc<dyn BackendStore>> {
        tokio::task::yield_now().await;
        if self.unavailable.load(Ordering::Relaxed) {
            return Err(Error::Backend("resolver unavailable".into()));
        }
        self.inner.resolve(request).await
    }
}

#[tokio::test]
async fn injected_resolver_preserves_target_validation_and_resolution_errors() {
    let http = Arc::new(OriginHttp::default());
    let resolver = Arc::new(GatedResolver {
        inner: DefaultBackendResolver::from_configs(
            Backend::S3,
            vec![static_config(Backend::S3, "alpha")],
            http.clone(),
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await
        .unwrap(),
        unavailable: AtomicBool::new(false),
    });
    let router = BackendRouter::new(Backend::S3, resolver.clone());
    let object = ObjectId::new(Backend::S3, "alpha", "shared/key");
    assert_eq!(router.head(&object).await.unwrap().len, 4);
    assert_eq!(
        router
            .list_objects("alpha", "", None, 1)
            .await
            .unwrap()
            .objects[0]
            .key,
        "shared/key"
    );
    assert_eq!(http.requests.lock().unwrap().len(), 2);

    let unknown = ObjectId::new(Backend::S3, "unknown", "shared/key");
    assert!(router.validate_object(&unknown).is_err());
    assert!(router.head(&unknown).await.is_err());
    assert!(router.list_objects("unknown", "", None, 1).await.is_err());
    let traversal = ObjectId::new(Backend::S3, "alpha", "../unknown/key");
    assert!(router.validate_object(&traversal).is_err());
    assert!(router.head(&traversal).await.is_err());

    resolver.unavailable.store(true, Ordering::Relaxed);
    // Cache eligibility remains a local check; resolution failures must not
    // trigger an origin request or fall back to another credential source.
    router.validate_object(&object).unwrap();
    assert!(matches!(
        router.head(&object).await,
        Err(Error::Backend(message)) if message == "resolver unavailable"
    ));
    assert!(matches!(
        router.list_objects("alpha", "", None, 1).await,
        Err(Error::Backend(message)) if message == "resolver unavailable"
    ));
    assert_eq!(http.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn static_buckets_route_all_operations_with_independent_credentials() {
    for backend in [Backend::S3, Backend::Gcs, Backend::Azure] {
        let http = Arc::new(OriginHttp::default());
        let router = router(
            backend,
            vec![
                static_config(backend, "alpha"),
                static_config(backend, "bravo"),
            ],
            http.clone(),
        )
        .await
        .unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), b"file").unwrap();
        for name in ["alpha", "bravo"] {
            let object = ObjectId::new(backend, name, "shared/key");
            let version = Version::new("1");
            assert_eq!(router.head(&object).await.unwrap().len, 4);
            assert_eq!(
                router.fetch_range(&object, 0, 4).await.unwrap(),
                b"data".as_slice()
            );
            assert_eq!(
                router
                    .fetch_range_if_match(&object, 0, 4, Some(&version))
                    .await
                    .unwrap(),
                b"data".as_slice()
            );
            router
                .put(&object, Bytes::from_static(b"put"))
                .await
                .unwrap();
            router
                .put_if_match(&object, Bytes::from_static(b"conditional"), Some(&version))
                .await
                .unwrap();
            router.put_file(&object, file.path(), 4).await.unwrap();
            router.delete(&object).await.unwrap();
            let page = router
                .list_objects(name, "shared/", Some("cursor"), 10)
                .await
                .unwrap();
            assert_eq!(page.objects[0].key, "shared/key");

            let requests = std::mem::take(&mut *http.requests.lock().unwrap());
            assert_eq!(requests.len(), 8);
            for request in &requests {
                assert!(request.url.starts_with(&format!("https://{name}.")));
                assert!(request.url.contains(&format!("/{name}")));
                let authorization = request.header("authorization").unwrap();
                match backend {
                    Backend::S3 => {
                        assert!(authorization.contains(&format!("Credential={name}-access/")));
                        assert!(authorization.contains(&format!("/{name}-region/s3/")));
                        assert_eq!(
                            request.header("x-amz-security-token"),
                            Some(format!("{name}-session").as_str())
                        );
                    }
                    Backend::Gcs => {
                        assert!(authorization
                            .starts_with(&format!("GOOG4-HMAC-SHA256 Credential={name}-access/")));
                    }
                    Backend::Azure => {
                        assert!(authorization.starts_with(&format!("SharedKey {name}account:")));
                    }
                }
            }
            let conditional_header = if backend == Backend::Gcs {
                "x-goog-if-generation-match"
            } else {
                "if-match"
            };
            assert!(requests[2].header(conditional_header).is_some());
            assert!(requests[4].header(conditional_header).is_some());
            assert_eq!(requests[3].body, b"put".as_slice());
            assert_eq!(requests[4].body, b"conditional".as_slice());
            assert_eq!(requests[5].body, b"file".as_slice());
            assert_eq!(requests[6].method, Method::Delete);
            assert!(requests[7].url.contains("cursor"));
        }
    }
}

#[tokio::test]
async fn unknown_bucket_or_backend_is_denied_before_every_origin_operation() {
    let http = Arc::new(OriginHttp::default());
    let router = router(
        Backend::S3,
        vec![static_config(Backend::S3, "alpha")],
        http.clone(),
    )
    .await
    .unwrap();
    for (backend, bucket) in [(Backend::S3, "unknown"), (Backend::Azure, "alpha")] {
        let object = ObjectId::new(backend, bucket, "shared/key");
        assert!(router.validate_bucket(backend, bucket).is_err());
        assert!(router.head(&object).await.is_err());
        // Zero-length reads must still reject an unknown bucket.
        assert!(router.fetch_range(&object, 0, 0).await.is_err());
        assert!(router
            .fetch_range_if_match(&object, 0, 0, Some(&Version::new("v1")))
            .await
            .is_err());
        assert!(router.put(&object, Bytes::new()).await.is_err());
        assert!(router
            .put_if_match(&object, Bytes::new(), Some(&Version::new("v1")))
            .await
            .is_err());
        assert!(router
            .put_file(&object, Path::new("/does-not-exist"), 1)
            .await
            .is_err());
        assert!(router.delete(&object).await.is_err());
    }
    assert!(router.list_objects("unknown", "", None, 1).await.is_err());
    assert!(http.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn programmatic_records_are_validated_before_any_credentials_are_acquired() {
    let http = Arc::new(OriginHttp::default());
    assert!(router(Backend::S3, vec![], http.clone()).await.is_err());
    let valid = static_config(Backend::S3, "alpha");
    assert!(router(
        Backend::S3,
        vec![valid.clone(), valid.clone()],
        http.clone()
    )
    .await
    .is_err());
    for name in ["", ".", "..", "a/b", "a?b", "a#b", "a%2fb", "a\\b", "a b"] {
        let mut invalid = valid.clone();
        invalid.name = name.into();
        assert!(router(Backend::S3, vec![invalid], http.clone())
            .await
            .is_err());
    }
    for source in ["", "auto", "static", "aws-web-identity", "BROKER"] {
        let mut invalid = valid.clone();
        invalid.credentials_source = source.into();
        assert!(router(Backend::S3, vec![invalid], http.clone())
            .await
            .is_err());
    }
    for endpoint in [
        "",
        "ftp://invalid.example",
        "https://user:secret-marker@invalid.example",
        "https://invalid.example/?secret-marker",
        "https://invalid.example/#secret-marker",
        "https://invalid.example/with space",
    ] {
        let mut invalid = valid.clone();
        invalid.s3_endpoint = Some(endpoint.into());
        let error = router(Backend::S3, vec![invalid], http.clone())
            .await
            .err()
            .expect("invalid endpoint must fail");
        assert!(!error.contains("secret-marker"));
    }
    let mut invalid = valid;
    invalid.s3_secret_access_key = None;
    assert!(router(Backend::S3, vec![invalid], http.clone())
        .await
        .is_err());
    assert!(http.requests.lock().unwrap().is_empty());
}

struct BrokerHttp {
    requests: Mutex<Vec<Value>>,
    first_expiry: SystemTime,
    second_expiry: SystemTime,
}

#[async_trait]
impl HttpClient for BrokerHttp {
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, String> {
        assert_eq!(request.method, Method::Post);
        let payload: Value = serde_json::from_slice(&request.body).unwrap();
        let bucket = payload["bucket"].as_str().unwrap();
        assert_eq!(
            request.url,
            format!("https://{bucket}.broker.example/exchange")
        );
        let mut requests = self.requests.lock().unwrap();
        let generation = requests
            .iter()
            .filter(|previous| previous["bucket"] == bucket)
            .count();
        let expiry = if bucket == "alpha" && generation == 0 {
            self.first_expiry
        } else if bucket == "bravo" {
            self.second_expiry
        } else {
            SystemTime::now() + Duration::from_secs(7200)
        };
        let token = format!("{bucket}-token-{generation}");
        let mut credentials = json!({
            "tempAk": format!("{bucket}-access-{generation}"),
            "tempSk": format!("{bucket}-secret-{generation}"),
            "sessionToken": token,
            "expiredAt": chrono::DateTime::<chrono::Utc>::from(expiry).to_rfc3339(),
        });
        if payload["csp"] == "azure" {
            credentials["tempAk"] = format!("{bucket}account").into();
            credentials["sessionToken"] = format!("sv=2024-01-01&sig={token}").into();
        }
        requests.push(payload);
        Ok(HttpResponse {
            status: 200,
            headers: vec![],
            body: json!({"success": true, "credentials": credentials})
                .to_string()
                .into(),
        })
    }
}

fn broker_config(backend: Backend, name: &str) -> BucketConfig {
    let mut config = static_config(backend, name);
    config.credentials_source = "broker".into();
    config.s3_access_key_id = None;
    config.s3_secret_access_key = None;
    config.s3_session_token = None;
    config.gcs_access_key_id = None;
    config.gcs_secret_access_key = None;
    config.azure_shared_key = None;
    config.credential_broker_url = Some(format!("https://{name}.broker.example/exchange").into());
    let csp = match backend {
        Backend::S3 => "aws",
        Backend::Gcs => "gcp",
        Backend::Azure => "azure",
    };
    config.credential_broker_request = Some(
        json!({
            "csp": csp, "bucket": name, "region": "us-east-1",
            "roleArn": format!("arn:aws:iam::123456789012:role/{name}"),
            "gcpServiceAccountEmail": format!("{name}@project.iam.gserviceaccount.com"),
            "azureClientId": format!("{name}-client"), "azureTenantId": "tenant",
            "azureAccountName": format!("{name}account"),
        })
        .into(),
    );
    config
}

#[derive(Default)]
struct Observer {
    expiries: Mutex<Vec<Option<SystemTime>>>,
}

impl CredentialsObserver for Observer {
    fn refresh_succeeded(&self, expiry: Option<SystemTime>) {
        self.expiries.lock().unwrap().push(expiry);
    }
}

#[tokio::test(start_paused = true)]
async fn broker_refresh_and_requests_stay_per_bucket() {
    for backend in [Backend::S3, Backend::Gcs, Backend::Azure] {
        let origin = Arc::new(OriginHttp::default());
        let first_expiry = SystemTime::now() + Duration::from_secs(90);
        let second_expiry = SystemTime::now() + Duration::from_secs(3600);
        let exchange = Arc::new(BrokerHttp {
            requests: Mutex::new(vec![]),
            first_expiry,
            second_expiry,
        });
        let observer = Arc::new(Observer::default());
        let router = BackendRouter::from_configs(
            backend,
            vec![
                broker_config(backend, "alpha"),
                broker_config(backend, "bravo"),
            ],
            origin.clone(),
            exchange.clone(),
            observer.clone(),
        )
        .await
        .unwrap();
        assert_eq!(
            *observer.expiries.lock().unwrap(),
            vec![Some(first_expiry), Some(second_expiry)]
        );
        for name in ["alpha", "bravo"] {
            router
                .head(&ObjectId::new(backend, name, "shared/key"))
                .await
                .unwrap();
        }
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(35)).await;
        tokio::task::yield_now().await;
        for name in ["alpha", "bravo"] {
            router
                .head(&ObjectId::new(backend, name, "shared/key"))
                .await
                .unwrap();
        }
        let requests = origin.requests.lock().unwrap();
        for (index, token) in [
            "alpha-token-0",
            "bravo-token-0",
            "alpha-token-1",
            "bravo-token-0",
        ]
        .into_iter()
        .enumerate()
        {
            let request = &requests[index];
            match backend {
                Backend::S3 => assert_eq!(request.header("x-amz-security-token"), Some(token)),
                Backend::Gcs => assert_eq!(
                    request.header("authorization"),
                    Some(format!("Bearer {token}").as_str())
                ),
                Backend::Azure => assert!(request.url.contains(&format!("sig={token}"))),
            }
        }
        let exchanges = exchange.requests.lock().unwrap();
        assert_eq!(exchanges.len(), 3);
        assert_eq!(exchanges[0]["bucket"], "alpha");
        assert_eq!(exchanges[1]["bucket"], "bravo");
        assert_eq!(exchanges[2], exchanges[0]);
        let expiries = observer.expiries.lock().unwrap();
        assert_eq!(expiries.len(), 3);
        assert!(expiries[2].unwrap() > second_expiry);
    }
}

#[tokio::test]
async fn broker_scope_mismatch_is_rejected_before_any_exchange() {
    for backend in [Backend::S3, Backend::Gcs, Backend::Azure] {
        for (field, value) in [
            ("bucket", json!("other")),
            ("bucket", Value::Null),
            ("csp", json!("other")),
            ("durationSeconds", json!("secret-marker")),
        ] {
            let http = Arc::new(OriginHttp::default());
            let mut bad = broker_config(backend, "bravo");
            let mut request = bad
                .credential_broker_request
                .take()
                .unwrap()
                .expose()
                .clone();
            request[field] = value;
            bad.credential_broker_request = Some(request.into());
            let error = router(
                backend,
                vec![broker_config(backend, "alpha"), bad],
                http.clone(),
            )
            .await
            .err()
            .expect("invalid broker configuration must fail before startup");
            assert!(!error.contains("secret-marker"));
            assert!(http.requests.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn azure_broker_requires_the_configured_container_and_account() {
    for field in ["bucket", "azureAccountName"] {
        for value in [None, Some(json!("other"))] {
            let http = Arc::new(OriginHttp::default());
            let mut config = broker_config(Backend::Azure, "alpha");
            let mut request = config
                .credential_broker_request
                .take()
                .unwrap()
                .expose()
                .clone();
            match value {
                Some(value) => request[field] = value,
                None => {
                    request.as_object_mut().unwrap().remove(field);
                }
            }
            config.credential_broker_request = Some(request.into());
            assert!(router(Backend::Azure, vec![config], http.clone())
                .await
                .is_err());
            assert!(http.requests.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn s3_uses_each_bucket_region_and_addressing_mode() {
    let http = Arc::new(OriginHttp::default());
    let mut virtual_host = static_config(Backend::S3, "alpha");
    virtual_host.s3_endpoint = None;
    virtual_host.s3_region = Some("us-west-2".into());
    virtual_host.s3_path_style = Some(false);
    let mut path_style = static_config(Backend::S3, "bravo");
    path_style.s3_endpoint = Some("http://localhost:9000/prefix/".into());
    let router = router(Backend::S3, vec![virtual_host, path_style], http.clone())
        .await
        .unwrap();
    for bucket in ["alpha", "bravo"] {
        router
            .head(&ObjectId::new(Backend::S3, bucket, "shared/key"))
            .await
            .unwrap();
    }
    let requests = http.requests.lock().unwrap();
    assert_eq!(
        requests[0].url,
        "https://alpha.s3.us-west-2.amazonaws.com/shared/key"
    );
    assert_eq!(
        requests[1].url,
        "http://localhost:9000/prefix/bravo/shared/key"
    );
    assert!(requests[0]
        .header("authorization")
        .unwrap()
        .contains("/us-west-2/s3/"));
    assert!(requests[1]
        .header("authorization")
        .unwrap()
        .contains("/bravo-region/s3/"));
}

#[tokio::test]
async fn ambiguous_object_targets_never_reach_real_http() {
    use axum::body::Body;
    use axum::extract::State;
    use axum::http::{Request, Response};

    let received = Arc::new(Mutex::new(Vec::<String>::new()));
    let app = axum::Router::new()
        .fallback(axum::routing::any(
            |State(received): State<Arc<Mutex<Vec<String>>>>, request: Request<Body>| async move {
                received.lock().unwrap().push(request.uri().to_string());
                Response::builder()
                    .header("content-length", "4")
                    .header("etag", "\"v1\"")
                    .header("x-goog-generation", "1")
                    .body(Body::from("data"))
                    .unwrap()
            },
        ))
        .with_state(received.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let origin = Arc::new(ReqwestClient::with_client(
        reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap(),
    ));
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), b"data").unwrap();
    // The token modes exercise the same raw URL construction used by IAM:
    // GCS Bearer and Azure SAS, alongside S3 signatures and static account keys.
    for backend in [Backend::S3, Backend::Gcs, Backend::Azure] {
        for broker in [false, true] {
            let mut config = if broker {
                broker_config(backend, "alpha")
            } else {
                static_config(backend, "alpha")
            };
            match backend {
                Backend::S3 => config.s3_endpoint = Some(endpoint.clone()),
                Backend::Gcs => config.gcs_endpoint = Some(endpoint.clone()),
                Backend::Azure => config.azure_endpoint = Some(endpoint.clone()),
            }
            let exchange = Arc::new(BrokerHttp {
                requests: Mutex::new(vec![]),
                first_expiry: SystemTime::now() + Duration::from_secs(3600),
                second_expiry: SystemTime::now() + Duration::from_secs(3600),
            });
            let router = BackendRouter::from_configs(
                backend,
                vec![config],
                origin.clone(),
                exchange,
                Arc::new(NoopObserver),
            )
            .await
            .unwrap();

            for (key, expected_suffix) in [
                ("safe/key..name", "/alpha/safe/key..name"),
                ("数据/a b.txt", "/alpha/%E6%95%B0%E6%8D%AE/a%20b.txt"),
            ] {
                let object = ObjectId::new(backend, "alpha", key);
                router.validate_object(&object).unwrap();
                assert_eq!(
                    router.fetch_range(&object, 0, 4).await.unwrap(),
                    b"data".as_slice()
                );
                let request = received.lock().unwrap().pop().unwrap();
                let path = request.split('?').next().unwrap();
                assert!(path.ends_with(expected_suffix), "{backend}: {path}");
            }

            for key in [
                "../unconfigured/key",
                "%2e%2e/unconfigured/key",
                ".%2E/unconfigured/key",
                "%2E./unconfigured/key",
                "..\\unconfigured\\key",
                "%2e%2e%2funconfigured/key",
                "%2e%2e%5cunconfigured/key",
                "%252e%252e/unconfigured/key",
                "%252e%252e%252funconfigured/key",
                "",
                ".",
                "..",
                ".. ",
                "./key",
                "a/../key",
                "a/./key",
                "/key",
                "//unconfigured/key",
                "a//key",
                "a/",
                "key ",
                "key?comp=metadata",
                "key#fragment",
                "key%3fcomp=metadata",
                "key%23fragment",
                "key%0d%0aheader",
                "key%00",
                "key%7f",
                "key\0",
                "key\r\n",
                "key\t",
            ] {
                let object = ObjectId::new(backend, "alpha", key);
                assert!(
                    router.head(&object).await.is_err(),
                    "{backend} broker={broker} accepted {key:?}; sent {:?}",
                    received.lock().unwrap()
                );
                assert!(router.validate_object(&object).is_err());
                assert!(router.fetch_range(&object, 0, 4).await.is_err());
                assert!(router.fetch_range(&object, 0, 0).await.is_err());
                assert!(router
                    .fetch_range_if_match(&object, 0, 4, Some(&Version::new("1")))
                    .await
                    .is_err());
                assert!(router
                    .put(&object, Bytes::from_static(b"data"))
                    .await
                    .is_err());
                assert!(router
                    .put_if_match(&object, Bytes::new(), Some(&Version::new("1")))
                    .await
                    .is_err());
                assert!(router.put_file(&object, file.path(), 4).await.is_err());
                assert!(router.delete(&object).await.is_err());
                assert!(
                    received.lock().unwrap().is_empty(),
                    "{backend} broker={broker} sent {key:?} to the origin"
                );
            }
        }
    }
    server.abort();
}
