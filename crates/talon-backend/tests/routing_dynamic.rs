use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::SystemTime;

use async_trait::async_trait;
use serde_json::json;
use talon_backend::{
    BackendRouter, BrokerConfig, DefaultBackendResolver, HttpClient, HttpRequest, HttpResponse,
    NoopObserver,
};
use talon_core::{Backend, BackendStore, ObjectId};

#[derive(Default)]
struct Http {
    requests: Mutex<Vec<HttpRequest>>,
    failures_remaining: AtomicUsize,
    gate: Option<Arc<tokio::sync::Semaphore>>,
}

#[async_trait]
impl HttpClient for Http {
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, String> {
        let broker = request.method == talon_backend::Method::Post;
        let payload: serde_json::Value = if broker {
            serde_json::from_slice(&request.body).unwrap()
        } else {
            serde_json::Value::Null
        };
        self.requests.lock().unwrap().push(request);
        if broker {
            if let Some(gate) = &self.gate {
                gate.acquire().await.unwrap().forget();
            }
            if self
                .failures_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err("sensitive transport detail".into());
            }
        }
        let (access, token) = if payload["csp"] == "azure" {
            (
                payload["azureAccountName"].as_str().unwrap(),
                "sv=2024-01-01&sig=token",
            )
        } else {
            ("access", "token")
        };
        Ok(HttpResponse {
            status: 200,
            headers: vec![
                ("content-length".into(), "4".into()),
                ("etag".into(), "v1".into()),
            ],
            body: if broker {
                json!({"success":true,"credentials":{"tempAk":access,"tempSk":"secret","sessionToken":token,"expiredAt":"2099-01-01T00:00:00Z"}}).to_string().into()
            } else {
                "data".into()
            },
        })
    }
}

fn origin_options(endpoint: &str) -> talon_core::OriginOptions {
    talon_core::OriginOptions {
        endpoint: Some(endpoint.into()),
        ..Default::default()
    }
}

fn aws() -> BrokerConfig {
    BrokerConfig {
        origin: Some(talon_core::OriginOptions {
            region: Some("us-west-2".into()),
            ..origin_options("https://s3.us-west-2.amazonaws.com")
        }),
        url: "https://broker.example/exchange".into(),
        request: json!({"roleArn":"arn:aws:iam::123:role/test","region":"us-west-2","csp":"aws"}),
    }
}

#[tokio::test]
async fn dynamic_requests_require_an_explicit_endpoint_before_broker_io() {
    for csp in ["aws", "ali", "tc", "hwc", "gcp", "azure"] {
        let backend = match csp {
            "gcp" => Backend::Gcs,
            "azure" => Backend::Azure,
            _ => Backend::S3,
        };
        let http = Arc::new(Http::default());
        let router = dynamic_router(backend, http.clone());
        for origin in [
            None,
            Some(talon_core::OriginOptions::default()),
            Some(talon_core::OriginOptions {
                endpoint: Some(String::new()),
                ..Default::default()
            }),
            Some(talon_core::OriginOptions {
                endpoint: Some(" ".into()),
                ..Default::default()
            }),
        ] {
            let broker = BrokerConfig {
                url: "https://broker.example/exchange".into(),
                request: json!({"csp":csp,"credentialType":"iam","region":"us-west-2","bucket":"bucket","azureAccountName":"account"}),
                origin,
            };
            assert!(
                router
                    .bind_request(backend, "bucket", &broker)
                    .await
                    .is_err(),
                "{csp} must require an explicit endpoint"
            );
            assert!(http.requests.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn s3_signing_region_is_explicit_and_independent_of_broker_region() {
    for csp in ["aws", "ali", "tc", "hwc"] {
        let http = Arc::new(Http::default());
        let router = dynamic_router(Backend::S3, http.clone());
        let mut broker = BrokerConfig {
            url: "https://broker.example/exchange".into(),
            request: json!({"csp":csp,"credentialType":"iam","region":"us-west-2"}),
            origin: Some(origin_options("https://origin.example")),
        };
        for region in [None, Some(String::new()), Some(" ".into())] {
            broker.origin.as_mut().unwrap().region = region;
            assert!(router
                .bind_request(Backend::S3, "bucket", &broker)
                .await
                .is_err());
            assert!(http.requests.lock().unwrap().is_empty());
        }
        broker.origin.as_mut().unwrap().region = Some("eu-central-1".into());
        let resolved = router
            .bind_request(Backend::S3, "bucket", &broker)
            .await
            .unwrap();
        resolved
            .store
            .head(&ObjectId::new(Backend::S3, "bucket", "key"))
            .await
            .unwrap();
        let requests = http.requests.lock().unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(payload["region"], "us-west-2");
        assert!(requests[1]
            .header("authorization")
            .unwrap()
            .contains("/eu-central-1/s3/"));
    }
}

#[tokio::test]
async fn request_origin_overrides_defaults_and_isolates_backend_bindings() {
    let http = Arc::new(Http::default());
    let router = BackendRouter::new(
        Backend::S3,
        Arc::new(
            DefaultBackendResolver::new(
                Backend::S3,
                http.clone(),
                http.clone(),
                Arc::new(NoopObserver),
            )
            .with_dynamic_defaults(talon_backend::DynamicBackendDefaults {
                s3_path_style: Some(false),
            }),
        ),
    );
    let mut broker = aws();
    broker.origin = Some(talon_core::OriginOptions {
        endpoint: Some("http://first.example:9000".into()),
        region: Some("eu-central-1".into()),
        use_ssl: Some(false),
        use_virtual_host: Some(false),
        ..Default::default()
    });
    let first = router
        .bind_request(Backend::S3, "bucket", &broker)
        .await
        .unwrap();
    let repeat = router
        .bind_request(Backend::S3, "bucket", &broker)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&first.store, &repeat.store));
    first
        .store
        .head(&ObjectId::new(Backend::S3, "bucket", "key"))
        .await
        .unwrap();
    broker.origin.as_mut().unwrap().endpoint = Some("http://second.example:9000".into());
    let second = router
        .bind_request(Backend::S3, "bucket", &broker)
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&first.store, &second.store));
    assert_ne!(first.scope, second.scope);
    second
        .store
        .head(&ObjectId::new(Backend::S3, "bucket", "key"))
        .await
        .unwrap();
    let requests = http.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    for (i, host) in [(1, "first"), (3, "second")] {
        assert_eq!(
            requests[i].url,
            format!("http://{host}.example:9000/bucket/key")
        );
        assert!(requests[i]
            .header("authorization")
            .unwrap()
            .contains("/eu-central-1/s3/"));
    }
    for i in [0, 2] {
        let payload: serde_json::Value = serde_json::from_slice(&requests[i].body).unwrap();
        assert_eq!(payload["region"], "us-west-2");
        assert!(
            payload.get("origin").is_none(),
            "origin settings are not Broker API arguments"
        );
    }
}

#[tokio::test]
async fn request_origin_uses_existing_cloud_endpoint_and_addressing_modes() {
    for (backend, request, virtual_host, expected) in [
        (
            Backend::S3,
            aws().request,
            true,
            "http://bucket.origin.example:9000/key",
        ),
        (
            Backend::S3,
            aws().request,
            false,
            "http://origin.example:9000/bucket/key",
        ),
        (
            Backend::Gcs,
            json!({"csp":"gcp","gcpServiceAccountEmail":"target@example"}),
            true,
            "http://origin.example:9000/bucket/key",
        ),
        (
            Backend::Azure,
            json!({"csp":"azure","credentialType":"iam","bucket":"bucket","azureAccountName":"account"}),
            true,
            "http://origin.example:9000/bucket/key",
        ),
        (
            Backend::Azure,
            json!({"csp":"azure","credentialType":"iam","bucket":"bucket","azureAccountName":"account"}),
            false,
            "http://origin.example:9000/account/bucket/key",
        ),
    ] {
        let http = Arc::new(Http::default());
        let router = dynamic_router(backend, http.clone());
        let config = BrokerConfig {
            url: aws().url,
            request,
            origin: Some(talon_core::OriginOptions {
                endpoint: Some("origin.example:9000".into()),
                region: (backend == Backend::S3).then(|| "us-west-2".into()),
                use_ssl: Some(false),
                use_virtual_host: Some(virtual_host),
                ..Default::default()
            }),
        };
        let bound = router
            .bind_request(backend, "bucket", &config)
            .await
            .unwrap();
        bound
            .store
            .head(&ObjectId::new(backend, "bucket", "key"))
            .await
            .unwrap();
        let requests = http.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].url.split('?').next().unwrap(), expected);
    }
}

#[tokio::test]
async fn explicit_endpoint_spellings_and_effective_options_reuse_the_binding() {
    let http = Arc::new(Http::default());
    let router = dynamic_router(Backend::S3, http.clone());
    let mut broker = aws();
    let baseline = router
        .bind_request(Backend::S3, "bucket", &broker)
        .await
        .unwrap();
    for origin in [
        talon_core::OriginOptions {
            region: Some("us-west-2".into()),
            ..origin_options("s3.us-west-2.amazonaws.com")
        },
        talon_core::OriginOptions {
            endpoint: Some("https://s3.us-west-2.amazonaws.com/".into()),
            region: Some("us-west-2".into()),
            use_ssl: Some(true),
            use_virtual_host: Some(true),
            ..Default::default()
        },
    ] {
        broker.origin = Some(origin);
        let bound = router
            .bind_request(Backend::S3, "bucket", &broker)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&baseline.store, &bound.store));
        assert_eq!(baseline.scope, bound.scope);
    }
    assert_eq!(http.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn origin_timeout_and_reserved_options_keep_identity_without_changing_storage_transport() {
    let http = Arc::new(Http::default());
    let router = dynamic_router(Backend::Gcs, http.clone());
    let mut broker = BrokerConfig {
        url: aws().url,
        request: json!({"csp":"gcp","gcpServiceAccountEmail":"target@example"}),
        origin: Some(origin_options("https://storage.googleapis.com")),
    };
    let baseline = router
        .bind_request(Backend::Gcs, "bucket", &broker)
        .await
        .unwrap();
    broker.origin = Some(talon_core::OriginOptions {
        endpoint: Some("https://storage.googleapis.com".into()),
        region: Some("reserved-region".into()),
        use_virtual_host: Some(true),
        tls_ca_pem: Some("reserved CA content, not parsed by the HTTP client".into()),
        tls_min_version: Some("1.0".into()),
        request_timeout_ms: Some(1),
        max_connections: Some(1),
        ..Default::default()
    });
    let reserved = router
        .bind_request(Backend::Gcs, "bucket", &broker)
        .await
        .unwrap();
    assert_ne!(baseline.scope, reserved.scope);
    let repeat = router
        .bind_request(Backend::Gcs, "bucket", &broker)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&reserved.store, &repeat.store));
    for binding in [baseline, reserved] {
        binding
            .store
            .head(&ObjectId::new(Backend::Gcs, "bucket", "key"))
            .await
            .unwrap();
    }
    let requests = http.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[2].url, "https://storage.googleapis.com/bucket/key");
    assert_eq!(requests[2].url, requests[3].url);
    for request in &requests[..2] {
        let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        assert!(body.get("origin").is_none());
        assert!(body.get("tls_ca_pem").is_none());
    }
}

#[tokio::test]
async fn request_origin_is_validated_before_broker_io() {
    let http = Arc::new(Http::default());
    let router = dynamic_router(Backend::S3, http.clone());
    let mut broker = aws();
    broker.origin = Some(talon_core::OriginOptions {
        endpoint: Some("https://origin.example".into()),
        region: Some("us-west-2".into()),
        use_ssl: Some(false),
        ..Default::default()
    });
    assert!(router
        .bind_request(Backend::S3, "bucket", &broker)
        .await
        .is_err());
    assert!(http.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn request_origin_preserves_explicit_ports_when_selecting_http() {
    let http = Arc::new(Http::default());
    let router = dynamic_router(Backend::S3, http.clone());
    let mut broker = aws();
    broker.origin = Some(talon_core::OriginOptions {
        endpoint: Some("origin.example:443".into()),
        region: Some("us-west-2".into()),
        use_ssl: Some(false),
        use_virtual_host: Some(false),
        ..Default::default()
    });
    let port443 = router
        .bind_request(Backend::S3, "bucket", &broker)
        .await
        .unwrap();
    broker.origin.as_mut().unwrap().endpoint = Some("origin.example".into());
    let port80 = router
        .bind_request(Backend::S3, "bucket", &broker)
        .await
        .unwrap();
    assert_ne!(port443.scope, port80.scope);
    port443
        .store
        .head(&ObjectId::new(Backend::S3, "bucket", "key"))
        .await
        .unwrap();
    assert_eq!(
        http.requests.lock().unwrap().last().unwrap().url,
        "http://origin.example:443/bucket/key"
    );
}

#[tokio::test]
async fn pod_iam_bindings_reuse_configuration_without_sharing_assumed_role_scopes() {
    for (backend, iam, legacy) in [
        (
            Backend::S3,
            json!({"csp":"aws","credentialType":"iam","region":"us-west-2"}),
            aws().request,
        ),
        (
            Backend::Gcs,
            json!({"csp":"gcp","credentialType":"iam"}),
            json!({"csp":"gcp","gcpServiceAccountEmail":"target@example"}),
        ),
        (
            Backend::Azure,
            json!({"csp":"azure","credentialType":"iam","bucket":"bucket","azureAccountName":"account"}),
            json!({"csp":"azure","region":"westus3","bucket":"bucket","azureAccountName":"account","azureClientId":"client","azureTenantId":"tenant"}),
        ),
    ] {
        let http = Arc::new(Http::default());
        let router = dynamic_router(backend, http.clone());
        let mut config = BrokerConfig {
            origin: Some(talon_core::OriginOptions {
                region: (backend == Backend::S3).then(|| "us-west-2".into()),
                ..origin_options(match backend {
                    Backend::S3 => "https://s3.us-west-2.amazonaws.com",
                    Backend::Gcs => "https://storage.googleapis.com",
                    Backend::Azure => "https://account.blob.core.windows.net",
                })
            }),
            url: aws().url,
            request: iam,
        };
        let ignored_targets = [
            ("roleArn", json!("ignored-role")),
            ("externalId", json!("ignored-external-id")),
            ("gcpServiceAccountEmail", json!(null)),
            ("azureClientId", json!(false)),
            ("azureTenantId", json!({"ignored": true})),
        ];
        for (field, value) in &ignored_targets {
            config.request[field] = value.clone();
        }
        let first = router
            .bind_request(backend, "bucket", &config)
            .await
            .unwrap();
        {
            let requests = http.requests.lock().unwrap();
            let sent: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
            for (field, _) in &ignored_targets {
                assert!(sent.get(field).is_none(), "IAM must not forward {field}");
                config.request.as_object_mut().unwrap().remove(*field);
            }
        }
        config.request["durationSeconds"] = 3600.into();
        let second = router
            .bind_request(backend, "bucket", &config)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first.store, &second.store));
        assert_eq!(first.scope, second.scope);
        assert!(first.scope.is_some());
        config.request = legacy;
        let assumed = router
            .bind_request(backend, "bucket", &config)
            .await
            .unwrap();
        assert_ne!(first.scope, assumed.scope);
        assert!(!Arc::ptr_eq(&first.store, &assumed.store));
        config.request["credentialType"] = "stsAssumeRole".into();
        let explicit = router
            .bind_request(backend, "bucket", &config)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&assumed.store, &explicit.store));
        assert_eq!(http.requests.lock().unwrap().len(), 2);
    }
}

#[tokio::test]
async fn pod_iam_static_bucket_accepts_the_same_broker_payload_from_toml() {
    let patch = talon_core::WorkerConfigPatch::from_toml(
        r#"
backend = "gcs"
[[buckets]]
name = "bucket"
credentials_source = "broker"
credential_broker_url = "https://broker.example/exchange"
credential_broker_request.csp = "gcp"
credential_broker_request.credentialType = "iam"
"#,
    )
    .unwrap();
    let config =
        talon_core::WorkerConfig::resolve(patch, Default::default(), Default::default()).unwrap();
    let http = Arc::new(Http::default());
    let router = BackendRouter::from_configs(
        Backend::Gcs,
        config.resolved_buckets().unwrap().unwrap(),
        http.clone(),
        http.clone(),
        Arc::new(NoopObserver),
    )
    .await
    .unwrap();
    router
        .head(&ObjectId::new(Backend::Gcs, "bucket", "key"))
        .await
        .unwrap();
    let requests = http.requests.lock().unwrap();
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["credentialType"], "iam");
    assert_eq!(requests[1].header("authorization"), Some("Bearer token"));
}

#[tokio::test]
async fn pod_iam_s3_compatible_routes_require_an_explicit_origin_endpoint() {
    for (csp, region, endpoint) in [
        ("ali", "cn-hangzhou", "s3.oss-cn-hangzhou.aliyuncs.com"),
        ("tc", "ap-beijing", "cos.ap-beijing.myqcloud.com"),
        ("hwc", "cn-east-3", "obs.cn-east-3.myhuaweicloud.com"),
    ] {
        let http = Arc::new(Http::default());
        let config = BrokerConfig {
            origin: None,
            url: aws().url,
            request: json!({"credentialType":"iam","csp":csp,"region":region}),
        };
        assert!(dynamic_router(Backend::S3, http.clone())
            .bind_request(Backend::S3, "bucket", &config)
            .await
            .is_err());
        assert!(
            http.requests.lock().unwrap().is_empty(),
            "missing endpoint must fail before credentials are acquired"
        );
        let mut bucket = talon_core::BucketConfig {
            name: "bucket".into(),
            credentials_source: "broker".into(),
            s3_region: Some(region.into()),
            credential_broker_url: Some(config.url.clone().into()),
            credential_broker_request: Some(config.request.clone().into()),
            ..Default::default()
        };
        assert!(BackendRouter::from_configs(
            Backend::S3,
            vec![bucket.clone()],
            http.clone(),
            http.clone(),
            Arc::new(NoopObserver)
        )
        .await
        .is_err());
        assert!(http.requests.lock().unwrap().is_empty());
        bucket.s3_endpoint = Some(endpoint.into());
        let router = BackendRouter::from_configs(
            Backend::S3,
            vec![bucket],
            http.clone(),
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await
        .unwrap();
        router
            .head(&ObjectId::new(Backend::S3, "bucket", "key"))
            .await
            .unwrap();
        let dynamic = dynamic_router(Backend::S3, http.clone());
        let mut config = config;
        config.origin = Some(talon_core::OriginOptions {
            region: Some(region.into()),
            endpoint: Some(endpoint.into()),
            ..Default::default()
        });
        let bound = dynamic
            .bind_request(Backend::S3, "bucket", &config)
            .await
            .unwrap();
        bound
            .store
            .head(&ObjectId::new(Backend::S3, "bucket", "key"))
            .await
            .unwrap();
        let requests = http.requests.lock().unwrap();
        assert_eq!(requests[1].url, format!("https://bucket.{endpoint}/key"));
        assert_eq!(requests[3].url, requests[1].url);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&requests[2].body).unwrap()["csp"],
            csp
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&requests[0].body).unwrap()["csp"],
            csp
        );
    }
}

#[tokio::test]
async fn dynamic_keys_separate_roles_urls_and_azure_accounts() {
    for backend in [Backend::S3, Backend::Azure] {
        let http = Arc::new(Http::default());
        let router = dynamic_router(backend, http.clone());
        let mut config = if backend == Backend::Azure {
            BrokerConfig {
                origin: Some(origin_options("https://accounta.blob.core.windows.net")),
                url: aws().url,
                request: json!({"csp":"azure","region":"eastus","bucket":"bucket","azureAccountName":"accounta","azureClientId":"client","azureTenantId":"tenant"}),
            }
        } else {
            aws()
        };
        let first = router
            .bind_request(backend, "bucket", &config)
            .await
            .unwrap();
        config.url = "https://other-broker.example/exchange".into();
        let second = router
            .bind_request(backend, "bucket", &config)
            .await
            .unwrap();
        assert_ne!(first.scope, second.scope);
        assert!(!Arc::ptr_eq(&first.store, &second.store));
        if backend == Backend::Azure {
            config.request["azureAccountName"] = "accountb".into();
        } else {
            config.request["roleArn"] = "arn:aws:iam::123:role/other".into();
        }
        let third = router
            .bind_request(backend, "bucket", &config)
            .await
            .unwrap();
        assert_ne!(second.scope, third.scope);
        assert!(!Arc::ptr_eq(&second.store, &third.store));
        config.request["externalId"] = "different-external-identity".into();
        let fourth = router
            .bind_request(backend, "bucket", &config)
            .await
            .unwrap();
        assert_ne!(third.scope, fourth.scope);
        assert!(!Arc::ptr_eq(&third.store, &fourth.store));
    }
}

#[tokio::test(start_paused = true)]
async fn failed_dynamic_acquisition_is_retryable_and_never_uses_static_route() {
    let http = Arc::new(Http::default());
    let config = talon_core::BucketConfig {
        name: "bucket".into(),
        credentials_source: "aksk".into(),
        s3_region: Some("us-east-1".into()),
        s3_access_key_id: Some("static-key".into()),
        s3_secret_access_key: Some("static-secret".to_owned().into()),
        ..Default::default()
    };
    let router = BackendRouter::from_configs(
        Backend::S3,
        vec![config],
        http.clone(),
        http.clone(),
        Arc::new(NoopObserver),
    )
    .await
    .unwrap();
    http.failures_remaining.store(4, Ordering::SeqCst);
    assert!(router
        .bind_request(Backend::S3, "bucket", &aws())
        .await
        .is_err());
    let resolved = router
        .bind_request(Backend::S3, "bucket", &aws())
        .await
        .unwrap();
    assert!(resolved.scope.is_some());
    resolved
        .store
        .head(&ObjectId::new(Backend::S3, "bucket", "key"))
        .await
        .unwrap();
    let requests = http.requests.lock().unwrap();
    assert_eq!(requests.len(), 6);
    assert!(requests[..5]
        .iter()
        .all(|request| request.method == talon_backend::Method::Post));
    assert!(requests[5]
        .header("authorization")
        .unwrap()
        .contains("Credential=access/"));
    assert!(!requests[5]
        .header("authorization")
        .unwrap()
        .contains("static-key"));
}

#[tokio::test]
async fn same_key_initializes_once_other_keys_progress_and_canceled_caller_does_not_stall() {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let http = Arc::new(Http {
        gate: Some(gate.clone()),
        ..Default::default()
    });
    let router = Arc::new(dynamic_router(Backend::S3, http.clone()));
    let first_router = router.clone();
    let first = tokio::spawn(async move {
        first_router
            .bind_request(Backend::S3, "bucket", &aws())
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while http.requests.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    first.abort();
    let mut callers = Vec::new();
    for _ in 0..8 {
        let router = router.clone();
        callers.push(tokio::spawn(async move {
            router
                .bind_request(Backend::S3, "bucket", &aws())
                .await
                .unwrap()
        }));
    }
    let other_router = router.clone();
    let other = tokio::spawn(async move {
        other_router
            .bind_request(Backend::S3, "other", &aws())
            .await
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while http.requests.lock().unwrap().len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(http.requests.lock().unwrap().len(), 2);
    gate.add_permits(2);
    let first = callers.remove(0).await.unwrap();
    for caller in callers {
        assert!(Arc::ptr_eq(&first.store, &caller.await.unwrap().store));
    }
    assert_ne!(first.scope, other.await.unwrap().scope);
}

#[tokio::test]
async fn dynamic_scope_validation_happens_before_exchange() {
    let http = Arc::new(Http::default());
    let router = dynamic_router(Backend::S3, http.clone());
    for (field, value) in [
        ("bucket", json!("other")),
        ("bucket", json!(null)),
        ("csp", json!("azure")),
        ("durationSeconds", json!("secret")),
    ] {
        let mut config = aws();
        config.request[field] = value;
        assert!(router
            .bind_request(Backend::S3, "bucket", &config)
            .await
            .is_err());
    }
    assert!(router
        .bind_request(Backend::Azure, "bucket", &aws())
        .await
        .is_err());
    assert!(http.requests.lock().unwrap().is_empty());
}

fn dynamic_router(backend: Backend, http: Arc<Http>) -> BackendRouter {
    BackendRouter::new(
        backend,
        Arc::new(DefaultBackendResolver::new(
            backend,
            http.clone(),
            http,
            Arc::new(NoopObserver),
        )),
    )
}

#[tokio::test]
async fn dynamic_binding_reuses_normalized_configuration_and_limits_target() {
    let http = Arc::new(Http::default());
    let router = dynamic_router(Backend::S3, http.clone());
    let broker = BrokerConfig {
        origin: aws().origin,
        url: "https://broker.example/exchange".into(),
        request: json!({"roleArn":"arn:aws:iam::123:role/test","region":"us-west-2","csp":"aws"}),
    };
    let first = router
        .bind_request(Backend::S3, "bucket", &broker)
        .await
        .unwrap();
    let equivalent = BrokerConfig {
        origin: aws().origin,
        url: broker.url.clone(),
        request: serde_json::from_str(r#"{"durationSeconds":3600,"bucket":"bucket","csp":"aws","region":"us-west-2","roleArn":"arn:aws:iam::123:role/test"}"#).unwrap(),
    };
    let second = router
        .bind_request(Backend::S3, "bucket", &equivalent)
        .await
        .unwrap();
    assert_eq!(first.scope, second.scope);
    assert!(first.scope.is_some());
    assert!(Arc::ptr_eq(&first.store, &second.store));
    let mut default_duration = equivalent.clone();
    default_duration.request["durationSeconds"] = (-1).into();
    let third = router
        .bind_request(Backend::S3, "bucket", &default_duration)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&first.store, &third.store));
    assert_eq!(http.requests.lock().unwrap().len(), 1);
    assert!(first
        .store
        .head(&ObjectId::new(Backend::S3, "other", "key"))
        .await
        .is_err());
    first
        .store
        .head(&ObjectId::new(Backend::S3, "bucket", "key"))
        .await
        .unwrap();
    let requests = http.requests.lock().unwrap();
    assert_eq!(
        requests[1].url,
        "https://bucket.s3.us-west-2.amazonaws.com/key"
    );
    assert!(requests[1]
        .header("authorization")
        .unwrap()
        .contains("/us-west-2/s3/"));
}

#[tokio::test]
async fn gcs_request_order_and_nested_fields_normalize_without_losing_identity() {
    let http = Arc::new(Http::default());
    let router = dynamic_router(Backend::Gcs, http.clone());
    let first = BrokerConfig {
        origin: Some(origin_options("https://storage.googleapis.com")),
        url: "https://BROKER.example:443/exchange".into(),
        request: serde_json::from_str(
            r#"{"csp":"gcp","gcpServiceAccountEmail":"account@example","context":{"z":2,"a":1}}"#,
        )
        .unwrap(),
    };
    let second = BrokerConfig {
        origin: Some(origin_options("https://storage.googleapis.com")),
        url: "https://broker.example/exchange".into(),
        request: serde_json::from_str(r#"{"context":{"a":1,"z":2},"bucket":"bucket","durationSeconds":3600,"gcpServiceAccountEmail":"account@example","csp":"gcp"}"#).unwrap(),
    };
    let first = router
        .bind_request(Backend::Gcs, "bucket", &first)
        .await
        .unwrap();
    let second = router
        .bind_request(Backend::Gcs, "bucket", &second)
        .await
        .unwrap();
    assert_eq!(first.scope, second.scope);
    assert!(Arc::ptr_eq(&first.store, &second.store));
    first
        .store
        .head(&ObjectId::new(Backend::Gcs, "bucket", "key"))
        .await
        .unwrap();
    let requests = http.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1].header("authorization"), Some("Bearer token"));
}

#[tokio::test]
async fn legacy_factory_is_lazy_and_keeps_legacy_paths_while_dynamic_requests_bypass_it() {
    let http = Arc::new(Http::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let factory_calls = calls.clone();
    let origin = http.clone();
    let factory: talon_backend::LegacyBackendFactory = Arc::new(move || {
        factory_calls.fetch_add(1, Ordering::SeqCst);
        let store: Arc<dyn BackendStore> = Arc::new(talon_backend::S3Backend::new(
            talon_backend::S3Config::aws("us-east-1"),
            talon_backend::S3Credentials {
                access_key_id: "legacy".into(),
                secret_access_key: "secret".into(),
                session_token: None,
            },
            origin.clone(),
        ));
        Box::pin(async move { Ok(store) })
    });
    let resolver =
        DefaultBackendResolver::new(Backend::S3, http.clone(), http, Arc::new(NoopObserver))
            .with_legacy_factory(factory);
    let router = BackendRouter::new(Backend::S3, Arc::new(resolver));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(!router.ready_for_background());
    router
        .bind_request(Backend::S3, "bucket", &aws())
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let legacy = ObjectId::new(Backend::S3, "bucket", "legacy//path");
    router.validate_object(&legacy).unwrap();
    router.head(&legacy).await.unwrap();
    router.head(&legacy).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(router.ready_for_background());
}

struct RotatingExchange(AtomicUsize);

#[async_trait]
impl HttpClient for RotatingExchange {
    async fn execute(&self, _: HttpRequest) -> Result<HttpResponse, String> {
        let generation = self.0.fetch_add(1, Ordering::SeqCst);
        let expiry =
            SystemTime::now() + Duration::from_secs(if generation == 0 { 90 } else { 3600 });
        Ok(HttpResponse {
            status: 200,
            headers: vec![],
            body: json!({"success":true,"credentials":{
                "tempAk":format!("access-{generation}"),
                "tempSk":"secret",
                "sessionToken":format!("token-{generation}"),
                "expiredAt":chrono::DateTime::<chrono::Utc>::from(expiry).to_rfc3339(),
            }})
            .to_string()
            .into(),
        })
    }
}

#[tokio::test(start_paused = true)]
async fn refreshed_credentials_keep_the_backend_and_access_scope() {
    assert_refresh_keeps_binding(aws()).await;
}

#[tokio::test(start_paused = true)]
async fn pod_iam_refresh_keeps_the_backend_and_access_scope() {
    assert_refresh_keeps_binding(BrokerConfig {
        origin: aws().origin,
        url: aws().url,
        request: json!({"csp":"aws","region":"us-west-2","credentialType":"iam"}),
    })
    .await;
}

async fn assert_refresh_keeps_binding(broker: BrokerConfig) {
    let origin = Arc::new(Http::default());
    let exchange = Arc::new(RotatingExchange(AtomicUsize::new(0)));
    let router = BackendRouter::new(
        Backend::S3,
        Arc::new(DefaultBackendResolver::new(
            Backend::S3,
            origin.clone(),
            exchange.clone(),
            Arc::new(NoopObserver),
        )),
    );
    let first = router
        .bind_request(Backend::S3, "bucket", &broker)
        .await
        .unwrap();
    let object = ObjectId::new(Backend::S3, "bucket", "key");
    first.store.head(&object).await.unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(35)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        exchange.0.load(Ordering::SeqCst),
        1,
        "idle providers must not refresh"
    );
    let second = router
        .bind_request(Backend::S3, "bucket", &broker)
        .await
        .unwrap();
    second.store.head(&object).await.unwrap();
    assert!(Arc::ptr_eq(&first.store, &second.store));
    assert_eq!(first.scope, second.scope);
    assert_eq!(exchange.0.load(Ordering::SeqCst), 2);
    let requests = origin.requests.lock().unwrap();
    assert_eq!(requests[0].header("x-amz-security-token"), Some("token-0"));
    assert_eq!(requests[1].header("x-amz-security-token"), Some("token-1"));
}
