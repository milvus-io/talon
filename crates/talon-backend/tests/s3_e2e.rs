//! S3 backend end-to-end test against a real S3-compatible emulator (LocalStack).
//!
//! Builds the real [`S3Backend`] over [`ReqwestClient`] against a live endpoint
//! (exercising AWS SigV4 signing #264 + path-style wiring) and runs the shared
//! backend-conformance suite against it, so S3 is held to the same contract as
//! GCS and Azure.
//!
//! Skipped unless `TALON_S3_TEST_ENDPOINT` is set. See CI's `s3-e2e` job for the
//! required env and the seeded object (4096 bytes, byte i == i % 251).

mod conformance;

use std::sync::Arc;

use talon_backend::{endpoint_host, ReqwestClient, S3Backend, S3Config, S3Credentials};
use talon_core::{Backend, ObjectId};

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

#[tokio::test]
async fn s3_backend_conformance_end_to_end() {
    let Some(endpoint) = env("TALON_S3_TEST_ENDPOINT") else {
        eprintln!("skipping S3 e2e: TALON_S3_TEST_ENDPOINT is not set");
        return;
    };
    let bucket = env("TALON_S3_TEST_BUCKET").expect("TALON_S3_TEST_BUCKET must be set");
    let key = env("TALON_S3_TEST_KEY").unwrap_or_else(|| "e2e/object.bin".to_string());
    let region = env("TALON_S3_TEST_REGION").unwrap_or_else(|| "us-east-1".to_string());
    let access_key_id = env("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID must be set");
    let secret_access_key =
        env("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY must be set");

    let (host, tls) = endpoint_host(&endpoint);
    let config = S3Config {
        region,
        endpoint: host,
        path_style: true,
        tls,
    };
    let creds = S3Credentials {
        access_key_id,
        secret_access_key,
        session_token: None,
    };
    let backend = Arc::new(S3Backend::new(
        config,
        creds,
        Arc::new(ReqwestClient::new()),
    ));

    let present = ObjectId::new(Backend::S3, bucket.clone(), key);
    let missing = ObjectId::new(Backend::S3, bucket, "e2e/does-not-exist.bin");
    conformance::run(backend, &present, &missing, true).await;
}
