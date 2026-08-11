//! GCS backend end-to-end test against a real emulator (fake-gcs-server).
//!
//! Builds the real [`GcsBackend`] over [`ReqwestClient`] against a live endpoint
//! (bearer auth + endpoint override #265) and runs the shared backend-
//! conformance suite against it.
//!
//! Skipped unless `TALON_GCS_TEST_ENDPOINT` is set. See CI's `gcs-e2e` job for
//! the seeded object (4096 bytes, byte i == i % 251).

mod conformance;

use std::sync::Arc;

use talon_backend::{endpoint_host, GcsBackend, GcsConfig, ReqwestClient};
use talon_core::{Backend, ObjectId};

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

#[tokio::test]
async fn gcs_backend_conformance_end_to_end() {
    let Some(endpoint) = env("TALON_GCS_TEST_ENDPOINT") else {
        eprintln!("skipping GCS e2e: TALON_GCS_TEST_ENDPOINT is not set");
        return;
    };
    let bucket = env("TALON_GCS_TEST_BUCKET").expect("TALON_GCS_TEST_BUCKET must be set");
    let key = env("TALON_GCS_TEST_KEY").unwrap_or_else(|| "e2e/object.bin".to_string());

    let (host, tls) = endpoint_host(&endpoint);
    let config = GcsConfig::emulator(host, tls);
    let backend = Arc::new(GcsBackend::new(
        config,
        env("TALON_GCS_TEST_BEARER"),
        Arc::new(ReqwestClient::new()),
    ));

    let present = ObjectId::new(Backend::Gcs, bucket.clone(), key);
    let missing = ObjectId::new(Backend::Gcs, bucket, "e2e/does-not-exist.bin");
    conformance::run(backend, &present, &missing, false).await;
}
