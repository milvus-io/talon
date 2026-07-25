//! GCS backend end-to-end test against a real emulator (fake-gcs-server).
//!
//! Drives the real [`GcsBackend`] over [`ReqwestClient`] against a live endpoint,
//! exercising the bearer-token auth path and the endpoint override (#265) end to
//! end by reading bytes an object actually contains.
//!
//! The test is **skipped** unless `TALON_GCS_TEST_ENDPOINT` points at a reachable
//! GCS-compatible endpoint (e.g. `http://127.0.0.1:4443` for fake-gcs-server).
//! CI launches fake-gcs-server, seeds a known object, and sets the env vars.
//!
//! Required env:
//!   TALON_GCS_TEST_ENDPOINT    e.g. http://127.0.0.1:4443
//!   TALON_GCS_TEST_BUCKET      bucket that already contains the object
//!   TALON_GCS_TEST_KEY         object key (defaults to "e2e/object.bin")
//!   TALON_GCS_TEST_BEARER      optional bearer token (fake-gcs ignores auth)
//!
//! The uploaded object is expected to be 4096 bytes where byte i == (i % 251).

use std::sync::Arc;

use talon_backend::{GcsBackend, GcsConfig, ReqwestClient};
use talon_core::{Backend, BackendStore, ObjectId};

/// Deterministic content byte for absolute offset `i` — matches what CI uploads.
fn content_byte(i: u64) -> u8 {
    (i % 251) as u8
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

#[tokio::test]
async fn gcs_backend_reads_real_object_end_to_end() {
    let Some(endpoint) = env("TALON_GCS_TEST_ENDPOINT") else {
        eprintln!("skipping GCS e2e: TALON_GCS_TEST_ENDPOINT is not set");
        return;
    };
    let bucket = env("TALON_GCS_TEST_BUCKET").expect("TALON_GCS_TEST_BUCKET must be set");
    let key = env("TALON_GCS_TEST_KEY").unwrap_or_else(|| "e2e/object.bin".to_string());

    // Point at the emulator: http:// selects plaintext.
    let host = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(&endpoint)
        .to_string();
    let tls = !endpoint.starts_with("http://");
    let config = GcsConfig::emulator(host, tls);
    let backend = GcsBackend::new(config, env("TALON_GCS_TEST_BEARER"), Arc::new(ReqwestClient::new()));

    let obj = ObjectId::new(Backend::Gcs, bucket, key);

    // HEAD resolves size + generation/ETag (the version).
    let stat = backend.head(&obj).await.expect("HEAD should succeed");
    assert_eq!(stat.len, 4096, "unexpected object size");
    assert!(!stat.version.as_str().is_empty(), "HEAD returned no version");

    // A ranged GET in the middle of the object returns exactly those bytes.
    let got = backend
        .fetch_range(&obj, 1000, 256)
        .await
        .expect("ranged GET should succeed");
    assert_eq!(got.len(), 256, "unexpected range length");
    let expected: Vec<u8> = (1000..1256).map(content_byte).collect();
    assert_eq!(&got[..], &expected[..], "range bytes mismatch");

    // A read of the first bytes too, to cover offset 0.
    let head_bytes = backend.fetch_range(&obj, 0, 16).await.expect("GET head");
    let expected_head: Vec<u8> = (0..16).map(content_byte).collect();
    assert_eq!(&head_bytes[..], &expected_head[..]);
}
