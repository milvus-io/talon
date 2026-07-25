//! S3 backend end-to-end test against a real S3-compatible emulator (LocalStack).
//!
//! Unlike the offline unit tests (which inject a mock `HttpClient`), this drives
//! the real [`S3Backend`] over [`ReqwestClient`] against a live endpoint: it
//! exercises **AWS SigV4 request signing** (#264) and the path-style endpoint
//! wiring end to end, reading bytes an object actually contains.
//!
//! The test is **skipped** unless `TALON_S3_TEST_ENDPOINT` points at a reachable
//! S3 endpoint (e.g. `http://127.0.0.1:4566` for LocalStack). CI launches
//! LocalStack, creates a bucket, uploads a known object, and sets the env vars;
//! locally you can do the same with the AWS CLI against LocalStack.
//!
//! Required env:
//!   TALON_S3_TEST_ENDPOINT     e.g. http://127.0.0.1:4566
//!   TALON_S3_TEST_BUCKET       bucket that already contains the object
//!   TALON_S3_TEST_KEY          object key (defaults to "e2e/object.bin")
//!   AWS_ACCESS_KEY_ID          credentials the emulator accepts (test/test)
//!   AWS_SECRET_ACCESS_KEY
//!   TALON_S3_TEST_REGION       defaults to us-east-1
//!
//! The uploaded object is expected to be 4096 bytes where byte i == (i % 251).

use std::sync::Arc;

use talon_backend::{ReqwestClient, S3Backend, S3Config, S3Credentials};
use talon_core::{Backend, BackendStore, ObjectId};

/// Deterministic content byte for absolute offset `i` — matches what CI uploads.
fn content_byte(i: u64) -> u8 {
    (i % 251) as u8
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

#[tokio::test]
async fn s3_backend_reads_real_object_end_to_end() {
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

    // Point at the emulator: http:// selects plaintext, path-style for LocalStack.
    let host = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(&endpoint)
        .to_string();
    let tls = !endpoint.starts_with("http://");
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
    let backend = S3Backend::new(config, creds, Arc::new(ReqwestClient::new()));

    let obj = ObjectId::new(Backend::S3, bucket, key);

    // HEAD resolves size + ETag (the version). SigV4 must sign this correctly.
    let stat = backend.head(&obj).await.expect("HEAD should succeed");
    assert_eq!(stat.len, 4096, "unexpected object size");
    assert!(!stat.version.as_str().is_empty(), "HEAD returned no ETag");

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
