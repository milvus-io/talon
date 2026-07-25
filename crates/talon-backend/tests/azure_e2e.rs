//! Azure backend end-to-end test against a real emulator (Azurite).
//!
//! Drives the real [`AzureBackend`] over [`ReqwestClient`] against a live Azurite
//! endpoint using **Shared Key** authorization (#266), exercising the signing
//! path and the path-style emulator endpoint end to end by reading bytes an
//! object actually contains.
//!
//! The test is **skipped** unless `TALON_AZURE_TEST_ENDPOINT` points at a
//! reachable Azurite blob endpoint (e.g. `http://127.0.0.1:10000`). CI launches
//! Azurite, creates a container, uploads a known blob, and sets the env vars.
//!
//! Required env:
//!   TALON_AZURE_TEST_ENDPOINT   e.g. http://127.0.0.1:10000
//!   TALON_AZURE_TEST_CONTAINER  container that already contains the blob
//!   TALON_AZURE_TEST_KEY        blob name (defaults to "e2e/object.bin")
//!
//! Uses Azurite's well-known dev account (`devstoreaccount1`) and its fixed dev
//! account key — public emulator values, not secrets. The uploaded blob is
//! expected to be 4096 bytes where byte i == (i % 251).

use std::sync::Arc;

use talon_backend::{AzureBackend, AzureConfig, ReqwestClient};
use talon_core::{Backend, BackendStore, ObjectId};

/// Azurite's well-known dev account name and key (public emulator credentials).
const DEV_ACCOUNT: &str = "devstoreaccount1";
const DEV_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

/// Deterministic content byte for absolute offset `i` — matches what CI uploads.
fn content_byte(i: u64) -> u8 {
    (i % 251) as u8
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

#[tokio::test]
async fn azure_backend_reads_real_object_end_to_end() {
    let Some(endpoint) = env("TALON_AZURE_TEST_ENDPOINT") else {
        eprintln!("skipping Azure e2e: TALON_AZURE_TEST_ENDPOINT is not set");
        return;
    };
    let container =
        env("TALON_AZURE_TEST_CONTAINER").expect("TALON_AZURE_TEST_CONTAINER must be set");
    let key = env("TALON_AZURE_TEST_KEY").unwrap_or_else(|| "e2e/object.bin".to_string());

    // Point at the emulator (Azurite): http:// selects plaintext, path-style.
    let host = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(&endpoint)
        .to_string();
    let tls = !endpoint.starts_with("http://");
    let config = AzureConfig::emulator(DEV_ACCOUNT, host, tls);
    // Shared Key authorization with Azurite's dev account key.
    let backend = AzureBackend::with_shared_key(config, DEV_KEY, Arc::new(ReqwestClient::new()));

    // Azure's ObjectId::bucket carries the container name.
    let obj = ObjectId::new(Backend::Azure, container, key);

    // HEAD resolves size + ETag (the version). Shared Key must sign it correctly.
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
