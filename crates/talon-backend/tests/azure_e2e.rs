//! Azure backend end-to-end test against a real emulator (Azurite).
//!
//! Builds the real [`AzureBackend`] with Shared Key authorization (#266) against
//! a live Azurite endpoint and runs the shared backend-conformance suite against
//! it.
//!
//! Skipped unless `TALON_AZURE_TEST_ENDPOINT` is set. See CI's `azure-e2e` job
//! for the seeded object (4096 bytes, byte i == i % 251). Uses Azurite's
//! well-known dev account + key (public emulator values, not secrets).

mod conformance;

use std::sync::Arc;

use talon_backend::{endpoint_host, AzureBackend, AzureConfig, ReqwestClient};
use talon_core::{Backend, ObjectId};

const DEV_ACCOUNT: &str = "devstoreaccount1";
const DEV_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

#[tokio::test]
async fn azure_backend_conformance_end_to_end() {
    let Some(endpoint) = env("TALON_AZURE_TEST_ENDPOINT") else {
        eprintln!("skipping Azure e2e: TALON_AZURE_TEST_ENDPOINT is not set");
        return;
    };
    let container =
        env("TALON_AZURE_TEST_CONTAINER").expect("TALON_AZURE_TEST_CONTAINER must be set");
    let key = env("TALON_AZURE_TEST_KEY").unwrap_or_else(|| "e2e/object.bin".to_string());

    let (host, tls) = endpoint_host(&endpoint);
    let config = AzureConfig::emulator(DEV_ACCOUNT, host, tls);
    let backend = Arc::new(AzureBackend::with_shared_key(
        config,
        DEV_KEY,
        Arc::new(ReqwestClient::new()),
    ));

    // Azure's ObjectId::bucket carries the container name.
    let present = ObjectId::new(Backend::Azure, container.clone(), key);
    let missing = ObjectId::new(Backend::Azure, container, "e2e/does-not-exist.bin");
    conformance::run(backend, &present, &missing, true).await;
}
