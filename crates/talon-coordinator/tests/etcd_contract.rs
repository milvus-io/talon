//! Behavioral contract suite for the etcd [`ClusterStateStore`] backend, run
//! against a real ephemeral etcd instance.
//!
//! The test is skipped unless `TALON_ETCD_TEST_ENDPOINT` points at a reachable
//! etcd (for example `127.0.0.1:2379`). CI starts an ephemeral etcd and sets the
//! variable; local runs without etcd skip cleanly.

#![cfg(feature = "etcd")]

use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use etcd_client::Client;
use talon_coordinator::state_store::testkit::{assert_store_contract, StoreContractHarness};
use talon_coordinator::{ClusterStateStore, EtcdStateStore};

/// Lease TTL used for the contract run. Kept comfortably longer than the whole
/// suite's operation stream so records only disappear when we intend them to.
const LEASE_TTL: Duration = Duration::from_secs(4);

/// Extra slack added on top of the requested elapse so etcd has time to
/// actually revoke an expired lease before the harness inspects the snapshot.
const EXPIRY_SLACK: Duration = Duration::from_secs(3);

struct EtcdHarness {
    store: Arc<EtcdStateStore>,
}

#[async_trait]
impl StoreContractHarness for EtcdHarness {
    fn store(&self) -> Arc<dyn ClusterStateStore> {
        self.store.clone()
    }

    fn lease_ttl(&self) -> Duration {
        LEASE_TTL
    }

    async fn elapse(&self, duration: Duration) {
        // Real time only: etcd owns lease expiry, so wait long enough that a
        // lease whose TTL has passed is guaranteed to be revoked server-side.
        tokio::time::sleep(duration + EXPIRY_SLACK).await;
    }
}

fn unique_prefix() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("/talon-test/{}-{nanos}-{seq}", process::id())
}

#[tokio::test]
async fn etcd_backend_passes_store_contract() {
    let Ok(endpoint) = std::env::var("TALON_ETCD_TEST_ENDPOINT") else {
        eprintln!("skipping etcd contract test: TALON_ETCD_TEST_ENDPOINT is not set");
        return;
    };

    let client = Client::connect([endpoint], None)
        .await
        .expect("connect to ephemeral etcd");
    let prefix = unique_prefix();
    let store = EtcdStateStore::from_client(client, prefix, Duration::from_secs(5))
        .expect("construct etcd store");

    let harness = EtcdHarness {
        store: Arc::new(store),
    };
    assert_store_contract(&harness).await;
}
