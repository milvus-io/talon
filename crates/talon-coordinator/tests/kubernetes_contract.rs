//! Live Kubernetes contract test for [`KubernetesStateStore`].
//!
//! This runs the reusable state-store contract (#75) against a **real**
//! Kubernetes API server. There is no API server in unit CI, so it is
//! `#[ignore]`d and must be invoked explicitly against a throwaway cluster
//! (kind/minikube/k3d) with the RBAC from `deploy/kubernetes/rbac.yaml` applied:
//!
//! ```sh
//! cargo test -p talon-coordinator --features "kubernetes state-store-testkit" \
//!     --test kubernetes_contract -- --ignored --nocapture
//! ```
//!
//! The test creates Lease objects under a unique cluster id and deletes them on
//! completion, so repeated runs against the same namespace do not interfere.

#![cfg(all(feature = "kubernetes", feature = "state-store-testkit"))]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use talon_coordinator::state_store::testkit::{assert_store_contract, StoreContractHarness};
use talon_coordinator::state_store::{ClusterStateStore, KubernetesConfig, KubernetesStateStore};

struct KubernetesHarness {
    store: Arc<KubernetesStateStore>,
}

#[async_trait]
impl StoreContractHarness for KubernetesHarness {
    fn store(&self) -> Arc<dyn ClusterStateStore> {
        self.store.clone()
    }

    fn lease_ttl(&self) -> Duration {
        // Kubernetes lease granularity is one second; the contract's expiry step
        // waits ttl + epsilon, so a small whole-second TTL keeps the test quick.
        Duration::from_secs(2)
    }

    async fn elapse(&self, duration: Duration) {
        // No injected clock against a real API server: actually wait out the TTL
        // so the server-side lease expiry the contract asserts can occur.
        tokio::time::sleep(duration).await;
    }
}

#[tokio::test]
#[ignore = "requires a live Kubernetes API server; run explicitly with --ignored"]
async fn kubernetes_store_contract() {
    // A unique cluster id per run isolates concurrent/repeat executions.
    let cluster_id = format!("contract-{}", std::process::id());
    let config = KubernetesConfig {
        namespace: std::env::var("TALON_TEST_NAMESPACE").unwrap_or_else(|_| "talon".into()),
        cluster_id,
        context: std::env::var("TALON_TEST_KUBE_CONTEXT").ok(),
    };
    let store = KubernetesStateStore::connect(&config, Duration::from_secs(5))
        .await
        .expect("connect to Kubernetes");
    let harness = KubernetesHarness {
        store: Arc::new(store),
    };
    assert_store_contract(&harness).await;
}
