//! etcd metadata backend against a real server.
//!
//! Runs the shared contract suite, so the etcd backend is held to the identical
//! cases as the memory backend. ADR 0003 §7 lists the properties TMS
//! correctness rests on; testing them per-implementation would let this backend
//! weaken one silently, and the symptom would be a hard-link divergence in
//! production rather than a red test.
//!
//! Requires `TALON_ETCD_TEST_ENDPOINT`. Skips when unset so a local
//! `cargo test` without etcd stays green.

#![cfg(feature = "etcd")]

use talon_metadata::{
    contract, Capability, EtcdMetadataConfig, EtcdMetadataStore, InodeNumber, MappingRevision,
    MetadataStore, NamespaceId, Operation, PathIndexEntry, Precondition, Transaction,
};

fn endpoint() -> Option<String> {
    std::env::var("TALON_ETCD_TEST_ENDPOINT")
        .ok()
        .filter(|value| !value.is_empty())
}

/// A prefix unique to one test, so cases cannot interfere through shared etcd.
fn unique_prefix(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    format!("/talon-metadata-test/{name}-{nanos}")
}

async fn store_for(name: &str) -> Option<EtcdMetadataStore> {
    let endpoint = endpoint()?;
    let mut config = EtcdMetadataConfig::new(endpoint);
    config.prefix = unique_prefix(name);
    Some(
        EtcdMetadataStore::connect(&config)
            .await
            .expect("etcd is reachable at TALON_ETCD_TEST_ENDPOINT"),
    )
}

#[tokio::test]
async fn etcd_backend_satisfies_the_shared_contract() {
    let Some(store) = store_for("contract").await else {
        eprintln!("skipping: TALON_ETCD_TEST_ENDPOINT is not set");
        return;
    };
    contract::run_all(&store).await;
}

#[tokio::test]
async fn etcd_advertises_hard_links_and_withholds_the_rest() {
    let Some(store) = store_for("capabilities").await else {
        eprintln!("skipping: TALON_ETCD_TEST_ENDPOINT is not set");
        return;
    };

    // etcd can satisfy the transactional part of every contract, but §9.11
    // keeps write-back unreachable until an ADR superseding ADR 0002 is
    // accepted, and locks await their own ADR. Advertising a capability whose
    // supporting mechanism does not exist is the over-promise §7 forbids.
    assert!(store.supports(Capability::HardLinks));
    assert!(!store.supports(Capability::Locks));
    assert!(!store.supports(Capability::WriteBack));
}

#[tokio::test]
async fn metadata_and_cluster_state_prefixes_do_not_collide() {
    let Some(endpoint) = endpoint() else {
        eprintln!("skipping: TALON_ETCD_TEST_ENDPOINT is not set");
        return;
    };

    // §7 permits sharing a physical etcd cluster but requires the two stores to
    // stay separate abstractions. ClusterStateStore uses /talon; TMS must not
    // land inside it, or ADR 0001 §2's "bounded, rebuildable" invariant would
    // become untrue by construction -- TMS records are neither.
    let config = EtcdMetadataConfig::new(endpoint);
    assert!(
        !config.prefix.starts_with("/talon/"),
        "TMS prefix {} must not nest inside the cluster-state prefix",
        config.prefix
    );
    assert_ne!(config.prefix, "/talon");
}

#[tokio::test]
async fn record_population_does_not_scale_with_object_count() {
    let Some(store) = store_for("sparsity").await else {
        eprintln!("skipping: TALON_ETCD_TEST_ENDPOINT is not set");
        return;
    };

    // §3's sparsity claim is what makes an etcd-class backend viable at all:
    // etcd holds its keyspace in memory, so per-object records for a
    // billion-object bucket would not fit. This asserts that reading many
    // ordinary single-path files creates nothing.
    let namespace = NamespaceId::new("sparsity-ns").expect("valid namespace");
    let before = store.record_count().await.expect("count records");

    for index in 0..500 {
        let resolved = store
            .resolve_path(&namespace, &format!("ordinary/file-{index}.bin"))
            .await
            .expect("resolving an unmapped path is not an error");
        assert_eq!(resolved, None, "a single-path file must have no record");
    }

    let after = store.record_count().await.expect("count records");
    assert_eq!(
        before, after,
        "500 ordinary files must add zero TMS records (ADR 0003 §3)"
    );

    // One promoted pair does create records -- sparsity is proportional to
    // feature use, not to data volume.
    let inode = InodeNumber::new(1).expect("non-zero inode");
    store
        .commit(
            &Transaction::new()
                .when(Precondition::MappingRevisionIs {
                    namespace: namespace.clone(),
                    expected: MappingRevision::INITIAL,
                })
                .then(Operation::PutPathIndex(PathIndexEntry {
                    namespace: namespace.clone(),
                    path: "linked/a.bin".to_owned(),
                    inode,
                }))
                .then(Operation::AdvanceMappingRevision {
                    namespace: namespace.clone(),
                }),
        )
        .await
        .expect("promotion commits");

    let promoted = store.record_count().await.expect("count records");
    assert!(
        promoted > after,
        "a multiply-linked file does occupy records"
    );
}
