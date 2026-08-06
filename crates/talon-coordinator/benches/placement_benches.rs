//! Microbenchmarks for cache placement and membership reconciliation.
//!
//! - `CachePlacementTable::primary` is the stable-membership client hot path.
//!   Table construction is deliberately outside the timed loop.
//! - `Epoch::for_nodes` computes the deterministic placement version (a content
//!   hash of the healthy node set) on every membership reconcile, so it is
//!   benchmarked across representative cluster sizes too (#80).

use talon_coordinator::Epoch;
use talon_core::{
    Backend, BlockId, CachePlacementTable, NodeId, NodeInfo, NodeRole, ObjectId, Version,
};

fn main() {
    divan::main();
}

fn nodes(n: u32) -> Vec<NodeInfo> {
    (0..n)
        .map(|i| NodeInfo {
            id: NodeId::new(format!("worker-{i}")),
            address: format!("10.0.0.{i}:7001"),
            role: NodeRole::Worker,
        })
        .collect()
}

fn block(i: u64) -> BlockId {
    BlockId::new(
        ObjectId::new(Backend::S3, "bucket", format!("obj-{i}")),
        i * 256 * 1024 * 1024,
        256 * 1024 * 1024,
        Version::new("v1"),
    )
}

#[divan::bench(args = [8, 64, 256, 10_000])]
fn primary(bencher: divan::Bencher, node_count: u32) {
    let table = CachePlacementTable::new(&nodes(node_count));
    let id = block(42);
    bencher.bench(|| table.primary(divan::black_box(&id)));
}

/// Top-3 replica ordering (the shape a RF>1 lookup would use).
#[divan::bench(args = [8, 64, 256, 10_000])]
fn top_k(bencher: divan::Bencher, node_count: u32) {
    let table = CachePlacementTable::new(&nodes(node_count));
    let id = block(42);
    bencher.bench(|| table.rank(divan::black_box(&id), 3));
}

/// Deterministic placement-version hash over the node set (per reconcile).
#[divan::bench(args = [8, 64, 256])]
fn placement_version(bencher: divan::Bencher, node_count: u32) {
    let nodes = nodes(node_count);
    bencher.bench(|| Epoch::for_nodes(divan::black_box(&nodes)));
}
