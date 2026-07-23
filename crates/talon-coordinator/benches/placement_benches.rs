//! Microbenchmarks for the coordinator placement hot path.
//!
//! - `RendezvousPlacement::locate` / `locate_top_k` run on every client
//!   placement lookup, scaling with cluster size.
//! - `Epoch::for_nodes` computes the deterministic placement version (a content
//!   hash of the healthy node set) on every membership reconcile, so it is
//!   benchmarked across representative cluster sizes too (#80).

use talon_coordinator::{Epoch, Placement, RendezvousPlacement};
use talon_core::{Backend, BlockId, NodeId, NodeInfo, NodeRole, ObjectId, Version};

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

#[divan::bench(args = [8, 64, 256])]
fn locate(bencher: divan::Bencher, node_count: u32) {
    let placement = RendezvousPlacement;
    let nodes = nodes(node_count);
    let id = block(42);
    bencher.bench(|| placement.locate(divan::black_box(&id), divan::black_box(&nodes)));
}

/// Top-3 replica ordering (the shape a RF>1 lookup would use).
#[divan::bench(args = [8, 64, 256])]
fn locate_top_k(bencher: divan::Bencher, node_count: u32) {
    let placement = RendezvousPlacement;
    let nodes = nodes(node_count);
    let id = block(42);
    bencher.bench(|| placement.locate_top_k(divan::black_box(&id), divan::black_box(&nodes), 3));
}

/// Deterministic placement-version hash over the node set (per reconcile).
#[divan::bench(args = [8, 64, 256])]
fn placement_version(bencher: divan::Bencher, node_count: u32) {
    let nodes = nodes(node_count);
    bencher.bench(|| Epoch::for_nodes(divan::black_box(&nodes)));
}
