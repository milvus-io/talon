//! Microbenchmarks for the coordinator placement hot path.
//!
//! `RendezvousPlacement::locate` runs on every client placement lookup, scaling
//! with cluster size, so it is benchmarked across representative node counts.

use talon_coordinator::{Placement, RendezvousPlacement};
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
