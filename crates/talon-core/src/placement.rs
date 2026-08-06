//! Stable client-side placement for immutable cache blocks.
//!
//! Every client receives the same worker membership and ranks it locally. The
//! byte encoding and hash are deliberately specified here rather than using
//! Rust's `Hash` trait, whose format is not a cross-language protocol.

use sha2::{Digest, Sha256};

use crate::{Backend, BlockId, NodeId, NodeInfo, NodeRole};

const PLACEMENT_DOMAIN: &[u8] = b"talon-cache-placement-v1\0";
const MEMBERSHIP_DOMAIN: &[u8] = b"talon-cache-membership-v1\0";

/// Compute the stable HRW score for one cache block and worker ID.
///
/// Scores are compared as unsigned 32-byte big-endian values. A larger score
/// ranks first; equal scores are resolved by ascending worker ID.
pub fn cache_placement_score(block: &BlockId, worker: &NodeId) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(PLACEMENT_DOMAIN);
    hash.update([backend_tag(block.object.backend)]);
    hash_field(&mut hash, block.object.bucket.as_bytes());
    hash_field(&mut hash, block.object.object_path.as_bytes());
    hash.update(block.offset.to_le_bytes());
    hash.update(block.block_size.to_le_bytes());
    hash_field(&mut hash, block.version.0.as_bytes());
    hash_field(&mut hash, worker.0.as_bytes());
    hash.finalize().into()
}

/// Rank up to `k` healthy-membership workers for an immutable cache block.
///
/// The caller is responsible for membership health/freshness. Non-worker
/// records are ignored defensively so a management node can never become a
/// data-plane target through a malformed snapshot.
pub fn rank_cache_workers(block: &BlockId, nodes: &[NodeInfo], k: usize) -> Vec<NodeId> {
    if k == 0 {
        return Vec::new();
    }
    let mut ranked: Vec<([u8; 32], &NodeId)> = nodes
        .iter()
        .filter(|node| node.role == NodeRole::Worker)
        .map(|node| (cache_placement_score(block, &node.id), &node.id))
        .collect();
    ranked.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1 .0.cmp(&b.1 .0)));
    ranked
        .into_iter()
        .take(k)
        .map(|(_, id)| id.clone())
        .collect()
}

/// Content-derived token for one client membership snapshot.
///
/// This token has equality semantics only. It lets a client invalidate all
/// block placements when node identity or address changes without consulting a
/// coordinator for each block.
pub fn cache_membership_epoch(nodes: &[NodeInfo]) -> u64 {
    if nodes.is_empty() {
        return 0;
    }
    let mut workers: Vec<&NodeInfo> = nodes
        .iter()
        .filter(|node| node.role == NodeRole::Worker)
        .collect();
    if workers.is_empty() {
        return 0;
    }
    workers.sort_unstable_by(|a, b| a.id.0.cmp(&b.id.0));
    let mut hash = Sha256::new();
    hash.update(MEMBERSHIP_DOMAIN);
    for worker in workers {
        hash_field(&mut hash, worker.id.0.as_bytes());
        hash_field(&mut hash, worker.address.as_bytes());
    }
    let digest: [u8; 32] = hash.finalize().into();
    let value = u64::from_be_bytes(digest[..8].try_into().expect("fixed digest length"));
    value.max(1)
}

fn hash_field(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_le_bytes());
    hash.update(value);
}

fn backend_tag(backend: Backend) -> u8 {
    match backend {
        Backend::S3 => 0,
        Backend::Gcs => 1,
        Backend::Azure => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ObjectId, Version};

    fn block() -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "datasets", "training/part-0001"),
            268_435_456,
            256 << 20,
            Version::new("etag-v7"),
        )
    }

    fn worker(id: &str, address: &str) -> NodeInfo {
        NodeInfo {
            id: NodeId::new(id),
            address: address.into(),
            role: NodeRole::Worker,
        }
    }

    #[test]
    fn ranking_is_deterministic_and_input_order_independent() {
        let nodes = vec![
            worker("worker-a", "10.0.0.1:7001"),
            worker("worker-b", "10.0.0.2:7001"),
            worker("worker-c", "10.0.0.3:7001"),
        ];
        let mut reversed = nodes.clone();
        reversed.reverse();
        assert_eq!(
            rank_cache_workers(&block(), &nodes, 3),
            rank_cache_workers(&block(), &reversed, 3)
        );
        assert_eq!(
            rank_cache_workers(&block(), &nodes, 3),
            vec![
                NodeId::new("worker-b"),
                NodeId::new("worker-a"),
                NodeId::new("worker-c"),
            ]
        );
    }

    #[test]
    fn membership_epoch_tracks_addresses_but_scores_do_not() {
        let nodes = vec![worker("worker-a", "old:7001")];
        let moved = vec![worker("worker-a", "new:7001")];
        assert_ne!(
            cache_membership_epoch(&nodes),
            cache_membership_epoch(&moved)
        );
        assert_eq!(
            cache_placement_score(&block(), &nodes[0].id),
            cache_placement_score(&block(), &moved[0].id)
        );
    }

    #[test]
    fn non_workers_are_never_ranked() {
        let coordinator = NodeInfo {
            id: NodeId::new("coordinator-a"),
            address: "10.0.0.9:7000".into(),
            role: NodeRole::Coordinator,
        };
        assert!(rank_cache_workers(&block(), &[coordinator], 1).is_empty());
    }
}
