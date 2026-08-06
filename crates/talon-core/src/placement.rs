//! Stable client-side placement for immutable cache blocks.
//!
//! Every client receives the same worker membership and ranks it locally. The
//! byte encoding and hash are deliberately specified here rather than using
//! Rust's `Hash` trait, whose format is not a cross-language protocol.

use sha2::{Digest, Sha256};

use crate::{Backend, BlockId, NodeId, NodeInfo, NodeRole};

const BLOCK_DOMAIN: &[u8] = b"talon-cache-maglev-block-v1\0";
const WORKER_DOMAIN: &[u8] = b"talon-cache-maglev-worker-v1\0";
const MEMBERSHIP_DOMAIN: &[u8] = b"talon-cache-membership-v1\0";
const MIN_TABLE_SIZE: usize = 4_096;
const SLOTS_PER_WORKER: usize = 64;
const EMPTY_SLOT: u32 = u32::MAX;

fn cache_block_hash(block: &BlockId) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(BLOCK_DOMAIN);
    hash.update([backend_tag(block.object.backend)]);
    hash_field(&mut hash, block.object.bucket.as_bytes());
    hash_field(&mut hash, block.object.object_path.as_bytes());
    hash.update(block.offset.to_le_bytes());
    hash.update(block.block_size.to_le_bytes());
    hash_field(&mut hash, block.version.0.as_bytes());
    hash.finalize().into()
}

/// A deterministic Maglev table built once per healthy-worker membership.
///
/// Construction is intentionally paid when membership changes. While the
/// membership is stable, primary block placement is one hash and one array
/// lookup: O(1) time with no worker-list scan.
#[derive(Debug, Clone)]
pub struct CachePlacementTable {
    workers: Vec<NodeInfo>,
    slots: Vec<u32>,
    mask: usize,
}

impl CachePlacementTable {
    /// Build the placement index for one membership snapshot.
    pub fn new(nodes: &[NodeInfo]) -> Self {
        let mut workers: Vec<NodeInfo> = nodes
            .iter()
            .filter(|node| node.role == NodeRole::Worker)
            .cloned()
            .collect();
        workers.sort_unstable_by(|a, b| a.id.0.cmp(&b.id.0));
        workers.dedup_by(|a, b| a.id == b.id);
        if workers.is_empty() {
            return Self {
                workers,
                slots: Vec::new(),
                mask: 0,
            };
        }
        assert!(
            workers.len() < EMPTY_SLOT as usize,
            "too many cache workers"
        );
        let wanted = workers
            .len()
            .saturating_mul(SLOTS_PER_WORKER)
            .max(MIN_TABLE_SIZE);
        let table_size = wanted
            .checked_next_power_of_two()
            .expect("cache placement table size overflow");
        let mask = table_size - 1;
        let mut slots = vec![EMPTY_SLOT; table_size];
        let mut next = vec![0usize; workers.len()];
        let permutations: Vec<(usize, usize)> = workers
            .iter()
            .map(|worker| worker_permutation(&worker.id, mask))
            .collect();
        let mut filled = 0usize;
        while filled < table_size {
            for (worker_index, &(offset, skip)) in permutations.iter().enumerate() {
                let slot = loop {
                    let candidate =
                        offset.wrapping_add(next[worker_index].wrapping_mul(skip)) & mask;
                    next[worker_index] += 1;
                    if slots[candidate] == EMPTY_SLOT {
                        break candidate;
                    }
                };
                slots[slot] = worker_index as u32;
                filled += 1;
                if filled == table_size {
                    break;
                }
            }
        }
        Self {
            workers,
            slots,
            mask,
        }
    }

    /// Return the primary worker in O(1) time.
    pub fn primary(&self, block: &BlockId) -> Option<&NodeInfo> {
        let digest = cache_block_hash(block);
        let slot = u64::from_le_bytes(digest[..8].try_into().unwrap()) as usize & self.mask;
        self.slots
            .get(slot)
            .map(|&worker| &self.workers[worker as usize])
    }

    /// Return up to `k` distinct workers, primary first.
    pub fn rank(&self, block: &BlockId, k: usize) -> Vec<&NodeInfo> {
        let target = k.min(self.workers.len());
        if target == 0 {
            return Vec::new();
        }
        let digest = cache_block_hash(block);
        let start = u64::from_le_bytes(digest[..8].try_into().unwrap()) as usize & self.mask;
        let step = (u64::from_le_bytes(digest[8..16].try_into().unwrap()) as usize | 1) & self.mask;
        let mut result = Vec::with_capacity(target);
        for probe in 0..self.slots.len() {
            let slot = start.wrapping_add(probe.wrapping_mul(step)) & self.mask;
            let worker = self.slots[slot];
            if !result
                .iter()
                .any(|node: &&NodeInfo| node.id == self.workers[worker as usize].id)
            {
                result.push(&self.workers[worker as usize]);
                if result.len() == target {
                    break;
                }
            }
        }
        result
    }

    /// Number of healthy workers indexed by this table.
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Canonically ID-sorted workers retained by the index.
    pub fn workers(&self) -> &[NodeInfo] {
        &self.workers
    }

    #[cfg(test)]
    fn slot_count(&self) -> usize {
        self.slots.len()
    }
}

fn worker_permutation(worker: &NodeId, mask: usize) -> (usize, usize) {
    let mut hash = Sha256::new();
    hash.update(WORKER_DOMAIN);
    hash_field(&mut hash, worker.0.as_bytes());
    let digest: [u8; 32] = hash.finalize().into();
    let offset = u64::from_le_bytes(digest[..8].try_into().unwrap()) as usize & mask;
    let skip = (u64::from_le_bytes(digest[8..16].try_into().unwrap()) as usize | 1) & mask;
    (offset, skip)
}

/// Cold-path convenience wrapper. Hot clients should cache a
/// [`CachePlacementTable`] with their membership snapshot.
pub fn rank_cache_workers(block: &BlockId, nodes: &[NodeInfo], k: usize) -> Vec<NodeId> {
    CachePlacementTable::new(nodes)
        .rank(block, k)
        .into_iter()
        .map(|node| node.id.clone())
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
                NodeId::new("worker-c"),
                NodeId::new("worker-a"),
                NodeId::new("worker-b"),
            ]
        );
    }

    #[test]
    fn membership_epoch_tracks_addresses_but_placement_does_not() {
        let nodes = vec![worker("worker-a", "old:7001")];
        let moved = vec![worker("worker-a", "new:7001")];
        assert_ne!(
            cache_membership_epoch(&nodes),
            cache_membership_epoch(&moved)
        );
        let before = CachePlacementTable::new(&nodes);
        let after = CachePlacementTable::new(&moved);
        assert_eq!(
            before.primary(&block()).unwrap().id,
            after.primary(&block()).unwrap().id
        );
        assert_ne!(
            before.primary(&block()).unwrap().address,
            after.primary(&block()).unwrap().address
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

    #[test]
    fn table_indexes_every_worker_and_bounds_replica_count() {
        let nodes: Vec<NodeInfo> = (0..128)
            .map(|i| worker(&format!("worker-{i:03}"), &format!("10.0.0.{i}:7001")))
            .collect();
        let table = CachePlacementTable::new(&nodes);
        assert_eq!(table.worker_count(), nodes.len());
        assert!(table.slot_count().is_power_of_two());
        assert!(table.slot_count() >= nodes.len() * SLOTS_PER_WORKER);
        for k in [0, 1, 2, 3, 16, 127, 128, 256] {
            let ranked = table.rank(&block(), k);
            assert_eq!(ranked.len(), k.min(nodes.len()));
            let mut ids: Vec<&NodeId> = ranked.iter().map(|node| &node.id).collect();
            ids.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            ids.dedup();
            assert_eq!(ids.len(), ranked.len());
        }
        assert_eq!(
            table.primary(&block()).unwrap().id,
            table.rank(&block(), 1)[0].id
        );
    }
}
