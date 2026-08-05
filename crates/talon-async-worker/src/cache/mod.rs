// SPDX-License-Identifier: Apache-2.0
//! The two-tier extent cache.
//!
//! [`memory`] is the L1 DRAM tier. The L2 NVMe tier and the facade that tiers
//! one over the other land in later changes; until then L1 is usable on its own
//! with a loader that goes straight to the origin.

pub mod memory;

/// Identifies one cached extent: an interned `(object, version)` stream plus a
/// byte offset within that object.
///
/// Deliberately *not* a block id. There is no block size and no page index —
/// the offset is wherever the reader asked to start, and the entry's length is
/// whatever was fetched. The version is folded into `stream_id` rather than
/// carried separately, so a republished object cannot resolve to the previous
/// version's extents. See ADR 0005 §2 and §3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExtentKey {
    /// Interned `(ObjectId, Version)` pair.
    pub stream_id: u64,
    /// Byte offset of the extent within the object.
    pub offset: u64,
}

impl ExtentKey {
    /// Create an extent key.
    pub fn new(stream_id: u64, offset: u64) -> Self {
        Self { stream_id, offset }
    }
}

impl std::fmt::Display for ExtentKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stream {}@{}", self.stream_id, self.offset)
    }
}
