// SPDX-License-Identifier: Apache-2.0
//! The two-tier extent cache.
//!
//! [`memory`] is the L1 DRAM tier and [`region`]/[`store`] the L2 NVMe tier;
//! [`ids`] interns the objects both are keyed by, and [`checkpoint`] is what
//! lets the NVMe tier survive a restart. [`tiered`] stacks them and decides
//! what earns a place on disk.

pub mod checkpoint;
pub mod ids;
pub mod memory;
pub mod region;
pub mod store;
pub mod tiered;

/// Identifies one cached extent: an interned object plus a byte offset within
/// it.
///
/// Deliberately *not* a block id. There is no block size and no page index —
/// the offset is wherever the reader asked to start, and the entry's length is
/// whatever was fetched.
///
/// The object's version is deliberately *not* folded in, unlike the block
/// worker's `BlockId`: a republish reuses the stream id, and staleness is
/// bounded by the runtime's version-TTL purge rather than made impossible. See
/// ADR 0005 §2 and §3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExtentKey {
    /// Interned [`ObjectId`](talon_core::ObjectId).
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
