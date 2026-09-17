//! Splitting a byte read into per-block fetch segments.
//!
//! A single POSIX `read(offset, len)` may span multiple 256 MiB blocks, and the
//! read path fetches one block at a time. [`iter_read`] turns a request into an
//! ordered stream of [`BlockSegment`]s — each naming the [`BlockId`] to fetch, the
//! offset *within* that block, and how many bytes to take from it — so a caller
//! can fetch each segment and concatenate the results in order to reconstruct
//! the requested range.
//!
//! The plan is clamped to the file's `size`: a read starting at or past EOF
//! yields an empty plan, and a read overrunning EOF is truncated at the last
//! valid byte (mirroring POSIX short reads). This is pure block-offset
//! arithmetic; no I/O happens here.

use talon_core::{BlockId, ObjectId, Version};

/// One block-local slice of a larger read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockSegment {
    /// The block to fetch this slice from.
    pub block: BlockId,
    /// Offset of the slice within the block (`0..block_size`).
    pub offset_in_block: u32,
    /// Number of bytes to take from the block starting at `offset_in_block`.
    pub len: u32,
}

/// Lazy ordered block segments for one clamped byte-range read.
///
/// Keeping the plan lazy lets callers bound in-flight work without first
/// allocating one descriptor per block in an arbitrarily large range.
pub struct ReadPlan<'a> {
    object: &'a ObjectId,
    version: &'a Version,
    block_size: u32,
    position: u64,
    end: u64,
}

impl Iterator for ReadPlan<'_> {
    type Item = BlockSegment;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.end {
            return None;
        }
        let block_size = u64::from(self.block_size);
        let block_start = (self.position / block_size) * block_size;
        let offset_in_block = (self.position - block_start) as u32;
        let segment_end = block_start.saturating_add(block_size).min(self.end);
        let len = (segment_end - self.position) as u32;
        self.position = segment_end;
        Some(BlockSegment {
            block: BlockId::new(
                self.object.clone(),
                block_start,
                self.block_size,
                self.version.clone(),
            ),
            offset_in_block,
            len,
        })
    }
}

/// Lazily split `[offset, offset+len)` into ordered block-local segments.
pub fn iter_read<'a>(
    object: &'a ObjectId,
    offset: u64,
    len: u64,
    block_size: u32,
    version: &'a Version,
    size: u64,
) -> ReadPlan<'a> {
    let end = if block_size == 0 || len == 0 || offset >= size {
        offset
    } else {
        offset.saturating_add(len).min(size)
    };
    ReadPlan {
        object,
        version,
        block_size,
        position: offset,
        end,
    }
}

/// Split `[offset, offset+len)` of `obj` into ordered per-block segments.
///
/// - `block_size` is the file's logical block size and `version` its source
///   version/etag (both from the coordinator/HEAD).
/// - `size` is the object's total byte length, used to clamp at EOF.
///
/// Returns segments in ascending offset order. The concatenation of every
/// segment's `len` equals `min(len, size.saturating_sub(offset))`.
pub fn plan_read(
    obj: &ObjectId,
    offset: u64,
    len: u64,
    block_size: u32,
    version: &Version,
    size: u64,
) -> Vec<BlockSegment> {
    iter_read(obj, offset, len, block_size, version, size).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::Backend;

    fn obj() -> ObjectId {
        ObjectId::new(Backend::S3, "b", "o")
    }

    fn v() -> Version {
        Version::new("etag-1")
    }

    /// Total bytes covered by a plan.
    fn total(plan: &[BlockSegment]) -> u64 {
        plan.iter().map(|s| s.len as u64).sum()
    }

    #[test]
    fn single_block_read_is_one_segment() {
        let bs = 1024u32;
        let plan = plan_read(&obj(), 100, 200, bs, &v(), 10_000);
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].block.offset, 0);
        assert_eq!(plan[0].offset_in_block, 100);
        assert_eq!(plan[0].len, 200);
    }

    #[test]
    fn read_spanning_two_blocks_splits_at_boundary() {
        let bs = 1024u32;
        // Start 100 before the boundary, run 300 bytes → 100 in block0, 200 in block1.
        let plan = plan_read(&obj(), 924, 300, bs, &v(), 10_000);
        assert_eq!(plan.len(), 2);
        assert_eq!(plan[0].block.offset, 0);
        assert_eq!(plan[0].offset_in_block, 924);
        assert_eq!(plan[0].len, 100);
        assert_eq!(plan[1].block.offset, 1024);
        assert_eq!(plan[1].offset_in_block, 0);
        assert_eq!(plan[1].len, 200);
        assert_eq!(total(&plan), 300);
    }

    #[test]
    fn read_spanning_many_blocks_has_full_middle_segments() {
        let bs = 1024u32;
        // 900..3200 → block0 tail(124), block1 full(1024), block2 full(1024),
        // block3 head(128).
        let plan = plan_read(&obj(), 900, 2300, bs, &v(), 100_000);
        assert_eq!(plan.len(), 4);
        assert_eq!(plan[0].len, 124); // 1024-900
        assert_eq!(plan[1].offset_in_block, 0);
        assert_eq!(plan[1].len, 1024); // full middle block
        assert_eq!(plan[2].block.offset, 2048);
        assert_eq!(plan[2].offset_in_block, 0);
        assert_eq!(plan[2].len, 1024); // full middle block
        assert_eq!(plan[3].block.offset, 3072);
        assert_eq!(plan[3].offset_in_block, 0);
        assert_eq!(plan[3].len, 128); // 3200-3072
        assert_eq!(total(&plan), 2300);
    }

    #[test]
    fn read_clamped_at_eof() {
        let bs = 1024u32;
        // File is 1500 bytes; read 1400..1400+400 overruns → clamped to 100 bytes.
        let plan = plan_read(&obj(), 1400, 400, bs, &v(), 1500);
        assert_eq!(total(&plan), 100);
        assert_eq!(plan.last().unwrap().block.offset, 1024);
    }

    #[test]
    fn read_at_or_past_eof_is_empty() {
        let bs = 1024u32;
        assert!(plan_read(&obj(), 1500, 10, bs, &v(), 1500).is_empty());
        assert!(plan_read(&obj(), 5000, 10, bs, &v(), 1500).is_empty());
    }

    #[test]
    fn zero_len_or_zero_block_size_is_empty() {
        assert!(plan_read(&obj(), 0, 0, 1024, &v(), 1000).is_empty());
        assert!(plan_read(&obj(), 0, 10, 0, &v(), 1000).is_empty());
    }

    #[test]
    fn exact_block_aligned_read() {
        let bs = 1024u32;
        // Exactly two full blocks starting at a boundary.
        let plan = plan_read(&obj(), 1024, 2048, bs, &v(), 10_000);
        assert_eq!(plan.len(), 2);
        assert!(plan.iter().all(|s| s.offset_in_block == 0 && s.len == 1024));
        assert_eq!(plan[0].block.offset, 1024);
        assert_eq!(plan[1].block.offset, 2048);
    }

    #[test]
    fn lazy_plan_does_not_materialize_the_whole_range() {
        let first_two: Vec<_> = iter_read(&obj(), 0, u64::MAX, 1, &v(), u64::MAX)
            .take(2)
            .collect();
        assert_eq!(first_two.len(), 2);
        assert_eq!(first_two[0].block.offset, 0);
        assert_eq!(first_two[1].block.offset, 1);
    }

    #[test]
    fn plan_near_u64_max_does_not_overflow_the_last_block_boundary() {
        let plan = plan_read(&obj(), u64::MAX - 2, 2, 4, &v(), u64::MAX);
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].block.offset, u64::MAX - 3);
        assert_eq!(plan[0].offset_in_block, 1);
        assert_eq!(plan[0].len, 2);
    }
}
