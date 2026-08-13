//! Sequential-read detection and next-N-block readahead.
//!
//! Talon relies on the kernel page cache and does no client-side disk cache in
//! v1. What the client *does* add is **sequential detection + readahead**: per
//! open file handle it watches the read cursor, and once it sees a run of
//! consecutive reads it prefetches the next N block indices ahead of the
//! cursor. Random access never triggers prefetch, so no bandwidth is wasted.
//!
//! [`ReadaheadState::on_read_range`] is called with each successful read's byte
//! range and returns the block indices to prefetch — empty until a sequential
//! run is established, and bounded by the configured window so prefetch memory
//! can't grow unbounded. [`ReadaheadState::on_read`] remains available for
//! block-granular callers.

/// Tunable readahead parameters.
#[derive(Debug, Clone, Copy)]
pub struct ReadaheadConfig {
    /// Consecutive in-order reads required before prefetch kicks in.
    pub trigger_run: u32,
    /// Number of blocks to prefetch ahead of the cursor once sequential.
    pub window: u32,
}

impl Default for ReadaheadConfig {
    fn default() -> Self {
        Self {
            // Require several consecutive in-order reads before prefetching, so a
            // couple of incidentally-adjacent reads are not mistaken for a
            // sequential scan and do not trigger speculative fetches. Random or
            // strided access therefore never prefetches.
            trigger_run: 3,
            window: 4,
        }
    }
}

/// Per-file-handle sequential-read detector + readahead planner.
#[derive(Debug)]
pub struct ReadaheadState {
    config: ReadaheadConfig,
    /// Cursor expected next, in the units selected by `mode`.
    expected_next: Option<u64>,
    /// Whether the current run tracks block indices or byte ranges.
    mode: Option<TrackingMode>,
    /// Current run length of consecutive in-order reads.
    run: u32,
    /// Highest block index already scheduled for prefetch (exclusive frontier).
    prefetched_upto: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrackingMode {
    Block,
    ByteRange { block_size: u32 },
}

impl ReadaheadState {
    /// Create detector state for one open file handle.
    pub fn new(config: ReadaheadConfig) -> Self {
        Self {
            config,
            expected_next: None,
            mode: None,
            run: 0,
            prefetched_upto: 0,
        }
    }

    /// Current consecutive-run length (for tests / metrics).
    pub fn run(&self) -> u32 {
        self.run
    }

    /// Whether the handle is currently in a sequential run.
    pub fn is_sequential(&self) -> bool {
        self.run >= self.config.trigger_run
    }

    /// Record a read at `block_index` and return blocks to prefetch.
    ///
    /// This block-granular API is retained for compatibility. Byte-range callers
    /// should use [`Self::on_read_range`]. Switching between the two APIs resets
    /// the current sequential run.
    pub fn on_read(&mut self, block_index: u64) -> Vec<u64> {
        let Some(next_block) = block_index.checked_add(1) else {
            self.reset();
            return Vec::new();
        };
        self.observe(TrackingMode::Block, block_index, next_block, next_block)
    }

    /// Record a successful byte-range read and return blocks to prefetch.
    ///
    /// A read is sequential when `offset` is the first byte after the previous
    /// successful read. `read_len` must be the number of bytes actually returned,
    /// not merely requested. Prefetch starts after the final block touched by the
    /// range. Zero-length reads are no-ops. A zero or changed `block_size`, an
    /// overflowing range, or switching from [`Self::on_read`] safely resets the
    /// current run.
    pub fn on_read_range(&mut self, offset: u64, read_len: u64, block_size: u32) -> Vec<u64> {
        if read_len == 0 {
            return Vec::new();
        }
        if block_size == 0 {
            self.reset();
            return Vec::new();
        }
        let Some(next_byte) = offset.checked_add(read_len) else {
            self.reset();
            return Vec::new();
        };
        let last_block = (next_byte - 1) / u64::from(block_size);
        let next_block = last_block + 1;
        self.observe(
            TrackingMode::ByteRange { block_size },
            offset,
            next_byte,
            next_block,
        )
    }

    fn observe(
        &mut self,
        mode: TrackingMode,
        cursor: u64,
        next_cursor: u64,
        next_block: u64,
    ) -> Vec<u64> {
        if self.mode != Some(mode) {
            self.expected_next = None;
            self.run = 0;
            self.prefetched_upto = next_block;
            self.mode = Some(mode);
        }

        let sequential = self.expected_next == Some(cursor);
        if sequential {
            self.run = self.run.saturating_add(1);
        } else {
            // Reset the pattern; a re-read or jump is not a sequential step.
            self.run = 1;
            self.prefetched_upto = next_block;
        }
        self.expected_next = Some(next_cursor);

        if !self.is_sequential() || self.config.window == 0 {
            return Vec::new();
        }

        // Prefetch the window ahead of the cursor, past what we've already
        // scheduled, so overlapping reads don't re-issue the same prefetch.
        let start = self.prefetched_upto.max(next_block);
        let end = next_block.saturating_add(self.config.window as u64);
        if start >= end {
            return Vec::new();
        }
        self.prefetched_upto = end;
        (start..end).collect()
    }

    fn reset(&mut self) {
        self.expected_next = None;
        self.mode = None;
        self.run = 0;
        self.prefetched_upto = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> ReadaheadState {
        ReadaheadState::new(ReadaheadConfig {
            trigger_run: 2,
            window: 4,
        })
    }

    #[test]
    fn sequential_scan_prefetches_window() {
        let mut s = state();
        // First read: no run yet -> no prefetch.
        assert!(s.on_read(0).is_empty());
        assert!(!s.is_sequential());

        // Second consecutive read: run hits trigger -> prefetch next 4 blocks.
        let pf = s.on_read(1);
        assert!(s.is_sequential());
        assert_eq!(pf, vec![2, 3, 4, 5]);

        // Third consecutive read: only the newly-exposed block is prefetched
        // (frontier dedup), not the whole window again.
        let pf = s.on_read(2);
        assert_eq!(pf, vec![6]);
    }

    #[test]
    fn random_access_never_prefetches() {
        let mut s = state();
        assert!(s.on_read(100).is_empty());
        assert!(s.on_read(3).is_empty()); // jump backwards
        assert!(s.on_read(57).is_empty()); // jump forward
        assert!(!s.is_sequential());
    }

    #[test]
    fn broken_run_resets() {
        let mut s = state();
        s.on_read(0);
        s.on_read(1); // sequential established
        assert!(s.is_sequential());
        // A jump breaks the run.
        assert!(s.on_read(50).is_empty());
        assert!(!s.is_sequential());
        // Rebuild the run from the new position.
        assert!(s.on_read(51).is_empty() || s.is_sequential());
    }

    #[test]
    fn window_zero_disables_prefetch() {
        let mut s = ReadaheadState::new(ReadaheadConfig {
            trigger_run: 1,
            window: 0,
        });
        s.on_read(0);
        assert!(s.on_read(1).is_empty());
    }

    #[test]
    fn prefetch_is_bounded_by_window() {
        let mut s = ReadaheadState::new(ReadaheadConfig {
            trigger_run: 1,
            window: 3,
        });
        // trigger_run 1 -> sequential immediately on the first read's follow-up.
        s.on_read(0);
        let pf = s.on_read(1);
        assert!(pf.len() <= 3, "prefetch must not exceed the window");
    }

    #[test]
    fn re_reading_same_block_does_not_advance() {
        let mut s = state();
        s.on_read(5);
        // Re-reading block 5 is not a sequential step; run resets.
        assert!(s.on_read(5).is_empty());
        assert!(!s.is_sequential());
    }

    #[test]
    fn sequential_scan_with_production_block_size_triggers_prefetch() {
        let mut s = ReadaheadState::new(ReadaheadConfig::default());
        const BLOCK_SIZE: u32 = 256 << 20;
        const READ_LEN: u64 = 128 << 10;

        assert!(s.on_read_range(0, READ_LEN, BLOCK_SIZE).is_empty());
        assert!(s.on_read_range(READ_LEN, READ_LEN, BLOCK_SIZE).is_empty());
        assert_eq!(
            s.on_read_range(2 * READ_LEN, READ_LEN, BLOCK_SIZE),
            vec![1, 2, 3, 4]
        );
    }

    #[test]
    fn repeated_or_gapped_ranges_reset_the_run() {
        const BLOCK_SIZE: u32 = 1024;
        let mut s = state();

        assert!(s.on_read_range(0, 128, BLOCK_SIZE).is_empty());
        assert_eq!(s.on_read_range(128, 128, BLOCK_SIZE), vec![1, 2, 3, 4]);
        assert!(s.on_read_range(128, 128, BLOCK_SIZE).is_empty());
        assert_eq!(s.run(), 1);

        assert!(s.on_read_range(512, 128, BLOCK_SIZE).is_empty());
        assert_eq!(s.run(), 1);
    }

    #[test]
    fn zero_length_range_is_a_noop() {
        const BLOCK_SIZE: u32 = 1024;
        let mut s = state();

        assert!(s.on_read_range(0, 128, BLOCK_SIZE).is_empty());
        assert!(s.on_read_range(128, 0, BLOCK_SIZE).is_empty());
        assert_eq!(s.run(), 1);
        assert_eq!(s.on_read_range(128, 128, BLOCK_SIZE), vec![1, 2, 3, 4]);
    }

    #[test]
    fn range_spanning_blocks_prefetches_after_final_block() {
        let mut s = ReadaheadState::new(ReadaheadConfig {
            trigger_run: 1,
            window: 4,
        });

        assert_eq!(s.on_read_range(512, 1536, 1024), vec![2, 3, 4, 5]);
    }

    #[test]
    fn invalid_or_overflowing_ranges_reset_detection() {
        const BLOCK_SIZE: u32 = 1024;
        let mut s = state();

        s.on_read_range(0, 128, BLOCK_SIZE);
        assert!(s.on_read_range(u64::MAX, 1, BLOCK_SIZE).is_empty());
        assert_eq!(s.run(), 0);

        s.on_read_range(0, 128, BLOCK_SIZE);
        assert!(s.on_read_range(128, 128, 0).is_empty());
        assert_eq!(s.run(), 0);
    }

    #[test]
    fn valid_range_ending_at_u64_max_is_safe() {
        let mut s = ReadaheadState::new(ReadaheadConfig {
            trigger_run: 1,
            window: 4,
        });

        assert!(s.on_read_range(u64::MAX - 1, 1, 1).is_empty());
        assert_eq!(s.run(), 1);
    }

    #[test]
    fn changing_block_size_starts_a_new_run() {
        let mut s = state();

        s.on_read_range(0, 128, 1024);
        assert!(s.on_read_range(128, 128, 2048).is_empty());
        assert_eq!(s.run(), 1);
        assert_eq!(s.on_read_range(256, 128, 2048), vec![1, 2, 3, 4]);
    }

    #[test]
    fn switching_tracking_apis_starts_a_new_run() {
        let mut s = state();

        s.on_read(0);
        assert!(s.on_read_range(1, 1, 1).is_empty());
        assert_eq!(s.run(), 1);
        assert_eq!(s.on_read_range(2, 1, 1), vec![3, 4, 5, 6]);
    }
}
