//! Sequential-read detection and next-N-block readahead.
//!
//! Talon relies on the kernel page cache and does no client-side disk cache in
//! v1. What the client *does* add is **sequential detection + readahead**: per
//! open file handle it watches the read cursor, and once it sees a run of
//! consecutive reads it prefetches the next N block indices ahead of the
//! cursor. Random access never triggers prefetch, so no bandwidth is wasted.
//!
//! [`ReadaheadState::on_read`] is called for each read (by block index) and
//! returns the block indices to prefetch — empty until a sequential run is
//! established, and bounded by the configured window so prefetch memory can't
//! grow unbounded.

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
            trigger_run: 2,
            window: 4,
        }
    }
}

/// Per-file-handle sequential-read detector + readahead planner.
#[derive(Debug)]
pub struct ReadaheadState {
    config: ReadaheadConfig,
    /// The block index expected next if the pattern is sequential.
    expected_next: Option<u64>,
    /// Current run length of consecutive in-order reads.
    run: u32,
    /// Highest block index already scheduled for prefetch (exclusive frontier).
    prefetched_upto: u64,
}

impl ReadaheadState {
    /// Create detector state for one open file handle.
    pub fn new(config: ReadaheadConfig) -> Self {
        Self {
            config,
            expected_next: None,
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
    /// A read is "sequential" if it lands on the block immediately after the
    /// previous one. Once the run reaches `trigger_run`, this returns the next
    /// up-to-`window` block indices ahead of the cursor that haven't already
    /// been scheduled (deduplicated via a frontier, so re-reads don't re-issue).
    /// Any non-consecutive read resets the run and returns nothing.
    pub fn on_read(&mut self, block_index: u64) -> Vec<u64> {
        let sequential = self.expected_next == Some(block_index);
        if sequential {
            self.run = self.run.saturating_add(1);
        } else {
            // Reset the pattern; a re-read of the same block or a jump is not a
            // sequential step.
            self.run = 1;
            self.prefetched_upto = block_index + 1;
        }
        self.expected_next = Some(block_index + 1);

        if !self.is_sequential() || self.config.window == 0 {
            return Vec::new();
        }

        // Prefetch the window ahead of the cursor, past what we've already
        // scheduled, so overlapping reads don't re-issue the same prefetch.
        let start = self.prefetched_upto.max(block_index + 1);
        let end = block_index + 1 + self.config.window as u64;
        if start >= end {
            return Vec::new();
        }
        self.prefetched_upto = end;
        (start..end).collect()
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
}
