//! Block materialization forms.
//!
//! A block has a fixed logical size (256MB in v1) but two physical forms,
//! chosen at LOAD time via a [`LoadHint`]:
//!
//! - [`BlockForm::Whole`]: the entire block is cached as one unit (best for
//!   sequential scans + readahead).
//! - [`BlockForm::Paged`]: only hot pages are materialized on demand (best for
//!   point queries), tracked by a [`PresentBitmap`].
//!
//! See `DESIGN.md` §3 for the full rationale.

use crate::{BlockId, PageIndex};
use serde::{Deserialize, Serialize};

/// The hint supplied at LOAD time that selects a block's physical form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LoadHint {
    /// Materialize the whole block as a single unit.
    Whole,
    /// Materialize pages on demand with the given page size (bytes).
    Paged {
        /// Page size in bytes (256KB–4MB in v1).
        page_size: u32,
    },
}

/// A compact presence bitset over the pages of a paged block.
///
/// Backed by `u64` words; 1024 pages (256KB pages over a 256MB block) fit in 16
/// words = 128 bytes. Hand-rolled to avoid an external dependency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PresentBitmap {
    words: Vec<u64>,
    len: u32,
}

impl PresentBitmap {
    /// Create an empty bitmap sized for `page_count` pages.
    pub fn new(page_count: u32) -> Self {
        let words = (page_count as usize).div_ceil(64);
        Self {
            words: vec![0; words],
            len: page_count,
        }
    }

    /// Total number of pages the bitmap can address.
    pub fn len(&self) -> u32 {
        self.len
    }

    /// Whether the bitmap addresses zero pages.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Mark a page present.
    pub fn set(&mut self, page: PageIndex) {
        if page.0 < self.len {
            self.words[(page.0 / 64) as usize] |= 1u64 << (page.0 % 64);
        }
    }

    /// Mark a page absent.
    pub fn clear(&mut self, page: PageIndex) {
        if page.0 < self.len {
            self.words[(page.0 / 64) as usize] &= !(1u64 << (page.0 % 64));
        }
    }

    /// Whether a page is present.
    pub fn is_present(&self, page: PageIndex) -> bool {
        page.0 < self.len && (self.words[(page.0 / 64) as usize] >> (page.0 % 64)) & 1 == 1
    }

    /// Whether every page in the half-open range `[start, end)` is present.
    pub fn range_present(&self, start: PageIndex, end: PageIndex) -> bool {
        (start.0..end.0).all(|p| self.is_present(PageIndex(p)))
    }

    /// Count of present pages.
    pub fn count(&self) -> u32 {
        self.words.iter().map(|w| w.count_ones()).sum()
    }
}

/// The physical materialization form of a cached block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockForm {
    /// The whole block is cached, backed by a single file.
    Whole,
    /// Only the pages marked in `present` are cached, backed by per-page files.
    Paged {
        /// Page size in bytes.
        page_size: u32,
        /// Which pages are currently materialized.
        present: PresentBitmap,
    },
}

/// A worker block-index entry (shape only; the index itself lives in the
/// worker crate).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockMeta {
    /// Identity of the block.
    pub id: BlockId,
    /// Physical form and materialization state.
    pub form: BlockForm,
    /// Total logical length of the block in bytes (may be < block_size for the
    /// last block of an object).
    pub len: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bitmap_set_get_count() {
        let mut bm = PresentBitmap::new(1024);
        assert_eq!(bm.len(), 1024);
        assert_eq!(bm.count(), 0);
        assert!(!bm.is_present(PageIndex(0)));

        bm.set(PageIndex(0));
        bm.set(PageIndex(63));
        bm.set(PageIndex(64));
        bm.set(PageIndex(1023));
        assert_eq!(bm.count(), 4);
        assert!(bm.is_present(PageIndex(64)));
        assert!(!bm.range_present(PageIndex(0), PageIndex(3)));

        bm.set(PageIndex(1));
        bm.set(PageIndex(2));
        assert!(bm.range_present(PageIndex(0), PageIndex(3)));

        bm.clear(PageIndex(1));
        assert!(!bm.is_present(PageIndex(1)));
        assert_eq!(bm.count(), 5);
    }

    #[test]
    fn out_of_range_is_ignored() {
        let mut bm = PresentBitmap::new(10);
        bm.set(PageIndex(100));
        assert!(!bm.is_present(PageIndex(100)));
        assert_eq!(bm.count(), 0);
    }
}
