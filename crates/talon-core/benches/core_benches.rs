//! Microbenchmarks for `talon-core` hot paths.
//!
//! Run with `cargo bench -p talon-core` or via the repo `just bench` harness.
//! These target deterministic, CPU-bound code (no I/O), so they are low-variance
//! and suitable for regression detection.

use talon_core::block::PresentBitmap;
use talon_core::{Backend, BlockId, ObjectId, PageIndex, Version};

fn main() {
    divan::main();
}

fn sample_block() -> BlockId {
    BlockId::new(
        ObjectId::new(Backend::S3, "my-bucket", "data/checkpoint-000042.bin"),
        256 * 1024 * 1024 * 7,
        256 * 1024 * 1024,
        Version::new("etag-abc123"),
    )
}

// ---- key path <-> id ----

#[divan::bench]
fn object_id_to_path(bencher: divan::Bencher) {
    let obj = sample_block().object;
    bencher.bench(|| divan::black_box(&obj).to_path());
}

#[divan::bench]
fn object_id_from_path(bencher: divan::Bencher) {
    let path = sample_block().object.to_path();
    bencher.bench(|| ObjectId::from_path(divan::black_box(&path)).unwrap());
}

// ---- present bitmap ----

#[divan::bench(args = [64, 1024])]
fn bitmap_set_all(bencher: divan::Bencher, pages: u32) {
    bencher.bench(|| {
        let mut bm = PresentBitmap::new(pages);
        for p in 0..pages {
            bm.set(PageIndex(divan::black_box(p)));
        }
        bm.count()
    });
}

#[divan::bench(args = [64, 1024])]
fn bitmap_range_present(bencher: divan::Bencher, pages: u32) {
    let mut bm = PresentBitmap::new(pages);
    for p in 0..pages {
        bm.set(PageIndex(p));
    }
    bencher.bench(|| bm.range_present(PageIndex(0), PageIndex(divan::black_box(pages))));
}

// ---- block id ----

#[divan::bench]
fn block_page_count(bencher: divan::Bencher) {
    let id = sample_block();
    bencher.bench(|| divan::black_box(&id).page_count(divan::black_box(256 * 1024)));
}
