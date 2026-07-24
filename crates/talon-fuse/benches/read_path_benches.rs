//! Read-path microbenchmarks for the FUSE client.
//!
//! These measure the CPU cost of the per-read planning that runs on every
//! `read(2)` before any I/O: splitting a POSIX byte range into per-block fetch
//! segments ([`plan_read`]) and the sequential-detection / readahead planner
//! ([`ReadaheadState::on_read`]). They are deterministic (no network, no disk)
//! so they can be committed as a baseline and diffed by `scripts/bench.py`.
//!
//! The end-to-end read *throughput* wins from sendfile (#179) and connection
//! pooling (#181) are measured separately by real-loopback comparison benches
//! (`talon-worker`'s `serve_path_benches` and this crate's `conn_pool_benches`),
//! which pair the old and new paths so the delta is the payoff. Those are
//! informational (timing-sensitive real TCP) and not committed to the baseline;
//! the deterministic planning benches here are.

use talon_core::{Backend, ObjectId, Version};
use talon_fuse::{plan_read, ReadaheadConfig, ReadaheadState};

fn main() {
    divan::main();
}

const BLOCK_SIZE: u32 = 256 << 20; // 256 MiB, the v1 default.

fn obj() -> ObjectId {
    ObjectId::new(Backend::S3, "bucket", "data/checkpoint.bin")
}

fn version() -> Version {
    Version::new("etag-abc123")
}

/// Plan a small read fully inside one block — the common case.
#[divan::bench]
fn plan_single_block(bencher: divan::Bencher) {
    let obj = obj();
    let version = version();
    let size = 4 * BLOCK_SIZE as u64;
    bencher.bench(|| {
        plan_read(
            divan::black_box(&obj),
            divan::black_box(4096),
            divan::black_box(64 * 1024),
            BLOCK_SIZE,
            &version,
            size,
        )
    });
}

/// Plan a read that spans four blocks (tail + two full + head) — the stitching
/// case that produces multiple segments.
#[divan::bench]
fn plan_multi_block(bencher: divan::Bencher) {
    let obj = obj();
    let version = version();
    let size = 8 * BLOCK_SIZE as u64;
    let bs = BLOCK_SIZE as u64;
    // Start near the end of block 0, run through blocks 1 and 2, into block 3.
    let offset = bs - 1024;
    let len = 3 * bs + 2048;
    bencher.bench(|| {
        plan_read(
            divan::black_box(&obj),
            divan::black_box(offset),
            divan::black_box(len),
            BLOCK_SIZE,
            &version,
            size,
        )
    });
}

/// Plan a read that runs past EOF, exercising the clamp path.
#[divan::bench]
fn plan_eof_clamp(bencher: divan::Bencher) {
    let obj = obj();
    let version = version();
    let size = BLOCK_SIZE as u64 + 500;
    bencher.bench(|| {
        plan_read(
            divan::black_box(&obj),
            divan::black_box(BLOCK_SIZE as u64),
            divan::black_box(4096),
            BLOCK_SIZE,
            &version,
            size,
        )
    });
}

/// Drive the readahead detector through a sequential run so it emits prefetch
/// windows — the per-read cost of prefetch planning on the hot path.
#[divan::bench]
fn readahead_sequential_run(bencher: divan::Bencher) {
    bencher.bench(|| {
        let mut state = ReadaheadState::new(ReadaheadConfig::default());
        let mut emitted = 0usize;
        for i in 0..64u64 {
            emitted += state.on_read(divan::black_box(i)).len();
        }
        emitted
    });
}
