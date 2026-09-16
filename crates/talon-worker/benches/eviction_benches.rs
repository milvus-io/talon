//! Run `cargo bench -p talon-worker --bench eviction_benches` to measure
//! capacity-pressure cost independently of origin I/O and filesystem unlink.
use talon_core::{Backend, BlockId, ObjectId, Version};
use talon_worker::eviction::{CacheUnit, Lru};

fn unit(i: u64) -> CacheUnit {
    CacheUnit::Whole(BlockId::new(
        ObjectId::new(Backend::S3, "benchmark", format!("object/{i}")),
        0,
        1 << 20,
        Version::new("v1"),
    ))
}

#[divan::bench(args = [1_000, 10_000, 100_000], sample_count = 10)]
fn reclaim_half(bencher: divan::Bencher, count: u64) {
    bencher
        .with_inputs(|| {
            let lru = Lru::new();
            for i in 0..count {
                let access = lru.insert(unit(i), 4096);
                if i % 3 == 0 {
                    access.touch();
                }
                if i % 11 == 0 {
                    lru.pin(&unit(i));
                }
            }
            lru
        })
        .bench_values(|lru| {
            let victims = lru.evict_to_fit(count / 2 * 4096);
            assert_eq!(victims.len() as u64, count / 2);
            divan::black_box(victims);
        });
}

#[divan::bench(threads = [1, 8, 16])]
fn stable_entry_hit(bencher: divan::Bencher) {
    static HANDLE: std::sync::OnceLock<talon_worker::eviction::AccessHandle> =
        std::sync::OnceLock::new();
    let access = HANDLE.get_or_init(|| Lru::new().insert(unit(0), 4096));
    bencher.bench(|| divan::black_box(access).touch());
}

fn main() {
    divan::main();
}
