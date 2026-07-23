use std::time::Duration;
use talon_worker::WorkerMetrics;

fn main() {
    divan::main();
}

#[divan::bench]
fn atomic_request_metrics(bencher: divan::Bencher) {
    let metrics = WorkerMetrics::new(64 << 30);
    bencher.bench(|| {
        divan::black_box(&metrics).record_request_success(4096, Duration::from_micros(50))
    });
}

#[divan::bench]
fn atomic_cache_hit_metric(bencher: divan::Bencher) {
    let metrics = WorkerMetrics::new(64 << 30);
    bencher.bench(|| divan::black_box(&metrics).record_cache_hit());
}
