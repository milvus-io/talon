# Superseded-version eviction: local cold-fill benchmark

The benchmark compares main `fc62db549cc5e2ecd3b014d823cd8f8aec52e65f` with the same source plus the logical-block version index in `eviction.rs`. The benchmark harness is identical in both builds. All runtime call sites retain their original superseded-version eviction behavior.

The cold-fill timings below were collected for indexed revision `650a42e`. The subsequent sparse-index memory fix is validated separately below; these timings were not remeasured. That fix only shrinks page sets during removal, which this no-eviction cold-fill workload does not exercise.

## Workload and measurement boundary

- A fresh WorkerRuntime and empty cache directory for every run.
- 100,000 distinct cold reads, each reading 4 KiB from a different 64 KiB page of one immutable object. Each successful read fetches, writes, fsyncs, and renames one full page: 6.10 GiB of final page data across 256 MiB logical blocks.
- L1 disabled. L2 capacity is twice the workload size, so capacity eviction does not affect the comparison. There are no other object versions to remove.
- Concurrency 1 and 128, eight Tokio runtime threads, three measured runs of each build at each concurrency. Build order alternates by repetition. A separate 1,000-page run checks both builds before measurement.
- A deterministic in-memory origin supplies page contents without network delay. Requests call the real WorkerRuntime page-miss path and include page writes, fsync, rename, indexing, and eviction. No TCP, SDK, coordinator, HTTP, cloud origin, or production latency is measured.
- Every response is length- and content-checked. Backend fetch count and cached page count must equal completed requests. After each run, a separate disk scan must recover all 100,000 pages, and the temporary cache must be removed. Any failure invalidates the run.
- Every 10,000-page interval records elapsed time, throughput, P50, and P99 request latency. Cumulative durations sum these intervals and exclude reporting, checkpoint verification, final disk scanning, and cleanup. Tables use the median of three runs. P99 figures are medians of per-run interval P99s.
- No system-wide cache dropping, CPU governor changes, or tuning of the host is performed. Each sample has new page files; filesystem metadata and OS page caches follow normal operating-system behavior.

## Environment

Intel Xeon Gold 6338 at 2.00 GHz; Linux 5.15.0-139-generic x86_64; local NVMe-backed ext4 filesystem. Rust 1.96.1, Cargo release profile. The benchmark runs on a shared development machine, so the reported range across repetitions matters.

## Results

| Concurrency | Filled pages | Before (s) | After (s) | Throughput gain | Before fill (MiB/s) | After fill (MiB/s) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 10,000 | 12.71 | 9.78 | 1.30x (+30.0%) | 49.2 | 63.9 |
| 1 | 50,000 | 98.28 | 36.05 | 2.73x (+172.6%) | 31.8 | 86.7 |
| 1 | 100,000 | 288.85 | 69.72 | 4.14x (+314.3%) | 21.6 | 89.6 |
| 128 | 10,000 | 3.44 | 1.32 | 2.60x (+160.4%) | 181.6 | 472.9 |
| 128 | 50,000 | 54.63 | 7.07 | 7.72x (+672.4%) | 57.2 | 441.8 |
| 128 | 100,000 | 192.10 | 15.40 | 12.47x (+1147.3%) | 32.5 | 405.8 |

Final 90,000-100,000-page window:

| Concurrency | Before reads/s | After reads/s | Speedup | Before P99 (ms) | After P99 (ms) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 229.9 | 1508.8 | 6.56x | 6.06 | 1.14 |
| 128 | 304.1 | 6126.0 | 20.14x | 3651.81 | 54.00 |

All 12 measured runs completed with zero errors. Each run independently recovered 100,000 pages from disk and removed its temporary cache. The matrix parent directory was also removed.

At 128 concurrent cold reads, filling 100,000 pages takes 15.40 s instead of 192.10 s at the median: 12.47x throughput (+1147.3%) and 92.0% less elapsed time. In the final 90,000-100,000-page interval, median throughput is 20.14x the original and median per-run P99 falls from 3651.81 ms to 54.00 ms.

At concurrency 1, the median total time falls from 288.85 s to 69.72 s: 4.14x throughput (+314.3%) and 75.9% less elapsed time.

Per-run total measured durations (seconds):

| Concurrency | Build | Run 1 | Run 2 | Run 3 |
| ---: | --- | ---: | ---: | ---: |
| 1 | before | 282.066 | 288.849 | 297.868 |
| 1 | after | 70.771 | 69.721 | 63.821 |
| 128 | before | 191.474 | 192.102 | 192.814 |
| 128 | after | 15.401 | 12.039 | 16.250 |

The patched 128-concurrency runs range from 12.039 to 16.250 s. This variation is why the report uses the median rather than the fastest run. The comparison is specific to this local disk and workload.

Binary SHA-256 fingerprints:

- Before: `9720f02fb3e6bb05698bc895cd78ba4fe1378b55f50fa32f1de67dc031c5f22c`.
- After: `5c35c2795ffd098ef5940456a4666ef13741ecfa79cfd9d4456ba3fce94963b8`.

## Reproduction

Build the same example against both the base revision and the patched revision, retaining separate copies of the executables:

```sh
CARGO_TARGET_DIR=/tmp/cold-fill-target-after cargo build --release -p talon-worker --example cold_fill_bench --locked
cp /tmp/cold-fill-target-after/release/examples/cold_fill_bench /tmp/cold-fill-after
```

For the baseline, copy only `crates/talon-worker/examples/cold_fill_bench.rs` into an isolated checkout of `fc62db549cc5e2ecd3b014d823cd8f8aec52e65f`, then build with a separate `CARGO_TARGET_DIR=/tmp/cold-fill-target-before` and preserve `/tmp/cold-fill-before`. Do not share the Cargo target directory between worktrees. Verify that the two executable hashes differ.

Run each binary three times at each concurrency, alternating before/after order. Supply a cache root on the storage device being measured:

```sh
/tmp/cold-fill-before --cache-root /path/to/benchmark-parent \
  --pages 100000 --interval-pages 10000 --page-bytes 65536 \
  --read-bytes 4096 --threads 8 --concurrency 128 > before.jsonl
/tmp/cold-fill-after --cache-root /path/to/benchmark-parent \
  --pages 100000 --interval-pages 10000 --page-bytes 65536 \
  --read-bytes 4096 --threads 8 --concurrency 128 > after.jsonl
```

Repeat with `--concurrency 1`. The last JSON record must contain `errors: 0`, `disk_pages_verified: 100000`, and `cleanup_ok: true`. Compare cumulative `elapsed_seconds` at the same `to_pages` checkpoint; throughput speedup is `before_seconds / after_seconds`. For the final-window figures compare only records with `from_pages: 90000` and `to_pages: 100000`.

## Follow-up syscall check

After the entire A/B matrix completed, a separate 1,000-page, concurrency-1 run of the fixed binary was traced with `strace -f -c -w`. It reported 1,001 `fsync` calls (one per page plus the block sidecar), averaging 298 microseconds under tracing. This confirms that the optimized path still pays a file synchronization for each cold page.

The same probe reported 2,007 `mkdir` calls, 2,002 returning the expected existing-directory result. Both `write_sidecar()` and `put_page_async()` call `create_dir_all()` on every page; `write_sidecar()` also checks whether the sidecar already exists. Avoiding redundant directory/sidecar checks is a possible separate optimization, but its benefit has not been established by an A/B change. Removing per-page fsync would require reviewing crash recovery and cache-file publication behavior.

Tracing changes timing, and its syscall percentages cover only the selected syscalls, not the full cold-read wall time. This diagnostic run is excluded from every performance table above.

## Sparse-residency memory check

Removing pages from a version's `HashSet` leaves its allocation in place until that set is empty. A block that retains one hot page can therefore retain storage for thousands of evicted page indices. The removal helper now shrinks sets with capacity above 32 when their occupancy falls below one quarter, requesting room for twice the remaining entries. This releases large sparse allocations while leaving growth headroom and avoiding repeated resizing of small sets.

An independent LRU probe used the actual `eviction.rs` from main, `650a42e`, and the fixed source, with the same `talon-core` library and a counting `System` allocator. For each of 128 blocks, it inserted 4,096 pages at 64 KiB per page, pinned page 0, and evicted to one page per block seen so far. Earlier blocks retained their pinned hot pages. No cache payload buffers were allocated: the table measures live requested heap allocation for the LRU, excluding the logical cache-data byte count and allocator overhead; it is not RSS.

| Implementation | Active heap (bytes) | Active heap (MiB) | Resident pages | Accounted cache data |
| --- | ---: | ---: | ---: | ---: |
| Main `fc62db5` | 2,114,466 | 2.02 | 128 | 8 MiB |
| Indexed `650a42e` | 11,625,028 | 11.09 | 128 | 8 MiB |
| Indexed with sparse-set shrinking | 2,224,708 | 2.12 | 128 | 8 MiB |

All three variants verified the same 524,160 victims in eviction order, the same resident page and byte counts, and release of all measured allocations when the LRU was dropped. Shrinking reduced active heap by 8.96 MiB in this workload. This probe measures retained memory, not eviction throughput.

Two permanent regression tests cover sparse capacity eviction across blocks and explicit/superseded removal followed by regrowth, pin release, and final index cleanup. Both fail on `650a42e` because the sparse sets retain peak capacity, and both pass with shrinking. Run them with:

```sh
cargo test -p talon-worker --all-features --locked sparse_version_index --lib
```

## Scope

The cold-fill throughput comparison measures cold reads and real local page persistence with an in-memory origin. It does not establish S3/network end-to-end throughput. It covers the no-superseded-version fill case responsible for the unconditional full-LRU scan. It does not measure L1 eviction, capacity pressure, or reclamation of many pinned old versions.
