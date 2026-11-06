# Small-store segment prefix dictionaries

Segment keys use a static prefix dictionary instead of radix tries grouped by
128 rows. The shared hash lookup and its segment/row handles retain their existing
roles. A row locates its key directly, without scanning neighboring keys.

Active segments append encoded keys to a byte buffer and keep an offset/length
pair per row. Sealing sorts temporary row indices, chooses shared prefixes by
their storage cost, and writes suffixes in the original row order. The sealed
layout has one 32-bit suffix boundary per row plus a final boundary, and one
32-bit prefix ID per row when compression saves space. Each prefix has an
offset/length pair into a shared byte buffer. Prefixes never reference other
prefixes. Incompressible segments retain just the raw buffer and boundaries.

The internal key encoding puts escaped bucket and path strings before backend,
kind, block size, logical offset and version. Zero offsets are implicit; nonzero
offsets have an explicit tag bit. Hex versions retain their existing lossless
packing. Different offsets, sizes and versions therefore do not interrupt path
prefix sharing. All identity fields still participate in hashing and exact
comparison. The on-disk journal/snapshot encoding is unchanged.

Row lookup returns a prefix slice and a suffix slice in O(1) work. Exact matching
compares both slices; constructing an owned key still copies/decodes O(key length)
bytes. Tombstones remain in the metadata rows until the existing row compaction
threshold is reached. Compaction copies surviving suffixes and referenced prefixes
without decoding every full key. Frozen snapshot clones preserve their row
descriptors and dictionaries. Snapshot recovery compresses completed sealed
segments as it advances, and activation finishes the final segment.

## Local measurements

Each corpus contains 1,934,886 synthetic keys reconstructed from the existing
histogram and seed `20261030`. Modeled segments use sequential 64 MiB charge-based
packing, not captured multi-ring arrival order. The same data-backed development
container, mimalloc allocator and pinned CPU were used for the earlier trie
microbenchmark and this implementation.

| Corpus | Old trie requested B/key | Current requested B/key | Old encoded-key reverse ns | Current encoded-key reverse ns |
|---|---:|---:|---:|---:|
| Long prefix, fixed version | 41.08 | 40.32 | 4,379.5 | 65.9 |
| Long prefix, independent hex versions | 43.08 | 42.33 | 4,639.6 | 68.1 |
| Random paths, independent versions | 162.53 | 159.91 | 5,077.7 | 66.2 |
| Long prefix, varying range offsets | 179.00 | 50.33 | 4,883.0 | 61.1 |

Memory includes key buffers, descriptors and dictionaries. It excludes metadata
rows, global indexes, allocator overhead, input storage and other Worker heap.
The probe directly includes the current `keys.rs` and `key_codec.rs`; reconstruction
timing stops at the encoded byte string and excludes decoding and owned `SmallKey`
construction. Every key was restored byte for byte. Timings are medians of three
loops of 150,000 random accesses, not independent host-level trials. These local
measurements do not represent whole-Worker RSS or request throughput.

Artifacts and source hashes are in
`target/benchmarks/prefix-production-20261106/`. Earlier trie baseline artifacts
are in `target/benchmarks/prefix-dictionary-20261106/`.

## QTP startup RSS

The release binary was deployed to the existing QTP 2-vCPU Worker with
1,934,886 persisted small objects and one paged block. Small-store admission stayed
enabled, L1 and idle/ownership/stale GC were disabled, and the default mimalloc
allocator was used. Three clean starts reused the same cache without dropping the
OS page cache. RSS comes from `/proc/<pid>/status`; idle RSS is the median of the
last five samples in a 30-second idle window after startup cleanup completed.

| Round | Ready RSS (MiB) | Idle RSS (MiB) | Startup peak RSS (MiB) | Cache recovery (s) |
|---|---:|---:|---:|---:|
| 1 | 185.08 | 187.80 | 203.58 | 3.792 |
| 2 | 186.17 | 188.36 | 195.62 | 3.689 |
| 3 | 184.19 | 190.28 | 200.66 | 3.696 |

Median ready RSS was 185.08 MiB and median idle RSS was 188.36 MiB. A subsequent
byte-exact sample verified all 17,123 objects with no origin fetch, cache miss or
eviction. Payload file counts and sizes were unchanged. The original Worker kept
running, and all test services were restored to their prior states. Request QPS
and sustained write/eviction RSS were not measured for this implementation.

Artifacts are in `target/benchmarks/prefix-rss-20261106/`. The deployed binary's
SHA-256 is `17aee5ab51783791b7b4755b907fadabda631cc9a069b535019223fc1447fa45`.

## Validation

The data-backed Talon development container passed 736 Worker library tests
(9 existing ignored tests), 24 binary tests, and four dependency-preparation tests.
The final release build, formatting check and maximum-length NUL-key regression
also passed.
Coverage includes binary/NUL and UTF-8 keys, duplicate identities, varying exact
ranges, reordered inserts, deletion/reuse, deeply nested prefixes, frozen snapshots,
journal recovery and native io_uring paths. Native tests use the existing container
wrapper that permits io_uring while preserving read-only root/data-disk mounts.

Clippy with warnings denied passed for library, binary, benchmark and example
targets with all features. The all-targets invocation remains blocked by the
existing compiler-wrapper environment issue: integration-test `env!` calls cannot
see `CARGO_BIN_EXE_talon-worker`, including when supplied explicitly. The same
failure is recorded in the preceding trie validation artifacts. Integration-test
Clippy and remote CI are not claimed as passing.
