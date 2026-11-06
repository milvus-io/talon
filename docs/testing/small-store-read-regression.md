# Compact small-store read regression

The trie measurements below are historical. The current segment key layout and
its local measurements are described in
[small-store-prefix-dictionary.md](small-store-prefix-dictionary.md).

The first 40-byte implementation introduced redundant metadata work on cache hits.
On the QTP 2-vCPU Worker, C128 reads of up to 4 KiB fell from 51,197 to 45,625 QPS
(-10.88%), while non-key requested heap fell from 378.00 to 37.94 bytes per item.
The same 1,934,886 small objects, one ordinary paged block, enabled small store,
and disabled L1/TTI/stale GC/ownership GC are used throughout this investigation.

## Cause

The runtime index had become global, but the read path still walked fallback slot
zero and each ring's directory. All three directories searched the same global
hash table, then rejected a match belonging to another stream. A hit could
therefore repeat the same key lookup three times, including an empty fallback
stream.

The compact access token also resolved its payload address again on every touch:
segment lookup, binary search by offset, record decoding and key hashing. The key
pool was a byte vector, so each record decode scanned valid key text again with
`from_utf8`. Finally, the locator path decoded its matched record again and
constructed a policy snapshot even though serving needed only its access token.

Software-clock CPU sampling on the same C128 workload corroborated these paths:

| Direct sampled CPU | Previous optimized store | First compact store |
|---|---:|---:|
| `Catalog::at` | absent | 3.06% |
| `Records::get` | absent | 1.48% |
| `from_utf8` | 0.11% | 1.39% |
| SipHash writes | 0.41% | 0.95% |
| `Catalog::location` | absent | 1.04% |

Percentages combine both data-plane rings. They are direct samples, not additive
call-tree totals or a complete attribution of the QPS delta. The VM did not expose
hardware performance counters; `perf record -e cpu-clock -F 199` with DWARF stacks
was used. Profiling materially reduced throughput, so its QPS is excluded from
all performance comparisons.

## Changes

Key text now lives in `String`, retaining the same allocation layout while safe
string slicing checks boundaries without rescanning entire keys. Access tokens
cache a lookup slot and epoch; insert/remove/rehash invalidates that cache, and
compaction updates both the physical address and slot. Retirement still prevents
old tokens from affecting a new admission.

Reads perform one lookup in the shared catalog. Matching produces a stack-local
record descriptor used directly to build the location. The read path obtains the
access token without constructing or locking a policy snapshot. No per-object
resident field or second index was added.

A shared read gate protects the interval from locator lookup through asynchronous
fd retention. Relocation hides the old location and publishes the new location
under one Catalog write lock, including for synchronous residency queries.
Compaction pays deletion-rate credits before taking the gate, and releases it
after publication and retirement of the old locator, before unlink/fsync.

Intermediate unprofiled same-host checks isolated the improvements:

| Implementation | Candidate QPS | Paired control QPS | Change |
|---|---:|---:|---:|
| Cached slot + validated string pool | 48,248.5 | 51,477.9 | -6.27% |
| One shared-index lookup | 50,563.6 | 51,100.6 | -1.05% |

These are diagnostic single runs. The final implementation also removes the
remaining repeated record decode and policy snapshot.

## Final paired measurements

The final binary includes production commit `e85f0dc`. Each case uses C128, a
deterministic 17,123-object sample, five seconds of warmup and 20 measured seconds.
The probe supplies the resolved version and object size. Round zero runs candidate
then control; round one reverses that order. Profiling is disabled for these reads.

| Round | Variant | Up to 4 KiB QPS | Entire object QPS |
|---|---|---:|---:|
| 0 | Candidate | 50,578.30 | 7,352.30 |
| 0 | Control | 50,937.10 | 7,339.30 |
| 1 | Control | 51,140.15 | 7,349.45 |
| 1 | Candidate | 50,788.55 | 7,362.45 |

| Warm cache-hit workload | Control QPS median | Candidate QPS median | Change | Control / candidate p99 |
|---|---:|---:|---:|---:|
| Up to 4 KiB | 51,038.63 | 50,683.43 | -0.70% | 3.891 / 3.854 ms |
| Entire object | 7,344.38 | 7,357.38 | +0.18% | 86.133 / 81.467 ms |

Both paired 4 KiB runs retain an approximately 0.7% throughput gap. Two repetitions
do not establish statistical parity or prove that the residual is noise. Worker
CPU time per 4 KiB request is 38.65 us for control and 38.85 us for candidate.
The repeated metadata work explains most of the original regression; the remaining
gap is not separately attributed by these measurements.

All eight points passed: 4,656,952 measured requests, zero errors, origin fetch
bytes, cache misses, evictions or concurrent cleanup scans. Physical disk-read
bytes were zero. Five byte-exact passes verified 85,615 responses, including a
restart after heap capture. Payload file counts, logical bytes and allocated bytes
were unchanged; control also reopened candidate-written metadata checkpoints.

| Memory or startup metric | Control | Candidate |
|---|---:|---:|
| RSS at ready, median | 1,078.92 MiB | 376.89 MiB |
| RSS during reads, maximum | 1,087.17 MiB | 384.57 MiB |
| Cache recovery, median | 3.823 s | 5.360 s |
| Process start to ready, two runs | 3.877 / 8.884 s | 10.398 / 10.450 s |

A fresh candidate heap capture contains 359,006,527 requested live bytes, including
285,593,045 key-string bytes. The remaining 73,413,482 bytes across 1,934,886 items
are **37.942 B/item**, including shared indexes and all other live Worker heap.
This preserves the 40-byte target with L1, TTI, stale GC and ownership GC disabled;
enabled optional tracking and write-heavy workloads are not covered here.

Recovery remains slower than control. Ready timing also contains additional delay
in some runs of both variants; readiness-reason timing was not captured, so that
delay is not attributed to cache recovery.

## Correctness and evidence

The data-backed development container passed 691 Worker library tests (8 existing
ignored tests), Clippy with warnings denied, formatting and the release build.
Regressions cover slot invalidation after rehash/record compaction, relocation
when the token already has the current epoch, atomic visibility during relocation,
cancellation while opening an fd, retained-fd reads after unlink, and another
ring reading while deletion is rate-limited. The last test waits on an explicit
budget-entry counter, not a journal event that can precede the actual wait.

Independent adversarial review found the relocation visibility gap and the
expanded lock scope during GC waits; both were fixed and rechecked. The review
also checked token identity, epoch invalidation, generic clock tracking and
charge overrides.

Artifacts are under `target/benchmarks/small-store-qps-20261105/`: software-clock
profiles, `profile-hotspots.json`, paired intermediate measurements, final
`resolved/` measurements and heap traces, validation logs, driver `compare.py`
and restoration audits. The preceding memory result and its accounting method
are in [small-store-40b-memory.md](small-store-40b-memory.md).

The original Worker was restored and verified ready. The test Worker, coordinator
and origin returned to their original inactive states. All changes remain in the
local stack; nothing was pushed or published.

| Binary | SHA-256 |
|---|---|
| Control (`815be6e`) | `c320c32a57d211130f7e47f6c0b9d452432c6fe2fb78fd727bc4d0b539ab3459` |
| Candidate (`e85f0dc`) | `d50f1d05f4539450144a1f48412c957852479aaab1fd7b1afd204b6745219e0f` |
| Read probe | `2f506473462a7b4e933d6e91901e78ea54e03f276310cfe226e2f106d1fd598e` |

## Local recovery cleanup

Recovery now publishes the already indexed journal rows using borrowed keys,
avoiding per-object key allocation and the live-admission staging path. The global
duplicate check and visibility update share one catalog lock. File validation and
duplicate rejection still precede orphan cleanup. Redundant counting/reservation
passes were removed, and empty streams skip the global CLOCK scan.

Validation passed 692 Worker library tests (8 existing ignored), formatting,
warnings-denied Clippy and the release build. The library suite ran in the same
data-backed development image with native io_uring enabled; the default Docker
seccomp profile rejects those syscalls. The added regression checks the first
fallback-stream admission after recovering another stream, including policy byte
accounting and removal.

Evidence is under `target/validation/small-store-finish-20261105/`. The performance
and heap tables above describe the earlier measured build; the startup benefit
of this cleanup has not been benchmarked in isolation.


## Integrated trie and mimalloc comparison

The initial production integration replaced the full-key string arena with radix
tries in groups of 128 ordered records, with decimal pairs packed in path keys.
Each record retains 26 bytes of extent,
checksum, hash and key-length metadata. The existing shared handle/CLOCK index
remains; hot lookups verify exact trie membership without reconstructing keys.
Reverse key lookup is bounded by one group, and snapshots decode groups in offset
order. Journal encoding and segment files remain compatible with the control.

This integration retains the offset directory needed by recovery, compaction and
stable eviction handles. The earlier standalone 61.68 B/item experiment measured
a different core index; it was not whole-Worker RSS. The results below are measured
RSS of the complete deployed Worker, without subtracting a baseline or excluding
key memory. No custom pool, mmap allocator or compact trie header is used.
The dependency patch only fixes the 256-child counter overflow.

All three variants use the same 1,934,886 small objects plus one paged block on
the existing QTP Worker (2 vCPU, 16 GiB), with small-store enabled, L1 disabled,
and optional TTI/stale/ownership tracking disabled. Three repetitions rotate
variant order. Cache-hit reads use C128, five seconds of warmup and 20 measured
seconds, with resolved version and size. Values below are medians.

| Variant | Whole Worker RSS at ready (MiB) | Cache recovery (s) | Up to 4 KiB QPS | Entire object QPS |
|---|---:|---:|---:|---:|
| Pre-trie control | 376.80 | 3.859 | 50,228.65 | 7,358.60 |
| Trie + system | 179.96 | 5.153 | 47,584.90 | 7,348.55 |
| Trie + mimalloc 3.3.2 | 196.02 | 4.126 | 49,811.25 | 7,347.05 |

Process start to readiness includes additional time beyond cache recovery:

- Pre-trie control: 3.923 / 3.927 / 3.925 s.
- Trie + system: 5.189 / 5.238 / 5.188 s.
- Trie + mimalloc 3.3.2: 4.237 / 9.184 / 4.181 s.

These initial comparisons used system allocation by default. The Worker now
enables mimalloc in its default Cargo features (mimalloc crate 0.1.52,
libmimalloc-sys 0.1.49, native 3.3.2). A normal `cargo build --release -p
talon-worker --bin talon-worker` uses mimalloc; `--no-default-features` selects
system allocation for comparison. No allocator environment tuning was applied.

The three runs did not reach 100 MiB. System allocation used less RSS; mimalloc
recovered faster and improved the small-read result relative to the trie system
build. This is a measured tradeoff on this corpus, not a universal allocator bound.

All 18 read points passed: 10,149,827 measured requests, zero errors, origin
fetch bytes, cache misses, evictions and overlapping cleanup scans. Physical disk
read bytes were 0. Nine byte-exact checks verified 154,107 responses.
Payload file counts and logical/allocated sizes were unchanged; control runs
successfully reopened trie-written metadata checkpoints. The original Worker PID
was unchanged and ready, and the test Worker, coordinator and origin were
restored to their original inactive states.

Final local validation passed 697 Worker library tests, with eight existing
ignored tests, formatting, warnings-denied Clippy and both release builds in the
data-backed development container. Independent cleanliness and correctness reviews
completed; codec overflow and deleted-offset reuse findings were fixed and retested.
New regressions cover all decimal pairs, 256 trie branches with long labels,
UTF-8/NUL identity round trips, cross-group reorder/delete/reinsert and frozen clones.

These throughput results cover warm reads. Sustained write/eviction RSS is not
measured: reverse lookups visit at most 128 keys, while repacking may temporarily
materialize a segment or its suffix. Ordinary allocation and the existing COW
and eviction lifecycle are retained.

Evidence: `target/benchmarks/small-store-trie-20261105/` contains the driver,
source and binary fingerprints, nine startup measurements, 18 read measurements,
process samples, content checks, payload inventories and restoration audit.
`target/validation/small-store-trie-20261105/` contains native test logs,
build report and review evidence.

| Binary | SHA-256 |
|---|---|
| Pre-trie control | `631ca65eb4024e8e55a4ad86348ddc920175eccbae88f594303f9a12a27a07a1` |
| Trie + system | `54d039e473cc594029caca1dbda895a09acf30ea6334965777f46ccefa6324e7` |
| Trie + mimalloc 3.3.2 | `adf96af40b6a52ce4045cb8408b15ae6a96a6ac5bcc30935ac27a959344a8b9c` |


## Trie lookup CPU follow-up

The warm-hit path with L1 disabled performs one small-store lookup per request.
Its regression comes from encoding the identity and verifying it through the trie,
including node traversal and child scans. It does not perform a second request-path
lookup. Ring-thread CPU sampling put trie lookup at 9.96% of ring user CPU and key
encoding at 2.91%; these are not percentages of total request time. Throughput
measurements below run without the profiler, which perturbs this workload.

A local component benchmark uses the corpus size distribution and key shape,
17,123 sampled identities, synthetic 512-record segments and two million random
queries per pass. The original flat lookup measured about 0.37 us versus 1.11 us
for the first trie integration. Reusing the encoding buffer saved only about
0.06 us. In the follow-up run, BCD encoding took 350 ns and plain-path encoding
105 ns; plain trie matching itself did not show an improvement. These local CPU
measurements isolate costs and are not deployed Worker request latencies.

The retained change removes decimal-pair packing and path escaping. The existing
version-length header separates the raw UTF-8 path from the version, preserving
exact identity checks, including NULs. It also computes hash/encoding before the
catalog write lock and reuses matched metadata for the read location. This
CPU-only comparison kept the shared index, 128-record groups, system allocator
and on-disk format unchanged.
No additional dependency or allocator-layout patch was introduced.

Metadata reuse alone did not show a clear end-to-end gain. A separate 32-record
BCD-group candidate reached 49,267 QPS versus its paired 50,336 control, at about
195 MiB RSS. Only its first completed round is used; subsequent repetitions were
stopped to test removing BCD. The simpler plain-path version used approximately
193 MiB with system allocation.

The final source passed 696 Worker library tests with eight existing ignored,
formatting and warnings-denied Clippy for library/binary targets. All-target
Clippy remains blocked by the existing `CARGO_BIN_EXE_talon-worker` compile-time
requirement in `tests/instance_profile.rs`. Independent cleanliness and correctness
reviews found no outstanding issue in this follow-up. Decimal-pair-only coverage
was removed with that encoding; UTF-8/NUL and lifecycle regressions remain.

Three final repetitions alternate the pre-trie control and plain-path candidate,
using the same 1,934,886 objects, C128, five-second warmup and 20-second measurements.
Both use the system allocator. Medians:

| Variant | RSS at ready (MiB) | Cache recovery (s) | Up to 4 KiB QPS | Entire object QPS |
|---|---:|---:|---:|---:|
| Pre-trie control | 377.01 | 3.862 | 50,593.10 | 7,360.55 |
| Plain-path trie | 192.66 | 3.805 | 48,797.40 | 7,351.90 |

The remaining 4 KiB QPS regression is 3.55%; entire-object QPS differs by 0.12%.
The earlier BCD trie had a 5.26% regression in its own three-round comparison.
The change therefore recovers part of the loss, without claiming full parity.
Peak sampled RSS during reads was 200.12 MiB for the candidate and 384.51 MiB for
the control. The 193 MiB figure refers to RSS at readiness, not the loaded peak.

Cache recovery and readiness are separate measurements. Control ready times were
3.926 / 8.883 / 3.926 s; candidate times were 8.878 / 8.832 / 3.926 s. Both variants
sometimes had an extra approximately five-second readiness wait after becoming
healthy. Recovery time excludes that wait; it is not total startup-to-ready time.

All 12 read points passed: 6,850,273 measured requests, zero errors, origin fetches,
cache misses, evictions, overlapping cleanup scans or physical disk read bytes.
Six byte-exact checks verified 102,738 responses. Payload file counts and allocated
and logical sizes were unchanged. Control successfully reopened candidate-written
metadata. The original Worker PID 163477 remained ready; all test services were
restored to their original inactive state.

Evidence: `target/benchmarks/trie-qps-20261105/plain-final/final-summary.json`,
its raw measurements, startup logs, source/binary fingerprints and restoration
audit. `plain-performance.patch` records this follow-up separately from the prior
integration; `rings/`, the component logs and `review.json` record attribution and
review boundaries. Validation logs are in
`target/validation/trie-qps-20261105/`. The tested candidate SHA-256 is
`c933cc8d99d90a6cd56f716cee66a1264650aa59deb3259f6736dd5d763b70bd`.


## Default mimalloc validation

The Worker now enables mimalloc through its default Cargo features. The normal
release build and Worker image build both select it without additional flags;
`--no-default-features` retains the system-allocation comparison. The global
allocator is installed in the Worker binary, not in the reusable library.

Three repetitions alternate the original BCD trie with mimalloc, the plain-path
trie with system allocation, and the plain-path trie with default mimalloc. The
same 1,934,886 small objects, C128, five-second warmup and 20-second measurement
windows are used for both read workloads. No allocator environment tuning is set.
The first two binaries are unchanged from their earlier measurements. Medians:

| Variant | RSS at ready (MiB) | Peak sampled read RSS (MiB) | Cache recovery (s) | Up to 4 KiB QPS | Entire object QPS |
|---|---:|---:|---:|---:|---:|
| Original BCD trie + mimalloc | 193.54 | 212.21 | 4.167 | 49,289.25 | 7,359.20 |
| Plain-path trie + system | 192.41 | 200.51 | 3.819 | 48,601.00 | 7,335.15 |
| Plain-path trie + default mimalloc | 204.06 | 225.37 | 3.025 | 50,069.20 | 7,358.30 |

Default mimalloc improves the current code's small-read QPS by 3.02% over system
allocation. Relative to the original trie with mimalloc, small-read QPS improves
by 1.58%, recovery falls from 4.167 to 3.025 seconds, and ready RSS increases by
10.52 MiB. The throughput improvement over the original mimalloc build is modest;
these are three-run point estimates, not a significance claim. Whole-object
throughput is effectively unchanged. Earlier 49,811 QPS and 196 MiB figures came
from the original comparison, rather than this paired run.

Memory columns distinguish current RSS from high-water marks. For the new default,
ready RSS was 201.79 / 207.44 / 204.06 MiB; startup VmHWM was
219.30 / 221.79 / 209.77 MiB. The startup high-water marks must not be reported as
ready RSS. Peak RSS sampled during reads was 225.37 MiB.

New-default startup-to-ready times were 3.171 / 3.065 / 8.069 seconds; health became
available after 3.119 / 3.065 / 3.066 seconds. Recovery time excludes occasional
extra readiness waits, also present in the old binary.

All 18 read points passed: 10,197,638 measured requests, zero errors, origin
fetches, cache misses, evictions, overlapping cleanup scans and physical disk reads.
Nine byte-exact checks verified 154,107 responses. Payload inventories were
unchanged, and all test services returned to their original inactive states;
the original Worker PID 163477 remained ready.

The default-feature release build, formatting and warnings-denied library/binary
Clippy passed. All 21 Worker binary tests passed with the global mimalloc allocator.
The prior 696 library tests (eight ignored) apply to unchanged Rust sources; those
library tests used the system allocator. All-target Clippy retains the previously
documented integration-test compile-time environment limitation.

Evidence is under `target/benchmarks/trie-qps-20261105/default-mimalloc/`, including
`final-summary.json`, exact binary/source fingerprints, process samples and the
restoration audit. `compare-mimalloc.py` and `summarize-mimalloc.py` describe the
comparison. Default-feature build and test evidence is under
`target/validation/trie-qps-20261105/default-mimalloc-*`. The new binary SHA-256 is
`4d3180dccbed28c947157dc2026ba4c6fe2c001c725ebe980504be307a360472`.
