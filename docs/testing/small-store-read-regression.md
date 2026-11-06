# Compact small-store read regression

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
