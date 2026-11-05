# Small-store metadata below 40 bytes per object

The compact implementation retains **37.942 bytes per small object excluding key
text**, measured on the existing QTP 2-vCPU Worker with 1,934,886 small objects.
The previous implementation retained 378.000 bytes under the same accounting.
This counts the entire live requested heap, including shared structures and container
capacity, after subtracting only the exact UTF-8 key pools. It is not `size_of` or
an RSS partition.

## Memory representation

The journal model and runtime directory share each segment's record array and key
pool. There are no separate resident BlockKey wrappers, per-record tree nodes,
locators, PageState objects or LRU queue nodes in the compact policy.

| Fixed row field | Bytes | Purpose |
|---|---:|---|
| Payload offset | 4 | Locate data within the owning segment |
| Payload length and backend | 4 | Low 28 bits are length; high 4 bits identify backend; zero length is a tombstone |
| Checksum | 8 | Preserve existing payload verification |
| Key-pool offset | 4 | Locate the concatenated UTF-8 identity |
| Block size | 4 | Preserve full BlockId identity |
| Total key length and two string boundaries | 6 | Recover bucket, path and version without resident String headers |
| **Total** | **30** | Segment identity belongs to the containing array; small-object block offset is zero |

The widths preserve existing limits: segment size at most 1 GiB, small-object
threshold at most 128 MiB, and encoded keys at most 64 KiB. The persisted journal
and snapshot formats are unchanged.

A single open-addressed index stores a four-byte row handle and one control byte
per physical slot. Five control bits filter hash collisions; the remaining bits
record visibility, policy membership and CLOCK reference state. Full identities
are compared on collisions. At 80% occupancy this costs 6.25 bytes per object.
Handles identify a segment and a row through shared 1,024-row chunks; they do not
reserve storage for unused rows. Growth uses bounded spare capacity, with separate
shrink hysteresis on removals.

Pins, candidate revisions and stable read tokens exist only while needed. Tokens
survive relocation, cannot affect a later admission, and are reclaimed after use.
Each stream maintains aggregate byte/count accounting. CLOCK scans release locks
every 64 physical slots, including empty slots; ticket-table maintenance is
amortized separately and is not a strict lock-duration guarantee.

## Scope and accounting

Small store remains enabled with the existing strict `< 1 MiB` threshold, 64 MiB
segments, two data-plane rings, 400 GiB capacity, 1 MiB pages and 256 MiB blocks.
L1, TTI, stale GC and ownership GC remain disabled, matching the prior workload.
Age/ownership tracking configurations retain their existing optional metadata;
**the 40-byte result does not cover those enabled configurations**.

The fixture contains 1,934,886 small items in 3,512 segments and one ordinary
1 MiB paged block. Neither payload refill nor cache clearing is part of this run.

| Live requested allocation | Total bytes | Bytes / small item |
|---|---:|---:|
| Actual key text, excluded from target | 285,593,045 | 147.602001 |
| Shared fixed record rows | 58,046,580 | 30.000000 |
| Hash handles and control/CLOCK bytes, including spare slots | 12,093,120 | 6.250043 |
| All other Worker heap, including segment/chunk structures | 3,273,940 | 1.692058 |
| **Non-key total** | **73,413,640** | **37.942101** |
| Entire live heap | 359,006,685 | 185.544102 |

The separate heaptrack run captures live allocations at readiness. SIGKILL of
only the profiled test Worker retains their traces for analysis; these allocations
are not a leak diagnosis. RSS measurements use ordinary unprofiled processes.
The key allocation is independently visible as exactly 285,593,045 bytes in the
new capture, matching the old fixture. Record allocations sum to exactly 30 times
the item count. Remaining allocations are all charged to the non-key budget.

## Same-host startup and reads

Control is the prior optimized binary `815be6e`; compact production code is
`a73f26c`. Three compact and two control warm starts used the same fixture.
Ordinary runs alternated compact/control, then control/compact, followed by a
third compact recovery. No profiler was attached to these measurements.

| Metric | Control | Compact |
|---|---:|---:|
| Ready RSS, median | 1,079.03 MiB | 376.68 MiB |
| Startup RSS high-water mark, maximum | 1,237.92 MiB | 377.01 MiB |
| Read-pressure RSS, maximum | 1,087.57 MiB | 384.88 MiB |
| Cache recovery, median | 3.841 s | 6.243 s |
| Cache recovery, range | 3.836–3.845 s | 6.222–6.279 s |
| Process start to ready, median | 3.878 s | 11.308 s |
| Process start to ready, range | 3.877–3.878 s | 11.257–11.313 s |

The memory target is met with a startup regression. Median compact recovery
spends 1.044 s replaying the journal, 0.403 s activating/querying the shared model,
2.103 s restoring locators and 2.691 s restoring policy. The latter now includes
compact membership/accounting scans; it is the largest measured phase. Ready
changes roughly five seconds after recovery in the compact runs; readiness-reason
timing was not captured, so that additional delay is not attributed here.

Reads use C128, a deterministic 17,123-object sample, a five-second warmup and a
20-second measured interval. Each case ran twice, reversing workload order in the
second iteration. The read probe supplies the resolved version and object size.

| Warm cache-hit workload | Control QPS median | Compact QPS median | Change | Control / compact p99 |
|---|---:|---:|---:|---:|
| Up to 4 KiB | 51,197 | 45,625 | -10.88% | 3.896 / 4.190 ms |
| Entire object | 7,349 | 7,357 | +0.10% | 85.398 / 81.244 ms |

All eight points passed: 4,461,105 measured requests, zero errors, zero origin
fetch bytes, zero misses, zero evictions and no concurrent cleanup scans. Physical
disk-read bytes were zero, so these figures cover warm hits. Five byte-exact
verification passes checked 85,615 responses, including recovery after the heap
capture. Payload file count, logical bytes and allocated bytes were unchanged.
The control binary also reopened metadata checkpointed by the compact binary.

This completes the memory reduction; startup and small-range read performance
remain follow-up optimization targets. Write-heavy performance and enabled age
tracking were not benchmarked.

## Validation

The data-backed Talon development container passed 685 Worker library tests
(8 existing ignored tests), Clippy with warnings denied, formatting and the release
build. A subsequently added real compaction/restart test also passed, for 686
passing tests in total. Coverage includes full-key collisions, 100,000-record
recovery, snapshot isolation, publication/removal ordering, compact explicit
invalidation with mixed stores, nested pins across compaction, stale tokens after
readmission, bounded scans and token reclamation.

Independent adversarial review found and rechecked fixes for generic-queue access
from compact explicit deletion, inactive token retention and unbounded CLOCK scan
critical sections. Earlier layers also corrected recovery cleanup and index-growth
hysteresis. No cold-page redesign is included.

Local stack additions are `cache/small-compact-records`,
`cache/small-shared-catalog`, `cache/small-clock-policy`, then
`test/small-40b-memory`. Production code is at `a73f26c`.

Evidence is under `target/benchmarks/small-store-40b-20261105/`: driver
`measure40.py`, binary hashes, raw startup logs/metrics, read samples, live heap
stacks, `heap-breakdown.json`, build/test logs and the service-restoration audit.

The original Worker service was restored and verified ready. The test Worker,
coordinator and origin returned to their original inactive states. All code and
results remain in the local stack; no remote PR was created or updated.

| Binary | SHA-256 |
|---|---|
| Control | `c320c32a57d211130f7e47f6c0b9d452432c6fe2fb78fd727bc4d0b539ab3459` |
| Compact | `f2cf11cfd003e92ea99508e003bf38a292a00964bcd6e36f3f01ad8e3d1c1dc0` |
| Read probe | `2f506473462a7b4e933d6e91901e78ea54e03f276310cfe226e2f106d1fd598e` |
