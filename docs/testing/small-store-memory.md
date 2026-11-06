# Small-store memory on the million-object fixture

Measured on 2026-11-04 on the existing QTP Worker `10.5.66.32`: two vCPUs,
16.77 GB RAM, two data-plane rings, and the existing NVMe RAID. Worker code is
`6109fff`; the control is merged #669, `ae15b98`. L1, TTI, stale GC and ownership
GC are disabled. Small-store admission is strictly below 1 MiB, with 64 MiB
segments and a 400 GiB cache capacity. All GB values below are decimal.

The same histogram, seed `20261030` and object prefix produce 1,934,887 objects
and 234,272,852,992 payload bytes. There are 1,934,886 small objects in 3,512
segments, plus one exactly-1-MiB object in one ordinary paged block. These are
1,934,887 eviction units, not 1,934,887 segment files.

| Measurement | Merged #669, small enabled | Current, small enabled |
|---|---:|---:|
| RSS at readiness | 2.404 GB | 2.119 GB |
| Maximum startup high-water RSS | 2.535 GB | 2.286 GB |
| Cache recovery | 3.894 s | 5.006 s |

Current values use three warm starts; the control uses one. Current readiness
RSS ranges from 2,119,303,168 to 2,119,757,824 bytes. Recovery ranges from 4.988
to 5.042 seconds. End-to-end readiness also includes coordinator registration,
which added approximately five seconds in two current runs.

The existing optimization stack reduces small-store readiness RSS by 11.82%.
The 1 GB target remains unmet. The previous same-machine small-disabled run used
1.608 GB at readiness. Fresh admission of this entire fixture took 656.807 seconds
and ended at 3.487 GB RSS, also the largest sampled RSS during filling. All
1,934,887 fills succeeded. Fill-time RSS and checkpoint-recovery RSS are distinct
measurements; their difference alone does not identify allocator fragmentation.

The additional recovery time is concentrated in policy membership construction:
0.945 seconds in the control versus a current median of 2.003 seconds. Total LRU
preparation/construction rises from 2.168 to 3.230 seconds. Journal replay remains
approximately 0.75–0.77 seconds; locator reconstruction is approximately one second.

## Reading method

The old fixture had a root-level catalog rejected by #669. A new isolated fixture
was populated with the same input; no catalog migration or payload shortcut was
used. Its payload size and object counts match the small-disabled fixture.

The cache-only API checks ordinary BlockIndex residency and does not serve packed
small objects in either #669 or the current code. An initial cache-only verification
therefore returned misses and was excluded. The probe's `--versioned-read` option
uses the version-pinned production interface, supplies the original object length,
and optionally caps only the requested range. It can fetch on a miss, so every
verification and measured case must also have zero backend fetch bytes and zero
cache-miss increments. Worker behavior was not changed for this measurement.

The pool contains the same 17,123 objects selected with `--sample-step 113`.
Full-content verification prewarms their 2,086,452,224 bytes. Reads request either
`min(object size, 4096)` bytes or the whole object, at 1, 64 and 128 connections,
with three repetitions, five seconds of warmup and twenty measured seconds per
case. Every response is checked byte-for-byte. The middle repetition reverses
workload and concurrency order. File cleanup completes before measurement and
must remain inactive with an unchanged scan count during every case.

This preserves the prior object distribution, ranges, concurrency and durations,
but changes the wire operation. Its throughput and latency must not be treated
as a direct comparison with the previous cache-only paged-read results. It does
not measure cold OS-cache reads, the full population as a random working set,
mixed writes, or TTI-enabled maintenance.

All 18 measured cases passed: 6,943,969 successful measured requests, zero errors,
backend fetch bytes, cache misses, evictions or cleanup scans. Maximum sampled
read RSS was **2,133,835,776 bytes (2.134 GB)**. No physical disk reads were recorded
in the measured windows after prewarming.

| Read | Connections | Median QPS | Median P99 | Median RSS |
|---|---:|---:|---:|---:|
| Up to 4 KiB | 1 | 1,251 | 0.846 ms | 2.133 GB |
| Up to 4 KiB | 64 | 47,494 | 2.208 ms | 2.133 GB |
| Up to 4 KiB | 128 | 50,170 | 3.890 ms | 2.133 GB |
| Whole object | 1 | 1,908 | 1.088 ms | 2.131 GB |
| Whole object | 64 | 7,340 | 27.849 ms | 2.131 GB |
| Whole object | 128 | 7,349 | 81.207 ms | 2.131 GB |

## Retained allocations and applicable optimizations

A separate heaptrack startup capture retained 1,937,417,568 requested allocation
bytes at readiness. The profiled Worker was killed after readiness to retain
live allocation traces; this is not a leak diagnosis. Profiler RSS is excluded
from the normal-process measurements. The following is a disjoint grouping of
live requested bytes, not a partition of RSS or a list of reclaimable bytes.

| Allocation owner | Live requested MB |
|---|---:|
| Second identity representation for LRU: packed strings and key wrappers | 409.426 |
| Small-store locator HashMaps | 373.293 |
| Encoded identity buffers already shared by small-store indexes | 371.498 |
| Whole-unit policy slots | 216.707 |
| Policy-only directory hash table and dense slots | 171.966 |
| Journal model record trees | 149.510 |
| Access tokens and idle clocks | 139.312 |
| Runtime segment offset trees and segment metadata | 54.304 |
| LRU ordered queue | 50.659 |
| Other | 0.742 |

The existing shared LRU directory and compact policy identity already apply to
small objects. However, small-object residency lives in `SegmentStore.entries`,
so its policy-only BlockIndex directory and separate whole-unit slots cannot
reuse ordinary resident slots. The paged-store catalog compaction does not
compact these small-store locator tables.

The same optimization principles have three concrete applications:

1. Share one encoded identity between small-store records and policy. Recovery
   currently decodes `Arc<[u8]>` into a temporary `BlockId`, then allocates a new
   `BlockKey` and string buffer for LRU. The second representation accounts for
   409 MB. Reusing identity storage can eliminate much of this duplication;
   wrapper and index costs still need to be measured after implementation.
2. Share locator and policy state in one stable object record. The current
   locator tables consume 373 MB while policy directory/slots consume another
   389 MB. Dense storage, as used by the current ordinary directory, also avoids
   placing a 72-byte locator in every spare hash bucket. A shared entry must
   preserve relocation tokens, reader-held file descriptors, admission
   generations and durable-publication ordering. These costs overlap; their
   totals are not an estimate of net savings.
3. Make the small-object idle clock conditional on TTI. Current recovery uses
   `AccessHandle::default()`, and independent admissions allocate a clock even
   when TTI is disabled. Those clocks account for 46.437 MB of the token row.
   The second-chance bit and stale-token protection remain necessary. Access-age
   restoration must follow the same TTI setting; merely removing the allocation
   would break its assumptions.

Journal records, segment offset trees and locator keys already share the encoded
identity buffer. Counting that buffer once per index would overstate duplication.
Journal and runtime trees also have different mutation/snapshot responsibilities;
sharing them requires preserving the existing copy-on-write and publication rules.

These are measured optimization opportunities, not implemented savings or proof
that RSS can reach 1 GB. No cold-page redesign was performed. After profiling,
the normal Worker again recovered every object and verified 17,123 sampled
objects without origin fetches. Original services were restored and the original
Worker reported ready.

Evidence is in `target/benchmarks/small-store-rss-20261104/`: startup JSON/logs,
configuration files, raw client results, metrics, 200-ms process samples and
the standalone report. The probe passed container formatting checks, Clippy
with warnings denied and a release build through `wt-build`.
