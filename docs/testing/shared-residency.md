# Shared Worker residency metadata

The Worker should look up a block once and share its identity and runtime state
between residency, lifecycle and eviction. The motivating fixture has 1,934,887
blocks with one cached page each, small store disabled, and a 2-vCPU Worker.
The baseline restores in 19.29 s and uses 9.69 GiB RSS. These are checkpoint-based
startup measurements, not recovery from a long uncheckpointed journal.

The stack separates independently testable changes:

1. Share the block directory between BlockIndex and PageLifecycle.
2. Share page storage, preserving read guards and deletion generations.
3. Reuse resident objects for unit-level eviction membership.
4. Share block-level policy state and identities; retain necessary policy queues
   and secondary indexes.

The first layer stores optional residency and lifecycle components under one
canonical `Arc<BlockId>`. A pending fill can own lifecycle state without appearing
in residency lookups or byte/page counters. Removing residency does not replace
its mutation gate. Retirement removes the lifecycle component only when its page
state is empty, cleanup is complete, and no external owner retains the block.
Ordered shard traversal and access checkpoints continue to reference that object.

Residency replacement still retires the old index entry. Page updates that retained
that entry cannot mutate a new admission through an old metadata incarnation.
The shared page layer uses registry -> directory -> lifecycle bookkeeping -> page
table for retirement. Scans release the page and bookkeeping locks before entering
retirement. Ordinary reads take directory -> resident entry -> page table, and
never acquire the lifecycle bookkeeping mutex.

The journal, access snapshot and request formats are unchanged. Tests cover pending
fills excluded from residency, gate identity across removal/reinsertion, retirement
with external owners, and scanning abandoned fills. The first layer passed all
650 Worker library tests (8 ignored) and Clippy with warnings denied in the prescribed
container. The test container permits io_uring; Docker's default seccomp profile
otherwise rejects its creation with EPERM.

Re-run the same fixture after the complete stack and report startup, steady RSS,
restored counts and cached-content checks. A reduction below 1 GB is a target to
measure, not an outcome established by these structural changes.

The second layer shares a page table and one PageState between residency and
lifecycle. One-page blocks store their entry inline; multiple pages use an ordered
BTreeMap. Reads take the table read lock, independently of lifecycle bookkeeping.
Clearing residency refunds allocation and clears access, but leaves a bound page
for the deletion claim to finish. A removed handle stays closed; recreating the
page gets a fresh handle, so an old candidate cannot modify the new generation.
Startup binds existing page objects by block instead of copying a second table.

The second layer passed 651 Worker library tests (8 ignored) and Clippy. The shared
fixture regression covers read/delete arbitration, a failed-unlink retry, container
transitions and stale candidates after re-admission. Neither layer changes disk
formats or has yet established the post-change RSS on the QTP fixture.

The third layer removes the LRU unit HashMap. A unit slot stores policy membership,
bytes, pins and queue revisions beside its access token. LRU retains its ordered
queues and secondary version index. A policy-only admission does not count as
resident data, which also supports packed small objects.

The directory retains policy references through `clear_page` / `index.remove`
until LRU removal completes. Replacing residency preserves the prior policy slot
until its admission is removed; the old allocation remains independent so delayed
index retirement cannot subtract a replacement's bytes. Ordinary admissions share
the resident slot. The policy lock precedes directory, entry and page-table locks.
Unbound policy-only nodes are reclaimed on removal, even while a sibling remains.
The third layer passed 654 Worker tests (8 ignored) and Clippy. Regressions cover
pin and token preservation, delayed index retirement, policy-only residency
isolation and bounded page churn. Independent concurrency and cleaness reviews
found no remaining defect in this layer.


The fourth local layer removes the block activity HashMap and uses the directory's
canonical block identity in block and ownership queues. Unit membership has one
count in `PolicyUnits`; activity reads expose a snapshot of that count. Reusing an
index to initialize another runtime rebuilds resident slots and discards the old
runtime's lifecycle and policy state while retaining metadata and allocation.
The previous runtime must be stopped before this startup-only initialization.

The complete local code passed 654 Worker library tests (8 ignored), Clippy with
warnings denied, and a release build. Independent cleaness and concurrency reviews
covered policy removal, retained incarnations, shared identity retirement, and
runtime reinitialization. The work remains local and has not been published.

Measured after these four local layers on the same QTP fixture:

| Startup condition | Recovery median | Ready median | RSS median |
|---|---:|---:|---:|
| Warm, 3 runs | 15.395 s | 15.619 s | 6.867 GiB |
| OS cache cleared, 3 runs | 15.676 s | 16.077 s | 6.867 GiB |

The original-binary control recovered in 19.370 s. All runs restored exactly
1,934,887 blocks/pages with zero evictions; 17,123 cached-content checks passed.
The final binary SHA256 is
`4d91b707d248f5702bcc4bba979cc26fcfb591e167fbec24ec0ceefaf4b74d61`.
A readiness observer captured `coordinator_not_registered` for 4.94 s during one
post-recovery delay; recovery time and end-to-end readiness remain separate metrics.
The original services were restored and the original Worker was ready afterward.

**The 1 GB target is not met.** RSS is still approximately 7.37 GB, or 3,811 bytes
per object. Further work must budget the complete retained representation against
approximately 500 bytes per object and verify read throughput, tail latency and CPU.

The fifth layer stores one catalog page inline and shares the recovered block
identity with the residency directory and metadata. Catalog snapshots retain the
same ordered JSON map encoding and copy-on-write isolation. Repeated whole-block
writes preserve the canonical identity. Online first admission still constructs
separate catalog and runtime identities; that path needs further consolidation.
Validation passed 657 Worker tests (8 ignored) and Clippy. Its RSS has not yet
been measured. Memory is now the first milestone; read performance follows.

The sixth layer creates lifecycle maintenance state on demand when TTI is off.
Cold recovery keeps the resident PageState for read/delete arbitration, but does
not allocate a BlockState, mutation gate or BlockWork for every block. Maintenance
scans retire idle sidecars after callers release them; retries, cleanup and retained
gates prevent retirement. Page handles keep their reader count through retirement
and rebinding. Enabling TTI before serving still restores every resident page's age.
Validation passed 659 Worker tests (8 ignored) and Clippy, including retained
readers, gates, retry cancellation and access tracking bound after an earlier read.

The seventh layer shares identities in the capacity queue and retains one block
reference per version. A global digest table stores the common single-version
case inline; extra versions and digest collisions use an overflow list. Queries
and removals still compare complete identities. Page membership comes from the
resident policy slots. Removing a page checks aggregate membership without a
second page scan. Independent whole-object policy remains excluded from version
eviction. Validation passed 660 Worker tests (8 ignored) and Clippy. One full-suite
run hit a connection reset in the unchanged cross-ring echo test; the rerun passed.

Two warm runs of the combined catalog, lifecycle and policy changes restored
1,934,887 blocks/pages in 10.052 s and 10.058 s. Ready RSS was 2,352,439,296 and
2,359,775,232 bytes; startup high-water RSS was 2,487,496,704 and 2,495,078,400 bytes.
The 17,123-object content check passed. Small store and TTI remained disabled,
evictions stayed zero, and the original services were restored afterward.
Measured binary SHA256:
`98c9462924ca125b206e4014d1f8bfd4e5e88c5b290e5812376279c6ec94828c`.
The final source additionally narrows the insertion interface and makes the
version-membership removal check constant-time; that binary has not been measured.

A separate heaptrack capture of the measured binary retained 1,994,000,654 requested
heap bytes. The leading allocations were the residency directory (406.85 MB),
page tables (232.19 MB), block identities (216.71 MB plus strings), resident entries
(201.23 MB), catalog records (123.83 MB) and PageState (108.35 MB). Profiler RSS is
not the normal-process RSS. The next work consolidates resident representations;
the 1,000,000,000-byte target remains unmet and read performance is still pending.

The eighth layer removes the resident bitmap. Page slots distinguish resident
pages, including zero-byte allocations, from lifecycle or policy-only entries;
public metadata reconstructs the bitmap on demand. Per-incarnation charged bytes
and page counts survive directory detachment, even if an old policy/lifecycle
owner subsequently removes shared slots before retirement finishes. Tests cover
that deterministic interleaving and pending/zero-byte transitions. Validation
passed 662 Worker tests (8 ignored) and Clippy; RSS has not yet been measured.

The ninth layer stores directory records in a dense array. Hash buckets retain
only the canonical identity and a slot number. Slot references never escape the
directory lock; external resident and lifecycle owners retain their original
Arcs, so free-slot reuse does not change generation identity. Removal and recovery
reuse cleared slots. Capacity follows the historical peak, as with the previous
HashMap. Validation passed 663 Worker tests (8 ignored), Clippy and independent
correctness/cleaness reviews. This combined binary still needs an RSS measurement.

The tenth layer skips block maintenance records, both block queues and ordinary
block clocks when TTI, stale GC and ownership GC are all disabled. Capacity pins,
second-chance bits and cross-version eviction remain active. Small-object access
clocks remain available, including a lazy clock when an existing whole unit changes
storage policy. Enabled maintenance configurations keep their original behavior.
Validation passed 665 Worker tests (8 ignored), Clippy and a release build.

Two warm runs through this layer recovered in 9.249 s and 9.379 s, with ready RSS
1,851,793,408 and 1,852,964,864 bytes. Startup high-water RSS was 2,002,546,688 and
2,003,795,968 bytes. Ready latency was 14.285 s and 14.433 s; those measurements
include registration after recovery. All 1,934,887 blocks/pages remained present,
zero evictions occurred, and 17,123 content checks passed. Original services were
restored. Binary SHA256:
`0788aaaca95fe986fe6b57934ffd69e813f4e8490d4e7c4feb506dd751a158b2`.
The 1 GB target is still unmet; read performance has not been compared.
