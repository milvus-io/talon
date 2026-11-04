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
