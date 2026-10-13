# Cache ownership cleanup in stale GC

Workers can reclaim clean local cache blocks outside their placement candidates.
This extends `DESIGN.md` section 3 (Worker storage). Ownership is a predicate in
the existing `StaleGc` block scan, alongside backend deletion. Both predicates
share one cursor, pending queue, round boundary, scheduler registration and
whole-block deletion transaction. No separate service or block index is added.
Origin objects, staging and WAL data are outside its scope.

## Configuration

```toml
ownership_gc_interval_secs = 3600
ownership_gc_replicas = 1
ownership_gc_scan_batch_size = 1024
ownership_gc_delete_batch_size = 1024
```

The interval defaults to `0`, disabling ownership cleanup. When enabled, it sets
the **shared stale-GC round interval**, including backend checks if enabled.
Without ownership cleanup, backend rounds retain their one-hour interval.
The first round starts immediately; subsequent rounds wait the interval after
the previous round finishes. Continuation batches wait one second and release
the scheduler slot. There is no nested policy scheduler.

`stale_gc_enabled` independently enables backend-deletion checks. Disabling it
leaves ownership cleanup available; disabling both removes the task. The scan
and deletion settings above cap blocks scanned and block candidates per shared
batch. They are further capped by the page-GC and worker-wide background limits,
and scans never exceed 4096 blocks. A paged block counts as one candidate;
**every resident page still consumes deletion-rate credit** before reclamation.
All fields support uppercase `TALON_WORKER_` environment variables, overriding
TOML. Changes require restart; batch sizes and replica count must be positive.

Set `ownership_gc_replicas` to at least the largest candidate count used by any
reader or load planner. The Worker retains the union of global and local-zone
top-k candidates, conservatively supporting mixed global and zone-affine readers.
There is no automatic replica-count negotiation. Explicit prewarming outside
these candidates is subject to cleanup.

## Scan and membership

Each batch obtains fresh `MembershipQuery` data from the Coordinator. Placement
uses retained logical Workers, including offline and conflicted instances, and
excludes retired members. Response ordering, addresses and instance availability
do not affect placement. There is no additional Worker stability timer.

A membership change between rounds resets the ownership watermark. A change
during a round, invalid membership, missing local Worker, or query failure pauses
ownership for the rest of that round and resets its watermark. Backend checks
can continue. The next round requires another successful membership query;
last-good membership never authorizes deletion after a failed query. Coordinator
upgrade must precede enabling cleanup: retained membership queries must also be
allowed on its authenticated Worker listener. No wire schema changes are needed.

The shared scanner checks ownership before applying backend idle and HEAD
filters. Misplaced blocks do not require 24-hour idleness or a HEAD. Remaining
blocks need more than 24 hours of idleness and authoritative backend `NotFound`,
with the existing positive-check cooldown and bounded HEAD work.

Ownership remembers a scalar checked-through watermark in the existing
admission-ordered block queue. With unchanged membership, already checked owned
blocks need no placement computation. Deferred misplaced blocks move beyond the
frozen round boundary in that same queue; new blocks already enter it on admission.
No ownership bookkeeping is added to cache hits or page fills. A full round has a
finite admission boundary, so concurrent fills and protected blocks cannot keep
it alive indefinitely.

With backend checks disabled, stable membership and no new/deferred blocks need
O(1) local scan work. With backend checks enabled, the existing cold-block scan
still runs, skipping ownership calculations before its watermark. Membership
transfer and canonicalization cost O(N log N); placement tables rebuild only on
membership changes. Deferring a block costs O(log B), reusing existing O(B)
block metadata. The task does not clone the complete resident index or scan disk.

## Reclamation and protection

Both reasons use the same whole-block mutation. Before deleting, it rechecks
admission revision, block activity and pins under the mutation gate, then claims
all resident pages. Any active reader defers the entire block. Successful removal
retires all files, L1 entries, lifecycle metadata, index entries and accounting.
Failed deletion retains conservative accounting and releases page claims. Failed
ownership cleanup waits for fresh membership next round; TTI does not replay it.

Ownership charges deletion credits before final authorization. If rate waiting
exhausts the fresh snapshot's authorization window, it queries membership again
and requires the same placement. This lets blocks larger than one second's rate
budget make progress without deleting on an old observation. Subsequent I/O,
page-GC semaphore and block-gate waits have an authorization deadline of at most
one second from query start, capped by the membership response validity;
expiry defers the block. The mutation rechecks the deadline before unlinking.
An already-started unlink completes its owned accounting transaction even if its
caller is cancelled or the deadline subsequently expires.

Polling cannot detect every membership transition, and membership can change
immediately after an observation. Cleanup is a cache policy, not a distributed
ownership transaction, and needs no peer-copy confirmation because the origin
retains the data. Logical retirement does not add a separate grace period.

## Observation and validation

`talon_worker_ownership_gc_checked_total` counts ownership block checks;
`talon_worker_ownership_gc_attempted_total` counts selected cache units.
Paged reclamation uses `reason="ownership"` in existing page/byte counters;
whole-file removal uses the general eviction counter. Scheduler task `stale_gc`
counts batches. Shutdown uses the existing maintenance and owned-mutation drain.

Regression tests cover shared scans, bounded round continuation, stable ownership
watermarks, new admissions, membership changes/failures, independent backend
checks, top-k/zone retention, whole/paged cleanup, whole-block reader/pin protection,
rate-wait reauthorization, and expiration behind disk permits or mutation gates.
The existing stale-block tests cover concurrent reads/fills, unlink failures,
cancellation, HEAD cooldown and request bounds. These are local correctness
checks, not production throughput or latency acceptance.
