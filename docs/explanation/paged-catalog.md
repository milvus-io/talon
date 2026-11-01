# Paged cache catalog

Namespace-backed Workers persist paged residency in a cache-root catalog.
The `.paged-catalog` name is reserved and cannot be used as a namespace.
The catalog is a business adapter over the common `talon-journal` engine; see
[the shared cache journal contract](cache-journal.md) for service ownership,
framing, durability and checkpoint publication.
Workers do not write one `block.meta` per block. Normal paged recovery reads a snapshot
and the subsequent journal, without listing block/page directories or statting
every page. Whole-block caching and access-time checkpoints remain separate.

This is a new cache format. Existing paged directories without a registered
catalog are rejected; use a new empty cache root. There is no legacy migration
or automatic deletion of the old cache. An older Worker cannot safely write a
catalog-backed cache root. Standalone low-level store fixtures still support
the sidecar layout; the Worker namespace startup path always uses the catalog.

## Layout and ownership

```text
<cache-root>/
  .worker.lock
  .paged-catalog/
    CURRENT
    <snapshot files selected by CURRENT>
    <log files selected by CURRENT>
  <namespace>/shared/paged/
    ab/
      access.shard
      <digest>.pages/
        0.page
        1.page
        <page>.page.catalog.<transaction>   # only while a put is incomplete
```

Credential scopes use `<namespace>/scopes/<scope>/paged` and share the same
writer. Every record identifies its store, so equal BlockIds in different
namespaces/scopes remain isolated. BlockId includes the source version.

`NamespaceRuntimes` creates one `JournalService` for the cache root. The paged
catalog and each small-store namespace/scope open independent typed streams on
that same service. The service owns the dedicated runtime/ring and the root
lease; neither adapter starts another journal thread. Stream state, checkpoints,
LSNs and failure status remain independent. Small objects are not entered into
the paged catalog.

The shared engine bounds queued and in-flight requests by both count and encoded
bytes, reserving admission before encoding. Paged business admission separately
bounds 1,024 transactions across Begin, data I/O and Complete. All paged log
records request `Ack::Durable`; success follows the covering fsync. Ordered queries
also cross a durable barrier before reading the state machine. Native io_uring
is preferred, with explicit independent-thread legacy compatibility where
required by the host. Native validation sets `require_native`.

Existing page/block mutation gates still protect data and index updates. The
writer only orders journal records; it does not replace those gates. Disjoint
warm-fill pages may proceed concurrently. Overlapping pending operations for
the same page, or a block deletion overlapping any page operation, are rejected.

## Publication protocol

Each Begin record's LSN is its transaction ID and gives its staging file a unique
name. The runtime owns the entire mutation even if the requesting client leaves.

Page put:

1. Persist Begin with block identity, logical length and page index.
2. Create the shard/block directories durably. Write the named staging file,
   fsync it, and measure its actual allocation (`st_blocks * 512`).
3. Rename to the final page path and fsync its directory.
4. Persist Complete with transaction ID and allocated bytes.
5. Publish the in-memory bitmap, capacity, eviction and lifecycle entries.

Page deletion persists Begin, unlinks and synchronizes the directory, then
persists Complete before releasing its capacity accounting. Block deletion
uses the page list from catalog memory and a nonrecursive directory removal;
it never discovers page names by listing the directory. Unexpected files cause
an error rather than recursive removal.

Prepare/Commit names here correspond to the encoded `Begin`/`Complete` records.
Store registration records page geometry. The first completed put creates the
block record; removal of the last page retires that record. Snapshot block
records contain identity, logical length and page-index-to-allocation mappings.

If a data mutation or journal operation fails, further catalog mutations fail
until restart. This conservative boundary prevents reuse of a path with an
unresolved operation. Already cached reads can still proceed; writes that need
cache admission return the error. The cache-root lease outlives accepted work.

## Recovery and checkpointing

The shared engine's `CURRENT` explicitly selects the authoritative snapshot and
all logs required for recovery. Their discovery never lists the catalog or data
directories. Common framing protects version, lengths, LSN/ordinal and payload;
the adapter owns its JSON record schema. Complete checksum, schema, geometry or
LSN failures stop recovery. Only an incomplete physical final-log tail can be
truncated. Snapshot ordinal is separate from the journal LSN: pending transaction
IDs are explicit fields of snapshot records.

Recovery loads the snapshot, replays consecutive journal LSNs, then reconciles
unfinished intents before serving traffic:

- An unfinished put discards its exact staging path and final page, including
  an older page if this was a replacement. The page is removed from recovered
  residency; it can subsequently be fetched from origin again.
- An unfinished delete retries the recorded page paths. Capacity is not made
  available until this cleanup succeeds.
- Each cleaned intent receives a durable Resolve record, then the reconciled state
  is checkpointed before any path can be reused. Failure
  stops startup, so stale intents cannot later delete newly admitted pages.

Recovery also tolerates interruption during this cleanup: the previous catalog
still contains the intents, and the filesystem operations are idempotent. It
does not require a separate persistent page generation because all old intents
are resolved before new mutations are admitted, and concurrent mutations obey
the same-key gates. Transaction IDs are never reused after a checkpoint.

The shared engine checkpoints after its configured log-byte threshold and at
orderly Worker background shutdown. Before acknowledging writes into a new log,
it publishes a recovery set containing the old snapshot, old log and new log.
The adapter freezes block references and pending intents at that boundary;
subsequent block mutations use copy-on-write. A CPU thread incrementally encodes
snapshot records through a bounded channel while the ring continues other work.
Freezing still builds an O(live blocks + pending intents) reference vector on the
shared I/O thread. This avoids a deep metadata copy but can briefly pause other
streams; it is not a claim of complete CPU isolation during checkpoint setup.
The engine publishes the new snapshot and tail set only after synchronization,
then deletes the exact obsolete paths recorded for cleanup. No business adapter
implements a second checkpoint protocol.

Recovery overlaps buffered frame reads with parallel checksum and payload decode,
while applying decoded records in file order. Independent page bitmaps and index
entries are built in parallel before the new index is published to readers.
Startup holds exclusive index ownership, avoiding per-entry directory-lock
contention. The index stage uses the shared journal default CPU budget. It
respects available parallelism and defaults to at most 4 CPUs (including the
replay caller); explicit journal options can override the default. Small inputs
remain serial. Encoded replay bytes and pending batches
are bounded. State application, final index membership insertion, namespace
initialization and access-time restoration still have ordered work, so this is
not a claim that every startup stage saturates all CPUs.

The writer retains catalog metadata alongside the runtime index. Startup still
discovers namespaces/scopes and restores whole-block caches and access-time
shards. Thus the guarantee concerns paged block/page enumeration, not zero
filesystem operations or zero startup work. Access timestamps remain sampled
in `access.shard`; ordinary read hits do not write residency log records.

Background file cleanup remains available for empty directories and unrelated
old temporary files. External filesystem edits/corruption are outside the
journal transaction protocol; on-read failures still require origin recovery.
Data-file allocation is tracked, while catalog/filesystem overhead requires
operational disk headroom. ENOSPC does not turn a failed mutation into success.

## Validation

The shared crate is introduced once in the small-store stack, and both adapters
use that implementation. The integrated Worker and its full-object startup
measurements are described in
[combined startup validation](cache-journal.md#combined-startup-validation).

The following results describe the earlier, separate catalog-adapter validation.
The frozen r2
archive SHA-256 is
`10d3dad68a752090d22bbefd083987eff947dd306b57f19f72211955996b7c0f`;
all eight crate source files matched its validation manifest. The later shared
r3 added the synchronous-bridge cooperative-budget fix used by the integrated
Worker. These historical r2 results do not substitute for the combined tests.

Adapter regressions cover exact residency/allocation replay without access to the
data tree, concurrent namespace writes, checkpoint plus incremental replay,
pending puts/deletes, replacement after recovery, frozen snapshot consistency,
explicit pending IDs, and isolation from another stream's failure. Namespace
tests cover restart, capacity/ownership eviction and rejection of legacy caches.
The shared crate owns framing, torn-tail, fsync acknowledgement, cancellation,
checkpoint crash-boundary and root-lease tests.

The frozen shared-engine r2 integration passed 525 Worker library tests and 21 main
program tests in a container requiring native io_uring; four manual tests were
ignored. The common crate passed its 15 tests in the same container, and
Worker library/main Clippy passed with warnings denied. Two focused core tests
covered the new namespace reservation. These are local tests,
not a claim that the paged and small-store feature branches were merged and run
as one Worker binary.

The successful Worker checks targeted the library and server binary. A separate
`cargo check --all-targets` stopped at the existing routing integration test's
`env!("CARGO_BIN_EXE_talon-worker")`, which that command did not provide; the
routing integration suite was not executed in this validation.

The release `catalog::benchmark::restart_replay_and_index` benchmark exercises
service startup, shared replay, the real paged state machine and index building.
Each mode runs in three fresh processes against a warm 100,000-block snapshot
with 32 pages per block. The first r1 measurement is retained in
`bench/results/catalog-validation/shared-restart-100k.log`:

| CPU budget | Median replay plus index |
| --- | --- |
| 1 | 1.040 s |
| 4 | 0.568 s |
| 8 | 0.758 s |
| 16 | 1.149 s |

After both worktrees' builds finished, the frozen r2 was measured with 500,000
blocks and the same three-process method. All twelve runs restored exactly
500,000 blocks and 16,000,000 pages. Results are retained in
`bench/results/catalog-validation/shared-r2-restart-500k.log` and its summary JSON.

| CPU budget | Median replay plus index | Mean CPU cores, median run metric |
| --- | --- | --- |
| 1 | 4.716 s | 1.00 |
| 4 | 2.052 s | 2.70 |
| 8 | 1.708 s | 3.90 |
| 16 | 1.887 s | 5.38 |

The default four-CPU budget was about 2.30 times faster than serial for this
larger sample. Eight was faster still here, while four was better in the earlier
small sample; the default remains conservative and can be overridden explicitly.

The shared host was not isolated. Four was a conservative default on these
measurements; more threads are not assumed to improve latency. These are
metadata-path diagnostics, not cold-disk or end-to-end Worker startup claims.
The earlier private-engine benchmark is retained separately under
`bench/results/catalog-validation/parallel-recovery-500k.log`; its format and
measured scope differ, so it is not a direct shared-engine performance baseline.
