# Packed small-object cache

## Goal and scope

Cache objects of a few KiB without a data file, JSON sidecar, directory, and
filesystem allocation unit per object. Preserve versioned reads, namespace and
credential isolation, existing placement, and fd-based sendfile/splice responses.
The origin remains the durability point. No write-back behavior is introduced.

The implementation is one local stack layer, `small-object/segment-store`, based
on `main`: engine, worker integration, tests, and this design form one feature.
Payload truncation, hole punching, compression, and a separate region hierarchy
are out of scope. The v2 metadata format replaces per-segment indexes with a
namespace log and recoverable snapshots through the shared `talon-journal` crate.
See [the shared cache journal contract](cache-journal.md) for the catalog boundary.

## Configuration and routing

```toml
small_object_threshold_bytes = 1048576
small_object_segment_bytes = 67108864
```

The equivalent environment variables are
`TALON_WORKER_SMALL_OBJECT_THRESHOLD_BYTES` and
`TALON_WORKER_SMALL_OBJECT_SEGMENT_BYTES`. The engine is enabled by default for
objects smaller than 1 MiB; exactly 1 MiB retains ordinary storage. Set the
threshold to `0` to disable it. Segment size defaults to 64 MiB. An enabled
engine requires capacity for four segments (256 MiB with the default segment
size), and the LOAD payload budget must accommodate an entire eligible object.

An eligible object has positive total size strictly below the threshold and
fits in a short first block. Empty objects and full blocks retain existing
storage. A small range of a large object, or its short final block, does not
qualify. READ and LOAD fetch an eligible object completely with the source
version precondition; short responses are rejected before publication. Same-key
fills coalesce. LOAD never populates L1.

`WorkerRuntime` owns `WholeBlockStore`, optional `PagedBlockStore`, and optional
`SmallObjectStore` as peer stores. Whole-block storage has no small-object engine,
routing, or maintenance responsibilities. Runtime initialization validates the
cache-root routing metadata before recovery, passes the shared root lease to
each store, and binds recovered small-object access tokens to the runtime LRU.
The runtime coordinates store selection, capacity accounting, GC, and shutdown
checkpoints.

`SmallObjectStore` owns its residency directory. READ and LOAD choose the store
from the complete object size; a packed hit resolves its segment extent and
access token without consulting `BlockIndex`. Ordinary whole and paged entries
remain in `BlockIndex`. Small objects register directly with the shared LRU as
whole-object eviction units, retaining capacity, idle, and ownership policies
without a second residency entry. The locator carries the shared access token;
relocation preserves that token and recovery reconstructs policy from locators.
`GetCachedRange` remains an ordinary whole/paged-store operation: an object
resident only in small storage returns `CacheMiss`, without origin access.
Packed keys do not participate in cross-version supersession. Page TTI applies
to the whole small object using its shared access timestamp, including when
paged L2 is disabled. With TTI enabled, reads update memory and coalesce dirty
notifications per resident token. Tokens share the store's encoded key and do
not pin payloads. The existing access-checkpoint interval flushes only dirty
keys to `small/access.log`; graceful shutdown performs the same flush. A clean
cycle does not traverse the resident index or write either access file.

`small/access.checkpoint` is a full base snapshot. The first flush, migration
from `TLSA0001`, or a log reaching `max(1 MiB, base snapshot size / 2)` rebuilds it
from current residents. This remains O(N) CPU, memory and sequential writes,
but is removed from the normal periodic flush. Recovery also requests a base
checkpoint when a resident key has no saved age, or an age needed clock repair,
so repeated restarts do not keep granting that key a fresh idle window.
Deleted identities disappear
at compaction; until then recovery ignores them. Each delta contains the full
encoded BlockId and a Unix-millisecond access time, with length and checksum
framing (20 bytes plus the encoded key). There is no persistent item-ID map.

The pending queue is capped at 262,144 tokens and an estimated 64 MiB of retained
metadata; overflow requests a full checkpoint. It is keyed by token identity,
so a late reader cannot replace a new admission's notification for the same key.
A flush resets markers before sampling. Concurrent hits remain pending, and
write failures request a full retry from live clocks. Consequently, high-cardinality
traffic, overflow or repeated I/O failure can still incur full checkpoint cost.
A flush can hold one detached pending batch and the next collecting batch.

Snapshot publication uses temporary-file sync, rename and directory sync, then
publishes an empty log for the same generation. A crash between these steps
leaves a valid new snapshot and an old-generation log, which recovery ignores.
Normal appends use one file sync per batch and share the background disk budget;
accepted flushes finish even if their caller is cancelled. The format uses
`TLSA0002` for snapshots and `TLSL0001` for logs. Older readers reject the new
advisory snapshot and start fresh idle windows; they cannot consume its deltas.

Recovery validates the base and log before applying ages, only to objects
within recovered durable segment prefixes, including relocated objects. A torn
final log frame retains the valid prefix and requests compaction before the
next append. Checksum/format corruption grants fresh idle intervals with a
warning; access metadata never establishes residency or makes payload recovery
fail. Future timestamps are clamped to startup time after clock rollback.
Downtime counts toward idle age, and a crash can lose unflushed accesses, so
expiry remains approximate. Ordinary whole admissions preserve paged residency during active fills;
conflicting small and ordinary admissions are rejected under the block gate.

Each namespace and credential scope owns its own store under its existing cache
root. `small-routing.json` binds the routing format and small-object threshold
to that root. A changed threshold (including enabling or disabling small storage),
malformed marker, or an existing cache without the marker is incompatible.
By default, `reset_incompatible_cache = true` clears the active Worker's cached
data and metadata, preserves its Worker ID, and retries startup once. The
identity remains durable throughout cleanup, including interrupted resets.
An explicit `node_id` that differs from the persisted identity aborts startup
without deleting data; an operator must resolve the mismatch. Invalid identities
and cluster mismatches also require operator intervention.
Set reset to `false` to reject incompatible cache state without deleting it.
When upgrading from the disabled default, explicitly
set `small_object_threshold_bytes = 0` to retain the previous routing and avoid
a reset caused by the new threshold. This prevents read-time probing for
historical routing choices. Do not run two owners against the same cache root;
use a fresh cache directory when rolling back to a binary with older routing.
Legacy `MANIFEST`, `ring-*`, and per-segment index/data layouts are also
rejected and follow the same reset policy.

## Files and identity

```text
small/
  access.checkpoint
  access.log
  metadata/
    CURRENT
    snapshot.00000000000000000042
    journal.00000000000000000042
  data/
    00/00000000000000000256.data
    01/00000000000000000257.data
    ...
```

Each segment is one raw payload file. Namespace-wide monotonic IDs select one of
256 shards using `id % 256`; shard directories are created only when used. Rings
retain independent active/spare segments but do not own separate directories or
index files. Payload files are created exclusively and never overwritten.

The metadata writer owns checksummed, versioned log records: insert (stream,
segment, offset, length, checksum, full identity), delete (segment, offset), flush
(segment, durable end), seal, recovery truncation, and atomic replacement of an
old segment with a new one. Metadata is kept separate
from payloads; it is not a redo log containing origin data. Identity includes
backend, bucket, path, version, block offset and block size, compared in full.
Sendfile/splice still use retained payload descriptors and explicit byte ranges.

`metadata/CURRENT` selects a checksummed snapshot and an explicit ordered set of
log generations to replay. It also records exact pending cleanup paths.
The snapshot stores the current segment table, durable ends and live-object mappings,
including uncommitted tails needed by later flush/seal records. Startup reads that
snapshot and replays the remaining logs in order. A durable flush makes an active
segment's prefix recoverable without closing it to further appends. Startup first
validates every retained payload range and all recovered identities. It then trims
uncommitted tails, syncs those files, and durably records recovery of each prefix.
Recovered prefixes are closed to further appends; new writes use a fresh active
segment. Interruption before the recovery record repeats the same trim; interruption
after it recovers the finalized prefix. Segments with no durable prefix are discarded.
Unreferenced payloads and obsolete metadata files are removed after validation.
It never scans payload contents or stats individual objects to build the index.
A partial final log frame is discarded because it cannot have been acknowledged;
checksum corruption, gaps in log generations, corrupt snapshots, and missing
committed data fail recovery closed. Removing a partial WAL tail is recovery
repair. Payload tail truncation also occurs only during exclusive startup recovery,
before any readers or writers can retain that file.

Snapshots written before durable-prefix support remain readable: sealed segments
derive their durable end from their length, and unsealed segments have no durable
prefix. Older binaries cannot read the new flush/recovery records or snapshots;
downgrades require a new empty cache root.

The engine and metadata writer both retain metadata proportional to resident
objects. Keys are compact binary identities, but the writer's checkpoint state
is additional to the engine lookup and shared LRU policy. Small objects do not
allocate generic BlockIndex entries or paged metadata. Encoded identity
buffers are shared between the small-store writer state and engine locators. Recovery still
requires O(live objects + log records since snapshot) CPU/memory work; this is
not constant-time startup or a disk-only index.

Before serving, the worker attaches access tokens directly to ordinary index
entries and small-store locators, then bulk-builds their shared LRU membership
and block indexes. Only ordinary
whole/paged entries enter the cross-version index; packed keys are independent.
The object-existence check table is built only when stale GC is enabled. Large namespaces use at most three CPU threads (bounded by available
parallelism, including the initializing thread); small namespaces stay serial.
Ordered eviction and ownership queues are collected in bulk. Paged lifecycle
initialization skips whole objects without cloning a full cache snapshot. These
steps preserve resident byte charges, block-wide idle clocks, and eviction
semantics. Persisted small-object access ages are restored after policy tokens
are attached.

Small-store startup constructs locators while validating each recoverable payload's
length. It reserves each ring's key directory from the recovered record counts
and bulk-collects segment offset tables. Cross-ring uniqueness uses a temporary
vector of fingerprints and borrowed key bytes collected during that same pass;
sorting compares the full bytes on fingerprint collisions. No separate global
key HashSet is built. All validation finishes before orphan payload cleanup or
abandoned-segment resolution, and no runtime is published on failure.

The LRU's block membership and its idle/ownership scan queues share an immutable
identity from an existing cache unit, instead of copying its path and version
into each structure. Removing that particular page leaves the identity alive
until the last sibling is removed; it retains metadata only, not a data pin.
Recovery reuses the first unit's access token and gives later pages independent
second-chance bits sharing the same block idle clock.

## I/O and publication

The Worker creates one `talon-journal::JournalService` per cache root, with a
dedicated Monoio thread named `cache-journal`. Each namespace/credential scope
opens an independent small-store stream on that service; that scope's whole/paged
catalog uses a separate stream on the same service. No private small-store journal engine
remains. Data rings send metadata requests through a bounded MPSC queue;
Tokio maintenance callers use the same service. Each stream owns its log descriptor,
file offsets, replay order, and fsync completion notifications. Payload I/O remains
on the originating ring, with the existing blocking-pool fallback for Tokio
payload callers. Runtime log/snapshot filesystem I/O uses native asynchronous
interfaces. Native io_uring is preferred, with an explicit Monoio legacy-driver
fallback on the dedicated thread when unavailable; native benchmarks require
the native driver. Startup waits for bounded parallel log/snapshot decode and
ordered replay before serving. Business payload reconciliation remains in the
small-store adapter.

Admission budgets cover queued and in-flight metadata requests: 1,024 requests
and 8 MiB of encoded request bytes across the service, reserved before encoding and retained until
completion. Each stream submits one log batch at a time, targeting 256 KiB and
at most 256 requests (a final record can exceed the
byte target). The writer drains immediately available requests without a timer
that delays low-load writes. Queue saturation applies asynchronous backpressure.
Each stream retains independent state, checkpoint progress and failure isolation.
Caller cancellation does not cancel accepted mutations or return their permits
early. Shutdown/root leases retain accepted I/O; last-drop does not join the I/O
thread on a serving reactor. Startup waits for retiring services before taking
the cache-root lock; a live owner is rejected.

Sealed segments retain no idle payload descriptors. Reads asynchronously
open data files on demand and share a weakly cached descriptor with overlapping
readers. Active, standby and sealing segments retain their writer descriptors;
sealing releases them, and recovery closes each segment's payload file after reconciliation.
Thus descriptor residency follows concurrent reads and bounded writer pipelines,
rather than the number of live sealed segments. `SmallObjectStore::get` is async.
LOAD residency checks and L1 hits use only the in-memory directory.

A read takes an asynchronous directory lease until its fd is retained. Compaction
excludes new leases while replacing locators and unlinking files, so a pending
open cannot race unlink. Returned handles retain the old inode independently.
Packed byte reads still verify the complete object's checksum; promoted L1 pages
each own only their charged bytes, even after sibling pages are evicted.

Each data stream still permits up to 32 concurrent appends. Append reserves its
data offset and estimated record charge under a short stream lock, then submits
payload and metadata writes independently. Both completions are awaited before
publishing the locator; ordinary insertion does not require fsync. A prepared
spare lets rollover switch files while the owned pipeline prepares another spare
and seals the previous segment. The pipeline bounds unsealed data to an active
and a sealing segment per stream, rather than accumulating an unlimited flush
queue. Any failed append poisons its stream; a failed metadata writer rejects
further metadata mutations across its namespace.

The log writer pipelines writes to separate offsets. It advances its contiguous
write-completion boundary in log order. Durable requests wait for group fsync,
started only after all writes through the covered boundary have completed;
submitting earlier io_uring operations alone is not sufficient ordering. Later
batches may be written while fsync is pending. This removes producer-side log
file contention but does not eliminate queueing, CPU scheduling, or shared-disk
contention.

Seal waits for a data segment's outstanding appends, syncs its payload and shard
ancestry, then submits a durable seal record. The log writer never waits for a
data ring to fsync: it receives the seal request only after that work finishes.
Different data streams can become recoverable independently. Segments seal when
full, when pressure reclamation requires it, or during graceful shutdown.

At a maintenance tick, an active segment with dirty data is flushed after 30 seconds
since its stream's last flush or after accumulating 8 MiB beyond its durable end.
This timing is subject to maintenance scheduling and I/O budgets. Flush captures the
reservation end, waits only for outstanding appends before that cut, syncs payload
and shard ancestry, then commits a durable flush record. Appends beyond the cut can
continue while it waits or syncs. Per-stream tracking is bounded by the existing
32-append admission limit. No per-object fsync is added, and an idle flushed segment
remains open for later appends instead of rotating an underfilled file.

An abrupt exit can lose the suffix after the last durable flush; the origin remains
the durability source. Deleting an object within a durable prefix waits for its log
tombstone to be synced before removing the locator, even if the segment is still
active. Per-identity guards retain cross-ring write/delete ordering; per-stream
publication gates order flush, seal, deletion and GC. Flush failures stop further
mutations on that data stream without invalidating its previously durable prefix.

## Snapshots and log reclamation

At 64 MiB of log growth, or an explicit maintenance/shutdown checkpoint, the
stream drains preceding writes and syncs them, creates the next log generation,
and publishes CURRENT with the old snapshot and both log generations before
acknowledging any append in the new log. It captures metadata at that boundary.
Segment metadata maps use copy-on-write while a separate CPU thread encodes the
frozen state into a bounded chunk channel, without a full encoded snapshot
buffer. One snapshot can be in progress per stream; the shared service permits
at most two encoders/writers at once while other streams keep appending. Its asynchronous
write and fsync overlap appends to the new log; snapshot encoding does not run on
the log reactor. The first mutation of a shared segment map may copy that map,
so snapshotting still has CPU/memory cost.

Publication writes and syncs the snapshot, syncs the metadata directory, atomically
replaces `metadata/CURRENT` with the new snapshot and tail set, and syncs its
directory. Exact obsolete paths remain recorded until cleanup completes. Only then may old
snapshots and pre-boundary logs be deleted. A crash before publication uses the
old snapshot plus retained logs; a crash afterwards uses the new snapshot plus
its tail. An incomplete new snapshot is never selected. Unlike data-segment GC,
this independently removes historical metadata without copying live payloads.

Snapshot writes/deletions use a crate-independent maintenance hook to the
worker's configured background byte, I/O and
delete budgets after background initialization. Foreground log writes and
commit fsyncs do not consume that background budget. Checkpoint publication
failure stops that metadata stream rather than proceeding with uncertain
recovery state; other streams remain independently usable. The TTI snapshot and access log remain separate advisory metadata and never
establish object residency.

## Reclamation and capacity

Logical eviction decreases live record bytes; it does not pretend to release
physical disk space. A maintenance call processes at most one eligible segment per writer stream:

1. An empty segment is durably retired in the metadata log and unlinked.
2. A sealed segment with less than half its logical record bytes live is relocated.
3. Relocation writes a new payload segment and logs its object mappings, syncs
   payload and directory ancestry, durably commits the segment replacement,
   updates locators, and then unlinks the old payload file.

An interruption before replacement commit leaves an orphan replacement. An
interruption afterwards leaves an orphan old file. Recovery applies the atomic
replacement record and removes unreferenced engine files; it never merges both
copies.
The collector enumerates only the selected segment's live records, not the
entire object directory. Background relocation pays the shared byte budget
before acquiring the mutation lock and rechecks the selected segment afterward.

No published payload extent is overwritten, truncated, punched, or reused.
An in-flight response retains its old inode across replacement/unlink. This is
necessary because sendfile completion does not imply peer consumption. Keeping
an fd alone would not protect against an in-place truncate or overwrite.

Worker capacity accounting adds measured small payload, log, snapshot, CURRENT
and access-checkpoint allocations to ordinary `BlockIndex` allocations, including
dead bytes and retired payload files retained by open handles. Directory blocks
are excluded, matching existing file-allocation accounting. Small policy charges
are not added a second time. Like the whole-file store, admission writes and publishes the object
before best-effort capacity eviction. Capacity is a soft target: concurrent
admissions, relocation copies, and retired files retained by readers can
transiently exceed it. Crossing that target does not reject an admission;
actual filesystem I/O errors still propagate. Filesystem free space must
accommodate those temporary allocations, normal large-object writes, and
filesystem overhead; this cache budget is not a filesystem-wide reservation.
Under pressure, small objects, ordinary blocks, and pages share each namespace's
second-chance eviction queue. Segment placement does not determine which objects
are cold. Capacity reclamation has two phases:

1. Select victims in bounded batches. Whole blocks/pages release their files;
   small objects append tombstones. Carry the small-store reclaim estimate across
   batches instead of repeatedly charging the same physical excess. The estimate
   uses live payload lengths rounded to filesystem allocation units, plus retired
   payload files awaiting their last reader. Global metadata remains charged and
   is reclaimed independently by checkpointing. Only segments
   exceeding the shared 50% garbage threshold (by logical payload plus metadata
   record charges) count
   toward this estimate. Below-threshold garbage provides no reclaim credit;
   continue selecting items until enough eligible segments exist.
2. Once the estimate covers the remaining excess (or a bounded no-progress walk
   spanning scheduler turns completes),
   stop selecting objects and reclaim the dirty segments. Empty segments go first;
   pressure relocation prefers more garbage but never bypasses the routine
   50% garbage threshold. Seal eligible active segments before relocation. Each
   maintenance step processes at most one segment per stream, retaining this
   phase across scheduler turns. Actual allocated bytes, not the estimate,
   determine when the low target is reached. If remaining items are pinned, keep
   the threshold and retry later rather than forcing a mostly-live rewrite.
   Reads that renew second chances between every scan batch cannot indefinitely
   defer existing eligible garbage or idle-segment maintenance. Successful
   victim batches reset the no-progress budget, preserving batched relocation.

This can evict more logical item bytes than the immediate physical deficit,
especially when cold items are scattered across segments. That tradeoff prevents
rewriting a nearly-full 64 MiB segment just to reclaim one 1 KiB item.

Routine small-store GC defers relocation during a capacity cycle, so it cannot
copy survivors halfway through victim selection. Foreground capacity enforcement
also finishes selection before relocation, using the same per-namespace gate.
With stable admissions and readers, one round batches same-segment victims into
one rewrite; concurrent admissions, failed deletions, or filesystem allocation
estimation differences can require another round. Relocation pays for live
payload read/write and relocation log records, rather than dead payload bytes.

Already-unlinked reader-held files remain charged; when they cover the remaining
excess, eviction waits for those descriptors instead of deleting more objects.
Compaction still needs temporary free space for the live-data copy: reaching the
cache watermark must happen before filesystem ENOSPC. Namespace queues remain
independent; the worker evaluates watermarks against their combined allocations
and credits pending reclamation in other namespaces before selecting more victims.

## Validation

Regression coverage includes queue count/byte permits retained after dequeue and
cancellation, serialized log batches with cross-stream progress, fsync ordering behind incomplete
writes, snapshot creation overlapping insert/delete/seal, automatic log rotation,
recovery of snapshot entries sealed later, snapshot/current crash boundaries,
truncated final log frames, corrupt committed metadata, old-format rejection
without modification, per-ring append isolation, retained sendfile descriptors,
TTI recovery, and threshold-preserving batched capacity reclamation.

`small-store-probe` remains the reproducible object-layout/recovery fixture. The
ignored `small_layout_probe` compares 10,000 complete 3 KiB objects with ordinary
whole-file storage in a temporary cache. Its allocated-size measure includes
directory blocks, unlike runtime file accounting. These local fixtures do not
establish cold-start, production throughput, or p99.9 latency results for v2.
The separate full-object startup experiment, including its configuration and
TTI coverage limits, is recorded in
[combined startup validation](cache-journal.md#combined-startup-validation).

### Independent keys and recovery policy

Packed objects use their complete key, including version, as independent cache identities.
Admitting a different version does not supersede another packed object, and packed
objects are not members of the Worker's cross-version eviction index. Capacity,
TTI, pinning, ownership cleanup and exact-key invalidation still apply. Ordinary
whole/paged entries retain their existing version policy, including when mixed
with packed objects in the same namespace.

Origin-existence checks remain a separate optional background policy. Namespace
startup constructs their reference-counted check table only when `stale_gc_enabled`
is true; disabling it skips both recovery-time and subsequent admission bookkeeping.
The full key and persisted record formats are unchanged.
