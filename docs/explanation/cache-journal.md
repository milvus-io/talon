# Shared cache journal contract

The paged catalog and packed small-object store share `talon-journal`, a storage
primitive crate with no dependency on `talon-worker` or `talon-core`. The two
implementations agreed this boundary before continuing their journal refactor.
This document specifies the implemented interface. Both adapters are integrated
in the same Worker, extending the cache persistence described in
[the v1 design](../../DESIGN.md).

## Ownership and isolation

One `JournalService` per cache root owns a dedicated Monoio runtime and I/O
thread. It hosts heterogeneous typed streams on that runtime. Each
namespace/credential scope owns a catalog stream for its whole-block and paged
`BlockIndex`, plus a separate small-store stream for packed-object locators.
Catalogs live at `<namespace>/shared/.paged-catalog` or
`<namespace>/scopes/<scope>/.paged-catalog`. Each stream owns its own LSN sequence,
CURRENT, snapshot, log files, state machine and failure state. They share bounded
service admission and the I/O runtime, not object identity, business transactions
or checkpoint state. Catalogs also share a root-wide 1,024-transaction limit
covering Begin, data I/O and Complete.

The service receives `Arc<dyn Send + Sync>` as an opaque lifetime lease. It does
not depend on Worker locking types. Accepted operations and shutdown retain that
lease. Dropping a handle must not join an I/O thread on a serving reactor.
Startup can explicitly wait for retiring services before reacquiring the cache
root lock; a live owner remains an error.

Native io_uring is preferred. `Options.require_native = false` preserves the
Worker's compatibility on hosts without io_uring using Monoio's legacy driver on
this dedicated thread. Tests and performance runs claiming native I/O set it to
true. Legacy mode is reported explicitly and does not move journal work into
Tokio's shared blocking pool.

## Public interface

The following signatures define the integration boundary. Errors use
`std::io::Result`, payloads use `bytes::Bytes`, and `Lsn` is `u64`.

```rust
pub enum Ack { Written, Durable }

pub trait Machine: Send + 'static {
    type Record: Send + 'static;
    type SnapshotRecord: Send + 'static;
    type Snapshot: Iterator<Item = io::Result<Bytes>> + Send + 'static;

    // Exact encoded length or a conservative upper bound; verified after encode.
    fn encoded_len(record: &Self::Record) -> io::Result<usize>;
    fn encode(record: &Self::Record) -> io::Result<Bytes>;
    fn decode(bytes: &[u8]) -> io::Result<Self::Record>;
    fn apply(&mut self, lsn: Lsn, record: Self::Record) -> io::Result<()>;

    // Freeze cheaply; iteration and encoding happen off the I/O thread.
    fn snapshot(&self) -> io::Result<Self::Snapshot>;
    fn decode_snapshot(bytes: &[u8]) -> io::Result<Self::SnapshotRecord>;
    fn restore(&mut self, record: Self::SnapshotRecord) -> io::Result<()>;
}

// Startup-only blocking methods.
JournalService::open(root, keepalive, Options) -> io::Result<Arc<JournalService>>;
JournalService::wait_for_retiring(root) -> io::Result<()>;
service.open_stream(relative_path, machine, StreamOptions) -> io::Result<Handle<M>>;
service.open_stream_with_maintenance(relative_path, machine, StreamOptions, policy)
    -> io::Result<Handle<M>>;
handle.query_blocking(|machine: &M| -> io::Result<R>) -> io::Result<R>;

// Async serving/maintenance methods.
handle.append(record, Ack) -> io::Result<Lsn>;
handle.query(|machine: &M| -> io::Result<R>) -> io::Result<R>;
handle.checkpoint() -> io::Result<()>;
handle.poison();
handle.allocated_bytes() -> u64;
```

Startup synchronous bridges wrap their entire future in
`tokio::task::unconstrained` before `futures::executor::block_on`. A caller
already inside a Tokio task cannot return to its executor to replenish the
cooperative budget while blocked; repeated queries or pending-intent recovery
otherwise spin or stall. This applies to the launch channel and adapter
append/checkpoint bridges too. It does not remove admission limits, ordering,
or durability, and does not change the normal asynchronous request path. These
bridges must not be called from the journal's own callbacks, whose progress
would then depend on the blocked journal thread.

Queries are ordered writer commands. They cross a durable barrier before reading
state, so a failed sync cannot expose provisional state. Apply runs only in the
stream actor, in contiguous log-completion order and before acknowledgements.
Callers must not independently apply records after awaiting append.

Defaults are 1,024 requests and 8 MiB encoded request bytes per service, including
queued and in-flight work. Producers acquire admission before encoding; permits
survive dequeue and caller cancellation through append/query completion.
Checkpoint requests release queue admission when accepted and share one
completion notification per snapshot cut, so waiting for maintenance cannot
consume the admission needed by a permit-holding writer. Additional batch
copies, snapshot chunks, and replay windows have separate explicit bounds.
The I/O scheduler allows unrelated streams to progress during data fsync and
snapshot encoding. CPU work in a machine callback still occupies the shared
thread; global admission saturation still applies backpressure to all streams. Data I/O stays outside the journal.

## Durability and checkpoints

`Written` means the complete log record was written and applied, without a
promise of crash durability. `Durable` additionally waits for group fsync after
all earlier writes through its boundary have completed. Submission order alone
is not a durability barrier. Each stream submits one log batch at a time, so
later writes cannot extend the file past an unfinished earlier batch after
process loss. Batching still combines queued records, and the next batch can
overlap the preceding batch's sync. Other streams and snapshot writes progress
independently.
Checkpoint requests queued during a snapshot wait for a later snapshot cut
while tail writes and queries continue.

The common frame includes version/domain-independent magic, length and its
complement, LSN or snapshot ordinal, and a checksum covering header and payload.
The crate treats payloads as opaque bytes; adapters own their codecs. Complete
corrupt frames, schema failures, and LSN gaps fail closed. Only a physically
incomplete final log frame can be removed. Neither adapter migrates old formats.

CURRENT identifies the snapshot, its LSN and expected length/frame count, and
an explicit bounded list of logs to replay. Rotation creates and syncs the next
log, then durably publishes the old snapshot plus old and new log recovery set
before allowing acknowledgements in the new log. One snapshot per stream can
be in progress, with at most two snapshot encoders/writers active per service.
Streams waiting for a snapshot slot continue accepting log appends. Its frozen state corresponds exactly to the rotation boundary.
Encoding streams bounded chunks from a CPU thread while new appends continue.

Publication syncs the new snapshot and its directory, then atomically replaces
and directory-syncs CURRENT with the new snapshot and tail log set. Only then
can obsolete files be deleted. Exact pending cleanup paths must remain
recoverable across interruption of cleanup. Journal recovery and cleanup use
these explicit names; they do not discover generations by listing directories.

Checksum and adapter decoding run in a bounded parallel recovery pipeline;
framing and state application retain file order. The CPU budget includes the
reader/apply caller and defaults to available parallelism capped at four;
`Options.recovery_threads` can override it. Both log and snapshot decode use this pipeline. This does
not remove the O(live records + replay tail) work of restoring an in-memory index.

## Adapter responsibilities

The file cache catalog retains paged/whole store registration, page/block identity, Begin and
Complete/Resolve records, pending intents, allocation accounting and startup
reconciliation by exact data path. Begin and Complete/Resolve request `Durable`.
Its business admission permit spans Begin, data I/O and Complete. Pending
transaction IDs are explicit snapshot payload fields, not overloaded snapshot
frame LSNs. Startup resolves pending operations and checkpoints before serving.
Whole and paged stores share this stream's failure status: a failed file mutation
stops further catalog mutations until restart. Failed eviction preserves runtime
residency and accounting. Small-store streams remain independent.

Small store retains per-ring data segment reservation, concurrent append,
pre-created spare segments, payload fsync, seal/delete/atomic replacement,
TTI checkpoints, capacity eviction and garbage-ratio policy. Insert requests
`Written`; flush, seal, deletion within a durable prefix, and replacement request
`Durable` after their required data/directory ordering. Flush records a recoverable
prefix without sealing the active segment. Snapshots retain its durable end and
any pending tail; startup trims the tail before durably finalizing the prefix.
Small objects are not also recorded in
the paged catalog. Each adapter decides which state is safe to recover.

Background rate/I/O budgets belong to Worker policy. Shared snapshot/cleanup
uses an optional crate-independent `Maintenance` hook whose futures run locally
on the journal runtime (including its timers); it must not import
Worker types or charge foreground log commits to the maintenance budget.
Namespace initialization attaches the same Worker budget to each scoped
catalog and its small-store stream before their writers start, including
automatic checkpoints of a recovered tail and reconciliation checkpoints.
Background startup preserves
the namespace budget instead of replacing the default small-store budget.
Small-store pressure reclamation checkpoints journal history when payload
reclamation cannot meet the target, releasing its I/O permit before waiting.

## Implementation ownership and validation

The small-store worktree owns the single shared engine implementation and its
small-store adapter. The catalog worktree owns its adapter and contributes its
bounded parallel replay implementation to the shared engine. Neither worktree
edits the other's files or implements a competing engine. Common code is
transferred at an agreed revision; adapter integration follows that interface.

Acceptance covers both adapters, not just independent unit suites: two streams
progress independently; Written/Durable acknowledgement and fsync ordering;
service-wide count/byte backpressure through cancellation; snapshot plus tail
replay; interruption at each CURRENT publication/cleanup boundary; strict
corruption handling; pending paged intents; small-store seal/replacement;
shutdown/root-lease lifetime; and native versus explicit legacy operation.
Production latency and cold-start conclusions require separate benchmarks.

## Combined startup validation

The integrated Worker was measured on a 2-vCPU, 16-GB Azure VM with three local
NVMe devices in RAID 0. Each cache contained 1,934,887 complete objects with
234,272,852,992 payload bytes. With the strict threshold below 1 MiB, 1,934,886
objects used the packed store and one boundary object used paged storage. With
the threshold disabled, all objects used paged storage. Both modes used the
same native journal engine and two data rings.

After the locator and bulk-index recovery optimizations, three fresh-process
runs per mode produced these median cache-recovery times:

| Mode | Warm filesystem cache | Cold filesystem cache |
| --- | --- | --- |
| Small store enabled | 10.155 s | 10.211 s |
| Small store disabled | 20.975 s | 21.072 s |

Cold runs used `sync` and Linux `drop_caches=3`, not a VM restart. All runs
followed clean shutdown checkpoints. Capacity was 400 GiB, pages 1 MiB, blocks
256 MiB and segments 64 MiB. L1, TTI, stale GC and ownership GC were disabled;
background byte throttling was disabled for checkpoint completion. Each of the
twelve timed runs restored every object, and four groups of 17,123 sampled
objects passed content verification. These are cache-recovery measurements,
not coordinator readiness or abnormal-exit recovery measurements.

A subsequent timing-only build measured runtime reconstruction separately:
6.122 s with the packed store and 12.816 s with paged storage, each a three-run
warm median. Packed-store access-token and LRU reconstruction took 6.014 s;
paged lifecycle initialization accounted for another 6.283 s in the paged case.
There was no packed-store `access.checkpoint` in this TTI-disabled fixture, so
these results do not include restoration from a populated access-time file.

The timing build passed 582 Worker library tests (five ignored), Worker
library/server Clippy with warnings denied, and a release server build. Its
eight timed startups and two 17,123-object verification groups also passed.
The unchanged shared r3 engine separately passed 16 tests; earlier adapter-only
benchmarks remain documented in [the paged catalog design](paged-catalog.md).
