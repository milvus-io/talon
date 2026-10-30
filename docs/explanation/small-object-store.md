# Packed small-object cache

## Goal and scope

Cache objects of a few KiB without a data file, JSON sidecar, directory, and
filesystem allocation unit per object. Preserve versioned reads, namespace and
credential isolation, existing placement, and fd-based sendfile/splice responses.
The origin remains the durability point. No write-back behavior is introduced.

The implementation is one local stack layer, `small-object/segment-store`, based
on `main`: engine, worker integration, tests, and this design form one feature.
Truncate, hole punching, compression, and a separate region hierarchy are out of
scope for v1.

## Configuration and routing

```toml
small_object_threshold_bytes = 1048576
small_object_segment_bytes = 67108864
```

The equivalent environment variables are
`TALON_WORKER_SMALL_OBJECT_THRESHOLD_BYTES` and
`TALON_WORKER_SMALL_OBJECT_SEGMENT_BYTES`. Admission defaults to a 1 MiB
threshold; setting it to zero disables new admissions. Segment size defaults
to 64 MiB. An enabled engine requires capacity for four segments, and the LOAD
payload budget must accommodate an entire eligible object.

An eligible object has positive total size strictly below the threshold and
fits in a short first block. Empty objects and full blocks retain existing
storage. A small range of a large object, or its short final block, does not
qualify. READ and LOAD fetch an eligible object completely with the source
version precondition; short responses are rejected before publication. Same-key
fills coalesce. LOAD never populates L1.

`SmallObjectStore` is an independent engine behind the worker's whole-block
storage facade. Small objects remain logical `BlockForm::Whole` cache units,
allowing capacity, superseded-version, stale, and ownership eviction to retain
their existing policy. Page TTI applies to the whole small object using its
shared access timestamp; recovery grants a fresh idle interval. Existing paged
residency is preserved rather than migrating an object during an active fill.

Each namespace and credential scope owns its own store under its existing cache
root. Existing small segments reopen even when admission is disabled or the
threshold changes. Rollback binaries ignore this directory, leaving a disposable
cache to rewarm; do not run two owners against the same cache root.

## Files and identity

Each segment has two files, `<id>.data` and `<id>.index`, shared by many objects.
One checksummed `MANIFEST` lists the sealed segments. New segment files use exclusive creation and IDs above the files observed
during recovery. Data is concatenated without per-record alignment,
padding, or compression. The index starts with a format version and contains
length-delimited, checksummed binary insertion and deletion records. Identity
encodes backend, bucket, object path, version, block offset and block size with
explicit lengths. It is compared in full, without treating a fingerprint as
proof of identity. Payload checksums are checked on byte reads and relocation;
sendfile/splice hits do not read the entire payload merely to verify a checksum.

The engine keeps one compact encoded identity shared by its lookup table and
segment-local live-record directory. Recovery reads the index streams and stats
each segment; it does not read all payloads or stat individual objects. Recovery
remains O(number of metadata records). V1 still uses the worker's existing
BlockIndex/LRU policy structures, so total RAM remains O(number of objects),
including those structures' allocations. This is not a disk-only index or a
claim of a fixed 32-byte total RAM cost per object.

## I/O and publication

All runtime payload, index and manifest operations use a buffered async adapter.
On a registered Monoio ring, open/read/write/fsync/stat/rename/unlink operations
are submitted to that current ring, with explicit file offsets. No direct I/O
alignment is required. Ordinary owned descriptors can be shared across rings;
short-lived Monoio wrappers remain on the submitting ring. Tokio callers,
including the current background scheduler, use the blocking pool through the
same async interface. Startup recovery runs synchronously before serving, or in
the namespace initializer's blocking task. No new private ring is created.

A namespace has one async mutation lock and one active segment. Reads use a
short directory lock to retain an fd, and do not hold the mutation lock. Accepted
mutations remain owned by the worker/ring if their caller stops waiting. The
mutation lock serializes writers, deletes, seals and compaction, making the
relocation snapshot current through publication. Compaction can delay writes
for one segment, but never holds the read directory lock while doing disk I/O.

An insert writes payload and index before exposing its locator. Active files
are deliberately absent from MANIFEST. Sealing synchronizes data and index,
atomically replaces the manifest, then synchronizes the directory. Segments
seal when full, at the next maintenance tick after 30 seconds, or at graceful
shutdown. An abrupt exit can lose the one unsealed segment per namespace/scope;
these are cache misses, not lost origin writes. This avoids per-insert fsync.

Deleting a sealed record appends and synchronizes a tombstone before removing
the locator. A failed mutation stops further mutations for that engine until
restart; continuing after a partial metadata write is unsafe. Malformed sealed
metadata, including torn tombstones, fails startup closed rather than replaying
a prefix that could resurrect an invalidated entry. An operator can move aside
that namespace's disposable cache while the worker is stopped to rewarm it.

## Reclamation and capacity

Logical eviction decreases live record bytes; it does not pretend to release
physical disk space. A maintenance call processes at most one eligible segment:

1. An empty segment is removed from the manifest and unlinked.
2. A sealed segment with less than half its record bytes live is relocated.
3. Relocation writes and synchronizes a new segment, publishes a manifest that
   replaces the old segment, updates locators, then unlinks the old files.

An interruption before manifest replacement leaves an orphan replacement. An
interruption afterwards leaves an orphan old file. Recovery only opens manifest
members and removes unreferenced engine files. It never merges both copies.
The collector enumerates only the selected segment's live records, not the
entire object directory. Background relocation pays the shared byte budget
before acquiring the mutation lock and rechecks the selected segment afterward.

No published payload extent is overwritten, truncated, punched, or reused.
An in-flight response retains its old inode across replacement/unlink. This is
necessary because sendfile completion does not imply peer consumption. Keeping
an fd alone would not protect against an in-place truncate or overwrite.

Worker capacity accounting replaces indexed small-record charges with measured
segment allocations, including dead bytes and retired files retained by open
handles. Like the whole-file store, admission writes and publishes the object
before best-effort capacity eviction. Capacity is a soft target: concurrent
admissions, relocation copies, and retired files retained by readers can
transiently exceed it. Crossing that target does not reject an admission;
actual filesystem I/O errors still propagate. Filesystem free space must
accommodate those temporary allocations, normal large-object writes, and
filesystem overhead; this cache budget is not a filesystem-wide reservation.
Under pressure, existing LRU policy chooses objects and segment maintenance
performs the physical reclamation. Foreground reclamation passes for a
namespace with a small store are serialized and recheck occupancy after
waiting, so concurrent commits do not each chase the same unreclaimed bytes.

## Validation

Run worker and core tests, the worker binary tests, formatting, Clippy, and
configuration-reference generation in the development container. Native tests
require an environment that permits io_uring; a seccomp rejection is not an I/O
correctness result.

Regressions cover complete-object routing, exact conditional fetches, LOAD
coalescing and L1 exclusion, version/short-fetch failures, write-through and
cache admission, restart, idle/capacity eviction, physical accounting, concurrent
read/delete/relocation, and a slow real sendfile receiver during unlink. Child
processes exit at payload/index writes, seal synchronization, manifest
publication, tombstone synchronization, and both relocation publication sides.
A native-ring test exercises writes, reads, sealing, deletion and compaction
and asserts zero blocking-pool fallbacks in the adapter.

`small_store::tests::small_layout_probe` is an ignored, explicitly invoked
10,000-object comparison against the old whole-file layout. It reports allocated
space, file count, write time and recovery time. It uses a warm filesystem and
is a layout check, not a cold-device or production throughput benchmark.

A local development-container run on 2026-10-29 with 10,000 objects of 3 KiB
reported the following. Timings are a single warm-filesystem observation;
allocation includes the data and metadata files, excluding directory blocks.

| Measurement | Whole-file layout | Packed layout |
| --- | ---: | ---: |
| Files | 20,000 | 3 |
| Allocated bytes | 82,972,672 | 31,580,160 |
| Recovery time | 300 ms | 134 ms |
| Write time | 6,544 ms | 6,094 ms |
