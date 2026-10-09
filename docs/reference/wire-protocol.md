# Wire protocol reference

This page specifies Talon's client-facing wire protocol precisely enough to
implement a client without reading the Rust source. It is normative: the
[conformance vectors](#conformance-vectors) are generated from the
implementation and a test fails if the two diverge.

Two planes share one framing format:

- the **control plane** carries small messages between clients, workers, and
  the coordinator, with a bincode-encoded body;
- the **data plane** carries object bytes between clients and workers, with a
  raw body and no envelope, so a worker can `sendfile` straight from a block
  file into the socket.

## Frame header

Every frame begins with 16 bytes. **All header fields are big-endian.**

| Offset | Size | Field | Notes |
|---|---|---|---|
| 0 | 2 | magic | `0x544C`, ASCII `TL`. Reject otherwise. |
| 2 | 1 | protocol version | Currently `1`. |
| 3 | 1 | message type | See below. |
| 4 | 2 | flags | Bit 0 `END_OF_STREAM`, bit 1 `ERROR`. |
| 6 | 2 | reserved | Zero on send, ignore on receive. |
| 8 | 4 | request id | Echoed in the response; correlates on a pipelined connection. |
| 12 | 4 | payload length | Bytes following the header. May be zero. |

Message types:

| Value | Type | Plane |
|---|---|---|
| 0 | `Control` | control |
| 1 | `Get` | data |
| 2 | `GetRange` | data |
| 3 | `Put` | data |
| 4 | `Ping` | either |
| 5 | `Delete` | data |
| 6 | `GetCachedRange` | data |
| 7 | `AdmitCachedBlock` | data |
| 8 | `GetRangeTenant` | data |
| 9 | `GetCachedRangeTenant` | data |
| 10 | `GetVersionedRange` | data |
| 11 | `GetVersionedRangeTenant` | data |

A zero payload length is legal and must not be treated as end-of-stream.

### Limits a client should expect

A server validates the advertised length against a **per-message-type cap
before allocating**, and bounds each read with a timeout. Control and `Ping`
frames are capped far below the data-plane maximum, so a control listener never
commits a data-plane-sized buffer. A client that advertises more than the cap
for a type receives no response and has its connection dropped.

Clients should apply the same discipline in reverse: a response header's length
field is attacker-controlled from the client's perspective if the worker is not
trusted, so bound it before allocating.

## Control plane

The payload is a bincode-encoded envelope:

```
struct Envelope {
    schema: u16,             // CONTROL_SCHEMA_VERSION at time of send
    message: ControlMessage, // externally tagged enum
}
```

A receiver rejects a schema it cannot decode rather than misinterpreting the
body. The current version and the oldest decodable version are both published
in the conformance vector file, so a client can check compatibility without
hardcoding them. Schema 6 is the only supported control schema; pre-deployment legacy
registration, heartbeats and membership queries have been consolidated. Receivers
reject earlier schemas before reading positional enum tags.

### Bincode encoding rules

The control plane uses bincode 1.3 with its default configuration. **Unlike the
frame header, bincode is little-endian.** A decoder needs these rules and
nothing more:

| Type | Encoding |
|---|---|
| `u8` … `u64`, `i8` … `i64` | Fixed width, **little-endian**. No varints. |
| `bool` | One byte, `0` or `1`. |
| enum variant | `u32` tag, little-endian, **in declaration order starting at 0**, followed by the variant's fields. |
| `String`, `str` | `u64` byte length, then UTF-8 bytes. The length counts **bytes, not characters**. |
| `Vec<T>`, sequences | `u64` element count, then each element. |
| `Option<T>` | One byte: `0` for `None`, `1` followed by the value for `Some`. |
| struct | Fields in declaration order, no names, no padding. |
| tuple | Elements in order. |

Two consequences worth stating because they are where naive decoders break:

- **An empty `Vec` or `String` is a `u64` zero followed by nothing.** It is not
  an absent field, and it is not a null.
- **Enum tags are positional.** Inserting a variant in the middle of the
  declaration renumbers everything after it. That is a breaking wire change and
  requires a schema bump.

### Messages used by a read-path client

Variant tags are the enum's declaration order. The read path needs these:

| Tag | Message | Direction | Fields |
|---|---|---|---|
| 0 | `PlacementLookup` | client → coordinator | `block: BlockId`, `k: u8` |
| 1 | `PlacementResponse` | coordinator → client | `owners: Vec<NodeId>`, `epoch: u64` |
| 4 | `MembershipQuery` | client → coordinator | *(none)* |
| 5 | `MembershipList` | coordinator → client | `view: WorkerDiscovery` |
| 8 | `StatObject` | client → coordinator | `object: ObjectId` |
| 9 | `ObjectStat` | coordinator → client | `size: u64`, `version: String` |
| 10 | `ListObjects` | client → coordinator | `prefix: String` |
| 11 | `ObjectList` | coordinator → client | `entries: Vec<ObjectEntry>` |

Workers send `NodeStatusHeartbeat { status: NodeStatus }` (tag 7). The coordinator
replies with `NodeStatusAck { accepted: bool, serving: bool, detail: Option<String> }`
(tag 17). New instances and admission-relevant changes are persisted and routing
is installed before acceptance. Unchanged reports from an admitted instance are
accepted in memory and coalesced for periodic publication; acceptance does not
promise that every heartbeat's metrics or sequence survives a Coordinator crash.
`serving` separately grants service admission. A conflicting or withdrawn instance
can be accepted without being allowed to serve. `NodeStatus.ready` reports local
readiness, independent of this grant. There is no separate registration RPC.

`MembershipQuery` reads the Coordinator's last installed discovery without a
backend RPC. Its validity is bounded by the observation's remaining cache lifetime;
requests never renew that lifetime. Coordinators refresh independently, so a client
switching replicas can temporarily observe different views. The equality tokens
do not provide monotonic revision ordering.

### File prewarm (control schema 6)

These variants are appended after `ControlFailure` in the schema-6 contract.
Existing variants retain their tags and encoding.

| Tag | Message | Direction | Fields |
|---|---|---|---|
| 19 | `LoadBlock` | client → worker | `block: BlockId`, `len: u64` |
| 20 | `BatchLoad` | client → worker | `blocks: Vec<LoadBlockRequest>` |

`LoadBlockRequest` contains `block: BlockId` followed by `len: u64`. A batch
contains 1–1024 assignments, which may refer to different files and versions.
The encoded schema and message body must fit within `MAX_CONTROL_PAYLOAD_LEN`
minus the maximum tracing-envelope overhead (currently 1 MiB minus 1026 bytes).
Both encoders and decoders enforce these limits. The client groups assignments
by worker and splits on either limit; it never falls back to individual LOAD
RPCs for batch input. An empty SDK batch sends no frames.

Each batch gets one correlated `Ack(true)` after all assignments complete.
Each batch occupies one LOAD RPC admission permit and runs a window of at most
eight block loads. Single and batch LOADs share a worker-wide limit of eight
active block loads. On the first observed failure, the worker stops adding
assignments and drains the existing window before returning `Ack(false)` with
the failing block. Completed fills remain cached. Batch execution is not atomic,
and separately dispatched batches can finish after the caller observes failure.
Batch RPCs and the complete SDK batch operation have a 30-minute deadline.

LOAD uses the worker's configured origin HTTP retry policy. Transient failures
(including S3 `429` and `503 SlowDown`) are retried within the block's concurrency
permit, including backoff. Defaults are three retries, exponential backoff with
full jitter (100 ms base, 5 s cap), and capped `Retry-After` hints in seconds.
`403`, `404`, and `412` are not retried. A retry does not resend the batch or
replay completed blocks; exhaustion is a block failure as described above.

The SDK assigns blocks to the same persistent Maglev primaries used by reads,
using one logical topology. Before dispatch, it resolves each primary through
valid instance discovery and its isolated connection pool. Expired discovery
must be refreshed; offline or conflicting owners fail without remapping.
Topology changes fail the operation. The coordinator only serves existing membership
discovery; it does not receive or orchestrate LOAD. The caller must provide
the correct size for the requested version. Workers use that extent without
issuing HEAD. Workers use their existing
version-pinned fill path and configured cache form, and reply with `Ack(true)`
after warming the block. `len` is the logical block length, including a short
final block. Errors return `Ack(false)` with a diagnostic. The SDK reports byte
and block counts locally after every assignment succeeds. Previously completed
cache fills are retained on failure and remain subject to normal eviction.
The unversioned `Load` variant (tag 2) remains reserved and unsupported.

Supporting types:

```
struct ObjectId  { backend: Backend, bucket: String, object_path: String }
struct BlockId   { object: ObjectId, offset: u64, block_size: u32, version: Version }
struct WorkerDiscovery { topology_token: u64, state_token: u64, valid_for_ms: u64,
                         workers: Vec<DiscoveredWorker> }
struct DiscoveredWorker { member: WorkerMember, state: InstanceState }
struct WorkerMember { worker_id: String, zone: Option<String>, retired: bool }
enum InstanceState { Offline, Conflict, Serving { instance_id: String, address: String } }
struct ObjectEntry { path: String, size: u64 }

enum Backend  { S3 = 0, Gcs = 1, Azure = 2 }   // u32 tag
enum NodeRole { Coordinator = 0, Worker = 1 }  // u32 tag

// NodeId and Version are newtypes over String: encoded exactly as a String.
```

For server-side placement lookup, `PlacementResponse` returns node **ids** that the
caller resolves to dialable addresses through `MembershipList`. Current clients
use `MembershipList` directly and compute placement locally.

## Data plane

A `GetRange` request frame carries a bincode `RangeRequest` body:

```
struct RangeRequest { object: ObjectId, offset: u64, len: u64 }
```

A client that already resolved an object's source version sends a distinct
`GetVersionedRange` request (message type 10):

```
struct VersionedRangeRequest { request: RangeRequest, version: Version }
```

The worker must serve the exact versioned cache identity or fill it from the
backend with `version` as a conditional request. It returns `VersionMismatch`
if that generation is no longer available; it must not re-resolve and serve a
newer generation. `GetVersionedRangeTenant` (message type 11) wraps the request
with a `TenantId`. A paged miss may issue HEAD to obtain the block length;
that metadata must match the requested version too. These distinct request types are fail-closed during rolling
upgrades: an older worker rejects them instead of silently ignoring `version`.
Deployments must therefore upgrade workers before enabling a client that emits
these messages; old clients continue using `GetRange` against new workers.
Custom `BackendStore` implementations must explicitly implement conditional
range reads; the trait default rejects a supplied version rather than silently
ignoring it.

The response is a header followed by **raw object bytes with no envelope** —
this is what allows the worker to `sendfile` from the block file directly into
the socket. The header's length field gives the exact byte count.

On failure the worker sets the `ERROR` flag and the body is a UTF-8 message,
not payload. A client must check the flag before treating the body as data.

Once a response header promising *N* bytes is on the wire, the worker cannot
retract it — a mid-transfer failure drops the connection rather than sending an
error frame, because an error frame would be read as payload. **A client that
sees a truncated body must treat the connection as desynchronised and
reconnect**, not attempt to resynchronise.

## Read-path semantics

A correct client does more than encode messages:

**Client-side placement.** Clients cache the persistent logical workers returned
by `MembershipList` and build a deterministic Maglev table when its topology
changes. Offline and conflicted workers remain in placement. Workers sort by stable ID. The table size is the next power of two at
or above `max(4096, 64 * worker_count)`; SHA-256 domains
`talon-cache-maglev-worker-v1\0` and `talon-cache-maglev-block-v1\0` derive
worker permutations and block slots from the same canonical length-delimited
fields in every language. A primary lookup is one block hash and one table
access, O(1) regardless of worker count. `PlacementLookup` remains a
compatibility operation for older clients.

**Instance discovery.** Placement selects a logical worker ID. The same discovery
snapshot resolves that ID to its sole serving instance and address. Instance or
address changes replace the isolated connection pool without changing logical
ownership. The observation expires after the smaller of the advertised lifetime,
the client's configured TTL, and 500 ms; expired instances cannot serve reads.

**Bounded replica fallback.** Each block read ranks up to `replicas_k` distinct
logical workers, primary first (default 1). Offline and conflicted candidates
retain their rank but may be skipped; retryable read failures try the next
candidate in the same snapshot, checking its discovery deadline before use.
A reused connection that fails with an I/O error may be redialed once; discovery
must still be valid before dialing and before resending. Fresh-connection failures
and Worker error responses do not trigger this transport retry. Failure does not
refresh discovery or restart the candidate list.
Invalid requests, missing origin objects, version mismatches, origin failures,
and tenant rate limits return immediately. Exhaustion returns
`BlockReadError::AllReplicasFailed { worker, source }`, preserving the last
candidate address (or logical ID when no address is known) and typed cause.
`replicas_k` bounds read candidates; it does not proactively populate replicas
or distribute successful primary reads across backups. Admission still targets
the primary only.

A later request refreshes expired discovery; concurrent refreshes are serialized
and cached snapshots limit refreshes to one per 100 ms. Last-good snapshots are
retained for diagnostics, but never permit reads through expired instances.

**Legacy epoch reconciliation.** `PlacementResponse` carries the epoch its
owners were computed at. Clients still using this compatibility operation must
invalidate a cached placement when they observe a different epoch.

**Multi-block ranges.** A range spanning block boundaries splits into one fetch
per block, each addressed by its own `BlockId`. Block size is a worker
configuration value and is part of every locally constructed `BlockId`.

## Conformance vectors

`crates/talon-transport/tests/conformance_vectors.json` holds byte-exact
encodings of the messages above, including the cases that break naive decoders:
empty strings and sequences, multi-byte UTF-8 in object keys, a zero-length
payload, and a `u64` value above 2^32.

```json
{
  "control_schema_version": 6,
  "min_control_schema_version": 6,
  "vectors": [
    { "name": "control.object_stat.large_size",
      "note": "A size above 2^32 — decoders that read u32 will silently truncate here",
      "hex": "544c010000000000000000030000002002000b00000000f2052a010000000a0000000000000030783844414243444546" }
  ]
}
```

The file is **generated, never hand-written**, and a test asserts the committed
copy matches what the current code produces. A change that alters the wire
therefore fails a test in this repository with a visible diff, rather than
failing silently in a client written in another language.

Every client implementation should assert against this file. Regenerate it with:

```sh
just gen-conformance-vectors
```

If that produces a diff, the wire format changed. Bump
`CONTROL_SCHEMA_VERSION` when the change is not backward compatible, update the
other clients, and commit the regenerated vectors in the same change.
