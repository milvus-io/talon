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
hardcoding them.

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
| 2 | `PlacementLookup` | client → coordinator | `block: BlockId`, `k: u8` |
| 3 | `PlacementResponse` | coordinator → client | `owners: Vec<NodeId>`, `epoch: u64` |
| 6 | `MembershipQuery` | client → coordinator | *(none)* |
| 7 | `MembershipList` | coordinator → client | `nodes: Vec<NodeInfo>` |
| 10 | `StatObject` | client → coordinator | `object: ObjectId` |
| 11 | `ObjectStat` | coordinator → client | `size: u64`, `version: String` |
| 12 | `ListObjects` | client → coordinator | `prefix: String` |
| 13 | `ObjectList` | coordinator → client | `entries: Vec<ObjectEntry>` |
| 17 | `RingPlacementLookup` | client → coordinator | `block: BlockId`, `ring: Ring`, `k: u8` |

`RingPlacementLookup` requires schema 5 and is only needed to reach the async
worker pool; see [Choosing a ring](#choosing-a-ring). A client that reads whole
blocks needs `PlacementLookup` alone, exactly as before.

Supporting types:

```
struct ObjectId  { backend: Backend, bucket: String, object_path: String }
struct BlockId   { object: ObjectId, offset: u64, block_size: u32, version: Version }
struct NodeInfo  { id: NodeId, address: String, role: NodeRole }
struct ObjectEntry { path: String, size: u64 }

enum Backend  { S3 = 0, Gcs = 1, Azure = 2 }   // u32 tag
enum Ring     { Block = 0, Async = 1 }         // u32 tag
enum NodeRole { Coordinator = 0, Worker = 1, AsyncWorker = 2 }  // u32 tag

// NodeId and Version are newtypes over String: encoded exactly as a String.
```

For legacy placement lookup, `PlacementResponse` returns node **ids** that the
caller resolves to dialable addresses through `MembershipList`. Current clients
use `MembershipList` directly and compute placement locally.

## Data plane

A `GetRange` request frame carries a bincode `RangeRequest` body:

```
struct RangeRequest { object: ObjectId, offset: u64, len: u64 }
```

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

**Client-side placement.** Current clients cache the healthy workers returned by
`MembershipList` and build a deterministic Maglev table when that membership
changes. Workers sort by stable ID. The table size is the next power of two at
or above `max(4096, 64 * worker_count)`; SHA-256 domains
`talon-cache-maglev-worker-v1\0` and `talon-cache-maglev-block-v1\0` derive
worker permutations and block slots from the same canonical length-delimited
fields in every language. A primary lookup is one block hash and one table
access, O(1) regardless of worker count. `PlacementLookup` remains a
compatibility operation for older clients.

**Placement caching.** Cache locally ranked worker addresses rather than node
IDs. When membership identity or address changes, invalidate affected placement
entries and resolve them against the rebuilt table.

**Replica fallback.** On a fetch failure, walk the cached replicas in order. If
all are exhausted, invalidate the entry, refresh membership once, and retry
before giving up. If membership refresh fails, an existing client retains its
last-good snapshot so a coordinator outage does not interrupt cached data-plane
placement.

**Legacy epoch reconciliation.** `PlacementResponse` carries the epoch its
owners were computed at. Clients still using this compatibility operation must
invalidate a cached placement when they observe a different epoch.

**Choosing a ring.** Two disjoint worker pools answer placement, and the client
picks which one:

| Ring | Message | Pool | Hashed on |
|---|---|---|---|
| block | `PlacementLookup`, or `RingPlacementLookup { ring: Block }` | `NodeRole::Worker` | the whole `BlockId` |
| async | `RingPlacementLookup { ring: Async }` | `NodeRole::AsyncWorker` | the object identity alone |

The async pool caches the exact byte range asked for rather than a whole block,
which is what a Parquet or Lance reader wants; the block pool is right for full
scans. The coordinator sees only a byte range and cannot tell them apart, so it
does not guess.

A lookup never crosses pools. An async lookup against a cluster with no async
workers answers with **no owners** — never a block worker, which holds no
extents and would fail the read a round trip later.

The two block-ring forms are defined to place identically, so a client may
migrate to the ring-aware message without moving any data. Prefer
`PlacementLookup` for the block ring anyway: it has meant the block ring since
schema 1, and it works against coordinators older than schema 5.

Both forms answer with the same `PlacementResponse`, so a client decodes one
reply shape either way. The data plane is identical: both worker types serve the
same `GetRange`.

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
  "control_schema_version": 2,
  "min_control_schema_version": 1,
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
