# ADR 0005: Object-Store-Compatible Gateways

- Status: Proposed
- Date: 2026-08-09
- Tracking issues: #469 (gateway epic), #440 (S3), #470 (Azure)
- Related issues: #441 (cache routing), #442 (cache granularity), #446
  (client/data-plane security)

## Context

Talon currently exposes cached objects through FUSE, language clients, and a
custom framed TCP protocol. Applications that already use an S3 or Azure Blob
SDK cannot adopt that path by changing an endpoint alone. This is especially
costly for engines that use several independent object-store clients in one
process.

An object-store-compatible HTTP endpoint gives those clients one integration
boundary. It also creates a new public data plane with stricter requirements
than the existing diagnostic CLI:

- protocol status, headers, range semantics, and errors must match the selected
  object-store API;
- responses must stream with bounded memory and backpressure;
- a cache failure may fall back only when policy allows it and must never hide
  an authentication, authorization, not-found, or invalid-request error;
- client credentials cannot simply be forwarded when endpoint rewriting
  changes what was signed; and
- the coordinator must not carry payload bytes.

The Spiro Azure proxy demonstrates that endpoint override is a practical
integration method and that a proxy can assemble byte ranges from cache before
falling back to the origin. It also buffers complete responses, hand-parses
HTTP/1.1, and treats HTTP/2 requests as bodyless. Those implementation choices
are not suitable for Talon's compatibility and bounded-memory requirements.

## Decision

### 1. Build one gateway core with S3 and Azure protocol adapters

Talon will add a dedicated gateway crate. It owns the HTTP server, request
lifecycle, Talon cache client, origin client, routing decision, metrics, and
shutdown behavior. S3 and Azure are adapters around that shared core, not two
copies of the cache path.

The shipped binaries are protocol-specific so configuration cannot
accidentally expose the wrong API:

- `talon-s3-proxy`
- `talon-azure-proxy`

They may share a library and container image. A process serves one configured
origin credential domain. The first Azure milestone supports one storage
account per process; the first S3 milestone supports buckets reachable through
one endpoint and credential set. This matches the worker backend configuration
and prevents cache-key aliasing: the current Azure `ObjectId` identifies a
container and blob but does not independently encode the account.

### 2. Use a staged request pipeline

Every request passes through these stages:

1. parse the protocol request without normalizing away signed path/query bytes;
2. authenticate the client and authorize the operation and namespace;
3. map the protocol target to a canonical `ObjectId`;
4. validate the signed `x-talon-cache-mark` and derive an internal route;
5. resolve origin metadata when the route requires a version or exact length;
6. stream from Talon workers or the origin according to that route;
7. translate the result into protocol-specific headers/errors and record
   bounded-cardinality metrics.

The raw cache-mark header is never forwarded to workers or the origin. The
default in its absence follows #441: lookup on, population on, and bounded
origin fallback. Production mode rejects a cache mark that is not covered by
the client signature.

### 3. Keep protocol compatibility in adapters

The shared core uses provider-neutral operations such as stat, ranged read,
list, put, and delete. The adapters retain behavior that cannot be safely
flattened into a lowest-common-denominator API.

The initial S3 read milestone implements:

- `HeadObject`;
- `GetObject` with no range or one RFC 9110 byte range;
- `ListObjectsV2`, including delimiter and continuation tokens;
- path-style and virtual-host-style addressing; and
- S3 XML errors, request IDs, conditional headers, and response metadata.

The initial Azure read milestone implements:

- Get Blob Properties (`HEAD`);
- Get Blob (`GET`) with no range or one `Range`/`x-ms-range`;
- List Blobs for one container with prefix, delimiter, marker, and max results;
- public-cloud virtual-host and Azurite-style path addressing; and
- Azure XML errors, request IDs, conditions, and `x-ms-*` response metadata.

Multiple ranges are rejected with the provider's documented error until a
multipart response implementation exists. Unsupported operations are either
explicitly rejected or transparently passed through; they are never silently
approximated.

Initial compatibility commitments are explicit:

| S3 operation | First read milestone | Data source |
|---|---|---|
| `HeadObject` | Supported | Origin metadata |
| `GetObject` (full or one range) | Supported | Talon or origin per route |
| `ListObjectsV2` | Supported | Paginated origin listing |
| Multi-range `GetObject` | Rejected explicitly | None |
| `PutObject`, `DeleteObject`, `CopyObject` | Later pass-through milestone | Origin |
| Multipart upload | Later pass-through milestone | Origin; no cache commit before completion |
| Other S3 APIs | Rejected unless listed by a later matrix revision | None |

| Azure operation | First read milestone | Data source |
|---|---|---|
| Get Blob Properties | Supported | Origin metadata |
| Get Blob (full or one range) | Supported | Talon or origin per route |
| List Blobs | Supported | Paginated origin listing |
| Multiple ranges | Rejected explicitly | None |
| Put Blob, Delete Blob, Copy Blob | Later pass-through milestone | Origin |
| Put Block / Put Block List | Later pass-through milestone | Origin; no cache commit before block-list commit |
| Other Azure APIs | Rejected unless listed by a later matrix revision | None |

### 4. Extract a reusable, streaming Talon cache client

Gateway code must not depend on `talon-fuse`. The coordinator client,
membership cache, placement, connection pool, read planning, and worker client
will move or be generalized into a reusable client crate. FUSE and both gateway
adapters will consume the same implementation.

The current worker response frame has a 32-bit payload length and clients
materialize a response as `Vec<u8>`. The reusable client therefore exposes a
bounded chunk stream:

- split an HTTP range at Talon block boundaries;
- further split each segment by a configurable transfer chunk cap;
- request at most one bounded chunk per stream slot;
- yield that chunk to the HTTP body before requesting more when backpressure is
  applied; and
- cancel the in-flight worker request when the HTTP client disconnects.

This is bounded-memory streaming even before the worker protocol gains a
multi-frame response. A later zero-copy gateway path may proxy worker frames or
file descriptors, but it is not required for correctness.

### 5. The origin remains authoritative

Metadata and mutation success come from the origin. A cache hit does not grant
authority to invent a version, length, content type, or conditional-request
result.

For a cacheable read, the gateway resolves the source version and length,
constructs versioned `BlockId`s, and requests the exact response range from
workers. Workers retain responsibility for read-through fill and conditional
origin fetch. The gateway preserves provider response metadata obtained during
resolution and returns only after it can produce a protocol-correct response.

An eligible cache infrastructure failure may fall back to a direct origin
request under #441. Not-found, precondition failure, invalid range,
authentication failure, and authorization failure are terminal and are never
converted into fallback success.

Writes remain write-through: acknowledge only after origin commit. The first
write milestone passes through operations that the gateway can preserve
exactly. Cache population after a successful write is optional and must follow
the signed cache mark. Multipart state is bound to the authenticated principal,
target, route decision, and upload ID; cache population occurs only after
successful completion.

### 6. Terminate client authentication and use gateway origin credentials

The gateway validates the client's S3 SigV4/presigned request or Azure Shared
Key/SAS/Bearer request before cache lookup. It maps that identity to allowed
accounts, buckets/containers, prefixes, and operations as specified by #446.
Authorization is evaluated on cache hits as well as misses.

After authorization, the gateway signs origin requests with its own scoped
origin identity. It does not forward a client `Authorization` header after
changing the authority or request target. This avoids invalid signatures and a
confused-deputy boundary. Client and origin secrets are redacted from logs,
errors, traces, and metrics.

An explicitly enabled development mode may omit client authentication while
binding to a loopback address. It is not a production configuration and must
emit a startup warning. Production readiness fails closed without TLS,
authentication, and an authorization policy.

### 7. Use the provider's HTTP stack, not a hand-written parser

The gateway uses the repository's Tokio runtime and a maintained HTTP server
implementation with streaming body support. Parsing limits apply before body
allocation: request-line/header bytes, header count, body length, concurrent
requests, idle time, and total request deadline are all bounded.

Origin access uses a pooled asynchronous HTTP client. Hop-by-hop headers are
removed, signed headers are reconstructed deliberately, redirects are disabled
unless the provider adapter explicitly handles them, and response bodies are
streamed without whole-object buffering.

### 8. Observability is part of the data-plane contract

Both adapters expose the same low-cardinality dimensions:

- protocol and operation;
- cache route and outcome (hit, miss/fill, bypass, fallback, cache-only miss);
- response class and typed failure reason;
- requested, cache, origin, and response bytes; and
- time to first byte and total latency.

Object keys, signatures, SAS tokens, and unbounded account identifiers are not
metric labels. Request IDs are returned in the provider's response headers and
may be used for sampled traces and structured logs.

## Delivery plan

Work is split into independently reviewable PRs. Every implementation PR starts
from the then-current `upstream/main` and must pass upstream CI before the next
one begins.

1. Land this ADR and an explicit S3/Azure compatibility matrix.
2. Extract the reusable Talon cache client without changing FUSE behavior
   (#471).
3. Add bounded chunk streaming and typed cache/fallback errors to that client
   (#472).
4. Add the shared HTTP gateway skeleton, limits, metrics, and development mode
   (#473).
5. Implement the Azure read adapter and Azurite conformance tests (#474, #475).
6. Implement the S3 read adapter under #440 and LocalStack/SDK conformance
   tests (#476, #477).
7. Implement signed cache marks and origin bypass/fallback under #441.
8. Implement production authentication, authorization, and TLS under #446.
9. Add provider-correct write pass-through incrementally, including multipart
   conformance before claiming transparent write support.
10. Publish deployment configuration, compatibility matrices, and gateway
    overhead benchmarks (#478).

## Consequences

### Positive

- Existing S3 and Azure clients can adopt Talon through endpoint configuration.
- One cache-routing implementation serves FUSE and both HTTP protocols.
- Chunked worker reads and HTTP backpressure bound gateway memory by
  concurrency rather than object size.
- Origin authority and provider-specific behavior stay explicit.

### Costs and risks

- A public HTTP data plane substantially expands the authentication,
  authorization, parsing, and conformance surface.
- Metadata resolution may add an origin round trip before a warm cache read;
  later metadata caching must preserve the freshness contract.
- The first streaming implementation still copies each bounded worker chunk
  through gateway memory.
- One-origin-per-process limits deployment flexibility until cache identity and
  backend routing encode account/endpoint identity explicitly.
- Two provider APIs require separate conformance suites even though they share
  the core.

## Rejected alternatives

### Fork the Spiro Azure proxy and add an S3 mode

Rejected. Its complete-response buffering, hand-written HTTP/1 parser, and
bodyless HTTP/2 assumption conflict with #440. The useful behavior is retained
as requirements, not copied as architecture.

### Put gateway endpoints on the coordinator

Rejected. The coordinator is a stateless management service. Sending object
payloads through it adds a throughput bottleneck and violates #440.

### Forward client authorization directly to the origin

Rejected. S3 signatures include the authority and signed headers, and endpoint
override commonly changes both. Re-signing after explicit authorization gives
the gateway a coherent trust boundary.

### Build two independent proxies

Rejected. Placement, worker streaming, fallback classification, metrics,
limits, and lifecycle behavior would diverge while solving the same problem.

## Open questions

1. Should the shared gateway be one crate with two binaries or a library plus
   two thin crates? Choose when the HTTP skeleton is implemented.
2. Which transfer chunk cap gives the best memory/throughput trade-off before
   #442 selects a server-side cache granularity?
3. Should protocol metadata be resolved directly from the origin or through a
   new paginated coordinator/worker API? Bulk data will not use the coordinator
   either way.
4. What additional identity must enter `ObjectId` before a single process may
   safely serve multiple Azure accounts or S3 endpoints?
