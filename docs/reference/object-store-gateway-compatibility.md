# Object-store gateway compatibility

The gateway crate provides a bounded HTTP runtime and provider protocol
adapters. This matrix describes behavior implemented and continuously tested;
it does not imply that the production authentication and deployment work is
complete.

## Azure Blob Storage

| Surface | Status | Authority and behavior |
|---|---|---|
| Get Blob Properties (`HEAD`) | Supported | Metadata and conditions are resolved by the configured origin identity. |
| Get Blob (`GET`) | Supported | Bytes stream from Talon or the origin without whole-object buffering. |
| One `Range` or `x-ms-range` | Supported | Closed, open-ended, suffix, aligned, unaligned, and EOF-clamped ranges are supported. |
| Multiple ranges | Rejected | Returns `MultipleConditionHeadersNotSupported`; multipart responses are not approximated. |
| List Blobs | Supported | `prefix`, `delimiter`, `marker`, and `maxresults` are forwarded to paginated origin listing. |
| Conditional reads | Supported | ETag and date conditions remain origin-authoritative; `If-Range` controls range use. |
| Virtual-host addressing | Supported | One configured public-cloud account per process. |
| Azurite path addressing | Supported | `/account/container/blob` endpoint-override clients are accepted. |
| Put, delete, copy, and block APIs | Not yet supported | Rejected explicitly until a write-through milestone preserves Azure commit semantics. |

Azure responses include UUID-shaped `x-ms-request-id` values, gateway request
correlation, Azure XML errors, and origin metadata. Blob names and listing
parameters use strict percent decoding. Unsupported or malformed requests fail
closed instead of being silently reinterpreted.

Cache infrastructure unavailability and timeout may fall back to a conditional
origin stream. Not-found, invalid range, version mismatch, authentication,
authorization, protocol, and internal failures are terminal. Response streams
are demand-driven; slow consumers do not trigger eager upstream reads, and
disconnecting drops the upstream stream.

The `azure backend and gateway e2e (azurite)` CI job runs both the backend
contract and the gateway through the official Azure SDK against Azurite. It
covers cold and warm reads, properties, full and ranged bytes, URL encoding,
listing pagination and delimiter behavior, conditions, and Azure errors.

Production exposure remains blocked until provider authentication and
authorization are installed as tracked by issue #446. The protocol adapter is
currently intended for conformance and integration work; executable packaging
and deployment are tracked separately.

## S3

| Surface | Status | Authority and behavior |
|---|---|---|
| HeadObject | Supported | Metadata and conditions are resolved by the configured origin identity. |
| GetObject | Supported | Bytes stream from Talon or the origin without whole-object buffering. |
| One `Range` | Supported | Closed, open-ended, suffix, aligned, unaligned, and EOF-clamped ranges are supported. |
| Multiple ranges | Rejected | Returns the S3 `InvalidRange` error; multipart responses are not approximated. |
| ListObjectsV2 | Supported | `prefix`, `delimiter`, `continuation-token`, `max-keys`, and URL encoding are forwarded to one bounded origin page. |
| Conditional reads | Supported | ETag and date conditions remain origin-authoritative; `If-Range` controls range use. |
| Virtual-host addressing | Supported | The bucket is parsed from the configured endpoint suffix. |
| Path addressing | Supported | `/bucket/key` endpoint-override clients are accepted. |
| PutObject | Supported | Fixed-length bodies are passed through once. `UNSIGNED-PAYLOAD` streams with backpressure; a declared SHA-256 is verified in a bounded-memory spool before origin dispatch. |
| CopyObject | Supported | Source read and destination write grants are both required. Copy conditions and metadata remain origin-authoritative. |
| DeleteObject | Supported | Passed through once; the origin status remains authoritative and confirmed absence invalidates local placement state. |
| Multipart upload | Not yet supported | Multipart query shapes return `NotImplemented`; tracked by issue #497. |

Incoming client authorization material is never forwarded to a rewritten
origin request. The gateway uses its scoped origin identity and signs the
validated method, target, conditions, range, and listing parameters again.
Incoming SigV4 authentication, bucket/prefix authorization, TLS, and optional
client mTLS are enforced before dispatch in production mode. Copy authorization
checks the source and destination independently.

Mutation responses remain origin-authoritative. A confirmed success invalidates
all local placement entries for the object; a failed origin response does not.
If transport fails after dispatch, the gateway returns
`x-talon-commit-state: indeterminate`; clients must inspect object state before
retrying. Request bodies are bounded by the configured gateway body and request
deadlines (16 MiB and 30 seconds by default). Multipart upload is the path for
objects that exceed the configured ordinary-request bound.

The S3 adapter emits S3 XML errors and gateway request IDs. Cache fallback and
streaming behavior match the Azure adapter rules above. The `s3 backend and
gateway e2e (localstack)` CI job runs the backend plus the gateway through
boto3 and the MinIO SDK. It covers cold and warm reads, exact ranges, URL
encoding, conditions, presigned URLs, listing pagination and delimiters, and
standard errors. Arrow-compatible signed HEAD and ranged GET fixtures run in
the same test.

## Cache routing mark

The version 1 cache-routing contract is parsed and signature-bound, while the
adapter continues to use its deployment-selected route until worker
lookup-only/populate operations land. S3 uses the SigV4-signed
`x-talon-cache-mark` header. Azure Shared Key uses the canonical
`x-ms-talon-cache-mark` header; Azure SAS requests cannot carry a non-default
mark because SAS does not sign custom headers. An absent mark means lookup on,
population on, and bounded origin fallback.

See the [deployment and failure runbook](../operations/object-store-gateway.md)
and [gateway benchmark results](object-store-gateway-benchmarks.md) for the
operational boundary around this matrix.
