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
| Put Blob (`BlockBlob`) | Supported | One fixed-length body streams to the origin once; conditions, content properties, metadata, tags, tier, and encryption headers use an explicit allowlist. |
| Copy Blob / Copy Blob From URL | Supported | Same-account concrete blob sources only. Source read and destination write grants are both required, and the origin source URL is rebuilt from trusted configuration. |
| Set Blob Metadata / Properties | Supported | Bodyless `comp=metadata` and `comp=properties` requests preserve their operation-specific headers and origin conditions. |
| Delete Blob | Supported | Passed through once; successful deletion and origin-confirmed absence invalidate local placement state. |
| Put Block / Put Block From URL | Supported | Fixed-length blocks stream once. Same-account URL sources require source read authorization and are rebuilt from trusted origin configuration. |
| Get Block List / Put Block List | Supported | Uncommitted state is process-bound to principal and cache decision; only confirmed commit success invalidates cached placement. |

Azure responses include UUID-shaped `x-ms-request-id` values, gateway request
correlation, Azure XML errors, and origin metadata. Blob names and listing
parameters use strict percent decoding. Unsupported or malformed requests fail
closed instead of being silently reinterpreted.

Incoming client authorization, dates, version headers, and copy-source URLs are
not forwarded to rewritten origin requests. The gateway signs the validated
operation with its scoped origin identity. Mutation responses remain
origin-authoritative: confirmed success invalidates cached placement, failed
HTTP responses retain it, and a lost response after dispatch returns
`x-talon-commit-state: indeterminate`. Clients must inspect the blob before
retrying an indeterminate mutation.

Cache infrastructure unavailability and timeout may fall back to a conditional
origin stream. Not-found, invalid range, version mismatch, authentication,
authorization, protocol, and internal failures are terminal. Response streams
are demand-driven; slow consumers do not trigger eager upstream reads, and
disconnecting drops the upstream stream.

The `azure backend and gateway e2e (azurite)` CI job runs both the backend
contract and the gateway through the official Azure SDK against Azurite. It
covers cold and warm reads, properties, full and ranged bytes, URL encoding,
listing pagination and delimiter behavior, conditions, create and overwrite,
metadata, content properties, same-account copy, deletion, block staging,
ordering, replacement, missing blocks, block lists, and Azure errors. Azurite
returns its native `APINotImplemented` for Put Block From URL; focused backend
tests cover trusted source reconstruction for that operation.

Block blob staging supports Put Block, same-account Put Block From URL, Get
Block List, and Put Block List. Uncommitted state is bound in memory to the
authenticated principal and cache decision for 24 hours by default. A gateway
restart intentionally loses that binding and fails closed rather than adopting
orphaned blocks. Only a successful Put Block List invalidates cached data.

Azure SAS passthrough for a trusted loopback sidecar is designed but not yet
implemented (#510). The target behavior preserves all SAS query fields on an
origin access and shares cache entries across capabilities. A resident hit is
not re-authorized; a miss, listing, or mutation remains origin-authoritative.
Shared Key passthrough is not part of the first milestone.

Production exposure remains blocked until provider authentication and
authorization are installed as tracked by issue #446. The protocol adapter is
currently intended for conformance and integration work; executable packaging
and deployment are tracked separately.

## S3

| Surface | Status | Authority and behavior |
|---|---|---|
| HeadBucket | Supported | Passed through once with the configured origin identity; the origin's 200/404/403 status stays authoritative so SDK existence probes work. `x-amz-bucket-region` is rewritten to the gateway region so the origin's real region never leaks into client signing. Requires a prefixless `probe` grant on the bucket, which authorizes nothing object-level. |
| GetBucketLocation | Supported | Answered locally with the gateway's configured signing region, because clients use the response to pick their SigV4 credential scope for this gateway, not for the origin. Bucket existence is not checked. Accepted with a `us-east-1` credential scope as the one bootstrap exception, matching the region-cache probe minio-family SDKs sign before they know the region. Requires a prefixless `probe` grant on the bucket, which authorizes nothing object-level. |
| CreateBucket, DeleteBucket, ListBuckets | Rejected | Buckets are provisioned at the origin out of band; the gateway's origin identity holds no bucket administration permissions. |
| HeadObject | Supported | Metadata and conditions are resolved by the configured origin identity. |
| GetObject | Supported | Bytes stream from Talon or the origin without whole-object buffering. |
| One `Range` | Supported | Closed, open-ended, suffix, aligned, unaligned, and EOF-clamped ranges are supported. |
| Multiple ranges | Rejected | Returns the S3 `InvalidRange` error; multipart responses are not approximated. |
| ListObjectsV2 | Supported | `prefix`, `delimiter`, `continuation-token`, `max-keys`, and URL encoding are forwarded to one bounded origin page. |
| ListObjects (V1) | Supported | A bucket-level `GET` whose query carries only listing parameters forwards `prefix`, `delimiter`, `marker`, `max-keys`, and URL encoding to one bounded origin page; the V1 response body passes back unchanged. Bucket sub-resource queries (`uploads`, `versions`, `acl`, and any unrecognized key) and `list-type` values other than `2` are rejected rather than approximated by a listing. |
| Conditional reads | Supported | ETag and date conditions remain origin-authoritative; `If-Range` controls range use. |
| Virtual-host addressing | Supported | The bucket is parsed from the configured endpoint suffix. |
| Path addressing | Supported | `/bucket/key` endpoint-override clients are accepted. |
| PutObject | Supported | Fixed-length bodies are passed through once. `UNSIGNED-PAYLOAD` streams with backpressure; a declared SHA-256 is verified in a bounded-memory spool before origin dispatch. |
| Streaming upload (`STREAMING-UNSIGNED-PAYLOAD-TRAILER`) | Supported | The AWS SDK aws-chunked framing is decoded to its payload and forwarded as `UNSIGNED-PAYLOAD`, using `x-amz-decoded-content-length` as the length. The body is decoded into a bounded spool so the trailer checksum (`crc32`, `crc32c`, `crc64nvme`, or `sha256`) is verified over the payload before origin dispatch; a length or checksum mismatch, or an unsupported declared algorithm, fails closed as a client error and never reaches the origin. The verified checksum is not propagated to the origin. `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` (signed chunks) stays rejected. |
| CopyObject | Supported | Source read and destination write grants are both required. Copy conditions and metadata remain origin-authoritative. |
| DeleteObject | Supported | Passed through once; the origin status remains authoritative and confirmed absence invalidates local placement state. |
| Multipart upload | Supported | Create, upload-part, upload-part-copy, list-parts, complete, and abort are passed through. Upload IDs remain origin-issued; source and destination authorization is enforced for part copy. |

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
deadlines (16 MiB and 30 seconds by default). Each multipart part and completion
document must fit that bound, while the completed object may be larger.

Active multipart uploads are held in a bounded, 24-hour in-memory registry by
default. The binding includes the destination object, authenticated principal,
provider account, and immutable cache decision. A missing, expired, mismatched,
or restart-lost binding returns `NoSuchUpload` before origin dispatch. Complete
invalidates local placement only after a confirmed success; an HTTP 200 body
containing `<Error>` remains a failure. Abort removes the binding after origin
success or confirmed absence.

The S3 adapter emits S3 XML errors and gateway request IDs. Cache fallback and
streaming behavior match the Azure adapter rules above. The `s3 backend and
gateway e2e (localstack)` CI job runs the backend plus the gateway through
boto3 and the MinIO SDK. It covers cold and warm reads, exact ranges, URL
encoding, conditions, presigned URLs, listing pagination and delimiters,
bucket existence probes and the locally answered location query, ordinary
mutations, boto3 and MinIO multipart operations, part copy, abort,
ordering failures, and standard errors. Arrow-compatible signed HEAD and ranged
GET fixtures run in the same test.

S3 presigned-query passthrough for a trusted loopback sidecar is designed but
not yet implemented (#510). The capability must have been signed for the
configured origin method, authority, canonical path, query, headers, and body;
the gateway restores that origin request after a cache miss. A resident hit is
not re-authorized. Header SigV4 and a standalone STS session token are not part
of the first milestone.

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
