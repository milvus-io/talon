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

The S3 compatibility matrix will be added with the S3 adapter and conformance
tasks. Existing `talon-backend` S3 support is an origin client, not an
S3-compatible gateway endpoint.
