# Object-store gateway deployment

`talon-gateway` exposes the compatibility surface documented in the
[compatibility matrix](../reference/object-store-gateway-compatibility.md). One
process serves either S3 or Azure Blob, connects to a Talon coordinator and
worker fleet for cached bytes, and uses a separately scoped origin identity for
metadata, fallback reads, and origin-authoritative mutations.

## Security state

S3 SigV4, Azure Shared Key/SAS, and URI-SAN client-certificate authentication
plus namespace authorization are available. This has the following enforced
consequences:

- `development` mode only binds loopback. It is suitable for an application
  sidecar in the same network namespace.
- `production` mode can bind a routable address, but `/readyz` remains failing
  and provider requests return `503` until TLS, authentication, and
  authorization are installed. S3 requires both configuration files documented
  below.

Only a validated provider or mTLS identity that maps to an explicit allow grant
can pass the production data plane. When both identity forms are present, both
must map to exactly the same principal and provider account.

## Credential boundary

The gateway never forwards incoming `Authorization`, SAS, or presigned query
credentials to a rewritten origin request. It builds a new request and signs it
with the process's origin identity.

| Direction | Credential | Storage |
|---|---|---|
| Client to S3 gateway | SigV4 access key, optional STS token, or presigned query | Identity file mounted read-only; never logged or forwarded. |
| Client to Azure gateway | Shared Key, service SAS, or account SAS | Identity file mounted read-only; never logged or forwarded. |
| Gateway to S3 | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN` | Environment/secret injection only. |
| Gateway to Azure | Exactly one of `TALON_GATEWAY_AZURE_SHARED_KEY` or `TALON_GATEWAY_AZURE_SAS` | Environment/secret injection only. |
| Gateway to Talon | Coordinator and worker data-plane connection | Existing Talon cache-client boundary; mTLS integration is part of #446. |

Use an origin identity limited to the advertised read and mutation operations
on namespaces assigned to that gateway. Do not grant bucket or account
administration permissions.

### Planned trusted passthrough mode

Issue #510 tracks `TALON_GATEWAY_ORIGIN_AUTH=trusted-passthrough`. It is a
planned sidecar mode, not a currently accepted configuration value. It will be
restricted to a loopback `development` listener and initially accept S3
presigned-query credentials and Azure service/account SAS.

In that mode a resident cache read is intentionally not authorized again. Any
process that can reach the listener can read matching resident data, so the
listener must share the training job's network namespace and must not be
published by a container port, Service, ingress, or host-network wildcard.
Cache keys exclude credentials. Listings, mutations, cold metadata, and data
misses go to the provider with the request-scoped capability; Talon service
credentials are not substituted.

The mode requires a cache-only worker probe and gateway-driven cache fill.
Until those protocol changes land, setting no origin credentials does not
produce passthrough: workers still read through using their configured backend
identity. For S3, a cold GET uses the original GET at the origin because its
presigned query cannot authorize a synthetic HEAD. Successful origin responses
seed a bounded in-memory metadata index; restart loses that index and forces a
new origin request. Capability revocation and mutable-object freshness are not
enforced for already resident bytes.

Clients must create the capability for the configured origin authority and
canonical target. Replacing the network destination is valid only when the
gateway can restore that authority without changing the signed method, path,
query, signed headers, or body. `x-amz-security-token` without a matching
SigV4 signature is not a bearer credential.

## Configuration

Common variables:

| Variable | Default | Meaning |
|---|---|---|
| `TALON_GATEWAY_PROTOCOL` | `s3` | `s3` or `azure`. |
| `TALON_GATEWAY_BIND` | `127.0.0.1:8080` | Listener; development mode rejects non-loopback addresses. |
| `TALON_GATEWAY_MODE` | `development` | `development` or fail-closed `production`. |
| `TALON_GATEWAY_ORIGIN_AUTH` | `service` | Planned: `service` or loopback-only `trusted-passthrough` (#510). |
| `TALON_COORDINATOR_ADDR` | required | Talon coordinator control address. |
| `TALON_GATEWAY_ROUTE` | `cache` | `cache`, `cache-only`, or direct `origin`. |
| `TALON_GATEWAY_PATH_STYLE` | `false` | Accept `/bucket/key` or `/account/container/blob`. |
| `TALON_GATEWAY_ENDPOINT_SUFFIX` | provider default | Virtual-host suffix accepted from clients. |
| `TALON_GATEWAY_BLOCK_SIZE` | `268435456` | Must match worker cache granularity. |
| `TALON_GATEWAY_MAX_BODY_BYTES` | `16777216` | Request body limit, enforced before spooling. Raise only with matching disk and concurrency capacity. |
| `TALON_GATEWAY_REQUEST_DEADLINE_MS` | `30000` | Total request deadline, covering the streamed response body. Raise for large objects on slow links. |
| `TALON_GATEWAY_TRANSFER_CHUNK_BYTES` | `1048576` | Maximum cache body frame. |
| `TALON_GATEWAY_PLACEMENT_TTL_MS` | `5000` | Client-side placement freshness. |
| `TALON_GATEWAY_REPLICAS` | `1` | Ordered worker replicas attempted by the cache client. |
| `TALON_GATEWAY_TLS_CERT_PATH` | unset | PEM server certificate chain; must be paired with the key. |
| `TALON_GATEWAY_TLS_KEY_PATH` | unset | PEM private key; its path and contents are redacted. |
| `TALON_GATEWAY_TLS_RELOAD_MS` | `5000` | Last-good certificate/key reload interval. |
| `TALON_GATEWAY_TLS_HANDSHAKE_TIMEOUT_MS` | `10000` | Maximum TLS handshake time per connection. |
| `TALON_GATEWAY_TLS_MAX_HANDSHAKES` | `256` | Maximum concurrent pre-HTTP TLS handshakes. |
| `TALON_GATEWAY_TLS_CLIENT_AUTH` | `off` | `off`, `optional`, or `required` client-certificate mode. |
| `TALON_GATEWAY_TLS_CLIENT_CA_PATH` | unset | PEM client trust-anchor bundle; required when client auth is enabled. |
| `TALON_GATEWAY_TLS_TRUST_DOMAIN` | unset | Exact SPIFFE URI SAN trust domain. |
| `TALON_GATEWAY_MTLS_IDENTITIES_PATH` | unset | JSON URI SAN to principal mapping file. |
| `TALON_GATEWAY_AUTHORIZATION_PATH` | unset | JSON allow-grant file. Required for production readiness. |

The listener permits TLS 1.3 only and advertises HTTP/2 and HTTP/1.1. A bad
rotation increments `talon_gateway_tls_events_total{event="reload_failure"}`
and retains the previous valid material. Plaintext and stalled handshakes are
discarded before HTTP dispatch and counted with bounded event labels.

Client authentication validates the certificate chain, validity period, and
client-authentication usage. The leaf must contain exactly one URI SAN, use the
configured `spiffe://` trust domain, and match an explicit mapping:

```json
{
  "identities": [
    {
      "uri_san": "spiffe://cluster.example/talon/cluster-a/worker/worker-1",
      "principal": "analytics-reader",
      "provider_account": "tenant-a"
    }
  ]
}
```

`required` mTLS can satisfy production authentication readiness without a
provider identity file. `optional` only controls the TLS handshake and still
requires a provider authenticator for readiness. A request with both a provider
credential and client certificate is denied unless their mappings agree. CA
bundles may contain old and new trust anchors during rotation; invalid reloads
retain the complete last-good server and client-auth configuration.

Authentication and authorization decisions emit `gateway security audit`
events with request ID, protocol, operation, decision, and bounded reason.
Principal and resource identifiers are truncated SHA-256 values; credentials,
certificate contents, account names, buckets, containers, and object paths are
not emitted.

S3 variables:

| Variable | Default | Meaning |
|---|---|---|
| `TALON_GATEWAY_S3_REGION` | `us-east-1` | Origin signing region, required incoming SigV4 scope region, `GetBucketLocation` answer, and the `x-amz-bucket-region` value stamped on `HeadBucket` responses. |
| `TALON_GATEWAY_S3_ENDPOINT` | AWS regional endpoint | Optional `http[s]://host[:port]`; custom endpoints use path addressing. |
| `AWS_ACCESS_KEY_ID` | required | Scoped origin access key. |
| `AWS_SECRET_ACCESS_KEY` | required | Scoped origin secret. |
| `AWS_SESSION_TOKEN` | unset | Optional STS token. |
| `TALON_GATEWAY_S3_CLIENT_IDENTITIES_PATH` | unset | JSON incoming identity file. Required for S3 production readiness. |
| `TALON_GATEWAY_S3_MAX_CLOCK_SKEW_MS` | `900000` | Maximum header-signature clock skew and presigned grace. |
| `TALON_GATEWAY_S3_MAX_MULTIPART_UPLOADS` | `1024` | Maximum active multipart bindings held by one gateway process. |
| `TALON_GATEWAY_S3_MULTIPART_TTL_MS` | `86400000` | Inactivity lifetime for one process-local multipart binding. |

The identity file is separate from the origin environment variables. Do not use
the gateway's origin access key as a client identity in production:

```json
{
  "identities": [
    {
      "access_key_id": "client-access-key",
      "secret_access_key": "client-secret",
      "session_token": "optional-sts-token",
      "principal": "analytics-reader",
      "provider_account": "tenant-a"
    }
  ]
}
```

The authorization file is allow-only and defaults to deny. Prefixes are literal
object-store prefixes; use a trailing slash when a path-like boundary is
intended:

```json
{
  "grants": [
    {
      "id": "tenant-a-datasets",
      "principal": "analytics-reader",
      "protocol": "s3",
      "provider_account": "tenant-a",
      "namespace": "datasets",
      "prefix": "published/",
      "operations": ["stat", "read", "list", "write", "delete"]
    }
  ]
}
```

SigV4 verification enforces the configured region, `s3` service, host,
canonical URI/query/headers, timestamp or presigned expiry, payload declaration,
and STS token. A present `x-talon-cache-mark` must be signed and syntactically
valid. Invalid requests fail before cache or origin dispatch.

Both listing dialects pass through: ListObjectsV2 and ListObjects V1, the
latter being what the AWS C++ SDK's `ListObjectsRequest` and its startup
connectivity checks emit. `HeadBucket` existence probes pass through to the
origin once and keep its status authoritative, so SDK startup checks (for
example minio-go `BucketExists`) distinguish a missing bucket (404) from a
denied one (403).
The response's `x-amz-bucket-region` is rewritten to the gateway region so
clients never re-sign for the origin's real region. `GetBucketLocation` is
answered locally with `TALON_GATEWAY_S3_REGION` because clients use it to pick
the SigV4 signing region for requests to this gateway; the probe is also the
one request accepted with a `us-east-1` credential scope, matching the
region-cache bootstrap that minio-family SDKs send before they know the real
region. Both probes authorize as the dedicated `probe` operation: grant SDK
clients one prefixless `probe` entry on the bucket they probe. A `probe` grant
discloses only bucket existence and the gateway region — it never authorizes
`HeadObject` or any other object access, so prefix isolation between tenants
is preserved. Buckets must be provisioned
at the origin out of band: `CreateBucket` remains rejected, and the origin
identity still must not hold bucket administration permissions.

Ordinary S3 `PutObject`, `CopyObject`, and `DeleteObject` requests are sent to
the origin exactly once. `PutObject` requires one valid `Content-Length` and is
limited by the gateway's 16 MiB default body limit and 30 second default total
deadline. `UNSIGNED-PAYLOAD` streams directly; a lowercase SHA-256 declaration
is verified in a secure temporary file before the origin is mutated. AWS
streaming chunk-signature modes are not supported. S3 multipart create,
upload-part, upload-part-copy, list-parts, complete, and abort are supported.
Active uploads are process-local, bounded to 1024 entries, and expire after 24
hours by default. A gateway restart or a binding mismatch fails closed with
`NoSuchUpload`; operators must abort the orphan directly at the origin before
starting a replacement upload. Completion alone invalidates local object
placement, and HTTP 200 completion responses carrying `<Error>` do not commit
gateway state. Increase deployment limits only with corresponding disk,
concurrency, and deadline capacity.

Azure variables:

| Variable | Default | Meaning |
|---|---|---|
| `TALON_GATEWAY_AZURE_ACCOUNT` | required | The single account served by the process. |
| `TALON_GATEWAY_AZURE_ENDPOINT` | public Azure | Optional `http[s]://host[:port]`; custom endpoints use path addressing. |
| `TALON_GATEWAY_AZURE_SHARED_KEY` | unset | Base64 account key. Mutually exclusive with SAS. |
| `TALON_GATEWAY_AZURE_SAS` | unset | Origin SAS without logging or serialization. |
| `TALON_GATEWAY_AZURE_CLIENT_IDENTITIES_PATH` | unset | JSON incoming account-key identity file. Required for Azure production readiness. |
| `TALON_GATEWAY_AZURE_MAX_BLOCK_BINDINGS` | `1024` | Maximum active block-staging bindings held by one gateway process. |
| `TALON_GATEWAY_AZURE_BLOCK_BINDING_TTL_MS` | `86400000` | Inactivity lifetime for one process-local block-staging binding. |
| `TALON_GATEWAY_AZURE_MAX_CLOCK_SKEW_MS` | `900000` | Maximum Shared Key clock skew and SAS start/expiry grace. |

Azure client keys are also separate from the origin credential. Each key maps
to the one account served by the process and to one policy principal:

```json
{
  "identities": [
    {
      "account_key": "base64-client-account-key",
      "principal": "analytics-reader",
      "provider_account": "storage-account-a"
    }
  ]
}
```

The verifier accepts full Shared Key plus service/account SAS for Blob service
reads, listings, writes, and deletes. It enforces signature scope, permissions,
resource type, protocol, start, expiry, and bounded skew. Stored access policies, signed IP
ranges, encryption scopes, and user-delegation SAS fail closed because this
gateway cannot independently enforce those external constraints. Shared Key
signs `x-ms-talon-cache-mark` through Azure canonicalized headers; SAS requests
carrying a cache mark are rejected because SAS cannot bind custom headers.

Azure `Put Blob` accepts only fixed-length `BlockBlob` uploads. Copy is limited
to a concrete source blob in the configured account and requires both source
read and destination write authorization. Set Blob Metadata, Set Blob
Properties, Delete Blob, Put Block, Put Block From URL, Get Block List, and Put
Block List are also passed through. Active block sets are held in a bounded,
expiring process-local registry and cannot be adopted after a restart. The
gateway never retries a consumed upload stream.
Confirmed mutation success, plus `404` from Delete Blob, invalidates local
placement state; staged blocks do not. A failed HTTP response does not
invalidate it, while transport loss after dispatch returns
`x-talon-commit-state: indeterminate` and requires an authoritative blob check
before retrying.

## Docker

Build the non-root image:

```sh
docker build -f deploy/docker/gateway.Dockerfile -t talon-gateway .
```

Loopback mode requires host networking when the client runs on the Docker host.
Keep secrets in an env file with restrictive permissions:

```sh
docker run --rm --network host --env-file /run/secrets/talon-s3.env \
  -e TALON_GATEWAY_PROTOCOL=s3 \
  -e TALON_COORDINATOR_ADDR=127.0.0.1:7411 \
  -e TALON_GATEWAY_PATH_STYLE=true talon-gateway
```

For Azure, set `TALON_GATEWAY_PROTOCOL=azure`, the account, and exactly one
Azure origin credential in the env file.

## Kubernetes

Use the gateway only as a sidecar. The application reaches
`127.0.0.1:8080`, while no Service selects the gateway port:

- [S3 sidecar template](https://github.com/milvus-io/talon/blob/main/deploy/kubernetes/gateway-s3-sidecar.yaml)
- [Azure sidecar template](https://github.com/milvus-io/talon/blob/main/deploy/kubernetes/gateway-azure-sidecar.yaml)

Replace placeholder images and Secrets before applying. The templates run as a
fixed non-root UID, drop all capabilities, use a read-only root filesystem, and
probe loopback with `exec` because kubelet HTTP probes target the Pod IP.

## Failure runbook

| Symptom | Meaning | Action |
|---|---|---|
| `/healthz` fails | Process/runtime failure. | Restart and inspect bounded gateway logs. |
| `/readyz` reports security reasons | Production security is incomplete. | Do not route traffic; install TLS, incoming identities, and authorization grants. |
| S3 request returns `403` | Signature, expiry, identity, account, namespace, prefix, or operation was denied. | Check client time and non-secret policy identifiers; never log signatures or keys. |
| `503` with `ServerBusy` or `ServiceUnavailable` | Origin unavailable, or cache fallback also unavailable. | Check origin reachability and scoped credentials; correlate `x-talon-request-id`. |
| `504` / timeout error | Worker or request deadline elapsed. | Check worker saturation, placement freshness, and network latency. |
| `412` | Origin ETag changed or a client condition failed. | Re-resolve metadata; do not retry stale cached bytes. |
| `500` with `x-talon-commit-state: indeterminate` | Mutation dispatch began but the origin response was lost. | Inspect the authoritative object before deciding whether to retry. |
| `404` | Origin-authoritative object absence. | Verify namespace and key; cache does not override this result. |
| `416` | Unsatisfiable or multi-range request. | Send one valid byte range. |
| Cache errors increase while reads succeed | Infrastructure fallback is active. | Repair coordinator/workers before origin load becomes sustained. |

Metrics use bounded labels only. Use request IDs for individual incidents;
object names, credentials, and origin URLs are deliberately absent from labels
and error bodies.
