# Object-store gateway deployment

`talon-gateway` exposes the read-only compatibility surface documented in the
[compatibility matrix](../reference/object-store-gateway-compatibility.md). One
process serves either S3 or Azure Blob, connects to a Talon coordinator and
worker fleet for cached bytes, and uses a separately scoped origin identity for
metadata and fallback reads.

## Security state

Incoming provider authentication and authorization are tracked by issue #446
and are not implemented. This has two enforced consequences:

- `development` mode only binds loopback. It is suitable for an application
  sidecar in the same network namespace.
- `production` mode can bind a routable address, but `/readyz` remains failing
  and provider requests return `503` until TLS, authentication, and
  authorization are installed.

Do not add a Kubernetes Service, ingress, host port, or public load balancer to
the current gateway. A client signature or SAS token is parsed only as protocol
input; it is not yet proof of identity.

## Credential boundary

The gateway never forwards incoming `Authorization`, SAS, or presigned query
credentials to a rewritten origin request. It builds a new request and signs it
with the process's origin identity.

| Direction | Credential | Storage |
|---|---|---|
| Client to gateway | Not yet validated | Never logged or forwarded; #446 owns validation. |
| Gateway to S3 | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN` | Environment/secret injection only. |
| Gateway to Azure | Exactly one of `TALON_GATEWAY_AZURE_SHARED_KEY` or `TALON_GATEWAY_AZURE_SAS` | Environment/secret injection only. |
| Gateway to Talon | Coordinator and worker data-plane connection | Existing Talon cache-client boundary; mTLS integration is part of #446. |

Use an origin identity limited to read metadata, object ranges, and bounded
listing on the namespaces assigned to that gateway. Do not grant write or
account administration permissions.

## Configuration

Common variables:

| Variable | Default | Meaning |
|---|---|---|
| `TALON_GATEWAY_PROTOCOL` | `s3` | `s3` or `azure`. |
| `TALON_GATEWAY_BIND` | `127.0.0.1:8080` | Listener; development mode rejects non-loopback addresses. |
| `TALON_GATEWAY_MODE` | `development` | `development` or fail-closed `production`. |
| `TALON_COORDINATOR_ADDR` | required | Talon coordinator control address. |
| `TALON_GATEWAY_ROUTE` | `cache` | `cache`, `cache-only`, or direct `origin`. |
| `TALON_GATEWAY_PATH_STYLE` | `false` | Accept `/bucket/key` or `/account/container/blob`. |
| `TALON_GATEWAY_ENDPOINT_SUFFIX` | provider default | Virtual-host suffix accepted from clients. |
| `TALON_GATEWAY_BLOCK_SIZE` | `268435456` | Must match worker cache granularity. |
| `TALON_GATEWAY_TRANSFER_CHUNK_BYTES` | `1048576` | Maximum cache body frame. |
| `TALON_GATEWAY_PLACEMENT_TTL_MS` | `5000` | Client-side placement freshness. |
| `TALON_GATEWAY_REPLICAS` | `1` | Ordered worker replicas attempted by the cache client. |

S3 variables:

| Variable | Default | Meaning |
|---|---|---|
| `TALON_GATEWAY_S3_REGION` | `us-east-1` | Origin signing region. |
| `TALON_GATEWAY_S3_ENDPOINT` | AWS regional endpoint | Optional `http[s]://host[:port]`; custom endpoints use path addressing. |
| `AWS_ACCESS_KEY_ID` | required | Scoped origin access key. |
| `AWS_SECRET_ACCESS_KEY` | required | Scoped origin secret. |
| `AWS_SESSION_TOKEN` | unset | Optional STS token. |

Azure variables:

| Variable | Default | Meaning |
|---|---|---|
| `TALON_GATEWAY_AZURE_ACCOUNT` | required | The single account served by the process. |
| `TALON_GATEWAY_AZURE_ENDPOINT` | public Azure | Optional `http[s]://host[:port]`; custom endpoints use path addressing. |
| `TALON_GATEWAY_AZURE_SHARED_KEY` | unset | Base64 account key. Mutually exclusive with SAS. |
| `TALON_GATEWAY_AZURE_SAS` | unset | Origin SAS without logging or serialization. |

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
| `/readyz` reports security reasons | Production security is incomplete. | Do not route traffic; complete #446. |
| `503` with `ServerBusy` or `ServiceUnavailable` | Origin unavailable, or cache fallback also unavailable. | Check origin reachability and scoped credentials; correlate `x-talon-request-id`. |
| `504` / timeout error | Worker or request deadline elapsed. | Check worker saturation, placement freshness, and network latency. |
| `412` | Origin ETag changed or a client condition failed. | Re-resolve metadata; do not retry stale cached bytes. |
| `404` | Origin-authoritative object absence. | Verify namespace and key; cache does not override this result. |
| `416` | Unsatisfiable or multi-range request. | Send one valid byte range. |
| Cache errors increase while reads succeed | Infrastructure fallback is active. | Repair coordinator/workers before origin load becomes sustained. |

Metrics use bounded labels only. Use request IDs for individual incidents;
object names, credentials, and origin URLs are deliberately absent from labels
and error bodies.
