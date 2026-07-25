# Latency lab

Talon's job is to hide object-store latency. The **latency lab** lets you see it
do that under controlled, reproducible conditions: it serves real object bytes
from a local emulator and reproduces the latency *pattern* of a real store (S3,
Azure Blob) — first-byte latency, tail jitter, bandwidth ceilings, timeouts —
without a cloud account.

It lives under
[`deploy/testenv/`](https://github.com/milvus-io/talon/tree/main/deploy/testenv).

## What it simulates

Two composable layers, either or both:

1. **Network layer — Toxiproxy** sits between the worker and the emulator and
   injects latency + jitter, bandwidth throttling, timeouts, and connection
   resets. This is closest to how a real object store misbehaves on the wire.
2. **In-process layer — the worker's delay decorator** adds precise per-request
   first-byte latency and an optional bandwidth ceiling inside the worker
   itself, independent of the network. Useful for exact, repeatable numbers.

The origin is **Azurite**, Microsoft's Azure Blob emulator. The worker reaches
it through the proxy using the endpoint override
(`TALON_WORKER_AZURE_ENDPOINT`), so all of its reads are subject to whatever
latency you dial in.

## Launch

From the repository root:

```sh
docker compose -f deploy/testenv/docker-compose.yml up -d --build

# Seed a container with sample objects:
docker compose -f deploy/testenv/docker-compose.yml run --rm seed
```

Open the management console at <http://127.0.0.1:8000/ui>.

## Dial the network layer

`toxics.sh` attaches named presets to the proxy at runtime:

```sh
./deploy/testenv/toxics.sh s3-warm           # ~30ms first byte, small jitter
./deploy/testenv/toxics.sh s3-cold-longtail  # ~150ms + heavy tail jitter
./deploy/testenv/toxics.sh throttled         # 50ms + ~10 MB/s bandwidth ceiling
./deploy/testenv/toxics.sh flaky             # 80ms + 20% of connections time out
./deploy/testenv/toxics.sh clear             # remove all toxics
```

## Dial the in-process layer

Uncomment the `TALON_WORKER_BACKEND_*` variables in the compose file and recreate
the worker. These model latency inside the worker regardless of the proxy:

| Variable | Effect |
|----------|--------|
| `TALON_WORKER_BACKEND_DELAY_MS` | Fixed first-byte latency per request. |
| `TALON_WORKER_BACKEND_JITTER_MS` | Uniform extra latency in `[0, jitter]`. |
| `TALON_WORKER_BACKEND_THROUGHPUT_BYTES` | Bandwidth ceiling in bytes/second. |

They are documented in the [configuration reference](../reference/configuration.md)
and default off, so the production path is never affected.

## Read the effect

A first read of an object block is a cache **miss** and pays the backend
latency; a repeat read of the same block is a local **hit** and does not. That
gap — visible under a latency preset — is exactly what the cache buys you.

Watch it in the worker's metrics:

```sh
curl -s http://127.0.0.1:8001/metrics | grep talon_worker_backend_fetch_duration_seconds
```

or in the management console's node-detail traffic panel. With `s3-cold-longtail`
applied, the first fetch of each object shows the injected latency; subsequent
reads served from cache stay flat.
