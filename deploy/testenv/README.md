# Talon object-store latency lab

A self-contained stack that serves real object bytes from **Azurite** (the Azure
Blob emulator) and reproduces the *latency pattern* of a real object store, so
you can see how Talon's cache masks backend latency.

Two composable latency layers:

1. **Toxiproxy** in front of Azurite — network-layer latency + jitter, bandwidth
   throttling, timeouts, connection resets. Added/removed at runtime with
   [`toxics.sh`](./toxics.sh).
2. The **worker's in-process delay decorator** — precise per-request first-byte
   latency and a bandwidth ceiling, via `TALON_WORKER_BACKEND_*` env in the
   compose file. Independent of the proxy; the two stack.

> The Azurite account name/key here are Microsoft's well-known **public**
> emulator credentials — not secrets. Never point this stack at real data.

## Launch

```sh
# From the repo root:
docker compose -f deploy/testenv/docker-compose.yml up -d --build

# Seed a container with three 8 MiB sample objects:
docker compose -f deploy/testenv/docker-compose.yml run --rm seed

# Management UI + API:
open http://127.0.0.1:8000/ui
```

## Dial the latency

```sh
./deploy/testenv/toxics.sh s3-warm           # ~30ms first byte, small jitter
./deploy/testenv/toxics.sh s3-cold-longtail  # ~150ms + heavy tail jitter
./deploy/testenv/toxics.sh throttled         # 50ms + ~10 MB/s bandwidth ceiling
./deploy/testenv/toxics.sh flaky             # 80ms + 20% of conns time out
./deploy/testenv/toxics.sh clear             # remove all toxics
./deploy/testenv/toxics.sh list              # show current toxics
```

Watch the effect on the worker's backend-fetch latency in the UI (node detail
traffic) or directly:

```sh
curl -s http://127.0.0.1:8001/metrics | grep talon_worker_backend_fetch_duration_seconds
```

A first read of an object is a cache **miss** and pays the backend latency; a
repeat read of the same block is a local **hit** and does not — which is the
whole point of the cache, now visible under controlled latency.

## In-process layer only

To model latency inside the worker instead of (or on top of) the proxy,
uncomment the `TALON_WORKER_BACKEND_*` env in the compose file and recreate the
worker:

```sh
docker compose -f deploy/testenv/docker-compose.yml up -d worker
```

## Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | The stack: azurite, toxiproxy, coordinator, worker, seed. |
| `toxiproxy.json` | Preloads the `azurite` proxy (the image has no shell). |
| `toxics.sh` | Attach/remove named latency presets via the Toxiproxy API. |
