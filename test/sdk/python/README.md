# Python SDK tests against an existing Talon instance

Runs the Python client suite against an **already-deployed** Talon cluster backed
by a real object store (MinIO, S3, ...). Unlike `clients/python/tests/test_client.py`
— which starts its own coordinator/worker/stub origin — this targets a running
cluster such as the one deployed by `test/stack/deploy.sh` or an existing
production instance.

## Prerequisites

- The `talon` wheel installed (`python -c "import talon"` succeeds).
  Build it once (output lands under git-ignored `target/`):
  `maturin build --release --manifest-path clients/python/Cargo.toml --out target/talon-wheel && pip install target/talon-wheel/*.whl`
- `pytest`
- A coordinator reachable from this machine (pod IP directly, or
  `kubectl port-forward svc/<release>-coordinator 17000:7000`).
- A deterministic seed object in the target bucket: bytes `i % 251`.
  See below for seeding an empty bucket with `mc`.

## Environment

| Variable | Default | Meaning |
|---|---|---|
| `TALON_E2E_COORDINATOR` | `127.0.0.1:17000` | Coordinator address (`host:port`) |
| `TALON_E2E_BLOCK_SIZE` | `8388608` (8 MiB) | Client-side block size; may differ from the worker's block size (bytes stay correct regardless) |
| `TALON_E2E_BUCKET` | `talon-e2e` | Bucket/container holding the seed object |
| `TALON_E2E_KEY` | `bench` | Seed object key |

## Run

```sh
export TALON_E2E_COORDINATOR="<coordinator-host>:7000"
export TALON_E2E_BUCKET="<bucket>"
python3 -m pytest test/sdk/python/ -v
```

## Example: existing Kubernetes instance

```sh
# 1. Find the coordinator pod IP (pod network must be reachable from this host).
kubectl get pod -o wide -n <namespace> | grep <release>-coordinator

# 2. If the bucket is empty, seed the ramp object via a throwaway mc pod
#    (kubectl cp fails: the mc image has no tar, so pipe with exec -i + cat).
kubectl -n <namespace> run mc-probe --image=minio/mc --restart=Never \
  --command -- /bin/sh -c "sleep 3600"
kubectl -n <namespace> exec -i mc-probe -- sh -c 'cat >/tmp/bench' < /tmp/talon-bench.bin
kubectl -n <namespace> exec mc-probe -- \
  mc alias set local http://<release>-minio:9000 <access-key> <secret-key>
kubectl -n <namespace> exec mc-probe -- \
  mc cp /tmp/bench local/<bucket>/bench

# 3. Run the suite.
export TALON_E2E_COORDINATOR="<coordinator-pod-ip>:7000"
export TALON_E2E_BLOCK_SIZE="8388608"
export TALON_E2E_BUCKET="<bucket>"
python3 -m pytest test/sdk/python/ -v

# 4. Clean up.
kubectl -n <namespace> delete pod mc-probe
```

Generate `/tmp/talon-bench.bin` (64 MiB, byte `i % 251`) on the local host first:

```sh
python3 -c "
chunk = bytes((i % 251) for i in range(251))
size = 64 << 20
with open('/tmp/talon-bench.bin','wb') as f:
    written = 0
    while written < size:
        take = min(len(chunk), size - written)
        f.write(chunk[:take]); written += take
"
```
