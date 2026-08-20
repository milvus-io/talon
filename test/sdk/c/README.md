# C SDK tests against an existing Talon instance

Runs the C client end-to-end suite (`minio_e2e.c`, dependency-free `main()` +
pass/fail accounting, no test framework) against an **already-deployed** Talon
cluster backed by a real object store. This is the C client's only test that
drives a real cluster through the public ABI (`talon.h`).

## Prerequisites

- `cc` (or another C11 compiler) and `cargo` (to build `libtalon_c.a`).
- A coordinator reachable from this machine (pod IP directly, or
  `kubectl port-forward svc/<release>-coordinator 17000:7000`).
- A deterministic seed object in the target bucket: bytes `i % 251`.

## Positional arguments

`test/sdk/c/run.sh [coordinator] [block_size] [bucket] [key]`
(any may be omitted; environment variables fill the gaps).

| Arg / env | Default | Meaning |
|---|---|---|
| `$1` / `TALON_E2E_COORDINATOR` | `127.0.0.1:17000` | Coordinator address (`host:port`) |
| `$2` / `TALON_E2E_BLOCK_SIZE` | `8388608` (8 MiB) | Client-side block size; may differ from the worker's block size |
| `$3` / `TALON_E2E_BUCKET` | `talon-e2e` | Bucket/container holding the seed object |
| `$4` / `TALON_E2E_KEY` | `bench` | Seed object key |

## Run

`run.sh` builds `libtalon_c.a` (`cargo build -p talon-c`), compiles the test,
links it, and runs it:

```sh
test/sdk/c/run.sh                            # defaults (local stack at :17000)
test/sdk/c/run.sh 10.0.0.5:7000 8388608      # explicit coordinator + block size
```

## Example: existing Kubernetes instance

```sh
# Find the coordinator pod IP, then run (bucket/key default from env).
export TALON_E2E_COORDINATOR="<coordinator-pod-ip>:7000"
export TALON_E2E_BUCKET="<bucket>"
test/sdk/c/run.sh "$TALON_E2E_COORDINATOR" 8388608
```

Seed an empty bucket first if needed — see
[test/sdk/python/README.md](../python/README.md) for the `mc-probe` seeding steps.
