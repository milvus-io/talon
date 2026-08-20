# Java SDK tests against an existing Talon instance

Runs the Java client end-to-end suite (`MinioE2ETest`, dependency-free
`main()` + assertions, no JUnit) against an **already-deployed** Talon cluster
backed by a real object store. Targets a running cluster such as the one from
`test/stack/deploy.sh` or an existing production instance.

## Prerequisites

- A JDK (`javac`/`java` on `PATH` or via `JAVA_HOME`).
- A coordinator reachable from this machine (pod IP directly, or
  `kubectl port-forward svc/<release>-coordinator 17000:7000`).
- A deterministic seed object in the target bucket: bytes `i % 251`.

## Positional arguments

`test/sdk/java/run.sh [coordinator] [block_size] [bucket] [key]`
(any may be omitted; environment variables fill the gaps).

| Arg / env | Default | Meaning |
|---|---|---|
| `$1` / `TALON_E2E_COORDINATOR` | `127.0.0.1:17000` | Coordinator address (`host:port`) |
| `$2` / `TALON_E2E_BLOCK_SIZE` | `8388608` (8 MiB) | Client-side block size; may differ from the worker's block size |
| `$3` / `TALON_E2E_BUCKET` | `talon-e2e` | Bucket/container holding the seed object |
| `$4` / `TALON_E2E_KEY` | `bench` | Seed object key |

## Run

```sh
test/sdk/java/run.sh                            # defaults (local stack at :17000)
test/sdk/java/run.sh 10.0.0.5:7000 8388608      # explicit coordinator + block size
```

## Example: existing Kubernetes instance

```sh
# Find the coordinator pod IP, then run (bucket/key default from env).
export TALON_E2E_COORDINATOR="<coordinator-pod-ip>:7000"
export TALON_E2E_BUCKET="<bucket>"
test/sdk/java/run.sh "$TALON_E2E_COORDINATOR" 8388608
```

Seed an empty bucket first if needed — see
[test/sdk/python/README.md](../python/README.md) for the `mc-probe` seeding steps.
