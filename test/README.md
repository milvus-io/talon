# Talon multi-language SDK end-to-end tests

A real distributed Talon instance backed by **MinIO**, driven by the **Python,
Java, and C** SDK end-to-end suites. This is the only place all three language
clients exercise the full `SDK -> worker -> object store` chain against a real
object store in one deployment.

## What gets deployed

```
kind cluster (1 control-plane + 3 workers, local-built images)
├── MinIO origin  (S3, path-style, deterministic seed: byte i == i % 251)
└── Talon via Helm
    ├── 3 HA coordinators (kubernetes Lease backend)
    └── 3 workers         (block size 8 MiB, L1 64 MiB, backend → MinIO)
```

The SDK tests connect only to the coordinator, exposed via
`kubectl port-forward svc/talon-coordinator 17000:7000`. The coordinator answers
membership/placement; workers serve bytes, fetching from MinIO on miss.

## Quick start

```sh
# Build client tooling + deploy + run all three SDK suites + tear down
test/run_all.sh

# Or drive the stack manually
test/stack/deploy.sh up        # deploy and seed, leave running
test/run_all.sh python         # run a single suite (stack must be up)
test/run_all.sh java
test/run_all.sh c
test/stack/deploy.sh down      # tear down (KEEP_CLUSTER=1 to keep the kind cluster)
```

Environment (defaults): `TALON_E2E_COORDINATOR=127.0.0.1:17000`,
`TALON_E2E_BLOCK_SIZE=8388608`, `TALON_E2E_BUCKET=talon-e2e`,
`TALON_E2E_KEY=bench`.

## Layout

```
test/
├── README.md
├── run_all.sh                  # deploy → run all three suites → tear down
├── stack/
│   └── deploy.sh               # kind + local images + MinIO + Helm + seed + forward
└── sdk/
    ├── python/
    │   ├── README.md           # run the Python suite against an existing instance
    │   └── test_minio.py       # pytest (talon.Client, s3:// URI)
    ├── java/
    │   ├── README.md           # run the Java suite against an existing instance
    │   ├── MinioE2ETest.java   # dependency-free main() + assertions (like E2ETest)
    │   └── run.sh
    └── c/
        ├── README.md           # run the C suite against an existing instance
        ├── minio_e2e.c         # dependency-free C ABI test (talon.h)
        └── run.sh
```

## Running against an existing instance

The `run_all.sh` path deploys its own stack. To point the suites at a cluster
that is already running (a production deployment, or a MinIO-backed instance you
deployed separately), read the per-SDK READMEs — they cover how to reach the
coordinator (pod IP or port-forward), how to seed the `i % 251` ramp object into
an empty bucket, and the exact env/args:

- [Python](sdk/python/README.md)
- [Java](sdk/java/README.md)
- [C](sdk/c/README.md)

## What the SDK suites assert

All suites share the same semantics against `s3://talon-e2e/bench`, whose bytes
are the deterministic ramp `i % 251`:

| Case | Assertion | Python | Java | C |
|---|---|---|---|---|
| stat | size > 0, version = MinIO ETag (non-empty) | ✅ | ✅ | ✅ |
| auto version resolution | `read` without a version resolves via stat (#318) | ✅ | ✅ | — |
| exact read (offset 0) | bytes == ramp(0, 4096) | ✅ | ✅ | ✅ |
| offset read | bytes == ramp(1000, 8192) | ✅ | ✅ | ✅ |
| cross-block read | `block_size + 4 MiB` reassembled in order | ✅ | ✅ | ✅ |
| single boundary read | `block_size - 2048`, 4096 bytes | ✅ | ✅ | ✅ |
| zero-length read | empty | ✅ | ✅ | ✅ |
| placement cache repeated read | two reads return identical bytes | — | ✅ | — |
| concurrent reads (8 threads) | each offset byte-exact | ✅ | ✅ | — |
| bad URI | rejected before any I/O | ✅ | ✅ | ✅ |

Note: the C suite (minio_e2e.c) is a dependency-free main() and omits the
threaded and placement-cache cases; the Java suite additionally verifies the
client-side placement cache serves repeated reads.

## CI

`.github/workflows/minio-sdk-e2e.yml` runs the same path on pull requests
touching `test/**`, the clients, or the worker/coordinator: kind cluster →
local image build → Helm deploy → seed → Python → Java → C → tear down, with
artifacts collected on any result.
