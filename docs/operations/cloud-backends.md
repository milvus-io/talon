# Cloud backends (S3, GCS, Azure)

Talon caches objects from a cloud object store. A worker selects **one** backend
at startup and reads every object from it. All three major clouds are
first-class and covered by an end-to-end test suite (see
[Testing against emulators](#testing-against-emulators)).

The backend is chosen with `TALON_WORKER_BACKEND` (`azure` — the default — `s3`,
or `gcs`). Endpoints come from configuration; **secrets come only from the
environment** and are never read from a config file or logged. The full list is
in the [configuration reference](../reference/configuration.md).

## Amazon S3 (and S3-compatible)

Authentication is AWS Signature v4, computed per request. Works against AWS S3
and S3-compatible stores (MinIO, Ceph, LocalStack) via an endpoint override.

| Setting | Env var | Notes |
|---------|---------|-------|
| Backend | `TALON_WORKER_BACKEND=s3` | |
| Region | `TALON_WORKER_S3_REGION` | required, e.g. `us-east-1` |
| Endpoint | `TALON_WORKER_S3_ENDPOINT` | override for MinIO/LocalStack; `http://` selects plaintext |
| Path style | `TALON_WORKER_S3_PATH_STYLE` | `true` for most S3-compatible stores |
| Access key id | `TALON_WORKER_S3_ACCESS_KEY_ID` | not secret |
| Secret key | `TALON_WORKER_S3_SECRET_ACCESS_KEY` | **env-only** |
| Session token | `TALON_WORKER_S3_SESSION_TOKEN` | optional (STS), **env-only** |

```sh
export TALON_WORKER_BACKEND=s3
export TALON_WORKER_S3_REGION=us-east-1
export TALON_WORKER_S3_ACCESS_KEY_ID=AKIA...
export TALON_WORKER_S3_SECRET_ACCESS_KEY=...      # env-only
talon-worker --coordinator talon-coordinator:7000 --cluster-id demo
```

## Google Cloud Storage

Authentication is an OAuth2 bearer token supplied via the environment (mint it
with your service account / workload identity out of band). Works against GCS and
fake-gcs-server via an endpoint override.

| Setting | Env var | Notes |
|---------|---------|-------|
| Backend | `TALON_WORKER_BACKEND=gcs` | |
| Endpoint | `TALON_WORKER_GCS_ENDPOINT` | override for fake-gcs-server; `http://` selects plaintext |
| Bearer token | `TALON_WORKER_GCS_BEARER_TOKEN` | **env-only** |

```sh
export TALON_WORKER_BACKEND=gcs
export TALON_WORKER_GCS_BEARER_TOKEN="$(gcloud auth print-access-token)"  # env-only
talon-worker --coordinator talon-coordinator:7000 --cluster-id demo
```

## Azure Blob Storage

Two authentication modes:

- **SAS token** (`TALON_WORKER_AZURE_SAS`, env-only) carried in the URL query, or
- **Shared Key** (the account key) signed per request.

Works against Azure and Azurite via an endpoint override (which also enables
path-style addressing for the emulator).

| Setting | Env var | Notes |
|---------|---------|-------|
| Backend | `TALON_WORKER_BACKEND=azure` (default) | |
| Account | `TALON_WORKER_AZURE_ACCOUNT` | required |
| Endpoint | `TALON_WORKER_AZURE_ENDPOINT` | override for Azurite; `http://` selects plaintext + path-style |
| SAS token | `TALON_WORKER_AZURE_SAS` | **env-only** |

```sh
export TALON_WORKER_BACKEND=azure
export TALON_WORKER_AZURE_ACCOUNT=mystorageacct
export TALON_WORKER_AZURE_SAS="sv=...&sig=..."     # env-only
talon-worker --coordinator talon-coordinator:7000 --cluster-id demo
```

## Workload identity (keyless)

On Kubernetes, every major cloud can hand the pod an identity instead of a
static key. When no static credential is configured, the worker and the
gateway resolve one of these mechanisms at startup and then refresh the
short-lived credentials in the background (at roughly 80% of their lifetime,
clamped between 1 and 15 minutes of margin). A failed refresh keeps the last
credentials, logs a warning without secret material, and retries every 30
seconds; a failed **initial** resolution is a startup error, exactly like a
missing static key.

| Cloud | Mechanism | Detection | Verified |
|---|---|---|---|
| AWS EKS | IRSA: `sts:AssumeRoleWithWebIdentity` | auto: `AWS_ROLE_ARN` + `AWS_WEB_IDENTITY_TOKEN_FILE` (webhook-injected) | live EKS: exchange + signed request |
| Alibaba ACK | RRSA: `sts:AssumeRoleWithOIDC` | auto: `ALIBABA_CLOUD_ROLE_ARN` + `ALIBABA_CLOUD_OIDC_PROVIDER_ARN` + `ALIBABA_CLOUD_OIDC_TOKEN_FILE` | live ACK: exchange + signed request |
| Tencent TKE | OIDC: `sts:AssumeRoleWithWebIdentity` | auto: `TKE_ROLE_ARN` + `TKE_PROVIDER_ID` + `TKE_WEB_IDENTITY_TOKEN_FILE` | protocol-level |
| Huawei CCE | agency metadata (`/openstack/latest/securitykey`) | explicit: `TALON_ORIGIN_CREDENTIALS_SOURCE=huawei-agency` | protocol-level; requires an agency bound to the node |
| GCP GKE | Workload Identity metadata token (GCS `Bearer`) | explicit: `TALON_ORIGIN_CREDENTIALS_SOURCE=gcp-metadata` | protocol-level |
| Azure AKS | Workload Identity: Azure AD token (`Bearer`) | auto when no SAS/key: `AZURE_CLIENT_ID` + `AZURE_TENANT_ID` + `AZURE_FEDERATED_TOKEN_FILE`; pods also need the `azure.workload.identity/use: "true"` label | protocol-level; injected env contract verified on live AKS |

Static credentials always win, so existing deployments are unaffected.
`TALON_ORIGIN_CREDENTIALS_SOURCE` (shared by the worker and the gateway)
forces one mechanism — `static`, `aws-web-identity`, `aliyun-oidc`,
`tencent-oidc`, `huawei-agency`, or `gcp-metadata` — instead of
auto-detection; the GCP and Huawei metadata endpoints have no env marker and
are never probed implicitly. Aliyun, Tencent, and Huawei OBS are consumed
through their S3-compatible endpoints with SigV4 plus the temporary security
token, the same shape their official Milvus storage integrations use. OSS and
COS require virtual-host addressing (path style is rejected with `403`) and a
bare region in the signing scope (`cn-hangzhou`, not `oss-cn-hangzhou`).
"Protocol-level" means the exchange is implemented and tested against the
documented API shape but has not yet been validated against a live cluster.
To validate a cluster, build the verification probe
(`cargo build -p talon-backend --example credentials_probe`) and run it in a
pod bound to the target ServiceAccount: it resolves the production chain and
HEADs one key with the resolved credentials, printing only redacted material.
A `NotFound` answer proves the exchange and the signature end to end.

Refresh outcomes are counted as
`talon_worker_origin_credentials_total{result="refresh_success"|"refresh_failure"}`
(the gateway equivalent is `talon_gateway_origin_credentials_total`), and
`talon_worker_origin_credentials_expiry_seconds` publishes the current
credentials' hard expiry so an alert can fire before it passes.

## Testing against emulators

Each backend has an end-to-end test that reads a real object through the actual
signing/auth path against a local emulator — LocalStack (S3), fake-gcs-server
(GCS), and Azurite (Azure). The tests skip unless their `TALON_*_TEST_ENDPOINT`
is set, and CI runs all three (the `s3-e2e`, `gcs-e2e`, and `azure-e2e` jobs).

All three run the **same conformance suite** (`crates/talon-backend/tests/conformance`),
so every backend is held to one contract: HEAD size + version, exact ranged-read
bytes at several offsets, whole-object reads, tail-past-EOF clamping, `If-Match`
preconditions, and `NotFound` for a missing object.

Run one locally — for example S3 against LocalStack:

```sh
docker compose -f deploy/testenv/s3-localstack.yml up -d
python3 -c "open('/tmp/object.bin','wb').write(bytes(i%251 for i in range(4096)))"
aws --endpoint-url http://127.0.0.1:4566 s3 mb s3://talon-e2e
aws --endpoint-url http://127.0.0.1:4566 s3 cp /tmp/object.bin s3://talon-e2e/e2e/object.bin

AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  TALON_S3_TEST_ENDPOINT=http://127.0.0.1:4566 \
  TALON_S3_TEST_BUCKET=talon-e2e TALON_S3_TEST_KEY=e2e/object.bin \
  cargo test -p talon-backend --test s3_e2e -- --nocapture
```

The `deploy/testenv/` directory has compose files for the S3 and GCS emulators;
Azurite is included in the [latency lab](../testing/latency-lab.md) stack.

## Secrets

Credentials (`*_SECRET_ACCESS_KEY`, `*_SESSION_TOKEN`, `*_BEARER_TOKEN`,
`*_AZURE_SAS`) are read **only** from the environment — never from a config file,
and never logged. Inject them with your platform's secret mechanism (Kubernetes
Secrets, a secrets manager) rather than baking them into images or manifests —
or configure no static credential at all and let
[workload identity](#workload-identity-keyless) keep the cluster keyless.
