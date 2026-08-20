#!/usr/bin/env bash
# Bring up / tear down a real distributed Talon instance for multi-language SDK
# end-to-end tests: a kind cluster, locally-built images (no registry), a real
# MinIO origin seeded with deterministic objects, and a 3-coordinator HA +
# 3-worker Talon deployment via the Helm chart.
#
# The tests under test/sdk/{python,java,c} run inside the cluster from a
# dedicated runner pod (test/docker/sdk-runner.Dockerfile), so they reach the
# coordinator via its in-cluster Service DNS and workers via their pod IPs. A
# host-side port-forward of the coordinator is still exposed for the
# membership-convergence check and debugging.
#
# Usage:
#   test/stack/deploy.sh up        # deploy and seed, leave running
#   test/stack/deploy.sh status    # show cluster + forward state
#   test/stack/deploy.sh down      # tear down (unless KEEP_CLUSTER=1)
#   test/stack/deploy.sh test-<lang>  # run one SDK suite in-cluster (stack up)
#
# Env:
#   KEEP_CLUSTER=1        keep the kind cluster after `down`/on failure
#   KIND_WORKERS=N        worker nodes (default 3)
#   COORD_PORT=NNNN       local port exposed for the coordinator (default 17000)
#   BLOCK_SIZE=NNNN       worker/client block size in bytes (default 8 MiB)
#
# Note: the in-cluster suites (test-*) build the Python wheel and the C
# staticlib on the host, then install/link them inside the Linux runner pod, so
# the host must be Linux x86_64 (as in CI) to run Python/C. On a non-Linux host
# only the Java suite works in-cluster; run the others against a reachable
# cluster instead (see test/sdk/*/README.md).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

CLUSTER_NAME="${CLUSTER_NAME:-talon-minio-e2e}"
NAMESPACE="${NAMESPACE:-talon-sdk-e2e}"
RELEASE="${RELEASE:-talon}"
IMAGE_TAG="${IMAGE_TAG:-local}"
KIND_WORKERS="${KIND_WORKERS:-3}"
KEEP_CLUSTER="${KEEP_CLUSTER:-0}"
COORD_PORT="${COORD_PORT:-17000}"
BLOCK_SIZE="${BLOCK_SIZE:-8388608}"               # 8 MiB, matches the SDK tests
L1_CAPACITY="${L1_CAPACITY:-67108864}"            # 64 MiB L1
PAGE_SIZE="${PAGE_SIZE:-262144}"                  # 256 KiB pages
ARTIFACT_DIR="${ARTIFACT_DIR:-$ROOT/.artifacts/minio-sdk-e2e}"
SEED_SIZE="${SEED_SIZE:-67108864}"                # 64 MiB seed object (i % 251)
RUNNER_IMAGE="${RUNNER_IMAGE:-talon-e2e/talon-sdk-runner:$IMAGE_TAG}"
# Coordinator address the in-cluster SDK suites dial (Service DNS, not port-forward).
COORD_IN_CLUSTER="$RELEASE-coordinator:7000"

MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
BUCKET="talon-e2e"
SEED_KEY="bench"

PF_PIDS=()

log() { printf '\n[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
fail() { echo "FAILED: $*" >&2; exit 1; }

require() { command -v "$1" >/dev/null || fail "required command not found: $1"; }

for command in kubectl helm kind docker cargo python3 curl; do
  require "$command"
done

wait_port() {
  local port="$1"
  for _ in $(seq 1 60); do
    # python socket probe instead of bash's /dev/tcp, which macOS bash 3.2
    # (Apple's default) does not support.
    if python3 - "$port" <<'PY' >/dev/null 2>&1; then
import socket, sys
s = socket.socket()
s.settimeout(1)
s.connect(("127.0.0.1", int(sys.argv[1])))
s.close()
PY
      return 0
    fi
    sleep 1
  done
  fail "port 127.0.0.1:$port did not become ready"
}

stop_port_forwards() {
  for pid in "${PF_PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  PF_PIDS=()
}

worker_pods() {
  kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/component=worker \
    --field-selector=status.phase=Running \
    -o custom-columns=NAME:.metadata.name,DELETING:.metadata.deletionTimestamp,READY:.status.containerStatuses[0].ready \
    --no-headers |
    awk '$2 == "<none>" && $3 == "true" {print $1}'
}

worker_addresses() {
  local pod
  while read -r pod; do
    kubectl -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.status.podIP}{":7001\n"}'
  done < <(worker_pods)
}

wait_membership_converged() {
  local expected actual output consecutive=0
  expected="$(worker_addresses | sort)"
  [[ "$(wc -l <<<"$expected")" -eq "$KIND_WORKERS" ]] ||
    fail "cannot validate membership without exactly $KIND_WORKERS ready workers"

  # `timeout` is GNU coreutils (absent on macOS); fall back to gtimeout, then to
  # no timeout at all — the 90-iteration loop bounds the wait either way.
  local t
  if command -v timeout >/dev/null 2>&1; then t="timeout 10s"
  elif command -v gtimeout >/dev/null 2>&1; then t="gtimeout 10s"
  else t=""; fi

  for _ in $(seq 1 90); do
    if output="$($t target/release/talon-client \
      --coordinator "127.0.0.1:$COORD_PORT" --membership-only 2>&1)"; then
      actual="$(awk '$1 == "member" {print $NF}' <<<"$output" | sort)"
      if [[ "$actual" == "$expected" ]]; then
        consecutive=$((consecutive + 1))
        [[ "$consecutive" -ge 5 ]] && return 0
      else
        consecutive=0
      fi
    else
      consecutive=0
    fi
    sleep 1
  done
  fail "coordinator membership did not converge to $KIND_WORKERS ready workers"
}

create_cluster() {
  log "creating kind cluster '$CLUSTER_NAME' (1 control-plane + $KIND_WORKERS workers)"
  local nodes="  - role: control-plane"
  local i
  for ((i = 0; i < KIND_WORKERS; i++)); do
    nodes+=$'\n  - role: worker'
  done
  kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
  kind create cluster --name "$CLUSTER_NAME" --wait 120s --config - <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
$nodes
EOF
}

build_images() {
  log "building and loading Talon images (local, no registry)"
  docker build -q -f deploy/docker/coordinator.Dockerfile \
    -t "talon-e2e/talon-coordinator:$IMAGE_TAG" .
  docker build -q -f deploy/docker/worker.Dockerfile \
    -t "talon-e2e/talon-worker:$IMAGE_TAG" .
  kind load docker-image --name "$CLUSTER_NAME" \
    "talon-e2e/talon-coordinator:$IMAGE_TAG" \
    "talon-e2e/talon-worker:$IMAGE_TAG"
}

build_clients() {
  log "building E2E client tooling"
  cargo build --release -q -p talon-client --bin talon-client
}

build_runner_image() {
  log "building and loading the SDK runner image"
  docker build -q -f test/docker/sdk-runner.Dockerfile -t "$RUNNER_IMAGE" .
  kind load docker-image --name "$CLUSTER_NAME" "$RUNNER_IMAGE"
}

deploy_minio() {
  log "deploying MinIO origin"
  kubectl create namespace "$NAMESPACE" >/dev/null 2>&1 || true
  kubectl -n "$NAMESPACE" apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
spec:
  replicas: 1
  selector:
    matchLabels: { app: minio }
  template:
    metadata:
      labels: { app: minio }
    spec:
      containers:
        - name: minio
          image: minio/minio:latest
          args: ["server", "/data"]
          env:
            - { name: MINIO_ROOT_USER, value: "$MINIO_ACCESS_KEY" }
            - { name: MINIO_ROOT_PASSWORD, value: "$MINIO_SECRET_KEY" }
          ports:
            - { name: s3, containerPort: 9000 }
          readinessProbe:
            httpGet: { path: /minio/health/ready, port: s3 }
            periodSeconds: 2
          resources:
            requests: { cpu: 100m, memory: 128Mi }
            limits: { cpu: "1", memory: 512Mi }
---
apiVersion: v1
kind: Service
metadata:
  name: minio
spec:
  selector: { app: minio }
  ports:
    - { name: s3, port: 9000, targetPort: s3 }
---
apiVersion: v1
kind: Pod
metadata:
  name: minio-client
spec:
  containers:
    - name: mc
      image: minio/mc:latest
      command: ["/bin/sh", "-c", "tail -f /dev/null"]
      resources:
        requests: { cpu: 10m, memory: 64Mi }
        limits: { cpu: 200m, memory: 512Mi }
  restartPolicy: Never
EOF
  kubectl -n "$NAMESPACE" rollout status deployment/minio --timeout=180s
  kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/minio-client --timeout=180s
  kubectl -n "$NAMESPACE" exec minio-client -- \
    mc alias set local http://minio:9000 "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY"
  kubectl -n "$NAMESPACE" exec minio-client -- mc mb --ignore-existing "local/$BUCKET"
}

deploy_talon() {
  log "deploying Talon via Helm (3 coordinators HA, $KIND_WORKERS workers, kubernetes backend)"
  kubectl -n "$NAMESPACE" create secret generic talon-worker-backend \
    --from-literal=azure-account=unused --from-literal=azure-sas=unused \
    --dry-run=client -o yaml | kubectl -n "$NAMESPACE" apply -f -

  helm upgrade --install "$RELEASE" deploy/helm/talon -n "$NAMESPACE" \
    --set image.registry=talon-e2e \
    --set image.tag="$IMAGE_TAG" \
    --set image.pullPolicy=Never \
    --set coordinator.backend=kubernetes \
    --set coordinator.replicas=3 \
    --set coordinator.clusterId=sdk-e2e \
    --set worker.replicas="$KIND_WORKERS" \
    --set worker.topologySpreadWhenUnsatisfiable=DoNotSchedule \
    --set worker.blockSizeBytes="$BLOCK_SIZE" \
    --set worker.capacityBytes=$((64 * BLOCK_SIZE)) \
    --set worker.l1CapacityBytes="$L1_CAPACITY" \
    --set worker.l1PageSizeBytes="$PAGE_SIZE" \
    --set worker.resources.requests.cpu=100m \
    --set worker.resources.requests.memory=128Mi \
    --set worker.resources.limits.cpu=2 \
    --set worker.resources.limits.memory=512Mi \
    --wait --timeout 5m >"$ARTIFACT_DIR/helm.log" 2>&1 || {
      tail -60 "$ARTIFACT_DIR/helm.log" >&2
      fail "helm upgrade failed; full log in $ARTIFACT_DIR/helm.log"
    }

  # Point the workers at the MinIO origin (S3, path-style) instead of the
  # chart's placeholder Azure backend.
  #
  # FORCE_TOKIO_DATA_PLANE=1: kind's Docker-based nodes typically block io_uring
  # (seccomp/sysctl), so this e2e intentionally exercises the Tokio data-plane
  # fallback; the io_uring path is covered by benches/dataplane_benches.rs and
  # scripts/dataplane_loadtest.sh (which require a bare-metal io_uring host).
  kubectl -n "$NAMESPACE" set env "deployment/$RELEASE-worker" \
    TALON_WORKER_BACKEND=s3 \
    TALON_WORKER_S3_REGION=us-east-1 \
    TALON_WORKER_S3_ENDPOINT=http://minio:9000 \
    TALON_WORKER_S3_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" \
    TALON_WORKER_S3_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
    TALON_WORKER_S3_PATH_STYLE=true \
    TALON_WORKER_FORCE_TOKIO_DATA_PLANE=1 >/dev/null

  log "waiting for $KIND_WORKERS ready workers"
  local ready=0
  for _ in $(seq 1 90); do
    ready="$(worker_pods | wc -l | tr -d ' ')"
    [[ "$ready" -eq "$KIND_WORKERS" ]] && break
    sleep 1
  done
  [[ "$ready" -eq "$KIND_WORKERS" ]] ||
    fail "expected $KIND_WORKERS ready workers, found $ready"
}

seed_objects() {
  log "seeding deterministic objects (byte i == i % 251)"
  mkdir -p "$ARTIFACT_DIR/seed"
  python3 - "$SEED_KEY" "$SEED_SIZE" "$ARTIFACT_DIR/seed" <<'PY'
import sys
key, size, out = sys.argv[1], int(sys.argv[2]), sys.argv[3]
path = f"{out}/{key}"
# Stream a (i % 251) ramp without materializing the whole 64 MiB at once.
chunk = bytes((i % 251) for i in range(251))
with open(path, "wb") as f:
    written = 0
    while written < size:
        take = min(len(chunk), size - written)
        f.write(chunk[:take])
        written += take
print(f"{path}: {written} bytes")
PY

  kubectl -n "$NAMESPACE" exec -i minio-client -- \
    sh -c 'mkdir -p /tmp/seed && cat >/tmp/seed/'"$SEED_KEY" <"$ARTIFACT_DIR/seed/$SEED_KEY"
  kubectl -n "$NAMESPACE" exec minio-client -- \
    mc cp "/tmp/seed/$SEED_KEY" "local/$BUCKET/$SEED_KEY" >/dev/null
  kubectl -n "$NAMESPACE" exec minio-client -- \
    mc stat "local/$BUCKET/$SEED_KEY"
}

expose_coordinator() {
  log "exposing coordinator at 127.0.0.1:$COORD_PORT (port-forward svc/$RELEASE-coordinator:7000)"
  stop_port_forwards
  kubectl -n "$NAMESPACE" port-forward "svc/$RELEASE-coordinator" "$COORD_PORT:7000" \
    >"$ARTIFACT_DIR/coordinator-port-forward.log" 2>&1 &
  PF_PIDS+=("$!")
  wait_port "$COORD_PORT"
}

deploy_runner() {
  log "deploying in-cluster SDK runner pod"
  kubectl -n "$NAMESPACE" run talon-sdk-runner \
    --image="$RUNNER_IMAGE" --restart=Never --image-pull-policy=Never \
    --command -- tail -f /dev/null
  kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/talon-sdk-runner --timeout=120s
  kubectl -n "$NAMESPACE" exec talon-sdk-runner -- \
    mkdir -p /e2e/wheel /e2e/python /e2e/classes /e2e/c-include
}

test_python() {
  log "running Python SDK suite in-cluster"
  require maturin
  if ! ls target/talon-wheel/*.whl >/dev/null 2>&1; then
    maturin build --release --manifest-path clients/python/Cargo.toml \
      --out target/talon-wheel
  fi
  kubectl -n "$NAMESPACE" cp target/talon-wheel/. talon-sdk-runner:/e2e/wheel/
  kubectl -n "$NAMESPACE" cp test/sdk/python/. talon-sdk-runner:/e2e/python/
  kubectl -n "$NAMESPACE" exec talon-sdk-runner -- \
    sh -c 'pip install --break-system-packages /e2e/wheel/*.whl'
  kubectl -n "$NAMESPACE" exec talon-sdk-runner -- \
    env TALON_E2E_COORDINATOR="$COORD_IN_CLUSTER" \
        TALON_E2E_BLOCK_SIZE="$BLOCK_SIZE" \
        TALON_E2E_BUCKET="$BUCKET" TALON_E2E_KEY="$SEED_KEY" \
    python3 -m pytest /e2e/python/ -v
}

test_java() {
  log "running Java SDK suite in-cluster"
  require javac
  classes="$(mktemp -d /tmp/talon-java-cls.XXXXXX)"
  trap 'rm -rf "$classes"' EXIT
  javac --release 17 -d "$classes" \
    clients/java/src/main/java/io/milvus/talon/*.java \
    test/sdk/java/MinioE2ETest.java
  kubectl -n "$NAMESPACE" cp "$classes/." talon-sdk-runner:/e2e/classes/
  kubectl -n "$NAMESPACE" exec talon-sdk-runner -- \
    java -cp /e2e/classes io.milvus.talon.MinioE2ETest \
      "$COORD_IN_CLUSTER" "$BLOCK_SIZE" "$BUCKET" "$SEED_KEY"
}

test_c() {
  log "running C SDK suite in-cluster"
  require cc
  cargo build --release -q -p talon-c --locked
  kubectl -n "$NAMESPACE" cp clients/c/include/. talon-sdk-runner:/e2e/c-include/
  kubectl -n "$NAMESPACE" cp target/release/libtalon_c.a talon-sdk-runner:/e2e/libtalon_c.a
  kubectl -n "$NAMESPACE" cp test/sdk/c/minio_e2e.c talon-sdk-runner:/e2e/minio_e2e.c
  kubectl -n "$NAMESPACE" exec talon-sdk-runner -- \
    cc -std=c11 -Wall -Wextra -O2 -I/e2e/c-include \
      /e2e/minio_e2e.c /e2e/libtalon_c.a -lpthread -lm -o /e2e/minio_e2e
  kubectl -n "$NAMESPACE" exec talon-sdk-runner -- \
    /e2e/minio_e2e "$COORD_IN_CLUSTER" "$BLOCK_SIZE" "$BUCKET" "$SEED_KEY"
}

cmd_up() {
  mkdir -p "$ARTIFACT_DIR"
  build_clients
  create_cluster
  build_images
  build_runner_image
  deploy_minio
  deploy_talon
  expose_coordinator
  wait_membership_converged
  seed_objects
  deploy_runner
  log "stack is up: coordinator 127.0.0.1:$COORD_PORT (host) / $COORD_IN_CLUSTER (in-cluster), bucket s3://$BUCKET/$SEED_KEY"
  kubectl -n "$NAMESPACE" get pods -o wide
}

cmd_down() {
  log "tearing down"
  stop_port_forwards
  kubectl -n "$NAMESPACE" get pods -o wide >"$ARTIFACT_DIR/pods-final.txt" 2>/dev/null || true
  kubectl -n "$NAMESPACE" logs -l app.kubernetes.io/component=worker --all-containers --prefix \
    >"$ARTIFACT_DIR/workers.log" 2>/dev/null || true
  kubectl -n "$NAMESPACE" logs -l app.kubernetes.io/component=coordinator --all-containers --prefix \
    >"$ARTIFACT_DIR/coordinators.log" 2>/dev/null || true
  kubectl -n "$NAMESPACE" get events --sort-by=.lastTimestamp >"$ARTIFACT_DIR/events.txt" 2>/dev/null || true
  # --wait=false: a namespace can linger on PVC/finalizer teardown; the kind
  # cluster deletion below reclaims everything regardless.
  kubectl delete namespace "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [[ "$KEEP_CLUSTER" != "1" ]]; then
    kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
  fi
  log "down; artifacts in $ARTIFACT_DIR"
}

cmd_status() {
  echo "cluster: $CLUSTER_NAME"
  kind get clusters | grep -qx "$CLUSTER_NAME" && echo "  kind cluster: up" || echo "  kind cluster: down"
  kubectl -n "$NAMESPACE" get pods -o wide 2>/dev/null || echo "  (no pods in $NAMESPACE)"
  echo "forward: coordinator 127.0.0.1:$COORD_PORT"
}

case "${1:-status}" in
  up)          trap 'cmd_down' EXIT; cmd_up; trap - EXIT ;;
  down)        cmd_down ;;
  status)      cmd_status ;;
  test-python) test_python ;;
  test-java)   test_java ;;
  test-c)      test_c ;;
  *)           echo "usage: $0 [up|down|status|test-python|test-java|test-c]" >&2; exit 2 ;;
esac
