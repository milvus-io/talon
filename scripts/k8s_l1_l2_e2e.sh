#!/usr/bin/env bash
# Multi-node Kubernetes E2E and benchmark for the inclusive L1/L2 worker cache.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CLUSTER_NAME="${CLUSTER_NAME:-talon-l1-l2}"
NAMESPACE="${NAMESPACE:-talon-e2e}"
RELEASE="${RELEASE:-talon}"
IMAGE_TAG="${IMAGE_TAG:-e2e}"
CREATE_KIND_CLUSTER="${CREATE_KIND_CLUSTER:-1}"
KEEP_CLUSTER="${KEEP_CLUSTER:-0}"
BENCH_SECONDS="${BENCH_SECONDS:-5}"
ARTIFACT_DIR="${ARTIFACT_DIR:-$ROOT/.artifacts/k8s-l1-l2}"
COORD_PORT="${COORD_PORT:-17000}"
WORKER_PORT="${WORKER_PORT:-17001}"
BLOCK_SIZE=$((1024 * 1024))
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
BUCKET="talon-e2e"

mkdir -p "$ARTIFACT_DIR"
PF_PIDS=()

log() {
  printf '\n[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"
}

fail() {
  echo "FAILED: $*" >&2
  exit 1
}

cleanup() {
  for pid in "${PF_PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  kubectl -n "$NAMESPACE" get pods -o wide >"$ARTIFACT_DIR/pods-final.txt" 2>/dev/null || true
  kubectl -n "$NAMESPACE" logs -l app.kubernetes.io/component=worker --all-containers \
    --prefix >"$ARTIFACT_DIR/workers.log" 2>/dev/null || true
  kubectl -n "$NAMESPACE" logs -l app.kubernetes.io/component=worker --all-containers \
    --prefix --previous >"$ARTIFACT_DIR/workers-previous.log" 2>/dev/null || true
  kubectl -n "$NAMESPACE" logs -l app.kubernetes.io/component=coordinator --all-containers \
    --prefix >"$ARTIFACT_DIR/coordinators.log" 2>/dev/null || true
  kubectl -n "$NAMESPACE" describe pods >"$ARTIFACT_DIR/pods-describe.txt" 2>/dev/null || true
  kubectl -n "$NAMESPACE" get events --sort-by=.lastTimestamp \
    >"$ARTIFACT_DIR/events.txt" 2>/dev/null || true
  if [[ "$CREATE_KIND_CLUSTER" == "1" && "$KEEP_CLUSTER" != "1" ]]; then
    kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

require() {
  command -v "$1" >/dev/null || fail "required command not found: $1"
}

for command in kubectl helm cargo curl awk sort uniq cmp dd kind docker timeout; do
  require "$command"
done

wait_port() {
  local port="$1"
  for _ in $(seq 1 60); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      exec 3>&-
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

start_coordinator_forward() {
  kubectl -n "$NAMESPACE" port-forward "svc/$RELEASE-coordinator" "$COORD_PORT:7000" \
    >"$ARTIFACT_DIR/coordinator-port-forward.log" 2>&1 &
  PF_PIDS+=("$!")
  wait_port "$COORD_PORT"
  wait_membership_converged
}

start_worker_forward() {
  local pod="$1"
  kubectl -n "$NAMESPACE" port-forward "pod/$pod" "$WORKER_PORT:7001" \
    >"$ARTIFACT_DIR/worker-port-forward.log" 2>&1 &
  PF_PIDS+=("$!")
  wait_port "$WORKER_PORT"
}

worker_pods() {
  kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/component=worker \
    --field-selector=status.phase=Running \
    -o custom-columns=NAME:.metadata.name,DELETING:.metadata.deletionTimestamp,READY:.status.containerStatuses[0].ready \
    --no-headers |
    awk '$2 == "<none>" && $3 == "true" {print $1}'
}

worker_addresses() {
  while read -r pod; do
    kubectl -n "$NAMESPACE" get pod "$pod" \
      -o jsonpath='{.status.podIP}{":7001\n"}'
  done < <(worker_pods)
}

wait_membership_converged() {
  local expected actual output
  local consecutive=0
  expected="$(worker_addresses | sort)"
  [[ "$(wc -l <<<"$expected")" -eq 3 ]] ||
    fail "cannot validate membership without exactly 3 ready workers"

  for _ in $(seq 1 90); do
    if output="$(timeout 10s target/release/talon-client \
      --coordinator "127.0.0.1:$COORD_PORT" --membership-only 2>&1)"; then
      actual="$(awk '$1 == "member" {print $NF}' <<<"$output" | sort)"
      if [[ "$actual" == "$expected" ]]; then
        consecutive=$((consecutive + 1))
        if [[ "$consecutive" -ge 5 ]]; then
          printf '%s\n' "$output" >"$ARTIFACT_DIR/membership-converged.txt"
          return 0
        fi
      else
        consecutive=0
      fi
    else
      consecutive=0
    fi
    sleep 1
  done

  {
    echo "expected:"
    echo "$expected"
    echo "last coordinator response:"
    echo "${output:-<none>}"
  } >"$ARTIFACT_DIR/membership-failure.txt"
  fail "coordinator membership did not converge to the 3 ready worker pods"
}

first_worker() {
  worker_pods | sort | head -1
}

assert_workers_spread() {
  local ready=0 nodes pod
  for _ in $(seq 1 60); do
    ready="$(worker_pods | wc -l)"
    [[ "$ready" -eq 3 ]] && break
    sleep 1
  done
  [[ "$ready" -eq 3 ]] || fail "expected 3 ready workers, found $ready"
  nodes="$(
    while read -r pod; do
      kubectl -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.spec.nodeName}{"\n"}'
    done < <(worker_pods)
  )"
  nodes="$(sort -u <<<"$nodes" | wc -l)"
  [[ "$nodes" -eq 3 ]] || fail "expected workers on 3 Kubernetes nodes, found $nodes"
}

rollout_workers() {
  kubectl -n "$NAMESPACE" rollout status "deployment/$RELEASE-worker" --timeout=180s
  assert_workers_spread
}

set_cache_limits() {
  local l1="$1"
  local l2="$2"
  kubectl -n "$NAMESPACE" set env "deployment/$RELEASE-worker" \
    "TALON_WORKER_L1_CAPACITY_BYTES=$l1" \
    "TALON_WORKER_L1_MAX_ENTRY_BYTES=$BLOCK_SIZE" \
    "TALON_WORKER_CAPACITY_BYTES=$l2" >/dev/null
  rollout_workers
}

metric_value() {
  local pod="$1"
  local prefix="$2"
  kubectl -n "$NAMESPACE" exec "$pod" -- \
    curl -fsS http://127.0.0.1:8001/metrics |
    awk -v prefix="$prefix" 'index($1, prefix) == 1 {sum += $2} END {printf "%.0f", sum + 0}'
}

metric_sum() {
  local prefix="$1"
  local sum=0 value
  while read -r pod; do
    value="$(metric_value "$pod" "$prefix")"
    sum=$((sum + value))
  done < <(worker_pods)
  echo "$sum"
}

make_repeated_file() {
  local byte="$1"
  local size="$2"
  local path="$3"
  head -c "$size" /dev/zero | tr '\000' "$byte" >"$path"
}

upload_file() {
  local source="$1"
  local key="$2"
  kubectl -n "$NAMESPACE" exec -i minio-client -- \
    mc pipe "local/$BUCKET/$key" <"$source"
}

stage_file() {
  local source="$1"
  local key="$2"
  # shellcheck disable=SC2016 # $1 expands in the container shell.
  kubectl -n "$NAMESPACE" exec -i minio-client -- \
    sh -c 'mkdir -p /tmp/fixtures && cat >"/tmp/fixtures/$1"' sh "$key" <"$source"
}

direct_read() {
  local path="$1"
  local offset="$2"
  local len="$3"
  local out="$4"
  target/release/talon-client \
    --worker "127.0.0.1:$WORKER_PORT" \
    --path "/s3/$BUCKET/$path" --offset "$offset" --len "$len" --out "$out"
}

placed_read() {
  local path="$1"
  local len="$2"
  local out="$3"
  timeout 15s target/release/talon-client \
    --coordinator "127.0.0.1:$COORD_PORT" \
    --placement-only --path "/s3/$BUCKET/$path" --len "$len" >"$out"
  cat "$out"
}

if [[ "$CREATE_KIND_CLUSTER" == "1" ]]; then
  log "creating a Kubernetes cluster with one control plane and three workers"
  kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
  kind create cluster --name "$CLUSTER_NAME" --wait 120s --config=- <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
  - role: worker
EOF

  log "building and loading Talon images"
  docker build -f deploy/docker/coordinator.Dockerfile \
    -t "talon-e2e/talon-coordinator:$IMAGE_TAG" .
  docker build -f deploy/docker/worker.Dockerfile \
    -t "talon-e2e/talon-worker:$IMAGE_TAG" .
  kind load docker-image --name "$CLUSTER_NAME" \
    "talon-e2e/talon-coordinator:$IMAGE_TAG" \
    "talon-e2e/talon-worker:$IMAGE_TAG"
fi

log "building E2E clients"
cargo build --release -p talon-client --bin talon-client
cargo build --release -p talon-worker --bin talon-loadgen

log "deploying MinIO and Talon"
kubectl create namespace "$NAMESPACE"
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
      command: ["/bin/sh", "-c", "sleep 7200"]
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

kubectl -n "$NAMESPACE" create secret generic talon-worker-backend \
  --from-literal=azure-account=unused --from-literal=azure-sas=unused

helm upgrade --install "$RELEASE" deploy/helm/talon -n "$NAMESPACE" \
  --set image.registry=talon-e2e \
  --set image.tag="$IMAGE_TAG" \
  --set image.pullPolicy=Never \
  --set coordinator.backend=kubernetes \
  --set coordinator.replicas=3 \
  --set coordinator.clusterId=e2e \
  --set worker.replicas=3 \
  --set worker.topologySpreadWhenUnsatisfiable=DoNotSchedule \
  --set worker.blockSizeBytes=$BLOCK_SIZE \
  --set worker.capacityBytes=$((64 * BLOCK_SIZE)) \
  --set worker.l1CapacityBytes=$((32 * BLOCK_SIZE)) \
  --set worker.l1MaxEntryBytes=$BLOCK_SIZE \
  --set worker.resources.requests.cpu=100m \
  --set worker.resources.requests.memory=128Mi \
  --set worker.resources.limits.cpu=2 \
  --set worker.resources.limits.memory=512Mi \
  --wait --timeout 5m

kubectl -n "$NAMESPACE" set env "deployment/$RELEASE-worker" \
  TALON_WORKER_BACKEND=s3 \
  TALON_WORKER_S3_REGION=us-east-1 \
  TALON_WORKER_S3_ENDPOINT=http://minio:9000 \
  TALON_WORKER_S3_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" \
  TALON_WORKER_S3_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
  TALON_WORKER_S3_PATH_STYLE=true \
  TALON_WORKER_BACKEND_DELAY_MS=50 \
  TALON_WORKER_FORCE_TOKIO_DATA_PLANE=1 >/dev/null
rollout_workers

kubectl -n "$NAMESPACE" get pods -o wide | tee "$ARTIFACT_DIR/pods-initial.txt"
start_coordinator_forward

log "seeding deterministic objects"
make_repeated_file H "$BLOCK_SIZE" "$ARTIFACT_DIR/hot.bin"
make_repeated_file S "$BLOCK_SIZE" "$ARTIFACT_DIR/singleflight.bin"
make_repeated_file O "$BLOCK_SIZE" "$ARTIFACT_DIR/version-old.bin"
make_repeated_file N "$BLOCK_SIZE" "$ARTIFACT_DIR/version-new.bin"
make_repeated_file R "$BLOCK_SIZE" "$ARTIFACT_DIR/restart.bin"
make_repeated_file B $((16 * BLOCK_SIZE)) "$ARTIFACT_DIR/bench"
{
  head -c "$BLOCK_SIZE" /dev/zero | tr '\000' A
  head -c "$BLOCK_SIZE" /dev/zero | tr '\000' B
  head -c "$BLOCK_SIZE" /dev/zero | tr '\000' C
} >"$ARTIFACT_DIR/cross.bin"

for object in hot.bin singleflight.bin version.bin restart.bin bench cross.bin; do
  case "$object" in
    version.bin) stage_file "$ARTIFACT_DIR/version-old.bin" "$object" ;;
    *) stage_file "$ARTIFACT_DIR/$object" "$object" ;;
  esac
done
for i in $(seq 0 7); do
  make_repeated_file "$(printf '\\%03o' $((65 + i)))" "$BLOCK_SIZE" "$ARTIFACT_DIR/evict-$i.bin"
  stage_file "$ARTIFACT_DIR/evict-$i.bin" "evict-$i.bin"
done
# shellcheck disable=SC2016 # Loop variables expand in the container shell.
kubectl -n "$NAMESPACE" exec minio-client -- sh -c \
  'i=0; while [ "$i" -lt 30 ]; do cp /tmp/fixtures/hot.bin "/tmp/fixtures/shard-$i.bin"; i=$((i + 1)); done'
kubectl -n "$NAMESPACE" exec minio-client -- \
  mc mirror --overwrite /tmp/fixtures "local/$BUCKET"

log "verifying multi-node placement reaches all three workers"
declare -A placed_workers=()
for i in $(seq 0 29); do
  output="$(placed_read "shard-$i.bin" 4096 "$ARTIFACT_DIR/shard-$i.out" 2>&1)"
  echo "$output" >>"$ARTIFACT_DIR/placement.log"
  address="$(awk '$1 == "placed" {print $NF}' <<<"$output")"
  [[ -n "$address" ]] || fail "could not parse worker address from client output"
  placed_workers["$address"]=1
done
[[ "${#placed_workers[@]}" -eq 3 ]] ||
  fail "expected placement reads to reach 3 workers, reached ${#placed_workers[@]}"

log "verifying byte-exact L1 hit and a cross-block read"
stop_port_forwards
start_coordinator_forward
TARGET_POD="$(first_worker)"
start_worker_forward "$TARGET_POD"
head -c 65536 "$ARTIFACT_DIR/hot.bin" >"$ARTIFACT_DIR/hot.expected"
direct_read hot.bin 0 65536 "$ARTIFACT_DIR/hot-cold.out" |
  tee "$ARTIFACT_DIR/hot-cold.log"
direct_read hot.bin 0 65536 "$ARTIFACT_DIR/hot-warm.out" |
  tee "$ARTIFACT_DIR/hot-warm.log"
cmp "$ARTIFACT_DIR/hot.expected" "$ARTIFACT_DIR/hot-cold.out"
cmp "$ARTIFACT_DIR/hot.expected" "$ARTIFACT_DIR/hot-warm.out"
[[ "$(metric_value "$TARGET_POD" 'talon_worker_cache_tier_hits_total{tier="l1"}')" -ge 1 ]] ||
  fail "warm read did not register an L1 hit"

CROSS_OFFSET=$((BLOCK_SIZE - 32768))
CROSS_LEN=65536
dd if="$ARTIFACT_DIR/cross.bin" of="$ARTIFACT_DIR/cross.expected" \
  bs=1 skip="$CROSS_OFFSET" count="$CROSS_LEN" status=none
direct_read cross.bin "$CROSS_OFFSET" "$CROSS_LEN" "$ARTIFACT_DIR/cross.out"
cmp "$ARTIFACT_DIR/cross.expected" "$ARTIFACT_DIR/cross.out"

log "verifying L1 eviction falls back to L2 and L2 eviction invalidates L1"
stop_port_forwards
set_cache_limits $((2 * BLOCK_SIZE)) $((4 * BLOCK_SIZE))
start_coordinator_forward
TARGET_POD="$(first_worker)"
start_worker_forward "$TARGET_POD"
for i in 0 1 2; do
  direct_read "evict-$i.bin" 0 4096 "$ARTIFACT_DIR/evict-$i.out" >/dev/null
done
[[ "$(metric_value "$TARGET_POD" talon_worker_l1_evictions_total)" -ge 1 ]] ||
  fail "L1 capacity pressure did not evict an entry"
fetch_before="$(metric_value "$TARGET_POD" 'talon_worker_backend_fetch_bytes_total{backend="s3"}')"
l2_hits_before="$(metric_value "$TARGET_POD" 'talon_worker_cache_tier_hits_total{tier="l2"}')"
direct_read evict-0.bin 0 4096 "$ARTIFACT_DIR/evict-0-l2.out" >/dev/null
fetch_after="$(metric_value "$TARGET_POD" 'talon_worker_backend_fetch_bytes_total{backend="s3"}')"
l2_hits_after="$(metric_value "$TARGET_POD" 'talon_worker_cache_tier_hits_total{tier="l2"}')"
[[ "$fetch_after" -eq "$fetch_before" ]] || fail "L1 eviction refetched origin instead of L2"
[[ "$l2_hits_after" -gt "$l2_hits_before" ]] || fail "L1 eviction did not fall back to L2"

direct_read evict-3.bin 0 4096 "$ARTIFACT_DIR/evict-3.out" >/dev/null
direct_read evict-4.bin 0 4096 "$ARTIFACT_DIR/evict-4.out" >/dev/null
fetch_before="$(metric_value "$TARGET_POD" 'talon_worker_backend_fetch_bytes_total{backend="s3"}')"
direct_read evict-1.bin 0 4096 "$ARTIFACT_DIR/evict-1-refetched.out" >/dev/null
fetch_after="$(metric_value "$TARGET_POD" 'talon_worker_backend_fetch_bytes_total{backend="s3"}')"
[[ $((fetch_after - fetch_before)) -eq "$BLOCK_SIZE" ]] ||
  fail "L2 eviction left an orphan L1 copy or fetched an unexpected byte count"
[[ "$(metric_value "$TARGET_POD" talon_worker_l1_blocks)" -le 2 ]] ||
  fail "L1 exceeded its configured block capacity"

log "verifying container restart rebuilds L2 and promotes into an empty L1"
stop_port_forwards
set_cache_limits $((32 * BLOCK_SIZE)) $((64 * BLOCK_SIZE))
start_coordinator_forward
TARGET_POD="$(first_worker)"
start_worker_forward "$TARGET_POD"
direct_read restart.bin 0 4096 "$ARTIFACT_DIR/restart-before.out" >/dev/null
restart_before="$(kubectl -n "$NAMESPACE" get pod "$TARGET_POD" \
  -o jsonpath='{.status.containerStatuses[0].restartCount}')"
kubectl -n "$NAMESPACE" exec "$TARGET_POD" -- /bin/sh -c 'kill 1' >/dev/null 2>&1 || true
for _ in $(seq 1 120); do
  restart_after="$(kubectl -n "$NAMESPACE" get pod "$TARGET_POD" \
    -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null || echo 0)"
  ready="$(kubectl -n "$NAMESPACE" get pod "$TARGET_POD" \
    -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || echo false)"
  if [[ "$restart_after" -gt "$restart_before" && "$ready" == "true" ]]; then
    break
  fi
  sleep 1
done
[[ "${restart_after:-0}" -gt "$restart_before" ]] || fail "worker container did not restart"
stop_port_forwards
start_coordinator_forward
start_worker_forward "$TARGET_POD"
[[ "$(metric_value "$TARGET_POD" talon_worker_l1_blocks)" -eq 0 ]] ||
  fail "L1 was not empty after process restart"
direct_read restart.bin 0 4096 "$ARTIFACT_DIR/restart-after.out" >/dev/null
cmp "$ARTIFACT_DIR/restart-before.out" "$ARTIFACT_DIR/restart-after.out"
[[ "$(metric_value "$TARGET_POD" 'talon_worker_backend_fetch_bytes_total{backend="s3"}')" -eq 0 ]] ||
  fail "restart promotion refetched the object body"
[[ "$(metric_value "$TARGET_POD" 'talon_worker_cache_tier_hits_total{tier="l2"}')" -ge 1 ]] ||
  fail "restart read did not hit rebuilt L2"
[[ "$(metric_value "$TARGET_POD" talon_worker_l1_blocks)" -eq 1 ]] ||
  fail "rebuilt L2 block was not promoted into L1"

log "verifying concurrent cold misses collapse to one backend body fetch"
fetch_before="$(metric_value "$TARGET_POD" 'talon_worker_backend_fetch_bytes_total{backend="s3"}')"
pids=()
for i in $(seq 1 32); do
  direct_read singleflight.bin 0 4096 "$ARTIFACT_DIR/singleflight-$i.out" \
    >"$ARTIFACT_DIR/singleflight-$i.log" 2>&1 &
  pids+=("$!")
done
for pid in "${pids[@]}"; do
  wait "$pid"
done
fetch_after="$(metric_value "$TARGET_POD" 'talon_worker_backend_fetch_bytes_total{backend="s3"}')"
[[ $((fetch_after - fetch_before)) -eq "$BLOCK_SIZE" ]] ||
  fail "32 concurrent misses fetched $((fetch_after - fetch_before)) bytes, expected $BLOCK_SIZE"

log "verifying source overwrite replaces stale L1 and L2 versions"
direct_read version.bin 0 4096 "$ARTIFACT_DIR/version-old.out" >/dev/null
head -c 4096 "$ARTIFACT_DIR/version-old.bin" >"$ARTIFACT_DIR/version-old.expected"
cmp "$ARTIFACT_DIR/version-old.expected" "$ARTIFACT_DIR/version-old.out"
upload_file "$ARTIFACT_DIR/version-new.bin" version.bin
sleep 4
direct_read version.bin 0 4096 "$ARTIFACT_DIR/version-new.out" >/dev/null
head -c 4096 "$ARTIFACT_DIR/version-new.bin" >"$ARTIFACT_DIR/version-new.expected"
cmp "$ARTIFACT_DIR/version-new.expected" "$ARTIFACT_DIR/version-new.out"

log "verifying coordinator and worker pod replacement recovery"
coord_victim="$(kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/component=coordinator \
  -o jsonpath='{.items[0].metadata.name}')"
kubectl -n "$NAMESPACE" delete pod "$coord_victim" --wait=false
kubectl -n "$NAMESPACE" rollout status "deployment/$RELEASE-coordinator" --timeout=180s
stop_port_forwards
start_coordinator_forward
start_worker_forward "$(first_worker)"
placed_read hot.bin 4096 "$ARTIFACT_DIR/after-coordinator-restart.out" >/dev/null

worker_victim="$(first_worker)"
kubectl -n "$NAMESPACE" delete pod "$worker_victim" --wait=false
rollout_workers
stop_port_forwards
start_coordinator_forward
recovered=0
for _ in $(seq 1 45); do
  if placed_read hot.bin 4096 "$ARTIFACT_DIR/after-worker-restart.out" >/dev/null 2>&1; then
    recovered=1
    break
  fi
  sleep 1
done
[[ "$recovered" -eq 1 ]] || fail "placement did not recover after worker replacement"

log "benchmarking cold, warm L1, and warm L2 paths"
stop_port_forwards
set_cache_limits $((32 * BLOCK_SIZE)) $((64 * BLOCK_SIZE))
start_coordinator_forward
TARGET_POD="$(first_worker)"
start_worker_forward "$TARGET_POD"
{
  /usr/bin/time -f 'cold_elapsed_s=%e' \
    target/release/talon-client --worker "127.0.0.1:$WORKER_PORT" \
    --path "/s3/$BUCKET/bench" --len 65536 --out "$ARTIFACT_DIR/bench-cold.out"
  /usr/bin/time -f 'warm_l1_elapsed_s=%e' \
    target/release/talon-client --worker "127.0.0.1:$WORKER_PORT" \
    --path "/s3/$BUCKET/bench" --len 65536 --out "$ARTIFACT_DIR/bench-warm.out"
} 2>&1 | tee "$ARTIFACT_DIR/cold-warm.txt"
target/release/talon-loadgen --addr "127.0.0.1:$WORKER_PORT" \
  --container "$BUCKET" --object bench --range 65536 \
  --conns 1,32,128 --seconds "$BENCH_SECONDS" --warmup 2 --json |
  tee "$ARTIFACT_DIR/l1-benchmark.jsonl"
[[ "$(metric_value "$TARGET_POD" 'talon_worker_cache_tier_hits_total{tier="l1"}')" -gt 0 ]] ||
  fail "L1 benchmark produced no L1 hits"

stop_port_forwards
set_cache_limits 0 $((64 * BLOCK_SIZE))
start_coordinator_forward
TARGET_POD="$(first_worker)"
start_worker_forward "$TARGET_POD"
target/release/talon-loadgen --addr "127.0.0.1:$WORKER_PORT" \
  --container "$BUCKET" --object bench --range 65536 \
  --conns 1,32,128 --seconds "$BENCH_SECONDS" --warmup 2 --json |
  tee "$ARTIFACT_DIR/l2-benchmark.jsonl"
[[ "$(metric_value "$TARGET_POD" 'talon_worker_cache_tier_hits_total{tier="l2"}')" -gt 0 ]] ||
  fail "L2 benchmark produced no L2 hits"

log "all multi-node Kubernetes L1/L2 E2E checks passed"
kubectl -n "$NAMESPACE" get pods -o wide
echo "Artifacts: $ARTIFACT_DIR"
