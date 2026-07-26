#!/usr/bin/env bash
# Compare the io_uring and Tokio data planes under concurrent load (#291).
#
# Starts a worker on each data plane in turn, drives it with talon-loadgen at a
# sweep of connection counts, and prints both result tables. This is the
# measurement the Divan microbenchmark cannot make: `dataplane_benches` drives
# one client serially, and io_uring's advantage is amortising syscalls across
# concurrent operations.
#
# Manual tool, not a CI gate — shared runners are too noisy for absolute-time
# comparison, the same reason the bench job is informational.
#
# Usage:
#   scripts/dataplane_loadtest.sh                     # default sweep
#   CONNS=1,128,512,1024 SECONDS_PER=15 scripts/dataplane_loadtest.sh
#
# Requires a Linux host with io_uring available; the worker logs a fallback and
# this script reports it if not.
set -euo pipefail
cd "$(dirname "$0")/.."

CONNS="${CONNS:-1,64,256,1024}"
RANGE="${RANGE:-65536}"
SECONDS_PER="${SECONDS_PER:-10}"
WARMUP="${WARMUP:-3}"
PORT="${PORT:-17801}"
COORD_PORT="${COORD_PORT:-17800}"
ORIGIN_PORT="${ORIGIN_PORT:-17700}"
CACHE_DIR="$(mktemp -d /tmp/talon-loadtest.XXXXXX)"
LOG_DIR="$(mktemp -d /tmp/talon-loadtest-logs.XXXXXX)"

cleanup() {
  [ -n "${ORIGIN_PID:-}" ] && kill "$ORIGIN_PID" 2>/dev/null || true
  pkill -x talon-worker 2>/dev/null || true
  pkill -f "${COORDINATOR:-talon-coordinator}" 2>/dev/null || true
  rm -rf "$CACHE_DIR"
}
trap cleanup EXIT

echo "building release binaries..."
cargo build --release -q -p talon-worker --bin talon-worker --bin talon-loadgen
cargo build --release -q -p talon-coordinator --bin talon-coordinator

WORKER=target/release/talon-worker
LOADGEN=target/release/talon-loadgen
COORDINATOR=target/release/talon-coordinator

# A local origin stands in for the object store. The sweep measures the serve
# path — after the first fetch every request is a resident cache hit — but the
# origin must still answer, because a worker resolves an object's version with a
# HEAD before serving and fails the read if that HEAD fails.
python3 scripts/loadtest_origin.py "$ORIGIN_PORT" >"$LOG_DIR/origin.log" 2>&1 &
ORIGIN_PID=$!
sleep 1
if ! kill -0 "$ORIGIN_PID" 2>/dev/null; then
  echo "FAILED to start the local origin; see $LOG_DIR/origin.log"
  exit 1
fi

export TALON_WORKER_CACHE_DIRS="$CACHE_DIR"
export TALON_WORKER_AZURE_ACCOUNT="${TALON_WORKER_AZURE_ACCOUNT:-loadtest}"
export TALON_WORKER_AZURE_SAS="${TALON_WORKER_AZURE_SAS:-loadtest}"
export TALON_WORKER_AZURE_ENDPOINT="http://127.0.0.1:$ORIGIN_PORT"
export RUST_LOG="${RUST_LOG:-info}"

# A worker only reports ready once it has registered with a coordinator, and an
# unready worker answers every read with an error frame. So the sweep needs a
# real coordinator even though it never exercises the control plane.
start_coordinator() {
  pkill -f "$COORDINATOR" 2>/dev/null || true
  sleep 1
  setsid "$COORDINATOR" \
    --listen "127.0.0.1:$COORD_PORT" \
    --admin-listen "127.0.0.1:$((COORD_PORT + 100))" \
    >"$LOG_DIR/coordinator.log" 2>&1 </dev/null &
  sleep 3
  if ! pgrep -f "$COORDINATOR" >/dev/null; then
    echo "FAILED to start coordinator; see $LOG_DIR/coordinator.log"
    exit 1
  fi
}

run_plane() {
  local label="$1"
  local extra_env="$2"
  local log="$LOG_DIR/$label.log"
  pkill -x talon-worker 2>/dev/null || true
  sleep 1
  rm -rf "$CACHE_DIR"/*

  env $extra_env setsid "$WORKER" \
    --listen "127.0.0.1:$PORT" \
    --admin-listen "127.0.0.1:$((PORT + 100))" \
    --coordinator "127.0.0.1:$COORD_PORT" \
    >"$log" 2>&1 </dev/null &
  sleep 5

  local pid
  pid="$(pgrep -x talon-worker | head -1 || true)"
  if [ -z "$pid" ]; then
    echo "  FAILED to start ($label); see $log"
    return 1
  fi

  # Wait for registration: an unready worker error-frames every read, which the
  # load generator reports as zero samples rather than as fast responses.
  local waited=0
  until curl -fsS "http://127.0.0.1:$((PORT + 100))/readyz" >/dev/null 2>&1; do
    sleep 1
    waited=$((waited + 1))
    if [ "$waited" -ge 30 ]; then
      echo "  worker never became ready ($label); see $log"
      return 1
    fi
  done

  local plane rings
  if grep -q "io_uring rings" "$log"; then
    rings="$(grep -c 'data-plane ring listening' "$log" || echo 0)"
    plane="io_uring, $rings rings"
  else
    plane="tokio"
  fi

  echo
  echo "=== $label ($plane) ==="
  "$LOADGEN" --addr "127.0.0.1:$PORT" --conns "$CONNS" --range "$RANGE" \
    --seconds "$SECONDS_PER" --warmup "$WARMUP" --server-pid "$pid"
  pkill -x talon-worker 2>/dev/null || true
}

echo "sweep: conns=$CONNS range=${RANGE}B ${SECONDS_PER}s each after ${WARMUP}s warmup"
start_coordinator
run_plane "io_uring" ""
run_plane "tokio" "TALON_WORKER_FORCE_TOKIO_DATA_PLANE=1"

echo
echo "Logs: $LOG_DIR"
echo "Compare rps and p99 at the highest connection count — that is the regime"
echo "the io_uring data plane is built for, and where the difference appears."
