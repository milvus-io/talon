#!/usr/bin/env bash
# End-to-end test for the Java client against a live cluster (#313).
#
# Starts a coordinator, a worker, and a local blob origin, then runs the Java
# conformance and e2e suites against them. Requires a JDK on PATH or JAVA_HOME.
set -uo pipefail
cd "$(dirname "$0")/.."

JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
JAVAC="${JAVA_HOME:+$JAVA_HOME/bin/}javac"
command -v "$JAVAC" >/dev/null 2>&1 || { echo "no JDK found; set JAVA_HOME"; exit 1; }

COORD_PORT=17620
WORKER_PORT=17621
ADMIN_PORT=17671
ORIGIN_PORT=17720
BLOCK_SIZE=$((8 << 20))
VERSION="0x8LOADTEST"

CACHE="$(mktemp -d /tmp/talon-java-e2e.XXXXXX)"
LOGS="$(mktemp -d /tmp/talon-java-e2e-logs.XXXXXX)"
CLASSES="$(mktemp -d /tmp/talon-java-classes.XXXXXX)"

cleanup() {
  # Kill by recorded pid rather than by name: pkill -f would also match the
  # shell running this script, and talon-coordinator exceeds pkill -x's
  # 15-character comm limit.
  for pid in "${ORIGIN_PID:-}" "${COORD_PID:-}" "${WORKER_PID:-}"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null
  done
  rm -rf "$CACHE" "$CLASSES"
}
trap cleanup EXIT

echo "building..."
cargo build --release -q -p talon-worker --bin talon-worker -p talon-coordinator --bin talon-coordinator
"$JAVAC" -d "$CLASSES" \
  clients/java/src/main/java/io/milvus/talon/*.java \
  clients/java/src/test/java/io/milvus/talon/*.java

export TALON_WORKER_CACHE_DIRS="$CACHE"
export TALON_WORKER_AZURE_ACCOUNT=test TALON_WORKER_AZURE_SAS=test
export TALON_WORKER_AZURE_ENDPOINT="http://127.0.0.1:$ORIGIN_PORT"
export RUST_LOG="${RUST_LOG:-warn}"

python3 scripts/loadtest_origin.py "$ORIGIN_PORT" >"$LOGS/origin.log" 2>&1 &
ORIGIN_PID=$!
sleep 1

setsid target/release/talon-coordinator \
  --listen "127.0.0.1:$COORD_PORT" --admin-listen "127.0.0.1:$((COORD_PORT + 50))" \
  >"$LOGS/coordinator.log" 2>&1 </dev/null &
COORD_PID=$!
sleep 3

setsid target/release/talon-worker \
  --listen "127.0.0.1:$WORKER_PORT" --admin-listen "127.0.0.1:$ADMIN_PORT" \
  --coordinator "127.0.0.1:$COORD_PORT" \
  >"$LOGS/worker.log" 2>&1 </dev/null &
WORKER_PID=$!

# An unready worker error-frames every read, so wait rather than sleeping.
for _ in $(seq 1 30); do
  curl -fsS "http://127.0.0.1:$ADMIN_PORT/readyz" >/dev/null 2>&1 && break
  sleep 1
done

echo
echo "=== conformance vectors ==="
"$JAVA" -cp "$CLASSES" io.milvus.talon.ConformanceTest || exit 1

echo
echo "=== end-to-end ==="
"$JAVA" -cp "$CLASSES" io.milvus.talon.E2ETest \
  "127.0.0.1:$COORD_PORT" "$BLOCK_SIZE" "$VERSION" || exit 1
