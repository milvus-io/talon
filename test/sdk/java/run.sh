#!/usr/bin/env bash
# Compile and run the Java SDK end-to-end test against a deployed MinIO-backed
# Talon stack. Mirrors scripts/java_client_e2e.sh: javac the client sources and
# the test, then run it with the coordinator address and block size as args.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"

COORDINATOR="${1:-${TALON_E2E_COORDINATOR:-127.0.0.1:17000}}"
BLOCK_SIZE="${2:-${TALON_E2E_BLOCK_SIZE:-8388608}}"
BUCKET="${3:-${TALON_E2E_BUCKET:-talon-e2e}}"
KEY="${4:-${TALON_E2E_KEY:-bench}}"

JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
JAVAC="${JAVA_HOME:+$JAVA_HOME/bin/}javac"
command -v "$JAVAC" >/dev/null 2>&1 || { echo "no JDK found; set JAVA_HOME" >&2; exit 1; }

CLASSES="$(mktemp -d /tmp/talon-java-minio.XXXXXX)"
trap 'rm -rf "$CLASSES"' EXIT

echo "compiling Java client and MinioE2ETest..."
"$JAVAC" -d "$CLASSES" \
  clients/java/src/main/java/io/milvus/talon/*.java \
  test/sdk/java/MinioE2ETest.java

echo "running MinioE2ETest against $COORDINATOR (block_size=$BLOCK_SIZE, s3://$BUCKET/$KEY)..."
"$JAVA" -cp "$CLASSES" io.milvus.talon.MinioE2ETest "$COORDINATOR" "$BLOCK_SIZE" "$BUCKET" "$KEY"
