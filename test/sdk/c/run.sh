#!/usr/bin/env bash
# Build the C client library and the MinIO e2e test, then run it against the
# deployed Talon stack. Uses the same artifacts as clients/c/package.sh:
# libtalon_c.a + include/talon.h.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"

COORDINATOR="${1:-${TALON_E2E_COORDINATOR:-127.0.0.1:17000}}"
BLOCK_SIZE="${2:-${TALON_E2E_BLOCK_SIZE:-8388608}}"
BUCKET="${3:-${TALON_E2E_BUCKET:-talon-e2e}}"
KEY="${4:-${TALON_E2E_KEY:-bench}}"

echo "building C client library..."
cargo build --release -q -p talon-c --locked

SDK_DIR="$(mktemp -d /tmp/talon-c-sdk.XXXXXX)"
trap 'rm -rf "$SDK_DIR"' EXIT

cc -std=c11 -Wall -Wextra -O2 \
  -I"$ROOT/clients/c/include" \
  test/sdk/c/minio_e2e.c \
  "$ROOT/target/release/libtalon_c.a" \
  -lpthread -lm \
  -o "$SDK_DIR/minio_e2e"

echo "running minio_e2e against $COORDINATOR (block_size=$BLOCK_SIZE, s3://$BUCKET/$KEY)..."
"$SDK_DIR/minio_e2e" "$COORDINATOR" "$BLOCK_SIZE" "$BUCKET" "$KEY"
