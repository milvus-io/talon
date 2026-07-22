#!/usr/bin/env bash
# End-to-end run: coordinator + worker (real Azure) + client.
#
# Prereqs (your Azure account — never committed, never logged):
#   export TALON_WORKER_AZURE_ACCOUNT=<storage-account>
#   export TALON_WORKER_AZURE_SAS='<container-scoped SAS query string, no leading ?>'
#   export TALON_BLOB_PATH=/az/<container>/<blob>      # object to read
#   export TALON_READ_LEN=1048576                       # bytes to read (default 1 MiB)
#
# Proves: first read = MISS -> Azure fetch -> commit; second read = HIT (faster);
# both reads byte-identical; and (if curl available) matches an independent
# Azure ranged GET.
set -euo pipefail

: "${TALON_WORKER_AZURE_ACCOUNT:?set TALON_WORKER_AZURE_ACCOUNT}"
: "${TALON_WORKER_AZURE_SAS:?set TALON_WORKER_AZURE_SAS}"
: "${TALON_BLOB_PATH:?set TALON_BLOB_PATH, e.g. /az/mycontainer/data.bin}"
LEN="${TALON_READ_LEN:-1048576}"
COORD_ADDR="127.0.0.1:7000"
WORKER_ADDR="127.0.0.1:7001"
CACHE_DIR="$(mktemp -d /tmp/talon-cache.XXXXXX)"

cd "$(dirname "$0")/.."
cargo build --workspace --bins

cleanup() { kill "${COORD_PID:-}" "${WORKER_PID:-}" 2>/dev/null || true; }
trap cleanup EXIT

./target/debug/talon-coordinator --listen "$COORD_ADDR" >/tmp/talon-coord.log 2>&1 &
COORD_PID=$!
sleep 1

TALON_WORKER_CACHE_DIRS="$CACHE_DIR" \
  ./target/debug/talon-worker \
  --listen "$WORKER_ADDR" --coordinator "$COORD_ADDR" --block-size 268435456 \
  >/tmp/talon-worker.log 2>&1 &
WORKER_PID=$!
sleep 1

echo "### First read (expect MISS -> Azure fetch -> commit)"
./target/debug/talon-client --coordinator "$COORD_ADDR" \
  --path "$TALON_BLOB_PATH" --offset 0 --len "$LEN" --out /tmp/talon-a.bin

echo "### Second read (expect HIT, faster)"
./target/debug/talon-client --coordinator "$COORD_ADDR" \
  --path "$TALON_BLOB_PATH" --offset 0 --len "$LEN" --out /tmp/talon-b.bin

echo "### Verify both reads are byte-identical"
cmp /tmp/talon-a.bin /tmp/talon-b.bin && echo "OK: reads identical"

echo "### Worker log (should show one MISS then one HIT; no SAS token present)"
grep -E "MISS|HIT|committed" /tmp/talon-worker.log || true
if grep -q "$TALON_WORKER_AZURE_SAS" /tmp/talon-worker.log; then
  echo "FAIL: SAS token leaked into worker log" >&2; exit 1
else
  echo "OK: SAS token absent from worker log"
fi

# Optional independent cross-check against Azure directly.
if command -v curl >/dev/null; then
  CONTAINER_BLOB="${TALON_BLOB_PATH#/az/}"
  URL="https://${TALON_WORKER_AZURE_ACCOUNT}.blob.core.windows.net/${CONTAINER_BLOB}?${TALON_WORKER_AZURE_SAS}"
  END=$((LEN - 1))
  curl -s -H "x-ms-range: bytes=0-${END}" -H "x-ms-version: 2021-12-02" "$URL" -o /tmp/talon-ref.bin
  cmp /tmp/talon-a.bin /tmp/talon-ref.bin && echo "OK: matches independent Azure GET"
fi

echo "### Done."
