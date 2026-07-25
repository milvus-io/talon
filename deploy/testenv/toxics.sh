#!/usr/bin/env bash
# Attach a named latency/failure profile to the Azurite proxy via the Toxiproxy
# admin API. Run against the latency lab (deploy/testenv/docker-compose.yml)
# while it is up.
#
# Usage:
#   ./deploy/testenv/toxics.sh <preset>
#   ./deploy/testenv/toxics.sh clear        # remove all toxics
#   ./deploy/testenv/toxics.sh list         # show current toxics
#
# Presets (model different object-store conditions):
#   s3-warm           ~30ms first byte, small jitter — a nearby, warm bucket.
#   s3-cold-longtail  ~150ms first byte + heavy tail jitter — cold/cross-region.
#   throttled         ~50ms first byte + a ~10 MB/s bandwidth ceiling.
#   flaky             adds a short timeout toxic so some requests stall out.
#
# The two latency layers compose: these network toxics stack on top of the
# worker's in-process TALON_WORKER_BACKEND_* delay (if enabled).
set -euo pipefail

API="${TOXIPROXY_API:-http://127.0.0.1:8474}"
PROXY="azurite"

api() { # method path [data]
  local method="$1" path="$2" data="${3:-}"
  if [ -n "$data" ]; then
    curl -fsS -X "$method" -H 'Content-Type: application/json' -d "$data" "$API$path"
  else
    curl -fsS -X "$method" "$API$path"
  fi
}

clear_toxics() {
  # Delete every toxic currently on the proxy.
  local names
  names=$(api GET "/proxies/$PROXY/toxics" | grep -o '"name":"[^"]*"' | cut -d'"' -f4 || true)
  for n in $names; do
    api DELETE "/proxies/$PROXY/toxics/$n" >/dev/null && echo "removed toxic: $n"
  done
}

add() { # json
  api POST "/proxies/$PROXY/toxics" "$1" >/dev/null
}

case "${1:-}" in
  s3-warm)
    clear_toxics
    add '{"name":"latency","type":"latency","attributes":{"latency":30,"jitter":10}}'
    echo "applied s3-warm: 30ms +/-10ms first-byte latency"
    ;;
  s3-cold-longtail)
    clear_toxics
    add '{"name":"latency","type":"latency","attributes":{"latency":150,"jitter":120}}'
    echo "applied s3-cold-longtail: 150ms + heavy 120ms tail jitter"
    ;;
  throttled)
    clear_toxics
    add '{"name":"latency","type":"latency","attributes":{"latency":50,"jitter":15}}'
    # bandwidth rate is in KB/s: 10240 KB/s ~= 10 MB/s.
    add '{"name":"bandwidth","type":"bandwidth","attributes":{"rate":10240}}'
    echo "applied throttled: 50ms latency + ~10 MB/s bandwidth ceiling"
    ;;
  flaky)
    clear_toxics
    add '{"name":"latency","type":"latency","attributes":{"latency":80,"jitter":40}}'
    # Some connections stall for 2s then close, exercising the read timeout path.
    add '{"name":"timeout","type":"timeout","toxicity":0.2,"attributes":{"timeout":2000}}'
    echo "applied flaky: 80ms latency + 20% of connections time out after 2s"
    ;;
  clear)
    clear_toxics
    echo "cleared all toxics"
    ;;
  list)
    api GET "/proxies/$PROXY/toxics"
    echo
    ;;
  *)
    echo "usage: $0 {s3-warm|s3-cold-longtail|throttled|flaky|clear|list}" >&2
    exit 2
    ;;
esac
