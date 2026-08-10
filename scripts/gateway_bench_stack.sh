#!/usr/bin/env bash
# Bring up the local stack that scripts/gateway_proxy_bench.py drives.
#
# Two independent coordinator+worker stacks, not one shared coordinator: the
# coordinator places blocks by object hash and is *not* backend-aware, so an s3
# and an az worker registered together will route an s3 object to the az worker,
# which rejects it ("request selects backend s3, but this worker is configured
# for az"). One backend per coordinator sidesteps that entirely.
#
# Four gateways run concurrently so a paired benchmark can alternate between
# them without paying restart cost between arms.
#
#   127.0.0.1:18080  dual-protocol origin stub
#   127.0.0.1:7411   coordinator (azure)   7100 worker (azure)
#   127.0.0.1:7412   coordinator (s3)      7101 worker (s3)
#   127.0.0.1:8081   gateway s3    route=cache
#   127.0.0.1:8082   gateway s3    route=origin
#   127.0.0.1:8083   gateway azure route=cache
#   127.0.0.1:8084   gateway azure route=origin
#
# Credentials here are deliberately meaningless placeholders against a local
# stub; the stub does not verify signatures, because a benchmark should measure
# the gateway rather than a Python implementation of SigV4.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN="${TALON_GW_BENCH_DIR:-/tmp/gwbench}"
BIN="$ROOT/target/release"
BLOCK=16777216

down() {
    ps -eo pid,args \
        | grep -E "[t]alon-(gateway|worker|coordinator)|[g]ateway_bench_origin" \
        | awk '{print $1}' | xargs -r kill -9 2>/dev/null || true
    sleep 2
    echo "stack down"
}

up() {
    mkdir -p "$RUN"/{cache-az,cache-s3,logs}
    rm -rf "$RUN"/cache-az/* "$RUN"/cache-s3/*

    cat > "$RUN/worker-az.toml" <<CFG
backend = "azure"
azure_account = "acct"
azure_endpoint = "http://127.0.0.1:18080"
cache_dirs = ["$RUN/cache-az"]
block_size = $BLOCK
CFG
    cat > "$RUN/worker-s3.toml" <<CFG
backend = "s3"
s3_region = "us-east-1"
s3_endpoint = "http://127.0.0.1:18080"
s3_path_style = true
cache_dirs = ["$RUN/cache-s3"]
block_size = $BLOCK
CFG

    python3 "$ROOT/scripts/gateway_bench_origin.py" 18080 \
        > "$RUN/logs/origin.log" 2>&1 &
    "$BIN/talon-coordinator" --listen 127.0.0.1:7411 --admin-listen 127.0.0.1:8000 \
        > "$RUN/logs/coord-az.log" 2>&1 &
    "$BIN/talon-coordinator" --listen 127.0.0.1:7412 --admin-listen 127.0.0.1:8010 \
        > "$RUN/logs/coord-s3.log" 2>&1 &
    sleep 4

    TALON_WORKER_AZURE_SAS='sv=stub&sig=stub' \
        "$BIN/talon-worker" --config "$RUN/worker-az.toml" \
        --listen 127.0.0.1:7100 --advertise-addr 127.0.0.1:7100 \
        --admin-listen 127.0.0.1:8001 --coordinator 127.0.0.1:7411 \
        > "$RUN/logs/worker-az.log" 2>&1 &
    TALON_WORKER_S3_ACCESS_KEY_ID=test TALON_WORKER_S3_SECRET_ACCESS_KEY=test \
        "$BIN/talon-worker" --config "$RUN/worker-s3.toml" \
        --listen 127.0.0.1:7101 --advertise-addr 127.0.0.1:7101 \
        --admin-listen 127.0.0.1:8002 --coordinator 127.0.0.1:7412 \
        > "$RUN/logs/worker-s3.log" 2>&1 &
    sleep 7

    gw() {  # protocol route port coordinator_port
        local proto=$1 route=$2 port=$3 coord=$4
        local -a env=(
            TALON_GATEWAY_PROTOCOL="$proto"
            TALON_GATEWAY_MODE=development
            TALON_GATEWAY_BIND="127.0.0.1:$port"
            TALON_COORDINATOR_ADDR="127.0.0.1:$coord"
            TALON_GATEWAY_ROUTE="$route"
            TALON_GATEWAY_PATH_STYLE=true
            TALON_GATEWAY_BLOCK_SIZE="$BLOCK"
        )
        if [ "$proto" = s3 ]; then
            env+=(TALON_GATEWAY_S3_REGION=us-east-1
                  TALON_GATEWAY_S3_ENDPOINT=http://127.0.0.1:18080
                  AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test)
        else
            env+=(TALON_GATEWAY_AZURE_ACCOUNT=acct
                  TALON_GATEWAY_AZURE_ENDPOINT=http://127.0.0.1:18080
                  TALON_GATEWAY_AZURE_SAS='sv=stub&sig=stub')
        fi
        env -i PATH="$PATH" "${env[@]}" "$BIN/talon-gateway" \
            > "$RUN/logs/gw-$proto-$route.log" 2>&1 &
    }
    gw s3    cache  8081 7412
    gw s3    origin 8082 7412
    gw azure cache  8083 7411
    gw azure origin 8084 7411
    sleep 8

    local failed=0
    for p in 8081 8082 8083 8084; do
        code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$p/readyz" || true)
        echo "  gateway :$p readyz $code"
        [ "$code" = 200 ] || failed=1
    done
    if [ "$failed" = 1 ]; then
        echo "stack failed to come up; see $RUN/logs/" >&2
        exit 1
    fi
    echo "stack up (logs in $RUN/logs)"
}

case "${1:-up}" in
    up)   up ;;
    down) down ;;
    *)    echo "usage: $0 [up|down]" >&2; exit 2 ;;
esac
