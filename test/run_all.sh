#!/usr/bin/env bash
# End-to-end multi-language SDK test against a real distributed Talon instance
# backed by MinIO.
#
# Deploys the stack (kind + local images + MinIO + Helm 3-coordinator HA /
# 3-worker), then runs the Python, Java, and C SDK suites in-cluster from a
# runner pod, tearing the stack down afterwards.
#
# Usage:
#   test/run_all.sh                 # deploy + run all three SDK suites + tear down
#   test/run_all.sh python|java|c   # run a single SDK suite (stack must be up)
#
# Note: the Python wheel and C staticlib are built on the host and installed/
# linked inside a Linux runner pod, so Python/C require a Linux x86_64 host
# (as in CI). On macOS only the Java suite works in-cluster; run Python/C
# against a reachable cluster instead (see test/sdk/*/README.md).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

run_python() {
  echo "=== Python SDK (in-cluster) ==="
  test/stack/deploy.sh test-python
}

run_java() {
  echo "=== Java SDK (in-cluster) ==="
  test/stack/deploy.sh test-java
}

run_c() {
  echo "=== C SDK (in-cluster) ==="
  test/stack/deploy.sh test-c
}

all() {
  test/stack/deploy.sh up
  trap 'test/stack/deploy.sh down' EXIT
  run_python
  run_java
  run_c
  test/stack/deploy.sh down
  trap - EXIT
  echo
  echo "all SDK suites passed"
}

case "${1:-all}" in
  all)    all ;;
  python) run_python ;;
  java)   run_java ;;
  c)      run_c ;;
  *)      echo "usage: $0 [all|python|java|c]" >&2; exit 2 ;;
esac
