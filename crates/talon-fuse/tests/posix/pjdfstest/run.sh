#!/usr/bin/env bash
# Build a pinned pjdfstest revision and run it against a real Talon FUSE mount.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../../.." && pwd)"
REVISION="$(tr -d '[:space:]' < "$SCRIPT_DIR/REVISION")"
PJDFSTEST_REPOSITORY="https://github.com/pjd/pjdfstest.git"
CACHE_ROOT="${PJDFSTEST_CACHE_ROOT:-$REPO_ROOT/target/posix/pjdfstest}"
SOURCE_DIR="$CACHE_ROOT/$REVISION"

[[ "$REVISION" =~ ^[0-9a-f]{40}$ ]] || {
  printf 'error: REVISION must contain one full Git commit hash\n' >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage:
  run.sh --mountpoint PATH [TEST ...]

Arguments:
  --mountpoint PATH  Writable directory inside a mounted FUSE filesystem.
  TEST               Optional pjdfstest group or test path, such as "open" or
                     "open/00.t". The complete suite runs when TEST is omitted.

Environment:
  PJDFSTEST_CACHE_ROOT  Override the download and build cache directory.

Examples:
  run.sh --mountpoint /mnt/talon/s3/test-bucket
  run.sh --mountpoint /mnt/talon/s3/test-bucket open
  run.sh --mountpoint /mnt/talon/s3/test-bucket open/00.t
EOF
}

fail() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

mountpoint_path=""
selectors=()

while (($# > 0)); do
  case "$1" in
    --mountpoint)
      (($# >= 2)) || fail "--mountpoint requires a path"
      mountpoint_path="$2"
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    --)
      shift
      selectors+=("$@")
      break
      ;;
    -*)
      fail "unknown option: $1"
      ;;
    *)
      selectors+=("$1")
      shift
      ;;
  esac
done

[[ -n "$mountpoint_path" ]] || {
  usage >&2
  fail "--mountpoint is required"
}

if ((EUID != 0)); then
  fail "pjdfstest must run as root"
fi

for command_name in git autoreconf make prove openssl findmnt; do
  require_command "$command_name"
done

if ! command -v cc >/dev/null 2>&1 &&
  ! command -v gcc >/dev/null 2>&1 &&
  ! command -v clang >/dev/null 2>&1; then
  fail "a C compiler is required (cc, gcc, or clang)"
fi

[[ -d "$mountpoint_path" ]] || fail "mountpoint directory does not exist: $mountpoint_path"
mountpoint_path="$(cd "$mountpoint_path" && pwd -P)"
[[ "$mountpoint_path" != "/" ]] || fail "refusing to run against the filesystem root"
[[ -w "$mountpoint_path" ]] || fail "mountpoint is not writable: $mountpoint_path"

filesystem_type="$(findmnt --raw --noheadings --output FSTYPE --target "$mountpoint_path" | head -n 1)"
[[ -n "$filesystem_type" ]] || fail "could not determine filesystem type for: $mountpoint_path"
case "$filesystem_type" in
  fuse | fuse.*)
    ;;
  *)
    fail "target is not on a FUSE filesystem: $mountpoint_path ($filesystem_type)"
    ;;
esac

mkdir -p "$SOURCE_DIR"
if [[ ! -d "$SOURCE_DIR/.git" ]]; then
  git -C "$SOURCE_DIR" init --quiet
  git -C "$SOURCE_DIR" remote add origin "$PJDFSTEST_REPOSITORY"
fi

if ! git -C "$SOURCE_DIR" rev-parse --verify HEAD >/dev/null 2>&1; then
  git -C "$SOURCE_DIR" fetch --quiet --depth 1 origin "$REVISION"
  git -C "$SOURCE_DIR" checkout --quiet --detach FETCH_HEAD
fi

actual_revision="$(git -C "$SOURCE_DIR" rev-parse HEAD)"
[[ "$actual_revision" == "$REVISION" ]] ||
  fail "cached pjdfstest revision mismatch: expected $REVISION, found $actual_revision"

if [[ ! -x "$SOURCE_DIR/pjdfstest" ]]; then
  (
    cd "$SOURCE_DIR"
    autoreconf -ifs
    ./configure
    make -j"${PJDFSTEST_BUILD_JOBS:-2}" pjdfstest
  )
fi

tests=()
if ((${#selectors[@]} == 0)); then
  tests=("$SOURCE_DIR/tests")
else
  for selector in "${selectors[@]}"; do
    [[ "$selector" != /* && "$selector" != *".."* ]] ||
      fail "test selector must be relative and may not contain '..': $selector"
    test_path="$SOURCE_DIR/tests/$selector"
    [[ -e "$test_path" ]] || fail "pjdfstest test does not exist: $selector"
    tests+=("$test_path")
  done
fi

printf 'pjdfstest revision: %s\n' "$REVISION"
printf 'target directory: %s\n' "$mountpoint_path"
printf 'filesystem type: %s\n' "$filesystem_type"
printf 'selected tests:'
printf ' %s' "${selectors[@]:-all}"
printf '\n'

cd "$mountpoint_path"
exec prove --verbose --recurse "${tests[@]}"
