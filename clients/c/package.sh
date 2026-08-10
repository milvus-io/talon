#!/usr/bin/env sh
# Build and stage the distributable C client files.
set -eu

if [ "$#" -gt 1 ]; then
    echo "usage: $0 [output-directory]" >&2
    exit 64
fi

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
OUTPUT_DIR=${1:-"$REPO_DIR/target/talon-c-sdk"}

case "$OUTPUT_DIR" in
    /*) ;;
    *) OUTPUT_DIR="$(pwd)/$OUTPUT_DIR" ;;
esac

cargo build --manifest-path "$SCRIPT_DIR/Cargo.toml" --release --locked

mkdir -p "$OUTPUT_DIR/include" "$OUTPUT_DIR/lib"
install -m 644 "$SCRIPT_DIR/include/talon.h" "$OUTPUT_DIR/include/talon.h"
install -m 644 "$REPO_DIR/target/release/libtalon_c.so" "$OUTPUT_DIR/lib/libtalon_c.so"
install -m 644 "$REPO_DIR/target/release/libtalon_c.a" "$OUTPUT_DIR/lib/libtalon_c.a"
install -m 644 "$REPO_DIR/LICENSE" "$OUTPUT_DIR/LICENSE"

echo "C client SDK staged in $OUTPUT_DIR"
