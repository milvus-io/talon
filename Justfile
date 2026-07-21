# Talon task runner. Run `just` to list recipes.
# The bench recipes are the agent-facing performance feedback loop.

# Show available recipes.
default:
    @just --list

# --- build / quality gates (mirror CI) ---

# Format the whole workspace.
fmt:
    cargo fmt --all

# Check formatting without modifying files.
fmt-check:
    cargo fmt --all --check

# Lint with warnings denied.
clippy:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

# Run the test suite.
test:
    cargo test --workspace --all-features --locked

# Everything CI checks, in order.
ci: fmt-check clippy test

# --- benchmarks (performance feedback loop) ---

# Run all microbenchmarks and write bench/results/latest.json.
bench *ARGS:
    python3 scripts/bench.py run {{ARGS}}

# Promote the latest run to a committed baseline (default name: main).
bench-save NAME="main":
    python3 scripts/bench.py save {{NAME}}

# Run benches and diff against a baseline; exits non-zero on regression.
# Pass --soft to report without failing, or --threshold N to tune sensitivity.
bench-check *ARGS:
    python3 scripts/bench.py check {{ARGS}}

# Print the current committed baseline.
bench-baseline NAME="main":
    @cat bench/baselines/{{NAME}}.json
