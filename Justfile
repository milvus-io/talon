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

# --- documentation ---

# Regenerate the configuration reference from the ConfigVar schemas.
gen-config-docs:
    cargo run -q -p talon-coordinator --features etcd,kubernetes \
      --bin talon-gen-config-docs -- docs/reference/configuration.md

# Fail if the committed configuration reference is stale vs. the schemas.
check-config-docs:
    cargo run -q -p talon-coordinator --features etcd,kubernetes \
      --bin talon-gen-config-docs > /tmp/talon-config-ref.md
    diff -u docs/reference/configuration.md /tmp/talon-config-ref.md

# Regenerate the REST API reference from openapi.json.
gen-api-docs:
    cargo run -q -p talon-coordinator --bin talon-gen-api-docs -- docs/reference/rest-api.md

# Fail if the committed REST API reference is stale vs. openapi.json.
check-api-docs:
    cargo run -q -p talon-coordinator --bin talon-gen-api-docs > /tmp/talon-api-ref.md
    diff -u docs/reference/rest-api.md /tmp/talon-api-ref.md

# Spell-check the repo (install: cargo install typos-cli). Config: typos.toml.
spell:
    typos

# Link-check the docs (install: cargo install lychee). Config: lychee.toml.
# Offline: internal/relative links only, matching the CI gate.
linkcheck:
    lychee --offline --no-progress README.md DESIGN.md CONTRIBUTING.md BENCHMARKS.md 'docs/**/*.md' '.github/**/*.md'

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
