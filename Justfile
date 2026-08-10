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

# Regenerate the wire-protocol conformance vectors. Run after any intentional
# change to the wire format, and commit the result in the same change.
gen-conformance-vectors:
    cargo run -q -p talon-transport --bin talon-gen-conformance-vectors -- \
      crates/talon-transport/tests/conformance_vectors.json

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

# Lint + render the Helm chart across every backend (install: helm v3).
helm-check:
    for be in memory kubernetes etcd; do \
      rp=3; [ "$be" = memory ] && rp=1; \
      helm lint deploy/helm/talon --strict --set coordinator.backend=$be --set coordinator.replicas=$rp; \
      helm template t deploy/helm/talon --set coordinator.backend=$be --set coordinator.replicas=$rp >/dev/null; \
    done

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

# Measure bounded HTTP gateway overhead and slow-consumer memory behavior.
gateway-bench:
    cargo run --release -p talon-gateway --example gateway_benchmark

# Benchmark the gateway's real S3 and Azure adapters against a live stack.
# Requires the four gateways, two workers, two coordinators and the origin stub
# from scripts/gateway_bench_stack.sh to be running.
gateway-proxy-bench *ARGS:
    python3 scripts/gateway_proxy_bench.py {{ARGS}}

# Bring up (or tear down) the local stack the proxy benchmark drives.
gateway-bench-stack ACTION="up":
    scripts/gateway_bench_stack.sh {{ACTION}}

# Print the current committed baseline.
bench-baseline NAME="main":
    @cat bench/baselines/{{NAME}}.json

# Render the data-plane charts used in the README and docs from
# bench/data/dataplane.json. Needs `uv` (deps are declared inline, PEP 723).
bench-charts *ARGS:
    uv run scripts/bench_charts.py {{ARGS}}

# --- supply chain / coverage ---

# Check the dependency tree for RUSTSEC advisories, banned crates, and unknown
# sources (install: cargo install cargo-deny). Mirrors the CI `audit` gate.
# Known advisories are acknowledged with written justifications in deny.toml.
audit:
    cargo deny check advisories bans sources

# Per-file line coverage (install: cargo install cargo-llvm-cov).
coverage:
    cargo llvm-cov --workspace --all-features --summary-only

# Coverage as a browsable HTML report under target/llvm-cov/html.
coverage-html:
    cargo llvm-cov --workspace --all-features --html
