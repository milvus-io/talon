#!/usr/bin/env python3
"""Talon microbenchmark harness.

Runs Divan benchmarks, parses their (human-only) table output into structured
JSON, and diffs a run against a committed baseline with an explicit per-bench
verdict. Designed to give humans *and* coding agents a fast, machine-readable
performance signal.

Subcommands:
  run     Run benches, write results/latest.json, print the parsed table.
  save    Promote results/latest.json to baselines/<name>.json (the committed
          reference). Run this deliberately when a perf change is intended.
  check   Run benches and diff vs a baseline; print a markdown table with Δ% and
          a verdict per bench. Exit non-zero on regression (unless --soft).

Divan emits no JSON (as of 0.1.21), so we parse its tree table. The parser keys
off value+unit tokens (e.g. "648.4 ns") rather than the box-drawing column
separators, which are ambiguous with tree-indent glyphs.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BASELINE_DIR = REPO / "bench" / "baselines"
RESULTS_DIR = REPO / "bench" / "results"
LATEST = RESULTS_DIR / "latest.json"

# Default regression/improvement threshold, percent. Set above typical
# microbench run-to-run noise on shared hardware to limit false positives.
DEFAULT_THRESHOLD = 10.0

UNIT_TO_NS = {"ns": 1.0, "us": 1e3, "µs": 1e3, "ms": 1e6, "s": 1e9}

# A value like "648.4 ns" / "1.322 µs" / "13.94 µs".
VALUE_RE = re.compile(r"(\d+\.?\d*)\s*(ns|µs|us|ms|s)\b")
# Leading box-drawing / whitespace we strip from a name cell.
TREE_CHARS = "│├╰┬─╭╮╯╰┐└┌ \t"
# The "Running .../<binary>.rs" line that precedes each bench binary's table.
RUNNING_RE = re.compile(r"Running .*/([A-Za-z0-9_]+)-[0-9a-f]+")


def to_ns(value: str, unit: str) -> float:
    return float(value) * UNIT_TO_NS[unit]


def strip_name(cell: str) -> str:
    return cell.strip(TREE_CHARS).strip()


def is_nested(line: str) -> bool:
    """True if a row is an args-child (indented under a group header)."""
    for ch in line:
        if ch in "├╰":  # first branch glyph reached
            # Anything before it (│ or spaces) means it's nested.
            return line.index(ch) > 0
        if ch not in "│ \t":
            return False
    return False


def parse_divan(output: str) -> dict[str, float]:
    """Parse Divan table output into {bench_name: median_ns}."""
    results: dict[str, float] = {}
    binary = "bench"
    parent: str | None = None

    for raw in output.splitlines():
        run = RUNNING_RE.search(raw)
        if run:
            binary = run.group(1)
            parent = None
            continue

        line = raw.rstrip()
        if not line or "fastest" in line and "median" in line:
            # Column header row.
            continue
        if line.startswith("Timer precision"):
            continue

        values = VALUE_RE.findall(line)
        # Name cell = text before the first value token (or whole line).
        if values:
            name_cell = line[: line.index(values_first_span(line))]
        else:
            name_cell = line
        name = strip_name(name_cell)
        if not name:
            continue

        if not values:
            # Group header row (no measurements): its children are args.
            parent = name
            continue

        # Leaf row with measurements: median is the 3rd value column.
        if len(values) < 3:
            continue
        median_ns = to_ns(*values[2])

        if is_nested(line) and parent:
            full = f"{binary}::{parent}/{name}"
        else:
            parent = None
            full = f"{binary}::{name}"
        results[full] = median_ns

    return results


def values_first_span(line: str) -> str:
    m = VALUE_RE.search(line)
    return m.group(0) if m else line


def run_benches(packages: list[str] | None) -> str:
    cmd = ["cargo", "bench"]
    if packages:
        for p in packages:
            cmd += ["-p", p]
    else:
        cmd.append("--workspace")
    proc = subprocess.run(cmd, cwd=REPO, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout)
        sys.exit(f"cargo bench failed (exit {proc.returncode})")
    return proc.stdout


def fmt_ns(ns: float) -> str:
    for unit, scale in (("s", 1e9), ("ms", 1e6), ("µs", 1e3), ("ns", 1.0)):
        if ns >= scale:
            return f"{ns / scale:.3g} {unit}"
    return f"{ns:.3g} ns"


def cmd_run(args: argparse.Namespace) -> int:
    output = run_benches(args.package)
    results = parse_divan(output)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    LATEST.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")
    print(f"Parsed {len(results)} benchmarks -> {LATEST.relative_to(REPO)}\n")
    for name in sorted(results):
        print(f"  {name:<48} {fmt_ns(results[name])}")
    return 0


def cmd_save(args: argparse.Namespace) -> int:
    if not LATEST.exists():
        sys.exit("no results/latest.json — run `just bench` first")
    BASELINE_DIR.mkdir(parents=True, exist_ok=True)
    dest = BASELINE_DIR / f"{args.name}.json"
    dest.write_text(LATEST.read_text())
    print(f"Saved baseline {dest.relative_to(REPO)}")
    return 0


def cmd_check(args: argparse.Namespace) -> int:
    baseline_path = BASELINE_DIR / f"{args.name}.json"
    if not baseline_path.exists():
        sys.exit(
            f"no baseline {baseline_path.relative_to(REPO)} — "
            f"run `just bench && just bench-save {args.name}`"
        )
    baseline = json.loads(baseline_path.read_text())
    output = run_benches(args.package)
    current = parse_divan(output)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    LATEST.write_text(json.dumps(current, indent=2, sort_keys=True) + "\n")

    threshold = args.threshold
    rows = []
    regressions = 0
    for name in sorted(set(baseline) | set(current)):
        base = baseline.get(name)
        cur = current.get(name)
        if base is None:
            rows.append((name, "—", fmt_ns(cur), "—", "new"))
            continue
        if cur is None:
            rows.append((name, fmt_ns(base), "—", "—", "missing"))
            continue
        delta = (cur - base) / base * 100.0
        if delta > threshold:
            verdict = "REGRESSION"
            regressions += 1
        elif delta < -threshold:
            verdict = "improved"
        else:
            verdict = "ok"
        rows.append((name, fmt_ns(base), fmt_ns(cur), f"{delta:+.1f}%", verdict))

    # Markdown table — friendly to PR comments and agent parsing.
    print(f"\n### Benchmark check vs `{args.name}` (threshold ±{threshold:.0f}%)\n")
    print("| benchmark | baseline | current | Δ | verdict |")
    print("|---|---:|---:|---:|---|")
    for name, base, cur, delta, verdict in rows:
        print(f"| `{name}` | {base} | {cur} | {delta} | {verdict} |")
    print()

    if regressions:
        msg = f"{regressions} regression(s) beyond ±{threshold:.0f}%"
        if args.soft:
            print(f"NOTE: {msg} (soft mode, not failing)")
            return 0
        print(f"FAIL: {msg}")
        return 1
    print("All benchmarks within threshold.")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Talon microbenchmark harness")
    sub = p.add_subparsers(dest="cmd", required=True)

    def add_common(sp):
        sp.add_argument(
            "-p", "--package", action="append",
            help="restrict to a cargo package (repeatable); default: whole workspace",
        )

    r = sub.add_parser("run", help="run benches and write latest.json")
    add_common(r)
    r.set_defaults(func=cmd_run)

    s = sub.add_parser("save", help="promote latest.json to a committed baseline")
    s.add_argument("name", nargs="?", default="main", help="baseline name (default: main)")
    s.set_defaults(func=cmd_save)

    c = sub.add_parser("check", help="run and diff vs a baseline")
    add_common(c)
    c.add_argument("name", nargs="?", default="main", help="baseline name (default: main)")
    c.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    c.add_argument("--soft", action="store_true", help="never exit non-zero")
    c.set_defaults(func=cmd_check)

    args = p.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
