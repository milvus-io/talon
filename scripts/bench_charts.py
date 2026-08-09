#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib>=3.8"]
# ///
"""Render the data-plane benchmark charts used in the README and docs.

The measurements live in `bench/data/dataplane.json`, not in this file. This
script only draws them, so a re-measurement means editing the JSON and re-running
`just bench-charts` — the numbers in the docs and the numbers in the charts
cannot drift apart because they have one source.

Charts are written as SVG (crisp at any zoom, diffable as text, and rendered by
both GitHub and mdbook):

  docs/assets/bench/throughput-ceilings.svg   loopback vs cross-node, vs the NIC
  docs/assets/bench/ring-scaling.svg          throughput and CPU across rings
  docs/assets/bench/cpu-split.svg             kernel vs user time on the worker

Run it with uv and there is nothing to install:

    just bench-charts          # or: uv run scripts/bench_charts.py

matplotlib is declared inline (PEP 723) so uv builds an ephemeral environment on
demand rather than requiring a project-wide dependency for a docs-only tool.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

import matplotlib

# Select the non-interactive backend before importing pyplot: these two imports
# are deliberately not at the top of the file, because pyplot picks a backend at
# import time and would otherwise fail on a machine with no display.
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

REPO = Path(__file__).resolve().parent.parent
DATA = REPO / "bench" / "data" / "dataplane.json"
OUT_DIR = REPO / "docs" / "assets" / "bench"

# A small deliberate palette rather than matplotlib's default cycle: one accent
# for "what Talon does", one muted tone for "what the hardware allows", and red
# reserved for the limit that binds. Colours are distinguishable in greyscale
# and pass contrast checks on both light and dark GitHub themes.
INK = "#1b2733"
MUTED = "#8a94a6"
ACCENT = "#2f6df6"
ACCENT_DIM = "#9db8fb"
LIMIT = "#d1495b"
KERNEL = "#e07a3f"


def style() -> None:
    """Apply a common look: no chartjunk, labels over legends where possible."""
    plt.rcParams.update(
        {
            # Byte-reproducible output: without this matplotlib gives clip paths
            # random ids, so re-rendering unchanged data would still diff.
            "svg.hashsalt": "talon-bench-charts",
            "figure.dpi": 130,
            "savefig.bbox": "tight",
            "savefig.transparent": True,
            "font.size": 10,
            "text.color": INK,
            "axes.edgecolor": MUTED,
            "axes.labelcolor": INK,
            "axes.titlesize": 12,
            "axes.titleweight": "bold",
            "axes.spines.top": False,
            "axes.spines.right": False,
            "xtick.color": MUTED,
            "ytick.color": MUTED,
            "axes.grid": True,
            "grid.color": "#e6e9ef",
            "grid.linewidth": 0.8,
            "axes.axisbelow": True,
        }
    )


def gbps(rps: int, range_bytes: int) -> float:
    return rps * range_bytes * 8 / 1e9


def save(fig, name: str) -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / name
    # metadata Date=None keeps the output byte-reproducible: matplotlib otherwise
    # stamps the render time into the SVG, so every run would look like a change
    # to git and --check could never pass.
    fig.savefig(path, format="svg", metadata={"Date": None})
    plt.close(fig)
    return path


def chart_ceilings(d: dict) -> Path:
    """The headline: local ceiling, what the network delivers, and the NIC line.

    Drawn as throughput in Gbps rather than rps so the NIC line rate can share
    the axis — that shared axis is the whole point of the chart.
    """
    block = d["loopback_vs_network"]
    rows = block["rows"]
    nic = block["nic_line_rate_gbps"]
    labels = [r["path"] for r in rows]
    values = [r["gbps"] for r in rows]

    fig, ax = plt.subplots(figsize=(7.2, 3.6))
    bars = ax.bar(labels, values, width=0.5, color=[ACCENT, ACCENT_DIM], zorder=3)

    ax.axhline(nic, color=LIMIT, linestyle="--", linewidth=1.5, zorder=4)
    ax.annotate(
        f"25 GbE line rate — {nic:g} Gbps",
        xy=(1.42, nic),
        xytext=(0, 6),
        textcoords="offset points",
        ha="right",
        color=LIMIT,
        fontsize=9,
        fontweight="bold",
    )

    for bar, row in zip(bars, rows):
        ax.annotate(
            f"{row['gbps']:.1f} Gbps\n{row['rps']:,} rps",
            xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
            xytext=(0, 5),
            textcoords="offset points",
            ha="center",
            fontsize=9,
            color=INK,
        )

    share = rows[1]["gbps"] / rows[0]["gbps"] * 100
    ax.set_title("The worker outruns the network")
    ax.set_ylabel("throughput (Gbps)")
    ax.set_ylim(0, max(values) * 1.28)
    ax.set_xlabel(
        f"{block['connections']} connections, depth {block['depth']}, 64 KiB ranges"
        f"  ·  cross-node reaches {share:.0f}% of local at equal connections,"
        " and saturates the link",
        fontsize=9,
        color=MUTED,
        labelpad=10,
    )
    return save(fig, "throughput-ceilings.svg")


def chart_ring_scaling(d: dict) -> Path:
    """Throughput against rings, with the CPU actually used underneath.

    Two series on one figure because the interesting fact is the relationship:
    throughput keeps climbing while CPU stops short of the 8-core cap, which is
    what "the kernel binds before the cores run out" looks like.
    """
    block = d["ring_scaling_depth16"]
    rows = block["rows"]
    rings = [r["rings"] for r in rows]
    rps = [r["rps"] for r in rows]
    cores = [r["cpu_cores"] for r in rows]
    x = range(len(rings))

    fig, (ax, ax2) = plt.subplots(
        2, 1, figsize=(7.2, 4.6), sharex=True, height_ratios=[2.1, 1]
    )

    ax.plot(x, rps, marker="o", color=ACCENT, linewidth=2.2, zorder=3)
    for xi, r in zip(x, rows):
        ax.annotate(
            f"{r['rps']:,}",
            xy=(xi, r["rps"]),
            xytext=(0, 8),
            textcoords="offset points",
            ha="center",
            fontsize=9,
        )
    ax.set_ylabel("reads/s")
    ax.set_title("Throughput scales with rings; CPU stops short of the cap")
    ax.set_ylim(0, max(rps) * 1.22)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v/1000:.0f}K"))

    ax2.bar(x, cores, width=0.45, color=MUTED, zorder=3)
    ax2.axhline(8, color=LIMIT, linestyle="--", linewidth=1.4, zorder=4)
    ax2.annotate(
        "8-core cgroup cap",
        xy=(len(rings) - 1 + 0.42, 8),
        xytext=(0, 4),
        textcoords="offset points",
        ha="right",
        color=LIMIT,
        fontsize=8.5,
        fontweight="bold",
    )
    for xi, c in zip(x, cores):
        ax2.annotate(
            f"{c:.2f}",
            xy=(xi, c),
            xytext=(0, 4),
            textcoords="offset points",
            ha="center",
            fontsize=8.5,
            color=INK,
        )
    ax2.set_ylabel("cores used")
    ax2.set_ylim(0, 9.6)
    ax2.set_xticks(list(x), [str(r) for r in rings])
    ax2.set_xlabel(
        f"io_uring rings  ·  {block['connections']} connections, depth {block['depth']}"
        "  ·  peak leaves 2 of 8 cores idle",
        fontsize=9,
        color=MUTED,
        labelpad=8,
    )
    return save(fig, "ring-scaling.svg")


def chart_cpu_split(d: dict) -> Path:
    """Where the worker's CPU goes. One stacked bar; the ratio is the message."""
    block = d["cpu_split_loopback"]
    kernel, user = block["kernel_ticks"], block["user_ticks"]
    total = kernel + user
    k_pct, u_pct = kernel / total * 100, user / total * 100

    fig, ax = plt.subplots(figsize=(7.2, 1.9))
    ax.barh([0], [k_pct], color=KERNEL, zorder=3, height=0.5)
    ax.barh([0], [u_pct], left=[k_pct], color=ACCENT, zorder=3, height=0.5)

    ax.text(
        k_pct / 2,
        0,
        f"kernel  {k_pct:.0f}%",
        ha="center",
        va="center",
        color="white",
        fontweight="bold",
        fontsize=10,
    )
    ax.text(
        k_pct + u_pct / 2,
        0,
        f"{u_pct:.0f}%",
        ha="center",
        va="center",
        color="white",
        fontweight="bold",
        fontsize=10,
    )

    ax.set_title("Where a serving worker's CPU goes")
    ax.set_xlim(0, 100)
    ax.set_yticks([])
    ax.set_xlabel(
        "sendfile and the TCP stack (kernel) vs Talon's own code (user)"
        "  ·  loopback, sampled from /proc/<pid>/stat",
        fontsize=9,
        color=MUTED,
        labelpad=8,
    )
    ax.grid(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_visible(False)
    ax.set_xticks([])
    return save(fig, "cpu-split.svg")


CHART_NAMES = ("throughput-ceilings.svg", "ring-scaling.svg", "cpu-split.svg")


def render(data: dict) -> list[Path]:
    return [chart_ceilings(data), chart_ring_scaling(data), chart_cpu_split(data)]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail if any chart would change (for CI drift detection)",
    )
    args = parser.parse_args()

    if not DATA.exists():
        print(f"error: missing measurements at {DATA}", file=sys.stderr)
        return 2
    data = json.loads(DATA.read_text())

    style()

    if args.check:
        # Render into a scratch directory and compare. Writing to the real paths
        # first would repair whatever drift we are trying to detect, so a stale
        # tree would pass on the second run and CI would never fail.
        global OUT_DIR
        committed = {}
        for name in CHART_NAMES:
            p = OUT_DIR / name
            committed[name] = p.read_bytes() if p.exists() else None

        with tempfile.TemporaryDirectory() as tmp:
            real_out, OUT_DIR = OUT_DIR, Path(tmp)
            try:
                fresh = {p.name: p.read_bytes() for p in render(data)}
            finally:
                OUT_DIR = real_out

        stale = [n for n in CHART_NAMES if committed.get(n) != fresh.get(n)]
        if stale:
            print(
                "error: charts are stale, run `just bench-charts` and commit:\n  "
                + "\n  ".join(stale),
                file=sys.stderr,
            )
            return 1
        print("charts up to date")
        return 0

    for p in render(data):
        print(f"wrote {p.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
