#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Closed-loop load generator for the object-store gateway's S3 and Azure paths.

The gateway speaks two client protocols in front of the same cache, so the
question this answers is: what does each protocol adapter cost, and what does
routing through the cache buy over passing through to origin?

Design notes that matter for trusting the numbers:

* **Paired, order-swapped.** Each round measures every arm once, and the arm
  order is reversed on alternate rounds. A drift in machine state (a background
  job, thermal, page cache) then lands on both arms rather than on whichever ran
  first, which a single sequential pass cannot separate from a real difference.
* **Every response is verified, not just counted.** A benchmark that accepts a
  500 or a short body measures the error path and reports it as throughput. Any
  wrong status, wrong length, or wrong checksum fails the run loudly.
* **Latency is per request, from the client.** `time.perf_counter` around the
  full request including reading the body to completion, since a streaming
  gateway can return headers long before bytes.
* **Warmup is discarded.** The first touch of an object is a cache miss that
  fetches a whole block from origin; mixing that into a steady-state number
  would understate the cache by a large factor.

Raw per-request samples are written as JSON so the summary can be recomputed
without re-running.
"""

import argparse
import hashlib
import http.client
import json
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor


class Arm:
    """One (protocol, route) deployment under test."""

    def __init__(self, name, port, path, range_header, protocol, route):
        self.name = name
        self.port = port
        self.path = path
        self.range_header = range_header
        self.protocol = protocol
        self.route = route


ARMS = [
    Arm("s3-cache", 8081, "/cont/bench", "Range", "s3", "cache"),
    Arm("s3-origin", 8082, "/cont/bench", "Range", "s3", "origin"),
    Arm("azure-cache", 8083, "/acct/cont/bench", "x-ms-range", "azure", "cache"),
    Arm("azure-origin", 8084, "/acct/cont/bench", "x-ms-range", "azure", "origin"),
]


def fetch(conn, arm, offset, length):
    """One ranged GET. Returns (elapsed_seconds, body). Raises on any anomaly."""
    headers = {arm.range_header: f"bytes={offset}-{offset + length - 1}"}
    t0 = time.perf_counter()
    conn.request("GET", arm.path, headers=headers)
    resp = conn.getresponse()
    body = resp.read()
    elapsed = time.perf_counter() - t0
    if resp.status != 206:
        raise RuntimeError(
            f"{arm.name}: expected 206, got {resp.status}: {body[:200]!r}")
    if len(body) != length:
        raise RuntimeError(
            f"{arm.name}: expected {length} bytes, got {len(body)}")
    return elapsed, body


def worker(arm, offsets, length, expect_digest):
    """Drive one connection through `offsets`, verifying every response."""
    conn = http.client.HTTPConnection("127.0.0.1", arm.port, timeout=120)
    samples = []
    try:
        for off in offsets:
            elapsed, body = fetch(conn, arm, off, length)
            digest = hashlib.sha256(body).hexdigest()
            if digest != expect_digest[off]:
                raise RuntimeError(
                    f"{arm.name}: content mismatch at offset {off}")
            samples.append(elapsed)
    finally:
        conn.close()
    return samples


def build_expectations(offsets, length):
    """Fetch each range from the ORIGIN directly; that is the reference."""
    conn = http.client.HTTPConnection("127.0.0.1", 18080, timeout=120)
    out = {}
    try:
        for off in offsets:
            conn.request("GET", "/cont/bench",
                         headers={"Range": f"bytes={off}-{off + length - 1}"})
            resp = conn.getresponse()
            body = resp.read()
            if resp.status != 206 or len(body) != length:
                raise RuntimeError(
                    f"origin reference failed at {off}: {resp.status} {len(body)}")
            out[off] = hashlib.sha256(body).hexdigest()
    finally:
        conn.close()
    return out


def run_arm(arm, offsets, length, expect, concurrency):
    """Run one arm at a fixed concurrency; returns every latency sample."""
    chunks = [offsets[i::concurrency] for i in range(concurrency)]
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(worker, arm, c, length, expect)
                   for c in chunks if c]
        t0 = time.perf_counter()
        results = [f.result() for f in futures]
        wall = time.perf_counter() - t0
    samples = [s for r in results for s in r]
    return samples, wall


def summarize(name, samples, wall, length, concurrency):
    s = sorted(samples)
    n = len(s)

    def pct(p):
        return s[min(n - 1, int(round(p / 100 * n)))] * 1000

    return {
        "arm": name,
        "requests": n,
        "concurrency": concurrency,
        "range_bytes": length,
        "wall_s": round(wall, 3),
        "rps": round(n / wall, 1),
        "throughput_MiBps": round(n * length / wall / (1 << 20), 1),
        "mean_ms": round(statistics.fmean(s) * 1000, 3),
        "p50_ms": round(pct(50), 3),
        "p90_ms": round(pct(90), 3),
        "p99_ms": round(pct(99), 3),
        "max_ms": round(s[-1] * 1000, 3),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--requests", type=int, default=2000,
                    help="measured requests per arm per round")
    ap.add_argument("--range-bytes", type=int, default=1 << 20)
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--rounds", type=int, default=4,
                    help="paired rounds; arm order reverses on odd rounds")
    ap.add_argument("--object-bytes", type=int, default=64 << 20)
    ap.add_argument("--out", default="/tmp/gwbench/results.json")
    args = ap.parse_args()

    length = args.range_bytes
    span = args.object_bytes - length
    # Fixed stride rather than random: every arm must see the exact same
    # offsets, or one arm could get a friendlier access pattern than another.
    step = max(1, span // args.requests)
    offsets = [(i * step) % span for i in range(args.requests)]

    print(f"building origin reference for {len(set(offsets))} distinct ranges...",
          flush=True)
    expect = build_expectations(sorted(set(offsets)), length)

    # Warm every arm: the first touch of a block is a miss by construction.
    print("warming all arms...", flush=True)
    for arm in ARMS:
        run_arm(arm, offsets[:200], length, expect, args.concurrency)

    rows = []
    for rnd in range(args.rounds):
        order = ARMS if rnd % 2 == 0 else list(reversed(ARMS))
        for arm in order:
            samples, wall = run_arm(arm, offsets, length, expect,
                                    args.concurrency)
            row = summarize(arm.name, samples, wall, length, args.concurrency)
            row["round"] = rnd
            row["protocol"] = arm.protocol
            row["route"] = arm.route
            rows.append(row)
            print(f"round {rnd} {arm.name:14s} "
                  f"rps={row['rps']:>8} {row['throughput_MiBps']:>7} MiB/s "
                  f"p50={row['p50_ms']:>7} p99={row['p99_ms']:>8} ms", flush=True)

    with open(args.out, "w") as f:
        json.dump({"config": vars(args), "rows": rows}, f, indent=2)

    print(f"\n{'arm':16s} {'rps':>9} {'MiB/s':>9} {'p50 ms':>9} {'p99 ms':>9}")
    for arm in ARMS:
        mine = [r for r in rows if r["arm"] == arm.name]
        print(f"{arm.name:16s} "
              f"{statistics.median(r['rps'] for r in mine):>9.1f} "
              f"{statistics.median(r['throughput_MiBps'] for r in mine):>9.1f} "
              f"{statistics.median(r['p50_ms'] for r in mine):>9.3f} "
              f"{statistics.median(r['p99_ms'] for r in mine):>9.3f}")
    print(f"\nraw samples -> {args.out}")


main()
