#!/usr/bin/env python3
"""Summarize raw repeats without pooling per-run percentiles."""
import collections
import json
from pathlib import Path
import statistics
import sys

path = Path(sys.argv[1])
rows = [json.loads(line) for line in (path / 'raw.jsonl').read_text().splitlines()]
groups = collections.defaultdict(list)
baselines = {}
for row in rows:
    groups[row['kind'], row['bytes'], row['build'], row['mode']].append(row)
    if row['build'] == 'baseline':
        baselines[row['kind'], row['bytes'], row['repeat']] = row
summary = []
for (kind, size, build, mode), values in sorted(groups.items()):
    item = dict(kind=kind, bytes=size, build=build, mode=mode, repeats=len(values))
    for field in ['ns_per_op', 'p50_ns', 'p99_ns', 'allocs_per_op', 'allocated_bytes_per_op']:
        item['median_' + field] = statistics.median(v[field] for v in values)
    deltas = [100 * (v['ns_per_op'] / baselines[kind, size, v['repeat']]['ns_per_op'] - 1) for v in values]
    item.update(paired_delta_percent_median=statistics.median(deltas), paired_delta_percent_min=min(deltas), paired_delta_percent_max=max(deltas))
    summary.append(item)
(path / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
for item in summary:
    if item['kind'] == 'rpc':
        print(f"| {item['bytes']} | {item['build']}/{item['mode']} | {item['median_ns_per_op']/1000:.2f} | {item['paired_delta_percent_median']:+.2f}% | {item['median_allocs_per_op']:.3f} | {item['median_allocated_bytes_per_op']:.1f} |")
