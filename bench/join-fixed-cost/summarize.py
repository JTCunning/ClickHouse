#!/usr/bin/env python3
"""Median / min / max per case from bench.py output. Usage: python3 summarize.py run.jsonl"""
import json
import statistics
import sys
from collections import defaultdict

rows = defaultdict(list)
for line in open(sys.argv[1]):
    r = json.loads(line)
    rows[r["case"]].append(r)

cols = [("ms", "wall ms", 1), ("OSCPUVirtualTimeMicroseconds", "cpu ms", 1000), ("RealTimeMicroseconds", "thread ms", 1000),
        ("QueryAnalysisMicroseconds", "analysis", 1000), ("QueryPlanBuildMicroseconds", "plan build", 1000),
        ("QueryPlanOptimizeMicroseconds", "plan opt", 1000), ("QueryPipelineBuildMicroseconds", "pipe build", 1000),
        ("mem", "mem MiB", 1 << 20), ("peak_threads", "threads", 1), ("read_rows", "read_rows", 1)]
print("| case | n | " + " | ".join(c[1] for c in cols) + " | result |")
print("|---|---|" + "---|" * len(cols) + "---|")
for case, rs in rows.items():
    cells = []
    for key, _, div in cols:
        v = [r[key] / div for r in rs]
        med = statistics.median(v)
        if key == "ms":
            cells.append(f"{med:.0f} ({min(v):.0f}–{max(v):.0f})")
        elif key in ("read_rows", "peak_threads"):
            cells.append(f"{med:.0f}")
        else:
            cells.append(f"{med:.1f}")
    print(f"| {case} | {len(rs)} | " + " | ".join(cells) + f" | {rs[0]['result']} |")
