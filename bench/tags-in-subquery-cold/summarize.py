#!/usr/bin/env python3
"""Medians per label from results.jsonl: wall, plan optimize, GETs, bytes from S3 (and the predownloaded share), logical read."""
import json, statistics, sys, collections
path = sys.argv[1] if len(sys.argv) > 1 else '/root/nerve-workspace/tools/clickhouse/data/tags-in-subquery-cold/results.jsonl'
by = collections.OrderedDict()
for l in open(path):
    r = json.loads(l)
    by.setdefault(r['label'], []).append(r)
print('| case | wall ms median (min–max) | plan optimize ms | S3 GETs | from S3 MB (predownload) | read_bytes MB |')
print('|---|---|---|---|---|---|')
for lab, rs in by.items():
    w = [r['wall_ms'] for r in rs]
    po = statistics.median(r['events']['QueryPlanOptimizeMicroseconds'] / 1000 for r in rs)
    g = statistics.median(r['events']['S3GetObject'] for r in rs)
    src = statistics.median(r['events']['CachedReadBufferReadFromSourceBytes'] for r in rs) / 1e6
    pre = statistics.median(r['events']['CachedReadBufferPredownloadedFromSourceBytes'] for r in rs) / 1e6
    rb = statistics.median(r['read_bytes'] for r in rs) / 1e6
    print(f"| {lab} (n={len(rs)}) | {statistics.median(w):.0f} ({min(w)}–{max(w)}) | {po:.0f} | {g:.0f} | {src:.1f} ({pre:.1f}) | {rb:.1f} |")
