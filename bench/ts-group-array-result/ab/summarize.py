#!/usr/bin/env python3
"""Summarize merge_ab.sh output: per port, median and min-max of the merge duration and merge CPU (part_log), and whether every hash is equal."""
import json, statistics, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
by = {}
for r in rows:
    by.setdefault(r['port'], []).append(r)
for port, rs in sorted(by.items()):
    w = [r['merge_ms'] for r in rs]; c = [r['merge_cpu_ms'] for r in rs]
    print(f"port {port}: n={len(rs)} merge median {statistics.median(w):.0f} ms ({min(w)}-{max(w)}), "
          f"cpu median {statistics.median(c):.0f} ms ({min(c):.0f}-{max(c):.0f}), rows {rs[0]['rows']}, samples {rs[0]['samples']}, parts {set(r['parts'] for r in rs)}")
hashes = {r['hash'] for r in rows}
print('hashes equal:', len(hashes) == 1, sorted(hashes)[:3])
ports = sorted(by)
if len(ports) == 2:
    a, b = (statistics.median([r['merge_cpu_ms'] for r in by[p]]) for p in ports)
    print(f"cpu {ports[1]} vs {ports[0]}: {100 * (b - a) / a:+.1f}%")
