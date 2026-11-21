#!/usr/bin/env python3
"""Summarize ab/run.py output: per port the median and min-max of CPU us per sample, the change of the second port
against the first, and whether the table hashes are equal."""
import json, statistics, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
by, hashes = {}, {}
for r in rows:
    if 'hash' in r:
        hashes[r['port']] = r['hash']
    else:
        by.setdefault(r['port'], []).append(r['cpu_us_per_sample'])
for port, v in sorted(by.items()):
    print(f'port {port}: n={len(v)} cpu us/sample median {statistics.median(v):.2f} ({min(v):.2f}-{max(v):.2f})')
ports = sorted(by)
if len(ports) == 2:
    a, b = (statistics.median(by[p]) for p in ports)
    print(f'{ports[1]} vs {ports[0]}: {100 * (b - a) / a:+.1f}%')
print('hashes:', hashes, 'equal:', len(set(hashes.values())) == 1)
