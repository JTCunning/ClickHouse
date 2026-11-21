#!/usr/bin/env python3
"""Summarize ab/run.py output: per case and port the median and min-max of the request time, the second port's change
against the first, whether every response body of a case has the same md5 on all ports, and whether the bodies
with values rounded to 12 significant digits do (the check for aggregated cases)."""
import json, statistics, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
cases = {}
for r in rows:
    cases.setdefault(r['case'], {}).setdefault(r['port'], []).append(r)
for case, by in cases.items():
    ports = sorted(by)
    med = {p: statistics.median(x['ms'] for x in by[p]) for p in ports}
    line = '  '.join(f"{p}: {med[p]:.1f} ms ({min(x['ms'] for x in by[p]):.0f}-{max(x['ms'] for x in by[p]):.0f}, n={len(by[p])})" for p in ports)
    change = f"{100 * (med[ports[1]] - med[ports[0]]) / med[ports[0]]:+.1f}%" if len(ports) == 2 else ''
    md5s = {x['md5'] for p in ports for x in by[p]}
    vmd5s = {x.get('values_md5') for p in ports for x in by[p]}
    print(f"{case:20s} {line}  {change}  bodies equal: {len(md5s) == 1}  values equal: {len(vmd5s) == 1}  bytes {by[ports[0]][0]['bytes']}")
