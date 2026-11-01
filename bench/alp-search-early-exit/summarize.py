#!/usr/bin/env python3
"""Summarize results-harness.txt: encode CPU change and whether master and branch bytes are identical."""
import re, sys
rows, cur = [], {}
for line in open(sys.argv[1] if len(sys.argv) > 1 else 'results-harness.txt'):
    if line.startswith('## '):
        cur = {'case': line[3:].strip()}
    m = re.match(r'(master|branch) .*alp_bytes=(\d+) alp_zstd_bytes=(\d+) alp_ms_median=([\d.]+)', line)
    if m:
        cur[m[1]] = (int(m[2]), int(m[3]), float(m[4]))
        if m[1] == 'branch':
            rows.append(cur)
    if 'MISMATCH' in line:
        print('ROUND TRIP MISMATCH in', cur.get('case'))
same = all(r['master'][:2] == r['branch'][:2] for r in rows)
print(f'{len(rows)} cases, bytes identical in all: {same}')
print('| case | master ms | branch ms | change |')
print('|---|---|---|---|')
for r in [r for r in rows if r["master"][2] >= 1.0]:
    m, b = r['master'][2], r['branch'][2]
    print(f"| {r['case']} | {m:.1f} | {b:.1f} | {100 * (b - m) / m:+.0f}% |")
