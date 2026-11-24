#!/usr/bin/env python3
"""Summarize ab/run.py output (copied from ../../ts-from-grid-typed-write/realistic/summarize.py) as the result table: Label | Query | Output | Base | This PR | Change | p | Results.
Base is the first port of PORTS (A), This PR the second (B). Change is the median of the per-round ratios B / A (the
two requests of a round run next to each other, so drift cancels); p is a two-sided Wilcoxon signed-rank test on the
log ratios (normal approximation); a difference counts at p < 0.005, the 99.5% of clickhouse benchmark --confidence 5.
Results: "equal" when every body has the same md5 on both ports ("equal (as a set)" when only the series sorted by
labels, values rounded, do: another series order or last-digit sum() differences).
Usage: ab/summarize.py run.jsonl [PORTS="9001 9002"]"""
import json, math, os, statistics, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
A, B = (int(p) for p in os.environ.get('PORTS', '9001 9002').split())


def wilcoxon_p(d):
    d = [x for x in d if x != 0]
    n = len(d)
    if n < 10:
        return float('nan')
    order = sorted(range(n), key=lambda i: abs(d[i]))
    ranks = [0.0] * n
    i = 0
    while i < n:
        j = i
        while j + 1 < n and abs(d[order[j + 1]]) == abs(d[order[i]]):
            j += 1
        for k in range(i, j + 1):
            ranks[order[k]] = (i + j) / 2 + 1
        i = j + 1
    w = sum(r for r, x in zip(ranks, d) if x > 0)
    z = (w - n * (n + 1) / 4) / math.sqrt(n * (n + 1) * (2 * n + 1) / 24)
    return math.erfc(abs(z) / math.sqrt(2))


def span(r):
    if r.get('kind') == 'instant':
        return 'instant'
    s = r['span']
    return (f'{s // 3600} h' if s % 3600 == 0 else f'{s} s') + (f', step {r["step"]} s' if r.get('kind') != 'series' else ' (series API)')


cases = {}
for r in rows:
    cases.setdefault(r['case'], {}).setdefault(r['round'], {})[r['port']] = r
print('| Label | Query | Output | Base | This PR | Change | p | Results |')
print('|---|---|---|---|---|---|---|---|')
for case, by_round in cases.items():
    pairs = [(v[A], v[B]) for v in by_round.values() if A in v and B in v]
    if not pairs:
        print(f'| {case} | (no round with both ports: see stderr of run.py) | | | | | | |')
        continue
    a, b = [x['ms'] for x, _ in pairs], [y['ms'] for _, y in pairs]
    ratios = [y / x for x, y in zip(a, b)]
    change = 100 * (statistics.median(ratios) - 1)
    p = wilcoxon_p([math.log(r) for r in ratios])
    first = pairs[0][0]
    md5s = {x['md5'] for pr in pairs for x in pr}
    vmd5s = {x['values_md5'] for pr in pairs for x in pr}
    res = 'equal' if len(md5s) == 1 else 'equal (as a set)' if len(vmd5s) == 1 else 'DIFFER'
    sig = '' if math.isnan(p) else (' (difference)' if p < 0.005 else ' (no difference)')
    print(f"| {case} | `{first['query']}`, {span(first)} | {first['series']} series, {first['points']} points, "
          f"{first['bytes'] / 1e6:.1f} MB | {statistics.median(a):.2f} ms | {statistics.median(b):.2f} ms | {change:+.1f}% | "
          f"{p:.3g}{sig} | {res} |")
retried = {c: sum(x.get('retries', 0) for v in br.values() for x in v.values()) for c, br in cases.items()}
print(f'\nn = {len(pairs)} rounds per case; Base = port {A}, This PR = port {B}. Retried requests (HTTP errors, see stderr): ' + (', '.join(f'{c} {n}' for c, n in retried.items() if n) or 'none') + '.')
