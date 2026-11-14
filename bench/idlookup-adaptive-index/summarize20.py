#!/usr/bin/env python3
"""Medians of the cold-lab matrix (results20.jsonl from matrix-cold.sh): one line per selector x variant x state, with the
planning time of the paired EXPLAIN indexes = 1 runs (label .../plan) and its share of the query's wall time."""
import collections, json, statistics, sys

runs = collections.defaultdict(list)
for line in open(sys.argv[1]):
    r = json.loads(line)
    runs[r['label']].append(r)
med = lambda xs: statistics.median(xs) if xs else float('nan')
print(f"{'selector/variant/state':22} {'wall':>6} {'min-max':>11} {'cpu':>5} {'thread':>7} {'plan':>5} {'share':>5} {'GETs':>5} "
      f"{'stats':>5} {'tagsGET':>7} {'trGET':>5}")
for label, rs in runs.items():
    if label.endswith('/plan'):
        continue
    ps = runs.get(label + '/plan', [])
    w = [r['wall_ms'] for r in rs]
    e = lambda k: med([r['events'].get(k, 0) for r in rs])
    g = lambda prefix: med([sum(v for k, v in r['gets_by_kind'].items() if k.startswith(prefix)) for r in rs])
    plan = med([p['wall_ms'] for p in ps])
    print(f"{label:22} {med(w):6.0f} {min(w):5d}-{max(w):<5d} {e('OSCPUVirtualTimeMicroseconds') / 1000:5.0f} "
          f"{e('RealTimeMicroseconds') / 1000:7.0f} {plan:5.0f} {plan / med(w):5.0%} {e('S3GetObject'):5.0f} {e('LoadedStatistics'):5.0f} "
          f"{g('tags'):7.0f} {g('timeranges'):5.0f}")


def markdown(path):
    """The report table: one row per selector x variant, one cell per state: wall ms (CPU ms / thread s / planning share)."""
    runs = collections.defaultdict(list)
    for line in open(path):
        r = json.loads(line)
        runs[r['label']].append(r)
    names = {'nc': '3 exact matchers, 397 ids', 'nr': '+ rare token, 1 id', 'w': 'name regex + matcher, 3,817 ids', 'lf': 'metric only, 3,172 ids'}
    print('| selector | variant | cold | prewarmed | warm |\n|---|---|---|---|---|')
    for sel in ['nc', 'nr', 'w', 'lf']:
        for v in ['base', 'noidx', 'thr', 'nosp', 'nojo', 'nostat', 'combo', 'tagsnostats']:
            src = 'noidx' if (v == 'thr' and sel != 'lf') else ('base' if v == 'thr' else v)
            cells = []
            for st in ['cold', 'meta', 'warm']:
                rs, ps = runs.get(f'{sel}/{src}/{st}', []), runs.get(f'{sel}/{src}/{st}/plan', [])
                if not rs:
                    cells.append('')
                    continue
                w = med([r['wall_ms'] for r in rs])
                cpu = med([r['events']['OSCPUVirtualTimeMicroseconds'] for r in rs]) / 1000
                thr = med([r['events']['RealTimeMicroseconds'] for r in rs]) / 1e6
                share = f', {med([p["wall_ms"] for p in ps]) / w:.0%}' if ps else ''
                cells.append(f'{w:,.0f} ({cpu:,.0f} / {thr:.1f}{share})')
            if any(cells):
                print(f'| {names[sel]} | {v} | ' + ' | '.join(cells) + ' |')


if len(sys.argv) > 2 and sys.argv[2] == '--md':
    markdown(sys.argv[1])
