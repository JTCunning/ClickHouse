#!/usr/bin/env python3
"""Synthetic Prometheus-like Float64 value streams, written as raw little-endian Float64 (one file per dataset).

Layouts:
  fresh  - a newly written TimeSeries part: rows sorted by (series id, time) with 1 sample per series, so consecutive
           values belong to different series (the remote-write shape: one request = one sample of each series).
  merged - a merged part: each series contributes 2,048 consecutive samples.
Value kinds (per series):
  counter   integer request counter, increasing, occasional reset
  bucket    histogram bucket count (small integers)
  gauge2    random-walk gauge with 2 decimals (e.g. 23.47)
  const     0 / 1 gauges (up, flags)
  accsec    seconds counter accumulated in float (sum of 0.1 steps, so values carry binary noise)
  fullprec  full-precision doubles (ratios, _sum of latencies)
"""
import random, struct, sys

def series_stream(kind, rnd):
    if kind == 'counter':
        v = rnd.randint(0, 10**7)
        while True:
            yield float(v); v += rnd.randint(0, 50)
            if rnd.random() < 0.001: v = 0
    if kind == 'bucket':
        v = rnd.randint(0, 2000)
        while True:
            yield float(v); v += rnd.randint(0, 3)
    if kind == 'gauge2':
        v = rnd.randint(0, 100000)
        while True:
            yield v / 100; v += rnd.randint(-30, 30)
    if kind == 'const':
        v = float(rnd.random() < 0.9)
        while True:
            yield v
    if kind == 'accsec':
        v = 0.0
        for _ in range(rnd.randint(0, 5000)): v += 0.1
        while True:
            yield v
            for _ in range(rnd.randint(0, 20)): v += 0.1
    if kind == 'fullprec':
        v = rnd.random() * 1000
        while True:
            yield v; v += rnd.random()

MIX = [('counter', 40), ('bucket', 20), ('gauge2', 10), ('const', 10), ('accsec', 10), ('fullprec', 10)]

def kinds_for(name, n, rnd):
    if name == 'mixed':
        pool = [k for k, w in MIX for _ in range(w)]
        return [rnd.choice(pool) for _ in range(n)]
    return [name] * n

def main(out_dir, total=1_000_000):
    for layout in ('fresh', 'merged'):
        for name in ['mixed'] + [k for k, _ in MIX]:
            rnd = random.Random(42)
            per_series = 1 if layout == 'fresh' else 2048
            n_series = total // per_series if layout == 'merged' else 20000
            kinds = kinds_for(name, n_series, rnd)
            streams = [series_stream(k, rnd) for k in kinds]
            vals = []
            if layout == 'fresh':
                # 50 requests of 20,000 series: each request is its own part, sorted by series inside it
                for _ in range(total // n_series):
                    vals.extend(next(s) for s in streams)
            else:
                for s in streams:
                    vals.extend(next(s) for _ in range(per_series))
            with open(f'{out_dir}/{layout}_{name}.f64', 'wb') as f:
                f.write(struct.pack(f'<{len(vals)}d', *vals))

if __name__ == '__main__':
    main(sys.argv[1])
