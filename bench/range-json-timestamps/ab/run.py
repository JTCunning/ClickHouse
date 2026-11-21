#!/usr/bin/env python3
"""A/B of /api/v1/query_range on two servers started with ../server.sh (Prometheus API on TCP + 2000), each filled by
../gen.sh with the same T_END (so both hold the same samples). Per round, every case is sent to every server in an
order that rotates with the round; the request's wall time, the size and the md5 of the response body are recorded,
plus the md5 with values rounded to 12 significant digits (values_md5), which is the check for aggregated cases.
Prints one JSON line per request; ab/summarize.py gives the medians, the change and whether the bodies are equal.

Env: PORTS="9001 9002" (TCP), ROUNDS=30, CASES (names from CASES below, default all), WINDOW=../window.txt
Usage: ab/run.py > ab/run-<date>.jsonl
"""
import hashlib, json, os, time, urllib.parse, urllib.request


def values_md5(data):
    """md5 of the response with every value rounded to 12 significant digits: an aggregate such as sum() over
    parallel threads can differ in its last digits between two requests to the same server (G1 on 2026-11-21)."""
    doc = json.loads(data)
    for s in doc['data']['result']:
        s['values'] = [[t, v if v in ('NaN', '+Inf', '-Inf') else f'{float(v):.12g}'] for t, v in s['values']]
    return hashlib.md5(json.dumps(doc, sort_keys=True).encode()).hexdigest()


HERE = os.path.dirname(os.path.abspath(__file__))
PORTS = [int(p) for p in os.environ.get('PORTS', '9001 9002').split()]
ROUNDS = int(os.environ.get('ROUNDS', 30))
T_START, T_END = map(int, open(os.environ.get('WINDOW', os.path.join(HERE, '..', 'window.txt'))).read().split())
CASES = {
    'T1_m_step15': ('m', 15),             # 2,000 series x 1,441 points, integer values: timestamps dominate the writer
    'T2_rate_step30': ('rate(m[5m])', 30),  # 2,000 x 721, long float values
    'T3_g_step60': ('g', 60),             # 2,000 x 361, 3-decimal gauge
    'E1_skewed_c_step15': ('c', 15),       # 1 series x 1,440 points next to 1,000 series x 72 points at its end
    'E2_every_other_h_step15': ('h', 15, {'lookback_delta': '10s'}),  # 1 x 1,440 next to 1,000 x 720: a point every other step
    'G1_sum_rate_step30': ('sum(rate(m[5m]))', 30),  # one series: the writer is negligible (control)
}
names = os.environ.get('CASES', ' '.join(CASES)).split()


def api(port, promql, step, extra=None):
    body = urllib.parse.urlencode({'query': promql, 'start': T_START, 'end': T_END, 'step': step, **(extra or {})}).encode()
    t = time.monotonic()
    with urllib.request.urlopen(f'http://127.0.0.1:{port + 2000}/api/v1/query_range', data=body, timeout=600) as r:
        data = r.read()
    return time.monotonic() - t, data


for name in names:  # warm-up: one request per case and server
    for port in PORTS:
        api(port, *CASES[name])
for rnd in range(ROUNDS):
    for k, name in enumerate(names):
        for j in range(len(PORTS)):
            port = PORTS[(rnd + k + j) % len(PORTS)]
            dt, data = api(port, *CASES[name])
            print(json.dumps(dict(case=name, port=port, round=rnd, ms=round(1000 * dt, 2), bytes=len(data),
                                  md5=hashlib.md5(data).hexdigest(), values_md5=values_md5(data))), flush=True)
