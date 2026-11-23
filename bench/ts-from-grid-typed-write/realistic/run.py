#!/usr/bin/env python3
"""Run 2 of ts-from-grid-typed-write: dashboard-shaped PromQL range queries through the Prometheus HTTP API
(/api/v1/query_range, JSON), the way Grafana sends them, on two servers started with
../../range-json-timestamps/server.sh (Prometheus API on TCP port + 2000) and filled by
../../range-json-timestamps/gen.sh with the same T_END: SERIES series of the counter `m` and of the gauge `g`,
labels job (10 values), instance and pod, one sample every 15 s over HOURS hours.

Per round, every case goes to both servers, in an order that rotates with the round and the case, so the two requests
of a pair run next to each other. Each request records its wall time, the response size, the md5 of the body and
the md5 with values rounded to 12 significant digits (sum() over threads can differ in its last digits).
summarize.py prints the result table (Label | Query | Base | This PR | ...).

Env: PORTS="9001 9002" (TCP ports, A first), ROUNDS=100, CASES (labels below, default all), WINDOW=./window.txt
Usage: realistic/run.py > realistic/run-<date>.jsonl
"""
import hashlib, json, os, sys, time, urllib.error, urllib.parse, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
PORTS = [int(p) for p in os.environ.get('PORTS', '9001 9002').split()]
ROUNDS = int(os.environ.get('ROUNDS', 100))
T_START, T_END = map(int, open(os.environ.get('WINDOW', 'window.txt')).read().split())
HOUR = 3600
# label: (PromQL, range in seconds ending at T_END, step in seconds). gen.sh: 10 jobs, SERIES / 10 series per job.
CASES = {
    'R1': ('rate(m{job="job3"}[5m])', HOUR, 15),      # one service's per-instance request rate: SERIES/10 series x 241 points
    'R2': ('rate(m[5m])', HOUR, 15),                  # the same over every service: SERIES series x 241 points
    'R3': ('sum by (job) (rate(m[5m]))', HOUR, 15),   # aggregated: 10 series x 241 points (control: few output points)
    'R4': ('g{job="job3"}', HOUR, 15),                # plain gauge panel, one service: SERIES/10 series x 241 points
    'R5': ('g', HOUR, 15),                            # plain gauge over every service: SERIES series x 241 points
    'R6': ('rate(m[5m])', 6 * HOUR, 30),              # a 6 h dashboard: SERIES series x 721 points
}
names = os.environ.get('CASES', ' '.join(CASES)).split()


def values_md5(data):
    doc = json.loads(data)
    for s in doc['data']['result']:
        s['values'] = [[t, v if v in ('NaN', '+Inf', '-Inf') else f'{float(v):.12g}'] for t, v in s['values']]
    return hashlib.md5(json.dumps(doc, sort_keys=True).encode()).hexdigest()


def api(port, promql, span, step):
    """One request; an HTTP error is printed to stderr with its body and retried up to twice. Returns (seconds,
    body, retries); the time is the successful request's."""
    start = max(T_START, T_END - span)
    body = urllib.parse.urlencode({'query': promql, 'start': start, 'end': T_END, 'step': step}).encode()
    for attempt in range(3):
        t = time.monotonic()
        try:
            with urllib.request.urlopen(f'http://127.0.0.1:{port + 2000}/api/v1/query_range', data=body, timeout=600) as r:
                data = r.read()
            return time.monotonic() - t, data, attempt
        except urllib.error.HTTPError as e:
            print(f'port {port} {promql!r} attempt {attempt}: HTTP {e.code}: {e.read()[:2000]!r}', file=sys.stderr, flush=True)
            if attempt == 2:
                raise


for name in names:  # warm-up: three requests per case and server
    for port in PORTS:
        for _ in range(3):
            api(port, *CASES[name])
for rnd in range(ROUNDS):
    for k, name in enumerate(names):
        for j in range(len(PORTS)):
            port = PORTS[(rnd + k + j) % len(PORTS)]
            dt, data, retries = api(port, *CASES[name])
            doc = json.loads(data)
            promql, span, step = CASES[name]
            print(json.dumps(dict(case=name, query=promql, span=span, step=step, port=port, round=rnd, retries=retries, ms=round(1000 * dt, 3), bytes=len(data),
                                  series=len(doc['data']['result']),
                                  points=sum(len(s['values']) for s in doc['data']['result']),
                                  md5=hashlib.md5(data).hexdigest(), values_md5=values_md5(data))), flush=True)
