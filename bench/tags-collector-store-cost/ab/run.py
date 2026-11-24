#!/usr/bin/env python3
"""A/B of the tags-collector-store-cost plan: dashboard and rule-evaluation PromQL queries through the Prometheus HTTP
API, the way Grafana and a rule evaluator send them, on two servers started with ../../range-json-timestamps/server.sh
(Prometheus API on TCP port + 2000) and filled by ../../range-json-timestamps/gen.sh with the same T_END: SERIES series
of the counter `m` and of the gauge `g`, labels job (10 values), instance and pod, one sample every 15 s over HOURS hours.

Cases: R* are range queries (/api/v1/query_range, a dashboard panel), I* instant queries at T_END (/api/v1/query, an
alerting or recording rule), S1 a series listing (/api/v1/series, Grafana's label browser).
Per round, every case goes to both servers, in an order that rotates with the round and the case, so the two requests
of a pair run next to each other. Each request records its wall time, the response size, the md5 of the body, and
set_md5: the md5 of the series sorted by labels with values rounded to 12 significant digits, which ignores the order
of the series (sum() over threads can differ in its last digits, and a build may order series differently).
summarize.py prints the result table (Label | Query | Output | Base | This PR | Change | p | Results).

Env: PORTS="9001 9002" (TCP ports, A first), ROUNDS=100, CASES (labels below, default all), WINDOW=./window.txt
Usage: ab/run.py > ab/run-<date>.jsonl
"""
import hashlib, json, os, sys, time, urllib.error, urllib.parse, urllib.request

PORTS = [int(p) for p in os.environ.get('PORTS', '9001 9002').split()]
ROUNDS = int(os.environ.get('ROUNDS', 100))
T_START, T_END = map(int, open(os.environ.get('WINDOW', 'window.txt')).read().split())
HOUR = 3600
# label: (kind, PromQL or series matcher, range in seconds ending at T_END, step in seconds).
# gen.sh: 10 jobs, SERIES / 10 series per job.
CASES = {
    'R1': ('range', 'rate(m{job="job3"}[5m])', HOUR, 15),     # one service's per-instance request rate
    'R2': ('range', 'rate(m[5m])', HOUR, 15),                 # the same over every service
    'R3': ('range', 'sum by (job) (rate(m[5m]))', HOUR, 15),  # aggregated per service
    'R4': ('range', 'g{job="job3"}', HOUR, 15),               # plain gauge panel, one service
    'R5': ('range', 'g', HOUR, 15),                           # plain gauge over every service
    'R6': ('range', 'rate(m[5m])', 6 * HOUR, 30),             # a 6 h dashboard
    'R7': ('range', 'topk(10, rate(m[5m]))', HOUR, 15),       # "top 10 pods by request rate"
    'I1': ('instant', 'sum by (job) (rate(m[5m]))', 0, 0),    # recording rule job:m:rate5m
    'I2': ('instant', 'rate(m[5m])', 0, 0),                   # per-series rule over the whole metric
    'I3': ('instant', 'sum(rate(m[5m])) > 0', 0, 0),          # alerting rule over the whole metric
    'I4': ('instant', 'g', 0, 0),                             # plain instant read of every series
    'S1': ('series', 'g{job="job3"}', HOUR, 0),               # label browser: the series of one service
}
names = os.environ.get('CASES', ' '.join(CASES)).split()


def set_md5(data, kind):
    doc = json.loads(data)
    res = doc['data'] if kind == 'series' else doc['data']['result']
    if kind == 'series':
        rows = sorted(json.dumps(s, sort_keys=True) for s in res)
    else:
        def r(v):
            return v if v in ('NaN', '+Inf', '-Inf') else f'{float(v):.12g}'
        rows = sorted(json.dumps([s['metric'], [[t, r(v)] for t, v in s['values']] if 'values' in s else [s['value'][0], r(s['value'][1])]],
                                 sort_keys=True) for s in res)
    return hashlib.md5('\n'.join(rows).encode()).hexdigest(), len(res), sum(len(s.get('values', [0])) for s in res) if kind != 'series' else 0


def api(port, kind, promql, span, step):
    """One request; an HTTP error is printed to stderr with its body and retried up to twice. Returns (seconds,
    body, retries); the time is the successful request's."""
    start = max(T_START, T_END - span)
    if kind == 'range':
        path, params = 'query_range', {'query': promql, 'start': start, 'end': T_END, 'step': step}
    elif kind == 'instant':
        path, params = 'query', {'query': promql, 'time': T_END}
    else:
        path, params = 'series', {'match[]': promql, 'start': start, 'end': T_END}
    body = urllib.parse.urlencode(params).encode()
    for attempt in range(3):
        t = time.monotonic()
        try:
            with urllib.request.urlopen(f'http://127.0.0.1:{port + 2000}/api/v1/{path}', data=body, timeout=600) as r:
                data = r.read()
            return time.monotonic() - t, data, attempt
        except urllib.error.HTTPError as e:
            print(f'port {port} {kind} {promql!r} attempt {attempt}: HTTP {e.code}: {e.read()[:2000]!r}', file=sys.stderr, flush=True)
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
            kind, promql, span, step = CASES[name]
            smd5, series, points = set_md5(data, kind)
            print(json.dumps(dict(case=name, kind=kind, query=promql, span=span, step=step, port=port, round=rnd, retries=retries,
                                  ms=round(1000 * dt, 3), bytes=len(data), series=series, points=points,
                                  md5=hashlib.md5(data).hexdigest(), values_md5=smd5)), flush=True)
