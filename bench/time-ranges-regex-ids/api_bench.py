#!/usr/bin/env python3
"""Handoff #17, end to end through master's own code: the Prometheus HTTP endpoints
/api/v1/label/__name__/values and /api/v1/series with a non-literal __name__ regex, against the same
request with the regex replaced by the literal list of metric names it matches (what resolving the regex
against the distinct metric names of the tags table would hand to the existing literal-name path).
The resolution query itself is timed separately (SELECT DISTINCT metric_name ... WHERE match(...)).

Usage: api_bench.py <clickhouse binary> <tcp port> <prometheus port> <rounds> <out.jsonl>
"""
import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request

CH, PORT, PPORT, ROUNDS, OUT = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), sys.argv[5]
START, END = 1700000000, 1700003599


def client(query):
    return subprocess.run([CH, "client", "--port", PORT, "-q", query], check=True, capture_output=True, text=True).stdout


TAGS = "default.`" + client("SELECT name FROM system.tables WHERE database = 'default' AND name LIKE '.inner_id.tags.%'").strip() + "`"


def resolve(regex):
    # Over the HTTP interface (port 18123 in server-config.xml), so the time has no client start-up in it.
    q = (f"SELECT DISTINCT metric_name FROM {TAGS} WHERE match(metric_name, '^(?:{regex})$') ORDER BY metric_name "
         f"SETTINGS use_query_condition_cache = 0")
    with urllib.request.urlopen("http://127.0.0.1:18123/", data=q.encode(), timeout=600) as r:
        rows = r.read().decode().split("\n")
    return [r for r in rows if r]


def api(endpoint, selector):
    body = urllib.parse.urlencode({"match[]": selector, "start": START, "end": END}).encode()
    t = time.monotonic()
    with urllib.request.urlopen(f"http://127.0.0.1:{PPORT}/api/v1/{endpoint}", data=body, timeout=600) as r:
        data = json.loads(r.read())
    wall = time.monotonic() - t
    assert data["status"] == "success", data
    res = data["data"]
    key = sorted(res) if endpoint.startswith("label") else sorted(json.dumps(s, sort_keys=True) for s in res)
    return wall, len(res), hash(tuple(key))


# case: (regex on __name__, other matchers)
CASES = {
    ".*cpu.* (4 names, 15k series in window)": (".*cpu.*", ""),
    ".*cpu.* + instance=host-1 (30 series)": (".*cpu.*", ', instance="host-1"'),
    "node_cpu.*|process_cpu.* (2 names, 10k)": ("node_cpu.*|process_cpu.*", ""),
    "metric_1.* (110 names, 550k series)": ("metric_1.*", ""),
    ".+ (419 names, every series)": (".+", ""),
}

rows = []
for case, (regex, rest) in CASES.items():
    t = time.monotonic()
    names = resolve(regex)
    resolve_s = time.monotonic() - t
    variants = {"regex": f'{{__name__=~"{regex}"{rest}}}', "resolved": f'{{__name__=~"{"|".join(names)}"{rest}}}'}
    endpoints = ["label/__name__/values"] + (["series"] if "every" not in case and "550k" not in case else [])
    for endpoint in endpoints:
        for v, sel in variants.items():
            api(endpoint, sel)  # warm-up
        for rnd in range(ROUNDS):
            for v in (("regex", "resolved") if rnd % 2 == 0 else ("resolved", "regex")):
                wall, n, h = api(endpoint, variants[v])
                rows.append({"case": case, "endpoint": endpoint, "variant": v, "round": rnd, "wall_s": wall, "n": n, "hash": h,
                             "names": len(names)})
    # the resolution query alone, warm
    rt = []
    for _ in range(ROUNDS):
        t = time.monotonic()
        resolve(regex)
        rt.append(time.monotonic() - t)
    rows.append({"case": case, "endpoint": "resolve", "variant": "resolve", "wall_s_list": rt, "names": len(names)})

with open(OUT, "w") as f:
    for r in rows:
        f.write(json.dumps(r) + "\n")


def med(xs):
    xs = sorted(xs)
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


print(f"{'case':42} {'endpoint':22} {'regex ms':>9} {'resolved ms':>11} {'ratio':>6} {'same':>5} {'n':>7}")
for case in CASES:
    res = [r for r in rows if r["case"] == case and r["endpoint"] != "resolve"]
    for endpoint in sorted({r["endpoint"] for r in res}):
        a = [r for r in res if r["endpoint"] == endpoint and r["variant"] == "regex"]
        b = [r for r in res if r["endpoint"] == endpoint and r["variant"] == "resolved"]
        ma, mb = med([r["wall_s"] for r in a]) * 1000, med([r["wall_s"] for r in b]) * 1000
        same = len({r["hash"] for r in a + b}) == 1
        print(f"{case:42} {endpoint:22} {ma:9.1f} {mb:11.1f} {ma / mb:6.2f} {str(same):>5} {a[0]['n']:7}")
    rr = [r for r in rows if r["case"] == case and r["endpoint"] == "resolve"][0]
    print(f"{case:42} {'resolve query (client)':22} {med(rr['wall_s_list']) * 1000:9.1f} {'':11} {'':6} {'':5} {rr['names']:7}")
