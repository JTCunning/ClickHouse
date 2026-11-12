#!/usr/bin/env python3
"""Handoff #17: resolving a __name__ regex to its literal metric names on a table with 20,000 names (gen_names.sh):
master's literal path with 11,111 / 20,000 names fails with "Query tree is too big" (max_expanded_ast_elements).
Lab server on HTTP port 18123."""
import time, urllib.request, urllib.parse, sys


def http(q, params=""):
    t = time.monotonic()
    try:
        with urllib.request.urlopen("http://127.0.0.1:18123/?" + params, data=q.encode(), timeout=900) as r: out = r.read().decode()
    except urllib.error.HTTPError as e:
        out = "ERR " + e.read().decode()[:250]
    return time.monotonic() - t, out.strip()
for re in ["m1.*", ".+"]:
    _, L = http(f"SELECT arrayStringConcat(groupArray(n), '|') FROM (SELECT DISTINCT metric_name AS n FROM ts2 WHERE match(metric_name, '^{re}$') ORDER BY n)")
    for label, sel in [("regex", f'{{__name__=~"{re}"}}'), (f"resolved {L.count('|')+1}", '{__name__=~"' + L + '"}')]:
        q = f"SELECT count() FROM timeSeriesSelector(ts2, '{sel}', 1700000000, 1700003599, 'samples') SETTINGS max_threads=2, use_query_condition_cache=0"
        res = [http(q, "max_query_size=100000000") for _ in range(3)]
        print(f"{re:5} {label:15} " + " ".join(f"{t*1000:.0f}ms" for t, _ in res) + f"  -> {res[-1][1][:200]}")
