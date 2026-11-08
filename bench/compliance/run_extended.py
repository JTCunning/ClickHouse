#!/usr/bin/env python3
"""Run the extended_support PromQL compliance suite against a local ClickHouse
server and print every behavioral (non-passed, non-unsupported) eval.

Stdlib only. Mirrors test_compliance.py::test_promql_extended_support, using the
in-tree loader from a ClickHouse checkout.

Usage:
  run_extended.py [--repo PATH]  (or set CLICKHOUSE_REPO) [--url http://127.0.0.1:18123] [--grep TEXT] [--file NAME]
"""
import argparse
import os
import sys
import urllib.parse
import urllib.request
import re
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("--repo", default=os.environ.get("CLICKHOUSE_REPO", "."), help="ClickHouse checkout (default: $CLICKHOUSE_REPO or .)")
ap.add_argument("--url", default="http://127.0.0.1:18123")
ap.add_argument("--grep", default="", help="only print failures whose reason contains this")
ap.add_argument("--file", default="", help="only run this .test file")
ap.add_argument("--sql", action="store_true", help="also print the SQL")
args = ap.parse_args()

sys.path.insert(0, str(Path(args.repo) / "tests/integration/test_prometheus_protocols"))
import promqltest_loader as pq  # noqa: E402

PARAMS = {"http_wait_end_of_query": "1", "http_response_buffer_size": "1048576"}
EXC_RE = re.compile(r"Code: \d+\. DB::Exception")


def run(sql):
    url = args.url + "/?" + urllib.parse.urlencode(PARAMS)
    req = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            body = r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return "", f"{e.code} {e.reason}: {e.read().decode('utf-8', 'replace')}"
    except Exception as e:  # noqa: BLE001
        return "", str(e)
    if EXC_RE.search(body):
        return "", body
    return body, None


T = pq.TABLE_NAME
run(f"DROP TABLE IF EXISTS {T} SYNC")
_, err = run(f"CREATE TABLE {T} ENGINE=TimeSeries")
if err:
    sys.exit(err)
counts = {}
try:
    for sc in pq.parse_all_files():
        if args.file and sc.file_name != args.file:
            continue
        run(f"TRUNCATE TABLE {T}")
        for cmd in sc.commands:
            if isinstance(cmd, pq.LoadBlock):
                vals = [v for v in (pq.series_insert_values(cmd.interval_ns, s) for s in cmd.series) if v]
                if vals:
                    _, err = run(f"INSERT INTO {T} (metric_name, tags, samples) VALUES " + ", ".join(vals))
                    if err:
                        print(f"INSERT failed {sc.file_name}:{cmd.line}: {err[:300]}")
                continue
            if pq.classify_eval(cmd):
                continue
            sql = pq.eval_sql(T, cmd)
            tsv, err = run(sql)
            status, reason = pq.compare_eval(cmd, tsv, err)
            counts[status] = counts.get(status, 0) + 1
            if status in ("passed", "unsupported") or args.grep not in reason:
                continue
            print(f"== {sc.file_name}:{cmd.line} [{cmd.kind}] {cmd.expr}")
            print(f"   reason: {reason[:300]}")
            exp = getattr(cmd, "expected", None)
            if exp is not None:
                print(f"   expected: {str(exp)[:400]}")
            print(f"   actual: {(tsv or err or '')[:400]!r}")
            if args.sql:
                print(f"   sql: {sql}")
finally:
    run(f"DROP TABLE IF EXISTS {T} SYNC")
print("counts:", counts)
