#!/usr/bin/env python3
"""Handoff #17: the ids query of a TimeSeries selector (the query behind /api/v1/series,
/api/v1/label/<name>/values with match[] and the tags subquery of every PromQL selector),
as master builds it and with the proposed filter `id IN (SELECT id FROM tags WHERE <matchers>)`
in the time ranges subquery, written out by hand (no build of the change exists).

Usage: bench.py <clickhouse binary> <tcp port> <rounds> <out.jsonl>
Runs master and fix interleaved per round, warm (one discarded warm-up per variant and case),
and reads read_rows, memory_usage and the selected marks of the time ranges table from the
server's system.query_log / system.text_log.
"""
import json
import subprocess
import sys
import time
import uuid

CH, PORT, ROUNDS, OUT = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
T0, T1 = 1700000000, 1700003599

# Settings that StorageTimeSeriesSelector::applySettingsForSelectIDsQuery sets for this query.
SETTINGS = ("join_algorithm = 'full_sorting_merge', optimize_read_in_order = 1, max_threads = 2, "
            "use_query_condition_cache = 0")  # the cache would hide the tags scan after the first run


def client(query, query_id=None, fmt="TSV"):
    args = [CH, "client", "--port", PORT, "-q", query, "--format", fmt]
    if query_id:
        args += ["--query_id", query_id]
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout


TS_UUID = client("SELECT uuid FROM system.tables WHERE database = 'default' AND name = 'ts'").strip()


def inner(kind):
    return f".inner_id.{kind}.{TS_UUID}"


TAGS, TR = f"default.`{inner('tags')}`", f"default.`{inner('timeranges')}`"
TIME = f"max_time >= toDateTime64({T0}, 3) AND min_time <= toDateTime64({T1}, 3)"

# case: (role, tags WHERE (all matchers, as matcherToAST makes them), the __name__ matchers only or None)
# Variants: master; fix = `id IN (SELECT id FROM tags WHERE <__name__ matchers>)`, only when there is an `=`/`=~` matcher
# on __name__ (the branch); fix_all = the same with all the matchers (the handoff's first idea, for comparison).
CASES = {
    "regex .*cpu.* (3 names, 15k series)": ("target", "match(metric_name, '^.*cpu.*$')", "match(metric_name, '^.*cpu.*$')"),
    "regex .*cpu.* + instance=host-1 (30 series)": ("target", "match(metric_name, '^.*cpu.*$') AND tags['instance'] = 'host-1'", "match(metric_name, '^.*cpu.*$')"),
    "regex node_cpu.*|process_cpu.* (2 names, 10k)": ("target", "match(metric_name, '^(node_cpu.*|process_cpu.*)$')", "match(metric_name, '^(node_cpu.*|process_cpu.*)$')"),
    "regex metric_1.* (111 names, 555k series)": ("edge", "match(metric_name, '^metric_1.*$')", "match(metric_name, '^metric_1.*$')"),
    "regex .+ (every series, 2.1M)": ("edge", "match(metric_name, '^.+$')", "match(metric_name, '^.+$')"),
    "job=job-3 only (no name matcher, 200k series)": ("guard", "tags['job'] = 'job-3'", None),
}
CASES_SELECTIVITY = {
    "regex metric_1[0-9]_total (10 names, 50k, 2.4%)": ("edge", "match(metric_name, '^metric_1[0-9]_total$')", "match(metric_name, '^metric_1[0-9]_total$')"),
    "regex metric_[1-4][0-9]_total (40 names, 200k, 9.5%)": ("edge", "match(metric_name, '^metric_[1-4][0-9]_total$')", "match(metric_name, '^metric_[1-4][0-9]_total$')"),
    "regex metric_[1-9][0-9]_total (90 names, 450k, 21%)": ("edge", "match(metric_name, '^metric_[1-9][0-9]_total$')", "match(metric_name, '^metric_[1-9][0-9]_total$')"),
}
VARIANTS = ("master", "fix", "fix_all", "first", "first_hash")
# first = `tupleElement(id, 1) IN (SELECT tupleElement(id, 1) FROM tags WHERE <__name__ matchers>)`: a set of metrics, not of series
#         (no primary key use: KeyCondition does not see through tupleElement); first_hash = the same set made from
#         sipHash64(metric_name), the first component of the default id generator, so only metric_name is read.
import os
if os.environ.get("CASESET") == "selectivity":
    CASES = CASES_SELECTIVITY
if os.environ.get("VARIANTS"):
    VARIANTS = tuple(os.environ["VARIANTS"].split(","))


def ids_query(where, name_where, variant):
    tr_where = TIME
    if variant == "fix" and name_where:
        tr_where += f" AND id IN (SELECT id FROM {TAGS} WHERE {name_where})"
    if variant == "fix_all":
        tr_where += f" AND id IN (SELECT id FROM {TAGS} WHERE {where})"
    if variant == "first" and name_where:
        tr_where += f" AND tupleElement(id, 1) IN (SELECT tupleElement(id, 1) FROM {TAGS} WHERE {name_where})"
    if variant == "first_hash" and name_where:
        tr_where += f" AND tupleElement(id, 1) IN (SELECT sipHash64(metric_name) FROM {TAGS} WHERE {name_where})"
    return (
        f"SELECT count(), sum(cityHash64(series_id)) FROM ("
        f"SELECT timeSeriesStoreTags(id, tags, '__name__', metric_name) AS series_id FROM {TAGS} "
        f"ALL LEFT JOIN (SELECT id, 1 AS matched FROM {TR} WHERE {tr_where}) AS time_ranges_subquery USING (id) "
        f"WHERE {where} AND time_ranges_subquery.matched = 1) SETTINGS {SETTINGS}"
    )


def run(case, variant, where, name_where, rnd):
    qid = f"b17-{variant}-{uuid.uuid4().hex[:12]}"
    t = time.monotonic()
    res = client(ids_query(where, name_where, variant), qid).strip()
    wall = time.monotonic() - t
    return {"case": case, "variant": variant, "round": rnd, "qid": qid, "result": res, "client_wall_s": round(wall, 4)}


rows = []
for case, (role, where, name_where) in CASES.items():
    for variant in VARIANTS:
        run(case, variant, where, name_where, -1)  # warm-up, discarded
    for rnd in range(ROUNDS):
        for variant in (VARIANTS if rnd % 2 == 0 else VARIANTS[::-1]):
            r = run(case, variant, where, name_where, rnd)
            r["role"] = role
            rows.append(r)

client("SYSTEM FLUSH LOGS")
for r in rows:
    q = client(
        f"SELECT query_duration_ms, read_rows, read_bytes, memory_usage, ProfileEvents['OSCPUVirtualTimeMicroseconds'] "
        f"FROM system.query_log WHERE query_id = '{r['qid']}' AND type = 'QueryFinish'").split()
    r.update(duration_ms=int(q[0]), read_rows=int(q[1]), read_bytes=int(q[2]), memory=int(q[3]), cpu_us=int(q[4]))
    marks = client(
        f"SELECT message FROM system.text_log WHERE query_id = '{r['qid']}' AND message LIKE '%marks by primary key%' "
        f"AND logger_name LIKE '%timeranges%'").strip()
    r["timeranges_marks"] = marks.split("parts by primary key, ")[-1].split(" marks by primary key")[0] if marks else "?"

with open(OUT, "w") as f:
    for r in rows:
        f.write(json.dumps(r) + "\n")


def med(xs):
    xs = sorted(xs)
    return xs[len(xs) // 2] if len(xs) % 2 else (xs[len(xs) // 2 - 1] + xs[len(xs) // 2]) / 2


print(f"{'case':48} {'variant':7} {'ms med':>7} {'min-max':>11} {'read_rows':>10} {'memory MiB':>10} {'tr marks':>9} result")
for case in CASES:
    for variant in VARIANTS:
        rs = [r for r in rows if r["case"] == case and r["variant"] == variant]
        d = [r["duration_ms"] for r in rs]
        print(f"{case:48} {variant:7} {med(d):7.0f} {min(d):5}-{max(d):<5} {med([r['read_rows'] for r in rs]):10.0f} "
              f"{med([r['memory'] for r in rs]) / 2**20:10.1f} {rs[0]['timeranges_marks']:>9} {rs[0]['result']}")
