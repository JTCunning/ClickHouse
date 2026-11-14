#!/usr/bin/env python3
"""Handoff #18: rows into the time ranges join and into the id set of a TimeSeries selector, master vs collapsed variants.
Usage: [MODE=limit1] bench.py <rounds> <out.jsonl> [variant,...] [case index,...]
Each run builds the id set the way the selector does (an `IN (<ids query>)` set, here probed by one row of numbers(1)),
warm, interleaved per round (order reversed every other round), one discarded warm-up per variant and case.
Reads duration, read_rows, memory, CPU from query_log; rows into and out of MergeJoinTransform from processors_profile_log;
the CreatingSetsTransform line (entries, rows, seconds) from text_log."""
import json
import re
import sys
import uuid

import idsq

ROUNDS, OUT = int(sys.argv[1]), sys.argv[2]
VARIANTS = sys.argv[3].split(",") if len(sys.argv) > 3 else [
    "any_none_none", "all_none_group", "any_none_distinct", "any_distinct_none", "any_distinct_distinct", "any_group_group",
    "any_limitby_none", "any_final_none"]
CASES = [list(idsq.CASES)[int(i)] for i in sys.argv[4].split(",")] if len(sys.argv) > 4 else list(idsq.CASES)


MODE = __import__("os").environ.get("MODE", "set")  # set: build the IN set; limit1: the first id only (probe, series?limit=)


def lookup(variant, case):
    if MODE == "limit1":
        return (f"SELECT count() FROM (SELECT series_id FROM ({idsq.ids_query(variant, case)}) LIMIT 1) "
                f"SETTINGS {idsq.SETTINGS}{idsq.extra_settings(variant)}, log_processors_profiles = 1")
    return (f"SELECT count() FROM numbers(1) WHERE (number, toUUID('00000000-0000-0000-0000-000000000000')) IN "
            f"({idsq.ids_query(variant, case)}) SETTINGS {idsq.SETTINGS}{idsq.extra_settings(variant)}, log_processors_profiles = 1")


rows = []
for case in CASES:
    for v in VARIANTS:
        idsq.client(lookup(v, case))
    for rnd in range(ROUNDS):
        for v in (VARIANTS if rnd % 2 == 0 else VARIANTS[::-1]):
            qid = f"b18-{v}-{uuid.uuid4().hex[:10]}"
            idsq.client(lookup(v, case), qid)
            rows.append({"case": case, "variant": v, "round": rnd, "qid": qid})
    # one standalone count of the ids query: distinct ids (must match master) and its rows
    for v in VARIANTS:
        r = idsq.client(f"SELECT count(), uniqExact(series_id) FROM ({idsq.ids_query(v, case)}) SETTINGS {idsq.SETTINGS}{idsq.extra_settings(v)}").split()
        for x in rows:
            if x["case"] == case and x["variant"] == v:
                x["ids_rows"], x["ids_distinct"] = int(r[0]), int(r[1])

idsq.client("SYSTEM FLUSH LOGS")
for r in rows:
    q = idsq.client(f"SELECT query_duration_ms, read_rows, memory_usage, ProfileEvents['OSCPUVirtualTimeMicroseconds'] "
                    f"FROM system.query_log WHERE query_id = '{r['qid']}' AND type = 'QueryFinish'").split()
    r.update(ms=int(q[0]), read_rows=int(q[1]), memory=int(q[2]), cpu_us=int(q[3]))
    j = idsq.client(f"SELECT sum(input_rows), sum(output_rows) FROM system.processors_profile_log "
                    f"WHERE query_id = '{r['qid']}' AND name = 'MergeJoinTransform'").split()
    r.update(join_in=int(j[0]), join_out=int(j[1]))
    m = idsq.client(f"SELECT message FROM system.text_log WHERE query_id = '{r['qid']}' AND message LIKE 'Created Set with%'").strip()
    g = re.search(r"with (\d+) entries from (\d+) rows in ([0-9.]+) sec", m)
    r.update(set_entries=int(g[1]), set_rows=int(g[2]), set_s=float(g[3])) if g else None

with open(OUT, "w") as f:
    for r in rows:
        f.write(json.dumps(r) + "\n")


def med(xs):
    xs = sorted(xs)
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


print(f"{'case':30} {'variant':22} {'ms med':>7} {'min-max':>11} {'cpu ms':>7} {'MiB':>6} {'join in':>9} {'join out':>9} "
      f"{'set rows':>9} {'set ent':>7} {'set s':>6} {'read_rows':>9} {'ids':>6}")
for case in CASES:
    for v in VARIANTS:
        rs = [r for r in rows if r["case"] == case and r["variant"] == v]
        d = [r["ms"] for r in rs]
        print(f"{case:30} {v:22} {med(d):7.0f} {min(d):5}-{max(d):<5} {med([r['cpu_us'] for r in rs]) / 1000:7.0f} "
              f"{med([r['memory'] for r in rs]) / 2**20:6.0f} {rs[0]['join_in']:9} {rs[0]['join_out']:9} "
              f"{rs[0].get('set_rows', -1):9} {rs[0].get('set_entries', -1):7} {med([r.get('set_s', -1) for r in rs]):6.3f} "
              f"{med([r['read_rows'] for r in rs]):9.0f} {rs[0]['ids_distinct']:6}")
