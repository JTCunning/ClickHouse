#!/usr/bin/env python3
"""Handoff #19: cost of the in-order read of the id lookup by selected parts (streams) and rows.
Usage: streams.py <rounds> <out.jsonl> <table>[,<table>...] [variant,...] [case index,...]
A <table> is a TimeSeries table name (its inner tags and time ranges tables are used), or `<name>=<tags fq name>|<time ranges fq name>`
to pair a tags table with another time ranges table (the emulated time ranges cache of loop.py).
Each run builds the id set the way the selector does (idsq/bench.py `lookup`), warm, interleaved per round (order reversed every
other round), one discarded warm-up per table, variant and case. Prints per table/variant/case the medians of: wall ms, CPU ms,
the in-order sources (`MergeTreeSelect(pool: ReadPoolInOrder ...)` processors = streams) and their summed elapsed time,
`MergingSortedTransform` elapsed, rows into `MergeJoinTransform`, SelectedParts/SelectedMarks, read_rows and the ids."""
import json
import os
import sys
import uuid

import idsq

ROUNDS, OUT = int(sys.argv[1]), sys.argv[2]
TABLES = sys.argv[3].split(",")
VARIANTS = sys.argv[4].split(",") if len(sys.argv) > 4 and sys.argv[4] else ["any_none_none"]
CASES = [list(idsq.CASES)[int(i)] for i in sys.argv[5].split(",")] if len(sys.argv) > 5 else list(idsq.CASES)


LOOKBACK = float(os.environ.get("LOOKBACK", 30))


def use_table(spec):
    """Points idsq at the tags and time ranges tables of `spec`; a table written by loop.py (<name>.parts.json in the current directory)
    is queried over the window [end - LOOKBACK, end] of its run, end = the moment its inserts stopped."""
    name, _, pair = spec.partition("=")
    if os.path.exists(f"{name}.parts.json"):
        end = json.load(open(f"{name}.parts.json"))["summary"]["end"]
        idsq.T0, idsq.T1 = round(end - LOOKBACK, 3), round(end, 3)
    if pair:
        tags, tr = pair.split("|")
        idsq.tables = lambda: (tags, tr)
    else:
        idsq.TABLE, idsq._uuid = name, None
        idsq.tables = _tables
    return name


_tables = idsq.tables


def lookup(variant, case):
    return (f"SELECT count() FROM numbers(1) WHERE (number, toUUID('00000000-0000-0000-0000-000000000000')) IN "
            f"({idsq.ids_query(variant, case)}) SETTINGS {idsq.SETTINGS}{idsq.extra_settings(variant)}, log_processors_profiles = 1")


rows = []
for case in CASES:
    for spec in TABLES:
        use_table(spec)
        for v in VARIANTS:
            idsq.client(lookup(v, case))
    for rnd in range(ROUNDS):
        order = [(s, v) for s in TABLES for v in VARIANTS]
        for spec, v in (order if rnd % 2 == 0 else order[::-1]):
            name = use_table(spec)
            qid = f"b19-{uuid.uuid4().hex[:12]}"
            idsq.client(lookup(v, case), qid)
            rows.append({"case": case, "table": name, "variant": v, "round": rnd, "qid": qid})
    for spec in TABLES:
        name = use_table(spec)
        for v in VARIANTS:
            r = idsq.client(f"SELECT count(), uniqExact(series_id) FROM ({idsq.ids_query(v, case)}) SETTINGS {idsq.SETTINGS}").split()
            for x in rows:
                if x["case"] == case and x["table"] == name and x["variant"] == v:
                    x["ids_rows"], x["ids"] = int(r[0]), int(r[1])

idsq.client("SYSTEM FLUSH LOGS")
for r in rows:
    q = idsq.client(f"SELECT query_duration_ms, read_rows, ProfileEvents['OSCPUVirtualTimeMicroseconds'], ProfileEvents['SelectedParts'], "
                    f"ProfileEvents['SelectedMarks'], memory_usage FROM system.query_log WHERE query_id = '{r['qid']}' AND type = 'QueryFinish'").split()
    r.update(ms=int(q[0]), read_rows=int(q[1]), cpu_us=int(q[2]), parts=int(q[3]), marks=int(q[4]), memory=int(q[5]))
    p = idsq.client(
        "SELECT countIf(name LIKE 'MergeTreeSelect(pool: ReadPoolInOrder%'), sumIf(elapsed_us, name LIKE 'MergeTreeSelect(pool: ReadPoolInOrder%'), "
        "sumIf(elapsed_us, name = 'MergingSortedTransform'), countIf(name = 'MergingSortedTransform'), "
        "sumIf(input_rows, name = 'MergeJoinTransform'), sumIf(elapsed_us, name = 'MergeJoinTransform'), "
        "sumIf(output_rows, name LIKE 'MergeTreeSelect(pool: ReadPoolInOrder%') "
        f"FROM system.processors_profile_log WHERE query_id = '{r['qid']}'").split()
    r.update(streams=int(p[0]), inorder_us=int(p[1]), mst_us=int(p[2]), mst_n=int(p[3]), join_in=int(p[4]), join_us=int(p[5]), inorder_rows=int(p[6]))

with open(OUT, "w") as f:
    for r in rows:
        f.write(json.dumps(r) + "\n")


def med(xs):
    xs = sorted(xs)
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


print(f"{'case':14} {'table':22} {'variant':16} {'ms':>6} {'min-max':>11} {'cpu':>6} {'streams':>7} {'inorder ms':>10} "
      f"{'merge ms':>8} {'join ms':>7} {'join in':>9} {'parts':>5} {'marks':>5} {'read_rows':>9} {'ids':>6}")
for case in CASES:
    for spec in TABLES:
        name = spec.partition("=")[0]
        for v in VARIANTS:
            rs = [r for r in rows if r["case"] == case and r["table"] == name and r["variant"] == v]
            d = [r["ms"] for r in rs]
            print(f"{case.split(':')[0]:14} {name:22} {v:16} {med(d):6.0f} {min(d):5}-{max(d):<5} {med([r['cpu_us'] for r in rs]) / 1000:6.0f} "
                  f"{rs[0]['streams']:7} {med([r['inorder_us'] for r in rs]) / 1000:10.0f} {med([r['mst_us'] for r in rs]) / 1000:8.0f} "
                  f"{med([r['join_us'] for r in rs]) / 1000:7.0f} {rs[0]['join_in']:9} {rs[0]['parts']:5} {rs[0]['marks']:5} "
                  f"{med([r['read_rows'] for r in rs]):9.0f} {rs[0]['ids']:6}")
