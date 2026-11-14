#!/usr/bin/env python3
"""Handoff #17, option A: the metric-level filter on the time ranges table, emulated on master (no build).

Data: the fill of tests/performance/promql_selector_name_alternatives.xml (200 metrics x 1000 series, 20 samples).
For each regex selector, master's generated SQL is captured from its "Will execute query" debug line, then run as:
  A    master's SQL
  set  + `tupleElement(id, 1) IN (SELECT tupleElement(id, 1) FROM <tags> WHERE <name matcher>)`: the branch's filter on
       master, where the primary key can't use it (the set's cost, no pruning)
  set in indexHint  the same set inside `indexHint`: no row-level check (the join drops unselected rows anyway)
  hint + only the `indexHint` below (what the hint alone costs and prunes)
  B~   + the same set, plus `indexHint(<id ranges of the matched metrics>)`: skips the granules that the branch on top of
       upstream #115482 should skip (the emulated branch)
Usage: emulate.py <rounds>   (lab server on HTTP port 18123, see README.md)
"""
import re, statistics, sys, time, urllib.request, uuid

ROUNDS = int(sys.argv[1])
TABLE = "promql_name_alternatives_ts"
REGEXES = {"narrow .*_199 (1 of 200 metrics)": ".*_199", "broad m_1[0-9][0-9] (100 of 200, XML control)": "m_1[0-9][0-9]"}
# What StorageTimeSeriesSelector::readImpl sets for its query, plus 4 streams as in the XML.
S = ("join_algorithm='full_sorting_merge', optimize_read_in_order=1, merge_tree_min_bytes_for_concurrent_read=4194304, "
     "merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem=4194304, merge_tree_min_rows_for_concurrent_read=0, "
     "max_threads=4, max_threads_min_free_memory_per_thread=0")


def http(q, qid=""):
    with urllib.request.urlopen("http://127.0.0.1:18123/?allow_experimental_time_series_table=1&query_id=" + qid, data=q.encode(), timeout=600) as r:
        return r.read().decode().strip()


if http(f"EXISTS TABLE {TABLE}") != "1":
    http(f"CREATE TABLE {TABLE} ENGINE = TimeSeries")
    http(f"""INSERT INTO {TABLE} (metric_name, tags, samples)
        SELECT concat('m_', toString(100 + number % 200)),
               map('instance', toString(intDiv(number, 200)), 'dc', toString(number % 8)),
               arrayMap(step -> (toDateTime64(1000000 + step * 15, 3), toFloat64(step)), range(20))
        FROM numbers_mt(200000) SETTINGS max_insert_threads = 8""")

cases = {}
for label, rx in REGEXES.items():
    tag = "tmf-cap-" + uuid.uuid4().hex[:8]
    http(f"SELECT count() FROM timeSeriesSelector('{TABLE}', '{{__name__=~\"{rx}\"}}', 1000000, 1000300, 'timestamp_and_value') "
         f"SETTINGS max_threads = 4, log_comment = '{tag}'")
    msg = ""
    for _ in range(10):  # the debug line can reach text_log after the first flush
        time.sleep(1)
        http("SYSTEM FLUSH LOGS query_log, text_log")
        msg = http(f"""SELECT t.message FROM system.text_log t INNER JOIN (SELECT query_id FROM system.query_log
        WHERE type = 'QueryFinish' AND log_comment = '{tag}') q USING (query_id)
            WHERE t.logger_name = 'StorageTimeSeriesSelector' AND startsWith(t.message, 'Will execute query:') FORMAT TSVRaw""")
        if msg:
            break
    sql = msg.split("Will execute query:", 1)[1].strip()
    tags_table = re.search(r"FROM (\S+\.`\.inner_id\.tags\.[0-9a-f-]+`)", sql).group(1)
    name_cond = re.search(r"match\(metric_name, '[^']*'\)", sql).group(0)
    names = http(f"SELECT DISTINCT metric_name FROM {tags_table} WHERE {name_cond} ORDER BY 1 FORMAT TSV").split("\n")
    lo, hi = "toUUID('00000000-0000-0000-0000-000000000000')", "toUUID('ffffffff-ffff-ffff-ffff-ffffffffffff')"
    ranges = [f"and(greaterOrEquals(id, tuple(sipHash64('{n}'), {lo})), lessOrEquals(id, tuple(sipHash64('{n}'), {hi})))" for n in names]
    hint = "indexHint(" + (f"or({', '.join(ranges)})" if len(ranges) > 1 else ranges[0]) + ")"
    metric_set = f"in(tupleElement(id, 1), (SELECT tupleElement(id, 1) FROM {tags_table} WHERE {name_cond}))"
    anchor = ")) AS time_ranges_subquery"
    assert sql.count(anchor) == 1, label
    variants = {"A": sql, "set": sql.replace(anchor, f", {metric_set}{anchor}"),
                "set in indexHint": sql.replace(anchor, f", indexHint({metric_set}){anchor}"),
                "hint": sql.replace(anchor, f", {hint}{anchor}"), "B~": sql.replace(anchor, f", {metric_set}, {hint}{anchor}")}
    for v, q in variants.items():
        cases[f"{label} | {v}"] = f"SELECT count() FROM ({q}) SETTINGS {S}"
    print(f"{label}: {len(names)} metrics matched", file=sys.stderr)

results = {c: http(q) for c, q in cases.items()}  # warm-up, and the results must agree within a regex
for label in REGEXES:
    got = {results[c] for c in cases if c.startswith(label)}
    assert len(got) == 1, (label, got)

ids = {c: [] for c in cases}
for rnd in range(ROUNDS):
    order = list(cases) if rnd % 2 == 0 else list(cases)[::-1]
    for c in order:
        qid = "tmf-" + uuid.uuid4().hex[:10]
        http(cases[c], qid)
        ids[c].append(qid)
time.sleep(1)
http("SYSTEM FLUSH LOGS query_log")
print(f"case\tresult\tmedian_ms\tmin_ms\tmax_ms\tread_rows\tmemory_MiB\t(n={ROUNDS}, interleaved)")
for c, lst in ids.items():
    rows = [http(f"SELECT query_duration_ms, read_rows, memory_usage FROM system.query_log WHERE query_id='{q}' AND type='QueryFinish' FORMAT TSV").split() for q in lst]
    ms = [int(r[0]) for r in rows]
    print(f"{c}\t{results[c]}\t{statistics.median(ms):.0f}\t{min(ms)}\t{max(ms)}\t{rows[0][1]}\t{int(rows[0][2]) / 2**20:.0f}")
