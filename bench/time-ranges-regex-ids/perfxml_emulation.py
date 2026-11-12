#!/usr/bin/env python3
"""Handoff #17: the regex control of tests/performance/promql_selector_name_alternatives.xml (`m_1[0-9][0-9]`,
100 of 200 metrics, half the series) and its 100-name list, on master, and the regex with the set of ids written
into the time ranges subquery by hand (no build of the change). A = master's generated SQL (captured from the
"Will execute query" debug line), B = the same SQL plus `in(id, (SELECT id FROM <tags> WHERE <name matcher>))`.
Usage: perfxml_emulation.py <sqlA> <sqlB> <rounds>   (lab server on HTTP port 18123)
"""
import sys, time, urllib.request, uuid
A, B, ROUNDS = open(sys.argv[1]).read().strip(), open(sys.argv[2]).read().strip(), int(sys.argv[3])
# What StorageTimeSeriesSelector::readImpl sets for its query, plus 4 streams as in the XML.
S = ("join_algorithm='full_sorting_merge', optimize_read_in_order=1, merge_tree_min_bytes_for_concurrent_read=4194304, "
     "merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem=4194304, merge_tree_min_rows_for_concurrent_read=0, "
     "max_threads=4, max_threads_min_free_memory_per_thread=0")
NAMES = "|".join(f"m_{i}" for i in range(100, 200))
CASES = {
    "regex A (master SQL)": f"SELECT count() FROM ({A}) SETTINGS {S}",
    "regex B (+ id IN set)": f"SELECT count() FROM ({B}) SETTINGS {S}",
    "regex, table function": "SELECT count() FROM timeSeriesSelector('promql_name_alternatives_ts', '{__name__=~\"m_1[0-9][0-9]\"}', 1000000, 1000300, 'timestamp_and_value') SETTINGS max_threads = 4, max_threads_min_free_memory_per_thread=0",
    "100-name list, table function": "SELECT count() FROM timeSeriesSelector('promql_name_alternatives_ts', '{__name__=~\"" + NAMES + "\"}', 1000000, 1000300, 'timestamp_and_value') SETTINGS max_threads = 4, max_threads_min_free_memory_per_thread=0",
}
def http(q, qid=""):
    with urllib.request.urlopen("http://127.0.0.1:18123/?query_id=" + qid, data=q.encode(), timeout=600) as r:
        return r.read().decode().strip()
ids = {c: [] for c in CASES}
for c, q in CASES.items():
    http(q)
for rnd in range(ROUNDS):
    order = list(CASES) if rnd % 2 == 0 else list(CASES)[::-1]
    for c in order:
        qid = "pt17-" + uuid.uuid4().hex[:10]
        res = http(CASES[c], qid)
        ids[c].append((qid, res))
time.sleep(2)
http("SYSTEM FLUSH LOGS")
for c, lst in ids.items():
    d = []
    for qid, res in lst:
        row = http(f"SELECT query_duration_ms, read_rows, memory_usage FROM system.query_log WHERE query_id='{qid}' AND type='QueryFinish' FORMAT TSV").split()
        if not row:
            sys.exit(f"{c}: no QueryFinish row for {qid}, result {res[:200]}")
        d.append((int(row[0]), int(row[1]), int(row[2])))
    ms = sorted(x[0] for x in d)
    print(f"{c:32} median {ms[len(ms)//2]:5} ms  min {ms[0]:5}  max {ms[-1]:5}  read_rows {d[0][1]:9}  mem {d[0][2]/2**20:7.1f} MiB  result {lst[0][1]}")
