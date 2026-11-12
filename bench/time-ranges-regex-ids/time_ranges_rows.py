#!/usr/bin/env python3
"""Rows read from the time ranges table alone, for a non-literal __name__ regex on master and with the id set,
and for the literal list of the same metric names (master's id-range path). The id set's own tags read is
measured separately and subtracted. Lab server on HTTP port 18123; the table from gen.sh."""
import time, urllib.request, uuid
def http(q, qid=""):
    with urllib.request.urlopen("http://127.0.0.1:18123/?query_id=" + qid, data=q.encode(), timeout=600) as r:
        return r.read().decode().strip()
U = http("SELECT uuid FROM system.tables WHERE database = 'default' AND name = 'ts'")
TAGS, TR = f"default.`.inner_id.tags.{U}`", f"default.`.inner_id.timeranges.{U}`"
TIME = "max_time >= toDateTime64(1700000000, 3) AND min_time <= toDateTime64(1700003599, 3)"
S = " SETTINGS use_query_condition_cache = 0, max_threads = 2"
NAMES = ["container_cpu_usage_seconds_total", "node_cpu_seconds_total", "old_cpu_seconds_total", "process_cpu_seconds_total"]
NAME_RE = "match(metric_name, '^.*cpu.*$')"
def ranges(names):
    r = " OR ".join(f"(id >= tuple(sipHash64('{n}'), toUUID('00000000-0000-0000-0000-000000000000')) AND id <= tuple(sipHash64('{n}'), toUUID('ffffffff-ffff-ffff-ffff-ffffffffffff')))" for n in names)
    h = ", ".join(f"sipHash64('{n}')" for n in names)
    return f"indexHint({r}) AND NOT (tupleElement(id, 1) NOT IN ({h}))"
Q = {
    "regex, master (time filter only)": f"SELECT count() FROM {TR} WHERE {TIME}",
    "regex, id set": f"SELECT count() FROM {TR} WHERE {TIME} AND id IN (SELECT id FROM {TAGS} WHERE {NAME_RE})",
    "the id set's tags read alone": f"SELECT count() FROM (SELECT id FROM {TAGS} WHERE {NAME_RE})",
    "literal list of the 4 names (master id ranges)": f"SELECT count() FROM {TR} WHERE {TIME} AND {ranges(NAMES)}",
}
out = {}
for name, q in Q.items():
    qid = "trr-" + uuid.uuid4().hex[:10]
    res = http(q + S, qid)
    out[name] = (qid, res)
time.sleep(2); http("SYSTEM FLUSH LOGS")
for name, (qid, res) in out.items():
    rr = http(f"SELECT read_rows FROM system.query_log WHERE query_id = '{qid}' AND type = 'QueryFinish'")
    out[name] = (int(rr), res)
    print(f"{name:48} read_rows {int(rr):9}  count {res}")
tags_alone = out["the id set's tags read alone"][0]
print(f"{'time ranges rows read with the id set':48} {out['regex, id set'][0] - tags_alone:9}")
