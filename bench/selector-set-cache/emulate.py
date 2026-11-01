# Emulates a set-cache hit without a build. For a selector, takes the SQL the selector logs ("Will execute query"),
# then times: full = that SQL as is; lookup = its id subquery alone; hit = the SQL with the subquery replaced by the
# literal ids it returns (no subquery run) - plus plan = EXPLAIN PLAN of the subquery (its analysis still runs on a hit).
import subprocess, sys, json, statistics, uuid, re
CH = "/root/nerve-workspace/tools/clickhouse/master/clickhouse"
DATA = "/root/nerve-workspace/tools/clickhouse/data/selector-set-cache"
T_END = int(open(f"{DATA}/t_end").read())
RUNS = 10
SET = " SETTINGS max_threads = 2, join_algorithm = 'full_sorting_merge', optimize_read_in_order = 1"
def q(sql, extra=()):
    r = subprocess.run([CH, "client", "--port", "19000", "--max_query_size", "1000000000", *extra, "--queries-file", "/dev/stdin"], input=sql, capture_output=True, text=True)
    if r.returncode: raise RuntimeError(r.stderr[:3000])
    return r.stdout
def extract(sql):
    i = sql.index("in(id, (SELECT") + len("in(id, ")
    d = 0
    for j in range(i, len(sql)):
        if sql[j] == '(': d += 1
        elif sql[j] == ')':
            d -= 1
            if d == 0: return i, j + 1
def timeit(sql, name):
    tag = f"emu:{name}:{uuid.uuid4().hex[:8]}"
    s = f"{sql} FORMAT Null{SET}, log_comment = '{tag}'"
    for _ in range(RUNS + 1): q(s)
    q("SYSTEM FLUSH LOGS")
    out = q(f"SELECT query_duration_ms, ProfileEvents['UserTimeMicroseconds']/1000 FROM system.query_log WHERE log_comment='{tag}' AND type='QueryFinish' ORDER BY event_time_microseconds FORMAT TSV")
    rows = [list(map(float, l.split("\t"))) for l in out.strip().split("\n")][1:]
    d = [r[0] for r in rows]
    return dict(median_ms=statistics.median(d), min_ms=min(d), max_ms=max(d), cpu_ms=round(statistics.median(r[1] for r in rows), 1))
for sel in ['metric_1{pod="pod-1"}', 'metric_1{job="job3"}']:
    tag = "gen" + uuid.uuid4().hex[:8]
    q(f"SELECT count() FROM timeSeriesSelector(ts, '{sel}', toDateTime64({T_END - 300}, 3), toDateTime64({T_END}, 3), 'samples') FORMAT Null SETTINGS log_comment='{tag}'")
    q("SYSTEM FLUSH LOGS")
    gen = q(f"SELECT message FROM system.text_log WHERE logger_name='StorageTimeSeriesSelector' AND message LIKE 'Will execute query:%' AND query_id IN (SELECT query_id FROM system.query_log WHERE log_comment='{tag}') LIMIT 1 FORMAT TSVRaw").split("\n", 1)[1].strip()
    a, b = extract(gen)
    sub = gen[a + 1:b - 1]
    ids = q(f"SELECT groupArray(*) FROM ({sub}) FORMAT TSVRaw{SET}").strip()
    lit = gen[:a] + ids + gen[b:]
    open(f"gen_{sel.split('{')[1].split('=')[0]}.sql", "w").write(gen)
    full = timeit(f"SELECT count() FROM ({gen})", "full")
    lookup = timeit(f"SELECT count() FROM ({sub})", "lookup")
    hit = timeit(f"SELECT count() FROM ({lit})", "hit")
    plan = timeit(f"EXPLAIN PLAN {sub}", "plan")
    rec = dict(selector=sel, full=full, lookup=lookup, hit_literal_ids=hit, plan_subquery=plan, n_ids=ids.count("("))
    print(json.dumps(rec)); open("emulate.jsonl", "a").write(json.dumps(rec) + "\n")
