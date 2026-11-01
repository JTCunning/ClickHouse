# Runs the <query> lines of tests/performance/promql_repeated_selector.xml against database `perf` (already filled),
# RUNS warm runs each after one cold run; prints the median from system.query_log.
import re, subprocess, statistics, uuid, json, sys
CH = "/root/nerve-workspace/tools/clickhouse/master/clickhouse"
RUNS = 10
LABEL = sys.argv[1] if len(sys.argv) > 1 else "master"
s = open('/root/nerve-workspace/repos/ClickHouse/tests/performance/promql_repeated_selector.xml').read()
def q(sql):
    r = subprocess.run([CH, "client", "--port", "19000", "-d", "perf", "-q", sql], stdin=subprocess.DEVNULL, capture_output=True, text=True)
    if r.returncode: raise RuntimeError(r.stderr[:1500])
    return r.stdout
for i, m in enumerate(re.findall(r'<query>(.*?)</query>', s, re.S)):
    sql = ' '.join(m.replace('{{', '{').replace('}}', '}').split())
    tag = f"perfxml:{i}:{uuid.uuid4().hex[:6]}"
    sql = sql.replace(" FORMAT Null", f" SETTINGS log_comment = '{tag}' FORMAT Null")
    for _ in range(RUNS + 1): q(sql)
    q("SYSTEM FLUSH LOGS")
    out = q(f"SELECT query_duration_ms, read_rows FROM system.query_log WHERE log_comment = '{tag}' AND type = 'QueryFinish' ORDER BY event_time_microseconds FORMAT TSV")
    rows = [list(map(int, l.split("\t"))) for l in out.strip().split("\n")][1:]
    d = [r[0] for r in rows]
    expr = re.search(r"prometheusQuery\('[^']*', '(.*)', \d+\)", sql).group(1)
    rec = dict(label=LABEL, i=i, expr=expr, median_ms=statistics.median(d), min_ms=min(d), max_ms=max(d), read_rows=rows[0][1])
    print(json.dumps(rec)); open("perf_xml.jsonl", "a").write(json.dumps(rec) + "\n")
