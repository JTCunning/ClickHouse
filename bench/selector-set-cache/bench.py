# Times PromQL shapes on a local server (port 19000). Median of N warm runs from system.query_log.
# Usage: python3 bench.py <label> [runs]   -> appends JSON lines to results.jsonl
import subprocess, sys, json, statistics, uuid
CH = "/root/nerve-workspace/tools/clickhouse/master/clickhouse"
DATA = "/root/nerve-workspace/tools/clickhouse/data/selector-set-cache"
T_END = int(open(f"{DATA}/t_end").read())
LABEL = sys.argv[1]
RUNS = int(sys.argv[2]) if len(sys.argv) > 2 else 10
def q(sql, extra=()):
    r = subprocess.run([CH, "client", "--port", "19000", *extra, "-q", sql], stdin=subprocess.DEVNULL, capture_output=True, text=True)
    if r.returncode: raise RuntimeError(r.stderr[:2000])
    return r.stdout, r.stderr
N = 'metric_1{pod="pod-1"}'
W = 'metric_1{job="job3"}'
SHAPES = [
    ("narrow x", N),
    ("narrow x + x", f"{N} + {N}"),
    ("narrow x + x + x", f"{N} + {N} + {N}"),
    ("wide x", W),
    ("wide x + x", f"{W} + {W}"),
    ("narrow rate", f"rate({N}[5m])"),
    ("narrow rate / rate offset", f"rate({N}[5m]) / rate({N}[5m] offset 10m)"),
    ("narrow x / on() group_left x", f"{N} / ignoring(pod) group_left {N}"),
]
for name, expr in SHAPES:
    esc = expr.replace("\\", "\\\\").replace("'", "\\'")
    tag = f"{LABEL}:{name}:{uuid.uuid4().hex[:8]}"
    sql = f"SELECT count() FROM prometheusQuery(ts, '{esc}', toDateTime64({T_END}, 3)) FORMAT Null SETTINGS max_threads = 2, log_comment = '{tag}'"
    _, err = q(sql, ("--send_logs_level=trace",))           # cold run, also counts set builds
    builds = err.count("Creating set, key")
    hits = err.count("Waiting for set to be built")
    for _ in range(RUNS):
        q(sql)
    q("SYSTEM FLUSH LOGS")
    out, _ = q(f"""SELECT query_duration_ms, read_rows, memory_usage,
        (ProfileEvents['QueryPlanBuildMicroseconds'] + ProfileEvents['QueryPlanOptimizeMicroseconds']) / 1000,
        ProfileEvents['UserTimeMicroseconds'] / 1000
        FROM system.query_log WHERE log_comment = '{tag}' AND type = 'QueryFinish' ORDER BY event_time_microseconds FORMAT TSV""")
    rows = [list(map(float, l.split("\t"))) for l in out.strip().split("\n")][1:]  # drop cold
    d = [r[0] for r in rows]
    rec = dict(label=LABEL, shape=name, expr=expr, runs=len(rows), median_ms=statistics.median(d), min_ms=min(d), max_ms=max(d),
               read_rows=int(statistics.median(r[1] for r in rows)), mem_mib=round(statistics.median(r[2] for r in rows) / 2**20, 1),
               plan_opt_ms=round(statistics.median(r[3] for r in rows), 1), user_cpu_ms=round(statistics.median(r[4] for r in rows), 1),
               set_builds=builds, set_cache_hits=hits)
    print(json.dumps(rec)); open("results.jsonl", "a").write(json.dumps(rec) + "\n")
