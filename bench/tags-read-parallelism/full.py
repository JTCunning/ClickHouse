# Full selector query per shape (warm median), to put the tags subquery in proportion.
import subprocess, json, statistics, time, sys
import os
CH=os.environ.get("CH","/root/nerve-workspace/tools/clickhouse/master/clickhouse"); PORT=os.environ.get("PORT","19000")
EXTRA=(", "+os.environ["EXTRA_SETTINGS"]) if os.environ.get("EXTRA_SETTINGS") else ""  # e.g. max_threads_min_free_memory_per_thread=0
T_END=int(open("t_end").read())
def q(sql):
    r=subprocess.run([CH,"client","--port",PORT,"--queries-file","/dev/stdin"],input=sql,capture_output=True,text=True)
    if r.returncode: raise RuntimeError(r.stderr[:1500])
    return r.stdout
SHAPES=['metric_1{job="job3"}','metric_0','{job="job3"}','{__name__=~"metric_1.*"}']
N=7; rid=str(int(time.time()))
for mt in [int(x) for x in (sys.argv[1] if len(sys.argv)>1 else "1,2").split(",")]:
    for k,sel in enumerate(SHAPES):
        esc=sel.replace("'","\\'")
        for i in range(N):
            q(f"SELECT count(), sum(length(samples)) FROM timeSeriesSelector(ts, '{esc}', toDateTime64({T_END-300}, 3), toDateTime64({T_END}, 3), 'samples') SETTINGS max_threads={mt}{EXTRA}, log_queries=1, use_query_cache=0, log_comment='f{rid}_{mt}_{k}#{i}' FORMAT Null")
    q("SYSTEM FLUSH LOGS")
    for k,sel in enumerate(SHAPES):
        d=json.loads(q(f"SELECT query_duration_ms, (ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds'])/1000, read_rows, memory_usage FROM system.query_log WHERE type='QueryFinish' AND log_comment LIKE 'f{rid}_{mt}_{k}#%' AND log_comment!='f{rid}_{mt}_{k}#0' FORMAT JSONCompact"))["data"]
        w=[float(r[0]) for r in d]
        print(json.dumps(dict(shape=k,selector=sel,max_threads=mt,med=statistics.median(w),mn=min(w),mx=max(w),cpu=statistics.median(float(r[1]) for r in d),read_rows=int(d[0][2]),mem_mib=round(int(d[0][3])/2**20,1))),flush=True)
