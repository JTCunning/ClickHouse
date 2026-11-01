# Interleaved A/B of selector shapes: full selector and PromQL instant query. Usage: ab.py rounds sel1 sel2 ...
import subprocess, sys, json, statistics, time
CH="/root/nerve-workspace/tools/clickhouse/master/clickhouse"
T_END=int(open("t_end").read())
def q(sql):
    r=subprocess.run([CH,"client","--port","19000","--max_query_size","100000000","--queries-file","/dev/stdin"],input=sql,capture_output=True,text=True)
    if r.returncode: raise RuntimeError(r.stderr[:1500])
    return r.stdout
rounds=int(sys.argv[1]); sels=sys.argv[2:]; rid=str(int(time.time()))
def sqls(sel):
    e=sel.replace("\\","\\\\").replace("'","\\'")
    return {"selector": f"SELECT count(), sum(length(samples)) FROM timeSeriesSelector(ts, '{e}', toDateTime64({T_END-300}, 3), toDateTime64({T_END}, 3), 'samples')",
            "promql_sum_rate": f"SELECT count() FROM prometheusQuery(ts, 'sum(rate({e}[5m]))', toDateTime64({T_END}, 3))",
            "promql_instant": f"SELECT count() FROM prometheusQuery(ts, '{e}', toDateTime64({T_END}, 3))"}
for r in range(rounds+1):
    for k,s in enumerate(sels):
        for kind,sql in sqls(s).items():
            q(sql+f" FORMAT Null SETTINGS max_threads=2, use_query_cache=0, log_queries=1, log_comment='ab{rid}_{k}_{kind}#{r}'")
q("SYSTEM FLUSH LOGS")
for k,s in enumerate(sels):
    for kind in sqls(s):
        d=[l.split("\t") for l in q(f"SELECT query_duration_ms, memory_usage, read_rows FROM system.query_log WHERE type='QueryFinish' AND log_comment LIKE 'ab{rid}_{k}_{kind}#%' AND log_comment!='ab{rid}_{k}_{kind}#0' FORMAT TSV").split("\n") if l]
        w=[float(x[0]) for x in d]
        print(json.dumps(dict(sel=s[:70], kind=kind, n=len(w), med=statistics.median(w), mn=min(w), mx=max(w), mem_mib=round(statistics.median(float(x[1]) for x in d)/2**20,1), read_rows=int(d[0][2]))), flush=True)
