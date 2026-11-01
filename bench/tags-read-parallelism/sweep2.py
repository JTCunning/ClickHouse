# Bounds the join's share: current tags subquery vs the same without the time ranges join vs id-only (no tags collector).
import subprocess, sys, json, statistics, time
CH="/root/nerve-workspace/tools/clickhouse/master/clickhouse"
def q(sql):
    r=subprocess.run([CH,"client","--port","19000","--max_query_size","1000000000","--queries-file","/dev/stdin"],input=sql,capture_output=True,text=True)
    if r.returncode: raise RuntimeError(r.stderr[:1500])
    return r.stdout
AS_EXECUTED="join_algorithm='full_sorting_merge', optimize_read_in_order=1, merge_tree_min_bytes_for_concurrent_read=4194304, merge_tree_min_rows_for_concurrent_read=0"
V={"current":"sub","no_join":"nojoin","ids_only":"ids"}
N=int(sys.argv[1]); run_id=str(int(time.time()))
for k in [int(x) for x in sys.argv[2].split(",")]:
    for mt in [int(x) for x in sys.argv[3].split(",")]:
        tag=lambda v:f"q{run_id}_{k}_{mt}_{v}"
        for i in range(N):
            for v,f in V.items():
                q(f"SELECT sum(sipHash64(x)) FROM ({open(f'{f}_{k}.sql').read()}) SETTINGS {AS_EXECUTED}, max_threads={mt}, log_queries=1, use_query_cache=0, log_comment='{tag(v)}#{i}' FORMAT Null")
        q("SYSTEM FLUSH LOGS")
        for v in V:
            d=json.loads(q(f"SELECT query_duration_ms, (ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds'])/1000 FROM system.query_log WHERE type='QueryFinish' AND log_comment LIKE '{tag(v)}#%' AND log_comment!='{tag(v)}#0' FORMAT JSONCompact"))["data"]
            w=[float(r[0]) for r in d]
            print(json.dumps(dict(shape=k,max_threads=mt,variant=v,n=len(w),med=statistics.median(w),mn=min(w),mx=max(w),cpu=statistics.median(float(r[1]) for r in d))),flush=True)
