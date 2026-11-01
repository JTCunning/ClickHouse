# Sweeps settings over the tags subquery of each shape (sub_<k>.sql); warm median of N runs; JSON lines to stdout.
import subprocess, sys, json, statistics, time, itertools
CH="/root/nerve-workspace/tools/clickhouse/master/clickhouse"
def q(sql):
    r=subprocess.run([CH,"client","--port","19000","--max_query_size","1000000000","--queries-file","/dev/stdin"],input=sql,capture_output=True,text=True)
    if r.returncode: raise RuntimeError(r.stderr[:1500])
    return r.stdout
AS_EXECUTED="merge_tree_min_bytes_for_concurrent_read=4194304, merge_tree_min_rows_for_concurrent_read=0"  # what readImpl adds on v11 tables
VARIANTS={
 "fsm_rio1":      "join_algorithm='full_sorting_merge', optimize_read_in_order=1",
 "fsm_rio0":      "join_algorithm='full_sorting_merge', optimize_read_in_order=0",
 "hash_rio1":     "join_algorithm='hash', optimize_read_in_order=1",
 "hash_rio0":     "join_algorithm='hash', optimize_read_in_order=0",
 "phash_rio0":    "join_algorithm='parallel_hash', optimize_read_in_order=0",
}
N=int(sys.argv[1]) if len(sys.argv)>1 else 7
shapes=[int(x) for x in sys.argv[2].split(",")] if len(sys.argv)>2 else [0,1,2,3]
threads=[int(x) for x in sys.argv[3].split(",")] if len(sys.argv)>3 else [1,2]
run_id=str(int(time.time()))
for k in shapes:
    sub=open(f"sub_{k}.sql").read()
    sql=f"SELECT sum(sipHash64(x)), count() FROM ({sub})"
    ref=None
    for mt in threads:
        tags={}
        for v in VARIANTS: tags[v]=f"r{run_id}_{k}_{mt}_{v}"
        for i in range(N):          # interleave variants: A B C D A B C D ...
            for v,s in VARIANTS.items():
                out=q(f"{sql} SETTINGS {s}, {AS_EXECUTED}, max_threads={mt}, log_queries=1, log_profile_events=1, use_query_cache=0, log_comment='{tags[v]}#{i}' FORMAT TSV").strip()
                if ref is None: ref=out
                assert out==ref, (v,out,ref)
        q("SYSTEM FLUSH LOGS")
        for v in VARIANTS:
            d=json.loads(q(f"""SELECT query_duration_ms, (ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds'])/1000, read_rows, memory_usage, peak_threads_usage
              FROM system.query_log WHERE type='QueryFinish' AND log_comment LIKE '{tags[v]}#%' AND log_comment!='{tags[v]}#0' FORMAT JSONCompact"""))["data"]
            wall=[float(r[0]) for r in d]
            print(json.dumps(dict(shape=k, max_threads=mt, variant=v, rows=int(ref.split()[1]), n=len(d), med=statistics.median(wall), mn=min(wall), mx=max(wall),
                cpu=statistics.median(float(r[1]) for r in d), read_rows=int(d[0][2]), mem_mib=round(int(d[0][3])/2**20,1), peak_threads=int(d[0][4]))), flush=True)
