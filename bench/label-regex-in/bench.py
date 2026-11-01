# Times a timeSeriesSelector shape in full and its tags lookup subquery alone, on a local server (port 19000).
import subprocess, sys, json, statistics, time
CH="/root/nerve-workspace/tools/clickhouse/master/clickhouse"
T_END=int(open("t_end").read())
def q(sql, fmt="TSVRaw", settings=""):
    full=sql+(" "+settings if settings else "")+(" FORMAT "+fmt if fmt else "")
    r=subprocess.run([CH,"client","--port","19000","--max_query_size","1000000000","--queries-file","/dev/stdin"],input=full,capture_output=True,text=True)
    if r.returncode: raise RuntimeError(r.stderr[:2000])
    return r.stdout
SET="SETTINGS max_threads=2, log_queries=1, use_query_cache=0, log_profile_events=1"
def run(sql, tag, n=7, extra=""):
    for i in range(n):
        q(sql, "Null", f"{SET}, log_comment='{tag}#{i}'{extra}")
    q("SYSTEM FLUSH LOGS",None)
    d=json.loads(q(f"""SELECT query_duration_ms, ProfileEvents['UserTimeMicroseconds']/1000, read_rows, read_bytes, memory_usage,
        ProfileEvents['TextIndexPostingsCacheHits']+ProfileEvents['TextIndexPostingsCacheMisses'], ProfileEvents['TextIndexReadPostings']
      FROM system.query_log WHERE type='QueryFinish' AND log_comment LIKE '{tag}#%' AND log_comment!='{tag}#0' ORDER BY event_time_microseconds""","JSONCompact"))["data"]
    wall=[float(r[0]) for r in d]
    return dict(med=statistics.median(wall), mn=min(wall), mx=max(wall), cpu=statistics.median(float(r[1]) for r in d),
                read_rows=int(d[0][2]), read_bytes=int(d[0][3]), mem=int(d[0][4]), postings_lookups=int(d[0][5]), read_postings=int(d[0][6]))
def extract_subquery(sql):
    i=sql.index("in(id, (SELECT")+len("in(id, ")
    depth=0
    for j in range(i,len(sql)):
        if sql[j]=='(': depth+=1
        elif sql[j]==')':
            depth-=1
            if depth==0: return i, j+1
run_id=str(int(time.time()))
for k,sel in enumerate(sys.argv[1:]):
    tag=f"r{run_id}_{k}"
    esc=sel.replace("\\","\\\\").replace("'","\\'")
    selsql=f"SELECT count(), sum(length(samples)) FROM timeSeriesSelector(ts, '{esc}', toDateTime64({T_END-300}, 3), toDateTime64({T_END}, 3), 'samples')"
    full=run(selsql, tag+"_full")
    gen=q(f"SELECT message FROM system.text_log WHERE logger_name='StorageTimeSeriesSelector' AND message LIKE 'Will execute query:%' AND query_id IN (SELECT query_id FROM system.query_log WHERE log_comment='{tag}_full#1' LIMIT 1) LIMIT 1").split("\n",1)[1].strip()
    a,b=extract_subquery(gen); sub=gen[a+1:b-1]
    js=", join_algorithm='full_sorting_merge', optimize_read_in_order=1"
    tags_only=run(f"SELECT count() FROM ({sub})", tag+"_tags", extra=js)
    series=int(q(f"SELECT count() FROM ({sub})", "TSVRaw", "SETTINGS join_algorithm='full_sorting_merge', optimize_read_in_order=1"))
    print(json.dumps(dict(selector=sel if len(sel)<120 else sel[:110]+"…", series=series, full=full, tags_only=tags_only, where=sub[sub.find("WHERE"):sub.find("WHERE")+220])), flush=True)
