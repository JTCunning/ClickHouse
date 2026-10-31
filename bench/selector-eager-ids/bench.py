import subprocess, sys, json, statistics, time, re
CH="/root/nerve-workspace/tools/clickhouse/master/clickhouse"
T_END=int(open("t_end").read())
def q(sql, fmt="TSVRaw", settings=""):
    full=sql+(" "+settings if settings else "")+(" FORMAT "+fmt if fmt else "")
    r=subprocess.run([CH,"client","--port","19000","--max_query_size","1000000000","--queries-file","/dev/stdin"],input=full,capture_output=True,text=True)
    if r.returncode: raise RuntimeError(r.stderr[:2000])
    return r.stdout
SET="SETTINGS max_threads=2, log_queries=1, use_query_cache=0"
def run(sql, tag, n=6, extra=""):
    for i in range(n):
        q(sql, "Null", f"{SET}, log_comment='{tag}#{i}'{extra}")
    q("SYSTEM FLUSH LOGS",None)
    rows=q(f"""SELECT query_duration_ms, (ProfileEvents['QueryPlanBuildMicroseconds']+ProfileEvents['QueryPlanOptimizeMicroseconds'])/1000, read_rows, memory_usage
      FROM system.query_log WHERE type='QueryFinish' AND log_comment LIKE '{tag}#%' AND log_comment!='{tag}#0' ORDER BY event_time_microseconds""","JSONCompact")
    d=json.loads(rows)["data"]
    wall=[float(r[0]) for r in d]; plan=[float(r[1]) for r in d]
    return dict(wall=statistics.median(wall), wmin=min(wall), wmax=max(wall), plan=statistics.median(plan), read_rows=int(d[0][2]), mem=int(d[0][3]))
def extract_subquery(sql):
    i=sql.index("in(id, (SELECT")+len("in(id, ")
    depth=0
    for j in range(i,len(sql)):
        if sql[j]=='(': depth+=1
        elif sql[j]==')':
            depth-=1
            if depth==0: return i, j+1
shapes=sys.argv[1:]
run_id=str(int(time.time()))
out=[]
for k,sel in enumerate(shapes):
    tag=f"b{run_id}_{k}"
    esc=sel.replace("'","\\'")
    selsql=f"SELECT count(), sum(length(samples)) FROM timeSeriesSelector(ts, '{esc}', toDateTime64({T_END-300}, 3), toDateTime64({T_END}, 3), 'samples')"
    before=run(selsql, tag+"_before")
    gen=q(f"SELECT message FROM system.text_log WHERE logger_name='StorageTimeSeriesSelector' AND message LIKE 'Will execute query:%' AND query_id IN (SELECT query_id FROM system.query_log WHERE log_comment='{tag}_before#1' LIMIT 1) LIMIT 1").split("\n",1)[1].strip()
    a,b=extract_subquery(gen); sub=gen[a+1:b-1]
    jsettings=", join_algorithm='full_sorting_merge', optimize_read_in_order=1"
    tags_only=run(f"SELECT count() FROM ({sub})", tag+"_tags", extra=jsettings)
    ids=q(f"SELECT * FROM ({sub})", "TSVRaw", "SETTINGS join_algorithm='full_sorting_merge', optimize_read_in_order=1").split()
    ids=[x for x in ids if x]
    lits=",".join(ids) if ids else ""
    if not ids: lit_expr="0"
    elif len(ids)==1: lit_expr=f"equals(id, _CAST({ids[0]}, 'Tuple(UInt64, LowCardinality(UUID))'))"
    else: lit_expr=f"in(id, _CAST([{lits}], 'Array(Tuple(UInt64, UUID))'))"
    ids_sql=gen[:a-len('in(id, ')]+lit_expr+gen[b+1:]
    outer=f"SELECT count(), sum(length(samples)) FROM ({ids_sql})"
    after=run(outer, tag+"_literal", extra=", max_ast_elements=100000000, max_expanded_ast_elements=100000000")
    r=dict(selector=sel, series=len(ids), before=before, tags_only=tags_only, literal_samples=after, query_bytes=len(ids_sql))
    print(json.dumps(r)); sys.stdout.flush()
