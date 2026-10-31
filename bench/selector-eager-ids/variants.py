import json, subprocess, sys, statistics
exec(open("bench.py").read().split("shapes=sys.argv[1:]")[0])
import time
def variants(sel, k):
    tag=f"v{int(time.time())}_{k}"
    esc=sel.replace("'","\\'")
    selsql=f"SELECT count(), sum(length(samples)) FROM timeSeriesSelector(ts, '{esc}', toDateTime64({T_END-300}, 3), toDateTime64({T_END}, 3), 'samples')"
    before=run(selsql, tag+"_before")
    gen=q(f"SELECT message FROM system.text_log WHERE logger_name='StorageTimeSeriesSelector' AND message LIKE 'Will execute query:%' AND query_id IN (SELECT query_id FROM system.query_log WHERE log_comment='{tag}_before#1' LIMIT 1) LIMIT 1").split("\n",1)[1].strip()
    a,b=extract_subquery(gen); sub=gen[a+1:b-1]
    pre=gen[:a-len('in(id, ')]; post=gen[b+1:]
    ids=[x for x in q(f"SELECT * FROM ({sub})","TSVRaw","SETTINGS join_algorithm='full_sorting_merge', optimize_read_in_order=1").split() if x]
    res={"selector":sel,"series":len(ids),"before":before}
    lits=",".join(ids)
    lc=f"in(id, _CAST([{lits}], 'Array(Tuple(UInt64, LowCardinality(UUID)))'))"
    res["literal_lc"]=run(f"SELECT count(), sum(length(samples)) FROM ({pre}{lc}{post})", tag+"_lc", extra=", max_ast_elements=100000000, max_expanded_ast_elements=100000000")
    # temp table in one session: create+fill (eager), then samples query reading `id IN tmp`
    walls=[];plans=[];fills=[]
    for i in range(6):
        script=f"""CREATE TEMPORARY TABLE tmp_ids (id Tuple(UInt64, LowCardinality(UUID))) ENGINE=Memory;
INSERT INTO tmp_ids SELECT * FROM ({sub}) SETTINGS join_algorithm='full_sorting_merge', optimize_read_in_order=1, log_comment='{tag}_fill#{i}';
SELECT count(), sum(length(samples)) FROM ({pre}in(id, tmp_ids){post}) {SET}, log_comment='{tag}_tmp#{i}' FORMAT Null;"""
        r=subprocess.run([CH,"client","--port","19000","--multiquery","--queries-file","/dev/stdin"],input=script,capture_output=True,text=True)
        if r.returncode: raise RuntimeError(r.stderr[:1500])
    q("SYSTEM FLUSH LOGS",None)
    for kind in ("fill","tmp"):
        d=json.loads(q(f"SELECT query_duration_ms,(ProfileEvents['QueryPlanBuildMicroseconds']+ProfileEvents['QueryPlanOptimizeMicroseconds'])/1000 FROM system.query_log WHERE type='QueryFinish' AND log_comment LIKE '{tag}_{kind}#%' AND log_comment!='{tag}_{kind}#0'","JSONCompact"))["data"]
        res[kind]=dict(wall=statistics.median([float(x[0]) for x in d]), plan=statistics.median([float(x[1]) for x in d]))
    print(json.dumps(res)); sys.stdout.flush()
for k,s in enumerate(sys.argv[1:]): variants(s,k)
