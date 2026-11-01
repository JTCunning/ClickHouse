# Runs each selector once, extracts the generated tags subquery from text_log, saves it to sub_<k>.sql
import subprocess, sys, json
CH="/root/nerve-workspace/tools/clickhouse/master/clickhouse"
T_END=int(open("t_end").read())
def q(sql):
    r=subprocess.run([CH,"client","--port","19000","--max_query_size","1000000000","--queries-file","/dev/stdin"],input=sql,capture_output=True,text=True)
    if r.returncode: raise RuntimeError(r.stderr[:2000])
    return r.stdout
def extract(sql):
    i=sql.index("in(id, (SELECT")+len("in(id, ")
    d=0
    for j in range(i,len(sql)):
        if sql[j]=='(': d+=1
        elif sql[j]==')':
            d-=1
            if d==0: return sql[i+1:j]
SHAPES=['metric_1{job="job3"}','metric_0','{job="job3"}','{__name__=~"metric_1.*"}']
for k,sel in enumerate(SHAPES):
    esc=sel.replace("\\","\\\\").replace("'","\\'")
    q(f"SELECT count(), sum(length(samples)) FROM timeSeriesSelector(ts, '{esc}', toDateTime64({T_END-300}, 3), toDateTime64({T_END}, 3), 'samples') SETTINGS log_comment='gen{k}', max_threads=2 FORMAT Null")
    q("SYSTEM FLUSH LOGS")
    gen=q(f"SELECT message FROM system.text_log WHERE logger_name='StorageTimeSeriesSelector' AND message LIKE 'Will execute query:%' AND query_id IN (SELECT query_id FROM system.query_log WHERE log_comment='gen{k}') ORDER BY event_time_microseconds DESC LIMIT 1 FORMAT TSVRaw").split("\n",1)[1].strip()
    open(f"full_{k}.sql","w").write(gen)
    open(f"sub_{k}.sql","w").write(extract(gen))
    print(k, sel, len(gen))
