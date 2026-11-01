# A/B emulation for handoff #8 (skip the tags read for label-free aggregations), no build needed.
# Runs on a local server (port 19000) holding the TimeSeries table `ts` (see README.md for the dataset).
# master   : prometheusQuery(ts, <expr>) as shipped.
# hand_tags: the SQL master generates, written out by hand (timeSeriesStoreTags + timeSeriesIdToGroup + __name__ drop).
# hand_ids : the same SQL with the tags lookup returning `id` only and the per-series step grouped by `id`
#            (what a label-free selector mode would generate).
import subprocess, sys, json, statistics, os
CH="/root/nerve-workspace/tools/clickhouse/master/clickhouse"
T=int(open(os.environ.get("T_END_FILE","t_end")).read())
def q(sql, fmt="TSVRaw"):
    r=subprocess.run([CH,"client","--port","19000","--max_query_size","100000000","--queries-file","/dev/stdin"],input=sql+(" FORMAT "+fmt if fmt else ""),capture_output=True,text=True,stdin=None)
    if r.returncode: raise RuntimeError(r.stderr[:1500])
    return r.stdout
tables={n.split('.')[2]:n for n in q("SELECT name FROM system.tables WHERE database='default' AND name LIKE '.inner_id.%'").split()}
TAGS=f"default.`{tables['tags']}`"; TR=f"default.`{tables['timeranges']}`"; RS=f"default.`{tables['recentsamples']}`"
LO=f"toDateTime64(toDecimal128('{T-300}.001', 3), 3)"; HI=f"toDateTime64(toDecimal128('{T}', 3), 3)"
def lookup(select_expr, metric):
    return (f"SELECT {select_expr} FROM {TAGS} ALL LEFT JOIN (SELECT id, 1 AS matched FROM {TR} WHERE max_time >= {LO} AND min_time <= {HI} "
            f"AND id >= tuple(sipHash64('{metric}'), toUUID('00000000-0000-0000-0000-000000000000')) AND id <= tuple(sipHash64('{metric}'), toUUID('ffffffff-ffff-ffff-ffff-ffffffffffff'))) "
            f"AS time_ranges_subquery USING (id) WHERE metric_name = '{metric}' AND (time_ranges_subquery.matched = 1 OR tupleElement(id, 1) != sipHash64('{metric}'))")
def samples(ids_sql):
    return (f"SELECT id, timeSeriesSliceSortedArray(samples, {LO}, {HI}) AS samples FROM {RS} WHERE bucket >= toDateTime64({T-900}, 3) AND bucket <= {HI} "
            f"AND max_time >= {LO} AND min_time <= {HI} AND id IN ({ids_sql})")
RATE=f"timeSeriesRateToGrid({HI}, {HI}, toDecimal64('0', 3), toDecimal64('300', 3))"
def hand_tags(metric):
    return (f"WITH s1 AS (SELECT timeSeriesIdToGroup(id) AS group, samples FROM ({samples(lookup('timeSeriesStoreTags(id, tags, '+chr(39)+'__name__'+chr(39)+', metric_name)', metric))}) WHERE notEmpty(samples)), "
            f"s2 AS (SELECT group, {RATE}(samples) AS values FROM s1 GROUP BY group), "
            f"s3 AS (SELECT timeSeriesRemoveTag(group, '__name__') AS new_group, anyForEach(values) AS values FROM s2 GROUP BY new_group "
            f"HAVING timeSeriesThrowDuplicateSeriesIf(arrayExists(c -> c > 1, countForEach(s2.values)), new_group) = 0), "
            f"s5 AS (SELECT sumForEach(values) AS values FROM s3) SELECT values FROM s5")
def hand_ids(metric):
    return (f"WITH s1 AS (SELECT id, samples FROM ({samples(lookup('id', metric))}) WHERE notEmpty(samples)), "
            f"s2 AS (SELECT {RATE}(samples) AS values FROM s1 GROUP BY id), "
            f"s5 AS (SELECT sumForEach(values) AS values FROM s2) SELECT values FROM s5")
def tags_noname(metric):
    # tags registered and grouped by tag set, but no __name__ drop step (isolates the cost of that step)
    return (f"WITH s1 AS (SELECT timeSeriesIdToGroup(id) AS group, samples FROM ({samples(lookup('timeSeriesStoreTags(id, tags, '+chr(39)+'__name__'+chr(39)+', metric_name)', metric))}) WHERE notEmpty(samples)), "
            f"s2 AS (SELECT group, {RATE}(samples) AS values FROM s1 GROUP BY group), "
            f"s5 AS (SELECT sumForEach(values) AS values FROM s2) SELECT values FROM s5")
def tags_byid(metric):
    # tags read and registered, but grouped by id and no __name__ drop (isolates the read + registration)
    return (f"WITH s1 AS (SELECT id, samples FROM ({samples(lookup('timeSeriesStoreTags(id, tags, '+chr(39)+'__name__'+chr(39)+', metric_name)', metric))}) WHERE notEmpty(samples)), "
            f"s2 AS (SELECT {RATE}(samples) AS values FROM s1 GROUP BY id), "
            f"s5 AS (SELECT sumForEach(values) AS values FROM s2) SELECT values FROM s5")
def master(expr):
    return f"SELECT tags, value FROM prometheusQuery(ts, '{expr}', {HI})"
SET="SETTINGS max_threads=2, use_query_cache=0, log_queries=1, log_profile_events=1, join_algorithm='full_sorting_merge', optimize_read_in_order=1"
def run(sql, tag, n=7):
    for i in range(n):
        q(f"{sql} {SET}, log_comment='{tag}#{i}'", "Null")
    q("SYSTEM FLUSH LOGS", None)
    d=json.loads(q(f"""SELECT query_duration_ms, ProfileEvents['UserTimeMicroseconds']/1000, read_rows, read_bytes, memory_usage
      FROM system.query_log WHERE type='QueryFinish' AND log_comment LIKE '{tag}#%' AND log_comment!='{tag}#0' ORDER BY event_time_microseconds""","JSONCompact"))["data"]
    w=[float(r[0]) for r in d]
    return dict(n=len(w), med=statistics.median(w), mn=min(w), mx=max(w), cpu_med=statistics.median(float(r[1]) for r in d),
                read_rows=int(statistics.median(int(r[2]) for r in d)), read_mib=round(statistics.median(int(r[3]) for r in d)/2**20,1),
                mem_mib=round(statistics.median(int(r[4]) for r in d)/2**20,1))
if __name__=="__main__":
    import time; rid=str(int(time.time()))
    metric=sys.argv[1] if len(sys.argv)>1 else "metric_0"
    expr=f"sum(rate({metric}[5m]))"
    # result parity first
    a=q(master(expr)).strip(); b=q(hand_tags(metric)).strip(); c=q(hand_ids(metric)).strip(); d=q(tags_noname(metric)).strip(); e=q(tags_byid(metric)).strip()
    print(json.dumps(dict(metric=metric, master=a, hand_tags=b, hand_ids=c, tags_noname=d, tags_byid=e)), flush=True)
    for rnd in range(int(os.environ.get("ROUNDS","3"))):   # interleaved A B C rounds
        for name,sql in (("master",master(expr)),("hand_tags",hand_tags(metric)),("hand_ids",hand_ids(metric)),("tags_noname",tags_noname(metric)),("tags_byid",tags_byid(metric)),("master_by",master(f"sum by (job, cluster) (rate({metric}[5m]))"))):
            print(json.dumps(dict(round=rnd, case=name, metric=metric, **run(sql, f"r{rid}_{metric}_{name}_{rnd}"))), flush=True)
