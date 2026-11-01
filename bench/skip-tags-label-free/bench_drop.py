# Splits the cost of the `__name__` drop step of sum(rate(x[5m])) (see bench.py for the shared pieces).
import json, statistics, sys, time
from bench import q, run, samples, lookup, RATE
def base(metric, s3):
    st="timeSeriesStoreTags(id, tags, '__name__', metric_name)"
    return (f"WITH s1 AS (SELECT timeSeriesIdToGroup(id) AS group, samples FROM ({samples(lookup(st, metric))}) WHERE notEmpty(samples)), "
            f"s2 AS (SELECT group, {RATE}(samples) AS values FROM s1 GROUP BY group), s3 AS ({s3}), "
            f"s5 AS (SELECT sumForEach(values) AS values FROM s3) SELECT values FROM s5")
CASES={
 "drop_groupby_check": "SELECT timeSeriesRemoveTag(group, '__name__') AS new_group, anyForEach(values) AS values FROM s2 GROUP BY new_group HAVING timeSeriesThrowDuplicateSeriesIf(arrayExists(c -> c > 1, countForEach(s2.values)), new_group) = 0",
 "drop_projection": "SELECT timeSeriesRemoveTag(group, '__name__') AS group, values FROM s2",
 "marker_projection": "SELECT if(isNotNull(timeSeriesExtractTag(group, '__name__')), timeSeriesReplaceTag(group, 'zz_marker', '1', '', ''), group) AS group, values FROM s2",
 "no_drop": "SELECT group, values FROM s2",
}
metric=sys.argv[1]; rid=str(int(time.time()))
print(json.dumps({k: q(base(metric,v)).strip() for k,v in CASES.items()}), flush=True)
for rnd in range(3):
    for k,v in CASES.items():
        print(json.dumps(dict(round=rnd, case=k, metric=metric, **run(base(metric,v), f"d{rid}_{k}_{rnd}"))), flush=True)
