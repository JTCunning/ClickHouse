WITH
 s1 AS (SELECT timeSeriesIdToGroup(id) AS group, arrayFilter(lambda(tuple(sample), NOT (equals(reinterpretAsUInt64(tupleElement(sample, 2)), 9218868437227405314))), samples) AS samples
        FROM timeSeriesSelector('lab', 'TBL', 'container_cpu_usage_seconds_total{job="cadvisor",image!=""}', toDateTime64(toDecimal128('1704067500.001', 3), 3), toDateTime64(toDecimal128('1704240000', 3), 3), 'samples')),
 s2 AS (SELECT group, timeSeriesRateToGrid(toDateTime64(toDecimal128('1704067800', 3), 3), toDateTime64(toDecimal128('1704240000', 3), 3), toDecimal64('900', 3), toDecimal64('300', 3))(samples) AS `values` FROM s1 GROUP BY group),
 s5 AS (SELECT timeSeriesRemoveAllTagsExcept(timeSeriesRemoveTag(group, '__name__'), ['namespace']) AS new_group, sumForEach(`values`) AS `values` FROM s2 GROUP BY new_group)
SELECT timeSeriesGroupToTags(new_group) AS tags, timeSeriesFromGrid(toDateTime64(toDecimal128('1704067800', 3), 3), toDateTime64(toDecimal128('1704240000', 3), 3), toDecimal64('900', 3), CAST(`values`, 'Array(Nullable(Float64))')) AS samples
FROM s5 WHERE notEmpty(`values`) ORDER BY tags FORMAT Null
SETTINGS max_threads = 2, use_uncompressed_cache = 0
