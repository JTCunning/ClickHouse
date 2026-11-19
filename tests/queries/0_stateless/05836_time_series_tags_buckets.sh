#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
# Tag no-replicated-database: `DatabaseReplicated` does not drop `TimeSeries` inner tables synchronously; deferred DROPs are rejected.

# A table of version 14 stores a row of a time series in the tags table for each time bucket with samples of the series,
# and has no time ranges table. Queries return the same results as for a table of version 11.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --session_timezone UTC"

function inner_table()
{
    $CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase()
        AND name = concat('.inner_id.$2.', (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = '$1'))"
}

$CLIENT -q "CREATE TABLE ts_v11 ENGINE = TimeSeries SETTINGS version = 11, recent_samples_ttl_seconds = 0"
$CLIENT -q "CREATE TABLE ts_1d ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0"
$CLIENT -q "CREATE TABLE ts_4d ENGINE = TimeSeries SETTINGS tags_bucket_step_seconds = 345600, recent_samples_ttl_seconds = 0"
$CLIENT -q "CREATE TABLE ts_no_cache ENGINE = TimeSeries SETTINGS tags_deduplication_cache_size_bytes = 0, recent_samples_ttl_seconds = 0"

echo '-- definition of the tags table'
tags_table=$(inner_table ts_1d tags)
$CLIENT -q "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = '$tags_table' ORDER BY position"
$CLIENT -q "SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = '$tags_table'"
$CLIENT -q "SELECT name, extract(create_table_query, 'version = [0-9]+'), extract(create_table_query, 'tags_bucket_step_seconds = [0-9]+')
    FROM system.tables WHERE database = currentDatabase() AND name IN ('ts_1d', 'ts_4d') ORDER BY name"

echo '-- time ranges tables: version 11, version 14'
inner_table ts_v11 timeranges | wc -l
inner_table ts_1d timeranges | wc -l

echo '-- the time ranges table and the step of the buckets for an earlier version are rejected'
$CLIENT -q "CREATE TABLE ts_rejected ENGINE = TimeSeries SETTINGS store_time_ranges = 1" 2>&1 | grep -o -m1 'INVALID_SETTING_VALUE'
$CLIENT -q "CREATE TABLE ts_rejected ENGINE = TimeSeries TIME RANGES INNER ENGINE = AggregatingMergeTree" 2>&1 | grep -o -m1 'INCORRECT_QUERY'
$CLIENT -q "CREATE TABLE ts_rejected ENGINE = TimeSeries SETTINGS version = 11, tags_bucket_step_seconds = 86400" 2>&1 | grep -o -m1 'INVALID_SETTING_VALUE'
$CLIENT -q "CREATE TABLE ts_rejected ENGINE = TimeSeries SETTINGS tags_bucket_step_seconds = 0" 2>&1 | grep -o -m1 'INVALID_SETTING_VALUE'

# The buckets of one day start at 0, 86400, and 345600. The buckets of four days start at 0 and 345600.
for table in ts_v11 ts_1d ts_4d ts_no_cache; do
    $CLIENT -q "INSERT INTO $table (metric_name, tags, samples) VALUES
        ('cpu', map('job', 'a', 'instance', '1'), [(toDateTime64(1000, 3), 1.), (toDateTime64(87400, 3), 2.)]),
        ('cpu', map('job', 'a', 'instance', '2'), [(toDateTime64(1000, 3), 10.)]),
        ('cpu', map('job', 'b', 'instance', '3'), [(toDateTime64(87400, 3), 100.)]),
        ('mem', map('job', 'a'), [(toDateTime64(1000, 3), 5.), (toDateTime64(87400, 3), 6.)])"
    $CLIENT -q "INSERT INTO $table (metric_name, tags, samples) VALUES
        ('cpu', map('job', 'a', 'instance', '1'), [(toDateTime64(87460, 3), 3.)]),
        ('cpu', map('job', 'b', 'instance', '3'), [(toDateTime64(87460, 3), 200.), (toDateTime64(400000, 3), 300.)])"
done

echo '-- rows of the tags table: one row per time series and bucket of one day'
$CLIENT -q "SELECT metric_name, tags['instance'], toUnixTimestamp(bucket), count() FROM timeSeriesTags(ts_1d) GROUP BY ALL ORDER BY ALL"
echo '-- rows of the tags table: one row per time series and bucket of four days'
$CLIENT -q "SELECT metric_name, tags['instance'], toUnixTimestamp(bucket), count() FROM timeSeriesTags(ts_4d) GROUP BY ALL ORDER BY ALL"
echo '-- partitions of the tags table: buckets of one day, buckets of four days'
$CLIENT -q "SELECT uniqExact(partition) FROM system.parts WHERE database = currentDatabase() AND table = '$(inner_table ts_1d tags)' AND active"
$CLIENT -q "SELECT uniqExact(partition) FROM system.parts WHERE database = currentDatabase() AND table = '$(inner_table ts_4d tags)' AND active"

function compare()
{
    local query="$1"
    local result_v11 result
    result_v11=$($CLIENT -q "${query//TABLE/ts_v11}")
    echo "$result_v11"
    for table in ts_1d ts_4d ts_no_cache; do
        result=$($CLIENT -q "${query//TABLE/$table}")
        if [ "$result_v11" == "$result" ]; then echo "$table: equal to version 11"; else echo "$table: DIFFERENT from version 11: $result"; fi
    done
}

function compare_all()
{
    echo '-- instant query in the first bucket'
    compare "SELECT tags, value FROM prometheusQuery(TABLE, 'cpu', 1010) ORDER BY ALL"
    echo '-- instant query in the second bucket'
    compare "SELECT tags, value FROM prometheusQuery(TABLE, 'cpu', 87470) ORDER BY ALL"
    echo '-- instant query in the last bucket'
    compare "SELECT tags, value FROM prometheusQuery(TABLE, '{__name__=~\"cpu|mem\"}', 400010) ORDER BY ALL"
    echo '-- aggregation in the second bucket'
    compare "SELECT tags, value FROM prometheusQuery(TABLE, 'sum by (job) (cpu)', 87470) ORDER BY ALL"
    echo '-- matchers'
    compare "SELECT tags, value FROM prometheusQuery(TABLE, 'cpu{instance=~\"1|3\", job!=\"b\"}', 87470) ORDER BY ALL"
    echo '-- window over two buckets'
    compare "SELECT tags, value FROM prometheusQuery(TABLE, 'max_over_time(cpu[2d])', 87470) ORDER BY ALL"
    compare "SELECT tags, value FROM prometheusQuery(TABLE, 'count_over_time({job=\"a\"}[2d])', 87470) ORDER BY ALL"
    echo '-- window over all buckets'
    compare "SELECT tags, value FROM prometheusQuery(TABLE, 'count_over_time({job=~\"a|b\"}[5d])', 400010) ORDER BY ALL"
    echo '-- range query over two buckets'
    compare "SELECT tags, samples FROM prometheusQueryRange(TABLE, 'count by (job) (cpu)', 1000, 87400, 43200) ORDER BY ALL"
    echo '-- window without samples'
    compare "SELECT count() FROM prometheusQuery(TABLE, 'cpu', 500000)"
    echo '-- selector'
    compare "SELECT count(), sum(length(samples)) FROM timeSeriesSelector(TABLE, 'cpu', 80000, 90000, 'samples')"
    echo '-- read of the table'
    compare "SELECT metric_name, tags, samples FROM TABLE ORDER BY ALL"
}

compare_all

echo '-- rows of a time series in one bucket: without the deduplication cache, with it'
$CLIENT -q "SELECT metric_name, tags['instance'], toUnixTimestamp(bucket), count() FROM timeSeriesTags(ts_no_cache) WHERE bucket = 86400 GROUP BY ALL ORDER BY ALL"
$CLIENT -q "SELECT metric_name, tags['instance'], toUnixTimestamp(bucket), count() FROM timeSeriesTags(ts_1d) WHERE bucket = 86400 GROUP BY ALL ORDER BY ALL"

echo '-- after a merge: one row per time series and bucket'
$CLIENT -q "OPTIMIZE TABLE \`$(inner_table ts_no_cache tags)\` FINAL"
$CLIENT -q "SELECT metric_name, tags['instance'], toUnixTimestamp(bucket), count() FROM timeSeriesTags(ts_no_cache) GROUP BY ALL ORDER BY ALL"

echo '-- the same queries after the merge'
compare_all | grep -c 'equal to version 11'
compare_all | grep -c 'DIFFERENT'

$CLIENT -q "DROP TABLE ts_v11"
$CLIENT -q "DROP TABLE ts_1d"
$CLIENT -q "DROP TABLE ts_4d"
$CLIENT -q "DROP TABLE ts_no_cache"
