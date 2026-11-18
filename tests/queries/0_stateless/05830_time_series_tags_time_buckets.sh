#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
# Tag no-replicated-database: `DatabaseReplicated` does not drop `TimeSeries` inner tables synchronously; deferred DROPs are rejected.

# A table of version 12 stores a row of a time series in the tags table for each time bucket with samples of the series,
# and has no time ranges table. PromQL queries return the same results as for a table of version 11.

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
$CLIENT -q "CREATE TABLE ts_v12 ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0"

echo '-- definition of the tags table'
tags_table=$(inner_table ts_v12 tags)
$CLIENT -q "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = '$tags_table' ORDER BY position"
$CLIENT -q "SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = '$tags_table'"
$CLIENT -q "SELECT extract(create_table_query, 'tags_bucket_step_seconds = [0-9]+') FROM system.tables WHERE database = currentDatabase() AND name = 'ts_v12'"

echo '-- time ranges tables: version 11, version 12'
inner_table ts_v11 timeranges | wc -l
inner_table ts_v12 timeranges | wc -l

echo '-- the time ranges table is rejected'
$CLIENT -q "CREATE TABLE ts_rejected ENGINE = TimeSeries SETTINGS store_time_ranges = 1" 2>&1 | grep -o -m1 'INVALID_SETTING_VALUE'
$CLIENT -q "CREATE TABLE ts_rejected ENGINE = TimeSeries TIME RANGES INNER ENGINE = AggregatingMergeTree" 2>&1 | grep -o -m1 'INCORRECT_QUERY'

# The buckets are 0 and 86400. The series 1 has samples in both buckets, the series 2 and 3 in one bucket each.
for table in ts_v11 ts_v12; do
    $CLIENT -q "INSERT INTO $table (metric_name, tags, samples) VALUES
        ('cpu', map('job', 'a', 'instance', '1'), [(toDateTime64(1000, 3), 1.), (toDateTime64(87400, 3), 2.)]),
        ('cpu', map('job', 'a', 'instance', '2'), [(toDateTime64(1000, 3), 10.)]),
        ('cpu', map('job', 'b', 'instance', '3'), [(toDateTime64(87400, 3), 100.)]),
        ('mem', map('job', 'a'), [(toDateTime64(1000, 3), 5.), (toDateTime64(87400, 3), 6.)])"
    $CLIENT -q "INSERT INTO $table (metric_name, tags, samples) VALUES
        ('cpu', map('job', 'a', 'instance', '1'), [(toDateTime64(87460, 3), 3.)]),
        ('cpu', map('job', 'b', 'instance', '3'), [(toDateTime64(87460, 3), 200.)])"
done

echo '-- rows of the tags table: one row per time series and bucket'
$CLIENT -q "SELECT metric_name, tags['instance'], toUnixTimestamp(bucket), count() FROM timeSeriesTags(ts_v12) GROUP BY ALL ORDER BY ALL"

function compare()
{
    local query="$1"
    local result_v11 result_v12
    result_v11=$($CLIENT -q "${query//TABLE/ts_v11}")
    result_v12=$($CLIENT -q "${query//TABLE/ts_v12}")
    echo "$result_v12"
    if [ "$result_v11" == "$result_v12" ]; then echo 'equal to version 11'; else echo "DIFFERENT from version 11: $result_v11"; fi
}

echo '-- instant query in the first bucket'
compare "SELECT tags, value FROM prometheusQuery(TABLE, 'cpu', 1010) ORDER BY ALL"
echo '-- instant query in the second bucket'
compare "SELECT tags, value FROM prometheusQuery(TABLE, 'cpu', 87470) ORDER BY ALL"
echo '-- aggregation in the second bucket'
compare "SELECT tags, value FROM prometheusQuery(TABLE, 'sum by (job) (cpu)', 87470) ORDER BY ALL"
echo '-- matchers'
compare "SELECT tags, value FROM prometheusQuery(TABLE, 'cpu{instance=~\"1|3\", job!=\"b\"}', 87470) ORDER BY ALL"
echo '-- window over both buckets'
compare "SELECT tags, value FROM prometheusQuery(TABLE, 'max_over_time(cpu[2d])', 87470) ORDER BY ALL"
compare "SELECT tags, value FROM prometheusQuery(TABLE, 'count_over_time({job=\"a\"}[2d])', 87470) ORDER BY ALL"
echo '-- range query over both buckets'
compare "SELECT tags, samples FROM prometheusQueryRange(TABLE, 'count by (job) (cpu)', 1000, 87400, 43200) ORDER BY ALL"
echo '-- window without samples'
compare "SELECT count() FROM prometheusQuery(TABLE, 'cpu', 500000)"

echo '-- TTL of the tags table'
$CLIENT -q "CREATE TABLE ts_ttl ENGINE = TimeSeries SETTINGS tags_bucket_step_seconds = 3600, tags_ttl_seconds = 7200, recent_samples_ttl_seconds = 0"
tags_table=$(inner_table ts_ttl tags)
$CLIENT -q "SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = '$tags_table'"

$CLIENT -q "INSERT INTO ts_ttl (metric_name, tags, samples) VALUES
    ('cpu', map('instance', 'old'), [(now64(3) - INTERVAL 10 DAY, 1.)]),
    ('cpu', map('instance', 'live'), [(now64(3) - INTERVAL 10 DAY, 2.), (now64(3) - INTERVAL 1 MINUTE, 3.)])"
$CLIENT -q "OPTIMIZE TABLE \`$tags_table\` FINAL"

echo '-- the expired bucket leaves the tags table, the live series stays'
$CLIENT -q "SELECT tags['instance'], bucket > now() - INTERVAL 1 DAY, count() FROM timeSeriesTags(ts_ttl) GROUP BY ALL ORDER BY ALL"
$CLIENT -q "SELECT tags, value FROM prometheusQuery(ts_ttl, 'cpu', now()) ORDER BY ALL"
echo '-- a query older than the retention finds no time series'
$CLIENT -q "SELECT count() FROM prometheusQuery(ts_ttl, 'cpu', now() - INTERVAL 10 DAY + INTERVAL 1 MINUTE)"

$CLIENT -q "DROP TABLE ts_v11"
$CLIENT -q "DROP TABLE ts_v12"
$CLIENT -q "DROP TABLE ts_ttl"
