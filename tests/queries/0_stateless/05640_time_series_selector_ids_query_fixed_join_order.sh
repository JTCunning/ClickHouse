#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

# The ids query of the selector joins the tags table with the time ranges table in a fixed order: the time ranges table
# is the right side of a `LEFT ANY JOIN`, and both tables are read in the order of `id` for the merge join. The join order
# optimization is off for it, so it does not load the statistics of both tables to decide whether to swap the two sides.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --session_timezone UTC --query_plan_optimize_join_order_limit 10"

$CLIENT -q "CREATE TABLE ts ENGINE = TimeSeries
    TAGS INNER ENGINE = ReplacingMergeTree SETTINGS auto_statistics_types = 'basic, uniq_v2'
    TIME RANGES INNER ENGINE = AggregatingMergeTree SETTINGS auto_statistics_types = 'basic, uniq_v2'"
for t in $($CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%'"); do
    $CLIENT -q "SYSTEM STOP MERGES \`$t\`"
done

for i in 1 2 3; do
    $CLIENT -q "INSERT INTO ts (metric_name, tags, samples) VALUES
        ('foo', map('env', 'prod', 'pod', 'p$i'), [(toDateTime64($i * 100, 3), $i)]),
        ('foo', map('env', 'dev'), [(toDateTime64($i * 100, 3), $i * 10)]),
        ('bar', map('env', 'prod'), [(toDateTime64($i * 100 + 50, 3), $i * 100)])"
done

for selector in 'foo{env="prod"}' 'foo{pod="p2"}' 'foo' '{__name__=~"foo|bar",env!="dev"}'; do
    echo "-- $selector"
    echo -n 'join order optimizations: '
    $CLIENT --send_logs_level debug -q "SELECT 1 FROM timeSeriesSelector(ts, '$selector', 0, 1000, 'timestamp_and_value') FORMAT Null" 2>&1 \
        | grep -c 'Optimizing join order'
    $CLIENT -q "SELECT count(), sum(value), groupArray(timestamp) FROM (SELECT timestamp, value FROM timeSeriesSelector(ts, '$selector', 0, 1000, 'timestamp_and_value') ORDER BY timestamp, value)"
done

echo '-- a time range outside the time ranges of some series'
$CLIENT -q "SELECT count(), sum(value) FROM timeSeriesSelector(ts, '{__name__=~\"foo|bar\"}', 120, 260, 'timestamp_and_value')"

$CLIENT -q "DROP TABLE ts"
