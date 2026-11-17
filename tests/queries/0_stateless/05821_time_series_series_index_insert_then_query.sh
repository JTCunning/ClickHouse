#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
# Tag no-parallel-replicas: a query with parallel replicas reads the tags table instead of the series index.

# A query which runs right after an insert finds the new time series, with the series index of a `TimeSeries` table
# and with other inserts which run at the same time: the query applies the new data parts to the index itself,
# or waits for them, or reads the tags table and the time ranges table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --session_timezone UTC"

$CLIENT -q "CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0"

echo '-- the index is built'
$CLIENT -q "INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('series', '0'), [(toDateTime64(100, 3), 1.)])"
$CLIENT --time_series_use_series_index 1 --time_series_check_series_index 1 -q "
    SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, 'm', 0, 1000, 'timestamp_and_value') ORDER BY tags"

stop_file="${CLICKHOUSE_TMP}/05821_stop_${CLICKHOUSE_DATABASE}"
rm -f "$stop_file"

# other_inserts <name>: inserts time series of another metric until the stop file appears.
other_inserts()
{
    local i=0
    while [ ! -e "$stop_file" ]; do
        i=$((i + 1))
        $CLIENT -q "
            INSERT INTO ts (metric_name, tags, samples)
            SELECT 'other', map('source', '$1', 'n', toString($i * 100 + number)), [(toDateTime64(100, 3), toFloat64(number))]
            FROM numbers(100)"
    done
}

other_inserts a &
other_inserts b &

num_queries=30
queries_without_new_series=0
for i in $(seq 1 $num_queries); do
    $CLIENT -q "INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('series', '$i'), [(toDateTime64(100, 3), 1.)])"
    found=$($CLIENT --time_series_use_series_index 1 --log_comment 05821_after_insert -q "
        SELECT count() FROM timeSeriesSelector(ts, 'm{series=\"$i\"}', 0, 1000, 'timestamp_and_value')")
    if [ "$found" != 1 ]; then
        queries_without_new_series=$((queries_without_new_series + 1))
    fi
done
touch "$stop_file"
wait
rm -f "$stop_file"
echo "queries without the new time series: $queries_without_new_series"

echo '-- each selector is counted once, and a hit after a wait is a hit'
$CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLIENT -q "
    SELECT
        count() AS queries,
        sum(ProfileEvents['TimeSeriesSeriesIndexHits']) + sum(ProfileEvents['TimeSeriesSeriesIndexMisses']) = queries AS all_counted,
        sum(ProfileEvents['TimeSeriesSeriesIndexHitsAfterWait']) <= sum(ProfileEvents['TimeSeriesSeriesIndexHits']) AS waits_are_hits
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND log_comment = '05821_after_insert'"

$CLIENT -q "DROP TABLE ts"
