#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
# Tag no-parallel-replicas: a query with parallel replicas reads the tags table instead of the series index.

# The series index of a `TimeSeries` table reads the data parts of the tags table and the time ranges table
# while merges replace these data parts. A query must return every time series inserted before the query started.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --session_timezone UTC"

# Without the cache every insert writes the tags, so the tags table gets many data parts to merge.
$CLIENT -q "CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0, tags_deduplication_cache_size_bytes = 0"
tags_table=$($CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.tags.%'")
time_ranges_table=$($CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.timeranges.%'")

num_inserts=40
series_per_insert=50
progress_file="${CLICKHOUSE_TMP}/05820_progress_${CLICKHOUSE_DATABASE}"
echo 0 > "$progress_file"

# The number of finished inserts is in the file.
(
    for i in $(seq 1 $num_inserts); do
        $CLIENT -q "
            INSERT INTO ts (metric_name, tags, samples)
            SELECT 'm', map('insert', toString($i), 'n', toString(number)), [(toDateTime64(100 + $i, 3), toFloat64(number))]
            FROM numbers($series_per_insert)"
        echo "$i" > "$progress_file.tmp"
        mv "$progress_file.tmp" "$progress_file"
    done
) &
inserter=$!

(
    while kill -0 $inserter 2> /dev/null; do
        $CLIENT -q "OPTIMIZE TABLE \`$tags_table\` FINAL"
        $CLIENT -q "OPTIMIZE TABLE \`$time_ranges_table\` FINAL"
    done
) &

# count_series <client options>
count_series()
{
    $CLIENT "$@" -q "SELECT uniqExact(id) FROM timeSeriesSelector(ts, 'm', 0, 1000, 'timestamp_and_value')"
}

queries_with_lost_series=0
while kill -0 $inserter 2> /dev/null; do
    inserted=$(cat "$progress_file")
    found=$(count_series --time_series_use_series_index 1 --log_comment 05820_concurrent)
    if [ "$found" -lt $((inserted * series_per_insert)) ]; then
        queries_with_lost_series=$((queries_with_lost_series + 1))
        echo "found $found time series after $inserted inserts"
    fi
done
wait
rm -f "$progress_file"
echo "queries with lost time series: $queries_with_lost_series"

echo '-- the index and the tables after the merges'
count_series --time_series_use_series_index 1 --time_series_check_series_index 1 --log_comment 05820_final
count_series --time_series_use_series_index 0

$CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLIENT -q "
    SELECT sum(ProfileEvents['TimeSeriesSeriesIndexHits']) > 0 AS has_hits
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND log_comment IN ('05820_concurrent', '05820_final')"

$CLIENT -q "DROP TABLE ts"
