#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
# Tag no-parallel-replicas: a query with parallel replicas reads the tags table instead of the series index.

# Random time series and selectors with concurrent inserts and merges: the series index must return
# every time series which the tags table and the time ranges table return, and the selected samples must be the same.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --session_timezone UTC"

# Without the cache every insert writes the tags, so the tags table gets many data parts to merge.
$CLIENT -q "CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0, tags_deduplication_cache_size_bytes = 0"
tags_table=$($CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.tags.%'")
time_ranges_table=$($CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.timeranges.%'")

insert_series()
{
    $CLIENT -q "
        INSERT INTO ts (metric_name, tags, samples)
        SELECT
            'm' || toString(rand(1) % 5),
            mapFilter((k, v) -> v != '', map(
                'a', ['', 'x', 'y', 'z'][1 + rand(2) % 4],
                'b', toString(rand(3) % 20),
                'c', ['', 'p', 'q'][1 + rand(4) % 3])),
            [(toDateTime64(100 + rand(5) % 900, 3), toFloat64(number))]
        FROM numbers(200)"
}

selectors=(
    '{__name__=~"m[0-2]"}'
    'm1{a="x"}'
    'm2{a!="x",b=~"1."}'
    '{__name__=~"m.*",c=""}'
    'm3{b!~"[0-9]"}'
    '{__name__="m4",a=~"x|y",c!=""}'
    'm0{a=~".*",b=~".+"}'
    '{__name__!="m0",b="7"}'
)

# select_samples <selector> <min_time> <max_time> <client options>
select_samples()
{
    local selector=$1 min_time=$2 max_time=$3
    shift 3
    $CLIENT "$@" --param_selector "$selector" -q "
        SELECT timeSeriesIdToTags(id) AS tags, timestamp, value
        FROM timeSeriesSelector(ts, {selector:String}, $min_time, $max_time, 'timestamp_and_value') ORDER BY ALL" 2>&1
}

insert_series

(
    for _ in $(seq 1 12); do
        insert_series
    done
) &
(
    for _ in $(seq 1 6); do
        $CLIENT -q "OPTIMIZE TABLE \`$tags_table\`"
        $CLIENT -q "OPTIMIZE TABLE \`$time_ranges_table\` FINAL"
    done
) &

# The queries run while the data parts appear and merge.
errors=0
for i in $(seq 1 40); do
    selector=${selectors[$((i % ${#selectors[@]}))]}
    min_time=$((RANDOM % 600))
    max_time=$((min_time + RANDOM % 600))
    if select_samples "$selector" "$min_time" "$max_time" --time_series_use_series_index 1 --time_series_check_series_index 1 --log_comment 05818_concurrent | grep -F "Exception"; then
        errors=$((errors + 1))
    fi
done
wait
echo "queries with errors: $errors"

# No concurrent changes now: the index and the tables must give the same samples.
different=0
for selector in "${selectors[@]}"; do
    for range in "0 1000" "100 400" "550 560"; do
        # shellcheck disable=SC2086
        from_index=$(select_samples "$selector" $range --time_series_use_series_index 1 --time_series_check_series_index 1 --log_comment 05818_final)
        # shellcheck disable=SC2086
        from_tables=$(select_samples "$selector" $range --time_series_use_series_index 0)
        if [ "$from_index" != "$from_tables" ]; then
            different=$((different + 1))
            echo "selector $selector range $range"
        fi
    done
done
echo "different results: $different"

$CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLIENT -q "
    SELECT sum(ProfileEvents['TimeSeriesSeriesIndexHits']) > 0 AS has_hits, sum(ProfileEvents['TimeSeriesSeriesIndexMisses']) AS misses
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND log_comment IN ('05818_concurrent', '05818_final')"

$CLIENT -q "DROP TABLE ts"
