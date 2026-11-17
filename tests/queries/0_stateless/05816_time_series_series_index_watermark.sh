#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
# Tag no-parallel-replicas: a query with parallel replicas reads the tags table instead of the series index.

# The series index of a `TimeSeries` table with the recent samples table does not admit the time series
# without samples newer than the retention of that table. It remembers the newest time of the last sample of these time series,
# and a query for a time range which does not start after that time reads the tags table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --session_timezone UTC"

now=$(date +%s)
old=$((now - 10 * 86400))

# select_series <table> <min_time> <max_time> <log_comment>
select_series()
{
    $CLIENT --time_series_use_series_index 1 --time_series_check_series_index 1 --log_comment "$4" -q "
        SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector($1, 'm', $2, $3, 'timestamp_and_value') ORDER BY tags"
}

# hits_and_misses <log_comment>
hits_and_misses()
{
    $CLIENT -q "SYSTEM FLUSH LOGS query_log"
    $CLIENT -q "
        SELECT sum(ProfileEvents['TimeSeriesSeriesIndexHits']) > 0 AS has_hits, sum(ProfileEvents['TimeSeriesSeriesIndexMisses']) > 0 AS has_misses
        FROM system.query_log
        WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND log_comment = '$1'"
}

# Without the cache the tags of a time series are written with every insert.
$CLIENT -q "CREATE TABLE ts ENGINE = TimeSeries SETTINGS tags_deduplication_cache_size_bytes = 0"
$CLIENT -q "INSERT INTO ts (metric_name, tags, samples) VALUES
    ('m', map('series', 'old'), [(toDateTime64($old, 3), 1.)]),
    ('m', map('series', 'both'), [(toDateTime64($old, 3), 2.)])"
$CLIENT -q "INSERT INTO ts (metric_name, tags, samples) VALUES
    ('m', map('series', 'recent'), [(toDateTime64($((now - 60)), 3), 3.)]),
    ('m', map('series', 'both'), [(toDateTime64($((now - 120)), 3), 4.)])"

echo '-- a recent time range: the index'
select_series ts $((now - 600)) "$now" 05816_recent
hits_and_misses 05816_recent

echo '-- an old time range: the tables'
select_series ts $((old - 100)) $((old + 100)) 05816_old
hits_and_misses 05816_old

echo '-- a time range which starts before the last sample of the old time series: the tables'
select_series ts $((old - 100)) "$now" 05816_wide
hits_and_misses 05816_wide

echo '-- an old time series gets a new sample and its tags are written again: the index'
$CLIENT -q "INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('series', 'old'), [(toDateTime64($((now - 30)), 3), 5.)])"
select_series ts $((now - 600)) "$now" 05816_again
hits_and_misses 05816_again

echo '-- an old time series gets a new sample and its tags are not written again: the index reads its tags from the tags table'
$CLIENT -q "CREATE TABLE ts_cached ENGINE = TimeSeries"
$CLIENT -q "INSERT INTO ts_cached (metric_name, tags, samples) VALUES ('m', map('series', 'old'), [(toDateTime64($old, 3), 1.)])"
$CLIENT -q "INSERT INTO ts_cached (metric_name, tags, samples) VALUES ('m', map('series', 'recent'), [(toDateTime64($((now - 60)), 3), 2.)])"
select_series ts_cached $((now - 600)) "$now" 05816_cached_before
hits_and_misses 05816_cached_before
$CLIENT -q "INSERT INTO ts_cached (metric_name, tags, samples) VALUES ('m', map('series', 'old'), [(toDateTime64($((now - 30)), 3), 3.)])"
select_series ts_cached $((now - 600)) "$now" 05816_cached_after
hits_and_misses 05816_cached_after

$CLIENT -q "DROP TABLE ts"
$CLIENT -q "DROP TABLE ts_cached"
