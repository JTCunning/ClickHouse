#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
# Tag no-parallel-replicas: a query with parallel replicas reads the tags table instead of the series index.

# The series index of a `TimeSeries` table which cannot hold all the time series with samples in the retention
# of the recent samples table keeps the ones with samples in a shorter time, and answers the queries for that time.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --session_timezone UTC"

# count_series <min_time> <max_time> <log_comment>: the number of the selected time series, resolved by the index and checked against the tables.
# The server warns when the index reaches its size limit, the warning must not get to the client.
count_series()
{
    $CLIENT --time_series_use_series_index 1 --time_series_check_series_index 1 --send_logs_level error --log_comment "$3" -q "
        SELECT uniqExact(id) FROM timeSeriesSelector(ts, 'm', $1, $2, 'timestamp_and_value')"
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

# The retention of the recent samples table is 4 days. The index has no place for the old time series.
$CLIENT -q "CREATE TABLE ts ENGINE = TimeSeries SETTINGS series_index_max_size_bytes = 1500000"

now=$(date +%s)
old=$((now - 302400))
$CLIENT -q "
    INSERT INTO ts (metric_name, tags, samples)
    SELECT 'm', map('series', 'old_' || toString(number)), [(toDateTime64($old, 3), 1.)] FROM numbers(30000)"
$CLIENT -q "
    INSERT INTO ts (metric_name, tags, samples)
    SELECT 'm', map('series', 'recent_' || toString(number)), [(toDateTime64($now, 3), 1.)] FROM numbers(10)"

echo '-- a recent time range: the index'
count_series $((now - 600)) $((now + 600)) 05823_recent
hits_and_misses 05823_recent

echo '-- a time range with the old time series: the tables'
count_series $((old - 600)) $((now + 600)) 05823_old
hits_and_misses 05823_old

echo '-- a new time series'
$CLIENT -q "INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('series', 'new'), [(toDateTime64($now, 3), 1.)])"
count_series $((now - 600)) $((now + 600)) 05823_new
hits_and_misses 05823_new

$CLIENT -q "DROP TABLE ts"
