#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
# Tag no-parallel-replicas: a query with parallel replicas reads the tags table instead of the series index.

# The series index of a `TimeSeries` table follows the data parts of the tags table and the time ranges table:
# it applies new data parts, and it is built anew after a change which can remove rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --session_timezone UTC"

# select_series <min_time> <max_time> <log_comment>: the tags of the selected time series, resolved by the index and checked against the tables.
# The server warns when the index reaches its size limit, the warning must not get to the client.
select_series()
{
    $CLIENT --time_series_use_series_index 1 --time_series_check_series_index 1 --send_logs_level error --log_comment "$3" -q "
        SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, 'm', $1, $2, 'timestamp_and_value') ORDER BY tags"
}

$CLIENT -q "CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0"
tags_table=$($CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.tags.%'")
time_ranges_table=$($CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.timeranges.%'")

echo '-- the first time series'
$CLIENT -q "INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('series', '1'), [(toDateTime64(100, 3), 1.)])"
select_series 0 1000 05815_used

echo '-- a new time series right after the insert'
$CLIENT -q "INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('series', '2'), [(toDateTime64(200, 3), 2.)])"
select_series 0 1000 05815_used

echo '-- a wider time range of a known time series'
select_series 4000 6000 05815_used
$CLIENT -q "INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('series', '1'), [(toDateTime64(5000, 3), 3.)])"
select_series 4000 6000 05815_used

echo '-- merged data parts'
$CLIENT -q "OPTIMIZE TABLE \`$tags_table\` FINAL"
$CLIENT -q "OPTIMIZE TABLE \`$time_ranges_table\` FINAL"
select_series 0 10000 05815_used
$CLIENT -q "INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('series', '3'), [(toDateTime64(300, 3), 4.)])"
select_series 0 10000 05815_used

echo '-- a mutation of the tags table'
$CLIENT -q "ALTER TABLE \`$tags_table\` DELETE WHERE tags['series'] = '2' SETTINGS mutations_sync = 2"
select_series 0 10000 05815_used

echo '-- a lightweight delete from the tags table'
$CLIENT -q "DELETE FROM \`$tags_table\` WHERE tags['series'] = '3'"
select_series 0 10000 05815_used

echo '-- a mutation of the time ranges table'
$CLIENT -q "ALTER TABLE \`$time_ranges_table\` DELETE WHERE max_time > toDateTime64(1000, 3) SETTINGS mutations_sync = 2"
select_series 4000 6000 05815_used
select_series 0 1000 05815_used

echo '-- a dropped partition of the tags table'
$CLIENT -q "ALTER TABLE \`$tags_table\` DROP PARTITION ID 'all'"
select_series 0 10000 05815_used
$CLIENT -q "INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('series', '4'), [(toDateTime64(400, 3), 5.)])"
select_series 0 10000 05815_used

echo '-- a truncated time ranges table'
$CLIENT -q "TRUNCATE TABLE \`$time_ranges_table\`"
select_series 0 10000 05815_used
$CLIENT -q "INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('series', '5'), [(toDateTime64(500, 3), 6.)])"
select_series 0 10000 05815_used

echo '-- an altered tags table'
$CLIENT -q "ALTER TABLE \`$tags_table\` MODIFY COMMENT 'altered'"
select_series 0 10000 05815_used

echo '-- a truncated table'
$CLIENT -q "TRUNCATE TABLE ts"
select_series 0 10000 05815_used
$CLIENT -q "INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('series', '6'), [(toDateTime64(600, 3), 7.)])"
select_series 0 10000 05815_used

echo '-- dropped caches'
$CLIENT -q "SYSTEM CLEAR TIME SERIES CACHES ts"
select_series 0 10000 05815_used

echo '-- the index was used every time'
$CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLIENT -q "
    SELECT sum(ProfileEvents['TimeSeriesSeriesIndexHits']) > 0 AS has_hits, sum(ProfileEvents['TimeSeriesSeriesIndexMisses']) AS misses
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND log_comment = '05815_used'"

echo '-- the index is disabled by the setting of the table'
$CLIENT -q "ALTER TABLE ts MODIFY SETTING use_series_index = 0"
select_series 0 10000 05815_disabled
$CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLIENT -q "
    SELECT sum(ProfileEvents['TimeSeriesSeriesIndexHits']) AS hits, sum(ProfileEvents['TimeSeriesSeriesIndexMisses']) AS misses
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND log_comment = '05815_disabled'"

echo '-- the index is bigger than its size limit'
$CLIENT -q "ALTER TABLE ts MODIFY SETTING use_series_index = 1, series_index_max_size_bytes = 1"
select_series 0 10000 05815_size_limit
$CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLIENT -q "
    SELECT sum(ProfileEvents['TimeSeriesSeriesIndexHits']) AS hits, sum(ProfileEvents['TimeSeriesSeriesIndexMisses']) AS misses
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND log_comment = '05815_size_limit'"

echo '-- the index is enabled again'
$CLIENT -q "ALTER TABLE ts RESET SETTING series_index_max_size_bytes"
select_series 0 10000 05815_enabled
$CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLIENT -q "
    SELECT sum(ProfileEvents['TimeSeriesSeriesIndexHits']) > 0 AS has_hits, sum(ProfileEvents['TimeSeriesSeriesIndexMisses']) AS misses
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND log_comment = '05815_enabled'"

$CLIENT -q "DROP TABLE ts"
