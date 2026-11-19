#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tags: no-fasttest
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

# A TimeSeries table of version 13 has the column `heartbeat` in its tags table and no time ranges table.
# The row of a time series is written again when its samples are `tags_heartbeat_seconds` later than the heartbeat,
# and a selector skips the time series whose heartbeat is older than the time range by that period or more.
# The results must be equal to the results of version 11.

$CLICKHOUSE_CLIENT -q "
    SET allow_experimental_time_series_table = 1;
    DROP TABLE IF EXISTS ts_v11;
    DROP TABLE IF EXISTS ts_v13;
    DROP TABLE IF EXISTS ts_v13_sql;
    CREATE TABLE ts_v11 ENGINE = TimeSeries SETTINGS version = 11;
    CREATE TABLE ts_v13 ENGINE = TimeSeries SETTINGS tags_heartbeat_seconds = 600;
    CREATE TABLE ts_v13_sql ENGINE = TimeSeries
        SETTINGS tags_heartbeat_seconds = 600, tags_series_index = 0, tags_deduplication_cache_size_bytes = 0;
"

inner_table()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT name FROM system.tables
        WHERE database = currentDatabase()
            AND name = '.inner_id.$1.' || (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = '$2')"
}

tags_v13=$(inner_table tags ts_v13)
tags_v13_sql=$(inner_table tags ts_v13_sql)

echo '-- the inner tables'
$CLICKHOUSE_CLIENT -q "
    SELECT outer.name, arraySort(groupArray(splitByChar('.', inner.name)[3]))
    FROM system.tables AS inner, system.tables AS outer
    WHERE inner.database = currentDatabase() AND outer.database = currentDatabase() AND outer.name IN ('ts_v11', 'ts_v13', 'ts_v13_sql')
        AND inner.name LIKE '.inner\_id.%' AND endsWith(inner.name, toString(outer.uuid))
    GROUP BY outer.name ORDER BY outer.name"

echo '-- the tags table'
$CLICKHOUSE_CLIENT -q "
    SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = '$tags_v13' ORDER BY position;
    SELECT engine, extract(engine_full, '^\\w+\\([^)]*\\)') FROM system.tables WHERE database = currentDatabase() AND name = '$tags_v13';
    SELECT table = '$tags_v13' ? 'ts_v13' : 'ts_v13_sql', name, expr FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table IN ('$tags_v13', '$tags_v13_sql') ORDER BY ALL;
"

echo '-- settings which a table with heartbeats rejects'
$CLICKHOUSE_CLIENT -q "
    SET allow_experimental_time_series_table = 1;
    CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS store_time_ranges = 1; -- { serverError INVALID_SETTING_VALUE }
    CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS tags_heartbeat_seconds = 0; -- { serverError INVALID_SETTING_VALUE }
    CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS version = 12, tags_heartbeat_seconds = 600; -- { serverError INVALID_SETTING_VALUE }
"

# The unmerged rows of the tags table show which inserts wrote a row.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES \`$tags_v13\`; SYSTEM STOP MERGES \`$tags_v13_sql\`"

for table in ts_v11 ts_v13 ts_v13_sql
do
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO $table (metric_name, tags, samples) VALUES
            ('up', map('instance', 'a'), [(toDateTime64(1000, 3), 1.)]),
            ('up', map('instance', 'b'), [(toDateTime64(1000, 3), 2.)]);
        INSERT INTO $table (metric_name, tags, samples) VALUES ('up', map('instance', 'b'), [(toDateTime64(1300, 3), 3.)]);
        INSERT INTO $table (metric_name, tags, samples) VALUES ('up', map('instance', 'b'), [(toDateTime64(1599, 3), 4.)]);
        INSERT INTO $table (metric_name, tags, samples) VALUES ('up', map('instance', 'b'), [(toDateTime64(1700, 3), 5.)]);
        INSERT INTO $table (metric_name, tags, samples) VALUES
            ('up', map('instance', 'b'), [(toDateTime64(4990, 3), 6.), (toDateTime64(5000, 3), 7.), (toDateTime64(4995, 3), 8.)]),
            ('up', map('instance', 'c'), [(toDateTime64(5000, 3), 9.)]),
            ('up', map('instance', 'd'), [(toDateTime64(5000, 3), 10.)]),
            ('down', map('instance', 'a'), [(toDateTime64(5000, 3), 11.)]);
        INSERT INTO $table (metric_name, tags, samples) VALUES ('up', map('instance', 'd'), [(toDateTime64(1000, 3), 12.)]);
    "
done

show_heartbeats()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT metric_name, tags['instance'], arraySort(groupArray(toUnixTimestamp(heartbeat)))
        FROM \`$1\` GROUP BY ALL ORDER BY ALL"
}

echo '-- heartbeats of the unmerged rows, with the deduplication cache'
show_heartbeats "$tags_v13"
echo '-- heartbeats of the unmerged rows, without the deduplication cache'
show_heartbeats "$tags_v13_sql"

run_queries()
{
    local queries="SET allow_experimental_time_series_table = 1;"
    for selector in 'up' 'up{instance=~"a|b"}' 'up{instance!="b"}' '{instance="a"}' '{__name__=~".+"}'
    do
        for time in 1100 1400 1650 1800 2300 5100 9000
        do
            queries+="
                WITH
                    (SELECT arraySort(groupArray((tags, value))) FROM prometheusQuery(ts_v11, '$selector', $time)) AS v11,
                    (SELECT arraySort(groupArray((tags, value))) FROM prometheusQuery(ts_v13, '$selector', $time)) AS v13,
                    (SELECT arraySort(groupArray((tags, value))) FROM prometheusQuery(ts_v13_sql, '$selector', $time)) AS v13_sql
                SELECT '$selector', $time, length(v13), v11 = v13, v11 = v13_sql;"
        done
        for range in '0, 10000' '1590, 1650' '1601, 1650' '2301, 4989' '4000, 6000' '6000, 7000'
        do
            queries+="
                WITH
                    (SELECT (count(), sum(length(samples))) FROM timeSeriesSelector(ts_v11, '$selector', $range, 'samples')) AS v11,
                    (SELECT (count(), sum(length(samples))) FROM timeSeriesSelector(ts_v13, '$selector', $range, 'samples')) AS v13,
                    (SELECT (count(), sum(length(samples))) FROM timeSeriesSelector(ts_v13_sql, '$selector', $range, 'samples')) AS v13_sql
                SELECT '$selector', '$range', v13, v11 = v13, v11 = v13_sql;"
        done
    done
    queries+="
        SELECT tags, value FROM prometheusQuery(ts_v13, 'count_over_time(up[30m])', 2300) ORDER BY ALL;
        SELECT tags, value FROM prometheusQuery(ts_v13_sql, 'count_over_time(up[30m])', 2300) ORDER BY ALL;"
    $CLICKHOUSE_CLIENT -q "$queries"
}

show_pruned_rows()
{
    local query_id="05832_${CLICKHOUSE_DATABASE}_$RANDOM$RANDOM"
    $CLICKHOUSE_CLIENT --query_id "$query_id" --allow_experimental_time_series_table 1 -q "
        SELECT count() FROM prometheusQuery(ts_v13, 'up', $1)"
    $CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS query_log;
        SELECT ProfileEvents['TimeSeriesIndexLookupSeries'], ProfileEvents['TimeSeriesIndexLookupRowsPrunedByHeartbeat']
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish'"
}

echo '-- queries, several parts'
run_queries

echo '-- series found and rows skipped by the heartbeat, several parts'
show_pruned_rows 1100
show_pruned_rows 5100

$CLICKHOUSE_CLIENT -q "
    SYSTEM START MERGES \`$tags_v13\`;
    SYSTEM START MERGES \`$tags_v13_sql\`;
    OPTIMIZE TABLE ts_v11 FINAL;
    OPTIMIZE TABLE ts_v13 FINAL;
    OPTIMIZE TABLE ts_v13_sql FINAL;
"

echo '-- heartbeats of the merged rows, with the deduplication cache'
show_heartbeats "$tags_v13"
echo '-- heartbeats of the merged rows, without the deduplication cache'
show_heartbeats "$tags_v13_sql"

echo '-- queries, one part'
run_queries

echo '-- series found and rows skipped by the heartbeat, one part'
show_pruned_rows 1100
show_pruned_rows 5100

$CLICKHOUSE_CLIENT -q "
    DROP TABLE ts_v11;
    DROP TABLE ts_v13;
    DROP TABLE ts_v13_sql;
"
