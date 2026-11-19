#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

# A selector reads the tags table through the uncompressed cache by default (the setting `tags_use_uncompressed_cache`),
# whatever the settings of the query are. The other tables are read with the settings of the query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The query settings turn the caches off, so only the settings of the table can turn them on.
CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --session_timezone UTC --use_uncompressed_cache 0 --use_columns_cache 0
    --enable_reads_from_columns_cache 1 --enable_writes_to_columns_cache 1"

$CLIENT -q "CREATE TABLE ts_default ENGINE = TimeSeries"
$CLIENT -q "CREATE TABLE ts_off ENGINE = TimeSeries SETTINGS tags_use_uncompressed_cache = 0"
$CLIENT -q "CREATE TABLE ts_no_ranges_default ENGINE = TimeSeries SETTINGS store_time_ranges = 0"
$CLIENT -q "CREATE TABLE ts_no_ranges_off ENGINE = TimeSeries SETTINGS store_time_ranges = 0, tags_use_uncompressed_cache = 0"
$CLIENT -q "CREATE TABLE ts_columns_cache ENGINE = TimeSeries SETTINGS tags_use_uncompressed_cache = 0, tags_use_columns_cache = 1"

TABLES="ts_default ts_off ts_no_ranges_default ts_no_ranges_off ts_columns_cache"

for table in $TABLES; do
    $CLIENT -q "INSERT INTO $table (metric_name, tags, samples) VALUES
        ('cpu', map('job', 'a', 'instance', '1'), [(toDateTime64(1000, 3), 1.), (toDateTime64(1060, 3), 2.)]),
        ('cpu', map('job', 'a', 'instance', '2'), [(toDateTime64(1000, 3), 10.)]),
        ('cpu', map('job', 'b', 'instance', '3'), [(toDateTime64(1060, 3), 100.)]),
        ('mem', map('job', 'a'), [(toDateTime64(1000, 3), 5.), (toDateTime64(1060, 3), 6.)])"
done

echo '-- the default values are not written into the definition of the table'
$CLIENT -q "SELECT name, extract(create_table_query, 'tags_use_[a-z_]+ = [a-z0-9]+')
    FROM system.tables WHERE database = currentDatabase() AND name IN ('ts_default', 'ts_off') ORDER BY name"

echo '-- settings of the subquery reading the tags table and of the subquery reading the time ranges table'
for table in $TABLES; do
    echo "$table"
    $CLIENT --send_logs_level debug -q "SELECT 1 FROM timeSeriesSelector($table, 'cpu{job=\"a\"}', 900, 1100, 'timestamp_and_value') FORMAT Null" 2>&1 \
        | grep -F 'Will execute query' -A 1 | grep -o -E 'use_(uncompressed|columns)_cache = [a-z0-9]+' | sort -u
done

echo '-- cache events of a selector: uncompressed cache used, columns cache used'
for table in $TABLES; do
    for run in 1 2; do
        $CLIENT --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_${table}_${run}" \
            -q "SELECT 1 FROM timeSeriesSelector($table, 'cpu{job=\"a\"}', 900, 1100, 'timestamp_and_value') FORMAT Null"
    done
done
$CLIENT -q "SYSTEM FLUSH LOGS query_log"
for table in $TABLES; do
    $CLIENT -q "SELECT '$table', count(),
            min(ProfileEvents['UncompressedCacheHits'] + ProfileEvents['UncompressedCacheMisses'] > 0),
            max(ProfileEvents['UncompressedCacheHits'] + ProfileEvents['UncompressedCacheMisses'] > 0),
            min(ProfileEvents['ColumnsCacheHits'] + ProfileEvents['ColumnsCacheMisses'] > 0),
            max(ProfileEvents['ColumnsCacheHits'] + ProfileEvents['ColumnsCacheMisses'] > 0)
        FROM system.query_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
            AND query_id IN ('${CLICKHOUSE_TEST_UNIQUE_NAME}_${table}_1', '${CLICKHOUSE_TEST_UNIQUE_NAME}_${table}_2')"
done

function compare()
{
    local query="$1"
    local expected result
    expected=$($CLIENT -q "${query//TABLE/ts_off}")
    echo "$expected"
    for table in ts_default ts_no_ranges_default ts_no_ranges_off ts_columns_cache; do
        result=$($CLIENT -q "${query//TABLE/$table}")
        if [ "$expected" == "$result" ]; then echo "$table: equal"; else echo "$table: DIFFERENT: $result"; fi
    done
}

echo '-- selector'
compare "SELECT count(), sum(value) FROM timeSeriesSelector(TABLE, 'cpu', 900, 1100, 'timestamp_and_value')"
echo '-- instant query'
compare "SELECT tags, value FROM prometheusQuery(TABLE, 'cpu{instance=~\"1|3\"}', 1070) ORDER BY ALL"
echo '-- aggregation'
compare "SELECT tags, value FROM prometheusQuery(TABLE, 'sum by (job) ({__name__=~\"cpu|mem\"})', 1070) ORDER BY ALL"
echo '-- window without samples'
compare "SELECT count() FROM prometheusQuery(TABLE, 'cpu', 5000)"

echo '-- the settings can be changed after the table is created'
$CLIENT -q "ALTER TABLE ts_default MODIFY SETTING tags_use_uncompressed_cache = 0, tags_use_columns_cache = 1"
$CLIENT --send_logs_level debug -q "SELECT 1 FROM timeSeriesSelector(ts_default, 'cpu', 900, 1100, 'timestamp_and_value') FORMAT Null" 2>&1 \
    | grep -F 'Will execute query' -A 1 | grep -o -E 'use_(uncompressed|columns)_cache = [a-z0-9]+' | sort -u

for table in $TABLES; do
    $CLIENT -q "DROP TABLE $table"
done
