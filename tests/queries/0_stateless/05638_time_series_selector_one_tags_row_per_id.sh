#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

# Until the parts of the tags table are merged, it has a row for a time series in each part written with it.
# The selector keeps one tags row per time series before joining the time ranges table, so the join and the id set
# get one row per time series. Only with the default id generator: a custom one may give different tags the same id.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The inserts repeat the same tags rows on purpose: no insert deduplication.
CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --session_timezone UTC --insert_deduplicate 0"

stop_merges()
{
    for target in tags timeranges; do
        table=$($CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.$target.%' AND name LIKE '%' || (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = '$1')")
        $CLIENT -q "SYSTEM STOP MERGES \`$table\`"
    done
}

$CLIENT -q "CREATE TABLE ts ENGINE = TimeSeries SETTINGS tags_deduplication_cache_size_bytes = 0"
stop_merges ts

# Three inserts of the same three time series; `stale` has no sample in the selected time range.
for i in 1 2 3; do
    $CLIENT -q "INSERT INTO ts (metric_name, tags, samples) VALUES
        ('foo', map('env', 'prod'), [(toDateTime64($i * 100, 3), $i)]),
        ('foo', map('env', 'dev'), [(toDateTime64($i * 100, 3), $i * 10)]),
        ('foo', map('env', 'stale'), [(toDateTime64(5000 + $i, 3), 0)])"
done

echo '-- rows of the tags and time ranges tables'
$CLIENT -q "SELECT replaceRegexpOne(table, '^.inner_id.([a-z]+).*', '\\\\1') AS target, sum(rows), count() FROM system.parts
    WHERE database = currentDatabase() AND active AND (table LIKE '.inner_id.tags.%' OR table LIKE '.inner_id.timeranges.%') GROUP BY table ORDER BY target"

echo '-- one tags row per time series before the join, and the id set is built from one row per time series'
$CLIENT --send_logs_level debug -q "SELECT 1 FROM timeSeriesSelector(ts, 'foo', 0, 1000, 'timestamp_and_value') FORMAT Null" 2>&1 \
    | grep -o -E 'LIMIT 1 BY id|Created Set with [0-9]+ entries from [0-9]+ rows' | sort -u

echo '-- the same without a metric name: the tags rows are sorted by id instead of read in that order'
$CLIENT --send_logs_level debug -q "SELECT 1 FROM timeSeriesSelector(ts, '{env=~\"prod|dev\"}', 0, 1000, 'timestamp_and_value') FORMAT Null" 2>&1 \
    | grep -o -E 'LIMIT 1 BY id|Created Set with [0-9]+ entries from [0-9]+ rows' | sort -u

echo '-- selected samples'
$CLIENT -q "SELECT timestamp, value FROM timeSeriesSelector(ts, 'foo{env=\"prod\"}', 0, 1000, 'timestamp_and_value') ORDER BY timestamp"
$CLIENT -q "SELECT count(), sum(value) FROM timeSeriesSelector(ts, 'foo', 0, 1000, 'timestamp_and_value')"
$CLIENT -q "SELECT count(), sum(value) FROM timeSeriesSelector(ts, 'foo', 0, 1000, 'timestamp_and_value') SETTINGS any_join_distinct_right_table_keys = 1"
$CLIENT -q "SELECT count(), sum(value) FROM timeSeriesSelector(ts, 'foo', 0, 10000, 'timestamp_and_value')"

echo '-- a custom id generator keeps every tags row'
$CLIENT -q "CREATE TABLE ts_custom_id ENGINE = TimeSeries SETTINGS tags_deduplication_cache_size_bytes = 0
    TAGS INNER COLUMNS (id UInt64 DEFAULT sipHash64(metric_name, tags))"
stop_merges ts_custom_id
for i in 1 2; do
    $CLIENT -q "INSERT INTO ts_custom_id (metric_name, tags, samples) VALUES ('foo', map('env', 'prod'), [(toDateTime64($i * 100, 3), $i)])"
done
$CLIENT --send_logs_level debug -q "SELECT 1 FROM timeSeriesSelector(ts_custom_id, 'foo', 0, 1000, 'timestamp_and_value') FORMAT Null" 2>&1 \
    | grep -o -E 'LIMIT 1 BY id|Created Set with [0-9]+ entries from [0-9]+ rows' | sort -u
$CLIENT -q "SELECT count(), sum(value) FROM timeSeriesSelector(ts_custom_id, 'foo', 0, 1000, 'timestamp_and_value')"

echo '-- tags stored in their own columns go through the subquery'
$CLIENT -q "CREATE TABLE ts_columns ENGINE = TimeSeries SETTINGS tags_deduplication_cache_size_bytes = 0, tags_to_columns = {'env': 'env_column'}"
stop_merges ts_columns
for i in 1 2; do
    $CLIENT -q "INSERT INTO ts_columns (metric_name, tags, samples) VALUES
        ('foo', map('env', 'prod', 'job', 'a'), [(toDateTime64($i * 100, 3), $i)]),
        ('foo', map('env', 'dev', 'job', 'a'), [(toDateTime64($i * 100, 3), $i * 10)])"
done
$CLIENT --send_logs_level debug -q "SELECT 1 FROM timeSeriesSelector(ts_columns, 'foo{env=\"prod\"}', 0, 1000, 'timestamp_and_value') FORMAT Null" 2>&1 \
    | grep -o -E 'LIMIT 1 BY id|Created Set with [0-9]+ entries from [0-9]+ rows' | sort -u
$CLIENT -q "SELECT timeSeriesIdToTags(id), timestamp, value FROM timeSeriesSelector(ts_columns, 'foo{env=\"prod\"}', 0, 1000, 'timestamp_and_value') ORDER BY timestamp"

echo '-- the text index on the tags drops the parts without the selected time series in the index analysis, not while reading'
$CLIENT -q "CREATE TABLE ts_parts ENGINE = TimeSeries SETTINGS tags_deduplication_cache_size_bytes = 0"
stop_merges ts_parts
for env in a b c; do
    $CLIENT -q "INSERT INTO ts_parts (metric_name, tags, samples) VALUES ('foo', map('env', '$env'), [(toDateTime64(100, 3), 1)])"
done
$CLIENT --send_logs_level debug -q "SELECT 1 FROM timeSeriesSelector(ts_parts, 'foo{env=\"a\"}', 0, 1000, 'timestamp_and_value') FORMAT Null" 2>&1 \
    | grep -c -E 'has dropped [0-9]+/[0-9]+ granules in part'
$CLIENT -q "SELECT timeSeriesIdToTags(id), timestamp, value FROM timeSeriesSelector(ts_parts, 'foo{env=\"a\"}', 0, 1000, 'timestamp_and_value')"

$CLIENT -q "DROP TABLE ts"
$CLIENT -q "DROP TABLE ts_custom_id"
$CLIENT -q "DROP TABLE ts_columns"
$CLIENT -q "DROP TABLE ts_parts"
