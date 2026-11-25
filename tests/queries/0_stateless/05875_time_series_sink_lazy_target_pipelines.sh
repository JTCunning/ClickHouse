#!/usr/bin/env bash

# The sink of a TimeSeries table builds the INSERT into each of its target tables with the first block with rows.
# Checks that inserts with rows write the same rows through every path, including an asynchronous insert sent as
# blocks over the native protocol (its handler builds a sink that never gets data, as the remote-write handler does),
# and that inserts without rows complete and write nothing.

# Server warnings must not reach stderr at the test runner's log level.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SETTINGS="--enable_time_series_table 1 --session_timezone UTC"

$CLICKHOUSE_CLIENT $SETTINGS -q "
    DROP TABLE IF EXISTS ts;
    CREATE TABLE ts ENGINE = TimeSeries;
"

echo "-- synchronous INSERT with rows, in several blocks"
$CLICKHOUSE_CLIENT $SETTINGS --async_insert 0 --max_block_size 3 --min_insert_block_size_rows 3 -q "
    INSERT INTO ts (metric_name, tags, samples)
    SELECT 'm1', map('host', 'h' || toString(number)), [(toDateTime64(100 + number, 3), toFloat64(number))]
    FROM numbers(7)
"

echo "-- asynchronous INSERT with rows, sent as blocks over the native protocol"
printf 'm2\t{\x27host\x27:\x27h1\x27}\t[(\x271970-01-01 00:03:20.000\x27,1.5)]\nm2\t{\x27host\x27:\x27h2\x27}\t[(\x271970-01-01 00:03:21.000\x27,2.5)]\n' \
    | $CLICKHOUSE_CLIENT $SETTINGS --async_insert 1 --wait_for_async_insert 1 -q "INSERT INTO ts (metric_name, tags, samples) FORMAT TSV"

echo "-- asynchronous INSERT with rows, inlined in the query"
$CLICKHOUSE_CLIENT $SETTINGS --async_insert 1 --wait_for_async_insert 1 -q "
    INSERT INTO ts (metric_name, tags, samples) VALUES ('m3', map('host', 'h1'), [(toDateTime64(300, 3), 3)])
"

echo "-- metric families only, synchronous and asynchronous"
$CLICKHOUSE_CLIENT $SETTINGS --async_insert 0 -q "
    INSERT INTO ts (metric_family, type, unit, help) VALUES ('m1', 'gauge', 'bytes', 'first')
"
printf 'm2\tcounter\tseconds\tsecond\n' \
    | $CLICKHOUSE_CLIENT $SETTINGS --async_insert 1 --wait_for_async_insert 1 -q "INSERT INTO ts (metric_family, type, unit, help) FORMAT TSV"

echo "-- INSERTs without rows"
$CLICKHOUSE_CLIENT $SETTINGS --async_insert 0 --max_insert_threads 4 -q "
    INSERT INTO ts (metric_name, tags, samples)
    SELECT 'empty', map('host', 'h1'), [(toDateTime64(500, 3), 5.0)]
    FROM numbers(10) WHERE number > 100
"
$CLICKHOUSE_CLIENT $SETTINGS --async_insert 0 -q "
    INSERT INTO ts SELECT * FROM generateRandom('metric_name String, tags Map(String, String), samples Array(Tuple(DateTime64(3), Float64)), metric_family String, type String, unit String, help String') LIMIT 0
"
echo "ok"

echo "-- outer table"
$CLICKHOUSE_CLIENT $SETTINGS -q "
    SELECT metric_name, tags, samples FROM ts ORDER BY metric_name, toString(tags), toString(samples)
"

echo "-- inner tables"
$CLICKHOUSE_CLIENT $SETTINGS -q "
    SELECT 'tags', count(), countIf(metric_name = 'empty') FROM timeSeriesTags(ts) FINAL;
    SELECT 'time ranges', count(), min(min_time), max(max_time) FROM timeSeriesTimeRanges(ts) FINAL;
    SELECT 'samples', count(), sum(length(samples)) FROM timeSeriesSamples(ts) FINAL;
    SELECT 'metric families', metric_family, type, unit, help FROM timeSeriesMetricFamilies(ts) FINAL ORDER BY metric_family;
"

$CLICKHOUSE_CLIENT -q "DROP TABLE ts"
