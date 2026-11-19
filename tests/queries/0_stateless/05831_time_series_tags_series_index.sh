#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tags: no-fasttest
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

# A TimeSeries table of version 12 has an index of type `time_series` in its tags table. A selector finds the time series
# in the files of the index of each part and does not read the tags table. The results must be equal to the results of version 11.

$CLICKHOUSE_CLIENT -q "
    SET allow_experimental_time_series_table = 1;
    DROP TABLE IF EXISTS ts_v11;
    DROP TABLE IF EXISTS ts_v12;
    DROP TABLE IF EXISTS ts_v12_no_ranges;
    CREATE TABLE ts_v11 ENGINE = TimeSeries SETTINGS version = 11;
    CREATE TABLE ts_v12 ENGINE = TimeSeries SETTINGS version = 12;
    CREATE TABLE ts_v12_no_ranges ENGINE = TimeSeries SETTINGS version = 12, store_time_ranges = 0;
"

echo '-- the index of the tags table'
$CLICKHOUSE_CLIENT -q "
    SELECT name, type_full, expr, granularity FROM system.data_skipping_indices
    WHERE database = currentDatabase()
        AND table = '.inner_id.tags.' || (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_v12')
    ORDER BY name"
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND type = 'time_series'
        AND table = '.inner_id.tags.' || (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_v11')"

for table in ts_v11 ts_v12 ts_v12_no_ranges
do
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO $table (metric_name, tags, samples) VALUES
            ('up', map('job', 'api', 'instance', 'i1', 'env', 'prod'), [(toDateTime64(1000, 3), 1.), (toDateTime64(1030, 3), 1.)]),
            ('up', map('job', 'api', 'instance', 'i2', 'env', 'dev'), [(toDateTime64(1000, 3), 0.), (toDateTime64(1030, 3), 1.)]),
            ('up', map('job', 'apiserver', 'instance', 'i3'), [(toDateTime64(1000, 3), 1.), (toDateTime64(1030, 3), 2.)]),
            ('up', map('job', 'db', 'instance', 'i4', 'env', 'prod'), [(toDateTime64(100, 3), 7.)]);
        INSERT INTO $table (metric_name, tags, samples) VALUES
            ('up', map('job', 'api', 'instance', 'i1', 'env', 'prod'), [(toDateTime64(1045, 3), 3.)]),
            ('up', map('job', 'node', 'instance', 'i5', 'env', 'prod', 'zone', 'a'), [(toDateTime64(1045, 3), 4.)]),
            ('http_requests_total', map('job', 'api', 'instance', 'i1', 'code', '200'), [(toDateTime64(1045, 3), 10.)]),
            ('http_requests_total', map('job', 'api', 'instance', 'i1', 'code', '500'), [(toDateTime64(1045, 3), 11.)]),
            ('http_errors_total', map('job', 'db', 'instance', 'i4'), [(toDateTime64(1045, 3), 12.)]),
            ('no_labels', map(), [(toDateTime64(1045, 3), 13.)]);
        INSERT INTO $table (metric_name, tags, samples)
            SELECT 'many', map('n', toString(number), 'parity', if(number % 2 = 0, 'even', 'odd')), [(toDateTime64(1045, 3), toFloat64(number))]
            FROM numbers(3000);
    "
done

SELECTORS=(
    'up'
    'up{job="api"}'
    'up{job!="api"}'
    'up{job=~"api|db"}'
    'up{job=~"api.*"}'
    'up{job!~"api.*"}'
    'up{job=~"a.i|n..e"}'
    'up{env=""}'
    'up{env!=""}'
    'up{env=~".+"}'
    'up{env=~".*"}'
    'up{env!~".+"}'
    'up{env=~"prod|"}'
    'up{zone="a"}'
    'up{zone!="a"}'
    'up{missing=""}'
    'up{missing="x"}'
    'up{missing!="x"}'
    'up{job="api",instance="i2",env="dev"}'
    'up{job="api",instance="i2",env="prod"}'
    '{job="api"}'
    '{instance=~"i[14]"}'
    '{code!=""}'
    '{__name__="no_labels"}'
    '{__name__=~".*_total"}'
    '{__name__=~"http_.*",job="db"}'
    '{__name__!="up",job="api"}'
    '{__name__!~"up|many",instance=~".+"}'
    '{__name__=~"up|http_requests_total",code=""}'
    'missing_metric'
    'many{parity="odd"}'
    'many{n="2999"}'
    'many{n=~"29[0-9]{2}"}'
    'many{n=~"1|22|333|4444"}'
    'many{parity="even",n!~".*[1-9]"}'
)

run_selectors()
{
    local queries="SET allow_experimental_time_series_table = 1;"
    for selector in "${SELECTORS[@]}"
    do
        for time in 1060 1340
        do
            queries+="
                WITH
                    (SELECT arraySort(groupArray((tags, value))) FROM prometheusQuery(ts_v11, '$selector', $time)) AS v11,
                    (SELECT arraySort(groupArray((tags, value))) FROM prometheusQuery(ts_v12, '$selector', $time)) AS v12,
                    (SELECT arraySort(groupArray((tags, value))) FROM prometheusQuery(ts_v12_no_ranges, '$selector', $time)) AS v12_no_ranges
                SELECT '$selector', $time, length(v12), v11 = v12, v11 = v12_no_ranges;"
        done
    done
    $CLICKHOUSE_CLIENT -q "$queries"
}

echo '-- selectors, several parts'
run_selectors

echo '-- a range selector and a query with two selectors'
for table in ts_v11 ts_v12 ts_v12_no_ranges
do
    $CLICKHOUSE_CLIENT -q "
        SET allow_experimental_time_series_table = 1;
        SELECT tags, value FROM prometheusQuery($table, 'count_over_time(up{env=\"prod\"}[20m])', 1060) ORDER BY ALL;
        SELECT tags, value FROM prometheusQuery($table, 'sum by (job) (up{job=~\"api.*\"}) + on (job) sum by (job) (http_requests_total)', 1060) ORDER BY ALL;
        SELECT count(), sum(length(samples)) FROM timeSeriesSelector($table, 'many{parity=\"even\"}', 0, 2000, 'samples');
    "
done

echo '-- the selector uses the index and does not read the tags table'
query_id="05831_${CLICKHOUSE_DATABASE}_$RANDOM$RANDOM"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "
    SET allow_experimental_time_series_table = 1;
    SELECT count() FROM prometheusQuery(ts_v12, 'up{job=~\"api.*\"}', 1060)"
$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT ProfileEvents['TimeSeriesIndexLookupParts'] > 0, ProfileEvents['TimeSeriesIndexLookupSeries'],
        countIf(t LIKE '%.inner\_id.tags.%') FROM system.query_log ARRAY JOIN tables AS t
    WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish'
    GROUP BY ALL"

$CLICKHOUSE_CLIENT -q "
    OPTIMIZE TABLE ts_v11 FINAL;
    OPTIMIZE TABLE ts_v12 FINAL;
    OPTIMIZE TABLE ts_v12_no_ranges FINAL;
"

echo '-- selectors, one part'
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM system.parts
    WHERE database = currentDatabase() AND active
        AND table = '.inner_id.tags.' || (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_v12')"
run_selectors

$CLICKHOUSE_CLIENT -q "
    DROP TABLE ts_v11;
    DROP TABLE ts_v12;
    DROP TABLE ts_v12_no_ranges;
"
