#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper, no-parallel-replicas, no-shared-merge-tree, no-replicated-database
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
# Tag no-parallel-replicas: a query with parallel replicas reads the tags table instead of the series index.
# Tag no-shared-merge-tree: the test declares `ReplicatedMergeTree` inner tables.
# Tag no-replicated-database: the two tables of the test are replicas of each other.

# The series index of a replica follows the data parts which the replica fetches from another replica,
# including a merged data part fetched instead of the data parts it was merged from.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --session_timezone UTC"

for replica in r1 r2; do
    $CLIENT -q "
        CREATE TABLE ts_$replica ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
        SAMPLES ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/05817/samples', '$replica')
        TAGS ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/05817/tags', '$replica')
        TIME RANGES ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/05817/time_ranges', '$replica')
        METRIC FAMILIES ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/05817/metric_families', '$replica')"
done

# inner_table <replica> <kind>
inner_table()
{
    $CLIENT -q "
        SELECT name FROM system.tables
        WHERE database = currentDatabase() AND name LIKE '.inner_id.$2.%' AND engine_full LIKE '%''$1''%'"
}

# sync_replica <replica>
sync_replica()
{
    for kind in samples tags timeranges; do
        $CLIENT -q "SYSTEM SYNC REPLICA \`$(inner_table "$1" "$kind")\`"
    done
}

# select_series <replica> <log_comment>
select_series()
{
    $CLIENT --time_series_use_series_index 1 --time_series_check_series_index 1 --log_comment "$2" -q "
        SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts_$1, 'm', 0, 1000, 'timestamp_and_value') ORDER BY tags"
}

echo '-- both replicas insert'
$CLIENT -q "INSERT INTO ts_r1 (metric_name, tags, samples) VALUES ('m', map('series', '1'), [(toDateTime64(100, 3), 1.)])"
$CLIENT -q "INSERT INTO ts_r2 (metric_name, tags, samples) VALUES ('m', map('series', '2'), [(toDateTime64(100, 3), 2.)])"
sync_replica r1
sync_replica r2
select_series r1 05817_used
select_series r2 05817_used

echo '-- a replica fetches new data parts'
$CLIENT -q "INSERT INTO ts_r1 (metric_name, tags, samples) VALUES ('m', map('series', '3'), [(toDateTime64(100, 3), 3.)])"
sync_replica r2
select_series r2 05817_used

echo '-- a replica fetches a merged data part'
$CLIENT -q "SYSTEM STOP FETCHES \`$(inner_table r2 tags)\`"
$CLIENT -q "SYSTEM STOP FETCHES \`$(inner_table r2 timeranges)\`"
$CLIENT -q "SYSTEM STOP FETCHES \`$(inner_table r2 samples)\`"
$CLIENT -q "INSERT INTO ts_r1 (metric_name, tags, samples) VALUES ('m', map('series', '4'), [(toDateTime64(100, 3), 4.)])"
$CLIENT -q "INSERT INTO ts_r1 (metric_name, tags, samples) VALUES ('m', map('series', '5'), [(toDateTime64(100, 3), 5.)])"
$CLIENT -q "OPTIMIZE TABLE \`$(inner_table r1 tags)\` FINAL"
$CLIENT -q "OPTIMIZE TABLE \`$(inner_table r1 timeranges)\` FINAL"
select_series r2 05817_used
$CLIENT -q "SYSTEM START FETCHES \`$(inner_table r2 tags)\`"
$CLIENT -q "SYSTEM START FETCHES \`$(inner_table r2 timeranges)\`"
$CLIENT -q "SYSTEM START FETCHES \`$(inner_table r2 samples)\`"
sync_replica r2
select_series r2 05817_used
select_series r1 05817_used

echo '-- the index was used every time'
$CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLIENT -q "
    SELECT sum(ProfileEvents['TimeSeriesSeriesIndexHits']) > 0 AS has_hits, sum(ProfileEvents['TimeSeriesSeriesIndexMisses']) AS misses
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND log_comment = '05817_used'"

$CLIENT -q "DROP TABLE ts_r1"
$CLIENT -q "DROP TABLE ts_r2"
