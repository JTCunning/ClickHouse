#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

# `timeSeriesSelector` probes the tags table to decide whether a selector matches the whole metric
# (see 04836_time_series_selector_whole_metric_pk_range). A distributed `INSERT SELECT` with parallel
# replicas interprets its `SELECT` twice on the initiator, once to check that the query can be distributed
# and once to build the local pipeline, and each interpretation reaches the probe. The verdict is kept on
# the query context, so the second interpretation reuses it. The probe lines are counted from the debug
# log sent to the client. Every replica of the cluster is this server, and the followers forward their own
# log lines too, so only the lines of the initiator's query id are counted.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1"
CLIENT_DEBUG_LOG=$(echo "${CLIENT}" | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=debug/g')

# The local pipeline is the second interpretation. Without it the initiator only ships the insert.
PARALLEL_REPLICAS="--enable_parallel_replicas=1 --max_parallel_replicas=3 --cluster_for_parallel_replicas=test_cluster_one_shard_three_replicas_localhost --parallel_replicas_for_non_replicated_merge_tree=1 --parallel_distributed_insert_select=2 --parallel_replicas_local_plan=1 --parallel_replicas_prefer_local_replica=1"

# The shipped insert keeps the selector's table name as written, so a follower needs it qualified.
TS="${CLICKHOUSE_DATABASE}.ts_probe_once"

$CLIENT -q "
DROP TABLE IF EXISTS ts_probe_once;
DROP TABLE IF EXISTS ts_probe_once_dst SYNC;

CREATE TABLE ts_probe_once ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, UUID));
CREATE TABLE ts_probe_once_dst (timestamp DateTime64(3), value Float64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/ts_probe_once_dst', 'r1') ORDER BY timestamp;

INSERT INTO ts_probe_once (metric_name, tags, samples) VALUES
    ('probe_once_foo', map('env', 'prod'), [(toDateTime64(100, 3), 1.), (toDateTime64(200, 3), 2.)]),
    ('probe_once_foo', map('env', 'dev'), [(toDateTime64(150, 3), 10.)]),
    ('probe_once_bar', map('env', 'prod'), [(toDateTime64(100, 3), 100.)]),
    ('probe_once_bar', map('env', 'dev'), [(toDateTime64(300, 3), 300.)]);
"

LOG="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.log"

count_initiator_log_lines()
{
    local query_id="$1" pattern="$2"
    grep -F "{${query_id}}" "$LOG" | grep -cF "$pattern" || true
}

echo "-- INSERT SELECT of a whole-metric selector: one probe, the second interpretation reuses the verdict"
QUERY_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_whole"
$CLIENT_DEBUG_LOG $PARALLEL_REPLICAS --query_id="$QUERY_ID" -q "INSERT INTO ts_probe_once_dst SELECT timestamp, value FROM timeSeriesSelector($TS, 'probe_once_foo', 0, 1000)" 2> "$LOG"
echo "probes: $(count_initiator_log_lines "$QUERY_ID" "Probing whether selector matches the whole metric 'probe_once_foo'")"
echo "reuses: $(count_initiator_log_lines "$QUERY_ID" "Reusing the whole-metric probe verdict for metric 'probe_once_foo': true")"

echo "-- INSERT SELECT of a partial selector: one probe, the second interpretation reuses the verdict"
QUERY_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_partial"
$CLIENT_DEBUG_LOG $PARALLEL_REPLICAS --query_id="$QUERY_ID" -q "INSERT INTO ts_probe_once_dst SELECT timestamp, value FROM timeSeriesSelector($TS, 'probe_once_bar{env=\"prod\"}', 0, 1000)" 2> "$LOG"
echo "probes: $(count_initiator_log_lines "$QUERY_ID" "Probing whether selector matches the whole metric 'probe_once_bar'")"
echo "reuses: $(count_initiator_log_lines "$QUERY_ID" "Reusing the whole-metric probe verdict for metric 'probe_once_bar': false")"

echo "-- plain SELECT: one probe, nothing to reuse"
QUERY_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_select"
$CLIENT_DEBUG_LOG --enable_parallel_replicas=0 --query_id="$QUERY_ID" -q "SELECT count() FROM timeSeriesSelector($TS, 'probe_once_foo', 0, 1000) FORMAT Null" 2> "$LOG"
echo "probes: $(count_initiator_log_lines "$QUERY_ID" "Probing whether selector matches the whole metric 'probe_once_foo'")"
echo "reuses: $(count_initiator_log_lines "$QUERY_ID" "Reusing the whole-metric probe verdict for metric 'probe_once_foo'")"

echo "-- the inserted rows are the rows of the selectors"
$CLIENT --enable_parallel_replicas=0 -q "
SELECT DISTINCT toUnixTimestamp64Milli(timestamp), value FROM ts_probe_once_dst ORDER BY value;
SELECT 'same as plain SELECT', uniqExact(timestamp, value) = 4 AND max(value) = 100 FROM ts_probe_once_dst;
SELECT 'same as plain SELECT', uniqExact(timestamp, value) = 4 AND max(value) = 100 FROM (
    SELECT timestamp, value FROM timeSeriesSelector($TS, 'probe_once_foo', 0, 1000)
    UNION ALL
    SELECT timestamp, value FROM timeSeriesSelector($TS, 'probe_once_bar{env=\"prod\"}', 0, 1000));

DROP TABLE ts_probe_once_dst SYNC;
DROP TABLE ts_probe_once;
"
