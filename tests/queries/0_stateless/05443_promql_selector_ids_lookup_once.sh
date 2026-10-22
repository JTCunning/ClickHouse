#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
# Tag no-parallel-replicas: the test counts reads of the tags table in the server log of one query.

# A PromQL query looks up the ids of each distinct selector once and reads the tags table only for that lookup.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n -q "
    SET allow_experimental_time_series_table = 1;
    CREATE TABLE ts ENGINE = TimeSeries;
    INSERT INTO ts (metric_name, tags, samples) VALUES
        ('kube_pod_owner', map('job', 'kube-state-metrics', 'owner_kind', 'ReplicaSet', 'owner_name', 'rs1'),
         [(toDateTime64(100, 3), 1.), (toDateTime64(200, 3), 2.), (toDateTime64(300, 3), 3.)]);
"

function check()
{
    local logs
    logs=$($CLICKHOUSE_CLIENT --send_logs_level=trace -q "SELECT count() FROM prometheusQuery(ts, '$1', 300) FORMAT Null" 2>&1)
    echo "lookups: $(grep -c 'StorageTimeSeriesSelector: Selector {' <<< "$logs"), tags table reads: $(grep -cE '\.inner_id\.tags\..*Selected [0-9]+/[0-9]+ parts by partition key' <<< "$logs") :: $1"
}

check 'kube_pod_owner{owner_kind="ReplicaSet"}'
check 'kube_pod_owner{owner_kind="ReplicaSet"} or label_join(kube_pod_owner{owner_kind="ReplicaSet"}, "workload", "/", "owner_name")'
check 'sum(kube_pod_owner{owner_kind="ReplicaSet"}) + sum(kube_pod_owner{owner_kind="ReplicaSet"} offset 100)'
check 'kube_pod_owner{owner_kind="Job"} or label_join(kube_pod_owner{owner_kind="Job"}, "workload", "/", "owner_name")'

$CLICKHOUSE_CLIENT -q "DROP TABLE ts"
