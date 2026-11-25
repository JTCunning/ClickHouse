#!/usr/bin/env bash

# An INSERT into a TimeSeries table needs the INSERT privilege on the target tables it writes. The privilege is checked
# before the INSERT runs or is queued: synchronous with and without rows, asynchronous with the data sent as blocks over
# the native protocol, with and without waiting for the flush. An INSERT of only the metric family columns needs it only
# on the metric families table.

# The expected ACCESS_DENIED errors are logged by the server at the Error level.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user_05876_${CLICKHOUSE_DATABASE}"
SETTINGS="--enable_time_series_table 1 --session_timezone UTC"

$CLICKHOUSE_CLIENT $SETTINGS -q "
    DROP TABLE IF EXISTS ts;
    DROP USER IF EXISTS $user;
    CREATE TABLE ts ENGINE = TimeSeries;
    CREATE USER $user IDENTIFIED WITH no_password;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.ts TO $user;
"

USER_CLIENT="$CLICKHOUSE_CLIENT $SETTINGS --user $user"

# Runs a client command with its data on stdin. Prints OK when it succeeds with nothing on stderr,
# ACCESS_DENIED when it fails with that error, and the error otherwise.
check()
{
    local err
    if err=$("$@" 2>&1 >/dev/null); then
        if [ -z "$err" ]; then echo "OK"; else echo "OK with stderr: $err"; fi
    elif [[ "$err" == *ACCESS_DENIED* ]]; then
        echo "ACCESS_DENIED"
    else
        echo "FAILED: $err"
    fi
}

# Every case writes its own series (or metric family), named after the stage and the case.
run_all()
{
    local s=$1
    echo -n "synchronous, rows: "
    check $USER_CLIENT --async_insert 0 -q "INSERT INTO ts (metric_name, tags, samples) VALUES ('${s}_sync', map('host', 'h'), [(toDateTime64(100, 3), 1)])" < /dev/null
    echo -n "synchronous, no rows: "
    check $USER_CLIENT --async_insert 0 -q "INSERT INTO ts (metric_name, tags, samples) SELECT '${s}_empty', map('host', 'h'), [] FROM numbers(1) WHERE number > 1" < /dev/null
    for wait in 1 0; do
        echo -n "asynchronous, native blocks, wait $wait: "
        printf "${s}_async_wait_${wait}\t{'host':'h'}\t[('1970-01-01 00:01:41.000',2)]\n" \
            | check $USER_CLIENT --async_insert 1 --wait_for_async_insert $wait -q "INSERT INTO ts (metric_name, tags, samples) FORMAT TSV"
    done
    echo -n "metric family columns only: "
    check $USER_CLIENT --async_insert 0 -q "INSERT INTO ts (metric_family, type, unit, help) VALUES ('${s}_family', 'counter', '', 'help')" < /dev/null

    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH ASYNC INSERT QUEUE"
    $CLICKHOUSE_CLIENT $SETTINGS -q "
        SELECT 'series', arraySort(groupArray(metric_name)) FROM timeSeriesTags(ts) FINAL WHERE startsWith(metric_name, '${s}_');
        SELECT 'samples', count() FROM timeSeriesSamples(ts) FINAL WHERE id IN (SELECT id FROM timeSeriesTags(ts) WHERE startsWith(metric_name, '${s}_'));
        SELECT 'metric families', count() FROM timeSeriesMetricFamilies(ts) FINAL WHERE startsWith(metric_family, '${s}_');
    "
}

inner_table()
{
    $CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.$1.%'"
}

echo "-- INSERT on the TimeSeries table only"
run_all s1

echo "-- and on the tags and metric families tables"
$CLICKHOUSE_CLIENT -q "
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.\`$(inner_table tags)\` TO $user;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.\`$(inner_table metricfamilies)\` TO $user;
"
run_all s2

echo "-- on all tables of the database"
$CLICKHOUSE_CLIENT -q "GRANT INSERT ON ${CLICKHOUSE_DATABASE}.* TO $user"
run_all s3

# The privilege is checked on the columns the sink writes: without the column `id` of the tags table, also the
# asynchronous INSERT without waiting must fail before it is queued.
echo "-- on the other inner tables, and on the columns metric_name and tags of the tags table"
$CLICKHOUSE_CLIENT -q "
    REVOKE INSERT ON ${CLICKHOUSE_DATABASE}.* FROM $user;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.ts TO $user;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.\`$(inner_table samples)\` TO $user;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.\`$(inner_table recentsamples)\` TO $user;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.\`$(inner_table timeranges)\` TO $user;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.\`$(inner_table metricfamilies)\` TO $user;
    GRANT INSERT(metric_name, tags) ON ${CLICKHOUSE_DATABASE}.\`$(inner_table tags)\` TO $user;
"
run_all s4

echo "-- and on the column id of the tags table"
$CLICKHOUSE_CLIENT -q "GRANT INSERT(id) ON ${CLICKHOUSE_DATABASE}.\`$(inner_table tags)\` TO $user"
run_all s5

$CLICKHOUSE_CLIENT -q "DROP TABLE ts; DROP USER $user"
