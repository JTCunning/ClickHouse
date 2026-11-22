#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

# CREATE DATABASE ... ENGINE = Ordinary and ATTACH TABLE with a full definition log warnings, which would reach the stderr of the test.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# CREATE refuses a `samples_bucket_step_seconds` of zero or one that overflows at the scale of the timestamps, but ATTACH with
# a full definition doesn't check the settings. A TimeSeries table attached so is neither written nor read: the INSERT and
# timeSeriesSelector throw INVALID_SETTING_VALUE. An Ordinary database lets the full definition be attached without a UUID.

db="${CLICKHOUSE_DATABASE}_1"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db SYNC"
$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary=1 -q "CREATE DATABASE $db ENGINE = Ordinary"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 -q "CREATE TABLE $db.ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0"

# 10^16 seconds overflow Int64 at the millisecond scale of the default DateTime64(3).
for step in 0 10000000000000000; do
    definition=$($CLICKHOUSE_CLIENT -q "SELECT create_table_query FROM system.tables WHERE database = '$db' AND name = 'ts' FORMAT TSVRaw" \
        | sed -E "s/^CREATE TABLE/ATTACH TABLE/; s/([ ,])samples_bucket_step_seconds = [0-9]+/\1samples_bucket_step_seconds = $step/")

    $CLICKHOUSE_CLIENT -q "DETACH TABLE $db.ts"
    $CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 -q "$definition"
    $CLICKHOUSE_CLIENT -q "SELECT extract(create_table_query, 'samples_bucket_step_seconds = [0-9]+') FROM system.tables WHERE database = '$db' AND name = 'ts'"

    $CLICKHOUSE_CLIENT -q "INSERT INTO $db.ts (metric_name, tags, samples) VALUES ('m', map(), [(toDateTime64(10, 3), 1.)])" 2>&1 \
        | grep -m1 -o -F 'INVALID_SETTING_VALUE'
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM timeSeriesSelector('$db', 'ts', 'm', 0, 1000, 'samples')" 2>&1 \
        | grep -m1 -o -F 'INVALID_SETTING_VALUE'
done

$CLICKHOUSE_CLIENT -q "DROP DATABASE $db SYNC"
