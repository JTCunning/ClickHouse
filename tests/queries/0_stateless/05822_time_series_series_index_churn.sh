#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
# Tag no-parallel-replicas: a query with parallel replicas reads the tags table instead of the series index.

# The series index of a `TimeSeries` table drops the time series which got no samples for some time,
# and a query returns the same time series as without the index: before and after a time series is dropped,
# and after it returns with its tags or without them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --session_timezone UTC"

# select_series <min_time> <max_time> <log_comment>: the tags of the selected time series, resolved by the index and checked against the tables.
select_series()
{
    $CLIENT --time_series_use_series_index 1 --time_series_check_series_index 1 --log_comment "$3" -q "
        SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, 'm', $1, $2, 'timestamp_and_value') ORDER BY tags"
}

# hits_and_misses <log_comment>
hits_and_misses()
{
    $CLIENT -q "SYSTEM FLUSH LOGS query_log"
    $CLIENT -q "
        SELECT sum(ProfileEvents['TimeSeriesSeriesIndexHits']) > 0 AS has_hits, sum(ProfileEvents['TimeSeriesSeriesIndexMisses']) > 0 AS has_misses
        FROM system.query_log
        WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND log_comment = '$1'"
}

# insert_sample <series> <time>
insert_sample()
{
    $CLIENT -q "INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('series', '$1'), [(toDateTime64($2, 3), 1.)])"
}

# The index drops a time series without samples in the last 4 seconds which got no samples for 2 seconds:
# the retention of the recent samples table, and twice the lifetime of the cache of the tags table.
$CLIENT -q "CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 4, tags_deduplication_cache_expiration_seconds = 1"
tags_table=$($CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.tags.%'")
time_ranges_table=$($CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.timeranges.%'")

# The time series which the index must keep have samples in the future, so the test does not depend on how long its steps take.
start=$(date +%s)
future=$((start + 3600))
far_future=$((start + 100000))
insert_sample active "$future"
insert_sample returns_with_tags "$start"
insert_sample returns_without_tags "$start"
insert_sample stopped "$start"

echo '-- all the time series'
select_series $((start - 10)) "$far_future" 05822_all

# The index has dropped the time series which stopped when a query for the time of their last samples reads the tables.
dropped=0
for i in $(seq 1 120); do
    select_series $((start - 10)) "$far_future" "05822_wait_$i" > /dev/null
    if [ "$(hits_and_misses "05822_wait_$i")" == "0	1" ]; then
        dropped=1
        break
    fi
    sleep 0.5
done
echo "the index has dropped the time series which stopped: $dropped"

echo '-- a time range which starts at the last samples of the dropped time series: the tables'
select_series "$start" "$far_future" 05822_old
hits_and_misses 05822_old

echo '-- a later time range: the index'
select_series $((start + 1)) "$far_future" 05822_later
hits_and_misses 05822_later

echo '-- a dropped time series returns after the cache of the tags table has forgotten it, so its tags are written again: the index'
insert_sample returns_with_tags "$future"
select_series $((start + 1)) "$far_future" 05822_with_tags
hits_and_misses 05822_with_tags

echo '-- a dropped time series returns with a time range only, as from a writer which has its tags in the cache: the index reads its tags from the tags table'
$CLIENT -q "
    INSERT INTO \`$time_ranges_table\` (id, min_time, max_time)
    SELECT id, toDateTime64($future, 3), toDateTime64($future, 3) FROM \`$tags_table\` WHERE tags['series'] = 'returns_without_tags'"
select_series $((start + 1)) "$far_future" 05822_without_tags
hits_and_misses 05822_without_tags

echo '-- a time range which ends before the time series returned: the index selects them, because their time ranges start earlier in the table'
select_series $((start + 1)) $((future - 1)) 05822_before_return
hits_and_misses 05822_before_return

echo '-- old samples of a new time series: the index does not take it, and a query for the old time reads the tables'
insert_sample backfilled $((start - 100))
select_series $((start + 1)) "$far_future" 05822_backfill_later
hits_and_misses 05822_backfill_later
select_series $((start - 200)) "$far_future" 05822_backfill_old
hits_and_misses 05822_backfill_old

echo '-- merged data parts with the rows of the dropped time series'
insert_sample active $((future + 1))
$CLIENT -q "OPTIMIZE TABLE \`$tags_table\` FINAL"
$CLIENT -q "OPTIMIZE TABLE \`$time_ranges_table\` FINAL"
select_series $((start + 1)) "$far_future" 05822_merged
hits_and_misses 05822_merged

$CLIENT -q "DROP TABLE ts"
