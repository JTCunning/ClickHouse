#!/usr/bin/env bash
# empty_insert.sh TCP_PORT [RUNS]: the fixed cost of an INSERT whose sink gets no rows, per target table set.
# Interleaves three zero-row INSERTs RUNS times (default 40) and prints the median and min-max of the server CPU and
# the duration of each, from system.query_log:
#   ts_samples  INSERT INTO a TimeSeries table (metric_name, tags, samples): master builds 4 inner INSERTs (tags,
#               time ranges, samples, recent samples), the branch none
#   ts_families INSERT of the metric family columns only: master builds 1 inner INSERT, the branch none
#   mergetree   the same zero-row INSERT into a plain MergeTree table: the control, no inner INSERTs on either side
# Run it on A and on B with the same binary-independent setup; the per-query difference ts_samples - mergetree is
# what the branch removes from every remote-write request with async insert.
set -euo pipefail
PORT=$1 RUNS=${2:-40}
C="${CH:-/root/nerve-workspace/tools/clickhouse/master/clickhouse} client --port $PORT"
$C -n -q "
    CREATE DATABASE IF NOT EXISTS sink_lazy;
    CREATE TABLE IF NOT EXISTS sink_lazy.ts ENGINE = TimeSeries SETTINGS tags_deduplication_cache_size_bytes = 1073741824;
    CREATE TABLE IF NOT EXISTS sink_lazy.mt (metric_name String, tags Map(String, String), samples Array(Tuple(DateTime64(3), Float64))) ENGINE = MergeTree ORDER BY metric_name;
" < /dev/null
TAG="sink_lazy_$(date +%s%N)"
declare -A Q=(
  [ts_samples]="INSERT INTO sink_lazy.ts (metric_name, tags, samples) SELECT 'm', map('a', toString(number)), [(toDateTime64(number, 3), 1.0)] FROM numbers(10) WHERE number > 100"
  [ts_families]="INSERT INTO sink_lazy.ts (metric_family, type, unit, help) SELECT 'm', 'gauge', '', '' FROM numbers(10) WHERE number > 100"
  [mergetree]="INSERT INTO sink_lazy.mt (metric_name, tags, samples) SELECT 'm', map('a', toString(number)), [(toDateTime64(number, 3), 1.0)] FROM numbers(10) WHERE number > 100"
)
ORDER=(ts_samples ts_families mergetree)
for r in $(seq 0 $((RUNS - 1))); do
  for k in 0 1 2; do
    name=${ORDER[$(( (r + k) % 3 ))]}   # rotate the order with the run
    $C --async_insert 0 --log_comment "$TAG:$name" -q "${Q[$name]}" < /dev/null
  done
done
$C -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT splitByChar(':', log_comment)[2] AS case, count() AS runs,
        quantileExact(0.5)(ProfileEvents['OSCPUVirtualTimeMicroseconds']) AS cpu_us_median,
        min(ProfileEvents['OSCPUVirtualTimeMicroseconds']) AS cpu_us_min, max(ProfileEvents['OSCPUVirtualTimeMicroseconds']) AS cpu_us_max,
        quantileExact(0.5)(query_duration_ms) AS ms_median
    FROM system.query_log
    WHERE type = 'QueryFinish' AND startsWith(log_comment, '$TAG:') AND event_date >= yesterday()
    GROUP BY case ORDER BY case
    FORMAT PrettyCompactMonoBlock
" < /dev/null
