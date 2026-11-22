#!/usr/bin/env bash
# promql.sh: interleaved rounds of the PromQL range queries in promql_cases.tsv (the table of
# tests/performance/promql_set_operator_presence_mask.xml, 100..3090 step 10 = 300 steps).
# Env: CH, PORT (19101), ROUNDS (7), MAX_THREADS (1), TAG, PROF (CPU profiler period in ns, 0 = off).
# Prints per case: wall and CPU, the summed elapsed_us of FilterTransform (the final
# `WHERE notEmpty(samples)` step computes timeSeriesFromGrid) and, with PROF, the share of CPU samples
# inside FunctionTimeSeriesRange and inside its IColumn::insert(Field) calls.
set -euo pipefail
CH=${CH:-/root/nerve-workspace/tools/clickhouse/master/clickhouse}; PORT=${PORT:-19101}
ROUNDS=${ROUNDS:-7}; MT=${MAX_THREADS:-1}; TAG=${TAG:-pq}; PROF=${PROF:-0}; DIR=$(cd "$(dirname "$0")" && pwd)
mapfile -t LINES < "$DIR/promql_cases.tsv"; N=${#LINES[@]}
for r in $(seq 1 "$ROUNDS"); do
  for k in $(seq 0 $((N - 1))); do
    line=${LINES[$(( (r + k) % N ))]}; label=${line%%$'\t'*}; expr=${line#*$'\t'}
    "$CH" client --port "$PORT" --max_threads "$MT" --use_query_condition_cache 0 --log_processors_profiles 1 \
      --query_profiler_cpu_time_period_ns "$PROF" --query_profiler_real_time_period_ns 0 \
      --log_comment "$TAG:$label:$r" \
      -q "SELECT * FROM prometheusQueryRange('promql_set_operator_presence_mask_ts', '$expr', 100, 3090, 10) FORMAT Null" < /dev/null
  done
done
"$CH" client --port "$PORT" -q "SYSTEM FLUSH LOGS" < /dev/null
"$CH" client --port "$PORT" --allow_introspection_functions 1 -q "
WITH q AS (SELECT query_id, splitByChar(':', log_comment)[2] AS label,
             dateDiff('microsecond', query_start_time_microseconds, event_time_microseconds) AS wall_us,
             ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS cpu_us
           FROM system.query_log WHERE type = 'QueryFinish' AND startsWith(log_comment, '$TAG:')
             AND toInt32(splitByChar(':', log_comment)[3]) > 1),
f AS (SELECT query_id, sum(elapsed_us) AS filter_us FROM system.processors_profile_log
      WHERE query_id IN (SELECT query_id FROM q) AND name = 'FilterTransform' GROUP BY query_id),
t AS (SELECT query_id, count() AS samples,
        countIf(arrayExists(s -> s LIKE '%FunctionTimeSeriesRange%', st)) AS in_fn,
        countIf(arrayExists(s -> s LIKE '%FunctionTimeSeriesRange%', st) AND arrayExists(s -> s LIKE '%::insert(DB::Field const&)%', st)) AS in_field
      FROM (SELECT query_id, arrayMap(a -> demangle(addressToSymbol(a)), trace) AS st FROM system.trace_log
            WHERE trace_type = 'CPU' AND query_id IN (SELECT query_id FROM q)) GROUP BY query_id)
SELECT label, count() AS n, round(median(wall_us) / 1000, 1) AS wall_med_ms, round(min(wall_us) / 1000, 1) AS wall_min,
  round(max(wall_us) / 1000, 1) AS wall_max, round(median(cpu_us) / 1000, 1) AS cpu_med_ms,
  round(median(filter_us) / 1000, 1) AS filter_med_ms, sum(samples) AS prof_samples,
  round(100 * sum(in_fn) / greatest(sum(samples), 1), 1) AS fn_pct, round(100 * sum(in_field) / greatest(sum(samples), 1), 1) AS field_pct
FROM q LEFT JOIN f USING query_id LEFT JOIN t USING query_id GROUP BY label ORDER BY label
FORMAT PrettyCompactMonoBlock" < /dev/null
echo "(round 1 discarded as warm-up; max_threads=$MT)"
