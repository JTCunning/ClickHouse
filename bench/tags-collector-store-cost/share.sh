#!/usr/bin/env bash
# How much of a dashboard-shaped PromQL range query the per-query tags collector costs, on ONE server (no branch needed).
# Gate for the tags-collector-store-cost A/B plan: a 2x cut of the collector can save at most half of the share printed here.
# Data: ../range-json-timestamps/gen.sh (counter m and gauge g, labels job/instance/pod, 15 s scrape), window.txt from it.
# Each case runs once to warm up, then RUNS times with the CPU profiler at 1 ms; prints per case the median wall and CPU,
# the trace_log samples, and the share of them in: teardown (~ContextTimeSeriesTagsCollector), store
# (timeSeriesStoreTags / storeTags / tryAddGroup), other collector code (id/group -> tags lookups for the output).
# The query is `SELECT * FROM prometheusQueryRange(...) FORMAT Null` (instant cases: prometheusQuery): no JSON writing, so the share is a little higher
# than through /api/v1/query_range or /api/v1/query.
# Usage: share.sh [tcp port]   Env: CH=<binary> RUNS=5 THREADS=2 CASES="R1 R2 R3 R4 R5 R6 I1 I2 I3 I4" TAG=smoke WINDOW=./window.txt
set -euo pipefail
CH=${CH:-/root/nerve-workspace/tools/clickhouse/master/clickhouse}; PORT=${1:-19601}
RUNS=${RUNS:-5}; THREADS=${THREADS:-2}; TAG=${TAG:-smoke}
read -r T_START T_END < "${WINDOW:-window.txt}"
q() { "$CH" client --port "$PORT" "$@" </dev/null; }
HOUR=3600
# The six dashboard queries of ts-from-grid-typed-write run 2 (realistic/run.py): label | PromQL | range s | step s.
declare -A Q=(
  [R1]='rate(m{job="job3"}[5m])|3600|15'
  [R2]='rate(m[5m])|3600|15'
  [R3]='sum by (job) (rate(m[5m]))|3600|15'
  [R4]='g{job="job3"}|3600|15'
  [R5]='g|3600|15'
  [R6]='rate(m[5m])|21600|30'
# Rule evaluation: alerting and recording rules run instant queries (/api/v1/query) every 15 s to 1 min.
  [I1]='sum by (job) (rate(m[5m]))|0|0'
  [I2]='rate(m[5m])|0|0'
  [I3]='sum(rate(m[5m])) > 0|0|0'
  [I4]='g|0|0'
)
SET="max_threads=$THREADS, log_queries=1, query_profiler_cpu_time_period_ns=1000000, query_profiler_real_time_period_ns=0"
for c in ${CASES:-R1 R2 R3 R4 R5 R6 I1 I2 I3 I4}; do
  IFS='|' read -r promql span step <<< "${Q[$c]}"
  if (( span == 0 )); then  # instant query at T_END
    sql="SELECT * FROM prometheusQuery(ts, '${promql}', $T_END) FORMAT Null"
  else
    start=$(( T_END - span )); (( start < T_START )) && start=$T_START
    sql="SELECT * FROM prometheusQueryRange(ts, '${promql}', $start, $T_END, $step) FORMAT Null"
  fi
  q --log_comment="tcs-$TAG-$c-warm" -q "$sql SETTINGS $SET"
  for i in $(seq "$RUNS"); do q --log_comment="tcs-$TAG-$c-$i" -q "$sql SETTINGS $SET"; done
  echo "$c	$promql	range ${span}s step ${step}s"
done
q -q "SYSTEM FLUSH LOGS"
q --allow_introspection_functions=1 -q "
WITH runs AS (
    SELECT query_id, extract(log_comment, '^tcs-$TAG-([RI][0-9]+)-') AS c, query_duration_ms AS ms,
           ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1000 AS cpu_ms, read_rows, memory_usage
    FROM system.query_log
    WHERE type = 'QueryFinish' AND match(log_comment, '^tcs-$TAG-[RI][0-9]+-[0-9]+\$') AND event_date >= today() - 1),
samples AS (
    SELECT query_id, arrayStringConcat(arrayMap(x -> demangle(addressToSymbol(x)), trace), ' ') AS s
    FROM system.trace_log WHERE trace_type = 'CPU' AND query_id IN (SELECT query_id FROM runs) AND event_date >= today() - 1),
buckets AS (
    SELECT query_id,
           multiIf(position(s, '~ContextTimeSeriesTagsCollector') > 0, 'teardown',
                   position(s, 'TimeSeriesStoreTags') > 0 OR position(s, 'storeTags') > 0 OR position(s, 'tryAddGroup') > 0, 'store',
                   position(s, 'ContextTimeSeriesTagsCollector') > 0, 'collector_other', 'rest') AS b
    FROM samples)
SELECT r.c AS case, count() AS runs, median(r.ms) AS wall_ms, round(median(r.cpu_ms)) AS cpu_ms, any(r.read_rows) AS read_rows,
       formatReadableSize(median(r.memory_usage)) AS mem,
       sum(t.n) AS trace_samples,
       round(100 * sum(t.teardown) / sum(t.n), 1) AS teardown_pct, round(100 * sum(t.store) / sum(t.n), 1) AS store_pct,
       round(100 * sum(t.other) / sum(t.n), 1) AS collector_other_pct,
       round(100 * (sum(t.teardown) + sum(t.store) + sum(t.other)) / sum(t.n), 1) AS collector_pct
FROM runs AS r
LEFT JOIN (SELECT query_id, count() AS n, countIf(b = 'teardown') AS teardown, countIf(b = 'store') AS store,
                  countIf(b = 'collector_other') AS other FROM buckets GROUP BY query_id) AS t USING (query_id)
GROUP BY case ORDER BY case
FORMAT PrettyCompactMonoBlock"
