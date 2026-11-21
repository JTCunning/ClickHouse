#!/usr/bin/env bash
# OPTIMIZE FINAL of the samples (or recent samples) table of `ts` (from gen.sh); prints the merge's wall time and CPU, and
# where its CPU profiler samples go. Start the server with the profiler in the default profile, so merges are sampled too:
#   PROFILE_EXTRA='<query_profiler_cpu_time_period_ns>1000000</query_profiler_cpu_time_period_ns><query_profiler_real_time_period_ns>0</query_profiler_real_time_period_ns>'
# Usage: merge_profile.sh [tcp port] [samples|recentsamples]
set -euo pipefail
CH=${CH:-/root/nerve-workspace/tools/clickhouse/master/clickhouse}; PORT=${1:-19201}; KIND=${2:-samples}
q() { "$CH" client --port "$PORT" "$@" </dev/null; }
t=$(q -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.$KIND.%'")
uuid=$(q -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = '$t'")
qid=merge-$KIND-$(date +%s%N)
start=$(q -q "SELECT now64(6)")
q -q "SYSTEM START MERGES \`$t\`"
q --query_id "$qid" -q "OPTIMIZE TABLE \`$t\` FINAL"
q -q "SYSTEM FLUSH LOGS"
q -q "SELECT 'optimize', query_duration_ms, round(ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1000) AS cpu_ms FROM system.query_log WHERE query_id = '$qid' AND type = 'QueryFinish' FORMAT TSV"
filter="(query_id = '$qid' OR query_id LIKE '$uuid::%') AND event_time_microseconds >= '$start' AND trace_type = 'CPU'"
q --allow_introspection_functions=1 -q "
WITH arrayMap(x -> demangle(addressToSymbol(x)), trace) AS syms
SELECT count() AS samples_1ms,
       round(100 * countIf(arrayExists(s -> s LIKE '%TimeSeriesGroupArray%insertResultInto%', syms)) / count(), 1) AS insertResultInto_pct,
       round(100 * countIf(arrayExists(s -> s LIKE '%TimeSeriesGroupArray%::add%' OR s LIKE '%TimeSeriesGroupArray%addBatch%', syms)) / count(), 1) AS add_pct,
       round(100 * countIf(arrayExists(s -> s LIKE '%ompress%', syms)) / count(), 1) AS codec_pct
FROM system.trace_log WHERE $filter FORMAT TSVWithNames"
q --allow_introspection_functions=1 -q "
SELECT count() AS c, substring(demangle(addressToSymbol(trace[1])), 1, 140) AS leaf FROM system.trace_log WHERE $filter
GROUP BY leaf ORDER BY c DESC LIMIT 12 FORMAT TSV"
