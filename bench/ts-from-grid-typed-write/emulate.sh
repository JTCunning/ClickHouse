#!/usr/bin/env bash
# emulate.sh: interleaved rounds of queries.tsv over fg_dense and fg_null on one server.
# Env: CH (binary), PORT (default 19101), ROUNDS (default 11), MAX_THREADS (default 1), TAG (log_comment prefix).
# Arm order rotates with the round (perf-lab §7). A runs twice per round (A and AA = same text) for the A/A.
set -euo pipefail
CH=${CH:-/root/nerve-workspace/tools/clickhouse/master/clickhouse}; PORT=${PORT:-19101}
ROUNDS=${ROUNDS:-11}; MT=${MAX_THREADS:-1}; TAG=${TAG:-fg}; DIR=$(cd "$(dirname "$0")" && pwd)
mapfile -t LINES < <(cat "$DIR/queries.tsv"; printf 'AA\t%s\n' "$(awk -F'\t' '$1=="A"{print $2}' "$DIR/queries.tsv")")
N=${#LINES[@]}
for r in $(seq 1 "$ROUNDS"); do
  for T in fg_dense fg_null; do
    for k in $(seq 0 $((N - 1))); do
      line=${LINES[$(( (r + k) % N ))]}; label=${line%%$'\t'*}; sql=${line#*$'\t'}; sql=${sql//\{T\}/$T}
      "$CH" client --port "$PORT" --max_threads "$MT" --use_query_condition_cache 0 \
        --log_comment "$TAG:$label:$T:$r" -q "$sql" < /dev/null
    done
  done
done
"$CH" client --port "$PORT" -q "SYSTEM FLUSH LOGS" < /dev/null
"$CH" client --port "$PORT" -q "
SELECT splitByChar(':', log_comment)[3] AS tbl, splitByChar(':', log_comment)[2] AS q, count() AS n,
  round(quantileExact(0.5)(dateDiff('microsecond', query_start_time_microseconds, event_time_microseconds)) / 1000, 2) AS med_ms,
  round(min(dateDiff('microsecond', query_start_time_microseconds, event_time_microseconds)) / 1000, 2) AS min_ms,
  round(max(dateDiff('microsecond', query_start_time_microseconds, event_time_microseconds)) / 1000, 2) AS max_ms,
  round(quantileExact(0.5)(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1000, 2) AS cpu_med_ms
FROM system.query_log WHERE type = 'QueryFinish' AND startsWith(log_comment, '$TAG:') AND toInt32(splitByChar(':', log_comment)[4]) > 1
GROUP BY tbl, q ORDER BY tbl, q FORMAT PrettyCompactMonoBlock" < /dev/null
echo "(round 1 discarded as warm-up)"
