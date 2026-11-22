#!/usr/bin/env bash
# A/B kit for lilred-perf-ts-from-grid-typed-write (see ab-plans/ts-from-grid-typed-write.md).
#   ab.sh gen PORT            create + fill the tables of ../gen.sql on one server, merge the TimeSeries inner tables to one part
#   ab.sh layout PORT...      rows and parts per table; with two ports, exits 1 with "layout differs" if they differ
#   ab.sh hash PORT...        each case's hash query on each server; exits 1 if a hash differs, is empty or counts 0 rows
#   ab.sh bench PA PB         each case (or CASES="T1 T3"): warm-up, then clickhouse benchmark on both servers, -i 2N, --confidence 5
#   ab.sh summary PORT...     median wall and CPU per case and server, from query_log (log_comment ab:<case>)
# Env: CH (client binary, default master), N (runs per server, default 100), OUT (raw output dir, default ./out), CASES.
set -euo pipefail
CH=${CH:-/root/nerve-workspace/tools/clickhouse/master/clickhouse}; N=${N:-100}; DIR=$(cd "$(dirname "$0")" && pwd)
OUT=${OUT:-$DIR/out}; cmd=$1; shift
cases() { rg -v '^#' "$DIR/cases.tsv" | while IFS=$'\t' read -r id role mt q h; do
  if [ -z "${CASES:-}" ] || [[ " $CASES " == *" $id "* ]]; then printf '%s\t%s\t%s\t%s\t%s\n' "$id" "$role" "$mt" "$q" "$h"; fi; done; }
cl() { local port=$1; shift; "$CH" client --port "$port" "$@" < /dev/null; }
case $cmd in
gen)
  "$CH" client --port "$1" --multiquery < "$DIR/../gen.sql"
  for t in $(cl "$1" -q "SELECT name FROM system.tables WHERE database = 'default' AND name LIKE '.inner_id.%'"); do
    cl "$1" -q "OPTIMIZE TABLE default.\`$t\` FINAL"; done ;;
layout)
  for p in "$@"; do cl "$p" -q "SELECT replaceRegexpOne(table, '\\.[0-9a-f-]{36}$', '') AS t, sum(rows), count() FROM system.parts WHERE database = 'default' AND active GROUP BY t ORDER BY t
    UNION ALL SELECT name, total_rows, 0 FROM system.tables WHERE database = 'default' AND engine = 'Memory' ORDER BY 1 FORMAT TSV" > "/tmp/ab-layout-$p.tsv"; echo "== $p"; cat "/tmp/ab-layout-$p.tsv"; done
  if [ $# -eq 2 ]; then cmp -s "/tmp/ab-layout-$1.tsv" "/tmp/ab-layout-$2.tsv" || { echo "layout differs" >&2; exit 1; }; echo "layout equal"; fi ;;
hash)
  bad=0
  while IFS=$'\t' read -r id role mt q h; do
    line="$id"; first=""
    for p in "$@"; do r=$(cl "$p" --max_threads "$mt" -q "$h FORMAT TSV" | tr '\t' ' '); line="$line | $p: $r"
      case $r in 0\ *|'') bad=1 ;; esac
      [ -z "$first" ] && first=$r; [ "$r" = "$first" ] || bad=1; done
    echo "$line"
  done < <(cases)
  [ $bad -eq 0 ] && echo "hashes equal" || { echo "HASH MISMATCH or empty result" >&2; exit 1; } ;;
bench)
  PA=$1; PB=$2; mkdir -p "$OUT"
  while IFS=$'\t' read -r id role mt q h; do
    for p in "$PA" "$PB"; do for _ in 1 2 3; do cl "$p" --max_threads "$mt" --log_comment "ab-warmup:$id" -q "$q"; done; done
    echo "== $id ($role, max_threads $mt)"
    echo "$q" | "$CH" benchmark --host 127.0.0.1 --port "$PA" --host 127.0.0.1 --port "$PB" -i $((2 * N)) --confidence 5 \
      --max_threads "$mt" --use_query_condition_cache 0 --log_comment "ab:$id" 2>&1 | tee "$OUT/bench-$id.txt" | tail -n 12
  done < <(cases) ;;
summary)
  for p in "$@"; do cl "$p" -q "SYSTEM FLUSH LOGS"; echo "== $p"; cl "$p" -q "
    SELECT substring(log_comment, 4) AS case, count() AS n,
      round(median(dateDiff('microsecond', query_start_time_microseconds, event_time_microseconds)) / 1000, 1) AS wall_med_ms,
      round(quantile(0.95)(dateDiff('microsecond', query_start_time_microseconds, event_time_microseconds)) / 1000, 1) AS wall_p95_ms,
      round(median(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1000, 1) AS cpu_med_ms, max(memory_usage) AS mem
    FROM system.query_log WHERE type = 'QueryFinish' AND startsWith(log_comment, 'ab:') GROUP BY case ORDER BY case
    FORMAT PrettyCompactMonoBlock"; done ;;
*) echo "unknown command $cmd" >&2; exit 2 ;;
esac
