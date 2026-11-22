#!/usr/bin/env bash
# Times PromQL shapes on the layouts made by gen.sh, server side, from system.query_log.
# Usage: time.sh <tag> <shapes> <arms> [runs]
#   shapes: comma-separated names from the SHAPES table below (or "short", "long", "all")
#   arms:   comma-separated <layout>[:<prefer>][#<name>]; prefer 0 sets time_series_prefer_recent_samples_table = 0
#           (m = master reading its recent samples table, m:0 = master reading its samples table);
#           #<name> makes a second arm of the same query for an A/A check (m,m#aa)
# Env: CH, PORT=19310, DATA, MAX_THREADS=2, EXTRA_SETTINGS (appended to every query's SETTINGS), OUT (default bench dir/results),
#      EVAL_OFFSET=0: evaluate every shape EVAL_OFFSET seconds before t_end (86400 puts every layout's newest partition
#      in the past, i.e. full: the end of each partition's fill cycle)
# Writes $OUT/<tag>.jsonl (one row per timed query) and prints, per arm and shape, the medians of the counters and the
# median (min-max) of query_duration_ms. Each run executes all (shape, arm) pairs in one client session, with the arm
# order rotated by run and shape; one warm-up round (run 0) is discarded. use_query_condition_cache = 0.
# `HASH=1` instead prints, per arm and shape, the row count and an order-independent hash of the result with the values
# rounded to 6 decimals (sum of cityHash64 of each row).
set -euo pipefail
CH=${CH:-/root/nerve-workspace/tools/clickhouse/master/clickhouse}
PORT=${PORT:-19310}; MAX_THREADS=${MAX_THREADS:-2}; EXTRA_SETTINGS=${EXTRA_SETTINGS:-}
DATA=${DATA:-/root/nerve-workspace/tools/clickhouse/data/recent-samples-merge}
DIR=$(cd "$(dirname "$0")" && pwd); OUT=${OUT:-$DIR/results}; mkdir -p "$OUT"
TAG=${1:?tag}; SHAPES_ARG=${2:?shapes}; ARMS_ARG=${3:?arms}; RUNS=${4:-7}
TE=$(( $(cat "$DATA/t_end") - ${EVAL_OFFSET:-0} ))
W='x'; N='x{shard="7"}'   # wide: all series of metric x; narrow: 2% of them (shard has 50 values)
declare -A SHAPES=(
  [i_rate]="i|rate($W[5m])"                        # alert rule, wide
  [i_sum]="i|sum by (job) (rate($W[5m]))"          # recording rule
  [i_rate_n]="i|rate($N[5m])"                      # alert rule, narrow
  [r1h_sum]="r|3600|30|sum by (job) (rate($W[5m]))"  # dashboard panel, 1 h at 30 s
  [r1h_n]="r|3600|30|rate($N[5m])"
  [r6h_sum]="r|21600|60|sum by (job) (rate($W[5m]))"
  [r6h_n]="r|21600|60|rate($N[5m])"
  [r1d_sum]="r|86400|300|sum by (job) (rate($W[5m]))"     # inside the recent samples TTL: master reads the recent table
  [r1d_n]="r|86400|300|rate($N[5m])"
  [r3d_sum]="r|302400|1800|sum by (job) (rate($W[5m]))"   # 3.5 d: still inside the TTL (needs now - t_end < 12 h)
  [r3d_n]="r|302400|1800|rate($N[5m])"
  [r7d_sum]="r|604800|1800|sum by (job) (rate($W[5m]))"
  [r7d_n]="r|604800|1800|rate($N[5m])"
  [r14d_sum]="r|1206000|3600|sum by (job) (rate($W[5m]))"   # 13 d 23 h: the longest range inside the data
  [r14d_n]="r|1206000|3600|rate($N[5m])"
  # the two halves of r7d_* in a tiered layout whose young table holds the last 1 d or 3.5 d (old half: offset 1 d / 3.5 d)
  [y1_sum]="r|86400|1800|sum by (job) (rate($W[5m]))"
  [y1_n]="r|86400|1800|rate($N[5m])"
  [o7_1_sum]="o|518400|1800|86400|sum by (job) (rate($W[5m]))"
  [o7_1_n]="o|518400|1800|86400|rate($N[5m])"
  [o7_35_sum]="o|302400|1800|302400|sum by (job) (rate($W[5m]))"
  [o7_35_n]="o|302400|1800|302400|rate($N[5m])"
)
case $SHAPES_ARG in
  short) SHAPES_ARG=i_rate,i_sum,i_rate_n,r1h_sum,r1h_n,r6h_sum,r6h_n ;;
  mid)   SHAPES_ARG=r1d_sum,r1d_n,r3d_sum,r3d_n ;;
  long)  SHAPES_ARG=r7d_sum,r7d_n,r14d_sum,r14d_n ;;
  tier)  SHAPES_ARG=y1_sum,y1_n,o7_1_sum,o7_1_n,r3d_sum,r3d_n,o7_35_sum,o7_35_n ;;
  all)   SHAPES_ARG=i_rate,i_sum,i_rate_n,r1h_sum,r1h_n,r6h_sum,r6h_n,r1d_sum,r1d_n,r3d_sum,r3d_n,r7d_sum,r7d_n,r14d_sum,r14d_n ;;
esac
IFS=, read -r -a SH <<< "$SHAPES_ARG"; IFS=, read -r -a ARMS <<< "$ARMS_ARG"
src() { # src <shape> <layout>
  local d=${SHAPES[$1]}; IFS='|' read -r kind a b c e <<< "$d"
  local promql=${d##*|}; promql=${promql//\'/\\\'}
  if [[ $kind == i ]]; then echo "prometheusQuery('rsm', '$2', '$promql', $TE)"
  elif [[ $kind == o ]]; then echo "prometheusQueryRange('rsm', '$2', '$promql', $((TE - c - a)), $((TE - c)), $b)"
  else echo "prometheusQueryRange('rsm', '$2', '$promql', $((TE - a)), $TE, $b)"; fi
}
settings() { # settings <arm> <comment>
  local p=${1#*:}; p=${p%%#*}; local s="max_threads = $MAX_THREADS, use_query_condition_cache = 0, log_comment = '$2'"
  [[ $1 == *:* ]] && s="$s, time_series_prefer_recent_samples_table = $p"
  [[ -n $EXTRA_SETTINGS ]] && s="$s, $EXTRA_SETTINGS"
  echo "$s"
}
if [[ ${HASH:-0} == 1 ]]; then
  for s in "${SH[@]}"; do for a in "${ARMS[@]}"; do
    L=${a%%[:#]*}; d=${SHAPES[$s]}
    if [[ $d == i* ]]; then h="sum(cityHash64(toString(tags), timestamp, round(value, 6)))"
    else h="sum(cityHash64(toString(tags), arrayMap(x -> (x.1, round(x.2, 6)), samples)))"; fi
    printf '%s\t%s\t' "$s" "$a"
    "$CH" client --port "$PORT" -q "SELECT count(), $h FROM $(src "$s" "$L") SETTINGS $(settings "$a" "rsm-hash|$TAG") FORMAT TSV" < /dev/null
  done; done
  exit 0
fi
SQL=$OUT/$TAG.sql; : > "$SQL"
NA=${#ARMS[@]}
for ((r = 0; r <= RUNS; r++)); do
  for ((ci = 0; ci < ${#SH[@]}; ci++)); do
    s=${SH[$ci]}
    for ((k = 0; k < NA; k++)); do
      a=${ARMS[$(( (r + ci + k) % NA ))]}; L=${a%%[:#]*}
      echo "SELECT * FROM $(src "$s" "$L") FORMAT Null SETTINGS $(settings "$a" "rsm|$TAG|$a|$s|$r");" >> "$SQL"
    done
  done
done
"$CH" client --port "$PORT" -n --max_memory_usage=2000000000 < "$SQL" > /dev/null
"$CH" client --port "$PORT" -q "SYSTEM FLUSH LOGS" < /dev/null
"$CH" client --port "$PORT" -q "
  SELECT splitByChar('|', log_comment)[3] AS arm, splitByChar('|', log_comment)[4] AS shape,
         toUInt32(splitByChar('|', log_comment)[5]) AS run, query_duration_ms AS ms, read_rows, read_bytes, memory_usage,
         ProfileEvents['SelectedParts'] AS parts, ProfileEvents['SelectedMarks'] AS marks, ProfileEvents['SelectedRows'] AS sel_rows,
         ProfileEvents['ReadCompressedBytes'] AS compressed, ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS cpu_us
  FROM system.query_log WHERE type = 'QueryFinish' AND log_comment LIKE 'rsm|$TAG|%' AND event_date >= yesterday()
  ORDER BY event_time_microseconds FORMAT JSONEachRow" < /dev/null > "$OUT/$TAG.jsonl"
"$CH" local -q "
  SELECT shape, arm, count() AS n, median(parts) AS parts, median(marks) AS marks, median(sel_rows) AS sel_rows,
         round(median(read_bytes) / 1e6, 1) AS read_MB, round(median(compressed) / 1e6, 1) AS cmp_MB,
         round(median(cpu_us) / 1e3, 1) AS cpu_ms, median(ms) AS ms_med, concat(toString(min(ms)), '-', toString(max(ms))) AS ms_range
  FROM file('$OUT/$TAG.jsonl', JSONEachRow) WHERE run > 0
  GROUP BY shape, arm ORDER BY indexOf(splitByChar(',', '$SHAPES_ARG'), shape), indexOf(splitByChar(',', '$ARMS_ARG'), arm)
  FORMAT PrettyCompactMonoBlock" < /dev/null
