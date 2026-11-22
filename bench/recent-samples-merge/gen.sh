#!/usr/bin/env bash
# Builds the lab of reports/2026-11-22-recent-samples-merge.md on a running server (`server.sh start lab 19310`).
# Every layout is a TimeSeries table in database `rsm` whose inner tables are filled directly with SQL (no remote write):
# the same SERIES series of metric `x`, one counter sample every 15 s per series, DAYS days ending at T_END.
# A counter: value (j % P) * M + noise in [0, M) for sample j (strictly increasing, noisy increments, a reset every P samples;
# M per series in 1..1000, P in 7..30 h). A first version with periodic increments compressed unrealistically well in long runs.
#
#   m     master defaults (version 11): samples = month partitions, 1 h buckets, 256 KiB granules;
#         recent samples = 5 h partitions, 15 min buckets, 128 KiB granules, 4 d TTL (filled with what it holds in steady state)
#   p5    samples only: 5 h partitions, 1 h buckets, 256 KiB          (decomposition: partition alone)
#   p5b   samples only: 5 h partitions, 15 min buckets, 256 KiB       (+ bucket)
#   p5bg  samples only: 5 h partitions, 15 min buckets, 128 KiB       (+ granule bytes = the recent layout with 14 d, candidate L1-5h)
#   mb    samples only: month partitions, 15 min buckets, 256 KiB     (bucket alone)
#   pd    samples only: day partitions, 1 h buckets, 256 KiB          (candidate L3)
#   pdbg  samples only: day partitions, 15 min buckets, 128 KiB       (candidate L1-day)
#   l2    samples only: day partitions, 1 h buckets + `sub` (15 min slice index) in ORDER BY (id, bucket, sub),
#         TTL GROUP BY id, bucket folds the slices of rows older than 4 d + 1 h into one 1 h row (candidate L2)
#   l2p5  as l2 with 5 h partitions (candidate L2-5h)
#
# Usage: gen.sh [layout ...]   (default: all of the above). Env: CH, PORT=19310, SERIES=2000, DAYS=14,
#        DATA (t_end is written to $DATA/t_end on the first run and reused, so every layout holds the same samples).
# The data must end near now: the selector reads the recent samples table only if the range starts inside its TTL window.
set -euo pipefail
CH=${CH:-/root/nerve-workspace/tools/clickhouse/master/clickhouse}
PORT=${PORT:-19310}; SERIES=${SERIES:-2000}; DAYS=${DAYS:-14}
DATA=${DATA:-/root/nerve-workspace/tools/clickhouse/data/recent-samples-merge}
q() { "$CH" client --port "$PORT" --database rsm --max_memory_usage=2000000000 --max_threads=2 "$@" < /dev/null; }
"$CH" client --port "$PORT" -q "CREATE DATABASE IF NOT EXISTS rsm" < /dev/null
if [[ ! -f $DATA/t_end ]]; then echo $(( $(date +%s) / 60 * 60 )) > "$DATA/t_end"; fi
T_END=$(cat "$DATA/t_end"); T0=$(( (T_END - DAYS * 86400) / 3600 * 3600 ))
RECENT_TTL=345600
# the recent samples table in steady state: everything from the start of the 5 h partition holding T_END - TTL - 15 min
R0=$(( (T_END - RECENT_TTL - 900) / 18000 * 18000 ))
P5="toStartOfInterval(toDateTime(bucket, 'UTC'), toIntervalHour(5))"
PDAY="toDate(toDateTime(bucket, 'UTC'))"
P5Q=${P5//\'/\\\'}; PDAYQ=${PDAY//\'/\\\'}  # quoted inside a setting string
COLS="id Tuple(UInt64, LowCardinality(UUID)),
  samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(3), value Float64))),
  bucket DateTime64(3) CODEC(Delta, T64, ZSTD(3)),
  sub UInt8 DEFAULT intDiv(toUnixTimestamp(min_time) % 3600, 900),
  min_time SimpleAggregateFunction(min, DateTime64(3)) CODEC(Delta, T64, ZSTD(3)),
  max_time SimpleAggregateFunction(max, DateTime64(3)) CODEC(Delta, T64, ZSTD(3)),
  CODEC samples.timestamp (Delta, T64, ZSTD(3)), CODEC samples.value (ALP, ZSTD(4))"
FOLD="TTL toDateTime(bucket) + toIntervalSecond($((RECENT_TTL + 3600))) GROUP BY id, bucket
  SET sub = min(sub), samples = timeSeriesGroupArray(samples), min_time = min(min_time), max_time = max(max_time)"

# the pinned settings are the defaults at 521c4bcb (pinned, so a later default change doesn't move the layout)
ddl() {
  local s="version = 11, samples_bucket_step_seconds = 3600, samples_index_granularity = 256"
  local r="recent_samples_ttl_seconds = 0"
  case $1 in
    m)    echo "SETTINGS $s, samples_index_granularity_bytes = 262144, recent_samples_ttl_seconds = $RECENT_TTL,
            recent_samples_bucket_step_seconds = 900, recent_samples_index_granularity = 256, recent_samples_index_granularity_bytes = 131072" ;;
    p5)   echo "SETTINGS $s, $r, samples_index_granularity_bytes = 262144, samples_partition_by = '$P5Q'" ;;
    p5b)  echo "SETTINGS ${s/3600/900}, $r, samples_index_granularity_bytes = 262144, samples_partition_by = '$P5Q'" ;;
    p5bg) echo "SETTINGS ${s/3600/900}, $r, samples_index_granularity_bytes = 131072, samples_partition_by = '$P5Q'" ;;
    mb)   echo "SETTINGS ${s/3600/900}, $r, samples_index_granularity_bytes = 262144" ;;
    pd)   echo "SETTINGS $s, $r, samples_index_granularity_bytes = 262144, samples_partition_by = '$PDAYQ'" ;;
    pdbg) echo "SETTINGS ${s/3600/900}, $r, samples_index_granularity_bytes = 131072, samples_partition_by = '$PDAYQ'" ;;
    l2)   echo "SETTINGS version = 11, samples_bucket_step_seconds = 3600, $r SAMPLES INNER COLUMNS ($COLS)
            SAMPLES INNER ENGINE = AggregatingMergeTree PARTITION BY $PDAY ORDER BY (id, bucket, sub) $FOLD
            SETTINGS index_granularity = 256, index_granularity_bytes = 131072" ;;
    l2p5) echo "SETTINGS version = 11, samples_bucket_step_seconds = 3600, $r SAMPLES INNER COLUMNS ($COLS)
            SAMPLES INNER ENGINE = AggregatingMergeTree PARTITION BY $P5 ORDER BY (id, bucket, sub) $FOLD
            SETTINGS index_granularity = 256, index_granularity_bytes = 131072" ;;
    *) echo "unknown layout $1" >&2; exit 1 ;;
  esac
}
inner() { # inner <layout> <kind>
  local u; u=$(q -q "SELECT uuid FROM system.tables WHERE database = 'rsm' AND name = '$1'")
  echo "\`.inner_id.$2.$u\`"
}
# fill <table> <from> <to> <slice step> <hour buckets with sub 0|1>: one row per (series, slice) with the slice's samples
fill() {
  local t=$1 from=$2 to=$3 step=$4 sub=$5 tags=$6 bexpr="lo" cols="id, bucket, samples, min_time, max_time" extra=""
  if [[ $sub == 1 ]]; then bexpr="intDiv(lo, 3600) * 3600"; cols="$cols, sub"; extra=", toUInt8(intDiv(lo % 3600, 900))"; fi
  local d
  for ((d = from; d < to; d += 86400)); do  # one INSERT per day: few partitions per block, few parts per partition
    local e=$(( d + 86400 < to ? d + 86400 : to ))
    q --max_insert_threads=1 --min_insert_block_size_rows=4000000 --min_insert_block_size_bytes=400000000 --max_block_size=4096 -q "
      INSERT INTO $t ($cols)
      SELECT id, toDateTime64(b, 3), s, s[1].1, s[-1].1 $extra
      FROM (
        SELECT id, lo, $bexpr AS b,
               arrayFilter(x -> x.1 >= toDateTime64(lo, 3, 'UTC') AND x.1 < toDateTime64(least(lo + $step, $T_END), 3, 'UTC'),
                 arrayMap(j -> (fromUnixTimestamp64Milli(toInt64(j) * 15000 + off + toInt64(sipHash64(i, j) % 40), 'UTC'),
                                toFloat64((j % P) * M + sipHash64(i, j, 'n') % M)),
                          range(toUInt64(intDiv(lo, 15) - 1), toUInt64(intDiv(lo + $step, 15) + 1)))) AS s
        FROM (SELECT id, toUInt32(substring(tags['pod'], 5)) AS i, toInt64(sipHash64(i, 'off') % 14960) AS off,
                     (7 + sipHash64(i, 'p') % 24) * 240 AS P, 1 + sipHash64(i, 'm') % 1000 AS M FROM $tags)
        ARRAY JOIN range($d, $e, $step) AS lo)
      WHERE notEmpty(s)"
  done
}
make_layout() {
  local L=$1
  q -q "DROP TABLE IF EXISTS $L SYNC"
  q -q "CREATE TABLE $L ENGINE = TimeSeries $(ddl "$L")"
  local tags; tags=$(inner "$L" tags)
  q -q "INSERT INTO $tags (metric_name, tags)
        SELECT 'x', map('job', concat('job-', toString(number % 10)), 'instance', concat('host-', toString(intDiv(number, 4))),
                        'shard', toString(number % 50), 'pod', concat('pod-', toString(number)))
        FROM numbers($SERIES)"
  q -q "INSERT INTO $(inner "$L" timeranges) (id, min_time, max_time)
        SELECT id, toDateTime64($T0, 3, 'UTC'), toDateTime64($T_END - 1, 3, 'UTC') FROM $tags"
  local step=3600 sub=0
  case $L in p5b|p5bg|mb|pdbg) step=900 ;; l2|l2p5) step=900; sub=1 ;; esac
  local t0=$SECONDS
  fill "$(inner "$L" samples)" $T0 $T_END $step $sub "$tags"
  q -q "OPTIMIZE TABLE $(inner "$L" samples) FINAL"
  if [[ $L == m ]]; then
    fill "$(inner m recentsamples)" $R0 $T_END 900 0 "$tags"
    q -q "OPTIMIZE TABLE $(inner m recentsamples) FINAL"
  fi
  q -q "OPTIMIZE TABLE $tags FINAL"
  echo "$L filled in $((SECONDS - t0)) s"
}
LAYOUTS=("$@"); [[ ${#LAYOUTS[@]} -gt 0 ]] || LAYOUTS=(m p5 p5b p5bg mb pd pdbg l2 l2p5)
for L in "${LAYOUTS[@]}"; do make_layout "$L"; done
echo "T0=$T0 R0=$R0 T_END=$T_END series=$SERIES"
q -q "SELECT t.name AS layout, replaceRegexpOne(p.table, '^.inner_id.([a-z]+).*', '\\\\1') AS kind, count() AS parts,
             uniqExact(p.partition) AS partitions, sum(p.rows) AS rows, sum(p.marks) AS marks,
             sum(p.bytes_on_disk) AS bytes, formatReadableSize(sum(p.bytes_on_disk)) AS size, sum(p.primary_key_bytes_in_memory) AS pk_bytes
      FROM system.parts AS p JOIN system.tables AS t ON p.table LIKE concat('%', toString(t.uuid))
      WHERE p.database = 'rsm' AND p.active AND t.database = 'rsm' AND t.engine = 'TimeSeries'
        AND (p.table LIKE '.inner_id.samples.%' OR p.table LIKE '.inner_id.recentsamples.%')
      GROUP BY layout, kind ORDER BY layout, kind FORMAT PrettyCompactMonoBlock"
