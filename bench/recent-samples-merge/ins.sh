#!/usr/bin/env bash
# Insert cost through the TimeSeries sink: for each layout L, `CREATE TABLE ins_L AS L` (same inner definitions, empty),
# then interleaved inserts of SERIES series of metric x (the labels of gen.sh) with timestamps just before now:
#   one: BATCHES inserts of 1 sample per series (a remote-write request of SERIES samples)
#   ten: BATCHES inserts of 40 samples per series (10 minutes of 15 s scrapes)
# The first insert of each table (which writes the tags) is discarded. Reports medians of query_duration_ms and
# OSCPUVirtualTimeMicroseconds from system.query_log, and the bytes the inserts wrote (system.parts of ins_L).
# Usage: ins.sh <tag> <layout,...>   Env: CH, PORT=19310, SERIES=2000, BATCHES=15, OUT
set -euo pipefail
CH=${CH:-/root/nerve-workspace/tools/clickhouse/master/clickhouse}
PORT=${PORT:-19310}; SERIES=${SERIES:-2000}; BATCHES=${BATCHES:-15}
DIR=$(cd "$(dirname "$0")" && pwd); OUT=${OUT:-$DIR/results}; mkdir -p "$OUT"
TAG=${1:?tag}; IFS=, read -r -a LS <<< "${2:?layouts}"
q() { "$CH" client --port "$PORT" --database rsm --max_memory_usage=2000000000 "$@" < /dev/null; }
for L in "${LS[@]}"; do q -q "DROP TABLE IF EXISTS ins_$L SYNC"; q -q "CREATE TABLE ins_$L AS $L"; done
NOW=$(( $(date +%s) / 60 * 60 ))
SQL=$OUT/$TAG-ins.sql; : > "$SQL"
row() { # row <first sample index> <samples per series>: SELECT of SERIES rows
  echo "SELECT 'x', map('job', concat('job-', toString(number % 10)), 'instance', concat('host-', toString(intDiv(number, 4))),
               'shard', toString(number % 50), 'pod', concat('pod-', toString(number))),
         arrayMap(k -> (fromUnixTimestamp64Milli(toInt64($NOW - 14400 + ($1 + k) * 15) * 1000 + toInt64(sipHash64(number, 'off') % 14960), 'UTC'),
                        toFloat64(($1 + k) * (1 + number % 7))), range($2))
  FROM numbers($SERIES)"
}
NL=${#LS[@]}
for ((b = 0; b <= BATCHES; b++)); do
  for kind in one ten; do
    for ((k = 0; k < NL; k++)); do
      L=${LS[$(( (b + k) % NL ))]}
      if [[ $kind == one ]]; then first=$b; n=1; else first=$((BATCHES + 1 + b * 40)); n=40; fi
      echo "INSERT INTO ins_$L (metric_name, tags, samples) SETTINGS max_threads = 2, max_insert_threads = 1, log_comment = 'rsm-ins|$TAG|$L|$kind|$b' $(row $first $n);" >> "$SQL"
    done
  done
done
"$CH" client --port "$PORT" --database rsm -n --max_memory_usage=2000000000 < "$SQL"
q -q "SYSTEM FLUSH LOGS"
q -q "SELECT splitByChar('|', log_comment)[3] AS layout, splitByChar('|', log_comment)[4] AS kind, count() AS n,
         median(query_duration_ms) AS ms, round(median(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1e3, 1) AS cpu_ms,
         round(median(ProfileEvents['InsertedBytes']) / 1e6, 2) AS inserted_MB, median(ProfileEvents['InsertedRows']) AS inserted_rows,
         median(ProfileEvents['MergeTreeDataWriterCompressedBytes']) AS written_compressed
  FROM system.query_log WHERE type = 'QueryFinish' AND log_comment LIKE 'rsm-ins|$TAG|%' AND toUInt32(splitByChar('|', log_comment)[5]) > 0
    AND event_date >= yesterday()
  GROUP BY layout, kind ORDER BY kind, layout FORMAT PrettyCompactMonoBlock"
for L in "${LS[@]}"; do
  q -q "SELECT '$L' AS layout, replaceRegexpOne(table, '^.inner_id.([a-z]+).*', '\\\\1') AS kind, count() AS parts, sum(rows) AS rows,
               sum(bytes_on_disk) AS bytes
        FROM system.parts WHERE database = 'rsm' AND active
          AND table LIKE concat('.inner_id.%samples.', (SELECT toString(uuid) FROM system.tables WHERE database = 'rsm' AND name = 'ins_$L'))
        GROUP BY kind ORDER BY kind FORMAT TSV"
done
