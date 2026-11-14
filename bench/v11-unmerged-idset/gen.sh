#!/usr/bin/env bash
# A version 11 TimeSeries table whose tags and time ranges tables are not merged yet: every live series has one tags row
# and one time ranges row per insert, in REPEATS separate parts per inner table (merges stopped, deduplication cache off).
# metric_0: SERIES live series (labels job = i % 10, instance = intDiv(i, 10), region = i % 7, pod = i), one sample per insert
#           in [T0, T0 + REPEATS * 60 s); plus STALE series written once with their only sample a day earlier (outside the window).
# metric_1..metric_<OTHER_METRICS>: OTHER_SERIES live series each, written by the same inserts (the primary key skips them).
# Usage: gen.sh <clickhouse binary> [tcp port]   (a server started with server-config.xml)
# MERGED_COPY=1 also makes `ts_merged`: the same rows copied into a second table and merged (OPTIMIZE FINAL), one row per id
# in the tags and time ranges tables: the steady state after merges, as a guard.
# Knobs (environment): SERIES=60000 REPEATS=30 STALE=5000 OTHER_METRICS=9 OTHER_SERIES=5000 STOP_MERGES=1 MERGED_COPY=1
set -euo pipefail
CH=${1:?clickhouse binary}; PORT=${2:-19100}
SERIES=${SERIES:-60000}; REPEATS=${REPEATS:-30}; STALE=${STALE:-5000}
OTHER_METRICS=${OTHER_METRICS:-9}; OTHER_SERIES=${OTHER_SERIES:-5000}; STOP_MERGES=${STOP_MERGES:-1}; MERGED_COPY=${MERGED_COPY:-1}
T0=1700000000
q() { "$CH" client --port "$PORT" --max_memory_usage=2000000000 "$@"; }
q -q "DROP TABLE IF EXISTS ts SYNC"
q -q "CREATE TABLE ts ENGINE = TimeSeries SETTINGS version = 11, tags_deduplication_cache_size_bytes = 0"
if [[ $STOP_MERGES == 1 ]]; then
  for t in $(q -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%'"); do
    q -q "SYSTEM STOP MERGES \`$t\`"
  done
fi
TOTAL=$(( SERIES + OTHER_METRICS * OTHER_SERIES ))
labels="map('job', concat('job', toString(i % 10)), 'instance', concat('host-', toString(intDiv(i, 10))), 'region', concat('r', toString(i % 7)), 'pod', concat('pod-', toString(i)))"
# The stale series of metric_0 (i >= SERIES): one insert, one sample a day before the window.
q --max_insert_threads=1 -q "INSERT INTO ts (metric_name, tags, samples)
SELECT 'metric_0', $labels, [(toDateTime64($T0 - 86400, 3), 1.)] FROM (SELECT number + $SERIES AS i FROM numbers($STALE))"
for ((r = 0; r < REPEATS; r++)); do
  q --max_insert_threads=1 --min_insert_block_size_rows=0 --min_insert_block_size_bytes=0 --max_block_size=$TOTAL -q "INSERT INTO ts (metric_name, tags, samples)
  SELECT concat('metric_', toString(m)), $labels, [(toDateTime64($T0 + $r * 60 + (i % 60), 3), toFloat64($r))]
  FROM (SELECT if(number < $SERIES, 0, intDiv(number - $SERIES, $OTHER_SERIES) + 1) AS m,
               if(number < $SERIES, number, (number - $SERIES) % $OTHER_SERIES) AS i
        FROM numbers($TOTAL))"
done
if [[ $MERGED_COPY == 1 ]]; then
  q -q "DROP TABLE IF EXISTS ts_merged SYNC"
  q -q "CREATE TABLE ts_merged ENGINE = TimeSeries SETTINGS version = 11, tags_deduplication_cache_size_bytes = 0"
  src=$(q -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 'ts'")
  dst=$(q -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 'ts_merged'")
  for kind in tags timeranges samples; do
    q -q "INSERT INTO \`.inner_id.$kind.$dst\` SELECT * FROM \`.inner_id.$kind.$src\`"
    q -q "OPTIMIZE TABLE \`.inner_id.$kind.$dst\` FINAL"
  done
fi
q -q "SELECT replaceRegexpOne(table, '^.inner_id.([a-z_]+).*', '\\\\1') AS t, count() AS parts, sum(rows) AS rows, sum(marks) AS marks, formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts WHERE database = currentDatabase() AND active AND table NOT LIKE '%recentsamples%' GROUP BY table ORDER BY table FORMAT PrettyCompactMonoBlock"
