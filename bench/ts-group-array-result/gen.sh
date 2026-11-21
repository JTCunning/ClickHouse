#!/usr/bin/env bash
# A default (latest version, bucketed samples) TimeSeries table `ts` whose samples and recent samples tables hold ROUNDS
# unmerged parts: each round is one INSERT of SERIES series x PER_ROUND samples (15 s apart), so every (id, bucket) row
# appears once per part and a merge of the parts runs timeSeriesGroupArray over SERIES x ROUNDS x PER_ROUND samples.
# The window ends at the start of the current hour (so the recent samples table keeps its rows; nothing is fixed in time).
# Usage: gen.sh [tcp port]   (a server from ../lc-uuid-id/ab/server.sh)
# Knobs: SERIES=20000 ROUNDS=8 PER_ROUND=30 CH=<binary> T0=<epoch s, default: the start of the previous hour>
set -euo pipefail
CH=${CH:-/root/nerve-workspace/tools/clickhouse/master/clickhouse}; PORT=${1:-19201}
SERIES=${SERIES:-20000}; ROUNDS=${ROUNDS:-8}; PER_ROUND=${PER_ROUND:-30}
q() { "$CH" client --port "$PORT" "$@" </dev/null; }
q -q "DROP TABLE IF EXISTS ts SYNC"
q -q "CREATE TABLE ts ENGINE = TimeSeries SETTINGS tags_deduplication_cache_size_bytes = 0"
for t in $(q -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND (name LIKE '.inner_id.samples.%' OR name LIKE '.inner_id.recentsamples.%')"); do
  q -q "SYSTEM STOP MERGES \`$t\`"
done
T0=${T0:-$(q -q "SELECT toUnixTimestamp(toStartOfHour(now())) - 3600")}
echo "t0=$T0"
labels="map('job', concat('job', toString(i % 10)), 'instance', concat('host-', toString(intDiv(i, 10))), 'pod', concat('pod-', toString(i)))"
for ((r = 0; r < ROUNDS; r++)); do
  q --max_insert_threads=1 --min_insert_block_size_rows=0 --min_insert_block_size_bytes=0 --max_block_size=$SERIES -q "INSERT INTO ts (metric_name, tags, samples)
  SELECT 'm', $labels, arrayMap(k -> (toDateTime64($T0 + ($r * $PER_ROUND + k) * 15, 3), if(i % 2 = 0, toFloat64((i % 1000) * 1000 + ($r * $PER_ROUND + k) * (1 + i % 7)), round(sipHash64(i, $r, k) % 100000 / 1000., 3))), range($PER_ROUND))
  FROM (SELECT number AS i FROM numbers($SERIES))"
done
q -q "SELECT replaceRegexpOne(table, '^.inner_id.([a-z_]+).*', '\\\\1') AS t, count() AS parts, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts WHERE database = currentDatabase() AND active GROUP BY table ORDER BY table FORMAT TSV" 
