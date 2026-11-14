#!/usr/bin/env bash
# Handoff #19, idea 4: the same rows in a different number of parts. Makes a version 11 TimeSeries table `g_p<P>_c<C>` whose inner tags
# and time ranges tables hold C copies of every row of `ts_merged` (one row per id, see gen.sh), spread over P parts at random
# (row (id, copy) goes to part cityHash64(id, copy) % P, so every part spans the whole id range, as an insert of all series does).
# The parts are written straight into the inner tables with optimize_on_insert = 0 (the copies stay) and merges stopped.
# Usage: gen-grid.sh <clickhouse binary> <port> <P> <C>     (needs ts_merged from gen.sh)
set -euo pipefail
CH=${1:?clickhouse binary}; PORT=${2:?port}; P=${3:?parts}; C=${4:?copies}
q() { "$CH" client --port "$PORT" --max_memory_usage=2000000000 "$@" </dev/null; }
T=g_p${P}_c${C}
q -q "DROP TABLE IF EXISTS $T SYNC"
q -q "CREATE TABLE $T ENGINE = TimeSeries SETTINGS version = 11, tags_deduplication_cache_size_bytes = 0"
src=$(q -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 'ts_merged'")
dst=$(q -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = '$T'")
for kind in tags timeranges; do
  q -q "SYSTEM STOP MERGES \`.inner_id.$kind.$dst\`"
  cols=$([[ $kind == tags ]] && echo "id, metric_name, tags" || echo "id, min_time, max_time")
  for ((k = 0; k < P; k++)); do
    q --optimize_on_insert=0 --max_insert_threads=1 --max_threads=1 --max_block_size=4000000 --max_insert_block_size=4000000 \
      --min_insert_block_size_rows=4000000 --min_insert_block_size_bytes=0 -q "INSERT INTO \`.inner_id.$kind.$dst\` ($cols)
      SELECT $cols FROM \`.inner_id.$kind.$src\` ARRAY JOIN range($C) AS c WHERE cityHash64(id, c) % $P = $k"
  done
done
q -q "SELECT '$T', replaceRegexpOne(table, '^.inner_id.([a-z_]+).*', '\\\\1') AS t, count() AS parts, sum(rows) AS rows, sum(marks) AS marks
FROM system.parts WHERE database = currentDatabase() AND active AND table LIKE '%$dst' AND (table LIKE '%tags%' OR table LIKE '%timeranges%')
GROUP BY table ORDER BY table FORMAT TSV"
