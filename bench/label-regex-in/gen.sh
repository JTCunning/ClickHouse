#!/usr/bin/env bash
# Builds the label-regex-in dataset (handoff #7 generator) on a running server, then merges every inner table to one part.
# Usage: SERIES=300000 WIDE=100000 SAMPLES=40 ./gen.sh
#   SERIES  total series (199 narrow metrics share SERIES - WIDE)
#   WIDE    series of the wide metric metric_0
#   SAMPLES samples per series at 15 s, ending at T_END - 600 + 15 * (SAMPLES - 1)
#   GRAN    samples_index_granularity_bytes, pinned (262144 = master default since #131678, 2026-11-16; 524288 before)
# Env: CH (binary), PORT (19000), THREADS (insert threads). T_END comes from ./t_end.
# 2026-11-17 on 2 vCPU: 300k series load 6 s, OPTIMIZE 1 s, 40 MiB on disk.
set -euo pipefail
cd "$(dirname "$0")"
CH=${CH:-/root/nerve-workspace/tools/clickhouse/master/clickhouse}
PORT=${PORT:-19000}; SERIES=${SERIES:-300000}; WIDE=${WIDE:-100000}; SAMPLES=${SAMPLES:-40}; GRAN=${GRAN:-262144}; THREADS=${THREADS:-2}
T_END=$(cat t_end); T0=$((T_END - 600))
c() { "$CH" client --port "$PORT" </dev/null "$@"; }
c -q "DROP TABLE IF EXISTS ts SYNC"
c -q "CREATE TABLE ts ENGINE = TimeSeries SETTINGS samples_index_granularity_bytes = $GRAN"
c -q "
INSERT INTO ts (metric_name, tags, samples)
WITH cityHash64(number) AS h
SELECT concat('metric_', toString(if(number < $WIDE, 0, (number % 199) + 1))) AS metric_name,
    map('job', concat('job', toString(h % 10)),
        'instance', concat('host-', toString(intDiv(h, 10) % 500), ':9100'),
        'cluster', concat('c', toString(intDiv(h, 5000) % 5)),
        'namespace', concat('ns', toString(intDiv(h, 25000) % 20)),
        'pod', concat('pod-', toString(number)),
        'mode', ['user','system','idle','iowait','nice','irq','softirq','steal'][(intDiv(h, 500000) % 8) + 1]) AS tags,
    arrayMap(i -> (toDateTime64($T0 + i * 15, 3), toFloat64((number % 1000) + i)), range($SAMPLES)) AS samples
FROM numbers($SERIES) SETTINGS max_insert_block_size = 65536, max_threads = $THREADS"
for t in $(c -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%'"); do
  c -q "OPTIMIZE TABLE \`$t\` FINAL"
done
c -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.tags.%'" > tags_table
c -q "SELECT table, count() AS parts, sum(rows), formatReadableSize(sum(bytes_on_disk)) FROM system.parts
      WHERE active AND database = currentDatabase() GROUP BY table ORDER BY table FORMAT TSV"
