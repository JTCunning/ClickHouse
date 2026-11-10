#!/usr/bin/env bash
# Loads the handoff #9 dataset into table `ts` on a running server and merges every inner table to one part.
# SERIES series (the first SERIES/10 are metric_0, the rest spread over metric_1..metric_199), SAMPLES samples 15 s apart ending at T_END.
# Writes t_end (read by gen.py and full.py). Report sizes: SERIES=1000000 SAMPLES=20; the handoff's: SAMPLES=240.
# Usage: SERIES=1000000 SAMPLES=20 [CH=...] [PORT=19000] ./load.sh
set -euo pipefail
CH=${CH:-/root/nerve-workspace/tools/clickhouse/master/clickhouse}; PORT=${PORT:-19000}
SERIES=${SERIES:-1000000}; SAMPLES=${SAMPLES:-20}; M0=$((SERIES / 10))
T_END=$(( $(date +%s) / 60 * 60 )); echo $T_END > t_end; T0=$((T_END - (SAMPLES - 1) * 15))
c() { "$CH" client --port "$PORT" "$@"; }
c --multiquery -q "
CREATE TABLE ts ENGINE = TimeSeries;
INSERT INTO ts (metric_name, tags, samples)
WITH cityHash64(number) AS h
SELECT concat('metric_', toString(if(number < $M0, 0, (number % 199) + 1))) AS metric_name,
    map('job', concat('job', toString(h % 10)), 'instance', concat('host-', toString(intDiv(h, 10) % 500), ':9100'),
        'cluster', concat('c', toString(intDiv(h, 5000) % 5)), 'namespace', concat('ns', toString(intDiv(h, 25000) % 20)),
        'pod', concat('pod-', toString(number)),
        'mode', ['user','system','idle','iowait','nice','irq','softirq','steal'][(intDiv(h, 500000) % 8) + 1]) AS tags,
    arrayMap(i -> (toDateTime64($T0 + i * 15, 3), toFloat64((number % 1000) + i)), range($SAMPLES)) AS samples
FROM numbers($SERIES) SETTINGS max_insert_block_size = 65536;"
for t in $(c -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%'"); do
  c -q "OPTIMIZE TABLE \`$t\` FINAL"
done
c -q "SELECT table, count() AS parts, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS on_disk FROM system.parts WHERE active AND database = currentDatabase() GROUP BY table ORDER BY table FORMAT PrettyCompactMonoBlock"
