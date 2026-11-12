#!/usr/bin/env bash
# Synthetic TimeSeries table for handoff #17 (time ranges read for a non-literal __name__ regex).
# 400 metric names x 5,000 series = 2,000,000 series with one sample inside the window [T0, T0 + 3600 s).
# 3 of the names contain "cpu": the regex .*cpu.* matches 15,000 of the series in the window.
# 100,000 more series (20 names, 2 of them with "cpu", one of those shared with the window) have their only
# sample one day before the window: a wrong time filter shows up as extra rows.
# Usage: gen.sh <clickhouse binary> <tcp port>   (a server started with server-config.xml)
set -euo pipefail
CH=${1:?clickhouse binary}; PORT=${2:-19000}
T0=1700000000
q() { "$CH" client --port "$PORT" --max_memory_usage=2500000000 -q "$1"; }
q "DROP TABLE IF EXISTS ts SYNC"
q "CREATE TABLE ts ENGINE = TimeSeries"
name_expr="multiIf(n = 7, 'node_cpu_seconds_total', n = 123, 'process_cpu_seconds_total', n = 311, 'container_cpu_usage_seconds_total', concat('metric_', toString(n), '_total'))"
for chunk in 0 1 2 3 4 5 6 7; do
  # 250,000 series per chunk: names chunk*50 .. chunk*50+49
  q "INSERT INTO ts (metric_name, tags, samples)
  SELECT $name_expr AS metric_name,
         map('job', concat('job-', toString(intDiv(s, 500))), 'instance', concat('host-', toString(s % 500)), 'region', concat('r', toString(s % 7))) AS tags,
         [(toDateTime64($T0 + (number % 3600), 3), toFloat64(number))] AS samples
  FROM (SELECT number, toUInt32(intDiv(number, 5000)) AS n, toUInt32(number % 5000) AS s
        FROM numbers($chunk * 250000, 250000))"
done
q "INSERT INTO ts (metric_name, tags, samples)
SELECT multiIf(n = 3, 'old_cpu_seconds_total', n = 4, 'node_cpu_seconds_total', concat('old_', toString(n), '_total')) AS metric_name,
       map('job', concat('job-old-', toString(intDiv(s, 500))), 'instance', concat('host-', toString(s % 500)), 'region', 'r0') AS tags,
       [(toDateTime64($T0 - 86400 + (number % 3600), 3), 1.)] AS samples
FROM (SELECT number, toUInt32(intDiv(number, 5000)) AS n, toUInt32(number % 5000) AS s FROM numbers(100000))"
q "OPTIMIZE TABLE ts FINAL" || true
q "SELECT replaceRegexpOne(table, '^.inner_id.([a-z_]+).*', '\\\\1') AS t, count() AS parts, sum(rows) AS rows, sum(marks) AS marks, formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts WHERE database = currentDatabase() AND active GROUP BY table ORDER BY table FORMAT PrettyCompactMonoBlock"
