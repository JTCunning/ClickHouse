#!/usr/bin/env bash
# A default TimeSeries table `ts` with SERIES series of the counter `m` and the gauge `g` (labels job, instance, pod),
# one sample every 15 s over HOURS hours ending at the start of the current minute; the window goes to window.txt
# (t_start t_end, epoch seconds), which ab/run.py reads, so the queries always cover the data.
# Usage: gen.sh [tcp port]   (a server from server.sh)   Knobs: SERIES=2000 HOURS=6 CHURN=1000 HALF=1000 CH=<binary> T_END=<epoch s>
set -euo pipefail
CH=${CH:-/root/nerve-workspace/tools/clickhouse/master/clickhouse}; PORT=${1:-19301}
SERIES=${SERIES:-2000}; HOURS=${HOURS:-6}; CHURN=${CHURN:-1000}; HALF=${HALF:-1000}
q() { "$CH" client --port "$PORT" "$@" </dev/null; }
T_END=${T_END:-$(q -q "SELECT toUnixTimestamp(toStartOfMinute(now()))")}
T_START=$(( T_END - HOURS * 3600 ))
N=$(( HOURS * 240 ))
q -q "DROP TABLE IF EXISTS ts SYNC"
q -q "CREATE TABLE ts ENGINE = TimeSeries"
labels="map('job', concat('job', toString(i % 10)), 'instance', concat('host-', toString(intDiv(i, 10)), '.example.com:9100'), 'pod', concat('pod-', toString(i)))"
q --max_insert_threads=1 -q "INSERT INTO ts (metric_name, tags, samples)
SELECT metric, $labels, arrayMap(k -> (toDateTime64($T_START + k * 15, 3), if(metric = 'm', toFloat64(k * (1 + i % 7)), round(sipHash64(i, k) % 100000 / 1000., 3))), range(1, $N + 1))
FROM (SELECT number AS i FROM numbers($SERIES)) ARRAY JOIN ['m', 'g'] AS metric"
# The skewed shape of a block: `c` has one series over the whole window and CHURN series that cover only its last
# 5% (series that come and go, next to a stable one), all 15 s apart and on the same timestamps as the stable one.
q --max_insert_threads=1 -q "INSERT INTO ts (metric_name, tags, samples)
SELECT 'c', map('pod', concat('churn-', toString(i))), arrayMap(k -> (toDateTime64($T_START + k * 15, 3), toFloat64(k)),
       if(i = 0, range(1, $N + 1), range($N - intDiv($N, 20) + 1, $N + 1)))
FROM (SELECT number AS i FROM numbers($CHURN + 1))"
# Series that skip steps: `h` has one series every 15 s and HALF series every 30 s; at step 15 s with lookback_delta=10s
# the HALF series have a point at every other step of the 15 s one.
q --max_insert_threads=1 -q "INSERT INTO ts (metric_name, tags, samples)
SELECT 'h', map('pod', concat('half-', toString(i))), arrayMap(k -> (toDateTime64($T_START + k * 15, 3), toFloat64(k)),
       if(i = 0, range(1, $N + 1), range(2, $N + 1, 2)))
FROM (SELECT number AS i FROM numbers($HALF + 1))"
q -q "OPTIMIZE TABLE ts FINAL" || true
echo "$T_START $T_END" > window.txt
q -q "SELECT replaceRegexpOne(table, '^.inner_id.([a-z_]+).*', '\\\\1') AS t, count() AS parts, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts WHERE database = currentDatabase() AND active GROUP BY table ORDER BY table FORMAT TSV"
