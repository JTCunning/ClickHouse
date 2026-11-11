#!/usr/bin/env bash
# One cold run with the real-time sampling profiler; prints the distinct stacks of the threads loading a primary index.
set -euo pipefail
D=/root/nerve-workspace/tools/clickhouse/data/tags-in-subquery-cold
CH="/root/nerve-workspace/tools/clickhouse/master/clickhouse client --port 9301"
echo "${LATENCY:-50}" > $D/s3root/.latency_ms
for c in "FILESYSTEM CACHE" "UNCOMPRESSED CACHE" "TEXT INDEX CACHES" "QUERY CONDITION CACHE" "PAGE CACHE" "INDEX UNCOMPRESSED CACHE" "MARK CACHE" "PRIMARY INDEX CACHE" "INDEX MARK CACHE" "STATISTICS CACHE"; do $CH -q "SYSTEM DROP $c"; done
$CH -q "SYSTEM UNLOAD PRIMARY KEY"
QID=stacks-$(date +%s)
$CH --query_id "$QID" --max_threads 16 --max_threads_min_free_memory_per_thread 0 --query_profiler_real_time_period_ns 5000000 \
  --query_profiler_cpu_time_period_ns 0 -q "$(cat $D/query.sql) FORMAT Null"
$CH -q "SYSTEM FLUSH LOGS"
$CH --allow_introspection_functions 1 -q "
SELECT count() AS samples, arrayStringConcat(arraySlice(arrayFilter(s -> s NOT LIKE '%std::%' AND s NOT LIKE '%Poco::%', arrayMap(a -> demangle(addressToSymbol(a)), trace)), 1, 14), '\n  ') AS stack
FROM system.trace_log
WHERE query_id = '$QID' AND trace_type = 'Real'
  AND arrayExists(a -> demangle(addressToSymbol(a)) LIKE '%IMergeTreeDataPart::loadIndex%', trace)
GROUP BY stack ORDER BY samples DESC LIMIT 4 FORMAT Vertical"
