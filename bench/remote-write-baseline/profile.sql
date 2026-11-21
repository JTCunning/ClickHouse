-- Shares of server CPU by part of the remote-write path, from the CPU profiler (research/ab-plans/remote-write-baseline.md, P cases).
-- Start the server with PROFILE_EXTRA='<query_profiler_cpu_time_period_ns>2000000</query_profiler_cpu_time_period_ns>'
-- (the profile reaches merges too), note T0 = now() before the pass, then:
--   clickhouse client --port 19001 --allow_introspection_functions=1 --param_t0="$T0" --queries-file profile.sql
-- A sample counts in every row whose pattern its stack matches, as in the 2026-11-01 report's table.
SYSTEM FLUSH LOGS;
WITH arrayMap(a -> demangle(addressToSymbol(a)), trace) AS st
SELECT
    count() AS samples,
    round(countIf(query_id LIKE '%::%') / samples, 3) AS merges,  -- background merges run with query_id '<table uuid>::<part>'
    round(countIf(arrayExists(s -> s LIKE '%CompressionCodecALP%', st)) / samples, 3) AS alp,
    round(countIf(arrayExists(s -> s LIKE '%ColumnLowCardinality%' OR s LIKE '%ReverseIndex%', st)) / samples, 3) AS low_cardinality,
    round(countIf(arrayExists(s -> s LIKE '%MergeFromImpl%' OR s LIKE '%~WriteRequest%' OR s LIKE '%ThreadSafeArena%', st)) / samples, 3) AS protobuf,
    round(countIf(arrayExists(s -> s LIKE '%ZSTD%', st)) / samples, 3) AS zstd,
    round(countIf(arrayExists(s -> s LIKE '%AsynchronousInsertQueue%', st)) / samples, 3) AS async_flush_total,  -- the whole flush: the insert pipeline runs under the queue's frames (not the report's 5% queue self-time)
    round(countIf(arrayExists(s -> s LIKE '%sipHash%', st)) / samples, 3) AS siphash,
    round(countIf(arrayExists(s -> s LIKE '%TimeSeriesDeduplicationCache%', st)) / samples, 3) AS dedup_cache
FROM system.trace_log
WHERE trace_type = 'CPU' AND event_time >= {t0:DateTime}
FORMAT Vertical;
