#!/usr/bin/env python3
"""Handoff #14, attribution pass: per-processor elapsed time (system.processors_profile_log) and CPU samples
(system.trace_log, 1 ms CPU profiler) for the ratio query at 16 streams. Run after bench.py on the same server.
Usage: python3 profile.py [runs] > profile.txt
"""
import subprocess
import sys
import uuid

sys.path.insert(0, ".")
from bench import CH, PORT, RATIO, FULL  # noqa: E402

RUNS = int(sys.argv[1]) if len(sys.argv) > 1 else 5
SETTINGS = ("max_threads = 16, max_threads_min_free_memory_per_thread = 0, log_processors_profiles = 1, "
            "query_profiler_cpu_time_period_ns = 1000000, query_profiler_real_time_period_ns = 0")


def ch(q, *extra):
    return subprocess.run([CH, "client", "--port", PORT, *extra, "-q", q], check=True, capture_output=True, text=True).stdout


qids = []
ch(f"SELECT count() FROM prometheusQueryRange('bench', $${RATIO}$$, {FULL[0]}, {FULL[1]}, {FULL[2]}) SETTINGS {SETTINGS}")
for i in range(RUNS):
    qid = f"jfc-prof-{uuid.uuid4().hex[:8]}"
    ch(f"SELECT count() FROM prometheusQueryRange('bench', $${RATIO}$$, {FULL[0]}, {FULL[1]}, {FULL[2]}) SETTINGS {SETTINGS}",
       "--query_id", qid)
    qids.append(qid)
ch("SYSTEM FLUSH LOGS")
ids = ", ".join(f"'{q}'" for q in qids)

print("## Per-processor elapsed (sum over a query's processors of that name; median over runs), ms")
print(ch(f"""
SELECT name, median(n) AS processors, round(median(el) / 1000, 2) AS elapsed_ms, round(median(inw) / 1000, 2) AS input_wait_ms,
       round(median(el) / sum(median(el)) OVER () * 100, 2) AS pct_of_elapsed
FROM (SELECT query_id, name, count() AS n, sum(elapsed_us) AS el, sum(input_wait_elapsed_us) AS inw
      FROM system.processors_profile_log WHERE query_id IN ({ids}) GROUP BY query_id, name)
GROUP BY name ORDER BY elapsed_ms DESC FORMAT PrettyCompactMonoBlock""", "--allow_introspection_functions", "1"))

print("## CPU samples (1 ms CPU profiler, all runs): share of samples whose stack contains a frame matching each pattern")
print(ch(f"""
WITH arrayMap(x -> demangle(addressToSymbol(x)), trace) AS st
SELECT count() AS samples,
       round(countIf(arrayExists(f -> f ILIKE '%Join%', st)) / count() * 100, 2) AS pct_any_join_frame,
       round(countIf(arrayExists(f -> f ILIKE '%HashJoin::addBlockToJoin%' OR f ILIKE '%FillingRightJoinSide%', st)) / count() * 100, 2) AS pct_build,
       round(countIf(arrayExists(f -> f ILIKE '%JoiningTransform%' OR f ILIKE '%joinBlock%', st)) / count() * 100, 2) AS pct_probe,
       round(countIf(arrayExists(f -> f ILIKE '%MergeTreeSelect%' OR f ILIKE '%MergeTreeReader%' OR f ILIKE '%Compression%' OR f ILIKE '%ZSTD%', st)) / count() * 100, 2) AS pct_read,
       round(countIf(arrayExists(f -> f ILIKE '%AggregatingTransform%' OR f ILIKE '%Aggregator%', st)) / count() * 100, 2) AS pct_aggregate,
       round(countIf(arrayExists(f -> f ILIKE '%InterpreterSelectQueryAnalyzer%' OR f ILIKE '%QueryPlan::optimize%' OR f ILIKE '%buildQueryPipeline%', st)) / count() * 100, 2) AS pct_plan
FROM system.trace_log WHERE query_id IN ({ids}) AND trace_type = 'CPU' FORMAT Vertical""", "--allow_introspection_functions", "1"))

print("## Top 25 leaf functions (CPU samples, all runs)")
print(ch(f"""
SELECT count() AS samples, round(count() / sum(count()) OVER () * 100, 2) AS pct, demangle(addressToSymbol(trace[1])) AS leaf
FROM system.trace_log WHERE query_id IN ({ids}) AND trace_type = 'CPU'
GROUP BY leaf ORDER BY samples DESC LIMIT 25 FORMAT TSV""", "--allow_introspection_functions", "1"))
