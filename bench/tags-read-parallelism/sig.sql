-- Per (shape_maxthreads) cell of one sweep.py run: each variant's median wall vs the current setting (fsm_rio1),
-- and the two-sided Mann-Whitney p-value. Significant = ratio outside 0.95-1.05 AND p < 0.005 (99.5%, as clickhouse benchmark --confidence 5).
-- Usage: clickhouse client --port 19000 --param_run=<run_id printed in sweep.py's log_comment prefix rNNN> --queries-file sig.sql
WITH extract(log_comment, '^r\\d+_(\\d+_\\d+)_') AS cell, extract(log_comment, '_([a-z]+_rio[01])#') AS v
SELECT cell, alt,
       round(medianIf(query_duration_ms, v = alt) / medianIf(query_duration_ms, v = 'fsm_rio1'), 3) AS wall_ratio,
       round(tupleElement(mannWhitneyUTest('two-sided')(query_duration_ms, v = alt), 2), 5) AS p,
       round(maxIf(memory_usage, v = alt) / maxIf(memory_usage, v = 'fsm_rio1'), 2) AS memory_ratio
FROM system.query_log ARRAY JOIN ['fsm_rio0', 'hash_rio1', 'hash_rio0', 'phash_rio0'] AS alt
WHERE type = 'QueryFinish' AND log_comment LIKE concat('r', {run:String}, '\\_%') AND log_comment NOT LIKE '%#0' AND v IN ('fsm_rio1', alt)
GROUP BY cell, alt
ORDER BY cell, alt
FORMAT PrettyCompactMonoBlock
