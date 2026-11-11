-- The primary key probe deciding whether to load statistics (see 05407_statistics_skip_small_reads) runs over the
-- parts in parallel when their primary indexes are not loaded yet. It decides as with one thread.

SET enable_analyzer = 1, optimize_move_to_prewhere = 1;
SET enable_parallel_replicas = 0;
SET use_query_cache = 0, use_query_condition_cache = 0;
SET materialize_statistics_on_insert = 1;
SET use_statistics = 1, use_statistics_for_part_pruning = 1;
SET min_marks_per_part_to_load_statistics = 100, max_threads_for_indexes = 0;

DROP TABLE IF EXISTS statistics_probe;

CREATE TABLE statistics_probe (key UInt64, value UInt64, flag UInt8)
ENGINE = MergeTree ORDER BY key PARTITION BY intDiv(key, 10000)
SETTINGS index_granularity = 16, index_granularity_bytes = '10Mi', auto_statistics_types = 'basic, uniq_v2',
    primary_key_lazy_load = 1, use_primary_key_cache = 0, prewarm_primary_key_cache = 0, prewarm_statistics_cache = 0;

SYSTEM STOP MERGES statistics_probe;

-- Eight parts of 625 marks each, `value` of the part `i` is in [i * 10000, i * 10000 + 9999].
INSERT INTO statistics_probe SELECT number, number, number % 2 FROM numbers(80000) SETTINGS max_partitions_per_insert_block = 8;

-- Each query runs after a reattach, which unloads the primary indexes and the statistics of the parts:
-- the probe loads the indexes, and with `max_threads` above one it probes the parts in parallel.

SELECT '-- the primary key leaves two marks of two parts: no statistics';
DETACH TABLE statistics_probe;
ATTACH TABLE statistics_probe;
SELECT count() FROM statistics_probe WHERE key IN (5, 75000) AND value >= 30000 AND flag = 0
SETTINGS max_threads = 4, log_comment = '05620_small_parallel';
DETACH TABLE statistics_probe;
ATTACH TABLE statistics_probe;
SELECT count() FROM statistics_probe WHERE key IN (5, 75000) AND value >= 30000 AND flag = 0
SETTINGS max_threads = 1, log_comment = '05620_small_serial';

SELECT '-- a large read: statistics are loaded and prune six parts';
DETACH TABLE statistics_probe;
ATTACH TABLE statistics_probe;
SELECT count() FROM statistics_probe WHERE key >= 1 AND value >= 60000 AND flag = 0
SETTINGS max_threads = 4, log_comment = '05620_large_parallel';
DETACH TABLE statistics_probe;
ATTACH TABLE statistics_probe;
SELECT count() FROM statistics_probe WHERE key >= 1 AND value >= 60000 AND flag = 0
SETTINGS max_threads = 1, log_comment = '05620_large_serial';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['LoadedStatistics'] > 0, ProfileEvents['SelectedParts']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND startsWith(log_comment, '05620_')
ORDER BY log_comment;

DROP TABLE statistics_probe;
