-- Since version 12 of the TimeSeries table engine every MergeTree-family inner table
-- gets `prewarm_mark_cache = 1` by default. The TimeSeries setting `prewarm_mark_cache`
-- turns that off, and an explicit INNER ENGINE SETTINGS clause overrides it.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_off;
DROP TABLE IF EXISTS ts_override;

SELECT '-- default: every MergeTree-family inner table has prewarm_mark_cache = 1';
CREATE TABLE ts ENGINE = TimeSeries;
SELECT extract(name, '^\.inner_id\.(\w+)\.') AS inner_table,
       extract(engine_full, 'prewarm_mark_cache = (\\d+)') AS prewarm_mark_cache
FROM system.tables
WHERE database = currentDatabase() AND name LIKE '.inner\\_id.%'
ORDER BY inner_table;
DROP TABLE ts;

SELECT '-- SETTINGS prewarm_mark_cache = 0 leaves the setting off the inner engines';
CREATE TABLE ts_off ENGINE = TimeSeries SETTINGS prewarm_mark_cache = 0;
SELECT extract(name, '^\.inner_id\.(\w+)\.') AS inner_table,
       position(engine_full, 'prewarm_mark_cache') > 0 AS has_prewarm_mark_cache
FROM system.tables
WHERE database = currentDatabase() AND name LIKE '.inner\\_id.%'
ORDER BY inner_table;
DROP TABLE ts_off;

SELECT '-- an explicit INNER ENGINE SETTINGS value is kept';
CREATE TABLE ts_override ENGINE = TimeSeries
SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS prewarm_mark_cache = 0;
SELECT extract(name, '^\.inner_id\.(\w+)\.') AS inner_table,
       extract(engine_full, 'prewarm_mark_cache = (\\d+)') AS prewarm_mark_cache
FROM system.tables
WHERE database = currentDatabase() AND name LIKE '.inner\\_id.%'
ORDER BY inner_table;
DROP TABLE ts_override;

SELECT '-- the setting requires version 12';
CREATE TABLE ts ENGINE = TimeSeries SETTINGS version = 11, prewarm_mark_cache = 1; -- { serverError INVALID_SETTING_VALUE }
