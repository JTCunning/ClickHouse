-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: the test checks which column the query plans read.

-- Rollup columns of a TimeSeries table of version 12: last-sample-per-interval writes
-- on the samples table, and step-based reads through timeSeriesSelector.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET enable_analyzer = 1;

DROP TABLE IF EXISTS ts_rollup;

SELECT '-- columns on the samples table';
CREATE TABLE ts_rollup ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 3600, samples_rollup_steps = '1m, 5m', recent_samples_ttl_seconds = 0;
SELECT
    countIf(name LIKE '.inner_id.samples.%') AS has_samples
FROM system.tables WHERE database = currentDatabase();
SELECT
    countIf(name = 'samples') AS has_raw,
    countIf(name = 'samples_60') AS has_60,
    countIf(name = 'samples_300') AS has_300
FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner_id.samples.%';
SELECT engine_full LIKE '%samples_rollup_steps = \'60, 300\'%' AS canonical_steps
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_rollup';

SELECT '-- rejections';
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS samples_rollup_steps = '2h'; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS samples_rollup_steps = '7s'; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS samples_rollup_steps = '1m, 1m'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS samples_rollup_steps = '1.5m'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS version = 11, samples_rollup_steps = '1m'; -- { serverError INVALID_SETTING_VALUE }
ALTER TABLE ts_rollup MODIFY SETTING samples_rollup_steps = '5m'; -- { serverError NOT_IMPLEMENTED }

SELECT '-- two inserts into the same 60s interval collapse to the last sample after OPTIMIZE FINAL';
INSERT INTO ts_rollup (metric_name, tags, samples) VALUES
    ('m', map('env', 'prod'), [(toDateTime64(10, 3), 1.), (toDateTime64(50, 3), 2.)]);
INSERT INTO ts_rollup (metric_name, tags, samples) VALUES
    ('m', map('env', 'prod'), [(toDateTime64(70, 3), 3.), (toDateTime64(110, 3), 4.)]);

OPTIMIZE TABLE ts_rollup FINAL;

SELECT arraySort(arrayFlatten(groupArray(samples))) AS raw,
       arraySort(arrayFlatten(groupArray(samples_60))) AS rollup_60,
       arraySort(arrayFlatten(groupArray(samples_300))) AS rollup_300
FROM merge(currentDatabase(), '^\\.inner_id\\.samples\\.');

SELECT '-- timeSeriesSelector chooses the largest rollup step dividing the query step';
SELECT sum(length(samples)) AS raw_count
FROM timeSeriesSelector(currentDatabase(), 'ts_rollup', 'm', 0, 1000, 'samples');
SELECT sum(length(samples)) AS rollup_300_count
FROM timeSeriesSelector(currentDatabase(), 'ts_rollup', 'm', 0, 1000, 'samples', 300);
SELECT sum(length(samples)) AS step_30_raw_count
FROM timeSeriesSelector(currentDatabase(), 'ts_rollup', 'm', 0, 1000, 'samples', 30);
SELECT sum(length(samples)) AS prefer_off_count
FROM timeSeriesSelector(currentDatabase(), 'ts_rollup', 'm', 0, 1000, 'samples', 300)
SETTINGS time_series_prefer_rollup_tables = 0;

SELECT '-- EXPLAIN: step 300 reads samples_300; step 30 reads samples';
SELECT countIf(explain LIKE '%timeSeriesSliceSortedArray(samples_300,%') > 0 AS reads_300,
       countIf(explain LIKE '%timeSeriesSliceSortedArray(samples,%') > 0 AS reads_raw
FROM (EXPLAIN SELECT sum(length(samples)) FROM timeSeriesSelector(currentDatabase(), 'ts_rollup', 'm', 0, 1000, 'samples', 300));
SELECT countIf(explain LIKE '%timeSeriesSliceSortedArray(samples_300,%') > 0 AS reads_300,
       countIf(explain LIKE '%timeSeriesSliceSortedArray(samples,%') > 0 AS reads_raw
FROM (EXPLAIN SELECT sum(length(samples)) FROM timeSeriesSelector(currentDatabase(), 'ts_rollup', 'm', 0, 1000, 'samples', 30));

SELECT '-- prometheusQueryRange at step 5m reads samples_300; step 15s reads samples';
SELECT countIf(position(explain, 'samples_300') > 0) > 0 AS range_5m_reads_300
FROM (EXPLAIN SELECT * FROM prometheusQueryRange(ts_rollup, 'm', 0, 120, '5m'));
SELECT countIf(position(explain, 'samples_300') > 0) = 0 AS range_15s_no_300,
       countIf(explain LIKE '%timeSeriesSliceSortedArray(samples,%') > 0 AS range_15s_reads_raw
FROM (EXPLAIN SELECT * FROM prometheusQueryRange(ts_rollup, 'm', 0, 120, '15s'));

SELECT '-- DROP removes the samples table';
DROP TABLE ts_rollup;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.samples.%';
