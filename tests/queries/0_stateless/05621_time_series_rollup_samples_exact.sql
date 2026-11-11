-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A rollup column `samples_<step>` keeps, in every row of the samples table, the last sample of every interval
-- (k * step, (k + 1) * step]. PromQL reads it only for instant selectors evaluated at multiples of a query step which
-- the rollup step divides: there the result is the same as with the raw samples.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET enable_analyzer = 1;

DROP TABLE IF EXISTS ts_rollup_exact;
DROP TABLE IF EXISTS ts_rollup_data;
DROP TABLE IF EXISTS ts_rollup_recent;

SELECT '-- the intervals are closed on the right: a sample at a multiple of the step ends its interval';
SELECT timeSeriesGroupArray(60)(timestamps, values)
FROM (SELECT [30, 60, 90, 120, 121]::Array(UInt32) AS timestamps, [1, 2, 3, 4, 5]::Array(Float64) AS values);
SELECT timeSeriesGroupArray(60)(timestamps, values)
FROM (SELECT arrayMap(x -> toDateTime64(x, 3), [-90, -60, -30, 0]) AS timestamps, [1, 2, 3, 4]::Array(Float64) AS values);

SELECT '-- the intervals start over in every bucket';
CREATE TABLE ts_rollup_exact ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 3600, samples_rollup_steps = '1m, 5m', recent_samples_ttl_seconds = 0;
INSERT INTO ts_rollup_exact (metric_name, tags, samples) VALUES
    ('b', map('k', 'v'), [(toDateTime64(3550, 3), 1.), (toDateTime64(3600, 3), 2.), (toDateTime64(3610, 3), 3.)]);
SELECT bucket, samples, samples_60, samples_300
FROM merge(currentDatabase(), '^\\.inner_id\\.samples\\.')
ORDER BY bucket;

SELECT '-- the last sample at or before a multiple of the step is kept: at 3600 the value is 2 with and without the rollup';
SELECT * FROM prometheusQueryRange(ts_rollup_exact, 'b', 3600, 3660, '1m');
SELECT * FROM prometheusQueryRange(ts_rollup_exact, 'b', 3600, 3660, '1m') SETTINGS time_series_prefer_rollup_tables = 0;

-- 8 series over [0, 14400): a sample every 15 seconds, every 4th one at a multiple of 60 seconds, the others with a jitter
-- in milliseconds, and some stale markers. Three inserts take every third sample each, so the parts overlap in time.
CREATE TABLE ts_rollup_data ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 3600, samples_rollup_steps = '1m, 5m', recent_samples_ttl_seconds = 0;
INSERT INTO ts_rollup_data (metric_name, tags, samples)
SELECT 'r', map('i', toString(number)),
       arrayMap(j -> (toDateTime64(j * 15 + if(j % 4 = 0, 0, (sipHash64(number, j) % 15000) / 1000), 3),
                      if(j % 37 = 5, reinterpretAsFloat64(toUInt64(9218868437227405314)), toFloat64(sipHash64(number, j, 1) % 1000))),
                arrayFilter(j -> j % 3 = 0, range(960)))
FROM numbers(8);
INSERT INTO ts_rollup_data (metric_name, tags, samples)
SELECT 'r', map('i', toString(number)),
       arrayMap(j -> (toDateTime64(j * 15 + if(j % 4 = 0, 0, (sipHash64(number, j) % 15000) / 1000), 3),
                      if(j % 37 = 5, reinterpretAsFloat64(toUInt64(9218868437227405314)), toFloat64(sipHash64(number, j, 1) % 1000))),
                arrayFilter(j -> j % 3 = 1, range(960)))
FROM numbers(8);
INSERT INTO ts_rollup_data (metric_name, tags, samples)
SELECT 'r', map('i', toString(number)),
       arrayMap(j -> (toDateTime64(j * 15 + if(j % 4 = 0, 0, (sipHash64(number, j) % 15000) / 1000), 3),
                      if(j % 37 = 5, reinterpretAsFloat64(toUInt64(9218868437227405314)), toFloat64(sipHash64(number, j, 1) % 1000))),
                arrayFilter(j -> j % 3 = 2, range(960)))
FROM numbers(8);

SELECT '-- every row: samples_<step> = timeSeriesGroupArray(<step>)(samples), before and after the merges';
SELECT
    count() > 0,
    countIf(arrayMap(x -> (x.1, reinterpretAsUInt64(x.2)), samples_60)
        != arrayMap(x -> (x.1, reinterpretAsUInt64(x.2)), arrayReduce('timeSeriesGroupArray(60)', arrayMap(x -> x.1, samples), arrayMap(x -> x.2, samples)))),
    countIf(arrayMap(x -> (x.1, reinterpretAsUInt64(x.2)), samples_300)
        != arrayMap(x -> (x.1, reinterpretAsUInt64(x.2)), arrayReduce('timeSeriesGroupArray(300)', arrayMap(x -> x.1, samples), arrayMap(x -> x.2, samples))))
FROM merge(currentDatabase(), '^\\.inner_id\\.samples\\.');

OPTIMIZE TABLE ts_rollup_data FINAL;

SELECT
    count() > 0,
    countIf(arrayMap(x -> (x.1, reinterpretAsUInt64(x.2)), samples_60)
        != arrayMap(x -> (x.1, reinterpretAsUInt64(x.2)), arrayReduce('timeSeriesGroupArray(60)', arrayMap(x -> x.1, samples), arrayMap(x -> x.2, samples)))),
    countIf(arrayMap(x -> (x.1, reinterpretAsUInt64(x.2)), samples_300)
        != arrayMap(x -> (x.1, reinterpretAsUInt64(x.2)), arrayReduce('timeSeriesGroupArray(300)', arrayMap(x -> x.1, samples), arrayMap(x -> x.2, samples))))
FROM merge(currentDatabase(), '^\\.inner_id\\.samples\\.');

SELECT '-- which column PromQL reads (raw, samples_60, samples_300), and whether the result is the same as with time_series_prefer_rollup_tables = 0';
SELECT 'r, step 1m',
    (SELECT (countIf(explain LIKE '%timeSeriesSliceSortedArray(samples,%') > 0, countIf(explain LIKE '%samples_60%') > 0, countIf(explain LIKE '%samples_300%') > 0)
     FROM (EXPLAIN SELECT * FROM prometheusQueryRange(ts_rollup_data, 'r', 1800, 14400, '1m'))),
    (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'r', 1800, 14400, '1m') ORDER BY tags))
        = (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'r', 1800, 14400, '1m') ORDER BY tags) SETTINGS time_series_prefer_rollup_tables = 0),
    (SELECT sum(length(samples)) > 0 FROM prometheusQueryRange(ts_rollup_data, 'r', 1800, 14400, '1m'));
SELECT 'r, step 5m',
    (SELECT (countIf(explain LIKE '%timeSeriesSliceSortedArray(samples,%') > 0, countIf(explain LIKE '%samples_60%') > 0, countIf(explain LIKE '%samples_300%') > 0)
     FROM (EXPLAIN SELECT * FROM prometheusQueryRange(ts_rollup_data, 'r', 1800, 14400, '5m'))),
    (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'r', 1800, 14400, '5m') ORDER BY tags))
        = (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'r', 1800, 14400, '5m') ORDER BY tags) SETTINGS time_series_prefer_rollup_tables = 0),
    (SELECT sum(length(samples)) > 0 FROM prometheusQueryRange(ts_rollup_data, 'r', 1800, 14400, '5m'));
SELECT 'r, step 10m: 300 divides it',
    (SELECT (countIf(explain LIKE '%timeSeriesSliceSortedArray(samples,%') > 0, countIf(explain LIKE '%samples_60%') > 0, countIf(explain LIKE '%samples_300%') > 0)
     FROM (EXPLAIN SELECT * FROM prometheusQueryRange(ts_rollup_data, 'r', 1800, 14400, '10m'))),
    (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'r', 1800, 14400, '10m') ORDER BY tags))
        = (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'r', 1800, 14400, '10m') ORDER BY tags) SETTINGS time_series_prefer_rollup_tables = 0),
    (SELECT sum(length(samples)) > 0 FROM prometheusQueryRange(ts_rollup_data, 'r', 1800, 14400, '10m'));
SELECT 'r, step 90s: no rollup step divides it',
    (SELECT (countIf(explain LIKE '%timeSeriesSliceSortedArray(samples,%') > 0, countIf(explain LIKE '%samples_60%') > 0, countIf(explain LIKE '%samples_300%') > 0)
     FROM (EXPLAIN SELECT * FROM prometheusQueryRange(ts_rollup_data, 'r', 1800, 14400, '90s'))),
    (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'r', 1800, 14400, '90s') ORDER BY tags))
        = (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'r', 1800, 14400, '90s') ORDER BY tags) SETTINGS time_series_prefer_rollup_tables = 0),
    (SELECT sum(length(samples)) > 0 FROM prometheusQueryRange(ts_rollup_data, 'r', 1800, 14400, '90s'));
SELECT 'r, step 5m, start not a multiple of the step',
    (SELECT (countIf(explain LIKE '%timeSeriesSliceSortedArray(samples,%') > 0, countIf(explain LIKE '%samples_60%') > 0, countIf(explain LIKE '%samples_300%') > 0)
     FROM (EXPLAIN SELECT * FROM prometheusQueryRange(ts_rollup_data, 'r', 1807, 14400, '5m'))),
    (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'r', 1807, 14400, '5m') ORDER BY tags))
        = (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'r', 1807, 14400, '5m') ORDER BY tags) SETTINGS time_series_prefer_rollup_tables = 0),
    (SELECT sum(length(samples)) > 0 FROM prometheusQueryRange(ts_rollup_data, 'r', 1807, 14400, '5m'));
SELECT 'r offset 30s, step 5m',
    (SELECT (countIf(explain LIKE '%timeSeriesSliceSortedArray(samples,%') > 0, countIf(explain LIKE '%samples_60%') > 0, countIf(explain LIKE '%samples_300%') > 0)
     FROM (EXPLAIN SELECT * FROM prometheusQueryRange(ts_rollup_data, 'r offset 30s', 1800, 14400, '5m'))),
    (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'r offset 30s', 1800, 14400, '5m') ORDER BY tags))
        = (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'r offset 30s', 1800, 14400, '5m') ORDER BY tags) SETTINGS time_series_prefer_rollup_tables = 0),
    (SELECT sum(length(samples)) > 0 FROM prometheusQueryRange(ts_rollup_data, 'r offset 30s', 1800, 14400, '5m'));
SELECT 'r offset 5m, step 5m',
    (SELECT (countIf(explain LIKE '%timeSeriesSliceSortedArray(samples,%') > 0, countIf(explain LIKE '%samples_60%') > 0, countIf(explain LIKE '%samples_300%') > 0)
     FROM (EXPLAIN SELECT * FROM prometheusQueryRange(ts_rollup_data, 'r offset 5m', 1800, 14400, '5m'))),
    (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'r offset 5m', 1800, 14400, '5m') ORDER BY tags))
        = (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'r offset 5m', 1800, 14400, '5m') ORDER BY tags) SETTINGS time_series_prefer_rollup_tables = 0),
    (SELECT sum(length(samples)) > 0 FROM prometheusQueryRange(ts_rollup_data, 'r offset 5m', 1800, 14400, '5m'));
SELECT 'sum(r), step 5m',
    (SELECT (countIf(explain LIKE '%timeSeriesSliceSortedArray(samples,%') > 0, countIf(explain LIKE '%samples_60%') > 0, countIf(explain LIKE '%samples_300%') > 0)
     FROM (EXPLAIN SELECT * FROM prometheusQueryRange(ts_rollup_data, 'sum(r)', 1800, 14400, '5m'))),
    (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'sum(r)', 1800, 14400, '5m') ORDER BY tags))
        = (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'sum(r)', 1800, 14400, '5m') ORDER BY tags) SETTINGS time_series_prefer_rollup_tables = 0),
    (SELECT sum(length(samples)) > 0 FROM prometheusQueryRange(ts_rollup_data, 'sum(r)', 1800, 14400, '5m'));
SELECT 'timestamp(r), step 5m',
    (SELECT (countIf(explain LIKE '%timeSeriesSliceSortedArray(samples,%') > 0, countIf(explain LIKE '%samples_60%') > 0, countIf(explain LIKE '%samples_300%') > 0)
     FROM (EXPLAIN SELECT * FROM prometheusQueryRange(ts_rollup_data, 'timestamp(r)', 1800, 14400, '5m'))),
    (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'timestamp(r)', 1800, 14400, '5m') ORDER BY tags))
        = (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'timestamp(r)', 1800, 14400, '5m') ORDER BY tags) SETTINGS time_series_prefer_rollup_tables = 0),
    (SELECT sum(length(samples)) > 0 FROM prometheusQueryRange(ts_rollup_data, 'timestamp(r)', 1800, 14400, '5m'));
SELECT 'max_over_time(r[30m:5m]), step 5m',
    (SELECT (countIf(explain LIKE '%timeSeriesSliceSortedArray(samples,%') > 0, countIf(explain LIKE '%samples_60%') > 0, countIf(explain LIKE '%samples_300%') > 0)
     FROM (EXPLAIN SELECT * FROM prometheusQueryRange(ts_rollup_data, 'max_over_time(r[30m:5m])', 1800, 14400, '5m'))),
    (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'max_over_time(r[30m:5m])', 1800, 14400, '5m') ORDER BY tags))
        = (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'max_over_time(r[30m:5m])', 1800, 14400, '5m') ORDER BY tags) SETTINGS time_series_prefer_rollup_tables = 0),
    (SELECT sum(length(samples)) > 0 FROM prometheusQueryRange(ts_rollup_data, 'max_over_time(r[30m:5m])', 1800, 14400, '5m'));
SELECT 'rate(r[5m]), step 5m',
    (SELECT (countIf(explain LIKE '%timeSeriesSliceSortedArray(samples,%') > 0, countIf(explain LIKE '%samples_60%') > 0, countIf(explain LIKE '%samples_300%') > 0)
     FROM (EXPLAIN SELECT * FROM prometheusQueryRange(ts_rollup_data, 'rate(r[5m])', 1800, 14400, '5m'))),
    (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'rate(r[5m])', 1800, 14400, '5m') ORDER BY tags))
        = (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'rate(r[5m])', 1800, 14400, '5m') ORDER BY tags) SETTINGS time_series_prefer_rollup_tables = 0),
    (SELECT sum(length(samples)) > 0 FROM prometheusQueryRange(ts_rollup_data, 'rate(r[5m])', 1800, 14400, '5m'));
SELECT 'max_over_time(r[10m]), step 5m',
    (SELECT (countIf(explain LIKE '%timeSeriesSliceSortedArray(samples,%') > 0, countIf(explain LIKE '%samples_60%') > 0, countIf(explain LIKE '%samples_300%') > 0)
     FROM (EXPLAIN SELECT * FROM prometheusQueryRange(ts_rollup_data, 'max_over_time(r[10m])', 1800, 14400, '5m'))),
    (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'max_over_time(r[10m])', 1800, 14400, '5m') ORDER BY tags))
        = (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'max_over_time(r[10m])', 1800, 14400, '5m') ORDER BY tags) SETTINGS time_series_prefer_rollup_tables = 0),
    (SELECT sum(length(samples)) > 0 FROM prometheusQueryRange(ts_rollup_data, 'max_over_time(r[10m])', 1800, 14400, '5m'));
SELECT 'count_over_time(r[10m]), step 5m',
    (SELECT (countIf(explain LIKE '%timeSeriesSliceSortedArray(samples,%') > 0, countIf(explain LIKE '%samples_60%') > 0, countIf(explain LIKE '%samples_300%') > 0)
     FROM (EXPLAIN SELECT * FROM prometheusQueryRange(ts_rollup_data, 'count_over_time(r[10m])', 1800, 14400, '5m'))),
    (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'count_over_time(r[10m])', 1800, 14400, '5m') ORDER BY tags))
        = (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'count_over_time(r[10m])', 1800, 14400, '5m') ORDER BY tags) SETTINGS time_series_prefer_rollup_tables = 0),
    (SELECT sum(length(samples)) > 0 FROM prometheusQueryRange(ts_rollup_data, 'count_over_time(r[10m])', 1800, 14400, '5m'));
SELECT 'last_over_time(r[10m]), step 5m',
    (SELECT (countIf(explain LIKE '%timeSeriesSliceSortedArray(samples,%') > 0, countIf(explain LIKE '%samples_60%') > 0, countIf(explain LIKE '%samples_300%') > 0)
     FROM (EXPLAIN SELECT * FROM prometheusQueryRange(ts_rollup_data, 'last_over_time(r[10m])', 1800, 14400, '5m'))),
    (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'last_over_time(r[10m])', 1800, 14400, '5m') ORDER BY tags))
        = (SELECT groupArray((tags, toString(samples))) FROM (SELECT * FROM prometheusQueryRange(ts_rollup_data, 'last_over_time(r[10m])', 1800, 14400, '5m') ORDER BY tags) SETTINGS time_series_prefer_rollup_tables = 0),
    (SELECT sum(length(samples)) > 0 FROM prometheusQueryRange(ts_rollup_data, 'last_over_time(r[10m])', 1800, 14400, '5m'));

SELECT '-- a rollup column is preferred to the recent samples table, which keeps every sample';
CREATE TABLE ts_rollup_recent ENGINE = TimeSeries SETTINGS samples_rollup_steps = '1m';
-- A sample in both tables, so that the plan names the table it reads.
INSERT INTO ts_rollup_recent (metric_name, tags, samples) VALUES ('m', map('k', 'v'), [(toDateTime64(now() - 60, 3), 1.)]);
SELECT countIf(explain LIKE '%samples_60%') > 0 AS reads_rollup, countIf(explain LIKE '%.inner_id.recentsamples.%') > 0 AS reads_recent
FROM (EXPLAIN SELECT * FROM timeSeriesSelector(currentDatabase(), 'ts_rollup_recent', 'm', now() - 3600, now(), 'samples', 60));
SELECT countIf(explain LIKE '%samples_60%') > 0 AS reads_rollup, countIf(explain LIKE '%.inner_id.recentsamples.%') > 0 AS reads_recent
FROM (EXPLAIN SELECT * FROM timeSeriesSelector(currentDatabase(), 'ts_rollup_recent', 'm', now() - 3600, now(), 'samples', 60))
SETTINGS time_series_prefer_rollup_tables = 0;

DROP TABLE ts_rollup_exact;
DROP TABLE ts_rollup_data;
DROP TABLE ts_rollup_recent;
