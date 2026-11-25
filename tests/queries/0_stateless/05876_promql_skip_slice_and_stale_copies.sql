-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: EXPLAIN actions inspects the local query plan.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_slice;
DROP TABLE IF EXISTS ts_dt0;
DROP TABLE IF EXISTS ts_dt6;
DROP TABLE IF EXISTS ts_f32;

CREATE TABLE ts_slice ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 3600, recent_samples_ttl_seconds = 0;

-- One 1h bucket [0, 3600) with samples around window (400, 700]:
--   100, 400 (excluded), 400.001 (included), 550, 700 (included), 700.001 (excluded), 3500.
INSERT INTO ts_slice (metric_name, tags, samples) VALUES
    ('m', map('env', 'prod'), [
        (toDateTime64(100, 3), 1.),
        (toDateTime64(400, 3), 2.),
        (toDateTime64(400.001, 3), 10.),
        (toDateTime64(550, 3), 25.),
        (toDateTime64(700, 3), 40.),
        (toDateTime64(700.001, 3), 1000.),
        (toDateTime64(3500, 3), 2000.)
    ]),
    ('m', map('env', 'gap'), [
        (toDateTime64(100, 3), 5.),
        (toDateTime64(3500, 3), 6.)
    ]),
    ('s', map('env', 'stale_mid'), [
        (toDateTime64(450, 3), 10.),
        (toDateTime64(500, 3), reinterpretAsFloat64(toUInt64(9218868437227405314))),
        (toDateTime64(600, 3), 20.)
    ]),
    ('s', map('env', 'stale_end'), [
        (toDateTime64(450, 3), 10.),
        (toDateTime64(600, 3), reinterpretAsFloat64(toUInt64(9218868437227405314)))
    ]),
    ('s', map('env', 'qnan'), [
        (toDateTime64(450, 3), 10.),
        (toDateTime64(500, 3), nan),
        (toDateTime64(600, 3), 20.)
    ]);

SELECT '-- plan checks: range functions and instant selectors skip slicing and arrayFilter';
SELECT
    plan NOT LIKE '%timeSeriesSliceSortedArray%' AS no_slice,
    plan NOT LIKE '%arrayFilter%' AS no_array_filter,
    plan LIKE '%__timeSeriesRemoveStaleMarkers%' AS uses_internal_stale_filter
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT * FROM prometheusQueryRange(ts_slice, 'rate(m[5m])', 700, 1300, 300)));

SELECT
    plan NOT LIKE '%timeSeriesSliceSortedArray%' AS no_slice,
    plan NOT LIKE '%__timeSeriesRemoveStaleMarkers%' AS no_stale_filter_before_last
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT * FROM prometheusQuery(ts_slice, 'm', 700)));

SELECT
    plan LIKE '%timeSeriesSliceSortedArray%' AS slices_bare_range,
    plan LIKE '%__timeSeriesRemoveStaleMarkers%' AS uses_internal_stale_filter
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT * FROM prometheusQuery(ts_slice, 'm[5m]', 700)));

SELECT
    plan LIKE '%timeSeriesSliceSortedArray%' AS slices_public_samples
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT id, samples FROM timeSeriesSelector(ts_slice, 'm', 400.001, 700, 'samples')));

SELECT
    plan NOT LIKE '%timeSeriesSliceSortedArray%' AS skips_raw_samples
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT id, samples FROM timeSeriesSelector(ts_slice, 'm', 400.001, 700, 'raw_samples')));

SELECT '-- boundary timestamps (400 excluded, 400.001..700 included, 700.001 excluded) and gap-only bucket';
SELECT tags, round(value, 6) FROM prometheusQuery(ts_slice, 'rate(m[5m])', 700) ORDER BY tags;
SELECT tags, round(value, 6) FROM prometheusQuery(ts_slice, 'increase(m[5m])', 700) ORDER BY tags;
SELECT tags, round(value, 6) FROM prometheusQuery(ts_slice, 'delta(m[5m])', 700) ORDER BY tags;
SELECT tags, value FROM prometheusQuery(ts_slice, 'min_over_time(m[5m])', 700) ORDER BY tags;
SELECT tags, value FROM prometheusQuery(ts_slice, 'max_over_time(m[5m])', 700) ORDER BY tags;
SELECT tags, value FROM prometheusQuery(ts_slice, 'sum_over_time(m[5m])', 700) ORDER BY tags;
SELECT tags, value FROM prometheusQuery(ts_slice, 'count_over_time(m[5m])', 700) ORDER BY tags;
SELECT tags, value FROM prometheusQuery(ts_slice, 'last_over_time(m[5m])', 700) ORDER BY tags;
SELECT tags, value FROM prometheusQuery(ts_slice, 'first_over_time(m[5m])', 700) ORDER BY tags;
SELECT tags, round(value, 6) FROM prometheusQuery(ts_slice, 'predict_linear(m[5m], 60)', 700) ORDER BY tags;
SELECT tags, value FROM prometheusQuery(ts_slice, 'quantile_over_time(0.5, m[5m])', 700) ORDER BY tags;
SELECT tags, value FROM prometheusQuery(ts_slice, 'absent_over_time(m{env="gap"}[5m])', 700) ORDER BY tags;
SELECT tags, value FROM prometheusQuery(ts_slice, 'm', 700) ORDER BY tags;
SELECT tags, value FROM prometheusQuery(ts_slice, 'timestamp(m)', 700) ORDER BY tags;
SELECT tags, samples FROM prometheusQuery(ts_slice, 'm[5m]', 700) ORDER BY tags;

SELECT '-- offset and @ modifiers on range selectors';
SELECT tags, round(value, 6) FROM prometheusQuery(ts_slice, 'rate(m[5m] offset 1m)', 760) ORDER BY tags;
SELECT tags, arrayMap(s -> (s.1, round(s.2, 6)), samples) FROM prometheusQueryRange(ts_slice, 'rate(m[5m] @ 700)', 1000, 1600, 300) ORDER BY tags;

SELECT '-- stale NaN vs ordinary quiet NaN in PromQL';
SELECT tags, value FROM prometheusQuery(ts_slice, 'count_over_time(s[5m])', 700) ORDER BY tags;
SELECT tags, isNaN(value), value FROM prometheusQuery(ts_slice, 'sum_over_time(s[5m])', 700) ORDER BY tags;
SELECT tags, value FROM prometheusQuery(ts_slice, 's', 700) ORDER BY tags;

SELECT '-- sub-second scale conversions (DateTime scale 0 and DateTime64(6))';
CREATE TABLE ts_dt0 (samples Array(Tuple(DateTime, Float64))) ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 3600, recent_samples_ttl_seconds = 0;
INSERT INTO ts_dt0 (metric_name, tags, samples) VALUES
    ('m', map(), [(toDateTime(400), 1.), (toDateTime(401), 10.), (toDateTime(700), 40.), (toDateTime(701), 999.)]);
SELECT value FROM prometheusQuery(ts_dt0, 'min_over_time(m[5m])', toDateTime64(700.5, 3));
SELECT value FROM prometheusQuery(ts_dt0, 'max_over_time(m[5m])', toDateTime64(700.5, 3));

CREATE TABLE ts_dt6 (samples Array(Tuple(DateTime64(6), Float64))) ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 3600, recent_samples_ttl_seconds = 0;
INSERT INTO ts_dt6 (metric_name, tags, samples) VALUES
    ('m', map(), [
        (toDateTime64(400.000000, 6), 1.),
        (toDateTime64(400.000001, 6), 10.),
        (toDateTime64(700.000000, 6), 40.),
        (toDateTime64(700.000001, 6), 999.)
    ]);
SELECT value FROM prometheusQuery(ts_dt6, 'min_over_time(m[5m])', toDateTime64(700, 6));
SELECT value FROM prometheusQuery(ts_dt6, 'max_over_time(m[5m])', toDateTime64(700, 6));

SELECT '-- Float32 TimeSeries table';
CREATE TABLE ts_f32 (samples Array(Tuple(DateTime64(3), Float32))) ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 3600, recent_samples_ttl_seconds = 0;
INSERT INTO ts_f32 (metric_name, tags, samples) VALUES
    ('m', map(), [(toDateTime64(450, 3), 10.), (toDateTime64(600, 3), 25.)]);
SELECT value FROM prometheusQuery(ts_f32, 'sum_over_time(m[5m])', 700);

SELECT '-- direct __timeSeriesRemoveStaleMarkers edge cases';
WITH
    reinterpretAsFloat64(toUInt64(9218868437227405314)) AS stale,
    reinterpretAsFloat64(toUInt64(9218868437227405313)) AS other_snan
SELECT
    row_id,
    arrayMap(s -> (s.1, reinterpretAsUInt64(s.2)), __timeSeriesRemoveStaleMarkers(arr)) AS cleaned
FROM
(
    SELECT 0 AS row_id, []::Array(Tuple(UInt32, Float64)) AS arr
    UNION ALL SELECT 1, [(1, 1.), (2, 2.)]
    UNION ALL SELECT 2, []::Array(Tuple(UInt32, Float64))
    UNION ALL SELECT 3, [(3, stale), (4, 4.), (5, stale), (6, stale), (7, other_snan), (8, nan), (9, stale)]
    UNION ALL SELECT 4, [(10, stale), (11, stale)]
    UNION ALL SELECT 5, [(12, 12.)]
)
ORDER BY row_id;

-- Zero-stale multi-row block and const/Float32 pass-through
SELECT count(), sum(length(__timeSeriesRemoveStaleMarkers(arr)))
FROM (SELECT arrayMap(j -> (toUInt32(j), toFloat64(j)), range(if(number = 5, 0, 20))) AS arr FROM numbers(32));

SELECT __timeSeriesRemoveStaleMarkers([(toUInt32(1), toFloat32(2.5)), (toUInt32(2), toFloat32(nan))]);

SELECT '-- direct timeSeriesSliceSortedArray untouched, mixed, empty, and non-const blocks';
SELECT count(), sum(length(timeSeriesSliceSortedArray(arr, 0, 1000)))
FROM (SELECT arrayMap(j -> (toUInt32(j * 10), toFloat64(j)), range(if(number = 3, 0, 5))) AS arr FROM numbers(10));

SELECT number, timeSeriesSliceSortedArray(
    arrayMap(j -> (toUInt32(number * 100 + j * 10), toFloat64(j)), range(if(number = 2, 0, 4))),
    115, 315)
FROM numbers(5)
ORDER BY number;

SELECT number, timeSeriesSliceSortedArray(
    arrayMap(j -> (toUInt32(j * 10), toFloat64(j)), range(5)),
    if(number = 2, 40, 0),
    if(number = 2, 10, 50))
FROM numbers(4)
ORDER BY number;

DROP TABLE ts_f32;
DROP TABLE ts_dt6;
DROP TABLE ts_dt0;
DROP TABLE ts_slice;
