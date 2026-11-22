-- timeSeriesFromGrid and timeSeriesRange write their timestamps straight into the typed column.
-- The results must be the same as computed from `range` for every timestamp type, with NULL values
-- skipped, and with several rows in one block (each row's points go after the points of the rows before it).

SET session_timezone = 'UTC';

SELECT 'DateTime64(3)';
SELECT timeSeriesFromGrid('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:01:30'::DateTime64(3), 30, [10, 20, NULL, 30]);
SELECT timeSeriesFromGrid('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:01:30'::DateTime64(3), 30, [10., 20., 25., 30.]);
SELECT timeSeriesRange('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:01:30'::DateTime64(3), 30);

SELECT 'DateTime64(6), Decimal step';
SELECT timeSeriesFromGrid(toDateTime64('2025-06-01 00:00:00', 6), toDateTime64('2025-06-01 00:00:01', 6), toDecimal64(0.25, 2), [1, NULL, 3, 4, NULL]);
SELECT timeSeriesRange(toDateTime64('2025-06-01 00:00:00', 6), toDateTime64('2025-06-01 00:00:01', 6), toDecimal64(0.25, 2));
SELECT toTypeName(timeSeriesFromGrid(toDateTime64('2025-06-01 00:00:00', 6), toDateTime64('2025-06-01 00:00:01', 6), toDecimal64(0.25, 2), [1, 2, 3, 4, 5]::Array(Float32)));

SELECT 'DateTime';
SELECT timeSeriesFromGrid(toDateTime('2025-06-01 00:00:00'), toDateTime('2025-06-01 00:02:00'), 60, [1, NULL, 3]);
SELECT timeSeriesFromGrid(toDateTime('2025-06-01 00:00:00'), toDateTime('2025-06-01 00:02:00'), 60, [1, 2, 3]::Array(Float32));
SELECT timeSeriesRange(toDateTime('2025-06-01 00:00:00'), toDateTime('2025-06-01 00:02:00'), 60);

SELECT 'UInt32';
SELECT timeSeriesFromGrid(toUInt32(100), toUInt32(130), 10, [1, 2, NULL, 4]);
SELECT timeSeriesFromGrid(toUInt32(100), toUInt32(130), 10, [1, 2, 3, 4]);
SELECT timeSeriesRange(toUInt32(100), toUInt32(130), 10), toTypeName(timeSeriesRange(toUInt32(100), toUInt32(130), 10));

SELECT 'all NULL, constant values';
SELECT timeSeriesFromGrid(toDateTime64(100, 3), toDateTime64(120, 3), 10, [NULL, NULL, NULL]::Array(Nullable(Float64)));
SELECT timeSeriesFromGrid(toDateTime64(100, 3), toDateTime64(120, 3), 10, arrayResize([]::Array(Float64), 3, 7.5));

SELECT 'before 1970';
SELECT timeSeriesFromGrid(toDateTime64('1969-12-31 23:59:00', 3), toDateTime64('1970-01-01 00:01:00', 3), 30, [1, NULL, 3, 4, 5]);
SELECT timeSeriesRange(toDateTime64('1969-12-31 23:59:00', 3), toDateTime64('1970-01-01 00:01:00', 3), 30);

SELECT 'one point';
SELECT timeSeriesFromGrid(toDateTime64(100, 3), toDateTime64(200, 3), toUInt64(18446744073709551615), [1]);
SELECT timeSeriesFromGrid(toDateTime64(100, 3), toDateTime64(200, 3), toUInt64(18446744073709551615), [NULL]::Array(Nullable(Float64)));
SELECT timeSeriesRange(toDateTime64(100, 3), toDateTime64(200, 3), toUInt64(18446744073709551615));
SELECT timeSeriesRange(toDateTime64(100, 3), toDateTime64(100, 3), 10);

SELECT 'many rows in one block';
-- Rows of 1 to 50 points: dense rows, rows with some NULLs and all-NULL rows, against the same pairs built from `range`.
SELECT count(), sum(length(r)), countIf(r = expected), countIf(arrayMap(t -> toUnixTimestamp64Milli(t), timestamps) = range(100000, 100000 + n * 10000, 10000))
FROM
(
    SELECT
        number % 50 + 1 AS n,
        arrayMap(j -> if(number % 3 = 0, j + 0.5, if(number % 3 = 1 AND j % 2 = 0, NULL, if(number % 3 = 2 AND number % 7 = 2, NULL, j * 2.))), range(n))::Array(Nullable(Float64)) AS v,
        timeSeriesFromGrid(toDateTime64(100, 3), toDateTime64(90 + n * 10, 3), 10, v) AS r,
        arrayFilter(p -> p.2 IS NOT NULL, arrayMap(j -> (fromUnixTimestamp64Milli(toInt64(100000 + j * 10000)), v[j + 1]), range(n))) AS expected,
        timeSeriesRange(toDateTime64(100, 3), toDateTime64(90 + n * 10, 3), 10) AS timestamps
    FROM numbers(1000)
);
SELECT count(), sum(length(r)), countIf(r = arrayMap(j -> toUInt32(1000 + j * 5), range(n)))
FROM (SELECT number % 20 + 1 AS n, timeSeriesRange(toUInt32(1000), toUInt32(995 + n * 5), 5) AS r FROM numbers(500));

SELECT 'max elements';
SELECT timeSeriesRange(toDateTime64(0, 3), toDateTime64(100, 3), 1) SETTINGS function_range_max_elements_in_block = 100; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT timeSeriesFromGrid(toDateTime64(0, 3), toDateTime64(100, 3), 1, arrayResize([1.], 101)) SETTINGS function_range_max_elements_in_block = 100; -- { serverError ARGUMENT_OUT_OF_BOUND }
-- 101 steps, but the NULL values are skipped, so the row has 50 points.
SELECT length(timeSeriesFromGrid(toDateTime64(0, 3), toDateTime64(100, 3), 1, arrayMap(j -> if(j % 2 = 1, NULL, 1.), arrayEnumerate(arrayResize([1], 101))))) SETTINGS function_range_max_elements_in_block = 100;
-- The bound is on the whole block: 10 rows of 10 points pass, 11 rows don't.
SELECT sum(length(timeSeriesRange(materialize(toDateTime64(0, 3)), toDateTime64(9, 3), 1))) FROM numbers(10) SETTINGS function_range_max_elements_in_block = 100;
SELECT sum(length(timeSeriesRange(materialize(toDateTime64(0, 3)), toDateTime64(9, 3), 1))) FROM numbers(11) SETTINGS function_range_max_elements_in_block = 100, max_block_size = 100; -- { serverError ARGUMENT_OUT_OF_BOUND }
