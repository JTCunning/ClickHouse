-- Tags: no-fasttest
-- Tag no-fasttest: the time series aggregate functions are not in the fast-test build.

SET session_timezone = 'UTC';

SELECT '-- a sample on a boundary belongs to the interval that ends there';
SELECT timeSeriesDownsample(60)([(toDateTime(0), 1.), (toDateTime(30), 2.), (toDateTime(60), 3.)])
    = [(toDateTime(0), 1.), (toDateTime(60), 3.)];

SELECT '-- the greatest value wins, and a NaN loses';
SELECT timeSeriesDownsample(60)([(toDateTime(10), 1.), (toDateTime(10), 3.)]) = [(toDateTime(10), 3.)];
SELECT timeSeriesDownsample(60)([(toDateTime(10), nan), (toDateTime(10), 1.)]) = [(toDateTime(10), 1.)];
SELECT timeSeriesDownsample(60)([(toDateTime(10), 1.), (toDateTime(10), nan)]) = [(toDateTime(10), 1.)];

SELECT '-- the same interval from either merge order';
WITH
    left AS (SELECT timeSeriesDownsampleState(60)([(toDateTime(0), 1.), (toDateTime(30), 2.)]) AS s),
    right AS (SELECT timeSeriesDownsampleState(60)([(toDateTime(60), 3.), (toDateTime(90), 4.)]) AS s)
SELECT
    (SELECT timeSeriesDownsampleMerge(60)(s) FROM (SELECT s FROM left UNION ALL SELECT s FROM right))
    = (SELECT timeSeriesDownsampleMerge(60)(s) FROM (SELECT s FROM right UNION ALL SELECT s FROM left));

SELECT '-- a coarser interval after a finer one matches the coarser interval alone';
SELECT timeSeriesDownsample(120)(fine) = timeSeriesDownsample(120)(raw)
FROM
(
    SELECT
        timeSeriesDownsample(60)([(toDateTime(0), 1.), (toDateTime(30), 2.), (toDateTime(60), 3.), (toDateTime(90), 4.), (toDateTime(120), 5.)]) AS fine,
        [(toDateTime(0), 1.), (toDateTime(30), 2.), (toDateTime(60), 3.), (toDateTime(90), 4.), (toDateTime(120), 5.)] AS raw
);

SELECT '-- the same function a second time does not change the result';
SELECT timeSeriesDownsample(60)(once) = timeSeriesDownsample(60)(raw)
FROM
(
    SELECT
        timeSeriesDownsample(60)([(toDateTime(0), 1.), (toDateTime(30), 2.), (toDateTime(60), 3.)]) AS once,
        [(toDateTime(0), 1.), (toDateTime(30), 2.), (toDateTime(60), 3.)] AS raw
);
