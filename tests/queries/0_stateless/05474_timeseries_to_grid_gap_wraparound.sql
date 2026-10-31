-- The bucket map of the `timeSeries*ToGrid` states is a `HashMap` with `TrivialHash`, so bucket index `k` lives in cell
-- `k % bufSize`. A series with a gap in the middle of the grid has bucket indexes above the buffer size that wrap onto
-- the cells of its first run of buckets; the map grows before such an index is inserted if it is more than half full
-- and the index collides with another one.
-- This test pins down the results on the shapes that take that path (the add, batch add, merge and deserialize paths)
-- against the expected grid computed without the aggregate functions.

SET enable_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS grid_gap_samples;
DROP TABLE IF EXISTS grid_gap_states;

-- One sample per minute at value = minute, on a 7-day grid at a 60 s step (10081 points) with a 60 s window, so grid
-- point `i` holds the sample of minute `i` if there is one and NULL otherwise.
--   gap_mid:    minutes [0, 3100) and [6981, 10081): with 8192 cells and 4311 buckets the indexes from 8192 wrap.
--   gap_sparse: minutes [0, 1500) and [8581, 10081): the wrapping indexes land on a map below half full.
--   gap_late:   minutes [0, 6000) and [10000, 10081): the map is 3/4 full when the wrapping indexes arrive.
--   late_start: minutes [4000, 10081): the indexes wrap without colliding, so the map keeps filling to 3/4.
CREATE TABLE grid_gap_samples (shape String, part UInt8, timestamp DateTime64(3), value Float64)
ENGINE = MergeTree ORDER BY (shape, timestamp);

INSERT INTO grid_gap_samples
SELECT shape, m % 2, toDateTime64(m * 60, 3), m
FROM
(
    SELECT number AS m, arrayJoin(['gap_mid', 'gap_sparse', 'gap_late', 'late_start']) AS shape
    FROM numbers(10081)
)
WHERE (shape = 'gap_mid' AND (m < 3100 OR m >= 6981))
   OR (shape = 'gap_sparse' AND (m < 1500 OR m >= 8581))
   OR (shape = 'gap_late' AND (m < 6000 OR m >= 10000))
   OR (shape = 'late_start' AND m >= 4000);

-- The expected grid of each shape (no sample is NaN, so NaN marks the empty points).
DROP VIEW IF EXISTS grid_gap_expected;
CREATE VIEW grid_gap_expected AS
SELECT shape, arrayMap(x -> if(isNaN(x), NULL, x), groupArrayInsertAt(nan, 10081)(value, toUInt64(value))) AS grid
FROM grid_gap_samples GROUP BY shape;

SELECT '-- add (one row per sample)';
SELECT shape, r = grid
FROM (SELECT shape, timeSeriesLastToGrid(0, 604800, 60, 60)(timestamp, value) AS r FROM grid_gap_samples GROUP BY shape) AS a
JOIN grid_gap_expected USING shape
ORDER BY shape
SETTINGS max_threads = 1;

SELECT '-- batch add (arrays of samples)';
SELECT shape, r = grid
FROM
(
    SELECT shape, timeSeriesLastToGrid(0, 604800, 60, 60)(ts, vs) AS r
    FROM (SELECT shape, groupArray(timestamp) AS ts, groupArray(value) AS vs FROM grid_gap_samples GROUP BY shape) AS b
    GROUP BY shape
) AS a
JOIN grid_gap_expected USING shape
ORDER BY shape;

SELECT '-- merge of two partial states (odd and even minutes)';
SELECT shape, r = grid
FROM
(
    SELECT shape, timeSeriesLastToGridMerge(0, 604800, 60, 60)(s) AS r
    FROM (SELECT shape, part, timeSeriesLastToGridState(0, 604800, 60, 60)(timestamp, value) AS s FROM grid_gap_samples GROUP BY shape, part) AS b
    GROUP BY shape
) AS a
JOIN grid_gap_expected USING shape
ORDER BY shape;

SELECT '-- states written to a table and read back (deserialize)';
CREATE TABLE grid_gap_states (shape String, s AggregateFunction(timeSeriesLastToGrid(0, 604800, 60, 60), DateTime64(3), Float64))
ENGINE = MergeTree ORDER BY shape;
INSERT INTO grid_gap_states
SELECT shape, timeSeriesLastToGridState(0, 604800, 60, 60)(timestamp, value) FROM grid_gap_samples GROUP BY shape;
SELECT shape, finalizeAggregation(s) = grid
FROM grid_gap_states
JOIN grid_gap_expected USING shape
ORDER BY shape;

SELECT '-- rate over the same shapes: one value per populated bucket pair, NULL across the gap';
SELECT shape, countIf(isNotNull(x)), countIf(isNull(x))
FROM
(
    SELECT shape, arrayJoin(timeSeriesRateToGrid(0, 604800, 60, 120)(timestamp, value)) AS x
    FROM grid_gap_samples GROUP BY shape
) AS a
GROUP BY shape
ORDER BY shape;

DROP VIEW grid_gap_expected;
DROP TABLE grid_gap_states;
DROP TABLE grid_gap_samples;
