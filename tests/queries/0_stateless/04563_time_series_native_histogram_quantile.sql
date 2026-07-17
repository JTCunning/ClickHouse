-- End-to-end SQL query path for native (exponential) histograms stored in the
-- "histograms" target of a TimeSeries table: counter-delta over a time window,
-- one-index shift from the Prometheus bucket convention to the
-- quantileExponentialHistogram grid, zero bucket mapped to the sentinel -1.
--
-- Prometheus positive bucket i at schema s covers (2^((i-1)/2^s), 2^(i/2^s)],
-- while quantileExponentialHistogram bucket j covers [2^(j/2^s), 2^((j+1)/2^s)),
-- so a stored index i maps to aggregate-function index i - 1.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_04563;
CREATE TABLE ts_04563 ENGINE = TimeSeries;

-- Two cumulative (counter) native-histogram samples of one series, 60 s apart,
-- as the Prometheus remote-write protocol would store them: schema 3,
-- zero bucket plus sparse positive buckets (bucket 24 appears only in the
-- later sample, i.e. its cumulative count was 0 at the start of the window).
-- The insert qualifies the database explicitly: with async inserts the query is executed
-- in a global context where the current database is not the test database.
INSERT INTO TABLE FUNCTION timeSeriesHistograms({CLICKHOUSE_DATABASE:Identifier}.ts_04563) VALUES
    ('9e39c66f-fbf2-4e0f-9078-df9d1e9930cb', '2026-07-15 10:00:00.000', 3, 150, 1000, 0.001, 50, [1, 9, 17], [40, 40, 20], [], [], 0),
    ('9e39c66f-fbf2-4e0f-9078-df9d1e9930cb', '2026-07-15 10:01:00.000', 3, 1550, 9000, 0.001, 550, [1, 9, 17, 24], [440, 340, 120, 100], [], [], 0);

SELECT 'counter-delta quantiles over the window';
-- Window delta per bucket (last minus first, absent = 0):
--   zero bucket: 550 - 50 = 500      -> sentinel index -1
--   bucket 1:    440 - 40 = 400      -> index 0
--   bucket 9:    340 - 40 = 300      -> index 8
--   bucket 17:   120 - 20 = 100      -> index 16
--   bucket 24:   100 - 0  = 100      -> index 23
-- Total 1400. p50 target 700: crossed in index 0 with fraction 0.5 -> 2^(0.5/8).
-- p90 target 1260: crossed in index 16 with fraction 0.6 -> 2^(16.6/8).
WITH
    (
        SELECT argMin((positive_bucket_indexes, positive_bucket_counts, zero_count), timestamp)
        FROM timeSeriesHistograms(ts_04563)
        WHERE timestamp BETWEEN '2026-07-15 10:00:00' AND '2026-07-15 10:01:00'
    ) AS first_sample,
    (
        SELECT argMax((positive_bucket_indexes, positive_bucket_counts, zero_count), timestamp)
        FROM timeSeriesHistograms(ts_04563)
        WHERE timestamp BETWEEN '2026-07-15 10:00:00' AND '2026-07-15 10:01:00'
    ) AS last_sample
SELECT
    round(quantileExponentialHistogram(3, 0.5)(bucket_index, bucket_count), 6) AS p50,
    round(quantileExponentialHistogram(3, 0.9)(bucket_index, bucket_count), 6) AS p90
FROM
(
    -- Positive buckets: last minus first per index (buckets absent from a sample count as 0),
    -- shifted by one index into the quantileExponentialHistogram grid.
    SELECT bucket.1 - 1 AS bucket_index, sum(bucket.2) AS bucket_count
    FROM
    (
        SELECT arrayJoin(arrayZip(last_sample.1, last_sample.2)) AS bucket
        UNION ALL
        SELECT arrayJoin(arrayZip(first_sample.1, arrayMap(c -> -c, first_sample.2)))
    )
    GROUP BY bucket.1
    UNION ALL
    -- Zero bucket: mapped to the sentinel index -1.
    SELECT -1, last_sample.3 - first_sample.3
);

SELECT 'same result via the plural form';
WITH
    (
        SELECT argMin((positive_bucket_indexes, positive_bucket_counts, zero_count), timestamp)
        FROM timeSeriesHistograms(ts_04563)
    ) AS first_sample,
    (
        SELECT argMax((positive_bucket_indexes, positive_bucket_counts, zero_count), timestamp)
        FROM timeSeriesHistograms(ts_04563)
    ) AS last_sample
SELECT arrayMap(x -> round(x, 6), quantilesExponentialHistogram(3, 0.5, 0.9)(bucket_index, bucket_count))
FROM
(
    SELECT bucket.1 - 1 AS bucket_index, sum(bucket.2) AS bucket_count
    FROM
    (
        SELECT arrayJoin(arrayZip(last_sample.1, last_sample.2)) AS bucket
        UNION ALL
        SELECT arrayJoin(arrayZip(first_sample.1, arrayMap(c -> -c, first_sample.2)))
    )
    GROUP BY bucket.1
    UNION ALL
    SELECT -1, last_sample.3 - first_sample.3
);

SELECT 'instant quantile from a single histogram sample';
-- The latest sample alone (no counter delta): cumulative distribution at that point.
-- Total 1550: zero 550, idx 0: 440, idx 8: 340, idx 16: 120, idx 23: 100.
-- p50 target 775: crossed in index 0 with fraction (775-550)/440 -> 2^((0 + 225/440)/8).
WITH
    (
        SELECT argMax((positive_bucket_indexes, positive_bucket_counts, zero_count), timestamp)
        FROM timeSeriesHistograms(ts_04563)
    ) AS last_sample
SELECT round(quantileExponentialHistogram(3, 0.5)(bucket_index, bucket_count), 6)
FROM
(
    SELECT bucket.1 - 1 AS bucket_index, bucket.2 AS bucket_count
    FROM (SELECT arrayJoin(arrayZip(last_sample.1, last_sample.2)) AS bucket)
    UNION ALL
    SELECT -1, last_sample.3
);

DROP TABLE ts_04563;
