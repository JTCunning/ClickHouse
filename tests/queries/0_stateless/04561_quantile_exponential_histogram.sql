-- Tests for quantileExponentialHistogram / quantilesExponentialHistogram.
--
-- The functions compute quantiles from pre-bucketed data following the exponential bucketing
-- convention of OpenTelemetry exponential histograms / Prometheus native histograms:
-- at schema `s`, bucket `i` covers [2^(i/2^s), 2^((i+1)/2^s)), a value v > 0 maps to bucket
-- floor(log2(v) * 2^s), and zero values are counted in the reserved sentinel bucket -1.

DROP TABLE IF EXISTS raw_requests;
DROP TABLE IF EXISTS latency_histogram;

-- Raw events: deterministic synthetic latency distribution plus zero-latency cache hits.
CREATE TABLE raw_requests
(
    ts DateTime, service LowCardinality(String), latency_ms Float64
) ENGINE = MergeTree ORDER BY (service, ts);

INSERT INTO raw_requests
SELECT toDateTime('2026-07-15 10:00:00') + number % 120, 'api',
       round(exp(0.7 * (number % 97) / 10) + (number % 13), 2)
FROM numbers(100000);
INSERT INTO raw_requests   -- zero-latency cache hits -> sentinel bucket -1
SELECT toDateTime('2026-07-15 10:00:00') + number % 120, 'api', 0 FROM numbers(5000);

-- Pre-bucketed histogram rows (schema 3: 8 buckets per doubling).
CREATE TABLE latency_histogram
(
    service LowCardinality(String), minute DateTime,
    bucket_index Int64, bucket_count UInt64
) ENGINE = SummingMergeTree(bucket_count) ORDER BY (service, minute, bucket_index);

INSERT INTO latency_histogram
SELECT service, toStartOfMinute(ts),
       if(latency_ms = 0, -1, toInt64(floor(log2(latency_ms) * 8))), count()
FROM raw_requests GROUP BY 1, 2, 3;

SELECT 'quantiles from pre-bucketed rows';
SELECT
    round(quantileExponentialHistogram(3, 0.50)(bucket_index, bucket_count), 2) AS p50,
    round(quantileExponentialHistogram(3, 0.90)(bucket_index, bucket_count), 2) AS p90,
    round(quantileExponentialHistogram(3, 0.99)(bucket_index, bucket_count), 2) AS p99
FROM latency_histogram;

SELECT 'plural form computes the same levels in one pass';
SELECT arrayMap(x -> round(x, 2), quantilesExponentialHistogram(3, 0.5, 0.9, 0.99)(bucket_index, bucket_count))
FROM latency_histogram;

SELECT 'errors stay within the schema-3 bucket width (~9%) of the exact quantiles';
SELECT
    abs(h50 - e50) / e50 < 0.09,
    abs(h90 - e90) / e90 < 0.09,
    abs(h99 - e99) / e99 < 0.09
FROM
(
    SELECT
        quantileExponentialHistogram(3, 0.50)(bucket_index, bucket_count) AS h50,
        quantileExponentialHistogram(3, 0.90)(bucket_index, bucket_count) AS h90,
        quantileExponentialHistogram(3, 0.99)(bucket_index, bucket_count) AS h99
    FROM latency_histogram
) AS h,
(
    SELECT
        quantileExact(0.50)(latency_ms) AS e50,
        quantileExact(0.90)(latency_ms) AS e90,
        quantileExact(0.99)(latency_ms) AS e99
    FROM raw_requests
) AS e;

SELECT 'geometric interpolation, hand-checkable';
-- Target rank 0.9 * 1000 = 900. Cumulative walk: cum(-1) = 500, cum(8) = 900, cum(16) = 1000.
-- The target is crossed at bucket 8 with fraction (900 - 500) / 400 = 1, giving 2^((8+1)/8) = 2.181015...
SELECT round(quantileExponentialHistogram(3, 0.9)(bucket_index, bucket_count), 6)
FROM VALUES('bucket_index Int64, bucket_count UInt64', (-1, 500), (8, 400), (16, 100));

SELECT 'zero sentinel: target rank inside bucket -1 returns 0';
SELECT quantileExponentialHistogram(3, 0.5)(bucket_index, bucket_count)
FROM VALUES('bucket_index Int64, bucket_count UInt64', (-1, 90), (0, 10));

SELECT 'all zeros: level 1 returns upper bound of the zero bucket, which is 0';
SELECT quantileExponentialHistogram(3)(bucket_index, bucket_count),
       quantileExponentialHistogram(3, 1.0)(bucket_index, bucket_count)
FROM VALUES('bucket_index Int64, bucket_count UInt64', (-1, 100));

SELECT 'level 1 returns the upper bound of the last bucket: 2^((16+1)/8)';
SELECT round(quantileExponentialHistogram(3, 1.0)(bucket_index, bucket_count), 6)
FROM VALUES('bucket_index Int64, bucket_count UInt64', (0, 50), (16, 50));

SELECT 'level 0 returns the lower bound of the first bucket: 2^(0/8) = 1';
SELECT quantileExponentialHistogram(3, 0.0)(bucket_index, bucket_count)
FROM VALUES('bucket_index Int64, bucket_count UInt64', (0, 50), (16, 50));

SELECT 'negative bucket indexes are legitimate for values below 1';
-- Bucket -3 at schema 3 covers [2^(-3/8), 2^(-2/8)) = [0.7711, 0.8409); median of a single bucket
-- interpolates to 2^((-3 + 0.5)/8) = 2^(-0.3125) = 0.805245.
SELECT round(quantileExponentialHistogram(3, 0.5)(bucket_index, bucket_count), 6)
FROM VALUES('bucket_index Int64, bucket_count UInt64', (-3, 100));

SELECT 'empty input returns NaN';
SELECT quantileExponentialHistogram(3, 0.5)(bucket_index, bucket_count),
       quantilesExponentialHistogram(3, 0.5, 0.9)(bucket_index, bucket_count)
FROM (SELECT toInt64(1) AS bucket_index, toUInt64(1) AS bucket_count WHERE 0);

SELECT 'float bucket counts are supported';
SELECT round(quantileExponentialHistogram(3, 0.9)(bucket_index, bucket_count), 6)
FROM VALUES('bucket_index Int64, bucket_count Float64', (-1, 500.), (8, 400.), (16, 100.));

SELECT 'unsigned bucket indexes are supported';
SELECT round(quantileExponentialHistogram(3, 1.0)(bucket_index, bucket_count), 6)
FROM VALUES('bucket_index UInt16, bucket_count UInt64', (0, 50), (16, 50));

SELECT 'schema 1 and schema 8 grids';
SELECT round(quantileExponentialHistogram(1, 1.0)(bucket_index, bucket_count), 6),
       round(quantileExponentialHistogram(8, 1.0)(bucket_index, bucket_count), 6)
FROM VALUES('bucket_index Int64, bucket_count UInt64', (1, 100));

SELECT 'states merge by per-bucket addition';
SELECT round(quantileExponentialHistogramMerge(3, 0.9)(state), 6)
FROM
(
    SELECT quantileExponentialHistogramState(3, 0.9)(bucket_index, bucket_count) AS state
    FROM VALUES('bucket_index Int64, bucket_count UInt64', (-1, 250), (8, 200))
    UNION ALL
    SELECT quantileExponentialHistogramState(3, 0.9)(bucket_index, bucket_count)
    FROM VALUES('bucket_index Int64, bucket_count UInt64', (-1, 250), (8, 200), (16, 100))
);

SELECT 'singular and plural variants share the same state representation';
SELECT arrayMap(x -> round(x, 6), quantilesExponentialHistogramMerge(3, 0.9)(state))
FROM
(
    SELECT quantileExponentialHistogramState(3, 0.9)(bucket_index, bucket_count) AS state
    FROM VALUES('bucket_index Int64, bucket_count UInt64', (-1, 500), (8, 400), (16, 100))
);

SELECT 'validation errors';
SELECT quantileExponentialHistogram(0, 0.5)(bucket_index, bucket_count)
FROM VALUES('bucket_index Int64, bucket_count UInt64', (0, 1)); -- { serverError BAD_ARGUMENTS }
SELECT quantileExponentialHistogram(9, 0.5)(bucket_index, bucket_count)
FROM VALUES('bucket_index Int64, bucket_count UInt64', (0, 1)); -- { serverError BAD_ARGUMENTS }
SELECT quantileExponentialHistogram(-1, 0.5)(bucket_index, bucket_count)
FROM VALUES('bucket_index Int64, bucket_count UInt64', (0, 1)); -- { serverError BAD_ARGUMENTS }
SELECT quantileExponentialHistogram(3.5, 0.5)(bucket_index, bucket_count)
FROM VALUES('bucket_index Int64, bucket_count UInt64', (0, 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT quantileExponentialHistogram(0.5)(bucket_index, bucket_count)
FROM VALUES('bucket_index Int64, bucket_count UInt64', (0, 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT quantileExponentialHistogram(3, 0.5, 0.9)(bucket_index, bucket_count)
FROM VALUES('bucket_index Int64, bucket_count UInt64', (0, 1)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT quantileExponentialHistogram(3, 0.5)(bucket_index)
FROM VALUES('bucket_index Int64', (0)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT quantileExponentialHistogram(3, 0.5)(bucket_index, bucket_count)
FROM VALUES('bucket_index Float64, bucket_count UInt64', (0., 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT quantileExponentialHistogram(3, 0.5)(bucket_index, bucket_count)
FROM VALUES('bucket_index Int64, bucket_count String', (0, '1')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT quantilesExponentialHistogram(3)(bucket_index, bucket_count)
FROM VALUES('bucket_index Int64, bucket_count UInt64', (0, 1)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT quantileExponentialHistogram(3, 1.5)(bucket_index, bucket_count)
FROM VALUES('bucket_index Int64, bucket_count UInt64', (0, 1)); -- { serverError PARAMETER_OUT_OF_BOUND }

DROP TABLE raw_requests;
DROP TABLE latency_histogram;
