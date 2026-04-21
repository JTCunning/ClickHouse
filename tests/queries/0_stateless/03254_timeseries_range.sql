SELECT timeSeriesRange('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:01:30.000'::DateTime64(3), 30);
SELECT timeSeriesRange('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:00:01.000'::DateTime64(3), '0.10'::Decimal64(3));
SELECT timeSeriesRange('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:00:30.000'::DateTime64(3), 30);
SELECT timeSeriesRange('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:00:00.000'::DateTime64(3), 30);
SELECT timeSeriesRange('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:00:00.000'::DateTime64(3), 0);

-- Different scales
SELECT timeSeriesRange('2025-06-01 00:00:00.0'::DateTime64(1), '2025-06-01 00:00:01.00'::DateTime64(2), '0.123'::Decimal64(3));

-- Wrong range: end_timestamp < start_timestamp
SELECT timeSeriesRange('2025-06-01 00:01:00'::DateTime64(3), '2025-06-01 00:00:00.000'::DateTime64(3), 30); -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesRange('2025-06-01 00:01:00'::DateTime64(3), '2025-06-01 00:00:00.000'::DateTime64(3), -30); -- {serverError BAD_ARGUMENTS}

-- Wrong step
SELECT timeSeriesRange('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:00:50.000'::DateTime64(3), 0); -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesRange('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:00:50.000'::DateTime64(3), -10); -- {serverError BAD_ARGUMENTS}

-- timeSeriesFromGrid without NULLs
SELECT timeSeriesFromGrid('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:01:30.000'::DateTime64(3), 30, [100, 200, 300, 400]);

-- timeSeriesFromGrid with NULLs
SELECT timeSeriesFromGrid('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:01:30.000'::DateTime64(3), 30, [100, 200, NULL, 400]);
SELECT timeSeriesFromGrid('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:03:00.000'::DateTime64(3), 30, [100, NULL, 300, NULL, NULL, 600, NULL]);

-- Wrong number of values
SELECT timeSeriesFromGrid('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:01:30.000'::DateTime64(3), 30, [10, 20, 30]); -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesFromGrid('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:01:30.000'::DateTime64(3), 30, [10, 20, 30, 40, 50]); -- {serverError BAD_ARGUMENTS}

-- Empty input array yields an empty per-row time series instead of throwing.
-- Regression guard for /api/v1/query_range over an aggregation that produced zero samples
-- (e.g. count(nonexistent_metric_name)).
SELECT timeSeriesFromGrid('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:01:30.000'::DateTime64(3), 30, []::Array(Nullable(Float64)));
SELECT timeSeriesFromGrid('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:01:30.000'::DateTime64(3), 30, []::Array(Float64));

-- Per-row empty must not poison sibling rows in the same block.
SELECT timeSeriesFromGrid('2025-06-01 00:00:00'::DateTime64(3), '2025-06-01 00:01:30.000'::DateTime64(3), 30, v)
FROM (SELECT arrayJoin([[100., 200., 300., 400.], []::Array(Float64), [10., 20., 30., 40.]]) AS v);
