-- NULL values in Nullable start, end, or step arguments are rejected with BAD_ARGUMENTS.
SELECT timeSeriesRange(if(number = 0, NULL, toDateTime64('2025-06-01 00:00:00', 3, 'UTC')), toDateTime64('2025-06-01 00:01:00', 3, 'UTC'), toDecimal64(0.001, 3)) FROM numbers(2); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesRange(toDateTime64('2025-06-01 00:00:00', 3, 'UTC'), if(number = 0, NULL, toDateTime64('2025-06-01 00:01:00', 3, 'UTC')), 30) FROM numbers(2); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesRange(toDateTime64('2025-06-01 00:00:00', 3, 'UTC'), toDateTime64('2025-06-01 00:01:00', 3, 'UTC'), if(number = 0, NULL, 30)) FROM numbers(2); -- { serverError BAD_ARGUMENTS }

SELECT timeSeriesFromGrid(if(number = 0, NULL, toDateTime64('2025-06-01 00:00:00', 3, 'UTC')), toDateTime64('2025-06-01 00:01:00', 3, 'UTC'), 30, [1, 2, 3]) FROM numbers(2); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesFromGrid(toDateTime64('2025-06-01 00:00:00', 3, 'UTC'), if(number = 0, NULL, toDateTime64('2025-06-01 00:01:00', 3, 'UTC')), 30, [1, 2, 3]) FROM numbers(2); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesFromGrid(toDateTime64('2025-06-01 00:00:00', 3, 'UTC'), toDateTime64('2025-06-01 00:01:00', 3, 'UTC'), if(number = 0, NULL, 30), [1, 2, 3]) FROM numbers(2); -- { serverError BAD_ARGUMENTS }

-- Constant Nullable(T) with NULL value is also rejected.
SELECT timeSeriesRange(CAST(NULL AS Nullable(DateTime64(3, 'UTC'))), toDateTime64('2025-06-01 00:01:00', 3, 'UTC'), 30); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesRange(toDateTime64('2025-06-01 00:00:00', 3, 'UTC'), CAST(NULL AS Nullable(DateTime64(3, 'UTC'))), 30); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesRange(toDateTime64('2025-06-01 00:00:00', 3, 'UTC'), toDateTime64('2025-06-01 00:01:00', 3, 'UTC'), CAST(NULL AS Nullable(UInt32))); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesFromGrid(CAST(NULL AS Nullable(DateTime64(3, 'UTC'))), toDateTime64('2025-06-01 00:01:00', 3, 'UTC'), 30, [1, 2, 3]); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesRange(CAST(NULL AS LowCardinality(Nullable(DateTime('UTC')))), toDateTime('2025-06-01 00:01:00', 'UTC'), 30) SETTINGS allow_suspicious_low_cardinality_types = 1; -- { serverError BAD_ARGUMENTS }

-- Invalid Nullable nested types are rejected.
SELECT timeSeriesRange(CAST('x' AS Nullable(String)), toDateTime64('2025-06-01 00:01:00', 3, 'UTC'), 30); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Nullable(T) arguments without NULL values succeed and return non-nullable arrays.
SELECT toTypeName(r), r FROM (SELECT timeSeriesRange(toNullable(toDateTime64('2025-06-01 00:00:00', 3, 'UTC')), toNullable(toDateTime64('2025-06-01 00:01:00', 3, 'UTC')), toNullable(30)) AS r);
SELECT toTypeName(r), r FROM (SELECT timeSeriesRange(materialize(toNullable(toDateTime64('2025-06-01 00:00:00', 3, 'UTC'))), materialize(toNullable(toDateTime64('2025-06-01 00:01:00', 3, 'UTC'))), materialize(toNullable(30))) AS r);
SELECT toTypeName(r), r FROM (SELECT timeSeriesRange(toLowCardinality(toNullable(toDateTime('2025-06-01 00:00:00', 'UTC'))), toLowCardinality(toNullable(toDateTime('2025-06-01 00:01:00', 'UTC'))), toLowCardinality(toNullable(30))) AS r) SETTINGS allow_suspicious_low_cardinality_types = 1;
SELECT toTypeName(r), r FROM (SELECT timeSeriesFromGrid(toNullable(toDateTime64('2025-06-01 00:00:00', 3, 'UTC')), toNullable(toDateTime64('2025-06-01 00:01:00', 3, 'UTC')), toNullable(30), [1, NULL, 3]) AS r);
SELECT toTypeName(r), r FROM (SELECT timeSeriesFromGrid(materialize(toNullable(toDateTime64('2025-06-01 00:00:00', 3, 'UTC'))), materialize(toNullable(toDateTime64('2025-06-01 00:01:00', 3, 'UTC'))), materialize(toNullable(30)), [1, NULL, 3]) AS r);

-- Empty blocks with Nullable(T) arguments return zero rows without error.
SELECT timeSeriesRange(CAST(NULL AS Nullable(DateTime64(3, 'UTC'))), toDateTime64(number, 3, 'UTC'), 30) FROM numbers(0);
SELECT timeSeriesRange(if(number = 0, NULL, toDateTime64('2025-06-01 00:00:00', 3, 'UTC')), toDateTime64('2025-06-01 00:01:00', 3, 'UTC'), 30) FROM numbers(0);
SELECT timeSeriesFromGrid(CAST(NULL AS Nullable(DateTime64(3, 'UTC'))), toDateTime64(number, 3, 'UTC'), 30, [1]) FROM numbers(0);

-- Bare NULL literals return NULL (Nullable(Nothing)); wrong arity still fails.
SELECT toTypeName(r), r FROM (SELECT timeSeriesRange(NULL, toDateTime64('2025-06-01 00:01:00', 3, 'UTC'), 30) AS r);
SELECT toTypeName(r), r FROM (SELECT timeSeriesRange(toDateTime64('2025-06-01 00:00:00', 3, 'UTC'), NULL, 30) AS r);
SELECT toTypeName(r), r FROM (SELECT timeSeriesRange(toDateTime64('2025-06-01 00:00:00', 3, 'UTC'), toDateTime64('2025-06-01 00:01:00', 3, 'UTC'), NULL) AS r);
SELECT toTypeName(r), r FROM (SELECT timeSeriesFromGrid(NULL, toDateTime64('2025-06-01 00:01:00', 3, 'UTC'), 30, [1, 2, 3]) AS r);
SELECT toTypeName(r), r FROM (SELECT timeSeriesFromGrid(toDateTime64('2025-06-01 00:00:00', 3, 'UTC'), toDateTime64('2025-06-01 00:01:00', 3, 'UTC'), 30, NULL) AS r);
SELECT timeSeriesRange(NULL); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT timeSeriesFromGrid(NULL); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- DateTime or UInt32 start with DateTime64(0) end promotes to DateTime64(0) and does not wrap after 2106.
SELECT toTypeName(r), r FROM (SELECT timeSeriesRange(toDateTime('2106-02-07 06:00:00', 'UTC'), toDateTime64('2106-02-07 07:00:00', 0, 'UTC'), 1800) AS r);
SELECT toTypeName(r), r FROM (SELECT timeSeriesFromGrid(toDateTime('2106-02-07 06:00:00', 'UTC'), toDateTime64('2106-02-07 07:00:00', 0, 'UTC'), 1800, [1, 2, 3]) AS r);
SELECT toTypeName(r), r FROM (SELECT timeSeriesRange(toUInt32(4294965600), toDateTime64(4294969200, 0, 'UTC'), 1800) AS r) SETTINGS session_timezone = 'UTC';
SELECT toTypeName(r), r FROM (SELECT timeSeriesFromGrid(toUInt32(4294965600), toDateTime64(4294969200, 0, 'UTC'), 1800, [1, NULL, 3]) AS r) SETTINGS session_timezone = 'UTC';
SELECT toTypeName(r), r FROM (SELECT timeSeriesRange(toDateTime('2106-02-07 15:00:00', 'Asia/Tokyo'), toDateTime64('2106-02-07 07:00:00', 0, 'UTC'), 1800) AS r) SETTINGS session_timezone = 'UTC';
SELECT toTypeName(r), r FROM (SELECT timeSeriesRange(toNullable(toDateTime('2106-02-07 06:00:00', 'UTC')), toNullable(toDateTime64('2106-02-07 07:00:00', 0, 'UTC')), toNullable(1800)) AS r);
