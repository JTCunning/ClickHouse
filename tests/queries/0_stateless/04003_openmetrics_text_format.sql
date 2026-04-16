SELECT 'm' AS name, 1. AS value, '' AS help, '' AS type, CAST(map(), 'Map(String, String)') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit
FORMAT OpenMetrics;

SELECT 'm' AS name, 2. AS value, 'h' AS help, 'gauge' AS type, map('x', 'y') AS labels, CAST(100 AS Nullable(Int64)) AS timestamp, 'u' AS unit
FORMAT OpenMetrics;

SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String',
$$
# HELP x help_text
# TYPE x counter
# UNIT x by_1
x 42 999
# EOF
$$
)
FORMAT TSV;

-- Tab is valid whitespace between metric descriptor and value (OpenMetrics / Prometheus text).
SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String',
    concat('tabbed', char(9), '7', char(10), '# EOF', char(10))
)
FORMAT TSV;

-- Escaped newline and quote inside label value (\\n, \", \\ per exposition format).
SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String',
    concat('esc{k="a', char(92), 'n', 'b"} 1', char(10), '# EOF', char(10))
)
FORMAT TSV;

SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String',
    concat('quot{k="a', char(92), '"', 'b"} 1', char(10), '# EOF', char(10))
)
FORMAT TSV;

-- Reject trailing garbage after sample value / timestamp (no silent truncation).
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 abc', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 2 extra', char(10))); -- { serverError INCORRECT_DATA }

-- Reject empty metric name and duplicate label keys (invalid exposition text).
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('{k="v"} 1', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m{a="1",a="2"} 1', char(10))); -- { serverError INCORRECT_DATA }

-- Reject malformed float sample tokens (no partial parse, e.g. 1abc must not become 1).
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1abc', char(10))); -- { serverError INCORRECT_DATA }

-- Reject trailing payload after logical end (# EOF).
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('# EOF', char(10), 'm 1', char(10))); -- { serverError INCORRECT_DATA }

-- Reject non-whitespace on the same line after `# EOF` (not a valid logical terminator).
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('# EOF trailing', char(10))); -- { serverError INCORRECT_DATA }

-- Reject malformed timestamp token (`-` without digits) even when the schema has no timestamp column.
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 -', char(10))); -- { serverError INCORRECT_DATA }

-- Reject incompatible declared column types (validated up front like Prometheus output format).
SELECT * FROM format(OpenMetrics, 'name String, value Float64, help UInt64', concat('x 1', char(10))); -- { serverError BAD_ARGUMENTS }

-- Schema must match insertion column types (non-nullable Float64; String not FixedString) — avoid release assert_cast UB.
SELECT * FROM format(OpenMetrics, 'name String, value Nullable(Float64)', concat('x 1', char(10))); -- { serverError BAD_ARGUMENTS }
SELECT * FROM format(OpenMetrics, 'name FixedString(16), value Float64', concat('x 1', char(10))); -- { serverError BAD_ARGUMENTS }

-- Do not fold `..._sum` / `..._count` into the base name unless # TYPE is histogram or summary (counter/gauge can legitimately use those suffixes).
SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String',
    concat('# HELP requests help\n', '# TYPE requests counter\n', 'requests_sum 1\n', '# EOF\n')
)
FORMAT TSV;

-- Histogram/summary `_sum` still maps to the family with synthetic `sum` label.
SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String',
    concat('# HELP h help\n', '# TYPE h histogram\n', 'h_sum 5\n', '# EOF\n')
)
FORMAT TSV;
