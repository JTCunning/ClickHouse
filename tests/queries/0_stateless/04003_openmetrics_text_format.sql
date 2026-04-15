SELECT 'm' AS name, 1. AS value, '' AS help, '' AS type, map() AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit
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
