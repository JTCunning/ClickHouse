-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- When the metric name of a selector is a literal or a list of alternatives, the time ranges table is read by
-- the id ranges of the selected metrics. The comparisons with the bounds of the ranges are wrapped in `indexHint`,
-- so the primary key uses them without evaluating them for every row, and the rows are filtered by the first
-- component of `id` instead. The selected samples must be the same as with a regular expression on the metric name,
-- which has no id ranges, including for time series with identifiers outside the ranges (written with another id generator).

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

-- Every metric has a series with samples inside the selected time range [100, 300] and a series with samples only after it.
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1), (toDateTime64(200, 3), 2)]),
    ('foo', map('env', 'dev'), [(toDateTime64(1000, 3), 3)]),
    ('bar', map('env', 'prod'), [(toDateTime64(300, 3), 4)]),
    ('bar', map('env', 'dev'), [(toDateTime64(2000, 3), 5)]),
    ('baz', map('env', 'prod'), [(toDateTime64(150, 3), 6)]),
    ('baz', map('env', 'dev'), [(toDateTime64(3000, 3), 7)]);

-- Two series of 'foo' with identifiers outside the id range of 'foo': one inside the selected time range, one after it.
-- The table name is qualified because an asynchronous insert through a table function resolves an unqualified name in the default database.
INSERT INTO FUNCTION timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) (id, metric_name, tags) VALUES
    ((1, '00000000-0000-0000-0000-000000000001'), 'foo', map('env', 'other_id_inside')),
    ((2, '00000000-0000-0000-0000-000000000002'), 'foo', map('env', 'other_id_after'));

INSERT INTO FUNCTION timeSeriesTimeRanges({CLICKHOUSE_DATABASE:Identifier}.ts) (id, min_time, max_time) VALUES
    ((1, '00000000-0000-0000-0000-000000000001'), toDateTime64(250, 3), toDateTime64(250, 3)),
    ((2, '00000000-0000-0000-0000-000000000002'), toDateTime64(4000, 3), toDateTime64(4000, 3));

-- All the samples belong to the bucket starting at 0 (the default bucket step is 1 hour).
INSERT INTO FUNCTION timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) (id, samples, bucket, min_time, max_time) VALUES
    ((1, '00000000-0000-0000-0000-000000000001'), [(toDateTime64(250, 3), 8.)], toDateTime64(0, 3), toDateTime64(250, 3), toDateTime64(250, 3)),
    ((2, '00000000-0000-0000-0000-000000000002'), [(toDateTime64(4000, 3), 9.)], toDateTime64(3600, 3), toDateTime64(4000, 3), toDateTime64(4000, 3));

SELECT '-- one metric';
SELECT timestamp, value FROM timeSeriesSelector(ts, 'foo', 100, 300, 'timestamp_and_value') ORDER BY timestamp, value
SETTINGS log_comment = '05479_one_metric';

SELECT '-- list of alternatives';
SELECT timestamp, value FROM timeSeriesSelector(ts, '{__name__=~"foo|bar"}', 100, 300, 'timestamp_and_value') ORDER BY timestamp, value
SETTINGS log_comment = '05479_alternatives';

SELECT '-- regular expression, no id ranges';
SELECT timestamp, value FROM timeSeriesSelector(ts, '{__name__=~"fo.|ba[r]"}', 100, 300, 'timestamp_and_value') ORDER BY timestamp, value
SETTINGS log_comment = '05479_regex';

SELECT '-- generated query: index hints, filters by the first component of id';

-- The outer WHERE of the tags query has one filter by the first component of `id` (the time series outside the ranges);
-- the time ranges subquery adds the second one next to the `indexHint`.
SYSTEM FLUSH LOGS query_log, text_log;
SELECT q.log_comment, countSubstrings(t.message, 'indexHint(') AS index_hints, countSubstrings(t.message, '(tupleElement(id, 1), ') AS first_component_filters
FROM system.text_log AS t
INNER JOIN
(
    SELECT query_id, log_comment FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND startsWith(log_comment, '05479_')
) AS q USING (query_id)
WHERE t.event_date >= yesterday() AND t.logger_name = 'StorageTimeSeriesSelector' AND startsWith(t.message, 'Will execute query:')
ORDER BY q.log_comment;

DROP TABLE ts;
