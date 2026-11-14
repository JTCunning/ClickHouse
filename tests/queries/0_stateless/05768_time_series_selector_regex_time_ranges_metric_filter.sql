-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A selector whose `__name__` regex isn't a list of literal names has no id ranges. Then the time ranges table is read with
-- `tupleElement(id, 1) IN (SELECT tupleElement(id, 1) FROM <tags table> WHERE <matchers on __name__>)`: the first component
-- of `id` is the hash of the metric name, so the primary key skips the granules of the other metrics. The selected samples
-- must be the same as with the list of the same names, including for time series with identifiers from another id generator.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries
TIME RANGES INNER ENGINE = AggregatingMergeTree ORDER BY id SETTINGS index_granularity = 128;

-- 'foo' and 'bar' have a series with samples inside the selected time range [100, 300] and a series with samples only after it.
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1), (toDateTime64(200, 3), 2)]),
    ('foo', map('env', 'dev'), [(toDateTime64(1000, 3), 3)]),
    ('bar', map('env', 'prod'), [(toDateTime64(300, 3), 4)]),
    ('bar', map('env', 'dev'), [(toDateTime64(2000, 3), 5)]),
    ('baz', map('env', 'prod'), [(toDateTime64(150, 3), 6)]);

-- A series of 'foo' with an identifier from another id generator, inside the selected time range.
-- The table name is qualified because an asynchronous insert through a table function resolves an unqualified name in the default database.
INSERT INTO FUNCTION timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) (id, metric_name, tags) VALUES
    ((1, '00000000-0000-0000-0000-000000000001'), 'foo', map('env', 'other_id_inside'));
INSERT INTO FUNCTION timeSeriesTimeRanges({CLICKHOUSE_DATABASE:Identifier}.ts) (id, min_time, max_time) VALUES
    ((1, '00000000-0000-0000-0000-000000000001'), toDateTime64(250, 3), toDateTime64(250, 3));
INSERT INTO FUNCTION timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) (id, samples, bucket, min_time, max_time) VALUES
    ((1, '00000000-0000-0000-0000-000000000001'), [(toDateTime64(250, 3), 8.)], toDateTime64(0, 3), toDateTime64(250, 3), toDateTime64(250, 3));

-- 10000 time ranges of another metric inside the selected time range, in about 80 granules of their own.
-- They have no tags, so no selector returns them; only the rows read from the time ranges table depend on them.
INSERT INTO FUNCTION timeSeriesTimeRanges({CLICKHOUSE_DATABASE:Identifier}.ts) (id, min_time, max_time)
SELECT (sipHash64('qux'), generateUUIDv4(number)), toDateTime64(150, 3), toDateTime64(150, 3) FROM numbers(10000);

SELECT '-- regular expression, no id ranges';
SELECT timestamp, value FROM timeSeriesSelector(ts, '{__name__=~"fo.|ba[r]"}', 100, 300, 'timestamp_and_value') ORDER BY timestamp, value
SETTINGS log_comment = '05768_regex', enable_parallel_replicas = 0;

SELECT '-- list of the same names, id ranges';
SELECT timestamp, value FROM timeSeriesSelector(ts, '{__name__=~"foo|bar"}', 100, 300, 'timestamp_and_value') ORDER BY timestamp, value
SETTINGS log_comment = '05768_list', enable_parallel_replicas = 0;

SELECT '-- no matcher on __name__';
SELECT timestamp, value FROM timeSeriesSelector(ts, '{env="prod"}', 100, 300, 'timestamp_and_value') ORDER BY timestamp, value
SETTINGS log_comment = '05768_no_name', enable_parallel_replicas = 0;

SELECT '-- only a negative matcher on __name__';
SELECT timestamp, value FROM timeSeriesSelector(ts, '{__name__!~"ba.", env="prod"}', 100, 300, 'timestamp_and_value') ORDER BY timestamp, value
SETTINGS log_comment = '05768_negative_name', enable_parallel_replicas = 0;

-- No filter when the first component of `id` doesn't depend on the metric name only (a set per time series could cost more
-- than it saves), or when `id` isn't a tuple: the same samples.
DROP TABLE IF EXISTS ts_uint64;
CREATE TABLE ts_uint64 ENGINE = TimeSeries TAGS INNER COLUMNS (id UInt64);
DROP TABLE IF EXISTS ts_tags_hash;
CREATE TABLE ts_tags_hash ENGINE = TimeSeries
TAGS INNER COLUMNS (id Tuple(UInt64, LowCardinality(UUID)) DEFAULT tuple(sipHash64(tags), toLowCardinality(reinterpretAsUUID(sipHash128(metric_name, tags)))));

INSERT INTO ts_uint64 (metric_name, tags, samples) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1)]),
    ('bar', map('env', 'prod'), [(toDateTime64(300, 3), 4)]),
    ('baz', map('env', 'prod'), [(toDateTime64(150, 3), 6)]);
INSERT INTO ts_tags_hash SELECT * FROM ts_uint64;

SELECT '-- regular expression, id is not a tuple';
SELECT timestamp, value FROM timeSeriesSelector(ts_uint64, '{__name__=~"fo.|ba[r]"}', 100, 300, 'timestamp_and_value') ORDER BY timestamp, value
SETTINGS log_comment = '05768_uint64_regex', enable_parallel_replicas = 0;

SELECT '-- regular expression, the first component of id is a hash of the tags';
SELECT timestamp, value FROM timeSeriesSelector(ts_tags_hash, '{__name__=~"fo.|ba[r]"}', 100, 300, 'timestamp_and_value') ORDER BY timestamp, value
SETTINGS log_comment = '05768_tags_hash_regex', enable_parallel_replicas = 0;

SELECT '-- generated query: the filter by the first component of id';
SYSTEM FLUSH LOGS query_log, text_log;
SELECT q.log_comment, countSubstrings(t.message, 'in(tupleElement(id, 1), (SELECT tupleElement(id, 1) FROM ') AS metric_filters
FROM system.text_log AS t
INNER JOIN
(
    SELECT query_id, log_comment FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND startsWith(log_comment, '05768_')
) AS q USING (query_id)
WHERE t.event_date >= yesterday() AND t.logger_name = 'StorageTimeSeriesSelector' AND startsWith(t.message, 'Will execute query:')
ORDER BY q.log_comment
SETTINGS max_rows_to_read = 0; -- system.text_log can be really big

SELECT '-- the regular expression skips the granules of the other metric, as the list does';
SELECT
    sumIf(read_rows, log_comment = '05768_no_name') - sumIf(read_rows, log_comment = '05768_regex') > 9000 AS regex_skips_other_metric,
    sumIf(read_rows, log_comment = '05768_no_name') - sumIf(read_rows, log_comment = '05768_list') > 9000 AS list_skips_other_metric
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05768_regex', '05768_list', '05768_no_name')
SETTINGS max_rows_to_read = 0; -- system.query_log can be really big

DROP TABLE ts_tags_hash;
DROP TABLE ts_uint64;
DROP TABLE ts;
