-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A regular expression on the metric name that is not a list of literals is resolved against the tags table into the
-- metric names it matches (up to 64), and the time ranges table is read by the id ranges of those metrics.
-- The selected series must be the same as without the id ranges, including series with identifiers outside the ranges
-- and regular expressions that match many metrics or none.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('http_a_total', map('env', 'prod'), [(toDateTime64(100, 3), 1), (toDateTime64(200, 3), 2)]),
    ('http_a_total', map('env', 'dev'), [(toDateTime64(1000, 3), 3)]),
    ('http_b_total', map('env', 'prod'), [(toDateTime64(300, 3), 4)]),
    ('http_b_total', map('env', 'dev'), [(toDateTime64(2000, 3), 5)]),
    ('http_c_seconds', map('env', 'prod'), [(toDateTime64(150, 3), 6)]),
    ('other', map('env', 'prod'), [(toDateTime64(150, 3), 7)]);

INSERT INTO ts (metric_name, tags, samples)
SELECT concat('many_', toString(number)), map('env', 'prod'), [(toDateTime64(100, 3), toFloat64(number))]
FROM numbers(70);

-- Two series of 'http_a_total' with identifiers outside the id range of that metric: one inside the selected time range, one after it.
-- The table name is qualified because an asynchronous insert through a table function resolves an unqualified name in the default database.
INSERT INTO FUNCTION timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) (id, metric_name, tags) VALUES
    ((1, '00000000-0000-0000-0000-000000000001'), 'http_a_total', map('env', 'other_id_inside')),
    ((2, '00000000-0000-0000-0000-000000000002'), 'http_a_total', map('env', 'other_id_after'));

INSERT INTO FUNCTION timeSeriesTimeRanges({CLICKHOUSE_DATABASE:Identifier}.ts) (id, min_time, max_time) VALUES
    ((1, '00000000-0000-0000-0000-000000000001'), toDateTime64(250, 3), toDateTime64(250, 3)),
    ((2, '00000000-0000-0000-0000-000000000002'), toDateTime64(4000, 3), toDateTime64(4000, 3));

-- All the samples belong to the bucket starting at 0 (the default bucket step is 1 hour).
INSERT INTO FUNCTION timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) (id, samples, bucket, min_time, max_time) VALUES
    ((1, '00000000-0000-0000-0000-000000000001'), [(toDateTime64(250, 3), 8.)], toDateTime64(0, 3), toDateTime64(250, 3), toDateTime64(250, 3)),
    ((2, '00000000-0000-0000-0000-000000000002'), [(toDateTime64(4000, 3), 9.)], toDateTime64(3600, 3), toDateTime64(4000, 3), toDateTime64(4000, 3));

SELECT '-- regular expression matching two metrics';
SELECT timestamp, value FROM timeSeriesSelector(ts, '{__name__=~"http_._total"}', 100, 300, 'timestamp_and_value') ORDER BY timestamp, value
SETTINGS log_comment = '05813_regex';

SELECT '-- the same two metrics as a list of alternatives';
SELECT timestamp, value FROM timeSeriesSelector(ts, '{__name__=~"http_a_total|http_b_total"}', 100, 300, 'timestamp_and_value') ORDER BY timestamp, value
SETTINGS log_comment = '05813_list';

SELECT '-- regular expression with a label matcher';
SELECT timestamp, value FROM timeSeriesSelector(ts, '{__name__=~"http_.*", env="prod"}', 100, 300, 'timestamp_and_value') ORDER BY timestamp, value;

SELECT '-- regular expression that matches no metric';
SELECT count() FROM timeSeriesSelector(ts, '{__name__=~"nothing_.*"}', 0, 5000, 'timestamp_and_value');

SELECT '-- regular expression that matches more than 64 metrics';
SELECT count(), sum(value) FROM timeSeriesSelector(ts, '{__name__=~"many_.*"}', 0, 5000, 'timestamp_and_value') SETTINGS log_comment = '05813_many';
SELECT count(), sum(value) FROM timeSeriesSelector(ts, '{__name__=~"many_[1-6].*"}', 0, 5000, 'timestamp_and_value');

SELECT '-- the selected series equal the union of the selectors of the metrics';
SELECT arraySort(groupArray((id, samples))) = (SELECT arraySort(groupArray((id, samples))) FROM (
        SELECT * FROM timeSeriesSelector(ts, 'http_a_total', 0, 5000, 'samples')
        UNION ALL SELECT * FROM timeSeriesSelector(ts, 'http_b_total', 0, 5000, 'samples')
        UNION ALL SELECT * FROM timeSeriesSelector(ts, 'http_c_seconds', 0, 5000, 'samples')))
FROM timeSeriesSelector(ts, '{__name__=~"http_.*"}', 0, 5000, 'samples');

SELECT '-- PromQL evaluation';
SELECT * FROM prometheusQuery(ts, 'sum({__name__=~"http_._total"})', 300);
SELECT * FROM prometheusQuery(ts, 'sum({__name__=~"http_.*", env="prod"})', 300);
SELECT * FROM prometheusQuery(ts, 'count({__name__=~"many_.*"})', 300);

SELECT '-- generated query: index hints appear only when the names are resolved';

SYSTEM FLUSH LOGS query_log, text_log;
SELECT q.log_comment, countSubstrings(t.message, 'indexHint(') AS index_hints
FROM system.text_log AS t
INNER JOIN
(
    SELECT query_id, log_comment FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05813_regex', '05813_list', '05813_many')
) AS q USING (query_id)
WHERE t.event_date >= yesterday() AND t.logger_name = 'StorageTimeSeriesSelector' AND startsWith(t.message, 'Will execute query:')
ORDER BY q.log_comment
SETTINGS max_rows_to_read = 0; -- system.text_log can be really big

DROP TABLE ts;
