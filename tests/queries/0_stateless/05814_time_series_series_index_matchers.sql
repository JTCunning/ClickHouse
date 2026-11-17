-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: a query with parallel replicas reads the tags table instead of the series index.

-- The series index of a `TimeSeries` table resolves every kind of matcher of a selector.
-- With `time_series_check_series_index` a query waits for the index, also reads the tags table and the time ranges table,
-- and throws an exception if the index has not returned a time series which the tables have returned.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET time_series_use_series_index = 1;
SET time_series_check_series_index = 1;

DROP TABLE IF EXISTS ts;

-- Without the recent samples table the index holds the time series of any time.
CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('up', map('job', 'api', 'instance', 'a:1', 'env', 'prod'), [(toDateTime64(100, 3), 1.), (toDateTime64(300, 3), 1.)]),
    ('up', map('job', 'api', 'instance', 'a:2', 'env', 'dev'), [(toDateTime64(100, 3), 0.)]),
    ('up', map('job', 'db', 'instance', 'b:1'), [(toDateTime64(200, 3), 1.)]),
    ('http_requests_total', map('job', 'api', 'code', '200', 'env', 'prod'), [(toDateTime64(100, 3), 10.)]),
    ('http_requests_total', map('job', 'api', 'code', '500', 'env', 'prod'), [(toDateTime64(100, 3), 20.)]),
    ('http_errors_total', map('job', 'web', 'env', 'staging'), [(toDateTime64(100, 3), 30.)]),
    ('cert_expire', map('kind', 'A'), [(toDateTime64(100, 3), 40.)]),
    ('other_cert_expire', map('kind', 'B'), [(toDateTime64(100, 3), 50.)]),
    ('late', map('job', 'api'), [(toDateTime64(5000, 3), 60.)]);

-- Many values of one tag: a regular expression on it is checked against the few time series selected by the other matchers.
INSERT INTO ts (metric_name, tags, samples)
SELECT 'many', map('instance', 'm:' || toString(number)), [(toDateTime64(100, 3), toFloat64(number))] FROM numbers(40);

SELECT '-- equality';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, 'up{job="api"}', 0, 1000, 'timestamp_and_value') ORDER BY tags;
SELECT '-- equality with the empty value: the time series without the tag';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, 'up{env=""}', 0, 1000, 'timestamp_and_value') ORDER BY tags;
SELECT '-- inequality';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, 'up{env!="prod"}', 0, 1000, 'timestamp_and_value') ORDER BY tags;
SELECT '-- inequality with the empty value: the time series with the tag';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, 'up{env!=""}', 0, 1000, 'timestamp_and_value') ORDER BY tags;
SELECT '-- list of literals';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, 'up{job=~"db|web"}', 0, 1000, 'timestamp_and_value') ORDER BY tags;
SELECT '-- list of literals with the empty one';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, 'up{env=~"prod|"}', 0, 1000, 'timestamp_and_value') ORDER BY tags;
SELECT '-- negated list of literals';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, 'up{job!~"api|web"}', 0, 1000, 'timestamp_and_value') ORDER BY tags;
SELECT '-- negated list of literals with the empty one';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, 'up{env!~"dev|"}', 0, 1000, 'timestamp_and_value') ORDER BY tags;
SELECT '-- prefix';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, '{__name__=~"http_.*"}', 0, 1000, 'timestamp_and_value') ORDER BY tags;
SELECT '-- suffix';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, '{__name__=~".*cert_expire"}', 0, 1000, 'timestamp_and_value') ORDER BY tags;
SELECT '-- regular expression';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, '{__name__=~"http_(requests|errors)_total",code=~"5.."}', 0, 1000, 'timestamp_and_value') ORDER BY tags;
SELECT '-- regular expression matching everything';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, 'up{env=~".*"}', 0, 1000, 'timestamp_and_value') ORDER BY tags;
SELECT '-- regular expression matching any non-empty value';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, 'up{env=~".+"}', 0, 1000, 'timestamp_and_value') ORDER BY tags;
SELECT '-- negated regular expression';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, 'up{instance!~"a:.*"}', 0, 1000, 'timestamp_and_value') ORDER BY tags;
SELECT '-- negated regular expression matching the empty value';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, 'up{env!~"(prod)?"}', 0, 1000, 'timestamp_and_value') ORDER BY tags;
SELECT '-- regular expression on a tag with many values';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, 'up{instance=~"a:.*"}', 0, 1000, 'timestamp_and_value') ORDER BY tags;
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, 'many{instance=~"m:1[0-2]"}', 0, 1000, 'timestamp_and_value') ORDER BY tags;
SELECT '-- unknown tag';
SELECT count() FROM timeSeriesSelector(ts, 'up{unknown="x"}', 0, 1000, 'timestamp_and_value');
SELECT count() FROM timeSeriesSelector(ts, 'up{unknown=~"x.*"}', 0, 1000, 'timestamp_and_value');
SELECT count() FROM timeSeriesSelector(ts, 'up{unknown=""}', 0, 1000, 'timestamp_and_value');
SELECT count() FROM timeSeriesSelector(ts, 'up{unknown!="x"}', 0, 1000, 'timestamp_and_value');
SELECT count() FROM timeSeriesSelector(ts, 'up{unknown=~".*"}', 0, 1000, 'timestamp_and_value');
SELECT '-- unknown metric';
SELECT count() FROM timeSeriesSelector(ts, 'unknown', 0, 1000, 'timestamp_and_value');
SELECT count() FROM timeSeriesSelector(ts, '{__name__=~"unknown.*"}', 0, 1000, 'timestamp_and_value');

SELECT '-- time range';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, '{job="api",code=""}', 0, 1000, 'timestamp_and_value') ORDER BY tags;
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, '{job="api",code=""}', 300, 6000, 'timestamp_and_value') ORDER BY tags;
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, '{job="api",code=""}', 4000, 6000, 'timestamp_and_value') ORDER BY tags;
SELECT count() FROM timeSeriesSelector(ts, '{job="api",code=""}', 6000, 7000, 'timestamp_and_value');

SELECT '-- samples';
SELECT timeSeriesIdToTags(id) AS tags, timestamp, value FROM timeSeriesSelector(ts, 'up', 0, 1000, 'timestamp_and_value') ORDER BY tags, timestamp;

SELECT '-- PromQL';
SELECT * FROM prometheusQuery(ts, 'sum by (job) (up{env!="dev"})', toDateTime64(300, 3)) ORDER BY ALL;

SELECT '-- every selector was resolved by the index';
SYSTEM FLUSH LOGS query_log;
SELECT sum(ProfileEvents['TimeSeriesSeriesIndexHits']) > 0 AS has_hits, sum(ProfileEvents['TimeSeriesSeriesIndexMisses']) AS misses
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
    AND (query LIKE '%FROM timeSeriesSelector(ts,%' OR query LIKE '%FROM prometheusQuery(ts,%');

SELECT '-- without the index';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, 'up{env!="prod"}', 0, 1000, 'timestamp_and_value') ORDER BY tags
SETTINGS time_series_use_series_index = 0, time_series_check_series_index = 0, log_comment = '05814_without_index';
SYSTEM FLUSH LOGS query_log;
SELECT sum(ProfileEvents['TimeSeriesSeriesIndexHits']) > 0 AS has_hits, sum(ProfileEvents['TimeSeriesSeriesIndexMisses']) AS misses
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND log_comment = '05814_without_index';

DROP TABLE ts;
