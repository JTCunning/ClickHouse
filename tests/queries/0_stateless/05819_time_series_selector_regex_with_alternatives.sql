-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A regex of a matcher must match the whole value. A regex with alternatives at the top level is put into a group
-- before the anchors are added: `^fo.|ba[r]$` would also match `fooo` and `xbar`.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('foo', map('job', 'a|b'), [(toDateTime64(100, 3), 1.)]),
    ('fooo', map('job', 'a'), [(toDateTime64(100, 3), 2.)]),
    ('bar', map('job', 'xb'), [(toDateTime64(100, 3), 3.)]),
    ('xbar', map('job', 'b'), [(toDateTime64(100, 3), 4.)]);

SELECT '-- the tags table';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, '{__name__=~"fo.|ba[r]"}', 0, 1000, 'timestamp_and_value') ORDER BY tags SETTINGS time_series_use_series_index = 0;
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, '{job=~".+",__name__!~"fo.|ba[r]"}', 0, 1000, 'timestamp_and_value') ORDER BY tags SETTINGS time_series_use_series_index = 0;
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, '{__name__=~".+",job=~"(a|b)|x."}', 0, 1000, 'timestamp_and_value') ORDER BY tags SETTINGS time_series_use_series_index = 0;
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, '{__name__=~".+",job=~"a[|]b"}', 0, 1000, 'timestamp_and_value') ORDER BY tags SETTINGS time_series_use_series_index = 0;
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, '{__name__=~"fo+"}', 0, 1000, 'timestamp_and_value') ORDER BY tags SETTINGS time_series_use_series_index = 0;

SELECT '-- the series index';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, '{__name__=~"fo.|ba[r]"}', 0, 1000, 'timestamp_and_value') ORDER BY tags SETTINGS time_series_check_series_index = 1;
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, '{job=~".+",__name__!~"fo.|ba[r]"}', 0, 1000, 'timestamp_and_value') ORDER BY tags SETTINGS time_series_check_series_index = 1;
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, '{__name__=~".+",job=~"(a|b)|x."}', 0, 1000, 'timestamp_and_value') ORDER BY tags SETTINGS time_series_check_series_index = 1;
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, '{__name__=~".+",job=~"a[|]b"}', 0, 1000, 'timestamp_and_value') ORDER BY tags SETTINGS time_series_check_series_index = 1;
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts, '{__name__=~"fo+"}', 0, 1000, 'timestamp_and_value') ORDER BY tags SETTINGS time_series_check_series_index = 1;

DROP TABLE ts;
