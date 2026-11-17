-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- `timeSeriesSelector` filters the time series by their time ranges with a merge join of the tags table and the time ranges table.
-- When the matchers give no id range (a regex on `__name__`), the time ranges side of the join is read over the whole table,
-- so a selector matching no series probes the tags table first and skips the join (see StorageTimeSeriesSelector).

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET log_queries = 1;

DROP TABLE IF EXISTS ts;

CREATE TABLE ts ENGINE = TimeSeries;
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(1000, 3), 1.), (toDateTime64(1060, 3), 2.)]),
    ('m', map('host', 'h2'), [(toDateTime64(2000, 3), 3.)]),
    ('n', map('host', 'h1'), [(toDateTime64(1000, 3), 4.)]);

SELECT '-- a regex on __name__ matching no metric returns no samples';
SELECT ifNull(sum(length(samples)), 0) FROM timeSeriesSelector(ts, '{__name__=~"nomatch(_total)?"}', 0, 2500, 'samples') SETTINGS log_comment = 'ts_regex_no_metric';

SELECT '-- a regex on __name__ matching a metric and a tag matching no series returns no samples';
SELECT ifNull(sum(length(samples)), 0) FROM timeSeriesSelector(ts, '{__name__=~"m(_total)?", host="h9"}', 0, 2500, 'samples') SETTINGS log_comment = 'ts_regex_no_series';

SELECT '-- a regex on __name__ matching a metric returns its samples in the time range';
SELECT ifNull(sum(length(samples)), 0) FROM timeSeriesSelector(ts, '{__name__=~"m(_total)?"}', 0, 2500, 'samples') SETTINGS log_comment = 'ts_regex_all';
SELECT ifNull(sum(length(samples)), 0) FROM timeSeriesSelector(ts, '{__name__=~"m(_total)?"}', 0, 1500, 'samples') SETTINGS log_comment = 'ts_regex_1500';

SELECT '-- an exact name matching no series returns no samples';
SELECT ifNull(sum(length(samples)), 0) FROM timeSeriesSelector(ts, 'm{host="h9"}', 0, 2500, 'samples') SETTINGS log_comment = 'ts_exact_no_series';

SELECT '-- the time ranges table is joined only when a regex selector matches a series; an exact name reads it by the id range of the metric';
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, arrayExists(t -> t LIKE '%.inner\_id.timeranges.%', tables) AS reads_time_ranges FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment IN ('ts_regex_no_metric', 'ts_regex_no_series', 'ts_regex_all', 'ts_regex_1500', 'ts_exact_no_series')
ORDER BY log_comment;

DROP TABLE ts;
