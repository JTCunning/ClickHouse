-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: a query with parallel replicas reads the tags table instead of the series index.

-- The series index of a `TimeSeries` table is used only if the identifiers are tuples of two components
-- with the first one calculated from the metric name alone. With other identifiers the queries read the tags table.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET time_series_use_series_index = 1;
SET time_series_check_series_index = 1;

CREATE TABLE ts_default ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;
CREATE TABLE ts_other_second ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
    TAGS INNER COLUMNS (id Tuple(UInt64, UInt64) DEFAULT tuple(sipHash64(metric_name), sipHash64(metric_name, tags)));
CREATE TABLE ts_tags_in_first ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
    TAGS INNER COLUMNS (id Tuple(UInt64, UUID) DEFAULT tuple(sipHash64(tags), reinterpretAsUUID(sipHash128(metric_name, tags))));
CREATE TABLE ts_uuid ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0 TAGS INNER COLUMNS (id UUID);
CREATE TABLE ts_uint64 ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0 TAGS INNER COLUMNS (id UInt64);

INSERT INTO ts_default (metric_name, tags, samples) VALUES ('m', map('series', '1'), [(toDateTime64(100, 3), 1.)]), ('m', map('series', '2'), [(toDateTime64(200, 3), 2.)]);
INSERT INTO ts_other_second (metric_name, tags, samples) VALUES ('m', map('series', '1'), [(toDateTime64(100, 3), 1.)]), ('m', map('series', '2'), [(toDateTime64(200, 3), 2.)]);
INSERT INTO ts_tags_in_first (metric_name, tags, samples) VALUES ('m', map('series', '1'), [(toDateTime64(100, 3), 1.)]), ('m', map('series', '2'), [(toDateTime64(200, 3), 2.)]);
INSERT INTO ts_uuid (metric_name, tags, samples) VALUES ('m', map('series', '1'), [(toDateTime64(100, 3), 1.)]), ('m', map('series', '2'), [(toDateTime64(200, 3), 2.)]);
INSERT INTO ts_uint64 (metric_name, tags, samples) VALUES ('m', map('series', '1'), [(toDateTime64(100, 3), 1.)]), ('m', map('series', '2'), [(toDateTime64(200, 3), 2.)]);

SELECT '-- the default identifiers: the index';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts_default, 'm', 0, 1000, 'timestamp_and_value') ORDER BY tags SETTINGS log_comment = '05824_1_default';

SELECT '-- another second component of the identifiers: the index';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts_other_second, 'm', 0, 1000, 'timestamp_and_value') ORDER BY tags SETTINGS log_comment = '05824_2_other_second';

SELECT '-- the tags in the first component of the identifiers: the tables';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts_tags_in_first, 'm', 0, 1000, 'timestamp_and_value') ORDER BY tags SETTINGS log_comment = '05824_3_tags_in_first';

SELECT '-- identifiers of one component: the tables';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts_uuid, 'm', 0, 1000, 'timestamp_and_value') ORDER BY tags SETTINGS log_comment = '05824_4_uuid';
SELECT DISTINCT timeSeriesIdToTags(id) AS tags FROM timeSeriesSelector(ts_uint64, 'm', 0, 1000, 'timestamp_and_value') ORDER BY tags SETTINGS log_comment = '05824_5_uint64';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, sum(ProfileEvents['TimeSeriesSeriesIndexHits']) AS hits, sum(ProfileEvents['TimeSeriesSeriesIndexMisses']) AS misses
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND match(log_comment, '^05824_[0-9]_')
GROUP BY log_comment ORDER BY log_comment;

DROP TABLE ts_default;
DROP TABLE ts_other_second;
DROP TABLE ts_tags_in_first;
DROP TABLE ts_uuid;
DROP TABLE ts_uint64;
