-- Tags: no-fasttest
-- Tag no-fasttest: timeSeriesSelector and the TimeSeries engine tests in this file follow the other TimeSeries tests that need a full build.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_stats;
DROP TABLE IF EXISTS ts_v7;

SELECT '--- version 8 stores bounds in series stats and deduplicates tags ---';
CREATE TABLE ts_stats ENGINE = TimeSeries;
SELECT extract(create_table_query, 'version = (\d+)') FROM system.tables WHERE database = currentDatabase() AND name = 'ts_stats';
SELECT position(create_table_query, 'SERIES STATS') > 0,
       position(extract(create_table_query, 'TAGS INNER COLUMNS \((.*?)\) TAGS INNER ENGINE'), 'min_time') = 0
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_stats';

CREATE TABLE ts_rejected ENGINE = TimeSeries SETTINGS store_min_time_and_max_time = 0; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts_rejected ENGINE = TimeSeries SETTINGS aggregate_min_time_and_max_time = 0; -- { serverError INVALID_SETTING_VALUE }

INSERT INTO ts_stats (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.), (toDateTime64('2026-01-01 00:01:00', 3), 2.)]);
INSERT INTO ts_stats (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:02:00', 3), 3.)]);
INSERT INTO ts_stats (metric_name, tags, samples) VALUES
    ('other', {'job': 'batch'}, [(toDateTime64('2026-01-01 01:00:00', 3), 9.)]);

SELECT count() FROM timeSeriesTags(ts_stats);
SELECT min(min_time) = toDateTime64('2026-01-01 00:00:00', 3),
       max(max_time) = toDateTime64('2026-01-01 00:02:00', 3),
       sum(sample_count)
FROM timeSeriesSeriesStats(ts_stats)
WHERE id IN (SELECT id FROM timeSeriesTags(ts_stats) WHERE metric_name = 'http_requests');

SELECT timestamp, value
FROM timeSeriesSelector(ts_stats, 'http_requests', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:01:30', 3))
ORDER BY timestamp;
SELECT count() FROM timeSeriesSelector(ts_stats, 'other', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:03:00', 3));

SELECT '--- filter_by can be turned off ---';
ALTER TABLE ts_stats MODIFY SETTING filter_by_min_time_and_max_time = 0;
SELECT count() FROM timeSeriesSelector(ts_stats, 'http_requests', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:01:30', 3));

SELECT '--- version 7 keeps bounds on the tags table ---';
CREATE TABLE ts_v7 ENGINE = TimeSeries SETTINGS version = 7;
SELECT position(create_table_query, 'SERIES STATS') > 0,
       position(extract(create_table_query, 'TAGS INNER COLUMNS \((.*?)\) TAGS INNER ENGINE'), 'min_time') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_v7';

DROP TABLE ts_stats;
DROP TABLE ts_v7;
