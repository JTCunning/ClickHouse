-- Tags: no-fasttest
-- Tag no-fasttest: PromQL / TimeSeries functions require ANTLR4.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_align_off;
DROP TABLE IF EXISTS ts_align_on;

SELECT '--- validation ---';
CREATE TABLE ts_align_bad ENGINE = TimeSeries SETTINGS version = 11, samples_timestamp_alignment_tolerance_ms = 100; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts_align_bad ENGINE = TimeSeries SETTINGS samples_timestamp_alignment_tolerance_ms = 20000; -- { serverError INVALID_SETTING_VALUE }

SELECT '--- default disabled (tolerance = 0) leaves timestamps unchanged ---';
CREATE TABLE ts_align_off ENGINE = TimeSeries;
INSERT INTO ts_align_off (metric_name, tags, samples) VALUES
    ('cpu', {'host': 'h1'}, [
        (fromUnixTimestamp64Milli(1761955200100), 1.0),
        (fromUnixTimestamp64Milli(1761955215120), 2.0),
        (fromUnixTimestamp64Milli(1761955230090), 3.0),
        (fromUnixTimestamp64Milli(1761955245115), 4.0)
    ]);
SELECT arrayMap(x -> toUnixTimestamp64Milli(x.1), samples)
FROM timeSeriesSamples(ts_align_off);

SELECT '--- enabled (tolerance = 100ms): single-scraper aligns after confirmation, preserves outlier ---';
CREATE TABLE ts_align_on ENGINE = TimeSeries SETTINGS samples_timestamp_alignment_tolerance_ms = 100;
INSERT INTO ts_align_on (metric_name, tags, samples) VALUES
    ('cpu', {'case': 'single'}, [
        (fromUnixTimestamp64Milli(1761955200100), 1.0),
        (fromUnixTimestamp64Milli(1761955215120), 2.0),
        (fromUnixTimestamp64Milli(1761955230090), 3.0),
        (fromUnixTimestamp64Milli(1761955245115), 4.0),
        (fromUnixTimestamp64Milli(1761955260350), 5.0),
        (fromUnixTimestamp64Milli(1761955275095), 6.0)
    ]);
SELECT arrayMap(x -> toUnixTimestamp64Milli(x.1), samples)
FROM timeSeriesSamples(ts_align_on)
WHERE id IN (SELECT id FROM timeSeriesTags(ts_align_on) WHERE tags['case'] = 'single');

SELECT '--- cross-insert cache persistence (1 sample per INSERT) + time_ranges consistency ---';
INSERT INTO ts_align_on (metric_name, tags, samples) VALUES ('cpu', {'case': 'cross_insert'}, [(fromUnixTimestamp64Milli(1761955200500), 1.0)]);
INSERT INTO ts_align_on (metric_name, tags, samples) VALUES ('cpu', {'case': 'cross_insert'}, [(fromUnixTimestamp64Milli(1761955215530), 2.0)]);
INSERT INTO ts_align_on (metric_name, tags, samples) VALUES ('cpu', {'case': 'cross_insert'}, [(fromUnixTimestamp64Milli(1761955230480), 3.0)]);
INSERT INTO ts_align_on (metric_name, tags, samples) VALUES ('cpu', {'case': 'cross_insert'}, [(fromUnixTimestamp64Milli(1761955245540), 4.0)]);
SELECT arrayMap(x -> toUnixTimestamp64Milli(x.1), timeSeriesGroupArray(samples))
FROM timeSeriesSamples(ts_align_on)
WHERE id IN (SELECT id FROM timeSeriesTags(ts_align_on) WHERE tags['case'] = 'cross_insert');
SELECT toUnixTimestamp64Milli(min(min_time)), toUnixTimestamp64Milli(max(max_time))
FROM timeSeriesTimeRanges(ts_align_on)
WHERE id IN (SELECT id FROM timeSeriesTags(ts_align_on) WHERE tags['case'] = 'cross_insert');

SELECT '--- HA-2 interleaved 30s scrapers align both phases ---';
INSERT INTO ts_align_on (metric_name, tags, samples) VALUES
    ('cpu', {'case': 'ha2'}, [
        (fromUnixTimestamp64Milli(1761955200000), 1.0),
        (fromUnixTimestamp64Milli(1761955207530), 1.0),
        (fromUnixTimestamp64Milli(1761955230008), 2.0),
        (fromUnixTimestamp64Milli(1761955237524), 2.0),
        (fromUnixTimestamp64Milli(1761955260005), 3.0),
        (fromUnixTimestamp64Milli(1761955267536), 3.0),
        (fromUnixTimestamp64Milli(1761955290012), 4.0),
        (fromUnixTimestamp64Milli(1761955297520), 4.0)
    ]);
SELECT arrayMap(x -> toUnixTimestamp64Milli(x.1), samples)
FROM timeSeriesSamples(ts_align_on)
WHERE id IN (SELECT id FROM timeSeriesTags(ts_align_on) WHERE tags['case'] = 'ha2');

SELECT '--- irregular multi-second jitter stays untouched ---';
INSERT INTO ts_align_on (metric_name, tags, samples) VALUES
    ('cpu', {'case': 'irregular'}, [
        (fromUnixTimestamp64Milli(1761955200000), 1.0),
        (fromUnixTimestamp64Milli(1761955217400), 2.0),
        (fromUnixTimestamp64Milli(1761955229100), 3.0),
        (fromUnixTimestamp64Milli(1761955248700), 4.0),
        (fromUnixTimestamp64Milli(1761955261300), 5.0)
    ]);
SELECT arrayMap(x -> toUnixTimestamp64Milli(x.1), samples)
FROM timeSeriesSamples(ts_align_on)
WHERE id IN (SELECT id FROM timeSeriesTags(ts_align_on) WHERE tags['case'] = 'irregular');

SELECT '--- 1% interval clamp protects fast 2s scrapes (max 20ms shift) ---';
INSERT INTO ts_align_on (metric_name, tags, samples) VALUES
    ('cpu', {'case': 'fast2s'}, [
        (fromUnixTimestamp64Milli(1761955200000), 1.0),
        (fromUnixTimestamp64Milli(1761955202010), 2.0),
        (fromUnixTimestamp64Milli(1761955204015), 3.0),
        (fromUnixTimestamp64Milli(1761955206050), 4.0)
    ]);
SELECT arrayMap(x -> toUnixTimestamp64Milli(x.1), samples)
FROM timeSeriesSamples(ts_align_on)
WHERE id IN (SELECT id FROM timeSeriesTags(ts_align_on) WHERE tags['case'] = 'fast2s');

SELECT '--- ALTER MODIFY SETTING and SYSTEM CLEAR TIME SERIES CACHES ---';
SYSTEM CLEAR TIME SERIES CACHES ts_align_on;
ALTER TABLE ts_align_on MODIFY SETTING samples_timestamp_alignment_tolerance_ms = 0;
INSERT INTO ts_align_on (metric_name, tags, samples) VALUES
    ('cpu', {'case': 'after_disable'}, [
        (fromUnixTimestamp64Milli(1761955200000), 1.0),
        (fromUnixTimestamp64Milli(1761955215010), 2.0),
        (fromUnixTimestamp64Milli(1761955230015), 3.0)
    ]);
SELECT arrayMap(x -> toUnixTimestamp64Milli(x.1), samples)
FROM timeSeriesSamples(ts_align_on)
WHERE id IN (SELECT id FROM timeSeriesTags(ts_align_on) WHERE tags['case'] = 'after_disable');

ALTER TABLE ts_align_on RESET SETTING samples_timestamp_alignment_tolerance_ms;

DROP TABLE ts_align_off;
DROP TABLE ts_align_on;
