SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ext_tags;

CREATE TABLE ts ENGINE = TimeSeries
SETTINGS store_min_time_and_max_time = 0, recent_samples_ttl_seconds = 0;

INSERT INTO ts (metric_name, tags, samples)
VALUES ('m', {'job': 'a'}, [(toDateTime64(1, 3), 1.)]);
INSERT INTO ts (metric_name, tags, samples)
VALUES ('m', {'job': 'a'}, [(toDateTime64(2, 3), 2.)]);

SELECT count() FROM timeSeriesTags(ts);
SELECT count() FROM timeSeriesData(ts);

TRUNCATE TABLE ts;
INSERT INTO ts (metric_name, tags, samples)
VALUES ('m', {'job': 'a'}, [(toDateTime64(3, 3), 3.)]);
SELECT count() FROM timeSeriesTags(ts);

DROP TABLE ts;
CREATE TABLE ts ENGINE = TimeSeries
SETTINGS store_min_time_and_max_time = 0, recent_samples_ttl_seconds = 0,
         id_type = 'UInt64', id_generator = 'xxHash64(tags)';
INSERT INTO ts (metric_name, tags, samples)
VALUES ('m', {'job': 'a'}, [(toDateTime64(1, 3), 1.)]);
ALTER TABLE ts MODIFY SETTING id_generator = 'sipHash64(tags)';
INSERT INTO ts (metric_name, tags, samples)
VALUES ('m', {'job': 'a'}, [(toDateTime64(2, 3), 2.)]);
SELECT uniqExact(id), count() FROM timeSeriesTags(ts);

DROP TABLE ts;
CREATE TABLE ts ENGINE = TimeSeries
SETTINGS store_min_time_and_max_time = 0, recent_samples_ttl_seconds = 0,
         insert_cache_max_size_bytes = 17;
INSERT INTO ts (metric_name, tags, samples)
VALUES ('m', {'job': 'a'}, [(toDateTime64(1, 3), 1.)]);
INSERT INTO ts (metric_name, tags, samples)
VALUES ('m', {'job': 'b'}, [(toDateTime64(2, 3), 2.)]);
INSERT INTO ts (metric_name, tags, samples)
VALUES ('m', {'job': 'a'}, [(toDateTime64(3, 3), 3.)]);
SELECT count() FROM timeSeriesTags(ts);

DROP TABLE ts;
CREATE TABLE ts ENGINE = TimeSeries
SETTINGS store_min_time_and_max_time = 0, recent_samples_ttl_seconds = 0,
         insert_cache_max_size_bytes = 0;
INSERT INTO ts (metric_name, tags, samples)
VALUES ('m', {'job': 'a'}, [(toDateTime64(1, 3), 1.)]);
INSERT INTO ts (metric_name, tags, samples)
VALUES ('m', {'job': 'a'}, [(toDateTime64(2, 3), 2.)]);
SELECT count() FROM timeSeriesTags(ts);

DROP TABLE ts;
CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;
INSERT INTO ts (metric_name, tags, samples)
VALUES ('m', {'job': 'a'}, [(toDateTime64(1, 3), 1.)]);
INSERT INTO ts (metric_name, tags, samples)
VALUES ('m', {'job': 'a'}, [(toDateTime64(2, 3), 2.)]);
SELECT count() FROM timeSeriesTags(ts);

DROP TABLE ts;
CREATE TABLE ext_tags
(
    id UInt64 DEFAULT sipHash64(tags),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String)
)
ENGINE = ReplacingMergeTree
ORDER BY (metric_name, id);
CREATE TABLE ts ENGINE = TimeSeries
SETTINGS store_min_time_and_max_time = 0, recent_samples_ttl_seconds = 0
TAGS ext_tags;
INSERT INTO ts (metric_name, tags, samples)
VALUES ('m', {'job': 'a'}, [(toDateTime64(1, 3), 1.)]);
INSERT INTO ts (metric_name, tags, samples)
VALUES ('m', {'job': 'a'}, [(toDateTime64(2, 3), 2.)]);
SELECT count() FROM ext_tags;

DROP TABLE ts;
DROP TABLE ext_tags;
CREATE TABLE ts ENGINE = TimeSeries
SETTINGS store_min_time_and_max_time = 0, recent_samples_ttl_seconds = 0;
INSERT INTO ts (metric_family, type, unit, help)
VALUES ('m', 'gauge', 'seconds', 'first');
INSERT INTO ts (metric_family, type, unit, help)
VALUES ('m', 'gauge', 'seconds', 'first');
SELECT count() FROM timeSeriesMetricFamilies(ts);
INSERT INTO ts (metric_family, type, unit, help)
VALUES ('m', 'counter', 'bytes', 'second');
SELECT type, unit, help FROM ts FINAL WHERE metric_family = 'm';

DROP TABLE ts;
