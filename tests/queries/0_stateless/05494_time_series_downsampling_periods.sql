-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_ds;

-- Background merges stay off. OPTIMIZE FINAL still applies the TTL.
CREATE TABLE ts_ds ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0, samples_downsampling_periods = '120:60,600:300'
SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO ts_ds (metric_name, tags, samples) VALUES
    ('m', map(), [
        (toDateTime64(0, 3), 1.),
        (toDateTime64(30, 3), 2.),
        (toDateTime64(60, 3), 3.),
        (toDateTime64(90, 3), 4.),
        (toDateTime64(120, 3), 5.),
        (toDateTime64(200, 3), 6.),
        (toDateTime64(400, 3), 7.),
        (toDateTime64(700, 3), 8.)
    ]);

OPTIMIZE TABLE ts_ds FINAL;

SELECT '-- after the merge each coarse interval keeps its last sample';
SELECT bucket, samples FROM timeSeriesSamples(ts_ds) ORDER BY bucket;

SELECT '-- a later sample in an old interval does not replace the kept sample';
INSERT INTO ts_ds (metric_name, tags, samples) VALUES ('m', map(), [(toDateTime64(100, 3), 0.)]);
OPTIMIZE TABLE ts_ds FINAL;
SELECT bucket, samples FROM timeSeriesSamples(ts_ds) ORDER BY bucket;

OPTIMIZE TABLE ts_ds FINAL;
SELECT count(), sum(length(samples)) FROM timeSeriesSamples(ts_ds);

DROP TABLE ts_ds;

SELECT '-- recent samples stay raw';
DROP TABLE IF EXISTS ts_recent_ds;
CREATE TABLE ts_recent_ds ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 864000, samples_downsampling_periods = '30d:5m';

INSERT INTO ts_recent_ds (metric_name, tags, samples)
SELECT 'm', map(), arrayMap(i -> (toDateTime64(now(), 3) - toIntervalSecond(i * 60), toFloat64(i)), range(5));

SELECT sum(length(samples)) FROM merge(currentDatabase(), '^\\.inner_id\\.recentsamples\\.');

DROP TABLE ts_recent_ds;
