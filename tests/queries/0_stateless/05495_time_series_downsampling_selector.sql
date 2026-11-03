-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_sel;

CREATE TABLE ts_sel ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0, samples_downsampling_periods = '120:60,600:300'
SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO ts_sel (metric_name, tags, samples) VALUES
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

SELECT '-- the selector matches a reduction of the raw samples, before and after the merge';
SELECT timeSeriesGroupArray(samples)
    = timeSeriesDownsample(300)([(toDateTime64(0, 3), 1.), (toDateTime64(30, 3), 2.), (toDateTime64(60, 3), 3.), (toDateTime64(90, 3), 4.), (toDateTime64(120, 3), 5.), (toDateTime64(200, 3), 6.), (toDateTime64(400, 3), 7.), (toDateTime64(700, 3), 8.)])
FROM timeSeriesSelector(ts_sel, 'm', 0, 1000, 'samples')
GROUP BY id;

OPTIMIZE TABLE ts_sel FINAL;

SELECT timeSeriesGroupArray(samples)
    = timeSeriesDownsample(300)([(toDateTime64(0, 3), 1.), (toDateTime64(30, 3), 2.), (toDateTime64(60, 3), 3.), (toDateTime64(90, 3), 4.), (toDateTime64(120, 3), 5.), (toDateTime64(200, 3), 6.), (toDateTime64(400, 3), 7.), (toDateTime64(700, 3), 8.)])
FROM timeSeriesSelector(ts_sel, 'm', 0, 1000, 'samples')
GROUP BY id;

DROP TABLE ts_sel;
