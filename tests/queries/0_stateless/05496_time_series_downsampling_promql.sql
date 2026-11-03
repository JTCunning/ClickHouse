-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_prom;

-- One sample per hour is the only level. The raw points are one hour apart, plus one extra point inside an hour.
CREATE TABLE ts_prom ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 3600, recent_samples_ttl_seconds = 0, samples_downsampling_periods = '3600:3600';

INSERT INTO ts_prom (metric_name, tags, samples) VALUES
    ('m', map(), [
        (toDateTime64(0, 3), 1.),
        (toDateTime64(1800, 3), 2.),
        (toDateTime64(3600, 3), 3.),
        (toDateTime64(5400, 3), 4.),
        (toDateTime64(7200, 3), 5.)
    ]);

SELECT '-- an instant selector uses the interval, so the point at 8000 still sees the sample at 7200';
SELECT value FROM prometheusQuery(ts_prom, 'm', 8000);

SELECT '-- the same selector with the adjustment off is empty';
SET promql_adjust_window_for_downsampling = 0;
SELECT count() FROM prometheusQuery(ts_prom, 'm', 8000);
SET promql_adjust_window_for_downsampling = 1;

SELECT '-- rate over 30 minutes uses two intervals and matches an explicit two-hour range';
SELECT value FROM prometheusQuery(ts_prom, 'rate(m[30m])', 8000);
SELECT value FROM prometheusQuery(ts_prom, 'rate(m[2h])', 8000);

DROP TABLE ts_prom;
