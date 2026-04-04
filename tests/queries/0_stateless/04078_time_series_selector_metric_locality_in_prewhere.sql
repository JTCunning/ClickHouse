-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN output structure can vary with parallel replicas

SET allow_experimental_time_series_table = 1;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS ts_sel_metric_loc SYNC;
CREATE TABLE ts_sel_metric_loc ENGINE = TimeSeries;

-- With an equality on `__name__`, the read from the inner data table must use `metric_locality_id`
-- in PREWHERE (prefix of the MergeTree sort key).
SELECT max(position(explain, 'metric_locality_id')) > 0
FROM (
    EXPLAIN QUERY TREE dump_ast = 1
    SELECT id, timestamp, value
    FROM timeSeriesSelector(currentDatabase(), 'ts_sel_metric_loc', '{__name__="up"}',
        toDateTime64('2020-01-01 00:00:00', 3),
        toDateTime64('2020-01-01 01:00:00', 3))
);
