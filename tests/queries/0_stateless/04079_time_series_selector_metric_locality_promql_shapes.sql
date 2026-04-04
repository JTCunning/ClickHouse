-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN output structure can vary with parallel replicas

SET allow_experimental_time_series_table = 1;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS ts_sel_metric_loc_promql SYNC;
CREATE TABLE ts_sel_metric_loc_promql ENGINE = TimeSeries;

-- PromQL-shaped instant selectors: equality on `__name__` can push a constant `metric_locality_id` PREWHERE;
-- regex on `__name__` still reads `metric_locality_id` in the inner scan (visible in QUERY TREE dump).

SELECT max(position(explain, 'metric_locality_id')) > 0
FROM (
    EXPLAIN QUERY TREE dump_ast = 1
    SELECT id, timestamp, value
    FROM timeSeriesSelector(currentDatabase(), 'ts_sel_metric_loc_promql', 'up',
        toDateTime64('2020-01-01 00:00:00', 3),
        toDateTime64('2020-01-01 01:00:00', 3))
);

SELECT max(position(explain, 'metric_locality_id')) > 0
FROM (
    EXPLAIN QUERY TREE dump_ast = 1
    SELECT id, timestamp, value
    FROM timeSeriesSelector(currentDatabase(), 'ts_sel_metric_loc_promql', 'up{job="prometheus"}',
        toDateTime64('2020-01-01 00:00:00', 3),
        toDateTime64('2020-01-01 01:00:00', 3))
);

SELECT max(position(explain, 'metric_locality_id')) > 0
FROM (
    EXPLAIN QUERY TREE dump_ast = 1
    SELECT id, timestamp, value
    FROM timeSeriesSelector(currentDatabase(), 'ts_sel_metric_loc_promql', '{__name__="kube_node_cpu_seconds_total"}',
        toDateTime64('2020-01-01 00:00:00', 3),
        toDateTime64('2020-01-01 01:00:00', 3))
);

SELECT max(position(explain, 'metric_locality_id')) > 0
FROM (
    EXPLAIN QUERY TREE dump_ast = 1
    SELECT id, timestamp, value
    FROM timeSeriesSelector(currentDatabase(), 'ts_sel_metric_loc_promql', 'node_memory_MemAvailable_bytes{instance="a:9100"}',
        toDateTime64('2020-01-01 00:00:00', 3),
        toDateTime64('2020-01-01 01:00:00', 3))
);

SELECT max(position(explain, 'metric_locality_id')) > 0
FROM (
    EXPLAIN QUERY TREE dump_ast = 1
    SELECT id, timestamp, value
    FROM timeSeriesSelector(currentDatabase(), 'ts_sel_metric_loc_promql', 'prometheus_http_requests_total{handler="/api"}',
        toDateTime64('2020-01-01 00:00:00', 3),
        toDateTime64('2020-01-01 01:00:00', 3))
);

SELECT max(position(explain, 'metric_locality_id')) > 0
FROM (
    EXPLAIN QUERY TREE dump_ast = 1
    SELECT id, timestamp, value
    FROM timeSeriesSelector(currentDatabase(), 'ts_sel_metric_loc_promql', 'ClickHouseProfileEvents_Query',
        toDateTime64('2020-01-01 00:00:00', 3),
        toDateTime64('2020-01-01 01:00:00', 3))
);

SELECT max(position(explain, 'metric_locality_id')) > 0
FROM (
    EXPLAIN QUERY TREE dump_ast = 1
    SELECT id, timestamp, value
    FROM timeSeriesSelector(currentDatabase(), 'ts_sel_metric_loc_promql', '{__name__=~"metric.*"}',
        toDateTime64('2020-01-01 00:00:00', 3),
        toDateTime64('2020-01-01 01:00:00', 3))
);
