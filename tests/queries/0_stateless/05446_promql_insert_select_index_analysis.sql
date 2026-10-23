-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

DROP TABLE IF EXISTS promql_source;
DROP TABLE IF EXISTS promql_result;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;
SET enable_shared_storage_snapshot_in_query = 1;
SET log_queries = 1;

CREATE TABLE promql_source ENGINE = TimeSeries;
CREATE TABLE promql_result ENGINE = TimeSeries;

INSERT INTO promql_source (metric_name, tags, samples) VALUES
    ('requests_total', map('job', 'api', 'instance', 'a'), [(toDateTime64(100, 3), 10.)]),
    ('requests_total', map('job', 'worker', 'instance', 'b'), [(toDateTime64(100, 3), 20.)]);
INSERT INTO promql_source (metric_name, tags, samples) VALUES
    ('requests_total', map('job', 'api', 'instance', 'a'), [(toDateTime64(110, 3), 20.)]),
    ('requests_total', map('job', 'worker', 'instance', 'b'), [(toDateTime64(110, 3), 40.)]);
INSERT INTO promql_source (metric_name, tags, samples) VALUES
    ('requests_total', map('job', 'api', 'instance', 'a'), [(toDateTime64(120, 3), 30.)]),
    ('requests_total', map('job', 'worker', 'instance', 'b'), [(toDateTime64(120, 3), 60.)]);
INSERT INTO promql_source (metric_name, tags, samples) VALUES
    ('requests_total', map('job', 'api', 'instance', 'a'), [(toDateTime64(130, 3), 40.)]),
    ('requests_total', map('job', 'worker', 'instance', 'b'), [(toDateTime64(130, 3), 80.)]);

SELECT *
FROM prometheusQuery(promql_source, 'sum by (job) (rate(requests_total[20s]))', 130)
SETTINGS log_comment = '05446_1_promql_warm'
FORMAT Null;

INSERT INTO promql_result (metric_name, tags, samples)
SELECT
    'recorded_requests_rate',
    mapFilter((key, value) -> key != '__name__', CAST(tags, 'Map(String, String)')),
    [(timestamp, value)]
FROM prometheusQuery(promql_source, 'sum by (job) (rate(requests_total[20s]))', 130)
SETTINGS log_comment = '05446_2_promql_insert_select';

SELECT tags, value
FROM prometheusQuery(promql_result, 'recorded_requests_rate', 130)
ORDER BY tags;

SELECT *
FROM prometheusQuery(promql_source, 'sum by (job) (rate(requests_total[20s]))', 130)
SETTINGS enable_shared_storage_snapshot_in_query = 0, log_comment = '05446_3_promql_shared_snapshot_disabled'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['IndexAnalysisRounds'],
    ProfileEvents['QueryPlanOptimizeFirstPassMicroseconds'] <= ProfileEvents['QueryPlanOptimizeMicroseconds'],
    ProfileEvents['QueryPlanOptimizeSecondPassMicroseconds'] <= ProfileEvents['QueryPlanOptimizeMicroseconds'],
    ProfileEvents['MergeTreeSelectRangesToReadMicroseconds'] <= ProfileEvents['QueryPlanOptimizeMicroseconds']
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time >= now() - INTERVAL 15 MINUTE
    AND current_database = currentDatabase()
    AND log_comment IN ('05446_1_promql_warm', '05446_2_promql_insert_select', '05446_3_promql_shared_snapshot_disabled')
    AND type = 'QueryFinish'
ORDER BY log_comment;

INSERT INTO FUNCTION timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.promql_source) (id, metric_name, tags) VALUES
    (tuple(1, toUUID('00000000-0000-0000-0000-000000000001')), 'requests_total', map('__name__', 'requests_total', 'job', 'foreign'));
INSERT INTO FUNCTION timeSeriesTimeRanges({CLICKHOUSE_DATABASE:Identifier}.promql_source) (id, min_time, max_time) VALUES
    (tuple(1, toUUID('00000000-0000-0000-0000-000000000001')), toDateTime64(100, 3), toDateTime64(100, 3));
INSERT INTO FUNCTION timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.promql_source) (id, timestamp, value) VALUES
    (tuple(1, toUUID('00000000-0000-0000-0000-000000000001')), toDateTime64(100, 3), 1.);

SELECT count() FROM timeSeriesSelector(promql_source, 'requests_total', 0, 200);

DROP TABLE promql_result;
DROP TABLE promql_source;
