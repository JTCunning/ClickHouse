-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: every parallel replica reads all rows of the table behind `timeSeriesTimeRanges`.

-- A selector which matches no series in its range is converted to an empty result before the SQL is built,
-- so the query reads no table and has no join.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_ranges;

CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('kube_pod_owner', map('job', 'kube-state-metrics', 'owner_kind', 'ReplicaSet', 'owner_name', 'rs1', 'cluster', 'c', 'namespace', 'n', 'pod', 'p1'),
     [(toDateTime64(100, 3), 1.), (toDateTime64(200, 3), 2.), (toDateTime64(300, 3), 3.)]);

SELECT 'the same selector with no series on both sides of or: empty, no table read, no join';
SELECT count()
FROM prometheusQuery(ts, 'max by (cluster, namespace, workload, pod) (label_replace(kube_pod_owner{job="kube-state-metrics", owner_kind="Job"}, "workload", "$1", "owner_name", "(.*)")) or label_join(kube_pod_owner{job="kube-state-metrics", owner_kind="Job"}, "workload", "/", "owner_name")', 300);
SELECT countIf(explain LIKE '%ReadFromMergeTree%') = 0 AS no_table_read, countIf(explain LIKE '%Join%') = 0 AS no_join
FROM (EXPLAIN SELECT * FROM prometheusQuery(ts, 'max by (cluster, namespace, workload, pod) (label_replace(kube_pod_owner{job="kube-state-metrics", owner_kind="Job"}, "workload", "$1", "owner_name", "(.*)")) or label_join(kube_pod_owner{job="kube-state-metrics", owner_kind="Job"}, "workload", "/", "owner_name")', 300));

SELECT 'the same selector with a series on both sides of or: both sides, then the left side alone';
SELECT tags, value
FROM prometheusQuery(ts, 'max by (cluster, namespace, workload, pod) (label_replace(kube_pod_owner{job="kube-state-metrics", owner_kind="ReplicaSet"}, "workload", "$1", "owner_name", "(.*)")) or label_join(kube_pod_owner{job="kube-state-metrics", owner_kind="ReplicaSet"}, "workload", "/", "owner_name")', 300)
ORDER BY tags;
SELECT tags, value
FROM prometheusQuery(ts, 'max by (cluster, namespace, workload, pod) (label_replace(kube_pod_owner{job="kube-state-metrics", owner_kind="ReplicaSet"}, "workload", "$1", "owner_name", "(.*)"))', 300)
ORDER BY tags;

SELECT 'the same selector text with two offsets';
SELECT value FROM prometheusQuery(ts, 'sum(kube_pod_owner{owner_kind="ReplicaSet"}) + sum(kube_pod_owner{owner_kind="ReplicaSet"} offset 100)', 300);
SELECT count() FROM prometheusQuery(ts, 'kube_pod_owner{owner_kind="ReplicaSet"} offset 1000', 300);

SELECT 'range vectors and functions of a selector with no series';
SELECT count() FROM prometheusQuery(ts, 'kube_pod_owner{owner_kind="Job"}[5m]', 300);
SELECT count() FROM prometheusQuery(ts, 'rate(kube_pod_owner{owner_kind="Job"}[5m])', 300);
SELECT count() FROM prometheusQuery(ts, 'timestamp(kube_pod_owner{owner_kind="Job"})', 300);
SELECT count() FROM prometheusQueryRange(ts, 'kube_pod_owner{owner_kind="Job"}', 100, 300, 100);

SELECT 'absent of a selector with no series';
SELECT tags, value FROM prometheusQuery(ts, 'absent(kube_pod_owner{owner_kind="Job"})', 300);
SELECT tags, samples FROM prometheusQueryRange(ts, 'absent_over_time(kube_pod_owner{owner_kind="Job"}[5m])', 100, 300, 100);
SELECT count() FROM prometheusQuery(ts, 'absent(kube_pod_owner{owner_kind="ReplicaSet"})', 300);

-- A time series written directly into the inner tables with a stored time range [3000, 3000] far from its samples.
CREATE TABLE ts_ranges ENGINE = TimeSeries TAGS INNER COLUMNS (id UInt64);
INSERT INTO ts_ranges (metric_name, tags, samples) VALUES ('m', map('host', 'h2'), [(toDateTime64(2000, 3), 3.)]);
INSERT INTO FUNCTION timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_ranges) (id, metric_name, tags) VALUES (1, 'm', map('__name__', 'm', 'host', 'h1'));
INSERT INTO FUNCTION timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts_ranges) (id, timestamp, value) VALUES (1, toDateTime64(1500, 3), 4.);
INSERT INTO FUNCTION timeSeriesTimeRanges({CLICKHOUSE_DATABASE:Identifier}.ts_ranges) (id, min_time, max_time) VALUES (1, toDateTime64(3000, 3), toDateTime64(3000, 3));

SELECT 'a series whose stored time range misses the window is empty, a series inside the window is returned';
SELECT count() FROM prometheusQuery(ts_ranges, 'm{host="h1"}', 1500);
SELECT countIf(explain LIKE '%ReadFromMergeTree%') = 0 AS no_table_read
FROM (EXPLAIN SELECT * FROM prometheusQuery(ts_ranges, 'm{host="h1"}', 1500));
SELECT tags, value FROM prometheusQuery(ts_ranges, 'm{host="h2"}', 2000);

DROP TABLE ts;
DROP TABLE ts_ranges;
