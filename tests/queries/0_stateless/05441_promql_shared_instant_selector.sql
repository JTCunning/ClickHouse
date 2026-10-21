-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: the test asserts on the query plan shape (the number of reads of the samples table).

-- When the same instant selector occurs more than once in a PromQL query with the same evaluation range,
-- its vector grid is built once as a materialized CTE and every occurrence reads from it, so the samples table
-- is read once. The same selector text with a different offset is a different selector and is read separately.

DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

CREATE TABLE prometheus ENGINE = TimeSeries;

INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('kube_pod_owner', map('job', 'ksm', 'owner_kind', 'Job', 'owner_name', 'j1', 'pod', 'p1'), [(toDateTime64(100, 3), 1)]),
    ('kube_pod_owner', map('job', 'ksm', 'owner_kind', 'Job', 'owner_name', 'j2', 'pod', 'p2'), [(toDateTime64(100, 3), 1)]),
    ('kube_pod_owner', map('job', 'ksm', 'owner_kind', 'ReplicaSet', 'owner_name', 'rs1', 'pod', 'p3'), [(toDateTime64(100, 3), 1)]),
    ('kube_pod_owner', map('job', 'other', 'owner_kind', 'Job', 'owner_name', 'j3', 'pod', 'p4'), [(toDateTime64(100, 3), 1)]);

SELECT '-- left side alone';
SELECT tags, value
FROM prometheusQuery('prometheus', 'max by (pod, workload) (label_replace(kube_pod_owner{job="ksm", owner_kind="Job"}, "workload", "$1", "owner_name", "(.*)"))', 100)
ORDER BY tags;

SELECT '-- right side alone';
SELECT tags, value
FROM prometheusQuery('prometheus', 'label_join(kube_pod_owner{job="ksm", owner_kind="Job"}, "workload", "/", "owner_name")', 100)
ORDER BY tags;

SELECT '-- or: the same selector on both sides returns the union of both sides';
SELECT tags, value
FROM prometheusQuery('prometheus', 'max by (pod, workload) (label_replace(kube_pod_owner{job="ksm", owner_kind="Job"}, "workload", "$1", "owner_name", "(.*)")) or label_join(kube_pod_owner{job="ksm", owner_kind="Job"}, "workload", "/", "owner_name")', 100)
ORDER BY tags;

SELECT '-- or: identical result with materialization disabled';
SET send_logs_level = 'error';
SELECT tags, value
FROM prometheusQuery('prometheus', 'max by (pod, workload) (label_replace(kube_pod_owner{job="ksm", owner_kind="Job"}, "workload", "$1", "owner_name", "(.*)")) or label_join(kube_pod_owner{job="ksm", owner_kind="Job"}, "workload", "/", "owner_name")', 100)
ORDER BY tags
SETTINGS enable_materialized_cte = 0;
SET send_logs_level = 'warning';

SELECT '-- or: the same selector on both sides is read once';
SELECT countIf(explain LIKE '%ReadFromMergeTree%.inner_id.samples.%') AS samples_table_reads,
       countIf(explain LIKE '%MaterializingCTEs%') > 0 AS uses_materialized_cte
FROM (EXPLAIN SELECT * FROM prometheusQuery('prometheus', 'max by (pod, workload) (label_replace(kube_pod_owner{job="ksm", owner_kind="Job"}, "workload", "$1", "owner_name", "(.*)")) or label_join(kube_pod_owner{job="ksm", owner_kind="Job"}, "workload", "/", "owner_name")', 100));

SELECT '-- or: an empty selector on both sides returns no series';
SELECT count()
FROM prometheusQuery('prometheus', 'max by (pod, workload) (label_replace(kube_pod_owner{job="nope", owner_kind="Job"}, "workload", "$1", "owner_name", "(.*)")) or label_join(kube_pod_owner{job="nope", owner_kind="Job"}, "workload", "/", "owner_name")', 100);

SELECT '-- three occurrences of the same selector are read once';
SELECT tags, value
FROM prometheusQuery('prometheus', 'kube_pod_owner{job="ksm", owner_kind="Job"} + kube_pod_owner{job="ksm", owner_kind="Job"} + kube_pod_owner{job="ksm", owner_kind="Job"}', 100)
ORDER BY tags;
SELECT countIf(explain LIKE '%ReadFromMergeTree%.inner_id.samples.%') AS samples_table_reads,
       countIf(explain LIKE '%MaterializingCTEs%') > 0 AS uses_materialized_cte
FROM (EXPLAIN SELECT * FROM prometheusQuery('prometheus', 'kube_pod_owner{job="ksm", owner_kind="Job"} + kube_pod_owner{job="ksm", owner_kind="Job"} + kube_pod_owner{job="ksm", owner_kind="Job"}', 100));

SELECT '-- the same selector with a different offset is read separately';
SELECT tags, value
FROM prometheusQuery('prometheus', 'kube_pod_owner{job="ksm", owner_kind="Job"} offset 10 + kube_pod_owner{job="ksm", owner_kind="Job"}', 110)
ORDER BY tags;
SELECT countIf(explain LIKE '%ReadFromMergeTree%.inner_id.samples.%') AS samples_table_reads,
       countIf(explain LIKE '%MaterializingCTEs%') > 0 AS uses_materialized_cte
FROM (EXPLAIN SELECT * FROM prometheusQuery('prometheus', 'kube_pod_owner{job="ksm", owner_kind="Job"} offset 10 + kube_pod_owner{job="ksm", owner_kind="Job"}', 110));

SELECT '-- a selector which occurs once is not materialized';
SELECT countIf(explain LIKE '%ReadFromMergeTree%.inner_id.samples.%') AS samples_table_reads,
       countIf(explain LIKE '%MaterializingCTEs%') > 0 AS uses_materialized_cte
FROM (EXPLAIN SELECT * FROM prometheusQuery('prometheus', 'max by (pod) (kube_pod_owner{job="ksm", owner_kind="Job"})', 100));

DROP TABLE prometheus;
