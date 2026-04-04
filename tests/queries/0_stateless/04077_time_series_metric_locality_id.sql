SET allow_experimental_time_series_table = 1;

-- Edge cases
SELECT hex(timeSeriesMetricLocalityId(''));
SELECT hex(timeSeriesMetricLocalityId('ab'));

-- Typical kube / node_exporter / Prometheus / ClickHouse dashboard metric names, ordered by locality id
-- (same order as MergeTree `ORDER BY metric_locality_id` prefix for these values).
SELECT
    hex(timeSeriesMetricLocalityId(name)) AS locality_hex,
    timeSeriesMetricLocalityId(name) AS locality_id,
    name
FROM VALUES(
    'name String',
    ('container_cpu_usage_seconds_total'),
    ('kube_node_cpu_seconds_total'),
    ('kube_namespace_labels'),
    ('kube_pod_info'),
    ('machine_memory_bytes'),
    ('node_disk_io_time_seconds_total'),
    ('node_memory_MemAvailable_bytes'),
    ('prometheus_http_requests_total'),
    ('prometheus_target_interval_length_seconds'),
    ('prometheus_tsdb_head_series'),
    ('up'),
    ('ClickHouseAsyncMetrics_BlockActiveTime_sdan'),
    ('ClickHouseAsyncMetrics_OSIOWaitMicroseconds'),
    ('ClickHouseMetrics_QueryProfilerRuns'),
    ('ClickHouseProfileEvents_AddressesDiscovered'),
    ('ClickHouseProfileEvents_Query')
) AS dashboard_metrics
ORDER BY locality_id, name;
