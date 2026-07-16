-- t4_1h_cpu_usage_by_namespace
-- type: range (1h), start: end-1h = 1706655585, end: end = 1706659185, step: 30s
-- source: cluster.libsonnet cpuUsage by namespace, 1h graph
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQueryRange(bench, '
sum(
  max by (cluster, namespace, pod, container) (
    node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate5m{cluster="bench"}
  )
) by (namespace)
', 1706655585, 1706659185, 30)
ORDER BY ALL;
