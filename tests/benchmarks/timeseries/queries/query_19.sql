-- t4_24h_cpu_usage_by_namespace
-- type: range (24h), start: end-24h = 1706572785, end: end = 1706659185, step: 60s
-- source: cluster.libsonnet cpuUsage by namespace, 24h graph
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQueryRange(bench, '
sum(
  max by (cluster, namespace, pod, container) (
    node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate5m{cluster="bench"}
  )
) by (namespace)
', 1706572785, 1706659185, 60)
ORDER BY ALL;
