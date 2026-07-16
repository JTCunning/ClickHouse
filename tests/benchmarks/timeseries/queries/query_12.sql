-- t3_panel_cpu_usage_by_namespace
-- type: instant, at: end-60 = 1706659125
-- source: cluster.libsonnet cpuUsage by namespace
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQuery(bench, '
sum(
  max by (cluster, namespace, pod, container) (
    node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate5m{cluster="bench"}
  )
) by (namespace)
', 1706659125)
ORDER BY ALL;
