-- t3_panel_memory_usage_by_namespace
-- type: instant, at: end-60 = 1706659125
-- source: cluster.libsonnet memoryUsage by namespace
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQuery(bench, '
sum(
  max by (cluster, namespace, pod, container) (
    container_memory_rss{job="cadvisor", cluster="bench", container!=""}
  )
) by (namespace)
', 1706659125)
ORDER BY ALL;
