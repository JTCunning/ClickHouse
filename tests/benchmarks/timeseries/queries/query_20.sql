-- t4_24h_memory_usage_by_namespace
-- type: range (24h), start: end-24h = 1706572785, end: end = 1706659185, step: 60s
-- source: cluster.libsonnet memoryUsage by namespace, 24h graph
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQueryRange(bench, '
sum(
  max by (cluster, namespace, pod, container) (
    container_memory_rss{job="cadvisor", cluster="bench", container!=""}
  )
) by (namespace)
', 1706572785, 1706659185, 60)
ORDER BY ALL;
