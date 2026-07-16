-- t4_1h_memory_usage_by_namespace
-- type: range (1h), start: end-1h = 1706655585, end: end = 1706659185, step: 30s
-- source: cluster.libsonnet memoryUsage by namespace, 1h graph
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQueryRange(bench, '
sum(
  max by (cluster, namespace, pod, container) (
    container_memory_rss{job="cadvisor", cluster="bench", container!=""}
  )
) by (namespace)
', 1706655585, 1706659185, 30)
ORDER BY ALL;
