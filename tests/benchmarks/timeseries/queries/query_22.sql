-- t5_7d_memory_rss_by_namespace
-- type: range (7d), start: end-7d = 1706054385, end: end = 1706659185, step: 300s
-- source: raw rss gauge scan over 7d
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQueryRange(bench, '
sum(
  max by (cluster, namespace, pod, container) (
    container_memory_rss{job="cadvisor", cluster="bench", container!=""}
  )
) by (namespace)
', 1706054385, 1706659185, 300)
ORDER BY ALL;
