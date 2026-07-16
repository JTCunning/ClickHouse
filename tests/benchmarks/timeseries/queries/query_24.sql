-- t5_30d_memory_rss_by_namespace
-- type: range (30d), start: 600 = 1704067800, end: end = 1706659185, step: 900s
-- source: raw rss gauge scan across full corpus
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQueryRange(bench, '
sum(
  max by (cluster, namespace, pod, container) (
    container_memory_rss{job="cadvisor", cluster="bench", container!=""}
  )
) by (namespace)
', 1704067800, 1706659185, 900)
ORDER BY ALL;
