-- t3_panel_memory_utilisation
-- type: instant, at: end-60 = 1706659125
-- source: cluster.libsonnet memoryUtilisation
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQuery(bench, '
1 - sum(:node_memory_MemAvailable_bytes:sum{cluster="bench"})
/
sum(node_memory_MemTotal_bytes{job="node-exporter", cluster="bench"})
', 1706659125)
ORDER BY ALL;
