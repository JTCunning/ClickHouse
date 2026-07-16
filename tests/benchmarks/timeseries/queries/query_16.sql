-- t3_panel_node_cpu_utilisation_raw
-- type: instant, at: end-60 = 1706659125
-- source: node-exporter raw CPU utilisation
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQuery(bench, '
1 - avg by (cluster) (
  rate(node_cpu_seconds_total{job="node-exporter", mode="idle", cluster="bench"}[5m])
)
', 1706659125)
ORDER BY ALL;
