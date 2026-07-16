-- t3_panel_cpu_utilisation
-- type: instant, at: end-60 = 1706659125
-- source: cluster.libsonnet cpuUtilisation
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQuery(bench, '
cluster:node_cpu:ratio_rate5m{cluster="bench"}
', 1706659125)
ORDER BY ALL;
