-- t3_panel_pods_by_namespace
-- type: instant, at: end-60 = 1706659125
-- source: cluster.libsonnet pod count by namespace
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQuery(bench, '
sum(kube_pod_owner{job="kube-state-metrics", cluster="bench"}) by (namespace)
', 1706659125)
ORDER BY ALL;
