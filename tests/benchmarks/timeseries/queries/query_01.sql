-- t1_select_kube_pod_info
-- type: instant, at: end-60 = 1706659125
-- source: selector primitive (kube-state-metrics)
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQuery(bench, '
kube_pod_info{cluster="bench", namespace="ns-0"}
', 1706659125)
ORDER BY ALL;
