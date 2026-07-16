-- t5_7d_cpu_usage_by_namespace
-- type: range (7d), start: end-7d = 1706054385, end: end = 1706659185, step: 300s
-- source: raw cAdvisor rate scan over 7d (block-resident on Prometheus)
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQueryRange(bench, '
sum by (namespace) (
  rate(container_cpu_usage_seconds_total{job="cadvisor", image!=""}[5m])
)
', 1706054385, 1706659185, 300)
ORDER BY ALL;
