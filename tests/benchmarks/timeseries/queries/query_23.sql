-- t5_30d_cpu_usage_by_namespace
-- type: range (30d), start: 600 = 1704067800, end: end = 1706659185, step: 900s
-- source: raw cAdvisor rate scan across full corpus (rate pad at start)
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQueryRange(bench, '
sum by (namespace) (
  rate(container_cpu_usage_seconds_total{job="cadvisor", image!=""}[5m])
)
', 1704067800, 1706659185, 900)
ORDER BY ALL;
