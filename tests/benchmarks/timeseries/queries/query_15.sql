-- t3_panel_workloads_by_namespace
-- type: instant, at: end-60 = 1706659125
-- source: cluster.libsonnet workload count by namespace
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQuery(bench, '
count(
  avg(namespace_workload_pod:kube_pod_owner:relabel{cluster="bench"}) by (workload, namespace)
) by (namespace)
', 1706659125)
ORDER BY ALL;
