-- t2_rule_namespace_cpu_requests_sum
-- type: instant, at: end-60 = 1706659125
-- source: rules/apps.libsonnet namespace_cpu:kube_pod_container_resource_requests:sum
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQuery(bench, '
sum by (namespace, cluster) (
  sum by (namespace, pod, cluster) (
    max by (namespace, pod, container, cluster) (
      kube_pod_container_resource_requests{resource="cpu", job="kube-state-metrics"}
    ) * on(namespace, pod, cluster) group_left() max by (namespace, pod, cluster) (
      kube_pod_status_phase{phase=~"Pending|Running"} == 1
    )
  )
)
', 1706659125)
ORDER BY ALL;
