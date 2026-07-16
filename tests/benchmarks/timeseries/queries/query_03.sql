-- t2_rule_node_ns_pod_ctr_cpu_sum_irate
-- type: instant, at: end-60 = 1706659125
-- source: rules/apps.libsonnet node_namespace_pod_container:..._seconds_total:sum_irate
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQuery(bench, '
sum by (cluster, namespace, pod, container) (
  irate(container_cpu_usage_seconds_total{job="cadvisor", image!=""}[5m])
) * on (cluster, namespace, pod) group_left(node) topk by (cluster, namespace, pod) (
  1, max by(cluster, namespace, pod, node) (kube_pod_info{node!=""})
)
', 1706659125)
ORDER BY ALL;
