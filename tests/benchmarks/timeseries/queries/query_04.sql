-- t2_rule_container_working_set_join
-- type: instant, at: end-60 = 1706659125
-- source: rules/apps.libsonnet node_namespace_pod_container:..._working_set_bytes
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQuery(bench, '
container_memory_working_set_bytes{job="cadvisor", image!=""}
* on (cluster, namespace, pod) group_left(node) topk by(cluster, namespace, pod) (
  1, max by(cluster, namespace, pod, node) (kube_pod_info{node!=""})
)
', 1706659125)
ORDER BY ALL;
