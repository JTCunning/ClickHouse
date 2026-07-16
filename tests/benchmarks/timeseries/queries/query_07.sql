-- t2_rule_pod_owner_relabel_deployment
-- type: instant, at: end-60 = 1706659125
-- source: rules/apps.libsonnet namespace_workload_pod:kube_pod_owner:relabel (Deployment)
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQuery(bench, '
max by (cluster, namespace, workload, pod) (
  label_replace(
    label_replace(
      kube_pod_owner{job="kube-state-metrics", owner_kind="ReplicaSet"},
      "replicaset", "$1", "owner_name", "(.*)"
    ) * on(replicaset, namespace, cluster) group_left(owner_name) topk by(replicaset, namespace, cluster) (
      1, max by (replicaset, namespace, owner_name, cluster) (
        kube_replicaset_owner{job="kube-state-metrics", owner_kind="Deployment"}
      )
    ),
    "workload", "$1", "owner_name", "(.*)"
  )
)
', 1706659125)
ORDER BY ALL;
