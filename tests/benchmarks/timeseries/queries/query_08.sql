-- t2_rule_pod_owner_relabel_job_or
-- type: instant, at: end-60 = 1706659125
-- source: rules/apps.libsonnet namespace_workload_pod:kube_pod_owner:relabel (Job variant)
-- note: label_join + `or` compat probe (empty on both backends; parse/execute signal)
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQuery(bench, '
max by (cluster, namespace, workload, pod) (
  label_replace(
    kube_pod_owner{job="kube-state-metrics", owner_kind="Job"},
    "workload", "$1", "owner_name", "(.*)"
  )
)
or
label_join(
  kube_pod_owner{job="kube-state-metrics", owner_kind="Job"},
  "workload", "/", "owner_name"
)
', 1706659125)
ORDER BY ALL;
