-- t3_panel_cpu_requests_commitment
-- type: instant, at: end-60 = 1706659125
-- source: cluster.libsonnet cpuRequestsCommitment
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQuery(bench, '
sum(namespace_cpu:kube_pod_container_resource_requests:sum{cluster="bench"})
/
sum(kube_node_status_allocatable{job="kube-state-metrics", resource="cpu", cluster="bench"})
', 1706659125)
ORDER BY ALL;
