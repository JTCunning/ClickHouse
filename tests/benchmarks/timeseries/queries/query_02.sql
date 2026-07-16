-- t1_select_container_mem_regex
-- type: instant, at: end-60 = 1706659125
-- source: cAdvisor selector with image!="" + namespace regex (mixin idiom)
-- provenance: kube-prometheus-stack mixin (expression verbatim, whitespace preserved)
SELECT *
FROM prometheusQuery(bench, '
container_memory_working_set_bytes{job="cadvisor", cluster="bench", image!="", namespace=~"ns-.*"}
', 1706659125)
ORDER BY ALL;
