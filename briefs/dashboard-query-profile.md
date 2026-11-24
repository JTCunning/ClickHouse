# Brief: where PromQL query time goes on real dashboards and alert rules

- **ID:** `dashboard-query-profile` · **Area:** querying · **Written:** 2026-11-24 by lil red, against master `05f159a69597`
- **Tracking issue:** none yet (see README, "Where the result goes")
- **Spend:** James's cap. Stop points below.

## Question

On a realistic dataset, with the queries that Grafana dashboards and alert rules actually send, where does PromQL query time go on today's master, stage by stage? Which three changes would cut it most on those queries? Count only changes that no open upstream PR already covers. If there's budget left, how much do the in-flight PRs that touch the top stages buy on the same queries?

## Why it matters

lil red's query work so far measured one mechanism at a time on small synthetic tables. Function-level gains have often shrunk end to end: `timeSeriesFromGrid`'s typed write was 2× at the function level and 3–4% on PromQL range queries. James's standing rule is to judge on normal-use queries. This brief builds the realistic query profile that every later query change should be judged against. That makes its query set and dataset scripts deliverables in their own right.

## What lil red already knows (verify, don't trust)

- **One wide range query at 16 streams** (`reports/2026-11-09-join-fixed-cost.md`): about 70% of CPU is decoding samples and about 19% is grid aggregation. The id lookup's join costs about 1 ms.
- **`/api/v1/query_range`:**
  - It formatted each step timestamp again for every series: 40–56 ms of a 700 ms request with 2,000 series × 1,441 points. James's draft upstream PR 133886 fixes that.
- **The recent-table choice** (`reports/2026-11-22-recent-samples-merge.md`, section 2): a range that starts inside the recent samples table's TTL reads that table (`time_series_prefer_recent_samples_table`). For narrow selectors over 1 day or more, that is 1.6–3.9× slower than reading the samples table. Small fix possible, not built.
- **The id lookup** (`reports/2026-11-14-idlookup-parts-and-rows.md`, `reports/2026-11-14-idlookup-adaptive-index.md`): it costs per unmerged part and per row in the time ranges table's tail. Cold S3 adds the text index and statistics loads.
- **Bytes read** (`reports/2026-11-08-samples-timestamp-read-cost.md`): timestamps with jitter can be most of the bytes read. See `timestamp-alignment.md`.
- **An idea, not done:** `arrayFilter` returns a copy even when every element passes, and the converter's stale-marker filter wraps every range selector. Upstream commit d31d9c4ed33d chose copying on purpose; the reason is unknown.

## Already in flight upstream (re-check each)

| what the PRs do | PRs |
|---|---|
| grid functions | 124777 (dense buckets as an array), 122983 (draft: one sorted buffer), 131989 (draft: arena), 133128 |
| the selector and the plan | 130402 (James: repeated selector's id set once), 126763 (James), 121069 (fused metric drop, subquery elimination), 128177 (push matchers into the "one" side), 121107, 131126, 118116, 118801, 127848, 132565 (tags as a column), 126221 (James, draft) |
| the HTTP API | 133886 (James, draft: shared step timestamps), 125230 (response compression), 125220 (query cache), 122585 (split long ranges, cache chunks) |
| executor | 121216 (draft: bucketed native execution) |

## Method

1. **Dataset.** Make it look like a real fleet, sized to the machine (aim for 0.5–2M active series, 14 days at a 15 s scrape, 1–5% churn per hour):
   - node_exporter-like hosts: `node_cpu_seconds_total{mode}`, memory, filesystem, network by device
   - container and Kubernetes-like pods: `container_cpu_usage_seconds_total`, `container_network_receive_bytes_total`, `kube_pod_container_status_restarts_total`
   - one HTTP service with handlers × instances: `http_requests_total{code,method,handler}` and `http_request_duration_seconds_bucket{le}` with 12 buckets
   - `up` for every target

   Load it through remote write from real Prometheus scraping (real jitter; see `ingest-at-scale.md`), or generate it directly with a stated jitter model. Use default table settings, and record the TimeSeries version.
2. **The query set.** Send the queries the way Grafana does, through `/api/v1/query_range` and `/api/v1/query`, with the panels of one dashboard running at the same time. Run the dashboard queries at four range and step pairs: 1 h at 15 s, 6 h at 30 s, 24 h at 2 m, 7 d at 10 m. Evaluate the alert rules as instant queries at "now".

   | label | PromQL | kind |
   |---|---|---|
   | D1 | `sum by (instance) (rate(node_cpu_seconds_total{mode!="idle"}[5m]))` | dashboard |
   | D2 | `node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes` | dashboard, one-to-one match |
   | D3 | `sum by (namespace) (rate(container_cpu_usage_seconds_total{container!=""}[5m]))` | dashboard |
   | D4 | `histogram_quantile(0.99, sum by (le, handler) (rate(http_request_duration_seconds_bucket[5m])))` | dashboard |
   | D5 | `sum by (code) (rate(http_requests_total{job="api"}[5m]))` | dashboard |
   | D6 | `topk(10, sum by (pod) (rate(container_network_receive_bytes_total[5m])))` | dashboard |
   | D7 | `rate(node_network_receive_bytes_total{device!~"lo\|veth.*"}[5m])` | dashboard, many output series |
   | D8 | `sum by (handler) (increase(http_requests_total[1h]))` | dashboard (24 h and 7 d only) |
   | A1 | `up == 0` | alert |
   | A2 | `sum by (job) (rate(http_requests_total{code=~"5.."}[5m])) / sum by (job) (rate(http_requests_total[5m])) > 0.05` | alert |
   | A3 | `histogram_quantile(0.99, sum by (le, job) (rate(http_request_duration_seconds_bucket[5m]))) > 1` | alert |
   | A4 | `predict_linear(node_filesystem_avail_bytes[6h], 4 * 3600) < 0` | alert |
   | A5 | `increase(kube_pod_container_status_restarts_total[1h]) > 3` | alert |
   | M1 | `/api/v1/series?match[]=up` over 1 h | Grafana variable |
   | M2 | `/api/v1/label/instance/values` over 1 h | Grafana variable |

   Replace a query when the dataset makes it unrealistic, and say why. Keep the labels.
3. **Latency.** For each query × range: p50 and p90 over at least 10 runs, warm. Then a cold variant: mark, uncompressed and page caches dropped. Optional and valuable: the same queries against a real Prometheus over the same data, as a reference.
4. **Stage breakdown.** For each query, split server time into: PromQL to SQL conversion and planning, the id lookup (tags and time ranges), the samples read and decode, the grid functions, aggregation and joins, and result formatting and HTTP. Use `system.query_log` (duration and ProfileEvents: `SelectedParts`, `SelectedMarks`, `ReadCompressedBytes`, CPU) and `system.trace_log` with the CPU profiler. Run `EXPLAIN PIPELINE` and `EXPLAIN indexes = 1` on the SQL each query runs.
5. **Stop point 1.** Report the profile table. If the top stages are all covered by in-flight PRs, go to step 7.
6. **Top three changes.** Rank them by the expected gain summed over the query set. Check overlap for each, and give a one-line design. Prototype the best one that's uncovered and A/B it on the full query set, with a `Query | Base | This PR` table, the full query in every row.
7. **Optional: in-flight PRs on the same set.** Apply the open PRs that touch the top stages, where they apply cleanly to master. Measure each one alone against master on the same query set. James's own PRs come first (130402, 126763, 133886): realistic numbers help them get reviewed.

## Deliverables

- The dataset generator and the query runner, reusable for every later A/B. That's the biggest deliverable.
- A profile table: query × range | p50 | p90 | the top three stages with their shares.
- The top three changes, each with its expected gain, its overlap and a design line.
- Optional: a prototype A/B, and an in-flight PR table.

## Out of scope

Correctness and compliance differences: note in one line, lil red routes them. Native histograms. Distributed tables.
