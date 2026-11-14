# A/B plan: fewer parts and rows in the in-order read of the id lookup
**Experiment:** ../reports/2026-11-14-idlookup-parts-and-rows.md · **Branch:** none yet: verifies the report's conclusions (the time ranges cache is offered, not built) · **Task:** 2026-11-13-handoff-fewer-parts-and-rows-in-the-in-
**Claim:** on a version 11 table under continuous small inserts, the id lookup costs per selected part and per row. A time ranges deduplication cache with outward rounding to the samples bucket cuts the time ranges rows into the join from ~5 to ~1 per id and makes the lookup 30–66% faster (wide to narrow) on a ~90-part tail, with false-positive ids ≈ new series per second × time since the bucket start. 5× larger inserts cut the streams ~4× and make it 15–35% faster.
**Evidence so far:** emulated and measured on 2 vCPU, local disk, master f0a9e6bffa71. The cache was emulated by writing its rows from a script into a separate table; the merge tail was forced (merges stopped for the last 15 s), because one node never lags. Unproven: the cache built into the sink; the numbers on object storage, where a part costs far more (~44 ms per in-order source in the #19 capture vs 0.5–6 ms here); the service's real steady-state part count under each variant; merge settings.

## Builds
- A: master at the base sha of the cache branch, once one exists. Until then: master only, for runs 1 and 2.
- B: the cache branch (if James says "build it"), rebased onto the same sha. Release build, no sanitizers.

## Machine
- Run 1: any machine; the per-part and per-row ratios are what count. 8 vCPU, 16 GiB, plus an S3-backed disk with a filesystem cache, to see the per-part cost on object storage (`bench/tags-in-subquery-cold/` has the S3 mock and disk config).
- Run 2: the service itself, or a staging copy with the same replicas and insert rate. A lagging merge tail can't be reproduced on one node.

## Data
- Run 1: `bench/v11-unmerged-idset/gen.sh` (`ts_merged`), then `gen-grid.sh` for 1/25/90 parts × 1/30 copies, plus 300 parts × 1 copy.
- Run 1, steady state: `steady.sh` with the variants of the report (60k series slots, 30 min lifetime, 300k history, 6 statements/s × 3,000 series, 15 s tail). Also `SLOTS=600000` (10× series), to check that the gains scale.
- Run 2: the production table. For one hour before and after each change, record `system.parts` per inner table (count, count with `level <= 1`) and `part_log` (new parts and merges per 5 min, tags and time ranges).

## Cases
| case | query | role | predicted A → B |
|---|---|---|---|
| grid narrow, 30 copies, 90 vs 1 part | `streams.py … g_p90_c30,g_p1_c30` narrow | target (idea 4) | 90 parts ≥ 10× one part, on S3 more |
| grid wide/mid/narrow, 1 copy, 90 vs 1 part | same, `_c1` | target (idea 4) | 2.5–4.5× |
| steady, cache vs baseline, wide/mid/narrow | `streams.py` on `s_base` / `s_tr` | target | −30% / −56% / −66% (±10 pts) |
| steady, cache, no new series | `s_basen` / `s_trn` | target | −42% / −64% / −75% |
| steady, cache at a bucket boundary | `s_tra` | edge | −21% / −47% / −56% |
| steady, batched ×5 | `s_b5` | target (settings) | −17% / −33% / −35% |
| false-positive ids | README query vs lookup ids | edge | +4.1% at a 30 min lifetime, 75 s into the bucket; ≈ B / (2 × lifetime) on average |
| PromQL results | `prometheusQuery` instant and range on `metric_0{job="job3"}` | correctness | identical A vs B |
| `/api/v1/series` over the last 30 s | Prometheus API | edge | B lists the series that stopped earlier in the bucket (disclosed) |
| merged table (`ts_merged`) | `streams.py ts_merged` | guard | unchanged |
| in-tree `promql_selector_*.xml`, `timeseries_*.xml` | perf.py | guard | unchanged |
| insert throughput | `tests/performance` TimeSeries insert test (upstream #125215 if merged), else `loop.py` latency p50/p95 | guard | B ≤ A + 5% |

## Method
- Run 1: two servers (A 9001, B 9002) on copies of the same data directory. For the lab tables, run `streams.py` against each (≥ 15 rounds, interleaved); for the in-tree XMLs, `clickhouse benchmark --host … --port 9001 --host … --port 9002 -i 50 --confidence 5`. Without a branch, run 1 is master only and checks the grid and the per-variant gaps on S3.
- Run 2 (settings, no build): change one writer setting at a time (remote-write `max_samples_per_send`, recording rule batching). Compare the lookup's `ProfileEvents['SelectedParts']`, the in-order processors' `elapsed_us` from `processors_profile_log`, and the rule's `query_duration_ms`, before vs after, over ≥ 100 rule executions each.

## Metrics
- Wall time (median, p95), CPU, `read_rows`, SelectedParts/SelectedMarks, in-order sources and their elapsed time, `MergingSortedTransform` elapsed, rows into `MergeJoinTransform`, the id set size (`Created Set with N entries`), memory.
- The cache (B): time ranges rows written per insert (the inner insert's `written_rows` in query_log), the cache's hit/miss ProfileEvents.

## Correctness
- PromQL results A vs B identical (`FORMAT TSV | md5sum`) for every case. The stateless test of the branch shows one time ranges row per series per bucket and an exact PromQL result for a series that stopped mid-bucket.

## Pass / fail
- **Pass:** the cache (B) makes the steady-state targets ≥ 20% / 40% / 50% faster (wide/mid/narrow) and significant, with no guard slower and insert throughput within 5%. Batching ×5 is ≥ 10% faster on the service's rule duration.
- **Fail:** if the gain on S3 is smaller than here, the per-row cost isn't the bottleneck there and parts are. Then the next step is merge scheduling or batching, not the cache.

## Checklist
- [ ] run 1 on master: grid + steady variants, on local disk and on S3; record sizes
- [ ] if "build it": the cache branch, then the A/B on the same data
- [ ] run 2 on the service: one writer setting at a time
- [ ] update the report verdict and the backlog row
