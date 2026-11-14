# A/B plan: keep one tags row per time series before the time ranges join
**Experiment:** ../reports/2026-11-13-selector-one-tags-row-per-id.md · **Branch:** `lilred-perf-selector-one-tags-row-per-id` @ e7c771817425 (base 2b1bef1d3b52) · **Task:** 2026-11-13-handoff-collapse-duplicate-tags-rows-be
**Claim:** on a version 11 table whose tags parts are not merged (60,000 series × 30 copies), a selector matching the whole metric builds its id set from 60,000 rows instead of 1.8M and its lookup is 15–25% faster; merged tables unchanged.
**Evidence so far:** emulated on 2 vCPU (the rewritten SQL on master f0a9e6bffa71): 1,820 → 1,473 ms (−19%), CPU −30%; merged ±2%; `LIMIT 1` on unmerged +24–50% (disclosed cost). Unproven: that the built branch produces this SQL (the test's reference is hand-edited), and the numbers at more threads.

## Builds
- A: master at 2b1bef1d3b52. B: the branch (based on the same sha). Release build, no sanitizers.

## Machine
- 8+ vCPU, 16+ GiB: the cost is per row on the merge path, so the ratio should hold; run `max_threads` 2 and 8 to see it. Nothing else running.

## Data
- `bench/v11-unmerged-idset/gen.sh` (defaults: `ts` 31 parts per inner table, 3.155M rows each; `ts_merged` 1 part). Also `REPEATS=10` (small) and `SERIES=200000` (large) for the scale check. Copy the data directory to B after the fill (`SYSTEM STOP MERGES` is in gen.sh), so both servers see the same parts.

## Cases
| case | query | role | predicted A → B |
|---|---|---|---|
| T1 | `SELECT count() FROM timeSeriesSelector(ts, 'metric_0{job=~"job.*",region!="none",instance=~"host-.*"}', 1700000000, 1700001800, 'samples')` | target | −10–20% end to end (lookup −19%, the samples read unchanged) |
| T2 | `bench.py` lookup, wide, `ts` (A runs `any_none_none` SQL, B must run its own query: use T1's `Created Set` log line + query_log) | target | rows into set 1.8M → 60k; lookup −15–25% |
| G1 | T1 with `metric_0{job="job3"}` and `metric_0{instance="host-17"}` | guard | ±3% |
| G2 | T1 on `ts_merged` (all three selectors) | guard | ±3% |
| G3 | `tests/performance/promql_selector_*.xml`, `timeseries_*.xml` that use selectors | guard | not slower |
| E1 | `/api/v1/series?match[]=metric_0&limit=1` on `ts` (Prometheus port 19094) | edge | +20–50% (known cost); on `ts_merged` ±10% |

## Method
- Two servers (A 19100, B 19200) on copies of the data directory, same config. `clickhouse benchmark --host 127.0.0.1 --port 19100 --host 127.0.0.1 --port 19200 -i 30 --confidence 5` per case, `max_threads` 2 and 8 (`max_threads_min_free_memory_per_thread = 0`). In-tree XMLs through `perf.py`.

## Metrics
- Wall time, CPU (`OSCPUVirtualTimeMicroseconds`), memory; `Created Set with N entries from M rows` (text_log, debug) for M; `MergeJoinTransform` input rows (`processors_profile_log`).

## Correctness
- Same results A vs B (`FORMAT TSV | md5sum`) for every case; `05638_time_series_selector_one_tags_row_per_id` passes on B and fails on A (the `from 6 rows` lines); `05452_time_series_selector_time_ranges_any_join`, `05493_time_series_selector_id_ranges_index_hint`, `04836_time_series_v10_selector_whole_metric_pk_range` pass on B.

## Pass / fail
- **Pass:** T1 at least 10% faster and T2 M = 60,000 on B, significant at 99.5%; G1–G3 not significantly slower; E1 on `ts_merged` not significantly slower.
- **Fail:** if T2's M stays 1.8M, `isDefaultIDGenerator` did not match the stored default (check the `Will execute query` line for `LIMIT 1 BY id`); if T1 does not move, the merge of the sorted streams dominates more than measured here (then #19 is the fix).

## Checklist
- [ ] build A and B; record both shas
- [ ] generate the data; record its size on disk
- [ ] run every case; save the raw output under `bench/v11-unmerged-idset/ab/`
- [ ] fill the branch column of `prs/selector-one-tags-row-per-id.md` and remove "pending build"
- [ ] update the report verdict and the backlog row
