# A/B plan: do not optimize the join order of the `TimeSeries` selector's ids query
**Experiment:** ../reports/2026-11-14-idlookup-adaptive-index.md · **Branch:** `lilred-perf-selector-ids-fixed-join-order` @ df2da4e4f1c3 (base 923c77c86030) · **Task:** 2026-11-13-handoff-use-the-tags-text-index-statis (#20)
**Claim:** On object storage with statistics not cached (64 fresh parts per inner table, 50 ms per GET), the selector's id lookup is 9–13% faster for narrow, rare-token, wide and metric-only selectors; `LoadedStatistics` of the lookup drops to 0. Prewarmed and warm: unchanged (±3%).
**Evidence so far:** emulated on 2 vCPU, master `f0a9e6bffa71` with `query_plan_optimize_join_order_limit = 0` on the ids query (exactly what the branch sets). Unproven: the built branch applies it to the in-place `IN` set build (the test's `join order optimizations: 0` lines say so), and the gain on more cores and bigger parts.

## Builds
- A: master at 923c77c86030. B: the branch (same base). Release, no sanitizers.

## Machine
- 8+ vCPU, 16 GiB: the S3 mock and the server need their own cores (on 2 vCPU they share them). Nothing else running.

## Data
- `bench/tags-in-subquery-cold/gen.sh` (64 inserts × 15 min, merges stopped, 21,560 series) on the lab server (`server-config.xml.template`, S3 mock at 50 ms per GET). Merges resume when the server restarts: run `gen.sh` again after every start, on both A and B (or copy `s3root` + metadata while both are stopped and stop merges right after start).
- Also one larger case: `BATCHES=128` (128 parts).

## Cases
| case | query | role | predicted A → B |
|---|---|---|---|
| T1 | `q/nc.sql` (3 exact matchers), cold | target | 2,177 → ~1,960 ms (−10%) |
| T2 | `q/nr.sql` (rare token), cold | target | 1,287 → ~1,165 ms (−9%) |
| T3 | `q/w.sql` (name regex + matcher), cold | target | 3,113 → ~2,810 ms (−10%) |
| T4 | `q/lf.sql` (metric only), cold | target | 2,283 → ~1,990 ms (−13%) |
| T5 | the full PromQL query of `gap.py` (narrow), cold | target | planning ends ~210 ms earlier; `Creating set` gap unchanged |
| G1 | T1–T4 prewarmed (`run.py --state meta`) | guard | unchanged (±3%) |
| G2 | T1–T4 warm, 30 interleaved rounds (`warm.py`) | guard | unchanged (±3%) |
| G3 | `tests/performance/promql_selector_*.xml`, `timeseries_*.xml` | guard | unchanged |
| E1 | `/api/v1/series?match[]=…` and `/api/v1/labels` on the lab table, warm | edge | unchanged |

On B the queries in `q/` are the same SQL as on A (the setting is inside the selector); for T1–T4 on B, run them through `prometheusQueryRange` like T5 or compare `capture.py` output from both builds first (it must be byte-identical).

## Method
- A and B one after the other on copies of the same lab data (the S3 state can't be shared by two servers), each case interleaved per round: `run.py --state cold|meta --reps 15`; warm with `warm.py <port> warm 30 q nc,nr,w,lf base`.
- Guard XMLs through `tests/performance/scripts/perf.py` against both servers.

## Metrics
- Wall (median, p95), CPU (`OSCPUVirtualTimeMicroseconds`), S3 GETs, `LoadedStatistics`, `LoadedStatisticsMicroseconds`; `EXPLAIN indexes = 1` wall for the planning share.

## Correctness
- Ids of the four selectors identical A vs B (`SELECT count(), sum(cityHash64(*)) FROM (<ids query>)`), and `prometheusQueryRange` results identical (`FORMAT TSV | md5sum`).
- `05640_time_series_selector_ids_query_fixed_join_order` passes on B; on A its four `join order optimizations:` lines print `1`. Also run 05452, 05493, 05638 (if #18's branch is merged first).

## Pass / fail
- **Pass:** T1–T4 at least 7% faster on B, significant at 99.5%, `LoadedStatistics` 0 on B cold; G1–G3 and E1 not significantly slower.
- **Fail:** if T1–T4 don't move, the ids plan is optimized with the outer query's settings (a non-in-place set build) and the setting must move to where the set's plan is optimized; if a guard is slower, the swap master chose (`Type: right`) was worth something: report the case.

## Checklist
- [ ] build A and B; record both shas
- [ ] generate the data on both; record parts and size
- [ ] run every case; save raw output under `bench/idlookup-adaptive-index/ab/`
- [ ] fill the branch column of `prs/selector-ids-fixed-join-order.md` and remove "pending build"
- [ ] update the report verdict and the backlog row
