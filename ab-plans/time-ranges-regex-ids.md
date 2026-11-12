# A/B plan: time ranges read for a non-literal `__name__` regex
**Experiment:** [../reports/2026-11-12-time-ranges-regex-ids.md](../reports/2026-11-12-time-ranges-regex-ids.md) · **Branch:** none yet. It checks the report's selectivity curve, and option A or B once James picks one. · **Task:** 2026-11-05-handoff-limit-the-time-ranges-read-for-
**Claim:**
- **Option B (`id IN` set from the `__name__` matchers):**
  - `{__name__=~".*cpu.*"}` at 0.7% of series: ids query −60–70%, time-ranges rows 2.1M → 65,536.
  - Break-even at about 10% of series; +15–40% above that, including +16% on the regex control of `promql_selector_name_alternatives.xml`.
- **Option A (metric-level set on top of upstream #115482):** the same pruning, with ≤2% overhead at every selectivity.

**Evidence so far:**
- Emulated on a master binary (the generated SQL with the set written in by hand), 2 vCPU, one part per table.
- Option A is modelled only: the metric-level set's cost was measured, its pruning wasn't (no build of #115482).
- Unproven: real builds, many parts per table, 16 cores, and capture-sized tables (tens of millions of series).

## Builds
- A: master at the base sha.
- B1: the option-B branch on the same sha.
- B2: #115482 merged into the same sha, plus the option-A change.
- Release builds without sanitizers, as in the CI perf job.

## Machine
- 16 vCPU / 64 GiB: the capture's 47.9M-series window needs it, and the set's memory has to be seen at that scale. Nothing else running. Note the CPU model.

## Data
- **Small:** `bench/time-ranges-regex-ids/gen.sh` (2.1M series, 400 names, one part per table) and its variant without `OPTIMIZE FINAL` (several parts).
- **Realistic:**
  - The same generator scaled to 20M series: 2,000 names × 10,000 series, plus a 20% churn of series outside the window.
  - Inserted in 8 batches, so the time ranges table keeps several parts per partition until merged; once with merges stopped, once merged.
- **Perf XML:** the fill of `tests/performance/promql_selector_name_alternatives.xml` (200k series, `max_insert_threads = 8`). Fill once on A, then copy the data directory to B (the multi-part fill is a draw between servers, see perf-lab section 4).

## Cases
| case | query | role | predicted A → B1 / B2 |
|---|---|---|---|
| narrow | `/api/v1/label/__name__/values?match[]={__name__=~".*cpu.*"}` (≤1% of series) | target | −60–70% / −65–75% |
| narrow + label | `{__name__=~".*cpu.*", instance="host-1"}` | target | −70% / same |
| 2.4% | `{__name__=~"metric_1[0-9]_total"}` | edge | −40% / same |
| 10% | `{__name__=~"metric_[1-4][0-9]_total"}` | edge | 0% / −5–15% |
| 25% | `{__name__=~"metric_1.*"}` | edge | +15–20% / ±5% |
| all | `{__name__=~".+"}` | edge | +30–40%, memory +12% / 0–2% |
| no name matcher | `{job="job-3"}` | guard | unchanged / unchanged |
| literal list | `{__name__=~"node_cpu_seconds_total\|process_cpu_seconds_total"}` | guard | unchanged / unchanged |
| in-tree | the 3 queries of `promql_selector_name_alternatives.xml` | guard | list and single metric unchanged; regex control +16% / ≤2% |
| PromQL | `count({__name__=~".*cpu.*"})` and `sum(rate({__name__=~".*cpu.*"}[5m]))` via `prometheusQuery` | target | ids share of the query time removed / same |

B2's predictions are B1's measured time minus the measured cost of the id set (about 0.25 µs per id plus the second `metric_name` scan); the pruning is assumed equal.

## Method
- Two servers (A on 9001, B on 9002) on copies of the same data directory, same config, `max_threads` set explicitly (2, 8 and 16), query condition cache off.
- **Per case:** warm-up, then `clickhouse benchmark --host 127.0.0.1 --port 9001 --host 127.0.0.1 --port 9002 -i 30 --confidence 5`. The HTTP endpoints go through a small loop like `api_bench.py` pointed at each server.
- **In-tree XML:** through `tests/performance/scripts/perf.py` against both servers.
- **A/A pass:** master against master, for the noise figure.

## Metrics
- Wall time (median, p95).
- CPU (`ProfileEvents['OSCPUVirtualTimeMicroseconds']`).
- `read_rows`, `read_bytes`, `memory_usage`.
- Selected granules of the time ranges table (`text_log` "marks by primary key", or `EXPLAIN indexes = 1` on the captured SQL).
- `CreatingSetsTransform` set sizes.

## Correctness
- Identical results A vs B for every case (`FORMAT TSV | md5sum`), including the series outside the window and the series written with another id generator (the 05493 setup).
- The new stateless test passes on B and fails on A on its read-rows line.

## Pass / fail
- **B1 passes** if the narrow targets improve by at least 40% at 99.5% significance, the guards don't move, and James accepts the measured cost above ~10% of series. Otherwise it fails, and option A or C stands.
- **B2 passes** if the narrow targets match B1 within 10%, and no case at any selectivity is significantly slower than A. **Fail** means #115482's chain doesn't serve `IN` sets: add that case there first.

## Checklist
- [ ] build A, B1 or B2; record the shas
- [ ] generate the data; record its size on disk and parts per table
- [ ] run every case; save the raw output under `bench/time-ranges-regex-ids/ab/`
- [ ] update the report verdict and the backlog row
