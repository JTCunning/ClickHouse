# A/B plan: read the TimeSeries time ranges table by metric for a `__name__` regex
**Experiment:** ../reports/2026-11-14-time-ranges-metric-filter.md · **Branch:** `lilred-perf-time-ranges-metric-filter` @ `22340b032da2` · **Task:** 2026-11-05-handoff-limit-the-time-ranges-read-for-
**Claim:** with upstream #115482, a selector whose `__name__` regex matches 1 of 200 metrics (`.*_199` in `promql_selector_name_alternatives.xml`) is at least 15% faster, and its time-ranges rows drop to that metric's granules. The broad-regex control `m_1[0-9][0-9]` is at most 10% slower.
**Evidence so far:** emulated on master, 2 vCPU: narrow −22% to −31%, broad +6% to +11%. Pruning was emulated with an `indexHint` over the matched metrics' id ranges, and the set's cost was measured directly. Not yet proven: that a rebased #115482 prunes `tupleElement(id, 1) IN (subquery)` (only a code reading so far), and where the broad case's extra ~10–19 ms goes.

## Builds
- **Blocked until #115482 is rebased onto master.** It conflicts in `KeyCondition.cpp`/`.h`.
- A = master at a common sha + #115482 (rebased). B = A + the branch, rebased onto the same sha. Release, no sanitizers.
- A2 = plain master: it shows #115482 alone changes nothing here.

## Machine
- 16 vCPU or more, nothing else running, CPU model noted. The claim isn't about parallelism, so 8 vCPU also works; the XML runs with `max_threads = 4`.

## Data
- **XML:** the fill of `promql_selector_name_alternatives.xml` (200 metrics × 1,000 series × 20 samples).
- **Scale:** `bench/time-ranges-regex-ids/gen.sh` (400 metrics × 5,000 series = 2.1M series, plus 100k off-window).
- **Wide:** the same generator with 4,000 metrics × 500 series, so that each granule holds fewer metrics.

## Cases
| case | query | role | predicted A → B |
|---|---|---|---|
| T1 | XML `.*_199` (new query) | target | −15 to −30% |
| T2 | scale: `{__name__=~".*cpu.*"}` (0.7% of series), ids query via `/api/v1/series` | target | 222 → ~80 ms (2 vCPU figures) |
| T3 | scale: `metric_1[0-9]_total` (2.4%) | target | −30 to −40% |
| G1 | XML `m_1[0-9][0-9]` (100 of 200 metrics) | guard | ≤ +10% (emulated +6–11%) |
| G2 | XML 100-name list, `m_100` | guard | unchanged (no filter: id ranges) |
| G3 | scale: `{job="job-3"}` (no `__name__` matcher) | guard | unchanged (no filter) |
| E1 | scale: `{__name__=~".+"}` (all metrics) | edge | the worst case for the set: report it |
| E2 | `promql_selector_pk_range.xml`, `promql_selector_id_range_row_cost.xml` | guard | unchanged |

## Method
- Two servers on copies of the same data directory (A on 9001, B on 9002), same config.
- Per case: `clickhouse benchmark --host 127.0.0.1 --port 9001 --host 127.0.0.1 --port 9002 -i 50 --confidence 5`.
- The XMLs through `tests/performance/scripts/perf.py` against both servers.
- **Profile G1 on B** with `log_processors_profiles = 1`: sum `elapsed_us` from `system.processors_profile_log` by processor name, B vs A. This attributes the extra ~10–19 ms the emulation couldn't attribute (the set build alone is 2–3 ms).

## Metrics
- Wall time (median, p95), `read_rows`, `read_bytes`, `memory_usage`.
- `ProfileEvents['SelectedMarks']` for the time ranges table: T1 should select about 1/25 of its marks on B.

## Correctness
- `05768` passes on B. On A2 its metric-filter line prints `05768_regex 0` and its last line `0 1`. Without #115482 (the branch alone), the last line is `0 1`.
- `05493` passes on B (`05493_regex 0 1`, pruning `1 1`). Also run `05452`, `04836` and `05446`.
- The results of every case hash identically on A and B.

## Pass / fail
- **Pass:** T1 and T3 at least 15% faster at 99.5%; T1 reads the time ranges table over ≤ 2 of ~25 granules; G1 at most 10% slower; G2, G3 and E2 not significantly slower.
- **Fail:** if G1 is more than 10% slower, or T1 doesn't prune, the profile shows why. Then either fix the plan cost or bring James the trade-off (the narrow gain vs the broad cost) before submitting. If #115482 doesn't prune the `IN` form, the branch waits for an `IN` case in #115482.

## Checklist
- [ ] #115482 rebased onto master (by its author, a reviewer or James); record the sha
- [ ] build A, A2 and B; record the shas
- [ ] generate the data; record its size on disk
- [ ] run every case and the G1 profile; save the raw output under `bench/time-ranges-metric-filter/ab/`
- [ ] fill the branch column of `prs/time-ranges-metric-filter.md` and remove "pending build"
- [ ] update the report verdict and the backlog row
