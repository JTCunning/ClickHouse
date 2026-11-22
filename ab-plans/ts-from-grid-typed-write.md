# A/B plan: write the timestamps of `timeSeriesFromGrid` and `timeSeriesRange` straight into their typed column
**Experiment:** ../reports/2026-11-22-ts-from-grid-typed-write.md · **Branch:** `lilred-perf-ts-from-grid-typed-write` @ `a25c640b65b3` (base master `521c4bcb7b94`) · **Task:** weekend batch 2026-11-22, target 2 · **Outbox:** `ts-from-grid-typed-write` (`ready-to-build`)
**Claim:** `timeSeriesFromGrid` alone gets 20–62% faster on rows without NULL values (8000 × 300 points: −50..−62% emulated) and `timeSeriesRange` 23–60%; rows with NULL values get faster too (size unproven); a PromQL range query that returns 4000 series × 300 steps gets about 1–4% faster (emulated −3.1..−3.6%), 2–6% at a step of half the scrape interval; an aggregated query (one output series) is unchanged; results identical.
**Evidence so far:**
- Emulated by subtraction on master `521c4bcb7b94`, 2 vCPU, `max_threads = 1`, two passes of 10 runs after a warm-up (`bench/ts-from-grid-typed-write/emulate-2026-11-22-pass*.txt`). The change is inside a function, so no SQL emulation of B exists. The timestamp write through `Field` costs 5.0–6.2 ns more per point than a typed write: `timeSeriesRange` − `range` of the same 300 points (5.0–5.4 ns, Field route against a typed loop, no values) and `timeSeriesFromGrid` − (`range` + `arrayFilter` copy of the values) (5.4–5.7 ns) up to − `range` alone (6.1–6.2 ns: that control skips the value copy, so it overstates). `timeSeriesFromGrid` over 8000 × 300 dense values: 23.6–23.9 ms on master; A/A (same text, same round) −0.5..0%.
- Rows with NULL values: no SQL control does what the branch still does there (a branch per position and a virtual `insertFrom` per kept value). The per-point delta above, applied to the 1.2M kept points, gives −23..−29% (an upper estimate); `arrayFilter` controls give −9..−10% but write and filter all 300 timestamps. So T4 is judged on direction only.
- Query level (`bench/ts-from-grid-typed-write/promql-2026-11-22.txt`, 7 runs after a warm-up): `last_over_time(foo[10])` on the table of `promql_set_operator_presence_mask.xml`, 100..3090 step 10 = 1.2M output points: 212.7 ms wall at 1 thread, 205.8 ms at 2; the final `FilterTransform` (the `WHERE notEmpty(samples)` step that computes `timeSeriesFromGrid`) 14.7–15.2 ms; saving 1.2M × 5.0–6.2 ns = 6–7.5 ms = −3.1..−3.6% wall (−3.4..−3.9% CPU). The `and`/`or` queries spend the same 6–7.5 ms in 350–370 ms (−1.8..−2.1%). At step 5 with `[20]` (2.4M points): 256.7 ms, −5.0..−5.8%. `sum(...)`: 300 points, 0. At `max_threads = 16` the final filter runs × 8 (`EXPLAIN PIPELINE`), so its share should not grow with cores.
- 2026-11-22 dry run on master `521c4bcb7b94` (`bench/ts-from-grid-typed-write/smoke-2026-11-22.txt`): kit ran end to end as A/A (ports 19001/19002, N=20); preconditions as below; hashes identical on all 10 cases, none empty; no case differs at 99.5%, A/A medians up to 7.1% apart on wall (T1) and 9.6% on CPU (T5) at 16–24 runs per server. B~ can't be run, so question 4 (direction) rests on the emulation. Edited after the dry run, not re-run: the role column of `cases.tsv` (T1, T1x8, T2 → `measured`, T4 → `target-direction`; printed only).
- Unproven: the size on the build host (#23 measured 5–7 ns per sample for two `Field` writes there, so one may cost 2.5–3.5 ns); the NULL-row path; whether the fill loop vectorizes (not needed for the claim).

## Builds
- A: master at `521c4bcb7b94`. B: the branch (one commit on the same base). Release build, no sanitizers, as in the CI performance job.

## Machine
- ≥ 8 vCPU / 16 GiB (T1x8 runs at `max_threads = 8`); the 16-core host of earlier runs is fine. Nothing else running. Note the CPU model.

## Data
- `bench/ts-from-grid-typed-write/gen.sql`, run by `ab/ab.sh gen PORT` on each server: `fg_dense` and `fg_null` (`Memory`, 8000 rows × 300 `Nullable(Float64)` values, 0% and 50% NULL at sipHash-random positions) and the `TimeSeries` table of `tests/performance/promql_set_operator_presence_mask.xml` (create and fill copied verbatim: `foo`/`bar` 4000 series each × 300 samples 10 s apart from t=100; `foo_sparse`/`bar_sparse` with half the samples). `gen` then merges each inner table to one part. Deterministic values, so both servers hold the same data; `ab.sh layout PA PB` checks it.
- One size: the effect is per output point, and the perf XML gives a second scale (q7/q8: 100000 × 300).

## Cases
All in `bench/ts-from-grid-typed-write/ab/cases.tsv` (query and hash query per case, the same text on A and B).

| case | query | role | predicted A → B |
|---|---|---|---|
| T3 | `timeSeriesFromGrid(toDateTime64(100, 3), toDateTime64(3090, 3), 10, v) FROM fg_dense`, 1 thread | target | 23.7 → 9–19 ms on 2 vCPU (−20..−62%) |
| T5 | `timeSeriesRange(materialize(toDateTime64(100, 3)), toDateTime64(3090, 3), 10) FROM fg_dense`, 1 thread | target | 21 → 8.5–16 ms (−23..−60%) |
| XML q7 | `promql_set_operator_presence_mask.xml` q7: `timeSeriesFromGrid` over 100000 × 300 dense points | target | 304 → 119–243 ms on 2 vCPU (−20..−61%) |
| T4 | T3 on `fg_null` (50% random NULL), 1 thread | target, direction only | 25.9 → faster (−9..−29% bounds, see Evidence) |
| XML q8 | same file q8: every second value NULL | target, direction only | 220 → faster (upper estimate −34..−42%) |
| T1, T1x8 | `last_over_time(foo[10])`, 100..3090 step 10 (4000 × 300 points), 2 and 8 threads | measured | −1.2..−3.6% (emulated −3.1..−3.6%) |
| T2 | `last_over_time(foo[20])`, step 5 (4000 × 599 points), 2 threads | measured | −2..−5.8% (emulated −5.0..−5.8%) |
| M1, M2, XML q6 | the XML's `and` / `or` queries (q0, q1) and the appended q6 (= T1) | measured | M1/M2 −0.7..−2.1%; q6 as T1 |
| G1, XML q9 | `sum(last_over_time(foo[10]))`: one output series (appended to the XML as q9 after the preflight's round 1) | guard | unchanged |
| G2 | the XML's sparse `and` (q3) | guard | unchanged |
| XML q0–q5 | the six set-operator queries | guard | not slower |

T1, T2, M1, M2 and q6 are measured, not judged: 0.4× their emulated low end is under CI's 5% (emulation skill §7), so a run can't show them significant. They are reported, and must not be significantly slower.

## Method
- Servers: `CH=<A binary> DATA=<dir> SERVER_MEM=<…> QUERY_MEM=<…> bench/ts-from-grid-typed-write/server.sh start A 9001` and the same with `<B binary>`, `start B 9002` (its `config.xml.in` logs `query_log` and `processors_profile_log`).
- `cd bench/ts-from-grid-typed-write/ab && ./ab.sh gen 9001 && ./ab.sh gen 9002 && ./ab.sh layout 9001 9002 && ./ab.sh hash 9001 9002`; stop if it prints `layout differs` or `HASH MISMATCH`.
- One A/A pass first: A's binary on both ports, `N=100 OUT=out-aa ./ab.sh bench 9001 9002`; then the A/B pass `N=100 OUT=out-ab ./ab.sh bench 9001 9002` and `./ab.sh summary 9001 9002` (median and p95 wall, median CPU per case; `-i 200` splits at random between the servers).
- Perf XML: `tests/performance/scripts/perf.py --runs 13 --result-digest` from B's tree (q6–q9 exist only there) on `promql_set_operator_presence_mask.xml` against both servers, then `compare_perf_tsv.py` with its defaults, as for #23.
- CPU next to wall for every target (`summary`; `perf.py` gives both).

## Metrics
- Wall time (median, p95), CPU (`ProfileEvents['OSCPUVirtualTimeMicroseconds']`), `memory_usage` (`ab.sh summary`).
- The mechanism: `processors_profile_log` `elapsed_us` of `FilterTransform` for T1 on A and B (`bench/ts-from-grid-typed-write/promql.sh` prints it with `PORT=<p>`); it should drop by about the predicted saving while the other processors stay.
- Preconditions to check before timing: T1/M1/M2 return 4000 series and 1,200,000 points, T2 2,396,000 points, T3 and T5 2,400,000, T4 1,200,493 (of 2,400,000 positions), G1 1 series × 300; every inner table one part, `fg_*` 8000 rows each.

## Correctness
- `ab.sh hash 9001 9002` prints `hashes equal` (count and `sum(cityHash64(...))` per case; G1's values rounded to 6 digits, because it sums across threads). Checked as A/A on master on 2026-11-22: T1 `4000 15558791480632491934`, T3 `8000 16754876117826158314`, T4 `8000 12223210904932699867`.
- `05850_time_series_from_grid_typed_write` passes on B, and on A (its reference is master's output: the change keeps every result). Also run on B: `03254_timeseries_functions`, `03254_timeseries_functions_with_resets`, `03254_timeseries_range`, `04042_time_series_range_overflow`, `04628_timeseries_sum_avg_count_over_time`, `05242_time_series_range_max_elements`, and the integration test `test_prometheus_protocols/test_evaluation.py`.

## Pass / fail
- **Pass:** T3 and XML q7 at least −20%, T5 at least −23%, all significant at 99.5%; T4 and XML q8 significantly faster; no measured or guard case significantly slower (G1 and q9 above all: the aggregated shape must not get slower); every hash equal; the tests pass. A built run replaces these floors for any later plan (perf-lab §7, "floors after a built run").
- **Fail, a target faster but under its floor:** the `Field` route is cheaper on the build host than here (#23: 5–7 ns for two writes); report the measured size and judge the PR against the 5% bar on the function-level cases.
- **Fail, a guard or measured case slower:** the resize or the loop costs more than the `Field` path in some shape; send the `FilterTransform` elapsed of T1 and G1 from both servers, and I look at the generated code with clang 20.
- **Fail, hashes differ or a test fails:** a bug; stop, and I withdraw the branch.

## Checklist
- [ ] build A and B; record both shas
- [ ] generate the data; check layout and hashes
- [ ] A/A pass, then A/B pass; save `out-aa/`, `out-ab/` and the summaries under `bench/ts-from-grid-typed-write/ab/`
- [ ] fill the table of `prs/ts-from-grid-typed-write.md`, remove "pending build" and `[unbuilt]`
- [ ] update the report verdict and the backlog row
