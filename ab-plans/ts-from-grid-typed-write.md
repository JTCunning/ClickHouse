# A/B plan: write the timestamps of `timeSeriesFromGrid` and `timeSeriesRange` straight into their typed column
**Experiment:** ../reports/2026-11-22-ts-from-grid-typed-write.md · **Branch:** `lilred-perf-ts-from-grid-typed-write` @ `a25c640b65b3` (base master `521c4bcb7b94`) · **Task:** weekend batch 2026-11-22, target 2 · **Outbox:** `ts-from-grid-typed-write` (`ready-to-build`)
**Claim:** `timeSeriesFromGrid` alone gets 20–62% faster on rows without NULL values (8000 × 300 points: −50..−62% emulated) and `timeSeriesRange` 23–60%; rows with NULL values get faster too (size unproven); a PromQL range query that returns 4000 series × 300 steps gets about 1–4% faster (emulated −3.1..−3.6%), 2–6% at a step of half the scrape interval; an aggregated query (one output series) is unchanged; results identical.
**Run 1 (James, 2026-11-23, A `521c4bcb7b94`, B `a25c640b65b3`): pass.** T3 −65.0%, T5 −80.7%, T4 −20.6%, XML q7 −51.1%, q8 −31.2%; no measured or guard case slower; hashes equal; tests pass (results on #26). The saving is 3.0–3.6 ns per point there. The branch head is now `92789976f58`: the timestamp expression is written inline again instead of in a lambda (same release code), so a tests-only run is enough; this plan applies unchanged if the A/B is run again.

**Run 2 (James, 2026-11-25, A `42f59f8a4c25`, B `62e531a2ff81`): pass on `pass_if`, no measurable change.** R1–R6 −2.0..+0.3%, none significant at p < 0.005 (100 rounds); A/A within 0.9%; every row equal; all tests pass (results on #26). As this plan said, the next step is to park or drop the PR, not to look for a shape where it shows: asked on #26 on 2026-11-27. No run 3 planned.

**Evidence so far:**
- Emulated by subtraction on master `521c4bcb7b94`, 2 vCPU, `max_threads = 1`, two passes of 10 runs after a warm-up (`bench/ts-from-grid-typed-write/emulate-2026-11-22-pass*.txt`). The change is inside a function, so no SQL emulation of B exists. The timestamp write through `Field` costs 5.0–6.2 ns more per point than a typed write: `timeSeriesRange` − `range` of the same 300 points (5.0–5.4 ns, Field route against a typed loop, no values) and `timeSeriesFromGrid` − (`range` + `arrayFilter` copy of the values) (5.4–5.7 ns) up to − `range` alone (6.1–6.2 ns: that control skips the value copy, so it overstates). `timeSeriesFromGrid` over 8000 × 300 dense values: 23.6–23.9 ms on master; A/A (same text, same round) −0.5..0%.
- Rows with NULL values: no SQL control does what the branch still does there (a branch per position and a virtual `insertFrom` per kept value). The per-point delta above, applied to the 1.2M kept points, gives −23..−29% (an upper estimate); `arrayFilter` controls give −9..−10% but write and filter all 300 timestamps. So T4 is judged on direction only.
- Query level (`bench/ts-from-grid-typed-write/promql-2026-11-22.txt`, 7 runs after a warm-up): `last_over_time(foo[10])` on the table of `promql_set_operator_presence_mask.xml`, 100..3090 step 10 = 1.2M output points: 212.7 ms wall at 1 thread, 205.8 ms at 2; the final `FilterTransform` (the `WHERE notEmpty(samples)` step that computes `timeSeriesFromGrid`) 14.7–15.2 ms; saving 1.2M × 5.0–6.2 ns = 6–7.5 ms = −3.1..−3.6% wall (−3.4..−3.9% CPU). The `and`/`or` queries spend the same 6–7.5 ms in 350–370 ms (−1.8..−2.1%). At step 5 with `[20]` (2.4M points): 256.7 ms, −5.0..−5.8%. `sum(...)`: 300 points, 0. At `max_threads = 16` the final filter runs × 8 (`EXPLAIN PIPELINE`), so its share should not grow with cores.
- 2026-11-22 dry run on master `521c4bcb7b94` (`bench/ts-from-grid-typed-write/smoke-2026-11-22.txt`): kit ran end to end as A/A (ports 19001/19002, N=20); preconditions as below; hashes identical on all 10 cases, none empty; no case differs at 99.5%, A/A medians up to 7.1% apart on wall (T1) and 9.6% on CPU (T5) at 16–24 runs per server. B~ can't be run, so question 4 (direction) rests on the emulation. Edited after the dry run, not re-run: the role column of `cases.tsv` (T1, T1x8, T2 → `measured`, T4 → `target-direction`; printed only).
- Unproven: the size on the build host (#23 measured 5–7 ns per sample for two `Field` writes there, so one may cost 2.5–3.5 ns); the NULL-row path; whether the fill loop vectorizes (not needed for the claim).

## Run 2: dashboard-shaped queries (James, 2026-11-23: "go with B")

**Why.** Run 1 judged the function alone (T3, T5, q7: −51..−81%). Its PromQL queries were the best case for this change: one sample per series per window (a sample every 10 s, step 10 s, `[10]`) and `FORMAT Null`, so building the output points was as large a share as it can be, and still they moved only −0.6..−2.4% (q6, not significant). Run 2 measures what a user sees: dashboard queries through `/api/v1/query_range` with JSON output, on data scraped every 15 s, so a `[5m]` window holds 20 samples. Its table is the headline of the PR; run 1's function-level rows stay as mechanism evidence only.

**Builds.** A = master `521c4bcb7b94` (as run 1), B = the branch head `92789976f58` (run 1's commit plus the inline timestamp expression). Release, no sanitizer, same host and pinning as run 1. Don't run `ab/ab.sh` or `perf.py` again: run 1's numbers stand.

**Data.** `bench/range-json-timestamps/gen.sh` (#22's kit), `SERIES=2000 HOURS=6`: a default `TimeSeries` table `ts` with 2,000 series of the counter `m` and 2,000 of the gauge `g` (labels `job` with 10 values, `instance`, `pod`), one sample every 15 s over 6 h, plus #22's `c` and `h` series (not queried here). Both servers get the same `T_END`; `gen.sh` writes `window.txt` in the working directory.

**Cases** (`bench/ts-from-grid-typed-write/realistic/run.py`; each window ends at `T_END`):

| Label | Query (`/api/v1/query_range`) | Output | Predicted change |
|---|---|---|---|
| R1 | `rate(m{job="job3"}[5m])`, 1 h, step 15 s | 200 series × 241 points | 0..−1% |
| R2 | `rate(m[5m])`, 1 h, step 15 s | 2,000 × 241 | −0.5..−2% |
| R3 | `sum by (job) (rate(m[5m]))`, 1 h, step 15 s | 10 × 241 | 0 (control) |
| R4 | `g{job="job3"}`, 1 h, step 15 s | 200 × 241 | 0..−1.5% |
| R5 | `g`, 1 h, step 15 s | 2,000 × 241 | −1..−3% |
| R6 | `rate(m[5m])`, 6 h, step 30 s | 2,000 × 721 | −0.5..−2% |

Prediction basis: run 1 saved 3.0–3.6 ns per output point of CPU; spread over the query's parallel streams (q6: 4.2 ms of CPU at one thread became about 1.5 ms of wall at 16 cores) and set against about 60 ns per output point for a plain selector through the JSON API on that host (#22 run 1: `m`, 2.88M points in 172–192 ms), a plain gauge panel (R5) can gain up to about 3%; `rate` does more work per point, and the HTTP round trip and planning dilute the small panels (R1, R4). **If no case shows a difference, the honest result is "no measurable change in normal PromQL use"**, and the next step is to talk about parking or dropping the PR (James, 2026-11-23 chat), not to look for a shape where it shows.

**Method.**
1. Start the servers with #22's script: `CH=<A binary> DATA=<dir> SERVER_MEM=48000000000 QUERY_MEM=24000000000 bench/range-json-timestamps/server.sh start a 9001`, and the same for `b` on 9002 with B's binary (the Prometheus API listens on the TCP port + 2000).
2. In one working directory: `T=$(( $(date +%s) / 60 * 60 ))`, then `T_END=$T SERIES=2000 bench/range-json-timestamps/gen.sh 9001 > gen-9001.log` and the same for 9002. `diff gen-9001.log gen-9002.log` prints nothing (same parts and rows on both).
3. A/A first, on fresh servers that both run A's binary: `PORTS="9001 9002" ROUNDS=100 bench/ts-from-grid-typed-write/realistic/run.py > aa.jsonl 2> aa.err` and `PORTS="9001 9002" bench/ts-from-grid-typed-write/realistic/summarize.py aa.jsonl`: every row says `no difference` and `equal`.
4. A/B on fresh servers (A on 9001, B on 9002), the same commands with `ab.jsonl`. Post the table exactly as `summarize.py` prints it: the `Query` column holds each query, `Base` is A's median, `This PR` is B's. `Change` is the median of the per-round B/A ratios (the two requests of a round run next to each other); `p` is a paired Wilcoxon signed-rank test, and a difference counts at p < 0.005.
5. Tests: `05850_time_series_from_grid_typed_write` on A and B; the `also_run` tests and `test_prometheus_protocols/test_evaluation.py` on B.

**Pass.** The tests pass; every row is `equal` (R3 may be `equal (rounded)`: `sum` over threads); no row is significantly slower; the A/A pass shows no difference. The realistic rows have no floor: they decide how the PR is framed, and whether it is worth submitting.

**Dry run 2026-11-23** (this host, 2 vCPU, master `521c4bcb7b94` on both servers, `SERIES=500`, 12 rounds; `realistic/smoke-2026-11-23.jsonl`): the kit ran end to end; the two layouts were equal; every row `no difference` (p 0.08–0.88) and `equal`; no retries. At `SERIES=1000` a 1.12 GiB server memory cap failed R6 with `MEMORY_LIMIT_EXCEEDED`, so `run.py` now prints the error body and counts retries (`summarize.py` reports them); the build host's 48 GB cap is far from that.

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
