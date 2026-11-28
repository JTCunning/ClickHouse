# `timeSeriesFromGrid` writes every output timestamp through a `Field`
**Verdict:** worth a small branch: the function itself gets about 2× faster on dense rows (emulated −50..−62%), but a PromQL range query that returns many series gains only about 3% (emulated −3.1..−3.6%; −5.0..−5.8% at a step of half the scrape interval), so the PR's evidence is the function-level perf queries.

**Update 2026-11-28: parked on James's answer (option A on #26, 2026-11-28: "park it."); don't submit it.** The 2026-11-27 update below has his machine, method and per-case numbers; they stand, and no rerun was needed for a park. The cause is understood, not a defect: the change saves 3.0–3.6 ns of the roughly 60 ns an output point costs through the JSON API, so dashboard queries can't show it (measured −2.0..+0.3%, inside the plan's 0..−3% prediction; run 1's function-level T3 −65% against −50..−62% emulated). Withdrawn: the verdict line's "worth a small branch" for normal use. The branch stays at `92789976f58`; on 2026-11-28 it still merges cleanly into master `9c5336e66e25`, upstream hasn't touched `timeSeriesRange.cpp` since `05f159a69597`, no open upstream PR names either function, and test number 05850 is still free. A revival needs a workload where these functions' own output is a large share of the query (for example SQL that reads `timeSeriesFromGrid` rows directly, not through the PromQL HTTP API); then run 1's kit is its evidence and run 2's kit its guard. The two pre-existing master defects in "Found on the way" (NULL start/end/step computed as 0, the 32-bit wrap after 2106) don't depend on this branch; their issue draft stays in #26's description.

**Update 2026-11-27: run 2 found no measurable change in normal PromQL use; park or drop asked on #26.** James's run 2 (2026-11-25; AMD EPYC 9R14, servers and clients pinned to 16 cores; A `42f59f8a4c25` = master, B `62e531a2ff81` = the branch merged onto A) timed six dashboard queries through `/api/v1/query_range` (JSON, 2000 series scraped every 15 s, 100 rounds each, an A/A pass first): −2.0..+0.3%, none significant at p < 0.005. The largest, `rate(m[5m])` over 6 h at a 30 s step (2000 series, 1.44M points), went 168.7 → 166.8 ms (−2.0%, p 0.29); the A/A pass stayed within 0.9%; every result was equal and every test passed. Run 1's function-level gains stand (`timeSeriesFromGrid` 2×, `timeSeriesRange` 5×), but they save 3.0–3.6 ns of the roughly 60 ns an output point costs through the JSON API (#22 run 1), so a user's query can't see them. The A/B plan predicted 0..−3% on these queries; the measured changes sit inside that range and under the noise.
**Task:** weekend batch 2026-11-22 (James's chat "one more weekend perf pass"), target 2, from the read-only scout · **Build:** 26.12.1.112, `521c4bcb7b94` · **Date:** 2026-11-22
**A/B plan:** ab-plans/ts-from-grid-typed-write.md · **Branch:** `lilred-perf-ts-from-grid-typed-write` @ `a25c640b65b3` (`[unbuilt]`) · **PR text:** prs/ts-from-grid-typed-write.md

## What happens (plain language)
The last step of a PromQL range query that returns an instant vector turns each series' grid of values into (timestamp, value) pairs with `timeSeriesFromGrid`. Master appends every timestamp through ClickHouse's generic value container (`Field`): one build, type check and destroy per output point. `timeSeriesRange` (scalars, `time()`) does the same. The branch writes the timestamps straight into the result column, as upstream PR 133502 did for `timeSeriesGroupArray`.

## Numbers
Emulated on master, 2 vCPU, `max_threads = 1` unless stated, medians of 10 runs after a warm-up (two passes; `bench/ts-from-grid-typed-write/`). Data: `fg_dense` / `fg_null` (`Memory`, 8000 rows × 300 `Nullable(Float64)`, 0% / 50% NULL), and the `TimeSeries` table of `promql_set_operator_presence_mask.xml` (4000 + 4000 dense series × 300 samples, 10 s apart).

| query | median (ms) | min–max | notes |
|---|---|---|---|
| A: `timeSeriesFromGrid(...)` over `fg_dense` (2.4M points) | 23.7–23.9 | 22.2–28.7 | A/A same text: 23.6–23.9 (−0.5..0%) |
| F0: `range(...)` (typed, 300 per row) + `v` passed through | 8.8–9.0 | 8.1–10.1 | skips the value copy: overstates the saving |
| F1: `range(...)` + `arrayFilter(x -> x IS NOT NULL, v)` (typed copy) | 10.3–10.8 | 9.4–12.7 | does what the branch still does on dense rows |
| R: `timeSeriesRange(...)` (Field route, 300 per row) | 20.6–21.7 | 14.0–24.7 | |
| G: `range(...)` (typed, 300 per row) | 8.6 | 7.7–10.3 | |
| A over `fg_null` (1.2M points kept of 2.4M) | 25.8–26.0 | 25.1–26.9 | A/A 25.8–25.9 |
| PromQL `last_over_time(foo[10])`, 4000 × 300 = 1.2M points, 1 / 2 threads | 212.7 / 205.8 | 194–231 | final `FilterTransform` (computes `timeSeriesFromGrid`) 14.7 / 15.2 ms |
| same, `[20]` at step 5 (2.4M points) | 256.7 | 244–286 | `FilterTransform` 26.7 ms |
| `and` / `or` of the XML (1.2M points) | 365 / 370 | 334–383 | `FilterTransform` 18.5 / 23.6 ms (several filters) |
| `sum(last_over_time(foo[10]))` (300 points) | 147.6 | 137–173 | `FilterTransform` 0.1 ms |

Saving per point: R − G = 5.0–5.4 ns, A − F1 = 5.4–5.7 ns, A − F0 = 6.1–6.2 ns (upper). Projected: `fg_dense` −50..−62%, `timeSeriesRange` −58..−60%, PromQL `last_over_time` −3.1..−3.6% wall (−3.4..−3.9% CPU), at step 5 −5.0..−5.8%, `and`/`or` −1.8..−2.1%, the aggregated query 0. By the emulation skill's calibration (0.4× to 1×) the query-level gain is 1–4%: under CI's 5% bar, so those cases are measured in the A/B, not judged. Rows with NULL values: no SQL control matches the branch's remaining work (−9..−29% bounds; direction only). Kit dry run (A/A on master, `smoke-2026-11-22.txt`): ran end to end, hashes equal on 10 cases, no case significant.

## Why (cause)
`src/Functions/TimeSeries/timeSeriesRange.cpp` `doExecute` (lines 388–423 at `521c4bcb7b94`, unchanged on master `e4f82e0c28b`): `res_timestamps->insert(timestamp)` for every point (397 in rows without NULLs, 417 in rows with them, plus a virtual `insertFrom` per value at 419). The function is called from `finalizeSQL.cpp` (the VECTOR_GRID result: series × steps points per query) and `getToGridAggregateFunctionArguments.cpp:57` (subqueries fed to a `*ToGrid` function); `timeSeriesRange` from `applyFunctionTimestamp.cpp`, `fromFunctionTime.cpp` and the scalar case of `getToGridAggregateFunctionArguments.cpp`.

## Proposed fix
Take the typed timestamps container once; a row without NULL values resizes it by its step count (the new size is the block's point count, already checked for overflow and against `function_range_max_elements_in_block` before) and fills it; a row with NULL values appends the timestamps of its kept values. Values, types, errors unchanged. Effort S; test `05850_time_series_from_grid_typed_write` (reference = master's output), four queries appended to `promql_set_operator_presence_mask.xml` (q6 the plain range query, q7/q8 the function alone, q9 the aggregated control). A harness of the new loop shape (clang 20, release flags; plain arrays, not ClickHouse's `PODArray`) vectorizes the dense fill (4 × Int64, 8 × UInt32). Not done: a typed value copy for rows with NULL values (the values' type is any array element type, so it would need a type dispatch).

## Found on the way (pre-existing, not fixed here)
A NULL start, end or step of either function is computed as 0, because the result is an `Array` that can't be `Nullable`: a NULL start gives one row from 1970 (58,291,203 timestamps at a 30 s step; 1.7e12 elements and code 69 at 1 ms), a NULL end or step throws "End timestamp is less than start timestamp" / "Step should be greater than zero"; `range` rejects NULL with a clear error. Only direct SQL reaches it, not PromQL. Preflight round 2 asks the PR to link a tracking issue; drafted for James to open, fix in a separate PR.

## Upstream overlap
Open PRs searched for `timeSeriesFromGrid` / `timeSeriesRange` / the file on 2026-11-22: none touch `src/Functions/TimeSeries/`. The last commits to the file are `[[nodiscard]]` additions (2026-10-17). Upstream PR 133502 (James, open) is the same change for `timeSeriesGroupArray`; no branch in the fork covers this function.

## How to reproduce
`bench/ts-from-grid-typed-write/`: `server.sh start lab 19101`, `clickhouse client --port 19101 --multiquery < gen.sql`, `ROUNDS=11 ./emulate.sh`, `ROUNDS=8 MAX_THREADS=1 ./promql.sh` (and `MAX_THREADS=2`); the A/B kit is `ab/ab.sh` (see the plan).
