# [unbuilt] Store last-sample rollup columns in `TimeSeries` samples tables and read them for wide-step selectors

Branch: [`lilred-perf-rollup-samples`](https://github.com/JTCunning/ClickHouse/tree/lilred-perf-rollup-samples) @ `6a1d8e48fae5`: James's `jtcunning/timeseries-rollup-samples` @ `daa53278310f` (6 commits, base `57ca89d382d2`) plus 12 commits. Merges cleanly into master `e8473460558a`. Compare: https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-rollup-samples

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above without [unbuilt]) -->
### Changelog category (leave one):
- Experimental Feature

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
`TimeSeries` tables can keep rollup columns (setting `samples_rollup_steps`, for example `'5m, 1h'`) with the last sample of every interval of each step; new `TimeSeries` tables get schema version 12. PromQL range queries read a rollup column for instant selectors when a rollup step divides the query step and the start time is a multiple of the step, with the same result as the raw samples: at a 1h step over 15-second scrapes they read 1 sample per series per hour instead of 240.

### Description

A PromQL range query with a wide step reads every raw sample of its range. An instant selector uses only the last sample at or before each evaluation time, so at a 1h step over 15-second scrapes it reads 240 samples per series per hour and uses one.

Now the engine setting `samples_rollup_steps` (schema version 12) adds one column `samples_<step>` per step to the samples table, of type `SimpleAggregateFunction(timeSeriesGroupArray(<step>), ...)`. The insert writes it from the same sorted samples, and merges collapse it with `timeSeriesGroupArray(<step>)`, which keeps the last sample of every interval `(k * step, (k + 1) * step]`. `timeSeriesSelector` takes an optional `step` and reads the column of the largest rollup step dividing it. PromQL passes the step only for instant selectors whose evaluation times are multiples of the step (the start time is, after offsets and subqueries). There the last sample at or before every evaluation time is the same as in the raw samples, so the result is identical. An instant selector in a subquery is evaluated at multiples of the subquery step and reads a rollup too. Range selectors (`rate`, `increase`, `*_over_time`) use every sample of their window and read the raw samples, as do instant selectors of instant queries and `time_series_prefer_rollup_tables = 0`. A rollup column is preferred to the recent samples table. `CREATE TABLE ... AS` regenerates the rollup columns for the steps of the copy. Tables of version 11 or earlier, and tables without the setting, are unchanged.

Release build, base `<sha>`, `<host>`, interleaved runs; branch columns pending build.

| query, 2000 series × 1 day | master | branch, raw | branch, rollup |
|---|---|---|---|
| `sum(m)`, step 1h | pending | pending | pending |
| `sum(m)`, step 5m | pending | pending | pending |
| insert of the day, `'5m, 1h'` | pending | pending | pending |
| control `sum(rate(m[5m]))`, step 1h | pending | pending | pending |

Each rollup column costs a fraction of the raw samples on disk with the default codecs over 15-second scrapes: about 25% at 1m, 5% at 5m, 0.5% at 1h.

Correctness: the results of 18 PromQL forms are identical with and without rollups in `05621` (instant selectors, `offset`, `@`, `timestamp()`, subqueries, an instant query, aggregations, and range functions), and every row of a rollup column equals `timeSeriesGroupArray(<step>)` of its raw samples for `DateTime`, `DateTime64(3|6|9)`, timestamps before 1970 and the end of the `DateTime64(9)` range. Tests: `05508`, `05621`, `gtest_normalize_time_series_definition`, and `tests/performance/promql_range_rollup_samples.xml` (no existing performance test has a table with rollup columns). Closing the intervals on the left, as `floor(timestamp / step)` does, turns the `05621` line of the sample at 3600 red (1 instead of 2); passing the step for range selectors turns the flags of `rate`, `max_over_time`, `count_over_time` and `last_over_time` red; choosing the largest step below the query step turns the 90s line red.
<!-- upstream:end -->

## Below the line (not for upstream)

### Evidence so far (no build): an emulation on a master binary

Rollup columns computed in SQL from hourly rows, grid functions as the converter calls them (`timeSeriesLastToGrid` with the 5m lookback for a selector, `timeSeriesRateToGrid` for `rate`, …). 200 series × 2 days, two timestamp shapes (exact 15 s grid; per-series offset plus 40 ms jitter) × counters and gauges. Script: `bench/rollup-samples/emulate/emulate.sh`.

| case | rollup | points that differ from raw |
|---|---|---|
| selector, steps 1m / 5m / 1h | right-closed (this branch) | 0 of 564k / 113k / 9.4k, all four shapes |
| selector, steps 1m / 5m / 1h | left-closed (`daa5327`) | every point on the exact 15 s grid; 0.02% with jitter |
| `rate(x[5m])`, step 5m | 5m | 92% of points empty on the exact grid, 100% with jitter |
| `rate(x[5m])`, step 5m | 1m | all differ, counters p50 2.5%, p99 15% |
| `increase(x[1h])`, step 1h | 1h | ~99% off (exact grid) or empty (jitter) |
| `max_over_time(x[1h])`, step 1h | 5m | gauges: 94% differ, p99 −66% (spikes lost) |
| `avg_over_time(x[1h])`, step 1h | 5m | gauges p50 1.2–2.6%, p99 21% |

Speed and storage, 1000 series × 7 days, jittered gauge, codecs of the samples table, 2 vCPU, `max_threads=2`, median of 5 (startup 0.06 s subtracted): selector grid at 1h 0.87 → 0.03 s (~29×), 5m 1.02 → 0.20 s (~5×), 1m 1.26 → 0.63 s (~2×). Columns: `samples_60` 25.2%, `samples_300` 5.3%, `samples_3600` 0.45% of the raw `samples` bytes. Script: `bench/rollup-samples/emulate/time.sh`.

### A/B

Plan: `../ab-plans/rollup-samples.md`; harness `bench/rollup-samples/ab/run.sh <branch binary> <master binary> <work dir>`.

### Fill in after the build
- the setup line (`<sha>`, `<host>`), the table, the correctness count from the harness's `equal` column.

<details><summary>Self-review (preflight, 3 rounds)</summary>

- **R1 (⚠️ Request changes, 4 Majors):** `CREATE TABLE ... AS` of a rollup table broke (dropped steps kept their codecs, a version-11 copy inherited the setting; the branch's own gtest threw) → fixed (`2d68a4d15453`); the sink's bucket end overflowed in the last hour of `DateTime64(9)` and looped → fixed with `makeSamplesBlock`'s membership test and overflow-free keys (`83fedeeff999`); the perf XML needs `do_not_check_in_pr` (needs the PR number: added at submission); `LIKE '%samples_60%'` matched random inner-table UUIDs (~1% flake) → `position()` (`957232272cf4`). Nits fixed: stale comments, old-design leftovers, `1500ms` truncated (`9ac54a871a3a`), the generated `.mdx` regions reverted to the base (`007aec550481`), changelog entry.
- **R2 (⚠️, 1 Major):** the new `samples_<n>` name check rejected valid definitions of version 11 or earlier → gated on version 12 (`f6829063dc7c`) with a gtest.
- **R3:** no code findings. Nit taken: the error message says how to keep such a column (`6a1d8e48fae5`). Accepted interaction: `CREATE ... AS` from a pre-12 table with an extra `samples_<n>` column now gets version 12 and is rejected with that message.
- **Open, needs the build:** 05508, 05621, `gtest_normalize_time_series_definition`, the perf XML; the A/B table (incl. the edge run with 60 s scrapes and a 1m rollup, where the rollup is preferred to the recent samples table without being smaller).
- Confirmed by the reviewer: every consumer of an instant selector uses only the last sample at or before `start + k·step` (offset, `@`, subqueries, `timestamp()`, fused aggregations); remote read and the HTTP API read raw.

</details>
