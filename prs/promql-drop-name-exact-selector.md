# [unbuilt] PromQL: skip the duplicate series check when dropping the metric name of one metric

Branch: `lilred-perf-promql-drop-name-exact-selector` · compare: https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-promql-drop-name-exact-selector

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above without [unbuilt]) -->

Functions and operators such as `rate`, `abs`, `clamp`, `x * 2` and `histogram_quantile` drop the metric name `__name__`. `dropMetricName` did that with a `GROUP BY timeSeriesRemoveTag(group, '__name__')` over every series, with `anyForEach(values)` and the per-step duplicate check `timeSeriesThrowDuplicateSeriesIf(...)`, because two series that differ only by name get the same tags after the drop. When the only vector argument of the function or operator is a selector with an equality matcher on `__name__` (`m`, `{__name__="m"}`, possibly with `offset`, `@` or as a subquery `m[5m:1m]`), all its series have the same name, so dropping it can't create duplicates. In that case `dropMetricName` now emits a plain projection: no GROUP BY, no `anyForEach`, no check. Other arguments (several names, regular expressions, label functions, aggregations, binary operators over two vectors) keep the existing query, so their SQL is unchanged.

The change is in `dropMetricName.cpp` (plus its precondition in `dropMetricName.h`), so it applies to every caller of `dropMetricName` whose argument is a one-name selector: range functions, `predict_linear`, `quantile_over_time`, math, clamp, round, date functions, `timestamp`, unary operators, and binary operators with a scalar. `histogram_quantile` takes the shortcut only for a bare `histogram_quantile(phi, x_bucket)`; its usual argument `sum by (le) (rate(...))` is an aggregation, so it keeps the check. The drop for `rate(...)` inside that argument does take the shortcut.

### Evidence
| case | host | base sha | workload | master CPU (median of 3×5) | branch CPU (median of 3×5) |
|---|---|---|---|---|---|
| `sum(rate(metric_0[5m]))`, instant | 2 vCPU VM, `max_threads=2` | `a7f713612c8` | 100k series | 548 ms | pending build (emulated: 447 ms, −19%) |
| `sum by (job, cluster) (rate(metric_0[5m]))`, instant | same | same | 100k series | 651 ms | pending build (emulated: 591 ms, −9%) |
| `sum(abs(metric_0))`, instant | same | same | 100k series | 547 ms | pending build (emulated: 438 ms, −20%) |
| `sum(rate(metric_0[1m]))`, range, 37 steps | same | same | 100k series | 1,217 ms | pending build (emulated: 851 ms, −30%; peak memory 341 → 326 MiB) |
| `rate(metric_1[5m])`, instant, 2,041 series returned | same | same | 2k series | 32.5 ms | pending build (emulated: 29.2 ms) |

Dataset: 200,000 series in one TimeSeries table (default settings, bucketed samples), 40 samples each at a 15 s interval; `metric_0` has 100,000 series and `metric_1` 2,041, with 6 tags per series. CPU is `UserTimeMicroseconds + SystemTimeMicroseconds` from `system.query_log`; 3 interleaved rounds of 6 runs, the first run of each round dropped. Rows read are the same in every case (321,504 for `metric_0`).

The "emulated" column runs the SQL that master generates for each query (taken from the `Will execute query` log line) with the drop step rewritten to the projection this change generates, on the master binary. The unmodified generated SQL gives the same CPU as `prometheusQuery` (559 vs 548 ms for the first case), and all three variants return identical results in every case. Branch column pending build; commands:

```bash
python3 bench.py   # in research bench/drop-name-exact-selector, against a local server on 127.0.0.1:19000 with the table `ts`
clickhouse-test 05482_promql_drop_metric_name_single_metric
tests/performance/scripts/compare.sh  # or the CI perf comparison for promql_drop_metric_name_single_metric.xml and promql_selector_pk_range.xml
```

### Tests
- `05482_promql_drop_metric_name_single_metric`: every form of a one-name argument (`m`, `{__name__="m"}`, `offset`, `@`, a subquery, `abs`, `clamp_max`, unary minus, `* 2`, `timestamp`, `histogram_quantile`, instant `sum(rate(...))`) returns the same tags and values as before. Selectors over several names still merge series that get the same tags at different steps, and still raise `CANNOT_EXECUTE_PROMQL_QUERY` when such series have values at the same step. The last two lines compare the number of aggregation steps in `EXPLAIN` against the same selector written as a regular expression: one fewer with this change (`-1`; master gives `0`), so the test fails if the shortcut is not taken.
- New perf test `promql_drop_metric_name_single_metric.xml`: `sum(rate(...))`, `sum by (job) (rate(...))`, a range query, `sum(abs(...))` over 100k series, and the same selector as a regular expression as a control. `promql_selector_pk_range.xml` (`sum(rate(pkr_metric[30m]))`) also covers the path.

Related: https://github.com/ClickHouse/ClickHouse/pull/121069
Related: https://github.com/ClickHouse/ClickHouse/pull/126763
Related: https://github.com/ClickHouse/ClickHouse/pull/123419

### Changelog category (leave one):
- Performance Improvement

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
PromQL functions and operators applied to a selector of one metric (for example `sum(rate(http_requests_total[5m]))` or `abs(m)`) no longer regroup all series to drop the metric name, which reduces the CPU time of such queries over many series.

<!-- upstream:end -->

<details>
<summary>Self-review (preflight) and notes for James</summary>

- Round 1 (independent run of `.claude/skills/review/SKILL.md`): ⚠️ Request changes. One Major: the shortcut's caller precondition was undocumented, so it's now written in `dropMetricName.h`. Two test asks: the `@` modifier (added) and the build-only items. Nits: histogram_quantile scope (clarified) and the duplicated rule in `pushDownLabelMatchers.cpp` (kept separate; cross-reference comment added).
- Round 2: ⚠️ Request changes **only** for build-dependent evidence: compile plus clang-tidy, the two predicted `-1` lines of 05482, and measured branch numbers. The reviewer would approve once those come back as expected.
- Overlap: #121069 (same rule, range functions only, fused into the range GROUP BY) and #126763 (James, wider). Land one. See the report `reports/2026-11-01-drop-name-exact-selector.md`.
- Base: upstream `c189bd99fbda`. Commits `99c452329b87`, `dadc843ffb74`.

</details>
