# Skip the duplicate-series check when dropping the name of one metric (handoff #8, candidate branch)
**Verdict:** candidate branch pushed (unbuilt). It's a narrow take on the `__name__` drop cost that #123419 also removes: one file, one condition, no new state, setting or threshold. Emulated CPU is −19% for `sum(rate(x[5m]))` over 100k series and −30% for a 37-step range query, with identical results. Two open PRs overlap (#121069 and your #126763), so pick one of the three to land.
**Task:** 2026-10-31-handoff-skip-the-tags-read-for-label-fr, https://github.com/JTCunning/ClickHouse/issues/8 · **Build:** 26.11.1.974, master `a7f713612c8` (prebuilt) · **Branch base:** upstream `c189bd99fbda` · **Date:** 2026-11-01

## What happens (plain language)
- `rate()`, `abs()`, `x * 2` and similar functions remove the `__name__` label from their result. Two series that differed only by name (`a{job="x"}` and `b{job="x"}`) can then end up with the same labels.
- To catch that case, master re-aggregates every result series by its new label set and checks each group for collisions. With 100k series, that's a 100k-group hash aggregation plus a per-step check, on every such function.
- When the input selector pins one metric name (`http_requests_total`, or `{__name__="http_requests_total"}`), a collision is impossible. All series have the same name, so removing it keeps them distinct.
- The branch detects that case and replaces the aggregation with a plain column rewrite. Everything else keeps master's exact SQL: regexes, several names, label functions, aggregations, and vector-vector operators.

## Numbers
Dataset: 200,000 series, 40 samples each at a 15 s interval. `metric_0` has 100,000 series and `metric_1` has 2,041, with 6 tags per series. The table is a default TimeSeries table (v11, bucketed samples) with each inner table compacted.
Settings: local server, `max_threads=2`, query cache off. Each case ran in 3 interleaved rounds of 6 runs, with the first run of each round dropped; the figure is the median of the round medians. CPU is user plus system time from `query_log`. The "branch" column runs master's generated SQL with only the drop step rewritten, on the master binary. Running that generated SQL unmodified gives the same CPU as `prometheusQuery`, which validates the method. All three variants returned identical results.

| query | master CPU | branch CPU (emulated) | change | wall master → branch | peak memory | read_rows |
|---|---|---|---|---|---|---|
| `sum(rate(metric_0[5m]))`, instant | 548 ms | 447 ms | −19% | 571 → 472 ms | 172 → 172 MiB | 321,504 |
| `sum by (job, cluster) (rate(metric_0[5m]))` | 651 ms | 591 ms | −9% | 668 → 622 ms | 172 → 172 MiB | 321,504 |
| `sum(abs(metric_0))`, instant | 547 ms | 438 ms | −20% | 586 → 462 ms | 136 → 131 MiB | 321,504 |
| `sum(rate(metric_0[1m]))`, range, 37 steps | 1,217 ms | 851 ms | **−30%** | 1,261 → 894 ms | 342 → 326 MiB | 321,504 |
| `rate(metric_1[5m])`, 2,041 series returned | 32.5 ms | 29.2 ms | ≈ −10% (noise level) | 33 → 31 ms | 6 MiB | 13,806 |

- The saving grows with the number of steps, because the per-step duplicate check runs over every grid point of every series.
- `sum by` gains less, because the rewritten labels are still needed for its own grouping.
- These numbers include system time, so they run higher than the user-only CPU in the first-pass report (352 ms there for the first case). The ratio is consistent with that report: the drop step was ~30% of user CPU there and ~19% of user plus system CPU here.

## Why (cause)
- `dropMetricName` always generated `GROUP BY timeSeriesRemoveTag(group, '__name__')` with `anyForEach(values)` and `HAVING timeSeriesThrowDuplicateSeriesIf(arrayExists(c -> c > 1, countForEach(values)), new_group) = 0`. With a single name, every group has exactly one row, so the GROUP BY returns its input unchanged and the check never fires.
  - Code: `src/Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.cpp`, the `VECTOR_GRID` case.
- The branch adds `cannotGetDuplicateSeries()` to the same file. It applies when the function or operator node has exactly one vector argument, and that argument is a selector with an `=` matcher on `__name__`. The selector may be wrapped in `offset`/`@` or used as a subquery.
- This is safe because every caller of `dropMetricName` keeps its argument's labels apart from `__name__`. The one exception is `histogram_quantile`, which removes `le` and regroups, so its groups stay distinct. The precondition is written in `dropMetricName.h`.

## Proposed fix
Branch `lilred/perf-promql-drop-name-exact-selector`, unbuilt: https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred/perf-promql-drop-name-exact-selector
- About 90 lines in `dropMetricName.cpp/.h`.
- New stateless test `05482`: every one-name form gives the same output. Several-name selectors still merge series whose values fall at different steps, and still raise `CANNOT_EXECUTE_PROMQL_QUERY` when they collide at the same step. An `EXPLAIN` check fails if the shortcut isn't taken. Its last two lines (`-1`) are predicted; master gives `0`.
- New perf test `promql_drop_metric_name_single_metric.xml`, with a regex control.
- Upstream-ready description: `prs/promql-drop-name-exact-selector.md`. Effort: S.

**What it doesn't do:** the remaining piece of the original handoff, the id-only tags read for label-free aggregations (~19% CPU and ~27% memory in the first report), is still M-size and best built on #126221. This branch leaves it alone on purpose.

## Upstream overlap (pick one to land)
| | this branch | #121069 (Valery Petrov, open, updated 2026-11-01) | #126763 (yours, open) | #123419 (yours, open) |
|---|---|---|---|---|
| where | `dropMetricName` only | 3 range-function files + new header | 15 files, new `resolveMetricNameDrop` | wide (compliance) |
| covers | all `dropMetricName` callers with a one-name selector | range functions, `predict_linear`, `quantile_over_time` | same plus folding the drop into `sum`/`avg` GROUP BY, defers to consumer | moves the check to the final result |
| extra subquery | keeps a cheap projection step | fused into the range function's GROUP BY (no extra step) | folded | marker tag projection |
| state/format change | none | none | none | none |

- #121069 uses the same rule as this branch (`hasExactMetricNameMatcher`) and saves one more projection step, but only for range functions.
- This branch covers math, clamp, round, date, `timestamp` and scalar-operator callers too, and touches one place.
- #126763 is the most complete, but it's the expansive shape you said you don't like.
- If #121069 lands first, this branch still adds the non-range callers. If this branch lands, #121069 adds little more.

## How to reproduce
Scripts and raw results: `bench/drop-name-exact-selector/` on `lilred/research` (`bench.py`, `run1.jsonl`, emulated branch SQL in `sql_*_branch.sql`). Dataset SQL is in its README. Run `python3 bench.py` against a server on 127.0.0.1:19000. After a build:
```bash
clickhouse-test 05482_promql_drop_metric_name_single_metric
# perf: promql_drop_metric_name_single_metric.xml and promql_selector_pk_range.xml via the CI perf comparison
```
