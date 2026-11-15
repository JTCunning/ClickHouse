# `quantile` across series drops NaN samples (3 evals)

**Status:** in PR #122914. Analysed 2026-11-15 at master `6ea176fa3035`, upstream Prometheus `3da2514eff5c`. One cause, already fixed by an open PR; nothing new to start.

## Failing evals

The group `{test="NaN sample"}` holds three series with the values 0, 1 and NaN at the evaluation time (`aggregators.test`, load block before line 504).

| file:line | query | Prometheus | ClickHouse |
|---|---|---|---|
| aggregators.test:513 | `quantile without(point)(0.2, data)` | `NaN` | `0.19999999999999996` |
| aggregators.test:532 | `quantile without(point)(scalar(foo), data)` (phi 0.8 at 1m) | `0.6` | `0.8` |
| aggregators.test:538 | `quantile without(point)((scalar(foo)), data)` | `0.6` | `0.8` |

Lines 504 and 520 run the same query with phi 0.8. They carry `expect` annotation lines, so the loader leaves them out (`annotation_excluded`, part of the 248 excluded assertion evals). The other groups in these evals pass. Reproduced with `research/bench/compliance/run_extended.py --file aggregators.test` on the master binary (26.11.1.2253).

## What Prometheus does

`quantile(q, values)` (`promql/quantile.go:725`) sorts all the samples of the group, NaN included. NaN sorts before every number (`vectorByValueHeap.Less`, `promql/functions.go:2792`: a NaN on the left is always "less"). Then:

- rank = q × (n − 1), counting the NaN samples in n;
- lower = floor(rank), upper = min(n − 1, lower + 1), weight = rank − lower;
- result = values[lower] × (1 − weight) + values[upper] × weight.

For {NaN, 0, 1}: at q = 0.8 the rank is 1.6, between 0 and 1, so the result is 0.6. At q = 0.2 the rank is 0.4, between NaN and 0, so the result is NaN.

## What ClickHouse does

The PromQL `quantile` operator becomes `quantileExactInclusiveForEach(phi)(values)` (`src/Storages/TimeSeries/PrometheusQueryToSQL/applyAggregationOperatorQuantile.cpp`, all three phi branches). `quantileExactInclusive` uses the same R-7 rank, but its state drops NaN on insert (`src/AggregateFunctions/QuantileExact.h:49`: "We must skip NaNs as they are not compatible with comparison sorting."). So the group is {0, 1}, n is 2, and q = 0.8 gives 0.8, q = 0.2 gives 0.2.

**Root cause:** the aggregate function that computes the cross-series quantile ignores NaN rows, while PromQL counts them as the lowest values of the group. A NaN sample changes both the rank and the values it lands on.

`quantile_over_time` does not have this problem. It runs on `timeSeriesQuantileToGrid`, which keeps NaN samples and sorts them first, like Prometheus (its documentation in `src/AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesQuantileToGrid.cpp:176`).

## Fix

**Overlap:** [#122914](https://github.com/ClickHouse/ClickHouse/pull/122914) "Keep NaN samples in the PromQL quantile aggregation" (open since 2026-09-29, last commit 2026-11-11, labels `pr-bugfix`, `comp-promql`). It collects the values of each step with `groupArray`, sorts them with NaN first, and applies Prometheus' formula `lower × (1 − weight) + upper × weight`. That also fixes the ±Inf cases where `a + w × (b − a)` gives NaN (`quantile(0.5, {-Inf, 0})` is -Inf in Prometheus). The PR's own compliance comment shows extended support 682 → 685 passed, 16 → 13 failed: these three evals. It adds `05203_promql_quantile_nan` and a cap test for the `groupArray` size limit, and reports the sorting cost (0.36 s against 0.21 s for one 10,000-series group over 1,441 steps; no change with 100 groups).

A shortcut that keeps `quantileExactInclusive` (replace NaN with -Inf before the aggregate) does not work. It merges NaN with real -Inf samples, and Prometheus sorts those apart. An exact match needs the sort.

**How to prove it:** the three evals pass in the extended suite. The PR's stateless test covers {0, 1, NaN} at phi 0.2 and 0.8, a constant and a runtime scalar phi.

Nothing to start here. When #122914 merges, set the README rows to `fixed on master <sha>`.
