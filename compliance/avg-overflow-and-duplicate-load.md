# `avg` / `avg_over_time` overflow and a duplicate load line (5 evals)

**Status:** analysed 2026-11-08 at master `7b5f2bf328f9`, upstream Prometheus `3da2514eff5c`. Three separate causes; one is covered by an open PR.

## Failing evals

| file:line | query | Prometheus | ClickHouse | cause |
|---|---|---|---|---|
| aggregators.test:651 | `avg(data{test="big"})` | `9.988465674311579e+307` | `+Inf` | A |
| aggregators.test:654 | `avg(data{test="-big"})` | `-9.988465674311579e+307` | `-Inf` | A |
| functions.test:1062 | `avg_over_time(metric8[1m])` | `9.988465674311579e+307` | `+Inf` | B |
| functions.test:1069 | `avg_over_time(metric9[1m])` | `-9.988465674311579e+307` | `-Inf` | B |
| aggregators.test:636 | `avg(data{test="inf3"})` | `NaN` | `+Inf` | C |

## A. `avg` across series overflows (2 evals): in PR #122974

PromQL `avg` becomes `avgForEach` (`applyOneArgumentAggregationOperator.cpp:77`), which adds the values and divides by the count. Four values near `1e308` add up past the largest double, so the sum is `+Inf` and so is the average. Prometheus adds directly too, but switches to an incremental mean (`mean += (x - mean) / n`) as soon as the sum would overflow (`promql/engine.go`, aggregation; same note in `functions.go` near line 1227).

Overlap: [#122974](https://github.com/ClickHouse/ClickHouse/pull/122974) "Make PromQL avg match Prometheus exactly" (open, updated 2026-11-02) replaces `avgForEach` with a new `timeSeriesAvgOverGroup` that uses Prometheus' algorithm and adds `05300_promql_avg_overflow`. Nothing new to start.

## B. `avg_over_time` overflows (2 evals): no one on it

`avg_over_time` becomes `timeSeriesAvgToGrid`. Its state is a sliding compensated (Kahan–Babuška–Neumaier) sum plus a count, and the result is `sum / count` (`src/AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesCompensatedSum.h:112`). Same overflow as A, but per window over time.

The fix is not small. The sum is kept for a sliding window (samples are added and removed as the window moves), so you cannot switch to an incremental mean once the sum overflows: the mean depends on the order of the samples, and removed samples cannot be taken back out. Two possible ways:

1. Keep the sum pre-divided by a fixed power of two (for example, add `x / 2^k`, then multiply the mean back). That costs precision for normal values unless it applies only when values get near the limit, which is a new threshold.
2. Also count the infinite samples in the state, so `getResult` can tell "the inputs contain ±Inf" (the answer really is ±Inf) from "the finite sum overflowed", and recompute that window only in the second case. That changes the state, and `FORMAT_VERSION` is 2.

Both fail the bar of one mechanism, a small diff, no `FORMAT_VERSION` change and no new threshold. Effort M; values within a factor of `count` of `1.8e308` are rare in practice. Parked: not a task.

## C. A duplicate series line in one `load` block (1 eval): in the test loader, not the engine

The `inf3` scenario lists `data{test="inf3",point="d"}` twice in the same `load 10s` block, first `Inf`, then `-Inf`. The upstream runner keeps one definition per series and the later line replaces the earlier one (`promql/promqltest/test.go:915`, `cmd.defs[h] = samples`), so the inputs are `Inf, Inf, -Inf` and the average is `NaN`.

The ClickHouse loader (`tests/integration/test_prometheus_protocols/promqltest_loader.py`) inserts both lines, so the series has two samples with the same timestamp. ClickHouse's grid functions then keep the larger one. Checked on the master binary:

```
timeSeriesResampleToGridWithStaleness(0,0,1,300)(ts, v) over (0, 3), (0, 5), (0, 1)  ->  [5]
                                                     over (0, inf), (0, -inf)   ->  [inf]
                                                     over (0, -inf), (0, inf)   ->  [inf]
```

So the series reads as `Inf, Inf, Inf` and the average is `+Inf`. The `-inf3` scenario (`c` listed as `-Inf`, then `Inf`) passes only because "keep the larger value" and "keep the later line" happen to agree there.

**Fix (S, Python only):** in the loader, keep one `SeriesSpec` per label set in each `LoadBlock`, the last one, as upstream does. No change in the engine. A real Prometheus server would also reject the second sample for a timestamp it already has, so "which duplicate wins" is not something to change in ClickHouse.

Overlap: none found (open PRs, master history of `promqltest_loader.py` since 2026-10-01, clickgapai issues).

**How to prove it:** `aggregators.test:636` passes, extended support goes from 681 to 682 passes, and no other eval changes status. Running `research/bench/compliance/run_extended.py` before and after shows that on one machine.
