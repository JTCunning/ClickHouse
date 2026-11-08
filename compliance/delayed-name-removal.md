# Delayed `__name__` removal (8 evals)

**Status:** in PR [#123419](https://github.com/ClickHouse/ClickHouse/pull/123419) (draft). Analysed 2026-11-08 at master `7b5f2bf328f9`, upstream Prometheus `3da2514eff5c`.

## Failing evals

All in `promqltest/testdata/name_label_dropping.test`:

| line | query | Prometheus | ClickHouse |
|---|---|---|---|
| 59 | `label_replace(rate({env="1"}[10m]), "my_name", "rate_$1", "__name__", "(.+)")` | 2 series, `my_name` = `rate_metric_total` / `rate_another_metric_total` | error 768, duplicate series `{env="1"}` |
| 64 | `label_replace(rate({env="1"}[10m]), "__name__", "rate_$1", "__name__", "(.+)")` | `rate_metric_total`, `rate_another_metric_total` | same error |
| 69 | `label_join(rate({env="1"}[10m]), "my_name", "_", "__name__")` | 2 series by `my_name` | same error |
| 74 | `label_join(rate({env="1"}[10m]), "__name__", "_", "__name__", "env")` | `metric_total_1`, `another_metric_total_1` | same error |
| 89 | `sum(rate({env="1"}[10m])) by (env)` | `{env="1"} 0.4` | same error |
| 115 | `label_replace(sum by (__name__) (rate(metric_total{env="2"}[5m])), "__name__", "$1", "__name__", "(.+)")` | `metric_total 0.05` | series set mismatch (1 vs 1: name missing) |
| 119 | `sum by (__name__) (metric_total{env="1"} or rate(metric_total{env="2"}[5m]))` | `{} 10.05` (one group) | 2 series |
| 123 | same, operands swapped | `{} 10.05` | 2 series |

CI reports these as three groups: "Multiple series have the same tags {'env': '1'}" ×5, "series set mismatch: 2 vs 1" ×2, "series set mismatch: 1 vs 1" ×1.

## Cause

The upstream test runner turns on `EnableDelayedNameRemoval` (`promql/promqltest/test.go:113`). In that mode Prometheus does not remove `__name__` when `rate`, an arithmetic operator or another name-dropping function runs. It marks the series (`DropName`) and removes the name once, at the end of the query (`cleanupMetricLabels`, `promql/engine.go:4339`). In between, the name still takes part in grouping and label functions:

- `label_replace` / `label_join` can read the to-be-dropped name, and writing `__name__` clears the mark (`promql/functions.go:2512`).
- `sum by (env)` aggregates the two `rate` series as different series; only the final result has to be unique.
- `sum by (__name__)` over a mix of marked and unmarked inputs drops the name of the whole group.

ClickHouse removes the name eagerly: `dropMetricName` (`src/Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.cpp`) runs right after each name-dropping step and immediately checks for duplicate tag sets (`timeSeriesThrowDuplicateSeriesIf`). After `rate({env="1"}[10m])` both series are `{env="1"}`, so the query fails before `label_replace` or `sum` can separate or combine them.

Worth knowing: in the Prometheus server this is an experimental feature flag (`--enable-feature=promql-delayed-name-removal`), off by default. With the flag off, Prometheus also fails on lines 59–89. So ClickHouse matches default Prometheus here and fails only because the conformance tests run with the flag on. Line 115 is the one case where eager removal changes a successful answer.

## Fix plan

One mechanism: delay the removal. `dropMetricName` keeps `__name__` and marks the series; the final `finalizeSQL` step removes the name from marked series, merges series that do not overlap in time, and raises the duplicate error only for a real overlap. Label functions that write `__name__` clear the mark. Aggregations and binary operators carry it. Effort: L (it changes every place that drops the name: about 40 files in the open PR).

## Overlap

- [#123419](https://github.com/ClickHouse/ClickHouse/pull/123419) "fix(promql): Delay metric name removal in evaluation." (draft, last updated 2026-10-15) implements exactly this. It uses an internal tag with an empty name as the mark.
- [#126763](https://github.com/ClickHouse/ClickHouse/pull/126763) skips the duplicate check when only one metric name is involved. It touches the same function: whichever merges second needs a rebase.

Nothing new to start. Not verified locally (no build on this machine).

## How to prove it

The 8 evals pass in `test_compliance.py::test_promql_extended_support`: behavioral failures go from 17 to 9 and passes from 681 to 689. A focused stateless test covers lines 59, 89 and 119. Line 84 (`sum by (__name__) (rate(...))` is expected to **fail**) has to keep failing.
