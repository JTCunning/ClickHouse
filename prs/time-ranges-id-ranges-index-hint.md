# [unbuilt] Do not filter every row of the TimeSeries time ranges table by the id ranges of the selected metrics

Branch: `lilred/perf-time-ranges-id-ranges-index-hint` · Compare: https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred/perf-time-ranges-id-ranges-index-hint

---

When the metric name of a PromQL selector is a literal or a list of alternatives (`{__name__=~"a|b|c|..."}`), the selector reads the time ranges table by the id ranges of the selected metrics. The subquery joined to the tags table gets `WHERE ... AND (id >= (sipHash64('a'), min) AND id <= (sipHash64('a'), max) OR ...)`. The primary key uses this condition to pick the granules, but it also stays a row filter, so every row read is compared with two tuples per selected metric. With 100 metric names that is 200 tuple comparisons per row, and the selector gets 2.5× slower than the equivalent regular expression, which reads the same rows without id ranges.

This PR splits the condition in two:
- The range comparisons go into `indexHint`, so the primary key still selects the same granules, the same way the selector already handles the whole-metric id range on the samples side.
- The row filter becomes `tupleElement(id, 1) IN (sipHash64('a'), ...)`, one set lookup per row.

A range covers every value of the second component of `id` (`TimeSeriesIDRange` sets its bounds to the minimum and maximum of the type), so the new row filter accepts exactly the rows the old one did. `makeIDOutOfRangesCondition`, which keeps the time series outside the ranges in the outer `WHERE`, already relies on this. The result of the query does not change, and the change works the same for a time ranges table whose sorting key does not start with `id`.

### Evidence

Host: 2 vCPU / 4 GiB VM. Base: master `a7f713612c8` (26.11.1.974). There is no change to `src/Storages/StorageTimeSeriesSelector.cpp` or `src/Storages/TimeSeries/` between it and the branch base `26f120e95b5`.

The branch column is emulated, not built. I ran the SQL the selector logs (`Will execute query:`) as is (master), and again with only the time ranges condition replaced by `indexHint(<ranges>), not(notIn(tupleElement(id, 1), (<hashes>)))`, which is the SQL the branch generates. Both used the settings the selector applies internally: `join_algorithm = 'full_sorting_merge'`, `optimize_read_in_order = 1`, `merge_tree_min_bytes_for_concurrent_read = 4194304`, `merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem = 4194304` and `merge_tree_min_rows_for_concurrent_read = 0`. Runs were interleaved: 1 cold run discarded, then the median (min–max) of 6 warm runs. Results are identical in both columns: `count()` and `sum(cityHash64(*))` match. `read_rows` and `SelectedMarks` are equal in both columns.

| case | workload | read_rows / marks | master | branch (emulated) |
|---|---|---|---|---|
| `promql_selector_name_alternatives.xml`, 100-name alternation | 200 metrics × 1000 series, 20 samples; `max_threads = 4` | 420,992 / 485 | 437 ms (434–483) | 233 ms (221–244) |
| same table, regex `m_1[0-9][0-9]` over the same 100 metrics (control, no id ranges) | same | 420,992 | 173 ms | no change in the generated SQL |
| 111-name alternation | 200k series over 200 metrics, 4 h of 15 s samples, 5 min window; `max_threads = 2` | 722,464 / 3,298 | 855 ms (834–895) | 648 ms (617–671) |
| 10-name alternation | same | 200,656 / 339 | 126 ms (123–134) | 121 ms (115–138) |
| one metric | same | 23,938 / 34 | 23 ms (22–25) | 23 ms (22–24) |

Commands for the branch build: run `tests/performance/promql_selector_name_alternatives.xml` through the performance comparison, or by hand. Run its `create_query` and `fill_query`, then each `<query>` once cold and 5 times warm, and take the median `query_duration_ms` from `system.query_log`.

### Tests

- `05479_time_series_selector_id_ranges_index_hint.sql` runs a selector by one metric, by a list of alternatives, and by a regular expression over the same metrics.
  - Each metric has a series inside the time range and one after it.
  - Two more series of `foo` are written with ids outside the id range of `foo`, one inside the time range and one after it.
  - All three selectors must select the same samples: only the in-range series inside the time range and the other-id series inside the time range.
  - The test also reads the generated query from `system.text_log` and counts the `indexHint` wrappers and the filters by the first component of `id`. On master it prints `0 1` for the two selectors with id ranges and fails against the reference (`1 2`).
- New perf test `tests/performance/promql_selector_name_alternatives.xml` runs a 100-name alternation, with a single metric and the equivalent regular expression as controls.
  - No existing `tests/performance/promql_*` or `timeseries_*` file has a selector with more than one metric name.
  - `promql_selector_pk_range.xml` and `promql_selector_id_range_row_cost.xml` select one metric. Their time ranges filter changes from two tuple comparisons per row to one integer comparison per row.

Related: https://github.com/ClickHouse/ClickHouse/pull/121924

### Changelog category (leave one):
- Performance Improvement

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
A PromQL selector whose metric name is a list of alternatives, such as `{__name__=~"a|b|c"}`, no longer compares every row of the `TimeSeries` time ranges table with the id range of each listed metric. With 100 metric names the selector is about 1.9× faster.

<details>
<summary>Self-review (preflight) — not part of the upstream text</summary>

Round 1 (2026-10-31): ⚠️ Request changes.
- Major: the pure `indexHint` form loses pruning for a time ranges table whose sorting key does not start with `id`. Fixed by adding the exact first-component filter.
- Major: the debug-log grep test was fragile. Replaced with a `.sql` test that reads `system.text_log` by `query_id`.
- Missing: an out-of-range-id case and the emulation settings. Both added.

Round 2: ✅ Approve.
- Nit (perf test comment wording) fixed.
- Remaining before merge: CI must confirm the two hand-predicted reference lines (`1 2`) and the perf comparison for the 100-name query.

</details>
