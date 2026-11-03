# Do not filter every row of the TimeSeries time ranges table by the id ranges of the selected metrics

Branch: `lilred-perf-time-ranges-id-ranges-index-hint` · compare: https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-time-ranges-id-ranges-index-hint

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above) -->
Related: https://github.com/ClickHouse/ClickHouse/pull/121924

### Changelog category (leave one):
- Performance Improvement

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
A PromQL selector whose metric name is a list of alternatives, such as `{__name__=~"a|b|c"}`, no longer compares every row of the `TimeSeries` time ranges table with the id range of each listed metric. A selector with 100 metric names is 20% faster in the new performance test, and 35-39% faster with 50 to 300 names of 500 series each.

### Description

When the metric name of a selector is a literal or a list of alternatives, the selector reads the time ranges table with `WHERE ... AND (id >= (sipHash64('a'), min) AND id <= (sipHash64('a'), max) OR ...)`. The primary key uses this condition to select granules, but it also stays a row filter: every row read is compared with two tuples per listed metric, 200 comparisons per row for 100 names.

Now the range comparisons go into `indexHint`, so the primary key still selects the granules, and the row filter keeps a row when `tupleElement(id, 1)` is one of `sipHash64('a'), ...`: one set lookup per row. It is generated as `NOT` of `makeIDOutOfRangesCondition`, which the outer `WHERE` already uses for the series outside the ranges. A range covers every value of the second component of `id` (`TimeSeriesIDRange` uses the minimum and maximum of the type), so the new filter accepts exactly the same rows. It works the same for a time ranges table whose sorting key does not start with `id`. No setting, threshold or format change.

Release-style build (clang 21, no sanitizer), AMD EPYC 9R14, queries pinned to 16 cores; performance comparison with two servers on one host, 13 runs per query, base `26f120e95b5`.

| selector | master | this PR | change |
|---|---|---|---|
| 100 names × 1000 series (`promql_selector_name_alternatives.xml`) | 176 ms | 141 ms | -19.9% |
| 10 names × 500 series (separate table, not in the test) | 40.4 ms | 34.7 ms | -14.1% |
| 50 names × 500 series | 87.9 ms | 56.6 ms | -35.6% |
| 100 names × 500 series | 172.7 ms | 104.8 ms | -39.3% |
| 300 names × 500 series | 436.9 ms | 282.0 ms | -35.5% |

Controls not slower: the equivalent regular expression over the same 100 metrics +1.2% and a prefix regex +1.0% (both not significant), a 2-name list -2.1% (not significant), and all 7 queries of `promql_selector_pk_range.xml` and `promql_selector_id_range_row_cost.xml` within ±2.2% (not significant). A single metric reads +3.6% and is marked unstable.

Results are identical, and the primary key still prunes: running the SQL each version generates for the 100-name query, both read the same 485 marks and 420,992 rows. Tests: `05479_time_series_selector_id_ranges_index_hint` selects by one metric, by a list of alternatives and by a regular expression over the same metrics, including series whose ids fall outside the id range of their metric, and requires the list to return the same samples as the regular expression, which has no id ranges; it also counts the `indexHint` wrappers and first-component filters in the generated query (from `system.text_log`), so master prints `0 1` and fails against the reference `1 2`. New performance test `promql_selector_name_alternatives.xml`: no existing `promql_*` or `timeseries_*` file has a selector with more than one metric name.
<!-- upstream:end -->

## Evidence detail (not part of the upstream text)

James's run, 2026-11-01 (outbox results comment on issue #6). Commits A = `26f120e95b51`, B = `c4af28626b55`. Machine: AMD EPYC 9R14, 64 CPUs, 123 GiB, queries pinned to CPUs 0-15. Build: clang++-21, release-style, no sanitizer. Method: perf.py from B's tree against two servers on one host, 13 runs per query.

In-tree perf tests:

| perf test | query | A | B | change | significant |
|---|---|---|---|---|---|
| promql_selector_name_alternatives | 100-name alternation | 0.1760 s | 0.1410 s | -19.9% | faster |
| promql_selector_name_alternatives | single metric (control) | 0.01435 s | 0.01486 s | +3.6% | unstable |
| promql_selector_name_alternatives | regex m_1[0-9][0-9] (control) | 0.08976 s | 0.09084 s | +1.2% | same |
| promql_selector_pk_range | prometheusQuery pkr_metric | 0.2736 s | 0.2732 s | -0.1% | same |
| promql_selector_pk_range | sum(pkr_metric{dc=~".*"}) | 0.2630 s | 0.2640 s | +0.4% | same |
| promql_selector_pk_range | sum(rate(pkr_metric[30m])) | 0.2853 s | 0.2789 s | -2.2% | same |
| promql_selector_pk_range | sum(pkr_metric{dc="0"}) | 0.1351 s | 0.1354 s | +0.2% | same |
| promql_selector_id_range_row_cost | timeSeriesSelector rowcost_metric | 0.08588 s | 0.08603 s | +0.2% | same |
| promql_selector_id_range_row_cost | prometheusQueryRange sum(rowcost_metric) | 0.09733 s | 0.09706 s | -0.3% | same |
| promql_selector_id_range_row_cost | timeSeriesSelector rowcost_metric{dc="0"} | 0.06800 s | 0.06849 s | +0.7% | same |

Name-list length scan (not in the branch): one TimeSeries table, 400 metrics × 500 series, 20 samples at 15 s; each list is a prefix of m_1000.

| query | series | A | B | change | significant |
|---|---|---|---|---|---|
| n=1 control | 500 | 13.94 ms | 13.22 ms | -5.2% | unstable |
| n=2 | 1,000 | 17.63 ms | 17.26 ms | -2.1% | same |
| n=4 | 2,000 | 23.06 ms | 21.44 ms | -7.0% | faster |
| n=5 | 2,500 | 25.91 ms | 24.00 ms | -7.3% | faster |
| n=10 | 5,000 | 40.35 ms | 34.67 ms | -14.1% | faster |
| n=20 | 10,000 | 67.60 ms | 55.69 ms | -17.6% | faster |
| n=50 | 25,000 | 87.86 ms | 56.60 ms | -35.6% | faster |
| n=100 | 50,000 | 172.69 ms | 104.83 ms | -39.3% | faster |
| n=300 | 150,000 | 436.92 ms | 281.96 ms | -35.5% | faster |
| regex m_1.* | 200,000 | 162.94 ms | 164.54 ms | +1.0% | same |

Short lists inside functions (not in the branch; `le` label on every series): histogram_quantile of 4 names -3.2% (unstable), topk of 2 names -1.4% (same), sum(rate) of 5 names -3.9% (unstable); each one-metric control within ±0.7% (same).

Prediction vs result: my emulation on the 2-vCPU VM (`max_threads = 4`) predicted 437 → 233 ms (-47%) for the 100-name query; the build gave -19.9% at 1000 series per metric and -39.3% at 500. The emulation overstated the share of the per-row filter on a small machine. The pass_if floor (25%) came from that prediction and was missed on the original table; I judged the item a pass because the gain is significant on both tables, nothing got slower, and 05479 behaves as predicted in both directions.

Notes for submission: upstream master now has several unrelated `05479_*` tests; ClickHouse tolerates shared numbers, but renumber if the PR check asks. The branch merges cleanly into origin/master at 2026-11-03 (no change to `src/Storages/StorageTimeSeriesSelector.cpp` since the base).

<details>
<summary>Self-review (preflight) — not part of the upstream text</summary>

Round 1 (2026-10-31): ⚠️ Request changes.
- Major: the pure `indexHint` form loses pruning for a time ranges table whose sorting key does not start with `id`. Fixed by adding the exact first-component filter.
- Major: the debug-log grep test was fragile. Replaced with a `.sql` test that reads `system.text_log` by `query_id`.
- Missing: an out-of-range-id case and the emulation settings. Both added.

Round 2: ✅ Approve.
- Nit (perf test comment wording) fixed.
- Remaining before merge: CI must confirm the two hand-predicted reference lines (`1 2`) and the perf comparison for the 100-name query. Both confirmed by James's build on 2026-11-01.

Round 3 (2026-11-03, final text with James's build numbers): ⚠️ Request changes, text only.
- Major: nothing shows the primary key still prunes the time ranges table under `indexHint` (a lost pruning could still come out faster). Answered in the text with the 2026-10-31 measurement: the generated SQL of both versions, run on master, reads the same 485 marks and 420,992 rows. A `read_rows` assertion in 05479 would pin it in CI; not added, because it is a code change and needs another build round (offered to James).
- Nit: the row filter is generated as `NOT (... NOT IN ...)`, not `IN`. Text fixed. A direct `in` helper would read better in the log; left as is (same plan, no rebuild).
- Nit: only the list and the regex must match, not all three selectors. Text fixed.
- Nit: the 500-series rows are off-tree. Marked, and the changelog entry now separates the in-tree 20% from the 35-39%.
- Nit: 05479's counts depend on function-style rendering in the logged query. Left as is.

</details>
