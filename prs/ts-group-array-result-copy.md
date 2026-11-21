# Write the result of `timeSeriesGroupArray` straight into its typed columns

**Branch:** [`lilred-perf-ts-group-array-result-copy`](https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-ts-group-array-result-copy) @ `aea89aafdf0b` (base master `521c4bcb7b94`) · **Status:** built (James, 2026-11-21, #23 issuecomment-7059501978): every target significantly faster but under my predicted floors; judged against the 5% bar, ready to submit · **Report:** [reports/2026-11-21-ts-group-array-result-copy.md](../reports/2026-11-21-ts-group-array-result-copy.md) · **A/B plan:** [ab-plans/ts-group-array-result-copy.md](../ab-plans/ts-group-array-result-copy.md)

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above) -->
Related: https://github.com/ClickHouse/ClickHouse/pull/123142

### Changelog category (leave one):
- Experimental Feature

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
`timeSeriesGroupArray` writes its result faster: merges of the samples tables of `TimeSeries` tables need 12% less CPU, `SELECT ... FINAL` from them is 11-14% faster and `INSERT`s into them with `optimize_on_insert` 7% faster. A `Float32` signaling NaN in its result now keeps its bits, as `Float64` values already did.

### Description

`timeSeriesGroupArray::insertResultInto` inserted every sample as two `Field`s (`timestamps_to.insert(element.timestamp)`, `values_to.insert(element.value)`): a `Field` is built and destroyed, type-checked by `safeGet` and appended with a capacity check, twice per sample. It runs for every sample of every row:
- in each merge of the samples and recent samples tables of a `TimeSeries` table (`AggregatingMergeTree` with a `SimpleAggregateFunction(timeSeriesGroupArray)` column): `AggregatingSortedAlgorithm` finishes every row through `insertResultInto`, rows that have nothing to merge included;
- in `INSERT`s with `optimize_on_insert` (the default), `FINAL` reads, the reads of tables of earlier versions (`timeSeriesGroupArray(timestamp, value) GROUP BY id`) and the raw samples output of PromQL.

Now the two result columns are resized once per row and the samples are copied into them. Unchanged: the order and deduplication of the samples, the state and its serialization, `add` and `merge`. One difference: a `Float32` signaling NaN keeps its bits; the `Field` round trip through `Float64` quieted it (`0x7F800001` became `0x7FC00001`). `Float64` values, the stale marker included, already kept theirs. So during a rolling upgrade a replicated samples table holding such a `Float32` value can merge to different bytes on old and new replicas, which then fetch the part instead.

Release build (clang 21, RelWithDebInfo), AMD EPYC 9R14, two servers pinned to the same 16 cores, master `521c4bcb7b94` and this PR, `perf.py` 13 runs per query per server (the merge: 15 rounds per server, fresh data each round):

| case | master | this PR | change |
|---|---|---|---|
| `timeseries_group_array`: `SELECT ... FINAL` of 4 unmerged parts, 9.6M samples | 439.6 ms | 392.3 ms | -10.8% |
| same test, `GROUP BY` over rows of 60 samples, 9.6M samples | 406.6 ms | 347.5 ms | -14.5% |
| same test, `GROUP BY` over one sample per row, 10M samples | 359.6 ms | 307.4 ms | -14.5% |
| same test, `INSERT` with `optimize_on_insert` | 814.8 ms | 760.3 ms | -6.7% |
| `OPTIMIZE ... FINAL` of 8 parts of a default `TimeSeries` samples table, 19.2M samples: merge CPU from `system.part_log` | 891 ms (880-902) | 783 ms (775-805) | -12.1% |

The four perf-test rows are significant (p < 0.001), and the merge's ranges over 15 rounds per server don't overlap; a second job of 25 runs per query, three passes with fresh servers (one with the ports swapped), gave -11.8% to -14.1% for `FINAL`, -13.0% to -15.1% for the `GROUP BY`s and -6.8% to -7.6% for the `INSERT`. That is 5-7 ns less per sample. Controls: the two `timeSeriesGroupArrayState` queries (-0.8% to +1.1%) and the read without `FINAL` (+4.0% in the first job, but -2.2%, +1.1% and +2.2% in the three passes of the second; an A/A pass of one binary on both servers moved the `FINAL` read by +2.9%, so differences of this size come from the server instances) do the same work without the result and don't move; neither do the 35 queries of `timeseries_to_grid_aggregation_functions` and the 4 of `promql_selector_pk_range` (-1.9% to +2.5%, none reported as changed at the 3% floor).

The merged rows are identical to master's (hash of 80,000 rows × 240 samples after each of the 30 merges, and of the 40,000 rows of the `FINAL` read). `05839_timeseries_group_array_result_columns` checks many groups finished into one block against `arraySort(groupArray(...))` (the array-of-pairs and the two-scalar forms, `DateTime64`/`Float64` and `DateTime`/`Float32`, out-of-order input), the bits of the stale marker, NaN and -0, and a three-part `AggregatingMergeTree` merge. Writing every row at offset 0 instead of after the rows already in the block fails its first two checks; the `Float32` signaling NaN line fails on master (one line of diff), and the test passes 25 of 25 runs with random settings. New perf test `timeseries_group_array.xml`, because no existing test aggregates with `timeSeriesGroupArray`; it is kept out of the Iceberg configuration (`iceberg="0"`), which has no equivalent of the `SimpleAggregateFunction` column.

Complementary to #123142, which skips the merge on insert for blocks with unique keys and scalar `SimpleAggregateFunction` columns: the samples column is an array, so it still takes this path.
<!-- upstream:end -->

**Note for James (not part of the PR):** the category follows your habit for TimeSeries work. If you want the AMD perf comparison in PR CI too, add the `pr-performance` label. The test number 05839 was free at `521c4bcb7b94`; renumber if it collides by the time you submit. #123142 is your own open PR; drop the `Related:` line if you'd rather not link it.

<details><summary>Self-review (lil red, preflight)</summary>

Preflight against the review prompt at master `521c4bcb7b94` (independent reviewer, 3 rounds):
- **Round 1: ⚠️ Request changes.** [Major] the new perf test would be selected for the Iceberg perf configuration, which has no equivalent of a `SimpleAggregateFunction` column, so those jobs would fail: fixed with `<test iceberg="0">`. Nits taken: the `Float32` case really aggregates `Float32` now; the fill tables are merged so no background merge overlaps the timed runs (the `INSERT` query keeps merges running); a `SELECT ... FINAL` of 4 unmerged parts with a no-`FINAL` control makes the merge claim measurable in CI; changelog entry with the `Float32` NaN note; a rolling-upgrade sentence.
- **Round 2: ✅ Approve.** Nits taken: a test comment, and checks of the perf test's part layout (`optimize_throw_if_noop = 1`, `throwIf` on `system.parts`), run on master: 1, 1 and 4 parts.
- **Round 3: ✅ Approve** at `aea89aafdf0b`. Follow-up noted, not in this PR: `timeSeriesLastTwoSamples::insertResultInto` still writes through `Field`.

</details>
