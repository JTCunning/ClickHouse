# [unbuilt] Write the timestamps of `timeSeriesFromGrid` and `timeSeriesRange` straight into their typed column

**Branch:** [`lilred-perf-ts-from-grid-typed-write`](https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-ts-from-grid-typed-write) @ `a25c640b65b3` (base master `521c4bcb7b94`) · **Status:** unbuilt, numbers pending James's build · **Report:** [reports/2026-11-22-ts-from-grid-typed-write.md](../reports/2026-11-22-ts-from-grid-typed-write.md) · **A/B plan:** [ab-plans/ts-from-grid-typed-write.md](../ab-plans/ts-from-grid-typed-write.md)

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above) -->
Related: #133502
Related: (tracking issue for the NULL-argument defect, James to open)

### Changelog category (leave one):
- Experimental Feature

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
`timeSeriesFromGrid` and `timeSeriesRange`, which build the samples of PromQL query results over `TimeSeries` tables, are faster: `timeSeriesFromGrid` by N%, and `prometheusQueryRange` queries that return many series by N% (pending build).

### Description

`timeSeriesFromGrid` and `timeSeriesRange` appended every output timestamp as a `Field` (`res_timestamps->insert(timestamp)`): for each point a `Field` is built, type-checked by `safeGet`, appended with a capacity check and destroyed. `timeSeriesFromGrid` builds the `samples` column of every PromQL range query that returns an instant vector (the last step of the generated SQL, `WHERE notEmpty(samples)`), once per series and step, and turns an instant-vector grid into timestamps and values for subqueries; `timeSeriesRange` builds the timestamps of scalars and of `time()` and `timestamp()`.

Now the timestamps column's container is taken once. A row without NULL values resizes it by the row's number of steps and fills it in a loop; a row with NULL values appends the timestamps of its non-NULL values. The new size is the block's point count, which was already checked for overflow and against `function_range_max_elements_in_block` before anything is written. Unchanged: the values (copied as one range, or one by one in rows with NULL values), the result types, the errors and their order.

Release build, BUILD_HOST, master `521c4bcb7b94` and this PR, `perf.py` N runs per query per server (pending build):

| case | master | this PR | change |
|---|---|---|---|
| `promql_set_operator_presence_mask` q6: `last_over_time(foo[10])`, 4000 series × 300 steps | pending | pending | pending |
| same test, q7: `timeSeriesFromGrid` alone, 100000 rows × 300 points | pending | pending | pending |
| same test, q8: the same with every second value NULL | pending | pending | pending |
| same test, q9: `sum(last_over_time(foo[10]))`, one output series (control) | pending | pending | pending |

Controls: q9 runs the same selector but builds 300 output points, and the six set-operator queries q0-q5 spend a smaller share in this step; A/A spread pending.

The results are identical to master's (`perf.py --result-digest` per query, pending build). `05850_time_series_from_grid_typed_write` checks `DateTime64(3)`, `DateTime64(6)` with a `Decimal` step, `DateTime` and `UInt32` timestamps, `Float32` and `Float64` values, rows with NULL values and all-NULL rows, a start before 1970, the single point of a step larger than the range, the element limit per row and per block, and 1000 rows of 1 to 50 points in one block against the same pairs built from `range`. Its reference is master's output. Resizing to the row's own size instead of after the points of the rows before it fails the many-rows lines, and appending a timestamp for a NULL value fails the NULL lines. Four queries are appended at the end of `promql_set_operator_presence_mask.xml` (10 in all, so CI runs every one).

The same change as #133502, which writes the result of `timeSeriesGroupArray` into its typed columns.
<!-- upstream:end -->

**Note for James (not part of the PR):** the category follows your habit for TimeSeries work (round 1 of the preflight found `Performance Improvement` defensible too, since the two functions aren't gated by a setting). Replace `N%`, `BUILD_HOST` and the table with your run (A/B plan: `ab-plans/ts-from-grid-typed-write.md`); if the query-level gain is under CI's 5% bar, drop "and `prometheusQueryRange` queries … N%" from the entry and keep the function-level number. `Related: #133502` resolves upstream as written; use the full link if you prefer. The test number 05850 was free on master `e4f82e0c28b` and on every `lilred-perf-*` branch; renumber if it collides by the time you submit. Add the `pr-performance` label if you want the AMD perf comparison. Before you submit: open the drafted NULL-argument issue (results issue) and put its link in place of the second `Related:` line; the reviewer asks for it because the defect is on this function's path, though not in the changed lines.

<details><summary>Self-review (lil red, preflight)</summary>

Preflight against the review prompt at master `e4f82e0c28b` (independent reviewer, 2 rounds; the machine can't build, so both verdicts wait on the numbers):
- **Round 1: ⚠️ Request changes**, for the pending build and numbers only: no Blocker or Major. It walked dense, NULL and all-NULL rows, several rows per block, every timestamp type, a start before 1970, the huge step and both element limits through the whole function, and re-ran 05850 on master (same as the reference). Nits taken: the aggregated control is now a query of the XML (q9; 10 queries, all run in CI) and the changelog entry is reworded. Kept: `Related: #133502` (it resolves upstream). It noted `Performance Improvement` as defensible too; kept `Experimental Feature` (James's habit).
- **Round 2: ⚠️ Request changes** at `a25c640b65b3`, again for the pending numbers; the code, the test (byte-identical to the reference on master, three code-69 errors) and the XML are fine. One **[Major] in unchanged code, pre-existing**: a NULL start, end or step is computed as 0 (1970, or step 0), because the result is an `Array` that can't be `Nullable` (a NULL start gives 58,291,203 timestamps for one row at a 30 s step; NULL end or step throw misleading errors). PromQL-generated SQL never reaches it. Not fixed here (a separate mechanism and a behavior change): the issue is drafted for James, and the `Related:` placeholder above takes its link.

</details>
