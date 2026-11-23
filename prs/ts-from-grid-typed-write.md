# Write the timestamps of `timeSeriesFromGrid` and `timeSeriesRange` straight into their typed column

**Branch:** [`lilred-perf-ts-from-grid-typed-write`](https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-ts-from-grid-typed-write) @ `92789976f58` (base master `521c4bcb7b94`) · **Status:** run 1 (function level, `a25c640b65b3`) passed 2026-11-23; run 2 (dashboard queries through the HTTP API, head `92789976f58`) pending, A/B plan section "Run 2" · **Report:** [reports/2026-11-22-ts-from-grid-typed-write.md](../reports/2026-11-22-ts-from-grid-typed-write.md) · **A/B plan:** [ab-plans/ts-from-grid-typed-write.md](../ab-plans/ts-from-grid-typed-write.md)

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above) -->
Related: https://github.com/ClickHouse/ClickHouse/pull/133502
Related: (tracking issue for the NULL-argument and 32-bit wrap defects, James to open)

### Changelog category (leave one):
- Experimental Feature

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
`timeSeriesFromGrid` and `timeSeriesRange`, which build the samples of PromQL query results over `TimeSeries` tables, write their timestamps straight into the result column. (Entry after run 2: the dashboard-query change leads; the function-level 2x only as the mechanism.)

### Description

`timeSeriesFromGrid` and `timeSeriesRange` appended every output timestamp as a `Field` (`res_timestamps->insert(timestamp)`): for each point a `Field` is built, type-checked by `safeGet`, appended with a capacity check and destroyed. `timeSeriesFromGrid` builds the `samples` column of every PromQL range query that returns an instant vector (the last step of the generated SQL, `WHERE notEmpty(samples)`), once per series and step, and turns an instant-vector grid into timestamps and values for subqueries; `timeSeriesRange` builds the timestamps of scalars and of `time()` and `timestamp()`.

Now the timestamps column's container is taken once. A row without NULL values resizes it by the row's number of steps and fills it in a loop; a row with NULL values appends the timestamps of its non-NULL values. The new size is the block's point count, which was already checked for overflow and against `function_range_max_elements_in_block` before anything is written. Unchanged: the timestamp arithmetic, the values (copied as one range, or one by one in rows with NULL values), the result types, the errors and their order.

Dashboard queries through the Prometheus HTTP API (`/api/v1/query_range`, JSON) on a `TimeSeries` table with 2,000 series of a counter `m` and 2,000 of a gauge `g` (labels `job` with 10 values, `instance`, `pod`), one sample every 15 s over 6 h. Release build (clang 21, `RelWithDebInfo`), AMD EPYC 9R14 with servers and clients pinned to 16 cores, master `521c4bcb7b94` and this PR, median of 100 rounds, the two requests of a round sent next to each other:

| Label | Query | Base | This PR |
|---|---|---|---|
| R1 | `rate(m{job="job3"}[5m])`, last 1 h, step 15 s (200 series) | pending | pending |
| R2 | `rate(m[5m])`, last 1 h, step 15 s (2,000 series) | pending | pending |
| R3 | `sum by (job) (rate(m[5m]))`, last 1 h, step 15 s (10 series, control) | pending | pending |
| R4 | `g{job="job3"}`, last 1 h, step 15 s (200 series) | pending | pending |
| R5 | `g`, last 1 h, step 15 s (2,000 series) | pending | pending |
| R6 | `rate(m[5m])`, last 6 h, step 30 s (2,000 series) | pending | pending |

The function alone, where the change is: the queries of `promql_set_operator_presence_mask.xml` (`perf.py --runs 13`, three passes on fresh servers, one with the ports swapped), and `clickhouse benchmark` at `max_threads = 1` over `fg_dense (v Array(Nullable(Float64))) ENGINE = Memory`, 8000 rows of 300 values, and `fg_null`, the same with half the values NULL (200 queries per case split at random between the servers):

| Label | Query | Base | This PR |
|---|---|---|---|
| q7 | `SELECT timeSeriesFromGrid(toDateTime64(100, 3), toDateTime64(3090, 3), 10, v) FROM (SELECT materialize(CAST(arrayResize([], 300, 1.5), 'Array(Nullable(Float64))')) AS v FROM numbers(100000)) SETTINGS max_block_size = 8192 FORMAT Null` | 210.5 ms | 102.9 ms |
| q8 | `SELECT timeSeriesFromGrid(toDateTime64(100, 3), toDateTime64(3090, 3), 10, v) FROM (SELECT materialize(CAST(arrayMap(j -> if(j % 2 = 0, NULL, 1.5), range(300)), 'Array(Nullable(Float64))')) AS v FROM numbers(100000)) SETTINGS max_block_size = 8192 FORMAT Null` | 146.1 ms | 100.5 ms |
| T3 | `SELECT timeSeriesFromGrid(toDateTime64(100, 3), toDateTime64(3090, 3), 10, v) FROM fg_dense FORMAT Null` | 12.8 ms | 4.5 ms |
| T4 | `SELECT timeSeriesFromGrid(toDateTime64(100, 3), toDateTime64(3090, 3), 10, v) FROM fg_null FORMAT Null` | 19.1 ms | 15.2 ms |
| T5 | `SELECT timeSeriesRange(materialize(toDateTime64(100, 3)), toDateTime64(3090, 3), 10) FROM fg_dense FORMAT Null` | 10.7 ms | 2.1 ms |
| q6 | `SELECT * FROM prometheusQueryRange('promql_set_operator_presence_mask_ts', 'last_over_time(foo[10])', 100, 3090, 10) FORMAT Null` | 69.3 ms | 67.7 ms |
| q9 | `SELECT * FROM prometheusQueryRange('promql_set_operator_presence_mask_ts', 'sum(last_over_time(foo[10]))', 100, 3090, 10) FORMAT Null` | 36.9 ms | 36.6 ms |

q7, q8, T3, T4 and T5 are differences (p < 0.001 and at 99.5% confidence); q6 and q9 are not. The saving is about 3.5 ns per output point. q6 has one sample per series per window and no output formatting, the shape where building the output points is the largest share of a range query, and still moves only 0.6–2.4%.

Controls: q9 and the set-operator queries q0–q5 (−3.1% to +0.2%) don't get slower. Master against master in the same harnesses differs by at most 1.6% (`perf.py`) and 1.7% (the benchmark cases).

The results are identical to master's: `perf.py --result-digest` gives the same row count and digest for q0–q8 in every pass (q9 sums floats across threads, so its digest varies between runs of master too), and so does a `cityHash64` of every benchmark output. `05850_time_series_from_grid_typed_write` checks `DateTime64(3)`, `DateTime64(6)` with a `Decimal` step, `DateTime` and `UInt32` timestamps, `Float32` and `Float64` values, rows with NULL values and all-NULL rows, a range that starts before 1970, the single point of a step larger than the range, the element limit per row and per block, and 1000 rows of 1 to 50 points in one block against the same pairs built from `range`. Its reference is master's output, and it passes 25 of 25 runs with random settings. Resizing to the row's own size instead of after the points of the rows before it fails the many-rows lines, and appending a timestamp for a NULL value fails the NULL lines. The existing `timeSeriesRange` and `timeSeriesFromGrid` tests and `test_prometheus_protocols/test_evaluation.py` pass. Four queries are appended at the end of `promql_set_operator_presence_mask.xml` (10 in all, so CI runs every one).

The same change as #133502, which writes the result of `timeSeriesGroupArray` into its typed columns.
<!-- upstream:end -->

**Note for James (not part of the PR):**
- Run 2 (your call on 2026-11-23, option B): the first table is the headline and comes from dashboard queries through `/api/v1/query_range`, A/B plan section "Run 2". The changelog entry gets written from it: if those rows show a change, it leads with that number; if none does, we talk about parking or dropping this PR before you submit. Run 1's function-level numbers (second table, `a25c640b65b3`) stand: `92789976f58` only moves the timestamp expression out of a helper lambda back into the two loops, as master writes it (identical assembly for both forms in a clang 20 `-O3 -march=x86-64-v3` harness).
- Why that commit: upstream PR 120906 ("Enable the UBSan check for unsigned integer overflows") merged on 2026-11-23 and put `NO_SANITIZE_UNSIGNED_OVERFLOW` on this function's `doExecute`. A lambda doesn't inherit that attribute (checked with clang 20 in trap mode), so with the lambda, 05850's range across 1970, whose unsigned sum wraps, would fail the UBSan job.
- Before you submit: open the drafted issue in #26's description (now two pre-existing defects: NULL arguments, and the 32-bit wrap after 2106 that round 3 found; both reproduced on master) and put its link in place of the second `Related:` line; the reviewer asks for it because both are on this function's path, though not in the changed lines. #133502 merged on 2026-11-23.
- The category follows your habit for TimeSeries work (`Performance Improvement` is defensible too: the two functions aren't gated by a setting). Add the `pr-performance` label if you want the AMD perf comparison. 05850 is still free on master `9cbdad9a7e3c`, and the branch merges into it without conflicts.

<details><summary>Self-review (lil red, preflight)</summary>

Preflight against the review prompt at master `e4f82e0c28b` (independent reviewer, 2 rounds before the build):
- **Round 1: ⚠️ Request changes**, for the pending build and numbers only: no Blocker or Major. It walked dense, NULL and all-NULL rows, several rows per block, every timestamp type, a start before 1970, the huge step and both element limits through the whole function, and re-ran 05850 on master (same as the reference). Nits taken: the aggregated control is now a query of the XML (q9; 10 queries, all run in CI) and the changelog entry is reworded. Kept: `Related: #133502` (it resolves upstream). It noted `Performance Improvement` as defensible too; kept `Experimental Feature` (James's habit).
- **Round 2: ⚠️ Request changes** at `a25c640b65b3`, again for the pending numbers; the code, the test (byte-identical to the reference on master, three code-69 errors) and the XML are fine. One **[Major] in unchanged code, pre-existing**: a NULL start, end or step is computed as 0 (1970, or step 0), because the result is an `Array` that can't be `Nullable` (a NULL start gives 58,291,203 timestamps for one row at a 30 s step; NULL end or step throw misleading errors). PromQL-generated SQL never reaches it. Not fixed here (a separate mechanism and a behavior change): the issue is drafted for James, and the `Related:` placeholder above takes its link.
- **Round 3: ⚠️ Request changes** at `92789976f58` with the built numbers, against the review prompt at master `9cbdad9a7e3c` (last changed `025fc5175c4`). No finding in the changed code; it re-derived every memory-safety precondition (the typed cast from the result type, the resize to `total_points`, the null-map index, equal counts in both tuple columns). Its findings and what I did:
  - [Major] the NULL-argument defect has only a placeholder `Related:`: stays as James's decision (open the drafted issue).
  - [Major] new, pre-existing, reproduced on master: a `DateTime`/`UInt32` start with a `DateTime64` end after 2106-02-07 06:28:15 wraps the later points to 1970. Added to the same drafted issue; not fixed here (a separate mechanism).
  - [Major] the `timeSeriesRange` 5x in the entry has no CI perf query and no query in the body: the entry now names only `timeSeriesFromGrid` (q7, plus the NULL case q8 it asked for), and the body gives both benchmark queries and the table.
  - Taken: the full link for `Related:`; "84 → 79 ms" for the whole query dropped (not significance-tested). Kept: `Experimental Feature` (James's habit); the four queries stay in the presence-mask XML (10 queries, all run in CI); the reason for the inline expression stays out of the body (only the final code goes upstream).

Round 3 was the last of the three rounds; the remaining Major is the one James closes by opening the issue.

</details>
