# Run the tags lookup once per distinct selector (handoff #5)

**Verdict:** already covered by James's branch `jtcunning/promql-empty-selector-short-circuit` (commit `14629dc794`). It looks up the ids once per distinct selector and time range, keeps them in a per-query temporary table, and reuses them. The approach is right and the saving is real, about 14 ms per repeated narrow selector on this machine. It needs a rebase onto current master before it can go upstream, though, and it has one cost to fix first: the eager temporary table also applies to selectors that occur only once, and the #4 measurements put that at ~30% slower on wide metrics. No new branch from me.
**Task:** 2026-10-31-handoff-run-the-tags-lookup-once-per-di · handoff https://github.com/JTCunning/ClickHouse/issues/5 · **Build:** 26.11.1.974, master binary of 2026-10-31 (clone at `505396149ab5`) · **Date:** 2026-10-31

## What happens (plain language)
Today every `timeSeriesSelector` occurrence in a PromQL query runs its own tags lookup ("which series ids match these labels in this time window"). Master already shares some repeated work: two aggregations over the same selector (`sum(x) / count(x)`) or the same range function (`rate(x[5m])` twice) read once. A binary operator with the same instant selector on both sides (`x + x`, `x unless x == 0`) still does two full reads. James's branch moves the lookup into the PromQL-to-SQL step. It runs each distinct (table, matchers, time window) lookup once, stores the ids in a temporary table for the query, and each `timeSeriesSelector` reads that small table instead of the tags table. A selector with no matching series is turned into an empty result before any SQL is built.

## Numbers
Dataset: synthetic, 200,000 series over 50 metrics (~4,000 series each), labels `job` (10 values), `instance` (500), `pod` (unique), 240 samples per series at 15 s (1 h), v11 (bucketed) TimeSeries table with default settings. Local `clickhouse server`, `max_threads=2`, instant query at the end of the hour, median of 10 warm runs (cold run dropped).

| query (master) | tags lookups | median ms | min–max | plan+opt ms | read_rows |
|---|---|---|---|---|---|
| `metric_1{pod="pod-1"}` | 1 | **25** | 22–30 | 22.7 | 44,484 |
| `metric_1{pod="pod-1"} + metric_1{pod="pod-1"}` | 2 | **49** | 46–58 | 45.8 | 88,968 |
| `sum(metric_1{pod="pod-1"}) / count(metric_1{pod="pod-1"})` | 1 | **24** | 23–31 | 22.5 | 44,484 |
| `metric_1{job="job3"}` (~400 series) | 1 | **53** | 49–59 | 27.9 | 76,525 |
| `metric_1{job="job3"} + metric_1{job="job3"}` | 2 | **105** | 99–112 | 56.9 | 153,050 |
| `rate(metric_1{pod="pod-1"}[5m])` | 1 | **25** | 23–36 | 22.3 | 44,484 |
| `rate(…[5m]) / rate(…[5m] offset 10m)` | 2 | **50** | 47–58 | 44.9 | 88,968 |
| tags lookup alone, `metric_1{pod="pod-1"}` | – | **14** | 14–15 | 2.6 | – |

- **What a lookup cache can save:** about 14 of the 25 ms of a narrow selector. The other ~11 ms is planning and reading the samples, which a lookup cache does not remove. Expected for `x + x`: 49 → ~35 ms on the branch (pending build). James's other branch `jtcunning/promql-share-identical-instant-selectors` shares the whole read for that shape, so 49 → ~25 ms.
- **Offset pairs don't benefit.** `rate(x[5m]) / rate(x[5m] offset 10m)` has two different time windows, so it correctly does two lookups both on master and on the branch.
- **Recording-rule `INSERT … SELECT` on one node already looks up once on master** (1 tags lookup, 1 PromQL plan). The double lookup comes from the parallel-replicas check (`isSuitableForInsertSelectWithParallelReplicas`), as described on James's `timeseries-whole-metric-probe-once-per-query` branch. The temp table of the short-circuit branch would serve that second pass too, since both run in one query context. Not measured; it needs a parallel-replicas cluster.

## Why (cause)
Each `timeSeriesSelector` is an independent table function. Its `readImpl` emits `id IN (SELECT timeSeriesStoreTags(...) FROM <tags> LEFT JOIN <time ranges> ... )`, and on v11 tables that set is built while the samples read is planned. Nothing keys the lookup by its inputs, so two identical selectors in one query each pay for it. Master's materialized-CTE sharing dedups identical *step bodies*. With `x + x`, though, the two sides are visited as separate nodes, so each gets its own step.
Pointers (master `505396149ab5`): `src/Storages/StorageTimeSeriesSelector.cpp:1219` (`readImpl`), `:1177` (`makeSelectIDsQuery`), `src/Storages/TimeSeries/PrometheusQueryToSQL/fromSelector.cpp` (`fromRangeSelector`).

## Review of James's branch (`14629dc794`)
**Right:**
- The cache key is the storage id, the matcher list (name, value, type), min/max time and the time scale. That's the same identity the handoff proposed, so a different window (offset) never hits.
- It lives for one query (temporary table in the query context), so there's nothing to invalidate.
- The HTTP API path passes a session query context, so it works there too.

**Needs work before upstream:**
1. **Rebase.** It is ~3,600 commits behind master. `StorageTimeSeriesSelector.cpp` and `fromSelector.cpp` conflict with the v11 bucketed-samples change (#117344). On master the whole-metric id-range part it edits is v10-only (its test is now `04836_time_series_v10_selector_whole_metric_pk_range`). Test numbers 05442 and 05443 are taken upstream.
2. **Wide single-use selectors get slower.** The eager temp table replaces today's in-place `IN` set for *every* selector, not only repeated ones. My #4 emulation (report `2026-10-31-selector-eager-ids.md`) measured a pre-built set ~30% slower on wide metrics: `metric_0` 108 → 140 ms, `metric_0{job="job3"}` 97 → 130 ms. The cause is that today's lookup streams in id order alongside the samples read. Options: use the temp table only when a selector occurs more than once (the pre-pass already exists on the share-identical branch) or when it is empty, or accept and show the trade-off. The review bot will ask for `tests/performance/promql_*` numbers either way.
3. **`EXPLAIN` now runs the tags lookup**, because the lookup happens while the SQL is built. That's intended for the empty short-circuit (the test relies on it), but on a wide selector `EXPLAIN` stops being free. It should be stated in the PR.
4. **One behaviour change rides along.** A selector with a non-name matcher no longer gets the whole-metric id range. That is v10-only after the rebase, but the description should say so.
5. **The test misses one handoff "done when" case:** it never runs an `INSERT … SELECT FROM prometheusQueryRange`. The parallel-replicas variant is what actually double-plans.

**Nits:** matcher order is part of the key (`{a="1",b="2"}` vs `{b="2",a="1"}` miss each other; harmless). The log line `Selector {...} over [...] selects N series` is at DEBUG, which the test depends on via `send_logs_level=debug`.

## Proposed fix
Report only. James's branch is the fix. Suggested path: rebase it onto master, use the temp table only for selectors that repeat or match nothing, and pair it with `promql-share-identical-instant-selectors` for the `x + x` shape. Expected effect: ~14 ms (about 55%) off each repeated narrow selector, and no change for single-use selectors. Effort: M (rebase plus gating).

## Upstream overlap
- #118801 (nikitamikhaylov, open): resolves selectors grouped by metric in one tags scan via `MaterializedCTE`, with analyzer changes. A broader mechanism for the same cost; whichever lands first shapes the other.
- #118116 (groeneai, open): stops re-probing selectors that are known not to match a whole metric. v10-only relevance after #117344.
- #127847 (valerypetrov, open): an `ANY` join with the time-ranges table. It cuts the per-lookup cost that this cache avoids repeating, so the two are complementary.
- `git log origin/master` on the touched files since James's base (`81ae09f7e802`): only #117344 (`daaa0be735e9`).

## How to reproduce
Data load (one `clickhouse local --path db` session; `T0 = floor(now/900)*900 - 3600`), 8 batches of 25,000:
```sql
SET allow_experimental_time_series_table = 1;
CREATE TABLE ts ENGINE = TimeSeries;
INSERT INTO ts (metric_name, tags, samples)
WITH cityHash64(number) AS h
SELECT concat('metric_', toString(number % 50)),
       map('job', concat('job', toString(h % 10)), 'instance', concat('host-', toString(intDiv(h, 10) % 500)), 'pod', concat('pod-', toString(number))),
       arrayMap(i -> (toDateTime64(T0 + i * 15, 3), toFloat64((number % 1000) + i)), range(240))
FROM numbers(<batch*25000>, 25000) SETTINGS max_insert_block_size = 4096, max_threads = 1, max_block_size = 4096;
```
Then start `clickhouse server` on that path (127.0.0.1, `query_log` and `text_log` on). For each shape, run 11 times with a `log_comment`:
```sql
SELECT count() FROM prometheusQuery(ts, '<expr>', toDateTime64(T_END, 3)) FORMAT Null SETTINGS max_threads = 2;
```
Read `query_duration_ms`, `ProfileEvents['QueryPlanBuildMicroseconds'] + ProfileEvents['QueryPlanOptimizeMicroseconds']` and `read_rows` from `system.query_log`. Count lookups as lines containing `timeSeriesStoreTags(id` in the client output with `--send_logs_level=debug`. For "tags lookup alone", cut the `in(id, (...))` subquery out of the `StorageTimeSeriesSelector` "Will execute query" log line and run `SELECT count() FROM (<subquery>) SETTINGS join_algorithm='full_sorting_merge', optimize_read_in_order=1`.

**Side note (not this task):** with `clickhouse local` reopening an existing path, any PromQL query with two or more selectors fails with `UNKNOWN_TABLE: The Tags inner table `` (<uuid>) ... doesn't exist`, while single-selector queries work. This happens even with `async_load_databases=0` and `enable_materialized_cte=0`. The same data works through `clickhouse server`. It is probably a `clickhouse local` lookup-by-UUID gap. Not investigated further.
