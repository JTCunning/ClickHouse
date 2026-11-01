# Repeated PromQL selectors: build the series-id set once per query (handoff #5, second pass)

**Verdict:** worth fixing, with a smaller mechanism than the temp-table branch. Fix branch `lilred/perf-promql-shared-selector-sets` (unbuilt). It reuses ClickHouse's existing per-query set cache (today only mutations use it), so a repeated selector adopts the id set the first one built. Expected: ~12–14 ms off each repeated occurrence (`x + x` 45 → ~32 ms on this machine), and no change for selectors used once.
**Task:** 2026-10-31-handoff-run-the-tags-lookup-once-per-di · handoff https://github.com/JTCunning/ClickHouse/issues/5 · **Build:** 26.11.1.974, master `a7f713612c80` (branch based on `c189bd99fbda`) · **Date:** 2026-11-01

## What happens (plain language)
In a PromQL query, every selector looks up "which series ids match these labels in this time window" in the tags table, then reads the samples of those ids. On v11 tables that lookup runs while the query is being planned, so the planner can use the id list for the samples table's primary key. When the same selector appears twice (`x + x`, `x / ignoring(pod) group_left x`, `x > 0 and x < 10`), the lookup runs twice. ClickHouse already names each IN-set by a hash of its subquery, and the two lookups get the same name. They just don't share, because each selector is planned separately and keeps its own sets.

The branch gives one PromQL evaluation a shared set cache, keyed by that hash. The second selector finds the finished set and skips its lookup.

## Numbers
Dataset: synthetic, 100,000 series over 50 metrics (2,000 each), labels `job` (10 values) and `pod` (unique), 240 samples per series at 15 s. v11 TimeSeries table, default settings. Local `clickhouse server`, `max_threads = 2`, instant query, median of 10 warm runs (cold run dropped). The perf-test rows use the new `tests/performance/promql_repeated_selector.xml` dataset (same shape, timestamps in 1970).

| query (master) | sets built | median | min–max | read_rows | branch |
|---|---|---|---|---|---|
| `metric_1{pod="pod-1"}` | 1 | 21.5 ms | 20–49 | 18,592 | pending build (no change expected) |
| `x + x` (x = the one-series selector) | 2 | 45.0 ms | 42–79 | 37,184 | pending build (~32 ms expected) |
| `x + x + x` | 3 | 67.5 ms | 63–109 | 55,776 | pending build (~41 ms expected) |
| `rate(x[5m]) / rate(x[5m] offset 10m)` | 2 | 49.0 ms | 42–57 | 37,184 | pending build (no change: two windows) |
| `metric_1{job="job3"}` (~200 series) | 1 | 39.0 ms | 36–68 | 22,494 | pending build (no change expected) |
| `y + y` (y = the job selector) | 2 | 80.0 ms | 74–108 | 44,988 | pending build (~67 ms expected) |

Where a selector's time goes (master, a selector's generated SQL run on its own, recent-data dataset):

| one-series selector | median | | ~200-series selector | median |
|---|---|---|---|---|
| whole selector | 26.5 ms | | whole selector | 46 ms |
| tags lookup alone | 14 ms | | tags lookup alone | 18 ms |
| selector with the ids given as a literal (a cache hit, minus planning) | 6 ms | | same | 29.5 ms |
| planning the lookup subquery (still done on a hit) | ~5 ms | | same | ~5 ms |

So a cache hit should save about the lookup's execution, 12–14 ms per repeated occurrence, whatever the width of the selector.

## Why (cause)
Each `timeSeriesSelector` is planned by its own interpreter (`StorageTimeSeriesSelector::readImpl`, `src/Storages/StorageTimeSeriesSelector.cpp:1219`). On v11 tables the samples read filters on `id IN (<tags lookup>)`, and the primary-key analysis builds that set in place (`FutureSetFromSubquery::buildOrderedSetInplace`, `src/Interpreters/PreparedSets.cpp:510`; the trace log shows `column 0 in 1-element set`). Prepared sets are per planner, so two identical selectors build two sets. The trace shows the same key twice: `Creating set, key: __set_6329262942667101475_5779774659040694636`.

ClickHouse has a per-query `PreparedSetsCache` for exactly this ("share sets between tasks"), but only mutations turn it on. Its lookup sits in `CreatingSetsTransform`, which only runs after the subquery has produced its first block. For a selector of a few series that is nearly the whole lookup, so turning the cache on is not enough by itself.

## Proposed fix (branch `lilred/perf-promql-shared-selector-sets`, commit `9f437b64375f`, unbuilt)
- The PromQL entry points (`StoragePrometheusQuery::readImpl` for the `prometheusQuery*` table functions, and the HTTP API) give their query context a `PreparedSetsCache`.
- `buildOrderedSetInplace` first asks the cache for a finished set with the same key (`PreparedSetsCache::findBuilt`, new, never waits). On a hit it adopts that set and runs nothing.
- The PromQL cache never waits for a set still being built (a new constructor option). All of a query's sets are built in one pipeline, and a waiting step could hold the very thread the builder needs. The review found that hang; this option closes it. Mutations keep waiting, as before.
- No cache under `make_distributed_plan`.
- Size: about 60 lines of C++ in 4 files, plus 2 gtest cases, a stateless test, an integration case and a perf test. No setting, no serialized state, no threshold.
- Preflight: three rounds. Round 1 found the hang above (fixed). After round 3 only the build-dependent numbers are open.
- Safe because the key is the hash of the whole lookup subquery (matchers, time window, tables), so only identical lookups share. Different windows (`offset`) get different keys, verified on master. Sharing one finished `Set` between two filters is what the existing cache already does for mutations.
- **Side effect:** mutations also gain the early adoption (a mutation task reuses a set another task of the same mutation built). That is what their cache is for; it is named in the PR text.
- Not covered: the recording-rule `INSERT … SELECT FROM prometheusQueryRange` with parallel replicas, which plans the whole PromQL query twice, each with a new cache. James's `timeseries-whole-metric-probe-once-per-query` notes cover that path. The empty-selector short circuit of James's branch is also not part of this.

Upstream-ready description: `prs/promql-shared-selector-sets.md`. Compare: https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred/perf-promql-shared-selector-sets

## Compared with `jtcunning/promql-empty-selector-short-circuit` (14629dc794)
| | temp-table branch | this branch |
|---|---|---|
| mechanism | new: runs each distinct lookup while the SQL is generated, keeps ids in a temporary table | existing set cache, keyed by the subquery hash ClickHouse already computes |
| single-use selectors | read the temp table instead of the in-place set; ~30% slower on wide selectors (#4 measurements) | unchanged (one hash-map miss) |
| `EXPLAIN` | runs the lookups | runs nothing new |
| empty selector short circuit | yes | no (separate change) |
| size | large, needs a rebase over #117344 | ~60 lines in 4 files, on current master |

## Upstream overlap
- #118801 (nikitamikhaylov, open, last update 2026-10-15): one tags scan per metric through `MaterializedCTE`, with analyzer changes. Broader; it would also cover selectors that differ in labels. Independent of this branch.
- No open PR touches `PreparedSetsCache` for SELECT queries (searched).
- James's `planner-concurrent-key-set-builds` (3e262919fd26) builds independent primary-key sets in parallel. If both land, two identical sets could build at the same time and miss each other. The PromQL cache then builds both, which is today's cost, never a hang. Worth grouping by key in that pass.
- Master since James's branch: `c74867d23cc1` (#122812, HTTP API resolution limit), unrelated.

## How to reproduce
Scripts in research `bench/selector-set-cache/`: `bench.py` (PromQL shapes, set builds from trace logs), `emulate.py` (the cost split above), `perf_xml.py` (the perf-test queries), `config.xml` (server with query/text log at trace).
```
clickhouse server --config-file=config.xml        # 127.0.0.1:19000
# data: CREATE TABLE ts ENGINE = TimeSeries; 4 batches of 25,000 series (see report 2026-10-31-selector-lookup-once.md, with 100k series)
python3 bench.py master 10
python3 emulate.py
# perf test: run its create/fill queries in database perf, then
python3 perf_xml.py master        # and `branch` on a build of the branch
```
Set builds per query: `ProfileEvents['SetsBuiltFromSubquery']` in `system.query_log`, or `Creating set, key:` lines with `send_logs_level = 'trace'`.

**Lab note:** `promql_push_down_label_matchers` copies matchers across a one-to-one binary operator, so `x{a="1"} + x{a="1", b!="z"}` already becomes one lookup on master (both sides become the same selector).
