# Collapse duplicate tags rows before the time ranges join (handoff #18)
**Verdict:** worth fixing, and partly fixed upstream already: #127847 (merged 2026-11-12) made the join `ANY`, which removed the product with the time ranges copies; the tags copies still reach the join and the id set. Branch `lilred-perf-selector-one-tags-row-per-id` keeps one tags row per series before the join: on 30 unmerged copies, rows into the set 1.8M → 60k and the lookup −19% (emulated), no change on merged tables.
**Task:** 2026-11-13-handoff-collapse-duplicate-tags-rows-be, https://github.com/JTCunning/ClickHouse/issues/18 · **Build:** 26.11.1.2253, `f0a9e6bffa71` · **Date:** 2026-11-13
**A/B plan:** ab-plans/selector-one-tags-row-per-id.md · **PR text:** prs/selector-one-tags-row-per-id.md · **Lab:** bench/v11-unmerged-idset/

## What happens (plain language)
The tags table is a `ReplacingMergeTree`. Until merges catch up, a series written by N inserts has N identical rows. The id lookup of a selector joins the tags rows to the time ranges table and inserts every surviving row into the `id IN` set, so each copy is joined, labelled (`timeSeriesStoreTags`) and hashed into the set. Your observation was made before #127847: with `ALL` the join also multiplied by the time ranges copies (2.81M rows for 56,754 ids). With `ANY` (master now) the join emits one row per tags row: 1.76M in your case. The branch drops the copies right after the read, in id order, so the join and the set see one row per series.

## Numbers
Dataset: `ts` = one metric with 60,000 live series written by 30 inserts with merges stopped (31 parts per inner table; tags 3.155M rows), plus 5,000 stale series and 9 other metrics of 5,000; `ts_merged` = the same rows merged (1 part). Lookup = the ids query built into an `IN` set, `max_threads = 2`, 9 interleaved rounds (11 on `ts_merged`), median (min–max).

| case | master (ANY) | branch (emulated) | rows into join (tags + time ranges) | rows into set |
|---|---|---|---|---|
| all 60k series, unmerged | 1,820 ms (1,791–1,884) | 1,473 ms (1,422–1,493), CPU −30%, memory 406 → 357 MiB | 1,805,000 + 1,800,000 → 65,000 + 1,800,000 | 1,800,000 → 60,000 |
| 6k series, unmerged | 860 | 822 | 180,500 + 1.8M → 6,500 + 1.8M | 180,000 → 6,000 |
| 10 series, unmerged | 599 | 595 | 300 + 1.44M → 10 + 1.44M | 300 → 10 |
| all / 6k / 10, merged | 66 / 27 / 20 | 65 / 27 / 19 | unchanged | unchanged |
| `LIMIT 1` (the `/api/v1/series?limit=` shape), unmerged all / 6k / 10 | 488 / 314 / 257 | 607 / 470 / 326 | | |
| `LIMIT 1`, merged all / 6k / 10 | 17 / 13 / 13 | 14 / 16 / 14 | | |
| before #127847 (`ALL`), all 60k, unmerged, 1 run | 28.2 s | | 54,000,000 out of the join | 54,000,000 |

Master varied 1,818–1,895 ms on the wide case over five harness runs (4%).

Which collapse (the issue's question): `ANY` alone is what master has. A right-side collapse adds nothing in time once the join is `ANY`: in-order `DISTINCT id` on the time ranges side cuts the join's right input from 1.8M to 60k rows but moves the lookup ±3%. Right-side `GROUP BY id` alone with `ALL` (1,847 ms) equals `ANY` (1,847 ms). So the simple fix is `ANY` (done) plus the left side. Left-side options, wide unmerged / wide merged:
- `ORDER BY id LIMIT 1 BY id` (chosen): 1,473 / 65 ms.
- `DISTINCT` in order: 1,672 / 75 (it hashes every row's tags map; +19% on merged).
- `LIMIT 1 BY id, metric_name, tags` (exact, keeps master's error on different maps): 1,516 / 76.
- newest part first (`ORDER BY id, _part_index DESC LIMIT 1 BY id`): 1,542 / 74.
- hash `GROUP BY id` both sides 999 / 376; hash `DISTINCT` both 986 / 133; `FINAL` 1,087 wide but 962 vs 617 ms narrow.

The hash variants are fast on unmerged parts only because they skip the k-way merge of the in-order read (see below); on merged tables they are 2–6× slower and they stop a `LIMIT` from ending the read early.

## Why (cause)
Processor time of the wide lookup on master (summed over threads, per query): merging the sorted part streams 1,498 ms, reading 917 ms, expressions 830 ms (mostly `timeSeriesStoreTags` on 1.8M rows, ~460 ns per row), merge join 197 ms, filter 171 ms, set 27 ms. The branch removes the per-copy part: expressions 830 → 60 ms, join 197 → 39 ms, for `LIMIT BY` 83 ms. The merge of 31 + 30 sorted streams stays; it is the in-order read cost of #19, and the bigger lever on unmerged tables.
Code: `src/Storages/StorageTimeSeriesSelector.cpp`, `makeSelectQueryFromTagsTable` builds the tags side; `addTimeRangesTableJoin` (unchanged, `JoinStrictness::Any` since #127847); `applySettingsForSelectIDsQuery` pins `full_sorting_merge` and `optimize_read_in_order`.

## Proposed fix
Branch: the tags side becomes `(SELECT id, tags, metric_name, <tag columns> FROM tags WHERE <matchers> ORDER BY id LIMIT 1 BY id) AS tags_subquery`. `timeSeriesStoreTags` stays in the outer SELECT after `matched = 1`. For a selector of one metric the read is already in id order, so the pipeline is per-part `LimitBySortedStream` → merge → `LimitBySortedStream` → merge join with no extra sort; other selectors sort, as the join did before.
- **Different maps for one id ("newest row wins").** With the default id generator the id is a hash of the whole `tags` map, so copies are equal by construction. A different map under one id needs a custom generator (`TAGS INNER COLUMNS (id ... DEFAULT ...)` or `id_generator` on version 9 or earlier). There master fails (`Cannot add identifier ... because it was added before with other tags`, tested on master); plain `LIMIT 1 BY id` would keep an arbitrary copy (it changed with `max_threads` in my test: oldest at 1 thread, others at 2 and 4). So the branch collapses only when the generator is the default one (tree-hash compare); custom generators keep master's query. Keeping the newest copy is possible (`_part_index DESC`, matches `FINAL` in the test) but costs 12% on merged tables and ties the query to MergeTree virtual columns.
- **The `LIMIT 1` probe.** The whole-metric probe builds its own query (`SELECT id FROM tags [ANY LEFT JOIN] ... LIMIT 1`) and does not get the collapse; it is unchanged. The other `LIMIT` consumer is `/api/v1/series?limit=`. On merged tables it still stops early (`LIMIT 1` 17 → 14 ms, read rows 73,728 → 65,536); on the unmerged table it is slower (488 → 607 ms), because more of the sorted streams are merged before the first row.
- Effort S; one function and one call-site parameter; test `05638`. Category Experimental Feature. Preflight 2 rounds: R1 Block (the subquery needed an alias under `joined_subquery_requires_alias`; fixed), R2 Approve.

## Upstream overlap
- #127847 (valerypetrov, merged 2026-11-12): `LEFT ANY JOIN`, the right-side half of this handoff. Master binary refreshed to include it.
- No open upstream PR collapses the tags side (searched comp-promql and "timeSeriesStoreTags", "TimeSeries selector join", "tags duplicate"). #118801 (CTE reuse for selectors, open) and #130402 (shared selector sets, yours) are orthogonal.
- James's fork branches: none touch this join (checked the 16 branches updated since 2026-10-25).

## How to reproduce
`bench/v11-unmerged-idset/README.md`: start the server, `gen.sh`, then `python3 bench.py 9 out.jsonl any_none_none,any_limitbyord_none` (`TABLE=ts_merged`, `MODE=limit1` for the guards).
