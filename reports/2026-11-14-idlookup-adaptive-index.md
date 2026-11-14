# Text index, statistics and join planning in the id lookup: what pays off
**Verdict:** No rule based on what the planner knows (marks left by the primary key, rows in the surviving parts) can tell when the tags text index pays, so I built no text-index rule. Statistics part pruning of the time ranges table pays in every state; keep it. Join planning does no useful work here: it loads statistics for a 2-table join that can only be swapped. The branch turns it off: −9 to −13% cold, unchanged prewarmed and warm (emulated).
**Task:** `2026-11-13-handoff-use-the-tags-text-index-statis`, https://github.com/JTCunning/ClickHouse/issues/20 · **Build:** master 26.11.1.2253 (`f0a9e6bffa71`) · **Date:** 2026-11-14
**A/B plan:** ab-plans/selector-ids-fixed-join-order.md · **Branch:** `lilred-perf-selector-ids-fixed-join-order` @ `df2da4e4f1c3` ([compare](https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-selector-ids-fixed-join-order), [PR text](../prs/selector-ids-fixed-join-order.md))

## What happens (plain language)
Before the id set of a selector starts to build, the lookup pays three kinds of planning work:
- **Text index.** It reads the header, the dictionary and the postings of every tags part the primary key keeps: three requests in a row per part.
- **Statistics part pruning.** It drops time ranges parts that are outside the time window.
- **Join planning.** It loads the statistics of every column of both tables in every part the primary key keeps, only to decide whether to swap the two sides of the join.

The text index pays only when it drops most parts. That happens when the matcher's value is missing from most fresh parts (a rare pod or instance), and only the index itself can show that. The primary key leaves the same marks for a selector where the index saves 29–80% and for one where it costs 26–41%. Join planning never pays: the merge join over two reads in `id` order has one possible order.

## Numbers
**Lab:** all cells come from the cold S3 lab `bench/tags-in-subquery-cold` (#15), rebuilt for this run with `gen.sh`. The local fixtures `bench/v11-unmerged-idset` were not used, and the warm column is this same lab with everything cached.
- **Data:** an S3 mock with 50 ms per GET, an `s3` disk under a 4 MiB filesystem cache, and 64 fresh parts per inner table (merges stopped). 21,560 series; tags parts of about 760 rows, one granule each.
- **Query:** the ids query as master builds it, captured from the `Will execute query` debug line (`bench/idlookup-adaptive-index/capture.py`), with the selector's settings. Window: the last 3 h of 16 h.
- **Runs:** `max_threads = 16` and `max_threads_min_free_memory_per_thread = 0` (16 streams). `use_query_condition_cache = 0`. Median of 5.

**States:**
- *cold:* every cache dropped and primary keys unloaded.
- *prewarmed:* marks, primary keys and statistics in memory; the filesystem cache and the text index caches dropped. This is James's state after his mark and primary-index prewarm.
- *warm:* nothing dropped.

**Cells:** wall ms (CPU ms / thread time s / planning share). Planning share = the wall time of `EXPLAIN indexes = 1` of the same query in the same state (3 runs), divided by the query's wall time. Thread time is ClickHouse's `RealTimeMicroseconds` summed over threads. Most of it is 16 streams waiting on S3, so CPU is the number to compare.

**Variants:**
- `noidx`: `use_skip_indexes = 0`.
- `thr`: "no text index below a threshold" of marks or rows left by the primary key. Here every tags part keeps 1 granule for every selector, so any threshold that switches the index off switches it off for all of them. The row repeats `noidx`, or `base` for the metric-only selector, which has no tag matcher.
- `nosp`: `use_statistics_for_part_pruning = 0`.
- `nojo`: `query_plan_optimize_join_order_limit = 0` (the branch).
- `nostat`: `use_statistics = 0`.
- `combo`: `noidx` + `nosp` + `nojo`.
- `tagsnostats`: the tags inner table with `auto_statistics_types = ''` (3 runs).

**Text index drops:**
- 3 exact matchers: 31 of 64 parts.
- Rare token: 59 of 64 parts.
- Name regex: 0 of 64 parts.

**Ids are identical** for every selector × variant (count and sum of `cityHash64` over the ids, checked warm).

| selector | variant | cold | prewarmed | warm |
|---|---|---|---|---|
| 3 exact matchers, 397 ids | base | 2,177 (317 / 30.2, 51%) | 1,174 (206 / 12.3, 54%) | 49 (68 / 0.2, 24%) |
| 3 exact matchers, 397 ids | noidx = thr | 2,257 (289 / 31.5, 26%) | **872** (141 / 5.1, 1%) | 53 (74 / 0.2, 15%) |
| 3 exact matchers, 397 ids | nosp | 3,582 (537 / 53.0, 41%) | 1,719 (315 / 15.6, 36%) | 81 (124 / 0.3, 15%) |
| 3 exact matchers, 397 ids | nojo | **1,958** (283 / 27.4, 46%) | 1,170 (204 / 12.4, 54%) | 48 (66 / 0.2, 23%) |
| 3 exact matchers, 397 ids | nostat | 1,960 (284 / 27.5, 46%) | 1,173 (207 / 12.3, 54%) | 47 (66 / 0.2, 23%) |
| 3 exact matchers, 397 ids | combo | 3,209 (397 / 46.8, 14%) | 1,428 (248 / 8.4, 1%) | 95 (148 / 0.4, 11%) |
| 3 exact matchers, 397 ids | tagsnostats | 2,021 (289 / 28.3) | 1,179 (206 / 12.4) | |
| + rare token, 1 id | base | 1,287 (242 / 19.0, 68%) | **671** (130 / 8.5, 73%) | **30** (37 / 0.1, 37%) |
| + rare token, 1 id | noidx = thr | 2,277 (306 / 31.7, 26%) | 866 (127 / 5.1, 1%) | 54 (73 / 0.2, 20%) |
| + rare token, 1 id | nosp | 2,750 (442 / 41.9, 45%) | 1,268 (249 / 12.1, 39%) | 68 (102 / 0.3, 16%) |
| + rare token, 1 id | nojo | **1,165** (212 / 17.6, 65%) | 673 (139 / 8.6, 74%) | 35 (46 / 0.1, 31%) |
| + rare token, 1 id | nostat | 1,156 (209 / 17.6, 66%) | 672 (130 / 8.6, 73%) | 33 (42 / 0.1, 27%) |
| + rare token, 1 id | combo | 3,217 (414 / 46.9, 14%) | 1,430 (237 / 8.4, 1%) | 87 (132 / 0.4, 11%) |
| name regex + matcher, 3,817 ids | base | 3,113 (529 / 39.6, 37%) | 1,537 (343 / 16.1, 42%) | 111 (158 / 0.6, 9%) |
| name regex + matcher, 3,817 ids | noidx = thr | **2,306** (398 / 28.3, 23%) | **909** (226 / 5.4, 1%) | 122 (167 / 0.6, 7%) |
| name regex + matcher, 3,817 ids | nosp | 7,029 (718 / 60.2, 19%) | 2,095 (414 / 19.4, 31%) | 130 (191 / 0.7, 8%) |
| name regex + matcher, 3,817 ids | nojo | 2,813 (453 / 35.1, 31%) | 1,563 (339 / 16.2, 42%) | 112 (158 / 0.6, 8%) |
| name regex + matcher, 3,817 ids | nostat | 2,816 (460 / 35.5, 31%) | 1,566 (340 / 16.2, 42%) | 122 (164 / 0.7, 7%) |
| name regex + matcher, 3,817 ids | combo | 5,798 (505 / 40.7, 4%) | 1,494 (298 / 8.9, 1%) | 137 (193 / 0.7, 6%) |
| name regex + matcher, 3,817 ids | tagsnostats | 2,863 (456 / 35.8) | 1,560 (342 / 16.2) | |
| metric only, 3,172 ids | base = thr | 2,283 (318 / 28.5, 26%) | 881 (146 / 5.2, 1%) | 57 (81 / 0.2, 16%) |
| metric only, 3,172 ids | nosp | 3,714 (547 / 51.4, 25%) | 1,425 (258 / 8.4, 1%) | 94 (145 / 0.4, 11%) |
| metric only, 3,172 ids | nojo | **1,989** (267 / 23.9, 15%) | 879 (147 / 5.2, 1%) | 56 (79 / 0.2, 16%) |
| metric only, 3,172 ids | nostat | 1,982 (264 / 23.8, 15%) | 875 (151 / 5.2, 1%) | 58 (82 / 0.2, 14%) |
| metric only, 3,172 ids | combo | 3,249 (422 / 44.0, 14%) | 1,438 (259 / 8.5, 1%) | 90 (140 / 0.4, 9%) |

**Warm, 15 interleaved rounds** (`warm.py`, `results20-warm15.jsonl`), median ms, base / nojo / noidx:
- 3 exact matchers: 47 / 46 / 54.
- Rare token: 32 / 33 / 49.
- Name regex: 111 / 113 / 111.
- Metric only: 53 / 56 / 57. Its `noidx` plan is the same as `base`, so this row is an A/A pair: the noise is about ±5%.

The 5-round warm column moves by up to ±10% between the same plans.

**Host limits:**
- 2 vCPU. The mock and the server share them, and the 16 streams run on at most 4 threads.
- The parts are tiny (one granule). James's capture has about 2.5 marks per part after the primary key, which raises the data cost per part (see below).
- Ratios between variants transfer to other hosts; absolute times don't.

## Why (cause)

**Text index (ideas 1, 2, 4).** One index granule per part: the analysis drops a whole part when its dictionary lacks the token, and three dependent requests decide that (header → dictionary → postings).
- **Cost per part, from the table:**
  - Planning time with vs without the index, cold: (1,116 − 578) ms / 64 parts = **8.4 ms**.
  - Prewarmed: (630 − 10) / 64 = **9.7 ms**.
  - Warm: under 0.1 ms.
- **What a dropped part saves:** the read of that part's data (marks, data).
  - Cold: (1,679 − 1,061) ms / 31 parts = **19.9 ms**.
  - Prewarmed: (862 − 544) / 31 = **10.3 ms**.
- **Break-even:** so the index pays when it drops more than about **42%** of the parts cold and **94%** prewarmed. Warm it is at least neutral: it drops nothing for the regex selector and costs nothing there (111 vs 111 ms, 15 rounds), because the cached index is cheap.
- **Measured fractions:**
  - 3 exact matchers: 48%. With the index the lookup is 4% faster cold (2,177 vs 2,257 ms) and 35% slower prewarmed (1,174 vs 872).
  - Rare token: 92%. The index wins in every state: 43% faster cold, 23% prewarmed, 35% warm (15 rounds).
  - Name regex: 0%. Without the index the lookup is 26% faster cold and 41% faster prewarmed.
- **Why no rule from the planner's information works:** the primary key leaves 1 granule in each of the 64 parts for all three selectors, so marks left by the primary key and rows in the surviving parts are identical across them. The fraction the index drops depends on how rare the token is per part, and only the index's dictionary knows that.
- **A no-regret rule fails too.** It would skip the index only where the data of the surviving marks costs less than the index (8.4 vs 19.9 ms cold, 9.7 vs 10.3 ms prewarmed), so here it keeps the index everywhere. It never fires, so it removes none of the measured losses.
- **Size rule against the limits.** A size rule (idea 2) equals `noidx` on fresh parts. On the wide selector it wins cold and prewarmed, but warm it is 0–10% slower, which is outside the ≤3% limit on the 5-round column. On the rare-token selector it is 29–80% slower.
- **James's capture.** The index dropped 12 of 62 and 14 of 102 marks, a fraction of about 0.15–0.2. That is under both break-evens, so on parts whose index isn't cached it cost more than it saved. Its time (129 and 38 ms summed over 30 threads) is small in wall time, though. On that replica most index files were probably cached, where the index is cheap.

**Statistics (idea 3).** In this 2-table `LEFT ANY JOIN`, the planner uses statistics for two things:
1. **Part pruning of the time ranges table** by `min_time` / `max_time` (`use_statistics_for_part_pruning`). It keeps 13 of 64 parts for a 3 h window. Off, the lookup is 17–127% slower in every state (`nosp`), because the time ranges side then reads all 64 parts. It must stay on. Its loads are already bounded by `min_marks_per_part_to_load_statistics` (#126627), so cold it loads nothing for these small reads.
2. **The join estimate.** The join order optimization runs for `ANY` joins as a swap-only case (`optimizeJoin.cpp`, `isSwapOnlyJoinStrictness`; the graph is capped at 2 relations). To estimate both inputs, `RelationStatisticsEstimator` calls `ReadFromMergeTree::getConditionSelectivityEstimator(getAllColumnNames(), analyzed_result)`. That loads the statistics of every column it reads, in every part the primary key keeps, and has **no** `min_marks_per_part_to_load_statistics` bound.
   - Cold that is 92 statistics (3 matchers) and 154 (regex), one GET per part and table: 46 of the 376 GETs.
   - All it can decide is a swap. On master it does swap: `EXPLAIN` shows `Type: right`, with the time ranges table as the left input. A merge join of two reads in `id` order gains nothing from that.
   - `nojo` and `nostat` remove exactly this cost: 10% cold for 3 matchers, 9% rare token, 10% regex, 13% metric only. Prewarmed and warm (statistics in memory), the change is within ±2%.
   - The tags table's statistics serve only this estimate, so `auto_statistics_types = ''` on the tags table recovers most of it (`tagsnostats`: −7% cold, 0 prewarmed).

This query shape can skip them: the join order is fixed by design, so there is nothing to estimate.

**Join planning:**
- `query_plan_optimize_join_order_limit = 0` keeps the join as written (`Type: left`). The pipeline is otherwise the same processor for processor: in-order reads, `MergingSortedTransform`, `MergeJoinTransform`.
- `query_plan_optimize_join_order_algorithm` doesn't matter for 2 relations.
- With `query_plan_optimize_join_order_limit = 0`, the only other effect is that hash joins get no size estimate (`parallel_hash_join_threshold`), and this query uses `full_sorting_merge`.

**The 0.57 / 0.67 s gap before set creation: reproduced and attributed.** `gap.py` runs the full PromQL query with trace logs and the real-time profiler (5 ms) and symbolizes with the master binary. Narrow selector:

| state | ids query planning ends | `Creating set` | gap |
|---|---|---|---|
| cold | 1,153 ms | 2,214 ms | **1,060 ms** |
| prewarmed | 639 ms | 1,186 ms | **547 ms** |
| warm | 25 ms | 58 ms | 32 ms |

- **What the gap is:** the execution of the ids subquery until its first block. `CreatingSetsTransform` logs `Creating set` when its first input chunk arrives (`init` → `startSubquery`). For a set that fits in one block, `Created Set` follows within 0.1 ms. Before the in-order merge join can emit a row, every tags part and every time ranges part must deliver its first granule.
- **Cold profile:** 51% of the gap's samples are in `MergeTreeMarksLoader::loadMarks` / `loadMarksSync`, the per-part marks loads on the executor threads. 31% are S3 responses (`receiveHTTPResponseImpl`).
- **Prewarmed profile:** marks are in memory, and 62% of the samples are S3 data responses.
- **Logs:** the gap has no debug-level lines. At trace level it starts with `Spreading ranges among streams with order` and one `Reading N ranges in order from part …` line per part.
- **James's capture:** his 0.57 / 0.67 s match the prewarmed case: about 25 tags + 90 time ranges parts, each fetching its first data granule.
- **Checks that would confirm it there:**
  - text_log at trace level: the `Reading … in order from part` lines right after planning;
  - the statement's `WaitMarksLoadMicroseconds`, `ReadBufferFromS3Microseconds` and `CachedReadBufferReadFromSourceMicroseconds`;
  - a `trace_log` with matching symbols, which would show `loadMarks` and `ReadBufferFromS3`.
- **What changes it:** fewer parts per in-order merge (#19's time ranges cache), not planning. The branch moves planning 214 ms earlier cold and leaves the gap as it is (1,066 ms; `gap-out/nc-cold-nojo.txt`).

## Proposed fix
**Branch `lilred-perf-selector-ids-fixed-join-order`** (1 commit, +5/−1 in `src/`, test `05640`):
- **What it changes:** `applySettingsForSelectIDsQuery` also sets `query_plan_optimize_join_order_limit = 0`. It is one mechanism with no threshold, and the settings stay in one place (the issue's constraint).
- **What it keeps:** the join algorithm, the analyzer, the read pools, the on-disk format, statistics part pruning and the text index.
- **Effect** (emulated by the same setting on master):
  - Cold: −219 ms (3 matchers), −122 (rare token), −300 (regex), −294 (metric only).
  - Prewarmed: +1.7% wide and −0.3% narrow. Warm: +1.8% wide over 15 rounds, inside the ±5% A/A spread.
  - Wide CPU: 529 → 453 ms cold and 158 → 158 warm. That is within the limits (≤3% wall, ≤1.3× CPU).
- **The test:** it pins the results of four selector forms over three unmerged parts and the absence of a join order optimization. Without the setting, its four `0` lines become `1`, as on master.
- **Preflight** (1 round, the bot's instructions): ⚠️ only for build evidence; no code findings. Its two nits are fixed. It also confirmed that the setting doesn't leak:
  - the samples read and its wrappers have no join;
  - the PromQL operator joins are planned with the outer query's settings;
  - the HTTP API's only joins are the ids joins.
- **With #18's branch** (`lilred-perf-selector-one-tags-row-per-id` @ 952df5b41e35):
  - The two merge without conflicts (`git merge-tree`).
  - Its `LIMIT 1 BY id` subquery still feeds the same 2-table join, so the setting removes the same estimate there.
  - Its `use_skip_indexes_on_data_read = 0` keeps the text index in the up-front analysis, which is the regime measured here, so the text-index conclusions apply to it unchanged.
- **With #15's branch** (`lilred-perf-parallel-statistics-probe`): it parallelizes the samples table's `PREWHERE` statistics probe, a different load. Both cut cold planning, and they add up.
- **The generic fix,** for upstream to decide: bound the join estimate's statistics load by `min_marks_per_part_to_load_statistics` in `RelationStatisticsEstimator` for every join.

**What a text-index fix would need.** It must not pay three dependent requests per fresh part before it knows whether the token is there. Either of these fits:
- a text-index prewarm on insert and fetch, like `prewarm_mark_cache`, so fresh parts are always in the warm case, where the index pays or is neutral;
- one ranged read for header, dictionary and postings of small parts: 430 KB for all 64 parts here, which would cut its cold and prewarmed cost about 3×.

Both are MergeTree text-index changes for the index owners, not a selector rule. Effort M.

**Settings James can apply now:**
1. **`query_plan_optimize_join_order_limit = 0`** in the settings profile of the user that runs PromQL and the recording rules, or per query. Master doesn't override it inside the selector, so this is the branch's effect today (−9 to −13% of a cold lookup). PromQL's own operator joins are 2-table joins too (#14: about 1 ms), so they lose nothing.
2. Alternatively, **`prewarm_statistics_cache = 1`** on the tags and time ranges inner tables. With statistics in memory, the join estimate costs nothing (prewarmed column: `nojo` = `base`).
3. **Keep `use_statistics_for_part_pruning = 1`**, and keep statistics on the time ranges table: they are what drops out-of-window parts.
4. **Text index:** no blanket switch. Keep the text index caches (`text_index_header_cache_size`, the dictionary/tokens and postings caches, 1–2 GiB by default) large enough for the fresh parts.
   - `exclude_materialize_skip_indexes_on_insert = 'tags_idx'` (#15's config 3) helps only if the rules rarely match rare values (pods, instances).
   - It equals `noidx` on fresh parts: −26 to −41% for common-value selectors prewarmed, +29 to +77% for rare-value ones.

## Upstream overlap
- **Code and PRs:**
  - Nothing open changes `applySettingsForSelectIDsQuery` or the selector's join planning.
  - Recent commits on the selector: #127847 (ANY join, merged), 17128b8ca6c9, e61d17428ed9.
  - The text index reader and analyzer only changed in unrelated merges.
  - `joinOrder.cpp`: cost-model work by Fisnik Kastrati (DPsub interesting orders, merge pricing); none of it touches swap-only joins.
- **Statistics:**
  - #126627 (merged): the `min_marks_per_part_to_load_statistics` probe, which this lookup's estimate path bypasses.
  - #131129 (open): computes the probe once per read.
  - #126621 (open): reads all statistics of a part at once.
  - #116348 (open): reuses part statistics within a query.
  - All four make loading cheaper; none skips the join estimate.
- **James's fork:**
  - `jtcunning/promql-planner-speedup` (a whole-metric probe cache plus `MergeTreeSelectRangesToReadMicroseconds`) is complementary.
  - `jtcunning/timeseries-prewarm-mark-cache` covers marks, not statistics or the text index.

## How to reproduce
```bash
D=tools/clickhouse/data/tags-in-subquery-cold; B=research/bench
python3 $B/tags-in-subquery-cold/s3mock.py --root $D/s3root --port 9100 --log $D/get.jsonl &
clickhouse server --config-file=$D/server/config.xml &          # merges resume on restart: rebuild with gen.sh after every start
echo 0 > $D/s3root/.latency_ms; OUT=$D bash $B/tags-in-subquery-cold/gen.sh
python3 $B/idlookup-adaptive-index/capture.py 9301 ts <T_END-10800> <T_END> 'nc=metric_0{label_a="v1",label_b="v1",label_c="v1"}' \
  'nr=metric_0{label_a="v1",label_b="v1",label_c="v1",instance="host-3:9100"}' 'w={__name__=~"metric_.*",namespace="ns3"}' 'lf=metric_0'
bash $B/idlookup-adaptive-index/matrix-cold.sh                  # ~40 min; then summarize20.py $D/results20.jsonl [--md]
python3 $B/idlookup-adaptive-index/warm.py 9301 warm 15 $B/idlookup-adaptive-index/q nc,nr,w,lf base,nojo,nostat,qcc,noidx
python3 $B/idlookup-adaptive-index/gap.py cold|meta|warm '<selector>' [k=v,...]   # timeline + profiler per phase
```
Raw results: `$D/results20.jsonl`, `results20-warm15.jsonl` (copied to `bench/idlookup-adaptive-index/`), and `gap-out/*.txt`.
