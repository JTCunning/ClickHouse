# [unbuilt] Do not optimize the join order of the `TimeSeries` selector's ids query

Branch: `lilred-perf-selector-ids-fixed-join-order` @ `df2da4e4f1c3` (base `923c77c86030`; 1 commit) · compare: https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-selector-ids-fixed-join-order · handoff: https://github.com/JTCunning/ClickHouse/issues/20

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above) -->
Related: https://github.com/ClickHouse/ClickHouse/pull/126627

### Changelog category (leave one):
- Experimental Feature

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
The id lookup of a `TimeSeries` selector no longer loads the column statistics of the tags and time ranges tables to plan its join, which can't be reordered. With 64 fresh parts on object storage and nothing cached, the lookup is 9-13% faster.

### Description

The ids query of the selector is `tags LEFT ANY JOIN (time ranges subquery) USING (id)`, a merge join over both tables read in `id` order (`applySettingsForSelectIDsQuery`). The join order optimization still runs for it (the swap-only case of `ANY`). To estimate both inputs, `RelationStatisticsEstimator` calls `ReadFromMergeTree::getConditionSelectivityEstimator`, which loads the statistics of every column it reads in every part the primary key keeps, without the bound of `min_marks_per_part_to_load_statistics` (#126627). When they aren't cached, that is one request per part and table, only to decide a swap the merge join doesn't need.

Now `applySettingsForSelectIDsQuery` sets `query_plan_optimize_join_order_limit = 0`: the join runs as written. Unchanged: statistics part pruning of the time ranges table (51 of 64 parts dropped below), the `PREWHERE` statistics probe, the text index, the join algorithm and the read order. `/api/v1/series` and `/api/v1/labels` share these settings.

Master `f0a9e6bffa71`, 2 vCPU, `max_threads = 16`, the ids query as master builds it; S3 endpoint with 50 ms per GET under a 4 MiB filesystem cache; 64 fresh parts per inner table, 21,560 series; cold = nothing cached, prewarmed = marks, primary keys and statistics in memory; median of 5; branch column pending build (in brackets: master with the setting):

| selector (ids) | cold, master | cold, branch | prewarmed, master | prewarmed, branch |
|---|---|---|---|---|
| 3 exact matchers (397) | 2,177 ms | pending (1,958) | 1,174 ms | pending (1,170) |
| 4 exact matchers, rare token (1) | 1,287 ms | pending (1,165) | 671 ms | pending (673) |
| name regex + matcher (3,817) | 3,113 ms | pending (2,813) | 1,537 ms | pending (1,563) |
| metric name only (3,172) | 2,283 ms | pending (1,989) | 881 ms | pending (879) |

Cold, `ProfileEvents['LoadedStatistics']` per lookup goes 92 → 0 (3 matchers) and 154 → 0 (regex), all from the join estimate (the `PREWHERE` probe loads none for these small reads), and S3 GETs 376 → 330. Warm (15 interleaved rounds) nothing moves beyond the ±5% A/A spread (regex 111 → 113 ms).

Ids are identical for the four selectors. Test `05640_…_fixed_join_order` pins the results of four selector forms over three unmerged parts and checks that no join order optimization runs: dropping the setting turns its four `join order optimizations: 0` lines into `1`, as on master.

A generic fix would apply `min_marks_per_part_to_load_statistics` to the join estimate of every join; this one is local because only the selector knows the order is fixed. #131129 and #126621 make statistics loading cheaper; this removes it here.
<!-- upstream:end -->

<details><summary>Full case tables, emulation method and self-review (not for upstream)</summary>

**Branch column is emulated, pending build.** No build on this machine. The branch column is master (`f0a9e6bffa71`) with `query_plan_optimize_join_order_limit = 0` on the ids query, the setting the branch adds. Lab: `research/bench/tags-in-subquery-cold/` (S3 mock, server, `run.py`, `gen.sh`) and `research/bench/idlookup-adaptive-index/` (`capture.py` captures the ids query from the debug log, `matrix-cold.sh` runs it, `summarize20.py` prints the medians). Raw: `results20.jsonl` (cold/prewarmed/warm × 6 variants) and `results20-warm15.jsonl` (15 interleaved warm rounds).

Full table: `reports/2026-11-14-idlookup-adaptive-index.md`.

Build A/B: `ab-plans/selector-ids-fixed-join-order.md`.

Self-review (preflight R1, independent agent with the bot's review instructions): ⚠️ Request changes on evidence only (build the branch, run 05640, fill the branch column); no code findings. Nits fixed: the header comment now says the setting covers the plans optimized with the selector's context (the in-place `IN` set of the samples read, the HTTP API queries); the `LoadedStatistics` counter is named. Noted by the reviewer and true: on master `swap_on_sizes` (or CI's `query_plan_optimize_join_order_randomize`) could flip the join to `RIGHT ANY`; now it always runs as written. Not traced: the ParallelReplicas job (the sibling test 05452 uses the same debug-log grep without a `no-parallel-replicas` tag).
</details>
