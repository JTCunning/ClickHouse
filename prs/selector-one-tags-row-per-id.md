# [unbuilt] Keep one tags row per time series before the time ranges join of the `TimeSeries` selector

Branch: `lilred-perf-selector-one-tags-row-per-id` @ `952df5b41e35` (base `2b1bef1d3b52`; 2 commits) · compare: https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-selector-one-tags-row-per-id · handoff: https://github.com/JTCunning/ClickHouse/issues/18

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above) -->
Related: https://github.com/ClickHouse/ClickHouse/pull/127847

### Changelog category (leave one):
- Experimental Feature

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
While the parts of a `TimeSeries` tags table are not merged yet, a selector now joins and collects one tags row per time series instead of one per part: a selector over 60,000 series written by 30 inserts builds its id set from 60,000 rows instead of 1.8 million and is about 20% faster.

### Description

Until its parts are merged, the `ReplacingMergeTree` tags table has a row for a time series in each part written with it, for example when the deduplication cache misses after a restart or a swap of tables. The ids query of the selector joined every such row with the time ranges table and passed each one to `timeSeriesStoreTags` and to the `id IN` set. #127847 made the join `LEFT ANY`, which stopped multiplying these rows by the copies in the time ranges table, but the tags copies still went through: with 31 unmerged parts and 60,000 series, 1.8 million rows went into the join and the set for 60,000 ids.

Now the tags side of the join is `(SELECT id, tags, metric_name, <tag columns> FROM tags WHERE <matchers> ORDER BY id LIMIT 1 BY id SETTINGS use_skip_indexes_on_data_read = 0) AS tags_subquery`. `ORDER BY id` is the order the merge join needs anyway: for a selector of one metric the tags table is read in that order, so `LIMIT 1 BY id` runs while streaming; for other selectors the rows are sorted, as the join did before. It applies only with the default id generator, a hash of the whole `tags` map, so the dropped rows are equal to the kept one; a custom generator, a table without a time ranges table (version 7 or earlier, or `store_time_ranges = 0`) and a time range covering all timestamps keep the previous query. The time ranges side and the whole-metric probe are unchanged.

The setting keeps the skip indexes of the tags read in the index analysis. A read without a join applies them while reading, and the read in `id` order keeps one stream per part even when the text index on `tags` drops all of its granules; the tags table read by the joining query never did that (`ReadFromMergeTree::supportsSkipIndexesOnDataRead` turns the read-time path off under a join). Without it, on 93 small tags parts of which the index keeps 3, the subquery read 93 streams (`SelectedParts` 186 instead of 96, same rows) and took 38 instead of 28 ms, with the query condition cache off; with the cache on (the default) that happens on the first run of a selector and for parts no query has seen yet, such as new parts of a table being written. Dropping the stream of a part whose granules are all dropped at read time would fix this for any in-order read; that is a separate change.

`clickhouse` master `f0a9e6bffa71`, 2 vCPU, `max_threads = 2`; the ids query as master builds it vs the same SQL with the subquery, built into an `IN` set, 9 interleaved rounds, median; every row runs the final SQL except the `LIMIT 1` one (first commit's SQL; on this table the text index keeps every part but one, so the setting does not change that plan). Fixture: 60,000 series of one metric written by 30 inserts with merges stopped (31 parts per inner table), selector matching all of them:

| | master | this change |
|---|---|---|
| time | 1,881 ms | 1,476 ms (−22%) |
| CPU | 3,393 ms | 2,390 ms (−30%) |
| rows into the join (tags + time ranges) | 1,805,000 + 1,800,000 | 65,000 + 1,800,000 |
| rows into the set | 1,800,000 | 60,000 |

Unchanged or faster: selectors matching 6,000 and 10 series of the same table (878 → 821, 610 → 615 ms), all three on a merged copy (65 / 27 / 20 → 64 / 28 / 20 ms), and all three on 93 small parts written by a steady insert loop (358 / 239 / 208 → 360 / 240 / 207 ms; 21 rounds for the last). Master varied by 4% across five runs of the harness. Cost: a `LIMIT 1` over the ids query on the unmerged table (the shape of `/api/v1/series?limit=`) takes 607 instead of 488 ms, because more of the 31 sorted streams are merged before the first row (merge time 92 → 195 ms); on the merged copy it takes 14 instead of 17 ms.

Results are identical to master for every case. The new test `05638_time_series_selector_one_tags_row_per_id` writes three parts per inner table with merges stopped and checks that the id set is built from one row per time series (master: 6 rows for 2 ids), also without a metric name and with `tags_to_columns`, and that a custom id generator keeps every row; a last check writes three parts with one series each and expects no read-time index lines for a selector of one of them. Dropping the `LIMIT 1 BY` turns the first check red, dropping the generator check the second, dropping the `SETTINGS` the last; master passes the last. No performance test: the case needs unmerged parts, and a CI fill merges them at a pace that differs per run.
<!-- upstream:end -->

<details><summary>Full case tables, emulation method and self-review (not for upstream)</summary>

**Branch column is emulated, pending build.** No build on this machine; the branch column is the rewritten ids query run on master (`f0a9e6bffa71`). Lab: `research/bench/v11-unmerged-idset/` (`gen.sh`, `idsq.py`, `bench.py`); report `reports/2026-11-13-selector-one-tags-row-per-id.md`; A/B plan `ab-plans/selector-one-tags-row-per-id.md`.

Variant `any_none_none` = master, `any_limitbyord_none` = this change (`results-run5-final*.jsonl`):

| case | table | master ms | change ms | rows into set | join input rows |
|---|---|---|---|---|---|
| all 60k series | unmerged | 1,820 (1,791–1,884) | 1,473 (1,422–1,493) | 1,800,000 → 60,000 | 3,605,000 → 1,865,000 |
| 6k series | unmerged | 860 | 822 | 180,000 → 6,000 | 1,980,500 → 1,806,500 |
| 10 series | unmerged | 599 | 595 | 300 → 10 | 1,439,298 → 1,439,008 |
| all 60k series | merged | 66 | 65 | 60,000 | 125,000 |
| 6k series | merged | 27 | 27 | 6,000 | 66,500 |
| 10 series | merged | 20 | 19 | 10 | 60,010 |
| LIMIT 1, all | unmerged | 488 | 607 | | |
| LIMIT 1, 6k | unmerged | 314 | 470 | | |
| LIMIT 1, 10 | unmerged | 257 | 326 | | |
| LIMIT 1, all / 6k / 10 | merged | 17 / 13 / 13 | 14 / 16 / 14 | | |

Rejected collapses (same harness, wide case unmerged / merged): hash `GROUP BY id` both sides 999 / 376 ms (merged +490%); `DISTINCT` in order 1,672 / 75 (+19% merged: it hashes the tags map of every row); `LIMIT 1 BY id, metric_name, tags` 1,516 / 76; newest part first (`ORDER BY id, _part_index DESC LIMIT 1 BY id`) 1,542 / 74; `FINAL` 1,087 wide but 962 vs 617 narrow (filters after the merge, no text index); right-side `DISTINCT id` in order: rows into the join −1.74M, time ±3%.

Commands: `research/bench/v11-unmerged-idset/README.md`.

Self-review (preflight, review bot rules at master):
- R1 ❌ Block: the tags subquery had no alias, so `joined_subquery_requires_alias = 1` (default) would reject every ids query on the new path (the emulation had the alias). Fixed: `AS tags_subquery`. Also fixed: "read in the order of `id`" holds only for a selector of one metric (reworded in code and text); the test now pins `--insert_deduplicate 0` (identical tags blocks under a Replicated database) and covers `tags_to_columns`. Not done: an `/api/v1/series` check (needs a server with the Prometheus handler, not a stateless test; the API re-parses the formatted query, which the alias fix covers). The `LIMIT 1` cost is disclosed with its attribution.
- R2 ✅ Approve, nits applied: the comment on `isDefaultIDGenerator` says rows of an earlier `id_generator` (versions 9 or earlier can change it) are collapsed as a merge would; the text names the tables without a time ranges table; the test adds a selector without a metric name (sorting plan, no id ranges). Remaining gate: build the branch, run 05638 (its reference was edited by hand from master's output) and fill the branch column.

Re-check 2026-11-14 (#19's side finding: 186 `SelectedParts` against master's 95 on 93 small tags parts). Cause: the subquery has no join, so `ReadFromMergeTree::supportsSkipIndexesOnDataRead` let it apply the text index while reading; the in-order read then opened a stream for all 93 parts and the index dropped the single granule of 90 of them inside the read (93 `MergeTreeSelect` sources in `processors_profile_log`, 93 `Index tags_idx has dropped 1/1 granules in part …` lines). Master's tags read sits under the join, so the index runs in the analysis (3 parts). `EXPLAIN indexes = 1` / `EXPLAIN PIPELINE` show 3 parts and `× 3` for both: EXPLAIN reports the analysis, not the read. Same rows (`SelectedRows`, `read_rows` 644,897 for both), so it is per-stream overhead only. Fix commit `952df5b41e35`: `SETTINGS use_skip_indexes_on_data_read = 0` on the subquery (checked on master: it applies to that subquery only, not to sibling or outer reads). Numbers (master / first commit / fix, median, `use_query_condition_cache = 0` as in the lab):
- 93 parts (`steady.sh s_base:base`), lookup: narrow 208 / 214 / 207 ms (21 rounds), mid 239 / 240 / 240, wide 358 / 402 (342–478) / 360 (9 rounds); parts 96 / 186 / 96 narrow, 116 / 186 / 116 mid, 186 all wide. Tags subquery alone: narrow 38 → 28 ms (93 → 3 parts), mid 47 → 40 ms (93 → 23).
- With the query condition cache on (the default) the cache prunes those parts after a selector's first run: narrow 206 / 211 / 205, mid 232 / 227 / 239, 96 / 116 parts for all three. So the cost was a first-run / unseen-part / cache-off cost of about 10 ms at 93 parts.
- 31 parts (`gen.sh`): wide 1,881 / 1,514 / 1,476, mid 878 / 816 / 821, narrow 610 / 606 / 615 (9 rounds); `ts_merged` 65 / 64 / 64, 27 / 28 / 28, 20 / 21 / 20 (11 rounds). Results identical (ids 10 / 6,000 / 60,000).
- R3 (preflight on the whole PR) ✅ Approve with nits: settings constraints drop a nested `SETTINGS` silently (`readonly = 1`, measured: the query runs and the read-time index lines come back), so the Prometheus HTTP API under a `readonly = 1` profile falls back to the first commit's read path; kept as a limitation (perf only, about 10 ms at 93 parts, and the condition cache prunes after the first run) rather than a second override in `PrometheusHTTPProtocolAPI.cpp` (two lines `query_context->setSetting("use_skip_indexes_on_data_read", false)` if wanted). The comment on rows of an earlier generator reworded. R4 ✅ Approve with comment nits (ids written explicitly also share the comment's case; call-site comment shape), applied. R4 also asks to confirm 05638 in the ParallelReplicas and DatabaseReplicated jobs (add `no-parallel-replicas, no-replicated-database` if they differ).

</details>
