# [unbuilt] Keep one tags row per time series before the time ranges join of the `TimeSeries` selector

Branch: `lilred-perf-selector-one-tags-row-per-id` @ `e7c771817425` (base `2b1bef1d3b52`) · compare: https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-selector-one-tags-row-per-id · handoff: https://github.com/JTCunning/ClickHouse/issues/18

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above) -->
Related: https://github.com/ClickHouse/ClickHouse/pull/127847

### Changelog category (leave one):
- Experimental Feature

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
While the parts of a `TimeSeries` tags table are not merged yet, a selector now joins and collects one tags row per time series instead of one per part: a selector over 60,000 series written by 30 inserts builds its id set from 60,000 rows instead of 1.8 million and is 19% faster.

### Description

Until its parts are merged, the `ReplacingMergeTree` tags table has a row for a time series in each part written with it, for example when the deduplication cache misses after a restart or a swap of tables. The ids query of the selector joined every such row with the time ranges table and passed each one to `timeSeriesStoreTags` and to the `id IN` set. #127847 made the join `LEFT ANY`, which stopped multiplying these rows by the copies in the time ranges table, but the tags copies still went through: with 31 unmerged parts and 60,000 series, 1.8 million rows went into the join and the set for 60,000 ids.

Now the tags side of the join is `(SELECT id, tags, metric_name, <tag columns> FROM tags WHERE <matchers> ORDER BY id LIMIT 1 BY id) AS tags_subquery`. `ORDER BY id` is the order the merge join needs anyway: for a selector of one metric the tags table is read in that order, so `LIMIT 1 BY id` runs while streaming; for other selectors the rows are sorted, as the join did before. It applies only with the default id generator, a hash of the whole `tags` map, so the dropped rows are equal to the kept one; a custom generator, a table without a time ranges table (version 7 or earlier, or `store_time_ranges = 0`) and a time range covering all timestamps keep the previous query. The time ranges side and the whole-metric probe are unchanged.

`clickhouse` master `f0a9e6bffa71`, 2 vCPU, `max_threads = 2`; the ids query as master builds it vs the same SQL with the subquery, built into an `IN` set, 9 interleaved rounds, median. Fixture: 60,000 series of one metric written by 30 inserts with merges stopped (31 parts per inner table), selector matching all of them:

| | master | this change |
|---|---|---|
| time | 1,820 ms | 1,473 ms (−19%) |
| CPU | 3,343 ms | 2,350 ms (−30%) |
| rows into the join (tags + time ranges) | 1,805,000 + 1,800,000 | 65,000 + 1,800,000 |
| rows into the set | 1,800,000 | 60,000 |

Unchanged (±3%): selectors matching 6,000 and 10 series of the same table, and all three on a merged copy (66, 27 and 20 ms). Master varied by 4% across five runs of the harness. Cost: a `LIMIT 1` over the ids query on the unmerged table (the shape of `/api/v1/series?limit=`) takes 607 instead of 488 ms, because more of the 31 sorted streams are merged before the first row (merge time 92 → 195 ms); on the merged copy it takes 14 instead of 17 ms.

Results are identical to master for every case. The new test `05638_time_series_selector_one_tags_row_per_id` writes three parts per inner table with merges stopped and checks that the id set is built from one row per time series (master: 6 rows for 2 ids), also without a metric name and with `tags_to_columns`, and that a custom id generator keeps every row; dropping the `LIMIT 1 BY` turns the first check red, dropping the generator check the second. No performance test: the case needs unmerged parts, and a CI fill merges them at a pace that differs per run.
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

</details>
