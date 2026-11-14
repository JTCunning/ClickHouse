# Tags lookup through an id-only projection: costs more than the write it skips
**Verdict:** Not worth building as specified. On master the proposed lookup of 17,000 missed ids costs 0.21 s at 300k series and 1.37 s at 3M, against 0.03 s to write the same 17,000 tags rows. The projection also makes every new tags row about 3× slower to write and keeps its primary index in RAM (72 MiB per 3M rows). The production figures fit a deduplication cache that is full at its default size, and raising that size is a per-table setting. Measure that first; no branch until a production number shows the tags write is the cost.
**Task:** `2026-11-13-handoff-tags-id-projection-lookup`, https://github.com/JTCunning/ClickHouse/issues/21 · **Build:** master 26.11.1.2253 (`f0a9e6bffa71`), `clickhouse local` · **Date:** 2026-11-14
**A/B plan:** ab-plans/tags-id-projection-lookup.md · **Branch:** none (report only, decision for James)

## What happens (plain language)

The proposal adds a projection `tags_by_id (SELECT id ORDER BY id)` with one row per mark to the inner tags table. After a cache miss, the insert asks that projection which missed ids are already stored. The planner does read the projection with about one or two rows per id. But each answer costs far more than writing the row again. The planner checks the 17,000-id set against hundreds of thousands of one-row marks (about 37 µs per id at 3M rows). Then it decompresses one block per row it reads. Writing a tags row costs about 2 µs here.

## Numbers

Dataset: the v11 inner tags table exactly as master generates it (`ReplacingMergeTree`, `ORDER BY (metric_name, id)`, id `Tuple(UInt64, LowCardinality(UUID))` with the default generator), plus the proposed projection. 50 metric names, 2 labels (`instance` unique, `job` 7 values), merges stopped. 300k series in 3 parts, then 3M series in 6 parts. The lookup set is 17,000 ids (8,500 stored, 8,500 new) in a `Memory` temporary table. `max_threads = 2`, 6 warm runs each, 2 vCPU / 4 GiB.

| case | 300k series | 3M series | notes |
|---|---|---|---|
| lookup through `tags_by_id` (the handoff's query) | **0.210 s** (0.204–0.295) | **1.37 s** (1.31–1.92) | 20,498 / 41,047 ranges; 31,252 / 53,734 rows read |
| of which index analysis (`FilteringMarksWithPrimaryKeyMicroseconds`) | 0.23 s | 0.63 s | generic exclusion search over one-row marks |
| same lookup, planner left alone (no `force_optimize_projection`) | 0.034 s (0.033–0.035) | 0.276 s (0.271–0.321) | the planner picks the parent and scans all of it (37 granules at 300k) |
| same lookup, `Tuple(UInt64, UUID)` id (no `LowCardinality`) | – | 0.445 s (0.340–0.564), 5 runs | 2.96M rows in 3 parts, not 6, so part of this gain comes from the part count |
| write 17,000 tags rows, v11 shape | 0.033 s (0.030–0.034) | – | new part, text index included |
| write 17,000 tags rows, with `tags_by_id` | **0.099 s** (0.095–0.105) | – | the projection triples the cost of every new tags row |

Size at 3M series: the projection is 73.8 MiB on disk (the parent, without its text index, is 71.6 MiB) and **72.1 MiB of primary index in memory** (the parent's: 1.9 KiB). The handoff priced in the disk; the memory comes on top, at about 25 bytes per tags row, including the unmerged duplicates.

## Why (cause)

- **The lookup is index analysis plus one decompressed block per row.** For an `IN` set, MergeTree runs a generic exclusion search: it splits the mark range, tests each piece against the set, and recurses. At one row per mark, that means hundreds of thousands of marks per part, for every part. It took 0.23 s at 300k rows and 0.63 s at 3M. `merge_tree_coarse_index_granularity` from 2 to 16 changed nothing (0.21–0.29 s at 300k). Then every selected row is its own compressed block (`CompressedReadBufferBlocks` = rows read, 73 bytes each). `LowCardinality(UUID)` adds a dictionary per one-row granule, which is why the plain-UUID id reads about 3× faster.
- **The write it skips is cheap.** A tags row costs about 2 µs to write on this host, text index included. The projection adds about 4 µs per row on every insert, found or not.
- **The production numbers point at the cache size.** `tags_deduplication_cache_size_bytes` defaults to 100 MiB. The cache estimates 144 bytes per entry (`APPROXIMATE_ENTRY_SIZE` in `TimeSeriesDeduplicationCache.h`). So it holds at most 104,857,600 / 144 = **728,177 series**: the "about 728,000" in the handoff is the cap. A series scraped every 15 s with the default 1 h expiration misses about once every 0.8–1 h, which allows a hit rate of about 99.5%. A hit rate of 85% means evictions from a full cache, or new series, not expirations.
- **Two settings in the handoff don't do what it expects:**
  - `statistics_max_set_size_for_exact_selectivity_estimation` only feeds the statistics selectivity estimator (`ConditionSelectivityEstimator.cpp:430`), not index analysis. With 17,000 ids, `EXPLAIN` shows the same 20,498 ranges at the default 10,000 and at 0. The setting that bounds index use for a large set is `use_index_for_in_with_subqueries_max_values` (default 0, no limit).
  - The DDL fails on master: `Projections are not supported for ReplacingMergeTree with deduplicate_merge_projection_mode = throw` (`SUPPORT_IS_DISABLED`). It needs `deduplicate_merge_projection_mode = 'rebuild'`. With `'drop'`, every merge removes the projection from the merged part.
- **`force_optimize_projection = 1` is needed, as the handoff says.** Without it the planner reads the parent, as the table above shows. With it, the lookup is 5× slower than the scan the planner would have picked.

## Proposed fix

No code yet. In order:

1. On the production writer, compare `CurrentMetrics.TimeSeriesTagsDeduplicationCacheEntries` with 728,177. At the cap, raise the size on that table: `ALTER TABLE <ts> MODIFY SETTING tags_deduplication_cache_size_bytes = <144 × series × 1.3>`. That is about 180 MiB per million series. It takes effect live and is reversible. Then re-read the hit rate (`TimeSeriesTagsDeduplicationCacheHits` / `...Misses`).
2. Find what the 3.3 s is made of. `system.part_log` per inner table (`event_type = 'NewPart'`, `duration_ms`, `rows`) shows the tags write next to the samples write. `MergeParts` rows for the tags table show the merge rewrite the lookup would save. Only a tags write that costs more than about 80 µs per row (the lookup cost at 3M rows here) would make the lookup pay.
3. A branch is still possible if step 2 shows that. It would need `deduplicate_merge_projection_mode = 'rebuild'` and no `statistics_max_set_size_for_exact_selectivity_estimation`, and should consider a plain `UUID` id component. It would add a new TimeSeries version, a projection, two settings and a row cap: wide, not "simple". Version 12 is also taken by rollup-samples (#16); whichever merges second becomes 13.

## Upstream overlap

- No open upstream PR adds a tags lookup or an id projection (checked `comp-promql`, 2026-11-14, master `d433f9232937`). James's #126132 (prewarm the tags cache at table start) is related: it fills the same cache after a restart, a different source of misses.
- James's fork branches `jtcunning/tags-dedup-cache-expiration-jitter` and `timeseries-dedup-cache-set-many` (the latter merged upstream) changed the cache's expiration and batching. Neither adds a lookup.

## How to reproduce

```bash
CH=/path/to/clickhouse bench/tags-id-projection-lookup/run.sh /tmp/tags-lookup-data   # about 2 minutes, about 400 MB on disk
```

`run.sh` creates the v12-shaped table (`create.sql`), prints `EXPLAIN indexes = 1` with and without each setting (`lookup.sql`), and times the lookup against the parent scan (`time.sql`) at 300k series. It then grows the table to 3M (`grow.sql`, which prints the sizes) and repeats the timing. Last come the 17,000-row writes (`ins.sql`) and the plain-UUID variant (`plain.sql`).
