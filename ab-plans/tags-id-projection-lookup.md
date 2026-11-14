# A/B plan: tags lookup through an id-only projection
**Experiment:** ../reports/2026-11-14-tags-id-projection-lookup.md · **Branch:** none yet: verifies a report conclusion · **Task:** 2026-11-13-handoff-tags-id-projection-lookup (handoff #21)
**Claim:** The proposed lookup (`id IN (SELECT id FROM <tmp>)` through `tags_by_id`, `index_granularity = 1`, forced) costs more than writing the rows it would skip. 17,000 ids at 3M series: lookup ≥ 10× the 17,000-row tags write, and ≥ 3× a scan of the parent table. The projection makes a tags write ≥ 2× slower.
**Evidence so far:** measured on 2 vCPU / 4 GiB with a master binary (`clickhouse local`, `max_threads = 2`). Lookup 1.37 s vs parent scan 0.276 s vs write 0.033 s (v11) / 0.099 s (with the projection). Unproven: the ratios at 16 threads, at 10M series, and on object storage, and what the production tags write costs.

## Builds
- None needed: the experiment runs SQL against a master binary. Use master at or after `f0a9e6bffa71` (26.11.1.2253). If a branch is ever written, A = master and B = the branch at the same base, release build.

## Machine
- 16 vCPU / 64 GiB, local NVMe: the production writer's class. The index analysis runs per part in parallel, so the 2-vCPU ratios may shrink. Nothing else running. Note the CPU model.
- Optional second pass on object storage (an `s3` disk under a filesystem cache; see `bench/tags-in-subquery-cold/`). Cold reads of 40k one-row ranges are the shape most likely to get worse there.

## Data
- `bench/tags-id-projection-lookup/run.sh <dir>`: the v11 inner tags table as master generates it, plus the projection with `deduplicate_merge_projection_mode = 'rebuild'`. 50 metric names, 2 labels, merges stopped.
- Sizes: 300k series (3 parts), 3M (6 parts), 10M (raise `numbers(300000, 2700000)` in `grow.sql` to `numbers(300000, 9700000)`).
- Lookup set: 17,000 ids, half stored and half new (`time.sql`). Also run 1,000 ids and 100,000 ids, to bracket the cap `tags_deduplication_lookup_max_rows`.

## Cases
| case | query | role | predicted |
|---|---|---|---|
| T1 | lookup through `tags_by_id`, forced (`time.sql` runs 1–6) | target | 1.0–1.5 s at 3M (2 vCPU); ≥ 10× W1 at every size from 3M |
| T2 | same lookup, `optimize_use_projections = 0` (parent scan, `time.sql` runs 7–12) | guard (the planner's own choice) | ≤ 1/3 of T1 |
| T3 | T1 with a `Tuple(UInt64, UUID)` id (`plain.sql`), on the same part count | edge | ~3× faster than T1, still > W1 |
| W1 | `INSERT` 17,000 tags rows, v11 shape (`ins.sql`) | baseline of the work saved | 30–35 ms (2 vCPU) |
| W2 | same `INSERT`, with `tags_by_id` | guard (the cost of the projection on every insert) | ≥ 2× W1 |

## Method
- Each `.sql` file is a single `clickhouse local` session with `--time`. Report median, min and max of the 6 warm runs. Run the whole script twice and quote both medians: the A/A spread.
- For T1, also record `FilteringMarksWithPrimaryKeyMicroseconds`, `SelectedRanges`, `SelectedMarks` and `CompressedReadBufferBlocks` (`prof.sql` with `--print-profile-events`). They show the split between index analysis and reading.
- `max_threads` = 16, with `max_threads_min_free_memory_per_thread = 0`, so the streams aren't capped by free memory.

## Production checks (James, no build needed)
- Cache at its cap: `SELECT value FROM system.metrics WHERE metric = 'TimeSeriesTagsDeduplicationCacheEntries'`, compared with 728,177 (100 MiB / 144 bytes).
- The tags write's share of an insert: `SELECT table, count(), quantiles(0.5, 0.9)(duration_ms), avg(rows) FROM system.part_log WHERE event_type = 'NewPart' AND database = '<db>' AND table LIKE '.inner_id.%' AND event_time > now() - INTERVAL 1 HOUR GROUP BY table`.
- The merge rewrite: the same query with `event_type = 'MergeParts'`, `sum(rows)` and `sum(duration_ms)` for the tags table.
- After `ALTER TABLE <ts> MODIFY SETTING tags_deduplication_cache_size_bytes = <144 × series × 1.3>`: the hit rate from `TimeSeriesTagsDeduplicationCacheHits` and `...Misses` (in `system.events`) over 1 h, before and after.

## Correctness
- T1 and T2 return the same count (8,500) at every size. `EXPLAIN indexes = 1` of T1 shows `ReadFromMergeTree (tags_by_id)`, with about one or two rows per id.

## Pass / fail
- **The report holds (pass):** T1 ≥ 10× W1 and ≥ 3× T2 at 3M and 10M on the 16-vCPU host; W2 ≥ 2× W1. Production: the tags `NewPart` duration is a small share of the insert, or the cache sits at its cap.
- **The report is wrong (fail):** T1 < 3× W1 at 16 threads, or the production tags write costs more than about 80 µs per row. Then the lookup can pay: write the branch (with `'rebuild'`, without `statistics_max_set_size_for_exact_selectivity_estimation`), and A/B it against W1 and W2.

## Checklist
- [ ] run `run.sh` twice on the 16-vCPU host at 300k, 3M and 10M; save the output under `bench/tags-id-projection-lookup/ab/`
- [ ] run the production checks; record the cache entries, the hit rate before and after, and the tags vs samples `NewPart` times
- [ ] update the report verdict and the backlog row
