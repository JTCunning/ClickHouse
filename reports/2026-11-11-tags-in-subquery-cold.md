# Where a cold PromQL range query spends its time on S3 (handoff #15)

**Verdict:** The cold time is a chain of small S3 requests, made one after another. The tags lookup alone is 67% of a fully cold query and 83% of what stays after James's mark and primary-index prewarm. Idea 1 (resolve the ids first and pass a literal set) doesn't remove any of those requests, so I didn't build it. One part of the chain is a code problem that arrived on master after the captures: the new statistics probe (#126627, merged 2026-11-07) loads the samples parts' primary indexes one at a time. Fix branch `lilred-perf-parallel-statistics-probe`, emulated at about −19% of cold wall time.
**Task:** 2026-11-02-handoff-tags-in-subquery-and-cold-part- · handoff https://github.com/JTCunning/ClickHouse/issues/15 · **Build:** 26.11.1.1642, master `7b5f2bf328f9` · **Date:** 2026-11-11
**A/B plan:** ab-plans/parallel-statistics-probe.md

## What happens (plain language)
A selector runs as a single query: read the samples table where `id IN (tags lookup)`. The tags lookup has to finish before ClickHouse can narrow the samples read by primary key, so it runs during planning (`QueryPlan::optimize`). On cold S3 every step of that lookup is a request that waits for the one before it:
1. Load each tags part's primary index.
2. Read its text index: header, then dictionary, then postings, three requests in a row.
3. Load the statistics.
4. Read the marks and the data of the tags and time-ranges parts (the merge join reads in id order).
5. Then the samples side: primary indexes, marks, data.

Each step is one request per part, spread over at most `max_threads` threads, so the time grows with the number of fresh parts × the requests per part × the latency. Prewarming marks and primary indexes (James's config) removes steps 1 and part of 4. What's left is mostly the text index and the first read of tags and time-ranges data.

## Numbers
- **Lab:** a local S3 mock adds 50 ms to every GET, close to the ~60 ms per request in James's capture. It feeds an `s3` disk with a 4 MiB-aligned filesystem cache. Script: `bench/tags-in-subquery-cold/s3mock.py`. GETs reuse pooled connections (623 reused, 0 created per cold query), and init time per GET is 58–61 ms.
- **Data:** 64 inserts of 15 minutes each (16 h) with merges stopped, so every inner table has 64 fresh parts.
  - 21,560 active series in 20 metrics. 2% of the pods restart per insert, so each insert brings about 430 new series.
  - Tags table: 48,723 rows, 2.3 MiB. Time ranges: 24 MiB. Samples and recent samples: 91 MiB each.
- **Query:** James's shape, `max by (pod) (metric_0{label_a="v1",…,label_c=~"v1",…,pod=~".*server.*"})`, 3 h range at 30 s steps, 12 series. `max_threads = 16`, 2 vCPU. Median of 5 runs.
- **States:**
  - *cold:* every cache dropped and primary keys unloaded.
  - *post-prewarm:* marks, primary keys and statistics in memory; the filesystem cache and the text index caches dropped. This is James's worst query after prewarm.

| case | wall ms median (min–max) | S3 GETs | from S3 MB (of which predownload) |
|---|---|---|---|
| cold | 3,204 (3,152–3,246) | 623 | 26.3 (14.7) |
| post-prewarm | 1,366 (1,357–1,374) | 324 | 26.2 (14.7) |
| post-prewarm + tags text-index files cached | 781 (732–864) | 163 | 25.9 (14.6) |
| post-prewarm + tags column data cached | 1,011 (981–1,017) | 291 | 24.8 (14.3) |
| post-prewarm + time-ranges data cached | 1,194 (1,177–1,267) | 311 | 21.1 (10.8) |
| post-prewarm + samples data cached | 1,225 (1,224–1,238) | 207 | 6.8 (4.4) |
| post-prewarm + all three tags-lookup classes cached | 231 (220–257) | 117 | 19.4 (10.3) |
| warm | 98 (96–132) | 0 | 0 |
| post-prewarm, `use_skip_indexes = 0` (no text index) | 1,056 (1,044–1,103) | 194 | 26.4 (14.8) |
| cold, `use_skip_indexes = 0` | 3,257 (3,238–3,304) | 555 | 26.5 (14.9) |
| post-prewarm, `filesystem_cache_boundary_alignment` = 1 MiB | 1,372 (1,342–1,374) | 324 | 25.7 (14.7) |
| post-prewarm, alignment 64 KiB | 1,669 (1,635–1,799) | 352 | 11.7 (2.0) |
| cold, statistics probe off (`min_marks_per_part_to_load_statistics = 0`) | 2,649 (2,606–2,709), n=3 | 636 | 26.3 |
| cold, 13 samples indexes preloaded in parallel first (branch emulation) | 2,540 (2,481–2,812) | 610 | 26.3 |
| cold, zero added latency | 653 (614–710) | 623 | 26.3 |

**Share per cause, post-prewarm (1,366 ms).** Each row is the time saved by caching that class alone. The four rows add up to within 3% of caching all of them together.

| cause | ms | share |
|---|---|---|
| tags text index (header, dictionary, postings files) | 585 | 43% |
| tags column data (first read) | 355 | 26% |
| time-ranges column data (first read) | 172 | 13% |
| samples data (first read) | 141 | 10% |
| warm floor (CPU, planning) | 98 | 7% |

**Share per cause, fully cold (3,204 ms).** From the GET timeline, in the order the phases run:

| phase | ms | share |
|---|---|---|
| tags index analysis: primary index → text-index header → dictionary → postings, 64 parts on 16 threads | 0–800 | 25% |
| tags statistics | 800–970 | 5% |
| time-ranges primary index and statistics | 970–1,100 | 4% |
| tags and time-ranges marks and data (the in-order merge join) | 1,100–2,150 | 33% |
| samples primary indexes, one at a time (the #126627 probe) | 2,150–2,820 | 21% |
| samples marks and data | 2,820–3,150 | 10% |

So in James's terms:
- **Tags `IN` subquery:** 67% cold, 83% after prewarm. Marks and the primary index are part of each phase above; prewarming them takes cold from 3.2 s to 1.4 s, which matches the 2.5–3 s of mark wait he saw disappear.
- **Text index:** 25% cold, 43% after prewarm.
- **Statistics:** 11% cold: probe off 2,649 ms vs statistics off 2,309 ms. They load in parallel, about one round trip per table.

## Why (cause)
- **The tags lookup runs inside `QueryPlan::optimize`** because the samples read is narrowed by the id set during index analysis (`FutureSetFromSubquery::buildOrderedSetInplace`, see report 2026-10-31-selector-eager-ids). That is why the planning counter grows. `QueryPlanOptimizeMicroseconds` also counts the lookup's own nested optimization, so it can exceed wall time (3,975 ms in a 3,204 ms query). Moving the lookup out of planning (idea 1) changes the counter but not the requests.
- **Text index, three requests in a row per part.** The tags table has `INDEX tags_idx tags TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 100000000`, one index granule per part. To decide whether a part can match, the reader fetches the index header (`skp_idx_tags_idx.idx`), then the dictionary block (`.dct.idx`), then the postings (`.pst.idx`), and only then the tags marks and data. A fresh part holds a few hundred series, so the index can at best skip one tiny part. It skipped 31 of 64 parts here. There is no prewarm setting for it (James's comment).
- **Read amplification is cache-segment predownload.** The filesystem cache cuts files into segments aligned to 4 MiB (`boundary_alignment`, `FILECACHE_DEFAULT_FILE_SEGMENT_ALIGNMENT`), and a segment is always downloaded from its start. A read in the middle of a segment first downloads everything before it (`CachedOnDiskReadBufferFromFile`, "predownload"). Here 14.7 of the 26.3 MB taken from S3 (56%) is predownload. The id order clusters each metric's series somewhere inside every samples part, so each part is downloaded from offset 0 up to that metric's rows. The share depends on where the metric's `sipHash64` sorts, which would explain worst and best queries with the same plan. A smaller `filesystem_cache_boundary_alignment` cut the bytes (64 KiB: 11.7 MB) but added requests and made the query 22% slower here. With files under 4 MiB, the request count matters more than bytes.
- **The statistics probe (new on master).** #126627 (2026-11-07, 26.11.1.1600, synced to Cloud) checks, before loading statistics, whether the primary key leaves a small read: `MergeTreeDataSelectExecutor::hasFewerMarksToReadThan`, called from `optimizePrewhere` → `ReadFromMergeTree::getPartsForPrewhere`. It walks the parts one by one, and each part whose primary index isn't loaded costs a request. Here the 13 samples parts loaded their indexes 51 ms apart (617 ms), on the main thread. With the probe off, the same indexes load in parallel in 3 ms. James's captures (1102, 1311) predate it.
- **S3 request init (idea 3).** In the lab every GET reuses a pooled connection, and init time per GET is the latency plus queueing (58–61 ms at 50 ms injected). James's 58 ms per request is most likely S3 first-byte latency under load, not connection setup. `ProfileEvents['DiskConnectionsCreated']` vs `['DiskConnectionsReused']` on his capture would confirm it.

## Proposed fix
- **Code, branch `lilred-perf-parallel-statistics-probe`:** probe the parts in parallel when the primary indexes of several parts aren't in memory. The decision is the same as before, and with the indexes in memory the probe stays serial.
  - Diff: +65/−7 in `MergeTreeDataSelectExecutor.cpp`; +22/−3 in `IMergeTreeDataPart` (`isIndexInMemory()` and one helper for the primary index cache key); stateless test `05620`. Effort S.
  - Emulated at 3,204 → about 2,600 ms cold (−19%). Post-prewarm and warm don't change (indexes in memory).
  - Compare: https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-parallel-statistics-probe · PR text `prs/parallel-statistics-probe.md`
- **Idea 1 (resolve ids first, literal set):** not built. It removes no request: the tags and time-ranges reads are the same whether the set is built in place or by a separate query. Report #4 (2026-10-31) measured it on local disk: same wall time at 1–89 series, 2–6× slower from about 900 series with a literal list.
- **Config to try on staging (no code):**
  1. **Confirm the amplification source:** read `ProfileEvents['CachedReadBufferPredownloadedFromSourceBytes']` for `2109d64e`. If most of the 138.8 MB is predownload, it's the 4 MiB segment start. Then try `filesystem_cache_boundary_alignment = 1048576` on the PromQL user's profile. It helped bytes but not time here, because my files are small; his 68 GETs averaged 2 MB each, so bytes may matter more there.
  2. **Keep fresh tags and time-ranges parts resident on every replica:** `cache_populated_by_fetch = 1` on the tags and time-ranges inner tables, optionally with `cache_populated_by_fetch_filename_regexp` limited to the small files (primary index, marks, `skp_idx_*`). This is a Cloud-only MergeTree setting, and I could not test it here. The inserting replica already writes through the cache (`enable_filesystem_cache_on_write_operations`, Cloud default 1). Merged parts: `merge_max_bytes_to_prewarm_cache` (Cloud, 1 GiB per part by default) prewarms on the merging replica. `prefer_warmed_unmerged_parts_seconds` (Cloud, session) reads the warm source parts instead of a freshly merged cold one.
  3. **Text index on fresh parts:** `exclude_materialize_skip_indexes_on_insert = 'tags_idx'` for the ingest user. Fresh tags parts then carry no text index, and merges build it. Its proxy here, `use_skip_indexes = 0` with every part fresh, saved 310 ms post-prewarm (−23%) and nothing cold, where all 64 tags parts' data must be read instead of 33. To verify: that the setting reaches the TimeSeries sink's inner inserts.
- **Structural (James invited these):**
  - Fewer, larger tags parts. Every fresh part adds about six chained requests (primary index, three text-index files, marks, data), and the tags table is tiny (2.3 MiB for 48k series here). Batching new series into fewer tags inserts would shrink the chain. The `TimeSeriesSink` writes tags per insert, so this would be a sink-side buffer, which is a design change rather than a simple fix.
  - Or let the selector skip the text index on small parts, which needs a size threshold. I'd measure config 3 first.

## Upstream overlap
- **#126627** (alexey-milovidov, merged 2026-11-07): source of the serial probe. Its cold test ran on NVMe, where a serial index load is fast. The branch keeps its decision.
- **#126621** (alexey-milovidov, open): reads all statistics of a part at once. It lowers the statistics share, not the probe.
- **#128883** (merged 2026-11-03): caps `boundary_alignment` at `max_file_segment_size`. Same knob, doesn't change the 4 MiB default.
- **#113607** (open): reads a partially downloaded segment in passive mode. Concurrent readers, not predownload.
- **#121816** (vdimir, open, sliced read pool for read-in-order) and **#100391** (open, parallel read-in-order from one part): the in-order read of step 4 is synchronous with no prefetch. Both are general efforts in that area.
- **#118801** (open): materialized CTE for selectors, the pre-v11 probe path only. **#127847** (open): `ANY` join for time ranges. **#130402** (James, submitted): one id set per repeated selector. None touches the cold chain.
- **James's fork:** `jtcunning/timeseries-prewarm-mark-cache` (2026-11-03) is a TimeSeries setting that turns on `prewarm_mark_cache` for the inner tables, the code version of his config mitigation. It complements this report. `planner-concurrent-key-set-builds` (2026-10-27) builds independent `IN` sets of one query together. It helps queries with several selectors, not the chain inside one lookup.

## How to reproduce
On a host with a prebuilt `clickhouse`:
```bash
cd research/bench/tags-in-subquery-cold
python3 s3mock.py --root $D/s3root --port 9100 --log $D/get.jsonl &      # S3 endpoint, latency via $D/s3root/.latency_ms
clickhouse server --config-file=config.xml &   # from server-config.xml.template ($D replaced): `s3` disk + 4 MiB cache disk, policy `s3`
OUT=$D bash gen.sh                                                       # the 64-insert dataset
L=50 N=5 bash matrix.sh                                                  # every row of the table above, into $D/results.jsonl
python3 summarize.py $D/results.jsonl        # raw results of this report: results.jsonl, matrix.log
bash stacks.sh                                                           # the stack of the serial primary-index loads
```
`run.py --state cold|meta|warm [--keep 'tags: text index'] [--settings k=v] [--pre SQL]` runs one case. For every GET it writes the table, part file and timeline (`get_span_ms`), mapped from the `s3` disk's metadata.
