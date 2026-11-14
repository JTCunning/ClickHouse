# Fewer parts and rows in the in-order read of the id lookup (version 11, continuous small inserts)
**Verdict:** both the part count and the rows per id set the cost, so the two levers stack. The biggest measured win is a time ranges deduplication cache that rounds the stored range out to a bucket: 30–66% faster lookups on a 94-part tail, and 42–75% when no new series arrive. It needs a second cache and two settings, so it isn't "simple": offered, not built. Batching the writers is a settings change that works today (5× larger inserts: 15–35% faster). I couldn't test merge settings here, because this lab's merges never lag.
**Task:** 2026-11-13-handoff-fewer-parts-and-rows-in-the-in- · [handoff #19](https://github.com/JTCunning/ClickHouse/issues/19) · **Build:** master 26.11.1.2253 (`f0a9e6bffa71`, includes #127847: the time ranges join is `ANY`, so the right side no longer multiplies rows) · **Date:** 2026-11-14
**A/B plan:** [ab-plans/idlookup-parts-and-rows.md](../ab-plans/idlookup-parts-and-rows.md) · **Lab:** [bench/v11-unmerged-idset](../bench/v11-unmerged-idset/README.md) (section "Steady state (#19)")

## What happens (plain language)
The id lookup reads two tables in id order, the tags of the metric and its time ranges, and merge-joins them. Each selected data part becomes one sorted input stream, and the streams are merged into one before the join. The cost therefore grows with the rows read and with the number of streams those rows are spread over. The time ranges table receives one row per series per insert, and nothing merges them until background merges catch up. Under continuous small inserts, the tail of recent parts holds several rows per live series. This lab holds 5.5 rows per live series on a 94-part tail; the capture in #19 holds 4.3 over 89 parts.

## Numbers
Host: 2 vCPU, 4 GiB, local disk; `max_threads = 2`, `max_threads_min_free_memory_per_thread = 0`. Each figure is the median of 5 warm runs, interleaved. The ids query is master's (`LEFT ANY JOIN`, `full_sorting_merge`, read in order), built as an `IN` set. Selectors on `metric_0`: wide (all series), mid `job="job3"` (10%), narrow `instance="host-17"` (10 series).

**1. Streams or rows? (idea 4).** The same rows of `ts_merged` (110k ids, 60k live `metric_0` series) are written into P parts with C copies of every row. Each part spans the whole id range, as an insert of all series does.

| copies × parts | wide ms | mid ms | narrow ms | in-order streams | in-order read, thread ms (wide) | sorted merge ms (wide) | rows into join (wide) |
|---|---|---|---|---|---|---|---|
| 1 × 1 | 68 | 25 | 18 | 2 | 29 | 0 | 125,000 |
| 1 × 25 | 155 | 72 | 53 | 50 | 78 | 52 | 125,000 |
| 1 × 90 | 182 | 108 | 73 | 180 | 72 | 64 | 125,000 |
| 30 × 1 | 1,021 | 260 | 52 | 4 | 509 | 6 | 3,750,000 |
| 30 × 25 | 1,661 | 672 | 450 | 50 | 786 | 1,232 | 3,750,000 |
| 30 × 90 | 2,157 | 1,117 | 908 | 180 | 1,145 | 1,811 | 3,750,000 |

- **At equal rows, streams multiply the cost.** At one copy, 90 parts cost 2.7–4.3× one part: about 0.6–1.3 ms per extra pair of parts, from opening each part and reading at least one granule at both ends of the metric's id range. At 30 copies, 90 parts cost 2.1× (wide) to 17× (narrow) one part. The sorted merge over 180 streams (1.8 s) then costs more than the read itself. `read_rows` also grows from 3.9M to 5.5M, because every part reads whole granules at the edges of the metric's range.
- **At equal parts, rows multiply the cost:** 30 copies cost 15× one copy at one part (wide) and 12× at 90 parts.
- The narrow selector is the most exposed. It still reads the metric's whole time ranges range: 52 ms from one part, 908 ms from 90.
- In the capture, 230 in-order sources took 10.1 s of thread time, about 44 ms each, against 0.5–6 ms each here. The per-part fixed cost on the Cloud's object storage is far larger (#15 measured cold part metadata), so cutting parts is worth more there than in this lab.

**2. Steady state: the variants.** `loop.py` keeps writing 6 statements per second, each `INSERT … SELECT` carrying 3,000 series × 1 sample. 60,000 series slots are written in turn, one round every 3.3 s. Pods restart, so a series lives 30 min: 33 new series per second. The table also holds 300,000 dead series, merged into one part. Merges run normally; for the last 15 s, the merges of the tags and time ranges tables are stopped. That leaves a tail of ~90 unmerged inserts, like the capture's 95-part time ranges table with 32 single-insert parts. Every variant gets the same lag in seconds. The lookup runs over the last 30 s.

| variant | TR parts | tags parts | in-order streams (wide) | TR rows into join per id | TR rows written | wide ms | mid ms | narrow ms | false-positive ids (wide) |
|---|---|---|---|---|---|---|---|---|---|
| baseline | 94 | 93 | 186 | 5.5 | 1,893,000 | 362 | 243 | 222 | 0 |
| TR cache, B = 1 h, mid-bucket | 97 | 93 | 189 | 1.0 | 63,400 | 253 (−30%) | 106 (−56%) | 75 (−66%) | 2,512 (+4.1%) |
| TR cache, bucket boundary at the tail start | 97 | 93 | 189 | 1.9 | 123,269 | 287 (−21%) | 130 (−47%) | 97 (−56%) | 2,496 (+4.1%) |
| batched ×5 (15,000 series, 1.2/s) | 20 | 25 | 44 | 5.4 | 1,905,000 | 299 (−17%) | 162 (−33%) | 144 (−35%) | 0 |
| batched ×20 (60,000 series, 0.3/s) | 10 | 9 | 18 | 8.9 | 1,980,000 | 306 (−15%) | 187 (−23%) | 156 (−30%) | 0 |
| baseline, no new series | 96 | 6 | 101 | 5.7 | 1,893,000 | 356 | 237 | 214 | 0 |
| TR cache, no new series | 3 | 6 | 8 | 1.0 | 60,000 | 205 (−42%) | 86 (−64%) | 54 (−75%) | 0 |
| merge settings | not measurable here (see below) | | | | | | | | |

- **The time ranges cache** (emulated): the loop writes the rows that a cache keyed by id and rounded to bucket B would write. A series gets one row per bucket, `[floor(t / B) * B, next bucket start − 1 ms]`, and every later insert in that bucket skips it.
  - Rows into the join drop from 5.5 to 1.0 per id; time ranges rows written drop 30×.
  - With new series in every insert, each insert still writes a tiny time ranges part (5 rows), so the stream count stays 189. That variant is still the fastest with churn: the sorted merge takes 138 ms instead of 244 ms (wide).
  - Without new series, the tail writes no time ranges parts at all: 8 streams, up to −75%.
  - Right after a bucket boundary, the first round writes every series again: 1.9 rows/id, still −21–56%.
- **False positives:** the rounded ranges are supersets, so the id set gains series that stopped earlier in the current bucket. Here that is +4.1% of ids (2,512, all pods that restarted since the bucket began), against truth computed from the samples table.
  - In general it is about (new series per second) × (time since the bucket start). On average that is B / (2 × series lifetime): 2% for B = 1 h and a 24 h lifetime, 0.5% with a 15 min bucket.
  - PromQL results stay identical, because samples are filtered by timestamp anyway. Each false-positive id costs one samples bucket row read and then discarded.
  - `/api/v1/series`, `labels` and `label values` with a time range would list such a series until its bucket ends. That is a visible change; Prometheus is itself chunk-granular there.
- **Batching** cuts the streams 186 → 44 at the same rows per id: −15–35%. It delays visibility by the batching window. Batching beyond ×5 adds little, because the big statements leave bigger unmerged parts with every series in each.
- **Wide selectors:** no variant slowed one down. The cache adds +4% ids to the set (this lab's churn).
- **Merge settings (idea 3):** this lab's merges never lag. With plain `MergeTree`, and with `Replicated*` inner engines on a single replica (embedded Keeper), the time ranges table stays at 3–6 parts with 1.0–1.1 rows/id, at 6–15 inserts/s, with or without 300k series of history. So the 95-part steady state in the capture comes from the service's merge scheduling at its insert rate (about 25 new parts/s in the tags table, across 10 replicas), which one node doesn't reproduce. I didn't test settings I can't observe working. The payoff per tail part removed is in tables 1 and 2: about 0.5 ms per stream here, and up to ~44 ms per source in the capture.
- **Interplay with #18** (one tags row per id before the join, `lilred-perf-selector-one-tags-row-per-id`): it works on the tags copies, this report on the time ranges rows and parts, so they don't overlap.
  - Here the tags side has one copy per id: the single server's tags cache writes a series once. So #18's shape is ±5% on these fixtures; its gain needs the copies (−19% at 30 copies, #18's report).
  - The capture's 7–8 tags copies per id fit 10 replicas with one tags cache each: each writes a series once per cache lifetime. That is a hypothesis; check it in `part_log` (tags rows written per series per hour).
  - Side finding for #18: on the 93 tiny tags parts, its subquery shape counted 186 SelectedParts at run time against master's 95 (narrow; `EXPLAIN indexes = 1` shows the same 2/93 text-index pruning for both), and the narrow lookup was 4–19% slower. Worth a guard case in #18's A/B.

## Why (cause)
- The time ranges table (`AggregatingMergeTree ORDER BY id`) gets one row per series per insert: `TimeSeriesSink.cpp` "Step 4", `makeTimeRangesBlock`, with no cache. The tags table has a deduplication cache (`TimeSeriesDeduplicationCache`); the time ranges table has none.
- The lookup (`StorageTimeSeriesSelector.cpp` `addTimeRangesTableJoin`, settings in `applySettingsForSelectIDsQuery`) reads both tables in id order. Every selected part is one in-order source, and a `MergingSortedTransform` merges the sources before the `full_sorting_merge` join. The primary key keeps every part that has rows of the metric, which is every insert part, and inside a part it can't skip by time.
- So: lookup ≈ (parts × fixed cost per part) + (rows × per-row cost, which grows with the number of streams merged).

## Proposed fix
1. **Now, settings (no code).** Write fewer, larger inserts.
   - Prometheus `remote_write` `queue_config`: raise `max_samples_per_send` (default 2,000) to 10,000–50,000. Visibility stays bounded by `batch_send_deadline` (5 s).
   - Recording rules: fewer, larger `INSERT … SELECT` statements where the evaluator allows.
   - Measured here: 5× larger inserts, 4.2× fewer streams, −15–35%.
2. **Offered: a time ranges deduplication cache with outward rounding** (version 11 only, `store_time_ranges = 1`). The winner, with no wide-selector regression. What it needs:
   - Round `min_time` down and `max_time` up to `samples_bucket_step_seconds` in `fillTimeRangesColumns`. That is an existing setting, so no new threshold, and the stored range only gets wider.
   - A second `TimeSeriesDeduplicationCache`, keyed by id, through its existing `filterOutWrittenRows` / `markRowsAsWritten`. A row counts as written only if all its values are equal, so a rounded row repeats exactly within a bucket. Mark the rows after `finish` (as for tags), and clear the cache on `TRUNCATE` and `SYSTEM DROP TIME SERIES CACHES`.
   - Settings `time_ranges_deduplication_cache_size_bytes` / `_expiration_seconds` (or the tags ones reused), plus ProfileEvents.
   - A stateless test: rows written per bucket, PromQL results identical, a series that stopped mid-bucket.
   - No format change. Effort M (~200 lines).
   - Choices for James: the bucket (1 h samples bucket vs 15 min) against the false-positive rate, and the change to the series/labels endpoints.
3. **Merge settings:** to be measured on the service. The quantity to watch is level-1 parts in the time ranges and tags inner tables against `ProfileEvents['SelectedParts']` of the lookup. This lab can't show a lagging tail.

## Upstream overlap (checked 2026-11-14, master 7717a83f451b)
- No open PR adds a time ranges cache or rounding.
  - #121062 (valerypetrov, an older version-8 design for min/max outside the tags table) has no time ranges cache.
  - #131384 (valerypetrov) changes the commit order and the tags cache for external tags tables.
  - #113045 (alexey-milovidov) does the `Too many parts` check once per insert.
- The general read-in-order work doesn't change the stream count per part set: #121816 (vdimir, sliced pool, open), #100391 (alexey-milovidov, one-part parallel in-order, open).
- Recent commits on the sink, settings and selector: #127847 (ANY join, merged 2026-11-12), James's dedup-cache expiration commits (2026-10-30 … 11-01), and the time ranges id-range hint commit (2026-11-03).
- James's fork branches: `timeseries-dedup-cache-set-many` (0 ahead of master), `jtcunning/timeseries-insert-series-cache` (tags cache, September) and `jtcunning/timeseries-parallel-inner-writes` (flush order). None caches time ranges.

## How to reproduce
Lab `bench/v11-unmerged-idset/` (README sections "Build it" and "Steady state (#19)"):
```bash
cd tools/clickhouse/data/v11-unmerged-idset && cp <lab>/server-config.xml . && clickhouse server --config-file=server-config.xml &
<lab>/gen.sh <clickhouse> 19100                       # ts_merged (source of the grid)
for pc in "1 1" "25 1" "90 1" "1 30" "25 30" "90 30"; do <lab>/gen-grid.sh <clickhouse> 19100 $pc; done
python3 <lab>/streams.py 5 grid.jsonl g_p1_c1,g_p25_c1,g_p90_c1,g_p1_c30,g_p25_c30,g_p90_c30 any_none_none
<lab>/steady.sh s_base:base s_tr:trcache3600 s_tra:trcache3600:ALIGN=1 s_b5:batch5 s_b20:batch20 \
  s_basen:base:CHURN=100000000 s_trn:trcache3600:CHURN=100000000      # ~2 min per variant
python3 <lab>/streams.py 5 steady.jsonl "s_base,s_tr=default.\`.inner_id.tags.<uuid>\`|default.s_tr_tr,…" any_none_none,any_limitbyord_none
```
Raw results: `bench/v11-unmerged-idset/results19-e1-grid.jsonl`, `results19-steady.jsonl`, `steady/*.parts.json`.
