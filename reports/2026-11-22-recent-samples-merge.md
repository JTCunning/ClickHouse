# Merging the recent samples table into the samples table

**Verdict:** the recent samples table is fast for one reason only: its 5-hour partitions keep the newest rows of every series next to each other (time clustering). Its 15-minute buckets, 128 KiB granules and 4-day size add nothing measurable. A single table with **partitions labelled by a column** ("labelled tiers", L4) keeps that: new rows land in 5-hour partitions, and a mover rewrites partitions older than about a day into the month partition. Measured on master with DDL only, it matches the recent table on alert-rule and short-dashboard shapes (0.93–1.05×). It stores each sample once (storage 0.81× at 14 days), and inserts cost 0.64–0.81× the wall time and 0.66–0.76× the CPU. Results are identical. It costs up to 1.7× on wide 1–4 day ranges and 1.6–1.8× on narrow 7–14 day ranges. No static single-table layout does better: time-clustered layouts make narrow multi-day ranges 3–11× slower, and id-clustered ones lose most of the alert-rule gain. **Report only, no branch**: the engine-side mover is not a small change.
**Task:** weekend batch 2026-11-22, target 1 (James's chat request) · **Build:** master 26.12.1.112, `521c4bcb7b9441a8f591a80965d9a9714703190e` · **Date:** 2026-11-22
**A/B plan:** [ab-plans/recent-samples-merge.md](../ab-plans/recent-samples-merge.md)
**Also seen on master:** the "range starts inside the TTL → read the recent table" rule picks the slower table for narrow selectors over 1 day or more (1.6–3.9× slower than the samples table, section 2).

## What happens (plain language)
A short-range query needs the last minutes of every selected series. The samples table sorts by (series, hour) inside a month partition, so each series' newest hour sits next to its older hours, in a granule (the smallest unit the engine reads) of about 67 hour-rows. A wide alert rule therefore reads one 256 KiB granule per series, mostly old data. The recent table's 5-hour partitions hold only 5–20 rows per series, so a granule holds the newest rows of many series, and the same query reads 11× fewer bytes. The price of time clustering is the opposite query: a narrow selector over many days needs one granule per series per partition. A table can only have one clustering per partition. Master keeps both for the young window by storing it twice. L4 keeps both by moving partitions from one clustering to the other as they age.

## 1. Setup
- One server (`bench/recent-samples-merge/server.sh`), master binary, 2 vCPU / 4 GiB, `max_threads = 2` (plus one 8-thread check), `use_query_condition_cache = 0`, `max_memory_usage` 2.0 GB. Timings are server side (`system.query_log`), medians of 5 warm runs after one discarded round, with the arm order rotated per run.
- Data (`gen.sh`, inner tables filled with SQL): 2000 series of one counter metric, labels `job` (10 values), `instance`, `shard` (50 values), `pod`; one sample per 15 s with up to 15 s of per-series offset and 40 ms of jitter; 14 days ending at fill time (`t_end`); values `(j mod P)·M + noise`, i.e. noisy increments, a reset every 7–30 h, M from 1 to 1000 per series. That is 161M samples per layout. The recent table holds what it holds in steady state (4 d 5 h, 21 partitions). Every layout is fully merged (one part per partition) unless stated.
- Shapes through `prometheusQuery`/`prometheusQueryRange`: instant `rate(x[5m])` (wide, all 2000 series), `sum by (job) (rate(x[5m]))`, `rate(x{shard="7"}[5m])` (narrow, 40 series = 2%); ranges of 1 h @30 s, 6 h @60 s, 1 d @5 m, 3.5 d @30 m, 7 d @30 m, 14 d @1 h, each wide (`sum by (job)`) and narrow.
- Layouts: `m` = master defaults (two tables; `m:0` = the same table read with `time_series_prefer_recent_samples_table = 0`). Single-table layouts have the recent table off (`recent_samples_ttl_seconds = 0`): `p5`/`p5b`/`p5bg` = 5 h partitions with 1 h buckets / + 15 min buckets / + 128 KiB granules; `mb` = month partitions, 15 min buckets; `pd`/`pdbg` = day partitions, 1 h / 15 min + 128 KiB; `l2`/`l2p5` = the sub-row fold (section 4) in day / 5 h partitions; `lt1d` = labelled tiers with a 1-day young window.
- Correctness: every layout returns the same row count and the same order-independent hash with values rounded to 6 decimals, for all 15 shapes (`results/hash-v2.tsv`, `results/hash-lt1d.tsv`; 2000, 40 or 10 rows, never empty). The same holds on the unmerged copies, and in the middle of a move, when 624k rows are present twice.
- Counters include the tags and time-ranges tables (one part each, 2000 rows each, identical in every layout); only the samples side differs.

## 2. Why the recent table is faster (decomposition)
Wide instant `rate(x[5m])`, evaluated one day before `t_end`, so that every layout's newest partition is full (the worst point of each partition's fill cycle). One factor changes per row, from the samples table (b) to the recent table (a):

| step | layout | SelectedMarks | read MB (uncompr.) | compressed MB | CPU ms | ms, median (min–max) | share of the 726 → 62 ms gap |
|---|---|---|---|---|---|---|---|
| (b) samples table: month partitions, 1 h buckets, 256 KiB, 14 d | `m:0` | 2033 | 336 | 106 | 691 | 726 (712–770) | |
| + 5 h partitions | `p5` | 121 | 31 | 6.8 | 58 | 61 (56–65) | **100%** |
| + 15 min buckets | `p5b` | 127 | 32 | 6.8 | 57 | 60 (58–65) | 0 |
| + 128 KiB granules | `p5bg` | 245 | 32 | 6.8 | 58 | 61 (60–62) | 0 |
| + 4-day TTL = (a) the recent table | `m` | 245 | 32 | 6.8 | 65 / 57 | 65 (57–83) / A/A 59 (57–71) | 0 |
| 15 min buckets alone (month partitions) | `mb` | 2010 | 348 | 105 | 736 | 773 (734–833) | −7% |
| day partitions alone | `pd` | 718 | 187 | 38 | 213 | 224 (218–229) | 75% |

- The same holds for the other shapes. On 6 h wide (1161 → 439 ms), partitions explain 99% and buckets, granules and the TTL each move it by 6% or less, within the A/A spread (440 vs 438). On narrow instant (39 → 28 ms), partitions explain 91%.
- **Why:** per selected series the engine reads at least one granule from each partition that has rows in the range. Month partitions: a series has 336 hour-rows in the partition, and the granule holding its newest hour (about 67 rows, 256 KiB) is the unit read. 5 h partitions: a series has 5 hour-rows (or 20 quarter-hour rows), so one granule covers the newest rows of 13 series. The bucket width doesn't matter because the cost is decompressing whole granules, not cutting arrays. The granule size doesn't matter because granules are capped by bytes (the rows per granule scale inversely with the row width).
- **What the 4-day TTL buys:** storage and a bounded partition count, not speed. `p5bg` with 14 days and the recent table with 4 days read the same 245 marks in the same time. It also decides which ranges may use the recent table: wide 1 d and 3.5 d ranges are 1.8× and 1.7× faster there (845 vs 1507 ms; 1253 vs 2147 ms).
- **Master picks the slower table for narrow multi-day ranges.** The same time clustering makes the recent table slow for few series over many partitions. Narrow 1 d: recent 94–105 ms vs samples 57 ms. Narrow 1 d at 30 min steps: 90 vs 47 ms. Narrow 3.5 d: 258–279 vs 68–72 ms, reading 727 marks against 90. Narrow 6 h at a full partition: 55 vs 47 ms. The selector's rule looks only at where the range starts, not at how many series match.
- **Fill cycle (phase):** the cost of a time-clustered layout grows with the time since the newest partition began. At `t_end`, 6 minutes after a 5 h boundary, the recent table answered the wide instant query in 23 ms (10 marks). At a full partition it took 59–65 ms. Day partitions took 192 ms at 20 h into the UTC day and 224 ms on a full day. The samples table doesn't cycle within a month once a series has more than one granule of rows (after about 3 days).
- **Merged vs unmerged:** the copies `u_m`/`u_pd` take the last 66 minutes as 33 two-minute parts, with merges stopped. The newest data is then in its own small parts, so the samples table is as fast as the recent table: wide instant 705 → 27 ms, against 24 → 21 ms for the recent table. The 11× gap only exists after merges fold the newest rows into the month's big part. Ranges that reach the big part keep the gap (6 h wide: 1070 → 1105 ms). Unmerged parts cost narrow shapes +45–70% in every layout (recent: `r1h_n` 30 → 45 ms, `r6h_n` 47 → 74 ms), except 1 h narrow on the samples table (+12%).
- **At 8 threads** (`max_threads_min_free_memory_per_thread = 0`, `EXPLAIN PIPELINE` shows × 8): the ratios hold. Wide instant: recent 53 ms, samples 405 ms, day partitions 127 ms. 6 h wide: 280 / 594 / 290 ms. Narrow 7 d: samples 60–63 ms, 5 h partitions 303 ms, day partitions 110 ms.

## 3. Single-table candidates
Ratios against master's two-table setup measured in the same run (short ranges at a full partition; a ratio above 1 is slower). Storage covers the samples tables after the fill (14 days, 2000 series). Master's insert reference is 21–27 ms wall and 16–17 ms CPU per insert of one sample for each of 2000 series.

| candidate | wide instant | narrow instant | 1 h wide | 6 h wide | 6 h narrow | 1 d wide / narrow | 3.5 d wide / narrow | 7 d wide / narrow | 14 d wide / narrow | storage (master 656 MB) | PK in memory (master 366 KB) | insert wall / CPU |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| L1-5h `p5bg` (recent's layout everywhere) | 0.98 | 1.00 | 0.96 | 0.96 | 1.04 | 1.04 / 0.89 | 0.99 / 1.06 | 0.71 / **6.2** | 1.06 / **10.9** | 534 MB (0.81) | 534 KB | as `p5` (1 run): 0.62–0.63 / 0.66–0.72 |
| L1-day `pdbg` | **4.0** | 1.39 | 1.69 | 1.23 | 0.75 | 1.19 / 0.72 | 1.06 / 0.42 | 0.76 / 2.8 | 1.02 / 4.0 | 512 MB (0.78) | 530 KB | (one table) |
| L2-day `l2` (sub-row fold) | **4.2** | 1.39 | 1.71 | 1.27 | 0.80 | 1.26 / 0.72 | 1.06 / 0.45 | 0.75 / 2.5 | 0.98 / 3.7 | 509 MB (0.78) | 529 KB | (one table) |
| L2-5h `l2p5` | 1.06 | 1.00 | 0.97 | 0.99 | 1.04 | 1.00 / 0.97 | 1.00 / 0.99 | 0.70 / **6.9** | 1.02 / **10.4** | 531 MB (0.81) | 530 KB | (one table) |
| L3 `pd` (1 h buckets, day partitions) | **3.6** | 1.25 | 1.59 | 1.22 | 0.67 | 1.07 / 0.67 | 1.02 / 0.41 | 0.72 / 2.8 | 0.97 / 3.8 | 510 MB (0.78) | 261 KB | 0.58–0.81 / 0.65–0.73 |
| **L4 `lt1d` (labelled tiers, young 1 d)** | **0.93** | 1.05 | 1.03 | 0.99 | 0.98 | 0.84 / 0.93 | **1.70** / 0.53 | 1.01 / **1.77** | 1.03 / **1.57** | 530 MB (0.81) | 219 KB | 0.64–0.81 / 0.66–0.76 |
| tiered, two tables, each sample once (estimated) | 1.0 (same table) | 1.0 | 1.0 | 1.0 | 1.0 | — | — | 0.95–1.0 / 2.0 (young 1 d), 3.8 (young 3.5 d) | — | ≈ 0.8 | — | like L4 + a mover |

- Insert cost (`ins.sh`, through the TimeSeries sink, 2 runs). One sample per series: master 21–27 ms wall, 15.9–16.9 ms CPU, 189 KB written. One table: 13–22 ms, 11.4–12.8 ms, 123–124 KB. 40 samples per series: master 40–45 ms, 31–33 ms, 339 KB. One table: 23–31 ms, 20–23 ms, 189–196 KB. Each master insert also creates 2 parts to merge, not 1.
- Partition counts per year of retention: `p5*`/`l2p5` 1752, `pd*`/`l2` 365, `lt1d` 12 month partitions plus about 6 young ones.
- The tiered row comes from two sub-range queries on `m` (old part from the samples table, young part from the recent table). It is an estimate: the fixed cost is counted twice and the boundary is exact. v11 can't wire it: it refuses external targets and requires an `AggregatingMergeTree` inner samples table, so a `Merge` table over two tiers fails.
- **Storage note:** a first fill with periodic counter increments (`results/v1-*`) made short partitions look 41–82% bigger. ZSTD found the repetition only in long per-series runs. With noisy increments the penalty is 1.4% (day) and 5.7% (5 h). The timing ratios agreed between the two fills (v1: wide instant 66 ms recent, 672 ms samples).

## 4. L2 (sub-rows folded by `TTL GROUP BY`): works, buys nothing
- **Accepted:** `ORDER BY (id, bucket, sub)` with `TTL toDateTime(bucket) + 4 d 1 h GROUP BY id, bucket SET sub = min(sub), samples = timeSeriesGroupArray(samples), min_time = min(min_time), max_time = max(max_time)` is taken by the TimeSeries engine on master. The GROUP BY is a sorting-key prefix.
- **The sink fills `sub`** from `sub UInt8 DEFAULT intDiv(toUnixTimestamp(min_time) % 3600, 900)`. A row that spans slices (one insert covering :10–:20) keeps the slice of its first sample. Pruning is a little looser; results are unaffected.
- **The selector needs no change:** `bucket` stays the hour start, so its lower bound `bucket >= roundDown(min_time, 1 h)` covers every sub-row. With a 15-minute `bucket` that folds to 1 h, the 15-minute lower bound would miss folded rows; that variant needs a selector change.
- **Folding:** `OPTIMIZE ... FINAL` and `MATERIALIZE TTL` fold every expired slice (after the fill: 0 expired unfolded rows, 4 young hours unfolded in the boundary day). In the background a part folds only when a TTL merge runs: at most every `merge_with_ttl_timeout` (14400 s) per partition, so the boundary day partition is rewritten up to 6 times while it crosses the line. A folded part was re-merged once more by TTL at `merge_with_ttl_timeout = 1` and then left alone (watched for 20 s). An unmerged young part holds one row per (series, hour, slice) per insert.
- **Time ranges table and statistics:** the time-ranges table is per series and doesn't depend on the samples layout. It read the same rows in every layout. The v11 selector uses no column statistics on the samples tables.
- **Result:** L2 equals plain day or 5 h partitions on every shape, because the bucket width was never a factor (section 2). The fold only matters for storage of very long histories, and the measured storage difference was 0.2–0.6%.
- **Master defect seen:** `SET sub = min(toUInt8(0))` (a constant inside the aggregate) is accepted at `CREATE`, but every merge then fails with `NOT_FOUND_COLUMN_IN_BLOCK: Not found column toUInt8(0) in block`. `SET sub = min(sub)` works.

## 5. Recommended design: one table, partitions labelled by a column (L4)
**DDL (works on master today; `lt.sh`):**
```sql
SAMPLES INNER COLUMNS (... the default columns ...,
  tier UInt32 DEFAULT toUInt32(toStartOfInterval(toDateTime(bucket, 'UTC'), toIntervalHour(5))))
SAMPLES INNER ENGINE = AggregatingMergeTree PARTITION BY tier ORDER BY (id, bucket)
  SETTINGS index_granularity = 256, index_granularity_bytes = 262144
-- recent_samples_ttl_seconds = 0
```
**Mover** (run for each young partition whose 5 h interval ended more than `young` seconds ago):
```sql
INSERT INTO <samples> SELECT * REPLACE (toUInt32(toYYYYMM(toDateTime(bucket, 'UTC'))) AS tier) FROM <samples> WHERE _partition_id = '<id>';
ALTER TABLE <samples> DROP PARTITION ID '<id>';
```
- Each sample is stored once and written once at insert. The mover rewrites it once more, into a large part of the month partition; that replaces most of the small-part merges the samples table does today. Measured: 13 s for 12.4 days of the lab (624k rows) on 2 vCPU.
- **Results are identical** before, during and after a move. During the move every moved row is present twice; `timeSeriesGroupArray` and the grid functions deduplicate equal timestamps, as they do for unmerged parts.
- **Trap:** in the mover, filtering on `tier` reads the `REPLACE` alias, not the column, and moves nothing. My first run did that and then dropped the data. Filter on `_partition_id`.
- **The young window is the one knob.** Shorter (6 h, 2 partitions) brings narrow 7–14 d close to master (estimate: about 1.15× the marks) but loses the recent table's speed for 1 d wide dashboards. Longer (3.5 d, as today) keeps wide multi-day dashboards and makes narrow 7 d about 3.8× slower.

**Code it needs (a branch would contain):**
1. Normalizer (`normalizeTimeSeriesDefinitionImpl.cpp`), in a new version 12, or behind a setting such as `samples_young_window_seconds`: generate the `tier` column and `PARTITION BY tier` for the inner samples table, and default `recent_samples_ttl_seconds` to 0. Docs: "Recent samples table", "Retention" and the version history.
2. Mover: a background task of `StorageTimeSeries` (`BackgroundSchedulePool`) that runs the two statements above for each young partition older than the window. It inserts with a deduplication token per partition id, so a retry is idempotent, and drops only after the insert succeeded. On `Replicated*`/`Shared*` it runs on one replica (a Keeper lock); the insert and the drop replicate.
3. Sink (`TimeSeriesSink.cpp`): no change; the DEFAULT fills `tier`. The recent-table branch of the sink goes unused.
4. Selector (`StorageTimeSeriesSelector.cpp`): no change; it reads one table with 1 h buckets. Optional: drop the `RecentSamples` choice for tables without the target. Size: M (the mover), S for the rest.

Without engine code, a user can run L4 today with that DDL and the two mover statements from cron. The A/B plan uses exactly that.

## 6. Risks
- **Mover lag:** if the mover stops, young partitions pile up (4.8 a day). Narrow multi-day ranges drift toward the `p5` numbers (6–11×), and the partition count grows. Results stay correct. Monitor the count of young partitions.
- **Merge/TTL lag** (L2 only): folding waits for TTL merges, at most every 4 h per partition, and rewrites the boundary partition each time.
- **Part count:** inserts go to the current 5 h partition, plus one more part for a batch that straddles a boundary (as the recent table does today). Unmerged young parts cost narrow shapes +45–70% in every layout (section 2).
- **PK memory:** L4 keeps the samples table's 256 KiB granules (219 KB for the lab vs 366 KB for master's two tables). L1/L2 with 128 KiB granules double it (530 KB).
- **Replicated:** the mover must be single-runner. If an insert succeeds and its drop fails, the duplicates persist (harmless for results, extra storage) until the next pass. The partition labels are UTC, so the time-zone partition bug of upstream issue 120944 can't occur.
- **Out-of-order or backfilled samples** land in an old 5 h label and are moved on the next pass.
- **Partition pruning:** the partition key is `tier`, not `bucket`, so parts are skipped by primary-key analysis, not by the partition min/max (9 parts touched per long query in the lab vs 3).

## 7. Interplay with rollups (#16, `lilred-perf-rollup-samples`, decision pending; not changed)
The branch reads the recent table first and rollups after it, because rollups before the recent table made 1 d 5m/1h 0.72–0.74× slower in run 1. Under L4 there is no recent table. The branch's rule would key on the young window instead: raw rows for ranges inside it, rollups for older ranges with a suitable step. Rollups computed at insert exist in young rows too. The draft downsampling PR 128889 composes with L4: its `TTL ... GROUP BY id, bucket` rules apply in the month partitions, and its "first offset ≥ the recent TTL" check becomes "≥ the young window".

## Upstream overlap
- Upstream PR 115441 (merged 2026-08-22) added the recent table ("several times faster on large tables", no breakdown). Section 2 is that breakdown.
- Open: 123144 (parallel flush of the two tables: 410 → 345 ms wall but 411 → 494 ms CPU, the double write's cost); 121501 (skip expired recent writes); 123922 (`apply_ttl_delete_on_insert`); 128889 (downsampling by `TTL GROUP BY`). L4 makes 123144 and 121501 unnecessary for tables that adopt it. Open issue 120944 (recent table partition expression) doesn't apply to UTC labels.
- No commit touched `src/Storages/TimeSeries` or `StorageTimeSeries*` between `521c4bcb` and origin/master.

## How to reproduce
`bench/recent-samples-merge/README.md` (preconditions and every command). The core:
```bash
D=research/bench/recent-samples-merge; $D/server.sh start lab 19310; SERIES=2000 $D/gen.sh
EVAL_OFFSET=86400 $D/time.sh d3full short m,m#aa,m:0,mb,p5,p5b,p5bg,pd,pdbg,l2,l2p5 5
$D/lt.sh lt1d 86400 hash && HASH=1 $D/time.sh hash-lt all lt1d
$D/time.sh lt-ml r1d_sum,r1d_n,r3d_sum,r3d_n,r7d_sum,r7d_n,r14d_sum,r14d_n m,m#aa,lt1d,p5,pd 5
$D/server.sh stop lab
```
