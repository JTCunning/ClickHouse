# Read parallelism of the tags subquery (handoff #9)

**Verdict:** not worth a settings change. In-order reading and the merge join are not what holds the tags subquery back: no alternative setting gives a lower wall time beyond noise, and turning in-order reading off is 10–40% slower. The serial part that matters is the tags collector, which takes an exclusive lock for every series it hasn't seen yet, and that lock is outside `StorageTimeSeriesSelector.cpp`.
**Task:** 2026-10-31-handoff-measure-and-raise-read-parallel, https://github.com/JTCunning/ClickHouse/issues/9 · **Build:** 26.11.1.974, `a7f713612c8` (same as the handoff) · **Date:** 2026-11-01

## What happens (plain language)
The tags subquery reads the matching series from the tags table, joins them by `id` with the time ranges table to keep only series active in the window, and passes each surviving series to `timeSeriesStoreTags`. That function registers the series' tags with the query's tags collector, and its output becomes the `id IN (...)` set.
- **Named metric** (`metric_0`): the read pool gives each side of the join one in-order stream at any `max_threads`, and the merge join runs on one thread.
- **No metric name** (`{job="job3"}`, `{__name__=~"metric_1.*"}`): the tags table is ordered by `(metric_name, id)`, so its rows aren't in `id` order. The planner puts the time ranges table on the in-order side and fully sorts the tags side before the merge.

None of that is the main cost. Once the subquery's output is actually used, 49–67% of its time is spent in the collector, and the collector serializes all writers.

## Numbers
Dataset: the handoff's generator: 1,000,000 series over 200 metrics; `metric_0` has 100,000 series, the others ~4,500 each; 6 labels; `job` has 10 values, independent of the metric. Two changes to fit this machine: **20 samples per series** (15 s apart, last 300 s) instead of 240, and all four inner tables merged to one part. Host: 2 vCPU / 4 GiB VM, so `max_threads` is 1 or 2 (the MergeTree read pool caps streams at 2 here even at `max_threads=16`). Each cell is the warm median of 6–7 runs, with variants interleaved. All variants return identical results (checked by a hash of the output).

### 1. The handoff's sweep: the tags subquery alone, as `readImpl` runs it
The subquery runs with the settings `readImpl` adds on v11 tables: `merge_tree_min_bytes_for_concurrent_read = 4 MiB`, `merge_tree_min_rows_for_concurrent_read = 0`. Cells are wall ms; memory is the peak.

| shape (series) | max_threads | **current**: merge join, in order | merge join, not in order | hash, in order | hash, not in order | parallel_hash |
|---|---|---|---|---|---|---|
| `metric_1{job="job3"}` (502) | 2 | **12** | 12 | 13 | 13 | 17 |
| `metric_0` (100,000) | 1 | **148** | 184 | 154 | 152 | 157 |
| `metric_0` | 2 | **120** | 182 | 129 | 154 | 170 |
| `metric_0` memory | 2 | **61 MiB** | 86 MiB | 88 MiB | 87 MiB | 97 MiB |
| `{job="job3"}` (100,118) | 1 | **488** | 646 | 502 | 522 | 534 |
| `{job="job3"}` | 2 | **377** (318–495) | 538 | 343 (324–510) | 514 | 347 |
| `{__name__=~"metric_1.*"}` (502,050) | 1 | **1,116** | 1,252 | 1,032 | 1,038 | 1,078 |
| `{__name__=~"metric_1.*"}` | 2 | **1,142** | 1,266 | 1,064 | 1,063 | 1,147 |
| name regex memory | 2 | **343 MiB** | 394 MiB | 389 MiB | 389 MiB | 397 MiB |

- **Turning in-order reading off** is slower on every wide shape (+11–51%) and uses 1.2–1.5× the memory: both sides then have to be sorted.
- **Hash join** is 5–8% slower on `metric_0`, which is the case idea 3 targets. It is 7–9% faster on the two shapes without a metric name, but with overlapping min–max ranges, the same CPU, and 1.1–1.5× the memory. That is not enough to justify an adaptive switch and a new row-estimate threshold.
- **The concurrent-read thresholds** (`merge_tree_min_rows/bytes_for_concurrent_read`, `max_streams_for_merge_tree_reading`) don't change the stream count in `EXPLAIN PIPELINE` for any shape (idea 2).

### 2. Where the time goes (`max_threads = 1`)
| shape | tags subquery (current) | without the time ranges join | join, `id` only, no collector | collector share | join + time ranges read share |
|---|---|---|---|---|---|
| `metric_0` | 156 ms | 103 ms | 52 ms | ~67% | ~34% |
| `{job="job3"}` | 480 ms | 287 ms | 247 ms | ~49% | ~40% |
| `{__name__=~"metric_1.*"}` | 1,013 ms | 667 ms | 399 ms | ~61% | ~34% |

The join's share matches the handoff's 25–55%. The collector's share is the new part.

**Evidence that the collector serializes:** without the join, the name-regex shape reads with 2 streams (`ReadPool × 2`), yet 2 threads take 716 ms against 667 ms on 1, and CPU stays flat (669 against 644 ms). The second thread waits on a lock instead of working. Across all variants in table 1, that shape never runs faster on 2 threads than on 1.

### 3. The full selector query, for proportion
| shape | full selector, mt=1 | mt=2 | tags subquery share (mt=1) |
|---|---|---|---|
| `metric_0` | 232 ms | 146 ms | ~64% |
| `{job="job3"}` | 1,021 ms | 876 ms | ~48% |
| `{__name__=~"metric_1.*"}` | 1,523 ms | 1,510 ms | ~73% |

With 20 samples per series, the samples read is smaller here than in the handoff (240), so these shares are upper bounds. They are still far above the handoff's "at most 18%". See the measurement note.

### Measurement note: `count()` hides the collector
The handoff's tags-only query is `SELECT count() FROM (<subquery>)`. Under `count()`, the analyzer drops the unused `timeSeriesStoreTags(...)` column, so the collector never runs: `count()` over the subquery without the join takes 3 ms on every shape. With `count()`, the current subquery takes 46 / 243 / 412 ms here. With its output consumed (`SELECT sum(sipHash64(x)) FROM (<subquery with AS x>)`), it takes 148 / 488 / 1,116 ms. The handoff's 24 / 50 / 97 ms are therefore probably join-and-read only. With the collector included, the "15–50 ms upper bound per wide query" is too low. The bench scripts here use the consuming form.

## Why (cause)
1. **Collector lock.** `ContextTimeSeriesTagsCollector` looks ids up under a shared lock. Every id not yet stored then goes in under one exclusive `std::lock_guard` for the whole block (`src/Interpreters/ContextTimeSeriesTagsCollector.cpp:1205–1260`; the group map is the same at `:933–985`). In a selector, every matching series is new to the collector, so every row takes the exclusive path, and all reading streams queue on one mutex. Cost grows with the number of matching series.
2. **The merge join is one thread by construction** (`MergeJoinTransform 2 → 1`), and a named metric reads each side with one in-order stream. That is a design trade-off: no hash table, early stop. Measured against a hash join it costs ~nothing at 2 threads.
3. **Selectors without a metric name** sort the whole tags side by `id`, because the tags table is ordered by `(metric_name, id)` (`.inner_id.tags` schema). That sort is part of the ~34–40% join share.

Pointers: `src/Storages/StorageTimeSeriesSelector.cpp:1167` (`applySettingsForSelectIDsQuery`), `:498` (`addTimeRangesTableJoin`), `:1284–1295` (`readImpl`'s read thresholds; the tags subquery shares that context, so it already gets the same thresholds).

## Proposed fix
Report only: no change inside `StorageTimeSeriesSelector.cpp` meets "lower tags-only wall, CPU ≤1.3×, identical rows". What a real fix would need:
- **Collector (the larger lever, M):** stop serializing first-time inserts. Options: shard the id map and group map by hash, with one mutex per shard; or have each stream build its id→tags mapping locally and merge once at the end of the subquery. The second may need care, because `timeSeriesStoreTags` returns an id that later stages resolve. Expected effect: the name-regex shape scales with threads instead of staying at 1 thread; the ceiling is the 49–67% collector share. This is the same code as handoff #3 and upstream #115223 (which materializes tags only for unique groups and doesn't change the locking). Best done together with #3.
- **Join (smaller, M–L):** for selectors without a metric name, the full sort of the tags side is unavoidable with the current sort key. Only a different join strategy for that case would help, and the measured gain is ≤9% here.
- **Not measurable on this host:** scaling past 2 cores. If James wants the 16-thread picture, the scripts below run unchanged on a bigger machine: `python3 sweep.py 7 0,1,2,3 1,4,16`.

## Upstream overlap
- #127847 (open, valerypetrov): `LEFT ANY JOIN` for the time ranges table. It fixes repeats from unmerged time-range parts; with one merged part, it doesn't change these numbers. It's compatible with everything here.
- #115223 (open): the collector's materialization of unique groups. The collector's lock is the natural next step there.
- Recent master commits on the selector (`91ed01c747e`, `97aef902422`) introduced the time ranges join and its id-range bounds; nothing open changes `applySettingsForSelectIDsQuery`.
- James's fork branches: none touch the join settings. `lilred/perf-time-ranges-id-ranges-index-hint` (handoff #6) narrows the time-ranges read for name alternatives, which is orthogonal.

## How to reproduce
Scripts and raw results: `bench/tags-read-parallelism/` on `lilred/research`.
```bash
# server on 127.0.0.1:19000 with query_log and text_log (config.xml); load the handoff's generator with range(20) samples, OPTIMIZE ... FINAL on each .inner_id.* table
python3 gen.py && python3 derive.py # extract the logged tags subquery per shape; derive the no-join and id-only variants
python3 sweep.py 7 0,1,2,3 1,2      # table 1: join algorithm × optimize_read_in_order × max_threads
python3 sweep2.py 6 0,1,2,3 1,2     # table 2: current vs no join vs id onlypython3 full.py                     # table 3: full selector queries
```
Pipeline shape: `EXPLAIN PIPELINE SELECT sum(sipHash64(x)) FROM (<subquery>) SETTINGS join_algorithm='full_sorting_merge', optimize_read_in_order=1, max_threads=16`.
