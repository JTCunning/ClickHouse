# Skip the tags read for label-free aggregations (handoff #8)

**Verdict:** worth doing, but not as a simple one-mechanism fix, so this is a report only. The handoff's group-0 shortcut gives wrong values. The biggest saving (about 30% of CPU) comes from the `__name__` drop step, not the tags read, and the open #123419 already removes that step. Skipping the tags read itself saves another ~19% of CPU and ~27% of peak memory, but doing it correctly needs a per-series key without the tags collector, so it's a multi-file change.
**Task:** 2026-10-31-handoff-skip-the-tags-read-for-label-fr, https://github.com/JTCunning/ClickHouse/issues/8 · **Build:** 26.11.1.974, master `a7f713612c8` (prebuilt) · **Upstream HEAD checked:** `1ee49128da7` · **Date:** 2026-11-01

## What happens (plain language)
`sum(rate(metric_0[5m]))` does three jobs before the final sum:
1. Every matching series' tag Map is read and registered in the per-query tags collector.
2. `rate()` runs once per tag set (a "group").
3. `rate()` drops `__name__`. That step is a `GROUP BY` over the new tag sets, with a duplicate-series check (`HAVING timeSeriesThrowDuplicateSeriesIf(...)`), which keeps master's behaviour of raising an error when two series collide.

The final `sum()` without `by` throws all labels away, so the work in steps 1 and 3 is wasted. Step 3 is the more expensive of the two.

## Numbers
Dataset: 200,000 series in one TimeSeries table (schema v11, bucketed samples), 40 samples each at a 15 s interval (10 min). `metric_0` has 100,000 series; `metric_1`…`metric_49` have ~2,000 each. Each series has 6 tags (job 10, instance 500, cluster 5, namespace 20, pod unique, mode 8 values), and every inner table was compacted with `OPTIMIZE … FINAL`.
Settings: local server on 127.0.0.1, `max_threads=2`, query cache off. Each case ran 3 rounds of 7 runs, interleaved across cases. The first run of each round is dropped, and the figure is the median of the round medians. Wall time on this shared 2-vCPU host is noisy (±20%), so **CPU (`UserTimeMicroseconds`) is the figure to trust**.

The cases are hand-written SQL, emulating what the converter would generate, because no build is possible here. `hand: master SQL` reproduces master's generated SQL by hand: it gives the same CPU and the same values as `prometheusQuery`, which validates the method.

`sum(rate(metric_0[5m]))`, 100,000 series:

| case | wall median | CPU median | read_rows | read | peak memory | value |
|---|---|---|---|---|---|---|
| master `prometheusQuery` | 357 ms | 352 ms | 321,504 | 84.6 MiB | 132–172 MiB | 6666.67 |
| hand: master SQL | 424 ms | 357 ms | 321,504 | 84.7 MiB | 173 MiB | 6666.67 |
| hand: tags kept, **no `__name__` drop step** | 352 ms | 234 ms (−34%) | 321,504 | 84.7 MiB | 172 MiB | 6666.67 |
| hand: tags kept, grouped by `id`, no drop step | 359 ms | 246 ms | 321,504 | 84.7 MiB | 184 MiB | 6666.67 |
| hand: **`SELECT id` only** (no tags read or registration), grouped by `id`, no drop step | 307 ms | **179 ms (−49%)** | 321,504 | 73.7 MiB | **126 MiB** | 6666.67 |
| master `sum by (job, cluster)` (control) | 400 ms | 417 ms | 321,504 | 84.6 MiB | 132 MiB | — |
| hand: handoff idea 4, every series in group 0 | — | — | — | — | — | **0.0667 (wrong)** |

The drop step on its own (same query, only step 3 changes; 3×6 warm runs):

| step 3 form | CPU median |
|---|---|
| `GROUP BY new_group HAVING timeSeriesThrowDuplicateSeriesIf(...)` (master) | 323 ms |
| `timeSeriesRemoveTag(group, '__name__')` as a plain projection | 227 ms |
| marker-tag projection, as on the #123419 branch | 233 ms |
| no step 3 | 223 ms |

The narrow metric `metric_1` (~2,000 series) gets the same split, but the absolute numbers are small: master 19.6 ms CPU, `SELECT id` only 13.4 ms.

## Why (cause)
- **The `__name__` drop step costs ~100 ms CPU of 352 (~30%).** `dropMetricName` wraps the grid in a `GROUP BY timeSeriesRemoveTag(group, '__name__')` and checks every group for duplicate series. The aggregation over 100k groups and the check can't be skipped. In the projection forms, the analyzer prunes the unused `group` column under a label-free `sum()`, so they cost nothing.
  - Code: `src/Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.cpp:53-80`, called from `applyFunctionOverRange.cpp:338`.
- **The tags read and registration cost ~67 ms CPU (~19%), ~46 MiB of peak memory (−27%) and 11 MiB of reads.** That covers `timeSeriesStoreTags(id, tags, '__name__', metric_name)` in the lookup subquery, plus the collector holding 100k tag sets until the query ends. Grouping by `id` instead of by tag set costs about the same (234 vs 246 ms), so the saving comes from not reading and registering, not from the grouping key.
  - Code: `src/Storages/StorageTimeSeriesSelector.cpp:619-648` (`makeSelectQueryFromTagsTable`) and `fromSelector.cpp:52` (`timeSeriesIdToGroup(id) AS group`).
- **Why idea 4 (map every unregistered id to group 0) is wrong.** `rate()` and every other range function aggregate per `group` (`applyFunctionOverRange.cpp:311`). With a single group, the samples of all 100k series go into one rate computation, and the result is off by a factor of 100,000. A label-free selector still needs one key per series.

## Proposed fix
Two independent pieces. Neither one is a simple single-mechanism diff, so there's no branch.

1. **Drop step (the larger win). This is already your #123419.** On `jtcunning/promql-delayed-metric-name-removal`, `dropMetricName` becomes a projection that adds a marker tag, and the check moves to `finalizeSQL`, which runs on the final result (one row for a plain `sum`). In the emulation that removes ~30% of CPU from `sum(rate(metric_0[5m]))`, and part of the same cost from `sum by (...)`. The PR is filed as an Experimental Feature (compliance). Adding these numbers as a "Performance" note would strengthen it. No separate fix is needed here.
2. **Label-free selector mode (the remaining ~19% CPU and ~27% memory).** Doing it correctly needs all of the following:
   - **A label-demand pass** over `PrometheusQueryTree`, marking selectors whose consumers up to the root drop every label (the handoff's list of operators).
   - **A selector option** on `timeSeriesSelector` (a new table-function argument, so a public interface change) that makes `makeSelectQueryFromTagsTable` emit `SELECT id`.
   - **A per-series key without the collector:** carry `id` instead of `group` through the range functions, then emit a constant no-tags group in the label-free aggregation. On master, each range function builds its own `GROUP BY group`. Upstream #126221 (your in-order aggregation) already carries `id` through the raw data and groups by `id`, so building on it is much smaller than building on master.
   - **Ids sharing one tag set:** these occur after `ALTER … MODIFY SETTING id_generator` on a table with data. They must still be merged before `rate()`, and finding them reads the tags. #126221's `ids_sharing_tags` mode does that, so this path keeps a cheaper tags query, not none.
   - **Multi-name selectors** (`{job="x"}`, `{__name__=~"a|b"}`): on master, two series that differ only by name raise the duplicate-series error. Either limit the mode to selectors with a `__name__` equality matcher, where dropping the name can't create collisions, or land #123419 first, which makes that error a final-result check.

   Effort: M (4–6 files, one new table-function option). Expected effect on top of #123419: ~−65 ms CPU and ~−46 MiB per 100k-series label-free aggregation; reads −13% here, more with wider tag Maps.

## Upstream overlap
- **#123419** (yours, open): removes the drop-step cost as a side effect (see above). Read-only check.
- **#126221** (yours, open; branch `jtcunning/promql-range-aggregation-in-order`): carries `id` and groups `rate()` by `id`, and handles ids that share a tag set. It's the natural base for piece 2. It still registers the tags today.
- **#115223** and handoff #3: make each registration cheaper, while piece 2 removes it for label-free shapes, so the two complement each other.
- **#126492:** collector capture for parallel replicas; it doesn't touch this path.
- No open PR or fork branch emits an id-only tags lookup. Searches covered `timeSeriesStoreTags`, tags collector, label demand, labels not needed, skip tags, and the `comp-promql` open list. Branches checked: `promql-planner-speedup`, `promql-fuse-agg-pair`, `prom-labels-direct-tags`, `promql-one-step-sql-reshape`, `promql-share-identical-instant-selectors`, `promql-range-aggregation-in-order`, `promql-delayed-metric-name-removal`.

## How to reproduce
Scripts: `bench/skip-tags-label-free/` on this branch (`bench.py` for the A/B cases, `bench_drop.py` for the drop-step split, raw results in `*.jsonl`).

```sql
-- server on 127.0.0.1:19000 with enable_time_series_table=1; T_END = now rounded down to 900 s, T0 = T_END - 600
CREATE TABLE ts ENGINE = TimeSeries;
INSERT INTO ts (metric_name, tags, samples)
WITH cityHash64(number) AS h
SELECT concat('metric_', toString(if(number < 100000, 0, (number % 49) + 1))),
       map('job', concat('job', toString(h % 10)), 'instance', concat('host-', toString(intDiv(h, 10) % 500), ':9100'),
           'cluster', concat('c', toString(intDiv(h, 5000) % 5)), 'namespace', concat('ns', toString(intDiv(h, 25000) % 20)),
           'pod', concat('pod-', toString(number)),
           'mode', ['user','system','idle','iowait','nice','irq','softirq','steal'][(intDiv(h, 500000) % 8) + 1]),
       arrayMap(i -> (toDateTime64(T0 + i * 15, 3), toFloat64((number % 1000) + i)), range(40))
FROM numbers(200000) SETTINGS max_insert_block_size = 65536, max_threads = 2;
-- then OPTIMIZE TABLE <each .inner_id.* table> FINAL
```
Then run `python3 bench.py metric_0`, `python3 bench.py metric_1` and `python3 bench_drop.py metric_0`, with `t_end` holding T_END.
