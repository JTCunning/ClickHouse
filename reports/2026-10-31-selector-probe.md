# Selector whole-metric probe re-runs a nested SELECT on every query (#117956)
**Verdict:** Already fixed upstream for new tables. #117344 (merged 2026-10-28, in 26.11) skips the probe on the new bucketed samples schema. Only legacy tables (version ≤ 10) still pay for it, and the maintainer's direction is to retire it, so it's not worth a fix.
**Task:** 2026-10-31-selector-probe-re-runs-a-nested-select-o (scout, not a handoff) · **Build:** 26.11.1.974, git `a7f71361` · **Date:** 2026-10-31

## What happens (plain language)
For each selector in a PromQL query, the planner used to run a small extra query against the tags table: "does this selector match every series of the metric?" A yes lets the samples read use a cheap primary-key range. That probe cost roughly 4–5 ms of planning per selector on every evaluation, which is a big share of a ~20–50 ms dashboard query. Since #117344, tables with the new bucketed samples schema (version 11, now the default) no longer probe at all, because their samples table is small enough to filter by the id set directly. Tables created before 26.11 stay pinned at their old version, so they keep probing.

## Numbers
Dataset: the issue's fixture. 2,000 series of one metric, 256 samples each at 15 s (512k samples), 2 tags (`instance` with 2,000 values, `dc` with 8). It was loaded twice: `ts_v10` (`SETTINGS version = 10`, one sample per row, `index_granularity = 64` as in the issue) and `ts_v11` (default schema). Instant queries at t=1003830, `max_threads=2`, local server. Each cell is the median of 10 warm runs (the cold first run was discarded), on 2 vCPU.

| query (selectors) | table | median ms | min–max | plan build µs | read_rows | marks | memory |
|---|---|--:|--:|--:|--:|--:|--:|
| `sum(m)` (1) | v10 | 52 | 48–57 | 10,385 | 136,000 | 2,004 | 3.4 MB |
| `sum(m)` (1) | v11 | 43.5 | 40–48 | 5,761 | 8,000 | 18 | 17.6 MB |
| `m{dc="3"}` (1) | v10 | 35 | 33–37 | 10,543 | 24,000 | 254 | 1.4 MB |
| `m{dc="3"}` (1) | v11 | 37.5 | 31–43 | 6,469 | 8,000 | 18 | 16.1 MB |
| 3 selectors summed | v10 | 148.5 | 139–155 | 32,619 | 280,000 | 4,012 | 7.7 MB |
| 3 selectors summed | v11 | 117 | 105–129 | 18,318 | 24,000 | 54 | 20.9 MB |

- Planning time (`QueryPlanBuildMicroseconds`) falls 40–45% on v11, and it scales with the selector count (3 selectors: 32.6 → 18.3 ms). That is the same size as the saving the issue's caching patch claimed (29.5 → 16.3 ms on 3 selectors), and it comes without a cache.
- Both tables return identical results.
- Wall time isn't a clean A/B here, because the schemas also read differently (v11 reads about 10× fewer rows and marks).
- Side observation, not investigated: v11 instant queries peak at 5–12× more memory (16–21 MB vs 1.4–7.7 MB). The likely cause is decoding whole one-hour sample buckets. That's small in absolute terms, but it's worth a look at high series counts in a future scout.

## Why (cause)
- On master, `StorageTimeSeriesSelector.cpp` (around line 1306) calls `tryMakeWholeMetricIDRangeConditions` only `if (!time_series_settings->hasBucketedSamples())`. The comment there gives the reason: on bucketed tables, the id set is cheap to use for the primary-key analysis, "while the probe would cost a query per selector".
- `hasBucketedSamples()` means `version >= 11` (`TimeSeriesSettings.cpp:147`). The version is pinned when a table is created and can't be changed afterwards, so existing tables never get the new path.

## Proposed fix
Report only. A fix for legacy tables would need either #118116's verdict cache or the "direct read" rework discussed in the issue. Neither fits "simple": the first adds process-wide cached state, and the second adds a second read path. Both go against the maintainer's stated plan. The better path for legacy users is a documented way to move to version 11; there doesn't seem to be one yet (an in-place upgrade isn't possible, because the version is pinned).

## Upstream overlap
- **#117344** (vitlibar, merged 2026-10-28): adds the bucketed schema and removes the probe for it. This is the fix.
- **#118116** (groeneai, open since 2026-09, last activity 2026-09-24): caches only the "no, not the whole metric" verdict. Its soundness argument holds: reusing that verdict emits exactly the SQL master emits. Now it helps only v≤10 tables and was written against code that has changed a lot since (the time-ranges join, buckets), so it most likely needs a rebase. I expect it to be closed as superseded.
- **#127817** (James, closed 2026-10-26): ran the probe locally under parallel replicas. vitlibar: "We'll stop using the whole-metric probe completely soon in #117344". **#127799** (valerypetrov, closed the same day, apparently for the same reason) skipped the time-ranges join inside the probe.
- **#119092** (merged 2026-09-10): stopped evaluating the probe's id range per row.
- James's fork has three related branches, none with an upstream PR:
  - `jtcunning/timeseries-whole-metric-probe-once-per-query` (1 commit, 2026-10-26): runs the probe once per query. It's now obsolete for v11 tables, where the probe never runs. On v≤10 it would still save the repeat inside one query, but not across dashboard refreshes. I'd retire it unless legacy tables matter to a customer.
  - `jtcunning/promql-selector-whole-metric-drop-id-set` (4 commits, 2026-10-24): filters a whole-metric selector by the id range, and runs probe and tags queries on a thread pool. It sits behind the same `!hasBucketedSamples()` gate, so it has the same status.
  - `jtcunning/promql-share-identical-instant-selectors` (2026-10-21): shares one grid between identical selectors. That's independent of the probe and still relevant: it saves samples reads on v11 too. It's 3,884 commits behind master.
  - The branch of #127817 is `jtcunning/timeseries-whole-metric-probe-local`.

## How to reproduce
```sql
SET enable_time_series_table = 1;
CREATE TABLE ts_v11 ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, UUID));
CREATE TABLE ts_v10 ENGINE = TimeSeries SETTINGS version = 10
    SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp) SETTINGS index_granularity = 64
    TAGS INNER COLUMNS (id Tuple(UInt64, UUID));
-- same INSERT into each (note: the column is `samples` now, not `time_series`)
INSERT INTO ts_v11 (metric_name, tags, samples)
SELECT 'node_cpu_seconds_total', map('instance', toString(number), 'dc', toString(number % 8)),
       arrayMap(step -> (toDateTime64(1000000 + step * 15, 3), (sipHash64(number, step) % 1000000) / 1000), range(256))
FROM numbers_mt(2000);
```
Run each query 11 times with `--log_comment`, for example `SELECT * FROM prometheusQuery('ts_v10', 'sum(node_cpu_seconds_total)', 1003830) FORMAT Null`. Then read `system.query_log` (`QueryFinish`, skip the first run) for `query_duration_ms`, `ProfileEvents['QueryPlanBuildMicroseconds']`, `read_rows`, `ProfileEvents['SelectedMarks']` and `memory_usage`.
