# Brief: what background merges cost a TimeSeries table at steady state

- **ID:** `merge-steady-state` · **Area:** merge · **Written:** 2026-11-24 by lil red, against master `05f159a69597`
- **Tracking issue:** none yet (see README, "Where the result goes")
- **Spend:** James's cap. Stop points below.

## Question

Under continuous realistic ingest, what do background merges cost per sample, for each inner table? How many times is a sample rewritten before its part stops merging? Which layout or settings changes cut that cost without slowing realistic queries?

## Why it matters

- Merges were about half of ingest CPU in lil red's only baseline (2 vCPU, one sender).
- Merge lag is also a query cost. The id lookup's time grows with the number of unmerged parts and with the rows per series in the time ranges table's tail (`reports/2026-11-14-idlookup-parts-and-rows.md`: 5.5 rows per live series over a 94-part tail).
- lil red could never make merges lag on its small machine, so it couldn't test any merge setting. This brief needs a machine that can.

## What lil red already knows (verify, don't trust)

- **Inner tables on master** (`src/Storages/TimeSeries/normalizeTimeSeriesDefinitionImpl.cpp`, `getDefaultInnerEngineKind`):

  | table | engine | rows | buckets |
  |---|---|---|---|
  | samples | `AggregatingMergeTree` | one per (series id, bucket); `samples` is `SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp, value)))` | 1 h (`samples_bucket_step_seconds`) |
  | recent samples | same engine and rows | same | 15 min, with a 4-day TTL |
  | time ranges | `AggregatingMergeTree` | | |
  | tags, metric families | `ReplacingMergeTree` | | |

  - The default codecs are `Delta, T64, ZSTD(3)` for the timestamps and `ALP, ZSTD(4)` for the values (with bucketed samples).
  - Samples tables use 256 rows per granule and 256 KiB `index_granularity_bytes`.
  - Samples partitions are monthly.
- **The merge function** (`src/AggregateFunctions/TimeSeries/AggregateFunctionTimeSeriesGroupArray.h`, `merge`): it merges two sorted sample arrays and deduplicates timestamps. It re-sorts an unsorted side first. Check whether it has a fast path for two states whose time ranges don't overlap (the common case: an older and a newer piece of the same hour). If it doesn't, that is a candidate.
- **Write amplification, a hypothesis (UNCONFIRMED):** at a 15 s scrape, an hour row gets about 240 pieces, one per insert. Its array is rewritten at every merge level until the hour closes and its parts stop merging, so amplification is about the merge depth. Measure it.
- `timeSeriesGroupArray`'s result copy was 15–26% of a samples-table merge's CPU. Fixed by upstream PR 133502, merged 2026-11-23: on 2026-11-21 it gave −12.1% merge CPU and −10.8 to −14.1% on `FINAL`.
- **Recent samples vs one table** (`reports/2026-11-22-recent-samples-merge.md`):
  - The recent table is fast only because its 5 h partitions keep each series' newest rows together.
  - A single table with partitions labelled by a `tier` column, plus a mover (`INSERT SELECT` into the month tier, then `DROP PARTITION`), matched it on short queries. Storage was 0.81× and insert CPU 0.66–0.76×.
  - The mover is a merge-like cost that nobody has measured at scale.
- On master, a TTL `GROUP BY ... SET x = min(<constant>)` is accepted at `CREATE`, then its merges fail with `NOT_FOUND_COLUMN_IN_BLOCK`. Only note it if you hit it. James's draft upstream PR 128889 (`samples_downsampling_periods`) uses TTL `GROUP BY` rules, so it's adjacent.

## Already in flight upstream (re-check each)

133907 (a test for merging re-sent samples), 133502 (merged: result copy), 122983 (draft: one sorted buffer in the rate grid functions), 121501, 123144, 128889 (James's draft: downsampling by TTL), 124777, 131989.

## Method

1. **Steady state.**
   - Use the ingest setup from `ingest-at-scale.md`: real Prometheus senders, 15 s scrape, 1M active series or as many as the machine allows, 1–5% churn per hour.
   - Run at least 3 hours, so that hour buckets close and their parts stop merging.
   - Record the server's background pool size. Leave it at the default for the baseline.
2. **Per inner table, from `system.part_log` and `system.trace_log`:**
   - merge CPU per inserted sample
   - bytes written by merges ÷ bytes inserted (write amplification), and the same for rows
   - merges per hour
   - active parts over time, and how long a part lives before it settles
   - the time ranges table's rows per live series in the unmerged tail (the id lookup's cost driver)
3. **Profile the samples merge.** Look at where `timeSeriesGroupArray` merge, serialization, the codecs (`Delta, T64` and `ALP`) and ZSTD spend their time. `ALP` encoding re-runs its search per granule (upstream PR 131497 is open), so decide whether to apply that PR for this run.
4. **Levers.** One at a time, each against the same steady state:
   - `samples_bucket_step_seconds` at 900, 3600 and 7200
   - the recent samples table off (`recent_samples_ttl_seconds = 0`)
   - async insert flush size: bigger inserts make fewer parts
   - merge settings that change how many parts a merge takes and when small parts merge, for example the MergeTree `merge_selector_*` settings and `min_age_to_force_merge_seconds`. Check the names on master.
   - the labelled-tiers table from `bench/recent-samples-merge`, with its mover running on a schedule

   Judge each lever on four things: merge CPU per sample, write amplification, the size of the unmerged tail, and the dashboard and alert-rule query set from `dashboard-query-profile.md` (same query text on both sides, full query in each row).
5. **Stop point 1.** If no lever moves merge CPU or the unmerged tail by at least 10% without costing the query set more than its noise floor, stop and report.
6. **Code candidate.** If the merge function lacks a non-overlap fast path, or the profile shows another single cost of at least 10% of merge CPU, prototype it and A/B it on the steady state. Report merge CPU per sample, plus the query set as a guard.

## Deliverables

- A table per inner table: merge CPU µs/sample | write amplification (bytes, rows) | merges/h | median part lifetime | tail rows/series.
- A lever table: lever | each of those columns | query set change.
- A one-line verdict on the write-amplification hypothesis, with the measured number.
- Optional: a prototype with its A/B.

## Out of scope

Replicated or shared engines: note in one line if a finding obviously depends on them. Correctness bugs: one line under "Anything odd".
