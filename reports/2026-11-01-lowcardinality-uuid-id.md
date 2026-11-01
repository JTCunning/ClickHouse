# TimeSeries id: LowCardinality(UUID) no longer pays for itself on the bucketed (v11) schema
**Verdict:** not worth keeping as the default. On v11 it saves nothing on disk, is 0–5% slower on wide reads, 8–22% slower on narrow reads and 1.5–1.75× slower on the in-tree `promql_selector_pk_range` benchmark, on top of ~7% ingest CPU. Recommend switching new tables back to `Tuple(UInt64, UUID)`. That's a maintainer decision about the default schema, so this is a report, not a branch.
**Task:** 2026-10-31-timeseries-id-what-lowcardinalityuuid (scout, from the [remote-write baseline](2026-11-01-remote-write-baseline.md)) · **Build:** 26.11.1.974, a7f713612c80 · **Date:** 2026-11-01

## What happens (plain language)
New TimeSeries tables store each series id as a pair: a hash of the metric name and a dictionary-encoded (`LowCardinality`) 128-bit hash of all tags. #117033 (2026-08-31) made this the default because, on the schema of the time, the samples table had **one row per sample**: the id was 24 of ~40 bytes per row, and dictionary encoding cut those bytes by ~58% (1.16× geomean on their 38-query suite). Since #117344 (2026-10-28, v11) the samples table has **one row per series per time bucket** holding an array of samples. So the id repeats 16× less often in the recent-samples table and ~120× less in the long-term one. Now it's about 3% of the bytes, and plain repeated UUIDs already compress well. Encoding saves nothing measurable, but the dictionary still has to be loaded and rebuilt on every insert, merge and scattered read.

## Numbers
Dataset: 150k series (100k in `metric_0`, 50k over 49 more metrics), 6 labels (pod unique), 480 samples per series every 30 s over 4 h, `OPTIMIZE FINAL`. The recent-samples table has 2.4M rows (900 s buckets, 16 rows/series), the samples table 750k (3600 s buckets). 2 vCPU, `max_threads = 2`, median of 5 warm interleaved runs. Results are identical between schemas (up to float summation order).

**On disk** (id column, compressed / in memory after decompression):

| inner table | LowCardinality id | plain UUID id |
|---|---|---|
| recent samples | 3.13 MiB / 25 MiB | **2.89** MiB / 55 MiB |
| samples | 2.92 MiB / 9.5 MiB | **2.81** MiB / 17 MiB |
| (samples column, for scale) | 89 MiB / 1.09 GiB | 90 MiB / 1.09 GiB |

**Own cases** (LC / plain, median ms; ratio > 1 means LC is slower):

| query | LC | plain | LC/plain time | LC/plain CPU | memory LC/plain |
|---|---|---|---|---|---|
| `metric_0` instant, 100k series | 944 | 934 | 1.01 | 1.02 | 120 / 119 MiB |
| `sum(rate(metric_0[5m]))` instant | 1374 | 1374 | 1.00 | 0.99 | 161 / 161 MiB |
| `sum by (job) (rate(metric_0[5m]))`, 4 h range, step 60 | 18574 | 18153 | 1.02 | 1.02 | 1006 / 1005 MiB |
| `rate(metric_0[5m])` 1 h range, step 60 | 3701 | 3560 | 1.04 | 1.02 | 766 / 765 MiB |
| `timeSeriesSelector` raw scan, 4 h | 1497 | 1432 | 1.05 | 1.01 | 72 / 72 MiB |
| `metric_0 + metric_0` | 2805 | 2760 | 1.02 | 1.00 | 190 / 189 MiB |
| `sum(rate(metric_0{job="job3"}[5m]))` (10%) | 1135 | 1077 | 1.05 | 1.08 | **27 / 30** MiB |
| `sum(rate(metric_7[5m]))` (~1k series) | 35 | 32 | 1.09 | 1.08 | 7.3 / 7.8 MiB |
| `metric_0{pod="pod-5"}` (1 series) | 28 | 23 | **1.22** | 1.23 | 5.3 / 5.3 MiB |

Uncompressed bytes read are 2.5% lower with LC on every case (815 vs 836 MiB on the wide ones). That's the only read-side gain, and it doesn't show in time.

**In-tree perf tests** (each test's own table and data, created with both id types; LC/plain median time):

| test | queries | LC/plain |
|---|---|---|
| `promql_selector_pk_range` | `pkr_metric`, `sum(pkr_metric{dc=~".*"})`, `sum(rate(pkr_metric[30m]))`, `sum(pkr_metric{dc="0"})` | **1.73, 1.75, 1.52, 1.66** (679 vs 393 ms on the first) |
| `promql_tags_transform` (TimeSeries part) | 3 queries | 1.10, 1.12, 1.08 |
| `promql_topk` | topk / bottomk / topk by / limitk | 1.05, 1.06, 1.05, 0.95 |
| `promql_set_operator_presence_mask` | and / or / unless, dense and sparse | 0.99–1.08 |
| `promql_selector_id_range_row_cost` | 3 queries | 1.00, 1.01, 1.03 |

No case is meaningfully faster with LC. The best is 0.95 (limitk), within noise.

## Why (cause)
1. **The gain is gone.** Dictionary encoding pays off when the same id fills consecutive rows. On v11 one row already holds up to 120 (recent) or 240+ (long-term) samples, so the id is ~3% of the stored bytes. The general-purpose codec already squeezes repeated UUIDs (2.89 MiB plain vs 3.13 MiB LC; LC also stores the 16-byte keys, in dictionaries that restart every 8,192 keys).
2. **The fixed cost remains, per read range rather than per row.** To read a granule of a LowCardinality column, the reader loads that granule's dictionary from disk and builds a hashed dictionary column, then throws it away. A selector that reads many small, scattered ranges pays this once per range. In `promql_selector_pk_range` every series is its own granule (20,000 ranges): the profile shows LowCardinality deserialization (dictionary `readBig` plus dictionary-column teardown) as the extra work, with 2.7× the system time and 2.5× the page faults of the plain schema. A one-series selector pays it on a few ranges (+5 ms of 23).
3. **Ingest pays for it too:** each insert and each merge rebuilds the dictionary index, ~12% of ingest CPU in the profile and −7% CPU per sample with a plain id (remote-write baseline).

Pointers: the default is set at `src/Storages/TimeSeries/normalizeTimeSeriesDefinitionImpl.cpp:486-487`. The generator's LowCardinality wrap is at `TimeSeriesIDGenerator.cpp:53-57` (added in 62369ec5fbe5, #117033), and the default flip is 4134b2fe62d2. Narrowing of the id to its dictionary-encoded half, which would have made `IN <ids>` run per dictionary key, was removed during #117033's review (6fd9c82acb28). Since then the LC id goes through exactly the same query shapes as a plain one.

## Proposed fix
- **What:** new tables default to `Tuple(UInt64, UUID)` again. That's one line at `normalizeTimeSeriesDefinitionImpl.cpp:487`, plus the documentation tables in `StorageTimeSeries.cpp`, the gtest constant, and ~13 `.reference` files that print the default schema. `04891_timeseries_lowcardinality_id` keeps covering explicit LowCardinality ids.
- **Why it's safe:** existing tables keep their declared id type in their stored inner-column definitions, so nothing on disk changes. 4134b2fe62d2 changed this default the same way (21 lines, no version bump).
- **Effect:** ~7% less ingest CPU per sample. Reads are 0–5% faster on wide queries, 8–22% on narrow ones and up to 1.75× on scattered-granule selectors, with the same results and ~3% less disk on the id column.
- **Effort:** S in code, but it reverses a maintainer's default that came with a published 1.16× claim. Upstream should decide whether to change the default within v11 or bundle it with the next version. The case for doing it now: v11 is 4 days old on master, so few tables have the old default yet.
- **Report only.** The branch is ready to write on request, with the A/B above as evidence plus a perf-test addition for a narrow selector. It would still need a build for the "after" numbers.

## Upstream overlap
- No open PR changes the id type (`comp-promql` search for LowCardinality / id type, 2026-11-01). #121062 (active-series cache in the sink) and #122051 (native histograms) touch ingest but not the id type.
- #117033 (merged 2026-08-31) introduced the default; its 1.16× number predates the bucketed schema (#117344).
- #121795 (merged, LowCardinality GROUP BY keys without materializing) is already in this build; the numbers above include it.
- No branch in JTCunning/ClickHouse touches the id type.

## How to reproduce
Dataset, server and scripts: research `bench/lc-uuid-id/` (`README.md`, `bench.py own|perf`, `pe.py`; results in `run1.jsonl`, `run2.jsonl`).
```sql
CREATE TABLE ts_lc ENGINE = TimeSeries;
CREATE TABLE ts_plain ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, UUID));
-- fill both identically (README), OPTIMIZE FINAL every inner table, then e.g.
SELECT count(), sum(value) FROM prometheusQuery(ts_lc, 'metric_0{pod="pod-5"}', <T_END>) SETTINGS max_threads = 2;
SELECT column, sum(column_data_compressed_bytes) FROM system.parts_columns WHERE active AND table LIKE '.inner_id.recentsamples.%' AND column = 'id' GROUP BY table, column;
```
