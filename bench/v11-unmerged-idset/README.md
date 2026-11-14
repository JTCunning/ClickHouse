# Lab: a version 11 `TimeSeries` table with unmerged tags and time ranges parts (handoffs #18, #19, #20)

All data is synthetic. Built for #18 (report `../../reports/2026-11-13-selector-one-tags-row-per-id.md`); #19 and #20 reuse it.
Master binary 26.11.1.2253 (`f0a9e6bffa71`), 2 vCPU / 4 GiB, local disk.

## Build it (about 25 s)

1. Work directory: `tools/clickhouse/data/v11-unmerged-idset/` (copy `server-config.xml` there). Start
   `clickhouse server --config-file=server-config.xml` (TCP 19100, HTTP 18124, Prometheus API on 19094 with `/api/v1` = `default.ts`;
   `query_log`, `text_log` at debug and `processors_profile_log` on; server cap 2.4 GB, query cap 2 GB).
2. `gen.sh <clickhouse> 19100`. It makes two tables:
   - `ts` (`version = 11`, `tags_deduplication_cache_size_bytes = 0`, merges stopped on every inner table): every insert writes
     every live series again, so each inner table has REPEATS + 1 parts and each live series REPEATS rows in the tags and the
     time ranges tables. Defaults: 31 parts, 3,155,000 rows in each inner table.
   - `ts_merged`: the same rows copied and merged with `OPTIMIZE ... FINAL` (1 part, one row per id): the steady state, as a guard.

| knob | default | meaning |
|---|---|---|
| `SERIES` | 60000 | live series of `metric_0` (labels `job` = i % 10, `instance` = host-intDiv(i, 10), `region` = i % 7, `pod` = i) |
| `REPEATS` | 30 | inserts of the same series = unmerged parts per inner table (+1 for the stale insert) |
| `STALE` | 5000 | extra `metric_0` series written once, with their only sample a day before the window (the time filter drops them) |
| `OTHER_METRICS`, `OTHER_SERIES` | 9, 5000 | other metrics written by the same inserts (the primary key on `metric_name` skips them) |
| `STOP_MERGES` | 1 | stop merges on all inner tables of `ts` before the inserts |
| `MERGED_COPY` | 1 | also build `ts_merged` |

Window: `[1700000000, 1700001800]`. Selectors (`idsq.py` `CASES`): wide `metric_0{job=~"job.*",region!="none",instance=~"host-.*"}`
(all 60,000), mid `metric_0{job="job3"}` (6,000), narrow `metric_0{instance="host-17"}` (10).

## Measure

- `idsq.py`: the ids query of the selector as master builds it (`any_none_none`; captured from the `Will execute query` debug line)
  and rewritten by hand. Variant = `<strictness>_<left>_<right>`: strictness `all` (before #127847) or `any`; left `none`, `limitbyord`
  (the #18 branch: `ORDER BY id LIMIT 1 BY id`), `limitby`, `distinct`, `distinctord`, `group`, `groupio`, `final`, `limitbynewest`, ...;
  right `none`, `distinct`, `distinctord`, `group`. `TABLE=ts_merged` switches the table.
- `bench.py <rounds> <out.jsonl> [variants] [cases]`: builds the `IN` set from the ids query (`MODE=limit1`: the first id only, the shape
  of `/api/v1/series?limit=`), interleaved, warm; prints time, CPU, memory, rows into `MergeJoinTransform` (both sides), rows out,
  `Created Set` rows and entries, read_rows. Results: `results-run*.jsonl` (run5 = the final #18 numbers, run6 = the newest-row variant).
- Time by processor: `system.processors_profile_log` for the run's query ids (`log_processors_profiles = 1` is on in `bench.py`).

## Notes for #19 and #20

- On `ts` the wide lookup spends most of its processor time merging the 31 + 30 sorted part streams for the in-order read
  (`MergingSortedTransform` ~1.5 s of ~3.4 s CPU on master; the hash variants that skip it are 45% faster on `ts` but 2–6× slower on
  `ts_merged`). The narrow and mid selectors still read the whole metric's time ranges rows (1.8M) for the merge join.
- `max_threads_min_free_memory_per_thread = 0` is set in `idsq.SETTINGS`: without it this host builds one stream.
- Run the client with `</dev/null` in scripts: an `INSERT ... VALUES` reads stdin when it is not a terminal and waits.

## Steady state (#19)

Report `../../reports/2026-11-14-idlookup-parts-and-rows.md`. Server as above (`server-config-keeper.xml` adds an embedded Keeper and
the interserver port, only for `REPLICATED=1` runs in a `Replicated` database `rdb`; plain runs use `server-config.xml`).

- `gen-grid.sh <clickhouse> <port> <P> <C>` (idea 4): table `g_p<P>_c<C>`, the rows of `ts_merged` written C times into P parts of the
  inner tags and time ranges tables (row (id, copy) → part `cityHash64(id, copy) % P`, `optimize_on_insert = 0`, merges stopped).
  The six tables 1/25/90 parts × 1/30 copies take ~1 min.
- `loop.py <variant> <table> <seconds> [rate] [series per statement]`: a continuous small-insert loop over HTTP keep-alive (one
  `INSERT … SELECT` per statement, `SLOTS` = 60,000 series slots of `metric_0` written in turn, one sample each, pods restarting every
  `CHURN` = 1800 s, `HISTORY` = 300,000 dead series merged first). `TAIL` = 15 stops the merges of the tags and time ranges tables for
  the last 15 s (the same merge lag in seconds for every variant: plain `MergeTree` here never lags on its own). Variants: `base`,
  `batch<K>` (K statements as one), `merge:<setting=value,…>` (ALTER of the two inner tables), `trcache<B>` (store_time_ranges = 0 plus
  table `<table>_tr` written as a time ranges cache with outward rounding to B s would write it; `ALIGN=1` puts a bucket boundary at
  the tail start). Writes `<table>.parts.json` (summary + part counts every 5 s). Copies of the #19 runs: `steady/`.
- `steady.sh <name:variant[:ENV=v,…]> …`: runs loop.py variants one after the other (~2 min each).
- `streams.py <rounds> <out.jsonl> <table|name=tags fq|tr fq>,… [variants] [cases]`: the lookup by table; prints streams (in-order
  sources), their thread time, `MergingSortedTransform` time, rows into the join, SelectedParts/Marks, read_rows, ids. A table with a
  `<name>.parts.json` in the current directory is queried over `[end − LOOKBACK, end]` (LOOKBACK = 30 s) of its run.
- False positives of `trcache`: the lookup's `ids` minus the truth, the series with samples in the window `[t0, t1]`:
  `SELECT count() FROM <inner tags> WHERE metric_name = 'metric_0' AND id IN (SELECT id FROM <inner samples> WHERE max_time >= t0 AND
  min_time <= t1 AND arrayExists(x -> x.1 >= t0 AND x.1 <= t1, samples))` (add the selector's matchers on `tags` for mid/narrow).
- Not reproduced: the Cloud's lagging merge tail. `MergeTree` and single-replica `Replicated*` inner tables stay at 3–6 time ranges
  parts at 6–15 inserts/s, so merge settings can't be judged here.
