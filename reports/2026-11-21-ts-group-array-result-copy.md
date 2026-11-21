# `timeSeriesGroupArray` spends about 19 ns per sample writing its result
**Verdict:** worth fixing: a 15-line change in the aggregate function that every merge of a `TimeSeries` samples table runs; branch `lilred-perf-ts-group-array-result-copy` (unbuilt).
**Task:** 2026-11-21 weekend scout (James, chat 2026-11-20) · **Build:** master 26.12.1.112, `521c4bcb7b94` · **Date:** 2026-11-21
**A/B plan:** [ab-plans/ts-group-array-result-copy.md](../ab-plans/ts-group-array-result-copy.md)

## What happens (plain language)
The samples tables of a `TimeSeries` table (and its recent samples table) are `AggregatingMergeTree` tables whose `samples` column is a `SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp, value)))`. Every merge of those tables, every `INSERT` (with the default `optimize_on_insert`), every `FINAL` read and every read of an older-version table finishes its rows through `timeSeriesGroupArray`. Finishing a row writes its samples into the output columns one at a time, each through a generic `Field` value: about 19 ns per sample on this host, where a plain copy costs 1–2 ns.

## Numbers
2 vCPU / 4 GiB, master `521c4bcb7b94`, `max_threads = 1`, median of 6 runs after a warm-up (the branch's new perf test, `tests/performance/timeseries_group_array.xml`):

| query | master | same aggregation as `-State` (no result written) | result cost |
|---|---|---|---|
| 40,000 series × 4 rows × 60 samples (9.6M samples), `GROUP BY id` | 660 ms (633–689) | 480 ms (458–515) | 180 ms = 19 ns/sample |
| 10,000 series × 1,000 one-sample rows (10M samples) | 559 ms (539–569) | 367 ms (361–371) | 192 ms = 19 ns/sample |
| `INSERT` of the 9.6M samples into an `AggregatingMergeTree` with `optimize_on_insert` | 625 ms (615–630) | – | 24–29% of its CPU profile (3 runs) |

Merges of a real default `TimeSeries` table (`bench/ts-group-array-result/`: 8 unmerged parts, 3 labels, half counters and half 3-decimal gauges, 15 s samples, one hourly bucket):

| table | samples | merge CPU (`part_log`) | `insertResultInto` share of the merge's CPU profile |
|---|---|---|---|
| 20,000 series | 4.8M | 394–408 ms (A/A, 4 rounds each) = 85 ns/sample | 13.5% (52 profiler samples, one run) |
| 80,000 series | 19.2M | 1.46–1.56 s (`OPTIMIZE` CPU) | 26%, 15%, 17.6% (3 runs, 214–245 profiler samples each) |

The rest of the merge is mostly the codecs (55–70%: `ALP`, `ZSTD`, `Delta`).

## Why (cause)
`AggregateFunctionTimeSeriesGroupArray::insertResultInto` (`src/AggregateFunctions/TimeSeries/AggregateFunctionTimeSeriesGroupArray.h:529-533` at `521c4bcb7b94`) loops over the samples and calls `timestamps_to.insert(element.timestamp)` and `values_to.insert(element.value)`: each builds a `Field`, `safeGet`s it back to the column type and appends with a capacity check. `AggregatingSortedAlgorithm::finishGroup` calls `insertResultInto` for every output row, including rows that had nothing to merge (`src/Processors/Merges/Algorithms/AggregatingSortedAlgorithm.cpp:218-227`), so every sample of a merged part goes through it once per merge level.

Side effect of the `Field` route: a `Float32` signaling NaN comes back quieted (`0x7F800001` → `0x7FC00001`, checked on master); `Float64` values, the stale marker `0x7FF0000000000002` included, keep their bits.

## Proposed fix
Resize the two result columns once per row and copy the samples into their typed data (branch `lilred-perf-ts-group-array-result-copy`, 1 file, +13/−6, plus test `05839_timeseries_group_array_result_columns` and perf test `timeseries_group_array.xml`). Same order, same deduplication, same state format; the only visible difference is the `Float32` signaling NaN keeping its bits. Effort S.

Predicted (emulated by subtraction, not a build; the 0.4×–1× range of the emulation skill): the perf test's two result queries −10..−25% and −12..−31%, the `optimize_on_insert` `INSERT` −10..−25%, samples-table merge CPU −8..−20%. Remote-write ingest end to end gains less: merges are about half of its CPU and `AggregatingMergeTree` merge logic about 11% (2026-11-01 baseline), so roughly −2..−5% server CPU (not a target).

## Upstream overlap
- No open `comp-promql` PR or issue touches `insertResultInto` of this function (checked 2026-11-21: 88 open PRs, 37 issues). Open PRs #122051 (native histograms) and draft #121216 touch other hunks of the same header.
- File history: `eca6bd3172f` (skip the sort of sorted samples), `23193c88d49` (`-If` with array arguments), bounds fixes; none changes the result write.
- James's open #123142 skips the merge on insert for blocks with unique keys, only for scalar `SimpleAggregateFunction` columns, so the samples column still takes this path: complementary.

## How to reproduce
```bash
cd research/bench/ts-group-array-result
DATA=/tmp/ga SERVER_MEM=1400000000 QUERY_MEM=1200000000 ab/server.sh start A 19201
SERIES=80000 ./gen.sh 19201 && ./merge_profile.sh 19201 samples   # start A with PROFILE_EXTRA=<the profiler settings in merge_profile.sh>
PORTS="19201" N=3 SERIES=20000 ab/merge_ab.sh | tee /tmp/merge.jsonl && ab/summarize.py /tmp/merge.jsonl
```
The perf-test numbers: the `create_query`/`fill_query` and `query` elements of `tests/performance/timeseries_group_array.xml` (branch) run in `clickhouse local` with `max_threads = 1`.
