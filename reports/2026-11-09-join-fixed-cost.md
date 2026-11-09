# JOIN fixed cost in a small-result `ignoring()` / `group_left` ratio query
**Verdict:** not worth changing. The JOIN costs about 1 ms of a ~640 ms query (≈0.1% of CPU), with or without a lighter join setting. The time goes to decoding samples (≈70% of CPU) and the grid aggregation (≈19%). No join-selection or lowering change is needed.
**Task:** `2026-11-01-handoff-join-pipeline-fixed-cost-for-sm`, handoff [#14](https://github.com/JTCunning/ClickHouse/issues/14) · **Build:** 26.11.1.1642, git `7b5f2bf328f9` (the join code is identical at master `840c468d1ceb`) · **Date:** 2026-11-09

## What happens (plain language)
The ratio is lowered to two independent aggregations: the numerator (`rate` per CPU series, then `sum without (mode)`: 40 rows, one per node and CPU, each holding a 2,880-step array) and the denominator (`count without (cpu, mode)`: 10 rows, one per node). They meet in one hash `SEMI LEFT JOIN` on the node key: 40 probe rows against a 10-row hash table. With 16 streams the pipeline gets one hash-table filler plus 16 probe workers chained one after another (plus 17 "delayed blocks" workers for the spill wrapper). That chain is the "ladder" that `EXPLAIN PIPELINE` prints. Together these handle 50 rows, and that is all the JOIN costs. The rest of the query reads and decodes 27.6 M samples.

## Numbers
Dataset: TimeSeries v11 with the capture's schema. 320 `node_cpu_seconds_total` series (10 nodes × 4 CPUs × 8 modes, labels `job`, `cluster`, `instance`, `cpu`, `mode`), 15 s scrape for 30 days from 2024-01-01 (172,800 samples per series, 55.3 M in all), plus 100,000 filler series of other metrics (4 samples each) for a realistic tags table. Everything merged to one part per inner table (samples: 330,400 rows, 200 MiB). The window is the capture's: `start=1704067800`, `end=1706659185`, `step=900`. The result is 40 series × 2,880 steps, as in the capture.
Host: 2 vCPU, 4 GiB. `max_threads=16` with `max_threads_min_free_memory_per_thread=0`, so that the pipeline has 16 streams like the capture's. (By default this VM builds 2 streams; execution is capped at 4 threads by `concurrent_threads_soft_limit_ratio_to_cores=2`.) The median is of 7 interleaved rounds after a warm-up round; the query cache is off.

| case | wall ms, median (min–max) | CPU ms | planning ms¹ | read_rows | memory |
|---|---|---|---|---|---|
| ratio (as shipped) | 641 (621–714) | 1,135 | 50.4 | 149,313 | 151 MiB |
| numerator alone | 484 (470–507) | 882 | 28.1 | 105,895 | 149 MiB |
| denominator alone | 145 (134–154) | 252 | 25.7 | 43,418 | 16 MiB |
| ratio, `hash_join_layout='serial'` (one hash table, not 16 buckets) | 651 (626–689) | 1,144 | 53.0 | 149,313 | 151 MiB |
| ratio, spill wrapper off (`max_bytes_ratio_before_external_join=0`) | 645 (610–669) | 1,148 | 53.5 | 149,313 | 152 MiB |
| ratio, empty range (same plan, no samples: the same 30 days in 2025) | 35 (34–40) | 33 | 42.0 | 4,032 | 5 MiB |
| numerator alone, empty range | 18 (17–19) | 16 | 20.6 | 2,016 | 2 MiB |
| denominator alone, empty range | 16 (15–19) | 16 | 19.5 | 2,016 | 2 MiB |

¹ The sum of `QueryAnalysis`, `QueryPlanBuild`, `QueryPlanOptimize` and `QueryPipelineBuild` microseconds. These counters nest, so the sum can exceed the wall time.

**Fixed cost vs probe, paired per round** (JOIN increment = ratio − numerator − denominator):
- Empty range, so pure setup (planning, pipeline, filler, 16 probe workers): **+1.0 ms wall (−2 … +6), +1.1 ms CPU (−1.6 … +3.4).** Of this, the pipeline build for the join's extra processors is ~0.3 ms (1.6 ms vs 0.7 + 0.6).
- Full data, so setup plus fill plus probe: +6 ms wall (−12 … +63), −1 ms CPU (−18 … +53). That is zero within a per-pair noise of ±50 ms.
- Lighter join, measured: the serial layout gives +5 ms wall and +18 ms CPU vs the default, and spill off gives +8 / +6. Neither is faster; both are noise.
- At 2 streams (this VM's default) the split is the same: ratio 665 ms = numerator 514 + denominator 151.

**Where the processor time goes** (`system.processors_profile_log`, ratio at 16 streams, median of 5 runs):
- Reading the samples (`MergeTreeSelect`): 70.9%.
- Aggregation 11.8%, converting aggregates 6.5%, expressions 9.8%.
- All join processors together (16 `JoiningTransform`, 1 `FillingRightJoinSide`, 16 `SimpleSquashingTransform`, 17 `DelayedJoinedBlocks*`, 16 `BuildRuntimeFilterTransform`): **0.45 ms, 0.02%**.

The CPU profiler (1 ms) agrees: 69% of samples are in the read path (ZSTD, ALP, Delta and T64 decoding, memcpy), 19% in aggregation, 2% in planning, and under 1% have any join frame on the stack (probe 0.2%, build 0%).

**The capture agrees.** Its own processor timers put `ReadFromMergeTree` 586 ms + `Aggregating` 395 + `Expression` 66 = 1,047 ms of its 1,106 ms OS CPU, against `Join` at 0.9 ms. The JOIN is not where the aggregate CPU goes on 16 real cores either.

## Why (cause)
- **The JOIN is cheap because its inputs are tiny.** Both sides are already aggregated to one row per output series (40 and 10), so the hash table holds 10 keys. The 16 probe workers are one per stream; each wakes up, finds a chunk or nothing, and finishes. The "ladder" is how `EXPLAIN PIPELINE` prints that chain, not 24 levels of hash building: this build has exactly one `FillingRightJoinSide`. The pipeline has 524 processors at 16 streams; each selector brings about 200 of them, and the JOIN adds 49 (66 with the spill wrapper).
- **The fixed cost is per selector, not per JOIN.** On an empty range the two-selector query costs 35 ms, against 18 + 16 ms for each selector alone. That cost is each selector's analysis and planning, including the tags and time-ranges lookups that run inside plan optimization. Handoff [#15](https://github.com/JTCunning/ClickHouse/issues/15) is about exactly that.
- **The real cost is decoding samples.** The query reads 149 k one-hour bucket rows, about 27.6 M samples (20.7 M for the numerator's 120 series, 6.9 M for the denominator's 40), at about 40 ns of CPU per sample for decompression and grid accumulation. A `[5m]` rate on a 15 min step uses only a third of those samples, but they share one-hour buckets with the samples it does use, so the read cannot skip them. The read cost per sample is the subject of [#12](https://github.com/JTCunning/ClickHouse/issues/12) (report `2026-11-08-samples-timestamp-read-cost.md`).

Code pointers (master `840c468d1ceb`):
- `src/Storages/TimeSeries/PrometheusQueryToSQL/applySimpleBinaryOperator.cpp:310-327` (`getJoinKindAndStrictness`): `group_left` with a "one" side that has several rows per key becomes `LEFT SEMI`.
- `src/Planner/PlannerJoins.cpp:1213-1215` and `src/Interpreters/TableJoin.cpp:1322-1338` (`preferParallelHashLayout`): with `hash_join_layout='auto'` and no row estimate for the right side, which is always the case for a CTE aggregate, the planner picks the 16-bucket parallel layout (`parallel_hash_join_threshold` = 100,000 applies only with an estimate). Forcing serial measured no gain.
- `src/Planner/PlannerJoins.cpp:1240-1250`: with the default `max_bytes_ratio_before_external_join=0.5` the hash join is wrapped in `SpillingHashJoin`, which adds the delayed-blocks workers. Turning it off measured no gain.
- `src/Planner/PlannerJoins.cpp:1223` (`tryDirectJoin`): a direct join only exists for key-value storages (`Join`, dictionaries), not for a CTE, so there is no lighter path to request.

## Proposed fix
None; report only. A lowering that forces `hash_join_layout='serial'` or turns off the spill wrapper for PromQL joins would save at most ~1 ms per query, below the noise, and would add another PromQL-specific settings override to `applySettingsForPrometheusQuery` (and with it the risk of a slower build for a large right side, e.g. a one-to-one ratio of two 100 k-series metrics). Not worth it.
The time in this query shape is in samples decoded per selector (#12; A/B backlog row 11 prefetch / buffer) and in each selector's planning (#15).

## Upstream overlap
- [#121107](https://github.com/ClickHouse/ClickHouse/pull/121107) (open; dynamic filter pushdown for `group_left`: filters the right side by the left side's join groups) and [#128177](https://github.com/ClickHouse/ClickHouse/pull/128177) (open; pushes label matchers into the "one" side) both cut what one side **reads** when the sides select different series. Here both sides cover the same 10 nodes, so neither changes this query. Neither touches join selection.
- No open PR changes the join algorithm or layout for PromQL. No join-related branch in JTCunning/ClickHouse.

## How to reproduce
Generator, harness and raw output: `bench/join-fixed-cost/` (`bench.py`, `profile.py`, `summarize.py`, `run2_16streams.jsonl`, `run1_2streams.jsonl`, `profile.txt`, `ep_*.txt` = `EXPLAIN PIPELINE` per variant).
```bash
# server: bench/join-fixed-cost/server-config.xml (127.0.0.1:19000, enable_time_series_table=1, query/trace/processors_profile logs)
clickhouse client --port 19000 -q "CREATE TABLE default.bench ENGINE = TimeSeries SETTINGS version = 11, recent_samples_ttl_seconds = 345600, samples_bucket_step_seconds = 3600, recent_samples_bucket_step_seconds = 900"
for d in $(seq 0 29); do clickhouse client --port 19000 --param_d=$d --queries-file bench/join-fixed-cost/gen_day.sql; done
clickhouse client --port 19000 --queries-file bench/join-fixed-cost/gen_filler.sql
# then OPTIMIZE TABLE ... FINAL on the samples, tags and timeranges inner tables
python3 bench.py 7 16 > run.jsonl && python3 summarize.py run.jsonl   # cases in bench.py
python3 profile.py 5 > profile.txt                                     # processors_profile_log + trace_log split
```
