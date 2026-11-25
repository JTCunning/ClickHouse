# A/B plan: build the inner INSERTs of the TimeSeries sink with its first block of rows

**Experiment:** [../reports/2026-11-25-sink-lazy-pipelines.md](../reports/2026-11-25-sink-lazy-pipelines.md) · **Branch:** `lilred-perf-sink-lazy-pipelines` @ `7b6fce7643e` (base master `2176a0e2a33`) · **Task:** `2026-11-25-handoff-remote-write-handler-builds-the` (#27)
**Claim:** Prometheus remote write with async insert at 1M active series, stock table: server CPU per ingested sample −5..−7% (James's prototype run of the same diff: −6.6%, 6 cells, p = 0.002), from the handler's unused target-table INSERTs falling from 1.5 to about 0.14 µs per sample; request latency, the flush, query speed and the table contents unchanged.
**Evidence so far:** built prototype of the identical `src/` diff, measured by James on 2026-11-24 (#27: master `a1ba05231040`, EPYC 9R14, 8 cores). On this 2 vCPU host, master only: the four unused inner INSERTs cost about 1.1 ms of CPU per INSERT (empty INSERT 2,557 µs vs 1,452 µs for a plain MergeTree control), and the remote-write handler spends 4.9 ms per 2,000-sample request. 2026-11-25 dry run on master `521c4bcb7b94` (`bench/sink-lazy-pipelines/smoke-2026-11-25.txt`): all three scripts ran; contents.sh printed rows for all five inner tables (metric families 0, rw.py sends no metadata).

## Builds
- A: master at the branch's base `2176a0e2a33` (or the newer master the branch is rebased onto; record it). B: the branch head. Same build type for both (RelWithDebInfo, as James's prototype run, or release; not sanitizers).

## Machine
- The `ingest-at-scale` host of #27 (AMD EPYC 9R14), server pinned to 8 cores per side, A and B side by side on separate core sets, as in the prototype run. The cheap cases M1–M3 run on any host with 4+ cores.

## Data
- T1, T2, G1–G3, E1: the `ingest-at-scale` brief: 1M series from 4 Prometheus 3.16.0 agents in agent mode, `scrape_interval: 15s`, default `queue_config`, remote write 1.0, a generator serving node exporter, kubelet, kube-state-metrics and HTTP service families. Fixed rate 66.7k samples/s.
- M2, C1: `bench/remote-write-baseline/rw.py` requests through `bench/sink-lazy-pipelines/handler_cpu.sh`: 70 requests of 2,000 series out of 20,000, one sample per series, fixed `TS0` (the same on A and B, within the last 4 days so the recent samples table gets rows).
- M3: `bench/sink-lazy-pipelines/empty_insert.sh`: zero-row INSERTs, no data.

## Cases
Every result row names its workload or its full query text.

| case | workload or query | role | predicted A → B |
|---|---|---|---|
| T1 | remote write at 1M series, stock `CREATE TABLE default.prometheus ENGINE = TimeSeries`, server settings `async_insert = 1`, `wait_for_async_insert = 1` (defaults): server CPU µs per sample, median over cells | target (pass/fail) | 21.2 → 19.8–20.2 (−5..−7%) |
| T2 | the same with `SETTINGS tags_deduplication_cache_size_bytes = 1073741824, tags_deduplication_cache_expiration_seconds = 86400` | target (reported) | 15.8 → 14.4–14.9 (−6..−9%) |
| M1 | T1's CPU profile (`global_profiler_cpu_time_period_ns = 10000000`): samples on the handler thread under `TimeSeriesSink::TimeSeriesSink` > `createTargetPipeline` and under `QueryPipeline::reset` > `TimeSeriesSink::~TimeSeriesSink`, µs per sample | mechanism | 1.52 → ≤ 0.3; B has no `createTargetPipeline` frame under the handler's `executeQuery` |
| M2 | `TS0=<fixed> bench/sink-lazy-pipelines/handler_cpu.sh <tcp> <rw> 70`, row `handler`: CPU per request | mechanism | −1.1 ms per request or more (2 vCPU: 4.9 → about 3.8 ms) |
| M3 | `bench/sink-lazy-pipelines/empty_insert.sh <tcp> 40`: `INSERT INTO sink_lazy.ts (metric_name, tags, samples) SELECT 'm', map('a', toString(number)), [(toDateTime64(number, 3), 1.0)] FROM numbers(10) WHERE number > 100` | mechanism | 2.56 ms → about the `mergetree` row (−1.1 ms on 2 vCPU) |
| M3c | same script, row `mergetree`: the same INSERT into a plain MergeTree table | control | unchanged |
| G1 | T1's server request duration p50 and p99 | guard | unchanged (prototype: 249 → 251 ms, p = 0.22) |
| G2 | the 12 dashboard and alert queries of the brief, run during T1's window, each row with its full PromQL text and range/step | guard | each unchanged; geometric mean 1.00x |
| G3 | `handler_cpu.sh`, row `flush`: CPU per async insert flush | guard | unchanged (the flush builds the same inner INSERTs, one chunk later) |
| E1 | T1's workload with `async_insert = 0` in the server's default profile (the handler pushes rows into the sink itself) | edge | unchanged |
| C1 | `bench/sink-lazy-pipelines/contents.sh <tcp>` after the same `handler_cpu.sh` run on fresh tables, with `async_insert = 1` and with `async_insert = 0` | correctness | the five lines equal on A and B in both modes |

## Method
- T1, T2, G1, G2, E1: the prototype run's method: A and B side by side on separate core sets of one host, 10 min warm-up, 20 min window, 6 cells per side for T1 (3 for T2), a second job with the core sets swapped. Mann-Whitney, exact, two-sided, per metric.
- M2, G3, C1: one server per build on its own data directory (`DATA=<dir> bench/remote-write-arena/server.sh start A 19101`, `... start B 19201`), fresh `default.prometheus`, then for each: `TS0=1795600800000 bench/sink-lazy-pipelines/handler_cpu.sh 19101 21101 70` and `TS0=1795600800000 bench/sink-lazy-pipelines/handler_cpu.sh 19201 21201 70` (replace `TS0` by a time within the last 4 days, the same value for both), alternating A, B, A, B for 4 rounds, then `bench/sink-lazy-pipelines/contents.sh 19101` and `... 19201`. For C1 with `async_insert = 0`, restart both servers with `PROFILE_EXTRA='<async_insert>0</async_insert>'`, drop `default.prometheus`, and repeat one round.
- M3: `bench/sink-lazy-pipelines/empty_insert.sh 19101 40` and `... 19201 40`, alternating, 3 rounds each.
- The handler and the flush are separate `query_log` rows (`query_kind` `Insert` and `AsyncInsertFlush`), so M2 and G3 compare the same query on both sides.

## Metrics
- T1, T2: server process CPU time in the window divided by the samples that reached the samples table. G1: the server's request duration. M1: CPU profile samples per stack, converted to µs per sample. M2, G3, M3: `ProfileEvents['OSCPUVirtualTimeMicroseconds']` from `system.query_log`.
- Failed samples, retried samples, query exceptions, error lines in the server log: all zero on both sides.

## Correctness
- C1's five lines equal on A and B, in both async modes. Run `contents.sh` on master first and check it prints a non-zero count for tags, time ranges, samples and recent samples.
- Tests on B: `05875_time_series_sink_lazy_target_pipelines` and `05876_time_series_insert_target_access` must pass. On A, 05875 must fail in exactly the 2 lines that differ in `bench/sink-lazy-pipelines/05875-on-master-2026-11-25.txt` (the `InsertQueriesWithSubqueries` lines of `05875_native_async` and `05875_no_rows`), and 05876 must pass: its reference is master's output (`05876-on-master-2026-11-25.txt`), because grants must behave as on master. Run both with `--test-runs 25`; also run `04416_timeseries_insert_cancel_no_crash`, `05019_time_series_recent_samples_table`, `05025_time_series_recent_samples_dedup`, `05213_timeseries_metric_families_target`, `05243_time_series_deduplication_caches`, `05294_time_series_samples_ttl`, `05452_timeseries_metric_family_low_cardinality`, `05814_timeseries_to_grid_insert_result_exception_destroys_states`; integration `test_prometheus_protocols/test_async_insert.py`, `test_write_read.py`, `test_insert_select.py`.

## Pass / fail
- **Pass:** T1 at least 5% lower on B (James's suggested rule) with p < 0.05 and C1 equal; M1 on B at most 0.3 µs per sample; no guard (G1, G2 each query, G3, E1) significantly slower; all tests pass on B.
- **Fail:** T1 under 5% while M1 drops: the unused INSERTs cost less at this commit than in the prototype run (compare M1's base number with 1.52), report the measured gain and decide submit or drop. M1 not dropping: the branch doesn't remove the work, which would be a defect in the branch. C1 differs: a defect, back to lil red with both outputs.

## Checklist
- [ ] build A and B; record both shas
- [ ] run T1/T2/G1/G2/E1 with the brief's jobs; save the cell tables
- [ ] run M2, G3, C1, M3; save the raw output under `bench/sink-lazy-pipelines/ab/`
- [ ] fill the branch column of `prs/sink-lazy-pipelines.md` and drop "[unbuilt]"
- [ ] update the report verdict and the backlog row
