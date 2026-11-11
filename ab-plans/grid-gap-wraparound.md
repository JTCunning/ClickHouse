# A/B plan: grow the timeSeries*ToGrid bucket map before inserting a colliding key (run 2)
**Experiment:** ../reports/2026-10-31-grid-gap-wraparound.md · **Branch:** `lilred-perf-grid-gap-wraparound` @ `e6d27f5cc1d4` (based on master `52529f0c7115`; merges cleanly with master `66b092b52662`, 2026-11-07) · **Task:** 2026-10-31-timeseriestogrid-gap-slowdown-125759 · **Outbox:** item 4, issue #10 (`ready-to-build`)
**Claim:** the gap slowdown goes away whichever run of buckets arrives first: in time order (run 1 showed 8.8× / 6.5×), in reverse order, and in a merge into a state that already holds the run after the gap. Contiguous, late-start, sparse-gap and every-2nd-minute series are unchanged in time and memory.
**Evidence so far:** run 1 (2026-11-05, James's build of `46291048116b`) passed for the time-order case. The final review then found that the first rule only grew the map for keys at or above the buffer size, so the reverse and merge orders still walked the whole run, and that a sparse gap doubled the map for no gain. `0113a7c96c12` fixes both, and `e6d27f5cc1d4` stops a sample in every 2nd bucket from doubling the map (round 4); a model of the table predicts it, nothing has been built.

## Builds
- A: master at `52529f0c7115` (the base, the same A as run 1). B: `e6d27f5cc1d4`. Master has no change under `src/AggregateFunctions/TimeSeries/` since the base, so no rebase.
- Release build, no sanitizers.

## Machine
- The 16-core pinned host of run 1 (AMD EPYC 9R14, servers on cores 0-15). Every target query uses `max_threads = 1`.

## Data
- All in `tests/performance/timeseries_to_grid_gap.xml` (B's tree): 200 series, one sample per minute, 7-day grid at 60 s; shapes `contiguous` [0, 6200), `gap` [0, 3100) + [6981, 10081), `late_start` [4000, 10081), `gap_sparse` [0, 1500) + [8581, 10081), `every_2nd` (even minutes), `dense` [0, 6200) at 4 samples per minute.

## Cases
| case | query | role | predicted A → B |
|---|---|---|---|
| T1 | `timeSeriesLastToGrid`, gap | target | 877 → ~99 ms (as run 1) |
| T2 | `timeSeriesRateToGrid` (window now 120 s), gap | target | ≥ 3× faster |
| T3 | `timeSeriesLastToGridMerge` of two states per series, later run first | target | ~3× faster (probes as with the 1/2 limit, about 3× fewer than master; master on 2 vCPU: as slow as T1) |
| G1 | `timeSeriesLastToGrid`, contiguous | guard | unchanged |
| G2 | `timeSeriesRateToGrid`, contiguous | guard | unchanged |
| G3 | `timeSeriesLastToGrid`, late_start | guard | unchanged |
| G4 | `timeSeriesLastToGrid`, gap_sparse | guard | unchanged (time and memory: the rule must not grow this map) |
| G6 | `timeSeriesLastToGrid`, every_2nd | guard | unchanged, memory too (master keeps 8192 cells; the first head doubled them) |
| G7 | `timeSeriesLastToGrid`, dense (4 samples per bucket) | guard | unchanged: most adds are hits, now checked with two cell reads and `find` in a more-than-half-full map |
| G5 | `timeseries_to_grid_bucketing.xml`, `timeseries_to_grid_aggregation_functions.xml` | guard | unchanged: a hit on an occupied home cell in a more-than-half-full map now takes `find` instead of `operator[]` |

## Method
- `tests/performance/scripts/perf.py --runs 13` from B's tree against two servers on one host, then `compare_perf_tsv.py`, as in run 1.

## Metrics
- Wall time medians and significance; `memory_usage` from the perf report for every case of `timeseries_to_grid_gap.xml` (G4 must not grow).

## Correctness
- `05474_timeseries_to_grid_gap_wraparound` on B, and on A (it must pass there too: results do not change; checked on a master binary 2026-11-07).

## Pass / fail
- **Pass:** 05474 passes on A and B; T1 and T2 at least 3× faster, T3 at least 2× faster; no G case significantly slower; G4 and G6 memory within ±5%.
- **Fail, T3 not faster:** the merge order of the states differs from the subquery's `ORDER BY`; check with `max_threads = 1` and report.
- **Fail, G5 or G7 slower:** move the hit check behind the cheaper `operator[]` and measure again.

## Checklist
- [ ] build B; reuse A from run 1 if still available
- [ ] run the three XMLs; post results on issue #10

## Run 2 outcome (2026-11-10)
- Fail on two pass rules: T3 1.93× (floor 2×, my mis-prediction; its ORDER BY was removed by `query_plan_remove_redundant_sorting`, so the order was mixed), and 5 of 7 `timeseries_to_grid_bucketing.xml` queries significantly slower. The second is most likely the layout draw of that file's fill (see the report update), not the change.
- No run 3 until James decides (issue #10). If there is one:
  - B = the new head; every query of `timeseries_to_grid_gap.xml` (the merge queries now pin their order: later run first, earlier run first, sparse gap later run first).
  - `timeseries_to_grid_bucketing.xml` on **byte-identical data**: start A, run the file's create and fill queries, `SYSTEM STOP MERGES`, record `SELECT table, count(), groupArray(rows) FROM system.parts WHERE active GROUP BY table`, stop A, copy its data directory to B's, start both, check the same part list on B. Then per query `clickhouse benchmark --host 127.0.0.1 --port 9001 --host 127.0.0.1 --port 9002 -i 30 --confidence 5`. Plus one A/A pass (A against a second A server on the same copy) for the noise figure. Keeps the multi-part layout, whose cross-thread merges the change touches.
  - `memory_usage` for every gap and bucketing case.

