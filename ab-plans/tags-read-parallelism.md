# A/B plan: does the tags subquery's "no settings change" verdict hold at 16 threads?
**Experiment:** ../reports/2026-11-01-tags-read-parallelism.md · **Branch:** none yet: verifies a report conclusion (handoff #9) · **Task:** 2026-10-31-handoff-measure-and-raise-read-parallel
**Claim:** at 4, 8 and 16 threads, the current tags-subquery settings (merge join, read in order, `readImpl`'s concurrent-read thresholds) stay within 10% of the best alternative on every selector shape. The selector `{__name__=~"metric_1.*"}` (502,050 series) does not get faster than about 650 ms however many threads it gets, because the tags collector serializes the first insert of every series. Without the collector, the same read scales at least 3× from 1 to 16 threads.
**Evidence so far:** measured on 2 vCPU / 4 GiB only, so at `max_threads` 1 and 2. A check on 2026-11-10 (`bench/tags-read-parallelism/streams-2026-11-10.txt`) showed that the report's "2 streams at any `max_threads`" came from this host's free-memory cap (`max_threads_min_free_memory_per_thread`, 1 GiB per thread). Lift the cap and the two shapes without a metric name read with 8–16 streams per side. The concurrent-read thresholds do change those counts (`{job="job3"}`: tags side 16 streams with `readImpl`'s thresholds, 12 with the defaults; time ranges side 15 against 5). So the report's "idea 2: the thresholds don't change the stream count" is unproven, and what more streams buy has never been measured. `metric_0` reads one in-order stream per join side under every setting, as the report says.

## Builds
- One build, current master. This experiment changes query settings only, so every variant runs on the same binary and the same server. Record the version and the git hash. The 2026-11-10 check ran on 26.11.1.1642, where TimeSeries tables also have an `.inner_id.recentsamples` table. The tags subquery doesn't read it.

## Machine
- 16 vCPU and at least 64 GiB RAM. The claim is about scaling to 16 threads, and 64 GiB keeps the 1 GiB-per-thread free-memory cap from binding. Still pass `max_threads_min_free_memory_per_thread=0` explicitly and confirm the stream counts with `streams.sh` before timing. Local NVMe. Warm runs only, since the claim is about CPU and locks, not I/O. Nothing else running. Note the CPU model.

## Data
The handoff #9 generator, scripted in `bench/tags-read-parallelism/load.sh`: the first SERIES/10 series are `metric_0`, the rest are spread over `metric_1` … `metric_199`; 6 labels; `job` has 10 values independent of the metric. Every inner table is merged to one part. The tags subquery reads only the tags and time ranges tables, so its cost depends on the series count, not on the samples per series.

| size | command | why |
|---|---|---|
| small | `SERIES=100000 SAMPLES=20 ./load.sh` | the 502-series and 100k-series shapes at low cost |
| report | `SERIES=1000000 SAMPLES=20 ./load.sh` | same data as the report's tables 1–3, so the numbers line up |
| handoff | `SERIES=1000000 SAMPLES=240 ./load.sh` | the handoff's real sample count, for the full selector's share (table 3) |
| large | `SERIES=5000000 SAMPLES=20 ./load.sh` | the collector's cost grows with matching series: the name regex then matches about 2.5M |

Run each size on a fresh server directory (`config.xml` in the bench directory; raise `max_server_memory_usage` and the profile's `max_memory_usage` to fit the machine). Then run `python3 gen.py && python3 derive.py` to extract `sub_<k>.sql` (current), `nojoin_<k>.sql` (no time ranges join) and `ids_<k>.sql` (id only, no collector) from the server's own log, so the SQL is what this build generates.

## Cases
Shapes: 0 `metric_1{job="job3"}` (502 series at the report size), 1 `metric_0` (100,000), 2 `{job="job3"}` (100,118), 3 `{__name__=~"metric_1.*"}` (502,050). Predictions are for the report size; the report's 2 vCPU medians come first.

| case | query | role | predicted |
|---|---|---|---|
| T1 settings sweep | `sweep.py` over `sub_<k>.sql`: merge join in order (current), merge join not in order, hash in order, hash not in order, parallel_hash | target | current within 10% of the best variant on every shape at 4, 8 and 16 threads. Not in order stays slower: +11–51% on 2 vCPU, predicted +10–50% |
| T2 named metric | shape 1, current | target | 148 ms (1 thread) → 120 ms (2) → **110–125 ms at 4–16 threads**: one in-order stream per side, so flat |
| T3 no metric name | shape 2, current | target | 488 → 377 ms → **260–380 ms at 16 threads**: the reads and the tags-side sort can use 15–16 streams; the merge join and the collector stay serial (~60% of the 1-thread time) |
| T4 collector serialization | shape 3, `sweep2.py`: current, no join, id only | target (mechanism) | current 1,116 → 1,142 ms → **650–1,150 ms at 16 threads**. No join: 667 ms at 1 thread, and **< 1.5× faster at 16 threads** (2 threads was slower: 716 ms). Id only: 399 ms at 1 thread, **≥ 3× faster at 16 threads** |
| T5 thresholds | shape 2 and 3, current, with `readImpl`'s thresholds (`merge_tree_min_bytes_for_concurrent_read=4194304, merge_tree_min_rows_for_concurrent_read=0`) against the defaults | target (idea 2) | stream counts as in `streams-2026-11-10.txt` (16/15 against 12/5 on shape 2); wall within 5%, since the 15→1 merge into a single-thread join consumes the extra streams |
| T6 full selector share | `full.py`, all 4 shapes, `max_threads` 1, 4, 16, on the handoff size (240 samples) and the report size | target (priority) | tags subquery share (T1 current ÷ full) at 16 threads: **20–50%** at 240 samples (48–73% at 20 samples, 1 thread). This decides whether the collector fix (handoff #3) comes first |
| T7 scale | T1 and T4 on the large size | edge | the collector's share grows with matching series: shape 3 current ≥ 60% collector at 16 threads |
| G1 | shape 0 (502 series), every variant | guard | 12 ms, unchanged at every thread count; parallel_hash stays slower (17 ms on 2 vCPU) |
| G2 | `tests/performance/promql_selector_pk_range.xml`: `sum(pkr_metric{dc=~".*"})`, `sum(rate(pkr_metric[30m]))` | guard | unchanged (no settings in this experiment reach it; it confirms the server is comparable to CI) |
| G3 | `tests/performance/promql_selector_id_range_row_cost.xml` (sets `max_threads = 4`) | guard | unchanged |
| G4 | `tests/performance/timeseries_id_to_group.xml` (`timeSeriesStoreTags` then `timeSeriesIdToGroup`) | guard | the collector's single-thread cost, for comparison with T4 |

## Method
- One server; variants interleaved per round (A B C D E A B …) as `sweep.py` does. Round 0 is the warm-up and is dropped. Rounds: N = 15 for shapes 1–3, N = 50 for shape 0.
- Every run sets `EXTRA_SETTINGS="max_threads_min_free_memory_per_thread=0"` so the stream count follows `max_threads` on any machine.
- Commands, in the data directory with `bench/tags-read-parallelism/` on the path:
  ```bash
  MT=16 ./streams.sh                                                  # stream counts first; save next to the timings
  EXTRA_SETTINGS="max_threads_min_free_memory_per_thread=0" python3 sweep.py 15 1,2,3 1,4,8,16 > ab/sweep.jsonl
  EXTRA_SETTINGS="max_threads_min_free_memory_per_thread=0" python3 sweep.py 50 0 1,4,16 >> ab/sweep.jsonl
  EXTRA_SETTINGS="max_threads_min_free_memory_per_thread=0" python3 sweep2.py 15 1,2,3 1,4,8,16 > ab/sweep2.jsonl
  EXTRA_SETTINGS="max_threads_min_free_memory_per_thread=0" python3 full.py 1,4,16 > ab/full.jsonl
  clickhouse client --param_run=<run id from the log_comment rNNN_ prefix> --queries-file sig.sql > ab/sig.txt
  ```
- T5: run `sweep2.py`'s current variant twice, once with `readImpl`'s thresholds and once with `EXTRA_SETTINGS` overriding them to the defaults (`merge_tree_min_bytes_for_concurrent_read=251658240, merge_tree_min_rows_for_concurrent_read=163840`, the later setting wins).
- G2–G4: `tests/performance/scripts/perf.py` with each XML against the server, 1 run.

## Metrics
- Wall time (median, min–max), CPU (`UserTimeMicroseconds + SystemTimeMicroseconds`), `memory_usage` and `peak_threads_usage` from `system.query_log`. All of these are already in the sweep output.
- Significance: `bench/tags-read-parallelism/sig.sql` gives each variant's median ratio to the current setting and a two-sided Mann-Whitney p-value. A variant counts as different only if the ratio is outside 0.95–1.05 and p < 0.005, the CI comparison's 5% floor at 99.5%.
- Collector lock wait (T4, T7): run shape 3 once more at 16 threads with `query_profiler_real_time_period_ns=1000000, log_comment='lockwait'` and count the samples taken inside the collector:
  ```sql
  SELECT count() AS samples, uniqExact(thread_id) AS threads, countIf(syms[1] ILIKE '%lock%' OR syms[1] ILIKE '%futex%' OR syms[1] ILIKE '%syscall%') AS in_lock_wait, topK(5)(syms[1]) AS top_frames
  FROM (SELECT thread_id, arrayMap(a -> demangle(addressToSymbol(a)), trace) AS syms FROM system.trace_log
        WHERE trace_type = 'Real' AND query_id = (SELECT query_id FROM system.query_log WHERE log_comment = 'lockwait' AND type = 'QueryFinish' ORDER BY event_time DESC LIMIT 1))
  WHERE arrayExists(s -> s LIKE '%TimeSeriesTagsCollector%', syms)
  SETTINGS allow_introspection_functions = 1
  ```
  Predicted: samples from about 16 threads, and most of them waiting on the lock (`in_lock_wait` > 50% of `samples`).

## Correctness
- `sweep.py` asserts that every variant returns the same `sum(sipHash64(x)), count()` as the first, and stops otherwise. Record the series count per shape and size.

## Pass / fail
- **Confirmed:** T1 has no variant significantly faster than the current one by more than 10% on any shape at any thread count; T4's id-only variant scales ≥ 3× while the no-join variant scales < 1.5×; the lock-wait query shows the collector waiting. The report's verdict stands at 16 threads; add the 16-thread table to the report.
- **Verdict flips for selectors without a metric name:** hash or parallel_hash is significantly faster than the current setting by more than 15% on both shapes 2 and 3 at 16 threads, with memory ≤ 1.5×. Then the candidate change is to use a hash join for the time ranges join only when the selector has no exact `__name__` match. That condition is structural (the tags side isn't in `id` order then), so it needs no new row threshold. It goes to James as a proposal with these numbers.
- **The cause is wrong:** the no-join variant also scales ≥ 3×, or the lock-wait share is small. Then the collector isn't the serial part at scale; find what is (`log_processors_profiles=1`, sum `elapsed_us` by processor) and revise the report's "Why" section.
- **T5:** `readImpl`'s thresholds significantly slower than the defaults by more than 5% at 16 threads means the subquery shouldn't inherit them. Report it; it's a one-line change in `readImpl`.
- **T6** decides the order of the next work, not pass/fail. A tags-subquery share ≥ 25% on shape 3 at 240 samples and 16 threads makes the collector change (handoff #3) the next branch.

## Checklist
- [ ] record the build version and hash, the CPU model and the RAM
- [ ] load each size (`load.sh`); record the on-disk sizes it prints
- [ ] `gen.py`, `derive.py`, `streams.sh` per size; save the stream counts under `bench/tags-read-parallelism/ab/`
- [ ] run T1–T7 and G1–G4; save the raw output under `bench/tags-read-parallelism/ab/`
- [ ] `sig.sql` per run; lock-wait query for shape 3
- [ ] add a 16-thread table to the report, correct or confirm the verdict, update the backlog row
