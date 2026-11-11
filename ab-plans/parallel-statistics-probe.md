# A/B plan: probe the primary key of parts in parallel when deciding to load statistics
**Experiment:** ../reports/2026-11-11-tags-in-subquery-cold.md · **Branch:** `lilred-perf-parallel-statistics-probe` @ 41e5fec573dc (base `5252bd271c80`) · **Task:** 2026-11-02-handoff-tags-in-subquery-and-cold-part-
**Claim:** A cold PromQL range query on an `s3` disk (50 ms per GET) whose samples read covers 13 parts with unloaded primary indexes drops from about 3.2 s to about 2.6 s (−15–22%). The probe's 13 index loads go from 13 sequential round trips (617 ms) to one parallel wave. Warm queries and post-prewarm queries (indexes in memory) don't change.
**Evidence so far:** emulated on master `7b5f2bf328f9`, 2 vCPU. The 13 indexes were loaded in parallel by a separate query before the timed one: 3,204 → 2,540 ms, plus about one round trip for the branch's parallel wave. Mechanism shown by the GET timeline (51 ms apart, on the main thread in `optimizePrewhere`) and by `trace_log`. Not yet built: the real parallel probe, its cost on warm reads with `use_primary_key_cache = 1`, and local-disk cold starts.

## Builds
- A: master at `5252bd271c80`. B: the branch at `41e5fec573dc` (on the same base). Both release builds without sanitizers, like the CI perf job.
- Also: a debug or TSan build of B for `05620` (the bot asked for a TSan run of the new thread pool).

## Machine
- At least 8 vCPU / 16 GiB, nothing else running. The claim is about request latency, not CPU, but the lab's S3 mock must not compete with the server for cores (on 2 vCPU it queued requests at ~26 ms each even with zero added latency). Record the CPU model.

## Data
- The lab of the report: `bench/tags-in-subquery-cold/` (`s3mock.py`, `server-config.xml.template`, `gen.sh`). 64 inserts of 15 minutes with merges stopped, 21,560 active series, 2% pod churn per insert. About 230 MiB in the mock bucket.
- A and B are two servers on copies of the same data directory **and the same bucket**. The `s3` disk keeps object keys in local metadata, so copy `server/data` and point both at the same mock (read-only workload). Each server gets its own filesystem cache directory.
- Sizes:
  - the lab as is (13 samples parts after pruning)
  - `BATCHES=128` (16 h → 32 h, but query the last 6 h: about 25 samples parts)
  - a non-TimeSeries table: 05407's shape, 200 parts on `s3`

## Cases
| case | query | role | predicted A → B |
|---|---|---|---|
| T1 | `query.sql` cold (`run.py --state cold --latency-ms 50`) | target | 3.2 s → 2.5–2.7 s |
| T2 | the same, `--latency-ms 20` | target | proportional: the probe's span ≈ 13 × 20 ms → ≈ 20 ms |
| T3 | the 32 h dataset, 6 h range, cold | target | saving ≈ (parts − ⌈parts/16⌉) × RTT |
| T4 | 05407-shape table on `s3`, 200 parts, the narrow read after `DETACH`/`ATTACH`, cold | target (non-PromQL) | probe from ~200 RTT to ~13 RTT |
| G1 | `query.sql` post-prewarm (`--state meta`) | guard | unchanged (indexes in memory) |
| G2 | `query.sql` warm (`--state warm`) | guard | unchanged, 98 ms ± noise |
| G3 | G2 with `use_primary_key_cache = 1` on the tags, time-ranges and recent-samples tables (`ALTER … MODIFY SETTING`) | guard: the round-1 Major | unchanged, 103 ms on master: cached indexes count as in memory, serial path |
| G4 | in-tree `tests/performance/prewhere.xml`, `read_hits_with_aio.xml`, `long_primary_key.xml`, `set_index.xml`, `prewhere_column_sizes_pruned_parts.xml` through `perf.py` (#126627 checked the first four) | guard | unchanged (warm: serial path) |
| E1 | local-disk cold start: restart the server, drop the page cache, 05407-shape table with 30 parts × 55 columns (Alexey's #126627 cold run) | edge: thread pool on a fast disk | not slower; ideally a little faster |
| E2 | a large read (statistics prune most parts) on `s3`, cold | edge: extra index loads | wall not slower; `LoadedPrimaryIndexFiles` may rise by up to `max_threads` |

## Method
- Interleave A and B per round: A cold, B cold, A cold, …, at least 9 pairs per case. `run.py` drops caches before every run. Use `--port` per server (add a `--http` option, or copy `run.py` per port).
- For the warm guards: `clickhouse benchmark --host 127.0.0.1 --port <A> --host 127.0.0.1 --port <B> -i 200 --confidence 5 < case.sql`.
- One A/A pass of T1 (A against A) for the noise figure. On 2 vCPU it was ±0.2% (3,204 vs 3,209 ms).

## Metrics
- Wall time from `query_duration_ms`. `QueryPlanOptimizeMicroseconds`. S3 GETs and their timeline: `get_span_ms['recentsamples: primary index']` must shrink from about 600 ms to one round trip.
- `LoadedStatistics`, `LoadedPrimaryIndexFiles`, `SelectedParts`, `S3GetObject`.

## Correctness
- Same `md5sum` of `query.sql ... FORMAT TSV` on A and B in every state.
- `05620_parallel_statistics_probe` passes on B, including under TSan. `05407_statistics_skip_small_reads` passes unchanged on B.

## Pass / fail
- **Pass:**
  - T1 at least 15% faster and significant at 99.5%.
  - The samples primary-index span is ≤ 2 round trips.
  - G1–G4 and E1 not significantly slower.
- **Fail:**
  - T1 under 10%: the emulation missed a cost, for example the pool sees fewer threads than `max_threads`, or the probe isn't the only serial loader. Re-run `stacks.sh` on B.
  - G3 or E1 slower: the pool's start-up cost matters. Gate the parallel path on the disk being remote, and re-measure.

## Checklist
- [ ] build A and B; record both shas
- [ ] generate the data; record its size in the bucket
- [ ] run every case; save the raw output under `bench/tags-in-subquery-cold/ab/`
- [ ] fill the branch column of `prs/parallel-statistics-probe.md` and remove "pending build"
- [ ] update the report verdict and the backlog row
