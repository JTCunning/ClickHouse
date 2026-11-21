# A/B plan: format the timestamps shared by the series of a range vector once per block
**Results 2026-11-21 (run 1, James, #22):** pass: T1 -10.2%, T2 -5.3%, T3 -3.3%, E2 -8.3%, E1 and G1 flat, A/A within ±4% not significant. Plan defect, mine: the md5 check can't hold for G1 (`sum()` differs in its last digits between two requests to master); `ab/run.py` now also records `values_md5` (values rounded to 12 significant digits). Ready to submit.
**Experiment:** ../reports/2026-11-21-range-json-timestamp-text.md · **Branch:** `lilred-perf-range-json-timestamp-text` @ `ff2ee0f3f036` (base master `521c4bcb7b94`) · **Task:** 2026-11-20-scout-format-the-shared-step-timestamps · **Outbox:** issue #22 (`ready-to-build`)
**Claim:** `/api/v1/query_range` over 2,000 series: `m` at step 15 s (2.88M points, integer values) −5..−20% wall, `rate(m[5m])` at step 30 s and `g` at step 60 s smaller gains (−2..−8%), the skewed block (one long series next to 1,000 short ones) not slower, the one-series control flat; response bodies byte-identical.
**Evidence so far:**
- measured on master `521c4bcb7b94`, 2 vCPU (the real-time profiler at 2 ms on the API requests, 4 requests per case): the writer is 60–90 ms of a ~700 ms `m` request (2,000 series × 1,441 points), and formatting the timestamps is 40–56 ms of it (15–19 ns/point); `rate(m[5m])`: 10–22 ms of 550 ms; `g`: 2–12 ms of 205 ms. The branch replaces that by a copy of ~10–15 bytes per point. Emulated by subtraction: −6..−8% wall on `m` here; 0.4×–1× gives −2..−8% on 2 vCPU. On 16 cores the parallel part of the request shrinks and the writer (one thread, after the final `ORDER BY`) does not, so the share should be larger there; that is a prediction, not a measurement.
- 2026-11-21 dry run on master `521c4bcb7b94` (`bench/range-json-timestamps/smoke-2026-11-21.txt`): the kit ran end to end as A/A at 1/2 scale (1,000 series), every case's bodies had the same md5 on both servers, medians within ±3.6% (5 rounds); the skewed case `E1` ran on one server (58 ms, 1,001 series, 1 × 1,440 and 1,000 × 72 points). Direction can't be dry-run (code in the writer).
- The galloping search was checked against `std::lower_bound` from the cursor on 200,000 random caches and series (`clang++-20 -O2 -Wsign-conversion`, the branch's lines verbatim): same cursor and same hits everywhere.
- Unproven: the size on 16 cores; the cost of the skewed (E1) and every-other-step (E2) blocks; long windows (`promql_max_points_per_series` up to 11,000 steps).

## Builds
- A: master at `521c4bcb7b94`. B: the branch (4 commits on the same base). Release build, no sanitizers.

## Machine
- 16 vCPU (the claim depends on the writer's share next to a parallel query; on 2 vCPU it is small). Nothing else running; note the CPU model. Both servers on the same host, the load generator (`ab/run.py`, Python) on the same host too: it only reads the response.

## Data
- `bench/range-json-timestamps/gen.sh` on each server with the same `T_END`: a default `TimeSeries` table `ts` with `SERIES` series of the counter `m` and the gauge `g` (labels `job`, `instance`, `pod`), one sample every 15 s over `HOURS` hours, plus the skewed metric `c` (1 series over the window and `CHURN` series over its last 5%) and `h` (1 series every 15 s and `HALF` series every 30 s). The window goes to `window.txt`, which `ab/run.py` reads. Size at `SERIES=2000 HOURS=6 CHURN=1000`: about 25 MiB per table.
  ```bash
  cd research/bench/range-json-timestamps
  CH=<A binary> DATA=<dir> ./server.sh start A 9001; CH=<B binary> DATA=<dir> ./server.sh start B 9002
  export T_END=$(date -d "$(date -u +%Y-%m-%dT%H:%M:00Z)" +%s)
  ./gen.sh 9001 && ./gen.sh 9002            # same T_END on both: same samples
  ```
  The Prometheus API of each server listens on TCP + 2000 (11001, 11002).

## Cases
| case | query | role | predicted A → B |
|---|---|---|---|
| T1 | `m`, step 15 s: 2,000 × 1,441 points, integer values, 57 MB body | target | −5..−20% wall |
| T2 | `rate(m[5m])`, step 30 s: 2,000 × 721, long float values | target | −2..−8% |
| T3 | `g`, step 60 s: 2,000 × 361, 3-decimal values | target | −2..−8% |
| E1 | `c`, step 15 s: 1 × 1,440 next to 1,000 × 72 points at its end | edge | not slower (each short series jumps once with a binary search, then hits) |
| E2 | `h`, step 15 s, `lookback_delta=10s`: 1 × 1,440 next to 1,000 × 720 (a point every other step) | edge | not slower (each point gallops two probes past the step it skips) |
| G1 | `sum(rate(m[5m]))`, step 30 s: one series | guard / control | unchanged (no cache for one-series blocks) |

## Method
- `PORTS="9001 9002" ROUNDS=40 python3 ab/run.py > ab/run-<date>.jsonl`, then `python3 ab/summarize.py ab/run-<date>.jsonl`. The script warms up each case on each server, then per round sends every case to both servers in an order that rotates; it records the request wall time, body size and md5.
- `max_threads` stays at the server default (the 16 cores); CPU: read `ProfileEvents['OSCPUVirtualTimeMicroseconds']` of the API queries from `system.query_log` (`log_comment` is the PromQL text) for T1 on both servers, to report CPU next to wall time.
- One A/A pass first (both ports on A's binary, `ROUNDS=10`) for the noise figure.

## Metrics
- Wall time per request (median, p95) from `ab/run.py`; CPU from `query_log`; body size and md5.

## Correctness
- Every case's response body has the same md5 on A and B in every round (`ab/summarize.py` prints `bodies equal: True`); checked as A/A on master at 1,000 series.
- `tests/integration/test_prometheus_protocols/test_query_api.py::test_range_vector_timestamps_shared_between_series` passes on B (its expected strings are master's output, so it passes on A too), and the rest of `test_query_api.py` and `test_evaluation.py` pass on B.

## Pass / fail
- **Pass:** T1 significantly faster, at least −5%; T2 and T3 not slower; E1, E2 and G1 not significantly slower; bodies equal everywhere; the integration tests pass.
- **Fail, T1 under 5%:** on that host the writer's timestamp share is smaller than predicted; I record the size and either withdraw the branch or keep it as a CPU saving if CPU drops clearly.
- **Fail, E1 slower:** the binary search or the cache build costs more than the formatting it saves on skewed blocks; I revisit the design (for example, build the cache only from the query's step grid).

## Checklist
- [ ] build A and B; record both shas
- [ ] A/A pass, then the A/B; save the raw output under `bench/range-json-timestamps/ab/`
- [ ] fill the branch column of `prs/range-json-timestamp-text.md`, remove "pending build" and `[unbuilt]`
- [ ] update the report verdict and the backlog row
