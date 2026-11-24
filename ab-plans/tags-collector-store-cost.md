# A/B plan: the per-query tags collector on normal PromQL queries (master vs. upstream PR 132565, then the store-path branch only if needed)
**Withdrawn 2026-11-24:** James closed handoff #3 as dead: upstream PR 132565 removes `timeSeriesStoreTags` from the PromQL selector, and the collector only mattered for instant rule queries. Nothing here is to be run. The kit (`bench/tags-collector-store-cost/`) and the smoke stay for reference.
**Experiment:** ../reports/2026-10-31-tags-collector-store-cost.md · **Branch:** none yet. Stage 1 measures upstream PR 132565, which removes `timeSeriesStoreTags` from the PromQL selector. A branch (the report's steps 1+3) gets written only if Stage 2 is reached · **Task:** 2026-10-31-handoff-cut-per-row-cpu-of-contexttimes (handoff #3)
**Claim:** On normal-use PromQL queries the collector matters for rule evaluation, not for dashboards. On master it is 15–20% of the CPU samples of an instant `rate()` / `sum by` rule over 25k series (5% from the store alone), and 2–4% of a 1 h dashboard panel. Stage 1: upstream PR 132565 passes tags as a column instead, so on B the collector disappears from these queries. Instant rule queries should come out between −15% and +10%: the collector's share goes, a per-id tags join comes in. Dashboard panels should stay within ±5%; the PR's own CI tables are PR/master 1.02–1.26. Stage 2, only if 132565 doesn't land: the report's steps 1+3 would give −3..−10% CPU on instant rule queries and at most −2% on dashboard panels. Step 1 alone stays under the 5% floor.
**Evidence so far:**
- 2026-10-31 report (`a7f713612c8`, 2 vCPU, 400k series): the store adds ~1.07 µs and ~590 B per series. The split is 39% teardown, 19% group map insert, 17% list building, 2% sort. The per-step estimates come from a g++ `-O2` / glibc malloc model, not the release toolchain (clang `-O3 -march=x86-64-v3`, jemalloc). Read them as directions, not numbers.
- 2026-11-24 smoke on master `521c4bcb7b94` (`bench/tags-collector-store-cost/smoke-2026-11-24.txt`): 25k series per metric, 1 h at 15 s, `max_threads = 2`, the SQL entry point.
  - Collector share of the CPU samples: 1.9–3.9% on 5 dashboard range queries, 15.4–19.7% on 3 instant rule queries, 5.8% on a plain instant gauge read.
  - The store alone: +16 ms CPU and +14 MiB per 25k series. That is 5% of the instant query's 324 ms CPU, a third of the trace share. The profiler overran ~12× per sample, so there are only ~250 samples per case.
  - The handoff's own 16-thread capture is consistent with the higher number: `collector_store_tags` was ~30% of an instant `sum by (job, cluster) (rate(metric_0[5m]))` over 100k series (1,251 samples over 5 runs of 819 ms CPU).
- 2026-11-24 dry run of `ab/run.py`, A/A on two master servers (2,000 series × 6 h, 1/50 of M): the kit ran end to end. All 12 cases came back with no HTTP errors; A/A was −3.6..+3.1%, none significant, and every body was identical. B is an upstream PR's code and can't be emulated, so the dry run answers questions 1–3 only. Not dry-run: the `PROFILE_EXTRA` restart for the 2-thread pass (the same `server.sh` mechanism that `lc-uuid-id` variant C used), `share.sh` with `THREADS=16`, and sizes above 1/50 of M (this host's memory).
- Upstream PR 132565 (open, mergeable, head `1a522a74debc`, updated 2026-11-23):
  - It implements RFC issue 132485; the maintainers' decision there was "implement and see the results".
  - It drops the `timeSeriesStoreTags` call from the selector's id query and joins `series(id, tags)` instead.
  - Its CI perf tables, PR/master: plain selector 1.02–1.11, topk/limitk 1.06–1.26, set operators 1.00–1.22.
  - The collector stays for `timeSeriesIdToTags` and the group functions in SQL.
- Unproven: all of it on the realistic queries below, and at 16 threads.

## Builds
- Stage 1: A is the first parent of PR 132565's merge ref at run time (`git fetch origin pull/132565/merge`); B is that merge commit, so the PR is the only difference. If the PR has merged by then, A is the merge commit's first parent and B is the merge commit. Release build, no sanitizers.
- Stage 2, only if 132565 closes unmerged or B still runs the collector on I1–I3: A is master; B is `lilred-perf-tags-collector-flat`, to be written as steps 1+3 in one branch, because step 1 alone is under the floor. It needs a new kit dry run before it goes out.

## Machine
- 16 vCPU / 64 GiB, local NVMe, nothing else running. Note the CPU model.
- 16 vCPU because the store runs in one thread: the id set is built in place during plan optimization. Its wall share grows with threads while the samples read parallelises. Every target also runs at `max_threads = 2`, the smoke's setting.
- Before trusting a 16-thread number, check that `EXPLAIN PIPELINE <q> SETTINGS max_threads = 16, max_threads_min_free_memory_per_thread = 0` shows `× 16`.

## Data
- Generator: `bench/range-json-timestamps/gen.sh`, run from `bench/tags-collector-store-cost/ab/`. It writes the counter `m` and the gauge `g` with labels `job` (10 values), `instance` and `pod` (one per series), at a 15 s scrape. `T_END` is the start of the current minute, so the recent samples table fills as it does in production. Both servers get the same `T_END`. One part per inner table after its `OPTIMIZE FINAL`.
- gen.sh writes all samples in one `INSERT … SELECT`, at ~100 B in flight per sample. 14.4M samples didn't fit in 1.3 GiB on the dry-run host. Start both servers with `SERVER_MEM=30000000000 QUERY_MEM=25000000000`.
- Sizes:
  - S: `SERIES=25000 HOURS=6` (72M samples). The only size with the 6 h window R6 needs.
  - M: `SERIES=100000 HOURS=1` (48M samples): the handoff's wide metric, 100k series per metric.
  - L: `SERIES=400000 HOURS=1` (192M samples): the report's 400k, where the store costs ~430 ms.
- Exact commands for M. `CH` is each side's own binary; check that both print the same `system.parts` lines:
  ```bash
  cd research/bench/tags-collector-store-cost/ab
  export SERVER_MEM=30000000000 QUERY_MEM=25000000000 DATA=$HOME/tcs-data
  CH=$A/clickhouse ../../range-json-timestamps/server.sh start a 9001
  CH=$B/clickhouse ../../range-json-timestamps/server.sh start b 9002
  CH=$A/clickhouse SERIES=100000 HOURS=1 CHURN=0 HALF=0 ../../range-json-timestamps/gen.sh 9001
  CH=$B/clickhouse SERIES=100000 HOURS=1 CHURN=0 HALF=0 T_END=$(cut -d' ' -f2 window.txt) ../../range-json-timestamps/gen.sh 9002
  ```

## Cases
All cases go through the Prometheus HTTP API, as Grafana and a rule evaluator send them. Range cases cover 1 h at a 15 s step unless noted; instant cases run at `T_END`. The predictions are B / A.

| case | query | role | Stage 1 (PR 132565) | Stage 2 (steps 1+3) |
|---|---|---|---|---|
| R1 | `rate(m{job="job3"}[5m])`, 1 h, step 15 s | dashboard panel, one service | ±5% | −0..−2% |
| R2 | `rate(m[5m])`, 1 h, step 15 s | dashboard panel, every service | ±5% | −0..−2% |
| R3 | `sum by (job) (rate(m[5m]))`, 1 h, step 15 s | dashboard panel, aggregated | ±5% | −0..−2% |
| R4 | `g{job="job3"}`, 1 h, step 15 s | gauge panel, one service | ±5% | −0..−2% |
| R5 | `g`, 1 h, step 15 s | gauge panel, every service | ±5% | −0..−2% |
| R6 | `rate(m[5m])`, 6 h, step 30 s (size S only) | 6 h dashboard | ±5% | −0..−1% |
| I1 | `sum by (job) (rate(m[5m]))`, instant | target: recording rule | −15..+10% | −3..−10% CPU |
| I2 | `rate(m[5m])`, instant | target: per-series rule | −15..+10% | −3..−10% CPU |
| I3 | `sum(rate(m[5m])) > 0`, instant | target: alerting rule | −15..+10% | −3..−10% CPU |
| I4 | `g`, instant | instant read of every series | ±5% | −0..−3% |
| R7 | `topk(10, rate(m[5m]))`, 1 h, step 15 s | edge: the PR's slowest CI shape (topk 1.06–1.26) | +0..+25% | unchanged |
| S1 | `g{job="job3"}` through `/api/v1/series`, 1 h | edge: the label browser; the PR changes the HTTP API's series path | ±5% | unchanged |

- **Guards**, the nearest in-tree XMLs, through `tests/performance/scripts/perf.py` against both servers:
  - `promql_tags_transform.xml`: the 9 tag functions the PR rewrites
  - `promql_topk.xml`
  - `promql_selector_pk_range.xml`
  - `promql_selector_name_alternatives.xml`
  - `timeseries_id_to_group.xml`: a collector-based SQL function, expected unchanged
  - the PR's new `promql_selector_churn.xml`, from B's tree, on both servers
- **Mechanism evidence, never the headline:** `share.sh` on each server at M, `THREADS=2` and `THREADS=16`. On B the collector share of I1–I3 must be about 0. If it isn't, the PR keeps the collector on that path, and the answer to Stage 1 is "not superseded".

## Method
- Two servers on the same config, started by `server.sh` as above. Check that the `system.parts` lines gen.sh printed for A and B match before timing.
- Timing, per size, at the default `max_threads` (16):
  ```bash
  PORTS="9001 9002" ROUNDS=100 ./run.py > run-<date>-M.jsonl && ./summarize.py run-<date>-M.jsonl > table-<date>-M.md
  ```
  - Size S adds `CASES="R1 R2 R3 R4 R5 R6 R7 I1 I2 I3 I4 S1"`. Sizes M and L leave R6 out: `CASES="R1 R2 R3 R4 R5 R7 I1 I2 I3 I4 S1"`.
  - 100 rounds give N = 100 per server for every case. Every round sends each case to both servers next to each other, in an order that rotates with the round and the case.
- The 2-thread pass at M: restart both servers with `PROFILE_EXTRA='<max_threads>2</max_threads>'`, which reaches HTTP API queries through the default profile. Check the parts again, then run the same command.
- CPU next to wall for every target case: `share.sh 9001` and `share.sh 9002` with `RUNS=10 THREADS=2` and again with `THREADS=16`, `TAG=a2`/`b2`/`a16`/`b16`. They run the same SQL text on both servers through `prometheusQuery` / `prometheusQueryRange` … `FORMAT Null`. That is a different entry point from the HTTP API, so their CPU numbers are reported next to run.py's wall times, never mixed into one ratio.
- Guards: `perf.py` with both servers, as the CI comparison does.

## Metrics
- Wall time (median, p95) per case from `run.py`; the summarize table is `Label | Query | Output | Base | This PR | Change | p | Results`.
- From `share.sh` (`system.query_log` and `system.trace_log`): CPU (`OSCPUVirtualTimeMicroseconds`), `read_rows`, `memory_usage`, and the collector share (teardown, store, other collector code).
- The mechanism: the collector share on B (Stage 1, expected ~0) or the store's share (Stage 2, expected about half of A's).

## Correctness
- `run.py` records each body's `md5` and `values_md5`. `values_md5` hashes the series sorted by labels with values rounded to 12 significant digits, so it ignores series order and last-digit `sum()` differences. The A/A dry run gave "equal" on every case. Expected on B: "equal" or "equal (as a set)" on every case except R7.
- R7 can differ on B by design. The PR breaks topk ties by a key made of the tags, and gen.sh's `m` has only 7 distinct rates per step, so ties are everywhere. Check R7 by its series and point counts (10 series × 241 points in the summarize table) and leave its labels to the PR's own reference changes (`04811`).
- Stage 2 must give "equal" on every case, R7 included.

## Pass / fail
Stage 1 is a decision, not a pass of my branch:
- **Superseded (expected):** B's collector share of I1–I3 is about 0, and no R or I case is significantly slower than A by more than 5% at M or L. Then handoff #3 closes as superseded by upstream PR 132565: row 10 moves to "No A/B needed" and no branch gets written.
- **Superseded but slower:** the collector is gone, but a realistic case is significantly slower by more than 5% at M or L. The cost moved into the per-id tags join. The rows go to James; he decides whether to raise them on the PR, and lil red writes nothing upstream. The next study is that join, not the store.
- **Not superseded:** 132565 closes unmerged, or B still runs the collector on I1–I3. Go to Stage 2.

Stage 2, for the branch:
- **Pass:** I1–I3 improve by at least 5% in CPU and in wall time at M, at 2 and 16 threads, significant at 99.5%; no R case or guard is significantly slower; all results are "equal". The floor rule gives 0.4 × 3% = 1.2%, so the floor is the 5% minimum. That is above the low end of the prediction: a pass needs the trace share (15–20%) to be right, not the store-alone 5%.
- **Fail:** the store-alone number was right and the collector is too small a part of rule queries for a 2× to show. Close handoff #3 as "not worth it" and update the report's verdict.

## Checklist
- [ ] fetch PR 132565's merge ref; build A (its first parent) and B; record both shas, and whether the PR has merged
- [ ] generate S, M and L on both servers; record `system.parts` and size on disk
- [ ] run `run.py` at S, M and L (default threads) and at M with `max_threads = 2`; save the raw `*.jsonl` and tables under `bench/tags-collector-store-cost/ab/`
- [ ] run `share.sh` on both servers at M, 2 and 16 threads
- [ ] run the guard XMLs through `perf.py`
- [ ] decide Stage 1; update the report's verdict, the task, and backlog row 10
