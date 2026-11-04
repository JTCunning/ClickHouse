# A/B plan: build the id set of a repeated PromQL selector once per query
**Experiment:** ../reports/2026-11-01-promql-shared-selector-sets.md · **Branch:** `lilred-perf-promql-shared-selector-sets` @ `15ec8bc23e76` (based on master `c189bd99fbda`) · **Task:** 2026-10-31-handoff-run-the-tags-lookup-once-per-di (handoff #5)
**Claim:** a selector repeated in one PromQL query builds its series-id set once instead of once per occurrence: about −5–7 ms per repeated occurrence (run 1: `x + x` 41.6 → 35.4 ms, −15%; `x + x + x` 59.1 → 48.6 ms, −18%; ~200-series `y + y` −12%), and no change for selectors used once or repeated with a different time window.
**Status:** this is the plan for **run 2**. Run 1 (64-vCPU host, 16 pinned cores, outbox status `results-in`) confirmed the mechanism but failed the old 8 ms-per-repeat floor, which came from the report's 12–14 ms prediction. That prediction counted per-query overhead as lookup cost; a hit removes only the lookup's execution, which is the ~6 ms run 1 measured. Run 2 re-checks with the corrected floor and settles the +1.1–1.8% seen on `promql_selector_pk_range.xml`.
**Evidence so far:** emulated on 2 vCPU / 4 GiB with master `a7f713612c80` (whole selector 26.5 ms, tags lookup alone 14 ms, lookup replaced by its literal ids 6 ms), then run 1 on real builds (numbers above). Still open: whether the pk_range guard's +1.1–1.8% is noise or a cost of the hash-map miss, the gtests `PreparedSetsCache.*` (need `ENABLE_TESTS=ON`), and mutations, which also get the early adoption.

## Builds
- A: master at `c189bd99fbda` (the branch base). B: `lilred-perf-promql-shared-selector-sets` @ `15ec8bc23e76`. If master has moved, rebase B and build A at the new base; record both shas.
- Release build, no sanitizers, same as the CI performance job. Also one debug or ASan build of B for the correctness section only (the hang review finding was a concurrency issue).

## Machine
- The run 1 host (64 vCPU, 16 pinned cores) again, so run 1 and run 2 compare directly; 8 vCPU / 16 GiB is the minimum (the claim is about work skipped per query, not parallelism). Run the target cases at `max_threads = 2` and `max_threads = 8`. Nothing else running; note the CPU model.

## Data
- **Perf-test dataset:** the `<create_query>` / `<fill_query>` of `tests/performance/promql_repeated_selector.xml` (from the branch) in database `perf`: 100,000 series over 50 metrics (2,000 each), `job` (10 values), `pod` (unique), 240 samples per series at 15 s, v11 table with default settings, timestamps in 1970.
- **Recent-data dataset** (the report's `ts` table): same shape with `instance` (500 values), samples ending at the last 15-minute boundary, loaded in 4 batches of 25,000 series with `max_insert_block_size = 4096`. Generator: `bench/selector-set-cache/gen_pod.sql`, `gen_job.sql`; commands in `bench/selector-set-cache/README.md`.
- Sizes: 10,000 series (small; the lookup is cheap, the saving should be a few ms), 100,000 (the report), 1,000,000 series over 50 metrics (the lookup grows with the tags table, so the saving should grow; check it does).

## Cases
| case | query | role | predicted A → B (run 1 host, 100k) |
|---|---|---|---|
| T1 | `metric_1{pod="pod-1"} + metric_1{pod="pod-1"}` | target | 41.6 → ~35.4 ms; sets built 2 → 1 |
| T2 | `metric_1{pod="pod-1"} + metric_1{pod="pod-1"} + metric_1{pod="pod-1"}` | target | 59.1 → ~48.6 ms; sets 3 → 1 |
| T3 | `metric_1{job="job3"} + metric_1{job="job3"}` (~200 series) | target | about −12%; sets 2 → 1 |
| T4 | `x / ignoring(pod) group_left x`, `x > 0 and x < 10` (x = T1's selector) | target | one lookup saved each; sets 2 → 1 |
| T5 | T1 through `/api/v1/query` (HTTP API entry point) | target | same saving as T1; sets 2 → 1 |
| G1 | `metric_1{pod="pod-1"}` | guard (single use) | unchanged |
| G2 | `metric_1{job="job3"}` | guard (single use, wider) | unchanged |
| G3 | `rate(x[5m]) / rate(x[5m] offset 10m)` | guard (two keys) | unchanged; sets 2 → 2 |
| G4 | `promql_selector_pk_range.xml`: `pkr_metric`, `sum(pkr_metric{dc=~".*"})`, `sum(rate(pkr_metric[30m]))`, `sum(pkr_metric{dc="0"})` | guard (in-tree; run 1 showed +1.1–1.8%) | unchanged: run it 3 times, interleaved, to tell noise from cost |
| G5 | `promql_selector_id_range_row_cost.xml`: the three `rowcost_metric` queries, `max_threads = 4` | guard (in-tree) | unchanged |
| G6 | `promql_tags_transform.xml`: `sum by() (foo)`, `sum by (namespace) (foo)`, `foo / on(pod) bar` | guard (in-tree; two different selectors in one query) | unchanged |
| G7 | `ALTER TABLE t DELETE WHERE k IN (SELECT k FROM s)` on a MergeTree with 20 parts, `mutations_sync = 1`; and the same with a TTL-delete merge | guard (mutations get the early adoption too) | equal or faster; same rows left |
| E1 | `x + x` where x matches 0 series | edge (empty set adopted) | equal or faster; same empty result |
| E2 | T2 with `max_threads = 1` and with `use_index_for_in_with_subqueries = 0` | edge (the never-wait path; the in-place build disabled) | no hang; with the index off, unchanged (no set built in place) |
| N1 | `rate(x[5m]) / rate(x[5m] offset 10m)`, `x - x offset 1h`; guard `x - x offset 1d` | next step, not this branch: share one id set built for the union of the windows (a superset of ids is safe because samples are still filtered by time) | G3's sets 2 → 1, ~same saving as T1; the 1d guard must not regress; a series present only in the earlier window must not appear in the later selector's output |
| E3 | 1,000 distinct single-use selectors in one query (`m1 + m2 + … `, generated) | edge (cache misses only) | unchanged within noise; a reviewer will ask about the hash-map cost |

## Method
- Two servers: A on port 9001, B on 9002, on copies of the same data directory, same config (query_log on), `max_threads` set explicitly per run.
- Warm-up 3 runs per case, then per case: `clickhouse benchmark --host 127.0.0.1 --port 9001 --host 127.0.0.1 --port 9002 -i 100 --confidence 5 < case.sql` (all target and guard queries are under 100 ms; use `-i 30` for the 1M-series size if they go above).
- The in-tree XMLs (`promql_repeated_selector.xml` from the branch, plus G4–G6) through `tests/performance/scripts/perf.py` against both servers, as the CI comparison does.
- G7: 10 runs per build, each on a freshly restored table copy; time from `system.mutations` / `system.part_log`.

## Metrics
- Wall time (median, p95); CPU (`ProfileEvents['OSCPUVirtualTimeMicroseconds']`); `read_rows`, `read_bytes`, `memory_usage` from `system.query_log`.
- The counter that proves the mechanism ran: `ProfileEvents['SetsBuiltFromSubquery']` (A: 2 / 3 / 2 for T1 / T2 / T3; B: 1 / 1 / 1), cross-checked with `Creating set, key:` lines at `send_logs_level = 'trace'`. `read_rows` on T1 should fall from 37,184 to about 18,592 plus the samples read.

## Correctness
- Identical results A vs B for every case: `... FORMAT TSV | md5sum` on both servers (G7: `SELECT * FROM t ORDER BY ALL FORMAT TSV | md5sum` after the mutation).
- `05481_promql_repeated_selector_shares_id_set` passes on B and fails on A (A prints 2, 3, 2, 2 for the set counts). `test_prometheus_protocols/test_query_api.py::test_repeated_selector_builds_id_set_once` passes on B. The gtest `gtest_prepared_sets_cache` passes on the debug/ASan build.
- E2 run 200 times in a loop on the debug build with `max_threads = 1` and `2`: no hang (bound each run with `max_execution_time = 10`).

## Pass / fail
- **Pass:** `05481` prints 1, 1, 2, 2 (only those four rows); T1–T3 each improve by at least 4 ms per repeated occurrence and are significant at 99.5%; `SetsBuiltFromSubquery` is 1 on B for T1–T5; no guard case (G1–G7) or E3 is significantly slower; E2 never hangs.
- **Fail:** if T1–T3 save less than 4 ms per repeat, the planning of the skipped subquery or the set adoption costs more than the emulation showed: profile B with `perf` on T2, and either skip planning the subquery on a hit or withdraw the branch and update the report verdict. If a guard is slower in all 3 interleaved runs (G4 especially), find which path pays (the hash-map miss, or mutations waiting differently) before anything goes upstream. A hang in E2 blocks the branch outright.

## Checklist
- [ ] build A and B (release), and B debug/ASan; record the shas
- [ ] generate the 10k, 100k and 1M datasets; record their size on disk
- [ ] run every case at 2 and 8 threads; save the raw output under `bench/selector-set-cache/ab/`
- [ ] fill the branch column of `prs/promql-shared-selector-sets.md` and remove "pending build"
- [ ] update the report verdict and the backlog row

## Run 3 (confirm, 2026-11-04)
Run 2 passed on the numbers (6.5–8.2 ms per repeat, guards flat). The branch was then rebased onto master `fd328d4baedf` (conflict in the HTTP API entry point) and got one review fix (`b56cfb0261aa`: the PromQL query always gets its own non-waiting cache). Run 3 only confirms: the tests (05481 on A and B, the integration test, `PreparedSetsCache.*` if a test build is at hand), `promql_repeated_selector.xml` and the `promql_selector_pk_range.xml` guard, same host and method as run 2. Pass: the run 2 pass rule.
