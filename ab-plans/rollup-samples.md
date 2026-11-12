# A/B plan: rollup columns of TimeSeries samples tables
**Experiment:** ../reports/2026-11-11-rollup-samples.md · **Branch:** `lilred-perf-rollup-samples` @ `6a1d8e48fae5` (James's `jtcunning/timeseries-rollup-samples` @ `daa53278310f` + 12 commits) · **Task:** 2026-11-05-handoff-timeseries-rollup-samples-table · **Issue:** https://github.com/JTCunning/ClickHouse/issues/16
**Harness:** `../bench/rollup-samples/ab/run.sh <branch binary> <master binary> <work dir>` (bash; needs only the two binaries and `awk`)
**Claim:** a PromQL range query whose instant selector is evaluated at a step that a rollup step divides reads `samples_<step>` and returns the same result as the raw samples. Over 7 and 30 days of 15 s scrapes the plain selector is at least 2× faster at a 5m step and at least 3× faster at 1h, with 4× and 20× fewer bytes read. Range functions (`rate`, `increase`, `avg_over_time`, `max_over_time`) read the raw samples and don't change. Rollup steps `'1m, 5m, 1h'` make inserts at most 25% slower and the samples table about 31% bigger; `'5m, 1h'` costs about 6% of storage.
**Evidence so far:** emulated on a master binary (rollups built in SQL from hourly rows, grid functions as PromQL calls them), 2 vCPU. Selector exact at every point for right-closed intervals; grid time at 1h ~29×, 5m ~5×, 1m ~2× (1000 series × 7 days, startup subtracted); column sizes 25.2% / 5.3% / 0.45% of `samples`. Unproven: the end-to-end query (tags lookup, planning, result), the cost of the sink's new pass, merges, and many cores.

## Arms (James's request, kept)
- **rollup:** branch binary, table `ab_rollup` (`samples_rollup_steps = '1m, 5m, 1h'`), `time_series_prefer_rollup_tables = 1`.
- **raw:** the same server, table and data, `time_series_prefer_rollup_tables = 0`.
- **master:** upstream master binary, table `ab_plain` without rollups, the same generated data. It measures insert cost and checks that the branch's raw path matches master.
- Insert cost only: on the branch, `ab_plain` (no rollups) and `ab_rollup_coarse` (`'5m, 1h'`).

## Builds
- A: master at the branch's merge target (the branch merges cleanly into `e8473460558a`). B: `lilred-perf-rollup-samples`. Release, no sanitizers. The test suite of the PR: `05508`, `05621`, the gtest `gtest_normalize_time_series_definition` (it needs `ENABLE_TESTS=ON`, which the first local build of the branch didn't have).

## Machine
- 16 vCPU, 64 GB RAM, local NVMe, nothing else running. The data is about 1 GB per server (2000 series × 30 days); both servers run at once, queries never overlap. Note the CPU model.

## Data
- Generator: in `run.sh` (SQL, `numbers()` and `sipHash64`). `SERIES` series (default 2000), half gauges `ab_gauge` (sine, noise, a rare +300 spike), half counters `ab_counter_total` (varying rate, a reset every 7–31 hours); labels `instance` (one per series), `job` (8), `dc` (4). One sample every 15 s for `DAYS` days (default 30) ending at the current hour: 345.6M samples.
- Timestamps `SHAPE=jittered` (default): a per-series offset in [0, 15 s) plus up to 40 ms, like Prometheus spreads its targets. `SHAPE=aligned`: exact multiples of 15 s, the shape where the left-closed intervals of `daa5327` were wrong at every evaluation time. Run both; aligned can use `DAYS=7`.
- Loading: one INSERT per hour of data and metric (all series, 240 samples each), the same statements on every server, then `OPTIMIZE ... FINAL`. Then 200 remote-write style INSERTs of one sample per series into a second table of the same settings.

## Cases
Steps 15s, 1m, 5m, 1h over windows 1d, 7d, 30d, skipping more than 11,000 points per series as Prometheus does (7d at 15s, 30d at 15s and 1m): 9 window/step pairs × 5 functions = 45 cases, all ending at the hour the data ends (start a multiple of every step).

| case | query | role | predicted rollup vs raw |
|---|---|---|---|
| `selector_{7d,30d}_1h` | `ab_gauge` | target | 3–20× faster p50, read_bytes ÷20–200 (reads `samples_3600`) |
| `selector_{7d,30d}_5m` | `ab_gauge` | target | 2–5× faster, read_bytes ÷4–20 (`samples_300`) |
| `selector_1d_{5m,1h}` | `ab_gauge` | target (edge) | ≥2× faster: the rollup column now wins over the recent samples table, which `raw` reads for a range within its 4-day TTL |
| `selector_{1d,7d}_1m` | `ab_gauge` | target | 1.25–2× faster, read_bytes ÷2–4 (`samples_60`) |
| `selector_1d_15s` | `ab_gauge` | guard | unchanged: no rollup step divides 15 s |
| edge run `SCRAPE=60 STEPS='1m' DAYS=7`: `selector_1d_1m` | `ab_gauge` | edge | not slower than 0.95×: the 1m column has every sample, and the rollup reads the main table where `raw` reads the recent table |
| `rate_*`, `increase_*`, `avg_over_time_*`, `max_over_time_*` | `rate(ab_counter_total[5m])`, `increase(ab_counter_total[1h])`, `avg_over_time(ab_gauge[1h])`, `max_over_time(ab_gauge[1h])` | guard | unchanged: range selectors read the raw samples |
| all, `raw` vs `master` | | guard | unchanged (the branch's raw path) |

`read_rows` stays the same in every case: a rollup column sits on the same `(id, bucket)` rows as `samples`, so the same rows are read with fewer bytes. The win shows in `read_bytes`, CPU and latency.

## Method
- `run.sh` starts both servers (ports 19101 and 19102, generated configs with `query_log` and `part_log`), loads the data into each, then runs every case `RUNS` times (default 20, after one warm-up run), interleaving rollup, raw and master for each case. Each query is tagged with `log_comment = 'ab-q:<case>:<arm>:<run>'` and measured from `system.query_log`.
- `max_threads` is set on every query (`MAX_THREADS`, default 16).
- For a case where the summary is within 10% of a threshold, rerun it with `clickhouse benchmark --host 127.0.0.1 --port 19101 --host 127.0.0.1 --port 19102 -i 50 --confidence 5` (the SQL is in `results/latency.tsv`'s case name; the query text is printed by `query_sql` in `run.sh`).

## Metrics
- Per case and arm: p50 and p95 `query_duration_ms`, median `read_rows`, `read_bytes`, `memory_usage`, `ProfileEvents['OSCPUVirtualTimeMicroseconds']` (`results/latency.tsv`, ratios in `results/summary.tsv`).
- Insert: total INSERT time and written bytes from `query_log` per table and arm (`results/insert_*.tsv`), wall time of the load and of `OPTIMIZE FINAL` (`results/load_wall.tsv`), new-part and merge time per inner table from `part_log` (`results/partlog_*.tsv`), average time of a remote-write style insert (`kind = small`).
- Storage: compressed and uncompressed bytes per inner table and column from `system.parts_columns` (`results/storage_*.tsv`): `samples`, `samples_60`, `samples_300`, `samples_3600`.

## Correctness
- Every case, `rollup` vs `raw` and `raw` vs `master`, compared point by point (`results/equality.tsv`, column `series_differ`):
  - instant selector: bit-identical (tolerance 0). The rollup keeps the last sample at or before every multiple of its step, which is all a selector evaluated there uses.
  - the range functions: identical too, they read the raw samples in both arms; compared with a relative tolerance of 1e-9 because the order of a float sum can change between runs.
- `05621` passes on B. The line `the last sample at or before a multiple of the step is kept` fails on `daa5327` (value 1 instead of 2).

## Pass / fail
- **Pass** (the `verdict` column of `results/summary.tsv`):
  - `equality.tsv` has 0 in `series_differ` on every line, for both shapes;
  - selector at 5m and 1h (1d, 7d, 30d): p50 at least 2× faster and `read_bytes` at least 4× lower;
  - selector at 1m: at least 1.25× faster and `read_bytes` at least 2× lower;
  - no guard more than 5% slower at p50 (`CHECK` lines get the `clickhouse benchmark` rerun; a significant slowdown fails);
  - insert of `ab_rollup` (`'1m, 5m, 1h'`): total INSERT time at most 1.25× `ab_plain` on the branch, remote-write style insert at most 1.2×, samples table at most 1.35× the bytes; `ab_rollup_coarse` (`'5m, 1h'`) at most 1.1× and 1.1×; `ab_plain` on the branch within 5% of master.
- **Fail:**
  - any `series_differ` > 0 on a selector case: the exactness argument is wrong, or the sink and the merges keep different samples. Withdraw the claim, look at which points differ (`results/out/<case>.*.tsv`).
  - a target below its floor: the end-to-end query is dominated by something else (the tags lookup, the selector's planning). Profile the case (`query_profiler_cpu_time_period_ns`) and update the report.
  - insert above its ceiling: the sink's pass or the extra columns' writes cost more than modeled. Compare `NewPart` and `MergeParts` times in `partlog_*.tsv`.

## Probe (not part of the pass rule): what range functions would get from a rollup
James asked about `rate`, `increase` and `*_over_time` over rollups. The branch keeps them on raw samples, because a last-sample rollup changes their results. The harness measures by how much and how much faster it would be: over the 7-day window at a 1h step, each function computed by the grid functions over `timeSeriesSelector(..., <rollup step>)` (which reads the rollup whatever the consumer) against the raw samples, for rollup steps 300 and 3600 (`results/probe.tsv`, `results/probe_latency.tsv`). Emulated on 2 vCPU: `rate(x[5m])` at a 5m step over `samples_300` is empty at 92–100% of points; `increase(x[1h])` at 1h over `samples_3600` is ~99% off or empty; over `samples_300` counters are 6% off at p50; `max_over_time` over `samples_300` misses spikes (p99 −66%); `avg_over_time` 1–3% off at p50, 21% at p99. These numbers decide whether a richer rollup (first, last, min, max, sum, count, counter resets) is worth a second version of the column type.

## Checklist
- [ ] build A and B; record both shas
- [ ] `run.sh <B> <A> <dir>`; then `SHAPE=aligned DAYS=7 run.sh <B> <A> <dir2>`; then `SCRAPE=60 STEPS='1m' DAYS=7 RUNS=10 run.sh <B> <A> <dir3>` (the edge run: look at `selector_1d_1m`)
- [ ] record the data size (`results/storage_*.tsv`)
- [ ] save `results/` of both runs under `bench/rollup-samples/ab/results-<date>/`
- [ ] fill the table of `prs/rollup-samples.md` and remove "pending"
- [ ] update the report verdict and the backlog row
