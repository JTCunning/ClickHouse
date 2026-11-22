# A/B plan: one samples table with labelled tiers (L4) instead of samples + recent samples
**Experiment:** ../reports/2026-11-22-recent-samples-merge.md · **Branch:** none yet (two table definitions on one build; the engine-side mover would be the branch) · **Task:** weekend batch 2026-11-22, target 1
**Claim:** with a 1-day young window, L4 matches master's two-table setup on alert-rule and short-dashboard shapes (wide instant `rate`, `sum by (job)`, 1 h and 6 h ranges: within ±5%). It cuts insert CPU by 24–34% and the bytes written per insert by 34–44%. It costs ≤ 1.8× on narrow 7–14 d ranges and ≤ 1.7× on wide 1–4 d ranges.
**Evidence so far:** measured on 2 vCPU with fully merged fills (2000 series × 14 d, `bench/recent-samples-merge/results/`, `max_threads` 2 and one 8-thread check). This leaves the steady state with continuous ingest and background merges unproven, and scale (100k+ series) too. 2026-11-22 dry run on master `521c4bcb7b94`: the kit (`gen.sh`, `time.sh`, `lt.sh`, `ins.sh`, `tail.sh`) ran end to end from a clean data directory, the final versions of every script included. Preconditions: `m` recent samples 21 parts, `lt1d` 7 parts (6 young + 1 month). Results identical on all 15 shapes, also mid-move. Targets within ±7% of master, guards as in the Claim. No separate smoke file: the lab is the dry run.

## Builds
- A and B: the same master build (release). A = `CREATE TABLE ... ENGINE = TimeSeries` with defaults (samples + recent samples). B = the L4 DDL of `lt.sh` (recent off, `tier` column, `PARTITION BY tier`), plus the mover statements of report section 5 run every 15 minutes.

## Machine
- 16 vCPU, 64 GB: enough for 100k series and the 8-thread check of the short shapes. Nothing else running. Note the CPU model.

## Data
- Size 1 (the dry run): `SERIES=2000 gen.sh m` then `lt.sh lt1d 86400`, 14 d. Size 2: `SERIES=100000 gen.sh m` then `lt.sh lt1d 86400` (about 8 B samples per layout, ~25 GB each).
- Steady state (the new part): continuous remote-write ingest for 6 h into A and B (the `ins.sh` row shape: one sample per series per 15 s, batches of 2000 samples), with merges running, the B mover on, and `t_end` = now. Record `system.parts` per inner table every 15 min.

## Cases
| case | query (time.sh shape) | role | predicted A → B |
|---|---|---|---|
| T1 | `i_rate` (wide instant `rate(x[5m])`) | target | ±5% |
| T2 | `i_sum` | target | ±5% |
| T3 | `r1h_sum`, `r6h_sum` | target | ±5% |
| T4 | insert CPU (`ins.sh`, `one` and `ten`) | target | −24..−34% CPU, −34..−44% bytes written |
| G1 | `i_rate_n`, `r1h_n`, `r6h_n` | guard | ±5% |
| G2 | `r1d_sum`, `r1d_n` | guard | 0.84–0.93× (B faster) |
| E1 | `r3d_sum` | edge | 1.7× slower (the 3.5 d wide range leaves the young window) |
| E2 | `r7d_n`, `r14d_n` | edge | 1.6–1.8× slower |
| E3 | `r3d_n` | edge | 0.5× (master reads the recent table here) |

## Method
- One server per arm on copies of the same data directory (A on 9001, B on 9002). Short shapes at `EVAL_OFFSET=10800` (inside a full 5 h partition), mid and long at 0. `time.sh` runs the same query text on both (layout `m` on A, `lt1d` on B). For T1–T3, also `clickhouse benchmark --host 127.0.0.1 --port 9001 --host 127.0.0.1 --port 9002 -i 200 --confidence 5` on the SQL lines `time.sh` writes, i.e. 100 runs per server.
- Every case at `MAX_THREADS=2` and at `MAX_THREADS=8 EXTRA_SETTINGS='max_threads_min_free_memory_per_thread = 0'`. Report CPU next to wall.
- Check the layout before timing: `system.parts` counts as recorded after the fill (a restart doesn't change a fully merged layout). The steady-state run times while ingest runs, at minute 60, 180 and 360.

## Metrics
- Wall (median, p95), `OSCPUVirtualTimeMicroseconds`, `SelectedParts`, `SelectedMarks`, `read_bytes`, `ReadCompressedBytes` from `system.query_log`. Inserts: wall, CPU, `MergeTreeDataWriterCompressedBytes`, parts created. Merges and the mover: `system.part_log` (`MergeParts` duration and bytes, enabled in `config.xml.in`).

## Correctness
- `HASH=1 time.sh hash all <layout>` on A (`m`) and B (`lt1d`): row count and the sum of `cityHash64` over rows with values rounded to 6 decimals (order-independent, so no ORDER BY is involved). Every shape prints 2000, 40 or 10 rows on master. A against A (`m` twice) must match too. Repeat the hash once in the middle of a mover pass (`lt.sh ... hash`).

## Pass / fail
- **Pass:** T1–T3 not significantly slower at 99.5% and within ±5%; T4 at least −10% CPU (0.4× the low end of −24%); E1/E2 within their predicted ratios; hashes equal.
- **Fail:** if T1–T3 lose in the steady state, the newest data in the 5 h partitions is less clustered under live merges than in the fill. Then measure the young partition's part count and narrow the window. If E2 exceeds 2×, shorten the young window (6 h) and re-measure G2/E1.

## Checklist
- [ ] fill size 1 and size 2; record sizes on disk
- [ ] run every case; save the raw output under `bench/recent-samples-merge/ab/`
- [ ] steady-state run with ingest + mover
- [ ] update the report verdict and the backlog row
