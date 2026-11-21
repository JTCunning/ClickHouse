# A/B plan: remote-write ingest CPU per sample and its merge share, at 16 cores and 1–16 senders
**Experiment:** ../reports/2026-11-01-remote-write-baseline.md · **Branch:** none yet: verifies a report conclusion (one master build, no B) · **Task:** 2026-10-31-baseline-remote-write-ingest-and-the-ded
**Claim:** at Prometheus's default batch shape (2,000 series × 1 sample, 10 labels, warm tags cache) the server spends 20–30 µs of CPU per sample with merges running, background merges are 40–55% of it, and more concurrent senders cut CPU per sample (4 senders −10..−20% against 1, 16 senders at least as low as 4) because the async insert queue writes fewer, larger parts per request.
**Evidence so far:**
- 2026-11-01, 2 vCPU, master a7f713612c80: 22.4–25.8 µs (1 sender, warm), 19.1 µs (4 senders), merges 50% (28.3 → 13.6 µs with merges stopped); parts per request 3.2 (1 sender) and 1.4 (4 senders).
- 2026-11-21, 16 cores (James's remote-write-arena run, A side = master 521c4bcb7b94, 1 sender, 2,000 × 1, 10 labels): 14.40–14.80 µs with merges stopped and 29.10 µs (23.3–35.3) with merges running, so merges are about 49% at 16 cores with one sender. Parse plus destroy of the request was 12.9% of the server's CPU with merges stopped. That leaves the sender curve and the batch shapes unmeasured at scale.
- 2026-11-21 dry run on master `521c4bcb7b94` (`bench/remote-write-baseline/smoke-2026-11-21.txt`): kit ran end to end after two run.sh fixes (pid lookup, per-run log directory); row counts as stated below; 1 sender 25.0 µs with merges and 15.0 µs without (merges 40%), 4 senders 22.6 / 12.1 µs (46%), 4 vs 1 sender −9.5% (one round, senders sharing the 2 vCPU). Edited after the dry run, not re-run: profile.sql column rename and comment only.

## Builds
- A only: master, a release build (RelWithDebInfo, as for the other plans, or the `builds.clickhouse.com` master binary). Use 521c4bcb7b94 if it's still at hand, so the numbers line up with the remote-write-arena A side; otherwise record the sha. No B: the report proposes no code change here; its levers have their own rows (ALP search: row 5, upstream PR 131497; `LowCardinality(UUID)` id: row 7; request parse: row 21).

## Machine
- 32 vCPU or more, 32 GiB RAM or more: the server on 16 pinned cores, the senders on 16 other cores. The claim is about server CPU per sample (read from `/proc/<pid>/stat`), which the 2 vCPU runs could not separate from the senders' CPU. Note the CPU model. The 64 vCPU EPYC 9R14 host of the arena run fits.

## Data
- `rw.py` (stdlib only): 10 labels per series (100 metric names, 1,000 instances, 7 jobs, 7 labels of cardinality 10–10,000), samples 15 s apart, timestamps from 2 h before the run, so the recent samples table keeps them. Each sender cycles through its own 20,000 series (`--id-offset`).
- Every cell gets a fresh `prometheus` table with today's TimeSeries defaults (`scale.sh` drops and creates it), so each starts with an empty tags cache and no parts. Don't use the 2026-11-01 `variant-*.sql` files: they are frozen copies of that day's schema.
- Sizes: the batch shape is the size axis: 2,000 × 1 (Prometheus default), 10,000 × 1 (large shard), 500 × 20 (catch-up / backfill).

## Cases
| case | env for `scale.sh` | role | predicted |
|---|---|---|---|
| S1 | `SENDERS_LIST=1 MERGES_LIST=run` | target | 20–30 µs per sample, about 3.2 parts per request |
| S2 | `SENDERS_LIST=4 MERGES_LIST=run` | target | −10..−20% against S1, about 1.4 parts per request |
| S3 | `SENDERS_LIST=16 MERGES_LIST=run` | target | at or below S2; parts per request under 1 |
| M1–M3 | the same senders with `MERGES_LIST=stop` | target (merge share = 1 − stop / run) | 40–55% at 1 sender; the report expects the share to fall as batching grows, not measured |
| E1 | `SENDERS_LIST=64 MERGES_LIST=run REQUESTS=10` | edge | the async queue under load: latency and parts per request; observe only |
| B1 | `SENDERS_LIST=1 MERGES_LIST=run SERIES=10000` | guard (shape) | about 20 µs per sample (2026-11-01 cold: 19.8) |
| B2 | `SENDERS_LIST=1 MERGES_LIST=run SERIES=500 SPS=20` | guard (shape) | about 6–7 µs per sample (2026-11-01 cold: 6.6) |
| P1, P3 | S1 and S3 again on a server started with the CPU profiler (below), `ROUNDS=1` | shares | for each: merges, ALP, `LowCardinality`, protobuf, ZSTD, sipHash, dedup cache; 2026-11-01 at 1 sender: 50 / 12 / 12 / 10 / 6 / 3 / 2% |

There is no in-tree `tests/performance` XML for remote write. The shape guards stand in for one.

## Method
- One server, started with the kit's script and pinned:
  ```bash
  cd research/bench/remote-write-baseline
  D=<scratch dir>; export DATA=$D CH=<the build's clickhouse>
  SERVER_MEM=48000000000 QUERY_MEM=24000000000 taskset -c 0-15 ../remote-write-arena/server.sh start R 19001
  ```
- `mkdir -p ab`, then run each case under the sender cores, `ROUNDS=5` (the order of its cells rotates per round), for example:
  `SENDERS_LIST="1 4 16" MERGES_LIST="run stop" ROUNDS=5 taskset -c 16-31 ./scale.sh > ab/scale-<date>.txt`
  (S1–S3 and M1–M3 in one call), then E1, B1 and B2 as separate calls with their env.
- Each cell runs a cold pass, then the measured warm pass. `SETTLE=1` makes run.sh wait until `system.merges` is empty before and after a pass, so a pass's CPU includes the merges it caused and none from the cell before. Report the warm line; keep the cold line (first sight of the series) next to it.
- Profile cases: stop the server, start it again with `PROFILE_EXTRA='<query_profiler_cpu_time_period_ns>2000000</query_profiler_cpu_time_period_ns>'`, note `T0=$(date -u '+%F %T')`, run the cell, then `"$CH" client --port 19001 --allow_introspection_functions=1 --param_t0="$T0" --queries-file profile.sql`. One cell per profile. Its `async_flush_total` row is the whole flush (the insert pipeline runs under the queue's frames), not the report's 5% queue self-time.
- Stop the server with `server.sh stop R` (it checks that the pid is a clickhouse server).

## Metrics
- Per warm pass: `cpu_us/sample` (server utime + stime, merges included), `parts/req` (`MergeTreeDataWriterBlocks` per request), `samples/s`, `p50_lat`, `peakRSS`, all printed by run.sh. The median and min–max over the 5 rounds per cell.
- Merge share per sender count: 1 − median(stop) / median(run).
- Preconditions before timing: `parts/req` of S1 about 3.2 (between 3 and 4.5), and `async_q` equal to the number of requests in every line (each request went through the async insert queue).

## Correctness
- After a 2,000 × 1 cell with merges stopped, `SELECT table, sum(rows) FROM system.parts WHERE active AND database = currentDatabase() GROUP BY table` shows samples rows = senders × 2,000 × REQUESTS × 2 (cold + warm), the same count in time ranges and recent samples, and tags rows = senders × 20,000 (the warm pass writes no tags rows). The dry run printed 160,000 / 160,000 / 160,000 / 80,000 for 4 senders at `REQUESTS=10`.
- No failed request: run.sh prints `FAILED` and the senders' last log lines on any error.

## Pass / fail
- **Pass (the report holds at scale):** S1 is in 20–30 µs; the merge share at 1 sender is in 40–55%; S2 is at least 10% below S1 and S3 is not above S2, with the min–max ranges of the 5 rounds not overlapping S1's.
- **Fail:** CPU per sample outside 20–30 µs, or the share outside 40–55%: correct the report's header numbers with the 16-core ones. A flat sender curve: the batching lever (more remote-write shards, or a longer `async_insert_busy_timeout_min_ms`) is weaker at scale than the report says, and its "No-code lever" paragraph gets a dated correction. In both cases the profile shares (P1, P3) re-rank the follow-ups: the lever with the largest share at 16 senders goes first.

## Checklist
- [ ] build or download A; record the sha and the CPU model
- [ ] run S1–S3 with M1–M3, then E1, B1, B2; save the raw output under `bench/remote-write-baseline/ab/`
- [ ] run P1 and P3; save the profile output next to it
- [ ] update the report's header (verdict line) and this row of the backlog
