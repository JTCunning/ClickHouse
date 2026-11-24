# Brief: where remote-write ingest spends its CPU at realistic scale

- **ID:** `ingest-at-scale` · **Area:** ingest · **Written:** 2026-11-24 by lil red, against master `05f159a69597`
- **Tracking issue:** none yet (see README, "Where the result goes")
- **Spend:** James's cap. Stop points below.

## Question

On today's master, at Prometheus-realistic scale, how much server CPU does one remote-written sample cost, and where does that CPU go? Which three levers would cut it most? Count only levers that no open upstream PR already covers. Settings count as levers.

## Why it matters

Ingest cost is the most visible bill of a Prometheus-compatible store: a 1M-active-series fleet at a 15 s scrape writes about 67k samples a second, all day, whatever the dashboards do. lil red's only baseline is from a 2 vCPU machine with one sender, three weeks of master ago. Every number below needs re-measuring at scale.

## What lil red already knows (verify, don't trust)

From `reports/2026-11-01-remote-write-baseline.md` (master `a7f713612c80`, 2 vCPU, one sender, Prometheus's default batch shape):

- **Cost:** about 20–26 µs of server CPU per sample. About half of it is background merges of the tiny parts that each request creates.
- **Write path:**
  - Async insert is on: each request waits 50–200 ms in the queue and is flushed with whatever else arrived.
  - A flush writes one part into each of 3–4 inner tables: tags (only for series the dedup cache hasn't seen), time ranges, samples, recent samples. That made 3.2 new parts per 2,000-series request.
  - The tags dedup cache saves about 27%.
- **Two levers measured then:**
  - The `ALP` value codec re-ran its parameter search on every 256-row granule (+16% ingest CPU). The fix is upstream PR 131497, still open.
  - The `LowCardinality(UUID)` half of the series id: +7%.

Since then, on master:

- The remote-write request is parsed into an arena (upstream PR 133485, merged 2026-11-23). Parse plus destroy fell from 12.9% to 4.0% of the profile, and CPU per sample by 9.5% with merges stopped (`reports/2026-11-21-remote-write-arena.md`).
- `timeSeriesGroupArray` writes its result without a per-sample copy (upstream PR 133502, merged 2026-11-23). This shows in merges and in inserts with `optimize_on_insert`.
- The recent samples table is on by default (`recent_samples_ttl_seconds = 345600`), so every sample is written twice: into samples and into recent samples. `0` turns it off.
- A truncated zstd request body answers 204 and drops the data. That is tracked upstream as issue 127714. Only note it if you hit it.

## Already in flight upstream (re-check each)

123144 (flush samples and recent samples in parallel), 121062 (active-series working-set cache in the sink), 121501 (skip recent-samples writes already expired on arrival), 125215 (a perf test for TimeSeries INSERT), 119734 (remote write v2, draft), 131497 (ALP search), 115220 (reject out-of-range timestamps), 125232 (503 on a timed-out write), 122797 (skip series without a name), 131384 (insert commit order), 93833 (routing by URL prefix).

## Method

1. **Realistic senders.**
   - Run real Prometheus servers that scrape synthetic targets and remote-write to ClickHouse: the protocol handler with a `remote_write` endpoint over a TimeSeries table. `bench/time-ranges-regex-ids/server-config.xml` shows the Prometheus handler config.
   - Use Prometheus's default `queue_config`. `prometheus-community/avalanche` or many node_exporter-like targets give series counts you can dial. Real scraping gives real timestamp jitter and real batch shapes. Record both.
   - `bench/remote-write-baseline/rw.py` is a synthetic sender, useful for controlled sweeps after the realistic baseline. Its batches are not Prometheus's: say so wherever you use it.
2. **Scale points.** As far as the machine allows: 100k, 1M and, if possible, 5M active series. 15 s scrape. 1, 4 and 16 Prometheus shards or senders. Add 1–5% series churn per hour (pods come and go). Run each point at least 30 minutes after a 10-minute warm-up, so merges reach a steady state.
3. **The breakdown.** For each scale point, report server CPU per sample, split into:
   - HTTP and decompression
   - protobuf parse
   - series id and tags hashing
   - the dedup caches
   - the sink, per inner table
   - part writes
   - background merges, per inner table

   Use `system.trace_log` with the CPU profiler on (`query_profiler_cpu_time_period_ns`), plus `system.query_log` ProfileEvents for the inserts and `system.part_log` for the merges. Also report the parts created per second per inner table, the active parts over time, and the merge backlog.
4. **Settings-only levers first.** Async insert flush size and timeout, `recent_samples_ttl_seconds = 0` (the cost of the second copy), the id type, and the TimeSeries table version. Report each lever's CPU per sample and its effect on the dashboard query set from `dashboard-query-profile.md`. A lever that makes queries slower must say so.
5. **Stop point 1.** If nothing outside the in-flight PRs is at least 5% of CPU per sample, stop here and report the breakdown.
6. **Code levers.** Rank the top three by share, with overlap checked. Prototype the best one if it's at least 10% and no open PR covers it. A/B it on the realistic setup: CPU per sample at a fixed ingest rate (not maximum throughput), plus the dashboard set as a guard.

## Deliverables

- A breakdown table per scale point: component | CPU µs/sample | share.
- A table of the settings levers: setting | CPU µs/sample | parts/s | dashboard set change.
- The top three code levers, each with its share, its overlap, and a one-line design.
- Optional: a prototype with its A/B table.

## Out of scope

Remote read, native histograms, exemplars, correctness bugs. Note any bug in one line under "Anything odd"; lil red routes it.
